use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::{BoxFuture, MAX_COMMAND_OUTPUT_BYTES};
use crate::error::{Error, Result};

pub const MAX_HTTP_BODY_BYTES: usize = MAX_COMMAND_OUTPUT_BYTES;
const MAX_TOKEN_BYTES: u64 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Delete,
}

#[derive(Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub path: String,
    pub body: Option<String>,
    pub headers: Vec<(String, String)>,
    pub secret_values: Vec<String>,
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("path", &redact_secrets(&self.secret_values, &self.path))
            .field(
                "body",
                &self
                    .body
                    .as_ref()
                    .map(|body| redact_secrets(&self.secret_values, body)),
            )
            .field("headers", &redacted_headers(self))
            .field("secret_values", &["[redacted]"])
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

pub trait HttpTransport: Send + Sync {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        timeout: Duration,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<HttpResponse>>;
}

#[derive(Debug, Clone)]
pub enum FakeHttpScript {
    Ok { status: u16, body: Vec<u8> },
    Fail { status: u16, body: Vec<u8> },
    Hang { duration: Duration },
}

impl FakeHttpScript {
    pub fn ok(body: impl AsRef<[u8]>) -> Self {
        Self::Ok {
            status: 200,
            body: body.as_ref().to_vec(),
        }
    }

    pub fn fail(status: u16, body: impl AsRef<[u8]>) -> Self {
        Self::Fail {
            status,
            body: body.as_ref().to_vec(),
        }
    }

    pub fn hang(duration: Duration) -> Self {
        Self::Hang { duration }
    }
}

impl From<Vec<u8>> for FakeHttpScript {
    fn from(body: Vec<u8>) -> Self {
        Self::ok(body)
    }
}

#[derive(Debug, Clone)]
pub struct FakeHttpTransport {
    scripts: Arc<Mutex<VecDeque<FakeHttpScript>>>,
    recorded: Arc<Mutex<Vec<HttpRequest>>>,
}

impl FakeHttpTransport {
    pub fn scripted(scripts: impl IntoIterator<Item = impl Into<FakeHttpScript>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into_iter().map(Into::into).collect())),
            recorded: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn failing(status: u16, body: impl AsRef<[u8]>) -> Self {
        Self::scripted([FakeHttpScript::fail(status, body)])
    }

    pub fn hang(duration: Duration) -> Self {
        Self::scripted([FakeHttpScript::hang(duration)])
    }

    pub fn recorded(&self) -> Vec<HttpRequest> {
        self.recorded
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

impl HttpTransport for FakeHttpTransport {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        timeout: Duration,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<HttpResponse>> {
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            validate_request_path(&request.path)?;
            reject_secrets_in_body(request)?;
            reject_secrets_in_path(request)?;

            {
                let mut recorded = self
                    .recorded
                    .lock()
                    .map_err(|_| Error::Snapshot("http transport lock poisoned".to_string()))?;
                recorded.push(redacted_request(request));
            }

            let script = {
                let mut scripts = self
                    .scripts
                    .lock()
                    .map_err(|_| Error::Snapshot("http transport lock poisoned".to_string()))?;
                scripts.pop_front().ok_or_else(|| {
                    Error::Snapshot("no scripted HTTP response remaining".to_string())
                })?
            };

            match script {
                FakeHttpScript::Hang { duration } => {
                    tokio::select! {
                        _ = cancel.cancelled() => Err(Error::Cancelled),
                        _ = sleep(timeout) => Err(Error::Snapshot(
                            "snapshot request timed out".to_string(),
                        )),
                        _ = sleep(duration) => Err(Error::Snapshot(
                            "snapshot request hung".to_string(),
                        )),
                    }
                }
                FakeHttpScript::Ok { status, body } => finish_http(request, status, body),
                FakeHttpScript::Fail { status, body } => finish_http(request, status, body),
            }
        })
    }
}

impl HttpTransport for Arc<dyn HttpTransport> {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        timeout: Duration,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<HttpResponse>> {
        (**self).send(request, timeout, cancel)
    }
}

#[derive(Clone)]
pub enum ApiAuth {
    File(PathBuf),
    Env(String),
}

impl ApiAuth {
    pub fn load(&self) -> Result<String> {
        match self {
            Self::File(path) => load_token_file(path),
            Self::Env(var) => {
                let value = std::env::var(var).map_err(|_| {
                    Error::Snapshot(
                        "API token is missing; use an environment value or a mode-0600 file"
                            .to_string(),
                    )
                })?;
                require_token(value.trim())
            }
        }
    }
}

impl fmt::Debug for ApiAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
            Self::Env(var) => f.debug_tuple("Env").field(var).finish(),
        }
    }
}

fn load_token_file(path: &std::path::Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(Error::Snapshot(
            "API token file must not be a symlink".to_string(),
        ));
    }
    if !metadata.is_file() || metadata.file_type().is_fifo() {
        return Err(Error::Snapshot(
            "API token path must be a regular file".to_string(),
        ));
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode != 0o600 {
        return Err(Error::Snapshot(format!(
            "API token file mode must be 0600, found {mode:04o}"
        )));
    }
    if metadata.len() > MAX_TOKEN_BYTES {
        return Err(Error::Snapshot(
            "API token file exceeds the 4 KiB maximum".to_string(),
        ));
    }
    let token = fs::read_to_string(path)?;
    if token.contains('\0') {
        return Err(Error::Snapshot(
            "API token must not contain a NUL byte".to_string(),
        ));
    }
    require_token(token.trim())
}

pub(crate) fn validate_https_endpoint(endpoint: &str, what: &str) -> Result<()> {
    let remainder = endpoint.strip_prefix("https://").ok_or_else(|| {
        Error::Snapshot(format!(
            "{what} endpoint must use https without credentials"
        ))
    })?;
    if remainder.is_empty()
        || remainder.contains('@')
        || remainder.contains('#')
        || remainder.contains('\\')
        || remainder.contains('\0')
        || remainder.contains("://")
        || remainder.contains('/')
        || remainder.contains('?')
        || remainder.contains(':') && remainder.matches(':').count() > 1
        || remainder.bytes().any(|byte| byte < 0x20)
    {
        return Err(Error::Snapshot(format!(
            "{what} endpoint must be an https origin without userinfo or fragment"
        )));
    }
    Ok(())
}

fn reject_secrets_in_path(request: &HttpRequest) -> Result<()> {
    if request
        .secret_values
        .iter()
        .any(|secret| !secret.is_empty() && request.path.contains(secret))
    {
        return Err(Error::Snapshot(
            "API token must not appear in request paths".to_string(),
        ));
    }
    Ok(())
}

fn reject_secrets_in_body(request: &HttpRequest) -> Result<()> {
    let Some(body) = &request.body else {
        return Ok(());
    };
    if request
        .secret_values
        .iter()
        .any(|secret| !secret.is_empty() && body.contains(secret))
    {
        return Err(Error::Snapshot(
            "API token must not appear in request bodies".to_string(),
        ));
    }
    Ok(())
}

fn validate_request_path(path: &str) -> Result<()> {
    if !path.starts_with('/')
        || path.contains("://")
        || path.contains('\\')
        || path.contains('\0')
        || path.bytes().any(|byte| byte < 0x20)
        || path
            .split('/')
            .any(|component| component == "." || component == "..")
    {
        return Err(Error::Snapshot(
            "HTTP request path must be a relative origin-free path".to_string(),
        ));
    }
    Ok(())
}

fn require_token(token: &str) -> Result<String> {
    if token.is_empty() {
        return Err(Error::Snapshot(
            "API token is missing; use an environment value or a mode-0600 file".to_string(),
        ));
    }
    Ok(token.to_string())
}

fn finish_http(request: &HttpRequest, status: u16, body: Vec<u8>) -> Result<HttpResponse> {
    if !(200..300).contains(&status) {
        let body_text = bounded_lossy(&body);
        return Err(Error::Snapshot(format!(
            "snapshot request failed with status {status}: {}",
            redact_secrets(&request.secret_values, &body_text)
        )));
    }
    if body.len() > MAX_HTTP_BODY_BYTES {
        return Err(Error::Snapshot(
            "snapshot request body exceeds bounded maximum".to_string(),
        ));
    }
    Ok(HttpResponse { status, body })
}

fn bounded_lossy(bytes: &[u8]) -> String {
    let slice = if bytes.len() > MAX_HTTP_BODY_BYTES {
        &bytes[..MAX_HTTP_BODY_BYTES]
    } else {
        bytes
    };
    String::from_utf8_lossy(slice).into_owned()
}

fn redact_secrets(secrets: &[String], text: &str) -> String {
    let mut out = text.to_string();
    for secret in secrets {
        if !secret.is_empty() {
            out = out.replace(secret, "[redacted]");
        }
    }
    out
}

fn redacted_headers(request: &HttpRequest) -> Vec<(String, String)> {
    request
        .headers
        .iter()
        .map(|(key, value)| {
            if key.eq_ignore_ascii_case("authorization") {
                (key.clone(), "[redacted]".to_string())
            } else {
                (key.clone(), redact_secrets(&request.secret_values, value))
            }
        })
        .collect()
}

fn redacted_request(request: &HttpRequest) -> HttpRequest {
    HttpRequest {
        method: request.method,
        path: redact_secrets(&request.secret_values, &request.path),
        body: request
            .body
            .as_ref()
            .map(|body| redact_secrets(&request.secret_values, body)),
        headers: redacted_headers(request),
        secret_values: Vec::new(),
    }
}
