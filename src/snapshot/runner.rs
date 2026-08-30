use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

pub const MAX_COMMAND_OUTPUT_BYTES: usize = 32 * 1024;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub secret_env: BTreeSet<String>,
}

impl CommandSpec {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
            secret_env: BTreeSet::new(),
        }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    pub fn secret_env(mut self, key: impl Into<String>) -> Self {
        self.secret_env.insert(key.into());
        self
    }

    fn env_for_debug(&self) -> Vec<(&str, &str)> {
        self.env
            .iter()
            .map(|(key, value)| {
                if self.secret_env.contains(key) && !value.is_empty() {
                    (key.as_str(), "[redacted]")
                } else {
                    (key.as_str(), value.as_str())
                }
            })
            .collect()
    }
}

impl fmt::Debug for CommandSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandSpec")
            .field("program", &self.program)
            .field("args", &self.args)
            .field("env", &self.env_for_debug())
            .field("secret_env", &self.secret_env)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub status: i32,
}

pub trait CommandRunner: Send + Sync {
    fn run<'a>(
        &'a self,
        spec: &'a CommandSpec,
        timeout: Duration,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<CommandOutput>>;
}

#[derive(Debug, Clone)]
pub enum FakeRunnerScript {
    Ok {
        stdout: Vec<u8>,
    },
    Fail {
        code: i32,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    Hang {
        duration: Duration,
    },
}

impl FakeRunnerScript {
    pub fn ok(stdout: impl AsRef<[u8]>) -> Self {
        Self::Ok {
            stdout: stdout.as_ref().to_vec(),
        }
    }

    pub fn fail(code: i32, stdout: impl AsRef<[u8]>, stderr: impl AsRef<[u8]>) -> Self {
        Self::Fail {
            code,
            stdout: stdout.as_ref().to_vec(),
            stderr: stderr.as_ref().to_vec(),
        }
    }

    pub fn hang(duration: Duration) -> Self {
        Self::Hang { duration }
    }
}

#[derive(Debug, Clone)]
pub struct FakeRunner {
    scripts: Arc<Mutex<VecDeque<FakeRunnerScript>>>,
    recorded: Arc<Mutex<Vec<RecordedCommand>>>,
}

impl FakeRunner {
    pub fn scripted(scripts: impl IntoIterator<Item = FakeRunnerScript>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into_iter().collect())),
            recorded: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn recorded(&self) -> Vec<RecordedCommand> {
        self.recorded
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

impl CommandRunner for FakeRunner {
    fn run<'a>(
        &'a self,
        spec: &'a CommandSpec,
        timeout: Duration,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<CommandOutput>> {
        Box::pin(async move {
            {
                let mut recorded = self
                    .recorded
                    .lock()
                    .map_err(|_| Error::Snapshot("runner lock poisoned".to_string()))?;
                recorded.push(RecordedCommand {
                    program: spec.program.clone(),
                    args: spec.args.clone(),
                });
            }

            let script = {
                let mut scripts = self
                    .scripts
                    .lock()
                    .map_err(|_| Error::Snapshot("runner lock poisoned".to_string()))?;
                scripts
                    .pop_front()
                    .ok_or_else(|| Error::Snapshot("no scripted command remaining".to_string()))?
            };

            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }

            match script {
                FakeRunnerScript::Hang { duration } => {
                    tokio::select! {
                        _ = cancel.cancelled() => Err(Error::Cancelled),
                        _ = sleep(timeout) => Err(Error::Snapshot("snapshot command timed out".to_string())),
                        _ = sleep(duration) => Err(Error::Snapshot("snapshot command hung".to_string())),
                    }
                }
                FakeRunnerScript::Ok { stdout } => finish_output(spec, 0, stdout, Vec::new()),
                FakeRunnerScript::Fail {
                    code,
                    stdout,
                    stderr,
                } => finish_output(spec, code, stdout, stderr),
            }
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessRunner;

impl ProcessRunner {
    pub fn new() -> Self {
        Self
    }
}

impl CommandRunner for ProcessRunner {
    fn run<'a>(
        &'a self,
        spec: &'a CommandSpec,
        timeout: Duration,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<CommandOutput>> {
        Box::pin(run_process(spec, timeout, cancel))
    }
}

impl CommandRunner for Arc<dyn CommandRunner> {
    fn run<'a>(
        &'a self,
        spec: &'a CommandSpec,
        timeout: Duration,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<CommandOutput>> {
        (**self).run(spec, timeout, cancel)
    }
}

pub fn redact_text(spec: &CommandSpec, text: &str) -> String {
    let mut out = text.to_string();
    for key in &spec.secret_env {
        for (_, value) in spec.env.iter().filter(|(candidate, _)| candidate == key) {
            if !value.is_empty() {
                out = out.replace(value, "[redacted]");
            }
        }
        if let Ok(value) = std::env::var(key) {
            if !value.is_empty() {
                out = out.replace(&value, "[redacted]");
            }
        }
    }
    out
}

fn finish_output(
    spec: &CommandSpec,
    status: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) -> Result<CommandOutput> {
    if stdout.len() > MAX_COMMAND_OUTPUT_BYTES || stderr.len() > MAX_COMMAND_OUTPUT_BYTES {
        return Err(Error::Snapshot(
            "snapshot command output exceeds bounded maximum".to_string(),
        ));
    }
    if status != 0 {
        let stdout_text = String::from_utf8_lossy(&stdout);
        let stderr_text = String::from_utf8_lossy(&stderr);
        let combined = format!("{stdout_text}{stderr_text}");
        return Err(Error::Snapshot(format!(
            "snapshot command failed with status {status}: {}",
            redact_text(spec, &combined)
        )));
    }
    Ok(CommandOutput {
        stdout,
        stderr,
        status,
    })
}

fn bounded_output_error() -> Error {
    Error::Snapshot("snapshot command output exceeds bounded maximum".to_string())
}

async fn reap(child: &mut tokio::process::Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

async fn run_process(
    spec: &CommandSpec,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<CommandOutput> {
    if cancel.is_cancelled() {
        return Err(Error::Cancelled);
    }

    let mut command = Command::new(&spec.program);
    command.args(&spec.args);
    for (key, value) in &spec.env {
        command.env(key, value);
    }
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.kill_on_drop(true);

    let mut child = command.spawn().map_err(|err| {
        Error::Snapshot(format!("failed to spawn {}: {err}", spec.program.display()))
    })?;
    let mut stdout = child.stdout.take().ok_or_else(|| {
        Error::Snapshot("snapshot command stdout pipe was not captured".to_string())
    })?;
    let mut stderr = child.stderr.take().ok_or_else(|| {
        Error::Snapshot("snapshot command stderr pipe was not captured".to_string())
    })?;

    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    let mut stdout_chunk = [0_u8; 4096];
    let mut stderr_chunk = [0_u8; 4096];
    let mut stdout_done = false;
    let mut stderr_done = false;
    let deadline = sleep(timeout);
    tokio::pin!(deadline);

    loop {
        if stdout_done && stderr_done {
            break;
        }
        tokio::select! {
            _ = cancel.cancelled() => {
                reap(&mut child).await;
                return Err(Error::Cancelled);
            }
            _ = &mut deadline => {
                reap(&mut child).await;
                return Err(Error::Snapshot("snapshot command timed out".to_string()));
            }
            result = stdout.read(&mut stdout_chunk), if !stdout_done => {
                match result {
                    Ok(0) => stdout_done = true,
                    Ok(n) => {
                        if stdout_buf.len().saturating_add(n) > MAX_COMMAND_OUTPUT_BYTES {
                            reap(&mut child).await;
                            return Err(bounded_output_error());
                        }
                        stdout_buf.extend_from_slice(&stdout_chunk[..n]);
                    }
                    Err(err) => {
                        reap(&mut child).await;
                        return Err(Error::Snapshot(format!(
                            "failed to read snapshot command stdout: {err}"
                        )));
                    }
                }
            }
            result = stderr.read(&mut stderr_chunk), if !stderr_done => {
                match result {
                    Ok(0) => stderr_done = true,
                    Ok(n) => {
                        if stderr_buf.len().saturating_add(n) > MAX_COMMAND_OUTPUT_BYTES {
                            reap(&mut child).await;
                            return Err(bounded_output_error());
                        }
                        stderr_buf.extend_from_slice(&stderr_chunk[..n]);
                    }
                    Err(err) => {
                        reap(&mut child).await;
                        return Err(Error::Snapshot(format!(
                            "failed to read snapshot command stderr: {err}"
                        )));
                    }
                }
            }
        }
    }

    let status = tokio::select! {
        _ = cancel.cancelled() => {
            reap(&mut child).await;
            return Err(Error::Cancelled);
        }
        _ = &mut deadline => {
            reap(&mut child).await;
            return Err(Error::Snapshot("snapshot command timed out".to_string()));
        }
        result = child.wait() => result.map_err(|err| {
            Error::Snapshot(format!("failed to wait for snapshot command: {err}"))
        })?,
    };
    let code = status.code().unwrap_or(-1);
    finish_output(spec, code, stdout_buf, stderr_buf)
}
