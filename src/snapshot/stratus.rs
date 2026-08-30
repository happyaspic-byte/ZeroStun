use std::path::PathBuf;

use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::http::{prepare_http_request, validate_https_endpoint};
use super::{
    ApiAuth, BoxFuture, HttpMethod, HttpRequest, HttpTransport, ProviderCapabilities,
    SnapshotHandle, SnapshotProvider, SnapshotRequest, PROVIDER_TIMEOUT,
};
use crate::error::{Error, Result};

const SNAPSHOT_PREFIX: &str = "zerostun-";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplianceKind {
    EverRun,
    ZtC,
}

#[derive(Debug, Clone)]
pub struct StratusConfig {
    pub endpoint: String,
    pub appliance: ApplianceKind,
    pub workload: String,
    pub auth: ApiAuth,
}

#[derive(Clone)]
pub struct StratusProvider<T> {
    transport: T,
    config: StratusConfig,
}

impl<T> StratusProvider<T> {
    pub fn new(transport: T, config: StratusConfig) -> Self {
        Self { transport, config }
    }

    fn supported_capabilities() -> ProviderCapabilities {
        ProviderCapabilities {
            crash_consistent: true,
            read_only: true,
            quiesce: false,
            changed_block: false,
        }
    }
}

impl<T: HttpTransport + Clone> StratusProvider<T> {
    async fn send(
        &self,
        method: HttpMethod,
        path: String,
        body: Option<String>,
        cancel: &CancellationToken,
    ) -> Result<Vec<u8>> {
        validate_https_endpoint(&self.config.endpoint, "Stratus")?;
        let token = self.config.auth.load()?;
        let request = HttpRequest {
            method,
            endpoint: self.config.endpoint.clone(),
            path,
            body,
            headers: vec![("Authorization".to_string(), format!("Bearer {token}"))],
            secret_values: vec![token.clone()],
        };
        prepare_http_request(&request)?;
        let body = self
            .transport
            .send(&request, PROVIDER_TIMEOUT, cancel)
            .await?
            .body;
        Ok(body)
    }

    fn root(&self) -> &'static str {
        match self.config.appliance {
            ApplianceKind::EverRun => "/everrun/api/v1",
            ApplianceKind::ZtC => "/ztc/api/v1",
        }
    }

    fn status_path(&self) -> &'static str {
        match self.config.appliance {
            ApplianceKind::EverRun => "/everrun/api/v1/pair/status",
            ApplianceKind::ZtC => "/ztc/api/v1/cluster/status",
        }
    }

    fn workload(&self) -> Result<&str> {
        validate_ident(&self.config.workload, "Stratus workload")
    }

    fn snapshot_collection(&self) -> Result<String> {
        Ok(format!(
            "{root}/workloads/{workload}/snapshots",
            root = self.root(),
            workload = self.workload()?
        ))
    }

    fn snapshot_item(&self, id: &str) -> Result<String> {
        let id = validate_managed_id(id)?;
        Ok(format!("{}/{id}", self.snapshot_collection()?))
    }

    fn expected_source(&self, id: &str) -> Result<PathBuf> {
        let id = validate_managed_id(id)?;
        Ok(PathBuf::from(format!(
            "/dev/stratus/{workload}/{id}",
            workload = self.workload()?
        )))
    }

    fn validate_handle(&self, handle: &SnapshotHandle) -> Result<()> {
        let expected = self.expected_source(&handle.id)?;
        if handle.source != expected {
            return Err(Error::Snapshot(
                "Stratus snapshot handle source does not match its derived path".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_target<'a>(&self, target: &'a str) -> Result<&'a str> {
        let workload = self.workload()?;
        if target != workload {
            return Err(Error::Snapshot(
                "Stratus target must match the configured workload identifier".to_string(),
            ));
        }
        Ok(target)
    }

    async fn fetch_health(&self, cancel: &CancellationToken) -> Result<Health> {
        let body = self
            .send(
                HttpMethod::Get,
                self.status_path().to_string(),
                None,
                cancel,
            )
            .await?;
        parse_health(&body, self.config.appliance)
    }
}

impl<T: HttpTransport + Clone + 'static> SnapshotProvider for StratusProvider<T> {
    fn probe<'a>(
        &'a self,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ProviderCapabilities>> {
        Box::pin(async move {
            self.fetch_health(cancel).await?;
            Ok(Self::supported_capabilities())
        })
    }

    fn create<'a>(
        &'a self,
        request: &'a SnapshotRequest,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<SnapshotHandle>> {
        Box::pin(async move {
            request.validate_requirements(Self::supported_capabilities())?;
            self.validate_target(&request.target)?;
            let health = self.fetch_health(cancel).await?;
            if !health.synchronized {
                return Err(Error::Snapshot(
                    "refusing snapshot while FT synchronization is unsafe".to_string(),
                ));
            }
            let id = new_managed_name()?;
            let body = format!(r#"{{"name":"{id}","read_only":true}}"#);
            self.send(
                HttpMethod::Post,
                self.snapshot_collection()?,
                Some(body),
                cancel,
            )
            .await?;
            Ok(SnapshotHandle {
                source: self.expected_source(&id)?,
                id,
            })
        })
    }

    fn open_source<'a>(
        &'a self,
        handle: &'a SnapshotHandle,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<PathBuf>> {
        Box::pin(async move {
            self.validate_handle(handle)?;
            let body = self
                .send(
                    HttpMethod::Get,
                    self.snapshot_item(&handle.id)?,
                    None,
                    cancel,
                )
                .await?;
            parse_open_source(&body, &handle.source)?;
            Ok(handle.source.clone())
        })
    }

    fn cleanup<'a>(
        &'a self,
        handle: &'a SnapshotHandle,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.validate_handle(handle)?;
            let item = self.snapshot_item(&handle.id)?;
            let body = self
                .send(HttpMethod::Get, item.clone(), None, cancel)
                .await?;
            parse_open_source(&body, &handle.source)?;
            self.send(HttpMethod::Delete, item, None, cancel).await?;
            Ok(())
        })
    }

    fn recover<'a>(&'a self, cancel: &'a CancellationToken) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(async move {
            let body = self
                .send(HttpMethod::Get, self.snapshot_collection()?, None, cancel)
                .await?;
            let mut ids = parse_managed_snapshots(&body)?;
            ids.sort();
            ids.reverse();
            for id in &ids {
                let item = self.snapshot_item(id)?;
                let owned = self
                    .send(HttpMethod::Get, item.clone(), None, cancel)
                    .await?;
                parse_open_source(&owned, &self.expected_source(id)?)?;
                self.send(HttpMethod::Delete, item, None, cancel).await?;
            }
            Ok(ids)
        })
    }

    fn capabilities(&self) -> ProviderCapabilities {
        Self::supported_capabilities()
    }
}

struct Health {
    synchronized: bool,
}

#[derive(Debug, Deserialize)]
struct EverRunStatus {
    appliance: String,
    pair: EverRunPair,
    ft: EverRunFt,
}

#[derive(Debug, Deserialize)]
struct EverRunPair {
    nodes: Vec<String>,
    #[serde(default)]
    sync: String,
}

#[derive(Debug, Deserialize)]
struct EverRunFt {
    #[serde(default)]
    state: String,
    synchronized: bool,
}

#[derive(Debug, Deserialize)]
struct ZtcStatus {
    appliance: String,
    cluster: ZtcCluster,
    ha: ZtcHa,
}

#[derive(Debug, Deserialize)]
struct ZtcCluster {
    nodes: Vec<String>,
    #[serde(default)]
    quorum: bool,
}

#[derive(Debug, Deserialize)]
struct ZtcHa {
    #[serde(default)]
    synchronized: bool,
    #[serde(default)]
    state: String,
}

#[derive(Debug, Deserialize)]
struct OpenBody {
    #[serde(default)]
    source: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SnapshotList {
    snapshots: Vec<String>,
}

fn parse_json<T: serde::de::DeserializeOwned>(bytes: &[u8], what: &str) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|_| Error::Snapshot(format!("invalid {what} JSON response")))
}

fn parse_health(bytes: &[u8], kind: ApplianceKind) -> Result<Health> {
    match kind {
        ApplianceKind::EverRun => {
            let parsed: EverRunStatus = parse_json(bytes, "everRun pair status")?;
            if parsed.appliance != "everrun" {
                return Err(Error::Snapshot(
                    "everRun status appliance field did not match the configured kind".to_string(),
                ));
            }
            if parsed.pair.nodes.len() < 2 {
                return Err(Error::Snapshot(
                    "everRun pair status did not include a node pair".to_string(),
                ));
            }
            Ok(Health {
                synchronized: parsed.pair.sync == "synchronized"
                    && parsed.ft.synchronized
                    && parsed.ft.state == "protected",
            })
        }
        ApplianceKind::ZtC => {
            let parsed: ZtcStatus = parse_json(bytes, "ztC cluster status")?;
            if parsed.appliance != "ztc" {
                return Err(Error::Snapshot(
                    "ztC status appliance field did not match the configured kind".to_string(),
                ));
            }
            if parsed.cluster.nodes.len() < 2 {
                return Err(Error::Snapshot(
                    "ztC cluster status did not include a node pair".to_string(),
                ));
            }
            Ok(Health {
                synchronized: parsed.cluster.quorum
                    && parsed.ha.synchronized
                    && parsed.ha.state == "protected",
            })
        }
    }
}

fn parse_open_source(bytes: &[u8], expected: &std::path::Path) -> Result<()> {
    let parsed: OpenBody = parse_json(bytes, "Stratus snapshot source")?;
    let source = parsed.source.ok_or_else(|| {
        Error::Snapshot("Stratus snapshot is not a verified owned source".to_string())
    })?;
    if PathBuf::from(source) != expected {
        return Err(Error::Snapshot(
            "Stratus snapshot source did not match the derived handle path".to_string(),
        ));
    }
    Ok(())
}

fn parse_managed_snapshots(bytes: &[u8]) -> Result<Vec<String>> {
    let parsed: SnapshotList = parse_json(bytes, "Stratus snapshot list")?;
    parsed
        .snapshots
        .into_iter()
        .filter(|name| name.starts_with(SNAPSHOT_PREFIX))
        .map(|name| validate_managed_id(&name).map(str::to_string))
        .collect()
}

fn validate_ident<'a>(value: &'a str, what: &str) -> Result<&'a str> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Error::Snapshot(format!(
            "{what} contains unsafe path characters"
        )));
    }
    Ok(value)
}

fn validate_managed_id(id: &str) -> Result<&str> {
    if !id.starts_with(SNAPSHOT_PREFIX)
        || id.len() <= SNAPSHOT_PREFIX.len()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(Error::Snapshot(
            "Stratus snapshot identifier is not ZeroStun-managed".to_string(),
        ));
    }
    Ok(id)
}

fn new_managed_name() -> Result<String> {
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random).map_err(|error| {
        Error::Snapshot(format!(
            "failed to generate Stratus snapshot identifier: {error}"
        ))
    })?;
    Ok(format!("{SNAPSHOT_PREFIX}{}", hex::encode(random)))
}
