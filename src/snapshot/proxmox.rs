use std::fmt;
use std::path::PathBuf;

use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::http::validate_https_endpoint;
use super::{
    ApiAuth, BoxFuture, HttpMethod, HttpRequest, HttpTransport, ProviderCapabilities,
    SnapshotHandle, SnapshotProvider, SnapshotRequest, PROVIDER_TIMEOUT,
};
use crate::error::{Error, Result};

const SNAPSHOT_PREFIX: &str = "zerostun-";

#[derive(Clone)]
pub struct ProxmoxConfig {
    pub endpoint: String,
    pub node: String,
    pub vmid: u32,
    pub token_id: String,
    pub auth: ApiAuth,
}

impl ProxmoxConfig {
    pub fn load_token(&self) -> Result<String> {
        self.auth.load()
    }
}

impl fmt::Debug for ProxmoxConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxmoxConfig")
            .field("endpoint", &self.endpoint)
            .field("node", &self.node)
            .field("vmid", &self.vmid)
            .field("token_id", &self.token_id)
            .field("auth", &self.auth)
            .finish()
    }
}

#[derive(Clone)]
pub struct ProxmoxProvider<T> {
    transport: T,
    config: ProxmoxConfig,
}

impl<T> ProxmoxProvider<T> {
    pub fn new(transport: T, config: ProxmoxConfig) -> Self {
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

impl<T: HttpTransport + Clone> ProxmoxProvider<T> {
    async fn send(
        &self,
        method: HttpMethod,
        path: String,
        body: Option<String>,
        cancel: &CancellationToken,
    ) -> Result<Vec<u8>> {
        validate_https_endpoint(&self.config.endpoint, "Proxmox")?;
        validate_token_id(&self.config.token_id)?;
        let token = self.config.load_token()?;
        let request = HttpRequest {
            method,
            path,
            body,
            headers: vec![(
                "Authorization".to_string(),
                format!(
                    "PVEAPIToken={token_id}={token}",
                    token_id = self.config.token_id
                ),
            )],
            secret_values: vec![token],
        };
        Ok(self
            .transport
            .send(&request, PROVIDER_TIMEOUT, cancel)
            .await?
            .body)
    }

    fn node(&self) -> Result<&str> {
        validate_ident(&self.config.node, "Proxmox node")
    }

    fn vm_root(&self) -> Result<String> {
        Ok(format!(
            "/api2/json/nodes/{node}/qemu/{vmid}",
            node = self.node()?,
            vmid = self.config.vmid
        ))
    }

    fn snapshot_collection(&self) -> Result<String> {
        Ok(format!("{}/snapshot", self.vm_root()?))
    }

    fn snapshot_item(&self, id: &str) -> Result<String> {
        let id = validate_managed_id(id)?;
        Ok(format!("{}/snapshot/{id}", self.vm_root()?))
    }

    fn expected_source(&self, id: &str) -> Result<PathBuf> {
        let id = validate_managed_id(id)?;
        Ok(PathBuf::from(format!(
            "/dev/pve/vm-{vmid}-state-{id}",
            vmid = self.config.vmid
        )))
    }

    fn validate_handle(&self, handle: &SnapshotHandle) -> Result<()> {
        let expected = self.expected_source(&handle.id)?;
        if handle.source != expected {
            return Err(Error::Snapshot(
                "Proxmox snapshot handle source does not match its derived path".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_target<'a>(&self, target: &'a str) -> Result<&'a str> {
        if target != self.config.vmid.to_string() {
            return Err(Error::Snapshot(
                "Proxmox target must be the configured numeric VMID".to_string(),
            ));
        }
        Ok(target)
    }

    async fn probe_capabilities(&self, cancel: &CancellationToken) -> Result<()> {
        let status = self
            .send(
                HttpMethod::Get,
                format!("{}/status/current", self.vm_root()?),
                None,
                cancel,
            )
            .await?;
        parse_vm_status(&status, self.config.vmid)?;
        let storage = self
            .send(
                HttpMethod::Get,
                format!("/api2/json/nodes/{node}/storage", node = self.node()?),
                None,
                cancel,
            )
            .await?;
        parse_storage(&storage)
    }
}

impl<T: HttpTransport + Clone + 'static> SnapshotProvider for ProxmoxProvider<T> {
    fn probe<'a>(
        &'a self,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ProviderCapabilities>> {
        Box::pin(async move {
            self.probe_capabilities(cancel).await?;
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
            self.probe_capabilities(cancel).await?;
            let id = new_managed_name()?;
            let body = format!("snapname={id}");
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
            parse_snapshot_name(&body, &handle.id)?;
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
            parse_snapshot_name(&body, &handle.id)?;
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
                parse_snapshot_name(&owned, id)?;
                self.send(HttpMethod::Delete, item, None, cancel).await?;
            }
            Ok(ids)
        })
    }

    fn capabilities(&self) -> ProviderCapabilities {
        Self::supported_capabilities()
    }
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct VmStatus {
    vmid: u32,
    #[serde(default)]
    status: String,
}

#[derive(Debug, Deserialize)]
struct StorageRow {
    #[serde(default)]
    storage: String,
    #[serde(default, rename = "type")]
    storage_type: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    active: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct SnapshotRow {
    name: String,
}

fn parse_json<T: serde::de::DeserializeOwned>(bytes: &[u8], what: &str) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| Error::Snapshot(format!("invalid {what} JSON response: {error}")))
}

fn parse_vm_status(bytes: &[u8], vmid: u32) -> Result<()> {
    let parsed: Envelope<VmStatus> = parse_json(bytes, "Proxmox VM status")?;
    if parsed.data.vmid != vmid {
        return Err(Error::Snapshot(
            "Proxmox VM status did not match the configured VMID".to_string(),
        ));
    }
    if parsed.data.status.is_empty() {
        return Err(Error::Snapshot(
            "Proxmox VM status did not include a status field".to_string(),
        ));
    }
    Ok(())
}

fn parse_storage(bytes: &[u8]) -> Result<()> {
    let parsed: Envelope<Vec<StorageRow>> = parse_json(bytes, "Proxmox storage")?;
    if parsed.data.is_empty() {
        return Err(Error::Snapshot(
            "Proxmox storage probe returned no storage entries".to_string(),
        ));
    }
    if !parsed.data.iter().any(|row| {
        !row.storage.is_empty()
            && matches!(
                row.storage_type.as_str(),
                "lvmthin" | "zfspool" | "rbd" | "qcow2"
            )
            && row
                .content
                .split(',')
                .any(|item| item == "images" || item == "rootdir")
            && match &row.active {
                serde_json::Value::Bool(true) => true,
                serde_json::Value::Number(value) => value.as_u64() == Some(1),
                _ => false,
            }
    }) {
        return Err(Error::Snapshot(
            "Proxmox storage probe found no active snapshot-capable storage".to_string(),
        ));
    }
    Ok(())
}

fn parse_snapshot_name(bytes: &[u8], expected: &str) -> Result<()> {
    let parsed: Envelope<SnapshotRow> = parse_json(bytes, "Proxmox snapshot")?;
    if parsed.data.name != expected {
        return Err(Error::Snapshot(
            "Proxmox snapshot name did not match the handle".to_string(),
        ));
    }
    Ok(())
}

fn parse_managed_snapshots(bytes: &[u8]) -> Result<Vec<String>> {
    let parsed: Envelope<Vec<SnapshotRow>> = parse_json(bytes, "Proxmox snapshot list")?;
    parsed
        .data
        .into_iter()
        .filter(|row| row.name.starts_with(SNAPSHOT_PREFIX))
        .map(|row| validate_managed_id(&row.name).map(str::to_string))
        .collect()
}

fn validate_token_id(value: &str) -> Result<&str> {
    if value.is_empty()
        || value.bytes().any(|byte| byte < 0x20)
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'@' | b'!' | b'.' | b'_' | b'-')
        })
    {
        return Err(Error::Snapshot(
            "Proxmox token identifier contains unsafe characters".to_string(),
        ));
    }
    Ok(value)
}

fn validate_ident<'a>(value: &'a str, what: &str) -> Result<&'a str> {
    if value.is_empty()
        || value == "."
        || value == ".."
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
            "Proxmox snapshot identifier is not ZeroStun-managed".to_string(),
        ));
    }
    Ok(id)
}

fn new_managed_name() -> Result<String> {
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random).map_err(|error| {
        Error::Snapshot(format!(
            "failed to generate Proxmox snapshot identifier: {error}"
        ))
    })?;
    Ok(format!("{SNAPSHOT_PREFIX}{}", hex::encode(random)))
}
