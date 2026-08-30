use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

mod http;
mod lvm;
mod proxmox;
mod runner;
mod stratus;
mod zfs;

pub use http::{
    ApiAuth, FakeHttpScript, FakeHttpTransport, HttpMethod, HttpRequest, HttpResponse,
    HttpTransport, MAX_HTTP_BODY_BYTES,
};
pub use lvm::LvmProvider;
pub use proxmox::{ProxmoxConfig, ProxmoxProvider};
pub use runner::{
    redact_text, BoxFuture, CommandOutput, CommandRunner, CommandSpec, FakeRunner,
    FakeRunnerScript, ProcessRunner, RecordedCommand, MAX_COMMAND_OUTPUT_BYTES,
};
pub use stratus::{ApplianceKind, StratusConfig, StratusProvider};
pub use zfs::{ZfsProvider, ZfsTargetCapabilities, ZfsTargetKind};

pub(crate) const PROVIDER_TIMEOUT: Duration = Duration::from_secs(5);
const PROVIDER_PROGRAM: &str = "/usr/bin/zst-probe";
const PROVIDER_TARGET: &str = "volume-a";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotHandle {
    pub id: String,
    pub source: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRequest {
    pub target: String,
    #[serde(default)]
    require_quiesce: bool,
    #[serde(default)]
    require_changed_block: bool,
}

impl SnapshotRequest {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            require_quiesce: false,
            require_changed_block: false,
        }
    }

    pub fn require_quiesce(mut self) -> Self {
        self.require_quiesce = true;
        self
    }

    pub fn require_changed_block(mut self) -> Self {
        self.require_changed_block = true;
        self
    }

    pub(crate) fn validate_requirements(&self, capabilities: ProviderCapabilities) -> Result<()> {
        if self.require_quiesce && !capabilities.quiesce {
            return Err(Error::Snapshot(
                "snapshot provider does not support requested quiesce semantics".to_string(),
            ));
        }
        if self.require_changed_block && !capabilities.changed_block {
            return Err(Error::Snapshot(
                "snapshot provider does not support requested changed-block semantics".to_string(),
            ));
        }
        Ok(())
    }

    fn validated_target(&self) -> Result<&str> {
        if self.target.is_empty() || self.target.contains('\0') {
            return Err(Error::Snapshot(
                "snapshot target must be non-empty and contain no NUL byte".to_string(),
            ));
        }
        Ok(&self.target)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub crash_consistent: bool,
    pub read_only: bool,
    pub quiesce: bool,
    pub changed_block: bool,
}

pub trait SnapshotProvider: Send + Sync {
    fn probe<'a>(
        &'a self,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ProviderCapabilities>>;
    fn create<'a>(
        &'a self,
        request: &'a SnapshotRequest,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<SnapshotHandle>>;
    fn open_source<'a>(
        &'a self,
        handle: &'a SnapshotHandle,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<PathBuf>>;
    fn cleanup<'a>(
        &'a self,
        handle: &'a SnapshotHandle,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>>;
    fn recover<'a>(&'a self, cancel: &'a CancellationToken) -> BoxFuture<'a, Result<Vec<String>>>;
    fn capabilities(&self) -> ProviderCapabilities;
}

#[derive(Debug, Clone)]
pub struct FakeProvider<R> {
    runner: R,
}

impl<R> FakeProvider<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }

    fn fake_capabilities() -> ProviderCapabilities {
        ProviderCapabilities {
            crash_consistent: true,
            read_only: true,
            quiesce: false,
            changed_block: false,
        }
    }
}

impl<R: CommandRunner + Clone> FakeProvider<R> {
    pub async fn probe_with_spec(
        &self,
        spec: CommandSpec,
        cancel: &CancellationToken,
    ) -> Result<ProviderCapabilities> {
        self.runner.run(&spec, PROVIDER_TIMEOUT, cancel).await?;
        Ok(Self::fake_capabilities())
    }

    async fn run_provider(
        &self,
        spec: CommandSpec,
        cancel: &CancellationToken,
    ) -> Result<CommandOutput> {
        self.runner.run(&spec, PROVIDER_TIMEOUT, cancel).await
    }
}

impl<R: CommandRunner + Clone + 'static> SnapshotProvider for FakeProvider<R> {
    fn probe<'a>(
        &'a self,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ProviderCapabilities>> {
        Box::pin(async move {
            self.run_provider(
                CommandSpec::new(PROVIDER_PROGRAM)
                    .arg("--target")
                    .arg(PROVIDER_TARGET),
                cancel,
            )
            .await?;
            Ok(self.capabilities())
        })
    }

    fn create<'a>(
        &'a self,
        request: &'a SnapshotRequest,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<SnapshotHandle>> {
        Box::pin(async move {
            let target = request.validated_target()?;
            let output = self
                .run_provider(
                    CommandSpec::new(PROVIDER_PROGRAM)
                        .arg("--create")
                        .arg(target),
                    cancel,
                )
                .await?;
            let id = utf8_stdout(&output.stdout)?;
            let id = validate_snapshot_id(&id)?;
            Ok(SnapshotHandle {
                source: PathBuf::from(format!("/dev/mapper/{id}")),
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
            let output = self
                .run_provider(
                    CommandSpec::new(PROVIDER_PROGRAM)
                        .arg("--open")
                        .arg(&handle.id),
                    cancel,
                )
                .await?;
            let source = utf8_stdout(&output.stdout)?;
            validate_source_path(&source)
        })
    }

    fn cleanup<'a>(
        &'a self,
        handle: &'a SnapshotHandle,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.run_provider(
                CommandSpec::new(PROVIDER_PROGRAM)
                    .arg("--cleanup")
                    .arg(&handle.id),
                cancel,
            )
            .await?;
            Ok(())
        })
    }

    fn recover<'a>(&'a self, cancel: &'a CancellationToken) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(async move {
            let output = self
                .run_provider(CommandSpec::new(PROVIDER_PROGRAM).arg("--recover"), cancel)
                .await?;
            let text = utf8_stdout(&output.stdout)?;
            if text.is_empty() || text == "[]" {
                return Ok(Vec::new());
            }
            Ok(vec![validate_snapshot_id(&text)?])
        })
    }

    fn capabilities(&self) -> ProviderCapabilities {
        Self::fake_capabilities()
    }
}

fn utf8_stdout(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec())
        .map(|value| value.trim().to_string())
        .map_err(|_| Error::Snapshot("snapshot command stdout was not UTF-8".to_string()))
}

fn validate_snapshot_id(id: &str) -> Result<String> {
    if id.is_empty() {
        return Err(Error::Snapshot(
            "snapshot create returned an empty identifier".to_string(),
        ));
    }
    if id.contains('/') || id.contains('\\') || id.contains('\0') || id == "." || id == ".." {
        return Err(Error::Snapshot(
            "snapshot identifier contains unsafe path characters".to_string(),
        ));
    }
    Ok(id.to_string())
}

fn validate_source_path(source: &str) -> Result<PathBuf> {
    if source.is_empty() {
        return Err(Error::Snapshot(
            "snapshot open returned an empty source".to_string(),
        ));
    }
    if source.contains('\0') {
        return Err(Error::Snapshot(
            "snapshot source path contains a NUL byte".to_string(),
        ));
    }
    let path = PathBuf::from(source);
    if !path.is_absolute() {
        return Err(Error::Snapshot(
            "snapshot source path must be absolute".to_string(),
        ));
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(Error::Snapshot(
            "snapshot source path must not contain parent-directory segments".to_string(),
        ));
    }
    Ok(path)
}
