use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

mod runner;

pub use runner::{
    redact_text, CommandOutput, CommandRunner, CommandSpec, FakeRunner, FakeRunnerScript,
    RecordedCommand, MAX_COMMAND_OUTPUT_BYTES,
};

const PROVIDER_TIMEOUT: Duration = Duration::from_secs(5);
const PROVIDER_PROGRAM: &str = "/usr/bin/zst-probe";
const PROVIDER_TARGET: &str = "volume-a";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotHandle {
    pub id: String,
    pub source: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub crash_consistent: bool,
    pub read_only: bool,
    pub quiesce: bool,
    pub changed_block: bool,
}

pub trait SnapshotProvider {
    fn probe(&self) -> impl std::future::Future<Output = Result<ProviderCapabilities>> + Send;
    fn create(&self) -> impl std::future::Future<Output = Result<SnapshotHandle>> + Send;
    fn open_source(
        &self,
        handle: &SnapshotHandle,
    ) -> impl std::future::Future<Output = Result<PathBuf>> + Send;
    fn cleanup(
        &self,
        handle: &SnapshotHandle,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn recover(&self) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;
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
}

impl<R: CommandRunner + Clone> FakeProvider<R> {
    pub async fn probe_with_spec(&self, spec: CommandSpec) -> Result<ProviderCapabilities> {
        self.runner
            .run(&spec, PROVIDER_TIMEOUT, &CancellationToken::new())
            .await?;
        Ok(self.capabilities())
    }

    async fn run_provider(&self, spec: CommandSpec) -> Result<CommandOutput> {
        self.runner
            .run(&spec, PROVIDER_TIMEOUT, &CancellationToken::new())
            .await
    }
}

impl<R: CommandRunner + Clone> SnapshotProvider for FakeProvider<R> {
    async fn probe(&self) -> Result<ProviderCapabilities> {
        self.run_provider(
            CommandSpec::new(PROVIDER_PROGRAM)
                .arg("--target")
                .arg(PROVIDER_TARGET),
        )
        .await?;
        Ok(self.capabilities())
    }

    async fn create(&self) -> Result<SnapshotHandle> {
        let output = self
            .run_provider(
                CommandSpec::new(PROVIDER_PROGRAM)
                    .arg("--create")
                    .arg(PROVIDER_TARGET),
            )
            .await?;
        let id = utf8_stdout(&output.stdout)?;
        if id.is_empty() {
            return Err(Error::Snapshot(
                "snapshot create returned an empty identifier".to_string(),
            ));
        }
        Ok(SnapshotHandle {
            source: PathBuf::from(format!("/dev/mapper/{id}")),
            id,
        })
    }

    async fn open_source(&self, handle: &SnapshotHandle) -> Result<PathBuf> {
        let output = self
            .run_provider(
                CommandSpec::new(PROVIDER_PROGRAM)
                    .arg("--open")
                    .arg(&handle.id),
            )
            .await?;
        let source = utf8_stdout(&output.stdout)?;
        if source.is_empty() {
            return Err(Error::Snapshot(
                "snapshot open returned an empty source".to_string(),
            ));
        }
        Ok(PathBuf::from(source))
    }

    async fn cleanup(&self, handle: &SnapshotHandle) -> Result<()> {
        self.run_provider(
            CommandSpec::new(PROVIDER_PROGRAM)
                .arg("--cleanup")
                .arg(&handle.id),
        )
        .await?;
        Ok(())
    }

    async fn recover(&self) -> Result<Vec<String>> {
        let output = self
            .run_provider(CommandSpec::new(PROVIDER_PROGRAM).arg("--recover"))
            .await?;
        let text = utf8_stdout(&output.stdout)?;
        if text.is_empty() || text == "[]" {
            return Ok(Vec::new());
        }
        Ok(vec![text])
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            crash_consistent: true,
            read_only: true,
            quiesce: false,
            changed_block: false,
        }
    }
}

fn utf8_stdout(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec())
        .map(|value| value.trim().to_string())
        .map_err(|_| Error::Snapshot("snapshot command stdout was not UTF-8".to_string()))
}
