use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

mod runner;

pub use runner::{
    redact_text, BoxFuture, CommandOutput, CommandRunner, CommandSpec, FakeRunner,
    FakeRunnerScript, ProcessRunner, RecordedCommand, MAX_COMMAND_OUTPUT_BYTES,
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

pub trait SnapshotProvider: Send + Sync {
    fn probe<'a>(
        &'a self,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ProviderCapabilities>>;
    fn create<'a>(&'a self, cancel: &'a CancellationToken)
        -> BoxFuture<'a, Result<SnapshotHandle>>;
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
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<SnapshotHandle>> {
        Box::pin(async move {
            let output = self
                .run_provider(
                    CommandSpec::new(PROVIDER_PROGRAM)
                        .arg("--create")
                        .arg(PROVIDER_TARGET),
                    cancel,
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
            if source.is_empty() {
                return Err(Error::Snapshot(
                    "snapshot open returned an empty source".to_string(),
                ));
            }
            Ok(PathBuf::from(source))
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
            Ok(vec![text])
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
