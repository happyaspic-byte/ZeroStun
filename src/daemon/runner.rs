use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::info;

use super::{DaemonState, JobConfig, ManualClock, RunRecord, RunStatus};
use crate::error::{Error, Result};
use crate::snapshot::{SnapshotHandle, SnapshotProvider, SnapshotRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    Transient,
    Configuration,
    Integrity,
    Unsupported,
    Cancelled,
}

pub fn classify_error(error: &Error) -> FailureClass {
    match error {
        Error::Cancelled => FailureClass::Cancelled,
        Error::InvalidConfig(_) | Error::InvalidDaemonConfig(_) | Error::InvalidIdentifier(_) => {
            FailureClass::Configuration
        }
        Error::ChunkCorrupt { .. }
        | Error::ChunkMissing { .. }
        | Error::ManifestCorrupt(_)
        | Error::RootHashMismatch { .. } => FailureClass::Integrity,
        Error::Snapshot(message) if message.contains("unsupported") => FailureClass::Unsupported,
        Error::Io(error) if error.kind() == std::io::ErrorKind::TimedOut => FailureClass::Transient,
        Error::Io(_) => FailureClass::Transient,
        Error::Snapshot(message) if message.contains("timed out") => FailureClass::Transient,
        _ => FailureClass::Transient,
    }
}

pub struct DaemonRunner<P> {
    provider: Arc<P>,
    clock: ManualClock,
}

impl<P: SnapshotProvider> DaemonRunner<P> {
    pub fn new(provider: Arc<P>, clock: ManualClock) -> Self {
        Self { provider, clock }
    }

    pub async fn run_job(
        &self,
        state: &DaemonState,
        job: &JobConfig,
        cancel: &CancellationToken,
    ) -> Result<RunRecord> {
        let started = self.clock.now_unix_ms();
        let mut record = state.admit_run(&job.id, started)?;
        record = state.transition(&record.run_id, RunStatus::Running, started, None)?;

        let mut handle: Option<SnapshotHandle> = None;
        let mut attempts = 0_u32;
        let mut outcome: Result<()>;
        loop {
            attempts = attempts.saturating_add(1);
            if cancel.is_cancelled() {
                outcome = Err(Error::Cancelled);
                break;
            }
            match self
                .provider
                .create(&SnapshotRequest::new(&job.target), cancel)
                .await
            {
                Ok(created) => {
                    handle = Some(created);
                    outcome = Ok(());
                    break;
                }
                Err(error) => {
                    let class = classify_error(&error);
                    let delays = job.retry.delays_ms(class);
                    let retry_index = attempts.saturating_sub(1) as usize;
                    if class == FailureClass::Transient && retry_index < delays.len() {
                        tokio::time::sleep(Duration::from_millis(delays[retry_index])).await;
                        continue;
                    }
                    outcome = Err(error);
                    break;
                }
            }
        }

        if let Some(created) = handle.as_ref() {
            state.record_snapshot(&record.run_id, &created.id)?;
            record.snapshot_id = created.id.clone();
            if outcome.is_ok() {
                if let Err(error) = self.provider.open_source(created, cancel).await {
                    outcome = Err(error);
                }
            }
        }

        let mut cleanup_outcome = None;
        if let Some(created) = handle.as_ref() {
            match self
                .provider
                .cleanup(created, &CancellationToken::new())
                .await
            {
                Ok(()) => cleanup_outcome = Some("succeeded".to_string()),
                Err(_) => {
                    cleanup_outcome = Some("recoverable_failure".to_string());
                    let _ = state.record_cleanup_failure(
                        &record.run_id,
                        &job.id,
                        &created.id,
                        self.clock.now_unix_ms(),
                    );
                }
            }
        }

        let finished = self.clock.now_unix_ms();
        record.attempts = attempts;
        record.duration_ms = finished.saturating_sub(started);
        record.finished_unix_ms = Some(finished);
        record.cleanup_outcome = cleanup_outcome.clone();
        match outcome {
            Ok(()) if cleanup_outcome.as_deref() == Some("succeeded") => {
                record.status = RunStatus::Succeeded;
            }
            Ok(()) => {
                record.status = RunStatus::Failed;
                record.failure_class = Some("transient".to_string());
            }
            Err(error) => {
                let class = classify_error(&error);
                record.failure_class = Some(format!("{class:?}").to_ascii_lowercase());
                record.status = match class {
                    FailureClass::Cancelled => RunStatus::Cancelled,
                    _ => RunStatus::Failed,
                };
            }
        }

        state.finish_run(&record)?;
        info!(
            job_id = %job.id,
            run_id = %record.run_id,
            provider = %job.provider,
            bytes = record.bytes_processed,
            duration_ms = record.duration_ms,
            dedupe_bytes = record.dedupe_bytes,
            limiter_wait_ms = record.limiter_wait_ms,
            cleanup_outcome = record.cleanup_outcome.as_deref().unwrap_or("none"),
            status = ?record.status,
            "daemon run committed"
        );
        Ok(record)
    }
}
