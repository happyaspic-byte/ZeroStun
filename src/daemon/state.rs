use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::JobConfig;
use crate::error::{Error, Result};

pub const DAEMON_JOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("daemon_jobs");
pub const DAEMON_RUNS: TableDefinition<&str, &[u8]> = TableDefinition::new("daemon_runs");
pub const DAEMON_SCHEDULE: TableDefinition<&str, u64> = TableDefinition::new("daemon_schedule");
pub const DAEMON_RECOVERY: TableDefinition<&str, &[u8]> = TableDefinition::new("daemon_recovery");
pub const DAEMON_STATE_VERSION: TableDefinition<&str, u64> =
    TableDefinition::new("daemon_state_version");

pub const DAEMON_STATE_FORMAT_VERSION: u64 = 1;
pub const MAX_STATE_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Recovering,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    pub job_id: String,
    pub snapshot_id: String,
    pub status: RunStatus,
    pub admitted_unix_ms: u64,
    pub finished_unix_ms: Option<u64>,
    pub failure_class: Option<String>,
    pub cleanup_outcome: Option<String>,
    pub bytes_processed: u64,
    pub dedupe_bytes: u64,
    pub limiter_wait_ms: u64,
    pub duration_ms: u64,
    pub attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanupRecovery {
    pub recovery_id: String,
    pub run_id: String,
    pub job_id: String,
    pub snapshot_id: String,
    pub recorded_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonMetrics {
    pub jobs: u64,
    pub runs_queued: u64,
    pub runs_running: u64,
    pub runs_succeeded: u64,
    pub runs_failed: u64,
    pub runs_cancelled: u64,
    pub runs_recovering: u64,
    pub snapshots_cleaned: u64,
    pub cleanup_failures: u64,
    pub bytes_processed: u64,
    pub dedupe_bytes: u64,
    pub limiter_wait_ms: u64,
}

#[derive(Debug)]
pub struct DaemonState {
    db: Arc<Database>,
}

impl DaemonState {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path)?;
        {
            let write = db.begin_write()?;
            {
                let mut version = write.open_table(DAEMON_STATE_VERSION)?;
                let _ = version.insert("version", DAEMON_STATE_FORMAT_VERSION)?;
                let _ = write.open_table(DAEMON_JOBS)?;
                let _ = write.open_table(DAEMON_RUNS)?;
                let _ = write.open_table(DAEMON_SCHEDULE)?;
                let _ = write.open_table(DAEMON_RECOVERY)?;
            }
            write.commit()?;
        }
        Ok(Self { db: Arc::new(db) })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::open(path)?;
        {
            let read = db.begin_read()?;
            let version = read.open_table(DAEMON_STATE_VERSION)?;
            match version.get("version")? {
                Some(found) if found.value() == DAEMON_STATE_FORMAT_VERSION => {}
                Some(found) => {
                    return Err(Error::Database(format!(
                        "unsupported daemon state version {}",
                        found.value()
                    )))
                }
                None => {
                    return Err(Error::Database(
                        "daemon state is missing its format version".to_string(),
                    ))
                }
            }
            let _ = read.open_table(DAEMON_JOBS)?;
            let _ = read.open_table(DAEMON_RUNS)?;
            let _ = read.open_table(DAEMON_SCHEDULE)?;
            let _ = read.open_table(DAEMON_RECOVERY)?;
        }
        Ok(Self { db: Arc::new(db) })
    }

    pub fn put_job(&self, job: &JobConfig) -> Result<()> {
        let payload = encode(&job)?;
        let write = self.db.begin_write()?;
        {
            let mut jobs = write.open_table(DAEMON_JOBS)?;
            jobs.insert(job.id.as_str(), payload.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    pub fn jobs(&self) -> Result<Vec<JobConfig>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(DAEMON_JOBS)?;
        let mut jobs = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            jobs.push(decode(value.value())?);
        }
        Ok(jobs)
    }

    pub fn admit_run(&self, job_id: &str, now_unix_ms: u64) -> Result<RunRecord> {
        let record = RunRecord {
            run_id: format!("run-{now_unix_ms:013x}-{}", random_hex(4)?),
            job_id: job_id.to_string(),
            snapshot_id: String::new(),
            status: RunStatus::Queued,
            admitted_unix_ms: now_unix_ms,
            finished_unix_ms: None,
            failure_class: None,
            cleanup_outcome: None,
            bytes_processed: 0,
            dedupe_bytes: 0,
            limiter_wait_ms: 0,
            duration_ms: 0,
            attempts: 0,
        };
        let payload = encode(&record)?;
        let write = self.db.begin_write()?;
        {
            let mut runs = write.open_table(DAEMON_RUNS)?;
            for item in runs.iter()? {
                let (_, value) = item?;
                let existing: RunRecord = decode(value.value())?;
                if existing.job_id == job_id
                    && matches!(existing.status, RunStatus::Queued | RunStatus::Running)
                {
                    return Err(Error::JobAlreadyRunning {
                        job_id: job_id.to_string(),
                    });
                }
            }
            runs.insert(record.run_id.as_str(), payload.as_slice())?;
        }
        write.commit()?;
        Ok(record)
    }

    pub fn transition(
        &self,
        run_id: &str,
        status: RunStatus,
        now_unix_ms: u64,
        failure_class: Option<String>,
    ) -> Result<RunRecord> {
        let write = self.db.begin_write()?;
        let record = {
            let mut runs = write.open_table(DAEMON_RUNS)?;
            let mut record = self
                .read_run(&runs, run_id)?
                .ok_or_else(|| Error::RunNotActive {
                    run_id: run_id.to_string(),
                })?;
            if record.status != status {
                record.attempts = record.attempts.saturating_add(1);
            }
            record.status = status;
            record.failure_class = failure_class;
            if matches!(
                status,
                RunStatus::Succeeded
                    | RunStatus::Failed
                    | RunStatus::Cancelled
                    | RunStatus::Recovering
            ) && record.finished_unix_ms.is_none()
            {
                record.finished_unix_ms = Some(now_unix_ms);
            }
            let payload = encode(&record)?;
            runs.insert(run_id, payload.as_slice())?;
            record
        };
        write.commit()?;
        Ok(record)
    }

    pub fn record_snapshot(&self, run_id: &str, snapshot_id: &str) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut runs = write.open_table(DAEMON_RUNS)?;
            let mut record = self
                .read_run(&runs, run_id)?
                .ok_or_else(|| Error::RunNotActive {
                    run_id: run_id.to_string(),
                })?;
            record.snapshot_id = snapshot_id.to_string();
            let payload = encode(&record)?;
            runs.insert(run_id, payload.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    pub fn finish_run(&self, updated: &RunRecord) -> Result<()> {
        let payload = encode(updated)?;
        let write = self.db.begin_write()?;
        {
            let mut runs = write.open_table(DAEMON_RUNS)?;
            if self.read_run(&runs, &updated.run_id)?.is_none() {
                return Err(Error::RunNotActive {
                    run_id: updated.run_id.clone(),
                });
            }
            runs.insert(updated.run_id.as_str(), payload.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    pub fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        let read = self.db.begin_read()?;
        let runs = read.open_table(DAEMON_RUNS)?;
        match runs.get(run_id)? {
            Some(value) => Ok(Some(decode(value.value())?)),
            None => Ok(None),
        }
    }

    pub fn runs(&self) -> Result<Vec<RunRecord>> {
        let read = self.db.begin_read()?;
        let runs = read.open_table(DAEMON_RUNS)?;
        let mut records: Vec<RunRecord> = Vec::new();
        for item in runs.iter()? {
            let (_, value) = item?;
            records.push(decode::<RunRecord>(value.value())?);
        }
        records.sort_by(|left, right| {
            left.admitted_unix_ms
                .cmp(&right.admitted_unix_ms)
                .then_with(|| left.run_id.cmp(&right.run_id))
        });
        Ok(records)
    }

    pub fn recoveries(&self) -> Result<Vec<CleanupRecovery>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(DAEMON_RECOVERY)?;
        let mut records: Vec<CleanupRecovery> = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            records.push(decode::<CleanupRecovery>(value.value())?);
        }
        records.sort_by(|left, right| {
            left.recorded_unix_ms
                .cmp(&right.recorded_unix_ms)
                .then_with(|| left.recovery_id.cmp(&right.recovery_id))
        });
        Ok(records)
    }

    pub fn record_cleanup_failure(
        &self,
        run_id: &str,
        job_id: &str,
        snapshot_id: &str,
        now_unix_ms: u64,
    ) -> Result<CleanupRecovery> {
        if snapshot_id.is_empty() {
            return Err(Error::Daemon(
                "cannot record cleanup recovery without a snapshot identifier".to_string(),
            ));
        }
        let recovery = CleanupRecovery {
            recovery_id: format!("recovery-{now_unix_ms:013x}-{}", random_hex(4)?),
            run_id: run_id.to_string(),
            job_id: job_id.to_string(),
            snapshot_id: snapshot_id.to_string(),
            recorded_unix_ms: now_unix_ms,
        };
        let payload = encode(&recovery)?;
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(DAEMON_RECOVERY)?;
            table.insert(recovery.recovery_id.as_str(), payload.as_slice())?;
        }
        write.commit()?;
        Ok(recovery)
    }

    pub fn set_last_scheduled(&self, job_id: &str, unix_ms: u64) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut schedule = write.open_table(DAEMON_SCHEDULE)?;
            schedule.insert(job_id, unix_ms)?;
        }
        write.commit()?;
        Ok(())
    }

    pub fn last_scheduled(&self, job_id: &str) -> Result<Option<u64>> {
        let read = self.db.begin_read()?;
        let schedule = read.open_table(DAEMON_SCHEDULE)?;
        Ok(schedule.get(job_id)?.map(|value| value.value()))
    }

    pub fn mark_catch_up_done(&self, job_id: &str) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut schedule = write.open_table(DAEMON_SCHEDULE)?;
            schedule.remove(job_id)?;
        }
        write.commit()?;
        Ok(())
    }

    pub fn active_runs(&self) -> Result<Vec<RunRecord>> {
        Ok(self
            .runs()?
            .into_iter()
            .filter(|run: &RunRecord| matches!(run.status, RunStatus::Queued | RunStatus::Running))
            .collect::<Vec<RunRecord>>())
    }

    pub fn recover_interrupted(&self, now_unix_ms: u64) -> Result<Vec<RunRecord>> {
        let interrupted: Vec<RunRecord> = self
            .runs()?
            .into_iter()
            .filter(|run: &RunRecord| matches!(run.status, RunStatus::Queued | RunStatus::Running))
            .collect::<Vec<RunRecord>>();
        let mut recovered = Vec::new();
        for run in interrupted {
            recovered.push(self.transition(
                &run.run_id,
                RunStatus::Recovering,
                now_unix_ms,
                None,
            )?);
        }
        Ok(recovered)
    }

    pub fn metrics(&self) -> Result<DaemonMetrics> {
        let mut metrics = DaemonMetrics {
            jobs: 0,
            runs_queued: 0,
            runs_running: 0,
            runs_succeeded: 0,
            runs_failed: 0,
            runs_cancelled: 0,
            runs_recovering: 0,
            snapshots_cleaned: 0,
            cleanup_failures: 0,
            bytes_processed: 0,
            dedupe_bytes: 0,
            limiter_wait_ms: 0,
        };
        metrics.jobs = self.jobs()?.len() as u64;
        for run in self.runs()? {
            match run.status {
                RunStatus::Queued => metrics.runs_queued += 1,
                RunStatus::Running => metrics.runs_running += 1,
                RunStatus::Succeeded => metrics.runs_succeeded += 1,
                RunStatus::Failed => metrics.runs_failed += 1,
                RunStatus::Cancelled => metrics.runs_cancelled += 1,
                RunStatus::Recovering => metrics.runs_recovering += 1,
            }
            metrics.bytes_processed += run.bytes_processed;
            metrics.dedupe_bytes += run.dedupe_bytes;
            metrics.limiter_wait_ms += run.limiter_wait_ms;
            if run.cleanup_outcome.as_deref() == Some("succeeded") {
                metrics.snapshots_cleaned += 1;
            }
            if run.cleanup_outcome.as_deref() == Some("recoverable_failure") {
                metrics.cleanup_failures += 1;
            }
        }
        Ok(metrics)
    }

    pub fn inject_corrupt_run_for_test(&self, run_id: &str, payload: &[u8]) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut runs = write.open_table(DAEMON_RUNS)?;
            runs.insert(run_id, payload)?;
        }
        write.commit()?;
        Ok(())
    }

    fn read_run(&self, runs: &redb::Table<&str, &[u8]>, run_id: &str) -> Result<Option<RunRecord>> {
        match runs.get(run_id)? {
            Some(value) => Ok(Some(decode(value.value())?)),
            None => Ok(None),
        }
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(value)
        .map_err(|error| Error::Database(format!("failed to encode daemon state: {error}")))?;
    if payload.len() > MAX_STATE_PAYLOAD_BYTES {
        return Err(Error::Database(format!(
            "daemon state payload exceeds bounded maximum {MAX_STATE_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(payload)
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_STATE_PAYLOAD_BYTES {
        return Err(Error::Database(
            "daemon state payload exceeds bounded maximum".to_string(),
        ));
    }
    serde_json::from_slice(bytes)
        .map_err(|error| Error::Database(format!("failed to decode daemon state: {error}")))
}

fn random_hex(len: usize) -> Result<String> {
    let mut buf = vec![0_u8; len];
    getrandom::fill(&mut buf)
        .map_err(|error| Error::Database(format!("daemon ID generation failed: {error}")))?;
    Ok(hex::encode(buf))
}
