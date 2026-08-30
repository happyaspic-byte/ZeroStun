use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::{DaemonState, JobConfig};
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct ManualClock {
    now_unix_ms: Arc<AtomicU64>,
}

impl ManualClock {
    pub fn new(now_unix_ms: u64) -> Self {
        Self {
            now_unix_ms: Arc::new(AtomicU64::new(now_unix_ms)),
        }
    }

    pub fn now_unix_ms(&self) -> u64 {
        self.now_unix_ms.load(Ordering::SeqCst)
    }

    pub fn set(&self, now_unix_ms: u64) {
        self.now_unix_ms.store(now_unix_ms, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct Scheduler {
    clock: ManualClock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueJob {
    pub job_id: String,
    pub scheduled_unix_ms: u64,
    pub catch_up: bool,
}

impl Scheduler {
    pub fn new(clock: ManualClock) -> Self {
        Self { clock }
    }

    pub fn due_jobs(&self, state: &DaemonState, jobs: &[JobConfig]) -> Result<Vec<DueJob>> {
        let now = self.clock.now_unix_ms();
        let mut due = Vec::new();
        for job in jobs {
            let interval_ms = job.interval_seconds.saturating_mul(1_000);
            match state.last_scheduled(&job.id)? {
                None => due.push(DueJob {
                    job_id: job.id.clone(),
                    scheduled_unix_ms: now,
                    catch_up: false,
                }),
                Some(last) => {
                    let next = last.saturating_add(interval_ms);
                    if now < next {
                        continue;
                    }
                    due.push(DueJob {
                        job_id: job.id.clone(),
                        scheduled_unix_ms: now,
                        catch_up: now > next,
                    });
                }
            }
        }
        due.sort_by(|left, right| left.job_id.cmp(&right.job_id));
        Ok(due)
    }
}
