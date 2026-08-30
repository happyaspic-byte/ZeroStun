use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::FailureClass;
use crate::error::{Error, Result};
use crate::ids::validate_backup_id;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    pub state_db: PathBuf,
    pub shutdown_deadline_ms: u64,
    pub jobs: Vec<JobConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobConfig {
    pub id: String,
    pub provider: String,
    pub target: String,
    pub interval_seconds: u64,
    pub timezone: String,
    pub retry: RetryConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl DaemonConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|error| {
            Error::InvalidDaemonConfig(format!("failed to read {}: {error}", path.display()))
        })?;
        Self::parse_toml(&text)
    }

    pub fn parse_toml(text: &str) -> Result<Self> {
        let parsed: DaemonConfigFile = toml::from_str(text)
            .map_err(|error| Error::InvalidDaemonConfig(format!("invalid daemon TOML: {error}")))?;
        parsed.validate()
    }
}

impl JobConfig {
    fn validate(&self) -> Result<()> {
        validate_backup_id(&self.id)
            .map_err(|error| Error::InvalidDaemonConfig(error.to_string()))?;
        if self.provider.trim().is_empty() {
            return Err(Error::InvalidDaemonConfig(
                "job provider must be non-empty".to_string(),
            ));
        }
        if self.target.trim().is_empty() || self.target.contains('\0') {
            return Err(Error::InvalidDaemonConfig(
                "job target must be a non-empty path-safe identifier".to_string(),
            ));
        }
        if self.interval_seconds == 0 {
            return Err(Error::InvalidDaemonConfig(
                "job interval_seconds must be greater than zero".to_string(),
            ));
        }
        validate_timezone(&self.timezone)?;
        self.retry.validate()
    }
}

impl RetryConfig {
    fn validate(&self) -> Result<()> {
        if self.max_attempts == 0 {
            return Err(Error::InvalidDaemonConfig(
                "retry max_attempts must be greater than zero".to_string(),
            ));
        }
        if self.initial_delay_ms == 0 || self.max_delay_ms < self.initial_delay_ms {
            return Err(Error::InvalidDaemonConfig(
                "retry delays must be positive and non-decreasing".to_string(),
            ));
        }
        Ok(())
    }

    pub fn delays_ms(&self, class: FailureClass) -> Vec<u64> {
        if class != FailureClass::Transient || self.max_attempts <= 1 {
            return Vec::new();
        }
        let retries = self.max_attempts.saturating_sub(1) as usize;
        let mut delay = self.initial_delay_ms;
        let mut delays = Vec::with_capacity(retries);
        for _ in 0..retries {
            delays.push(delay.min(self.max_delay_ms));
            delay = delay.saturating_mul(2).min(self.max_delay_ms);
        }
        delays
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonConfigFile {
    state_db: PathBuf,
    shutdown_deadline_ms: u64,
    jobs: Vec<JobConfigFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobConfigFile {
    id: String,
    provider: String,
    target: String,
    interval_seconds: u64,
    timezone: String,
    retry: RetryConfig,
}

impl DaemonConfigFile {
    fn validate(self) -> Result<DaemonConfig> {
        if self.shutdown_deadline_ms == 0 {
            return Err(Error::InvalidDaemonConfig(
                "shutdown_deadline_ms must be greater than zero".to_string(),
            ));
        }
        if self.jobs.is_empty() {
            return Err(Error::InvalidDaemonConfig(
                "daemon configuration must define at least one job".to_string(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut jobs = Vec::with_capacity(self.jobs.len());
        for job in self.jobs {
            let job = JobConfig {
                id: job.id,
                provider: job.provider,
                target: job.target,
                interval_seconds: job.interval_seconds,
                timezone: job.timezone,
                retry: job.retry,
            };
            job.validate()?;
            if !ids.insert(job.id.clone()) {
                return Err(Error::InvalidDaemonConfig(format!(
                    "duplicate job id {}",
                    job.id
                )));
            }
            jobs.push(job);
        }
        Ok(DaemonConfig {
            state_db: self.state_db,
            shutdown_deadline_ms: self.shutdown_deadline_ms,
            jobs,
        })
    }
}

fn validate_timezone(value: &str) -> Result<()> {
    if value == "UTC" || IANA_TIMEZONES.contains(&value) {
        return Ok(());
    }
    Err(Error::InvalidDaemonConfig(format!(
        "unsupported timezone {value}"
    )))
}

const IANA_TIMEZONES: &[&str] = &[
    "Africa/Cairo",
    "America/New_York",
    "America/Los_Angeles",
    "Asia/Seoul",
    "Asia/Tokyo",
    "Australia/Sydney",
    "Europe/Berlin",
    "Europe/London",
    "UTC",
];
