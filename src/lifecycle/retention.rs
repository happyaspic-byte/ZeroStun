use std::collections::{BTreeSet, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::repository::BackupSummaryItem;

const DAY_MS: u64 = 86_400_000;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub keep_last: usize,
    pub daily_days: u32,
    pub weekly_weeks: u32,
    pub monthly_months: u32,
    pub protected_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrunePlan {
    pub keep: Vec<String>,
    pub delete: Vec<String>,
    pub evaluated_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

pub fn evaluate_retention(
    backups: &[BackupSummaryItem],
    policy: &RetentionPolicy,
    now_unix_ms: u64,
) -> Result<PrunePlan> {
    evaluate_retention_with_mode(backups, policy, now_unix_ms, false)
}

pub fn evaluate_retention_strict(
    backups: &[BackupSummaryItem],
    policy: &RetentionPolicy,
    now_unix_ms: u64,
) -> Result<PrunePlan> {
    evaluate_retention_with_mode(backups, policy, now_unix_ms, true)
}

fn evaluate_retention_with_mode(
    backups: &[BackupSummaryItem],
    policy: &RetentionPolicy,
    now_unix_ms: u64,
    strict: bool,
) -> Result<PrunePlan> {
    validate_policy(policy)?;

    let mut available_ids = HashSet::with_capacity(backups.len());
    for backup in backups {
        if !available_ids.insert(backup.backup_id.as_str()) {
            return Err(Error::InvalidConfig(format!(
                "duplicate backup ID: {}",
                backup.backup_id
            )));
        }
    }
    let missing_protected: Vec<&String> = policy
        .protected_ids
        .iter()
        .filter(|id| !available_ids.contains(id.as_str()))
        .collect();
    if strict && !missing_protected.is_empty() {
        return Err(Error::InvalidConfig(format!(
            "protected backup not found: {}",
            missing_protected
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let warnings = missing_protected
        .into_iter()
        .map(|id| format!("protected backup not found: {id}"))
        .collect();
    let mut ordered: Vec<&BackupSummaryItem> = backups.iter().collect();
    ordered.sort_by(|left, right| {
        right
            .created_unix_ms
            .cmp(&left.created_unix_ms)
            .then_with(|| left.backup_id.cmp(&right.backup_id))
    });

    let mut keep = BTreeSet::new();
    keep.extend(
        ordered
            .iter()
            .take(policy.keep_last)
            .map(|backup| backup.backup_id.clone()),
    );
    keep.extend(
        policy
            .protected_ids
            .iter()
            .filter(|id| available_ids.contains(id.as_str()))
            .cloned(),
    );

    select_elapsed_buckets(
        &ordered,
        now_unix_ms,
        policy.daily_days,
        |timestamp| timestamp / DAY_MS,
        &mut keep,
    );
    select_elapsed_buckets(
        &ordered,
        now_unix_ms,
        policy.weekly_weeks,
        |timestamp| (timestamp / DAY_MS) / 7,
        &mut keep,
    );
    select_elapsed_buckets(
        &ordered,
        now_unix_ms,
        policy.monthly_months,
        month_index,
        &mut keep,
    );

    let all_ids: BTreeSet<String> = backups
        .iter()
        .map(|backup| backup.backup_id.clone())
        .collect();
    let delete = all_ids.difference(&keep).cloned().collect();

    Ok(PrunePlan {
        keep: keep.into_iter().collect(),
        delete,
        evaluated_at_unix_ms: now_unix_ms,
        warnings,
    })
}

fn validate_policy(policy: &RetentionPolicy) -> Result<()> {
    if policy.keep_last == 0
        && policy.daily_days == 0
        && policy.weekly_weeks == 0
        && policy.monthly_months == 0
        && policy.protected_ids.is_empty()
    {
        return Err(Error::InvalidConfig(
            "retention policy must enable at least one selector".to_string(),
        ));
    }
    Ok(())
}

fn select_elapsed_buckets<F>(
    ordered: &[&BackupSummaryItem],
    now_unix_ms: u64,
    bucket_count: u32,
    bucket_for: F,
    keep: &mut BTreeSet<String>,
) where
    F: Fn(u64) -> u64,
{
    if bucket_count == 0 {
        return;
    }

    let now_bucket = bucket_for(now_unix_ms);
    let oldest_bucket = now_bucket.saturating_sub(u64::from(bucket_count - 1));
    let mut selected = HashSet::new();
    for backup in ordered {
        if backup.created_unix_ms > now_unix_ms {
            continue;
        }
        let bucket = bucket_for(backup.created_unix_ms);
        if bucket >= oldest_bucket && bucket <= now_bucket && selected.insert(bucket) {
            keep.insert(backup.backup_id.clone());
        }
    }
}

fn month_index(unix_ms: u64) -> u64 {
    let days = unix_ms / DAY_MS;
    let (year, month) = civil_year_month(days);
    year * 12 + u64::from(month - 1)
}

// Gregorian civil date conversion adapted to non-negative Unix days. All calculations are UTC.
fn civil_year_month(days_since_unix_epoch: u64) -> (u64, u32) {
    let days = days_since_unix_epoch + 719_468;
    let era = days / 146_097;
    let day_of_era = days % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    if month <= 2 {
        year += 1;
    }
    (year, month as u32)
}

#[cfg(test)]
mod tests {
    use super::{civil_year_month, month_index, DAY_MS};

    #[test]
    fn converts_utc_days_at_calendar_boundaries() {
        assert_eq!(civil_year_month(0), (1970, 1));
        assert_eq!(civil_year_month(31), (1970, 2));
        assert_eq!(civil_year_month(19_782), (2024, 2));
        assert_eq!(civil_year_month(19_783), (2024, 3));
        assert_eq!(civil_year_month(20_089), (2025, 1));
    }

    #[test]
    fn month_index_changes_only_at_utc_month_boundary() {
        let january_2025 = 1_735_689_600_000;
        assert_eq!(month_index(january_2025 - 1), month_index(january_2025) - 1);
        assert_eq!(
            month_index(january_2025),
            month_index(january_2025 + DAY_MS)
        );
    }
}
