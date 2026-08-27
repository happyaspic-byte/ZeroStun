use std::collections::BTreeSet;

use zerostun::lifecycle::{
    evaluate_retention, evaluate_retention_strict, PrunePlan, RetentionPolicy,
};
use zerostun::repository::BackupSummaryItem;

const DAY_MS: u64 = 86_400_000;
const FIXED_NOW: u64 = 1_735_689_600_000; // 2025-01-01T00:00:00Z

#[test]
fn newest_and_protected_backups_are_kept() {
    let backups = fixtures_at_utc_days(&[0, 1, 2, 3, 4]);
    let policy = RetentionPolicy {
        keep_last: 2,
        protected_ids: BTreeSet::from(["bkp-day-4".to_string()]),
        ..RetentionPolicy::default()
    };

    let plan = evaluate_retention(&backups, &policy, day_ms(10)).unwrap();

    assert_eq!(plan.keep, vec!["bkp-day-0", "bkp-day-1", "bkp-day-4"]);
    assert_eq!(plan.delete, vec!["bkp-day-2", "bkp-day-3"]);
    assert_eq!(plan.evaluated_at_unix_ms, day_ms(10));
}

#[test]
fn daily_weekly_monthly_buckets_are_deterministic() {
    let backups = calendar_fixture();
    let policy = RetentionPolicy {
        keep_last: 1,
        daily_days: 7,
        weekly_weeks: 4,
        monthly_months: 3,
        protected_ids: BTreeSet::new(),
    };

    let a = evaluate_retention(&backups, &policy, FIXED_NOW).unwrap();
    let b = evaluate_retention(&backups, &policy, FIXED_NOW).unwrap();

    assert_eq!(a, b);
    assert!(a.keep.iter().all(|id| !a.delete.contains(id)));
    assert_eq!(
        a.keep,
        vec![
            "2024-11-01",
            "2024-12-11",
            "2024-12-18",
            "2024-12-25",
            "2024-12-26",
            "2024-12-27",
            "2024-12-28",
            "2024-12-29",
            "2024-12-30",
            "2024-12-31",
        ]
    );
}

#[test]
fn same_timestamp_tie_breaks_by_backup_id() {
    let backups = vec![summary("z-backup", DAY_MS), summary("a-backup", DAY_MS)];
    let policy = RetentionPolicy {
        keep_last: 1,
        ..RetentionPolicy::default()
    };

    let plan = evaluate_retention(&backups, &policy, DAY_MS * 2).unwrap();

    assert_eq!(plan.keep, vec!["a-backup"]);
    assert_eq!(plan.delete, vec!["z-backup"]);
}

#[test]
fn duplicate_ids_with_different_timestamps_are_rejected() {
    let backups = vec![
        summary("duplicate", DAY_MS * 3),
        summary("duplicate", DAY_MS),
    ];
    let policy = RetentionPolicy {
        keep_last: 1,
        ..RetentionPolicy::default()
    };

    let error = evaluate_retention(&backups, &policy, DAY_MS * 4).unwrap_err();

    assert!(matches!(error, zerostun::error::Error::InvalidConfig(_)));
    assert!(error.to_string().contains("duplicate"));
}

#[test]
fn duplicate_ids_with_identical_metadata_are_rejected() {
    let backup = summary("duplicate", DAY_MS);
    let policy = RetentionPolicy {
        keep_last: 1,
        ..RetentionPolicy::default()
    };

    let error = evaluate_retention(&[backup.clone(), backup], &policy, DAY_MS * 2).unwrap_err();

    assert!(matches!(error, zerostun::error::Error::InvalidConfig(_)));
    assert!(error.to_string().contains("duplicate"));
}

#[test]
fn weekly_exact_boundary_is_included() {
    let now = DAY_MS * 21;
    let backups = vec![
        summary("boundary", DAY_MS * 14),
        summary("outside", DAY_MS * 13),
    ];
    let policy = RetentionPolicy {
        weekly_weeks: 2,
        ..RetentionPolicy::default()
    };

    let plan = evaluate_retention(&backups, &policy, now).unwrap();

    assert_eq!(plan.keep, vec!["boundary"]);
    assert_eq!(plan.delete, vec!["outside"]);
}

#[test]
fn future_backup_can_be_kept_by_keep_last() {
    let backups = vec![
        summary("present", FIXED_NOW),
        summary("future", FIXED_NOW + DAY_MS),
    ];
    let policy = RetentionPolicy {
        keep_last: 1,
        daily_days: 1,
        ..RetentionPolicy::default()
    };

    let plan = evaluate_retention(&backups, &policy, FIXED_NOW).unwrap();

    assert_eq!(plan.keep, vec!["future", "present"]);
    assert!(plan.delete.is_empty());
}

#[test]
fn maximum_timestamp_does_not_overflow() {
    let backups = vec![summary("maximum", u64::MAX)];
    let policy = RetentionPolicy {
        keep_last: 1,
        daily_days: u32::MAX,
        weekly_weeks: u32::MAX,
        monthly_months: u32::MAX,
        protected_ids: BTreeSet::new(),
    };

    let plan = evaluate_retention(&backups, &policy, u64::MAX).unwrap();

    assert_eq!(plan.keep, vec!["maximum"]);
    assert!(plan.delete.is_empty());
}

#[test]
fn empty_policy_is_rejected() {
    let error = evaluate_retention(&[summary("backup", 0)], &RetentionPolicy::default(), DAY_MS)
        .unwrap_err();

    assert!(error.to_string().contains("retention policy"));
}

#[test]
fn absent_protected_ids_are_reported_as_sorted_warnings() {
    let policy = RetentionPolicy {
        protected_ids: BTreeSet::from(["missing-z".to_string(), "missing-a".to_string()]),
        ..RetentionPolicy::default()
    };

    let plan = evaluate_retention(&[summary("present", 0)], &policy, DAY_MS).unwrap();

    assert_eq!(
        plan.warnings,
        vec![
            "protected backup not found: missing-a",
            "protected backup not found: missing-z",
        ]
    );
    assert_eq!(plan.delete, vec!["present"]);
}

#[test]
fn strict_policy_rejects_absent_protected_ids() {
    let policy = RetentionPolicy {
        keep_last: 1,
        protected_ids: BTreeSet::from(["missing".to_string()]),
        ..RetentionPolicy::default()
    };

    let error = evaluate_retention_strict(&[summary("present", 0)], &policy, DAY_MS).unwrap_err();

    assert!(error.to_string().contains("missing"));
}

#[test]
fn plan_and_policy_have_stable_serde_shapes() {
    let policy = RetentionPolicy {
        keep_last: 1,
        daily_days: 2,
        weekly_weeks: 3,
        monthly_months: 4,
        protected_ids: BTreeSet::from(["protected".to_string()]),
    };
    let policy_json = serde_json::to_value(&policy).unwrap();
    assert_eq!(policy_json["keep_last"], 1);
    assert_eq!(policy_json["daily_days"], 2);
    assert_eq!(policy_json["weekly_weeks"], 3);
    assert_eq!(policy_json["monthly_months"], 4);
    assert_eq!(
        policy_json["protected_ids"],
        serde_json::json!(["protected"])
    );
    assert_eq!(
        serde_json::from_value::<RetentionPolicy>(policy_json).unwrap(),
        policy
    );

    let plan = PrunePlan {
        keep: vec!["kept".to_string()],
        delete: vec!["deleted".to_string()],
        evaluated_at_unix_ms: FIXED_NOW,
        warnings: vec!["warning".to_string()],
    };
    let plan_json = serde_json::to_value(&plan).unwrap();
    assert_eq!(plan_json["keep"], serde_json::json!(["kept"]));
    assert_eq!(plan_json["delete"], serde_json::json!(["deleted"]));
    assert_eq!(plan_json["evaluated_at_unix_ms"], FIXED_NOW);
    assert_eq!(plan_json["warnings"], serde_json::json!(["warning"]));
    assert_eq!(
        serde_json::from_value::<PrunePlan>(plan_json).unwrap(),
        plan
    );
}

fn fixtures_at_utc_days(ages: &[u64]) -> Vec<BackupSummaryItem> {
    ages.iter()
        .map(|age| summary(&format!("bkp-day-{age}"), day_ms(10 - age)))
        .collect()
}

fn calendar_fixture() -> Vec<BackupSummaryItem> {
    [
        ("2024-10-01", 1_727_740_800_000),
        ("2024-11-01", 1_730_419_200_000),
        ("2024-12-04", 1_733_270_400_000),
        ("2024-12-11", 1_733_875_200_000),
        ("2024-12-18", 1_734_480_000_000),
        ("2024-12-25", 1_735_084_800_000),
        ("2024-12-26", 1_735_171_200_000),
        ("2024-12-27", 1_735_257_600_000),
        ("2024-12-28", 1_735_344_000_000),
        ("2024-12-29", 1_735_430_400_000),
        ("2024-12-30", 1_735_516_800_000),
        ("2024-12-31", 1_735_603_200_000),
    ]
    .into_iter()
    .rev()
    .map(|(id, timestamp)| summary(id, timestamp))
    .collect()
}

fn summary(backup_id: &str, created_unix_ms: u64) -> BackupSummaryItem {
    BackupSummaryItem {
        backup_id: backup_id.to_string(),
        created_unix_ms,
        source_path: "/source".to_string(),
        total_logical_bytes: 1,
        total_chunks: 1,
        stored_bytes: 1,
        root_hash: "hash".to_string(),
    }
}

const fn day_ms(day: u64) -> u64 {
    day * DAY_MS
}
