use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use zerostun::daemon::{
    classify_error, DaemonConfig, DaemonRunner, DaemonState, FailureClass, JobConfig, ManualClock,
    RetryConfig, RunStatus, Scheduler, ShutdownController,
};
use zerostun::snapshot::{FakeProvider, FakeRunner, FakeRunnerScript};
use zerostun::Error;

fn job(id: &str) -> JobConfig {
    JobConfig {
        id: id.to_string(),
        provider: "fake".to_string(),
        target: "volume-a".to_string(),
        interval_seconds: 60,
        timezone: "UTC".to_string(),
        retry: RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 1,
            max_delay_ms: 4,
        },
    }
}

fn state() -> (tempfile::TempDir, DaemonState) {
    let temp = tempfile::tempdir().unwrap();
    let state = DaemonState::create(temp.path().join("daemon.redb")).unwrap();
    (temp, state)
}

#[test]
fn toml_jobs_are_validated_before_admission_and_timezones_are_checked() {
    let text = r#"
state_db = "/var/lib/zerostun/daemon.redb"
shutdown_deadline_ms = 30000

[[jobs]]
id = "daily-root"
provider = "lvm"
target = "vg/root"
interval_seconds = 3600
timezone = "Asia/Seoul"

[jobs.retry]
max_attempts = 3
initial_delay_ms = 100
max_delay_ms = 1000
"#;
    let parsed = DaemonConfig::parse_toml(text).unwrap();
    assert_eq!(parsed.jobs.len(), 1);
    assert_eq!(parsed.jobs[0].timezone, "Asia/Seoul");

    for bad in [
        text.replace("Asia/Seoul", "../etc/passwd"),
        text.replace("interval_seconds = 3600", "interval_seconds = 0"),
        text.replace("id = \"daily-root\"", "id = \"bad/id\""),
        text.replace("max_attempts = 3", "max_attempts = 0"),
        text.replace("max_delay_ms = 1000", "max_delay_ms = 10"),
    ] {
        assert!(DaemonConfig::parse_toml(&bad).is_err());
    }
}

#[test]
fn state_tracks_all_run_states_and_rejects_duplicate_same_job() {
    let (_temp, state) = state();
    state.put_job(&job("job-a")).unwrap();
    let first = state.admit_run("job-a", 1_000).unwrap();
    assert_eq!(first.status, RunStatus::Queued);
    state
        .transition(&first.run_id, RunStatus::Running, 1_001, None)
        .unwrap();
    assert!(state.admit_run("job-a", 1_002).is_err());
    let other = state.admit_run("job-b", 1_002).unwrap();
    assert_eq!(other.status, RunStatus::Queued);

    for status in [
        RunStatus::Succeeded,
        RunStatus::Failed,
        RunStatus::Cancelled,
        RunStatus::Recovering,
    ] {
        state
            .transition(&other.run_id, status, 2_000, Some("classified".into()))
            .unwrap();
        assert_eq!(
            state.get_run(&other.run_id).unwrap().unwrap().status,
            status
        );
    }
}

#[test]
fn corrupted_state_fails_closed_without_replacing_data() {
    let (temp, state) = state();
    state.put_job(&job("job-a")).unwrap();
    state
        .inject_corrupt_run_for_test("run-corrupt", b"not-json")
        .unwrap();
    assert!(state.runs().is_err());
    drop(state);
    assert!(DaemonState::open(temp.path().join("daemon.redb")).is_ok());
}

#[test]
fn scheduler_uses_injected_clock_and_emits_at_most_one_restart_catch_up() {
    let (_temp, state) = state();
    state.put_job(&job("job-a")).unwrap();
    state.set_last_scheduled("job-a", 1_000).unwrap();
    let clock = ManualClock::new(181_000);
    let scheduler = Scheduler::new(clock.clone());
    let due = scheduler.due_jobs(&state, &[job("job-a")]).unwrap();
    assert_eq!(due.len(), 1);
    assert!(due[0].catch_up);
    state
        .set_last_scheduled("job-a", due[0].scheduled_unix_ms)
        .unwrap();
    assert!(scheduler
        .due_jobs(&state, &[job("job-a")])
        .unwrap()
        .is_empty());

    clock.set(241_000);
    let due = scheduler.due_jobs(&state, &[job("job-a")]).unwrap();
    assert_eq!(due.len(), 1);
    assert!(!due[0].catch_up);
}

#[test]
fn retry_classification_is_bounded_and_only_transient_errors_retry() {
    assert_eq!(
        classify_error(&Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timeout"
        ))),
        FailureClass::Transient
    );
    assert_eq!(
        classify_error(&Error::InvalidConfig("bad".into())),
        FailureClass::Configuration
    );
    assert_eq!(
        classify_error(&Error::ChunkCorrupt {
            content_id: "c".into(),
            reason: "bad".into()
        }),
        FailureClass::Integrity
    );
    assert_eq!(
        classify_error(&Error::Snapshot("unsupported capability".into())),
        FailureClass::Unsupported
    );
    assert_eq!(classify_error(&Error::Cancelled), FailureClass::Cancelled);

    let retry = job("job-a").retry;
    assert_eq!(retry.delays_ms(FailureClass::Transient), vec![1, 2]);
    assert!(retry.delays_ms(FailureClass::Integrity).is_empty());
    assert!(retry.delays_ms(FailureClass::Unsupported).is_empty());
}

#[tokio::test]
async fn runner_cleans_snapshot_then_commits_success_and_metrics() {
    let (_temp, state) = state();
    state.put_job(&job("job-a")).unwrap();
    let provider = FakeProvider::new(FakeRunner::scripted([
        FakeRunnerScript::ok(b"snap-a"),
        FakeRunnerScript::ok(b"/dev/mapper/snap-a"),
        FakeRunnerScript::ok(b""),
    ]));
    let clock = ManualClock::new(1_000);
    let runner = DaemonRunner::new(Arc::new(provider), clock.clone());
    let record = runner
        .run_job(&state, &job("job-a"), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(record.status, RunStatus::Succeeded);
    assert_eq!(record.cleanup_outcome.as_deref(), Some("succeeded"));
    let metrics = state.metrics().unwrap();
    assert_eq!(metrics.runs_succeeded, 1);
    assert_eq!(metrics.snapshots_cleaned, 1);
}

#[tokio::test]
async fn cancellation_cleans_snapshot_and_marks_run_cancelled() {
    let (_temp, state) = state();
    state.put_job(&job("job-a")).unwrap();
    let fake = FakeRunner::scripted([
        FakeRunnerScript::ok(b"snap-a"),
        FakeRunnerScript::hang(Duration::from_secs(30)),
        FakeRunnerScript::ok(b""),
    ]);
    let provider = FakeProvider::new(fake.clone());
    let runner = DaemonRunner::new(Arc::new(provider), ManualClock::new(1_000));
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        trigger.cancel();
    });
    let record = runner
        .run_job(&state, &job("job-a"), &cancel)
        .await
        .unwrap();
    assert_eq!(record.status, RunStatus::Cancelled);
    assert_eq!(record.cleanup_outcome.as_deref(), Some("succeeded"));
    assert!(fake
        .recorded()
        .iter()
        .any(|command| command.args.first().map(String::as_str) == Some("--cleanup")));
}

#[tokio::test]
async fn cleanup_failure_creates_recoverable_record_and_failed_run() {
    let (_temp, state) = state();
    state.put_job(&job("job-a")).unwrap();
    let provider = FakeProvider::new(FakeRunner::scripted([
        FakeRunnerScript::ok(b"snap-a"),
        FakeRunnerScript::ok(b"/dev/mapper/snap-a"),
        FakeRunnerScript::fail(9, b"", b"cleanup denied"),
    ]));
    let runner = DaemonRunner::new(Arc::new(provider), ManualClock::new(1_000));
    let record = runner
        .run_job(&state, &job("job-a"), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(record.status, RunStatus::Failed);
    assert_eq!(
        record.cleanup_outcome.as_deref(),
        Some("recoverable_failure")
    );
    let recovery = state.recoveries().unwrap();
    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery[0].snapshot_id, "snap-a");
}

#[test]
fn restart_recovery_moves_interrupted_runs_to_recovering_once() {
    let (_temp, state) = state();
    state.put_job(&job("job-a")).unwrap();
    let run = state.admit_run("job-a", 1_000).unwrap();
    state
        .transition(&run.run_id, RunStatus::Running, 1_001, None)
        .unwrap();
    let recovered = state.recover_interrupted(2_000).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status, RunStatus::Recovering);
    assert!(state.recover_interrupted(2_001).unwrap().is_empty());
}

#[tokio::test]
async fn sigterm_stops_admission_cancels_stage_and_honors_deadline() {
    let shutdown = ShutdownController::new(Duration::from_millis(20));
    assert!(shutdown.admission_allowed());
    let token = shutdown.running_token();
    shutdown.request_shutdown();
    assert!(!shutdown.admission_allowed());
    assert!(token.is_cancelled());
    let error = shutdown
        .wait_for_cleanup(async { tokio::time::sleep(Duration::from_secs(1)).await })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("deadline"));
}

#[test]
fn metrics_json_shape_is_stable() {
    let (_temp, state) = state();
    let json = serde_json::to_value(state.metrics().unwrap()).unwrap();
    assert_eq!(
        json,
        serde_json::json!({
            "jobs": 0,
            "runs_queued": 0,
            "runs_running": 0,
            "runs_succeeded": 0,
            "runs_failed": 0,
            "runs_cancelled": 0,
            "runs_recovering": 0,
            "snapshots_cleaned": 0,
            "cleanup_failures": 0,
            "bytes_processed": 0,
            "dedupe_bytes": 0,
            "limiter_wait_ms": 0
        })
    );
}
