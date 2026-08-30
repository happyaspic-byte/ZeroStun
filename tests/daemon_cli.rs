use std::fs;
use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::time::Duration;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use zerostun::daemon::{DaemonConfig, DaemonState, RunStatus};

fn zerostun() -> Command {
    Command::cargo_bin("zerostun").unwrap()
}

fn fixture() -> (TempDir, PathBuf, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let config = temp.path().join("daemon.toml");
    Command::cargo_bin("zerostun")
        .unwrap()
        .args(["init", "--repo"])
        .arg(&repo)
        .assert()
        .success();
    let state_db = temp.path().join("daemon-state.redb");
    fs::write(
        &config,
        format!(
            r#"state_db = "{}"
shutdown_deadline_ms = 2000

[[jobs]]
id = "job-a"
provider = "fake"
target = "volume-a"
interval_seconds = 60
timezone = "UTC"

[jobs.retry]
max_attempts = 1
initial_delay_ms = 1
max_delay_ms = 1
"#,
            state_db.display()
        ),
    )
    .unwrap();
    (temp, repo, config)
}

fn seed_run(config: &std::path::Path, status: RunStatus) -> String {
    let parsed = DaemonConfig::load(config).unwrap();
    if !parsed.state_db.exists() {
        let _ = DaemonState::create(&parsed.state_db).unwrap();
    }
    let parsed = DaemonConfig::load(config).unwrap();
    let state = DaemonState::open(&parsed.state_db).unwrap();
    state.put_job(&parsed.jobs[0]).unwrap();
    let run = state.admit_run("job-a", 1_000).unwrap();
    if status != RunStatus::Queued {
        state.transition(&run.run_id, status, 1_001, None).unwrap();
    }
    run.run_id
}

#[test]
fn daemon_status_jobs_list_runs_list_and_metrics_json_are_stable() {
    let (_temp, _repo, config) = fixture();
    let run_id = seed_run(&config, RunStatus::Succeeded);

    zerostun()
        .args(["daemon", "status", "--config"])
        .arg(&config)
        .arg("--json")
        .assert()
        .success()
        .stdout(predicate::str::contains("\"jobs\": 1"));

    zerostun()
        .args(["jobs", "list", "--config"])
        .arg(&config)
        .arg("--json")
        .assert()
        .success()
        .stdout(predicate::str::contains("job-a"));

    zerostun()
        .args(["runs", "list", "--config"])
        .arg(&config)
        .arg("--json")
        .assert()
        .success()
        .stdout(predicate::str::contains(&run_id));

    zerostun()
        .args(["metrics", "--json", "--config"])
        .arg(&config)
        .assert()
        .success()
        .stdout(predicate::str::contains("\"runs_succeeded\": 1"));
}

#[test]
fn cancel_command_marks_a_running_job_cancelled() {
    let (_temp, _repo, config) = fixture();
    let run_id = seed_run(&config, RunStatus::Running);
    zerostun()
        .args(["cancel", &run_id, "--config"])
        .arg(&config)
        .arg("--json")
        .assert()
        .success()
        .stdout(predicate::str::contains("\"status\": \"cancelled\""));
}

#[test]
fn invalid_config_is_rejected_before_daemon_status() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("bad.toml");
    fs::write(&config, "not toml = [\n").unwrap();
    zerostun()
        .args(["daemon", "status", "--config"])
        .arg(&config)
        .assert()
        .failure();
}

#[test]
fn sigterm_stops_admission_and_exits_within_deadline() {
    let (_temp, _repo, config) = fixture();
    let mut child = StdCommand::new(assert_cmd::cargo::cargo_bin("zerostun"))
        .args(["daemon", "run", "--config"])
        .arg(&config)
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let kill = StdCommand::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(kill.success());
    let started = std::time::Instant::now();
    let status = child.wait().unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(status.success() || status.code() == Some(130));
}
