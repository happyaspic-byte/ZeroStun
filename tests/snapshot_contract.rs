use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use zerostun::snapshot::{
    CommandRunner, CommandSpec, FakeProvider, FakeRunner, FakeRunnerScript, ProcessRunner,
    ProviderCapabilities, SnapshotProvider, MAX_COMMAND_OUTPUT_BYTES,
};

fn secret_spec() -> CommandSpec {
    CommandSpec::new("/usr/bin/probe")
        .arg("--target")
        .arg("volume-a")
        .env("ZEROSTUN_TOKEN", "super-secret-token")
        .secret_env("ZEROSTUN_TOKEN")
}

#[tokio::test]
async fn fake_provider_probe_create_open_cleanup_recover_succeeds() {
    let runner = FakeRunner::scripted([
        FakeRunnerScript::ok(b"{\"ok\":true}"),
        FakeRunnerScript::ok(b"snap-1"),
        FakeRunnerScript::ok(b"/dev/mapper/snap-1"),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b"[]"),
    ]);
    let provider = FakeProvider::new(runner.clone());
    let caps = provider.probe(&CancellationToken::new()).await.unwrap();
    assert_eq!(
        caps,
        ProviderCapabilities {
            crash_consistent: true,
            read_only: true,
            quiesce: false,
            changed_block: false,
        }
    );
    let handle = provider.create(&CancellationToken::new()).await.unwrap();
    assert_eq!(handle.id, "snap-1");
    let source = provider
        .open_source(&handle, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(source, PathBuf::from("/dev/mapper/snap-1"));
    provider
        .cleanup(&handle, &CancellationToken::new())
        .await
        .unwrap();
    assert!(provider
        .recover(&CancellationToken::new())
        .await
        .unwrap()
        .is_empty());
    let recorded = runner.recorded();
    assert_eq!(recorded[0].program, PathBuf::from("/usr/bin/zst-probe"));
    assert_eq!(recorded[0].args, vec!["--target", "volume-a"]);
    assert_eq!(recorded[1].args, vec!["--create", "volume-a"]);
    assert_eq!(recorded[2].args, vec!["--open", "snap-1"]);
    assert_eq!(recorded[3].args, vec!["--cleanup", "snap-1"]);
    assert_eq!(recorded[4].args, vec!["--recover"]);
}

#[tokio::test]
async fn fake_runner_records_exact_argv_without_shell_interpolation() {
    let runner = FakeRunner::scripted([FakeRunnerScript::ok(b"ok")]);
    let spec = CommandSpec::new("/bin/echo")
        .arg("hello world")
        .arg("a; rm -rf /");
    runner
        .run(&spec, Duration::from_secs(1), &CancellationToken::new())
        .await
        .unwrap();
    let recorded = runner.recorded();
    assert_eq!(recorded[0].program, PathBuf::from("/bin/echo"));
    assert_eq!(recorded[0].args, vec!["hello world", "a; rm -rf /"]);
}

#[tokio::test]
async fn injected_failures_are_classified_and_do_not_leak_secrets() {
    let runner = FakeRunner::scripted([FakeRunnerScript::fail(
        2,
        b"",
        b"denied ZEROSTUN_TOKEN=super-secret-token",
    )]);
    let err = runner
        .run(
            &secret_spec(),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    let display = err.to_string();
    assert!(!display.contains("super-secret-token"));
    assert!(display.contains("[redacted]"));
}

#[tokio::test]
async fn timeout_and_cancellation_stop_the_command() {
    let runner = FakeRunner::scripted([FakeRunnerScript::hang(Duration::from_secs(30))]);
    let err = runner
        .run(
            &CommandSpec::new("/bin/sleep").arg("30"),
            Duration::from_millis(10),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("timed out"));

    let runner = FakeRunner::scripted([FakeRunnerScript::hang(Duration::from_secs(30))]);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = runner
        .run(
            &CommandSpec::new("/bin/sleep").arg("30"),
            Duration::from_secs(5),
            &cancel,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, zerostun::Error::Cancelled));
}

#[tokio::test]
async fn command_output_is_bounded() {
    let runner = FakeRunner::scripted([FakeRunnerScript::ok(vec![b'x'; 64 * 1024].as_slice())]);
    let err = runner
        .run(
            &CommandSpec::new("/bin/yes"),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("bounded"));
}

#[tokio::test]
async fn provider_create_failure_still_recovers_leftovers() {
    let runner = FakeRunner::scripted([
        FakeRunnerScript::ok(b"{\"ok\":true}"),
        FakeRunnerScript::fail(1, b"", b"create failed"),
        FakeRunnerScript::ok(b"snap-orphan"),
        FakeRunnerScript::ok(b""),
    ]);
    let provider = FakeProvider::new(runner.clone());
    provider.probe(&CancellationToken::new()).await.unwrap();
    assert!(provider.create(&CancellationToken::new()).await.is_err());
    let leftovers = provider.recover(&CancellationToken::new()).await.unwrap();
    assert_eq!(leftovers, vec!["snap-orphan"]);
    provider
        .cleanup(
            &zerostun::snapshot::SnapshotHandle {
                id: "snap-orphan".into(),
                source: PathBuf::from("/dev/mapper/snap-orphan"),
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn diagnostics_never_include_secret_env_values() {
    let runner =
        FakeRunner::scripted([FakeRunnerScript::fail(1, b"token=super-secret-token", b"")]);
    let provider = FakeProvider::new(runner);
    let err = provider
        .probe_with_spec(secret_spec(), &CancellationToken::new())
        .await
        .unwrap_err();
    let display = err.to_string();
    assert!(!display.contains("super-secret-token"));
    assert!(display.contains("[redacted]"));
}

#[test]
fn command_spec_debug_redacts_secret_values() {
    let debug = format!("{:?}", secret_spec());
    assert!(!debug.contains("super-secret-token"));
    assert!(debug.contains("[redacted]"));
}

#[tokio::test]
async fn process_runner_executes_exact_argv_and_environment() {
    let runner: Arc<dyn CommandRunner> = Arc::new(ProcessRunner::new());
    let spec = CommandSpec::new("/usr/bin/python3")
        .arg("-c")
        .arg("import os,sys; print(sys.argv[1]); print(os.environ['ZERO_STUN_TEST'])")
        .arg("hello world; not a shell")
        .env("ZERO_STUN_TEST", "present");
    let output = runner
        .run(&spec, Duration::from_secs(2), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "hello world; not a shell\npresent\n"
    );
}

#[tokio::test]
async fn process_runner_redacts_secret_failure_output() {
    let runner = ProcessRunner::new();
    let spec = CommandSpec::new("/usr/bin/python3")
        .arg("-c")
        .arg("import os,sys; sys.stderr.write(os.environ['ZEROSTUN_TOKEN']); sys.exit(7)")
        .env("ZEROSTUN_TOKEN", "real-process-secret")
        .secret_env("ZEROSTUN_TOKEN");
    let error = runner
        .run(&spec, Duration::from_secs(2), &CancellationToken::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(!error.contains("real-process-secret"));
    assert!(error.contains("[redacted]"));
    assert!(error.contains("status 7"));
}

#[test]
fn redact_text_masks_inherited_secret_env_values() {
    std::env::set_var("ZEROSTUN_INHERITED_TOKEN", "inherited-secret-value");
    let spec = CommandSpec::new("/usr/bin/true").secret_env("ZEROSTUN_INHERITED_TOKEN");
    let redacted =
        zerostun::snapshot::redact_text(&spec, "leak inherited-secret-value in diagnostics");
    assert!(!redacted.contains("inherited-secret-value"));
    assert!(redacted.contains("[redacted]"));
}

#[tokio::test]
async fn process_runner_bounds_output_while_the_process_is_running() {
    let runner = ProcessRunner::new();
    let spec = CommandSpec::new("/usr/bin/python3").arg("-c").arg(format!(
        "import sys; sys.stdout.write('x' * {})",
        MAX_COMMAND_OUTPUT_BYTES + 1
    ));
    let error = runner
        .run(&spec, Duration::from_secs(2), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("bounded"));
}

#[tokio::test]
async fn process_runner_times_out_after_output_pipes_close() {
    let runner = ProcessRunner::new();
    let spec = CommandSpec::new("/usr/bin/python3")
        .arg("-c")
        .arg("import os,time; os.close(1); os.close(2); time.sleep(2)");
    let started = Instant::now();
    let error = runner
        .run(&spec, Duration::from_millis(20), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn process_runner_enforces_timeout_and_cancellation() {
    let runner = ProcessRunner::new();
    let sleep = CommandSpec::new("/bin/sleep").arg("30");
    let started = Instant::now();
    let timeout_error = runner
        .run(&sleep, Duration::from_millis(20), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(timeout_error.to_string().contains("timed out"));
    assert!(started.elapsed() < Duration::from_secs(2));

    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        trigger.cancel();
    });
    let started = Instant::now();
    let cancel_error = runner
        .run(&sleep, Duration::from_secs(5), &cancel)
        .await
        .unwrap_err();
    assert!(matches!(cancel_error, zerostun::Error::Cancelled));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn snapshot_provider_is_object_safe_and_accepts_cancellation() {
    let runner: Arc<dyn CommandRunner> = Arc::new(FakeRunner::scripted([FakeRunnerScript::hang(
        Duration::from_secs(30),
    )]));
    let provider: Arc<dyn SnapshotProvider> = Arc::new(FakeProvider::new(runner));
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = provider.probe(&cancel).await.unwrap_err();
    assert!(matches!(error, zerostun::Error::Cancelled));
}
