use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use zerostun::snapshot::{
    CommandRunner, CommandSpec, FakeProvider, FakeRunner, FakeRunnerScript, ProviderCapabilities,
    SnapshotProvider,
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
    let caps = provider.probe().await.unwrap();
    assert_eq!(
        caps,
        ProviderCapabilities {
            crash_consistent: true,
            read_only: true,
            quiesce: false,
            changed_block: false,
        }
    );
    let handle = provider.create().await.unwrap();
    assert_eq!(handle.id, "snap-1");
    let source = provider.open_source(&handle).await.unwrap();
    assert_eq!(source, PathBuf::from("/dev/mapper/snap-1"));
    provider.cleanup(&handle).await.unwrap();
    assert!(provider.recover().await.unwrap().is_empty());
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
    provider.probe().await.unwrap();
    assert!(provider.create().await.is_err());
    let leftovers = provider.recover().await.unwrap();
    assert_eq!(leftovers, vec!["snap-orphan"]);
    provider
        .cleanup(&zerostun::snapshot::SnapshotHandle {
            id: "snap-orphan".into(),
            source: PathBuf::from("/dev/mapper/snap-orphan"),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn diagnostics_never_include_secret_env_values() {
    let runner =
        FakeRunner::scripted([FakeRunnerScript::fail(1, b"token=super-secret-token", b"")]);
    let provider = FakeProvider::new(runner);
    let err = provider.probe_with_spec(secret_spec()).await.unwrap_err();
    let display = err.to_string();
    assert!(!display.contains("super-secret-token"));
    assert!(display.contains("[redacted]"));
}
