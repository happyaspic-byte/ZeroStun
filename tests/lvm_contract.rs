use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use zerostun::snapshot::{
    CommandRunner, CommandSpec, FakeRunner, FakeRunnerScript, LvmProvider, ProcessRunner,
    SnapshotHandle, SnapshotProvider, SnapshotRequest,
};

const EMPTY_LVS: &[u8] = br#"{"report":[{"lv":[]}]}"#;
const ORIGIN_LVS: &[u8] =
    br#"{"report":[{"lv":[{"vg_name":"vg-main","lv_name":"data","lv_tags":""}]}]}"#;
const MANAGED_LVS: &[u8] = br#"{"report":[{"lv":[{"vg_name":"vg-main","lv_name":"zerostun-a1","lv_tags":"zerostun.snapshot"}]}]}"#;

fn request() -> SnapshotRequest {
    SnapshotRequest::new("vg-main/data")
}

fn handle() -> SnapshotHandle {
    SnapshotHandle {
        id: "vg-main/zerostun-a1".to_string(),
        source: PathBuf::from("/dev/mapper/vg--main-zerostun--a1"),
    }
}

#[tokio::test]
async fn lvm_probe_create_open_cleanup_and_recover_use_exact_argv() {
    let stale = br#"{"report":[{"lv":[
        {"vg_name":"vg-main","lv_name":"zerostun-old1","lv_tags":"zerostun.snapshot"},
        {"vg_name":"vg-main","lv_name":"zerostun-old2","lv_tags":"zerostun.snapshot"}
    ]}]}"#;
    let runner = FakeRunner::scripted([
        FakeRunnerScript::ok(EMPTY_LVS),
        FakeRunnerScript::ok(ORIGIN_LVS),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(MANAGED_LVS),
        FakeRunnerScript::ok(MANAGED_LVS),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(stale),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
    ]);
    let provider = LvmProvider::new(runner.clone());
    let cancel = CancellationToken::new();

    assert!(provider.probe(&cancel).await.unwrap().read_only);
    let created = provider.create(&request(), &cancel).await.unwrap();
    assert!(created.id.starts_with("vg-main/zerostun-"));
    assert_eq!(
        created.source,
        PathBuf::from(format!(
            "/dev/mapper/vg--main-{}",
            created.id.split_once('/').unwrap().1.replace('-', "--")
        ))
    );

    let source = provider.open_source(&handle(), &cancel).await.unwrap();
    assert_eq!(source, handle().source);
    provider.cleanup(&handle(), &cancel).await.unwrap();
    let recovered = provider.recover(&cancel).await.unwrap();
    assert_eq!(
        recovered,
        vec![
            "vg-main/zerostun-old2".to_string(),
            "vg-main/zerostun-old1".to_string()
        ]
    );

    let commands = runner.recorded();
    assert_eq!(commands[0].program, PathBuf::from("/usr/sbin/lvs"));
    assert_eq!(
        commands[0].args,
        vec![
            "--reportformat",
            "json",
            "--options",
            "vg_name,lv_name,lv_tags"
        ]
    );
    assert_eq!(commands[1].args.last().unwrap(), "vg-main/data");
    assert_eq!(commands[2].program, PathBuf::from("/usr/sbin/lvcreate"));
    assert_eq!(
        &commands[2].args[..10],
        [
            "--snapshot",
            "--permission",
            "r",
            "--extents",
            "20%ORIGIN",
            "--addtag",
            "zerostun.snapshot",
            "--name",
            created.id.split_once('/').unwrap().1,
            "vg-main/data",
        ]
    );
    assert_eq!(commands[5].program, PathBuf::from("/usr/sbin/lvremove"));
    assert_eq!(commands[5].args, vec!["--force", "vg-main/zerostun-a1"]);
    assert_eq!(commands[7].args, vec!["--force", "vg-main/zerostun-old2"]);
    assert_eq!(commands[8].args, vec!["--force", "vg-main/zerostun-old1"]);
}

#[tokio::test]
async fn lvm_rejects_unsupported_requirements_and_invalid_targets_before_commands() {
    let runner = FakeRunner::scripted([]);
    let provider = LvmProvider::new(runner.clone());
    let error = provider
        .create(&request().require_quiesce(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("quiesce"));

    for target in ["", "vg", "/lv", "vg/../lv", "vg/lv;destroy"] {
        assert!(provider
            .create(&SnapshotRequest::new(target), &CancellationToken::new())
            .await
            .is_err());
    }
    assert!(runner.recorded().is_empty());
}

#[tokio::test]
async fn lvm_classifies_probe_create_open_cleanup_and_recover_failures() {
    let cancel = CancellationToken::new();

    let provider = LvmProvider::new(FakeRunner::scripted([FakeRunnerScript::ok(b"not-json")]));
    assert!(provider.probe(&cancel).await.is_err());

    let logical_error = br#"{
        "report":[{"lv":[]}],
        "log":[{"log_type":"error","log_message":"lock denied"}]
    }"#;
    let provider = LvmProvider::new(FakeRunner::scripted([FakeRunnerScript::ok(logical_error)]));
    let error = provider.probe(&cancel).await.unwrap_err().to_string();
    assert!(error.contains("lock denied"));

    let provider = LvmProvider::new(FakeRunner::scripted([FakeRunnerScript::fail(
        2,
        b"",
        b"probe denied",
    )]));
    assert!(provider.create(&request(), &cancel).await.is_err());

    let provider = LvmProvider::new(FakeRunner::scripted([
        FakeRunnerScript::ok(ORIGIN_LVS),
        FakeRunnerScript::fail(5, b"", b"create denied"),
    ]));
    assert!(provider.create(&request(), &cancel).await.is_err());

    let provider = LvmProvider::new(FakeRunner::scripted([FakeRunnerScript::fail(
        3,
        b"",
        b"open denied",
    )]));
    assert!(provider.open_source(&handle(), &cancel).await.is_err());

    let provider = LvmProvider::new(FakeRunner::scripted([
        FakeRunnerScript::ok(MANAGED_LVS),
        FakeRunnerScript::fail(4, b"", b"cleanup denied"),
    ]));
    assert!(provider.cleanup(&handle(), &cancel).await.is_err());

    let provider = LvmProvider::new(FakeRunner::scripted([FakeRunnerScript::fail(
        6,
        b"",
        b"recover denied",
    )]));
    assert!(provider.recover(&cancel).await.is_err());
}

#[tokio::test]
async fn lvm_timeout_cancel_and_redaction_are_enforced_by_the_runner() {
    let provider = LvmProvider::new(FakeRunner::scripted([FakeRunnerScript::hang(
        Duration::from_secs(30),
    )]));
    assert!(provider
        .probe(&CancellationToken::new())
        .await
        .unwrap_err()
        .to_string()
        .contains("timed out"));

    let cancel = CancellationToken::new();
    cancel.cancel();
    let runner = FakeRunner::scripted([FakeRunnerScript::ok(EMPTY_LVS)]);
    let provider = LvmProvider::new(runner.clone());
    assert!(matches!(
        provider.probe(&cancel).await.unwrap_err(),
        zerostun::Error::Cancelled
    ));
    assert!(runner.recorded().is_empty());

    let runner = FakeRunner::scripted([FakeRunnerScript::fail(1, b"", b"token=lvm-secret")]);
    let error = runner
        .run(
            &CommandSpec::new("/usr/sbin/lvs")
                .env("ZEROSTUN_TOKEN", "lvm-secret")
                .secret_env("ZEROSTUN_TOKEN"),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(!error.contains("lvm-secret"));
    assert!(error.contains("[redacted]"));
}

#[tokio::test]
async fn lvm_rejects_tampered_handle_id_and_path_before_cleanup_mutation() {
    let runner = FakeRunner::scripted([]);
    let provider = LvmProvider::new(runner.clone());
    for bad in [
        SnapshotHandle {
            id: "vg-main/data".into(),
            source: PathBuf::from("/dev/mapper/vg--main-data"),
        },
        SnapshotHandle {
            id: "vg-main/zerostun-a1".into(),
            source: PathBuf::from("/tmp/not-the-lv"),
        },
        SnapshotHandle {
            id: "../zerostun-a1".into(),
            source: PathBuf::from("/dev/mapper/unsafe"),
        },
    ] {
        assert!(provider
            .cleanup(&bad, &CancellationToken::new())
            .await
            .is_err());
    }
    assert!(runner.recorded().is_empty());
}

#[tokio::test]
async fn lvm_host_tooling_is_detected_with_a_non_destructive_probe_only() {
    if !std::path::Path::new("/usr/sbin/lvs").is_file() {
        return;
    }
    let provider = LvmProvider::new(ProcessRunner::new());
    if let Ok(capabilities) = provider.probe(&CancellationToken::new()).await {
        assert!(capabilities.crash_consistent);
        assert!(capabilities.read_only);
        assert!(!capabilities.quiesce);
        assert!(!capabilities.changed_block);
    }
}
