use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use zerostun::snapshot::{
    CommandRunner, CommandSpec, FakeRunner, FakeRunnerScript, ProcessRunner, SnapshotHandle,
    SnapshotProvider, SnapshotRequest, ZfsProvider, ZfsTargetKind,
};

const FS_PROBE: &[u8] = b"tank/data\tfilesystem\t/tank/data\n";
const ZVOL_PROBE: &[u8] = b"tank/vol\tvolume\t-\n";
const FS_CLONE: &[u8] = b"tank/zerostun-a1\tfilesystem\tyes\ttank/data@zerostun-a1\n";
const ZVOL_CLONE: &[u8] = b"tank/zerostun-b2\tvolume\t-\ttank/vol@zerostun-b2\n";

fn fs_handle() -> SnapshotHandle {
    SnapshotHandle {
        id: "tank/data@zerostun-a1".into(),
        source: PathBuf::from("/run/zerostun/zfs/tank_data_zerostun-a1"),
    }
}

fn zvol_handle() -> SnapshotHandle {
    SnapshotHandle {
        id: "tank/vol@zerostun-b2".into(),
        source: PathBuf::from("/dev/zvol/tank/zerostun-b2"),
    }
}

#[tokio::test]
async fn zfs_filesystem_lifecycle_uses_exact_argv_and_reverse_cleanup() {
    let runner = FakeRunner::scripted([
        FakeRunnerScript::ok(FS_PROBE),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(FS_CLONE),
        FakeRunnerScript::ok(FS_CLONE),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
    ]);
    let provider = ZfsProvider::new(runner.clone());
    let cancel = CancellationToken::new();

    let created = provider
        .create(&SnapshotRequest::new("tank/data"), &cancel)
        .await
        .unwrap();
    let suffix = created.id.split_once('@').unwrap().1;
    let clone = format!("tank/{suffix}");
    assert_eq!(
        created.source,
        PathBuf::from(format!("/run/zerostun/zfs/tank_data_{suffix}"))
    );
    assert_eq!(
        provider.open_source(&fs_handle(), &cancel).await.unwrap(),
        fs_handle().source
    );
    provider.cleanup(&fs_handle(), &cancel).await.unwrap();

    let c = runner.recorded();
    assert_eq!(c[0].program, PathBuf::from("/usr/sbin/zfs"));
    assert_eq!(
        c[0].args,
        vec![
            "list",
            "-H",
            "-p",
            "-o",
            "name,type,mountpoint",
            "-t",
            "filesystem,volume",
            "tank/data"
        ]
    );
    assert_eq!(c[1].args, vec!["snapshot", created.id.as_str()]);
    assert_eq!(
        c[2].args,
        vec![
            "clone",
            "-o",
            "readonly=on",
            "-o",
            format!("mountpoint={}", created.source.display()).as_str(),
            created.id.as_str(),
            clone.as_str()
        ]
    );
    assert_eq!(c[3].args, vec!["mount", clone.as_str()]);
    assert_eq!(c[6].args, vec!["unmount", "tank/zerostun-a1"]);
    assert_eq!(c[7].args, vec!["destroy", "tank/zerostun-a1"]);
    assert_eq!(c[8].args, vec!["destroy", "tank/data@zerostun-a1"]);
}

#[tokio::test]
async fn zfs_zvol_lifecycle_exposes_read_only_clone_device_without_mount() {
    let runner = FakeRunner::scripted([
        FakeRunnerScript::ok(ZVOL_PROBE),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(ZVOL_CLONE),
        FakeRunnerScript::ok(ZVOL_CLONE),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
    ]);
    let provider = ZfsProvider::new(runner.clone());
    let cancel = CancellationToken::new();

    let created = provider
        .create(&SnapshotRequest::new("tank/vol"), &cancel)
        .await
        .unwrap();
    assert_eq!(
        created.source,
        PathBuf::from(format!(
            "/dev/zvol/tank/{}",
            created.id.split_once('@').unwrap().1
        ))
    );
    assert_eq!(
        provider.open_source(&zvol_handle(), &cancel).await.unwrap(),
        zvol_handle().source
    );
    provider.cleanup(&zvol_handle(), &cancel).await.unwrap();

    let c = runner.recorded();
    assert_eq!(c[1].args[0], "snapshot");
    assert_eq!(c[2].args[0], "clone");
    assert!(!c.iter().any(|cmd| matches!(
        cmd.args.first().map(String::as_str),
        Some("mount" | "unmount")
    )));
    assert_eq!(c[5].args, vec!["destroy", "tank/zerostun-b2"]);
    assert_eq!(c[6].args, vec!["destroy", "tank/vol@zerostun-b2"]);
}

#[tokio::test]
async fn zfs_probe_distinguishes_filesystem_and_zvol_capabilities() {
    let runner = FakeRunner::scripted([
        FakeRunnerScript::ok(FS_PROBE),
        FakeRunnerScript::ok(ZVOL_PROBE),
    ]);
    let provider = ZfsProvider::new(runner);
    let cancel = CancellationToken::new();
    let fs = provider.probe_target("tank/data", &cancel).await.unwrap();
    let volume = provider.probe_target("tank/vol", &cancel).await.unwrap();
    assert_eq!(fs.kind, ZfsTargetKind::Filesystem);
    assert!(fs.mounted_filesystem_source);
    assert!(!fs.block_device_source);
    assert_eq!(volume.kind, ZfsTargetKind::Volume);
    assert!(!volume.mounted_filesystem_source);
    assert!(volume.block_device_source);
    assert!(fs.capabilities.read_only && volume.capabilities.read_only);
}

#[tokio::test]
async fn zfs_recovery_removes_managed_clones_then_snapshots_in_reverse_order() {
    let clones = b"tank/zerostun-old1\tfilesystem\tyes\ttank/data@zerostun-old1\n\
                   tank/zerostun-old2\tvolume\t-\ttank/vol@zerostun-old2\n";
    let snapshots = b"tank/data@zerostun-old1\ntank/vol@zerostun-old2\ntank/orphan@zerostun-old3\n";
    let runner = FakeRunner::scripted([
        FakeRunnerScript::ok(clones),
        FakeRunnerScript::ok(snapshots),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
        FakeRunnerScript::ok(b""),
    ]);
    let provider = ZfsProvider::new(runner.clone());
    let recovered = provider.recover(&CancellationToken::new()).await.unwrap();
    assert_eq!(
        recovered,
        vec![
            "tank/vol@zerostun-old2",
            "tank/data@zerostun-old1",
            "tank/orphan@zerostun-old3"
        ]
    );
    let c = runner.recorded();
    assert_eq!(c[2].args, vec!["destroy", "tank/zerostun-old2"]);
    assert_eq!(c[3].args, vec!["destroy", "tank/vol@zerostun-old2"]);
    assert_eq!(c[4].args, vec!["unmount", "tank/zerostun-old1"]);
    assert_eq!(c[5].args, vec!["destroy", "tank/zerostun-old1"]);
    assert_eq!(c[6].args, vec!["destroy", "tank/data@zerostun-old1"]);
    assert_eq!(c[7].args, vec!["destroy", "tank/orphan@zerostun-old3"]);
}

#[tokio::test]
async fn zfs_rejects_mixed_or_unsupported_semantics_before_any_command() {
    let runner = FakeRunner::scripted([]);
    let provider = ZfsProvider::new(runner.clone());
    for request in [
        SnapshotRequest::new("tank/data,tank/vol"),
        SnapshotRequest::new("tank/data").require_changed_block(),
        SnapshotRequest::new("tank/data").require_quiesce(),
        SnapshotRequest::new("../tank/data"),
        SnapshotRequest::new("tank/data@snapshot"),
        SnapshotRequest::new("tank/data;destroy"),
    ] {
        assert!(provider
            .create(&request, &CancellationToken::new())
            .await
            .is_err());
    }
    assert!(runner.recorded().is_empty());
}

#[tokio::test]
async fn zfs_probe_create_and_open_failures_are_propagated() {
    let cancel = CancellationToken::new();
    let provider = ZfsProvider::new(FakeRunner::scripted([FakeRunnerScript::ok(b"bad")]));
    assert!(provider.probe(&cancel).await.is_err());

    for scripts in [
        vec![FakeRunnerScript::fail(2, b"", b"probe denied")],
        vec![
            FakeRunnerScript::ok(FS_PROBE),
            FakeRunnerScript::fail(3, b"", b"snapshot denied"),
        ],
        vec![
            FakeRunnerScript::ok(FS_PROBE),
            FakeRunnerScript::ok(b""),
            FakeRunnerScript::fail(4, b"", b"clone denied"),
        ],
        vec![
            FakeRunnerScript::ok(FS_PROBE),
            FakeRunnerScript::ok(b""),
            FakeRunnerScript::ok(b""),
            FakeRunnerScript::fail(5, b"", b"mount denied"),
        ],
    ] {
        let provider = ZfsProvider::new(FakeRunner::scripted(scripts));
        assert!(provider
            .create(&SnapshotRequest::new("tank/data"), &cancel)
            .await
            .is_err());
    }

    let provider = ZfsProvider::new(FakeRunner::scripted([FakeRunnerScript::fail(
        6,
        b"",
        b"open denied",
    )]));
    assert!(provider.open_source(&fs_handle(), &cancel).await.is_err());
}

#[tokio::test]
async fn zfs_cleanup_and_recovery_failures_stop_in_reverse_order() {
    let cancel = CancellationToken::new();
    for scripts in [
        vec![FakeRunnerScript::fail(2, b"", b"cleanup probe denied")],
        vec![
            FakeRunnerScript::ok(FS_CLONE),
            FakeRunnerScript::fail(3, b"", b"unmount denied"),
        ],
        vec![
            FakeRunnerScript::ok(FS_CLONE),
            FakeRunnerScript::ok(b""),
            FakeRunnerScript::fail(4, b"", b"clone destroy denied"),
        ],
        vec![
            FakeRunnerScript::ok(FS_CLONE),
            FakeRunnerScript::ok(b""),
            FakeRunnerScript::ok(b""),
            FakeRunnerScript::fail(5, b"", b"snapshot destroy denied"),
        ],
    ] {
        let provider = ZfsProvider::new(FakeRunner::scripted(scripts));
        assert!(provider.cleanup(&fs_handle(), &cancel).await.is_err());
    }

    for scripts in [
        vec![FakeRunnerScript::fail(
            6,
            b"",
            b"clone recovery probe denied",
        )],
        vec![
            FakeRunnerScript::ok(b""),
            FakeRunnerScript::fail(7, b"", b"snapshot recovery probe denied"),
        ],
        vec![
            FakeRunnerScript::ok(FS_CLONE),
            FakeRunnerScript::ok(b"tank/data@zerostun-a1\n"),
            FakeRunnerScript::fail(8, b"", b"recovery unmount denied"),
        ],
    ] {
        let provider = ZfsProvider::new(FakeRunner::scripted(scripts));
        assert!(provider.recover(&cancel).await.is_err());
    }
}

#[tokio::test]
async fn zfs_timeout_cancel_redaction_and_handle_validation_fail_closed() {
    let provider = ZfsProvider::new(FakeRunner::scripted([FakeRunnerScript::hang(
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
    let runner = FakeRunner::scripted([FakeRunnerScript::ok(FS_PROBE)]);
    let provider = ZfsProvider::new(runner.clone());
    assert!(matches!(
        provider.probe(&cancel).await.unwrap_err(),
        zerostun::Error::Cancelled
    ));
    assert!(runner.recorded().is_empty());

    let runner = FakeRunner::scripted([FakeRunnerScript::fail(1, b"", b"token=zfs-secret")]);
    let error = runner
        .run(
            &CommandSpec::new("/usr/sbin/zfs")
                .env("ZEROSTUN_TOKEN", "zfs-secret")
                .secret_env("ZEROSTUN_TOKEN"),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(!error.contains("zfs-secret"));
    assert!(error.contains("[redacted]"));

    let runner = FakeRunner::scripted([]);
    let provider = ZfsProvider::new(runner.clone());
    for bad in [
        SnapshotHandle {
            id: "tank/data@not-managed".into(),
            source: PathBuf::from("/tmp/escape"),
        },
        SnapshotHandle {
            id: "tank/data@zerostun-a1".into(),
            source: PathBuf::from("/tmp/escape"),
        },
        SnapshotHandle {
            id: "../tank@zerostun-a1".into(),
            source: PathBuf::from("/dev/zvol/escape"),
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
async fn zfs_host_tooling_is_detected_with_a_non_destructive_probe_only() {
    if !std::path::Path::new("/usr/sbin/zfs").is_file() {
        return;
    }
    let provider = ZfsProvider::new(ProcessRunner::new());
    if let Ok(capabilities) = provider.probe(&CancellationToken::new()).await {
        assert!(capabilities.crash_consistent);
        assert!(capabilities.read_only);
        assert!(!capabilities.quiesce);
        assert!(!capabilities.changed_block);
    }
}
