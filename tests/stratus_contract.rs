use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use zerostun::snapshot::{
    ApiAuth, ApplianceKind, FakeHttpScript, FakeHttpTransport, HttpMethod, SnapshotHandle,
    SnapshotProvider, SnapshotRequest, StratusConfig, StratusProvider,
};

fn fixture(name: &str) -> Vec<u8> {
    fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/stratus")
            .join(name),
    )
    .unwrap()
}

fn config(kind: ApplianceKind) -> StratusConfig {
    std::env::set_var("ZEROSTUN_TEST_STRATUS_TOKEN", "stratus-secret");
    StratusConfig {
        endpoint: "https://ft.example".into(),
        appliance: kind,
        workload: "workload-1".into(),
        auth: ApiAuth::Env("ZEROSTUN_TEST_STRATUS_TOKEN".into()),
    }
}

fn handle() -> SnapshotHandle {
    SnapshotHandle {
        id: "zerostun-a1".into(),
        source: PathBuf::from("/dev/stratus/workload-1/zerostun-a1"),
    }
}

#[tokio::test]
async fn everrun_and_ztc_probe_schema_variants_are_supported() {
    for (kind, fixture_name, path) in [
        (
            ApplianceKind::EverRun,
            "everrun-healthy.json",
            "/everrun/api/v1/pair/status",
        ),
        (
            ApplianceKind::ZtC,
            "ztc-healthy.json",
            "/ztc/api/v1/cluster/status",
        ),
    ] {
        let transport = FakeHttpTransport::scripted([fixture(fixture_name)]);
        let provider = StratusProvider::new(transport.clone(), config(kind));
        assert!(
            provider
                .probe(&CancellationToken::new())
                .await
                .unwrap()
                .read_only
        );
        assert_eq!(transport.recorded()[0].path, path);
    }
}

#[tokio::test]
async fn unsafe_ft_state_is_rejected_before_snapshot_mutation() {
    let transport = FakeHttpTransport::scripted([fixture("everrun-unsafe.json")]);
    let provider = StratusProvider::new(transport.clone(), config(ApplianceKind::EverRun));
    let error = provider
        .create(
            &SnapshotRequest::new("workload-1"),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("synchron"));
    assert_eq!(transport.recorded().len(), 1);
    assert_eq!(transport.recorded()[0].method, HttpMethod::Get);
}

#[tokio::test]
async fn stratus_lifecycle_and_recovery_use_exact_requests() {
    let transport = FakeHttpTransport::scripted([
        fixture("everrun-healthy.json"),
        b"{\"snapshot\":\"zerostun-a1\"}".to_vec(),
        b"{\"source\":\"/dev/stratus/workload-1/zerostun-a1\"}".to_vec(),
        b"{\"source\":\"/dev/stratus/workload-1/zerostun-a1\"}".to_vec(),
        b"".to_vec(),
        b"{\"snapshots\":[\"zerostun-old1\",\"user-snap\",\"zerostun-old2\"]}".to_vec(),
        b"{\"source\":\"/dev/stratus/workload-1/zerostun-old2\"}".to_vec(),
        b"".to_vec(),
        b"{\"source\":\"/dev/stratus/workload-1/zerostun-old1\"}".to_vec(),
        b"".to_vec(),
    ]);
    let provider = StratusProvider::new(transport.clone(), config(ApplianceKind::EverRun));
    let cancel = CancellationToken::new();
    let created = provider
        .create(&SnapshotRequest::new("workload-1"), &cancel)
        .await
        .unwrap();
    assert!(created.id.starts_with("zerostun-"));
    assert_eq!(
        provider.open_source(&handle(), &cancel).await.unwrap(),
        handle().source
    );
    provider.cleanup(&handle(), &cancel).await.unwrap();
    assert_eq!(
        provider.recover(&cancel).await.unwrap(),
        vec!["zerostun-old2", "zerostun-old1"]
    );
    let r = transport.recorded();
    assert_eq!(r[1].path, "/everrun/api/v1/workloads/workload-1/snapshots");
    assert_eq!(r[1].method, HttpMethod::Post);
    assert_eq!(r[3].method, HttpMethod::Get);
    assert_eq!(r[4].method, HttpMethod::Delete);
    assert_eq!(r[6].method, HttpMethod::Get);
    assert_eq!(r[7].method, HttpMethod::Delete);
    assert!(!format!("{r:?}").contains("stratus-secret"));
}

#[tokio::test]
async fn stratus_auth_failures_timeout_cancel_redaction_and_validation_fail_closed() {
    let transport = FakeHttpTransport::scripted(Vec::<Vec<u8>>::new());
    let mut cfg = config(ApplianceKind::ZtC);
    cfg.auth = ApiAuth::Env("ZEROSTUN_TEST_STRATUS_TOKEN_MISSING_71D9".into());
    let provider = StratusProvider::new(transport.clone(), cfg);
    assert!(provider.probe(&CancellationToken::new()).await.is_err());
    assert!(transport.recorded().is_empty());

    let provider = StratusProvider::new(
        FakeHttpTransport::hang(Duration::from_secs(30)),
        config(ApplianceKind::ZtC),
    );
    assert!(provider
        .probe(&CancellationToken::new())
        .await
        .unwrap_err()
        .to_string()
        .contains("timed out"));

    let cancel = CancellationToken::new();
    cancel.cancel();
    let transport = FakeHttpTransport::scripted([fixture("ztc-healthy.json")]);
    let provider = StratusProvider::new(transport.clone(), config(ApplianceKind::ZtC));
    assert!(matches!(
        provider.probe(&cancel).await.unwrap_err(),
        zerostun::Error::Cancelled
    ));
    assert!(transport.recorded().is_empty());

    let transport = FakeHttpTransport::scripted(Vec::<Vec<u8>>::new());
    let provider = StratusProvider::new(transport.clone(), config(ApplianceKind::ZtC));
    assert!(provider
        .create(&SnapshotRequest::new("../bad"), &CancellationToken::new())
        .await
        .is_err());
    assert!(provider
        .cleanup(
            &SnapshotHandle {
                id: "user-snap".into(),
                source: PathBuf::from("/tmp/x")
            },
            &CancellationToken::new()
        )
        .await
        .is_err());
    assert!(transport.recorded().is_empty());
}

#[tokio::test]
async fn stratus_rejects_insecure_endpoint_before_any_request() {
    let transport = FakeHttpTransport::scripted([fixture("everrun-healthy.json")]);
    let mut cfg = config(ApplianceKind::EverRun);
    cfg.endpoint = "http://ft.example".into();
    let provider = StratusProvider::new(transport.clone(), cfg);
    assert!(provider.probe(&CancellationToken::new()).await.is_err());
    assert!(transport.recorded().is_empty());
}

#[tokio::test]
async fn stratus_cleanup_rejects_unowned_snapshot_before_delete() {
    let transport = FakeHttpTransport::scripted([b"{}".to_vec()]);
    let provider = StratusProvider::new(transport.clone(), config(ApplianceKind::EverRun));
    assert!(provider
        .cleanup(&handle(), &CancellationToken::new())
        .await
        .is_err());
    let recorded = transport.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].method, HttpMethod::Get);
}

#[tokio::test]
async fn every_stratus_stage_failure_is_propagated() {
    let cancel = CancellationToken::new();

    let transport = FakeHttpTransport::failing(401, b"probe denied");
    let provider = StratusProvider::new(transport, config(ApplianceKind::EverRun));
    assert!(provider.probe(&cancel).await.is_err());

    let transport = FakeHttpTransport::scripted([
        FakeHttpScript::ok(fixture("everrun-healthy.json")),
        FakeHttpScript::fail(500, b"create denied"),
    ]);
    let provider = StratusProvider::new(transport, config(ApplianceKind::EverRun));
    assert!(provider
        .create(&SnapshotRequest::new("workload-1"), &cancel)
        .await
        .is_err());

    let transport = FakeHttpTransport::failing(404, b"open denied");
    let provider = StratusProvider::new(transport, config(ApplianceKind::EverRun));
    assert!(provider.open_source(&handle(), &cancel).await.is_err());

    let transport = FakeHttpTransport::failing(403, b"cleanup denied");
    let provider = StratusProvider::new(transport, config(ApplianceKind::EverRun));
    assert!(provider.cleanup(&handle(), &cancel).await.is_err());

    let transport = FakeHttpTransport::failing(503, b"recover denied");
    let provider = StratusProvider::new(transport, config(ApplianceKind::EverRun));
    assert!(provider.recover(&cancel).await.is_err());
}

#[test]
fn stratus_lab_credentials_are_detected_without_printing_values() {
    if std::env::var_os("ZEROSTUN_STRATUS_TOKEN").is_some()
        || std::env::var_os("ZEROSTUN_STRATUS_TOKEN_FILE").is_some()
    {
        eprintln!("stratus lab credentials configured: yes");
    }
}
