use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use zerostun::snapshot::{
    ApiAuth, FakeHttpScript, FakeHttpTransport, HttpMethod, HttpRequest, HttpTransport,
    ProxmoxConfig, ProxmoxProvider, SnapshotHandle, SnapshotProvider, SnapshotRequest,
    MAX_HTTP_BODY_BYTES,
};

fn fixture(name: &str) -> Vec<u8> {
    fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/proxmox")
            .join(name),
    )
    .unwrap()
}

fn config() -> ProxmoxConfig {
    std::env::set_var("ZEROSTUN_TEST_PROXMOX_TOKEN", "pve-secret-token");
    ProxmoxConfig {
        endpoint: "https://pve.example:8006".into(),
        node: "pve".into(),
        vmid: 100,
        token_id: "backup@pve!zerostun".into(),
        auth: ApiAuth::Env("ZEROSTUN_TEST_PROXMOX_TOKEN".into()),
    }
}

fn handle() -> SnapshotHandle {
    SnapshotHandle {
        id: "zerostun-a1".into(),
        source: PathBuf::from("/dev/pve/vm-100-state-zerostun-a1"),
    }
}

#[tokio::test]
async fn proxmox_probe_create_open_cleanup_and_recover_use_exact_requests() {
    let transport = FakeHttpTransport::scripted([
        fixture("vm-status.json"),
        fixture("storage.json"),
        fixture("vm-status.json"),
        fixture("storage.json"),
        b"{\"data\":{\"name\":\"zerostun-created\"}}".to_vec(),
        b"{\"data\":{\"name\":\"zerostun-a1\"}}".to_vec(),
        b"{\"data\":{\"name\":\"zerostun-a1\"}}".to_vec(),
        b"".to_vec(),
        fixture("snapshots.json"),
        b"{\"data\":{\"name\":\"zerostun-old2\"}}".to_vec(),
        b"".to_vec(),
        b"{\"data\":{\"name\":\"zerostun-old1\"}}".to_vec(),
        b"".to_vec(),
    ]);
    let provider = ProxmoxProvider::new(transport.clone(), config());
    let cancel = CancellationToken::new();
    let caps = provider.probe(&cancel).await.unwrap();
    assert!(caps.crash_consistent && caps.read_only);

    let created = provider
        .create(&SnapshotRequest::new("100"), &cancel)
        .await
        .unwrap();
    assert!(created.id.starts_with("zerostun-"));
    assert_eq!(
        created.source,
        PathBuf::from(format!("/dev/pve/vm-100-state-{}", created.id))
    );
    assert_eq!(
        provider.open_source(&handle(), &cancel).await.unwrap(),
        handle().source
    );
    provider.cleanup(&handle(), &cancel).await.unwrap();
    assert_eq!(
        provider.recover(&cancel).await.unwrap(),
        vec!["zerostun-old2".to_string(), "zerostun-old1".to_string()]
    );

    let recorded = transport.recorded();
    assert_eq!(recorded[0].method, HttpMethod::Get);
    assert_eq!(
        recorded[0].path,
        "/api2/json/nodes/pve/qemu/100/status/current"
    );
    assert_eq!(recorded[1].path, "/api2/json/nodes/pve/storage");
    assert_eq!(recorded[2].method, HttpMethod::Get);
    assert_eq!(
        recorded[2].path,
        "/api2/json/nodes/pve/qemu/100/status/current"
    );
    assert_eq!(recorded[3].path, "/api2/json/nodes/pve/storage");
    assert_eq!(recorded[4].method, HttpMethod::Post);
    assert_eq!(recorded[4].path, "/api2/json/nodes/pve/qemu/100/snapshot");
    let created_body = recorded[4].body.as_deref().unwrap();
    assert!(created_body.starts_with("snapname=zerostun-"));
    assert!(!created_body.contains('&'));
    assert_eq!(recorded[4].endpoint, "https://pve.example:8006");
    assert_eq!(recorded[5].method, HttpMethod::Get);
    assert_eq!(
        recorded[5].path,
        "/api2/json/nodes/pve/qemu/100/snapshot/zerostun-a1"
    );
    assert_eq!(recorded[6].method, HttpMethod::Get);
    assert_eq!(recorded[6].path, recorded[5].path);
    assert_eq!(recorded[7].method, HttpMethod::Delete);
    assert_eq!(
        recorded[7].path,
        "/api2/json/nodes/pve/qemu/100/snapshot/zerostun-a1"
    );
    assert_eq!(recorded[8].path, "/api2/json/nodes/pve/qemu/100/snapshot");
    assert_eq!(recorded[9].method, HttpMethod::Get);
    assert_eq!(
        recorded[9].path,
        "/api2/json/nodes/pve/qemu/100/snapshot/zerostun-old2"
    );
    assert_eq!(recorded[10].method, HttpMethod::Delete);
    assert_eq!(recorded[10].path, recorded[9].path);
    assert_eq!(recorded[11].method, HttpMethod::Get);
    assert_eq!(
        recorded[11].path,
        "/api2/json/nodes/pve/qemu/100/snapshot/zerostun-old1"
    );
    assert_eq!(recorded[12].method, HttpMethod::Delete);
    assert_eq!(recorded[12].path, recorded[11].path);
    let debug = format!("{recorded:?}");
    assert!(!debug.contains("pve-secret-token"));
}

#[tokio::test]
async fn proxmox_rejects_missing_token_and_insecure_token_file_before_any_request() {
    let transport = FakeHttpTransport::scripted(Vec::<Vec<u8>>::new());
    let mut missing_env = config();
    missing_env.auth = ApiAuth::Env("ZEROSTUN_TEST_TOKEN_MUST_NOT_EXIST_71D9".into());
    let provider = ProxmoxProvider::new(transport.clone(), missing_env);
    assert!(provider.probe(&CancellationToken::new()).await.is_err());
    assert!(transport.recorded().is_empty());

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("token");
    fs::write(&path, "pve-secret-token").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    let mut file_auth = config();
    file_auth.auth = ApiAuth::File(path);
    let provider = ProxmoxProvider::new(transport.clone(), file_auth);
    let error = provider
        .create(&SnapshotRequest::new("100"), &CancellationToken::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("0600"));
    assert!(transport.recorded().is_empty());
}

#[tokio::test]
async fn proxmox_classifies_probe_create_open_cleanup_and_recover_failures() {
    let cancel = CancellationToken::new();
    let provider = ProxmoxProvider::new(
        FakeHttpTransport::scripted([b"not-json".to_vec()]),
        config(),
    );
    assert!(provider.probe(&cancel).await.is_err());

    let provider = ProxmoxProvider::new(
        FakeHttpTransport::failing(401, b"{\"data\":null}"),
        config(),
    );
    assert!(provider.probe(&cancel).await.is_err());

    let provider = ProxmoxProvider::new(
        FakeHttpTransport::scripted([
            FakeHttpScript::ok(fixture("vm-status.json")),
            FakeHttpScript::ok(fixture("storage.json")),
            FakeHttpScript::fail(500, b"create denied"),
        ]),
        config(),
    );
    assert!(provider
        .create(&SnapshotRequest::new("100"), &cancel)
        .await
        .is_err());

    let provider = ProxmoxProvider::new(
        FakeHttpTransport::scripted([
            fixture("vm-status.json"),
            fixture("storage.json"),
            b"{\"data\":\"UPID:pve:0000:zerostun\"}".to_vec(),
        ]),
        config(),
    );
    assert!(provider
        .create(&SnapshotRequest::new("100"), &cancel)
        .await
        .unwrap_err()
        .to_string()
        .contains("UPID"));

    let provider = ProxmoxProvider::new(FakeHttpTransport::failing(404, b"open denied"), config());
    assert!(provider.open_source(&handle(), &cancel).await.is_err());

    let provider =
        ProxmoxProvider::new(FakeHttpTransport::failing(403, b"cleanup denied"), config());
    assert!(provider.cleanup(&handle(), &cancel).await.is_err());

    let provider =
        ProxmoxProvider::new(FakeHttpTransport::failing(503, b"recover denied"), config());
    assert!(provider.recover(&cancel).await.is_err());
}

#[tokio::test]
async fn proxmox_timeout_cancel_and_redaction_are_enforced() {
    let provider = ProxmoxProvider::new(FakeHttpTransport::hang(Duration::from_secs(30)), config());
    assert!(provider
        .probe(&CancellationToken::new())
        .await
        .unwrap_err()
        .to_string()
        .contains("timed out"));

    let cancel = CancellationToken::new();
    cancel.cancel();
    let transport = FakeHttpTransport::scripted([fixture("vm-status.json")]);
    let provider = ProxmoxProvider::new(transport.clone(), config());
    assert!(matches!(
        provider.probe(&cancel).await.unwrap_err(),
        zerostun::Error::Cancelled
    ));
    assert!(transport.recorded().is_empty());

    let transport = FakeHttpTransport::failing(401, b"token=pve-secret-token");
    let error = transport
        .send(
            &HttpRequest {
                method: HttpMethod::Get,
                endpoint: "https://pve.example:8006".into(),
                path: "/probe".into(),
                body: None,
                headers: vec![(
                    "Authorization".into(),
                    "PVEAPIToken=pve-secret-token".into(),
                )],
                secret_values: vec!["pve-secret-token".into()],
            },
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(!error.contains("pve-secret-token"));
    assert!(error.contains("[redacted]"));
}

#[tokio::test]
async fn proxmox_rejects_unsupported_storage_before_snapshot_mutation() {
    let storage =
        br#"{"data":[{"storage":"local-dir","type":"lvmthin","content":"iso","active":1}]}"#;
    let transport = FakeHttpTransport::scripted([fixture("vm-status.json"), storage.to_vec()]);
    let provider = ProxmoxProvider::new(transport.clone(), config());
    assert!(provider
        .create(&SnapshotRequest::new("100"), &CancellationToken::new())
        .await
        .is_err());
    let recorded = transport.recorded();
    assert_eq!(recorded.len(), 2);
    assert!(recorded
        .iter()
        .all(|request| request.method == HttpMethod::Get));
}

#[tokio::test]
async fn http_transport_rejects_unsafe_paths_and_oversized_responses() {
    let transport = FakeHttpTransport::scripted([b"unused".to_vec()]);
    let unsafe_request = HttpRequest {
        method: HttpMethod::Get,
        endpoint: "https://pve.example:8006".into(),
        path: "https://attacker.example/probe".into(),
        body: None,
        headers: Vec::new(),
        secret_values: Vec::new(),
    };
    assert!(transport
        .send(
            &unsafe_request,
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .is_err());
    assert!(transport.recorded().is_empty());

    let oversized = FakeHttpTransport::scripted([vec![b'x'; MAX_HTTP_BODY_BYTES + 1]]);
    let request = HttpRequest {
        method: HttpMethod::Get,
        endpoint: "https://pve.example:8006".into(),
        path: "/probe".into(),
        body: None,
        headers: Vec::new(),
        secret_values: Vec::new(),
    };
    assert!(oversized
        .send(&request, Duration::from_secs(1), &CancellationToken::new(),)
        .await
        .unwrap_err()
        .to_string()
        .contains("bounded"));

    let scheme_relative = HttpRequest {
        method: HttpMethod::Get,
        endpoint: "https://pve.example:8006".into(),
        path: "//attacker.example/probe".into(),
        body: None,
        headers: Vec::new(),
        secret_values: Vec::new(),
    };
    assert!(transport
        .send(
            &scheme_relative,
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .is_err());
}

#[tokio::test]
async fn provider_configuration_and_token_placement_fail_closed() {
    let transport = FakeHttpTransport::scripted([fixture("vm-status.json")]);
    let mut insecure = config();
    insecure.endpoint = "http://pve.example:8006".into();
    let provider = ProxmoxProvider::new(transport.clone(), insecure);
    assert!(provider.probe(&CancellationToken::new()).await.is_err());
    assert!(transport.recorded().is_empty());

    let mut unsafe_token_id = config();
    unsafe_token_id.token_id = "backup@pve!zerostun\r\nInjected: yes".into();
    let provider = ProxmoxProvider::new(transport.clone(), unsafe_token_id);
    assert!(provider.probe(&CancellationToken::new()).await.is_err());
    assert!(transport.recorded().is_empty());

    let token_in_body = HttpRequest {
        method: HttpMethod::Post,
        endpoint: "https://pve.example:8006".into(),
        path: "/probe".into(),
        body: Some("token=pve-secret-token".into()),
        headers: vec![(
            "Authorization".into(),
            "PVEAPIToken=pve-secret-token".into(),
        )],
        secret_values: vec!["pve-secret-token".into()],
    };
    assert!(transport
        .send(
            &token_in_body,
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .is_err());
    assert!(transport.recorded().is_empty());
}

#[tokio::test]
async fn proxmox_rejects_unsafe_target_and_handle_before_mutation() {
    let transport = FakeHttpTransport::scripted(Vec::<Vec<u8>>::new());
    let provider = ProxmoxProvider::new(transport.clone(), config());
    for target in ["../100", "100;rm", "not-a-vm", ""] {
        assert!(provider
            .create(&SnapshotRequest::new(target), &CancellationToken::new())
            .await
            .is_err());
    }
    for bad in [
        SnapshotHandle {
            id: "current".into(),
            source: PathBuf::from("/dev/pve/vm-100-state-current"),
        },
        SnapshotHandle {
            id: "zerostun-a1".into(),
            source: PathBuf::from("/tmp/escape"),
        },
        SnapshotHandle {
            id: "../zerostun-a1".into(),
            source: PathBuf::from("/dev/pve/unsafe"),
        },
    ] {
        assert!(provider
            .cleanup(&bad, &CancellationToken::new())
            .await
            .is_err());
    }
    assert!(transport.recorded().is_empty());
}

#[test]
fn proxmox_token_file_mode_0600_is_accepted_without_logging_the_secret() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("token");
    fs::write(&path, "pve-secret-token").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let mut cfg = config();
    cfg.auth = ApiAuth::File(path);
    let loaded = cfg.load_token().unwrap();
    assert_eq!(loaded, "pve-secret-token");
    let debug = format!("{cfg:?}");
    assert!(!debug.contains("pve-secret-token"));
}

#[test]
fn proxmox_lab_credentials_are_detected_without_printing_values() {
    let present = std::env::var_os("ZEROSTUN_PROXMOX_TOKEN").is_some()
        || std::env::var_os("ZEROSTUN_PROXMOX_TOKEN_FILE").is_some();
    if present {
        eprintln!("proxmox lab credentials configured: yes");
    }
}
