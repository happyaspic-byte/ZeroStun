use redb::{Database, ReadableDatabase, TableDefinition};
use zerostun::codec::CompressionCodec;
use zerostun::config::BackupConfig;
use zerostun::error::Error;
use zerostun::repository::Repository;

struct BackupFixture {
    _temp: tempfile::TempDir,
    repo: Repository,
    backup_id: String,
    source_bytes: Vec<u8>,
}

async fn backup_fixture() -> BackupFixture {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let source = temp.path().join("source.bin");
    let source_bytes = vec![42_u8; 32 * 1024];
    std::fs::write(&source, &source_bytes).unwrap();
    let config = BackupConfig {
        min_chunk: 1024,
        avg_chunk: 4096,
        max_chunk: 8192,
        codec: CompressionCodec::None,
        ..Default::default()
    };
    let summary = zerostun::engine::backup(&repo, &source, &config)
        .await
        .unwrap();
    BackupFixture {
        _temp: temp,
        repo,
        backup_id: summary.backup_id,
        source_bytes,
    }
}

#[tokio::test]
async fn reader_lease_is_visible_until_guard_drops() {
    let fixture = backup_fixture().await;
    let guard = fixture
        .repo
        .acquire_reader_lease(&fixture.backup_id)
        .unwrap();
    let leases = fixture.repo.active_reader_leases().unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].backup_id, fixture.backup_id);
    drop(guard);
    assert!(fixture.repo.active_reader_leases().unwrap().is_empty());
}

#[tokio::test]
async fn deleted_and_missing_lease_errors_keep_repository_classification() {
    let fixture = backup_fixture().await;
    let plan = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    fixture.repo.apply_delete(&plan).unwrap();
    let deleted = fixture
        .repo
        .acquire_reader_lease(&fixture.backup_id)
        .unwrap_err();
    assert!(matches!(deleted, Error::BackupDeleted(ref id) if id == &fixture.backup_id));
    assert_eq!(deleted.exit_code(), zerostun::ExitCode::Repository);
    let missing = fixture
        .repo
        .acquire_reader_lease("backup-missing")
        .unwrap_err();
    assert!(matches!(missing, Error::BackupNotFound(ref id) if id == "backup-missing"));
    assert_eq!(missing.exit_code(), zerostun::ExitCode::Repository);
}

#[tokio::test]
async fn verify_holds_reader_lease_until_completion() {
    let fixture = backup_fixture().await;
    let report = zerostun::engine::verify(&fixture.repo, &fixture.backup_id)
        .await
        .unwrap();
    assert!(report.is_ok());
    assert!(fixture.repo.active_reader_leases().unwrap().is_empty());
}

#[tokio::test]
async fn restore_uses_single_reader_lease() {
    let fixture = backup_fixture().await;
    let target = fixture._temp.path().join("restore.bin");
    zerostun::engine::restore(&fixture.repo, &fixture.backup_id, &target, false)
        .await
        .unwrap();
    assert_eq!(std::fs::read(target).unwrap(), fixture.source_bytes);
    assert!(fixture.repo.active_reader_leases().unwrap().is_empty());
}

#[test]
fn live_current_process_lease_is_not_stale() {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let manifest = zerostun::manifest::Manifest::new("backup-live", 0, 64, 128, 256);
    repo.commit_manifest(&manifest).unwrap();
    let _guard = repo.acquire_reader_lease("backup-live").unwrap();
    assert!(repo.remove_stale_reader_leases().unwrap().is_empty());
    assert_eq!(repo.active_reader_leases().unwrap().len(), 1);
}

#[cfg(target_os = "linux")]
#[test]
fn stale_dead_pid_is_removed() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    drop(repo);
    insert_lease(
        &repo_path,
        &zerostun::ReaderLease {
            lease_id: "lease-dead".to_string(),
            backup_id: "backup-dead".to_string(),
            pid: u32::MAX,
            process_start_token: "0".to_string(),
            acquired_unix_ms: 1,
        },
    );
    let repo = Repository::open(&repo_path).unwrap();

    assert_eq!(
        repo.remove_stale_reader_leases().unwrap(),
        vec!["lease-dead"]
    );
    assert!(repo.active_reader_leases().unwrap().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn pid_reuse_start_token_mismatch_is_removed() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    drop(repo);
    insert_lease(
        &repo_path,
        &zerostun::ReaderLease {
            lease_id: "lease-reused".to_string(),
            backup_id: "backup-reused".to_string(),
            pid: std::process::id(),
            process_start_token: "definitely-not-current-token".to_string(),
            acquired_unix_ms: 1,
        },
    );
    let repo = Repository::open(&repo_path).unwrap();

    assert_eq!(
        repo.remove_stale_reader_leases().unwrap(),
        vec!["lease-reused"]
    );
}

#[test]
fn open_does_not_create_missing_reader_lease_table() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    std::fs::create_dir_all(repo_path.join("chunks")).unwrap();
    std::fs::create_dir_all(repo_path.join("manifests")).unwrap();
    std::fs::create_dir_all(repo_path.join("tmp")).unwrap();
    std::fs::write(repo_path.join("VERSION"), "2\n").unwrap();
    let db = Database::create(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    const CHUNKS: TableDefinition<&str, u64> = TableDefinition::new("chunks");
    const TOMBSTONES: TableDefinition<&str, u64> = TableDefinition::new("tombstones");
    let _ = write.open_table(BACKUPS).unwrap();
    let _ = write.open_table(CHUNKS).unwrap();
    let _ = write.open_table(TOMBSTONES).unwrap();
    write.commit().unwrap();
    drop(db);

    let repo = Repository::open(&repo_path).unwrap();
    assert!(repo.active_reader_leases().is_err());
    drop(repo);

    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let read = db.begin_read().unwrap();
    let names: Vec<_> = read
        .list_tables()
        .unwrap()
        .map(|table| redb::TableHandle::name(&table).to_string())
        .collect();
    assert!(!names.contains(&"reader_leases".to_string()));
}

fn insert_lease(repo_path: &std::path::Path, lease: &zerostun::ReaderLease) {
    const READER_LEASES: TableDefinition<&str, &[u8]> = TableDefinition::new("reader_leases");
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let encoded = serde_json::to_vec(lease).unwrap();
        let mut table = write.open_table(READER_LEASES).unwrap();
        table
            .insert(lease.lease_id.as_str(), encoded.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
}
