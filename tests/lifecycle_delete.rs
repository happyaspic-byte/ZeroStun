use redb::{Database, ReadableDatabase, TableDefinition};
use zerostun::codec::CompressionCodec;
use zerostun::config::BackupConfig;
use zerostun::error::Error;
use zerostun::hash::content_id_from_bytes;
use zerostun::manifest::{ChunkDescriptor, Manifest};
use zerostun::repository::Repository;

struct BackupFixture {
    _temp: tempfile::TempDir,
    repo: Repository,
    backup_id: String,
    source_bytes: Vec<u8>,
}

impl BackupFixture {
    fn chunk_paths(&self) -> Vec<std::path::PathBuf> {
        self.repo
            .load_manifest(&self.backup_id)
            .unwrap()
            .chunks
            .iter()
            .map(|chunk| self.repo.chunk_path(&chunk.content_id))
            .collect()
    }
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
async fn tombstone_hides_backup_without_deleting_chunks() {
    let fixture = backup_fixture().await;
    let chunk_paths = fixture.chunk_paths();
    let plan = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    assert!(!plan.already_deleted);

    let result = fixture.repo.apply_delete(&plan).unwrap();

    assert!(result.tombstoned);
    let load_error = fixture.repo.load_manifest(&fixture.backup_id).unwrap_err();
    assert!(matches!(
        load_error,
        Error::BackupDeleted(ref id) if id == &fixture.backup_id
    ));
    assert_eq!(load_error.exit_code(), zerostun::ExitCode::Repository);
    assert!(!fixture
        .repo
        .list_backups()
        .unwrap()
        .contains(&fixture.backup_id));
    assert!(fixture.repo.list_backup_summaries().unwrap().is_empty());
    assert!(matches!(
        zerostun::engine::inspect(&fixture.repo, &fixture.backup_id),
        Err(Error::BackupDeleted(ref id)) if id == &fixture.backup_id
    ));
    let verify = zerostun::engine::verify(&fixture.repo, &fixture.backup_id)
        .await
        .unwrap();
    assert!(!verify.is_ok());
    let restore_target = fixture._temp.path().join("restored.bin");
    let restore_error =
        zerostun::engine::restore(&fixture.repo, &fixture.backup_id, &restore_target, false)
            .await
            .unwrap_err();
    assert!(matches!(
        restore_error,
        Error::BackupDeleted(ref id) if id == &fixture.backup_id
    ));
    assert_eq!(restore_error.exit_code(), zerostun::ExitCode::Repository);
    assert!(!restore_target.exists());
    assert!(chunk_paths.iter().all(|path| path.exists()));
}

#[tokio::test]
async fn undelete_restores_visibility_verify_and_bytes_without_changing_chunks() {
    let fixture = backup_fixture().await;
    let chunk_paths = fixture.chunk_paths();
    let chunk_bytes: Vec<_> = chunk_paths
        .iter()
        .map(|path| std::fs::read(path).unwrap())
        .collect();
    let delete = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    fixture.repo.apply_delete(&delete).unwrap();

    let plan = fixture.repo.plan_undelete(&fixture.backup_id).unwrap();
    assert!(plan.tombstoned);
    let json = serde_json::to_value(&plan).unwrap();
    assert_eq!(
        serde_json::from_value::<zerostun::UndeletePlan>(json).unwrap(),
        plan
    );
    let result = fixture.repo.apply_undelete(&plan).unwrap();

    assert!(result.restored);
    assert!(fixture
        .repo
        .list_backups()
        .unwrap()
        .contains(&fixture.backup_id));
    assert_eq!(
        fixture
            .repo
            .load_manifest(&fixture.backup_id)
            .unwrap()
            .backup_id,
        fixture.backup_id
    );
    assert_eq!(
        zerostun::engine::inspect(&fixture.repo, &fixture.backup_id)
            .unwrap()
            .backup_id,
        fixture.backup_id
    );
    assert!(zerostun::engine::verify(&fixture.repo, &fixture.backup_id)
        .await
        .unwrap()
        .is_ok());
    let restored = fixture._temp.path().join("undeleted.bin");
    zerostun::engine::restore(&fixture.repo, &fixture.backup_id, &restored, false)
        .await
        .unwrap();
    assert_eq!(std::fs::read(restored).unwrap(), fixture.source_bytes);
    assert_eq!(
        chunk_paths
            .iter()
            .map(|path| std::fs::read(path).unwrap())
            .collect::<Vec<_>>(),
        chunk_bytes
    );
}

#[tokio::test]
async fn undelete_is_idempotent_and_missing_backup_rejects() {
    let fixture = backup_fixture().await;
    let delete = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    fixture.repo.apply_delete(&delete).unwrap();
    let first = fixture.repo.plan_undelete(&fixture.backup_id).unwrap();
    fixture.repo.apply_undelete(&first).unwrap();

    let second = fixture.repo.plan_undelete(&fixture.backup_id).unwrap();
    assert!(!second.tombstoned);
    assert!(!fixture.repo.apply_undelete(&second).unwrap().restored);
    assert!(matches!(
        fixture.repo.plan_undelete("backup-missing"),
        Err(Error::BackupNotFound(id)) if id == "backup-missing"
    ));
    let missing = zerostun::UndeletePlan {
        backup_id: "backup-missing".to_string(),
        tombstoned: true,
    };
    assert!(matches!(
        fixture.repo.apply_undelete(&missing),
        Err(Error::BackupNotFound(id)) if id == "backup-missing"
    ));
}

#[tokio::test]
async fn delete_plan_is_stable_json() {
    let fixture = backup_fixture().await;

    let plan = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    let json = serde_json::to_value(&plan).unwrap();

    assert_eq!(json["backup_id"], fixture.backup_id);
    assert_eq!(json["already_deleted"], false);
    assert_eq!(
        serde_json::from_value::<zerostun::DeletePlan>(json).unwrap(),
        plan
    );
}

#[tokio::test]
async fn completed_backup_record_remains_immutable() {
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    let fixture = backup_fixture().await;
    let db_path = fixture.repo.root().join("index.redb");
    let plan = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    fixture.repo.apply_delete(&plan).unwrap();
    drop(fixture.repo);

    let db = Database::open(db_path).unwrap();
    let read_txn = db.begin_read().unwrap();
    let backups = read_txn.open_table(BACKUPS).unwrap();

    assert!(backups.get(fixture.backup_id.as_str()).unwrap().is_some());
}

#[test]
fn v1_open_rejects_without_schema_or_version_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    make_v1_repository(&repo_path);
    let db_path = repo_path.join("index.redb");

    assert!(matches!(
        Repository::open(&repo_path),
        Err(Error::UnsupportedRepositoryVersion {
            found: 1,
            supported: 2
        })
    ));
    assert_eq!(
        std::fs::read_to_string(repo_path.join("VERSION")).unwrap(),
        "1\n"
    );

    let db = Database::open(db_path).unwrap();
    let read_txn = db.begin_read().unwrap();
    let names: Vec<_> = read_txn
        .list_tables()
        .unwrap()
        .map(|table| redb::TableHandle::name(&table).to_string())
        .collect();
    assert!(!names.contains(&"tombstones".to_string()));
}

#[test]
fn init_explicitly_migrates_v1_repository_to_v2() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    make_v1_repository(&repo_path);

    let repo = Repository::init(&repo_path).unwrap();

    assert_eq!(
        std::fs::read_to_string(repo_path.join("VERSION")).unwrap(),
        "2\n"
    );
    assert!(!repo.is_tombstoned("backup-existing-format").unwrap());
}

fn make_v1_repository(repo_path: &std::path::Path) {
    std::fs::create_dir_all(repo_path.join("chunks")).unwrap();
    std::fs::create_dir_all(repo_path.join("manifests")).unwrap();
    std::fs::create_dir_all(repo_path.join("tmp")).unwrap();
    std::fs::write(repo_path.join("VERSION"), "1\n").unwrap();
    let db = Database::create(repo_path.join("index.redb")).unwrap();
    let write_txn = db.begin_write().unwrap();
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    const CHUNKS: TableDefinition<&str, u64> = TableDefinition::new("chunks");
    let _ = write_txn.open_table(BACKUPS).unwrap();
    let _ = write_txn.open_table(CHUNKS).unwrap();
    write_txn.commit().unwrap();
}

#[test]
fn corrupt_indexed_manifest_can_be_tombstoned() {
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    drop(repo);
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut backups = write_txn.open_table(BACKUPS).unwrap();
        backups
            .insert("backup-corrupt", b"not-a-manifest".as_slice())
            .unwrap();
    }
    write_txn.commit().unwrap();
    drop(db);
    let repo = Repository::open(&repo_path).unwrap();

    let plan = repo.plan_delete("backup-corrupt").unwrap();
    assert!(!plan.already_deleted);
    assert!(repo.apply_delete(&plan).unwrap().tombstoned);
    assert!(matches!(
        repo.load_manifest("backup-corrupt"),
        Err(Error::BackupDeleted(id)) if id == "backup-corrupt"
    ));
    assert!(!repo
        .list_backups()
        .unwrap()
        .contains(&"backup-corrupt".to_string()));
}

#[test]
fn duplicate_live_backup_id_preserves_manifest_and_chunk_refs() {
    assert_rejected_manifest_replacement(false);
}

#[test]
fn tombstoned_backup_id_preserves_manifest_and_chunk_refs() {
    assert_rejected_manifest_replacement(true);
}

fn assert_rejected_manifest_replacement(tombstone: bool) {
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    const CHUNKS: TableDefinition<&str, u64> = TableDefinition::new("chunks");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let backup_id = "backup-immutable";
    let original_cid = content_id_from_bytes(b"original");
    let replacement_cid = content_id_from_bytes(b"replacement");
    let mut original = manifest_with_chunk(backup_id, original_cid);
    original.root_hash = zerostun::hash::root_hash_from_manifest(&original);
    repo.commit_manifest(&original).unwrap();
    if tombstone {
        let plan = repo.plan_delete(backup_id).unwrap();
        repo.apply_delete(&plan).unwrap();
    }
    let original_bytes = original.encode().unwrap();
    let mut replacement = manifest_with_chunk(backup_id, replacement_cid);
    replacement.root_hash = zerostun::hash::root_hash_from_manifest(&replacement);

    assert!(repo.commit_manifest(&replacement).is_err());
    drop(repo);

    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let read_txn = db.begin_read().unwrap();
    let backups = read_txn.open_table(BACKUPS).unwrap();
    assert_eq!(
        backups.get(backup_id).unwrap().unwrap().value(),
        original_bytes.as_slice()
    );
    let chunks = read_txn.open_table(CHUNKS).unwrap();
    assert_eq!(
        chunks
            .get(original_cid.to_hex().as_str())
            .unwrap()
            .unwrap()
            .value(),
        1
    );
    assert!(chunks
        .get(replacement_cid.to_hex().as_str())
        .unwrap()
        .is_none());
}

fn manifest_with_chunk(backup_id: &str, content_id: zerostun::ids::ContentId) -> Manifest {
    let mut manifest = Manifest::new(backup_id, 8, 64, 128, 256);
    manifest.add_chunk(ChunkDescriptor {
        index: 0,
        logical_offset: 0,
        original_length: 8,
        stored_length: 8,
        codec: CompressionCodec::None,
        content_id,
    });
    manifest
}

#[tokio::test]
async fn delete_is_idempotent() {
    let fixture = backup_fixture().await;
    let first = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    fixture.repo.apply_delete(&first).unwrap();

    let second = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    assert!(second.already_deleted);
    let result = fixture.repo.apply_delete(&second).unwrap();

    assert!(!result.tombstoned);
    assert_eq!(result.backup_id, fixture.backup_id);
}
