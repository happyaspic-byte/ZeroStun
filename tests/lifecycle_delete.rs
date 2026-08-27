use redb::{Database, ReadableDatabase, TableDefinition};
use zerostun::codec::CompressionCodec;
use zerostun::config::BackupConfig;
use zerostun::error::Error;
use zerostun::manifest::Manifest;
use zerostun::repository::Repository;

struct BackupFixture {
    _temp: tempfile::TempDir,
    repo: Repository,
    backup_id: String,
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
    std::fs::write(&source, vec![42_u8; 32 * 1024]).unwrap();
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
    assert!(matches!(
        fixture.repo.load_manifest(&fixture.backup_id),
        Err(Error::BackupDeleted(id)) if id == fixture.backup_id
    ));
    assert!(!fixture
        .repo
        .list_backups()
        .unwrap()
        .contains(&fixture.backup_id));
    assert!(fixture.repo.list_backup_summaries().unwrap().is_empty());
    assert!(zerostun::engine::inspect(&fixture.repo, &fixture.backup_id).is_err());
    let verify = zerostun::engine::verify(&fixture.repo, &fixture.backup_id)
        .await
        .unwrap();
    assert!(!verify.is_ok());
    let restore_target = fixture._temp.path().join("restored.bin");
    assert!(
        zerostun::engine::restore(&fixture.repo, &fixture.backup_id, &restore_target, false,)
            .await
            .is_err()
    );
    assert!(!restore_target.exists());
    assert!(chunk_paths.iter().all(|path| path.exists()));
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
fn existing_repository_format_gains_tombstone_table_on_open() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let mut manifest = Manifest::new("backup-existing-format", 0, 64, 128, 256);
    manifest.root_hash = zerostun::hash::root_hash_from_manifest(&manifest);
    repo.commit_manifest(&manifest).unwrap();
    drop(repo);

    let db_path = repo_path.join("index.redb");
    let db = Database::open(&db_path).unwrap();
    let write_txn = db.begin_write().unwrap();
    const TOMBSTONES: TableDefinition<&str, u64> = TableDefinition::new("tombstones");
    write_txn.delete_table(TOMBSTONES).unwrap();
    write_txn.commit().unwrap();
    drop(db);

    let repo = Repository::open(&repo_path).unwrap();
    assert!(!repo.is_tombstoned("backup-existing-format").unwrap());
    assert_eq!(repo.list_backups().unwrap(), ["backup-existing-format"]);
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
