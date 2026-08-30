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

fn commit_payloads(repo: &Repository, backup_id: &str, payloads: &[&[u8]]) -> Vec<String> {
    use zerostun::hash::{content_id_from_bytes, root_hash_from_manifest};
    use zerostun::manifest::{ChunkDescriptor, Manifest};

    let mut manifest = Manifest::new(backup_id, 0, 64, 128, 256);
    let mut offset = 0_u64;
    let mut ids = Vec::new();
    for (index, payload) in payloads.iter().enumerate() {
        let cid = content_id_from_bytes(payload);
        repo.write_chunk(&cid, CompressionCodec::None, payload)
            .unwrap();
        manifest.add_chunk(ChunkDescriptor {
            index: index as u64,
            logical_offset: offset,
            original_length: payload.len() as u64,
            stored_length: payload.len() as u64,
            codec: CompressionCodec::None,
            content_id: cid,
        });
        offset += payload.len() as u64;
        ids.push(cid.to_hex());
    }
    manifest.total_logical_bytes = offset;
    manifest.root_hash = root_hash_from_manifest(&manifest);
    repo.commit_manifest(&manifest).unwrap();
    ids
}

#[test]
fn gc_preserves_chunks_referenced_by_live_backup() {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let first = commit_payloads(&repo, "backup-first", &[b"first", b"shared"]);
    let second = commit_payloads(&repo, "backup-second", &[b"shared", b"second"]);
    repo.apply_delete(&repo.plan_delete("backup-first").unwrap())
        .unwrap();

    let plan = repo.plan_gc().unwrap();

    assert!(plan
        .reclaim_chunks
        .iter()
        .any(|item| item.content_id == first[0]));
    assert!(!plan
        .reclaim_chunks
        .iter()
        .any(|item| item.content_id == first[1]));
    repo.apply_gc(&plan).unwrap();
    let shared = zerostun::ids::ContentId::parse(&second[0]).unwrap();
    assert!(repo.read_chunk(&shared).is_ok());
}

#[test]
fn gc_reclaims_last_reference_and_orphan() {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let owned = commit_payloads(&repo, "backup-only", &[b"owned-a", b"owned-b"]);
    let orphan = zerostun::hash::content_id_from_bytes(b"orphan");
    repo.write_chunk(&orphan, CompressionCodec::None, b"orphan")
        .unwrap();
    repo.apply_delete(&repo.plan_delete("backup-only").unwrap())
        .unwrap();

    let plan = repo.plan_gc().unwrap();

    assert_eq!(plan.reclaim_chunks.len(), owned.len() + 1);
    assert!(plan
        .reclaim_chunks
        .windows(2)
        .all(|pair| pair[0].content_id < pair[1].content_id));
    let result = repo.apply_gc(&plan).unwrap();
    assert_eq!(result.reclaimed_chunks, plan.reclaim_chunks.len() as u64);
    assert_eq!(result.reclaimed_bytes, plan.reclaim_bytes);
    assert!(repo.plan_undelete("backup-only").is_err());
}

#[test]
fn gc_refuses_active_reader_lease_at_plan_and_apply() {
    let fixture = futures_lite_backup_fixture();
    let guard = fixture
        .repo
        .acquire_reader_lease(&fixture.backup_id)
        .unwrap();
    let error = fixture.repo.plan_gc().unwrap_err();
    assert!(error.to_string().contains("active reader"));
    drop(guard);
    let plan = fixture.repo.plan_gc().unwrap();
    let _guard = fixture
        .repo
        .acquire_reader_lease(&fixture.backup_id)
        .unwrap();
    let error = fixture.repo.apply_gc(&plan).unwrap_err();
    assert!(error.to_string().contains("active reader"));
}

fn futures_lite_backup_fixture() -> BackupFixture {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(backup_fixture())
}

#[test]
fn gc_plan_rejects_corrupt_live_manifest_and_unexpected_chunk_names() {
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    drop(repo);
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut backups = write.open_table(BACKUPS).unwrap();
        backups
            .insert("backup-corrupt", b"corrupt".as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    drop(db);
    let repo = Repository::open(&repo_path).unwrap();
    assert!(matches!(repo.plan_gc(), Err(Error::ManifestCorrupt(_))));
    drop(repo);

    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut backups = write.open_table(BACKUPS).unwrap();
        backups.remove("backup-corrupt").unwrap();
    }
    write.commit().unwrap();
    drop(db);
    std::fs::write(repo_path.join("chunks").join("unexpected"), b"data").unwrap();
    let repo = Repository::open(&repo_path).unwrap();
    assert!(repo
        .plan_gc()
        .unwrap_err()
        .to_string()
        .contains("unexpected chunk"));

    std::fs::remove_file(repo_path.join("chunks").join("unexpected")).unwrap();
    std::fs::create_dir(repo_path.join("chunks").join("AA")).unwrap();
    assert!(repo
        .plan_gc()
        .unwrap_err()
        .to_string()
        .contains("unexpected chunk"));
}

#[test]
fn gc_fails_closed_for_corrupt_tombstoned_manifest() {
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    drop(repo);
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut backups = write.open_table(BACKUPS).unwrap();
        backups
            .insert("backup-corrupt-deleted", b"corrupt".as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    drop(db);
    let repo = Repository::open(&repo_path).unwrap();
    repo.apply_delete(&repo.plan_delete("backup-corrupt-deleted").unwrap())
        .unwrap();

    assert!(matches!(repo.plan_gc(), Err(Error::ManifestCorrupt(_))));
}

#[test]
fn gc_reports_missing_live_chunk_instead_of_ignoring_it() {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let ids = commit_payloads(&repo, "backup-live-missing", &[b"missing"]);
    let cid = zerostun::ids::ContentId::parse(&ids[0]).unwrap();
    std::fs::remove_file(repo.chunk_path(&cid)).unwrap();

    assert!(matches!(
        repo.plan_gc(),
        Err(Error::ChunkMissing { content_id }) if content_id == ids[0]
    ));
}

#[test]
fn apply_rejects_forged_paths_metadata_and_stale_plan_before_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let ids = commit_payloads(&repo, "backup-doomed", &[b"owned"]);
    repo.apply_delete(&repo.plan_delete("backup-doomed").unwrap())
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let source = repo.root().join(&plan.reclaim_chunks[0].source);
    let original = std::fs::read(&source).unwrap();

    for mutate in 0..6 {
        let mut forged = plan.clone();
        match mutate {
            0 => forged.reclaim_chunks[0].source = std::path::PathBuf::from("../outside"),
            1 => forged.reclaim_chunks[0].trash = std::path::PathBuf::from("../outside"),
            2 => forged.reclaim_chunks[0].bytes += 1,
            3 => forged.reclaim_chunks[0].content_id = "00".repeat(32),
            4 => forged.live_chunks += 1,
            _ => forged.reclaim_chunks.push(forged.reclaim_chunks[0].clone()),
        }
        assert!(repo.apply_gc(&forged).is_err());
        assert_eq!(std::fs::read(&source).unwrap(), original);
    }

    let live = commit_payloads(&repo, "backup-new-live", &[b"owned"]);
    assert_eq!(live, ids);
    assert!(repo.apply_gc(&plan).is_err());
    assert!(source.exists());
    assert_eq!(ids.len(), 1);
}

#[cfg(unix)]
#[test]
fn apply_rejects_chunk_symlink_without_following_it() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let cid = zerostun::hash::content_id_from_bytes(b"orphan");
    repo.write_chunk(&cid, CompressionCodec::None, b"orphan")
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let path = repo.chunk_path(&cid);
    let external = temp.path().join("external");
    std::fs::write(&external, b"external").unwrap();
    std::fs::remove_file(&path).unwrap();
    symlink(&external, &path).unwrap();

    assert!(repo.apply_gc(&plan).is_err());
    assert_eq!(std::fs::read(external).unwrap(), b"external");
}

#[test]
fn init_creates_gc_journal_table_but_open_does_not() {
    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    drop(repo);
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let read = db.begin_read().unwrap();
    assert!(read.open_table(GC_JOURNALS).is_ok());
}

#[test]
fn gc_finalizes_tombstone_manifest_and_index() {
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    const TOMBSTONES: TableDefinition<&str, u64> = TableDefinition::new("tombstones");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    commit_payloads(&repo, "backup-final", &[b"final"]);
    repo.apply_delete(&repo.plan_delete("backup-final").unwrap())
        .unwrap();
    repo.apply_gc(&repo.plan_gc().unwrap()).unwrap();
    drop(repo);

    assert!(!repo_path.join("manifests/backup-final.manifest").exists());
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let read = db.begin_read().unwrap();
    assert!(read
        .open_table(BACKUPS)
        .unwrap()
        .get("backup-final")
        .unwrap()
        .is_none());
    assert!(read
        .open_table(TOMBSTONES)
        .unwrap()
        .get("backup-final")
        .unwrap()
        .is_none());
}

#[test]
fn recover_gc_rolls_back_uncommitted_moves_from_fresh_open() {
    use zerostun::{GcJournal, GcPhase};

    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let cid = zerostun::hash::content_id_from_bytes(b"orphan");
    repo.write_chunk(&cid, CompressionCodec::None, b"orphan")
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let item = &plan.reclaim_chunks[0];
    let trash = repo.root().join(&item.trash);
    std::fs::create_dir_all(trash.parent().unwrap()).unwrap();
    std::fs::rename(repo.root().join(&item.source), &trash).unwrap();
    let journal = GcJournal {
        plan: plan.clone(),
        phase: GcPhase::Moving,
        moved: Vec::new(),
    };
    drop(repo);
    insert_journal(&repo_path, &journal, GC_JOURNALS);

    let reopened = Repository::open(&repo_path).unwrap();
    let recovered = reopened.recover_gc().unwrap();
    assert_eq!(recovered[0].phase, GcPhase::Planned);
    assert!(reopened.chunk_path(&cid).exists());
    assert!(!trash.exists());
    assert!(reopened.recover_gc().unwrap().is_empty());
}

#[test]
fn recover_gc_reports_missing_uncommitted_chunk() {
    use zerostun::{GcJournal, GcPhase};

    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let cid = zerostun::hash::content_id_from_bytes(b"missing-orphan");
    repo.write_chunk(&cid, CompressionCodec::None, b"missing-orphan")
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    std::fs::remove_file(repo.chunk_path(&cid)).unwrap();
    let journal = GcJournal {
        plan,
        phase: GcPhase::Moving,
        moved: vec![cid.to_hex()],
    };
    drop(repo);
    insert_journal(&repo_path, &journal, GC_JOURNALS);

    let reopened = Repository::open(&repo_path).unwrap();
    assert!(reopened
        .recover_gc()
        .unwrap_err()
        .to_string()
        .contains("missing source and trash"));
}

#[test]
fn recover_gc_rolls_forward_committed_work_from_fresh_open() {
    use zerostun::{GcJournal, GcPhase};

    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let cid = zerostun::hash::content_id_from_bytes(b"orphan");
    repo.write_chunk(&cid, CompressionCodec::None, b"orphan")
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let item = &plan.reclaim_chunks[0];
    let trash = repo.root().join(&item.trash);
    std::fs::create_dir_all(trash.parent().unwrap()).unwrap();
    std::fs::rename(repo.root().join(&item.source), &trash).unwrap();
    let journal = GcJournal {
        plan: plan.clone(),
        phase: GcPhase::Committed,
        moved: vec![cid.to_hex()],
    };
    drop(repo);
    insert_journal(&repo_path, &journal, GC_JOURNALS);

    let reopened = Repository::open(&repo_path).unwrap();
    let recovered = reopened.recover_gc().unwrap();
    assert_eq!(recovered[0].phase, GcPhase::Complete);
    assert!(!reopened.chunk_path(&cid).exists());
    assert!(!trash.exists());
    assert!(reopened.recover_gc().unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn apply_rejects_symlinked_trash_ancestor_without_touching_external_sentinel() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let cid = zerostun::hash::content_id_from_bytes(b"escape-orphan");
    repo.write_chunk(&cid, CompressionCodec::None, b"escape-orphan")
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let external = temp.path().join("external");
    std::fs::create_dir(&external).unwrap();
    let sentinel = external.join("sentinel");
    std::fs::write(&sentinel, b"untouched").unwrap();
    symlink(&external, repo.root().join("trash")).unwrap();

    assert!(repo.apply_gc(&plan).is_err());
    assert_eq!(std::fs::read(sentinel).unwrap(), b"untouched");
    assert!(repo.chunk_path(&cid).exists());
}

#[test]
fn recovery_rejects_dual_source_trash_without_overwrite() {
    use zerostun::{GcJournal, GcPhase};

    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let cid = zerostun::hash::content_id_from_bytes(b"dual-copy");
    repo.write_chunk(&cid, CompressionCodec::None, b"dual-copy")
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let item = &plan.reclaim_chunks[0];
    let trash = repo.root().join(&item.trash);
    std::fs::create_dir_all(trash.parent().unwrap()).unwrap();
    std::fs::copy(repo.root().join(&item.source), &trash).unwrap();
    let journal = GcJournal {
        plan,
        phase: GcPhase::Moving,
        moved: vec![cid.to_hex()],
    };
    drop(repo);
    insert_journal(&repo_path, &journal, GC_JOURNALS);

    let reopened = Repository::open(&repo_path).unwrap();
    assert!(reopened
        .recover_gc()
        .unwrap_err()
        .to_string()
        .contains("conflicting source and trash"));
    assert!(reopened.chunk_path(&cid).exists());
    assert!(trash.exists());
}

#[cfg(unix)]
#[test]
fn recovery_rejects_symlinked_trash_prefix_without_touching_external_sentinel() {
    use std::os::unix::fs::symlink;
    use zerostun::{GcJournal, GcPhase};

    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let cid = zerostun::hash::content_id_from_bytes(b"prefix-escape");
    repo.write_chunk(&cid, CompressionCodec::None, b"prefix-escape")
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let item = &plan.reclaim_chunks[0];
    let gc_root = repo.root().join("trash").join(&plan.gc_id);
    std::fs::create_dir_all(&gc_root).unwrap();
    let external = temp.path().join("external-prefix");
    std::fs::create_dir(&external).unwrap();
    let sentinel = external.join("sentinel");
    std::fs::write(&sentinel, b"untouched").unwrap();
    symlink(&external, gc_root.join(&item.content_id[..2])).unwrap();
    let journal = GcJournal {
        plan,
        phase: GcPhase::Committed,
        moved: Vec::new(),
    };
    drop(repo);
    insert_journal(&repo_path, &journal, GC_JOURNALS);

    let reopened = Repository::open(&repo_path).unwrap();
    assert!(reopened.recover_gc().is_err());
    assert_eq!(std::fs::read(sentinel).unwrap(), b"untouched");
    assert!(reopened.chunk_path(&cid).exists());
}

#[test]
fn plan_rejects_index_key_manifest_id_mismatch() {
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let manifest = zerostun::manifest::Manifest::new("payload-id", 0, 64, 128, 256);
    drop(repo);
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        write
            .open_table(BACKUPS)
            .unwrap()
            .insert("different-key", manifest.encode().unwrap().as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    drop(db);

    let repo = Repository::open(&repo_path).unwrap();
    assert!(repo.plan_gc().is_err());
}

#[test]
fn apply_rejects_new_tombstone_even_when_chunk_sets_are_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    commit_payloads(&repo, "backup-first", &[b"shared-only"]);
    commit_payloads(&repo, "backup-second", &[b"shared-only"]);
    repo.apply_delete(&repo.plan_delete("backup-first").unwrap())
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    repo.apply_delete(&repo.plan_delete("backup-second").unwrap())
        .unwrap();

    assert!(repo.apply_gc(&plan).is_err());
    assert!(repo.is_tombstoned("backup-second").unwrap());
}

fn insert_journal(
    repo_path: &std::path::Path,
    journal: &zerostun::GcJournal,
    table: TableDefinition<&str, &[u8]>,
) {
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let bytes = journal.encode().unwrap();
        let mut journals = write.open_table(table).unwrap();
        journals
            .insert(journal.plan.gc_id.as_str(), bytes.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
}

#[cfg(unix)]
#[test]
fn gc_rejects_symlinked_chunk_prefix_without_touching_external_sentinel() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let payload = b"prefix-orphan";
    let cid = zerostun::hash::content_id_from_bytes(payload);
    repo.write_chunk(&cid, CompressionCodec::None, payload)
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let prefix = repo_path.join("chunks").join(&cid.to_hex()[..2]);
    let external = temp.path().join("external-prefix");
    std::fs::create_dir(&external).unwrap();
    let sentinel = external.join("sentinel");
    std::fs::write(&sentinel, b"untouched").unwrap();
    let external_chunk = external.join(&cid.to_hex()[2..]);
    std::fs::rename(repo.chunk_path(&cid), &external_chunk).unwrap();
    std::fs::remove_dir(&prefix).unwrap();
    symlink(&external, &prefix).unwrap();

    assert!(repo.apply_gc(&plan).is_err());
    assert_eq!(std::fs::read(sentinel).unwrap(), b"untouched");
    assert!(external_chunk.exists());
}

#[cfg(unix)]
#[test]
fn gc_rejects_symlinked_manifests_directory_without_touching_external_sentinel() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let ids = commit_payloads(&repo, "backup-doomed", &[b"owned"]);
    repo.apply_delete(&repo.plan_delete("backup-doomed").unwrap())
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let external = temp.path().join("external-manifests");
    std::fs::create_dir(&external).unwrap();
    let sentinel = external.join("sentinel");
    std::fs::write(&sentinel, b"untouched").unwrap();
    std::fs::remove_dir_all(repo_path.join("manifests")).unwrap();
    symlink(&external, repo_path.join("manifests")).unwrap();

    assert!(repo.apply_gc(&plan).is_err());
    assert_eq!(std::fs::read(sentinel).unwrap(), b"untouched");
    let cid = zerostun::ids::ContentId::parse(&ids[0]).unwrap();
    assert!(repo.chunk_path(&cid).exists());
}

#[test]
fn committed_recovery_refuses_chunk_that_became_live_after_crash() {
    use zerostun::hash::root_hash_from_manifest;
    use zerostun::manifest::{ChunkDescriptor, Manifest};
    use zerostun::{GcJournal, GcPhase};

    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let payload = b"became-live";
    let cid = zerostun::hash::content_id_from_bytes(payload);
    repo.write_chunk(&cid, CompressionCodec::None, payload)
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let item = &plan.reclaim_chunks[0];
    let trash = repo.root().join(&item.trash);
    std::fs::create_dir_all(trash.parent().unwrap()).unwrap();
    std::fs::rename(repo.chunk_path(&cid), &trash).unwrap();
    drop(repo);
    insert_journal(
        &repo_path,
        &GcJournal {
            plan,
            phase: GcPhase::Committed,
            moved: vec![cid.to_hex()],
        },
        GC_JOURNALS,
    );
    let repo = Repository::open(&repo_path).unwrap();
    let mut manifest = Manifest::new("backup-new-live", payload.len() as u64, 64, 128, 256);
    manifest.add_chunk(ChunkDescriptor {
        index: 0,
        logical_offset: 0,
        original_length: payload.len() as u64,
        stored_length: payload.len() as u64,
        codec: CompressionCodec::None,
        content_id: cid,
    });
    manifest.root_hash = root_hash_from_manifest(&manifest);
    repo.commit_manifest(&manifest).unwrap();
    drop(repo);

    let reopened = Repository::open(&repo_path).unwrap();
    assert!(reopened.recover_gc().is_err());
    assert!(trash.exists());
    assert!(reopened.load_manifest("backup-new-live").is_ok());
}

#[test]
fn committed_recovery_refuses_chunk_that_became_tombstoned_after_crash() {
    use zerostun::hash::root_hash_from_manifest;
    use zerostun::manifest::{ChunkDescriptor, Manifest};
    use zerostun::{GcJournal, GcPhase};

    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let payload = b"became-tombstoned";
    let cid = zerostun::hash::content_id_from_bytes(payload);
    repo.write_chunk(&cid, CompressionCodec::None, payload)
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let item = &plan.reclaim_chunks[0];
    let trash = repo.root().join(&item.trash);
    std::fs::create_dir_all(trash.parent().unwrap()).unwrap();
    std::fs::rename(repo.chunk_path(&cid), &trash).unwrap();
    drop(repo);
    insert_journal(
        &repo_path,
        &GcJournal {
            plan,
            phase: GcPhase::Committed,
            moved: vec![cid.to_hex()],
        },
        GC_JOURNALS,
    );
    std::fs::remove_file(&trash).unwrap();
    let repo = Repository::open(&repo_path).unwrap();
    repo.write_chunk(&cid, CompressionCodec::None, payload)
        .unwrap();
    let mut manifest = Manifest::new("backup-new-tombstoned", payload.len() as u64, 64, 128, 256);
    manifest.add_chunk(ChunkDescriptor {
        index: 0,
        logical_offset: 0,
        original_length: payload.len() as u64,
        stored_length: payload.len() as u64,
        codec: CompressionCodec::None,
        content_id: cid,
    });
    manifest.root_hash = root_hash_from_manifest(&manifest);
    repo.commit_manifest(&manifest).unwrap();
    repo.apply_delete(&repo.plan_delete("backup-new-tombstoned").unwrap())
        .unwrap();
    drop(repo);

    let reopened = Repository::open(&repo_path).unwrap();
    assert!(reopened.recover_gc().is_err());
    assert!(reopened.chunk_path(&cid).exists());
    reopened
        .apply_undelete(&reopened.plan_undelete("backup-new-tombstoned").unwrap())
        .unwrap();
    assert!(reopened.load_manifest("backup-new-tombstoned").is_ok());
}

#[test]
fn reader_lease_refuses_atomic_gc_barrier() {
    const GC_STATE: TableDefinition<&str, u8> = TableDefinition::new("gc_state");
    let fixture = futures_lite_backup_fixture();
    let repo_path = fixture.repo.root().to_path_buf();
    let backup_id = fixture.backup_id.clone();
    drop(fixture.repo);
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut state = write.open_table(GC_STATE).unwrap();
        state.insert("barrier", 1).unwrap();
    }
    write.commit().unwrap();
    drop(db);
    let repo = Repository::open(&repo_path).unwrap();

    assert!(repo
        .acquire_reader_lease(&backup_id)
        .unwrap_err()
        .to_string()
        .contains("garbage collection"));
    assert!(repo.active_reader_leases().unwrap().is_empty());
}

#[test]
fn backup_ids_reject_surrounding_whitespace() {
    assert!(zerostun::ids::validate_backup_id(" backup").is_err());
    assert!(zerostun::ids::validate_backup_id("backup ").is_err());
}

#[test]
fn manifest_encode_enforces_decode_chunk_bound() {
    use zerostun::manifest::{ChunkDescriptor, Manifest, MAX_MANIFEST_CHUNKS};

    let cid = zerostun::hash::content_id_from_bytes(b"bounded");
    let descriptor = ChunkDescriptor {
        index: 0,
        logical_offset: 0,
        original_length: 7,
        stored_length: 7,
        codec: CompressionCodec::None,
        content_id: cid,
    };
    let mut manifest = Manifest::new("backup-bounded", 0, 64, 128, 256);
    manifest.chunks = vec![descriptor; MAX_MANIFEST_CHUNKS + 1];

    assert!(manifest.encode().is_err());
}

#[test]
fn gc_rehashes_reclaim_chunk_payload_before_planning() {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let cid = zerostun::hash::content_id_from_bytes(b"payload-a");
    repo.write_chunk(&cid, CompressionCodec::None, b"payload-a")
        .unwrap();
    let wrong = zerostun::codec::StoredChunk::encode(CompressionCodec::None, b"payload-b");
    std::fs::write(repo.chunk_path(&cid), wrong).unwrap();

    assert!(matches!(repo.plan_gc(), Err(Error::ChunkCorrupt { .. })));
}

#[test]
fn legacy_journal_accepts_cumulative_moved_and_absent_tombstones() {
    use serde_json::json;
    use zerostun::GcJournal;

    let moved = (0..129)
        .map(|index| format!("{index:064x}"))
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&json!({
        "plan": {
            "gc_id": "gc-legacy",
            "live_chunks": 0,
            "reclaim_chunks": [],
            "reclaim_bytes": 0
        },
        "phase": "Moving",
        "moved": moved
    }))
    .unwrap();

    assert_eq!(GcJournal::decode(&bytes).unwrap().moved.len(), 129);
}

#[test]
fn gc_plan_and_journal_json_are_stable_and_bounded() {
    use zerostun::{GcJournal, GcPhase};

    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    let cid = zerostun::hash::content_id_from_bytes(b"orphan");
    repo.write_chunk(&cid, CompressionCodec::None, b"orphan")
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let value = serde_json::to_value(&plan).unwrap();
    assert_eq!(
        serde_json::from_value::<zerostun::GcPlan>(value).unwrap(),
        plan
    );
    let journal = GcJournal {
        plan,
        phase: GcPhase::Moving,
        moved: vec![cid.to_hex()],
    };
    let bytes = journal.encode().unwrap();
    assert_eq!(GcJournal::decode(&bytes).unwrap(), journal);
    assert!(GcJournal::decode(&vec![0; 257 * 1024 * 1024]).is_err());
}
