use std::path::{Path, PathBuf};

use redb::{Database, TableDefinition};
use zerostun::codec::CompressionCodec;
use zerostun::config::BackupConfig;
use zerostun::hash::{content_id_from_bytes, root_hash_from_manifest};
use zerostun::lifecycle::{FindingKind, FindingSeverity, RepairScope, DEFAULT_MAX_REPAIR_FINDINGS};
use zerostun::manifest::{ChunkDescriptor, Manifest};
use zerostun::repository::Repository;
use zerostun::{GcJournal, GcPhase, ReaderLease};

struct BackupFixture {
    _temp: tempfile::TempDir,
    repo: Repository,
    first_chunk_path: PathBuf,
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
    let first_chunk_path = repo
        .load_manifest(&summary.backup_id)
        .unwrap()
        .chunks
        .first()
        .map(|chunk| repo.chunk_path(&chunk.content_id))
        .unwrap();
    BackupFixture {
        _temp: temp,
        repo,
        first_chunk_path,
    }
}

fn commit_payloads(repo: &Repository, backup_id: &str, payloads: &[&[u8]]) -> Vec<String> {
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

fn create_empty_index(repo_path: &Path) {
    const BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
    const CHUNKS: TableDefinition<&str, u64> = TableDefinition::new("chunks");
    const TOMBSTONES: TableDefinition<&str, u64> = TableDefinition::new("tombstones");
    const READER_LEASES: TableDefinition<&str, &[u8]> = TableDefinition::new("reader_leases");
    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    const GC_STATE: TableDefinition<&str, u8> = TableDefinition::new("gc_state");
    let db = Database::create(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let _ = write.open_table(BACKUPS).unwrap();
        let _ = write.open_table(CHUNKS).unwrap();
        let _ = write.open_table(TOMBSTONES).unwrap();
        let _ = write.open_table(READER_LEASES).unwrap();
        let _ = write.open_table(GC_JOURNALS).unwrap();
        let _ = write.open_table(GC_STATE).unwrap();
    }
    write.commit().unwrap();
}

struct LostIndexFixture {
    _temp: tempfile::TempDir,
    repo: Repository,
    backup_id: String,
}

fn repo_with_lost_index_and_valid_manifest_file() -> LostIndexFixture {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    commit_payloads(&repo, "backup-repair", &[b"payload-bytes"]);
    let backup_id = "backup-repair".to_string();
    assert!(repo_path
        .join("manifests")
        .join("backup-repair.manifest")
        .is_file());
    drop(repo);
    std::fs::remove_file(repo_path.join("index.redb")).unwrap();
    create_empty_index(&repo_path);
    let repo = Repository::open(&repo_path).unwrap();
    LostIndexFixture {
        _temp: temp,
        repo,
        backup_id,
    }
}

fn insert_lease(repo_path: &Path, lease: &ReaderLease) {
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

fn insert_journal(repo_path: &Path, journal: &GcJournal) {
    const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let bytes = journal.encode().unwrap();
        let mut journals = write.open_table(GC_JOURNALS).unwrap();
        journals
            .insert(journal.plan.gc_id.as_str(), bytes.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
}

#[tokio::test]
async fn repair_reports_missing_chunk_without_fabrication() {
    let fixture = backup_fixture().await;
    std::fs::remove_file(&fixture.first_chunk_path).unwrap();
    let report = fixture.repo.inspect_repair().unwrap();
    assert!(report
        .findings
        .iter()
        .any(|finding| finding.kind == FindingKind::MissingChunk));
    let plan = fixture.repo.plan_repair(&report).unwrap();
    fixture.repo.apply_repair(&plan).unwrap();
    assert!(!fixture.first_chunk_path.exists());
}

#[test]
fn repair_rebuilds_index_only_from_valid_manifest_copies() {
    let fixture = repo_with_lost_index_and_valid_manifest_file();
    std::fs::write(
        fixture.repo.root().join("manifests/not-a-backup.manifest"),
        b"this is not a manifest",
    )
    .unwrap();
    let report = fixture.repo.inspect_repair().unwrap();
    let plan = fixture.repo.plan_repair(&report).unwrap();
    assert!(plan.rebuild_index);
    fixture.repo.apply_repair(&plan).unwrap();
    assert_eq!(
        fixture.repo.list_backups().unwrap(),
        vec![fixture.backup_id.clone()]
    );
}

#[test]
fn inspect_reports_every_repository_scope() {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    commit_payloads(&repo, "backup-inspect", &[b"inspect-payload"]);
    let report = repo.inspect_repair().unwrap();
    for scope in [
        RepairScope::Version,
        RepairScope::Redb,
        RepairScope::Manifests,
        RepairScope::Chunks,
        RepairScope::Tombstones,
        RepairScope::Leases,
        RepairScope::Temp,
        RepairScope::Trash,
        RepairScope::GcJournals,
    ] {
        assert!(
            report.inspected.contains(&scope),
            "missing inspected scope {scope:?}"
        );
    }
    assert!(report.valid_manifests >= 1);
    assert!(report.valid_chunks >= 1);
    assert!(!report.findings_truncated);
    assert!(report.findings.len() <= DEFAULT_MAX_REPAIR_FINDINGS);
}

#[test]
fn inspect_reports_planted_findings_for_each_scope() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    commit_payloads(&repo, "backup-planted", &[b"planted-payload"]);
    std::fs::write(
        repo.root().join("manifests/corrupt.manifest"),
        b"bad-manifest",
    )
    .unwrap();
    std::fs::write(repo.root().join("chunks/unexpected"), b"data").unwrap();
    std::fs::write(repo.root().join("tmp/orphan.tmp"), b"tmp").unwrap();
    std::fs::create_dir_all(repo.root().join("trash/gc-leftover")).unwrap();
    drop(repo);

    const TOMBSTONES: TableDefinition<&str, u64> = TableDefinition::new("tombstones");
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut tombstones = write.open_table(TOMBSTONES).unwrap();
        tombstones.insert("ghost-backup", 1_u64).unwrap();
    }
    write.commit().unwrap();
    drop(db);

    insert_lease(
        &repo_path,
        &ReaderLease {
            lease_id: "lease-dead".to_string(),
            backup_id: "backup-planted".to_string(),
            pid: u32::MAX,
            process_start_token: "0".to_string(),
            acquired_unix_ms: 1,
        },
    );

    let repo = Repository::init(&temp.path().join("journal-src")).unwrap();
    let cid = content_id_from_bytes(b"journal-orphan");
    repo.write_chunk(&cid, CompressionCodec::None, b"journal-orphan")
        .unwrap();
    let plan = repo.plan_gc().unwrap();
    let journal = GcJournal {
        plan,
        phase: GcPhase::Moving,
        moved: Vec::new(),
    };
    drop(repo);
    insert_journal(&repo_path, &journal);

    let repo = Repository::open(&repo_path).unwrap();
    std::fs::write(repo.root().join("VERSION"), "99\n").unwrap();
    let report = repo.inspect_repair().unwrap();
    let kinds: Vec<_> = report.findings.iter().map(|finding| finding.kind).collect();
    assert!(kinds.contains(&FindingKind::VersionInvalid));
    assert!(kinds.contains(&FindingKind::ManifestCorrupt));
    assert!(kinds.contains(&FindingKind::UnexpectedChunkName));
    assert!(kinds.contains(&FindingKind::TombstoneInconsistency));
    assert!(kinds.contains(&FindingKind::StaleLease));
    assert!(kinds.contains(&FindingKind::TempResidue));
    assert!(kinds.contains(&FindingKind::TrashResidue));
    assert!(kinds.contains(&FindingKind::GcJournal));
    assert!(report
        .findings
        .iter()
        .any(|finding| finding.severity == FindingSeverity::Warning
            || finding.severity == FindingSeverity::Critical));
}

#[test]
fn findings_cap_truncates_explicitly() {
    let temp = tempfile::tempdir().unwrap();
    let repo = Repository::init(&temp.path().join("repo")).unwrap();
    for index in 0..8 {
        std::fs::write(
            repo.root().join("tmp").join(format!("orphan-{index}.tmp")),
            b"tmp",
        )
        .unwrap();
    }
    let report = repo.inspect_repair_with_max_findings(3).unwrap();
    assert!(report.findings_truncated);
    assert!(report
        .findings
        .iter()
        .any(|finding| finding.kind == FindingKind::FindingsTruncated));
    assert!(report.findings.len() <= 4);
}

#[cfg(target_os = "linux")]
#[test]
fn stale_lease_is_planned_only_when_process_identity_mismatch_is_proven() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    commit_payloads(&repo, "backup-lease", &[b"lease-payload"]);
    drop(repo);
    insert_lease(
        &repo_path,
        &ReaderLease {
            lease_id: "lease-dead".to_string(),
            backup_id: "backup-lease".to_string(),
            pid: u32::MAX,
            process_start_token: "0".to_string(),
            acquired_unix_ms: 1,
        },
    );
    let repo = Repository::open(&repo_path).unwrap();
    let live = repo.acquire_reader_lease("backup-lease").unwrap();
    let report = repo.inspect_repair().unwrap();
    let plan = repo.plan_repair(&report).unwrap();
    assert!(plan.stale_leases.contains(&"lease-dead".to_string()));
    assert!(!plan.stale_leases.iter().any(|id| id == live.lease_id()));
    drop(live);
}

#[test]
fn apply_repair_is_idempotent() {
    let fixture = repo_with_lost_index_and_valid_manifest_file();
    let report = fixture.repo.inspect_repair().unwrap();
    let plan = fixture.repo.plan_repair(&report).unwrap();
    assert!(plan.rebuild_index);
    let first = fixture.repo.apply_repair(&plan).unwrap();
    assert!(first.rebuilt_index);
    assert!(fixture.repo.root().join("index.redb.previous").is_file());
    let report = fixture.repo.inspect_repair().unwrap();
    let plan = fixture.repo.plan_repair(&report).unwrap();
    assert!(!plan.rebuild_index);
    assert!(plan.stale_leases.is_empty());
    assert!(plan.gc_recoveries.is_empty());
    let second = fixture.repo.apply_repair(&plan).unwrap();
    assert!(!second.rebuilt_index);
    assert!(second.removed_leases.is_empty());
    assert!(second.gc_recoveries.is_empty());
    assert_eq!(
        fixture.repo.list_backups().unwrap(),
        vec![fixture.backup_id]
    );
}

#[test]
fn apply_repair_delegates_gc_recovery_to_recover_gc() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    let cid = content_id_from_bytes(b"repair-orphan");
    repo.write_chunk(&cid, CompressionCodec::None, b"repair-orphan")
        .unwrap();
    let gc_plan = repo.plan_gc().unwrap();
    let item = &gc_plan.reclaim_chunks[0];
    let trash = repo.root().join(&item.trash);
    std::fs::create_dir_all(trash.parent().unwrap()).unwrap();
    std::fs::rename(repo.root().join(&item.source), &trash).unwrap();
    let journal = GcJournal {
        plan: gc_plan.clone(),
        phase: GcPhase::Moving,
        moved: Vec::new(),
    };
    drop(repo);
    insert_journal(&repo_path, &journal);

    let repo = Repository::open(&repo_path).unwrap();
    let report = repo.inspect_repair().unwrap();
    assert!(report
        .findings
        .iter()
        .any(|finding| finding.kind == FindingKind::GcJournal));
    let plan = repo.plan_repair(&report).unwrap();
    assert!(plan.gc_recoveries.contains(&gc_plan.gc_id));
    let result = repo.apply_repair(&plan).unwrap();
    assert!(result.gc_recoveries.contains(&gc_plan.gc_id));
    assert!(repo.chunk_path(&cid).exists());
    assert!(!trash.exists());
    assert!(repo.recover_gc().unwrap().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn apply_repair_removes_only_proven_stale_leases() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    commit_payloads(&repo, "backup-lease", &[b"lease-payload"]);
    drop(repo);
    insert_lease(
        &repo_path,
        &ReaderLease {
            lease_id: "lease-dead".to_string(),
            backup_id: "backup-lease".to_string(),
            pid: u32::MAX,
            process_start_token: "0".to_string(),
            acquired_unix_ms: 1,
        },
    );
    let repo = Repository::open(&repo_path).unwrap();
    let report = repo.inspect_repair().unwrap();
    let plan = repo.plan_repair(&report).unwrap();
    let result = repo.apply_repair(&plan).unwrap();
    assert_eq!(result.removed_leases, vec!["lease-dead".to_string()]);
    assert!(repo.active_reader_leases().unwrap().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn repair_apply_unblocks_gc_after_proven_stale_lease() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();
    commit_payloads(&repo, "backup-lease", &[b"lease-payload"]);
    drop(repo);
    insert_lease(
        &repo_path,
        &ReaderLease {
            lease_id: "lease-dead".to_string(),
            backup_id: "backup-lease".to_string(),
            pid: u32::MAX,
            process_start_token: "0".to_string(),
            acquired_unix_ms: 1,
        },
    );
    let repo = Repository::open(&repo_path).unwrap();
    assert!(!repo.active_reader_leases().unwrap().is_empty());
    let report = repo.inspect_repair().unwrap();
    let plan = repo.plan_repair(&report).unwrap();
    assert!(plan.stale_leases.contains(&"lease-dead".to_string()));
    repo.apply_repair(&plan).unwrap();
    assert!(repo.active_reader_leases().unwrap().is_empty());
    assert!(repo.plan_gc().is_ok());
}
