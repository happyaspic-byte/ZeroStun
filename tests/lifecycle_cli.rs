use std::path::{Path, PathBuf};

use assert_cmd::Command;
use redb::{Database, TableDefinition};
use zerostun::codec::CompressionCodec;
use zerostun::config::BackupConfig;
use zerostun::lifecycle::PrunePlan;
use zerostun::repository::Repository;
use zerostun::{DeletePlan, GcPlan, ReaderLease, RepairPlan};

struct CliFixture {
    _temp: tempfile::TempDir,
    repo: PathBuf,
    backup_id: String,
}

impl CliFixture {
    fn repository(&self) -> Repository {
        Repository::open(&self.repo).unwrap()
    }
}

struct TwoBackupFixture {
    _temp: tempfile::TempDir,
    repo: PathBuf,
    first_id: String,
    second_id: String,
    second_bytes: Vec<u8>,
}

impl TwoBackupFixture {
    fn repository(&self) -> Repository {
        Repository::open(&self.repo).unwrap()
    }
}

fn zerostun_cmd() -> Command {
    Command::cargo_bin("zerostun").unwrap()
}

fn backup_config() -> BackupConfig {
    BackupConfig {
        min_chunk: 1024,
        avg_chunk: 4096,
        max_chunk: 8192,
        codec: CompressionCodec::None,
        ..Default::default()
    }
}

fn cli_backup_fixture() -> CliFixture {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let temp = tempfile::tempdir().unwrap();
        let repo_path = temp.path().join("repo");
        let repo = Repository::init(&repo_path).unwrap();
        let source = temp.path().join("source.bin");
        std::fs::write(&source, vec![42_u8; 32 * 1024]).unwrap();
        let summary = zerostun::engine::backup(&repo, &source, &backup_config())
            .await
            .unwrap();
        drop(repo);
        CliFixture {
            _temp: temp,
            repo: repo_path,
            backup_id: summary.backup_id,
        }
    })
}

fn two_overlapping_backups() -> TwoBackupFixture {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let temp = tempfile::tempdir().unwrap();
        let repo_path = temp.path().join("repo");
        let repo = Repository::init(&repo_path).unwrap();
        let first_source = temp.path().join("first.bin");
        let second_source = temp.path().join("second.bin");
        let mut first_bytes = vec![7_u8; 24 * 1024];
        first_bytes.extend_from_slice(&[1_u8; 8 * 1024]);
        let mut second_bytes = vec![7_u8; 24 * 1024];
        second_bytes.extend_from_slice(&[9_u8; 8 * 1024]);
        std::fs::write(&first_source, &first_bytes).unwrap();
        std::fs::write(&second_source, &second_bytes).unwrap();
        let first = zerostun::engine::backup(&repo, &first_source, &backup_config())
            .await
            .unwrap();
        let second = zerostun::engine::backup(&repo, &second_source, &backup_config())
            .await
            .unwrap();
        drop(repo);
        TwoBackupFixture {
            _temp: temp,
            repo: repo_path,
            first_id: first.backup_id,
            second_id: second.backup_id,
            second_bytes,
        }
    })
}

#[test]
fn delete_defaults_to_dry_run_and_json_is_stable() {
    let fixture = cli_backup_fixture();
    let output = zerostun_cmd()
        .args(["--json", "delete", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.backup_id])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: DeletePlan = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(plan.backup_id, fixture.backup_id);
    assert!(!plan.already_deleted);
    assert!(fixture
        .repository()
        .load_manifest(&fixture.backup_id)
        .is_ok());
}

#[test]
fn delete_apply_hides_backup() {
    let fixture = cli_backup_fixture();
    zerostun_cmd()
        .args(["delete", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.backup_id, "--apply"])
        .assert()
        .success();
    assert!(fixture
        .repository()
        .load_manifest(&fixture.backup_id)
        .is_err());
}

#[test]
fn delete_dry_run_human_output_does_not_claim_deletion() {
    let fixture = cli_backup_fixture();
    let output = zerostun_cmd()
        .args(["delete", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.backup_id])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout).to_lowercase();
    assert!(text.contains("dry-run"));
    assert!(!text.contains("deleted backup"));
    assert!(!text.contains("tombstoned"));
    assert!(fixture
        .repository()
        .load_manifest(&fixture.backup_id)
        .is_ok());
}

#[test]
fn prune_defaults_to_dry_run() {
    let fixture = two_overlapping_backups();
    let output = zerostun_cmd()
        .args(["--json", "prune", "--repo"])
        .arg(&fixture.repo)
        .args(["--protect", &fixture.second_id])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: PrunePlan = serde_json::from_slice(&output.stdout).unwrap();
    assert!(plan.delete.contains(&fixture.first_id));
    assert!(plan.keep.contains(&fixture.second_id));
    assert!(fixture
        .repository()
        .load_manifest(&fixture.first_id)
        .is_ok());
    assert!(fixture
        .repository()
        .load_manifest(&fixture.second_id)
        .is_ok());
}

#[test]
fn prune_apply_tombstones_unretained_backups() {
    let fixture = two_overlapping_backups();
    zerostun_cmd()
        .args(["prune", "--repo"])
        .arg(&fixture.repo)
        .args(["--protect", &fixture.second_id, "--apply"])
        .assert()
        .success();
    assert!(fixture
        .repository()
        .load_manifest(&fixture.first_id)
        .is_err());
    assert!(fixture
        .repository()
        .load_manifest(&fixture.second_id)
        .is_ok());
}

#[test]
fn gc_defaults_to_dry_run() {
    let fixture = two_overlapping_backups();
    zerostun_cmd()
        .args(["delete", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.first_id, "--apply"])
        .assert()
        .success();
    let output = zerostun_cmd()
        .args(["--json", "gc", "--repo"])
        .arg(&fixture.repo)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: GcPlan = serde_json::from_slice(&output.stdout).unwrap();
    assert!(!plan.reclaim_chunks.is_empty());
    assert!(fixture
        .repository()
        .load_manifest(&fixture.second_id)
        .is_ok());
    for chunk in &plan.reclaim_chunks {
        assert!(fixture.repo.join(&chunk.source).is_file());
    }
}

#[test]
fn gc_apply_reclaims_unreferenced_chunks() {
    let fixture = two_overlapping_backups();
    zerostun_cmd()
        .args(["delete", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.first_id, "--apply"])
        .assert()
        .success();
    let planned = zerostun_cmd()
        .args(["--json", "gc", "--repo"])
        .arg(&fixture.repo)
        .output()
        .unwrap();
    let plan: GcPlan = serde_json::from_slice(&planned.stdout).unwrap();
    zerostun_cmd()
        .args(["gc", "--repo"])
        .arg(&fixture.repo)
        .arg("--apply")
        .assert()
        .success();
    assert!(fixture
        .repository()
        .load_manifest(&fixture.second_id)
        .is_ok());
    for chunk in &plan.reclaim_chunks {
        assert!(!fixture.repo.join(&chunk.source).exists());
    }
}

#[test]
fn repair_defaults_to_dry_run() {
    let fixture = cli_backup_fixture();
    let output = zerostun_cmd()
        .args(["--json", "repair", "--repo"])
        .arg(&fixture.repo)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: RepairPlan = serde_json::from_slice(&output.stdout).unwrap();
    assert!(!plan.rebuild_index);
    assert!(plan.stale_leases.is_empty());
    assert!(plan.gc_recoveries.is_empty());
    assert!(fixture
        .repository()
        .load_manifest(&fixture.backup_id)
        .is_ok());
}

#[test]
fn repair_apply_on_healthy_repository() {
    let fixture = cli_backup_fixture();
    zerostun_cmd()
        .args(["repair", "--repo"])
        .arg(&fixture.repo)
        .arg("--apply")
        .assert()
        .success();
    assert!(fixture
        .repository()
        .load_manifest(&fixture.backup_id)
        .is_ok());
}

#[test]
fn gc_exits_nonzero_when_reader_lease_is_active() {
    let fixture = cli_backup_fixture();
    plant_live_reader_lease(&fixture.repo, &fixture.backup_id);
    let output = zerostun_cmd()
        .args(["gc", "--repo"])
        .arg(&fixture.repo)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        output.status.code(),
        Some(zerostun::ExitCode::Locked as i32)
    );
}

fn plant_live_reader_lease(repo_path: &Path, backup_id: &str) {
    const READER_LEASES: TableDefinition<&str, &[u8]> = TableDefinition::new("reader_leases");
    let lease = ReaderLease {
        lease_id: "lease-cli-active".to_string(),
        backup_id: backup_id.to_string(),
        pid: std::process::id(),
        process_start_token: linux_start_token(std::process::id()),
        acquired_unix_ms: 1,
    };
    let db = Database::open(repo_path.join("index.redb")).unwrap();
    let write = db.begin_write().unwrap();
    {
        let encoded = serde_json::to_vec(&lease).unwrap();
        let mut table = write.open_table(READER_LEASES).unwrap();
        table
            .insert(lease.lease_id.as_str(), encoded.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
}

fn linux_start_token(pid: u32) -> String {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|contents| {
            let close = contents.rfind(')')?;
            contents[close + 1..]
                .split_whitespace()
                .nth(19)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "0".to_string())
}

#[test]
fn repair_exits_nonzero_for_critical_finding() {
    let fixture = cli_backup_fixture();
    let repo = fixture.repository();
    let first_chunk = repo
        .load_manifest(&fixture.backup_id)
        .unwrap()
        .chunks
        .first()
        .map(|chunk| repo.chunk_path(&chunk.content_id))
        .unwrap();
    drop(repo);
    std::fs::remove_file(&first_chunk).unwrap();
    let output = zerostun_cmd()
        .args(["--json", "repair", "--repo"])
        .arg(&fixture.repo)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        output.status.code(),
        Some(zerostun::ExitCode::Integrity as i32)
    );
    let plan: RepairPlan = serde_json::from_slice(&output.stdout).unwrap();
    let _ = plan;
    assert!(!first_chunk.exists());
}

#[test]
fn overlapping_backup_lifecycle_smoke() {
    let fixture = two_overlapping_backups();
    zerostun_cmd()
        .args(["delete", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.first_id])
        .assert()
        .success();
    zerostun_cmd()
        .args(["prune", "--repo"])
        .arg(&fixture.repo)
        .args(["--protect", &fixture.second_id])
        .assert()
        .success();
    zerostun_cmd()
        .args(["gc", "--repo"])
        .arg(&fixture.repo)
        .assert()
        .success();
    assert!(fixture
        .repository()
        .load_manifest(&fixture.first_id)
        .is_ok());

    zerostun_cmd()
        .args(["delete", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.first_id, "--apply"])
        .assert()
        .success();

    let restored = fixture
        .repo
        .parent()
        .unwrap()
        .join("restored-after-tombstone.bin");
    zerostun_cmd()
        .args(["restore", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.second_id, "--target"])
        .arg(&restored)
        .assert()
        .success();
    assert_eq!(std::fs::read(&restored).unwrap(), fixture.second_bytes);

    zerostun_cmd()
        .args(["gc", "--repo"])
        .arg(&fixture.repo)
        .arg("--apply")
        .assert()
        .success();
    zerostun_cmd()
        .args(["repair", "--repo"])
        .arg(&fixture.repo)
        .assert()
        .success();
    zerostun_cmd()
        .args(["verify", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.second_id])
        .assert()
        .success();

    let restored_after_gc = fixture.repo.parent().unwrap().join("restored-after-gc.bin");
    zerostun_cmd()
        .args(["restore", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.second_id, "--target"])
        .arg(&restored_after_gc)
        .assert()
        .success();
    assert_eq!(
        std::fs::read(&restored_after_gc).unwrap(),
        fixture.second_bytes
    );
}
