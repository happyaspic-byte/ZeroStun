use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn cli_end_to_end_round_trip_and_json() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let source = temp.path().join("source.bin");
    let restored = temp.path().join("restored.bin");

    let mut data = Vec::new();
    for i in 0..100_000u32 {
        data.extend_from_slice(&i.to_le_bytes());
    }
    std::fs::write(&source, &data).unwrap();

    Command::cargo_bin("zerostun")
        .unwrap()
        .args(["init", "--repo"])
        .arg(&repo)
        .assert()
        .success()
        .stdout(predicate::str::contains("Repository initialized"));

    let backup_output = Command::cargo_bin("zerostun")
        .unwrap()
        .args(["--json", "backup", "--repo"])
        .arg(&repo)
        .arg("--source")
        .arg(&source)
        .arg("--min-chunk")
        .arg("1KiB")
        .arg("--avg-chunk")
        .arg("4KiB")
        .arg("--max-chunk")
        .arg("16KiB")
        .output()
        .unwrap();
    assert!(backup_output.status.success());

    let summary: serde_json::Value = serde_json::from_slice(&backup_output.stdout).unwrap();
    let backup_id = summary["backup_id"].as_str().unwrap();
    assert!(summary["total_chunks"].as_u64().unwrap() > 0);

    Command::cargo_bin("zerostun")
        .unwrap()
        .args(["--json", "inspect", "--repo"])
        .arg(&repo)
        .arg("--backup-id")
        .arg(backup_id)
        .assert()
        .success()
        .stdout(predicate::str::contains("root_hash"));

    Command::cargo_bin("zerostun")
        .unwrap()
        .args(["verify", "--repo"])
        .arg(&repo)
        .arg("--backup-id")
        .arg(backup_id)
        .assert()
        .success()
        .stdout(predicate::str::contains("VALID"));

    Command::cargo_bin("zerostun")
        .unwrap()
        .args(["restore", "--repo"])
        .arg(&repo)
        .arg("--backup-id")
        .arg(backup_id)
        .arg("--target")
        .arg(&restored)
        .assert()
        .success();

    assert_eq!(std::fs::read(&restored).unwrap(), data);

    Command::cargo_bin("zerostun")
        .unwrap()
        .args(["restore", "--repo"])
        .arg(&repo)
        .arg("--backup-id")
        .arg(backup_id)
        .arg("--target")
        .arg(&restored)
        .assert()
        .code(7)
        .stderr(predicate::str::contains("already exists"));
}
