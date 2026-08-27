use zerostun::config::BackupConfig;
use zerostun::error::Error;
use zerostun::repository::Repository;

#[tokio::test]
async fn test_empty_file_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();

    let empty_src = temp.path().join("empty.bin");
    std::fs::write(&empty_src, b"").unwrap();

    let cfg = BackupConfig::default();
    let summary = zerostun::engine::backup(&repo, &empty_src, &cfg)
        .await
        .unwrap();
    assert_eq!(summary.original_bytes, 0);

    let verify_res = zerostun::engine::verify(&repo, &summary.backup_id)
        .await
        .unwrap();
    assert!(verify_res.is_ok());

    let restored = temp.path().join("empty_restored.bin");
    zerostun::engine::restore(&repo, &summary.backup_id, &restored, false)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&restored).unwrap().len(), 0);
}

#[tokio::test]
async fn test_source_inside_repository_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();

    let inside_src = repo_path.join("inside.bin");
    std::fs::write(&inside_src, b"forbidden data inside repo").unwrap();

    let cfg = BackupConfig::default();
    let res = zerostun::engine::backup(&repo, &inside_src, &cfg).await;
    match res {
        Err(Error::SourceInsideRepository { .. }) => {}
        other => panic!("expected SourceInsideRepository error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_writer_lock_prevents_concurrent_writes() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();

    let lock1 = repo.acquire_writer_lock().unwrap();
    let lock2_res = repo.acquire_writer_lock();
    match lock2_res {
        Err(Error::RepositoryLocked(_)) => {}
        other => panic!("expected RepositoryLocked error, got {other:?}"),
    }
    drop(lock1);
    assert!(repo.acquire_writer_lock().is_ok());
}

#[tokio::test]
async fn test_restore_force_overwrite() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();

    let src = temp.path().join("src.bin");
    std::fs::write(&src, b"original content to restore").unwrap();

    let summary = zerostun::engine::backup(&repo, &src, &BackupConfig::default())
        .await
        .unwrap();

    let target = temp.path().join("target.bin");
    std::fs::write(&target, b"existing stale content").unwrap();

    let err = zerostun::engine::restore(&repo, &summary.backup_id, &target, false).await;
    assert!(matches!(err, Err(Error::RestoreTargetExists(_))));

    zerostun::engine::restore(&repo, &summary.backup_id, &target, true)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(&target).unwrap(),
        b"original content to restore"
    );
}
