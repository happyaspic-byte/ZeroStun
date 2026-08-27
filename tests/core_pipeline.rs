use zerostun::codec::CompressionCodec;
use zerostun::config::BackupConfig;
use zerostun::hash::{content_id_from_bytes, root_hash_from_manifest};
use zerostun::manifest::{ChunkDescriptor, Manifest};
use zerostun::repository::Repository;
use zerostun::telemetry::ProgressMode;

#[test]
fn test_content_id_golden_vector() {
    let payload = b"zerostun test payload for deterministic hashing";
    let cid = content_id_from_bytes(payload);
    assert_eq!(
        cid.to_hex(),
        "abd43c66c4e1d9021f7a3027ac88ce387a836a870ca8a86ae1ed41ca2d017ae2"
    );
}

#[test]
fn test_root_hash_golden_vector() {
    let mut manifest = Manifest::new("backup-test-01", 100, 64, 128, 256);
    manifest.add_chunk(ChunkDescriptor {
        index: 0,
        logical_offset: 0,
        original_length: 50,
        stored_length: 50,
        codec: CompressionCodec::None,
        content_id: content_id_from_bytes(b"first half"),
    });
    manifest.add_chunk(ChunkDescriptor {
        index: 1,
        logical_offset: 50,
        original_length: 50,
        stored_length: 30,
        codec: CompressionCodec::Zstd { level: 3 },
        content_id: content_id_from_bytes(b"second half"),
    });

    let root = root_hash_from_manifest(&manifest);
    assert_eq!(
        root.to_hex(),
        "5243af2ba43f84df7d8e30fa71a90b9ac9bf4b7f7b4a51941c9bde1e134a3cdf"
    );
}

#[tokio::test]
async fn test_init_and_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();

    let src_file = temp.path().join("source.bin");
    let mut data = Vec::new();
    for i in 0..50000usize {
        data.extend_from_slice(&(i as u32).to_le_bytes());
    }
    std::fs::write(&src_file, &data).unwrap();

    let cfg = BackupConfig {
        min_chunk: 1024,
        avg_chunk: 4096,
        max_chunk: 16384,
        codec: CompressionCodec::Zstd { level: 3 },
        read_bytes_per_sec: None,
        read_iops: None,
        write_bytes_per_sec: None,
        workers: 2,
        queue_depth: 8,
        progress: ProgressMode::None,
    };

    let summary = zerostun::engine::backup(&repo, &src_file, &cfg)
        .await
        .unwrap();
    assert_eq!(summary.original_bytes, data.len() as u64);
    assert!(summary.total_chunks > 0);

    let verify_res = zerostun::engine::verify(&repo, &summary.backup_id)
        .await
        .unwrap();
    assert!(verify_res.is_ok());

    let restored_file = temp.path().join("restored.bin");
    zerostun::engine::restore(&repo, &summary.backup_id, &restored_file, false)
        .await
        .unwrap();
    let restored_bytes = std::fs::read(&restored_file).unwrap();
    assert_eq!(restored_bytes, data);
}

#[tokio::test]
async fn test_codecs_and_deduplication() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();

    let src1 = temp.path().join("src1.bin");
    let src2 = temp.path().join("src2.bin");

    let repeated = vec![42u8; 128 * 1024];
    std::fs::write(&src1, &repeated).unwrap();
    std::fs::write(&src2, &repeated).unwrap();

    let cfg_lz4 = BackupConfig {
        min_chunk: 1024,
        avg_chunk: 4096,
        max_chunk: 16384,
        codec: CompressionCodec::Lz4,
        read_bytes_per_sec: None,
        read_iops: None,
        write_bytes_per_sec: None,
        workers: 2,
        queue_depth: 8,
        progress: ProgressMode::None,
    };

    zerostun::engine::backup(&repo, &src1, &cfg_lz4)
        .await
        .unwrap();
    let cfg_none = BackupConfig {
        codec: CompressionCodec::None,
        ..cfg_lz4.clone()
    };
    let s2 = zerostun::engine::backup(&repo, &src2, &cfg_none)
        .await
        .unwrap();

    assert_eq!(s2.unique_chunks, 0);
    assert!(s2.reused_chunks > 0);
    let verify = zerostun::engine::verify(&repo, &s2.backup_id)
        .await
        .unwrap();
    assert!(
        verify.is_ok(),
        "cross-codec reuse must remain valid: {verify:?}"
    );
}

#[tokio::test]
async fn test_corruption_detection() {
    let temp = tempfile::tempdir().unwrap();
    let repo_path = temp.path().join("repo");
    let repo = Repository::init(&repo_path).unwrap();

    let src = temp.path().join("data.bin");
    let data = b"some data that will form at least one chunk in the repository";
    std::fs::write(&src, data).unwrap();

    let cfg = BackupConfig {
        min_chunk: 64,
        avg_chunk: 256,
        max_chunk: 1024,
        codec: CompressionCodec::None,
        ..Default::default()
    };
    let summary = zerostun::engine::backup(&repo, &src, &cfg).await.unwrap();

    let manifest = repo.load_manifest(&summary.backup_id).unwrap();
    let first_chunk_cid = &manifest.chunks[0].content_id;
    let chunk_file = repo.chunk_path(first_chunk_cid);

    let mut chunk_bytes = std::fs::read(&chunk_file).unwrap();
    chunk_bytes[0] ^= 0xFF; // flip bits
    std::fs::write(&chunk_file, &chunk_bytes).unwrap();

    let v_report = zerostun::engine::verify(&repo, &summary.backup_id)
        .await
        .unwrap();
    assert!(!v_report.is_ok());

    let restore_target = temp.path().join("fail_restore.bin");
    let r_res = zerostun::engine::restore(&repo, &summary.backup_id, &restore_target, false).await;
    assert!(r_res.is_err());
    assert!(!restore_target.exists());
}
