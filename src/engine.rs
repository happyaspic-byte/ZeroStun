use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::chunking::stream_chunks;
use crate::codec::{Compressor, MAX_ORIGINAL_CHUNK_BYTES};
use crate::config::BackupConfig;
use crate::error::{Error, Result};
use crate::hash::{content_id_from_bytes, root_hash_from_manifest};
use crate::ids::{generate_backup_id, ContentId};
use crate::manifest::{ChunkDescriptor, Manifest};
use crate::rate_limit::TokenBucket;
use crate::repository::Repository;
use crate::source::FileSource;
use crate::telemetry::ProgressMode;

#[derive(Debug)]
struct IndexedChunk {
    index: u64,
    offset: u64,
    data: Vec<u8>,
}

#[derive(Debug)]
struct ProcessedChunk {
    index: u64,
    offset: u64,
    original_length: u64,
    content_id: ContentId,
    compressed: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSummary {
    pub backup_id: String,
    pub original_bytes: u64,
    pub stored_bytes: u64,
    pub total_chunks: usize,
    pub unique_chunks: usize,
    pub reused_chunks: usize,
    pub root_hash: String,
    pub dedupe_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyReport {
    pub backup_id: String,
    pub total_chunks: usize,
    pub total_bytes: u64,
    pub root_hash: String,
    pub is_valid: bool,
    pub error: Option<String>,
}

impl VerifyReport {
    pub fn is_ok(&self) -> bool {
        self.is_valid && self.error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InspectReport {
    pub backup_id: String,
    pub created_unix_ms: u64,
    pub source_path: String,
    pub total_logical_bytes: u64,
    pub total_chunks: usize,
    pub unique_chunks: usize,
    pub stored_bytes: u64,
    pub root_hash: String,
    pub fastcdc_params: (u32, u32, u32),
}

pub async fn backup(
    repo: &Repository,
    source_path: &Path,
    config: &BackupConfig,
) -> Result<BackupSummary> {
    let source_canon = source_path.canonicalize().map_err(|e| Error::SourceRead {
        path: source_path.to_path_buf(),
        source: e,
    })?;
    let repo_canon = repo
        .root()
        .canonicalize()
        .unwrap_or_else(|_| repo.root().to_path_buf());
    if source_canon.starts_with(&repo_canon) {
        return Err(Error::SourceInsideRepository {
            source_path: source_canon,
            repo_path: repo_canon,
        });
    }

    let _lock = repo.acquire_writer_lock()?;
    let chunk_params = config.validate()?;

    let source = FileSource::open(&source_canon)?;
    let total_bytes = source.len();

    let backup_id = generate_backup_id();
    let mut manifest = Manifest::new(
        &backup_id,
        total_bytes,
        chunk_params.min as u32,
        chunk_params.avg as u32,
        chunk_params.max as u32,
    );
    manifest.source_path = source_canon.to_string_lossy().to_string();

    let (raw_tx, raw_rx) = mpsc::channel::<IndexedChunk>(config.queue_depth);
    let (processed_tx, mut processed_rx) =
        mpsc::channel::<Result<ProcessedChunk>>(config.queue_depth);

    let mut read_bucket = TokenBucket::new(config.read_bytes_per_sec, config.read_iops)?;
    let mut write_bucket = TokenBucket::new(config.write_bytes_per_sec, None)?;

    let progress = match config.progress {
        ProgressMode::None => None,
        ProgressMode::Auto => {
            let pb = ProgressBar::new(total_bytes);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})",
                    )
                    .unwrap_or_else(|_| ProgressStyle::default_bar()),
            );
            Some(pb)
        }
    };

    let source_file = source.file()?;
    let reader_handle = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut index = 0u64;
        stream_chunks(source_file, chunk_params, |chunk| {
            let nbytes = chunk.data.len() as u64;
            if nbytes > MAX_ORIGINAL_CHUNK_BYTES {
                return Err(Error::ChunkEncode(format!(
                    "chunk at offset {} exceeds max original size {MAX_ORIGINAL_CHUNK_BYTES}",
                    chunk.offset
                )));
            }
            read_bucket.consume_blocking(nbytes);
            if raw_tx
                .blocking_send(IndexedChunk {
                    index,
                    offset: chunk.offset,
                    data: chunk.data,
                })
                .is_err()
            {
                return Err(Error::Cancelled);
            }
            index += 1;
            Ok(())
        })
    });

    let codec = config.codec;
    let raw_rx = Arc::new(tokio::sync::Mutex::new(raw_rx));
    let mut worker_handles = Vec::with_capacity(config.workers);
    for _ in 0..config.workers {
        let raw_rx = Arc::clone(&raw_rx);
        let processed_tx = processed_tx.clone();
        worker_handles.push(tokio::task::spawn_blocking(move || -> Result<()> {
            loop {
                let chunk = {
                    let mut rx = raw_rx.blocking_lock();
                    rx.blocking_recv()
                };
                let Some(chunk) = chunk else {
                    break;
                };
                let original_length = chunk.data.len() as u64;
                let content_id = content_id_from_bytes(&chunk.data);
                let compressed = Compressor::compress(codec, &chunk.data)?;
                if processed_tx
                    .blocking_send(Ok(ProcessedChunk {
                        index: chunk.index,
                        offset: chunk.offset,
                        original_length,
                        content_id,
                        compressed,
                    }))
                    .is_err()
                {
                    break;
                }
            }
            Ok(())
        }));
    }
    drop(processed_tx);

    let mut stored_bytes = 0u64;
    let mut unique_chunks = 0usize;
    let mut reused_chunks = 0usize;
    let mut next_index = 0u64;
    let mut pending: BTreeMap<u64, ProcessedChunk> = BTreeMap::new();

    while let Some(processed) = processed_rx.recv().await {
        let processed = processed?;
        pending.insert(processed.index, processed);
        while let Some(chunk) = pending.remove(&next_index) {
            write_bucket.consume(chunk.compressed.len() as u64).await;
            let (is_new, stored_codec, stored_len) =
                repo.write_chunk(&chunk.content_id, codec, &chunk.compressed)?;
            if is_new {
                unique_chunks += 1;
            } else {
                reused_chunks += 1;
            }
            stored_bytes += stored_len;
            if let Some(pb) = &progress {
                pb.inc(chunk.original_length);
            }
            manifest.add_chunk(ChunkDescriptor {
                index: chunk.index,
                logical_offset: chunk.offset,
                original_length: chunk.original_length,
                stored_length: stored_len,
                codec: stored_codec,
                content_id: chunk.content_id,
            });
            next_index += 1;
        }
    }

    reader_handle
        .await
        .map_err(|e| Error::ChunkEncode(format!("join error: {e}")))??;
    for handle in worker_handles {
        handle
            .await
            .map_err(|e| Error::ChunkEncode(format!("worker join error: {e}")))??;
    }
    if !pending.is_empty() {
        return Err(Error::ChunkEncode(
            "backup ended with unordered leftover chunks".to_string(),
        ));
    }

    if let Some(pb) = progress {
        pb.finish_and_clear();
    }

    source.verify_unchanged()?;

    manifest.root_hash = root_hash_from_manifest(&manifest);
    repo.commit_manifest(&manifest)?;

    let total_chunks = unique_chunks + reused_chunks;
    let dedupe_ratio = if stored_bytes > 0 {
        total_bytes as f64 / stored_bytes as f64
    } else {
        1.0
    };

    Ok(BackupSummary {
        backup_id,
        original_bytes: total_bytes,
        stored_bytes,
        total_chunks,
        unique_chunks,
        reused_chunks,
        root_hash: manifest.root_hash.to_hex(),
        dedupe_ratio,
    })
}

pub async fn verify(repo: &Repository, backup_id: &str) -> Result<VerifyReport> {
    let manifest = match repo.load_manifest(backup_id) {
        Ok(m) => m,
        Err(e) => {
            return Ok(VerifyReport {
                backup_id: backup_id.to_string(),
                total_chunks: 0,
                total_bytes: 0,
                root_hash: String::new(),
                is_valid: false,
                error: Some(e.to_string()),
            });
        }
    };

    let expected_root = root_hash_from_manifest(&manifest);
    if manifest.root_hash != expected_root {
        return Ok(VerifyReport {
            backup_id: backup_id.to_string(),
            total_chunks: manifest.chunks.len(),
            total_bytes: manifest.total_logical_bytes,
            root_hash: manifest.root_hash.to_hex(),
            is_valid: false,
            error: Some("root hash mismatch in manifest metadata".to_string()),
        });
    }

    let mut logical_pos = 0u64;
    for (i, c) in manifest.chunks.iter().enumerate() {
        if c.index != i as u64 || c.logical_offset != logical_pos {
            return Ok(VerifyReport {
                backup_id: backup_id.to_string(),
                total_chunks: manifest.chunks.len(),
                total_bytes: manifest.total_logical_bytes,
                root_hash: manifest.root_hash.to_hex(),
                is_valid: false,
                error: Some(format!(
                    "chunk sequence broken at index {i}: expected offset {logical_pos}, got {}",
                    c.logical_offset
                )),
            });
        }

        if c.original_length > MAX_ORIGINAL_CHUNK_BYTES {
            return Ok(VerifyReport {
                backup_id: backup_id.to_string(),
                total_chunks: manifest.chunks.len(),
                total_bytes: manifest.total_logical_bytes,
                root_hash: manifest.root_hash.to_hex(),
                is_valid: false,
                error: Some(format!(
                    "chunk {} original length {} exceeds max {MAX_ORIGINAL_CHUNK_BYTES}",
                    c.content_id.to_hex(),
                    c.original_length
                )),
            });
        }

        let stored = match repo.read_chunk(&c.content_id) {
            Ok(chunk) => chunk,
            Err(e) => {
                return Ok(VerifyReport {
                    backup_id: backup_id.to_string(),
                    total_chunks: manifest.chunks.len(),
                    total_bytes: manifest.total_logical_bytes,
                    root_hash: manifest.root_hash.to_hex(),
                    is_valid: false,
                    error: Some(format!("chunk {} read error: {e}", c.content_id.to_hex())),
                });
            }
        };

        if stored.payload.len() as u64 != c.stored_length
            || stored.codec.type_tag() != c.codec.type_tag()
        {
            return Ok(VerifyReport {
                backup_id: backup_id.to_string(),
                total_chunks: manifest.chunks.len(),
                total_bytes: manifest.total_logical_bytes,
                root_hash: manifest.root_hash.to_hex(),
                is_valid: false,
                error: Some(format!(
                    "chunk {} stored metadata mismatch: expected codec {:?}/{} bytes, got {:?}/{}",
                    c.content_id.to_hex(),
                    c.codec,
                    c.stored_length,
                    stored.codec,
                    stored.payload.len()
                )),
            });
        }

        let raw =
            match Compressor::decompress(stored.codec, &stored.payload, c.original_length as usize)
            {
                Ok(b) => b,
                Err(e) => {
                    return Ok(VerifyReport {
                        backup_id: backup_id.to_string(),
                        total_chunks: manifest.chunks.len(),
                        total_bytes: manifest.total_logical_bytes,
                        root_hash: manifest.root_hash.to_hex(),
                        is_valid: false,
                        error: Some(format!(
                            "chunk {} decompression failure: {e}",
                            c.content_id.to_hex()
                        )),
                    });
                }
            };

        let calculated_cid = content_id_from_bytes(&raw);
        if calculated_cid != c.content_id {
            return Ok(VerifyReport {
                backup_id: backup_id.to_string(),
                total_chunks: manifest.chunks.len(),
                total_bytes: manifest.total_logical_bytes,
                root_hash: manifest.root_hash.to_hex(),
                is_valid: false,
                error: Some(format!(
                    "chunk {} hash mismatch after decompression (got {})",
                    c.content_id.to_hex(),
                    calculated_cid.to_hex()
                )),
            });
        }

        logical_pos += c.original_length;
    }

    if logical_pos != manifest.total_logical_bytes {
        return Ok(VerifyReport {
            backup_id: backup_id.to_string(),
            total_chunks: manifest.chunks.len(),
            total_bytes: manifest.total_logical_bytes,
            root_hash: manifest.root_hash.to_hex(),
            is_valid: false,
            error: Some(format!(
                "total logical bytes mismatch: expected {}, reconstructed {}",
                manifest.total_logical_bytes, logical_pos
            )),
        });
    }

    Ok(VerifyReport {
        backup_id: backup_id.to_string(),
        total_chunks: manifest.chunks.len(),
        total_bytes: manifest.total_logical_bytes,
        root_hash: manifest.root_hash.to_hex(),
        is_valid: true,
        error: None,
    })
}

pub async fn restore(
    repo: &Repository,
    backup_id: &str,
    target_path: &Path,
    force: bool,
) -> Result<()> {
    let report = verify(repo, backup_id).await?;
    if !report.is_ok() {
        return Err(Error::RootHashMismatch {
            backup_id: backup_id.to_string(),
        });
    }

    let manifest = repo.load_manifest(backup_id)?;
    let expected_root = root_hash_from_manifest(&manifest);
    if manifest.root_hash != expected_root {
        return Err(Error::RootHashMismatch {
            backup_id: backup_id.to_string(),
        });
    }

    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_target = if let Some(parent) = target_path.parent() {
        parent.join(format!(".tmp-restore-{}", getrandom_hex(6)))
    } else {
        PathBuf::from(format!(".tmp-restore-{}", getrandom_hex(6)))
    };

    let restore_result = (|| -> Result<()> {
        let mut out_file = File::create(&tmp_target).map_err(|source| Error::OutputWrite {
            path: tmp_target.clone(),
            source,
        })?;

        let mut written = 0u64;
        let mut expected_offset = 0u64;
        for (i, c) in manifest.chunks.iter().enumerate() {
            if c.index != i as u64 || c.logical_offset != expected_offset {
                return Err(Error::ManifestCorrupt(format!(
                    "chunk sequence broken at {i}"
                )));
            }
            if c.original_length > MAX_ORIGINAL_CHUNK_BYTES {
                return Err(Error::ChunkCorrupt {
                    content_id: c.content_id.to_hex(),
                    reason: format!("original length {} exceeds max", c.original_length),
                });
            }
            let stored = repo.read_chunk(&c.content_id)?;
            let raw =
                Compressor::decompress(stored.codec, &stored.payload, c.original_length as usize)?;
            let cid = content_id_from_bytes(&raw);
            if cid != c.content_id {
                return Err(Error::ChunkCorrupt {
                    content_id: c.content_id.to_hex(),
                    reason: "hash mismatch on restore".to_string(),
                });
            }
            out_file.seek(SeekFrom::Start(c.logical_offset))?;
            out_file.write_all(&raw)?;
            written += raw.len() as u64;
            expected_offset += c.original_length;
        }

        if written != manifest.total_logical_bytes {
            return Err(Error::OutputWrite {
                path: tmp_target.clone(),
                source: std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "restored bytes do not match manifest total",
                ),
            });
        }

        out_file.flush()?;
        out_file.sync_all()?;
        drop(out_file);

        if force {
            fs::rename(&tmp_target, target_path)?;
        } else {
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(target_path)
            {
                Ok(placeholder) => {
                    drop(placeholder);
                    fs::rename(&tmp_target, target_path)?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(Error::RestoreTargetExists(target_path.to_path_buf()));
                }
                Err(e) => {
                    return Err(Error::OutputWrite {
                        path: target_path.to_path_buf(),
                        source: e,
                    });
                }
            }
        }
        Ok(())
    })();

    if restore_result.is_err() {
        let _ = fs::remove_file(&tmp_target);
    }
    restore_result
}

pub fn inspect(repo: &Repository, backup_id: &str) -> Result<InspectReport> {
    let manifest = repo.load_manifest(backup_id)?;
    let unique_count = manifest
        .chunks
        .iter()
        .map(|c| c.content_id)
        .collect::<std::collections::HashSet<_>>()
        .len();
    let stored_bytes = manifest.chunks.iter().map(|c| c.stored_length).sum();

    Ok(InspectReport {
        backup_id: manifest.backup_id,
        created_unix_ms: manifest.created_unix_ms,
        source_path: manifest.source_path,
        total_logical_bytes: manifest.total_logical_bytes,
        total_chunks: manifest.chunks.len(),
        unique_chunks: unique_count,
        stored_bytes,
        root_hash: manifest.root_hash.to_hex(),
        fastcdc_params: (
            manifest.fastcdc_min,
            manifest.fastcdc_avg,
            manifest.fastcdc_max,
        ),
    })
}

fn getrandom_hex(len: usize) -> String {
    let mut buf = vec![0u8; len];
    let _ = getrandom::fill(&mut buf);
    hex::encode(buf)
}
