use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs4::FileExt;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use serde::{Deserialize, Serialize};

use crate::codec::{CompressionCodec, StoredChunk};
use crate::error::{Error, Result};
use crate::ids::{validate_backup_id, ContentId};
use crate::lifecycle::delete::{
    DeletePlan, DeleteResult, UndeletePlan, UndeleteResult, TOMBSTONES,
};
use crate::lifecycle::lease::{
    insert_reader_lease, is_process_stale, read_active_reader_leases, ReaderLease,
    ReaderLeaseGuard, READER_LEASES,
};
use crate::manifest::Manifest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupSummaryItem {
    pub backup_id: String,
    pub created_unix_ms: u64,
    pub source_path: String,
    pub total_logical_bytes: u64,
    pub total_chunks: usize,
    pub stored_bytes: u64,
    pub root_hash: String,
}

pub const REPO_FORMAT_VERSION: u32 = 2;
pub const REPO_LAYOUT_VERSION_FILE: &str = "VERSION";
pub const REPO_CHUNKS_DIR: &str = "chunks";
pub const REPO_MANIFESTS_DIR: &str = "manifests";
pub const REPO_TMP_DIR: &str = "tmp";
pub const REPO_LOCK_FILE: &str = ".lock";
pub const REPO_DB_FILE: &str = "index.redb";

const TABLE_BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
const TABLE_CHUNKS: TableDefinition<&str, u64> = TableDefinition::new("chunks");

#[derive(Debug)]
pub struct RepositoryLock {
    _file: File,
}

impl Drop for RepositoryLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self._file);
    }
}

pub struct Repository {
    root: PathBuf,
    db: Arc<Database>,
}

impl Repository {
    pub fn init(path: &Path) -> Result<Self> {
        fs::create_dir_all(path.join(REPO_CHUNKS_DIR))?;
        fs::create_dir_all(path.join(REPO_MANIFESTS_DIR))?;
        fs::create_dir_all(path.join(REPO_TMP_DIR))?;

        let version_file = path.join(REPO_LAYOUT_VERSION_FILE);
        let found = if version_file.exists() {
            let content = fs::read_to_string(&version_file)?;
            Some(content.trim().parse::<u32>().map_err(|_| {
                Error::UnsupportedRepositoryVersion {
                    found: 0,
                    supported: REPO_FORMAT_VERSION,
                }
            })?)
        } else {
            None
        };
        if let Some(found) = found {
            if found != 1 && found != REPO_FORMAT_VERSION {
                return Err(Error::UnsupportedRepositoryVersion {
                    found,
                    supported: REPO_FORMAT_VERSION,
                });
            }
        }

        let db_path = path.join(REPO_DB_FILE);
        let db = if found == Some(1) {
            if !db_path.is_file() {
                return Err(Error::RepositoryNotInitialized(db_path));
            }
            let db = Database::open(&db_path)?;
            {
                let read_txn = db.begin_read()?;
                let _ = read_txn.open_table(TABLE_BACKUPS)?;
                let _ = read_txn.open_table(TABLE_CHUNKS)?;
            }
            {
                let write_txn = db.begin_write()?;
                {
                    let _ = write_txn.open_table(TOMBSTONES)?;
                    let _ = write_txn.open_table(READER_LEASES)?;
                }
                write_txn.commit()?;
            }
            db
        } else {
            let db = Database::create(&db_path)?;
            {
                let write_txn = db.begin_write()?;
                {
                    let _ = write_txn.open_table(TABLE_BACKUPS)?;
                    let _ = write_txn.open_table(TABLE_CHUNKS)?;
                    let _ = write_txn.open_table(TOMBSTONES)?;
                    let _ = write_txn.open_table(READER_LEASES)?;
                }
                write_txn.commit()?;
            }
            db
        };
        if found != Some(REPO_FORMAT_VERSION) {
            publish_repository_version(path, &version_file)?;
        }

        Ok(Self {
            root: path.to_path_buf(),
            db: Arc::new(db),
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(Error::RepositoryNotInitialized(path.to_path_buf()));
        }
        let version_file = path.join(REPO_LAYOUT_VERSION_FILE);
        if !version_file.exists() {
            return Err(Error::NotARepository(path.to_path_buf()));
        }
        let content = fs::read_to_string(&version_file)?;
        let found: u32 =
            content
                .trim()
                .parse()
                .map_err(|_| Error::UnsupportedRepositoryVersion {
                    found: 0,
                    supported: REPO_FORMAT_VERSION,
                })?;
        if found != REPO_FORMAT_VERSION {
            return Err(Error::UnsupportedRepositoryVersion {
                found,
                supported: REPO_FORMAT_VERSION,
            });
        }
        let db_path = path.join(REPO_DB_FILE);
        let db = Database::open(db_path)?;
        Ok(Self {
            root: path.to_path_buf(),
            db: Arc::new(db),
        })
    }

    pub fn acquire_writer_lock(&self) -> Result<RepositoryLock> {
        let lock_path = self.root.join(REPO_LOCK_FILE);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        FileExt::try_lock(&file).map_err(|e| {
            let io_err: std::io::Error = e.into();
            if io_err.kind() == std::io::ErrorKind::WouldBlock {
                Error::RepositoryLocked(self.root.clone())
            } else {
                Error::Io(io_err)
            }
        })?;
        Ok(RepositoryLock { _file: file })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn chunk_path(&self, cid: &ContentId) -> PathBuf {
        let hex = cid.to_hex();
        let prefix = &hex[0..2];
        let rest = &hex[2..];
        self.root.join(REPO_CHUNKS_DIR).join(prefix).join(rest)
    }

    pub fn has_chunk(&self, cid: &ContentId) -> bool {
        self.chunk_path(cid).is_file()
    }

    pub fn read_chunk(&self, cid: &ContentId) -> Result<StoredChunk> {
        let path = self.chunk_path(cid);
        if !path.is_file() {
            return Err(Error::ChunkMissing {
                content_id: cid.to_hex(),
            });
        }
        let bytes = fs::read(&path).map_err(|e| Error::ChunkCorrupt {
            content_id: cid.to_hex(),
            reason: e.to_string(),
        })?;
        StoredChunk::decode(&bytes).map_err(|e| Error::ChunkCorrupt {
            content_id: cid.to_hex(),
            reason: e.to_string(),
        })
    }

    pub fn write_chunk(
        &self,
        cid: &ContentId,
        codec: CompressionCodec,
        compressed_payload: &[u8],
    ) -> Result<(bool, CompressionCodec, u64)> {
        if let Ok(existing) = self.read_chunk(cid) {
            return Ok((false, existing.codec, existing.payload.len() as u64));
        }
        let target = self.chunk_path(cid);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        let encoded = StoredChunk::encode(codec, compressed_payload);
        let rnd = getrandom_hex(6);
        let tmp_path = self
            .root
            .join(REPO_TMP_DIR)
            .join(format!("chunk-{}-{rnd}", cid.to_hex()));

        let mut f = File::create(&tmp_path)?;
        f.write_all(&encoded)?;
        f.flush()?;
        f.sync_all()?;
        drop(f);

        match fs::rename(&tmp_path, &target) {
            Ok(()) => Ok((true, codec, compressed_payload.len() as u64)),
            Err(e) => {
                let _ = fs::remove_file(&tmp_path);
                if let Ok(existing) = self.read_chunk(cid) {
                    Ok((false, existing.codec, existing.payload.len() as u64))
                } else {
                    Err(Error::RepositoryWrite(format!(
                        "failed to commit chunk {}: {e}",
                        cid.to_hex()
                    )))
                }
            }
        }
    }

    pub fn commit_manifest(&self, manifest: &Manifest) -> Result<()> {
        validate_backup_id(&manifest.backup_id)?;
        self.ensure_backup_id_available(&manifest.backup_id)?;
        let encoded = manifest.encode()?;
        let target = self
            .root
            .join(REPO_MANIFESTS_DIR)
            .join(format!("{}.manifest", manifest.backup_id));

        let rnd = getrandom_hex(6);
        let tmp_path = self
            .root
            .join(REPO_TMP_DIR)
            .join(format!("manifest-{}-{rnd}", manifest.backup_id));

        let result = (|| -> Result<()> {
            let mut f = File::create(&tmp_path)?;
            f.write_all(&encoded)?;
            f.flush()?;
            f.sync_all()?;
            drop(f);

            let write_txn = self.db.begin_write()?;
            {
                let tombstones = write_txn.open_table(TOMBSTONES)?;
                if tombstones.get(manifest.backup_id.as_str())?.is_some() {
                    return Err(Error::BackupDeleted(manifest.backup_id.clone()));
                }
                drop(tombstones);
                let mut backups = write_txn.open_table(TABLE_BACKUPS)?;
                if backups.get(manifest.backup_id.as_str())?.is_some() {
                    return Err(Error::BackupAlreadyExists(manifest.backup_id.clone()));
                }
                backups.insert(manifest.backup_id.as_str(), encoded.as_slice())?;

                let mut chunks = write_txn.open_table(TABLE_CHUNKS)?;
                for c in &manifest.chunks {
                    let hex = c.content_id.to_hex();
                    let prev = chunks.get(hex.as_str())?.map(|v| v.value()).unwrap_or(0);
                    chunks.insert(hex.as_str(), prev + 1)?;
                }
            }
            write_txn.commit()?;
            fs::rename(&tmp_path, &target)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        result
    }

    fn ensure_backup_id_available(&self, backup_id: &str) -> Result<()> {
        let read_txn = self.db.begin_read()?;
        let tombstones = read_txn.open_table(TOMBSTONES)?;
        if tombstones.get(backup_id)?.is_some() {
            return Err(Error::BackupDeleted(backup_id.to_string()));
        }
        let backups = read_txn.open_table(TABLE_BACKUPS)?;
        if backups.get(backup_id)?.is_some() {
            return Err(Error::BackupAlreadyExists(backup_id.to_string()));
        }
        Ok(())
    }

    pub fn plan_delete(&self, backup_id: &str) -> Result<DeletePlan> {
        validate_backup_id(backup_id)?;
        let read_txn = self.db.begin_read()?;
        let backups = read_txn.open_table(TABLE_BACKUPS)?;
        if backups.get(backup_id)?.is_none() {
            return Err(Error::BackupNotFound(backup_id.to_string()));
        }
        let tombstones = read_txn.open_table(TOMBSTONES)?;
        Ok(DeletePlan {
            backup_id: backup_id.to_string(),
            already_deleted: tombstones.get(backup_id)?.is_some(),
        })
    }

    /// Applies a delete plan. The caller must hold the repository writer lock.
    pub fn apply_delete(&self, plan: &DeletePlan) -> Result<DeleteResult> {
        validate_backup_id(&plan.backup_id)?;
        let write_txn = self.db.begin_write()?;
        let tombstoned = {
            let backups = write_txn.open_table(TABLE_BACKUPS)?;
            if backups.get(plan.backup_id.as_str())?.is_none() {
                return Err(Error::BackupNotFound(plan.backup_id.clone()));
            }
            drop(backups);

            let mut tombstones = write_txn.open_table(TOMBSTONES)?;
            if tombstones.get(plan.backup_id.as_str())?.is_some() {
                false
            } else {
                tombstones.insert(plan.backup_id.as_str(), unix_ms())?;
                true
            }
        };
        write_txn.commit()?;
        Ok(DeleteResult {
            backup_id: plan.backup_id.clone(),
            tombstoned,
        })
    }

    pub fn plan_undelete(&self, backup_id: &str) -> Result<UndeletePlan> {
        validate_backup_id(backup_id)?;
        let read_txn = self.db.begin_read()?;
        let backups = read_txn.open_table(TABLE_BACKUPS)?;
        if backups.get(backup_id)?.is_none() {
            return Err(Error::BackupNotFound(backup_id.to_string()));
        }
        let tombstones = read_txn.open_table(TOMBSTONES)?;
        Ok(UndeletePlan {
            backup_id: backup_id.to_string(),
            tombstoned: tombstones.get(backup_id)?.is_some(),
        })
    }

    /// Applies an undelete plan. The caller must hold the repository writer lock.
    pub fn apply_undelete(&self, plan: &UndeletePlan) -> Result<UndeleteResult> {
        validate_backup_id(&plan.backup_id)?;
        let write_txn = self.db.begin_write()?;
        let restored = {
            let backups = write_txn.open_table(TABLE_BACKUPS)?;
            if backups.get(plan.backup_id.as_str())?.is_none() {
                return Err(Error::BackupNotFound(plan.backup_id.clone()));
            }
            drop(backups);
            let mut tombstones = write_txn.open_table(TOMBSTONES)?;
            let removed = tombstones.remove(plan.backup_id.as_str())?.is_some();
            removed
        };
        write_txn.commit()?;
        Ok(UndeleteResult {
            backup_id: plan.backup_id.clone(),
            restored,
        })
    }

    pub fn is_tombstoned(&self, backup_id: &str) -> Result<bool> {
        validate_backup_id(backup_id)?;
        let read_txn = self.db.begin_read()?;
        let tombstones = read_txn.open_table(TOMBSTONES)?;
        Ok(tombstones.get(backup_id)?.is_some())
    }

    pub fn acquire_reader_lease(&self, backup_id: &str) -> Result<ReaderLeaseGuard> {
        self.resolve_manifest_with_reader_lease(backup_id)
            .map(|(_, guard)| guard)
    }

    pub(crate) fn resolve_manifest_with_reader_lease(
        &self,
        backup_id: &str,
    ) -> Result<(Manifest, ReaderLeaseGuard)> {
        validate_backup_id(backup_id)?;
        let write_txn = self.db.begin_write()?;
        let manifest = {
            let backups = write_txn.open_table(TABLE_BACKUPS)?;
            let bytes = backups
                .get(backup_id)?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| Error::BackupNotFound(backup_id.to_string()))?;
            drop(backups);
            let tombstones = write_txn.open_table(TOMBSTONES)?;
            if tombstones.get(backup_id)?.is_some() {
                return Err(Error::BackupDeleted(backup_id.to_string()));
            }
            drop(tombstones);
            Manifest::decode(&bytes)?
        };
        let (_, lease_id) = insert_reader_lease(&write_txn, backup_id)?;
        write_txn.commit()?;
        Ok((
            manifest,
            ReaderLeaseGuard::new(Arc::clone(&self.db), lease_id),
        ))
    }

    pub fn active_reader_leases(&self) -> Result<Vec<ReaderLease>> {
        let read_txn = self.db.begin_read()?;
        read_active_reader_leases(&read_txn)
    }

    pub fn remove_stale_reader_leases(&self) -> Result<Vec<String>> {
        let write_txn = self.db.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(READER_LEASES)?;
            let mut stale_ids = Vec::new();
            for item in table.iter()? {
                let (key, value) = item?;
                let lease = ReaderLease::decode(value.value())?;
                if is_process_stale(lease.pid, &lease.process_start_token) {
                    stale_ids.push(key.value().to_string());
                }
            }
            stale_ids.sort();
            for lease_id in &stale_ids {
                let _ = table.remove(lease_id.as_str())?;
            }
            stale_ids
        };
        write_txn.commit()?;
        Ok(removed)
    }

    pub fn load_manifest(&self, backup_id: &str) -> Result<Manifest> {
        validate_backup_id(backup_id)?;
        let read_txn = self.db.begin_read()?;
        let backups = read_txn.open_table(TABLE_BACKUPS)?;
        let bytes = backups
            .get(backup_id)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| Error::BackupNotFound(backup_id.to_string()))?;
        let tombstones = read_txn.open_table(TOMBSTONES)?;
        if tombstones.get(backup_id)?.is_some() {
            return Err(Error::BackupDeleted(backup_id.to_string()));
        }
        Manifest::decode(&bytes)
    }

    pub fn list_backups(&self) -> Result<Vec<String>> {
        Ok(self
            .list_backup_summaries()?
            .into_iter()
            .map(|item| item.backup_id)
            .collect())
    }

    pub fn list_backup_summaries(&self) -> Result<Vec<BackupSummaryItem>> {
        let read_txn = self.db.begin_read()?;
        let backups = read_txn.open_table(TABLE_BACKUPS)?;
        let tombstones = read_txn.open_table(TOMBSTONES)?;
        let mut items = Vec::new();
        for item in backups.iter()? {
            let (key, value) = item?;
            if tombstones.get(key.value())?.is_some() {
                continue;
            }
            let manifest = Manifest::decode(value.value())?;
            let stored_bytes = manifest.chunks.iter().map(|c| c.stored_length).sum();
            items.push(BackupSummaryItem {
                backup_id: manifest.backup_id,
                created_unix_ms: manifest.created_unix_ms,
                source_path: manifest.source_path,
                total_logical_bytes: manifest.total_logical_bytes,
                total_chunks: manifest.chunks.len(),
                stored_bytes,
                root_hash: manifest.root_hash.to_hex(),
            });
        }
        items.sort_by(|a, b| a.backup_id.cmp(&b.backup_id));
        Ok(items)
    }
}

fn publish_repository_version(path: &Path, version_file: &Path) -> Result<()> {
    let tmp_version = path.join(REPO_TMP_DIR).join("VERSION.init");
    let mut file = File::create(&tmp_version)?;
    file.write_all(format!("{REPO_FORMAT_VERSION}\n").as_bytes())?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    fs::rename(tmp_version, version_file)?;
    Ok(())
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn getrandom_hex(bytes_len: usize) -> String {
    let mut buf = vec![0u8; bytes_len];
    let _ = getrandom::fill(&mut buf);
    hex::encode(buf)
}
