use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use fs4::FileExt;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use serde::{Deserialize, Serialize};

use crate::codec::{CompressionCodec, StoredChunk};
use crate::error::{Error, Result};
use crate::ids::{validate_backup_id, ContentId};
use crate::lifecycle::delete::{
    DeletePlan, DeleteResult, UndeletePlan, UndeleteResult, TOMBSTONES,
};
use crate::lifecycle::gc::{
    ChunkMove, GcJournal, GcPhase, GcPlan, GcRecoveryResult, GcResult, GcTombstone, GC_BARRIER_KEY,
    GC_BARRIER_VALUE, GC_JOURNALS, GC_STATE, MAX_GC_CHUNKS,
};
use crate::lifecycle::lease::{
    insert_reader_lease, is_process_stale, read_active_reader_leases, ReaderLease,
    ReaderLeaseGuard, READER_LEASES,
};
use crate::manifest::{ChunkDescriptor, Manifest};

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
pub const REPO_TRASH_DIR: &str = "trash";
pub const REPO_LOCK_FILE: &str = ".lock";
pub const REPO_DB_FILE: &str = "index.redb";

pub(crate) const TABLE_BACKUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("backups");
pub(crate) const TABLE_CHUNKS: TableDefinition<&str, u64> = TableDefinition::new("chunks");

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
    db: Mutex<Option<Arc<Database>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcFaultPoint {
    AfterRename(usize),
    AfterCommitted,
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
                    let _ = write_txn.open_table(GC_JOURNALS)?;
                    let _ = write_txn.open_table(GC_STATE)?;
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
                    let _ = write_txn.open_table(GC_JOURNALS)?;
                    let _ = write_txn.open_table(GC_STATE)?;
                }
                write_txn.commit()?;
            }
            db
        };
        if found != Some(REPO_FORMAT_VERSION) {
            publish_repository_version(path, &version_file)?;
        }

        let root = fs::canonicalize(path)?;
        validate_repository_directories(&root)?;
        Ok(Self {
            root,
            db: Mutex::new(Some(Arc::new(db))),
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
        let root = fs::canonicalize(path)?;
        validate_repository_directories(&root)?;
        let db_path = root.join(REPO_DB_FILE);
        let db = Database::open(db_path)?;
        Ok(Self {
            root,
            db: Mutex::new(Some(Arc::new(db))),
        })
    }

    pub(crate) fn database(&self) -> Result<Arc<Database>> {
        let guard = match self.db.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::Database("repository database is closed".to_string()))
    }

    pub(crate) fn take_database(&self) -> Result<Arc<Database>> {
        let mut guard = match self.db.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .take()
            .ok_or_else(|| Error::Database("repository database is closed".to_string()))
    }

    pub(crate) fn install_database(&self, db: Database) {
        let handle = Some(Arc::new(db));
        match self.db.lock() {
            Ok(mut guard) => *guard = handle,
            Err(poisoned) => *poisoned.into_inner() = handle,
        }
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

        let db = self.database()?;
        let result = (|| -> Result<()> {
            let mut f = File::create(&tmp_path)?;
            f.write_all(&encoded)?;
            f.flush()?;
            f.sync_all()?;
            drop(f);

            let write_txn = db.begin_write()?;
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
        let db = self.database()?;
        let read_txn = db.begin_read()?;
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
        let db = self.database()?;
        let read_txn = db.begin_read()?;
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
        let db = self.database()?;
        let write_txn = db.begin_write()?;
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
        let db = self.database()?;
        let read_txn = db.begin_read()?;
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
        let db = self.database()?;
        let write_txn = db.begin_write()?;
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
        let db = self.database()?;
        let read_txn = db.begin_read()?;
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
        let db = self.database()?;
        let write_txn = db.begin_write()?;
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
        {
            let state = write_txn.open_table(GC_STATE)?;
            if state.get(GC_BARRIER_KEY)?.is_some() {
                return Err(Error::GarbageCollection(
                    "reader lease refused while garbage collection barrier is held".to_string(),
                ));
            }
        }
        let (_, lease_id) = insert_reader_lease(&write_txn, backup_id)?;
        write_txn.commit()?;
        Ok((manifest, ReaderLeaseGuard::new(db, lease_id)))
    }

    pub fn active_reader_leases(&self) -> Result<Vec<ReaderLease>> {
        let db = self.database()?;
        let read_txn = db.begin_read()?;
        read_active_reader_leases(&read_txn)
    }

    fn allocate_gc_id(&self) -> Result<String> {
        const MAX_ATTEMPTS: usize = 8;
        for _ in 0..MAX_ATTEMPTS {
            let gc_id = format!("gc-{}-{}", unix_ms(), getrandom_hex_result(8)?);
            let db = self.database()?;
            let read_txn = db.begin_read()?;
            let journals = read_txn.open_table(GC_JOURNALS)?;
            let journal_exists = journals.get(gc_id.as_str())?.is_some();
            drop(journals);
            drop(read_txn);
            let trash_exists =
                fs::symlink_metadata(self.root.join(REPO_TRASH_DIR).join(&gc_id)).is_ok();
            if !journal_exists && !trash_exists {
                return Ok(gc_id);
            }
        }
        Err(Error::GarbageCollection(
            "failed to allocate unique GC ID".to_string(),
        ))
    }

    pub fn plan_gc(&self) -> Result<GcPlan> {
        let db = self.database()?;
        let read_txn = db.begin_read()?;
        if !read_active_reader_leases(&read_txn)?.is_empty() {
            return Err(Error::GarbageCollection(
                "garbage collection refused while active reader leases exist".to_string(),
            ));
        }
        let backups = read_txn.open_table(TABLE_BACKUPS)?;
        let tombstones = read_txn.open_table(TOMBSTONES)?;
        let mut live = BTreeMap::new();
        let mut planned_tombstones = Vec::new();
        for item in backups.iter()? {
            let (key, value) = item?;
            validate_backup_id(key.value())?;
            let manifest = Manifest::decode(value.value())?;
            if manifest.backup_id != key.value() {
                return Err(Error::ManifestCorrupt(format!(
                    "backup index key {} does not match manifest ID {}",
                    key.value(),
                    manifest.backup_id
                )));
            }
            if let Some(deleted) = tombstones.get(key.value())? {
                planned_tombstones.push(GcTombstone {
                    backup_id: key.value().to_string(),
                    deleted_unix_ms: deleted.value(),
                });
                continue;
            }
            for chunk in manifest.chunks {
                if let Some(existing) = live.insert(chunk.content_id, chunk.clone()) {
                    if existing.codec != chunk.codec
                        || existing.original_length != chunk.original_length
                        || existing.stored_length != chunk.stored_length
                    {
                        return Err(Error::ManifestCorrupt(format!(
                            "live metadata disagrees for chunk {}",
                            chunk.content_id
                        )));
                    }
                }
            }
        }
        for item in tombstones.iter()? {
            let (key, _) = item?;
            validate_backup_id(key.value())?;
            if backups.get(key.value())?.is_none() {
                return Err(Error::GarbageCollection(format!(
                    "tombstone {} has no authoritative backup",
                    key.value()
                )));
            }
        }
        planned_tombstones.sort();
        for (content_id, descriptor) in &live {
            let path = self.chunk_path(content_id);
            let bytes = fs::symlink_metadata(&path)
                .map(|metadata| metadata.len())
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        Error::ChunkMissing {
                            content_id: content_id.to_hex(),
                        }
                    } else {
                        error.into()
                    }
                })?;
            validate_chunk_file(&path, content_id, Some(descriptor), bytes)?;
        }

        let gc_id = self.allocate_gc_id()?;
        let mut reclaim_chunks = Vec::new();
        for prefix_entry in fs::read_dir(self.root.join(REPO_CHUNKS_DIR))? {
            let prefix_entry = prefix_entry?;
            let prefix = prefix_entry.file_name().into_string().map_err(|_| {
                Error::GarbageCollection("unexpected non-UTF-8 chunk directory name".to_string())
            })?;
            if prefix.len() != 2
                || !prefix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || !prefix_entry.file_type()?.is_dir()
            {
                return Err(Error::GarbageCollection(format!(
                    "unexpected chunk directory entry: {prefix}"
                )));
            }
            for chunk_entry in fs::read_dir(prefix_entry.path())? {
                let chunk_entry = chunk_entry?;
                let rest = chunk_entry.file_name().into_string().map_err(|_| {
                    Error::GarbageCollection("unexpected non-UTF-8 chunk file name".to_string())
                })?;
                if rest.len() != 62
                    || !rest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    || !chunk_entry.file_type()?.is_file()
                {
                    return Err(Error::GarbageCollection(format!(
                        "unexpected chunk file entry: {prefix}/{rest}"
                    )));
                }
                let content_id = ContentId::parse(&format!("{prefix}{rest}"))?;
                if live.contains_key(&content_id) {
                    continue;
                }
                if reclaim_chunks.len() == MAX_GC_CHUNKS {
                    return Err(Error::GarbageCollection(
                        "GC inventory exceeds bounded chunk maximum".to_string(),
                    ));
                }
                let bytes = chunk_entry.metadata()?.len();
                validate_chunk_ancestor(&self.root, &chunk_entry.path())?;
                validate_chunk_file(&chunk_entry.path(), &content_id, None, bytes)?;
                reclaim_chunks.push(ChunkMove {
                    content_id: content_id.to_hex(),
                    source: PathBuf::from(REPO_CHUNKS_DIR).join(&prefix).join(&rest),
                    trash: PathBuf::from(REPO_TRASH_DIR)
                        .join(&gc_id)
                        .join(&prefix)
                        .join(&rest),
                    bytes,
                });
            }
        }
        reclaim_chunks.sort_by(|a, b| a.content_id.cmp(&b.content_id));
        let reclaim_bytes = reclaim_chunks.iter().map(|item| item.bytes).sum();
        Ok(GcPlan {
            gc_id,
            live_chunks: live.len() as u64,
            reclaim_chunks,
            reclaim_bytes,
            tombstones: planned_tombstones,
        })
    }

    /// Applies a GC plan. The caller must hold the repository writer lock.
    ///
    /// This primitive intentionally does not acquire a nested OS writer lock. Task 6 callers
    /// acquire the lock and revalidate the plan before calling this method.
    pub fn apply_gc(&self, plan: &GcPlan) -> Result<GcResult> {
        self.apply_gc_inner(plan, None)
    }

    fn apply_gc_inner(&self, plan: &GcPlan, fault: Option<GcFaultPoint>) -> Result<GcResult> {
        if !self.active_reader_leases()?.is_empty() {
            return Err(Error::GarbageCollection(
                "garbage collection refused while active reader leases exist".to_string(),
            ));
        }
        let current = self.plan_gc()?;
        validate_gc_plan(plan, &current, &self.root)?;
        let db = self.database()?;
        ensure_gc_namespace_available(&db, &self.root, &plan.gc_id)?;
        ensure_confined_real_directory(&self.root, &self.root.join(REPO_MANIFESTS_DIR))?;
        let journal = GcJournal {
            plan: plan.clone(),
            phase: GcPhase::Moving,
            moved: Vec::new(),
        };
        install_gc_barrier_and_journal(&db, &self.root, &journal)?;
        for (index, item) in plan.reclaim_chunks.iter().enumerate() {
            let source = self.root.join(&item.source);
            let trash = self.root.join(&item.trash);
            ensure_gc_trash_parent(&self.root, plan, item)?;
            validate_chunk_ancestor(&self.root, &source)?;
            validate_stored_chunk_file(&source, item)?;
            validate_gc_trash_parent(&self.root, plan, item)?;
            fs::rename(&source, &trash)?;
            sync_directory(trash.parent().unwrap_or(&self.root))?;
            sync_directory(source.parent().unwrap_or(&self.root))?;
            if fault == Some(GcFaultPoint::AfterRename(index + 1)) {
                return Err(Error::GarbageCollection(
                    "injected GC fault after rename".to_string(),
                ));
            }
        }
        let mut journal = journal;
        journal.phase = GcPhase::Committed;
        persist_gc_journal_durable(&db, &self.root, &journal)?;
        if fault == Some(GcFaultPoint::AfterCommitted) {
            return Err(Error::GarbageCollection(
                "injected GC fault after committed marker".to_string(),
            ));
        }
        delete_gc_trash(&self.root, plan)?;
        self.finalize_gc_tombstones(&plan.tombstones)?;
        remove_gc_trash_directories(&self.root, plan)?;
        remove_gc_journal_and_barrier(&db, &self.root, &plan.gc_id)?;
        Ok(GcResult {
            gc_id: plan.gc_id.clone(),
            reclaimed_chunks: plan.reclaim_chunks.len() as u64,
            reclaimed_bytes: plan.reclaim_bytes,
        })
    }

    #[cfg(test)]
    fn apply_gc_with_fault(&self, plan: &GcPlan, fault: GcFaultPoint) -> Result<GcResult> {
        self.apply_gc_inner(plan, Some(fault))
    }

    /// Recovers interrupted GC work. The caller must hold the repository writer lock.
    ///
    /// This low-level primitive intentionally does not acquire a nested OS writer lock.
    pub fn recover_gc(&self) -> Result<Vec<GcRecoveryResult>> {
        let db = self.database()?;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(GC_JOURNALS)?;
        let mut journals = Vec::new();
        for item in table.iter()? {
            let (_, value) = item?;
            journals.push(GcJournal::decode(value.value())?);
        }
        drop(table);
        drop(read_txn);
        journals.sort_by(|a, b| a.plan.gc_id.cmp(&b.plan.gc_id));

        let mut results = Vec::new();
        for mut journal in journals {
            validate_gc_journal_plan(&journal.plan, &self.root)?;
            match journal.phase {
                GcPhase::Planned | GcPhase::Moving => {
                    ensure_gc_barrier(&db)?;
                    rollback_gc_moves(&self.root, &journal.plan)?;
                    remove_gc_trash_directories(&self.root, &journal.plan)?;
                    remove_gc_journal_and_barrier(&db, &self.root, &journal.plan.gc_id)?;
                }
                GcPhase::Committed | GcPhase::Deleting => {
                    ensure_gc_barrier(&db)?;
                    validate_recovery_live_set(&db, &journal.plan)?;
                    journal.phase = GcPhase::Deleting;
                    delete_gc_trash(&self.root, &journal.plan)?;
                    self.finalize_gc_tombstones(&journal.plan.tombstones)?;
                    remove_gc_trash_directories(&self.root, &journal.plan)?;
                    remove_gc_journal_and_barrier(&db, &self.root, &journal.plan.gc_id)?;
                }
                GcPhase::Complete => {
                    remove_gc_trash_directories(&self.root, &journal.plan)?;
                    remove_gc_journal_and_barrier(&db, &self.root, &journal.plan.gc_id)?;
                }
            }
            results.push(GcRecoveryResult {
                gc_id: journal.plan.gc_id,
                phase: match journal.phase {
                    GcPhase::Planned | GcPhase::Moving => GcPhase::Planned,
                    _ => GcPhase::Complete,
                },
            });
        }
        Ok(results)
    }

    fn finalize_gc_tombstones(&self, planned: &[GcTombstone]) -> Result<()> {
        ensure_confined_real_directory(&self.root, &self.root.join(REPO_MANIFESTS_DIR))?;
        let db = self.database()?;
        match tombstone_set_status(&db, planned)? {
            TombstoneSetStatus::Pending => {}
            TombstoneSetStatus::Finalized => return Ok(()),
        }
        for tombstone in planned {
            validate_backup_id(&tombstone.backup_id)?;
        }
        for tombstone in planned {
            let manifest = self
                .root
                .join(REPO_MANIFESTS_DIR)
                .join(format!("{}.manifest", tombstone.backup_id));
            match fs::symlink_metadata(&manifest) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                    return Err(Error::GarbageCollection(format!(
                        "GC manifest copy is not a regular file: {}",
                        manifest.display()
                    )))
                }
                Ok(_) => fs::remove_file(&manifest)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        sync_directory(&self.root.join(REPO_MANIFESTS_DIR))?;

        let write_txn = db.begin_write()?;
        {
            let mut tombstones = write_txn.open_table(TOMBSTONES)?;
            let mut backups = write_txn.open_table(TABLE_BACKUPS)?;
            for tombstone in planned {
                match tombstones.get(tombstone.backup_id.as_str())? {
                    Some(value) if value.value() == tombstone.deleted_unix_ms => {}
                    _ => {
                        return Err(Error::GarbageCollection(
                            "stale GC tombstone set".to_string(),
                        ))
                    }
                }
                let _ = backups.remove(tombstone.backup_id.as_str())?;
                let _ = tombstones.remove(tombstone.backup_id.as_str())?;
            }
        }
        write_txn.commit()?;
        sync_database_parent(&self.root)?;
        Ok(())
    }

    pub fn remove_stale_reader_leases(&self) -> Result<Vec<String>> {
        let db = self.database()?;
        let write_txn = db.begin_write()?;
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
        let db = self.database()?;
        let read_txn = db.begin_read()?;
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
        let db = self.database()?;
        let read_txn = db.begin_read()?;
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

fn ensure_gc_namespace_available(db: &Database, root: &Path, gc_id: &str) -> Result<()> {
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(GC_JOURNALS)?;
    if table.get(gc_id)?.is_some() {
        return Err(Error::GarbageCollection(
            "GC journal ID already exists".to_string(),
        ));
    }
    drop(table);
    drop(read_txn);
    if fs::symlink_metadata(root.join(REPO_TRASH_DIR).join(gc_id)).is_ok() {
        return Err(Error::GarbageCollection(
            "GC trash namespace already exists".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TombstoneSetStatus {
    Pending,
    Finalized,
}

fn tombstone_set_status(db: &Database, planned: &[GcTombstone]) -> Result<TombstoneSetStatus> {
    let read_txn = db.begin_read()?;
    let tombstones = read_txn.open_table(TOMBSTONES)?;
    let backups = read_txn.open_table(TABLE_BACKUPS)?;
    let mut current = Vec::new();
    for item in tombstones.iter()? {
        let (key, value) = item?;
        validate_backup_id(key.value())?;
        current.push(GcTombstone {
            backup_id: key.value().to_string(),
            deleted_unix_ms: value.value(),
        });
    }
    current.sort();
    if current == planned {
        return Ok(TombstoneSetStatus::Pending);
    }
    if current.is_empty()
        && planned.iter().all(|entry| {
            backups
                .get(entry.backup_id.as_str())
                .is_ok_and(|value| value.is_none())
        })
    {
        return Ok(TombstoneSetStatus::Finalized);
    }
    Err(Error::GarbageCollection(
        "stale GC tombstone set".to_string(),
    ))
}

fn install_gc_barrier_and_journal(db: &Database, root: &Path, journal: &GcJournal) -> Result<()> {
    let bytes = journal.encode()?;
    let write_txn = db.begin_write()?;
    {
        let leases = write_txn.open_table(READER_LEASES)?;
        if leases.iter()?.next().is_some() {
            return Err(Error::GarbageCollection(
                "garbage collection refused while active reader leases exist".to_string(),
            ));
        }
        drop(leases);
        let mut state = write_txn.open_table(GC_STATE)?;
        if let Some(existing) = state.get(GC_BARRIER_KEY)? {
            if existing.value() != GC_BARRIER_VALUE {
                return Err(Error::GarbageCollection(
                    "invalid garbage collection barrier state".to_string(),
                ));
            }
            return Err(Error::GarbageCollection(format!(
                "garbage collection barrier already held before {}",
                journal.plan.gc_id
            )));
        }
        state.insert(GC_BARRIER_KEY, GC_BARRIER_VALUE)?;
        drop(state);
        let mut journals = write_txn.open_table(GC_JOURNALS)?;
        journals.insert(journal.plan.gc_id.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    sync_database_parent(root)
}

fn ensure_gc_barrier(db: &Database) -> Result<()> {
    let write_txn = db.begin_write()?;
    {
        let leases = write_txn.open_table(READER_LEASES)?;
        if leases.iter()?.next().is_some() {
            return Err(Error::GarbageCollection(
                "garbage collection recovery refused while active reader leases exist".to_string(),
            ));
        }
        drop(leases);
        let mut state = write_txn.open_table(GC_STATE)?;
        let barrier = state.get(GC_BARRIER_KEY)?.map(|value| value.value());
        match barrier {
            Some(value) if value == GC_BARRIER_VALUE => {}
            Some(_) => {
                return Err(Error::GarbageCollection(
                    "invalid garbage collection barrier state".to_string(),
                ))
            }
            None => {
                state.insert(GC_BARRIER_KEY, GC_BARRIER_VALUE)?;
            }
        }
    }
    write_txn.commit()?;
    Ok(())
}

fn persist_gc_journal(db: &Database, journal: &GcJournal) -> Result<()> {
    let bytes = journal.encode()?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(GC_JOURNALS)?;
        table.insert(journal.plan.gc_id.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn persist_gc_journal_durable(db: &Database, root: &Path, journal: &GcJournal) -> Result<()> {
    persist_gc_journal(db, journal)?;
    sync_database_parent(root)
}

fn remove_gc_journal_and_barrier(db: &Database, root: &Path, gc_id: &str) -> Result<()> {
    let write_txn = db.begin_write()?;
    {
        let mut journals = write_txn.open_table(GC_JOURNALS)?;
        let _ = journals.remove(gc_id)?;
        drop(journals);
        let mut state = write_txn.open_table(GC_STATE)?;
        let _ = state.remove(GC_BARRIER_KEY)?;
    }
    write_txn.commit()?;
    sync_database_parent(root)
}

fn validate_repository_directories(root: &Path) -> Result<()> {
    ensure_confined_real_directory(root, root)?;
    for directory in [REPO_CHUNKS_DIR, REPO_MANIFESTS_DIR, REPO_TMP_DIR] {
        ensure_confined_real_directory(root, &root.join(directory))?;
    }
    Ok(())
}

fn ensure_confined_real_directory(root: &Path, path: &Path) -> Result<()> {
    ensure_real_directory(path)?;
    let canonical = fs::canonicalize(path)?;
    if !canonical.starts_with(root) {
        return Err(Error::GarbageCollection(format!(
            "GC directory escapes repository root: {}",
            path.display()
        )));
    }
    Ok(())
}

fn ensure_real_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::GarbageCollection(format!(
            "GC path is not a real directory: {}",
            path.display()
        )));
    }
    Ok(())
}

fn create_and_sync_directory(root: &Path, parent: &Path, child: &Path) -> Result<()> {
    ensure_confined_real_directory(root, parent)?;
    match fs::symlink_metadata(child) {
        Ok(_) => ensure_confined_real_directory(root, child),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(child)?;
            sync_directory(parent)?;
            ensure_confined_real_directory(root, child)
        }
        Err(error) => Err(error.into()),
    }
}

fn validate_gc_trash_parent(root: &Path, plan: &GcPlan, item: &ChunkMove) -> Result<()> {
    let trash_root = root.join(REPO_TRASH_DIR);
    let gc_root = trash_root.join(&plan.gc_id);
    let prefix = item
        .trash
        .parent()
        .and_then(Path::file_name)
        .ok_or_else(|| Error::GarbageCollection("invalid GC trash prefix".to_string()))?;
    ensure_confined_real_directory(root, root)?;
    ensure_confined_real_directory(root, &trash_root)?;
    ensure_confined_real_directory(root, &gc_root)?;
    ensure_confined_real_directory(root, &gc_root.join(prefix))
}

fn ensure_gc_trash_parent(root: &Path, plan: &GcPlan, item: &ChunkMove) -> Result<()> {
    ensure_real_directory(root)?;
    let trash_root = root.join(REPO_TRASH_DIR);
    create_and_sync_directory(root, root, &trash_root)?;
    let gc_root = trash_root.join(&plan.gc_id);
    create_and_sync_directory(root, &trash_root, &gc_root)?;
    let prefix = item
        .trash
        .parent()
        .and_then(Path::file_name)
        .ok_or_else(|| Error::GarbageCollection("invalid GC trash prefix".to_string()))?;
    create_and_sync_directory(root, &gc_root, &gc_root.join(prefix))?;
    validate_gc_trash_parent(root, plan, item)
}

fn validate_regular_file(path: &Path, bytes: u64) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != bytes {
        return Err(Error::GarbageCollection(format!(
            "GC file metadata mismatch: {}",
            path.display()
        )));
    }
    Ok(())
}

fn rollback_gc_moves(root: &Path, plan: &GcPlan) -> Result<()> {
    for item in plan.reclaim_chunks.iter().rev() {
        let source = root.join(&item.source);
        let trash = root.join(&item.trash);
        let source_exists = fs::symlink_metadata(&source).is_ok();
        let trash_exists = fs::symlink_metadata(&trash).is_ok();
        match (source_exists, trash_exists) {
            (true, true) => {
                return Err(Error::GarbageCollection(format!(
                    "conflicting source and trash copies for {}",
                    item.content_id
                )))
            }
            (false, true) => {
                validate_gc_trash_parent(root, plan, item)?;
                let content_id = ContentId::parse(&item.content_id)?;
                validate_chunk_file(&trash, &content_id, None, item.bytes)?;
                let source_parent = source.parent().ok_or_else(|| {
                    Error::GarbageCollection("invalid GC source parent".to_string())
                })?;
                ensure_confined_real_directory(root, source_parent)?;
                fs::rename(&trash, &source)?;
                sync_directory(source.parent().unwrap_or(root))?;
                sync_directory(trash.parent().unwrap_or(root))?;
            }
            (true, false) => {
                validate_chunk_ancestor(root, &source)?;
                let content_id = ContentId::parse(&item.content_id)?;
                validate_chunk_file(&source, &content_id, None, item.bytes)?;
            }
            (false, false) => {
                return Err(Error::GarbageCollection(format!(
                    "missing source and trash copies for uncommitted chunk {}",
                    item.content_id
                )))
            }
        }
    }
    Ok(())
}

fn validate_recovery_live_set(db: &Database, plan: &GcPlan) -> Result<()> {
    let read_txn = db.begin_read()?;
    let backups = read_txn.open_table(TABLE_BACKUPS)?;
    let tombstones = read_txn.open_table(TOMBSTONES)?;
    let reclaim: BTreeSet<_> = plan
        .reclaim_chunks
        .iter()
        .map(|item| ContentId::parse(&item.content_id))
        .collect::<Result<_>>()?;
    for item in backups.iter()? {
        let (key, value) = item?;
        validate_backup_id(key.value())?;
        if tombstones.get(key.value())?.is_some() {
            continue;
        }
        let manifest = Manifest::decode(value.value())?;
        if manifest.backup_id != key.value() {
            return Err(Error::ManifestCorrupt(format!(
                "backup index key {} does not match manifest ID {}",
                key.value(),
                manifest.backup_id
            )));
        }
        if let Some(chunk) = manifest
            .chunks
            .iter()
            .find(|chunk| reclaim.contains(&chunk.content_id))
        {
            return Err(Error::GarbageCollection(format!(
                "recovery refuses chunk {} because it became live",
                chunk.content_id
            )));
        }
    }
    Ok(())
}

fn remove_gc_trash_directories(root: &Path, plan: &GcPlan) -> Result<()> {
    let trash_root = root.join(REPO_TRASH_DIR);
    let gc_root = trash_root.join(&plan.gc_id);
    if !gc_root.exists() {
        return Ok(());
    }
    ensure_confined_real_directory(root, &trash_root)?;
    ensure_confined_real_directory(root, &gc_root)?;
    for entry in fs::read_dir(&gc_root)? {
        let entry = entry?;
        ensure_confined_real_directory(root, &entry.path())?;
        fs::remove_dir(entry.path())?;
    }
    fs::remove_dir(&gc_root)?;
    sync_directory(&root.join(REPO_TRASH_DIR))
}

fn delete_gc_trash(root: &Path, plan: &GcPlan) -> Result<()> {
    for item in &plan.reclaim_chunks {
        let source = root.join(&item.source);
        let trash = root.join(&item.trash);
        let source_exists = fs::symlink_metadata(&source).is_ok();
        let trash_exists = fs::symlink_metadata(&trash).is_ok();
        if source_exists && trash_exists {
            return Err(Error::GarbageCollection(format!(
                "conflicting source and trash copies for {}",
                item.content_id
            )));
        }
        if source_exists {
            ensure_gc_trash_parent(root, plan, item)?;
            validate_chunk_ancestor(root, &source)?;
            let content_id = ContentId::parse(&item.content_id)?;
            validate_chunk_file(&source, &content_id, None, item.bytes)?;
            validate_gc_trash_parent(root, plan, item)?;
            fs::rename(&source, &trash)?;
            sync_directory(trash.parent().unwrap_or(root))?;
            sync_directory(source.parent().unwrap_or(root))?;
        }
        if trash_exists {
            validate_gc_trash_parent(root, plan, item)?;
            let content_id = ContentId::parse(&item.content_id)?;
            validate_chunk_file(&trash, &content_id, None, item.bytes)?;
        }
        match fs::remove_file(&trash) {
            Ok(()) => sync_directory(trash.parent().unwrap_or(root))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn sync_database_parent(root: &Path) -> Result<()> {
    File::open(root.join(REPO_DB_FILE))?.sync_all()?;
    sync_directory(root)
}

fn validate_chunk_ancestor(root: &Path, path: &Path) -> Result<()> {
    ensure_confined_real_directory(root, &root.join(REPO_CHUNKS_DIR))?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::GarbageCollection("invalid chunk parent directory".to_string()))?;
    ensure_confined_real_directory(root, parent)
}

fn validate_gc_journal_plan(plan: &GcPlan, root: &Path) -> Result<()> {
    if validate_backup_id(&plan.gc_id).is_err() || !plan.gc_id.starts_with("gc-") {
        return Err(Error::GarbageCollection("invalid GC ID".to_string()));
    }
    if plan.reclaim_chunks.len() > MAX_GC_CHUNKS {
        return Err(Error::GarbageCollection(
            "GC plan exceeds bounded chunk maximum".to_string(),
        ));
    }
    for item in &plan.reclaim_chunks {
        let cid = ContentId::parse(&item.content_id)?;
        let hex = cid.to_hex();
        let expected_source = PathBuf::from(REPO_CHUNKS_DIR)
            .join(&hex[..2])
            .join(&hex[2..]);
        let expected_trash = PathBuf::from(REPO_TRASH_DIR)
            .join(&plan.gc_id)
            .join(&hex[..2])
            .join(&hex[2..]);
        if item.source != expected_source || item.trash != expected_trash {
            return Err(Error::GarbageCollection(
                "GC journal path does not match canonical repository layout".to_string(),
            ));
        }
        let source = root.join(&item.source);
        let trash = root.join(&item.trash);
        let chunks_root = root.join(REPO_CHUNKS_DIR);
        ensure_confined_real_directory(root, &chunks_root)?;
        let source_parent = source
            .parent()
            .ok_or_else(|| Error::GarbageCollection("invalid GC source parent".to_string()))?;
        ensure_confined_real_directory(root, source_parent)?;
        ensure_confined_real_directory(root, &root.join(REPO_MANIFESTS_DIR))?;
        for path in [&source, &trash] {
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(Error::GarbageCollection(format!(
                        "GC recovery refuses symlink {}",
                        path.display()
                    )))
                }
                Ok(metadata) if !metadata.is_file() => {
                    return Err(Error::GarbageCollection(format!(
                        "GC recovery expected regular file {}",
                        path.display()
                    )))
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_chunk_file(
    path: &Path,
    content_id: &ContentId,
    descriptor: Option<&ChunkDescriptor>,
    bytes: u64,
) -> Result<()> {
    use crate::codec::{Compressor, MAX_ORIGINAL_CHUNK_BYTES, STORED_CHUNK_HEADER_LEN};
    use crate::hash::content_id_from_bytes;

    validate_regular_file(path, bytes)?;
    let max_stored = MAX_ORIGINAL_CHUNK_BYTES
        .checked_mul(2)
        .and_then(|value| value.checked_add(STORED_CHUNK_HEADER_LEN as u64))
        .ok_or_else(|| Error::ChunkCorrupt {
            content_id: content_id.to_hex(),
            reason: "stored chunk bound overflow".to_string(),
        })?;
    if bytes > max_stored {
        return Err(Error::ChunkCorrupt {
            content_id: content_id.to_hex(),
            reason: "stored chunk exceeds bounded maximum".to_string(),
        });
    }
    let encoded = fs::read(path).map_err(|error| Error::ChunkCorrupt {
        content_id: content_id.to_hex(),
        reason: error.to_string(),
    })?;
    let stored = StoredChunk::decode(&encoded).map_err(|error| Error::ChunkCorrupt {
        content_id: content_id.to_hex(),
        reason: error.to_string(),
    })?;
    if let Some(chunk) = descriptor {
        if chunk.original_length > MAX_ORIGINAL_CHUNK_BYTES {
            return Err(Error::ChunkCorrupt {
                content_id: content_id.to_hex(),
                reason: "original chunk exceeds bounded maximum".to_string(),
            });
        }
        if chunk.codec != stored.codec || chunk.stored_length != stored.payload.len() as u64 {
            return Err(Error::ChunkCorrupt {
                content_id: content_id.to_hex(),
                reason: "stored chunk metadata does not match manifest".to_string(),
            });
        }
    }
    let raw = match descriptor {
        Some(chunk) => Compressor::decompress(
            stored.codec,
            &stored.payload,
            chunk.original_length as usize,
        ),
        None => decompress_chunk_bounded(stored.codec, &stored.payload),
    }
    .map_err(|error| Error::ChunkCorrupt {
        content_id: content_id.to_hex(),
        reason: error.to_string(),
    })?;
    if content_id_from_bytes(&raw) != *content_id {
        return Err(Error::ChunkCorrupt {
            content_id: content_id.to_hex(),
            reason: "stored chunk payload does not match path content ID".to_string(),
        });
    }
    Ok(())
}

fn decompress_chunk_bounded(codec: CompressionCodec, payload: &[u8]) -> Result<Vec<u8>> {
    use crate::codec::MAX_ORIGINAL_CHUNK_BYTES;

    match codec {
        CompressionCodec::None => Ok(payload.to_vec()),
        CompressionCodec::Zstd { .. } => {
            let decoder = zstd::stream::Decoder::new(payload)
                .map_err(|error| Error::Decompression(error.to_string()))?;
            read_to_end_bounded(decoder, MAX_ORIGINAL_CHUNK_BYTES as usize)
        }
        CompressionCodec::Lz4 => {
            let decoder = lz4_flex::frame::FrameDecoder::new(payload);
            read_to_end_bounded(decoder, MAX_ORIGINAL_CHUNK_BYTES as usize)
        }
    }
}

fn read_to_end_bounded(mut reader: impl Read, maximum: usize) -> Result<Vec<u8>> {
    let mut raw = Vec::new();
    reader
        .by_ref()
        .take(maximum as u64 + 1)
        .read_to_end(&mut raw)
        .map_err(|error| Error::Decompression(error.to_string()))?;
    if raw.len() > maximum {
        return Err(Error::Decompression(
            "decompressed chunk exceeds bounded maximum".to_string(),
        ));
    }
    Ok(raw)
}

fn validate_stored_chunk_file(path: &Path, item: &ChunkMove) -> Result<()> {
    let content_id = ContentId::parse(&item.content_id)?;
    validate_chunk_file(path, &content_id, None, item.bytes)
}

fn validate_gc_plan(plan: &GcPlan, current: &GcPlan, root: &Path) -> Result<()> {
    validate_gc_journal_plan(plan, root)?;
    if plan.reclaim_bytes
        != plan
            .reclaim_chunks
            .iter()
            .try_fold(0_u64, |total, item| total.checked_add(item.bytes))
            .ok_or_else(|| Error::GarbageCollection("GC byte count overflow".to_string()))?
    {
        return Err(Error::GarbageCollection(
            "GC reclaim byte total mismatch".to_string(),
        ));
    }
    if plan.live_chunks != current.live_chunks
        || plan.reclaim_chunks.len() != current.reclaim_chunks.len()
        || plan.reclaim_bytes != current.reclaim_bytes
        || plan.tombstones != current.tombstones
    {
        return Err(Error::GarbageCollection("stale GC plan".to_string()));
    }
    let current_by_id: std::collections::BTreeMap<_, _> = current
        .reclaim_chunks
        .iter()
        .map(|item| (item.content_id.as_str(), item))
        .collect();
    if current_by_id.len() != plan.reclaim_chunks.len() {
        return Err(Error::GarbageCollection(
            "duplicate or stale GC plan entries".to_string(),
        ));
    }
    for item in &plan.reclaim_chunks {
        let _cid = ContentId::parse(&item.content_id)?;
        let source = root.join(&item.source);
        let metadata = fs::symlink_metadata(&source)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != item.bytes
        {
            return Err(Error::GarbageCollection(format!(
                "GC chunk metadata mismatch for {}",
                item.content_id
            )));
        }
        let expected = current_by_id
            .get(item.content_id.as_str())
            .ok_or_else(|| Error::GarbageCollection("stale GC plan".to_string()))?;
        if expected.bytes != item.bytes || expected.source != item.source {
            return Err(Error::GarbageCollection("stale GC plan".to_string()));
        }
        let trash = root.join(&item.trash);
        if fs::symlink_metadata(&trash).is_ok() {
            return Err(Error::GarbageCollection(format!(
                "GC trash destination already exists for {}",
                item.content_id
            )));
        }
        validate_stored_chunk_file(&source, item)?;
    }
    Ok(())
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
    getrandom_hex_result(bytes_len).unwrap_or_default()
}

fn getrandom_hex_result(bytes_len: usize) -> Result<String> {
    let mut buf = vec![0u8; bytes_len];
    getrandom::fill(&mut buf).map_err(|error| {
        Error::GarbageCollection(format!("random ID generation failed: {error}"))
    })?;
    Ok(hex::encode(buf))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod gc_fault_tests {
    use super::*;
    use crate::hash::{content_id_from_bytes, root_hash_from_manifest};
    use crate::manifest::ChunkDescriptor;

    fn tombstoned_fixture() -> (tempfile::TempDir, PathBuf, GcPlan, ContentId) {
        let temp = tempfile::tempdir().unwrap();
        let repo_path = temp.path().join("repo");
        let repo = Repository::init(&repo_path).unwrap();
        let payload = b"fault-boundary";
        let cid = content_id_from_bytes(payload);
        repo.write_chunk(&cid, CompressionCodec::None, payload)
            .unwrap();
        let mut manifest = Manifest::new("backup-fault", payload.len() as u64, 64, 128, 256);
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
        repo.apply_delete(&repo.plan_delete("backup-fault").unwrap())
            .unwrap();
        let plan = repo.plan_gc().unwrap();
        (temp, repo_path, plan, cid)
    }

    #[test]
    fn fault_after_rename_recovers_by_rolling_back() {
        let (_temp, repo_path, plan, cid) = tombstoned_fixture();
        let repo = Repository::open(&repo_path).unwrap();
        let error = repo
            .apply_gc_with_fault(&plan, GcFaultPoint::AfterRename(1))
            .unwrap_err();
        assert!(error.to_string().contains("injected GC fault"));
        drop(repo);

        let reopened = Repository::open(&repo_path).unwrap();
        assert_eq!(reopened.recover_gc().unwrap()[0].phase, GcPhase::Planned);
        assert!(reopened.chunk_path(&cid).exists());
        assert!(reopened.is_tombstoned("backup-fault").unwrap());
    }

    #[test]
    fn fault_after_committed_marker_recovers_by_rolling_forward() {
        let (_temp, repo_path, plan, cid) = tombstoned_fixture();
        let repo = Repository::open(&repo_path).unwrap();
        let error = repo
            .apply_gc_with_fault(&plan, GcFaultPoint::AfterCommitted)
            .unwrap_err();
        assert!(error.to_string().contains("injected GC fault"));
        drop(repo);

        let reopened = Repository::open(&repo_path).unwrap();
        assert_eq!(reopened.recover_gc().unwrap()[0].phase, GcPhase::Complete);
        assert!(!reopened.chunk_path(&cid).exists());
        assert!(matches!(
            reopened.plan_undelete("backup-fault"),
            Err(Error::BackupNotFound(_))
        ));
        assert!(reopened.recover_gc().unwrap().is_empty());
    }
}
