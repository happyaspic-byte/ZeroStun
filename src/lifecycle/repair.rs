use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use redb::{Database, ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::hash::root_hash_from_manifest;
use crate::ids::{validate_backup_id, ContentId};
use crate::lifecycle::delete::TOMBSTONES;
use crate::lifecycle::gc::{GcJournal, GC_JOURNALS, GC_STATE};
use crate::lifecycle::lease::{is_process_stale, ReaderLease, READER_LEASES};
use crate::manifest::Manifest;
use crate::repository::{
    validate_chunk_file, Repository, REPO_CHUNKS_DIR, REPO_DB_FILE, REPO_LAYOUT_VERSION_FILE,
    REPO_MANIFESTS_DIR, REPO_TMP_DIR, REPO_TRASH_DIR, TABLE_BACKUPS, TABLE_CHUNKS,
};

pub const DEFAULT_MAX_REPAIR_FINDINGS: usize = 4096;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    VersionInvalid,
    RedbInconsistency,
    ManifestCorrupt,
    ManifestFileMissing,
    MissingChunk,
    ChunkCorrupt,
    UnexpectedChunkName,
    TombstoneInconsistency,
    StaleLease,
    TempResidue,
    TrashResidue,
    GcJournal,
    FindingsTruncated,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RepairScope {
    Version,
    Redb,
    Manifests,
    Chunks,
    Tombstones,
    Leases,
    Temp,
    Trash,
    GcJournals,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairFinding {
    pub severity: FindingSeverity,
    pub kind: FindingKind,
    pub path: Option<PathBuf>,
    pub backup_id: Option<String>,
    pub content_id: Option<String>,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairReport {
    pub findings: Vec<RepairFinding>,
    pub inspected: Vec<RepairScope>,
    pub valid_manifests: u64,
    pub valid_chunks: u64,
    pub findings_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairPlan {
    pub rebuild_index: bool,
    pub gc_recoveries: Vec<String>,
    pub stale_leases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairResult {
    pub rebuilt_index: bool,
    pub removed_leases: Vec<String>,
    pub gc_recoveries: Vec<String>,
}

struct FindingSink {
    findings: Vec<RepairFinding>,
    truncated: bool,
    max: usize,
}

impl FindingSink {
    fn new(max: usize) -> Self {
        Self {
            findings: Vec::new(),
            truncated: false,
            max: max.max(1),
        }
    }

    fn push(&mut self, finding: RepairFinding) {
        if self.truncated {
            return;
        }
        if self.findings.len() >= self.max {
            self.truncated = true;
            self.findings.push(RepairFinding {
                severity: FindingSeverity::Warning,
                kind: FindingKind::FindingsTruncated,
                path: None,
                backup_id: None,
                content_id: None,
                detail: format!("repair findings truncated after {} entries", self.max),
            });
            return;
        }
        self.findings.push(finding);
    }
}

fn finding(
    severity: FindingSeverity,
    kind: FindingKind,
    path: Option<PathBuf>,
    backup_id: Option<String>,
    content_id: Option<String>,
    detail: impl Into<String>,
) -> RepairFinding {
    RepairFinding {
        severity,
        kind,
        path,
        backup_id,
        content_id,
        detail: detail.into(),
    }
}

impl Repository {
    pub fn inspect_repair(&self) -> Result<RepairReport> {
        self.inspect_repair_with_max_findings(DEFAULT_MAX_REPAIR_FINDINGS)
    }

    pub fn inspect_repair_with_max_findings(&self, max_findings: usize) -> Result<RepairReport> {
        let mut sink = FindingSink::new(max_findings);
        let mut inspected = Vec::new();
        let snapshot = inspect_snapshot(self, &mut sink, &mut inspected)?;
        Ok(RepairReport {
            findings: sink.findings,
            inspected,
            valid_manifests: snapshot.valid_file_manifests.len() as u64,
            valid_chunks: snapshot.valid_chunks,
            findings_truncated: sink.truncated,
        })
    }

    pub fn plan_repair(&self, _report: &RepairReport) -> Result<RepairPlan> {
        let mut sink = FindingSink::new(DEFAULT_MAX_REPAIR_FINDINGS);
        let mut inspected = Vec::new();
        let snapshot = inspect_snapshot(self, &mut sink, &mut inspected)?;
        Ok(plan_from_snapshot(&snapshot))
    }

    /// Applies a repair plan. The caller owns the exclusive writer lock in CLI
    /// orchestration; this primitive does not acquire a nested lock.
    pub fn apply_repair(&self, plan: &RepairPlan) -> Result<RepairResult> {
        let mut gc_recoveries = Vec::new();
        if !plan.gc_recoveries.is_empty() {
            let recovered = self.recover_gc()?;
            gc_recoveries = recovered.into_iter().map(|item| item.gc_id).collect();
        }

        let mut removed_leases = Vec::new();
        if !plan.stale_leases.is_empty() {
            removed_leases = remove_proven_stale_leases(self, &plan.stale_leases)?;
        }

        let mut rebuilt_index = false;
        if plan.rebuild_index {
            rebuilt_index = rebuild_index_from_valid_manifests(self)?;
        }

        Ok(RepairResult {
            rebuilt_index,
            removed_leases,
            gc_recoveries,
        })
    }
}

struct InspectSnapshot {
    valid_file_manifests: BTreeMap<String, Vec<u8>>,
    valid_redb_manifests: BTreeMap<String, Vec<u8>>,
    valid_chunks: u64,
    stale_leases: Vec<String>,
    gc_journal_ids: Vec<String>,
}

fn inspect_snapshot(
    repo: &Repository,
    sink: &mut FindingSink,
    inspected: &mut Vec<RepairScope>,
) -> Result<InspectSnapshot> {
    inspect_version(repo, sink);
    inspected.push(RepairScope::Version);

    let db = repo.database().ok();
    inspect_redb(db.as_deref(), sink);
    inspected.push(RepairScope::Redb);

    let valid_redb_manifests = collect_redb_manifests(repo, db.as_deref(), sink);
    let valid_file_manifests = inspect_manifest_files(repo, sink);
    inspected.push(RepairScope::Manifests);

    let valid_chunks = inspect_chunks(repo, sink);
    inspected.push(RepairScope::Chunks);

    inspect_tombstones(db.as_deref(), &valid_redb_manifests, sink);
    inspected.push(RepairScope::Tombstones);

    let stale_leases = inspect_leases(db.as_deref(), sink);
    inspected.push(RepairScope::Leases);

    inspect_temp(repo, sink);
    inspected.push(RepairScope::Temp);

    inspect_trash(repo, sink);
    inspected.push(RepairScope::Trash);

    let gc_journal_ids = inspect_gc_journals(db.as_deref(), sink);
    inspected.push(RepairScope::GcJournals);

    Ok(InspectSnapshot {
        valid_file_manifests,
        valid_redb_manifests,
        valid_chunks,
        stale_leases,
        gc_journal_ids,
    })
}

fn plan_from_snapshot(snapshot: &InspectSnapshot) -> RepairPlan {
    let desired = desired_backups(
        &snapshot.valid_file_manifests,
        &snapshot.valid_redb_manifests,
    );
    RepairPlan {
        rebuild_index: desired != snapshot.valid_redb_manifests,
        gc_recoveries: snapshot.gc_journal_ids.clone(),
        stale_leases: snapshot.stale_leases.clone(),
    }
}

fn desired_backups(
    files: &BTreeMap<String, Vec<u8>>,
    redb: &BTreeMap<String, Vec<u8>>,
) -> BTreeMap<String, Vec<u8>> {
    let mut desired = redb.clone();
    for (id, encoded) in files {
        desired.insert(id.clone(), encoded.clone());
    }
    desired
}

fn inspect_version(repo: &Repository, sink: &mut FindingSink) {
    let path = repo.root().join(REPO_LAYOUT_VERSION_FILE);
    match fs::read_to_string(&path) {
        Ok(content) => match content.trim().parse::<u32>() {
            Ok(version) if version == crate::repository::REPO_FORMAT_VERSION => {}
            Ok(version) => sink.push(finding(
                FindingSeverity::Critical,
                FindingKind::VersionInvalid,
                Some(PathBuf::from(REPO_LAYOUT_VERSION_FILE)),
                None,
                None,
                format!(
                    "VERSION is {version}, expected {}",
                    crate::repository::REPO_FORMAT_VERSION
                ),
            )),
            Err(_) => sink.push(finding(
                FindingSeverity::Critical,
                FindingKind::VersionInvalid,
                Some(PathBuf::from(REPO_LAYOUT_VERSION_FILE)),
                None,
                None,
                "VERSION is not an integer",
            )),
        },
        Err(error) => sink.push(finding(
            FindingSeverity::Critical,
            FindingKind::VersionInvalid,
            Some(PathBuf::from(REPO_LAYOUT_VERSION_FILE)),
            None,
            None,
            format!("failed to read VERSION: {error}"),
        )),
    }
}

fn inspect_redb(db: Option<&Database>, sink: &mut FindingSink) {
    let Some(db) = db else {
        sink.push(finding(
            FindingSeverity::Critical,
            FindingKind::RedbInconsistency,
            Some(PathBuf::from(REPO_DB_FILE)),
            None,
            None,
            "index.redb is not openable",
        ));
        return;
    };
    let Ok(read_txn) = db.begin_read() else {
        sink.push(finding(
            FindingSeverity::Critical,
            FindingKind::RedbInconsistency,
            Some(PathBuf::from(REPO_DB_FILE)),
            None,
            None,
            "failed to begin redb read transaction",
        ));
        return;
    };
    for (name, open) in [
        (
            "backups",
            read_txn.open_table(TABLE_BACKUPS).map(|_| ()).err(),
        ),
        (
            "chunks",
            read_txn.open_table(TABLE_CHUNKS).map(|_| ()).err(),
        ),
        (
            "tombstones",
            read_txn.open_table(TOMBSTONES).map(|_| ()).err(),
        ),
        (
            "reader_leases",
            read_txn.open_table(READER_LEASES).map(|_| ()).err(),
        ),
        (
            "gc_journals",
            read_txn.open_table(GC_JOURNALS).map(|_| ()).err(),
        ),
        ("gc_state", read_txn.open_table(GC_STATE).map(|_| ()).err()),
    ] {
        if let Some(error) = open {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::RedbInconsistency,
                Some(PathBuf::from(REPO_DB_FILE)),
                None,
                None,
                format!("redb table {name} is missing or unreadable: {error}"),
            ));
        }
    }
}

fn collect_redb_manifests(
    repo: &Repository,
    db: Option<&Database>,
    sink: &mut FindingSink,
) -> BTreeMap<String, Vec<u8>> {
    let mut valid = BTreeMap::new();
    let Some(db) = db else {
        return valid;
    };
    let Ok(read_txn) = db.begin_read() else {
        return valid;
    };
    let Ok(table) = read_txn.open_table(TABLE_BACKUPS) else {
        return valid;
    };
    let Ok(iter) = table.iter() else {
        sink.push(finding(
            FindingSeverity::Critical,
            FindingKind::RedbInconsistency,
            Some(PathBuf::from(REPO_DB_FILE)),
            None,
            None,
            "failed to iterate backups table",
        ));
        return valid;
    };
    for item in iter {
        let Ok((key, value)) = item else {
            sink.push(finding(
                FindingSeverity::Critical,
                FindingKind::RedbInconsistency,
                Some(PathBuf::from(REPO_DB_FILE)),
                None,
                None,
                "failed to read backups table entry",
            ));
            continue;
        };
        let backup_id = key.value().to_string();
        let encoded = value.value().to_vec();
        match verify_encoded_manifest(repo, &backup_id, &encoded) {
            Ok(()) => {
                valid.insert(backup_id, encoded);
            }
            Err(error) => {
                classify_manifest_error(sink, None, Some(backup_id), error);
            }
        }
    }
    valid
}

fn inspect_manifest_files(repo: &Repository, sink: &mut FindingSink) -> BTreeMap<String, Vec<u8>> {
    let mut valid = BTreeMap::new();
    let dir = repo.root().join(REPO_MANIFESTS_DIR);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) => {
            sink.push(finding(
                FindingSeverity::Critical,
                FindingKind::ManifestCorrupt,
                Some(PathBuf::from(REPO_MANIFESTS_DIR)),
                None,
                None,
                format!("failed to read manifests directory: {error}"),
            ));
            return valid;
        }
    };
    for entry in entries {
        let Ok(entry) = entry else {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::ManifestCorrupt,
                Some(PathBuf::from(REPO_MANIFESTS_DIR)),
                None,
                None,
                "failed to read manifests directory entry",
            ));
            continue;
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::ManifestCorrupt,
                Some(PathBuf::from(REPO_MANIFESTS_DIR).join(entry.file_name())),
                None,
                None,
                "non-UTF-8 manifest file name",
            ));
            continue;
        };
        let relative = PathBuf::from(REPO_MANIFESTS_DIR).join(name);
        if !name.ends_with(".manifest") {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::ManifestCorrupt,
                Some(relative),
                None,
                None,
                "unexpected file in manifests directory",
            ));
            continue;
        }
        let backup_id = name.trim_end_matches(".manifest").to_string();
        let bytes = match fs::read(entry.path()) {
            Ok(bytes) => bytes,
            Err(error) => {
                sink.push(finding(
                    FindingSeverity::Critical,
                    FindingKind::ManifestCorrupt,
                    Some(relative),
                    Some(backup_id),
                    None,
                    format!("failed to read manifest copy: {error}"),
                ));
                continue;
            }
        };
        match verify_encoded_manifest(repo, &backup_id, &bytes) {
            Ok(()) => {
                valid.insert(backup_id, bytes);
            }
            Err(error) => classify_manifest_error(sink, Some(relative), Some(backup_id), error),
        }
    }
    valid
}

fn classify_manifest_error(
    sink: &mut FindingSink,
    path: Option<PathBuf>,
    backup_id: Option<String>,
    error: Error,
) {
    match error {
        Error::ChunkMissing { content_id } => sink.push(finding(
            FindingSeverity::Critical,
            FindingKind::MissingChunk,
            path,
            backup_id,
            Some(content_id.clone()),
            format!("chunk {content_id} is missing"),
        )),
        Error::ChunkCorrupt { content_id, reason } => sink.push(finding(
            FindingSeverity::Critical,
            FindingKind::ChunkCorrupt,
            path,
            backup_id,
            Some(content_id),
            reason,
        )),
        other => sink.push(finding(
            FindingSeverity::Critical,
            FindingKind::ManifestCorrupt,
            path,
            backup_id,
            None,
            other.to_string(),
        )),
    }
}

fn verify_encoded_manifest(repo: &Repository, expected_id: &str, encoded: &[u8]) -> Result<()> {
    validate_backup_id(expected_id)?;
    let manifest = Manifest::decode(encoded)?;
    if manifest.backup_id != expected_id {
        return Err(Error::ManifestCorrupt(format!(
            "manifest ID {} does not match {expected_id}",
            manifest.backup_id
        )));
    }
    if root_hash_from_manifest(&manifest) != manifest.root_hash {
        return Err(Error::RootHashMismatch {
            backup_id: expected_id.to_string(),
        });
    }
    let mut logical = 0_u64;
    for (index, chunk) in manifest.chunks.iter().enumerate() {
        if chunk.index != index as u64 || chunk.logical_offset != logical {
            return Err(Error::ManifestCorrupt(format!(
                "chunk sequence broken at {index}"
            )));
        }
        let path = repo.chunk_path(&chunk.content_id);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::ChunkMissing {
                    content_id: chunk.content_id.to_hex(),
                });
            }
            Err(error) => {
                return Err(Error::ChunkCorrupt {
                    content_id: chunk.content_id.to_hex(),
                    reason: error.to_string(),
                });
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::ChunkCorrupt {
                content_id: chunk.content_id.to_hex(),
                reason: "chunk is not a regular file".to_string(),
            });
        }
        validate_chunk_file(&path, &chunk.content_id, Some(chunk), metadata.len())?;
        logical = logical
            .checked_add(chunk.original_length)
            .ok_or_else(|| Error::ManifestCorrupt("logical byte overflow".to_string()))?;
    }
    if logical != manifest.total_logical_bytes {
        return Err(Error::ManifestCorrupt(format!(
            "total logical bytes mismatch: expected {}, reconstructed {logical}",
            manifest.total_logical_bytes
        )));
    }
    Ok(())
}

fn inspect_chunks(repo: &Repository, sink: &mut FindingSink) -> u64 {
    let mut valid_chunks = 0_u64;
    let dir = repo.root().join(REPO_CHUNKS_DIR);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) => {
            sink.push(finding(
                FindingSeverity::Critical,
                FindingKind::ChunkCorrupt,
                Some(PathBuf::from(REPO_CHUNKS_DIR)),
                None,
                None,
                format!("failed to read chunks directory: {error}"),
            ));
            return 0;
        }
    };
    for prefix_entry in entries {
        let Ok(prefix_entry) = prefix_entry else {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::UnexpectedChunkName,
                Some(PathBuf::from(REPO_CHUNKS_DIR)),
                None,
                None,
                "failed to read chunks directory entry",
            ));
            continue;
        };
        let prefix = match prefix_entry.file_name().into_string() {
            Ok(prefix) => prefix,
            Err(_) => {
                sink.push(finding(
                    FindingSeverity::Warning,
                    FindingKind::UnexpectedChunkName,
                    Some(PathBuf::from(REPO_CHUNKS_DIR).join(prefix_entry.file_name())),
                    None,
                    None,
                    "non-UTF-8 chunk directory name",
                ));
                continue;
            }
        };
        let file_type = match prefix_entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                sink.push(finding(
                    FindingSeverity::Warning,
                    FindingKind::UnexpectedChunkName,
                    Some(PathBuf::from(REPO_CHUNKS_DIR).join(&prefix)),
                    None,
                    None,
                    format!("failed to stat chunk directory entry: {error}"),
                ));
                continue;
            }
        };
        if prefix.len() != 2 || !is_lower_hex(&prefix) || !file_type.is_dir() {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::UnexpectedChunkName,
                Some(PathBuf::from(REPO_CHUNKS_DIR).join(&prefix)),
                None,
                None,
                format!("unexpected chunk directory entry: {prefix}"),
            ));
            continue;
        }
        let children = match fs::read_dir(prefix_entry.path()) {
            Ok(children) => children,
            Err(error) => {
                sink.push(finding(
                    FindingSeverity::Warning,
                    FindingKind::UnexpectedChunkName,
                    Some(PathBuf::from(REPO_CHUNKS_DIR).join(&prefix)),
                    None,
                    None,
                    format!("failed to read chunk prefix directory: {error}"),
                ));
                continue;
            }
        };
        for chunk_entry in children {
            let Ok(chunk_entry) = chunk_entry else {
                sink.push(finding(
                    FindingSeverity::Warning,
                    FindingKind::UnexpectedChunkName,
                    Some(PathBuf::from(REPO_CHUNKS_DIR).join(&prefix)),
                    None,
                    None,
                    "failed to read chunk file entry",
                ));
                continue;
            };
            let rest = match chunk_entry.file_name().into_string() {
                Ok(rest) => rest,
                Err(_) => {
                    sink.push(finding(
                        FindingSeverity::Warning,
                        FindingKind::UnexpectedChunkName,
                        Some(
                            PathBuf::from(REPO_CHUNKS_DIR)
                                .join(&prefix)
                                .join(chunk_entry.file_name()),
                        ),
                        None,
                        None,
                        "non-UTF-8 chunk file name",
                    ));
                    continue;
                }
            };
            let relative = PathBuf::from(REPO_CHUNKS_DIR).join(&prefix).join(&rest);
            let file_type = match chunk_entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) => {
                    sink.push(finding(
                        FindingSeverity::Warning,
                        FindingKind::UnexpectedChunkName,
                        Some(relative),
                        None,
                        None,
                        format!("failed to stat chunk file: {error}"),
                    ));
                    continue;
                }
            };
            if rest.len() != 62 || !is_lower_hex(&rest) || !file_type.is_file() {
                sink.push(finding(
                    FindingSeverity::Warning,
                    FindingKind::UnexpectedChunkName,
                    Some(relative),
                    None,
                    None,
                    format!("unexpected chunk file entry: {prefix}/{rest}"),
                ));
                continue;
            }
            let Ok(content_id) = ContentId::parse(&format!("{prefix}{rest}")) else {
                sink.push(finding(
                    FindingSeverity::Warning,
                    FindingKind::UnexpectedChunkName,
                    Some(relative),
                    None,
                    None,
                    "chunk file name is not a content ID",
                ));
                continue;
            };
            let metadata = match fs::symlink_metadata(chunk_entry.path()) {
                Ok(metadata) => metadata,
                Err(error) => {
                    sink.push(finding(
                        FindingSeverity::Warning,
                        FindingKind::ChunkCorrupt,
                        Some(relative),
                        None,
                        Some(content_id.to_hex()),
                        format!("failed to stat chunk: {error}"),
                    ));
                    continue;
                }
            };
            match validate_chunk_file(&chunk_entry.path(), &content_id, None, metadata.len()) {
                Ok(()) => valid_chunks += 1,
                Err(Error::ChunkCorrupt { reason, .. }) => sink.push(finding(
                    FindingSeverity::Critical,
                    FindingKind::ChunkCorrupt,
                    Some(relative),
                    None,
                    Some(content_id.to_hex()),
                    reason,
                )),
                Err(error) => sink.push(finding(
                    FindingSeverity::Critical,
                    FindingKind::ChunkCorrupt,
                    Some(relative),
                    None,
                    Some(content_id.to_hex()),
                    error.to_string(),
                )),
            }
        }
    }

    valid_chunks
}

fn inspect_tombstones(
    db: Option<&Database>,
    redb_manifests: &BTreeMap<String, Vec<u8>>,
    sink: &mut FindingSink,
) {
    let Some(db) = db else {
        return;
    };
    let Ok(read_txn) = db.begin_read() else {
        return;
    };
    let Ok(table) = read_txn.open_table(TOMBSTONES) else {
        return;
    };
    let Ok(iter) = table.iter() else {
        sink.push(finding(
            FindingSeverity::Warning,
            FindingKind::TombstoneInconsistency,
            Some(PathBuf::from(REPO_DB_FILE)),
            None,
            None,
            "failed to iterate tombstones",
        ));
        return;
    };
    for item in iter {
        let Ok((key, _)) = item else {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::TombstoneInconsistency,
                Some(PathBuf::from(REPO_DB_FILE)),
                None,
                None,
                "failed to read tombstone entry",
            ));
            continue;
        };
        let backup_id = key.value().to_string();
        if validate_backup_id(&backup_id).is_err() || !redb_manifests.contains_key(&backup_id) {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::TombstoneInconsistency,
                None,
                Some(backup_id.clone()),
                None,
                format!("tombstone {backup_id} has no authoritative backup"),
            ));
        }
    }
}

fn inspect_leases(db: Option<&Database>, sink: &mut FindingSink) -> Vec<String> {
    let mut stale = Vec::new();
    let Some(db) = db else {
        return stale;
    };
    let Ok(read_txn) = db.begin_read() else {
        return stale;
    };
    let Ok(table) = read_txn.open_table(READER_LEASES) else {
        return stale;
    };
    let Ok(iter) = table.iter() else {
        sink.push(finding(
            FindingSeverity::Warning,
            FindingKind::StaleLease,
            Some(PathBuf::from(REPO_DB_FILE)),
            None,
            None,
            "failed to iterate reader leases",
        ));
        return stale;
    };
    for item in iter {
        let Ok((key, value)) = item else {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::StaleLease,
                Some(PathBuf::from(REPO_DB_FILE)),
                None,
                None,
                "failed to read reader lease entry",
            ));
            continue;
        };
        let lease_id = key.value().to_string();
        match ReaderLease::decode(value.value()) {
            Ok(lease) => {
                if is_process_stale(lease.pid, &lease.process_start_token) {
                    sink.push(finding(
                        FindingSeverity::Warning,
                        FindingKind::StaleLease,
                        None,
                        Some(lease.backup_id),
                        None,
                        format!("reader lease {lease_id} process identity mismatch is proven"),
                    ));
                    stale.push(lease_id);
                }
            }
            Err(error) => sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::StaleLease,
                None,
                None,
                None,
                format!("failed to decode reader lease {lease_id}: {error}"),
            )),
        }
    }
    stale.sort();
    stale
}

fn inspect_temp(repo: &Repository, sink: &mut FindingSink) {
    let dir = repo.root().join(REPO_TMP_DIR);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::TempResidue,
                Some(PathBuf::from(REPO_TMP_DIR)),
                None,
                None,
                format!("failed to read tmp directory: {error}"),
            ));
            return;
        }
    };
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        sink.push(finding(
            FindingSeverity::Warning,
            FindingKind::TempResidue,
            Some(PathBuf::from(REPO_TMP_DIR).join(entry.file_name())),
            None,
            None,
            "temporary residue present",
        ));
    }
}

fn inspect_trash(repo: &Repository, sink: &mut FindingSink) {
    let dir = repo.root().join(REPO_TRASH_DIR);
    match fs::symlink_metadata(&dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::TrashResidue,
                Some(PathBuf::from(REPO_TRASH_DIR)),
                None,
                None,
                format!("failed to stat trash directory: {error}"),
            ));
            return;
        }
        Ok(_) => {}
    }
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) => {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::TrashResidue,
                Some(PathBuf::from(REPO_TRASH_DIR)),
                None,
                None,
                format!("failed to read trash directory: {error}"),
            ));
            return;
        }
    };
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        sink.push(finding(
            FindingSeverity::Warning,
            FindingKind::TrashResidue,
            Some(PathBuf::from(REPO_TRASH_DIR).join(entry.file_name())),
            None,
            None,
            "trash residue present",
        ));
    }
}

fn inspect_gc_journals(db: Option<&Database>, sink: &mut FindingSink) -> Vec<String> {
    let mut ids = Vec::new();
    let Some(db) = db else {
        return ids;
    };
    let Ok(read_txn) = db.begin_read() else {
        return ids;
    };
    let Ok(table) = read_txn.open_table(GC_JOURNALS) else {
        return ids;
    };
    let Ok(iter) = table.iter() else {
        sink.push(finding(
            FindingSeverity::Warning,
            FindingKind::GcJournal,
            Some(PathBuf::from(REPO_DB_FILE)),
            None,
            None,
            "failed to iterate GC journals",
        ));
        return ids;
    };
    for item in iter {
        let Ok((key, value)) = item else {
            sink.push(finding(
                FindingSeverity::Warning,
                FindingKind::GcJournal,
                Some(PathBuf::from(REPO_DB_FILE)),
                None,
                None,
                "failed to read GC journal entry",
            ));
            continue;
        };
        let gc_id = key.value().to_string();
        match GcJournal::decode(value.value()) {
            Ok(_) => {
                sink.push(finding(
                    FindingSeverity::Warning,
                    FindingKind::GcJournal,
                    None,
                    None,
                    None,
                    format!("GC journal {gc_id} requires recovery"),
                ));
                ids.push(gc_id);
            }
            Err(error) => sink.push(finding(
                FindingSeverity::Critical,
                FindingKind::GcJournal,
                None,
                None,
                None,
                format!("GC journal {gc_id} is corrupt: {error}"),
            )),
        }
    }
    ids.sort();
    ids
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn remove_proven_stale_leases(repo: &Repository, planned: &[String]) -> Result<Vec<String>> {
    let planned: BTreeSet<_> = planned.iter().cloned().collect();
    let db = repo.database()?;
    let write_txn = db.begin_write()?;
    let removed = {
        let mut table = write_txn.open_table(READER_LEASES)?;
        let mut stale_ids = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let lease = ReaderLease::decode(value.value())?;
            if planned.contains(key.value())
                && is_process_stale(lease.pid, &lease.process_start_token)
            {
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

fn rebuild_index_from_valid_manifests(repo: &Repository) -> Result<bool> {
    let mut sink = FindingSink::new(DEFAULT_MAX_REPAIR_FINDINGS);
    let mut inspected = Vec::new();
    let snapshot = inspect_snapshot(repo, &mut sink, &mut inspected)?;
    let desired = desired_backups(
        &snapshot.valid_file_manifests,
        &snapshot.valid_redb_manifests,
    );
    if desired == snapshot.valid_redb_manifests {
        return Ok(false);
    }

    let repair_id = random_hex(8)?;
    let tmp_path = repo
        .root()
        .join(REPO_TMP_DIR)
        .join(format!("index-repair-{repair_id}.redb"));
    if tmp_path.exists() {
        fs::remove_file(&tmp_path)?;
    }

    let new_db = Database::create(&tmp_path)?;
    {
        let write_txn = new_db.begin_write()?;
        {
            let mut backups = write_txn.open_table(TABLE_BACKUPS)?;
            let mut chunks = write_txn.open_table(TABLE_CHUNKS)?;
            let mut tombstones = write_txn.open_table(TOMBSTONES)?;
            let mut leases = write_txn.open_table(READER_LEASES)?;
            let mut journals = write_txn.open_table(GC_JOURNALS)?;
            let mut state = write_txn.open_table(GC_STATE)?;

            let mut refcounts: BTreeMap<String, u64> = BTreeMap::new();
            for (backup_id, encoded) in &desired {
                backups.insert(backup_id.as_str(), encoded.as_slice())?;
                let manifest = Manifest::decode(encoded)?;
                for chunk in &manifest.chunks {
                    *refcounts.entry(chunk.content_id.to_hex()).or_insert(0) += 1;
                }
            }
            for (content_id, count) in refcounts {
                chunks.insert(content_id.as_str(), count)?;
            }

            copy_matching_tombstones(repo, &desired, &mut tombstones)?;
            copy_live_leases(repo, &mut leases)?;
            copy_gc_tables(repo, &mut journals, &mut state)?;
        }
        write_txn.commit()?;
    }
    drop(new_db);
    File::open(&tmp_path)?.sync_all()?;
    sync_dir(&repo.root().join(REPO_TMP_DIR))?;

    let live = repo.root().join(REPO_DB_FILE);
    let previous = repo.root().join(format!("{REPO_DB_FILE}.previous"));
    let old = repo.take_database()?;
    drop(old);

    if previous.exists() {
        fs::remove_file(&previous)?;
    }
    if live.exists() {
        fs::rename(&live, &previous)?;
        sync_dir(repo.root())?;
    }
    fs::rename(&tmp_path, &live)?;
    File::open(&live)?.sync_all()?;
    sync_dir(repo.root())?;

    match Database::open(&live) {
        Ok(opened) => {
            repo.install_database(opened);
            Ok(true)
        }
        Err(error) => {
            if previous.exists() {
                let _ = fs::rename(&previous, &live);
                if let Ok(restored) = Database::open(&live) {
                    repo.install_database(restored);
                }
            }
            Err(Error::Database(format!(
                "failed to reopen repaired index: {error}"
            )))
        }
    }
}

fn copy_matching_tombstones(
    repo: &Repository,
    desired: &BTreeMap<String, Vec<u8>>,
    tombstones: &mut redb::Table<&str, u64>,
) -> Result<()> {
    let Ok(db) = repo.database() else {
        return Ok(());
    };
    let Ok(read_txn) = db.begin_read() else {
        return Ok(());
    };
    let Ok(table) = read_txn.open_table(TOMBSTONES) else {
        return Ok(());
    };
    for item in table.iter()? {
        let (key, value) = item?;
        if desired.contains_key(key.value()) {
            tombstones.insert(key.value(), value.value())?;
        }
    }
    Ok(())
}

fn copy_live_leases(repo: &Repository, leases: &mut redb::Table<&str, &[u8]>) -> Result<()> {
    let Ok(db) = repo.database() else {
        return Ok(());
    };
    let Ok(read_txn) = db.begin_read() else {
        return Ok(());
    };
    let Ok(table) = read_txn.open_table(READER_LEASES) else {
        return Ok(());
    };
    for item in table.iter()? {
        let (key, value) = item?;
        if let Ok(lease) = ReaderLease::decode(value.value()) {
            if !is_process_stale(lease.pid, &lease.process_start_token) {
                leases.insert(key.value(), value.value())?;
            }
        }
    }
    Ok(())
}

fn copy_gc_tables(
    repo: &Repository,
    journals: &mut redb::Table<&str, &[u8]>,
    state: &mut redb::Table<&str, u8>,
) -> Result<()> {
    let Ok(db) = repo.database() else {
        return Ok(());
    };
    let Ok(read_txn) = db.begin_read() else {
        return Ok(());
    };
    if let Ok(table) = read_txn.open_table(GC_JOURNALS) {
        for item in table.iter()? {
            let (key, value) = item?;
            journals.insert(key.value(), value.value())?;
        }
    }
    if let Ok(table) = read_txn.open_table(GC_STATE) {
        for item in table.iter()? {
            let (key, value) = item?;
            state.insert(key.value(), value.value())?;
        }
    }
    Ok(())
}

fn random_hex(bytes_len: usize) -> Result<String> {
    let mut buf = vec![0u8; bytes_len];
    getrandom::fill(&mut buf)
        .map_err(|error| Error::Database(format!("repair ID generation failed: {error}")))?;
    Ok(hex::encode(buf))
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}
