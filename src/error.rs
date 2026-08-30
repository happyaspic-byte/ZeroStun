use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("unsupported repository format version {found} (supported: {supported})")]
    UnsupportedRepositoryVersion { found: u32, supported: u32 },

    #[error("repository is not initialized: {0}")]
    RepositoryNotInitialized(PathBuf),

    #[error("path is not a ZeroStun repository: {0}")]
    NotARepository(PathBuf),

    #[error("repository is locked by another writer: {0}")]
    RepositoryLocked(PathBuf),

    #[error("source file changed during backup: {0}")]
    SourceChanged(PathBuf),

    #[error("failed to read source {path}: {source}")]
    SourceRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("chunk encode failure: {0}")]
    ChunkEncode(String),

    #[error("compression failure: {0}")]
    Compression(String),

    #[error("decompression failure: {0}")]
    Decompression(String),

    #[error("repository write failure: {0}")]
    RepositoryWrite(String),

    #[error("manifest commit failure: {0}")]
    ManifestCommit(String),

    #[error("backup not found: {0}")]
    BackupNotFound(String),

    #[error("backup already exists: {0}")]
    BackupAlreadyExists(String),

    #[error("backup has been deleted: {0}")]
    BackupDeleted(String),

    #[error("chunk {content_id} is missing from the repository")]
    ChunkMissing { content_id: String },

    #[error("chunk {content_id} is corrupt: {reason}")]
    ChunkCorrupt { content_id: String, reason: String },

    #[error("manifest is corrupt: {0}")]
    ManifestCorrupt(String),

    #[error("root hash mismatch for backup {backup_id}")]
    RootHashMismatch { backup_id: String },

    #[error("restore target already exists: {0}")]
    RestoreTargetExists(PathBuf),

    #[error("failed to write restore output {path}: {source}")]
    OutputWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("operation cancelled")]
    Cancelled,

    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),

    #[error("path traversal rejected: {0}")]
    PathTraversal(String),

    #[error("source path {source_path} is inside the repository {repo_path}")]
    SourceInsideRepository {
        source_path: PathBuf,
        repo_path: PathBuf,
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Database(String),

    #[error("garbage collection error: {0}")]
    GarbageCollection(String),

    #[error("lifecycle plan is stale: {0}")]
    StalePlan(String),

    #[error("active reader: {0}")]
    ActiveReader(String),

    #[error("critical repair finding: {0}")]
    CriticalRepair(String),

    #[error("snapshot error: {0}")]
    Snapshot(String),
}

impl From<redb::Error> for Error {
    fn from(value: redb::Error) -> Self {
        Error::Database(value.to_string())
    }
}

impl From<redb::DatabaseError> for Error {
    fn from(value: redb::DatabaseError) -> Self {
        Error::Database(value.to_string())
    }
}

impl From<redb::TransactionError> for Error {
    fn from(value: redb::TransactionError) -> Self {
        Error::Database(value.to_string())
    }
}

impl From<redb::TableError> for Error {
    fn from(value: redb::TableError) -> Self {
        Error::Database(value.to_string())
    }
}

impl From<redb::StorageError> for Error {
    fn from(value: redb::StorageError) -> Self {
        Error::Database(value.to_string())
    }
}

impl From<redb::CommitError> for Error {
    fn from(value: redb::CommitError) -> Self {
        Error::Database(value.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    Success = 0,
    Generic = 1,
    InvalidConfig = 2,
    Repository = 3,
    Locked = 4,
    SourceChanged = 5,
    Integrity = 6,
    TargetExists = 7,
    Cancelled = 130,
}

impl Error {
    pub fn exit_code(&self) -> ExitCode {
        match self {
            Error::InvalidConfig(_) | Error::InvalidIdentifier(_) | Error::PathTraversal(_) => {
                ExitCode::InvalidConfig
            }
            Error::RepositoryNotInitialized(_)
            | Error::NotARepository(_)
            | Error::UnsupportedRepositoryVersion { .. }
            | Error::BackupNotFound(_)
            | Error::BackupAlreadyExists(_)
            | Error::BackupDeleted(_) => ExitCode::Repository,
            Error::RepositoryLocked(_) => ExitCode::Locked,
            Error::SourceChanged(_) => ExitCode::SourceChanged,
            Error::ChunkMissing { .. }
            | Error::ChunkCorrupt { .. }
            | Error::ManifestCorrupt(_)
            | Error::RootHashMismatch { .. } => ExitCode::Integrity,
            Error::RestoreTargetExists(_) => ExitCode::TargetExists,
            Error::Cancelled => ExitCode::Cancelled,
            Error::StalePlan(_) => ExitCode::Repository,
            Error::ActiveReader(_) => ExitCode::Locked,
            Error::CriticalRepair(_) => ExitCode::Integrity,
            Error::GarbageCollection(message) => {
                if message.contains("active reader") {
                    ExitCode::Locked
                } else if message.contains("stale") {
                    ExitCode::Repository
                } else {
                    ExitCode::Generic
                }
            }
            _ => ExitCode::Generic,
        }
    }
}
