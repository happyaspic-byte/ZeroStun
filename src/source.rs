use std::fs::{File, Metadata};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct SourceFingerprint {
    pub len: u64,
    pub modified: Option<SystemTime>,
}

impl SourceFingerprint {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        }
    }

    pub fn matches(&self, other: &Self) -> bool {
        self.len == other.len && self.modified == other.modified
    }
}

pub struct FileSource {
    path: PathBuf,
    file: File,
    start: SourceFingerprint,
}

impl FileSource {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|source| Error::SourceRead {
            path: path.to_path_buf(),
            source,
        })?;
        let metadata = file.metadata().map_err(|source| Error::SourceRead {
            path: path.to_path_buf(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(Error::InvalidConfig(format!(
                "source must be a regular file in MVP: {}",
                path.display()
            )));
        }
        Ok(Self {
            path: path.to_path_buf(),
            file,
            start: SourceFingerprint::from_metadata(&metadata),
        })
    }

    pub fn file(&self) -> Result<File> {
        self.file.try_clone().map_err(|source| Error::SourceRead {
            path: self.path.clone(),
            source,
        })
    }

    pub fn len(&self) -> u64 {
        self.start.len
    }

    pub fn is_empty(&self) -> bool {
        self.start.len == 0
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn verify_unchanged(&self) -> Result<()> {
        let current = std::fs::metadata(&self.path).map_err(|source| Error::SourceRead {
            path: self.path.clone(),
            source,
        })?;
        let now = SourceFingerprint::from_metadata(&current);
        if !self.start.matches(&now) {
            return Err(Error::SourceChanged(self.path.clone()));
        }
        Ok(())
    }
}
