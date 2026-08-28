use std::path::PathBuf;

use redb::TableDefinition;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const GC_JOURNALS: TableDefinition<&str, &[u8]> = TableDefinition::new("gc_journals");
pub const MAX_GC_JOURNAL_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_GC_CHUNKS: usize = 1_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkMove {
    pub content_id: String,
    pub source: PathBuf,
    pub trash: PathBuf,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcPlan {
    pub gc_id: String,
    pub live_chunks: u64,
    pub reclaim_chunks: Vec<ChunkMove>,
    pub reclaim_bytes: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum GcPhase {
    Planned,
    Moving,
    Committed,
    Deleting,
    Complete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcJournal {
    pub plan: GcPlan,
    pub phase: GcPhase,
    pub moved: Vec<String>,
}

impl GcJournal {
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.plan.reclaim_chunks.len() > MAX_GC_CHUNKS || self.moved.len() > MAX_GC_CHUNKS {
            return Err(Error::GarbageCollection(
                "GC journal chunk count exceeds bounded maximum".to_string(),
            ));
        }
        let bytes = serde_json::to_vec(self).map_err(|error| {
            Error::GarbageCollection(format!("GC journal encode failed: {error}"))
        })?;
        if bytes.len() > MAX_GC_JOURNAL_BYTES {
            return Err(Error::GarbageCollection(
                "GC journal exceeds bounded maximum size".to_string(),
            ));
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_GC_JOURNAL_BYTES {
            return Err(Error::GarbageCollection(
                "GC journal exceeds bounded maximum size".to_string(),
            ));
        }
        let journal: Self = serde_json::from_slice(bytes).map_err(|error| {
            Error::GarbageCollection(format!("GC journal decode failed: {error}"))
        })?;
        if journal.plan.reclaim_chunks.len() > MAX_GC_CHUNKS || journal.moved.len() > MAX_GC_CHUNKS
        {
            return Err(Error::GarbageCollection(
                "GC journal chunk count exceeds bounded maximum".to_string(),
            ));
        }
        Ok(journal)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcResult {
    pub gc_id: String,
    pub reclaimed_chunks: u64,
    pub reclaimed_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcRecoveryResult {
    pub gc_id: String,
    pub phase: GcPhase,
}
