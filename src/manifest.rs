use serde::{Deserialize, Serialize};

use crate::codec::CompressionCodec;
use crate::error::{Error, Result};
use crate::ids::{ContentId, RootHash};

pub const MANIFEST_FORMAT_VERSION: u32 = 1;
pub const MANIFEST_MAGIC: &[u8; 8] = b"ZSTNMFST";
pub const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_MANIFEST_CHUNKS: usize = 1_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkDescriptor {
    pub index: u64,
    pub logical_offset: u64,
    pub original_length: u64,
    pub stored_length: u64,
    pub codec: CompressionCodec,
    pub content_id: ContentId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub format_version: u32,
    pub backup_id: String,
    pub created_unix_ms: u64,
    pub source_path: String,
    pub total_logical_bytes: u64,
    pub fastcdc_min: u32,
    pub fastcdc_avg: u32,
    pub fastcdc_max: u32,
    pub chunks: Vec<ChunkDescriptor>,
    pub root_hash: RootHash,
}

impl Manifest {
    pub fn new(
        backup_id: impl Into<String>,
        total_logical_bytes: u64,
        fastcdc_min: u32,
        fastcdc_avg: u32,
        fastcdc_max: u32,
    ) -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            backup_id: backup_id.into(),
            created_unix_ms: unix_ms(),
            source_path: String::new(),
            total_logical_bytes,
            fastcdc_min,
            fastcdc_avg,
            fastcdc_max,
            chunks: Vec::new(),
            root_hash: RootHash::from_bytes([0u8; 32]),
        }
    }

    pub fn add_chunk(&mut self, chunk: ChunkDescriptor) {
        self.chunks.push(chunk);
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_vec(self)
            .map_err(|e| Error::ManifestCorrupt(format!("encode failed: {e}")))?;
        let mut out = Vec::with_capacity(12 + json.len());
        out.extend_from_slice(MANIFEST_MAGIC);
        out.extend_from_slice(&self.format_version.to_le_bytes());
        out.extend_from_slice(&json);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::ManifestCorrupt(
                "manifest exceeds bounded maximum size".to_string(),
            ));
        }
        if bytes.len() < 12 {
            return Err(Error::ManifestCorrupt("manifest too short".to_string()));
        }
        if &bytes[0..8] != MANIFEST_MAGIC {
            return Err(Error::ManifestCorrupt("invalid manifest magic".to_string()));
        }
        let version = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .map_err(|_| Error::ManifestCorrupt("invalid version bytes".to_string()))?,
        );
        if version != MANIFEST_FORMAT_VERSION {
            return Err(Error::UnsupportedRepositoryVersion {
                found: version,
                supported: MANIFEST_FORMAT_VERSION,
            });
        }
        let manifest: Self = serde_json::from_slice(&bytes[12..])
            .map_err(|e| Error::ManifestCorrupt(format!("json decode failed: {e}")))?;
        if manifest.chunks.len() > MAX_MANIFEST_CHUNKS {
            return Err(Error::ManifestCorrupt(
                "manifest chunk count exceeds bounded maximum".to_string(),
            ));
        }
        Ok(manifest)
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
