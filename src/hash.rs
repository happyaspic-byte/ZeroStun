use blake3::Hasher;

use crate::ids::{ContentId, RootHash};
use crate::manifest::Manifest;

pub const DOMAIN_CHUNK_CONTENT: &[u8] = b"ZeroStun/ChunkContent/v1";
pub const DOMAIN_ROOT_MANIFEST: &[u8] = b"ZeroStun/RootManifest/v1";

pub fn content_id_from_bytes(raw: &[u8]) -> ContentId {
    let mut hasher = Hasher::new();
    hasher.update(DOMAIN_CHUNK_CONTENT);
    hasher.update(&(raw.len() as u64).to_le_bytes());
    hasher.update(raw);
    let output = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(output.as_bytes());
    ContentId::from_bytes(bytes)
}

pub fn root_hash_from_manifest(manifest: &Manifest) -> RootHash {
    let mut hasher = Hasher::new();
    hasher.update(DOMAIN_ROOT_MANIFEST);
    hasher.update(&manifest.format_version.to_le_bytes());
    hasher.update(&manifest.total_logical_bytes.to_le_bytes());
    hasher.update(&(manifest.chunks.len() as u64).to_le_bytes());
    hasher.update(&manifest.fastcdc_min.to_le_bytes());
    hasher.update(&manifest.fastcdc_avg.to_le_bytes());
    hasher.update(&manifest.fastcdc_max.to_le_bytes());

    for chunk in &manifest.chunks {
        hasher.update(&chunk.index.to_le_bytes());
        hasher.update(&chunk.logical_offset.to_le_bytes());
        hasher.update(&chunk.original_length.to_le_bytes());
        hasher.update(&chunk.stored_length.to_le_bytes());
        hasher.update(chunk.content_id.as_bytes());
        let codec_tag = chunk.codec.type_tag();
        hasher.update(&[codec_tag]);
    }

    let output = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(output.as_bytes());
    RootHash::from_bytes(bytes)
}
