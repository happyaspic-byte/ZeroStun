use std::io::{Read, Write};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum CompressionCodec {
    None,
    Zstd { level: i32 },
    Lz4,
}

impl CompressionCodec {
    pub fn type_tag(&self) -> u8 {
        match self {
            CompressionCodec::None => 0,
            CompressionCodec::Zstd { .. } => 1,
            CompressionCodec::Lz4 => 2,
        }
    }

    pub fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(CompressionCodec::None),
            1 => Ok(CompressionCodec::Zstd { level: 3 }),
            2 => Ok(CompressionCodec::Lz4),
            other => Err(Error::Decompression(format!(
                "unknown stored codec tag {other}"
            ))),
        }
    }
}

pub const STORED_CHUNK_MAGIC: &[u8; 8] = b"ZSTNCHNK";
pub const STORED_CHUNK_VERSION: u32 = 1;
pub const STORED_CHUNK_HEADER_LEN: usize = 24;
pub const MAX_ORIGINAL_CHUNK_BYTES: u64 = 16_777_216;

#[derive(Debug, Clone)]
pub struct StoredChunk {
    pub codec: CompressionCodec,
    pub payload: Vec<u8>,
}

impl StoredChunk {
    pub fn encode(codec: CompressionCodec, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(STORED_CHUNK_HEADER_LEN + payload.len());
        out.extend_from_slice(STORED_CHUNK_MAGIC);
        out.extend_from_slice(&STORED_CHUNK_VERSION.to_le_bytes());
        out.push(codec.type_tag());
        out.extend_from_slice(&[0u8; 3]);
        out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < STORED_CHUNK_HEADER_LEN {
            return Err(Error::Decompression(
                "stored chunk shorter than header".to_string(),
            ));
        }
        if &bytes[0..8] != STORED_CHUNK_MAGIC {
            return Err(Error::Decompression(
                "invalid stored chunk magic".to_string(),
            ));
        }
        let version = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .map_err(|_| Error::Decompression("invalid version bytes".to_string()))?,
        );
        if version != STORED_CHUNK_VERSION {
            return Err(Error::UnsupportedRepositoryVersion {
                found: version,
                supported: STORED_CHUNK_VERSION,
            });
        }
        let codec = CompressionCodec::from_tag(bytes[12])?;
        let payload_len = u64::from_le_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| Error::Decompression("invalid payload length bytes".to_string()))?,
        ) as usize;
        if bytes.len() != STORED_CHUNK_HEADER_LEN + payload_len {
            return Err(Error::Decompression(format!(
                "stored chunk length mismatch: header says {payload_len}, file has {}",
                bytes.len().saturating_sub(STORED_CHUNK_HEADER_LEN)
            )));
        }
        Ok(Self {
            codec,
            payload: bytes[STORED_CHUNK_HEADER_LEN..].to_vec(),
        })
    }
}

impl FromStr for CompressionCodec {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "none" => Ok(CompressionCodec::None),
            "zstd" => Ok(CompressionCodec::Zstd { level: 3 }),
            "lz4" => Ok(CompressionCodec::Lz4),
            other => {
                if let Some(rest) = other.strip_prefix("zstd:") {
                    let lvl: i32 = rest.parse().map_err(|_| {
                        Error::InvalidConfig(format!("invalid zstd level in '{s}'"))
                    })?;
                    if !(1..=22).contains(&lvl) {
                        return Err(Error::InvalidConfig(
                            "zstd level must be between 1 and 22".to_string(),
                        ));
                    }
                    Ok(CompressionCodec::Zstd { level: lvl })
                } else {
                    Err(Error::InvalidConfig(format!(
                        "unknown compression codec '{s}' (supported: none, zstd, zstd:N, lz4)"
                    )))
                }
            }
        }
    }
}

pub struct Compressor;

impl Compressor {
    pub fn compress(codec: CompressionCodec, raw: &[u8]) -> Result<Vec<u8>> {
        match codec {
            CompressionCodec::None => Ok(raw.to_vec()),
            CompressionCodec::Zstd { level } => {
                let mut encoder = zstd::stream::Encoder::new(Vec::new(), level)
                    .map_err(|e| Error::Compression(e.to_string()))?;
                encoder
                    .write_all(raw)
                    .map_err(|e| Error::Compression(e.to_string()))?;
                encoder
                    .finish()
                    .map_err(|e| Error::Compression(e.to_string()))
            }
            CompressionCodec::Lz4 => {
                let mut output = Vec::new();
                let mut frame_writer = lz4_flex::frame::FrameEncoder::new(&mut output);
                frame_writer
                    .write_all(raw)
                    .map_err(|e| Error::Compression(e.to_string()))?;
                frame_writer
                    .finish()
                    .map_err(|e| Error::Compression(e.to_string()))?;
                Ok(output)
            }
        }
    }

    pub fn decompress(
        codec: CompressionCodec,
        compressed: &[u8],
        expected_len: usize,
    ) -> Result<Vec<u8>> {
        match codec {
            CompressionCodec::None => {
                if compressed.len() != expected_len {
                    return Err(Error::Decompression(format!(
                        "uncompressed size mismatch: got {}, expected {}",
                        compressed.len(),
                        expected_len
                    )));
                }
                Ok(compressed.to_vec())
            }
            CompressionCodec::Zstd { .. } => {
                let mut decoder = zstd::stream::Decoder::new(compressed)
                    .map_err(|e| Error::Decompression(e.to_string()))?;
                let mut buf = vec![0u8; expected_len];
                decoder
                    .read_exact(&mut buf)
                    .map_err(|e| Error::Decompression(e.to_string()))?;

                let mut extra = [0u8; 1];
                let n = decoder
                    .read(&mut extra)
                    .map_err(|e| Error::Decompression(e.to_string()))?;
                if n != 0 {
                    return Err(Error::Decompression(
                        "extra trailing decompressed bytes beyond expected length".to_string(),
                    ));
                }
                Ok(buf)
            }
            CompressionCodec::Lz4 => {
                let mut frame_reader = lz4_flex::frame::FrameDecoder::new(compressed);
                let mut buf = vec![0u8; expected_len];
                frame_reader
                    .read_exact(&mut buf)
                    .map_err(|e| Error::Decompression(e.to_string()))?;

                let mut extra = [0u8; 1];
                let n = frame_reader
                    .read(&mut extra)
                    .map_err(|e| Error::Decompression(e.to_string()))?;
                if n != 0 {
                    return Err(Error::Decompression(
                        "extra trailing decompressed bytes beyond expected length".to_string(),
                    ));
                }
                Ok(buf)
            }
        }
    }
}
