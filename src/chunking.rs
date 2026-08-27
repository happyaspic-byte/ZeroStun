use std::io::Read;

use fastcdc::v2020::{
    StreamCDC, AVERAGE_MAX, AVERAGE_MIN, MAXIMUM_MAX, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN,
};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy)]
pub struct ChunkParams {
    pub min: usize,
    pub avg: usize,
    pub max: usize,
}

impl ChunkParams {
    pub fn new(min: usize, avg: usize, max: usize) -> Result<Self> {
        if !(MINIMUM_MIN..=MINIMUM_MAX).contains(&min) {
            return Err(Error::InvalidConfig(format!(
                "min chunk {min} outside FastCDC range {MINIMUM_MIN}..={MINIMUM_MAX}"
            )));
        }
        if !(AVERAGE_MIN..=AVERAGE_MAX).contains(&avg) {
            return Err(Error::InvalidConfig(format!(
                "avg chunk {avg} outside FastCDC range {AVERAGE_MIN}..={AVERAGE_MAX}"
            )));
        }
        if !(MAXIMUM_MIN..=MAXIMUM_MAX).contains(&max) {
            return Err(Error::InvalidConfig(format!(
                "max chunk {max} outside FastCDC range {MAXIMUM_MIN}..={MAXIMUM_MAX}"
            )));
        }
        if !(min <= avg && avg <= max) {
            return Err(Error::InvalidConfig(
                "chunk sizes must satisfy min <= avg <= max".to_string(),
            ));
        }
        Ok(Self { min, avg, max })
    }

    pub fn defaults() -> Self {
        Self {
            min: 8 * 1024,
            avg: 64 * 1024,
            max: 256 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub offset: u64,
    pub data: Vec<u8>,
}

pub fn stream_chunks<R, F>(reader: R, params: ChunkParams, mut on_chunk: F) -> Result<()>
where
    R: Read,
    F: FnMut(Chunk) -> Result<()>,
{
    let cdc = StreamCDC::new(reader, params.min, params.avg, params.max);
    for item in cdc {
        let data = item.map_err(|e| Error::ChunkEncode(e.to_string()))?;
        on_chunk(Chunk {
            offset: data.offset,
            data: data.data,
        })?;
    }
    Ok(())
}

pub fn chunk_bytes(bytes: &[u8], params: ChunkParams) -> Vec<Chunk> {
    let cdc = fastcdc::v2020::FastCDC::new(bytes, params.min, params.avg, params.max);
    cdc.map(|c| Chunk {
        offset: c.offset as u64,
        data: bytes[c.offset..c.offset + c.length].to_vec(),
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_inverted_sizes() {
        let err = ChunkParams::new(4096, 1024, 8192).expect_err("should reject");
        match err {
            Error::InvalidConfig(_) => {}
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn deterministic_chunk_boundaries() {
        let params = ChunkParams::new(1024, 4096, 16384).expect("valid params");
        let mut data = Vec::new();
        for i in 0..20000u32 {
            data.extend_from_slice(&i.to_le_bytes());
        }
        let a = chunk_bytes(&data, params);
        let b = chunk_bytes(&data, params);
        let a_lens: Vec<usize> = a.iter().map(|c| c.data.len()).collect();
        let b_lens: Vec<usize> = b.iter().map(|c| c.data.len()).collect();
        assert_eq!(a_lens, b_lens);
        assert_eq!(
            a.iter().map(|c| c.data.len() as u64).sum::<u64>(),
            data.len() as u64
        );
    }
}
