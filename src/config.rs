use crate::chunking::ChunkParams;
use crate::codec::CompressionCodec;
use crate::error::{Error, Result};
use crate::telemetry::ProgressMode;

#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub min_chunk: usize,
    pub avg_chunk: usize,
    pub max_chunk: usize,
    pub codec: CompressionCodec,
    pub read_bytes_per_sec: Option<u64>,
    pub read_iops: Option<u64>,
    pub write_bytes_per_sec: Option<u64>,
    pub workers: usize,
    pub queue_depth: usize,
    pub progress: ProgressMode,
}

impl Default for BackupConfig {
    fn default() -> Self {
        let chunks = ChunkParams::defaults();
        Self {
            min_chunk: chunks.min,
            avg_chunk: chunks.avg,
            max_chunk: chunks.max,
            codec: CompressionCodec::Zstd { level: 3 },
            read_bytes_per_sec: None,
            read_iops: None,
            write_bytes_per_sec: None,
            workers: std::thread::available_parallelism()
                .map(|n| n.get().min(4))
                .unwrap_or(1),
            queue_depth: 8,
            progress: ProgressMode::Auto,
        }
    }
}

impl BackupConfig {
    pub fn validate(&self) -> Result<ChunkParams> {
        let params = ChunkParams::new(self.min_chunk, self.avg_chunk, self.max_chunk)?;
        if self.workers == 0 {
            return Err(Error::InvalidConfig(
                "worker count must be greater than zero".to_string(),
            ));
        }
        if self.queue_depth == 0 || self.queue_depth > 4096 {
            return Err(Error::InvalidConfig(
                "queue depth must be between 1 and 4096".to_string(),
            ));
        }
        if self.max_chunk.saturating_mul(self.queue_depth) > 1024 * 1024 * 1024 {
            return Err(Error::InvalidConfig(
                "max_chunk * queue_depth exceeds the 1 GiB in-flight payload safety limit"
                    .to_string(),
            ));
        }
        if matches!(self.read_bytes_per_sec, Some(0))
            || matches!(self.write_bytes_per_sec, Some(0))
            || matches!(self.read_iops, Some(0))
        {
            return Err(Error::InvalidConfig(
                "rate limits must be greater than zero when configured".to_string(),
            ));
        }
        Ok(params)
    }

    pub fn max_in_flight_payload_bytes(&self) -> usize {
        self.max_chunk
            .saturating_mul(self.queue_depth.saturating_add(self.workers))
    }
}

pub fn parse_byte_size(input: &str) -> Result<u64> {
    let normalized = input.trim().to_ascii_lowercase().replace('_', "");
    if normalized.is_empty() {
        return Err(Error::InvalidConfig("empty byte size".to_string()));
    }
    let split = normalized
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(normalized.len());
    let (number, suffix) = normalized.split_at(split);
    let value: f64 = number
        .parse()
        .map_err(|_| Error::InvalidConfig(format!("invalid byte size '{input}'")))?;
    if !value.is_finite() || value < 0.0 {
        return Err(Error::InvalidConfig(format!("invalid byte size '{input}'")));
    }
    let multiplier = match suffix.trim() {
        "" | "b" => 1.0,
        "k" | "kb" => 1_000.0,
        "m" | "mb" => 1_000_000.0,
        "g" | "gb" => 1_000_000_000.0,
        "ki" | "kib" => 1024.0,
        "mi" | "mib" => 1024.0 * 1024.0,
        "gi" | "gib" => 1024.0 * 1024.0 * 1024.0,
        _ => {
            return Err(Error::InvalidConfig(format!(
                "unknown byte-size suffix in '{input}'"
            )))
        }
    };
    let bytes = value * multiplier;
    if bytes > u64::MAX as f64 {
        return Err(Error::InvalidConfig(format!(
            "byte size '{input}' overflows u64"
        )));
    }
    Ok(bytes.round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_human_byte_sizes() {
        assert_eq!(
            parse_byte_size("64KiB").expect("64KiB should parse"),
            65_536
        );
        assert_eq!(
            parse_byte_size("10 MB").expect("10 MB should parse"),
            10_000_000
        );
        assert_eq!(
            parse_byte_size("1.5MiB").expect("1.5MiB should parse"),
            1_572_864
        );
    }

    #[test]
    fn rejects_unbounded_memory_configuration() {
        let cfg = BackupConfig {
            max_chunk: 16 * 1024 * 1024,
            queue_depth: 100,
            ..BackupConfig::default()
        };
        assert!(cfg.validate().is_err());
    }
}
