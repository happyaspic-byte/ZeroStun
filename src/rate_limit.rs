use std::time::Duration;

use tokio::time::sleep;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct TokenBucket {
    bytes_per_sec: Option<u64>,
    iops: Option<u64>,
    byte_tokens: f64,
    iop_tokens: f64,
    last: std::time::Instant,
}

impl TokenBucket {
    pub fn new(bytes_per_sec: Option<u64>, iops: Option<u64>) -> Result<Self> {
        if let Some(bps) = bytes_per_sec {
            if bps == 0 {
                return Err(Error::InvalidConfig(
                    "bytes_per_sec must be greater than 0 when set".to_string(),
                ));
            }
        }
        if let Some(i) = iops {
            if i == 0 {
                return Err(Error::InvalidConfig(
                    "iops must be greater than 0 when set".to_string(),
                ));
            }
        }
        Ok(Self {
            bytes_per_sec,
            iops,
            byte_tokens: bytes_per_sec.map(|v| v as f64).unwrap_or(0.0),
            iop_tokens: iops.map(|v| v as f64).unwrap_or(0.0),
            last: std::time::Instant::now(),
        })
    }

    pub fn unlimited() -> Self {
        Self {
            bytes_per_sec: None,
            iops: None,
            byte_tokens: 0.0,
            iop_tokens: 0.0,
            last: std::time::Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = std::time::Instant::now();
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        if let Some(bps) = self.bytes_per_sec {
            let max_burst = bps as f64;
            self.byte_tokens = (self.byte_tokens + dt * bps as f64).min(max_burst);
        }
        if let Some(iops) = self.iops {
            let max_burst = iops as f64;
            self.iop_tokens = (self.iop_tokens + dt * iops as f64).min(max_burst);
        }
    }

    pub fn consume_blocking(&mut self, nbytes: u64) {
        if self.bytes_per_sec.is_none() && self.iops.is_none() {
            return;
        }
        loop {
            self.refill();
            let bytes_ready = match self.bytes_per_sec {
                None => true,
                Some(_) => self.byte_tokens >= 0.0,
            };
            let iops_ready = match self.iops {
                None => true,
                Some(_) => self.iop_tokens >= 0.0,
            };

            if bytes_ready && iops_ready {
                if let Some(bps) = self.bytes_per_sec {
                    self.byte_tokens -= nbytes as f64;
                    if self.byte_tokens < 0.0 {
                        let debt_secs = (-self.byte_tokens) / (bps as f64);
                        std::thread::sleep(Duration::from_secs_f64(debt_secs));
                    }
                }
                if let Some(iops) = self.iops {
                    self.iop_tokens -= 1.0;
                    if self.iop_tokens < 0.0 {
                        let debt_secs = (-self.iop_tokens) / (iops as f64);
                        std::thread::sleep(Duration::from_secs_f64(debt_secs));
                    }
                }
                return;
            }

            let mut wait_secs = 0.005_f64;
            if let Some(bps) = self.bytes_per_sec {
                if self.byte_tokens < 0.0 {
                    wait_secs = wait_secs.max((-self.byte_tokens) / (bps as f64));
                }
            }
            if let Some(iops) = self.iops {
                if self.iop_tokens < 0.0 {
                    wait_secs = wait_secs.max((-self.iop_tokens) / (iops as f64));
                }
            }
            std::thread::sleep(Duration::from_secs_f64(wait_secs.min(0.5)));
        }
    }

    pub async fn consume(&mut self, nbytes: u64) {
        if self.bytes_per_sec.is_none() && self.iops.is_none() {
            return;
        }
        loop {
            self.refill();
            let bytes_ready = match self.bytes_per_sec {
                None => true,
                Some(_) => self.byte_tokens >= 0.0,
            };
            let iops_ready = match self.iops {
                None => true,
                Some(_) => self.iop_tokens >= 0.0,
            };

            if bytes_ready && iops_ready {
                if let Some(bps) = self.bytes_per_sec {
                    self.byte_tokens -= nbytes as f64;
                    if self.byte_tokens < 0.0 {
                        let debt_secs = (-self.byte_tokens) / (bps as f64);
                        sleep(Duration::from_secs_f64(debt_secs)).await;
                    }
                }
                if let Some(iops) = self.iops {
                    self.iop_tokens -= 1.0;
                    if self.iop_tokens < 0.0 {
                        let debt_secs = (-self.iop_tokens) / (iops as f64);
                        sleep(Duration::from_secs_f64(debt_secs)).await;
                    }
                }
                return;
            }

            let mut wait_secs = 0.005_f64;
            if let Some(bps) = self.bytes_per_sec {
                if self.byte_tokens < 0.0 {
                    wait_secs = wait_secs.max((-self.byte_tokens) / (bps as f64));
                }
            }
            if let Some(iops) = self.iops {
                if self.iop_tokens < 0.0 {
                    wait_secs = wait_secs.max((-self.iop_tokens) / (iops as f64));
                }
            }
            sleep(Duration::from_secs_f64(wait_secs.min(0.5))).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_limits() {
        assert!(TokenBucket::new(Some(0), None).is_err());
        assert!(TokenBucket::new(None, Some(0)).is_err());
    }

    #[tokio::test]
    async fn respects_bytes_per_sec_lower_bound() {
        let mut bucket = TokenBucket::new(Some(100_000), None).expect("valid limiter");
        let start = std::time::Instant::now();
        bucket.consume(100_000).await;
        bucket.consume(100_000).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(800),
            "elapsed too short: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(4),
            "elapsed too long: {elapsed:?}"
        );
    }
}
