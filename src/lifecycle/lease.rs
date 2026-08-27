use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::ids::validate_backup_id;

pub const READER_LEASES: TableDefinition<&str, &[u8]> = TableDefinition::new("reader_leases");
pub const MAX_READER_LEASE_PAYLOAD_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReaderLease {
    pub lease_id: String,
    pub backup_id: String,
    pub pid: u32,
    pub process_start_token: String,
    pub acquired_unix_ms: u64,
}

impl ReaderLease {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| Error::Database(format!("failed to encode reader lease: {e}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_READER_LEASE_PAYLOAD_BYTES {
            return Err(Error::Database(format!(
                "reader lease payload exceeds maximum bounded size {} bytes",
                MAX_READER_LEASE_PAYLOAD_BYTES
            )));
        }
        serde_json::from_slice(bytes)
            .map_err(|e| Error::Database(format!("failed to decode reader lease: {e}")))
    }
}

pub struct ReaderLeaseGuard {
    db: Arc<Database>,
    lease_id: String,
}

impl std::fmt::Debug for ReaderLeaseGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReaderLeaseGuard")
            .field("lease_id", &self.lease_id)
            .finish_non_exhaustive()
    }
}

impl ReaderLeaseGuard {
    pub(crate) fn new(db: Arc<Database>, lease_id: String) -> Self {
        Self { db, lease_id }
    }

    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }
}

impl Drop for ReaderLeaseGuard {
    fn drop(&mut self) {
        let _ = (|| -> Result<()> {
            let write_txn = self.db.begin_write()?;
            {
                let mut table = write_txn.open_table(READER_LEASES)?;
                let _ = table.remove(self.lease_id.as_str())?;
            }
            write_txn.commit()?;
            Ok(())
        })();
    }
}

pub fn current_process_start_token(pid: u32) -> String {
    #[cfg(target_os = "linux")]
    {
        read_linux_process_start_token(pid).unwrap_or_else(|| "unknown-start-token".to_string())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        "unsupported-platform-start-token".to_string()
    }
}

#[cfg(target_os = "linux")]
pub fn read_linux_process_start_token(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close_paren = stat.rfind(')')?;
    let rest = stat.get(close_paren + 1..)?.trim_start();
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // In /proc/<pid>/stat:
    // field 1: pid
    // field 2: comm
    // fields after ')' start from field 3 (state = index 0).
    // starttime is field 22 (index 19 in fields).
    fields.get(19).map(|val| (*val).to_string())
}

pub fn is_process_stale(pid: u32, process_start_token: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        if pid == 0 {
            return true;
        }
        let proc_path = format!("/proc/{pid}");
        if !std::path::Path::new(&proc_path).exists() {
            return true;
        }
        match read_linux_process_start_token(pid) {
            Some(token) => token != process_start_token,
            None => true,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (pid, process_start_token);
        false
    }
}

pub fn insert_reader_lease(
    write_txn: &redb::WriteTransaction,
    backup_id: &str,
) -> Result<(ReaderLease, String)> {
    validate_backup_id(backup_id)?;
    let lease_id = format!("lease-{}-{}", backup_id, getrandom_hex(8));
    let pid = std::process::id();
    let process_start_token = current_process_start_token(pid);
    let lease = ReaderLease {
        lease_id: lease_id.clone(),
        backup_id: backup_id.to_string(),
        pid,
        process_start_token,
        acquired_unix_ms: unix_ms(),
    };
    let encoded = lease.encode()?;
    let mut table = write_txn.open_table(READER_LEASES)?;
    table.insert(lease_id.as_str(), encoded.as_slice())?;
    Ok((lease, lease_id))
}

pub fn read_active_reader_leases(read_txn: &redb::ReadTransaction) -> Result<Vec<ReaderLease>> {
    let table = read_txn.open_table(READER_LEASES)?;
    let mut leases = Vec::new();
    for item in table.iter()? {
        let (_, value) = item?;
        let lease = ReaderLease::decode(value.value())?;
        leases.push(lease);
    }
    leases.sort_by(|a, b| {
        a.acquired_unix_ms
            .cmp(&b.acquired_unix_ms)
            .then_with(|| a.lease_id.cmp(&b.lease_id))
    });
    Ok(leases)
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn getrandom_hex(bytes_len: usize) -> String {
    let mut buf = vec![0u8; bytes_len];
    let _ = getrandom::fill(&mut buf);
    hex::encode(buf)
}
