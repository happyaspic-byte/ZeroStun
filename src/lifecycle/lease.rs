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

#[cfg(target_os = "linux")]
#[derive(Debug, PartialEq, Eq)]
enum ProcessIdentityStatus {
    Dead,
    Alive(String),
    Unknown,
}

pub fn current_process_start_token(pid: u32) -> Result<String> {
    #[cfg(target_os = "linux")]
    {
        match probe_linux_process_identity(pid) {
            ProcessIdentityStatus::Alive(token) => Ok(token),
            ProcessIdentityStatus::Dead => Err(Error::Database(format!(
                "current process {pid} disappeared while acquiring reader lease"
            ))),
            ProcessIdentityStatus::Unknown => Err(Error::Database(format!(
                "unable to determine process identity for pid {pid}"
            ))),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        Ok("unsupported-platform-start-token".to_string())
    }
}

#[cfg(target_os = "linux")]
fn parse_linux_process_start_token(stat: &str) -> Option<String> {
    let close_paren = stat.rfind(')')?;
    let rest = stat.get(close_paren + 1..)?.trim_start();
    let fields: Vec<&str> = rest.split_whitespace().collect();
    fields.get(19).map(|value| (*value).to_string())
}

#[cfg(target_os = "linux")]
fn probe_linux_process_identity(pid: u32) -> ProcessIdentityStatus {
    probe_linux_process_identity_with(pid, |path| std::fs::read_to_string(path))
}

#[cfg(target_os = "linux")]
fn probe_linux_process_identity_with(
    pid: u32,
    read_stat: impl FnOnce(&std::path::Path) -> std::io::Result<String>,
) -> ProcessIdentityStatus {
    let path = std::path::PathBuf::from(format!("/proc/{pid}/stat"));
    match read_stat(&path) {
        Ok(stat) => parse_linux_process_start_token(&stat)
            .map(ProcessIdentityStatus::Alive)
            .unwrap_or(ProcessIdentityStatus::Unknown),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ProcessIdentityStatus::Dead,
        Err(_) => ProcessIdentityStatus::Unknown,
    }
}

#[cfg(target_os = "linux")]
fn process_identity_is_stale(status: &ProcessIdentityStatus, stored_token: &str) -> bool {
    matches!(status, ProcessIdentityStatus::Dead)
        || matches!(status, ProcessIdentityStatus::Alive(current) if current != stored_token)
}

pub fn is_process_stale(pid: u32, process_start_token: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        process_identity_is_stale(&probe_linux_process_identity(pid), process_start_token)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (pid, process_start_token);
        false
    }
}

const MAX_LEASE_ID_ATTEMPTS: usize = 8;

pub fn insert_reader_lease(
    write_txn: &redb::WriteTransaction,
    backup_id: &str,
) -> Result<(ReaderLease, String)> {
    let pid = std::process::id();
    let process_start_token = current_process_start_token(pid)?;
    insert_reader_lease_with(write_txn, backup_id, pid, process_start_token, || {
        Ok(format!("lease-{backup_id}-{}", getrandom_hex(8)?))
    })
}

fn insert_reader_lease_with(
    write_txn: &redb::WriteTransaction,
    backup_id: &str,
    pid: u32,
    process_start_token: String,
    mut generate_id: impl FnMut() -> Result<String>,
) -> Result<(ReaderLease, String)> {
    validate_backup_id(backup_id)?;
    let mut table = write_txn.open_table(READER_LEASES)?;
    for _ in 0..MAX_LEASE_ID_ATTEMPTS {
        let lease_id = generate_id()?;
        if table.get(lease_id.as_str())?.is_some() {
            continue;
        }
        let lease = ReaderLease {
            lease_id: lease_id.clone(),
            backup_id: backup_id.to_string(),
            pid,
            process_start_token: process_start_token.clone(),
            acquired_unix_ms: unix_ms(),
        };
        let encoded = lease.encode()?;
        table.insert(lease_id.as_str(), encoded.as_slice())?;
        return Ok((lease, lease_id));
    }
    Err(Error::Database(format!(
        "failed to allocate unique reader lease ID after {MAX_LEASE_ID_ATTEMPTS} attempts"
    )))
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

fn getrandom_hex(bytes_len: usize) -> Result<String> {
    let mut buf = vec![0u8; bytes_len];
    getrandom::fill(&mut buf)
        .map_err(|error| Error::Database(format!("reader lease ID generation failed: {error}")))?;
    Ok(hex::encode(buf))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io;

    use redb::{Database, ReadableDatabase};

    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn unknown_process_status_is_preserved() {
        let status = probe_linux_process_identity_with(123, |_| {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied"))
        });
        assert_eq!(status, ProcessIdentityStatus::Unknown);
        assert!(!process_identity_is_stale(&status, "stored-token"));

        let malformed = probe_linux_process_identity_with(123, |_| Ok("malformed".to_string()));
        assert_eq!(malformed, ProcessIdentityStatus::Unknown);
        assert!(!process_identity_is_stale(&malformed, "stored-token"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn confirmed_dead_and_token_mismatch_are_stale() {
        let dead = probe_linux_process_identity_with(123, |_| {
            Err(io::Error::new(io::ErrorKind::NotFound, "gone"))
        });
        assert!(process_identity_is_stale(&dead, "stored-token"));

        let live = ProcessIdentityStatus::Alive("new-token".to_string());
        assert!(process_identity_is_stale(&live, "stored-token"));
        assert!(!process_identity_is_stale(&live, "new-token"));
    }

    #[test]
    fn lease_id_collision_retries_without_overwrite() {
        let temp = tempfile::tempdir().unwrap();
        let db = Database::create(temp.path().join("leases.redb")).unwrap();
        let setup = db.begin_write().unwrap();
        {
            let mut table = setup.open_table(READER_LEASES).unwrap();
            table
                .insert("lease-backup-test-collision", b"original".as_slice())
                .unwrap();
        }
        setup.commit().unwrap();

        let write = db.begin_write().unwrap();
        let mut ids = [
            Ok("lease-backup-test-collision".to_string()),
            Ok("lease-backup-test-fresh".to_string()),
        ]
        .into_iter();
        let (_, inserted) =
            insert_reader_lease_with(&write, "backup-test", 7, "token".to_string(), || {
                ids.next().unwrap()
            })
            .unwrap();
        write.commit().unwrap();

        assert_eq!(inserted, "lease-backup-test-fresh");
        let read = db.begin_read().unwrap();
        let table = read.open_table(READER_LEASES).unwrap();
        assert_eq!(
            table
                .get("lease-backup-test-collision")
                .unwrap()
                .unwrap()
                .value(),
            b"original"
        );
        assert!(table.get("lease-backup-test-fresh").unwrap().is_some());
    }

    #[test]
    fn lease_id_generation_failure_inserts_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let db = Database::create(temp.path().join("leases.redb")).unwrap();
        let setup = db.begin_write().unwrap();
        let _ = setup.open_table(READER_LEASES).unwrap();
        setup.commit().unwrap();

        let write = db.begin_write().unwrap();
        let error = insert_reader_lease_with(&write, "backup-test", 7, "token".to_string(), || {
            Err(Error::Database("rng failed".to_string()))
        })
        .unwrap_err();
        assert!(matches!(error, Error::Database(message) if message == "rng failed"));
        drop(write);

        let read = db.begin_read().unwrap();
        let table = read.open_table(READER_LEASES).unwrap();
        assert!(table.iter().unwrap().next().is_none());
    }
}
