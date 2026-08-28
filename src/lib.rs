#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used)]

pub mod chunking;
pub mod codec;
pub mod config;
pub mod engine;
pub mod error;
pub mod hash;
pub mod ids;
pub mod lifecycle;
pub mod manifest;
pub mod rate_limit;
pub mod repository;
pub mod source;
pub mod telemetry;

pub use config::BackupConfig;
pub use engine::{backup, inspect, restore, verify, BackupSummary, InspectReport, VerifyReport};
pub use error::{Error, ExitCode};
pub use lifecycle::{
    ChunkMove, DeletePlan, DeleteResult, GcJournal, GcPhase, GcPlan, GcRecoveryResult, GcResult,
    GcTombstone, UndeletePlan, UndeleteResult,
};
pub use lifecycle::{ReaderLease, ReaderLeaseGuard};
pub use repository::{BackupSummaryItem, Repository};
pub use source::FileSource;
