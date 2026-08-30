#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used)]

pub mod chunking;
pub mod codec;
pub mod config;
pub mod daemon;
pub mod engine;
pub mod error;
pub mod hash;
pub mod ids;
pub mod lifecycle;
pub mod manifest;
pub mod rate_limit;
pub mod repository;
pub mod snapshot;
pub mod source;
pub mod telemetry;

pub use config::BackupConfig;
pub use engine::{backup, inspect, restore, verify, BackupSummary, InspectReport, VerifyReport};
pub use error::{Error, ExitCode};
pub use lifecycle::{
    ChunkMove, DeletePlan, DeleteResult, FindingKind, FindingSeverity, GcJournal, GcPhase, GcPlan,
    GcRecoveryResult, GcResult, GcTombstone, PrunePlan, PruneResult, RepairFinding, RepairPlan,
    RepairReport, RepairResult, RepairScope, UndeletePlan, UndeleteResult,
    DEFAULT_MAX_REPAIR_FINDINGS,
};
pub use lifecycle::{ReaderLease, ReaderLeaseGuard};
pub use repository::{BackupSummaryItem, Repository};
pub use source::FileSource;
