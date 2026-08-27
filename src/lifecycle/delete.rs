use redb::TableDefinition;
use serde::{Deserialize, Serialize};

pub const TOMBSTONES: TableDefinition<&str, u64> = TableDefinition::new("tombstones");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeletePlan {
    pub backup_id: String,
    pub already_deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteResult {
    pub backup_id: String,
    pub tombstoned: bool,
}
