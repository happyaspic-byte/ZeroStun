use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProgressMode {
    #[default]
    Auto,
    None,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobStats {
    pub original_bytes: u64,
    pub stored_bytes: u64,
    pub unique_chunks: u64,
    pub reused_chunks: u64,
    pub total_chunks: u64,
}
