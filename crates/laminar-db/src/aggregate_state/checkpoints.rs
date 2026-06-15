//! Serializable checkpoint shapes for aggregate, join, and window state.
//!
//! Scalar fields (`key`, `acc_states`, `values`) hold Arrow IPC one-row batches
//! from `scalars_to_ipc`. Join buffer fields hold full multi-row IPC batches.

use std::hash::{Hash, Hasher};

use arrow::datatypes::Schema;

#[derive(
    Clone, serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct GroupCheckpoint {
    pub key: Vec<u8>,
    pub acc_states: Vec<Vec<u8>>,
    #[serde(default = "default_last_updated")]
    pub last_updated_ms: i64,
}

fn default_last_updated() -> i64 {
    i64::MIN
}

#[derive(
    Clone, serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct AggStateCheckpoint {
    pub fingerprint: u64,
    pub groups: Vec<GroupCheckpoint>,
    #[serde(default)]
    pub last_emitted: Vec<EmittedCheckpoint>,
}

#[derive(
    Clone, serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct EmittedCheckpoint {
    pub key: Vec<u8>,
    pub values: Vec<u8>,
}

#[derive(
    Clone, serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct WindowCheckpoint {
    pub window_start: i64,
    pub groups: Vec<GroupCheckpoint>,
}

#[derive(
    Clone, serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct EowcStateCheckpoint {
    pub fingerprint: u64,
    pub windows: Vec<WindowCheckpoint>,
    // Bumps the rkyv schema; old checkpoints fail to deserialize and recovery restarts fresh.
    pub high_watermark_ms: i64,
}

#[derive(
    Clone, serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct JoinStateCheckpoint {
    #[serde(default)]
    pub left_buffer_rows: u64,
    #[serde(default)]
    pub right_buffer_rows: u64,
    #[serde(default)]
    pub left_batches: Vec<Vec<u8>>,
    #[serde(default)]
    pub right_batches: Vec<Vec<u8>>,
    #[serde(default = "default_evicted_watermark")]
    pub last_evicted_watermark: i64,
    #[serde(default = "default_evicted_watermark")]
    pub last_evicted_watermark_right: i64,
}

fn default_evicted_watermark() -> i64 {
    i64::MIN
}

/// Stable hash of the pre-agg SQL and output schema; invalidates restored state on query change.
pub(crate) fn query_fingerprint(pre_agg_sql: &str, output_schema: &Schema) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    pre_agg_sql.hash(&mut hasher);
    for field in output_schema.fields() {
        field.name().hash(&mut hasher);
        field.data_type().to_string().hash(&mut hasher);
    }
    hasher.finish()
}
