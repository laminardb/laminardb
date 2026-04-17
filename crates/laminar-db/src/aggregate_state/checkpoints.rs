//! Serializable checkpoint shapes for aggregate / window / join state.
//!
//! These are the on-disk forms — field names and `#[serde(default = ...)]`
//! fallbacks are part of the format. Don't rename without a migration.

use std::hash::{Hash, Hasher};

use arrow::datatypes::Schema;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct GroupCheckpoint {
    pub key: Vec<serde_json::Value>,
    pub acc_states: Vec<Vec<serde_json::Value>>,
    #[serde(default = "default_last_updated")]
    pub last_updated_ms: i64,
}

fn default_last_updated() -> i64 {
    i64::MIN
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct AggStateCheckpoint {
    pub fingerprint: u64,
    pub groups: Vec<GroupCheckpoint>,
    #[serde(default)]
    pub last_emitted: Vec<EmittedCheckpoint>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct EmittedCheckpoint {
    pub key: Vec<serde_json::Value>,
    pub values: Vec<serde_json::Value>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct WindowCheckpoint {
    pub window_start: i64,
    pub groups: Vec<GroupCheckpoint>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct EowcStateCheckpoint {
    pub fingerprint: u64,
    pub windows: Vec<WindowCheckpoint>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

/// Stable hash of the pre-aggregate SQL + output schema shape. Used to
/// invalidate restored state when the query has changed.
pub(crate) fn query_fingerprint(pre_agg_sql: &str, output_schema: &Schema) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    pre_agg_sql.hash(&mut hasher);
    for field in output_schema.fields() {
        field.name().hash(&mut hasher);
        field.data_type().to_string().hash(&mut hasher);
    }
    hasher.finish()
}
