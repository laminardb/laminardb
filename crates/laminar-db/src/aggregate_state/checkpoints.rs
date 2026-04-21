//! Serializable checkpoint shapes.
//!
//! **Wire format note:** scalar aggregate fields (`GroupCheckpoint.key`,
//! `GroupCheckpoint.acc_states`, `EmittedCheckpoint.key`,
//! `EmittedCheckpoint.values`) are Arrow IPC streams produced by
//! [`scalars_to_ipc`](super::scalars_to_ipc) — one-row batches encoding
//! a tuple of `ScalarValue`s. Historically these were
//! `Vec<serde_json::Value>` / `Vec<Vec<serde_json::Value>>`; see
//! [`scalar_ipc`](super::scalar_ipc) for the rationale.
//!
//! `JoinStateCheckpoint.left_batches` / `right_batches` are a different
//! shape: they hold Arrow IPC streams of full multi-row `RecordBatch`es
//! and are *not* produced by `scalars_to_ipc`.

use std::hash::{Hash, Hasher};

use arrow::datatypes::Schema;

#[derive(
    Clone, serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct GroupCheckpoint {
    /// IPC bytes encoding the group key tuple (`Vec<ScalarValue>`).
    pub key: Vec<u8>,
    /// One IPC blob per aggregate, each encoding that aggregate's
    /// `Accumulator::state()` tuple.
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
    /// IPC bytes for the key tuple.
    pub key: Vec<u8>,
    /// IPC bytes for the emitted value tuple.
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
