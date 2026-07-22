//! Serializable checkpoint shapes for aggregate, join, and window state.
//!
//! `AggStateCheckpoint` is columnar; `GroupCheckpoint` (window/EOWC) and
//! `EmittedCheckpoint` hold one-row IPC tuples; join buffers hold multi-row batches.

use arrow::datatypes::Schema;
use xxhash_rust::xxh3::Xxh3;

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

/// Columnar running-aggregate state: all keys in one IPC batch, each accumulator's
/// state across all groups in one IPC batch. Row `j` of `keys_ipc`, every
/// `acc_state_ipc[i]`, and `last_updated_ms[j]` refer to the same group.
#[derive(
    Clone, serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct AggStateCheckpoint {
    pub fingerprint: u64,
    pub keys_ipc: Vec<u8>,
    pub acc_state_ipc: Vec<Vec<u8>>,
    pub last_updated_ms: Vec<i64>,
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

/// Stable hash of the state-producing SQL and output schema; invalidates state on query change.
pub(crate) fn query_fingerprint(state_sql: &str, output_schema: &Schema) -> u64 {
    query_fingerprint_with_config(state_sql, output_schema, &[])
}

/// Query fingerprint with an operator-specific binary configuration suffix.
pub(crate) fn query_fingerprint_with_config(
    state_sql: &str,
    output_schema: &Schema,
    config: &[u8],
) -> u64 {
    let mut hasher = Xxh3::new();
    hasher.update(b"laminardb.state-query.v2\0");
    hash_bytes(&mut hasher, state_sql.as_bytes());
    hasher.update(&(output_schema.fields().len() as u64).to_le_bytes());
    for field in output_schema.fields() {
        hash_bytes(&mut hasher, field.name().as_bytes());
        hash_bytes(&mut hasher, field.data_type().to_string().as_bytes());
        hasher.update(&[u8::from(field.is_nullable())]);
    }
    hash_bytes(&mut hasher, config);
    hasher.digest()
}

fn hash_bytes(hasher: &mut Xxh3, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}
