//! Serializable checkpoint shapes for aggregate and window state.
//!
//! `AggStateCheckpoint` is columnar; `GroupCheckpoint` (window/EOWC) and
//! `EmittedCheckpoint` hold one-row IPC tuples.

use arrow::datatypes::Schema;
use xxhash_rust::xxh3::Xxh3;

use crate::error::DbError;

mod archive;
mod ipc;

pub(crate) use archive::AggStateArchiveRestoreProfile;
#[cfg(feature = "cluster")]
pub(crate) use archive::AggStateRestorePreflight;
#[cfg(all(test, feature = "cluster"))]
use ipc::preflight_scalar_ipc_restore;

// Restore accounting follows the same deterministic charged-byte contract as resident aggregate
// state: allocator metadata and fragmentation are excluded, while every requested payload and
// inline element is included. The expansion factors deliberately dominate Arrow array creation,
// row conversion, scalar extraction, and concrete-state construction for the scalar-only schemas
// admitted below.
const RESTORE_IPC_EXPANSION_FACTOR: usize = 8;
const RESTORE_ROW_SCRATCH_CHARGE: usize = 2_048;
const RESTORE_CELL_SCRATCH_CHARGE: usize = 64;
const MAX_ACCUMULATOR_STATE_COLUMNS: usize = 2;

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct GroupCheckpoint {
    pub key: Vec<u8>,
    pub acc_states: Vec<Vec<u8>>,
}

/// Columnar running-aggregate state: all keys in one IPC batch, each accumulator's
/// state across all groups in one IPC batch. Row `j` of `keys_ipc`, every
/// `acc_state_ipc[i]`, `input_weights[j]`, and `last_updated_ms[j]` refer to the same group.
#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct AggStateCheckpoint {
    pub fingerprint: u64,
    pub keys_ipc: Vec<u8>,
    pub acc_state_ipc: Vec<Vec<u8>>,
    pub input_weights: Vec<i64>,
    pub last_updated_ms: Vec<i64>,
    pub last_emitted: Vec<EmittedCheckpoint>,
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct EmittedCheckpoint {
    pub key: Vec<u8>,
    pub values: Vec<u8>,
}

impl AggStateCheckpoint {
    pub(crate) fn retained_serialization_bytes(&self) -> Result<usize, DbError> {
        fn add(total: &mut usize, bytes: usize) -> Result<(), DbError> {
            *total = total.checked_add(bytes).ok_or_else(|| {
                DbError::Checkpoint("aggregate checkpoint serialization accounting overflow".into())
            })?;
            Ok(())
        }

        fn roster<T>(capacity: usize) -> Result<usize, DbError> {
            capacity
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| {
                    DbError::Checkpoint("aggregate checkpoint serialization roster overflow".into())
                })
        }

        let mut bytes = self.keys_ipc.capacity();
        add(
            &mut bytes,
            roster::<Vec<u8>>(self.acc_state_ipc.capacity())?,
        )?;
        for state in &self.acc_state_ipc {
            add(&mut bytes, state.capacity())?;
        }
        add(&mut bytes, roster::<i64>(self.input_weights.capacity())?)?;
        add(&mut bytes, roster::<i64>(self.last_updated_ms.capacity())?)?;
        add(
            &mut bytes,
            roster::<EmittedCheckpoint>(self.last_emitted.capacity())?,
        )?;
        for emitted in &self.last_emitted {
            add(&mut bytes, emitted.key.capacity())?;
            add(&mut bytes, emitted.values.capacity())?;
        }
        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct IpcRestorePreflight {
    rows: usize,
    columns: usize,
    dictionary_rows: usize,
    dictionary_body_bytes: usize,
    shared_payload_bytes: usize,
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct WindowCheckpoint {
    pub window_start: i64,
    pub groups: Vec<GroupCheckpoint>,
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
    hasher.update(b"laminardb.state-query.v3\0");
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

#[cfg(all(test, feature = "cluster"))]
mod tests;
