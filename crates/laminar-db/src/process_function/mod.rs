//! Keyed process functions with engine-owned state and timers.
//!
//! The engine owns every value and timer. A handler receives immutable activation snapshots and
//! returns proposed changes; its private memory is never authoritative. Native handlers execute
//! on the compute thread and must be trusted, bounded, and nonblocking. The optional remote
//! transport is restricted to loopback workers on each database host. At-least-once delivery admits
//! trusted native Rust and remote Rust with one replayable source that reproduces one physical
//! channel in fixed batches. Each batch defines a reproducible event-time cut; independent-channel
//! merging remains unadmitted. Cluster execution additionally requires splittable placement and
//! the same immutable binding in every owner's sealed startup catalog. Python remains local
//! best-effort; exactly-once process delivery is unsupported.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::DbError;

mod catalog;
mod descriptor;
mod operator;
mod registration;
#[cfg(feature = "process-remote")]
pub mod remote;
mod schema;
mod source_order;

#[cfg(all(test, feature = "cluster"))]
mod cluster_recovery_tests;

#[cfg(feature = "benchmark-internals")]
pub mod benchmark;

pub(crate) use operator::ProcessFunctionOperator;
pub(crate) use schema::canonical_fields;

// RECOVERY: v2 binds the complete descriptor; v1 carried only a schema digest.
pub(crate) const STATE_CODEC_VERSION: u32 = 2;

/// Execution boundary bound by the function manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessRuntime {
    /// Trusted Rust code running on the compute thread.
    NativeRust,
    /// Rust code in a separate worker process over the local reference transport.
    RemoteRust,
    /// Python worker over the local loopback transport.
    RemotePython,
}

/// Immutable, versioned binding for one process function. The initial state codec is an
/// optional signed 64-bit value; a present null is distinct from absent state.
#[derive(Clone, Debug)]
pub struct ProcessFunctionDescriptor {
    /// Descriptor format version. Currently only 1 is admitted.
    pub version: u32,
    /// Runtime that executes the immutable implementation.
    pub runtime: ProcessRuntime,
    /// Stable function identity, independent of the output stream name.
    pub function_id: String,
    /// Stable pipeline state identity. Two pipelines must use different identities.
    pub pipeline_state_id: String,
    /// Hex SHA-256 of the trusted native build or other immutable implementation identity.
    pub implementation_digest: String,
    /// Optional Python runtime and import-tree identity checked by the local supervisor.
    /// File hashes detect deployment drift; they do not enforce lifetime immutability.
    pub python_environment: Option<PythonEnvironmentBinding>,
    /// Exact input schema admitted from one direct source. V1 retains accepted per-key arrival
    /// order; timestamps do not sort input. Replayable source positions alone do not define the
    /// merge order of independent input partitions.
    pub input_schema: SchemaRef,
    /// Exact append-only output schema.
    pub output_schema: SchemaRef,
    /// Ordered, nonnullable canonical partition-key columns.
    pub key_columns: Vec<String>,
    /// Nonnullable UTC microsecond input timestamp column.
    pub event_time_column: String,
    /// UTC microsecond output timestamp column; output may not precede its activation.
    pub output_event_time_column: String,
    /// Stable name of the one managed `ValueState<Int64?>` slot.
    pub value_state_name: String,
    /// Names of supported event-time timers.
    pub timer_names: Vec<String>,
    /// Hard execution and retained-state limits.
    pub limits: ProcessFunctionLimits,
}

/// Versioned file-tree binding for a supervised Python deployment. Absolute installation
/// paths are excluded so an identical package can move without changing checkpoint identity.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PythonEnvironmentBinding {
    /// Tree fingerprint format. Currently only 1 is admitted.
    pub version: u32,
    /// Selected interpreter's UTF-8 path relative to the runtime root, using `/` separators.
    pub executable: String,
    /// Exact top-level handler module and callable, written as `module:function`.
    pub handler: String,
    /// Lowercase hex SHA-256 of the complete runtime tree, including bytecode and native files.
    pub runtime_sha256: String,
    /// Ordered tree digests: handler directory first, then the configured import roots.
    pub import_roots_sha256: Vec<String>,
}

/// Per-function bounds, enforced before accepting a handler response.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessFunctionLimits {
    /// Maximum distinct keys with state or timers.
    pub max_keys: usize,
    /// Maximum retained timers.
    pub max_timers: usize,
    /// Maximum retained state bytes, including key and timer index estimates.
    pub max_state_bytes: usize,
    /// Maximum key-distinct activations in one handler call.
    pub max_batch_rows: usize,
    /// Maximum input rows accepted in one graph step.
    pub max_input_rows: usize,
    /// Maximum Arrow input bytes accepted in one graph step.
    pub max_input_bytes: usize,
    /// Maximum output rows in one graph step.
    pub max_output_rows: usize,
    /// Maximum retained Arrow output bytes in one graph step.
    pub max_output_bytes: usize,
    /// Maximum timer callbacks in one graph step.
    pub max_timer_callbacks_per_step: usize,
}

impl Default for ProcessFunctionLimits {
    fn default() -> Self {
        Self {
            max_keys: 100_000,
            max_timers: 100_000,
            max_state_bytes: 64 * 1024 * 1024,
            max_batch_rows: 256,
            max_input_rows: 4096,
            max_input_bytes: 16 * 1024 * 1024,
            max_output_rows: 4096,
            max_output_bytes: 16 * 1024 * 1024,
            max_timer_callbacks_per_step: 256,
        }
    }
}

/// Immutable view of a declared value before one activation's transition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ValueState {
    /// The key has never set this state, or has cleared it.
    #[default]
    Absent,
    /// The state exists and contains SQL NULL.
    Null,
    /// The state exists and contains a signed 64-bit value.
    Value(i64),
}

/// Explicit state transition; an output without a mutation leaves state unchanged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ValueMutation {
    /// Retain the pre-transition value.
    #[default]
    Unchanged,
    /// Set the state to a non-null value.
    Set(i64),
    /// Set a present SQL NULL.
    SetNull,
    /// Make the state absent.
    Clear,
}

/// An input row or a named event-time timer callback.
#[derive(Clone, Debug)]
pub enum ProcessCallback {
    /// One input row, with the descriptor's input schema.
    Input(RecordBatch),
    /// A timer callback with no fabricated input row.
    Timer {
        /// Declared timer name.
        name: String,
    },
}

/// One engine-assigned activation. Local `id` values follow the checkpointed operator sequence.
/// Private cluster execution encodes a checkpointed vnode sequence and its fixed vnode index;
/// replay identity requires the same callback order within that vnode. The key is the canonical
/// partition ABI encoding and must never be rehashed by the handler.
/// Preserving these IDs also requires the same watermark cuts relative to input and timer
/// callbacks. V1 does not certify deterministic replay of an independent-channel merge.
#[derive(Clone, Debug)]
pub struct ProcessActivation {
    /// Logical callback identity within the pipeline and operator, subject to its replay order.
    pub id: u64,
    /// Host-assigned canonical key bytes.
    pub key: Arc<[u8]>,
    /// The v1 UTF-8 key value, retained for callbacks after input has passed.
    pub key_text: String,
    /// Per-activation UTC event time in microseconds.
    pub event_time_us: i64,
    /// Callback kind and optional data row.
    pub callback: ProcessCallback,
    /// Consistent state before this activation.
    pub state: ValueState,
}

/// Named event-time timer operation, applied with its activation's state transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerOperation {
    /// Register or replace the named timer. The timestamp must exceed the current watermark.
    Set {
        /// Declared timer name.
        name: String,
        /// UTC firing timestamp in microseconds.
        at_us: i64,
    },
    /// Cancel a named timer; cancelling an absent timer is idempotent.
    Cancel {
        /// Declared timer name.
        name: String,
    },
}

/// Proposed result for one activation. The engine validates the complete handler response before
/// applying any result in that invocation batch.
#[derive(Clone, Debug)]
pub struct ProcessActivationResult {
    /// Must name exactly one activation in the request.
    pub activation_id: u64,
    /// Zero or more batches with the descriptor's output schema.
    pub output: Vec<RecordBatch>,
    /// Explicit managed-state mutation.
    pub value: ValueMutation,
    /// Ordered timer operations for this key.
    pub timers: Vec<TimerOperation>,
}

/// Trusted, synchronous native handler. A call contains at most one activation for each key.
/// The handler may vectorize over distinct keys but must not make one key's result depend on
/// unrelated batch members or their order. Return one result per activation, including those
/// producing zero rows. Errors discard the entire unaccepted call.
pub trait NativeProcessFunction: Send + Sync + 'static {
    /// Compute proposed outputs and mutations without external side effects.
    ///
    /// # Errors
    /// Return an error for an invalid input or a failed computation. The engine keeps the
    /// invocation unaccepted and replays from its last committed checkpoint when necessary.
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError>;
}

#[derive(Clone)]
pub(crate) struct ProcessFunctionRegistration {
    pub(crate) output_name: String,
    pub(crate) source_name: String,
    pub(crate) descriptor: ProcessFunctionDescriptor,
    pub(crate) handler: ProcessHandler,
}

#[derive(Clone)]
pub(crate) enum ProcessHandler {
    Native(Arc<dyn NativeProcessFunction>),
    #[cfg(feature = "process-remote")]
    Remote(Arc<remote::RemoteProcessClient>),
}

/// Registered function and its input/output stream binding.
#[derive(Clone, Debug)]
pub struct ProcessFunctionInfo {
    /// Output stream name.
    pub output_name: String,
    /// Input source name.
    pub source_name: String,
    /// Validated immutable descriptor.
    pub descriptor: ProcessFunctionDescriptor,
}

#[cfg(test)]
pub(crate) mod tests;
