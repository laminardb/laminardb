//! Core streaming engine for `LaminarDB`.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::duration_suboptimal_units)] // MSRV 1.85; from_mins/from_hours are 1.91+
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::too_many_arguments, clippy::too_many_lines)]
// Protocol state machines remain contiguous and keep authority inputs explicit.
#![allow(clippy::enum_variant_names, clippy::trivially_copy_pass_by_ref)]
// Generated protobuf APIs retain schema-defined names and callback signatures.
// Protocol fixtures use explicit boundary values and full state construction.
#![cfg_attr(
    test,
    allow(
        clippy::cast_possible_truncation,
        clippy::default_trait_access,
        clippy::disallowed_types,
        clippy::field_reassign_with_default,
        clippy::items_after_statements,
        clippy::large_futures,
        clippy::similar_names,
        clippy::struct_excessive_bools
    )
)]

/// Feature-neutral catalog identity types.
pub mod catalog;
/// Z-set changelog `__weight` column name, shared between the MV producer and
/// upsert-sink consumers.
pub mod changelog;
/// Distributed checkpoint barrier protocol.
pub mod checkpoint;
/// Crash-durable same-directory file publication primitives.
pub mod durable_fs;
mod durable_local_store;
/// Structured error code registry (`LDB-NNNN`) and Ring 0 hot path error type.
pub mod error_codes;
/// Refreshable Google workload-identity credentials for pinned object-store clients.
#[cfg(feature = "gcs")]
pub mod gcs_credentials;
/// Lookup table types and predicate pushdown.
pub mod lookup;
pub mod mv;
pub mod operator;
/// Shared Arrow IPC serialization for `RecordBatch` ↔ bytes.
pub mod serialization;
/// Cross-instance shuffle: message codec, credit flow, wire protocol.
pub mod shuffle;
/// Partition-key encoding and virtual-node routing.
pub mod state;
/// Non-secret object-store credential-source classification.
pub mod storage_auth;
/// Provider-neutral object-store locations and consumer-specific URL adapters.
pub mod storage_location;
pub mod streaming;
pub mod time;

/// Distributed cluster coordination. Runtime services are gated behind `cluster`; the control
/// namespace retains feature-neutral checkpoint value types in every build.
pub mod cluster;

/// Per-epoch checkpoint commit marker store. Used by the checkpoint
/// coordinator's 2PC to record the commit decision durably before
/// sinks are told to commit, so recovery can re-establish the verdict
/// after a crash. Lives outside the cluster gate because
/// single-instance also needs it.
pub mod checkpoint_decision;

/// Result type for laminar-core operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for laminar-core
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Operator errors
    #[error("Operator error: {0}")]
    Operator(#[from] operator::OperatorError),

    /// Time-related errors
    #[error("Time error: {0}")]
    Time(#[from] time::TimeError),

    /// Materialized view errors
    #[error("MV error: {0}")]
    Mv(#[from] mv::MvError),
}
