//! Key routing specification and error types.
//!
//! Provides [`KeySpec`] for specifying how events are partitioned across cores
//! and [`RouterError`] for routing failures.

/// Routing errors with no heap allocation.
///
/// All error variants use static strings to avoid allocation on error paths,
/// which is critical for Ring 0 hot path performance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RouterError {
    /// A column specified by name was not found in the schema.
    #[error("column not found by name")]
    ColumnNotFoundByName,

    /// A column index is out of range for the batch.
    #[error("column index out of range")]
    ColumnIndexOutOfRange,

    /// A row index is out of range for the array.
    #[error("row index out of range")]
    RowIndexOutOfRange,

    /// The data type is not supported for key extraction.
    #[error("unsupported data type for routing")]
    UnsupportedDataType,

    /// The batch is empty and cannot be routed.
    #[error("empty batch")]
    EmptyBatch,
}

/// Specifies how to extract routing keys from events.
///
/// The key determines which core processes an event. All events with the
/// same key are guaranteed to go to the same core.
#[derive(Debug, Clone, Default)]
pub enum KeySpec {
    /// Use specific column names as the key.
    ///
    /// The columns are concatenated in order to form the key.
    Columns(Vec<String>),

    /// Use column indices as the key.
    ///
    /// Useful when column names are not known or for performance.
    ColumnIndices(Vec<usize>),

    /// Round-robin distribution (no key).
    ///
    /// Events are distributed evenly across cores without regard to content.
    /// Use this when state locality is not required.
    #[default]
    RoundRobin,

    /// Use all columns as the key.
    ///
    /// The entire row is hashed to determine routing.
    AllColumns,
}
