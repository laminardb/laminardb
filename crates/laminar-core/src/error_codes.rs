//! LaminarDB structured error code registry.
//!
//! Every error in LaminarDB carries a stable `LDB-NNNN` code that is:
//! - Present in the error message (grep-able in logs)
//! - Present in the source code (grep-able in code)
//! - Stable across versions (codes are never reused)
//!
//! # Code Ranges
//!
//! | Range | Category |
//! |-------|----------|
//! | `LDB-0xxx` | General / configuration |
//! | `LDB-1xxx` | SQL parsing & validation |
//! | `LDB-2xxx` | Window / watermark operations |
//! | `LDB-3xxx` | Join operations |
//! | `LDB-4xxx` | Serialization / state |
//! | `LDB-5xxx` | Connector / I/O |
//! | `LDB-6xxx` | Checkpoint / recovery |
//! | `LDB-7xxx` | DataFusion / Arrow interop |
//! | `LDB-8xxx` | Internal / should-not-happen |
//!
//! Codes within the `LDB-1xxx` through `LDB-3xxx` ranges are defined in
//! `laminar-sql::error::codes` (the SQL layer) and are re-exported here
//! for reference. This module is the canonical registry for all other ranges.

// ── General / Configuration (LDB-0xxx) ──

/// Invalid configuration value.
pub const INVALID_CONFIG: &str = "LDB-0001";
/// Missing required configuration key.
pub const MISSING_CONFIG: &str = "LDB-0002";
/// Unresolved config variable (e.g. `${VAR}` placeholder).
pub const UNRESOLVED_CONFIG_VAR: &str = "LDB-0003";
/// Database is shut down.
pub const SHUTDOWN: &str = "LDB-0004";
/// Invalid operation for the current state.
pub const INVALID_OPERATION: &str = "LDB-0005";
/// Schema mismatch between Rust type and SQL definition.
pub const SCHEMA_MISMATCH: &str = "LDB-0006";

// ── SQL Parsing & Validation (LDB-1xxx) ──
// Canonical definitions are in `laminar_sql::error::codes`.
// Repeated here for reference only.

/// Unsupported SQL syntax (canonical: `laminar_sql::error::codes::UNSUPPORTED_SQL`).
pub const SQL_UNSUPPORTED: &str = "LDB-1001";
/// Query planning failed.
pub const SQL_PLANNING_FAILED: &str = "LDB-1002";
/// Column not found.
pub const SQL_COLUMN_NOT_FOUND: &str = "LDB-1100";
/// Table or source not found.
pub const SQL_TABLE_NOT_FOUND: &str = "LDB-1101";
/// Type mismatch.
pub const SQL_TYPE_MISMATCH: &str = "LDB-1200";

// ── Window / Watermark (LDB-2xxx) ──

/// Watermark required for this operation.
pub const WATERMARK_REQUIRED: &str = "LDB-2001";
/// Invalid window specification.
pub const WINDOW_INVALID: &str = "LDB-2002";
/// Window size must be positive.
pub const WINDOW_SIZE_INVALID: &str = "LDB-2003";
/// Late data rejected by window policy.
pub const LATE_DATA_REJECTED: &str = "LDB-2004";

// ── Join (LDB-3xxx) ──

/// Join key column not found or invalid.
pub const JOIN_KEY_MISSING: &str = "LDB-3001";
/// Time bound required for stream-stream join.
pub const JOIN_TIME_BOUND_MISSING: &str = "LDB-3002";
/// Temporal join requires a primary key on the right-side table.
pub const TEMPORAL_JOIN_NO_PK: &str = "LDB-3003";
/// Unsupported join type for streaming queries.
pub const JOIN_TYPE_UNSUPPORTED: &str = "LDB-3004";

// ── Serialization / State (LDB-4xxx) ──

/// State serialization failed for an operator.
pub const SERIALIZATION_FAILED: &str = "LDB-4001";
/// State deserialization failed for an operator.
pub const DESERIALIZATION_FAILED: &str = "LDB-4002";
/// JSON parse error (connector config, CDC payload, etc.).
pub const JSON_PARSE_ERROR: &str = "LDB-4003";
/// Base64 decode error (inline checkpoint state).
pub const BASE64_DECODE_ERROR: &str = "LDB-4004";
/// State store key not found.
pub const STATE_KEY_MISSING: &str = "LDB-4005";
/// State corruption detected (checksum mismatch, invalid data).
pub const STATE_CORRUPTION: &str = "LDB-4006";

// ── Connector / I/O (LDB-5xxx) ──

/// Connector failed to establish a connection.
pub const CONNECTOR_CONNECTION_FAILED: &str = "LDB-5001";
/// Connector authentication failed.
pub const CONNECTOR_AUTH_FAILED: &str = "LDB-5002";
/// Connector read error.
pub const CONNECTOR_READ_ERROR: &str = "LDB-5003";
/// Connector write error.
pub const CONNECTOR_WRITE_ERROR: &str = "LDB-5004";
/// Connector configuration error.
pub const CONNECTOR_CONFIG_ERROR: &str = "LDB-5005";
/// Source not found.
pub const SOURCE_NOT_FOUND: &str = "LDB-5010";
/// Sink not found.
pub const SINK_NOT_FOUND: &str = "LDB-5011";
/// Source already exists.
pub const SOURCE_ALREADY_EXISTS: &str = "LDB-5012";
/// Sink already exists.
pub const SINK_ALREADY_EXISTS: &str = "LDB-5013";
/// Connector serde (serialization/deserialization) error.
pub const CONNECTOR_SERDE_ERROR: &str = "LDB-5020";
/// Schema inference or compatibility error.
pub const CONNECTOR_SCHEMA_ERROR: &str = "LDB-5021";

// ── Checkpoint / Recovery (LDB-6xxx) ──

/// Checkpoint creation failed.
pub const CHECKPOINT_FAILED: &str = "LDB-6001";
/// Checkpoint not found.
pub const CHECKPOINT_NOT_FOUND: &str = "LDB-6002";
/// Checkpoint recovery failed.
pub const RECOVERY_FAILED: &str = "LDB-6003";
/// Sink rollback failed during checkpoint abort.
pub const SINK_ROLLBACK_FAILED: &str = "LDB-6004";
/// WAL (write-ahead log) error.
pub const WAL_ERROR: &str = "LDB-6005";
/// WAL entry has invalid length (possible corruption).
pub const WAL_INVALID_LENGTH: &str = "LDB-6006";
/// WAL checksum mismatch.
pub const WAL_CHECKSUM_MISMATCH: &str = "LDB-6007";
/// Checkpoint manifest persistence failed.
pub const MANIFEST_PERSIST_FAILED: &str = "LDB-6008";
/// Checkpoint prune (old checkpoint cleanup) failed.
pub const CHECKPOINT_PRUNE_FAILED: &str = "LDB-6009";
/// Sidecar state data missing or corrupted.
pub const SIDECAR_CORRUPTION: &str = "LDB-6010";
/// Source offset metadata missing during recovery.
pub const OFFSET_METADATA_MISSING: &str = "LDB-6011";

// ── DataFusion / Arrow Interop (LDB-7xxx) ──

/// Query execution failed (`DataFusion` engine error).
/// Note: the SQL layer uses `LDB-9001` for execution failures visible to users;
/// `LDB-7001` is for internal `DataFusion` interop issues.
pub const QUERY_EXECUTION_FAILED: &str = "LDB-7001";
/// Arrow schema or record batch error.
pub const ARROW_ERROR: &str = "LDB-7002";
/// `DataFusion` plan optimization failed.
pub const PLAN_OPTIMIZATION_FAILED: &str = "LDB-7003";

// ── Internal / Should-Not-Happen (LDB-8xxx) ──

/// Internal error — this is a bug.
pub const INTERNAL: &str = "LDB-8001";
/// Pipeline error (start/shutdown lifecycle).
pub const PIPELINE_ERROR: &str = "LDB-8002";
/// Materialized view error.
pub const MATERIALIZED_VIEW_ERROR: &str = "LDB-8003";
/// Query pipeline error (stream execution context).
pub const QUERY_PIPELINE_ERROR: &str = "LDB-8004";

// ── Ring 0 Hot Path Errors ──

/// Ring 0 error — no heap allocation, no formatting on construction.
///
/// Only formatted when actually displayed (which happens outside Ring 0).
/// Uses `Copy` and `repr(u16)` for zero-cost error reporting via counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum HotPathError {
    /// Event arrived after watermark — dropped.
    LateEvent = 0x0001,
    /// State store key not found.
    StateKeyMissing = 0x0002,
    /// Downstream backpressure — event buffered.
    Backpressure = 0x0003,
    /// Serialization buffer overflow.
    SerializationOverflow = 0x0004,
    /// Record batch schema does not match expected.
    SchemaMismatch = 0x0005,
    /// Aggregate state corruption detected.
    AggregateStateCorruption = 0x0006,
    /// Queue is full — cannot push event.
    QueueFull = 0x0007,
    /// Channel is closed/disconnected.
    ChannelClosed = 0x0008,
}

impl HotPathError {
    /// Returns a static error message. Cost: one match. No allocation.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::LateEvent => "Event arrived after watermark; dropped",
            Self::StateKeyMissing => "State key not found in store",
            Self::Backpressure => "Downstream backpressure; event buffered",
            Self::SerializationOverflow => "Serialization buffer capacity exceeded",
            Self::SchemaMismatch => "Record batch schema does not match expected",
            Self::AggregateStateCorruption => "Aggregate state checksum mismatch detected",
            Self::QueueFull => "Queue is full; cannot push event",
            Self::ChannelClosed => "Channel is closed or disconnected",
        }
    }

    /// Numeric code for metrics counters. Zero-cost.
    #[must_use]
    pub const fn code(self) -> u16 {
        self as u16
    }

    /// Returns the `LDB-NNNN` error code string for this hot path error.
    #[must_use]
    pub const fn ldb_code(self) -> &'static str {
        match self {
            Self::LateEvent => LATE_DATA_REJECTED,
            Self::StateKeyMissing => STATE_KEY_MISSING,
            Self::Backpressure => "LDB-8010",
            Self::SerializationOverflow => SERIALIZATION_FAILED,
            Self::SchemaMismatch => SCHEMA_MISMATCH,
            Self::AggregateStateCorruption => STATE_CORRUPTION,
            Self::QueueFull => "LDB-8011",
            Self::ChannelClosed => "LDB-8012",
        }
    }
}

impl std::fmt::Display for HotPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.ldb_code(), self.message())
    }
}

impl std::error::Error for HotPathError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_path_error_is_copy_and_small() {
        let e = HotPathError::LateEvent;
        let e2 = e; // Copy
        assert_eq!(e, e2);
        assert_eq!(std::mem::size_of::<HotPathError>(), 2);
    }

    #[test]
    fn hot_path_error_codes_are_nonzero() {
        let variants = [
            HotPathError::LateEvent,
            HotPathError::StateKeyMissing,
            HotPathError::Backpressure,
            HotPathError::SerializationOverflow,
            HotPathError::SchemaMismatch,
            HotPathError::AggregateStateCorruption,
            HotPathError::QueueFull,
            HotPathError::ChannelClosed,
        ];
        for v in &variants {
            assert!(v.code() > 0, "{v:?} has zero code");
            assert!(!v.message().is_empty(), "{v:?} has empty message");
            assert!(
                v.ldb_code().starts_with("LDB-"),
                "{v:?} has bad ldb_code: {}",
                v.ldb_code()
            );
        }
    }

    #[test]
    fn hot_path_error_display() {
        let e = HotPathError::LateEvent;
        let s = e.to_string();
        assert!(s.starts_with("[LDB-"));
        assert!(s.contains("watermark"));
    }

    #[test]
    fn error_codes_are_stable_strings() {
        assert_eq!(INVALID_CONFIG, "LDB-0001");
        assert_eq!(SERIALIZATION_FAILED, "LDB-4001");
        assert_eq!(CHECKPOINT_FAILED, "LDB-6001");
        assert_eq!(INTERNAL, "LDB-8001");
    }
}
