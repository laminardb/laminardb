//! Z-set changelog encoding shared between the materialized-view producer
//! (laminar-db) and the upsert-sink consumers (laminar-connectors).
//!
//! An aggregating MV emits one row per changed group carrying an Int64
//! `WEIGHT_COLUMN`: `+n` inserts a value with multiplicity `n`, `-n` retracts
//! it (a value change for key `K` is `{(K, V_old): -1, (K, V_new): +1}`). The
//! column name lives here so the producer and consumers cannot drift on it.

/// Name of the Int64 Z-set weight column appended to aggregating-MV changelog
/// output. `+n` = insert with multiplicity `n`, `-n` = retract.
pub const WEIGHT_COLUMN: &str = "__weight";
