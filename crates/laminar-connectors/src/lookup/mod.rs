//! Lookup tables for enrichment joins.
//!
//! Production code uses `laminar_core::lookup::LookupSource` for the
//! actual on-demand lookups; this module also hosts finite startup snapshot
//! adapters for replicated reference tables.

/// Delta Lake reference table source for lookup/enrichment joins.
pub mod delta_reference;

/// Delta Lake on-demand lookup source for cache-miss fallback.
#[cfg(feature = "delta-lake")]
pub mod delta_lookup;

/// Iceberg on-demand lookup source for cache-miss fallback.
#[cfg(feature = "iceberg")]
pub mod iceberg_lookup;

// Re-export the canonical lookup types from laminar-core.
pub use laminar_core::lookup::{LookupError, LookupResult};
