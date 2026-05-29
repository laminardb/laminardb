//! Lookup tables for enrichment joins.
//!
//! Production code uses `laminar_core::lookup::LookupSource` for the
//! actual on-demand lookups; this module hosts the reference-table
//! adapters (CDC, Delta Lake, Postgres) that hydrate those caches.

/// CDC-to-reference-table adapter for using CDC sources as lookup tables.
pub mod cdc_adapter;

/// Delta Lake reference table source for lookup/enrichment joins.
pub mod delta_reference;

/// Delta Lake on-demand lookup source for cache-miss fallback.
#[cfg(feature = "delta-lake")]
pub mod delta_lookup;

/// Iceberg on-demand lookup source for cache-miss fallback.
#[cfg(feature = "iceberg")]
pub mod iceberg_lookup;

/// PostgreSQL poll-based reference table source (no CDC required).
#[cfg(feature = "postgres-cdc")]
pub mod postgres_reference;

// Re-export the canonical lookup types from laminar-core.
pub use laminar_core::lookup::{LookupError, LookupResult};
