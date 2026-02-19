//! # Lookup Table Module
//!
//! Types and traits for lookup joins in streaming queries.
//!
//! Lookup tables allow stream events to be enriched with data from
//! external sources (databases, object stores, caches) at query time.
//!
//! ## Module Overview
//!
//! - [`predicate`]: Filter predicates for source pushdown
//! - [`table`]: `LookupTable` trait, `LookupResult`, strategy/config types

/// foyer-backed in-memory cache for lookup tables.
pub mod foyer_cache;
pub mod predicate;
/// Async lookup source trait for Phase 6.
pub mod source;
/// Lookup table trait and configuration.
pub mod table;

// Re-export commonly used types
pub use predicate::{
    Predicate, ScalarValue, SourceCapabilities, SplitPredicates, predicate_to_sql,
    split_predicates,
};
pub use foyer_cache::{
    CachedValue, FoyerMemoryCache, FoyerMemoryCacheConfig, HierarchyMetrics,
    HybridCacheConfig, LookupCacheHierarchy, LookupCacheKey,
};
pub use source::{ColumnId, LookupError, LookupSource, LookupSourceCapabilities, PushdownAdapter};
pub use table::{LookupResult, LookupStrategy, LookupTable, LookupTableConfig};
