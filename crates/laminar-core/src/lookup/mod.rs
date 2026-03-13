//! # Lookup Table Module
//!
//! Types and traits for lookup joins in streaming queries.
//!
//! Lookup tables allow stream events to be enriched with data from
//! external sources (databases, object stores, caches) at query time.
//!
//! ## Module Overview
//!
//! - `predicate`: Filter predicates for source pushdown
//! - `table`: `LookupResult`, strategy/config types

/// foyer-backed in-memory cache for lookup tables.
pub mod foyer_cache;
pub mod predicate;
/// Async lookup source trait.
pub mod source;
/// Lookup table configuration types.
pub mod table;

// Re-export commonly used types
pub use foyer_cache::{FoyerMemoryCache, FoyerMemoryCacheConfig, LookupCacheKey};
pub use predicate::{
    predicate_to_sql, split_predicates, Predicate, ScalarValue, SourceCapabilities, SplitPredicates,
};
pub use source::{
    ColumnId, LookupError, LookupSource, LookupSourceCapabilities, LookupSourceDyn, PushdownAdapter,
};
pub use table::{LookupResult, LookupStrategy, LookupTableConfig};
