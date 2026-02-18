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

pub mod predicate;

// Re-export commonly used types
pub use predicate::{
    Predicate, ScalarValue, SourceCapabilities, SplitPredicates, predicate_to_sql,
    split_predicates,
};
