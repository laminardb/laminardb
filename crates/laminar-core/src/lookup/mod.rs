//! Lookup table traits, predicate pushdown, and caching.

pub mod align;
pub mod foyer_cache;
pub mod predicate;
pub mod source;

pub use align::KeyAligner;
pub use foyer_cache::{FoyerMemoryCache, FoyerMemoryCacheConfig, LookupCacheKey};
pub use predicate::{
    predicate_to_sql, split_predicates, Predicate, ScalarValue, SourceCapabilities, SplitPredicates,
};
pub use source::{
    ColumnId, LookupError, LookupSource, LookupSourceCapabilities, LookupSourceDyn, PushdownAdapter,
};

mod table;
pub use table::LookupResult;
