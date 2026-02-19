//! Lookup table trait and configuration types.
//!
//! A [`LookupTable`] provides synchronous key-value access for enriching
//! stream events via lookup joins. The trait is `Send + Sync` so tables
//! can be shared across operator instances in a thread-per-core runtime.
//!
//! ## Lookup Flow
//!
//! 1. Stream event arrives at a lookup join operator
//! 2. Operator calls [`get_cached`](LookupTable::get_cached) (Ring 0, < 500ns)
//! 3. On cache miss, calls [`get`](LookupTable::get) (may hit disk/network)
//! 4. Result is joined with the stream event

use std::time::Duration;

use bytes::Bytes;

/// Result of a lookup operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    /// Cache hit — value found in memory.
    Hit(Bytes),
    /// The lookup is in progress (async source query pending).
    Pending,
    /// Key does not exist in the source.
    NotFound,
}

impl LookupResult {
    /// Returns `true` if this is a cache hit.
    #[must_use]
    pub const fn is_hit(&self) -> bool {
        matches!(self, Self::Hit(_))
    }

    /// Returns `true` if the key was not found.
    #[must_use]
    pub const fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound)
    }

    /// Extracts the value from a `Hit`, consuming `self`.
    #[must_use]
    pub fn into_bytes(self) -> Option<Bytes> {
        match self {
            Self::Hit(b) => Some(b),
            _ => None,
        }
    }
}

/// Strategy for how lookup table data is distributed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum LookupStrategy {
    /// Full copy on every node (small reference tables).
    #[default]
    Replicated,
    /// Data partitioned across nodes by key hash.
    Partitioned {
        /// Number of partitions.
        num_partitions: u32,
    },
    /// Queries go directly to the source (no local cache/copy).
    SourceDirect,
}

/// Configuration for a lookup table instance.
#[derive(Debug, Clone)]
pub struct LookupTableConfig {
    /// How data is distributed.
    pub strategy: LookupStrategy,
    /// Time-to-live for cached entries.
    pub ttl: Option<Duration>,
    /// Maximum number of cached entries.
    pub max_cache_entries: usize,
    /// Source connector name (for async refresh).
    pub source: Option<String>,
}

impl Default for LookupTableConfig {
    fn default() -> Self {
        Self {
            strategy: LookupStrategy::Replicated,
            ttl: None,
            max_cache_entries: 100_000,
            source: None,
        }
    }
}

/// Synchronous lookup table interface for Ring 0 operators.
///
/// Implementations must be `Send + Sync` to allow sharing across threads
/// in the thread-per-core runtime. The hot-path method is
/// [`get_cached`](Self::get_cached) which should be < 500ns.
///
/// Writes (insert/invalidate) come from the Ring 1 CDC adapter or
/// bulk-load path, not from the hot path.
pub trait LookupTable: Send + Sync {
    /// Look up a key in the in-memory cache only (Ring 0).
    ///
    /// This is the fast path — must not block or perform I/O.
    /// Returns [`LookupResult::NotFound`] on cache miss; callers should
    /// then try [`get`](Self::get) for a deeper lookup.
    ///
    /// # Performance
    ///
    /// Target: < 500ns
    fn get_cached(&self, key: &[u8]) -> LookupResult;

    /// Look up a key, potentially checking slower storage tiers.
    ///
    /// May check a disk cache, remote cache, or trigger an async
    /// source query. Returns [`LookupResult::Pending`] if the lookup
    /// requires an async fetch that hasn't completed yet.
    fn get(&self, key: &[u8]) -> LookupResult;

    /// Insert or update a cached entry.
    ///
    /// Called by the CDC adapter or bulk-load path when new data arrives
    /// from the source. This is NOT on the hot path.
    fn insert(&self, key: &[u8], value: Bytes);

    /// Invalidate a cached entry.
    ///
    /// Called when the source signals a delete or the TTL expires.
    fn invalidate(&self, key: &[u8]);

    /// Number of entries currently in the cache.
    fn len(&self) -> usize;

    /// Whether the cache is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_result_methods() {
        let hit = LookupResult::Hit(Bytes::from_static(b"value"));
        assert!(hit.is_hit());
        assert!(!hit.is_not_found());
        assert_eq!(hit.into_bytes(), Some(Bytes::from_static(b"value")));

        let miss = LookupResult::NotFound;
        assert!(miss.is_not_found());
        assert!(!miss.is_hit());
        assert!(miss.into_bytes().is_none());

        let pending = LookupResult::Pending;
        assert!(!pending.is_hit());
        assert!(!pending.is_not_found());
    }

    #[test]
    fn test_lookup_strategy_default() {
        let strategy = LookupStrategy::default();
        assert_eq!(strategy, LookupStrategy::Replicated);
    }

    #[test]
    fn test_lookup_table_config_default() {
        let config = LookupTableConfig::default();
        assert_eq!(config.strategy, LookupStrategy::Replicated);
        assert!(config.ttl.is_none());
        assert_eq!(config.max_cache_entries, 100_000);
        assert!(config.source.is_none());
    }
}
