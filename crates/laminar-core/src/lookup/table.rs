//! Lookup table types — runtime results, distribution strategies, and configuration.
//!
//! - [`LookupResult`]: Outcome of a cache lookup (hit, pending, or miss)
//! - [`LookupStrategy`]: How table data is distributed across nodes
//! - [`LookupTableConfig`]: Per-table instance configuration

use std::time::Duration;

use arrow_array::RecordBatch;

/// Result of a lookup operation.
#[derive(Debug, Clone)]
pub enum LookupResult {
    /// Cache hit — value found in memory.
    Hit(RecordBatch),
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

    /// Extracts the `RecordBatch` from a `Hit`, consuming `self`.
    #[must_use]
    pub fn into_batch(self) -> Option<RecordBatch> {
        match self {
            Self::Hit(b) => Some(b),
            _ => None,
        }
    }
}

impl PartialEq for LookupResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Hit(a), Self::Hit(b)) => a == b,
            (Self::Pending, Self::Pending) | (Self::NotFound, Self::NotFound) => true,
            _ => false,
        }
    }
}

impl Eq for LookupResult {}

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["value"]))]).unwrap()
    }

    #[test]
    fn test_lookup_result_methods() {
        let hit = LookupResult::Hit(test_batch());
        assert!(hit.is_hit());
        assert!(!hit.is_not_found());
        assert!(hit.into_batch().is_some());

        let miss = LookupResult::NotFound;
        assert!(miss.is_not_found());
        assert!(!miss.is_hit());
        assert!(miss.into_batch().is_none());

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
