//! # External Lookup Tables for Enrichment Joins
//!
//! This module provides the `TableLoader` trait for loading data from external
//! reference tables (dimension tables) to enrich streaming events.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    LookupJoinOperator                       │
//! │  (laminar-core)                                             │
//! │  ┌─────────┐    ┌─────────┐    ┌─────────────────────────┐ │
//! │  │ Event   │───▶│  Cache  │───▶│  Output (enriched)      │ │
//! │  │ Stream  │    │ (State) │    │                         │ │
//! │  └─────────┘    └────┬────┘    └─────────────────────────┘ │
//! │                      │ miss                                 │
//! │                      ▼                                      │
//! │               ┌─────────────┐                               │
//! │               │ TableLoader │  (trait, implemented here)    │
//! │               └──────┬──────┘                               │
//! └──────────────────────┼──────────────────────────────────────┘
//!                        │
//!                        ▼
//!             ┌──────────────────────┐
//!             │   External Systems   │
//!             │ (Redis, PostgreSQL,  │
//!             │  HTTP APIs, etc.)    │
//!             └──────────────────────┘
//! ```
//!
//! ## Implementations
//!
//! - `InMemoryTableLoader` - For testing and static reference data
//! - (Phase 3) Redis, PostgreSQL, HTTP loaders
//!
//! ## Example
//!
//! ```rust,no_run
//! use laminar_connectors::lookup::{TableLoader, InMemoryTableLoader, LookupResult};
//! use arrow_array::RecordBatch;
//!
//! // Create an in-memory lookup table
//! let mut loader = InMemoryTableLoader::new();
//! // loader.insert(b"customer_1".to_vec(), batch);
//!
//! // Use with LookupJoinOperator in laminar-core
//! ```

/// CDC-to-cache adapter for lookup table refresh.
pub mod cdc_adapter;

/// PostgreSQL lookup source with connection pooling and predicate pushdown.
#[cfg(feature = "postgres-cdc")]
pub mod postgres_source;

#[cfg(feature = "postgres-cdc")]
pub use postgres_source::{PostgresLookupSource, PostgresLookupSourceConfig};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use rustc_hash::FxHashMap;
use std::sync::Arc;
use thiserror::Error;

/// Errors that can occur during table lookup operations.
#[derive(Debug, Error)]
pub enum LookupError {
    /// The requested key was not found in the table.
    #[error("Key not found")]
    KeyNotFound,

    /// Connection to the external system failed.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Query execution failed.
    #[error("Query failed: {0}")]
    QueryFailed(String),

    /// Timeout waiting for response.
    #[error("Timeout after {0}ms")]
    Timeout(u64),

    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// The table loader is not available (e.g., not initialized).
    #[error("Loader not available: {0}")]
    NotAvailable(String),
}

/// Result of a lookup operation.
#[derive(Debug, Clone)]
pub enum LookupResult {
    /// The key was found with the associated data.
    Found(RecordBatch),
    /// The key was not found in the table.
    NotFound,
}

impl LookupResult {
    /// Returns `true` if the lookup found a result.
    #[must_use]
    pub fn is_found(&self) -> bool {
        matches!(self, LookupResult::Found(_))
    }

    /// Returns the found batch, or `None` if not found.
    #[must_use]
    pub fn into_batch(self) -> Option<RecordBatch> {
        match self {
            LookupResult::Found(batch) => Some(batch),
            LookupResult::NotFound => None,
        }
    }
}

/// Trait for loading data from external reference tables.
///
/// Implementations of this trait provide access to external data sources
/// for enriching streaming events with dimension data.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow concurrent access from
/// multiple operator instances.
///
/// # Performance Considerations
///
/// - Lookups may be called frequently (per-event), so implementations
///   should be efficient
/// - Consider batch lookups ([`TableLoader::lookup_batch`]) for better
///   throughput when multiple keys need to be looked up
/// - The `LookupJoinOperator` (in `laminar-core`)
///   caches results, so implementations don't need their own cache
#[async_trait]
pub trait TableLoader: Send + Sync {
    /// Looks up a single key in the table.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up (typically the join column value)
    ///
    /// # Returns
    ///
    /// - `Ok(LookupResult::Found(batch))` if the key exists
    /// - `Ok(LookupResult::NotFound)` if the key doesn't exist
    /// - `Err(LookupError)` if the lookup failed
    async fn lookup(&self, key: &[u8]) -> Result<LookupResult, LookupError>;

    /// Looks up multiple keys in a single batch operation.
    ///
    /// Default implementation calls [`lookup`](TableLoader::lookup) for each key.
    /// Implementations should override this for better performance when the
    /// underlying system supports batch queries.
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys to look up
    ///
    /// # Returns
    ///
    /// A vector of results in the same order as the input keys.
    async fn lookup_batch(&self, keys: &[&[u8]]) -> Result<Vec<LookupResult>, LookupError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.lookup(key).await?);
        }
        Ok(results)
    }

    /// Returns the name of this table loader for logging/debugging.
    fn name(&self) -> &str;

    /// Checks if the table loader is healthy and can accept requests.
    ///
    /// Default implementation returns `true`. Override for loaders that
    /// need to maintain connections to external systems.
    async fn health_check(&self) -> bool {
        true
    }

    /// Closes the table loader and releases any resources.
    ///
    /// Default implementation does nothing. Override for loaders that
    /// hold connections or other resources.
    async fn close(&self) -> Result<(), LookupError> {
        Ok(())
    }
}

/// In-memory table loader for testing and static reference data.
///
/// This implementation stores all data in memory using a `HashMap`.
/// Suitable for:
/// - Testing lookup join operators
/// - Small, static reference tables
/// - Development and prototyping
///
/// # Example
///
/// ```rust
/// use laminar_connectors::lookup::InMemoryTableLoader;
/// use arrow_array::{RecordBatch, StringArray, Int64Array};
/// use arrow_schema::{Schema, Field, DataType};
/// use std::sync::Arc;
///
/// // Create loader with customer data
/// let mut loader = InMemoryTableLoader::new();
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("customer_id", DataType::Utf8, false),
///     Field::new("name", DataType::Utf8, false),
///     Field::new("tier", DataType::Utf8, false),
/// ]));
///
/// let batch = RecordBatch::try_new(
///     schema,
///     vec![
///         Arc::new(StringArray::from(vec!["cust_1"])),
///         Arc::new(StringArray::from(vec!["Alice"])),
///         Arc::new(StringArray::from(vec!["gold"])),
///     ],
/// ).unwrap();
///
/// loader.insert(b"cust_1".to_vec(), batch);
/// ```
#[derive(Debug, Clone)]
pub struct InMemoryTableLoader {
    /// The underlying data store.
    data: Arc<parking_lot::RwLock<FxHashMap<Vec<u8>, RecordBatch>>>,
    /// Name for logging.
    name: String,
}

impl InMemoryTableLoader {
    /// Creates a new empty in-memory table loader.
    #[must_use]
    pub fn new() -> Self {
        Self::with_name("in_memory")
    }

    /// Creates a new empty in-memory table loader with a custom name.
    #[must_use]
    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            data: Arc::new(parking_lot::RwLock::new(FxHashMap::default())),
            name: name.into(),
        }
    }

    /// Creates a new in-memory table loader from existing data.
    #[must_use]
    pub fn from_map(data: FxHashMap<Vec<u8>, RecordBatch>) -> Self {
        Self {
            data: Arc::new(parking_lot::RwLock::new(data)),
            name: "in_memory".to_string(),
        }
    }

    /// Inserts a key-value pair into the table.
    pub fn insert(&self, key: Vec<u8>, value: RecordBatch) {
        self.data.write().insert(key, value);
    }

    /// Removes a key from the table.
    ///
    /// Returns the previous value if the key existed.
    #[must_use]
    pub fn remove(&self, key: &[u8]) -> Option<RecordBatch> {
        self.data.write().remove(key)
    }

    /// Returns the number of entries in the table.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    /// Returns `true` if the table is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    /// Clears all entries from the table.
    pub fn clear(&self) {
        self.data.write().clear();
    }
}

impl Default for InMemoryTableLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableLoader for InMemoryTableLoader {
    async fn lookup(&self, key: &[u8]) -> Result<LookupResult, LookupError> {
        let data = self.data.read();
        match data.get(key) {
            Some(batch) => Ok(LookupResult::Found(batch.clone())),
            None => Ok(LookupResult::NotFound),
        }
    }

    async fn lookup_batch(&self, keys: &[&[u8]]) -> Result<Vec<LookupResult>, LookupError> {
        let data = self.data.read();
        let results = keys
            .iter()
            .map(|key| match data.get(*key) {
                Some(batch) => LookupResult::Found(batch.clone()),
                None => LookupResult::NotFound,
            })
            .collect();
        Ok(results)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A no-op table loader that always returns `NotFound`.
///
/// Useful for testing the lookup join operator without an actual data source.
#[derive(Debug, Clone, Default)]
pub struct NoOpTableLoader;

impl NoOpTableLoader {
    /// Creates a new no-op table loader.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TableLoader for NoOpTableLoader {
    async fn lookup(&self, _key: &[u8]) -> Result<LookupResult, LookupError> {
        Ok(LookupResult::NotFound)
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "no_op"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};

    fn create_customer_batch(id: &str, name: &str, tier: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("tier", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
                Arc::new(StringArray::from(vec![tier])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_in_memory_loader_basic() {
        let loader = InMemoryTableLoader::new();

        // Insert test data
        loader.insert(
            b"cust_1".to_vec(),
            create_customer_batch("cust_1", "Alice", "gold"),
        );
        loader.insert(
            b"cust_2".to_vec(),
            create_customer_batch("cust_2", "Bob", "silver"),
        );

        assert_eq!(loader.len(), 2);

        // Lookup existing key
        let result = loader.lookup(b"cust_1").await.unwrap();
        assert!(result.is_found());
        let batch = result.into_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Lookup missing key
        let result = loader.lookup(b"cust_999").await.unwrap();
        assert!(!result.is_found());
    }

    #[tokio::test]
    async fn test_in_memory_loader_batch_lookup() {
        let loader = InMemoryTableLoader::new();
        loader.insert(b"k1".to_vec(), create_customer_batch("k1", "A", "gold"));
        loader.insert(b"k3".to_vec(), create_customer_batch("k3", "C", "bronze"));

        let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3"];
        let results = loader.lookup_batch(&keys).await.unwrap();

        assert_eq!(results.len(), 3);
        assert!(results[0].is_found()); // k1 exists
        assert!(!results[1].is_found()); // k2 doesn't exist
        assert!(results[2].is_found()); // k3 exists
    }

    #[tokio::test]
    async fn test_in_memory_loader_remove() {
        let loader = InMemoryTableLoader::new();
        loader.insert(
            b"key".to_vec(),
            create_customer_batch("key", "Test", "gold"),
        );

        assert_eq!(loader.len(), 1);

        let removed = loader.remove(b"key");
        assert!(removed.is_some());
        assert_eq!(loader.len(), 0);

        let result = loader.lookup(b"key").await.unwrap();
        assert!(!result.is_found());
    }

    #[tokio::test]
    async fn test_no_op_loader() {
        let loader = NoOpTableLoader::new();

        let result = loader.lookup(b"any_key").await.unwrap();
        assert!(!result.is_found());
        assert_eq!(loader.name(), "no_op");
    }

    #[tokio::test]
    async fn test_in_memory_loader_clear() {
        let loader = InMemoryTableLoader::new();
        loader.insert(b"k1".to_vec(), create_customer_batch("k1", "A", "gold"));
        loader.insert(b"k2".to_vec(), create_customer_batch("k2", "B", "silver"));

        assert!(!loader.is_empty());
        loader.clear();
        assert!(loader.is_empty());
        assert_eq!(loader.len(), 0);
    }

    #[tokio::test]
    async fn test_table_loader_health_check() {
        let loader = InMemoryTableLoader::new();
        assert!(loader.health_check().await);
    }
}
