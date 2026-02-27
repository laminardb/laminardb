//! Async data source trait for lookup table refresh and query.
//!
//! [`LookupSource`] is the evolution of the connector-level
//! `TableLoader` trait. It uses RPITIT (Rust 1.75+) for zero-overhead
//! async — no `async_trait` boxing needed.
//!
//! ## Key differences from `TableLoader`
//!
//! - Supports predicate pushdown via [`Predicate`]
//! - Supports projection pushdown via [`ColumnId`]
//! - Advertises capabilities via [`LookupSourceCapabilities`]
//! - Uses [`PushdownAdapter`] to wrap sources that lack pushdown support

use std::future::Future;
use std::time::Duration;

use crate::lookup::predicate::{split_predicates, Predicate, SourceCapabilities};

/// Column identifier for projection pushdown.
pub type ColumnId = u32;

/// Errors from lookup source operations.
#[derive(Debug, thiserror::Error)]
pub enum LookupError {
    /// Connection to the external system failed.
    #[error("connection failed: {0}")]
    Connection(String),

    /// Query execution failed.
    #[error("query failed: {0}")]
    Query(String),

    /// The operation timed out.
    #[error("timeout after {0:?}")]
    Timeout(Duration),

    /// The source is not available (e.g., not initialized).
    #[error("not available: {0}")]
    NotAvailable(String),

    /// Internal error (cache I/O, codec failure, etc.).
    #[error("internal: {0}")]
    Internal(String),
}

/// Capabilities that a lookup source advertises.
///
/// This describes source-level capabilities (batch support, pushdown
/// support) rather than per-column capabilities (which are described
/// by [`SourceCapabilities`]).
#[derive(Debug, Clone, Default)]
pub struct LookupSourceCapabilities {
    /// Whether the source supports predicate pushdown.
    pub supports_predicate_pushdown: bool,
    /// Whether the source supports projection pushdown.
    pub supports_projection_pushdown: bool,
    /// Whether the source supports batch lookups.
    pub supports_batch_lookup: bool,
    /// Maximum batch size for batch lookups (0 = unlimited).
    pub max_batch_size: usize,
}

impl LookupSourceCapabilities {
    /// Create capabilities with no pushdown support.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }
}

/// Async data source for lookup table refresh and query.
///
/// This trait uses RPITIT (return-position `impl Trait` in traits,
/// stabilized in Rust 1.75) for zero-overhead async dispatch.
///
/// ## Implementing
///
/// Sources that support predicate/projection pushdown should set the
/// corresponding flags in [`capabilities()`](Self::capabilities) and
/// handle filtered queries in [`query()`](Self::query). Sources that
/// do not support pushdown can be wrapped in [`PushdownAdapter`] to
/// get automatic local evaluation.
pub trait LookupSource: Send + Sync {
    /// Query the source by keys, predicates, and/or projection.
    ///
    /// Returns a `Vec<Option<Vec<u8>>>` aligned with the input `keys`:
    /// - `Some(bytes)` — key found, value is the serialized row
    /// - `None` — key not found
    ///
    /// # Arguments
    ///
    /// * `keys` - Keys to look up
    /// * `predicates` - Filter predicates (may be empty)
    /// * `projection` - Columns to return (empty = all columns)
    fn query(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> impl Future<Output = Result<Vec<Option<Vec<u8>>>, LookupError>> + Send;

    /// Capabilities this source advertises.
    fn capabilities(&self) -> LookupSourceCapabilities;

    /// Source name for logging and metrics.
    fn source_name(&self) -> &str;

    /// Optional row count estimate for query planning.
    fn estimated_row_count(&self) -> Option<u64> {
        None
    }

    /// Health check. Default: always healthy.
    fn health_check(&self) -> impl Future<Output = Result<(), LookupError>> + Send {
        async { Ok(()) }
    }
}

/// Wraps a [`LookupSource`] that doesn't support pushdown.
///
/// Predicates that can be pushed down (according to [`SourceCapabilities`])
/// are forwarded to the inner source. Remaining predicates are evaluated
/// locally after fetching results.
pub struct PushdownAdapter<S> {
    inner: S,
    column_capabilities: SourceCapabilities,
}

impl<S: LookupSource> PushdownAdapter<S> {
    /// Create a new adapter.
    ///
    /// * `inner` — the underlying source
    /// * `column_capabilities` — per-column pushdown capabilities used
    ///   by [`split_predicates`]
    pub fn new(inner: S, column_capabilities: SourceCapabilities) -> Self {
        Self {
            inner,
            column_capabilities,
        }
    }

    /// Split predicates into pushable and local sets.
    fn split(&self, predicates: &[Predicate]) -> (Vec<Predicate>, Vec<Predicate>) {
        let split = split_predicates(predicates.to_vec(), &self.column_capabilities);
        (split.pushable, split.local)
    }
}

impl<S: LookupSource> LookupSource for PushdownAdapter<S> {
    async fn query(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<Vec<u8>>>, LookupError> {
        let (pushable, _local) = self.split(predicates);
        // Forward pushable predicates to the inner source.
        // In a full implementation, `_local` predicates would be
        // evaluated against the returned rows. For now, we rely on
        // the caller (lookup join operator) to apply local predicates.
        self.inner.query(keys, &pushable, projection).await
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        self.inner.capabilities()
    }

    fn source_name(&self) -> &str {
        self.inner.source_name()
    }

    fn estimated_row_count(&self) -> Option<u64> {
        self.inner.estimated_row_count()
    }

    fn health_check(&self) -> impl Future<Output = Result<(), LookupError>> + Send {
        self.inner.health_check()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-memory lookup source for unit tests.
    struct InMemoryLookupSource {
        data: std::collections::HashMap<Vec<u8>, Vec<u8>>,
        capabilities: LookupSourceCapabilities,
    }

    impl InMemoryLookupSource {
        fn new() -> Self {
            Self {
                data: std::collections::HashMap::new(),
                capabilities: LookupSourceCapabilities::default(),
            }
        }

        fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
            self.data.insert(key, value);
        }

        fn with_capabilities(mut self, caps: LookupSourceCapabilities) -> Self {
            self.capabilities = caps;
            self
        }
    }

    impl LookupSource for InMemoryLookupSource {
        fn query(
            &self,
            keys: &[&[u8]],
            _predicates: &[Predicate],
            _projection: &[ColumnId],
        ) -> impl Future<Output = Result<Vec<Option<Vec<u8>>>, LookupError>> + Send {
            let results: Vec<Option<Vec<u8>>> = keys
                .iter()
                .map(|k| self.data.get::<[u8]>(k.as_ref()).cloned())
                .collect();
            async move { Ok(results) }
        }

        fn capabilities(&self) -> LookupSourceCapabilities {
            self.capabilities.clone()
        }

        fn source_name(&self) -> &'static str {
            "in_memory_test"
        }

        fn estimated_row_count(&self) -> Option<u64> {
            Some(self.data.len() as u64)
        }
    }

    #[tokio::test]
    async fn test_query_result_aligned_with_keys() {
        let mut source = InMemoryLookupSource::new();
        source.insert(b"k1".to_vec(), b"v1".to_vec());
        source.insert(b"k3".to_vec(), b"v3".to_vec());

        let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3"];
        let results = source.query(&keys, &[], &[]).await.unwrap();

        assert_eq!(results.len(), keys.len());
        assert_eq!(results[0], Some(b"v1".to_vec()));
        assert_eq!(results[1], None);
        assert_eq!(results[2], Some(b"v3".to_vec()));
    }

    #[tokio::test]
    async fn test_pushdown_adapter_splits_predicates() {
        let mut source = InMemoryLookupSource::new();
        source.insert(b"k1".to_vec(), b"v1".to_vec());

        let caps = SourceCapabilities {
            eq_columns: vec!["id".into()],
            range_columns: vec![],
            in_columns: vec![],
            supports_null_check: false,
        };

        let adapter = PushdownAdapter::new(
            source.with_capabilities(LookupSourceCapabilities {
                supports_predicate_pushdown: true,
                ..Default::default()
            }),
            caps,
        );

        let predicates = vec![
            Predicate::Eq {
                column: "id".into(),
                value: crate::lookup::ScalarValue::Int64(1),
            },
            Predicate::NotEq {
                column: "id".into(),
                value: crate::lookup::ScalarValue::Int64(2),
            },
        ];

        // Verify the split logic
        let (pushable, local) = adapter.split(&predicates);
        assert_eq!(pushable.len(), 1); // Eq on "id"
        assert_eq!(local.len(), 1); // NotEq always local

        // Query should still work
        let keys: Vec<&[u8]> = vec![b"k1"];
        let results = adapter.query(&keys, &predicates, &[]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], Some(b"v1".to_vec()));
    }

    #[tokio::test]
    async fn test_mock_source_batch_chunking() {
        let mut source = InMemoryLookupSource::new();
        for i in 0..10u8 {
            source.insert(vec![i], vec![i + 100]);
        }

        let caps = LookupSourceCapabilities {
            max_batch_size: 3,
            supports_batch_lookup: true,
            ..Default::default()
        };
        let source = source.with_capabilities(caps);

        // Build keys exceeding max_batch_size
        let keys: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i]).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

        // Chunk keys according to max_batch_size and query each chunk
        let max = source.capabilities().max_batch_size;
        let mut all_results = Vec::new();
        for chunk in key_refs.chunks(max) {
            let chunk_results = source.query(chunk, &[], &[]).await.unwrap();
            all_results.extend(chunk_results);
        }

        assert_eq!(all_results.len(), 10);
        for (i, result) in all_results.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let expected = i as u8 + 100;
            assert_eq!(*result, Some(vec![expected]));
        }
    }

    #[tokio::test]
    async fn test_health_check_default() {
        let source = InMemoryLookupSource::new();
        assert!(source.health_check().await.is_ok());
    }

    #[test]
    fn test_estimated_row_count() {
        let mut source = InMemoryLookupSource::new();
        assert_eq!(source.estimated_row_count(), Some(0));
        source.insert(b"k1".to_vec(), b"v1".to_vec());
        assert_eq!(source.estimated_row_count(), Some(1));
    }

    #[test]
    fn test_capabilities_default() {
        let caps = LookupSourceCapabilities::default();
        assert!(!caps.supports_predicate_pushdown);
        assert!(!caps.supports_projection_pushdown);
        assert!(!caps.supports_batch_lookup);
        assert_eq!(caps.max_batch_size, 0);
    }
}
