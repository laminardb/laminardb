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

use arrow::compute::filter_record_batch;
use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::lookup::predicate::{split_predicates, Predicate, ScalarValue, SourceCapabilities};

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
    /// Returns a `Vec<Option<RecordBatch>>` aligned with the input `keys`:
    /// - `Some(batch)` — key found, value is a single-row `RecordBatch`
    /// - `None` — key not found
    fn query(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> impl Future<Output = Result<Vec<Option<RecordBatch>>, LookupError>> + Send;

    /// Capabilities this source advertises.
    fn capabilities(&self) -> LookupSourceCapabilities;

    /// Source name for logging and metrics.
    fn source_name(&self) -> &str;

    /// Arrow schema of the data this source returns.
    fn schema(&self) -> SchemaRef;

    /// Optional row count estimate for query planning.
    fn estimated_row_count(&self) -> Option<u64> {
        None
    }

    /// Health check. Default: always healthy.
    fn health_check(&self) -> impl Future<Output = Result<(), LookupError>> + Send {
        async { Ok(()) }
    }
}

/// Dyn-compatible version of [`LookupSource`] for use as `Arc<dyn LookupSourceDyn>`.
///
/// `LookupSource` uses RPITIT which is not dyn-compatible. This trait
/// uses `async_trait` boxing instead, suitable for the cold path
/// (cache miss → source query).
#[async_trait::async_trait]
pub trait LookupSourceDyn: Send + Sync {
    /// Query the source by keys, predicates, and/or projection.
    async fn query_batch(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError>;

    /// Arrow schema of the data this source returns.
    fn schema(&self) -> SchemaRef;
}

#[async_trait::async_trait]
impl<T: LookupSource> LookupSourceDyn for T {
    async fn query_batch(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        self.query(keys, predicates, projection).await
    }

    fn schema(&self) -> SchemaRef {
        LookupSource::schema(self)
    }
}

/// Wraps a [`LookupSource`] that doesn't support pushdown.
///
/// Predicates that can be pushed down (according to [`SourceCapabilities`])
/// are forwarded to the inner source. Remaining predicates are evaluated
/// locally after fetching results using Arrow SIMD filtering.
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

/// Apply a comparison from `arrow::compute::kernels::cmp` between a column
/// and a scalar value. Builds a typed single-element array for the scalar
/// side so `Scalar<T>` implements `Datum`.
fn compare_column_scalar(
    batch: &RecordBatch,
    column: &str,
    value: &ScalarValue,
    cmp_fn: fn(
        &dyn arrow_array::Datum,
        &dyn arrow_array::Datum,
    ) -> Result<BooleanArray, arrow::error::ArrowError>,
) -> Option<BooleanArray> {
    use arrow_array::{Float64Array, Int64Array, Scalar, StringArray};

    let idx = batch.schema().index_of(column).ok()?;
    let col = batch.column(idx);
    match value {
        ScalarValue::Int64(v) => cmp_fn(col, &Scalar::new(Int64Array::from(vec![*v]))).ok(),
        ScalarValue::Float64(v) => cmp_fn(col, &Scalar::new(Float64Array::from(vec![*v]))).ok(),
        ScalarValue::Utf8(v) => cmp_fn(col, &Scalar::new(StringArray::from(vec![v.as_str()]))).ok(),
        ScalarValue::Bool(v) => cmp_fn(col, &Scalar::new(BooleanArray::from(vec![*v]))).ok(),
        _ => None,
    }
}

/// Evaluate a single predicate against a `RecordBatch`, returning a boolean mask.
fn evaluate_predicate(batch: &RecordBatch, predicate: &Predicate) -> Option<BooleanArray> {
    use arrow::compute::kernels::cmp;

    match predicate {
        Predicate::Eq { column, value } => compare_column_scalar(batch, column, value, cmp::eq),
        Predicate::NotEq { column, value } => compare_column_scalar(batch, column, value, cmp::neq),
        Predicate::Lt { column, value } => compare_column_scalar(batch, column, value, cmp::lt),
        Predicate::LtEq { column, value } => {
            compare_column_scalar(batch, column, value, cmp::lt_eq)
        }
        Predicate::Gt { column, value } => compare_column_scalar(batch, column, value, cmp::gt),
        Predicate::GtEq { column, value } => {
            compare_column_scalar(batch, column, value, cmp::gt_eq)
        }
        Predicate::IsNull { column } => {
            let idx = batch.schema().index_of(column).ok()?;
            let col = batch.column(idx);
            Some(arrow::compute::is_null(col).ok()?)
        }
        Predicate::IsNotNull { column } => {
            let idx = batch.schema().index_of(column).ok()?;
            let col = batch.column(idx);
            Some(arrow::compute::is_not_null(col).ok()?)
        }
        Predicate::In { column, values } => {
            let idx = batch.schema().index_of(column).ok()?;
            let col = batch.column(idx);
            let mut mask: Option<BooleanArray> = None;
            for v in values {
                let eq_mask = evaluate_predicate(
                    batch,
                    &Predicate::Eq {
                        column: column.clone(),
                        value: v.clone(),
                    },
                )?;
                mask = Some(match mask {
                    Some(existing) => arrow::compute::or(&existing, &eq_mask).ok()?,
                    None => eq_mask,
                });
            }
            mask.or_else(|| Some(BooleanArray::from(vec![false; col.len()])))
        }
    }
}

/// Apply local predicates to a `RecordBatch`, filtering out non-matching rows.
fn apply_local_predicates(batch: &RecordBatch, predicates: &[Predicate]) -> Option<RecordBatch> {
    if predicates.is_empty() {
        return Some(batch.clone());
    }
    let mut combined: Option<BooleanArray> = None;
    for pred in predicates {
        let mask = evaluate_predicate(batch, pred)?;
        combined = Some(match combined {
            Some(existing) => arrow::compute::and(&existing, &mask).ok()?,
            None => mask,
        });
    }
    match combined {
        Some(mask) => filter_record_batch(batch, &mask).ok(),
        None => Some(batch.clone()),
    }
}

impl<S: LookupSource> LookupSource for PushdownAdapter<S> {
    async fn query(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        let (pushable, local) = self.split(predicates);
        let results = self.inner.query(keys, &pushable, projection).await?;

        if local.is_empty() {
            return Ok(results);
        }

        Ok(results
            .into_iter()
            .map(|opt| {
                opt.and_then(|batch| {
                    let filtered = apply_local_predicates(&batch, &local)?;
                    if filtered.num_rows() == 0 {
                        None
                    } else {
                        Some(filtered)
                    }
                })
            })
            .collect())
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        self.inner.capabilities()
    }

    fn source_name(&self) -> &str {
        self.inner.source_name()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn estimated_row_count(&self) -> Option<u64> {
        self.inner.estimated_row_count()
    }

    fn health_check(&self) -> impl Future<Output = Result<(), LookupError>> + Send {
        self.inner.health_check()
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_types)] // test code
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn make_batch(id: i64, name: &str) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
            ],
        )
        .unwrap()
    }

    /// In-memory lookup source for unit tests.
    struct InMemoryLookupSource {
        data: std::collections::HashMap<Vec<u8>, RecordBatch>,
        capabilities: LookupSourceCapabilities,
        source_schema: SchemaRef,
    }

    impl InMemoryLookupSource {
        fn new() -> Self {
            Self {
                data: std::collections::HashMap::new(),
                capabilities: LookupSourceCapabilities::default(),
                source_schema: test_schema(),
            }
        }

        fn insert(&mut self, key: Vec<u8>, value: RecordBatch) {
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
        ) -> impl Future<Output = Result<Vec<Option<RecordBatch>>, LookupError>> + Send {
            let results: Vec<Option<RecordBatch>> = keys
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

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.source_schema)
        }

        fn estimated_row_count(&self) -> Option<u64> {
            Some(self.data.len() as u64)
        }
    }

    #[tokio::test]
    async fn test_query_result_aligned_with_keys() {
        let mut source = InMemoryLookupSource::new();
        source.insert(b"k1".to_vec(), make_batch(1, "Alice"));
        source.insert(b"k3".to_vec(), make_batch(3, "Carol"));

        let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3"];
        let results = source.query(&keys, &[], &[]).await.unwrap();

        assert_eq!(results.len(), keys.len());
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        assert!(results[2].is_some());
    }

    #[tokio::test]
    async fn test_pushdown_adapter_splits_predicates() {
        let mut source = InMemoryLookupSource::new();
        source.insert(b"k1".to_vec(), make_batch(1, "Alice"));

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

        let (pushable, local) = adapter.split(&predicates);
        assert_eq!(pushable.len(), 1); // Eq on "id"
        assert_eq!(local.len(), 1); // NotEq always local

        let keys: Vec<&[u8]> = vec![b"k1"];
        let results = adapter.query(&keys, &predicates, &[]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_some());
    }

    #[tokio::test]
    async fn test_pushdown_adapter_local_predicate_filters() {
        let mut source = InMemoryLookupSource::new();
        source.insert(b"k1".to_vec(), make_batch(1, "Alice"));
        source.insert(b"k2".to_vec(), make_batch(2, "Bob"));

        let caps = SourceCapabilities {
            eq_columns: vec![],
            range_columns: vec![],
            in_columns: vec![],
            supports_null_check: false,
        };

        let adapter = PushdownAdapter::new(source, caps);

        // Filter: id > 1 — should keep k2 but filter out k1
        let predicates = vec![Predicate::Gt {
            column: "id".into(),
            value: ScalarValue::Int64(1),
        }];

        let keys: Vec<&[u8]> = vec![b"k1", b"k2"];
        let results = adapter.query(&keys, &predicates, &[]).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_none()); // id=1, filtered by id > 1
        assert!(results[1].is_some()); // id=2, passes id > 1
    }

    #[tokio::test]
    async fn test_pushdown_adapter_not_eq_local_evaluation() {
        let mut source = InMemoryLookupSource::new();
        source.insert(b"k1".to_vec(), make_batch(1, "Alice"));
        source.insert(b"k2".to_vec(), make_batch(2, "Bob"));

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

        // NotEq is always evaluated locally
        let predicates = vec![Predicate::NotEq {
            column: "id".into(),
            value: ScalarValue::Int64(1),
        }];

        let keys: Vec<&[u8]> = vec![b"k1", b"k2"];
        let results = adapter.query(&keys, &predicates, &[]).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_none()); // id=1, filtered by id != 1
        assert!(results[1].is_some()); // id=2, passes id != 1
    }

    #[tokio::test]
    async fn test_mock_source_batch_chunking() {
        let mut source = InMemoryLookupSource::new();
        for i in 0..10i64 {
            source.insert(vec![i as u8], make_batch(i, &format!("name_{i}")));
        }

        let caps = LookupSourceCapabilities {
            max_batch_size: 3,
            supports_batch_lookup: true,
            ..Default::default()
        };
        let source = source.with_capabilities(caps);

        let keys: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i]).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

        let max = source.capabilities().max_batch_size;
        let mut all_results = Vec::new();
        for chunk in key_refs.chunks(max) {
            let chunk_results = source.query(chunk, &[], &[]).await.unwrap();
            all_results.extend(chunk_results);
        }

        assert_eq!(all_results.len(), 10);
        for result in &all_results {
            assert!(result.is_some());
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
        source.insert(b"k1".to_vec(), make_batch(1, "Alice"));
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

    #[test]
    fn test_schema_propagation() {
        let source = InMemoryLookupSource::new();
        let schema = LookupSource::schema(&source);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_pushdown_adapter_schema_propagation() {
        let source = InMemoryLookupSource::new();
        let caps = SourceCapabilities {
            eq_columns: vec![],
            range_columns: vec![],
            in_columns: vec![],
            supports_null_check: false,
        };
        let adapter = PushdownAdapter::new(source, caps);
        let schema = LookupSource::schema(&adapter);
        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn test_evaluate_predicate_is_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
        )
        .unwrap();

        let pred = Predicate::IsNull {
            column: "id".into(),
        };
        let mask = evaluate_predicate(&batch, &pred).unwrap();
        assert_eq!(mask.value(0), false);
        assert_eq!(mask.value(1), true);
        assert_eq!(mask.value(2), false);
    }

    #[test]
    fn test_evaluate_predicate_in_list() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"]))],
        )
        .unwrap();

        let pred = Predicate::In {
            column: "name".into(),
            values: vec![
                ScalarValue::Utf8("Alice".into()),
                ScalarValue::Utf8("Carol".into()),
            ],
        };
        let mask = evaluate_predicate(&batch, &pred).unwrap();
        assert_eq!(mask.value(0), true);
        assert_eq!(mask.value(1), false);
        assert_eq!(mask.value(2), true);
    }
}
