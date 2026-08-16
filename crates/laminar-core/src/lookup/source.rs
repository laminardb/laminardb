//! Async lookup source trait with predicate and projection pushdown.

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

/// Resolve projection column indices to their names in `schema`, in projection
/// order. An empty projection means "all columns" and returns every name in
/// schema order. Shared by the lookup backends to turn the `query` projection
/// into a column selection.
///
/// # Errors
/// Returns [`LookupError::Internal`] if a projection index is out of range.
pub fn projection_names(
    schema: &SchemaRef,
    projection: &[ColumnId],
) -> Result<Vec<String>, LookupError> {
    if projection.is_empty() {
        return Ok(schema.fields().iter().map(|f| f.name().clone()).collect());
    }
    projection
        .iter()
        .map(|&c| {
            schema
                .fields()
                .get(c as usize)
                .map(|f| f.name().clone())
                .ok_or_else(|| LookupError::Internal(format!("projection column {c} out of range")))
        })
        .collect()
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
    fn source_name(&self) -> &'static str;

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
    use arrow_array::types::{TimestampMicrosecondType, TimestampMillisecondType};
    use arrow_array::{Float64Array, Int64Array, PrimitiveArray, Scalar, StringArray};

    let idx = batch.schema().index_of(column).ok()?;
    let col = batch.column(idx);
    match value {
        ScalarValue::Int64(v) => cmp_fn(col, &Scalar::new(Int64Array::from(vec![*v]))).ok(),
        ScalarValue::Float64(v) => cmp_fn(col, &Scalar::new(Float64Array::from(vec![*v]))).ok(),
        ScalarValue::Utf8(v) => cmp_fn(col, &Scalar::new(StringArray::from(vec![v.as_str()]))).ok(),
        ScalarValue::Bool(v) => cmp_fn(col, &Scalar::new(BooleanArray::from(vec![*v]))).ok(),
        ScalarValue::Timestamp(us) => {
            if col
                .as_any()
                .is::<PrimitiveArray<TimestampMicrosecondType>>()
            {
                let scalar = PrimitiveArray::<TimestampMicrosecondType>::from(vec![*us]);
                cmp_fn(col, &Scalar::new(scalar)).ok()
            } else if col
                .as_any()
                .is::<PrimitiveArray<TimestampMillisecondType>>()
            {
                let scalar = PrimitiveArray::<TimestampMillisecondType>::from(vec![*us / 1000]);
                cmp_fn(col, &Scalar::new(scalar)).ok()
            } else {
                None
            }
        }
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

    fn source_name(&self) -> &'static str {
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
mod tests;
