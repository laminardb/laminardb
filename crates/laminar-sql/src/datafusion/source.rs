//! Stream source trait for `DataFusion` integration
//!
//! This module defines the `StreamSource` trait that bridges `LaminarDB`'s
//! push-based streaming model with `DataFusion`'s pull-based query execution.

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_expr::Expr;

/// Declares a column's sort ordering for a streaming source.
///
/// When a source declares output ordering, `DataFusion` can elide unnecessary
/// `SortExec` nodes from the physical plan, enabling ORDER BY queries on
/// pre-sorted unbounded streams.
///
/// # Example
///
/// ```rust,ignore
/// // Source sorted by event_time ascending
/// let ordering = vec![SortColumn {
///     name: "event_time".to_string(),
///     descending: false,
///     nulls_first: false,
/// }];
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortColumn {
    /// Column name to sort by
    pub name: String,
    /// Whether the sort is descending (false = ascending)
    pub descending: bool,
    /// Whether nulls sort first (before non-null values)
    pub nulls_first: bool,
}

impl SortColumn {
    /// Creates a new ascending sort column with nulls last.
    #[must_use]
    pub fn ascending(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            descending: false,
            nulls_first: false,
        }
    }

    /// Creates a new descending sort column with nulls last.
    #[must_use]
    pub fn descending(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            descending: true,
            nulls_first: false,
        }
    }

    /// Sets whether nulls sort first.
    #[must_use]
    pub fn with_nulls_first(mut self, nulls_first: bool) -> Self {
        self.nulls_first = nulls_first;
        self
    }
}

/// A source of streaming data for `DataFusion` queries.
///
/// This trait enables integration between `LaminarDB`'s push-based event
/// processing and `DataFusion`'s pull-based query execution model.
///
/// Implementations must be thread-safe as `DataFusion` may access them
/// from multiple threads during query planning and execution.
///
/// # Filter Pushdown
///
/// Sources can optionally support filter pushdown by implementing
/// `supports_filters`. When filters are pushed down, the source should
/// apply them before yielding batches to reduce data transfer.
///
/// # Projection Pushdown
///
/// Sources should respect the `projection` parameter in `stream()` to
/// only read columns that are needed by the query, improving performance.
#[async_trait]
pub trait StreamSource: Send + Sync + Debug {
    /// Returns the schema of records produced by this source.
    ///
    /// The schema must be consistent across all calls and must match
    /// the schema of `RecordBatch` instances yielded by `stream()`.
    fn schema(&self) -> SchemaRef;

    /// Creates a stream of `RecordBatch` instances.
    ///
    /// # Arguments
    ///
    /// * `projection` - Optional column indices to project. If `None`,
    ///   all columns are returned. Indices refer to the source schema.
    /// * `filters` - Filter expressions that can be applied at the source.
    ///   The source may partially or fully apply these filters.
    ///
    /// # Returns
    ///
    /// A stream that yields `RecordBatch` instances asynchronously.
    ///
    /// # Errors
    ///
    /// Returns `DataFusionError` if the stream cannot be created.
    fn stream(
        &self,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> Result<SendableRecordBatchStream, DataFusionError>;

    /// Returns which filters this source can apply.
    ///
    /// For each filter in `filters`, returns `true` if the source will
    /// apply that filter, `false` otherwise. `DataFusion` uses this to
    /// know which filters still need to be applied after the scan.
    ///
    /// The default implementation returns all `false`, indicating no
    /// filter pushdown support.
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters being considered for pushdown.
    ///
    /// # Returns
    ///
    /// A vector of booleans, one per filter, indicating support.
    fn supports_filters(&self, filters: &[Expr]) -> Vec<bool> {
        vec![false; filters.len()]
    }

    /// Returns the output ordering of this source, if any.
    ///
    /// When a source is pre-sorted (e.g., by event time from an ordered
    /// Kafka partition), declaring the ordering allows `DataFusion` to
    /// elide `SortExec` from the physical plan for ORDER BY queries that
    /// match the declared ordering.
    ///
    /// The default implementation returns `None`, indicating the source
    /// has no guaranteed output ordering.
    fn output_ordering(&self) -> Option<Vec<SortColumn>> {
        None
    }
}

/// A shared reference to a stream source.
pub type StreamSourceRef = Arc<dyn StreamSource>;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    #[derive(Debug)]
    struct MockSource {
        schema: SchemaRef,
    }

    #[async_trait]
    impl StreamSource for MockSource {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn stream(
            &self,
            _projection: Option<Vec<usize>>,
            _filters: Vec<Expr>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            // Just testing trait implementation
            Err(DataFusionError::NotImplemented("mock".to_string()))
        }
    }

    #[test]
    fn test_stream_source_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let source = MockSource {
            schema: Arc::clone(&schema),
        };

        assert_eq!(source.schema(), schema);
        assert_eq!(source.schema().fields().len(), 2);
    }

    #[test]
    fn test_supports_filters_default() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let source = MockSource { schema };

        // Default implementation returns all false
        let filters = vec![Expr::Literal(
            datafusion_common::ScalarValue::Int64(Some(1)),
            None,
        )];
        let supported = source.supports_filters(&filters);
        assert_eq!(supported, vec![false]);
    }

    #[test]
    fn test_sort_column_creation() {
        let col = SortColumn::ascending("event_time");
        assert_eq!(col.name, "event_time");
        assert!(!col.descending);
        assert!(!col.nulls_first);

        let col = SortColumn::descending("price");
        assert_eq!(col.name, "price");
        assert!(col.descending);
        assert!(!col.nulls_first);

        let col = SortColumn::ascending("ts").with_nulls_first(true);
        assert!(!col.descending);
        assert!(col.nulls_first);
    }

    #[test]
    fn test_stream_source_default_ordering() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let source = MockSource { schema };

        // Default implementation returns None
        assert!(source.output_ordering().is_none());
    }

    #[test]
    fn test_stream_source_with_ordering() {
        #[derive(Debug)]
        struct OrderedSource {
            schema: SchemaRef,
        }

        #[async_trait]
        impl StreamSource for OrderedSource {
            fn schema(&self) -> SchemaRef {
                Arc::clone(&self.schema)
            }

            fn stream(
                &self,
                _projection: Option<Vec<usize>>,
                _filters: Vec<Expr>,
            ) -> Result<SendableRecordBatchStream, DataFusionError> {
                Err(DataFusionError::NotImplemented("mock".to_string()))
            }

            fn output_ordering(&self) -> Option<Vec<SortColumn>> {
                Some(vec![SortColumn::ascending("event_time")])
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Int64,
            false,
        )]));
        let source = OrderedSource { schema };
        let ordering = source.output_ordering().unwrap();
        assert_eq!(ordering.len(), 1);
        assert_eq!(ordering[0].name, "event_time");
        assert!(!ordering[0].descending);
    }
}
