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
}
