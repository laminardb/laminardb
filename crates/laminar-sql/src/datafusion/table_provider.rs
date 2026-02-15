//! Streaming table provider for `DataFusion` integration
//!
//! This module provides `StreamingTableProvider` which implements `DataFusion`'s
//! `TableProvider` trait, allowing streaming sources to be registered as
//! tables in a `SessionContext` and queried with SQL.

use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};

use super::exec::StreamingScanExec;
use super::source::StreamSourceRef;

/// A `DataFusion` table provider backed by a streaming source.
///
/// This allows streaming sources to be registered as tables in `DataFusion`'s
/// `SessionContext` and queried using SQL. The provider handles:
///
/// - Schema exposure to `DataFusion`'s catalog
/// - Projection pushdown to the source
/// - Filter pushdown when supported by the source
///
/// # Usage
///
/// ```rust,ignore
/// let source = Arc::new(ChannelStreamSource::new(schema));
/// let provider = StreamingTableProvider::new("events", source);
/// ctx.register_table("events", Arc::new(provider))?;
///
/// let df = ctx.sql("SELECT * FROM events WHERE id > 100").await?;
/// ```
#[derive(Debug)]
pub struct StreamingTableProvider {
    /// Table name
    name: String,
    /// The underlying streaming source
    source: StreamSourceRef,
}

impl StreamingTableProvider {
    /// Creates a new streaming table provider.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the table (used for display/debugging)
    /// * `source` - The streaming source backing this table
    #[must_use]
    pub fn new(name: impl Into<String>, source: StreamSourceRef) -> Self {
        Self {
            name: name.into(),
            source,
        }
    }

    /// Returns the table name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the underlying streaming source.
    #[must_use]
    pub fn source(&self) -> &StreamSourceRef {
        &self.source
    }
}

#[async_trait]
impl TableProvider for StreamingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    fn table_type(&self) -> TableType {
        // Streaming tables behave like base tables but are read-only
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        // Ask the source which filters it can handle
        let expr_refs: Vec<Expr> = filters.iter().map(|e| (*e).clone()).collect();
        let supported = self.source.supports_filters(&expr_refs);

        Ok(supported
            .into_iter()
            .map(|s| {
                if s {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Determine which filters the source supports
        let supported = self.source.supports_filters(filters);
        let pushed_filters: Vec<Expr> = filters
            .iter()
            .zip(supported.iter())
            .filter_map(|(f, &s)| if s { Some(f.clone()) } else { None })
            .collect();

        Ok(Arc::new(StreamingScanExec::new(
            Arc::clone(&self.source),
            projection.cloned(),
            pushed_filters,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::source::StreamSource;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::SendableRecordBatchStream;

    #[derive(Debug)]
    struct MockSource {
        schema: SchemaRef,
        supports_eq_filter: bool,
    }

    #[async_trait]
    impl StreamSource for MockSource {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn stream(
            &self,
            _partition: usize,
            _projection: Option<Vec<usize>>,
            _filters: Vec<Expr>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            Err(DataFusionError::NotImplemented("mock".to_string()))
        }

        fn supports_filters(&self, filters: &[Expr]) -> Vec<bool> {
            filters
                .iter()
                .map(|f| {
                    if self.supports_eq_filter {
                        // Only support equality filters for testing
                        matches!(f, Expr::BinaryExpr(e) if e.op == datafusion_expr::Operator::Eq)
                    } else {
                        false
                    }
                })
                .collect()
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_table_provider_schema() {
        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource {
            schema: Arc::clone(&schema),
            supports_eq_filter: false,
        });
        let provider = StreamingTableProvider::new("test_table", source);

        assert_eq!(provider.schema(), schema);
        assert_eq!(provider.name(), "test_table");
    }

    #[test]
    fn test_table_provider_type() {
        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource {
            schema,
            supports_eq_filter: false,
        });
        let provider = StreamingTableProvider::new("test", source);

        assert_eq!(provider.table_type(), TableType::Base);
    }

    #[test]
    fn test_filter_pushdown_unsupported() {
        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource {
            schema,
            supports_eq_filter: false,
        });
        let provider = StreamingTableProvider::new("test", source);

        let filter = Expr::Literal(datafusion_common::ScalarValue::Int64(Some(1)), None);
        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(
            result[0],
            TableProviderFilterPushDown::Unsupported
        ));
    }

    #[test]
    fn test_filter_pushdown_supported() {
        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource {
            schema,
            supports_eq_filter: true,
        });
        let provider = StreamingTableProvider::new("test", source);

        // Create an equality filter: id = 1
        let filter = Expr::BinaryExpr(datafusion_expr::BinaryExpr {
            left: Box::new(Expr::Column(datafusion_common::Column::new_unqualified(
                "id",
            ))),
            op: datafusion_expr::Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion_common::ScalarValue::Int64(Some(1)),
                None,
            )),
        });
        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
    }

    #[tokio::test]
    async fn test_scan_creates_exec() {
        use datafusion::prelude::SessionContext;

        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource {
            schema: Arc::clone(&schema),
            supports_eq_filter: false,
        });
        let provider = StreamingTableProvider::new("test", source);

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let exec = provider
            .scan(&session_state, None, &[], None)
            .await
            .unwrap();

        // Verify it's a StreamingScanExec
        assert!(exec.as_any().is::<StreamingScanExec>());
        assert_eq!(exec.schema(), schema);
    }

    #[tokio::test]
    async fn test_scan_with_projection() {
        use datafusion::prelude::SessionContext;

        let schema = test_schema();
        let source: StreamSourceRef = Arc::new(MockSource {
            schema,
            supports_eq_filter: false,
        });
        let provider = StreamingTableProvider::new("test", source);

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let projection = vec![0]; // Only id column
        let exec = provider
            .scan(&session_state, Some(&projection), &[], None)
            .await
            .unwrap();

        let output_schema = exec.schema();
        assert_eq!(output_schema.fields().len(), 1);
        assert_eq!(output_schema.field(0).name(), "id");
    }
}
