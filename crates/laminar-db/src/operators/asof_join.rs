//! ASOF join operator for the `OperatorGraph`.
//!
//! Wraps `crate::asof_batch::execute_asof_join_batch`, which matches each
//! left row to the closest right row by timestamp within the same key
//! partition. The operator is stateless (no cross-cycle state).

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_sql::translator::AsofJoinTranslatorConfig;

use crate::asof_batch::execute_asof_join_batch;
use crate::error::DbError;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};
use crate::stream_executor::CompiledPostProjection;

/// ASOF join operator.
///
/// Two input ports: `inputs[0]` = left, `inputs[1]` = right.
/// Delegates to the batch-level `execute_asof_join_batch` and optionally
/// applies a post-join projection SQL via `DataFusion`.
pub(crate) struct AsofJoinOperator {
    /// Operator name (query name).
    op_name: Arc<str>,
    /// ASOF join configuration (key, time columns, direction, tolerance).
    config: AsofJoinTranslatorConfig,
    /// Optional post-join projection SQL (rewrites column aliases/expressions).
    projection_sql: Option<Arc<str>>,
    /// `SessionContext` for post-projection SQL execution.
    ctx: SessionContext,
    /// Lazily compiled post-projection (replaces SQL fallback on success).
    compiled_post_proj: Option<CompiledPostProjection>,
    /// Whether compilation was attempted and failed (skip further attempts).
    post_proj_compile_failed: bool,
}

impl AsofJoinOperator {
    /// Create a new ASOF join operator.
    pub(crate) fn new(
        name: &str,
        config: AsofJoinTranslatorConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self {
            op_name: Arc::from(name),
            config,
            projection_sql,
            ctx,
            compiled_post_proj: None,
            post_proj_compile_failed: false,
        }
    }

    async fn apply_projection(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        super::apply_post_projection(
            &self.ctx,
            &self.op_name,
            "__asof_tmp",
            self.projection_sql.as_deref(),
            &mut self.compiled_post_proj,
            &mut self.post_proj_compile_failed,
            batches,
        )
        .await
    }
}

#[async_trait]
impl GraphOperator for AsofJoinOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let right_batches = inputs.get(1).map_or(&[][..], Vec::as_slice);

        if left_batches.is_empty() {
            return Ok(Vec::new());
        }

        let joined = execute_asof_join_batch(left_batches, right_batches, &self.config)?;

        if joined.num_rows() == 0 {
            return Ok(Vec::new());
        }

        self.apply_projection(vec![joined]).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        // Stateless — no checkpoint needed.
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        // Stateless — nothing to restore.
        Ok(())
    }

    fn name(&self) -> &str {
        &self.op_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_sql::parser::join_parser::AsofSqlDirection;
    use laminar_sql::translator::AsofSqlJoinType;

    fn trades_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(Int64Array::from(vec![100, 150])),
                Arc::new(Float64Array::from(vec![150.0, 2800.0])),
            ],
        )
        .unwrap()
    }

    fn quotes_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("quote_ts", DataType::Int64, false),
            Field::new("bid", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(Int64Array::from(vec![90, 140])),
                Arc::new(Float64Array::from(vec![149.0, 2790.0])),
            ],
        )
        .unwrap()
    }

    fn test_config() -> AsofJoinTranslatorConfig {
        AsofJoinTranslatorConfig {
            left_table: "trades".to_string(),
            right_table: "quotes".to_string(),
            key_column: "symbol".to_string(),
            left_time_column: "trade_ts".to_string(),
            right_time_column: "quote_ts".to_string(),
            direction: AsofSqlDirection::Backward,
            tolerance: None,
            join_type: AsofSqlJoinType::Left,
        }
    }

    #[tokio::test]
    async fn test_basic_asof_join() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let result = op
            .process(&[vec![trades_batch()], vec![quotes_batch()]], 0)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_empty_left() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let result = op
            .process(&[vec![], vec![quotes_batch()]], 0)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let result = op.process(&[], 0).await.unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_checkpoint_returns_none() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        assert!(op.checkpoint().unwrap().is_none());
    }

    #[test]
    fn test_name() {
        let ctx = laminar_sql::create_session_context();
        let op = AsofJoinOperator::new("my_asof_query", test_config(), None, ctx);
        assert_eq!(op.name(), "my_asof_query");
    }
}
