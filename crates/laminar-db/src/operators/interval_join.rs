//! Interval join operator for the `OperatorGraph`.
//!
//! Wraps `crate::interval_join::IntervalJoinState` and the standalone
//! `execute_interval_join_cycle` function. The operator is **stateful**
//! across cycles: it buffers left/right rows and matches pairs where
//! `|left_ts - right_ts| <= time_bound_ms`. Expired rows are evicted
//! when the watermark advances.

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_sql::translator::StreamJoinConfig;

use crate::aggregate_state::JoinStateCheckpoint;
use crate::error::DbError;
use crate::interval_join::{execute_interval_join_cycle, IntervalJoinState};
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};
use crate::stream_executor::CompiledPostProjection;

/// Checkpoint format version for interval join state.
pub(crate) struct IntervalJoinOperator {
    op_name: Arc<str>,
    config: StreamJoinConfig,
    state: IntervalJoinState,
    projection_sql: Option<Arc<str>>,
    ctx: SessionContext,
    compiled_post_proj: Option<CompiledPostProjection>,
    post_proj_compile_failed: bool,
}

impl IntervalJoinOperator {
    pub(crate) fn new(
        name: &str,
        config: StreamJoinConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self {
            op_name: Arc::from(name),
            config,
            state: IntervalJoinState::new(),
            projection_sql,
            ctx,
            compiled_post_proj: None,
            post_proj_compile_failed: false,
        }
    }

    async fn apply_projection(
        &mut self,
        join_result: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        super::apply_post_projection(
            &self.ctx,
            &self.op_name,
            "__interval_tmp",
            self.projection_sql.as_deref(),
            &mut self.compiled_post_proj,
            &mut self.post_proj_compile_failed,
            join_result,
        )
        .await
    }
}

#[async_trait]
impl GraphOperator for IntervalJoinOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let right_batches = inputs.get(1).map_or(&[][..], Vec::as_slice);

        let join_result = execute_interval_join_cycle(
            &mut self.state,
            left_batches,
            right_batches,
            &self.config,
            watermark,
        )?;

        self.apply_projection(join_result).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let cp = self.state.snapshot_checkpoint(
            &self.config.left_key,
            &self.config.left_time_column,
            &self.config.right_key,
            &self.config.right_time_column,
        )?;

        let data = serde_json::to_vec(&cp).map_err(|e| {
            DbError::Pipeline(format!(
                "interval join [{}]: checkpoint serialization: {e}",
                self.op_name
            ))
        })?;

        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let cp: JoinStateCheckpoint = serde_json::from_slice(&checkpoint.data).map_err(|e| {
            DbError::Pipeline(format!(
                "interval join [{}]: checkpoint deserialization: {e}",
                self.op_name
            ))
        })?;

        self.state = IntervalJoinState::from_checkpoint(
            &cp,
            &self.config.left_key,
            &self.config.left_time_column,
            &self.config.right_key,
            &self.config.right_time_column,
        )?;

        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        self.state.estimated_size_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_sql::translator::StreamJoinType;
    use std::time::Duration;

    fn test_config() -> StreamJoinConfig {
        StreamJoinConfig {
            left_key: "id".to_string(),
            right_key: "id".to_string(),
            left_time_column: "ts".to_string(),
            right_time_column: "ts".to_string(),
            left_table: "left_stream".to_string(),
            right_table: "right_stream".to_string(),
            time_bound: Duration::from_millis(100),
            join_type: StreamJoinType::Inner,
        }
    }

    fn left_batch(ids: &[&str], timestamps: &[i64], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn right_batch(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_basic_interval_join() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        let left = left_batch(&["A", "B"], &[100, 200], &[10.0, 20.0]);
        let right = right_batch(&["A", "B"], &[110, 250], &[1.0, 2.0]);

        let result = op.process(&[vec![left], vec![right]], 0).await.unwrap();

        // A: |100 - 110| = 10 <= 100 -> match
        // B: |200 - 250| = 50 <= 100 -> match
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_cross_cycle_matching() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        // Cycle 1: only left data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let result = op.process(&[vec![left], vec![]], 0).await.unwrap();
        assert!(result.is_empty());

        // Cycle 2: right data arrives, should match the buffered left
        let right = right_batch(&["A"], &[150], &[1.0]);
        let result = op.process(&[vec![], vec![right]], 0).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        let result = op.process(&[], 0).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_checkpoint_roundtrip() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx.clone());

        // Buffer some data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);
        let _ = op.process(&[vec![left], vec![right]], 50).await.unwrap();

        // Checkpoint
        let cp = op.checkpoint().unwrap().expect("should have state");
        assert!(!cp.data.is_empty());

        // Restore into a new operator
        let mut op2 = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
        op2.restore(cp).unwrap();

        // New right data should match the restored left
        let right2 = right_batch(&["A"], &[120], &[2.0]);
        let result = op2.process(&[vec![], vec![right2]], 50).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[test]
    fn test_estimated_state_bytes() {
        let ctx = laminar_sql::create_session_context();
        let op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
        // Empty state should be zero or very small
        assert_eq!(op.estimated_state_bytes(), 0);
    }

    #[test]
    fn test_name() {
        let ctx = laminar_sql::create_session_context();
        let op = IntervalJoinOperator::new("my_interval_join", test_config(), None, ctx);
        assert_eq!(&*op.op_name, "my_interval_join");
    }
}
