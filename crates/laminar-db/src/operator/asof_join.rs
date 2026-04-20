//! ASOF join operator for the `OperatorGraph`.
//!
//! Buffers right-side data across execution cycles so that left events can match
//! against the full right-side history (up to watermark-driven eviction).

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_sql::translator::AsofJoinTranslatorConfig;

use crate::asof_batch::{execute_asof_join_with_state, AsofBufferCheckpoint, AsofRightBuffer};
use crate::error::DbError;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

/// Version byte prefixed to the rkyv payload so bad/old checkpoints fail
/// loudly instead of corrupting state. Bump on any wire-format change to
/// `AsofBufferCheckpoint`.
const ASOF_CHECKPOINT_VERSION: u8 = 1;

pub(crate) struct AsofJoinOperator {
    config: AsofJoinTranslatorConfig,
    projection: ProjectingJoinState,
    right_buffer: AsofRightBuffer,
    last_evicted_watermark: i64,
}

impl AsofJoinOperator {
    pub(crate) fn new(
        name: &str,
        config: AsofJoinTranslatorConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self {
            config,
            projection: ProjectingJoinState::new(name, ctx, projection_sql, "__asof_tmp"),
            right_buffer: AsofRightBuffer::default(),
            last_evicted_watermark: i64::MIN,
        }
    }
}

#[async_trait]
impl GraphOperator for AsofJoinOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let right_batches = inputs.get(1).map_or(&[][..], Vec::as_slice);

        self.right_buffer.ingest(
            right_batches,
            &self.config.key_column,
            &self.config.right_time_column,
        )?;

        // Right rows are needed until no future left event can match them.
        // Future left events have ts >= left_wm, so right rows at
        // ts < left_wm - tolerance are unreachable. Crossed logic:
        // right eviction depends on LEFT watermark.
        let max_lookback_ms = self.config.tolerance.map_or(i64::MAX, |d| {
            i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
        });
        let left_wm = watermarks.first().copied().unwrap_or(i64::MIN);
        let cutoff = left_wm.saturating_sub(max_lookback_ms);
        if cutoff > self.last_evicted_watermark {
            self.right_buffer.evict_before(cutoff)?;
            self.last_evicted_watermark = cutoff;
        }

        if left_batches.is_empty() {
            return Ok(Vec::new());
        }

        let joined = execute_asof_join_with_state(left_batches, &self.right_buffer, &self.config)?;

        if joined.num_rows() == 0 {
            return Ok(Vec::new());
        }

        self.projection.apply(vec![joined]).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let cp = self
            .right_buffer
            .snapshot_checkpoint(self.last_evicted_watermark)?;

        let body = rkyv::to_bytes::<rkyv::rancor::Error>(&cp).map_err(|e| {
            DbError::Pipeline(format!(
                "ASOF join [{}]: checkpoint serialization: {e}",
                self.projection.op_name
            ))
        })?;

        // Version goes in the trailer so rkyv body stays at offset 0
        // (preserves the alignment invariant `from_bytes` requires).
        let mut data = Vec::with_capacity(body.len() + 1);
        data.extend_from_slice(&body);
        data.push(ASOF_CHECKPOINT_VERSION);

        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let Some((&version, body)) = checkpoint.data.split_last() else {
            return Err(DbError::Pipeline(format!(
                "ASOF join [{}]: checkpoint empty (missing version trailer)",
                self.projection.op_name
            )));
        };
        if version != ASOF_CHECKPOINT_VERSION {
            return Err(DbError::Pipeline(format!(
                "ASOF join [{}]: unsupported checkpoint version {version} (expected {ASOF_CHECKPOINT_VERSION})",
                self.projection.op_name
            )));
        }

        let cp: AsofBufferCheckpoint =
            rkyv::from_bytes::<AsofBufferCheckpoint, rkyv::rancor::Error>(body).map_err(|e| {
                DbError::Pipeline(format!(
                    "ASOF join [{}]: checkpoint deserialization: {e}",
                    self.projection.op_name
                ))
            })?;

        let (buffer, last_wm) = AsofRightBuffer::from_checkpoint(&cp)?;
        self.right_buffer = buffer;
        self.last_evicted_watermark = last_wm;
        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        self.right_buffer.estimated_size_bytes()
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
            .process(&[vec![trades_batch()], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_cross_cycle_match() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        // Cycle 1: right data only
        let result = op
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        assert!(result.is_empty());

        // Cycle 2: left data arrives — should match against buffered right
        let result = op
            .process(&[vec![trades_batch()], vec![]], &[0, 0])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_eviction_on_watermark_advance() {
        let mut config = test_config();
        config.tolerance = Some(std::time::Duration::from_millis(50));
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", config, None, ctx);

        // Buffer right data at ts=90 and ts=140
        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();

        // Advance watermark to 200 → cutoff = 200 - 50 = 150
        // quote@90 (< 150) evicted, quote@140 (< 150) evicted
        op.process(&[vec![], vec![]], &[200, 200]).await.unwrap();

        // Left at ts=100: backward match needs quote@90, but it's evicted
        let result = op
            .process(&[vec![trades_batch()], vec![]], &[200, 200])
            .await
            .unwrap();

        // AAPL trade@100 can't match (quote@90 evicted), GOOG trade@150 can't match (quote@140 evicted)
        // Left join: both emitted with null right columns
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
        // Right-side columns (quote_ts, bid) should all be null
        let right_start = 3; // After symbol, trade_ts, price
        for col_idx in right_start..result[0].num_columns() {
            assert!(
                result[0].column(col_idx).is_null(0),
                "col {col_idx} row 0 should be null"
            );
            assert!(
                result[0].column(col_idx).is_null(1),
                "col {col_idx} row 1 should be null"
            );
        }
    }

    #[tokio::test]
    async fn test_checkpoint_roundtrip() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());

        // Buffer right data
        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();

        // Checkpoint
        let cp = op.checkpoint().unwrap().expect("should have state");
        assert!(!cp.data.is_empty());

        // Restore into new operator
        let mut op2 = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        op2.restore(cp).unwrap();

        // Left data should match against restored right buffer
        let result = op2
            .process(&[vec![trades_batch()], vec![]], &[0, 0])
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
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let result = op.process(&[], &[0]).await.unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_name() {
        let ctx = laminar_sql::create_session_context();
        let op = AsofJoinOperator::new("my_asof_query", test_config(), None, ctx);
        assert_eq!(&*op.projection.op_name, "my_asof_query");
    }

    #[test]
    fn test_estimated_state_bytes_starts_zero() {
        let ctx = laminar_sql::create_session_context();
        let op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        assert_eq!(op.estimated_state_bytes(), 0);
    }
}
