//! ASOF join operator for the `OperatorGraph`.
//!
//! Buffers right-side data across execution cycles so that left events can match
//! against the full right-side history (up to watermark-driven eviction).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_sql::parser::join_parser::AsofSqlDirection;
use laminar_sql::translator::AsofJoinTranslatorConfig;

use crate::asof_batch::{execute_asof_join_with_state, AsofBufferCheckpoint, AsofRightBuffer};
use crate::error::DbError;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

/// Bump on any wire-format change to `AsofBufferCheckpoint`.
const ASOF_CHECKPOINT_VERSION: u8 = 1;

fn partial_apply(error: DbError) -> DbError {
    if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
        error
    } else {
        DbError::StatefulOperatorPartialApply(format!(
            "ASOF join may have changed right-side state before the cycle failed: {error}"
        ))
    }
}

fn classify_after_apply(state_changed: bool, error: DbError) -> DbError {
    if state_changed {
        partial_apply(error)
    } else {
        error
    }
}

pub(crate) struct AsofJoinOperator {
    config: AsofJoinTranslatorConfig,
    projection: ProjectingJoinState,
    right_buffer: AsofRightBuffer,
    last_evicted_watermark: i64,
    // Captured from the first non-empty right batch so a later cycle with an
    // empty right buffer can still emit left rows with null right columns.
    right_schema: Option<SchemaRef>,
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
            right_schema: None,
        }
    }
}

#[async_trait]
impl GraphOperator for AsofJoinOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::fixed(
            crate::operator::capability::OperatorImplementation::AsofJoin,
        )
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let right_batches = inputs.get(1).map_or(&[][..], Vec::as_slice);

        let admitted_rows = self.right_buffer.ingest(
            right_batches,
            &self.config.key_column,
            &self.config.right_time_column,
        )?;

        let mut learned_schema = false;
        if self.right_schema.is_none() {
            if let Some(b) = right_batches.first() {
                self.right_schema = Some(b.schema());
                learned_schema = true;
            }
        }
        let state_changed = admitted_rows || learned_schema;

        // Join before evicting: a batch's rows can still backward-match right
        // rows whose timestamps they themselves set the watermark past.
        let output = if left_batches.is_empty() {
            Vec::new()
        } else {
            let joined = execute_asof_join_with_state(
                left_batches,
                &self.right_buffer,
                &self.config,
                self.right_schema.as_ref(),
            )
            .map_err(|error| classify_after_apply(state_changed, error))?;
            if joined.num_rows() == 0 {
                Vec::new()
            } else {
                self.projection
                    .apply(vec![joined])
                    .await
                    .map_err(|error| classify_after_apply(state_changed, error))?
            }
        };

        // Prune: Backward/Nearest keep the latest right <= left_wm per key;
        // bounded tolerance also evicts rows below left_wm - tol. Forward drops
        // everything below left_wm. Driving off the watermark (not tolerance)
        // bounds memory even when tolerance is None.
        let left_wm = watermarks.first().copied().unwrap_or(i64::MIN);
        if left_wm > self.last_evicted_watermark {
            match self.config.direction {
                AsofSqlDirection::Forward => {
                    self.right_buffer
                        .evict_before(left_wm)
                        .map_err(partial_apply)?;
                }
                AsofSqlDirection::Backward | AsofSqlDirection::Nearest => {
                    self.right_buffer
                        .evict_superseded(left_wm)
                        .map_err(partial_apply)?;
                    if let Some(tol) = self
                        .config
                        .tolerance
                        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
                    {
                        self.right_buffer
                            .evict_before(left_wm.saturating_sub(tol))
                            .map_err(partial_apply)?;
                    }
                }
            }
            self.last_evicted_watermark = left_wm;
        }

        Ok(output)
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

        // Version in the trailer so the rkyv body stays at offset 0.
        let mut data = Vec::with_capacity(body.len() + 1);
        data.extend_from_slice(&body);
        data.push(ASOF_CHECKPOINT_VERSION);

        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let Some((&version, body)) = checkpoint.data.split_last() else {
            return Err(DbError::Checkpoint(format!(
                "ASOF join [{}]: checkpoint empty (missing version trailer)",
                self.projection.op_name
            )));
        };
        if version != ASOF_CHECKPOINT_VERSION {
            return Err(DbError::Checkpoint(format!(
                "ASOF join [{}]: unsupported checkpoint version {version} (expected {ASOF_CHECKPOINT_VERSION})",
                self.projection.op_name
            )));
        }

        let cp: AsofBufferCheckpoint =
            rkyv::from_bytes::<AsofBufferCheckpoint, rkyv::rancor::Error>(body).map_err(|e| {
                DbError::Checkpoint(format!(
                    "ASOF join [{}]: checkpoint deserialization: {e}",
                    self.projection.op_name
                ))
            })?;

        let (buffer, last_wm) = AsofRightBuffer::from_checkpoint(&cp).map_err(|error| {
            DbError::Checkpoint(format!(
                "ASOF join [{}]: checkpoint restore: {error}",
                self.projection.op_name
            ))
        })?;
        self.right_buffer = buffer;
        self.last_evicted_watermark = last_wm;
        Ok(())
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

    fn trades_without_key_column() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Float64Array::from(vec![150.0])),
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

    fn quotes_without_time_column() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("bid", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL"])),
                Arc::new(Float64Array::from(vec![149.0])),
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

    fn decoded_checkpoint(op: &mut AsofJoinOperator) -> AsofBufferCheckpoint {
        let checkpoint = op.checkpoint().unwrap().unwrap();
        let (_, body) = checkpoint.data.split_last().unwrap();
        rkyv::from_bytes::<AsofBufferCheckpoint, rkyv::rancor::Error>(body).unwrap()
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

    #[tokio::test]
    async fn pre_apply_right_validation_failure_leaves_asof_state_unchanged() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let error = op
            .process(&[vec![], vec![quotes_without_time_column()]], &[0, 0])
            .await
            .expect_err("missing right time column must fail before ingest");

        assert!(!error.requires_pipeline_recovery());
        assert!(!error.requires_pipeline_halt());
        assert!(!matches!(error, DbError::StatefulOperatorPartialApply(_)));
        assert!(op.right_schema.is_none());

        let decoded = decoded_checkpoint(&mut op);
        assert!(decoded.right_buffer_ipc.is_empty());
        assert!(decoded.index_entries.is_empty());
    }

    #[tokio::test]
    async fn pre_apply_right_validation_failure_preserves_prior_asof_state() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let before = decoded_checkpoint(&mut op);

        let error = op
            .process(&[vec![], vec![quotes_without_time_column()]], &[0, 0])
            .await
            .expect_err("malformed right input must not disturb prior state");
        assert!(!error.requires_pipeline_recovery());

        let after = decoded_checkpoint(&mut op);
        assert_eq!(before.right_buffer_ipc, after.right_buffer_ipc);
        let mut before_entries = before.index_entries;
        let mut after_entries = after.index_entries;
        before_entries.sort_unstable();
        after_entries.sort_unstable();
        assert_eq!(before_entries, after_entries);
    }

    #[tokio::test]
    async fn left_only_failure_after_prior_asof_state_remains_ordinary() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();

        let error = op
            .process(&[vec![trades_without_key_column()], vec![]], &[0, 0])
            .await
            .expect_err("left validation must fail without changing retained right state");

        assert!(!error.requires_pipeline_recovery());
        assert!(!error.requires_pipeline_halt());
        assert!(!matches!(error, DbError::StatefulOperatorPartialApply(_)));

        let decoded = decoded_checkpoint(&mut op);
        assert_eq!(decoded.index_entries.len(), 2);
    }

    #[tokio::test]
    async fn post_projection_failure_requires_recovery_after_asof_state_admission() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new(
            "test_asof",
            test_config(),
            Some(Arc::from("SELECT missing FROM __asof_tmp")),
            ctx,
        );

        let error = op
            .process(&[vec![trades_batch()], vec![quotes_batch()]], &[0, 0])
            .await
            .expect_err("invalid projection must fail after right-state admission");

        assert!(matches!(
            &error,
            DbError::StatefulOperatorPartialApply(message)
                if message.contains("may have changed right-side state")
        ));
        assert!(error.requires_pipeline_recovery());

        // Forensic inspection only: production recovery propagation prevents checkpoint admission
        // after this error (covered by the coordinator recovery-exclusion test from Cycle 42).
        let decoded = decoded_checkpoint(&mut op);
        assert!(!decoded.right_buffer_ipc.is_empty());
        assert_eq!(decoded.index_entries.len(), 2);
    }

    #[test]
    fn asof_partial_apply_preserves_stronger_dispositions() {
        let recovery = partial_apply(DbError::Checkpoint("injected recovery".into()));
        assert!(matches!(recovery, DbError::Checkpoint(_)));

        let partial_send = partial_apply(DbError::ShufflePartialSend("injected recovery".into()));
        assert!(matches!(partial_send, DbError::ShufflePartialSend(_)));

        let halt = partial_apply(DbError::BackpressureFail("injected halt".into()));
        assert!(matches!(halt, DbError::BackpressureFail(_)));

        let terminal = partial_apply(DbError::ShuffleTerminal("injected halt".into()));
        assert!(matches!(terminal, DbError::ShuffleTerminal(_)));
    }

    #[test]
    fn test_name() {
        let ctx = laminar_sql::create_session_context();
        let op = AsofJoinOperator::new("my_asof_query", test_config(), None, ctx);
        assert_eq!(&*op.projection.op_name, "my_asof_query");
    }
}
