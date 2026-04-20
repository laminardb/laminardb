//! Temporal probe join operator for the `OperatorGraph`.
//!
//! Wraps `crate::temporal_probe::TemporalProbeState` and the
//! `execute_temporal_probe_cycle` function. The operator is **stateful**
//! across cycles: it buffers right-side reference data and pending probes
//! for offsets not yet resolvable. Probes are emitted when the watermark
//! advances past their probe timestamp.

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_sql::translator::TemporalProbeConfig;

use crate::error::DbError;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};
use crate::temporal_probe::{
    execute_temporal_probe_cycle, TemporalProbeCheckpoint, TemporalProbeState,
};

pub(crate) struct TemporalProbeJoinOperator {
    config: TemporalProbeConfig,
    state: TemporalProbeState,
    projection: ProjectingJoinState,
}

impl TemporalProbeJoinOperator {
    pub(crate) fn new(
        name: &str,
        config: TemporalProbeConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self {
            config,
            state: TemporalProbeState::new(),
            projection: ProjectingJoinState::new(name, ctx, projection_sql, "__temporal_probe_tmp"),
        }
    }
}

#[async_trait]
impl GraphOperator for TemporalProbeJoinOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let right_batches = inputs.get(1).map_or(&[][..], Vec::as_slice);
        let watermark = watermarks.iter().copied().min().unwrap_or(i64::MIN);

        let join_result = execute_temporal_probe_cycle(
            &mut self.state,
            left_batches,
            right_batches,
            &self.config,
            watermark,
        )?;

        self.projection.apply(join_result).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let cp = self.state.snapshot_checkpoint()?;

        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&cp)
            .map(|v| v.to_vec())
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "temporal probe [{}]: checkpoint serialization: {e}",
                    self.projection.op_name
                ))
            })?;

        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let cp: TemporalProbeCheckpoint =
            rkyv::from_bytes::<TemporalProbeCheckpoint, rkyv::rancor::Error>(&checkpoint.data)
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "temporal probe [{}]: checkpoint deserialization: {e}",
                        self.projection.op_name
                    ))
                })?;

        self.state = TemporalProbeState::from_checkpoint(&cp)?;
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
    use laminar_sql::translator::ProbeOffsetSpec;

    fn test_config() -> TemporalProbeConfig {
        TemporalProbeConfig::new(
            "trades".into(),
            "market_data".into(),
            None,
            None,
            vec!["symbol".into()],
            "ts".into(),
            "mts".into(),
            &ProbeOffsetSpec::List(vec![0, 5000]),
            "p".into(),
        )
    }

    fn left_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL"])),
                Arc::new(Int64Array::from(vec![100_000])),
                Arc::new(Float64Array::from(vec![152.5])),
            ],
        )
        .unwrap()
    }

    fn right_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("mts", DataType::Int64, false),
            Field::new("mprice", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "AAPL"])),
                Arc::new(Int64Array::from(vec![100_000, 105_000])),
                Arc::new(Float64Array::from(vec![150.0, 155.0])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_basic_temporal_probe() {
        let ctx = laminar_sql::create_session_context();
        let mut op = TemporalProbeJoinOperator::new("test_probe", test_config(), None, ctx);

        let result = op
            .process(
                &[vec![left_batch()], vec![right_batch()]],
                &[110_000, 110_000],
            )
            .await
            .unwrap();

        // 1 trade × 2 offsets = 2 rows (both resolved since watermark=110000)
        let total: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_checkpoint_roundtrip() {
        let ctx = laminar_sql::create_session_context();
        let mut op = TemporalProbeJoinOperator::new("test_probe", test_config(), None, ctx.clone());

        // Process with low watermark: only offset=0 resolves
        let _ = op
            .process(
                &[vec![left_batch()], vec![right_batch()]],
                &[102_000, 102_000],
            )
            .await
            .unwrap();

        // Checkpoint
        let cp = op.checkpoint().unwrap().expect("should have state");
        assert!(!cp.data.is_empty());

        // Restore into a new operator
        let mut op2 = TemporalProbeJoinOperator::new("test_probe", test_config(), None, ctx);
        op2.restore(cp).unwrap();

        // Advance watermark to resolve remaining
        let result = op2
            .process(&[vec![], vec![]], &[110_000, 110_000])
            .await
            .unwrap();
        let total: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1); // offset=5000 resolves
    }

    #[tokio::test]
    async fn test_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        let mut op = TemporalProbeJoinOperator::new("test_probe", test_config(), None, ctx);

        let result = op.process(&[], &[0]).await.unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_estimated_state_bytes() {
        let ctx = laminar_sql::create_session_context();
        let op = TemporalProbeJoinOperator::new("test_probe", test_config(), None, ctx);
        assert_eq!(op.estimated_state_bytes(), 0);
    }
}
