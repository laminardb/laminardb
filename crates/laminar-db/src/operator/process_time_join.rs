//! Processing-time equi-join: a stateless per-cycle inner join, no watermark.
//!
//! It joins this cycle's left and right batches on the equi-key — what aligns
//! two windowed views closing the same bucket in the same cycle. The cached
//! `DataFusion` `HashJoin` cannot: it memoizes its build side across cycles
//! (`OnceAsync`), so a bucket closing in a later cycle never matches.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use rustc_hash::FxHashMap;

use laminar_sql::translator::StreamJoinConfig;

use crate::error::DbError;
use crate::interval_join::build_output_schema;
use crate::key_column::extract_key_column;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

pub(crate) struct ProcessTimeJoinOperator {
    config: StreamJoinConfig,
    projection: ProjectingJoinState,
}

impl ProcessTimeJoinOperator {
    pub(crate) fn new(
        name: &str,
        config: StreamJoinConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self {
            config,
            // The `FROM` name `build_stream_join_projection_sql` emits (shared
            // with the interval join; registered transiently per `apply`).
            projection: ProjectingJoinState::new(name, ctx, projection_sql, "__interval_tmp"),
        }
    }

    /// Inner-join this cycle's batches on the equi-key. Empty when either side
    /// has no rows (so a minute where only one view closed yields nothing).
    fn join_cycle(
        &self,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let (Some(left_schema), Some(right_schema)) = (
            left_batches.first().map(RecordBatch::schema),
            right_batches.first().map(RecordBatch::schema),
        ) else {
            return Ok(Vec::new());
        };
        let left = concat_batches(&left_schema, left_batches)
            .map_err(|e| DbError::Pipeline(format!("process-time join: concat left: {e}")))?;
        let right = concat_batches(&right_schema, right_batches)
            .map_err(|e| DbError::Pipeline(format!("process-time join: concat right: {e}")))?;
        if left.num_rows() == 0 || right.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let left_keys = extract_key_column(&left, &self.config.left_key)?;
        let right_keys = extract_key_column(&right, &self.config.right_key)?;

        // Build a hash index over the (smaller, by convention right) side, then
        // probe with the left. Collisions are resolved by `keys_equal`.
        let mut index: FxHashMap<u64, Vec<u32>> = FxHashMap::default();
        for j in 0..right.num_rows() {
            if let Some(h) = right_keys.hash_at(j) {
                index
                    .entry(h)
                    .or_default()
                    .push(u32::try_from(j).unwrap_or(u32::MAX));
            }
        }

        let mut left_idx: Vec<u32> = Vec::new();
        let mut right_idx: Vec<u32> = Vec::new();
        for i in 0..left.num_rows() {
            let Some(h) = left_keys.hash_at(i) else {
                continue;
            };
            let Some(candidates) = index.get(&h) else {
                continue;
            };
            for &j in candidates {
                if left_keys.keys_equal(i, &right_keys, j as usize) {
                    left_idx.push(u32::try_from(i).unwrap_or(u32::MAX));
                    right_idx.push(j);
                }
            }
        }
        if left_idx.is_empty() {
            return Ok(Vec::new());
        }

        let out_schema = build_output_schema(&left_schema, &right_schema, &self.config);
        let li = UInt32Array::from(left_idx);
        let ri = UInt32Array::from(right_idx);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
        for col in left.columns() {
            columns.push(gather(col, &li)?);
        }
        for col in right.columns() {
            columns.push(gather(col, &ri)?);
        }
        let batch = RecordBatch::try_new(out_schema, columns)
            .map_err(|e| DbError::Pipeline(format!("process-time join: build output: {e}")))?;
        Ok(vec![batch])
    }
}

fn gather(column: &ArrayRef, indices: &UInt32Array) -> Result<ArrayRef, DbError> {
    take(column.as_ref(), indices, None)
        .map_err(|e| DbError::Pipeline(format!("process-time join: take: {e}")))
}

#[async_trait]
impl GraphOperator for ProcessTimeJoinOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left = inputs.first().map_or(&[][..], Vec::as_slice);
        let right = inputs.get(1).map_or(&[][..], Vec::as_slice);
        let joined = self.join_cycle(left, right)?;
        self.projection.apply(joined).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None) // stateless: nothing carries across cycles
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}
