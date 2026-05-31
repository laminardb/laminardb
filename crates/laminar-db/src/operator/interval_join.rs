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

#[cfg(feature = "cluster-unstable")]
use crate::operator::sql_query::ClusterShuffleConfig;
#[cfg(feature = "cluster-unstable")]
use crate::key_column::{extract_key_column, extract_column_as_timestamps};

use crate::aggregate_state::JoinStateCheckpoint;
use crate::error::DbError;
use crate::interval_join::{execute_interval_join_cycle, IntervalJoinState};
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

pub(crate) struct IntervalJoinOperator {
    config: StreamJoinConfig,
    state: IntervalJoinState,
    projection: ProjectingJoinState,
    #[cfg(feature = "cluster-unstable")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
}

impl IntervalJoinOperator {
    pub(crate) fn new(
        name: &str,
        config: StreamJoinConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self {
            config,
            state: IntervalJoinState::new(),
            projection: ProjectingJoinState::new(name, ctx, projection_sql, "__interval_tmp"),
            #[cfg(feature = "cluster-unstable")]
            cluster_shuffle: None,
        }
    }

    #[cfg(feature = "cluster-unstable")]
    pub(crate) fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        self.cluster_shuffle = Some(config);
    }

    #[cfg(feature = "cluster-unstable")]
    async fn repartition_side(
        &self,
        batches: &[RecordBatch],
        key_name: &str,
        stage_name: &str,
        cfg: &ClusterShuffleConfig,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let vnode_count = cfg.registry.vnode_count();
        let mut local: Vec<RecordBatch> = Vec::new();
        let mut outbound: Vec<(u64, laminar_core::shuffle::ShuffleMessage)> = Vec::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let key_idx = batch.schema().index_of(key_name).map_err(|e| {
                DbError::Pipeline(format!(
                    "interval join [{}]: key column '{}' not found in schema: {e}",
                    self.projection.op_name, key_name
                ))
            })?;
            let key_indices = vec![key_idx];
            let vnodes = laminar_core::shuffle::row_vnodes(batch, &key_indices, vnode_count);
            for &v in &vnodes {
                let owner = cfg.registry.owner(v);
                if owner.is_unassigned() {
                    return Err(DbError::Pipeline(format!(
                        "interval join [{}]: shuffle vnode {v} is unassigned — refusing to drop rows",
                        self.projection.op_name
                    )));
                }
            }

            let (local_slices, remote_slices) = laminar_core::shuffle::slice_batch_by_targets(
                batch,
                &vnodes,
                &cfg.registry,
                cfg.self_id,
            );

            for (_v, slice) in local_slices {
                local.push(slice);
            }

            for (owner, slice) in remote_slices {
                outbound.push((
                    owner.0,
                    laminar_core::shuffle::ShuffleMessage::VnodeData(stage_name.to_string(), 0, slice),
                ));
            }
        }

        for (peer, msg) in outbound {
            cfg.sender.send_to(peer, &msg).await.map_err(|e| {
                DbError::Pipeline(format!(
                    "interval join [{}]: shuffle send to peer {peer}: {e}",
                    self.projection.op_name
                ))
            })?;
        }

        for batch in cfg.receiver.drain_vnode_data_for(stage_name) {
            if batch.num_rows() > 0 {
                local.push(batch);
            }
        }

        Ok(local)
    }
}

#[async_trait]
impl GraphOperator for IntervalJoinOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        #[cfg(feature = "cluster-unstable")]
        let (left_batches_local, right_batches_local) = if let Some(ref cfg) = self.cluster_shuffle {
            let left_stage = format!("{}::left", self.projection.op_name);
            let right_stage = format!("{}::right", self.projection.op_name);
            let left = self.repartition_side(
                inputs.first().map_or(&[][..], Vec::as_slice),
                &self.config.left_key,
                &left_stage,
                cfg,
            ).await?;
            let right = self.repartition_side(
                inputs.get(1).map_or(&[][..], Vec::as_slice),
                &self.config.right_key,
                &right_stage,
                cfg,
            ).await?;
            (left, right)
        } else {
            (
                inputs.first().map_or(&[][..], Vec::as_slice).to_vec(),
                inputs.get(1).map_or(&[][..], Vec::as_slice).to_vec(),
            )
        };

        #[cfg(not(feature = "cluster-unstable"))]
        let (left_batches_local, right_batches_local) = (
            inputs.first().map_or(&[][..], Vec::as_slice).to_vec(),
            inputs.get(1).map_or(&[][..], Vec::as_slice).to_vec(),
        );

        let left_wm = watermarks.first().copied().unwrap_or(i64::MIN);
        let right_wm = watermarks.get(1).copied().unwrap_or(i64::MIN);

        let join_result = execute_interval_join_cycle(
            &mut self.state,
            &left_batches_local,
            &right_batches_local,
            &self.config,
            left_wm,
            right_wm,
        )?;

        self.projection.apply(join_result).await
    }

    #[cfg(feature = "cluster-unstable")]
    async fn ingest_shuffle(
        &mut self,
        stage: &str,
        batch: RecordBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        if self.cluster_shuffle.is_none() {
            return Ok(());
        }
        let op_name = &self.projection.op_name;
        if stage == format!("{}::left", op_name) {
            if let Some(neg) = crate::changelog_filter::extract_negative_events(&batch)? {
                let keys = extract_key_column(&neg, &self.config.left_key)?;
                let timestamps = extract_column_as_timestamps(&neg, &self.config.left_time_column)?;
                for (i, &ts) in timestamps.iter().enumerate() {
                    if let Some(kh) = keys.hash_at(i) {
                        self.state
                            .left
                            .remove_by_key_ts(kh, ts, &keys, i, &self.config.left_key)?;
                    }
                }
            }
            let pos = crate::changelog_filter::filter_positive_events(&batch)?;
            if pos.num_rows() > 0 {
                self.state.left.add_batch(&pos, &self.config.left_key, &self.config.left_time_column)?;
            }
        } else if stage == format!("{}::right", op_name) {
            if let Some(neg) = crate::changelog_filter::extract_negative_events(&batch)? {
                let keys = extract_key_column(&neg, &self.config.right_key)?;
                let timestamps = extract_column_as_timestamps(&neg, &self.config.right_time_column)?;
                for (i, &ts) in timestamps.iter().enumerate() {
                    if let Some(kh) = keys.hash_at(i) {
                        self.state
                            .right
                            .remove_by_key_ts(kh, ts, &keys, i, &self.config.right_key)?;
                    }
                }
            }
            let pos = crate::changelog_filter::filter_positive_events(&batch)?;
            if pos.num_rows() > 0 {
                self.state.right.add_batch(&pos, &self.config.right_key, &self.config.right_time_column)?;
            }
        }
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let cp = self.state.snapshot_checkpoint(
            &self.config.left_key,
            &self.config.left_time_column,
            &self.config.right_key,
            &self.config.right_time_column,
        )?;

        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&cp)
            .map(|v| v.to_vec())
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "interval join [{}]: checkpoint serialization: {e}",
                    self.projection.op_name
                ))
            })?;

        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let cp: JoinStateCheckpoint =
            rkyv::from_bytes::<JoinStateCheckpoint, rkyv::rancor::Error>(&checkpoint.data)
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "interval join [{}]: checkpoint deserialization: {e}",
                        self.projection.op_name
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

        let result = op
            .process(&[vec![left], vec![right]], &[0, 0])
            .await
            .unwrap();

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
        let result = op.process(&[vec![left], vec![]], &[0, 0]).await.unwrap();
        assert!(result.is_empty());

        // Cycle 2: right data arrives, should match the buffered left
        let right = right_batch(&["A"], &[150], &[1.0]);
        let result = op.process(&[vec![], vec![right]], &[0, 0]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        let result = op.process(&[], &[0]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_checkpoint_roundtrip() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx.clone());

        // Buffer some data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);
        let _ = op
            .process(&[vec![left], vec![right]], &[50, 50])
            .await
            .unwrap();

        // Checkpoint
        let cp = op.checkpoint().unwrap().expect("should have state");
        assert!(!cp.data.is_empty());

        // Restore into a new operator
        let mut op2 = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
        op2.restore(cp).unwrap();

        // New right data should match the restored left
        let right2 = right_batch(&["A"], &[120], &[2.0]);
        let result = op2
            .process(&[vec![], vec![right2]], &[50, 50])
            .await
            .unwrap();
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
        assert_eq!(&*op.projection.op_name, "my_interval_join");
    }
}
