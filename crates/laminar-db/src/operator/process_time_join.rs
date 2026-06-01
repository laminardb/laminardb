//! Processing-time equi-join: a bounded stateful inner join between two windowed
//! views, joined on their equi-key (e.g. `bucket_start`).
//!
//! The two sides do **not** close a bucket in the same cycle — an upstream AI
//! operator's watermark hold delays one side by its scoring latency — so a
//! stateless per-cycle join would drop buckets. Each side keeps its unmatched
//! rows and matches whenever the second side arrives, via the symmetric hash-join
//! rule (new×buffered, buffered×new, new×new; buffered×buffered already emitted).
//!
//! Retention is **watermark-driven**: a side's buffered rows are evicted once the
//! *opposite* side's watermark has passed them — the partner can no longer arrive
//! — so state is bounded by the real cross-side skew, not a fixed count, and an
//! unmatched row ages out (correct for an inner join). Eviction runs **after** the
//! join, so a row still matches a partner arriving in the same cycle it would age
//! out. `MAX_BUFFERED_ROWS` is a hard memory backstop for when watermarks don't
//! advance (e.g. an idle or watermark-less source). The cached `DataFusion`
//! `HashJoin` can't serve this: it memoizes its build side across cycles
//! (`OnceAsync`).

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use rustc_hash::FxHashMap;

use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};
use laminar_sql::translator::StreamJoinConfig;

use crate::error::DbError;
use crate::interval_join::build_output_schema;
use crate::key_column::extract_key_column;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

/// Hard memory backstop per side: when watermarks don't advance, retention falls
/// back to keeping at most this many newest rows. With watermarks it is rarely
/// reached — the cross-side skew is a handful of windows.
const MAX_BUFFERED_ROWS: usize = 4096;

/// One join side's unmatched rows. Each cycle's arrivals are a chunk tagged with
/// the input watermark at which they arrived; eviction drops whole chunks the
/// opposite side has advanced past, oldest first.
#[derive(Default)]
struct SideBuffer {
    chunks: VecDeque<(i64, RecordBatch)>,
}

impl SideBuffer {
    fn push(&mut self, watermark: i64, batch: RecordBatch) {
        if batch.num_rows() > 0 {
            self.chunks.push_back((watermark, batch));
        }
    }

    /// All buffered rows as one batch to probe against, or `None` when empty.
    fn concat(&self) -> Result<Option<RecordBatch>, DbError> {
        let Some((_, first)) = self.chunks.front() else {
            return Ok(None);
        };
        concat_batches(&first.schema(), self.chunks.iter().map(|(_, b)| b))
            .map(Some)
            .map_err(|e| DbError::Pipeline(format!("process-time join: concat buffer: {e}")))
    }

    /// Evict rows the opposite side has passed (`tag < opposite_watermark`), then
    /// enforce the memory backstop oldest-first.
    fn evict(&mut self, opposite_watermark: i64) {
        while self
            .chunks
            .front()
            .is_some_and(|(tag, _)| *tag < opposite_watermark)
        {
            self.chunks.pop_front();
        }
        // Memory backstop: keep the newest MAX_BUFFERED_ROWS, slicing the oldest
        // chunk rather than dropping a whole (possibly large) batch wholesale.
        let mut rows: usize = self.chunks.iter().map(|(_, b)| b.num_rows()).sum();
        while rows > MAX_BUFFERED_ROWS {
            let overage = rows - MAX_BUFFERED_ROWS;
            let front_rows = match self.chunks.front() {
                Some((_, b)) => b.num_rows(),
                None => break,
            };
            if front_rows <= overage {
                self.chunks.pop_front();
                rows -= front_rows;
            } else if let Some((_, front)) = self.chunks.front_mut() {
                *front = front.slice(overage, front_rows - overage); // drop oldest `overage`
                rows -= overage;
            }
        }
    }

    fn bytes(&self) -> usize {
        self.chunks
            .iter()
            .map(|(_, b)| b.get_array_memory_size())
            .sum()
    }
}

#[cfg(feature = "cluster-unstable")]
use crate::operator::sql_query::ClusterShuffleConfig;

pub(crate) struct ProcessTimeJoinOperator {
    config: StreamJoinConfig,
    left: SideBuffer,
    right: SideBuffer,
    /// The user's SELECT with the join's columns rewritten over the temp table.
    projection: ProjectingJoinState,
    #[cfg(feature = "cluster-unstable")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
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
            left: SideBuffer::default(),
            right: SideBuffer::default(),
            // The `FROM` name `build_stream_join_projection_sql` emits (shared
            // with the interval join; registered transiently per `apply`).
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
                    "process-time join [{}]: key column '{}' not found in schema: {e}",
                    self.projection.op_name, key_name
                ))
            })?;
            let key_indices = vec![key_idx];
            let vnodes = laminar_core::shuffle::row_vnodes(batch, &key_indices, vnode_count);
            for &v in &vnodes {
                let owner = cfg.registry.owner(v);
                if owner.is_unassigned() {
                    return Err(DbError::Pipeline(format!(
                        "process-time join [{}]: shuffle vnode {v} is unassigned — refusing to drop rows",
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
                    laminar_core::shuffle::ShuffleMessage::VnodeData(
                        stage_name.to_string(),
                        0,
                        slice,
                    ),
                ));
            }
        }

        for (peer, msg) in outbound {
            cfg.sender.send_to(peer, &msg).await.map_err(|e| {
                DbError::Pipeline(format!(
                    "process-time join [{}]: shuffle send to peer {peer}: {e}",
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

    /// Inner-join two batches on the equi-key. `None` when either is empty or
    /// nothing matches.
    fn join_pair(
        &self,
        left: &RecordBatch,
        right: &RecordBatch,
    ) -> Result<Option<RecordBatch>, DbError> {
        if left.num_rows() == 0 || right.num_rows() == 0 {
            return Ok(None);
        }
        let left_keys = extract_key_column(left, &self.config.left_key)?;
        let right_keys = extract_key_column(right, &self.config.right_key)?;

        // Hash-index the right side, probe with the left; collisions are resolved
        // by `keys_equal`.
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
            return Ok(None);
        }

        let out_schema = build_output_schema(&left.schema(), &right.schema(), &self.config);
        let (li, ri) = (UInt32Array::from(left_idx), UInt32Array::from(right_idx));
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
        for col in left.columns() {
            columns.push(gather(col, &li)?);
        }
        for col in right.columns() {
            columns.push(gather(col, &ri)?);
        }
        RecordBatch::try_new(out_schema, columns)
            .map(Some)
            .map_err(|e| DbError::Pipeline(format!("process-time join: build output: {e}")))
    }

    /// Concat this cycle's batches for one side into one (or `None` if empty).
    fn concat_new(batches: &[RecordBatch]) -> Result<Option<RecordBatch>, DbError> {
        let Some(schema) = batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .map(RecordBatch::schema)
        else {
            return Ok(None);
        };
        concat_batches(&schema, batches.iter())
            .map(Some)
            .map_err(|e| DbError::Pipeline(format!("process-time join: concat input: {e}")))
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
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        #[cfg(feature = "cluster-unstable")]
        let (left_batches_local, right_batches_local) = if let Some(ref cfg) = self.cluster_shuffle
        {
            let left_stage = format!("{}::left", self.projection.op_name);
            let right_stage = format!("{}::right", self.projection.op_name);
            let left = self
                .repartition_side(
                    inputs.first().map_or(&[][..], Vec::as_slice),
                    &self.config.left_key,
                    &left_stage,
                    cfg,
                )
                .await?;
            let right = self
                .repartition_side(
                    inputs.get(1).map_or(&[][..], Vec::as_slice),
                    &self.config.right_key,
                    &right_stage,
                    cfg,
                )
                .await?;
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

        let new_left = Self::concat_new(&left_batches_local)?;
        let new_right = Self::concat_new(&right_batches_local)?;
        let wm_left = watermarks.first().copied().unwrap_or(i64::MIN);
        let wm_right = watermarks.get(1).copied().unwrap_or(i64::MIN);

        // Emit against the OLD buffers first, then fold the new rows in — so each
        // pair emits exactly once (when its later side arrives) and old×old is
        // never re-emitted. Only concat a buffer when there are new rows to probe
        // it with.
        let right_buf = if new_left.is_some() {
            self.right.concat()?
        } else {
            None
        };
        let left_buf = if new_right.is_some() {
            self.left.concat()?
        } else {
            None
        };
        let mut out = Vec::new();
        if let (Some(nl), Some(rb)) = (&new_left, &right_buf) {
            out.extend(self.join_pair(nl, rb)?);
        }
        if let (Some(lb), Some(nr)) = (&left_buf, &new_right) {
            out.extend(self.join_pair(lb, nr)?);
        }
        if let (Some(nl), Some(nr)) = (&new_left, &new_right) {
            out.extend(self.join_pair(nl, nr)?);
        }

        if let Some(nl) = new_left {
            self.left.push(wm_left, nl);
        }
        if let Some(nr) = new_right {
            self.right.push(wm_right, nr);
        }

        // Evict AFTER joining (a row can match a partner arriving the same cycle it
        // ages out): each side is bounded by the OPPOSITE side's progress.
        self.left.evict(wm_right);
        self.right.evict(wm_left);

        self.projection.apply(out).await
    }

    #[cfg(feature = "cluster-unstable")]
    async fn ingest_shuffle(
        &mut self,
        stage: &str,
        batch: RecordBatch,
        watermark: i64,
    ) -> Result<(), DbError> {
        if self.cluster_shuffle.is_none() {
            return Ok(());
        }
        let op_name = &self.projection.op_name;
        if stage == format!("{}::left", op_name) {
            self.left.push(watermark, batch);
        } else if stage == format!("{}::right", op_name) {
            self.right.push(watermark, batch);
        }
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        // (side, watermark, batch-blob); side 0 = left, 1 = right.
        let mut blobs: Vec<(u8, i64, Vec<u8>)> = Vec::new();
        let mut serialize =
            |side: u8, chunks: &VecDeque<(i64, RecordBatch)>| -> Result<(), DbError> {
                for (tag, batch) in chunks {
                    let blob = serialize_batch_stream(batch).map_err(|e| {
                        DbError::Pipeline(format!("process-time join: checkpoint: {e}"))
                    })?;
                    blobs.push((side, *tag, blob));
                }
                Ok(())
            };
        serialize(0, &self.left.chunks)?;
        serialize(1, &self.right.chunks)?;
        if blobs.is_empty() {
            return Ok(None);
        }
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&blobs)
            .map(|v| v.to_vec())
            .map_err(|e| DbError::Pipeline(format!("process-time join: checkpoint encode: {e}")))?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let blobs: Vec<(u8, i64, Vec<u8>)> = rkyv::from_bytes::<
            Vec<(u8, i64, Vec<u8>)>,
            rkyv::rancor::Error,
        >(&checkpoint.data)
        .map_err(|e| DbError::Pipeline(format!("process-time join: restore decode: {e}")))?;
        self.left.chunks.clear();
        self.right.chunks.clear();
        for (side, tag, blob) in &blobs {
            let batch = deserialize_batch_stream(blob)
                .map_err(|e| DbError::Pipeline(format!("process-time join: restore: {e}")))?;
            let target = if *side == 0 {
                &mut self.left
            } else {
                &mut self.right
            };
            target.chunks.push_back((*tag, batch));
        }
        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        self.left.bytes() + self.right.bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_sql::translator::StreamJoinType;

    fn config() -> StreamJoinConfig {
        StreamJoinConfig {
            left_key: "bucket".to_string(),
            right_key: "bucket".to_string(),
            left_time_column: String::new(),
            right_time_column: String::new(),
            left_table: "price".to_string(),
            right_table: "sent".to_string(),
            time_bound: std::time::Duration::ZERO,
            join_type: StreamJoinType::Inner,
        }
    }

    fn operator() -> ProcessTimeJoinOperator {
        // No projection_sql ⇒ ProjectingJoinState passes the combined batch through.
        ProcessTimeJoinOperator::new("pt", config(), None, laminar_sql::create_session_context())
    }

    fn left_row(bucket: i64, price: f64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![bucket])),
                Arc::new(Float64Array::from(vec![price])),
            ],
        )
        .unwrap()]
    }

    fn right_row(bucket: i64, ms: f64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Int64, false),
            Field::new("ms", DataType::Float64, false),
        ]));
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![bucket])),
                Arc::new(Float64Array::from(vec![ms])),
            ],
        )
        .unwrap()]
    }

    fn rows(out: &[RecordBatch]) -> usize {
        out.iter().map(RecordBatch::num_rows).sum()
    }

    /// The two sides close the same bucket in DIFFERENT cycles (the AI-hold case
    /// the stateless join dropped): the match must still emit when the second
    /// side arrives.
    #[tokio::test]
    async fn matches_across_cycles() {
        let mut op = operator();
        // Cycle 1: only the left side closes bucket 1 → buffered, nothing emitted.
        assert_eq!(
            rows(
                &op.process(&[left_row(1, 100.0), vec![]], &[0, 0])
                    .await
                    .unwrap()
            ),
            0
        );
        // Cycle 2: the right side closes bucket 1 → the buffered left matches.
        assert_eq!(
            rows(
                &op.process(&[vec![], right_row(1, 0.5)], &[0, 0])
                    .await
                    .unwrap()
            ),
            1
        );
        // Cycle 3: bucket 1 already matched; only bucket 2's left arrives → nothing.
        assert_eq!(
            rows(
                &op.process(&[left_row(2, 200.0), vec![]], &[0, 0])
                    .await
                    .unwrap()
            ),
            0
        );
    }

    /// Same-cycle arrival still matches, and a matched pair is not re-emitted on
    /// a later cycle.
    #[tokio::test]
    async fn matches_same_cycle_without_reemission() {
        let mut op = operator();
        let out = op
            .process(&[left_row(1, 100.0), right_row(1, 0.5)], &[0, 0])
            .await
            .unwrap();
        assert_eq!(rows(&out), 1);
        // A new, non-matching bucket must not re-emit bucket 1.
        assert_eq!(
            rows(
                &op.process(&[left_row(2, 200.0), vec![]], &[0, 0])
                    .await
                    .unwrap()
            ),
            0
        );
    }

    /// Once the opposite side's watermark passes a buffered row, it ages out, so a
    /// late partner no longer matches — retention tracks progress, state stays
    /// bounded.
    #[tokio::test]
    async fn watermark_evicts_rows_the_other_side_passed() {
        let mut op = operator();
        // Left bucket 1 buffered at watermark 1; no right side yet.
        assert_eq!(
            rows(
                &op.process(&[left_row(1, 100.0), vec![]], &[1, 1])
                    .await
                    .unwrap()
            ),
            0
        );
        // The right side advances its watermark to 5 with no matching row; this
        // passes the buffered left@1, which is evicted.
        assert_eq!(
            rows(&op.process(&[vec![], vec![]], &[1, 5]).await.unwrap()),
            0
        );
        // A late right@1 now finds nothing — left@1 was correctly aged out.
        assert_eq!(
            rows(
                &op.process(&[vec![], right_row(1, 0.5)], &[1, 5])
                    .await
                    .unwrap()
            ),
            0
        );
    }
}
