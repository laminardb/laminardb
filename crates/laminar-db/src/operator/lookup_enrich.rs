//! Async-decoupled on-demand lookup-enrich join.
//!
//! `partial`/`none` lookup joins run here instead of inside a `DataFusion` plan so
//! a cache-miss source fetch never blocks the `laminar-compute` thread. Like
//! [`AiInferenceOperator`](super::ai_inference), `process` is decoupled across
//! cycles: cache hits are served inline, misses go to a Ring 1 worker over a
//! bounded channel, and resolved batches emit in a later cycle. The output
//! watermark is held behind the oldest in-flight batch so enriched rows are not
//! dropped as late downstream.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{new_null_array, Array, ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{concat, take};
use arrow::row::RowConverter;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use rustc_hash::FxHashMap;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use laminar_core::lookup::foyer_cache::FoyerMemoryCache;
use laminar_core::lookup::source::{ColumnId, LookupSourceDyn};
use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};
use laminar_sql::datafusion::lookup_join::LookupJoinType;
use laminar_sql::datafusion::{LookupTableRegistry, PartialLookupState, RegisteredLookup};

use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

#[cfg(feature = "cluster-unstable")]
use crate::operator::sql_query::ClusterShuffleConfig;
#[cfg(feature = "cluster-unstable")]
use laminar_core::shuffle::ShuffleMessage;

const SUBMIT_CAPACITY: usize = 256;
const RESULT_CAPACITY: usize = 256;
const MAX_IN_FLIGHT_ROWS: usize = 8192;
/// Deadline for one worker fetch; on timeout the item is retried next cycle.
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const LOOKUP_TMP_TABLE: &str = "__lookup_enrich_tmp";

/// Static config produced by the planner detector and routed by `create_operator`.
pub(crate) struct LookupEnrichConfig {
    /// Name of the registered (partial/none) lookup table.
    pub table_name: String,
    /// Stream-side join-key column names, in lookup primary-key order.
    pub key_columns: Vec<String>,
    /// `Inner` (drop non-matches) or `LeftOuter` (NULL non-matches).
    pub join_type: LookupJoinType,
}

// ── Ring 1 worker ────────────────────────────────────────────────

/// A batch of cache-miss keys submitted to the worker.
struct WorkItem {
    batch_id: u64,
    row_indices: Vec<usize>,
    keys: Vec<Vec<u8>>,
}

/// The worker's reply. `keys` echo back so the operator can populate the cache
/// (positive and negative) keyed by the same bytes it submitted.
struct WorkResult {
    batch_id: u64,
    row_indices: Vec<usize>,
    keys: Vec<Vec<u8>>,
    /// Per-key lookup rows aligned to `row_indices`, or a batch-level error.
    outputs: Result<Vec<Option<RecordBatch>>, String>,
}

async fn run_worker(
    source: Arc<dyn LookupSourceDyn>,
    projection: Vec<ColumnId>,
    mut submit_rx: mpsc::Receiver<WorkItem>,
    result_tx: mpsc::Sender<WorkResult>,
) {
    while let Some(item) = submit_rx.recv().await {
        let key_refs: Vec<&[u8]> = item.keys.iter().map(Vec::as_slice).collect();
        let outputs = match tokio::time::timeout(
            FETCH_TIMEOUT,
            source.query_batch(&key_refs, &[], &projection),
        )
        .await
        {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(format!(
                "lookup source query timed out after {FETCH_TIMEOUT:?}"
            )),
        };
        let result = WorkResult {
            batch_id: item.batch_id,
            row_indices: item.row_indices,
            keys: item.keys,
            outputs,
        };
        if result_tx.send(result).await.is_err() {
            break;
        }
    }
}

// ── Operator ─────────────────────────────────────────────────────

/// One row's lookup result: `Pending` (in flight) or `Resolved` (hit = `Some`,
/// miss = `None`).
enum Slot {
    Pending,
    Resolved(Option<RecordBatch>),
}

/// A held batch whose `Pending` slots fill in as the worker resolves them.
struct PendingBatch {
    batch: RecordBatch,
    slots: Vec<Slot>,
    pending: usize,
    ingest_watermark: i64,
}

/// Registry-resolved state, materialised on the first `process` (the
/// `PartialLookupState` is only registered at pipeline start).
struct Resolved {
    cache: Arc<FoyerMemoryCache>,
    converter: RowConverter,
    key_indices: Vec<usize>,
    /// `(lookup key column, expected type)` per key, used to fail fast with a
    /// clear message if the input join-key type differs from the lookup
    /// primary-key type (otherwise the row-encoder errors cryptically later).
    key_checks: Vec<(String, DataType)>,
    lookup_schema: SchemaRef,
    /// `None` = cache-only mode (no source); misses resolve to not-found.
    submit_tx: Option<mpsc::Sender<WorkItem>>,
    result_rx: Option<mpsc::Receiver<WorkResult>>,
    _worker: Option<JoinHandle<()>>,
}

pub(crate) struct LookupEnrichOperator {
    table_name: String,
    /// This operator's output (MV) name; used as the shuffle stage tag in
    /// cluster mode so peers route this join's rows back to this operator.
    #[cfg(feature = "cluster-unstable")]
    op_name: Arc<str>,
    key_columns: Vec<String>,
    join_type: LookupJoinType,
    registry: Arc<LookupTableRegistry>,
    runtime: Handle,
    projection: ProjectingJoinState,
    resolved: Option<Resolved>,
    pending: FxHashMap<u64, PendingBatch>,
    unsubmitted: VecDeque<WorkItem>,
    /// Input batches awaiting (re-)ingestion, with their original watermark.
    replay: VecDeque<(i64, RecordBatch)>,
    next_batch_id: u64,
    max_in_flight: usize,
    /// Per-table cache/source/in-flight metrics, when the engine has a registry.
    metrics: Option<Arc<EngineMetrics>>,
    /// Cluster key-shuffle config; `None` outside cluster mode (the operator
    /// then runs purely per-node). When set, `process` key-shards incoming
    /// stream rows across the cluster by lookup key before probing the cache,
    /// so each node caches only the key range it owns (cache affinity).
    #[cfg(feature = "cluster-unstable")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
}

impl LookupEnrichOperator {
    pub(crate) fn new(
        name: &str,
        config: LookupEnrichConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
        registry: Arc<LookupTableRegistry>,
        runtime: Handle,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Self {
        Self {
            table_name: config.table_name,
            #[cfg(feature = "cluster-unstable")]
            op_name: Arc::from(name),
            key_columns: config.key_columns,
            join_type: config.join_type,
            registry,
            runtime,
            projection: ProjectingJoinState::new(name, ctx, projection_sql, LOOKUP_TMP_TABLE),
            resolved: None,
            pending: FxHashMap::default(),
            unsubmitted: VecDeque::new(),
            replay: VecDeque::new(),
            next_batch_id: 0,
            max_in_flight: MAX_IN_FLIGHT_ROWS,
            metrics,
            #[cfg(feature = "cluster-unstable")]
            cluster_shuffle: None,
        }
    }

    /// Install the cluster key-shuffle config. When present, `process`
    /// key-shards incoming stream rows by lookup key across the cluster
    /// (shipping rows whose key-vnode this node does not own to the owner,
    /// and ingesting rows peers ship here) before probing the cache.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        self.cluster_shuffle = Some(config);
    }

    /// Record per-batch cache effectiveness, if metrics are wired.
    fn record_cache(&self, hits: u64, misses: u64) {
        if let Some(m) = &self.metrics {
            if hits > 0 {
                m.lookup_cache_hits
                    .with_label_values(&[&self.table_name])
                    .inc_by(hits);
            }
            if misses > 0 {
                m.lookup_cache_misses
                    .with_label_values(&[&self.table_name])
                    .inc_by(misses);
            }
        }
    }

    /// Publish the current in-flight row count to the gauge, if metrics are wired.
    fn publish_in_flight(&self) {
        if let Some(m) = &self.metrics {
            let rows = i64::try_from(self.in_flight_rows()).unwrap_or(i64::MAX);
            m.lookup_in_flight_rows
                .with_label_values(&[&self.table_name])
                .set(rows);
        }
    }

    /// Resolve the `PartialLookupState` from the registry and spawn the worker.
    /// Idempotent: a no-op once resolved.
    fn ensure_resolved(&mut self) -> Result<(), DbError> {
        if self.resolved.is_some() {
            return Ok(());
        }
        let Some(RegisteredLookup::Partial(state)) = self.registry.get_entry(&self.table_name)
        else {
            return Err(DbError::Pipeline(format!(
                "lookup-enrich: table '{}' is not registered as a partial lookup",
                self.table_name
            )));
        };
        let PartialLookupState {
            foyer_cache,
            schema,
            key_columns,
            key_sort_fields,
            source,
            projection,
            ..
        } = state.as_ref();

        let converter = RowConverter::new(key_sort_fields.clone())
            .map_err(|e| DbError::Pipeline(format!("lookup-enrich: row converter: {e}")))?;

        // Expected input join-key types = the lookup table's primary-key types.
        let key_checks: Vec<(String, DataType)> = key_columns
            .iter()
            .map(|name| {
                let dt = schema
                    .field_with_name(name)
                    .map_or(DataType::Null, |f| f.data_type().clone());
                (name.clone(), dt)
            })
            .collect();

        // The worker fetches only the projected columns, so the joined-in lookup
        // rows carry the projected schema. Empty projection = the full schema.
        let lookup_schema: SchemaRef = if projection.is_empty() {
            Arc::clone(schema)
        } else {
            let idx: Vec<usize> = projection.iter().map(|&c| c as usize).collect();
            Arc::new(schema.project(&idx).map_err(|e| {
                DbError::Pipeline(format!("lookup-enrich: project lookup schema: {e}"))
            })?)
        };

        let (submit_tx, result_rx, worker) = match source {
            Some(src) => {
                let (submit_tx, submit_rx) = mpsc::channel(SUBMIT_CAPACITY);
                let (result_tx, result_rx) = mpsc::channel(RESULT_CAPACITY);
                let handle = self.runtime.spawn(run_worker(
                    Arc::clone(src),
                    projection.clone(),
                    submit_rx,
                    result_tx,
                ));
                (Some(submit_tx), Some(result_rx), Some(handle))
            }
            None => (None, None, None),
        };

        self.resolved = Some(Resolved {
            cache: Arc::clone(foyer_cache),
            converter,
            key_indices: Vec::new(),
            key_checks,
            lookup_schema,
            submit_tx,
            result_rx,
            _worker: worker,
        });
        Ok(())
    }

    fn in_flight_rows(&self) -> usize {
        let pending: usize = self.pending.values().map(|pb| pb.pending).sum();
        let queued: usize = self.unsubmitted.iter().map(|i| i.keys.len()).sum();
        pending + queued
    }

    /// Resolve the stream-side key-column indices against the input schema
    /// once, failing fast if a join-key type differs from the lookup
    /// primary-key type (the row encoder would otherwise error cryptically
    /// deep in the hot path). Idempotent once the indices are populated.
    /// Shared by [`Self::ingest`] and the cluster key-shuffle.
    fn ensure_key_indices(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        let resolved = self.resolved.as_mut().expect("resolved before ingest");
        if !resolved.key_indices.is_empty() {
            return Ok(());
        }
        resolved.key_indices = self
            .key_columns
            .iter()
            .map(|c| {
                batch.schema().index_of(c).map_err(|_| {
                    DbError::Pipeline(format!("lookup-enrich: key column '{c}' not in input"))
                })
            })
            .collect::<Result<_, _>>()?;

        for (i, &col_idx) in resolved.key_indices.iter().enumerate() {
            let actual = batch.column(col_idx).data_type();
            if let Some((lookup_key, expected)) = resolved.key_checks.get(i) {
                if actual != expected {
                    return Err(DbError::Pipeline(format!(
                        "lookup-enrich: join key type mismatch — input column '{}' is \
                         {actual:?}, but lookup table '{}' key '{lookup_key}' is {expected:?}; \
                         the join key and lookup primary key must have the same type",
                        self.key_columns[i], self.table_name
                    )));
                }
            }
        }
        Ok(())
    }

    /// Probe the cache for every row; serve a fully-resolved batch immediately,
    /// else hold it and submit the misses to the worker.
    fn ingest(
        &mut self,
        batch: RecordBatch,
        watermark: i64,
        out: &mut Vec<RecordBatch>,
    ) -> Result<(), DbError> {
        self.ensure_key_indices(&batch)?;
        let resolved = self.resolved.as_mut().expect("resolved before ingest");

        let key_cols: Vec<ArrayRef> = resolved
            .key_indices
            .iter()
            .map(|&i| Arc::clone(batch.column(i)))
            .collect();
        let rows = resolved
            .converter
            .convert_columns(&key_cols)
            .map_err(|e| DbError::Pipeline(format!("lookup-enrich: encode keys: {e}")))?;

        let n = batch.num_rows();
        let mut slots: Vec<Slot> = Vec::with_capacity(n);
        let mut miss_rows: Vec<usize> = Vec::new();
        let mut miss_keys: Vec<Vec<u8>> = Vec::new();
        let (mut hits, mut misses) = (0u64, 0u64);

        for row in 0..n {
            // SQL NULL never equi-matches.
            if key_cols.iter().any(|c| c.is_null(row)) {
                slots.push(Slot::Resolved(None));
                continue;
            }
            let key = rows.row(row);
            match resolved.cache.get(key.as_ref()).into_batch() {
                // Zero-row batch = negative-cache tombstone (known miss).
                Some(b) if b.num_rows() == 0 => {
                    hits += 1;
                    slots.push(Slot::Resolved(None));
                }
                Some(b) => {
                    hits += 1;
                    slots.push(Slot::Resolved(Some(b)));
                }
                None if resolved.submit_tx.is_some() => {
                    misses += 1;
                    slots.push(Slot::Pending);
                    miss_rows.push(row);
                    miss_keys.push(key.as_ref().to_vec());
                }
                // Cache-only mode: an absent key is a miss.
                None => {
                    misses += 1;
                    slots.push(Slot::Resolved(None));
                }
            }
        }
        self.record_cache(hits, misses);

        if miss_rows.is_empty() {
            out.push(self.build_output(&batch, &slots)?);
            return Ok(());
        }

        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;
        let pending = miss_rows.len();
        self.pending.insert(
            batch_id,
            PendingBatch {
                batch,
                slots,
                pending,
                ingest_watermark: watermark,
            },
        );
        self.submit(WorkItem {
            batch_id,
            row_indices: miss_rows,
            keys: miss_keys,
        });
        Ok(())
    }

    /// Apply a worker reply: populate the cache, fill slots, and emit when the
    /// batch is fully resolved. On fetch error, re-queue for retry next cycle so
    /// a transient source failure backpressures rather than NULL-fills.
    fn apply_result(
        &mut self,
        result: WorkResult,
        out: &mut Vec<RecordBatch>,
    ) -> Result<(), DbError> {
        let resolved = self.resolved.as_ref().expect("resolved");
        let cache = Arc::clone(&resolved.cache);
        let lookup_schema = Arc::clone(&resolved.lookup_schema);

        let Some(pb) = self.pending.get_mut(&result.batch_id) else {
            return Ok(()); // batch no longer tracked (cleared on restore)
        };

        let rows = match result.outputs {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(table = %self.table_name, error = %e, "lookup-enrich: fetch failed, retrying");
                if let Some(m) = &self.metrics {
                    m.lookup_source_errors
                        .with_label_values(&[&self.table_name])
                        .inc();
                }
                self.unsubmitted.push_back(WorkItem {
                    batch_id: result.batch_id,
                    row_indices: result.row_indices,
                    keys: result.keys,
                });
                return Ok(());
            }
        };

        for ((&row_index, key), value) in result.row_indices.iter().zip(&result.keys).zip(rows) {
            if matches!(pb.slots[row_index], Slot::Resolved(_)) {
                continue;
            }
            match &value {
                Some(b) => cache.insert(key, b.clone()),
                // Negative cache: tombstone = empty batch in the lookup schema.
                None => cache.insert(key, RecordBatch::new_empty(Arc::clone(&lookup_schema))),
            }
            pb.slots[row_index] = Slot::Resolved(value);
            pb.pending -= 1;
        }

        if pb.pending == 0 {
            let pb = self
                .pending
                .remove(&result.batch_id)
                .expect("present above");
            out.push(self.build_output(&pb.batch, &pb.slots)?);
        }
        Ok(())
    }

    /// Assemble stream columns + lookup columns. Inner drops non-matches;
    /// `LeftOuter` keeps all rows with NULL lookup columns for misses.
    fn build_output(&self, stream: &RecordBatch, slots: &[Slot]) -> Result<RecordBatch, DbError> {
        let resolved = self.resolved.as_ref().expect("resolved");
        let lookup_schema = &resolved.lookup_schema;

        let mut keep: Vec<u32> = Vec::with_capacity(stream.num_rows());
        let mut lookups: Vec<Option<&RecordBatch>> = Vec::with_capacity(stream.num_rows());
        for (i, slot) in slots.iter().enumerate() {
            let hit = match slot {
                Slot::Resolved(b) => b.as_ref(),
                Slot::Pending => None, // unreachable once pending == 0
            };
            if hit.is_none() && self.join_type == LookupJoinType::Inner {
                continue;
            }
            keep.push(u32::try_from(i).expect("row index fits u32"));
            lookups.push(hit);
        }

        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(stream.num_columns() + lookup_schema.fields().len());
        let take_idx = UInt32Array::from(keep);
        for col in stream.columns() {
            columns.push(
                take(col.as_ref(), &take_idx, None)
                    .map_err(|e| DbError::Pipeline(format!("lookup-enrich: take: {e}")))?,
            );
        }
        for (c, field) in lookup_schema.fields().iter().enumerate() {
            let per_row: Vec<ArrayRef> = lookups
                .iter()
                .map(|opt| match opt {
                    Some(b) => Arc::clone(b.column(c)),
                    None => new_null_array(field.data_type(), 1),
                })
                .collect();
            let refs: Vec<&dyn Array> = per_row.iter().map(AsRef::as_ref).collect();
            columns.push(
                concat(&refs)
                    .map_err(|e| DbError::Pipeline(format!("lookup-enrich: concat: {e}")))?,
            );
        }

        let out_schema = output_schema(
            stream.schema().as_ref(),
            lookup_schema,
            self.join_type,
            &self.table_name,
        );
        RecordBatch::try_new(out_schema, columns)
            .map_err(|e| DbError::Pipeline(format!("lookup-enrich: build output: {e}")))
    }

    fn flush_unsubmitted(&mut self) {
        let Some(tx) = self.resolved.as_ref().and_then(|r| r.submit_tx.clone()) else {
            return;
        };
        while let Some(item) = self.unsubmitted.pop_front() {
            match tx.try_send(item) {
                Err(mpsc::error::TrySendError::Full(item)) => {
                    self.unsubmitted.push_front(item);
                    break;
                }
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
    }

    fn submit(&mut self, item: WorkItem) {
        let Some(tx) = self.resolved.as_ref().and_then(|r| r.submit_tx.clone()) else {
            return;
        };
        if !self.unsubmitted.is_empty() {
            self.unsubmitted.push_back(item);
            return;
        }
        if let Err(mpsc::error::TrySendError::Full(item)) = tx.try_send(item) {
            self.unsubmitted.push_back(item);
        }
    }

    /// Drain whatever the worker has resolved since the last cycle.
    fn drain_results(&mut self, out: &mut Vec<RecordBatch>) -> Result<(), DbError> {
        loop {
            let Some(rx) = self.resolved.as_mut().and_then(|r| r.result_rx.as_mut()) else {
                return Ok(());
            };
            match rx.try_recv() {
                Ok(result) => self.apply_result(result, out)?,
                Err(_) => return Ok(()),
            }
        }
    }

    /// In cluster mode, route each input row to the node owning its lookup-key
    /// vnode: ship rows this node doesn't own, drain rows peers shipped here,
    /// and return the local set to enrich. Forward-only — any node can look up
    /// any key. `None` config (single node) is a pass-through.
    #[cfg(feature = "cluster-unstable")]
    async fn shuffle_input(
        &mut self,
        batches: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let Some(cfg) = self.cluster_shuffle.clone() else {
            return Ok(batches.to_vec());
        };
        let vnode_count = cfg.registry.vnode_count();
        let mut local: Vec<RecordBatch> = Vec::new();
        let mut outbound: Vec<(u64, ShuffleMessage)> = Vec::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            self.ensure_key_indices(batch)?;
            // Hash the same key columns the cache uses, so a key's vnode and its
            // cache slot agree — the owner is the node that caches it.
            let key_indices = &self.resolved.as_ref().expect("resolved").key_indices;
            let vnodes = laminar_core::shuffle::row_vnodes(batch, key_indices, vnode_count);
            for &v in &vnodes {
                let owner = cfg.registry.owner(v);
                if owner.is_unassigned() {
                    return Err(DbError::Pipeline(format!(
                        "lookup-enrich: shuffle vnode {v} is unassigned — refusing to drop rows"
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
                    ShuffleMessage::VnodeData(self.op_name.to_string(), 0, slice),
                ));
            }
        }

        // Ship remote slices; fail the cycle on send error so offsets don't
        // advance past undelivered rows. The await is a socket write bounded by
        // TCP backpressure (same as the aggregate shuffle).
        for (peer, msg) in outbound {
            cfg.sender.send_to(peer, &msg).await.map_err(|e| {
                DbError::Pipeline(format!("lookup-enrich: shuffle send to peer {peer}: {e}"))
            })?;
        }

        // Drain rows peers shipped here, but only with in-flight headroom so a
        // backpressured node lets them queue (TCP backpressure) instead of
        // overrunning the cap.
        if self.wants_input() {
            for batch in cfg.receiver.drain_vnode_data_for(&self.op_name) {
                if batch.num_rows() > 0 {
                    local.push(batch);
                }
            }
        }

        Ok(local)
    }
}

/// Disambiguated name for a lookup column in the flattened join output: a
/// lookup column whose name also exists on the stream side is suffixed with the
/// lookup table name. The plan-time projection rewriter applies the identical
/// rule, so its column references match this schema.
pub(crate) fn disambiguated_lookup_name(
    lookup_col: &str,
    stream_cols: &[String],
    lookup_table: &str,
) -> String {
    if stream_cols.iter().any(|s| s == lookup_col) {
        format!("{lookup_col}_{lookup_table}")
    } else {
        lookup_col.to_string()
    }
}

/// Output schema: stream fields followed by lookup fields (collision-suffixed
/// per [`disambiguated_lookup_name`]; lookup fields forced nullable for `LeftOuter`).
fn output_schema(
    stream: &arrow_schema::Schema,
    lookup: &SchemaRef,
    join_type: LookupJoinType,
    lookup_table: &str,
) -> SchemaRef {
    use arrow_schema::{Field, Schema};
    let stream_names: Vec<String> = stream.fields().iter().map(|f| f.name().clone()).collect();
    let mut fields: Vec<Arc<Field>> = stream.fields().iter().cloned().collect();
    for f in lookup.fields() {
        let name = disambiguated_lookup_name(f.name(), &stream_names, lookup_table);
        let nullable = f.is_nullable() || join_type == LookupJoinType::LeftOuter;
        fields.push(Arc::new(Field::new(&name, f.data_type().clone(), nullable)));
    }
    Arc::new(Schema::new(fields))
}

#[async_trait]
impl GraphOperator for LookupEnrichOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.ensure_resolved()?;
        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        let mut enriched = Vec::new();

        self.flush_unsubmitted();
        self.drain_results(&mut enriched)?;
        while let Some((wm, batch)) = self.replay.pop_front() {
            self.ingest(batch, wm, &mut enriched)?;
        }

        // New input: in cluster mode, key-shard across the cluster first (ship
        // rows whose key-vnode we don't own to the owner, receive ours), then
        // ingest the local + inbound set. Replayed batches above bypass the
        // shuffle — they are already local. Single-node: a straight passthrough.
        let new_input = inputs.first().map_or(&[][..], Vec::as_slice);
        #[cfg(feature = "cluster-unstable")]
        let to_ingest = self.shuffle_input(new_input).await?;
        #[cfg(not(feature = "cluster-unstable"))]
        let to_ingest: Vec<RecordBatch> = new_input.to_vec();
        for batch in to_ingest {
            if batch.num_rows() > 0 {
                self.ingest(batch, watermark, &mut enriched)?;
            }
        }

        self.publish_in_flight();
        self.projection.apply(enriched).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        // Capture every not-yet-emitted input batch (in-flight + replay) with its
        // ingest watermark. On restore they re-ingest and re-fetch; source reads
        // are idempotent, so replay is safe.
        let mut blobs: Vec<(i64, Vec<u8>)> =
            Vec::with_capacity(self.pending.len() + self.replay.len());
        for pb in self.pending.values() {
            blobs.push((
                pb.ingest_watermark,
                serialize_batch_stream(&pb.batch)
                    .map_err(|e| DbError::Pipeline(format!("lookup-enrich: checkpoint: {e}")))?,
            ));
        }
        for (wm, batch) in &self.replay {
            blobs.push((
                *wm,
                serialize_batch_stream(batch)
                    .map_err(|e| DbError::Pipeline(format!("lookup-enrich: checkpoint: {e}")))?,
            ));
        }
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&blobs)
            .map(|v| v.to_vec())
            .map_err(|e| DbError::Pipeline(format!("lookup-enrich: checkpoint encode: {e}")))?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let blobs: Vec<(i64, Vec<u8>)> =
            rkyv::from_bytes::<Vec<(i64, Vec<u8>)>, rkyv::rancor::Error>(&checkpoint.data)
                .map_err(|e| DbError::Pipeline(format!("lookup-enrich: checkpoint decode: {e}")))?;
        self.pending.clear();
        self.unsubmitted.clear();
        self.replay.clear();
        for (wm, blob) in &blobs {
            let batch = deserialize_batch_stream(blob)
                .map_err(|e| DbError::Pipeline(format!("lookup-enrich: restore: {e}")))?;
            self.replay.push_back((*wm, batch));
        }
        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        let pending: usize = self
            .pending
            .values()
            .map(|pb| pb.batch.get_array_memory_size())
            .sum();
        let replay: usize = self
            .replay
            .iter()
            .map(|(_, b)| b.get_array_memory_size())
            .sum();
        pending + replay
    }

    fn watermark_hold(&self) -> Option<i64> {
        self.pending.values().map(|pb| pb.ingest_watermark).min()
    }

    fn wants_input(&self) -> bool {
        self.in_flight_rows() < self.max_in_flight
    }

    #[cfg(feature = "cluster-unstable")]
    async fn ingest_shuffle(
        &mut self,
        _stage: &str,
        batch: RecordBatch,
        watermark: i64,
    ) -> Result<(), DbError> {
        // Peer-shipped rows are already owned by this node. Queue them on the
        // replay path — which bypasses the shuffle and is itself checkpointed —
        // so they enter this snapshot and re-ingest idempotently on restore.
        self.replay.push_back((watermark, batch));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::row::SortField;
    use arrow_schema::{DataType, Field, Schema};
    use laminar_core::lookup::source::LookupError;

    fn stream_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, true),
        ]))
    }

    fn lookup_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn stream_batch(orders: &[i64], customers: &[Option<i64>]) -> RecordBatch {
        RecordBatch::try_new(
            stream_schema(),
            vec![
                Arc::new(Int64Array::from(orders.to_vec())),
                Arc::new(Int64Array::from(customers.to_vec())),
            ],
        )
        .unwrap()
    }

    fn lookup_row(id: i64, name: &str) -> RecordBatch {
        RecordBatch::try_new(
            lookup_schema(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
            ],
        )
        .unwrap()
    }

    /// Source returning a fixed map id -> name, counting calls.
    struct MapSource {
        rows: FxHashMap<i64, &'static str>,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl LookupSourceDyn for MapSource {
        async fn query_batch(
            &self,
            keys: &[&[u8]],
            _predicates: &[laminar_core::lookup::predicate::Predicate],
            projection: &[laminar_core::lookup::source::ColumnId],
        ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
            let parser = converter.parser();
            // Honor projection like a real backend: return only those columns.
            let proj: Vec<usize> = projection.iter().map(|&c| c as usize).collect();
            Ok(keys
                .iter()
                .map(|k| {
                    let row = parser.parse(k);
                    let cols = converter.convert_rows(std::iter::once(row)).unwrap();
                    let id = cols[0]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0);
                    self.rows.get(&id).map(|name| {
                        let full = lookup_row(id, name);
                        if proj.is_empty() {
                            full
                        } else {
                            full.project(&proj).unwrap()
                        }
                    })
                })
                .collect())
        }

        fn schema(&self) -> SchemaRef {
            lookup_schema()
        }
    }

    fn operator_with(
        join_type: LookupJoinType,
        source: Arc<dyn LookupSourceDyn>,
    ) -> LookupEnrichOperator {
        operator_with_metrics(join_type, source, None)
    }

    fn operator_with_metrics(
        join_type: LookupJoinType,
        source: Arc<dyn LookupSourceDyn>,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> LookupEnrichOperator {
        let registry = Arc::new(LookupTableRegistry::new());
        registry.register_partial(
            "customers",
            PartialLookupState {
                foyer_cache: Arc::new(FoyerMemoryCache::with_defaults(0)),
                schema: lookup_schema(),
                key_columns: vec!["id".into()],
                key_sort_fields: vec![SortField::new(DataType::Int64)],
                source: Some(source),
                fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(16)),
                projection: Vec::new(),
            },
        );
        LookupEnrichOperator::new(
            "enrich",
            LookupEnrichConfig {
                table_name: "customers".into(),
                key_columns: vec!["customer_id".into()],
                join_type,
            },
            None,
            laminar_sql::create_session_context(),
            registry,
            Handle::current(),
            metrics,
        )
    }

    #[tokio::test]
    async fn join_key_type_mismatch_errors_clearly() {
        let source = Arc::new(MapSource {
            rows: FxHashMap::default(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut op = operator_with(LookupJoinType::Inner, source);

        // Input `customer_id` is Int32, but the lookup key `id` is Int64.
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int32Array::from(vec![Some(7)])),
            ],
        )
        .unwrap();

        let err = op
            .process(&[vec![batch]], &[0])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("type mismatch"), "got: {err}");
        assert!(err.contains("customer_id") && err.contains("Int32") && err.contains("Int64"));
    }

    #[tokio::test]
    async fn metrics_count_cold_misses_then_warm_hits() {
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(EngineMetrics::new(&registry));
        let source = Arc::new(MapSource {
            rows: [(1, "Alice")].into_iter().collect(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut op = operator_with_metrics(
            LookupJoinType::LeftOuter,
            source,
            Some(Arc::clone(&metrics)),
        );
        let m = |c: &prometheus::IntCounterVec| c.with_label_values(&["customers"]).get();

        // Cold cache: both keys miss and go to the source.
        run_until_output(&mut op, stream_batch(&[10, 11], &[Some(1), Some(99)])).await;
        assert_eq!(m(&metrics.lookup_cache_misses), 2);

        // Warm cache: the same keys are now served from cache (value + tombstone).
        run_until_output(&mut op, stream_batch(&[12, 13], &[Some(1), Some(99)])).await;
        assert_eq!(m(&metrics.lookup_cache_hits), 2);
    }

    /// Drive `process` until the held batch resolves and emits (the worker runs
    /// on another task, so a miss emits a cycle or two later).
    async fn run_until_output(
        op: &mut LookupEnrichOperator,
        input: RecordBatch,
    ) -> Vec<RecordBatch> {
        let mut out = op.process(&[vec![input]], &[0]).await.unwrap();
        for _ in 0..50 {
            if !out.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            out = op.process(&[vec![]], &[0]).await.unwrap();
        }
        out
    }

    #[tokio::test]
    async fn miss_then_fetch_enriches() {
        let source = Arc::new(MapSource {
            rows: [(1, "Alice"), (2, "Bob")].into_iter().collect(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut op = operator_with(LookupJoinType::Inner, source);
        let out = run_until_output(&mut op, stream_batch(&[100, 101], &[Some(1), Some(2)])).await;
        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn inner_drops_miss_left_keeps_with_null() {
        let source = Arc::new(MapSource {
            rows: [(1, "Alice")].into_iter().collect(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut inner = operator_with(LookupJoinType::Inner, source.clone());
        let out =
            run_until_output(&mut inner, stream_batch(&[100, 101], &[Some(1), Some(9)])).await;
        assert_eq!(out.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        let mut left = operator_with(LookupJoinType::LeftOuter, source);
        let out = run_until_output(&mut left, stream_batch(&[100, 101], &[Some(1), Some(9)])).await;
        assert_eq!(out.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }

    #[tokio::test]
    async fn negative_cache_avoids_refetch() {
        let source = Arc::new(MapSource {
            rows: FxHashMap::default(), // every key misses
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let calls = Arc::clone(&source) as Arc<dyn LookupSourceDyn>;
        let _ = calls;
        let mut op = operator_with(LookupJoinType::LeftOuter, source.clone());
        run_until_output(&mut op, stream_batch(&[100], &[Some(7)])).await;
        let after_first = source.calls.load(std::sync::atomic::Ordering::Relaxed);
        // Second occurrence of the same missing key must hit the tombstone, not the source.
        run_until_output(&mut op, stream_batch(&[101], &[Some(7)])).await;
        let after_second = source.calls.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(after_first, after_second, "missing key was re-fetched");
    }

    #[tokio::test]
    async fn projection_pushdown_enriches_with_projected_schema() {
        // The table registers projection=[0] (id only), so the lookup side
        // contributes "id" and never "name" — the operator's joined schema must
        // match what the (projection-honoring) source returns.
        let registry = Arc::new(LookupTableRegistry::new());
        registry.register_partial(
            "customers",
            PartialLookupState {
                foyer_cache: Arc::new(FoyerMemoryCache::with_defaults(0)),
                schema: lookup_schema(), // id, name
                key_columns: vec!["id".into()],
                key_sort_fields: vec![SortField::new(DataType::Int64)],
                source: Some(Arc::new(MapSource {
                    rows: [(1, "Alice")].into_iter().collect(),
                    calls: std::sync::atomic::AtomicUsize::new(0),
                }) as Arc<dyn LookupSourceDyn>),
                fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(16)),
                projection: vec![0],
            },
        );
        let mut op = LookupEnrichOperator::new(
            "enrich",
            LookupEnrichConfig {
                table_name: "customers".into(),
                key_columns: vec!["customer_id".into()],
                join_type: LookupJoinType::Inner,
            },
            None,
            laminar_sql::create_session_context(),
            registry,
            Handle::current(),
            None,
        );
        let out = run_until_output(&mut op, stream_batch(&[100], &[Some(1)])).await;
        assert_eq!(out.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let batch = out.iter().find(|b| b.num_rows() > 0).unwrap();
        let names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(
            names.iter().any(|n| n == "id"),
            "projected key present: {names:?}"
        );
        assert!(
            names.iter().all(|n| n != "name"),
            "unprojected column absent: {names:?}"
        );
    }

    #[tokio::test]
    async fn null_key_never_matches() {
        let source = Arc::new(MapSource {
            rows: [(1, "Alice")].into_iter().collect(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut op = operator_with(LookupJoinType::LeftOuter, source);
        let out = run_until_output(&mut op, stream_batch(&[100], &[None])).await;
        assert_eq!(out.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }

    /// A cluster node: owns its vnodes per `assignment`, ships to `peer` at
    /// `peer_addr`, receives on `receiver`, and looks `keys` up in an in-memory
    /// source. Inner join over `customers(id)` on `customer_id`.
    #[cfg(feature = "cluster-unstable")]
    async fn cluster_node(
        self_id: u64,
        peer: u64,
        peer_addr: std::net::SocketAddr,
        receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
        assignment: &Arc<[laminar_core::state::NodeId]>,
        keys: &[i64],
    ) -> LookupEnrichOperator {
        use laminar_core::state::{NodeId, VnodeRegistry};

        let registry = Arc::new(VnodeRegistry::new(u32::try_from(assignment.len()).unwrap()));
        registry.set_assignment(Arc::clone(assignment));
        let sender = laminar_core::shuffle::ShuffleSender::new(self_id);
        sender.register_peer(peer, peer_addr).await;

        let lookups = Arc::new(LookupTableRegistry::new());
        lookups.register_partial(
            "customers",
            PartialLookupState {
                foyer_cache: Arc::new(FoyerMemoryCache::with_defaults(0)),
                schema: lookup_schema(),
                key_columns: vec!["id".into()],
                key_sort_fields: vec![SortField::new(DataType::Int64)],
                source: Some(Arc::new(MapSource {
                    rows: keys.iter().map(|&k| (k, "x")).collect(),
                    calls: std::sync::atomic::AtomicUsize::new(0),
                }) as Arc<dyn LookupSourceDyn>),
                fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(16)),
                projection: Vec::new(),
            },
        );
        let mut op = LookupEnrichOperator::new(
            "out",
            LookupEnrichConfig {
                table_name: "customers".into(),
                key_columns: vec!["customer_id".into()],
                join_type: LookupJoinType::Inner,
            },
            None,
            laminar_sql::create_session_context(),
            lookups,
            Handle::current(),
            None,
        );
        op.attach_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
            registry,
            sender: Arc::new(sender),
            receiver,
            self_id: NodeId(self_id),
        });
        op
    }

    /// A row whose lookup-key vnode a node doesn't own is shipped to the owner,
    /// enriched there, and emitted there — so every input key surfaces on
    /// exactly one node, the one that owns it.
    #[cfg(feature = "cluster-unstable")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_key_shuffle_routes_remote_keys_to_owner() {
        use laminar_core::shuffle::ShuffleReceiver;
        use laminar_core::state::NodeId;

        const VNODES: u32 = 4;
        let assignment: Arc<[NodeId]> = vec![NodeId(1), NodeId(2), NodeId(1), NodeId(2)].into();
        // Oracle: a key's owner under the exact hashing the operator uses.
        let owner = |id: i64| {
            let v =
                laminar_core::shuffle::row_vnodes(&stream_batch(&[0], &[Some(id)]), &[1], VNODES);
            assignment[v[0] as usize]
        };

        let n1: Vec<i64> = (1..500)
            .filter(|&k| owner(k) == NodeId(1))
            .take(3)
            .collect();
        let n2: Vec<i64> = (1..500)
            .filter(|&k| owner(k) == NodeId(2))
            .take(3)
            .collect();
        assert!(
            !n1.is_empty() && !n2.is_empty(),
            "need keys owned by both nodes"
        );
        let all: Vec<i64> = n1.iter().chain(&n2).copied().collect();

        let recv1 = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let recv2 = Arc::new(
            ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let mut node1 = cluster_node(
            1,
            2,
            recv2.local_addr(),
            Arc::clone(&recv1),
            &assignment,
            &all,
        )
        .await;
        let mut node2 = cluster_node(
            2,
            1,
            recv1.local_addr(),
            Arc::clone(&recv2),
            &assignment,
            &all,
        )
        .await;

        // node1 sees every key, keeps its own, ships node2's; pump both until
        // all keys surface (the worker + loopback are async).
        let customers: Vec<Option<i64>> = all.iter().map(|&k| Some(k)).collect();
        let input = stream_batch(&vec![0; all.len()], &customers);
        let mut a = node1.process(&[vec![input]], &[0]).await.unwrap();
        let mut b = node2.process(&[vec![]], &[0]).await.unwrap();
        for _ in 0..100 {
            if a.iter().chain(&b).map(RecordBatch::num_rows).sum::<usize>() >= all.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            a.extend(node1.process(&[vec![]], &[0]).await.unwrap());
            b.extend(node2.process(&[vec![]], &[0]).await.unwrap());
        }

        let ids = |batches: &[RecordBatch]| {
            let mut out: Vec<i64> = batches
                .iter()
                .flat_map(|x| {
                    let c = x.column(x.schema().index_of("customer_id").unwrap());
                    let c = c.as_any().downcast_ref::<Int64Array>().unwrap();
                    (0..x.num_rows()).map(|i| c.value(i)).collect::<Vec<_>>()
                })
                .collect();
            out.sort_unstable();
            out
        };
        let (a_ids, b_ids) = (ids(&a), ids(&b));

        assert!(
            a_ids.iter().all(|&k| owner(k) == NodeId(1)),
            "node1 has a foreign key: {a_ids:?}"
        );
        assert!(
            b_ids.iter().all(|&k| owner(k) == NodeId(2)),
            "node2 has a foreign key: {b_ids:?}"
        );
        assert!(!b_ids.is_empty(), "node2 never received the shuffled keys");
        let mut got: Vec<i64> = a_ids.into_iter().chain(b_ids).collect();
        got.sort_unstable();
        let mut want = all.clone();
        want.sort_unstable();
        assert_eq!(got, want, "each input key enriched on exactly its owner");
    }
}
