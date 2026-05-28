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
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use rustc_hash::FxHashMap;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use laminar_core::lookup::foyer_cache::FoyerMemoryCache;
use laminar_core::lookup::source::LookupSourceDyn;
use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};
use laminar_sql::datafusion::lookup_join::LookupJoinType;
use laminar_sql::datafusion::{LookupTableRegistry, PartialLookupState, RegisteredLookup};

use crate::error::DbError;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

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
    mut submit_rx: mpsc::Receiver<WorkItem>,
    result_tx: mpsc::Sender<WorkResult>,
) {
    while let Some(item) = submit_rx.recv().await {
        let key_refs: Vec<&[u8]> = item.keys.iter().map(Vec::as_slice).collect();
        let outputs = match tokio::time::timeout(
            FETCH_TIMEOUT,
            source.query_batch(&key_refs, &[], &[]),
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
    lookup_schema: SchemaRef,
    /// `None` = cache-only mode (no source); misses resolve to not-found.
    submit_tx: Option<mpsc::Sender<WorkItem>>,
    result_rx: Option<mpsc::Receiver<WorkResult>>,
    _worker: Option<JoinHandle<()>>,
}

pub(crate) struct LookupEnrichOperator {
    table_name: String,
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
}

impl LookupEnrichOperator {
    pub(crate) fn new(
        name: &str,
        config: LookupEnrichConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
        registry: Arc<LookupTableRegistry>,
        runtime: Handle,
    ) -> Self {
        Self {
            table_name: config.table_name,
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
            key_sort_fields,
            source,
            ..
        } = state.as_ref();

        let converter = RowConverter::new(key_sort_fields.clone())
            .map_err(|e| DbError::Pipeline(format!("lookup-enrich: row converter: {e}")))?;

        let (submit_tx, result_rx, worker) = match source {
            Some(src) => {
                let (submit_tx, submit_rx) = mpsc::channel(SUBMIT_CAPACITY);
                let (result_tx, result_rx) = mpsc::channel(RESULT_CAPACITY);
                let handle = self
                    .runtime
                    .spawn(run_worker(Arc::clone(src), submit_rx, result_tx));
                (Some(submit_tx), Some(result_rx), Some(handle))
            }
            None => (None, None, None),
        };

        self.resolved = Some(Resolved {
            cache: Arc::clone(foyer_cache),
            converter,
            key_indices: Vec::new(),
            lookup_schema: Arc::clone(schema),
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

    /// Probe the cache for every row; serve a fully-resolved batch immediately,
    /// else hold it and submit the misses to the worker.
    fn ingest(
        &mut self,
        batch: RecordBatch,
        watermark: i64,
        out: &mut Vec<RecordBatch>,
    ) -> Result<(), DbError> {
        let resolved = self.resolved.as_mut().expect("resolved before ingest");

        // Resolve key-column indices against the actual input schema once.
        if resolved.key_indices.is_empty() {
            resolved.key_indices = self
                .key_columns
                .iter()
                .map(|c| {
                    batch.schema().index_of(c).map_err(|_| {
                        DbError::Pipeline(format!("lookup-enrich: key column '{c}' not in input"))
                    })
                })
                .collect::<Result<_, _>>()?;
        }

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

        for row in 0..n {
            // SQL NULL never equi-matches.
            if key_cols.iter().any(|c| c.is_null(row)) {
                slots.push(Slot::Resolved(None));
                continue;
            }
            let key = rows.row(row);
            match resolved.cache.get(key.as_ref()).into_batch() {
                // Zero-row batch = negative-cache tombstone (known miss).
                Some(b) if b.num_rows() == 0 => slots.push(Slot::Resolved(None)),
                Some(b) => slots.push(Slot::Resolved(Some(b))),
                None if resolved.submit_tx.is_some() => {
                    slots.push(Slot::Pending);
                    miss_rows.push(row);
                    miss_keys.push(key.as_ref().to_vec());
                }
                // Cache-only mode: an absent key is a miss.
                None => slots.push(Slot::Resolved(None)),
            }
        }

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
        for batch in inputs.first().map_or(&[][..], Vec::as_slice) {
            if batch.num_rows() > 0 {
                self.ingest(batch.clone(), watermark, &mut enriched)?;
            }
        }

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
        rows: std::collections::HashMap<i64, &'static str>,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl LookupSourceDyn for MapSource {
        async fn query_batch(
            &self,
            keys: &[&[u8]],
            _predicates: &[laminar_core::lookup::predicate::Predicate],
            _projection: &[laminar_core::lookup::source::ColumnId],
        ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
            let parser = converter.parser();
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
                    self.rows.get(&id).map(|name| lookup_row(id, name))
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
        )
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
            rows: std::collections::HashMap::new(), // every key misses
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
    async fn null_key_never_matches() {
        let source = Arc::new(MapSource {
            rows: [(1, "Alice")].into_iter().collect(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut op = operator_with(LookupJoinType::LeftOuter, source);
        let out = run_until_output(&mut op, stream_batch(&[100], &[None])).await;
        assert_eq!(out.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
}
