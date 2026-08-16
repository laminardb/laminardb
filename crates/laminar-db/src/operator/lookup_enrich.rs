//! Async-decoupled lookup-enrich join for `partial`/`none` lookup tables.
//!
//! Cache hits serve inline; misses go to a Ring 1 worker and emit in a later cycle.
//! Output watermark held behind the oldest in-flight batch.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{new_null_array, Array, ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{concat, take};
use arrow::row::RowConverter;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use crossfire::AsyncTxTrait as _;
use datafusion::prelude::SessionContext;
use rustc_hash::FxHashMap;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use laminar_core::lookup::lookup_cache::LookupMemoryCache;
use laminar_core::lookup::source::{ColumnId, LookupSourceDyn};
use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};
use laminar_sql::datafusion::lookup_join::LookupJoinType;
use laminar_sql::datafusion::{LookupTableRegistry, PartialLookupState, RegisteredLookup};

use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, InputFrontier, OperatorCheckpoint};

const SUBMIT_CAPACITY: usize = 256;
const RESULT_CAPACITY: usize = 256;
const MAX_IN_FLIGHT_ROWS: usize = 8192;
// On timeout the item is retried next cycle.
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const LOOKUP_TMP_TABLE: &str = "__lookup_enrich_tmp";

/// Config produced by the planner detector.
pub(crate) struct LookupEnrichConfig {
    pub table_name: String,
    /// Stream-side join-key columns in lookup primary-key order.
    pub key_columns: Vec<String>,
    pub join_type: LookupJoinType,
}

// `MAsyncTx` is `Send+Sync`; result channel uses tokio mpsc because `AsyncRx` is `!Sync`.
type SubmitTx = crossfire::MAsyncTx<crossfire::mpsc::Array<WorkItem>>;
type SubmitRx = crossfire::AsyncRx<crossfire::mpsc::Array<WorkItem>>;

struct WorkItem {
    batch_id: u64,
    row_indices: Vec<usize>,
    keys: Vec<Vec<u8>>,
}

/// `keys` echo back so the operator can populate the cache with the same bytes.
struct WorkResult {
    batch_id: u64,
    row_indices: Vec<usize>,
    keys: Vec<Vec<u8>>,
    outputs: Result<Vec<Option<RecordBatch>>, String>,
}

async fn run_worker(
    source: Arc<dyn LookupSourceDyn>,
    projection: Vec<ColumnId>,
    submit_rx: SubmitRx,
    result_tx: mpsc::Sender<WorkResult>,
) {
    while let Ok(item) = submit_rx.recv().await {
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

enum Slot {
    Pending,
    Resolved(Option<RecordBatch>),
}

struct PendingBatch {
    batch: RecordBatch,
    slots: Vec<Slot>,
    pending: usize,
    ingest_watermark: i64,
}

/// Registry-resolved state, populated on the first `process` call.
struct Resolved {
    cache: Arc<LookupMemoryCache>,
    converter: RowConverter,
    key_indices: Vec<usize>,
    // (lookup key, expected type) for fast type-mismatch errors before encoding.
    key_checks: Vec<(String, DataType)>,
    lookup_schema: SchemaRef,
    // None = cache-only mode; misses resolve to not-found.
    submit_tx: Option<SubmitTx>,
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
    replay: VecDeque<(i64, RecordBatch)>,
    next_batch_id: u64,
    max_in_flight: usize,
    metrics: Option<Arc<EngineMetrics>>,
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
        }
    }

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

    fn publish_in_flight(&self) {
        if let Some(m) = &self.metrics {
            let rows = i64::try_from(self.in_flight_rows()).unwrap_or(i64::MAX);
            m.lookup_in_flight_rows
                .with_label_values(&[&self.table_name])
                .set(rows);
        }
    }

    /// Resolve the `PartialLookupState` from the registry and spawn the worker. Idempotent.
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
            lookup_cache,
            schema,
            key_columns,
            key_sort_fields,
            source,
            projection,
            ..
        } = state.as_ref();

        let converter = RowConverter::new(key_sort_fields.clone())
            .map_err(|e| DbError::Pipeline(format!("lookup-enrich: row converter: {e}")))?;

        let key_checks: Vec<(String, DataType)> = key_columns
            .iter()
            .map(|name| {
                let dt = schema
                    .field_with_name(name)
                    .map_or(DataType::Null, |f| f.data_type().clone());
                (name.clone(), dt)
            })
            .collect();

        // Empty projection = the full schema (worker fetches all columns).
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
                let (submit_tx, submit_rx) = crossfire::mpsc::bounded_async(SUBMIT_CAPACITY);
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
            cache: Arc::clone(lookup_cache),
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

    fn retained_pending_rows(&self) -> usize {
        // A single unresolved lookup keeps the complete input batch and its shuffle admission
        // alive. Count retained input rows here, not just unresolved keys, or cache-heavy batches
        // can exceed the active-work cap by orders of magnitude.
        self.pending
            .values()
            .map(|batch| batch.batch.num_rows())
            .sum()
    }

    fn in_flight_rows(&self) -> usize {
        let replay: usize = self.replay.iter().map(|(_, batch)| batch.num_rows()).sum();
        self.retained_pending_rows().saturating_add(replay)
    }

    // Resolves key-column indices on the first call; fails fast on type mismatch.
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
            match resolved.cache.get_cached(key.as_ref()).into_batch() {
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
                Err(crossfire::TrySendError::Full(item)) => {
                    self.unsubmitted.push_front(item);
                    break;
                }
                Ok(()) | Err(crossfire::TrySendError::Disconnected(_)) => {}
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
        if let Err(crossfire::TrySendError::Full(item)) = tx.try_send(item) {
            self.unsubmitted.push_back(item);
        }
    }

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

    fn ingest_replay(&mut self, out: &mut Vec<RecordBatch>) -> Result<(), DbError> {
        loop {
            let available = self
                .max_in_flight
                .saturating_sub(self.retained_pending_rows());
            if available == 0 {
                return Ok(());
            }
            let Some((watermark, batch)) = self.replay.pop_front() else {
                return Ok(());
            };
            if batch.num_rows() == 0 {
                continue;
            }
            if batch.num_rows() > available {
                let head = batch.slice(0, available);
                let remaining = batch.num_rows() - available;
                self.replay
                    .push_front((watermark, batch.slice(available, remaining)));
                self.ingest(head, watermark, out)?;
            } else {
                self.ingest(batch, watermark, out)?;
            }
        }
    }
}

/// Suffix a lookup column with the table name when it collides with a stream column.
/// The plan-time projection rewriter uses the same rule.
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
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::fixed(
            crate::operator::capability::OperatorImplementation::LookupEnrich,
        )
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.ensure_resolved()?;
        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        let mut enriched = Vec::new();

        let new_input = inputs.first().map_or(&[][..], Vec::as_slice);
        let to_ingest: Vec<RecordBatch> = new_input.to_vec();

        self.replay.extend(
            to_ingest
                .into_iter()
                .filter(|batch| batch.num_rows() > 0)
                .map(|batch| (watermark, batch)),
        );

        self.flush_unsubmitted();
        self.drain_results(&mut enriched)?;
        self.ingest_replay(&mut enriched)?;

        self.publish_in_flight();
        self.projection.apply(enriched).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        // In-flight batches re-ingest and re-fetch on restore; source reads are idempotent.
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
                .map_err(|e| {
                    DbError::Checkpoint(format!("lookup-enrich: checkpoint decode: {e}"))
                })?;
        let mut replay = VecDeque::with_capacity(blobs.len());
        for (wm, blob) in &blobs {
            let batch = deserialize_batch_stream(blob)
                .map_err(|e| DbError::Checkpoint(format!("lookup-enrich: restore: {e}")))?;
            replay.push_back((*wm, batch));
        }
        self.pending.clear();
        self.unsubmitted.clear();
        self.replay = replay;
        Ok(())
    }

    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        input.held_at(
            self.pending
                .values()
                .map(|batch| batch.ingest_watermark)
                .chain(self.replay.iter().map(|(watermark, _)| *watermark))
                .min(),
        )
    }

    fn wants_input(&self) -> bool {
        self.in_flight_rows() < self.max_in_flight
    }

    fn deferred_work_is_runnable(&self) -> bool {
        let resolved = self.resolved.as_ref();
        let submit_ready = !self.unsubmitted.is_empty()
            && resolved
                .and_then(|resolved| resolved.submit_tx.as_ref())
                .is_some_and(|sender| !sender.is_full());
        let result_ready = resolved
            .and_then(|resolved| resolved.result_rx.as_ref())
            .is_some_and(|receiver| !receiver.is_empty());
        let replay_ready =
            !self.replay.is_empty() && self.retained_pending_rows() < self.max_in_flight;
        submit_ready || result_ready || replay_ready
    }
}

#[cfg(test)]
mod tests;
