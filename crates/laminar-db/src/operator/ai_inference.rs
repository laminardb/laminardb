//! Async-decoupled AI inference operator.
//!
//! This is the one operator that does not do its work inside `process`.
//! `GraphOperator::process` runs on the `laminar-compute` thread (Ring 0), where
//! network and blocking calls are forbidden, so the model call cannot happen
//! there. Instead `process` is **decoupled across cycles**:
//!
//! - serve cache hits inline (a memory lookup);
//! - hand cache misses to the Ring 1 worker over a bounded channel and return;
//! - drain whatever results the worker has completed since the last cycle and
//!   emit those batches.
//!
//! Crucially, `process` contains no `await` on the inference path — it only does
//! synchronous channel and Arrow work — so it provably cannot block on a model
//! call. Enriched rows are therefore emitted in a later cycle than they arrived
//! (see the checkpoint handling for how in-flight rows survive recovery).
//!
//! Backpressure: once in-flight misses exceed a cap, `wants_input` returns
//! false, so the graph stops draining this node's input buffer and the source
//! is throttled through the existing input-buffer gate (effective when
//! input-buffer caps are configured). The output watermark is held behind the
//! oldest in-flight batch (`watermark_hold`) so async-enriched rows aren't
//! dropped as late by a downstream window. `estimated_state_bytes` reports
//! retained input batches plus queued miss text.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Builder, Float64Builder, ListBuilder, RecordBatch, StringArray,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use rustc_hash::FxHashMap;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use laminar_ai::{
    content_hash, params_version, AiCacheKey, AiCallLog, AiResultCache, BackendKind, CachedOutput,
    InferenceParams, InferenceProvider, Task,
};
use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};

use crate::ai_worker::{run_worker, MissRow, WorkItem, WorkResult, WorkerContext};
use crate::error::DbError;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

/// Temp table the enriched batch is registered under for the residual
/// projection. Must match `sql_analysis`'s `AI_TMP_TABLE` (the generated
/// `projection_sql` reads from this name).
const AI_TMP_TABLE: &str = "__ai_tmp";

const SUBMIT_CAPACITY: usize = 256;
const RESULT_CAPACITY: usize = 256;
/// In-flight miss rows above which the operator refuses new input, so its input
/// buffer fills and the graph backpressures the source (when input-buffer caps
/// are configured).
const MAX_IN_FLIGHT_ROWS: usize = 8192;

/// Static configuration for an [`AiInferenceOperator`].
pub(crate) struct AiOperatorConfig {
    /// The task this operator performs.
    pub task: Task,
    /// The backend kind (selects the worker's adapter path).
    pub kind: BackendKind,
    /// Stable per-model integer for the cache key.
    pub model_id: u32,
    /// Provider/runtime model identifier.
    pub model: String,
    /// Name of the input column to read text from.
    pub input_column: String,
    /// Name of the output column to append.
    pub output_column: String,
    /// Effective labels (local classification + cache versioning).
    pub labels: Option<Vec<String>>,
}

/// A batch awaiting enrichment: the original rows plus per-row outputs that fill
/// in as the worker resolves them. `pending` counts rows still in flight.
struct PendingBatch {
    batch: RecordBatch,
    outputs: Vec<Option<CachedOutput>>,
    pending: usize,
    /// Input watermark when this batch was ingested. Holds the operator's output
    /// watermark until the batch emits, so its rows aren't late downstream.
    ingest_watermark: i64,
}

/// Backend-agnostic AI inference operator (Ring 1 work, off the compute thread).
pub(crate) struct AiInferenceOperator {
    task: Task,
    model_id: u32,
    input_column: String,
    output_column: String,
    params_version: u64,
    cache: Arc<AiResultCache>,
    submit_tx: mpsc::Sender<WorkItem>,
    result_rx: mpsc::Receiver<WorkResult>,
    pending: FxHashMap<u64, PendingBatch>,
    /// Work items the worker was too busy to accept; retried next cycle.
    unsubmitted: VecDeque<WorkItem>,
    /// Input batches awaiting (re-)ingestion, each with the watermark it was
    /// originally ingested under — filled on restore so recovered rows re-ingest
    /// at their original watermark, not a newer one (else they'd be dropped as
    /// late downstream).
    replay: VecDeque<(i64, RecordBatch)>,
    next_batch_id: u64,
    /// In-flight cap; above this, [`wants_input`](Self::wants_input) is false.
    max_in_flight: usize,
    /// Residual projection applied to enriched batches (the user's SELECT with
    /// the `ai_*` call rewritten to its alias column). Bounded local work — not
    /// the inference path.
    projection: ProjectingJoinState,
    _worker: tokio::task::JoinHandle<()>,
}

impl AiInferenceOperator {
    /// Build the operator and spawn its Ring 1 worker on `runtime`.
    ///
    /// `runtime` MUST be the engine's main (multi-threaded) runtime handle, not
    /// the `laminar-compute` runtime — the worker must run off the compute
    /// thread. When wiring construction during a dynamic DDL hot-add (which
    /// itself runs on the compute thread), pass the main handle captured at
    /// start, never `Handle::current()`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        name: &str,
        config: AiOperatorConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
        provider: Arc<dyn InferenceProvider>,
        cache: Arc<AiResultCache>,
        call_log: Arc<AiCallLog>,
        runtime: &Handle,
    ) -> Self {
        let params = InferenceParams {
            labels: config.labels.clone(),
        };
        let params_version = params_version(&params);

        let (submit_tx, submit_rx) = mpsc::channel(SUBMIT_CAPACITY);
        let (result_tx, result_rx) = mpsc::channel(RESULT_CAPACITY);

        let worker_ctx = WorkerContext {
            provider,
            cache: Arc::clone(&cache),
            call_log,
            task: config.task,
            kind: config.kind,
            model: config.model,
            params,
            labels: config.labels,
        };
        let worker = runtime.spawn(run_worker(worker_ctx, submit_rx, result_tx));

        Self {
            task: config.task,
            model_id: config.model_id,
            input_column: config.input_column,
            output_column: config.output_column,
            params_version,
            cache,
            submit_tx,
            result_rx,
            pending: FxHashMap::default(),
            unsubmitted: VecDeque::new(),
            replay: VecDeque::new(),
            next_batch_id: 0,
            max_in_flight: MAX_IN_FLIGHT_ROWS,
            projection: ProjectingJoinState::new(name, ctx, projection_sql, AI_TMP_TABLE),
            _worker: worker,
        }
    }

    /// Total miss rows currently in flight (submitted-but-unresolved + queued).
    fn in_flight_rows(&self) -> usize {
        let pending: usize = self.pending.values().map(|pb| pb.pending).sum();
        let queued: usize = self.unsubmitted.iter().map(|item| item.rows.len()).sum();
        pending + queued
    }

    #[cfg(test)]
    fn set_max_in_flight(&mut self, cap: usize) {
        self.max_in_flight = cap;
    }

    /// Resolve a row against the cache, or queue it as a miss. `watermark` is the
    /// input watermark at ingest, used to hold the output watermark while the
    /// batch is in flight.
    fn ingest(
        &mut self,
        batch: RecordBatch,
        watermark: i64,
        out: &mut Vec<RecordBatch>,
    ) -> Result<(), DbError> {
        let n = batch.num_rows();
        let (outputs, misses) = {
            let texts = self.input_texts(&batch)?;
            let mut outputs: Vec<Option<CachedOutput>> = vec![None; n];
            let mut misses: Vec<MissRow> = Vec::new();
            for (row_index, text) in texts.iter().enumerate() {
                let Some(text) = text else { continue }; // null input → null output
                let key = AiCacheKey {
                    content_hash: content_hash(text),
                    model_id: self.model_id,
                    task: self.task,
                    params_version: self.params_version,
                };
                if let Some(cached) = self.cache.get(&key) {
                    outputs[row_index] = Some(cached);
                } else {
                    misses.push(MissRow {
                        row_index,
                        text: (*text).to_string(),
                        key,
                    });
                }
            }
            (outputs, misses)
        };

        if misses.is_empty() {
            out.push(self.build_output(&batch, &outputs)?);
            return Ok(());
        }

        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;
        self.pending.insert(
            batch_id,
            PendingBatch {
                batch,
                outputs,
                pending: misses.len(),
                ingest_watermark: watermark,
            },
        );
        self.submit(WorkItem {
            batch_id,
            rows: misses,
        });
        Ok(())
    }

    /// Apply a worker result, emitting the batch if it is now fully resolved.
    fn apply_result(
        &mut self,
        result: &WorkResult,
        out: &mut Vec<RecordBatch>,
    ) -> Result<(), DbError> {
        let Some(pb) = self.pending.get_mut(&result.batch_id) else {
            return Ok(()); // batch no longer tracked (e.g. cleared on restore)
        };
        for (position, &row_index) in result.row_indices.iter().enumerate() {
            if pb.outputs[row_index].is_some() {
                continue;
            }
            pb.outputs[row_index] = match &result.outputs {
                Ok(values) => values.get(position).cloned(),
                Err(_) => None, // batch failed → NULL for these rows
            };
            pb.pending -= 1;
        }
        if pb.pending == 0 {
            let pb = self
                .pending
                .remove(&result.batch_id)
                .expect("present above");
            out.push(self.build_output(&pb.batch, &pb.outputs)?);
        }
        Ok(())
    }

    /// Retry work items the worker rejected while saturated, preserving order.
    fn flush_unsubmitted(&mut self) {
        while let Some(item) = self.unsubmitted.pop_front() {
            match self.submit_tx.try_send(item) {
                Err(mpsc::error::TrySendError::Full(item)) => {
                    self.unsubmitted.push_front(item);
                    break;
                }
                // Delivered, or the worker is gone (shutdown) — nothing to retry.
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
    }

    /// Submit a work item, queueing it if the worker is saturated.
    fn submit(&mut self, item: WorkItem) {
        if !self.unsubmitted.is_empty() {
            self.unsubmitted.push_back(item);
            return;
        }
        match self.submit_tx.try_send(item) {
            Err(mpsc::error::TrySendError::Full(item)) => self.unsubmitted.push_back(item),
            // Delivered, or the worker is gone (shutdown) — nothing to queue.
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Extract the input column as per-row optional text.
    fn input_texts<'a>(&self, batch: &'a RecordBatch) -> Result<Vec<Option<&'a str>>, DbError> {
        let column = batch.column_by_name(&self.input_column).ok_or_else(|| {
            DbError::InvalidOperation(format!(
                "ai operator: input column '{}' not found",
                self.input_column
            ))
        })?;
        let array = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DbError::InvalidOperation(format!(
                    "ai operator: input column '{}' must be Utf8",
                    self.input_column
                ))
            })?;
        Ok((0..array.len())
            .map(|i| (!array.is_null(i)).then(|| array.value(i)))
            .collect())
    }

    /// Append the AI output column to a batch.
    fn build_output(
        &self,
        batch: &RecordBatch,
        outputs: &[Option<CachedOutput>],
    ) -> Result<RecordBatch, DbError> {
        let (array, field) = match self.task {
            Task::Embed => (
                build_embedding_array(outputs)?,
                Field::new(&self.output_column, embedding_type(), true),
            ),
            Task::Sentiment => (
                build_score_array(outputs)?,
                Field::new(&self.output_column, DataType::Float64, true),
            ),
            _ => (
                build_text_array(outputs)?,
                Field::new(&self.output_column, DataType::Utf8, true),
            ),
        };

        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(field);
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(array);

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| DbError::InvalidOperation(format!("ai operator: build output: {e}")))
    }
}

#[async_trait]
impl GraphOperator for AiInferenceOperator {
    // The only `await` is the residual projection — bounded local DataFusion
    // work over the just-enriched batches, the same step every join operator
    // runs. The model call never happens here: misses go to the Ring 1 worker
    // and results are drained on a later cycle, so this cannot block on
    // inference (R1).
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        let mut enriched = Vec::new();

        self.flush_unsubmitted();
        while let Ok(result) = self.result_rx.try_recv() {
            self.apply_result(&result, &mut enriched)?;
        }
        while let Some((replay_watermark, batch)) = self.replay.pop_front() {
            self.ingest(batch, replay_watermark, &mut enriched)?;
        }
        for batch in inputs.first().map_or(&[][..], Vec::as_slice) {
            self.ingest(batch.clone(), watermark, &mut enriched)?;
        }

        self.projection.apply(enriched).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        // Capture every not-yet-emitted input batch: in-flight pending batches
        // plus any queued for replay. In-flight requests are not treated as
        // complete; on restore they are re-ingested and re-run (local is
        // deterministic, remote dedups via the content-hash cache).
        let serialize = |b: &RecordBatch| {
            serialize_batch_stream(b).map_err(|e| {
                DbError::Pipeline(format!("ai operator: checkpoint serialization: {e}"))
            })
        };
        // Each entry keeps the watermark its batch was ingested under, so on
        // restore the rows re-ingest at the same watermark and aren't dropped as
        // late by a downstream window.
        let mut blobs: Vec<(i64, Vec<u8>)> =
            Vec::with_capacity(self.pending.len() + self.replay.len());
        for pb in self.pending.values() {
            blobs.push((pb.ingest_watermark, serialize(&pb.batch)?));
        }
        for (watermark, batch) in &self.replay {
            blobs.push((*watermark, serialize(batch)?));
        }

        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&blobs)
            .map(|v| v.to_vec())
            .map_err(|e| DbError::Pipeline(format!("ai operator: checkpoint encode: {e}")))?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let blobs: Vec<(i64, Vec<u8>)> =
            rkyv::from_bytes::<Vec<(i64, Vec<u8>)>, rkyv::rancor::Error>(&checkpoint.data)
                .map_err(|e| DbError::Pipeline(format!("ai operator: checkpoint decode: {e}")))?;
        self.pending.clear();
        self.unsubmitted.clear();
        self.replay.clear();
        for (watermark, blob) in &blobs {
            let batch = deserialize_batch_stream(blob).map_err(|e| {
                DbError::Pipeline(format!("ai operator: checkpoint deserialization: {e}"))
            })?;
            self.replay.push_back((*watermark, batch));
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
        let unsubmitted: usize = self
            .unsubmitted
            .iter()
            .flat_map(|item| item.rows.iter())
            .map(|row| row.text.len())
            .sum();
        pending + replay + unsubmitted
    }

    /// Hold the output watermark behind the oldest in-flight batch so async
    /// enrichment isn't dropped as late by a downstream window.
    fn watermark_hold(&self) -> Option<i64> {
        self.pending.values().map(|pb| pb.ingest_watermark).min()
    }

    /// Refuse new input once the in-flight cap is reached. The graph then leaves
    /// this node's input buffered (backpressuring the source) while still
    /// stepping the operator with empty input so it drains and emits.
    fn wants_input(&self) -> bool {
        self.in_flight_rows() < self.max_in_flight
    }
}

fn embedding_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
}

fn build_text_array(outputs: &[Option<CachedOutput>]) -> Result<ArrayRef, DbError> {
    let mut builder = StringBuilder::new();
    for output in outputs {
        match output {
            Some(CachedOutput::Text(s)) => builder.append_value(s),
            None => builder.append_null(),
            Some(CachedOutput::Vector(_) | CachedOutput::Score(_)) => {
                return Err(DbError::InvalidOperation(
                    "ai operator: expected text output, got a vector/score".to_string(),
                ));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build the `Float64` score column for `ai_sentiment`. A null input or a
/// failed inference yields a null score.
fn build_score_array(outputs: &[Option<CachedOutput>]) -> Result<ArrayRef, DbError> {
    let mut builder = Float64Builder::new();
    for output in outputs {
        match output {
            Some(CachedOutput::Score(v)) => builder.append_value(*v),
            None => builder.append_null(),
            Some(CachedOutput::Text(_) | CachedOutput::Vector(_)) => {
                return Err(DbError::InvalidOperation(
                    "ai operator: expected a score output, got text/vector".to_string(),
                ));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_embedding_array(outputs: &[Option<CachedOutput>]) -> Result<ArrayRef, DbError> {
    let mut builder = ListBuilder::new(Float32Builder::new());
    for output in outputs {
        match output {
            Some(CachedOutput::Vector(v)) => {
                builder.values().append_slice(v);
                builder.append(true);
            }
            None => builder.append(false),
            Some(CachedOutput::Text(_) | CachedOutput::Score(_)) => {
                return Err(DbError::InvalidOperation(
                    "ai operator: expected a vector output, got text/score".to_string(),
                ));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use laminar_ai::{InferenceOutputs, InferenceRequest, InferenceResponse, ProviderError, Usage};
    use tokio::runtime::Handle;

    /// Provider that sleeps then echoes `L:<input>` for each row.
    struct SlowEcho {
        delay: Duration,
        calls: Arc<AtomicU64>,
    }

    #[async_trait]
    impl InferenceProvider for SlowEcho {
        async fn infer_batch(
            &self,
            request: InferenceRequest,
        ) -> Result<InferenceResponse, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            let outputs =
                InferenceOutputs::Text(request.inputs.iter().map(|s| format!("L:{s}")).collect());
            Ok(InferenceResponse {
                outputs,
                usage: Usage::ZERO,
            })
        }

        fn name(&self) -> &'static str {
            "slow-echo"
        }
    }

    /// Provider that always fails.
    struct Failing;

    #[async_trait]
    impl InferenceProvider for Failing {
        async fn infer_batch(
            &self,
            _request: InferenceRequest,
        ) -> Result<InferenceResponse, ProviderError> {
            Err(ProviderError::Transport("boom".to_string()))
        }

        fn name(&self) -> &'static str {
            "failing"
        }
    }

    fn config() -> AiOperatorConfig {
        AiOperatorConfig {
            task: Task::Classify,
            kind: BackendKind::Remote,
            model_id: 1,
            model: "echo".to_string(),
            input_column: "text".to_string(),
            output_column: "label".to_string(),
            labels: None,
        }
    }

    fn text_batch(values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))]).unwrap()
    }

    fn label_column(batch: &RecordBatch) -> Vec<Option<String>> {
        let idx = batch.schema().index_of("label").unwrap();
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..array.len())
            .map(|i| (!array.is_null(i)).then(|| array.value(i).to_string()))
            .collect()
    }

    fn operator(provider: Arc<dyn InferenceProvider>) -> AiInferenceOperator {
        // projection_sql = None → ProjectingJoinState passes the enriched batch
        // through, so the appended `label` column is the output.
        AiInferenceOperator::new(
            "ai_test",
            config(),
            None,
            laminar_sql::create_session_context(),
            provider,
            Arc::new(AiResultCache::with_defaults()),
            Arc::new(AiCallLog::with_defaults()),
            &Handle::current(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_never_blocks_on_inference() {
        let calls = Arc::new(AtomicU64::new(0));
        let mut op = operator(Arc::new(SlowEcho {
            delay: Duration::from_millis(400),
            calls: Arc::clone(&calls),
        }));

        let start = Instant::now();
        let first = op
            .process(&[vec![text_batch(&["good news", "bad news"])]], &[0])
            .await
            .unwrap();
        let elapsed = start.elapsed();

        // Returned well before the 400ms inference, with nothing ready yet.
        assert!(
            elapsed < Duration::from_millis(200),
            "process blocked: {elapsed:?}"
        );
        assert!(first.is_empty());

        tokio::time::sleep(Duration::from_millis(700)).await;
        let second = op.process(&[], &[0]).await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(
            label_column(&second[0]),
            vec![
                Some("L:good news".to_string()),
                Some("L:bad news".to_string())
            ]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one batched call");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_hits_emit_in_the_same_cycle() {
        let cache = Arc::new(AiResultCache::with_defaults());
        let key = AiCacheKey {
            content_hash: content_hash("hello"),
            model_id: 1,
            task: Task::Classify, // must match config()'s task or the hit becomes a miss
            params_version: params_version(&InferenceParams { labels: None }),
        };
        cache.insert(key, CachedOutput::Text("cached".to_string()));

        let mut op = AiInferenceOperator::new(
            "ai_test",
            config(),
            None,
            laminar_sql::create_session_context(),
            Arc::new(SlowEcho {
                delay: Duration::from_secs(10),
                calls: Arc::new(AtomicU64::new(0)),
            }),
            Arc::clone(&cache),
            Arc::new(AiCallLog::with_defaults()),
            &Handle::current(),
        );

        let out = op
            .process(&[vec![text_batch(&["hello"])]], &[0])
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(label_column(&out[0]), vec![Some("cached".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn provider_failure_emits_null() {
        let mut op = operator(Arc::new(Failing));
        let first = op.process(&[vec![text_batch(&["x"])]], &[0]).await.unwrap();
        assert!(first.is_empty());

        tokio::time::sleep(Duration::from_millis(100)).await;
        let out = op.process(&[], &[0]).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(label_column(&out[0]), vec![None]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_reenriches_in_flight_rows() {
        let mut op = operator(Arc::new(SlowEcho {
            delay: Duration::from_millis(50),
            calls: Arc::new(AtomicU64::new(0)),
        }));
        let first = op
            .process(&[vec![text_batch(&["pending row"])]], &[100])
            .await
            .unwrap();
        assert!(first.is_empty(), "row should still be in flight");
        let checkpoint = op.checkpoint().unwrap().unwrap();

        // Fresh operator with a fresh cache, restored from the checkpoint.
        let mut restored = operator(Arc::new(SlowEcho {
            delay: Duration::from_millis(50),
            calls: Arc::new(AtomicU64::new(0)),
        }));
        restored.restore(checkpoint).unwrap();

        // Re-ingest at a far later watermark than the row first arrived under.
        let _ = restored.process(&[], &[10_000]).await.unwrap();
        // The recovered row must still hold the watermark at its original ingest
        // time (100), not the current one, or a downstream window drops it late.
        assert_eq!(restored.watermark_hold(), Some(100));
        tokio::time::sleep(Duration::from_millis(250)).await;
        let out = restored.process(&[], &[10_000]).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            label_column(&out[0]),
            vec![Some("L:pending row".to_string())]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refuses_input_over_in_flight_cap() {
        let mut op = operator(Arc::new(SlowEcho {
            delay: Duration::from_secs(10), // keep misses in flight
            calls: Arc::new(AtomicU64::new(0)),
        }));
        op.set_max_in_flight(1);
        assert!(op.wants_input(), "accepts input when empty");

        // Two distinct misses exceed the cap of 1 → backpressured.
        let _ = op
            .process(&[vec![text_batch(&["a", "b"])]], &[0])
            .await
            .unwrap();
        assert!(!op.wants_input(), "over the in-flight cap → refuses input");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watermark_is_held_while_in_flight() {
        let mut op = operator(Arc::new(SlowEcho {
            delay: Duration::from_millis(50),
            calls: Arc::new(AtomicU64::new(0)),
        }));
        assert_eq!(op.watermark_hold(), None, "no hold with nothing in flight");

        // Ingest a cache miss at watermark 1000 — held there while in flight.
        let _ = op
            .process(&[vec![text_batch(&["x"])]], &[1000])
            .await
            .unwrap();
        assert_eq!(op.watermark_hold(), Some(1000));

        // After the worker resolves and the row is emitted, the hold is released
        // even though the input watermark has since advanced.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = op.process(&[], &[5000]).await.unwrap();
        assert_eq!(op.watermark_hold(), None, "released after emission");
    }
}
