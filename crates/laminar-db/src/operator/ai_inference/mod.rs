//! Async-decoupled AI inference operator.
//!
//! Cache hits serve inline; misses go to a Ring 1 worker over a bounded channel
//! and emit in a later cycle. `process` never awaits on the inference path.
//! Backpressure via `wants_input`; output watermark held behind oldest in-flight
//! batch so enriched rows aren't dropped late by a downstream window.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Builder, Float64Builder, ListBuilder, RecordBatch, StringArray,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use crossfire::AsyncTxTrait as _;
use datafusion::prelude::SessionContext;
use rustc_hash::FxHashMap;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::ai::{
    content_hash, params_version, AiCacheKey, AiCallLog, AiResultCache, BackendKind, CachedOutput,
    InferenceParams, InferenceProvider, Task,
};
use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};

use crate::ai_worker::{run_worker, MissRow, WorkItem, WorkResult, WorkerContext};
use crate::error::DbError;
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, InputFrontier, OperatorCheckpoint};

/// Must match `sql_analysis::AI_TMP_TABLE` — `projection_sql` reads from this name.
const AI_TMP_TABLE: &str = "__ai_tmp";

const SUBMIT_CAPACITY: usize = 256;
const RESULT_CAPACITY: usize = 256;
const MAX_IN_FLIGHT_ROWS: usize = 8192;

/// Static configuration for an [`AiInferenceOperator`].
pub(crate) struct AiOperatorConfig {
    pub task: Task,
    pub kind: BackendKind,
    pub model_id: u32,
    pub model: String,
    pub input_column: String,
    pub output_column: String,
    pub labels: Option<Vec<String>>,
}

/// A batch awaiting enrichment: original rows plus per-row outputs that fill in as
/// the worker resolves them. `pending` counts rows still in flight.
struct PendingBatch {
    batch: RecordBatch,
    outputs: Vec<Option<CachedOutput>>,
    pending: usize,
    // Held until the batch emits so its rows aren't dropped late downstream.
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
    submit_tx: crate::ai_worker::SubmitTx,
    result_rx: mpsc::Receiver<WorkResult>,
    pending: FxHashMap<u64, PendingBatch>,
    // Rejected by a saturated worker; retried next cycle in order.
    unsubmitted: VecDeque<WorkItem>,
    // Recovered batches queued with their original ingest watermark so they
    // don't re-ingest under a newer watermark and get dropped late downstream.
    replay: VecDeque<(i64, RecordBatch)>,
    next_batch_id: u64,
    max_in_flight: usize,
    projection: ProjectingJoinState,
    _worker: tokio::task::JoinHandle<()>,
}

impl AiInferenceOperator {
    /// Build the operator and spawn its Ring 1 worker on `runtime`.
    ///
    /// `runtime` must be the main (multi-threaded) runtime handle, not the
    /// `laminar-compute` one. Never pass `Handle::current()` from a compute thread.
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

        let (submit_tx, submit_rx) = crossfire::mpsc::bounded_async(SUBMIT_CAPACITY);
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

    fn in_flight_rows(&self) -> usize {
        let pending: usize = self.pending.values().map(|pb| pb.pending).sum();
        let queued: usize = self.unsubmitted.iter().map(|item| item.rows.len()).sum();
        pending + queued
    }

    #[cfg(test)]
    fn set_max_in_flight(&mut self, cap: usize) {
        self.max_in_flight = cap;
    }

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

    fn apply_result(
        &mut self,
        result: &WorkResult,
        out: &mut Vec<RecordBatch>,
    ) -> Result<(), DbError> {
        let Some(pb) = self.pending.get_mut(&result.batch_id) else {
            return Ok(()); // cleared on restore
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

    fn flush_unsubmitted(&mut self) {
        while let Some(item) = self.unsubmitted.pop_front() {
            match self.submit_tx.try_send(item) {
                Err(crossfire::TrySendError::Full(item)) => {
                    self.unsubmitted.push_front(item);
                    break;
                }
                Ok(()) | Err(crossfire::TrySendError::Disconnected(_)) => {}
            }
        }
    }

    fn submit(&mut self, item: WorkItem) {
        if !self.unsubmitted.is_empty() {
            self.unsubmitted.push_back(item);
            return;
        }
        match self.submit_tx.try_send(item) {
            Err(crossfire::TrySendError::Full(item)) => self.unsubmitted.push_back(item),
            Ok(()) | Err(crossfire::TrySendError::Disconnected(_)) => {}
        }
    }

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
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::fixed(
            crate::operator::capability::OperatorImplementation::AiInference,
        )
    }

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
        // In-flight requests are re-run on restore; local is deterministic,
        // remote dedups via the content-hash cache.
        let serialize = |b: &RecordBatch| {
            serialize_batch_stream(b).map_err(|e| {
                DbError::Pipeline(format!("ai operator: checkpoint serialization: {e}"))
            })
        };
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
                .map_err(|e| DbError::Checkpoint(format!("ai operator: checkpoint decode: {e}")))?;
        let mut replay = VecDeque::with_capacity(blobs.len());
        for (watermark, blob) in &blobs {
            let batch = deserialize_batch_stream(blob).map_err(|e| {
                DbError::Checkpoint(format!("ai operator: checkpoint deserialization: {e}"))
            })?;
            replay.push_back((*watermark, batch));
        }
        self.pending.clear();
        self.unsubmitted.clear();
        self.replay = replay;
        Ok(())
    }

    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        input.held_at(self.pending.values().map(|pb| pb.ingest_watermark).min())
    }

    fn wants_input(&self) -> bool {
        self.in_flight_rows() < self.max_in_flight
    }

    fn deferred_work_is_runnable(&self) -> bool {
        (!self.unsubmitted.is_empty() && !self.submit_tx.is_full())
            || !self.replay.is_empty()
            || !self.result_rx.is_empty()
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
mod tests;
