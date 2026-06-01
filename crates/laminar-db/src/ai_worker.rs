//! Ring 1 inference worker.
//!
//! Runs off the compute thread on the main tokio runtime. It pulls cache-miss
//! batches submitted by [`AiInferenceOperator`](crate::operator::ai_inference),
//! runs them through the provider, parses the response into the task's output,
//! writes the result cache and the `laminar.ai_calls` log, and returns the
//! per-row outputs over a channel. The operator never blocks on any of this —
//! it submits and later drains, so the model call never touches Ring 0.
//!
//! One `infer_batch` call per work item; cross-item concurrency, retry,
//! backoff, and timeout are the provider's concern.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::ai::{
    parse_response, AiCacheKey, AiCallLog, AiCallRecord, AiResultCache, BackendKind, CachedOutput,
    CallOutcome, InferenceOutputs, InferenceParams, InferenceProvider, InferenceRequest, Task,
    Usage,
};
use std::sync::Arc;
use tokio::sync::mpsc;

/// A row that missed the cache and needs inference.
pub(crate) struct MissRow {
    /// Row index within the originating batch.
    pub row_index: usize,
    /// Input text to run through the model.
    pub text: String,
    /// Cache key under which the result is stored.
    pub key: AiCacheKey,
}

/// A batch of cache-miss rows submitted to the worker.
pub(crate) struct WorkItem {
    /// Id of the originating pending batch.
    pub batch_id: u64,
    /// The miss rows, in input order.
    pub rows: Vec<MissRow>,
}

/// The worker's reply for one [`WorkItem`].
pub(crate) struct WorkResult {
    /// Id of the originating pending batch.
    pub batch_id: u64,
    /// Row indices the item covered, in order — present even on failure so the
    /// operator knows which rows to resolve (to NULL on failure).
    pub row_indices: Vec<usize>,
    /// Per-row outputs aligned to `row_indices`, or a batch-level error message.
    pub outputs: Result<Vec<CachedOutput>, String>,
}

/// Immutable inference context shared by the worker across items.
pub(crate) struct WorkerContext {
    /// Transport backend.
    pub provider: Arc<dyn InferenceProvider>,
    /// Result cache (written on success).
    pub cache: Arc<AiResultCache>,
    /// Call log (written for every call, success or failure).
    pub call_log: Arc<AiCallLog>,
    /// The task performed.
    pub task: Task,
    /// The backend kind (selects the adapter path).
    pub kind: BackendKind,
    /// Provider/runtime model identifier.
    pub model: String,
    /// Request parameters (labels).
    pub params: InferenceParams,
    /// Effective labels for local classification.
    pub labels: Option<Vec<String>>,
}

/// Drive the worker until the submit channel closes (operator dropped).
pub(crate) async fn run_worker(
    ctx: WorkerContext,
    mut submit_rx: mpsc::Receiver<WorkItem>,
    result_tx: mpsc::Sender<WorkResult>,
) {
    while let Some(item) = submit_rx.recv().await {
        let result = infer_one(&ctx, item).await;
        if result_tx.send(result).await.is_err() {
            break;
        }
    }
}

async fn infer_one(ctx: &WorkerContext, item: WorkItem) -> WorkResult {
    let batch_id = item.batch_id;
    let row_indices: Vec<usize> = item.rows.iter().map(|r| r.row_index).collect();
    let started = Instant::now();
    let batch_size = u32::try_from(item.rows.len()).unwrap_or(u32::MAX);

    let request = InferenceRequest {
        task: ctx.task,
        model: ctx.model.clone(),
        inputs: item.rows.iter().map(|r| r.text.clone()).collect(),
        params: ctx.params.clone(),
    };

    match run_request(ctx, request, item.rows.len()).await {
        Ok((outputs, usage)) => {
            for (row, output) in item.rows.iter().zip(outputs.iter()) {
                ctx.cache.insert(row.key, output.clone());
            }
            ctx.call_log.record(record(
                ctx,
                batch_size,
                started,
                usage,
                &CallOutcome::Success,
            ));
            WorkResult {
                batch_id,
                row_indices,
                outputs: Ok(outputs),
            }
        }
        Err(message) => {
            ctx.call_log.record(record(
                ctx,
                batch_size,
                started,
                Usage::ZERO,
                &CallOutcome::Failure(message.clone()),
            ));
            WorkResult {
                batch_id,
                row_indices,
                outputs: Err(message),
            }
        }
    }
}

async fn run_request(
    ctx: &WorkerContext,
    request: InferenceRequest,
    expected_rows: usize,
) -> Result<(Vec<CachedOutput>, Usage), String> {
    let response = ctx
        .provider
        .infer_batch(request)
        .await
        .map_err(|e| e.to_string())?;
    let usage = response.usage;
    // Labels known at startup win; otherwise fall back to the backend's intrinsic
    // labels, now resolvable since the call above ensured the model is on disk —
    // this is what lets a lazily downloaded local classifier score.
    let labels = ctx
        .labels
        .clone()
        .or_else(|| ctx.provider.intrinsic_labels(&ctx.model));
    let parsed = parse_response(ctx.task, ctx.kind, response.outputs, labels.as_deref())
        .map_err(|e| e.to_string())?;
    if parsed.len() != expected_rows {
        return Err(format!(
            "provider returned {} outputs for a {}-row batch",
            parsed.len(),
            expected_rows
        ));
    }
    Ok((to_cached(parsed), usage))
}

fn to_cached(outputs: InferenceOutputs) -> Vec<CachedOutput> {
    match outputs {
        InferenceOutputs::Text(values) => values.into_iter().map(CachedOutput::Text).collect(),
        InferenceOutputs::Vectors(values) => values.into_iter().map(CachedOutput::Vector).collect(),
        InferenceOutputs::Scores(values) => values.into_iter().map(CachedOutput::Score).collect(),
    }
}

fn record(
    ctx: &WorkerContext,
    batch_size: u32,
    started: Instant,
    usage: Usage,
    outcome: &CallOutcome,
) -> AiCallRecord {
    AiCallRecord {
        timestamp_ms: now_ms(),
        model: ctx.model.clone(),
        provider: ctx.provider.name(),
        task: ctx.task,
        kind: ctx.kind,
        batch_size,
        usage,
        latency_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        outcome: outcome.clone(),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
