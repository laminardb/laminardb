use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::ai::{InferenceOutputs, InferenceRequest, InferenceResponse, ProviderError, Usage};
use async_trait::async_trait;
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

fn idle_frontier(watermark: i64) -> InputFrontier {
    InputFrontier {
        watermark: Some(watermark),
        idle: true,
    }
}

#[tokio::test]
async fn late_checkpoint_decode_failure_preserves_replay_state() {
    let mut op = operator(Arc::new(Failing));
    op.replay.push_back((7, text_batch(&["existing"])));
    let valid = serialize_batch_stream(&text_batch(&["replacement"])).unwrap();
    let blobs = vec![(8, valid), (9, b"not-arrow-ipc".to_vec())];
    let data = rkyv::to_bytes::<rkyv::rancor::Error>(&blobs)
        .unwrap()
        .to_vec();

    let error = op.restore(OperatorCheckpoint { data }).unwrap_err();

    assert!(matches!(error, DbError::Checkpoint(_)));
    assert!(error.requires_pipeline_recovery());
    assert_eq!(op.replay.len(), 1);
    assert_eq!(op.replay.front().map(|(watermark, _)| *watermark), Some(7));
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
    let output = restored.output_frontier(idle_frontier(10_000));
    assert_eq!(output.watermark, Some(100));
    assert!(!output.idle);
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
    assert!(!op.deferred_work_is_runnable());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontier_is_held_while_in_flight() {
    let mut op = operator(Arc::new(SlowEcho {
        delay: Duration::from_millis(50),
        calls: Arc::new(AtomicU64::new(0)),
    }));
    assert_eq!(
        op.output_frontier(idle_frontier(5)),
        idle_frontier(5),
        "no hold with nothing in flight"
    );

    // Ingest a cache miss at watermark 1000 — held there while in flight.
    let _ = op
        .process(&[vec![text_batch(&["x"])]], &[1000])
        .await
        .unwrap();
    let output = op.output_frontier(idle_frontier(5_000));
    assert_eq!(output.watermark, Some(1000));
    assert!(!output.idle);

    // After the worker resolves and the row is emitted, the hold is released
    // even though the input watermark has since advanced.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = op.process(&[], &[5000]).await.unwrap();
    assert_eq!(
        op.output_frontier(idle_frontier(5_000)),
        idle_frontier(5_000),
        "released after emission"
    );
}
