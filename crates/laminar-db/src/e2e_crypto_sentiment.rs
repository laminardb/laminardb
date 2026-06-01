//! End-to-end tests for the crypto-sentiment pipeline, backed by wiremock.
//!
//! wiremock (HTTP) backs the **Anthropic provider**, so these tests drive the
//! *real* `AnthropicProvider` HTTP path — request build, 500/retry, response
//! parse — not a stub. The Binance/Jetstream feeds are non-replayable
//! websockets whose transport is exercised in `laminar-connectors`; here their
//! traces are hand-built source batches fed through the graph, which is what the
//! engine composition actually consumes. The analytically-exact correlation
//! check lives with the operator, in `operator::window_frame::tests`.

#![cfg(feature = "remote")]

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rustc_hash::FxHashMap;
use tokio::runtime::Handle;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::ai::backends::AnthropicProvider;
use crate::ai::{
    AiCallLog, AiResultCache, AiRuntime, CallOutcome, InferenceProvider, ModelBackend, ModelEntry,
    ModelRegistry, Task,
};

use crate::operator_graph::OperatorGraph;

// ── wiremock + runtime helpers ─────────────────────────────────────────────

/// A wiremock Anthropic Messages endpoint that always replies with `text`.
async fn anthropic_replying(text: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"content":[{{"type":"text","text":"{text}"}}],"usage":{{"input_tokens":5,"output_tokens":1}}}}"#
        )))
        .mount(&server)
        .await;
    server
}

/// An `AiRuntime` whose `sentiment` model is the real Anthropic provider pointed
/// at `base_url`. `call_log` is shared so a test can read `laminar.ai_calls`.
fn runtime_at(base_url: &str, call_log: Arc<AiCallLog>) -> Arc<AiRuntime> {
    let mut registry = ModelRegistry::new();
    registry
        .register(ModelEntry {
            id: "m".into(),
            tasks: vec![Task::Sentiment],
            backend: ModelBackend::Remote {
                provider: "anthropic".into(),
                model: "stub-model".into(),
            },
        })
        .unwrap();
    let provider = Arc::new(AnthropicProvider::new(base_url, "test-key", 4).unwrap())
        as Arc<dyn InferenceProvider>;
    Arc::new(AiRuntime::new(
        registry,
        [("anthropic".to_string(), provider)],
        None,
        Arc::new(AiResultCache::with_defaults()),
        call_log,
    ))
}

fn posts_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}

fn posts_batch(ids: &[i64], texts: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        posts_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(texts.to_vec())),
        ],
    )
    .unwrap()
}

fn sentiment_values(batches: &[RecordBatch]) -> Vec<Option<f64>> {
    let mut out = Vec::new();
    for b in batches {
        let col = b
            .column(b.schema().index_of("sentiment").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("sentiment is Float64");
        for i in 0..col.len() {
            out.push((!col.is_null(i)).then(|| col.value(i)));
        }
    }
    out
}

/// Drain empty cycles until `query` emits rows or a deadline — polls the async
/// Ring-1 worker instead of a fixed sleep that flakes under CI load.
async fn drain_until(graph: &mut OperatorGraph, query: &str) -> Vec<RecordBatch> {
    let key = Arc::from(query) as Arc<str>;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let results = graph
            .execute_cycle(&FxHashMap::default(), i64::MAX, None)
            .await
            .unwrap();
        if let Some(out) = results.get(&key) {
            if out.iter().map(RecordBatch::num_rows).sum::<usize>() > 0 {
                return out.clone();
            }
        }
        if std::time::Instant::now() >= deadline {
            return Vec::new();
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Feed one cycle, then drain until the Ring-1 worker's results land. Returns the
/// drained `query` output.
async fn score_and_drain(
    graph: &mut OperatorGraph,
    source: &str,
    query: &str,
    batch: RecordBatch,
) -> Vec<RecordBatch> {
    let mut sources = FxHashMap::default();
    sources.insert(Arc::from(source), vec![batch]);
    let _ = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();
    drain_until(graph, query).await
}

fn build_scoring_graph(runtime: Arc<AiRuntime>) -> OperatorGraph {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);
    graph.set_ai_runtime(runtime, Handle::current());
    graph.register_source_schema("posts".to_string(), posts_schema());
    graph.add_query(
        "scored".to_string(),
        "SELECT id, ai_sentiment(text, model => 'm') AS sentiment FROM posts".to_string(),
        None,
        None,
        None,
        None,
        None,
    );
    graph
        .take_build_errors()
        .expect("ai_sentiment routes cleanly");
    graph
}

// ── tests ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_smoke_scores_dedupes_and_logs() {
    let server = anthropic_replying("0.8").await;
    let call_log = Arc::new(AiCallLog::with_defaults());
    let mut graph = build_scoring_graph(runtime_at(&server.uri(), Arc::clone(&call_log)));

    // Cycle A: three distinct posts → three misses → three provider calls.
    let first = score_and_drain(
        &mut graph,
        "posts",
        "scored",
        posts_batch(&[1, 2, 3], &["bull run", "rug pull", "sideways chop"]),
    )
    .await;
    assert_eq!(
        sentiment_values(&first),
        vec![Some(0.8), Some(0.8), Some(0.8)]
    );

    // Cycle B: two of the same posts → both are cache hits, scored inline this
    // cycle with NO new provider call.
    let mut repeats = FxHashMap::default();
    repeats.insert(
        Arc::from("posts"),
        vec![posts_batch(&[4, 5], &["bull run", "rug pull"])],
    );
    let results = graph.execute_cycle(&repeats, i64::MAX, None).await.unwrap();
    let second = results[&(Arc::from("scored") as Arc<str>)].clone();
    assert_eq!(
        sentiment_values(&second),
        vec![Some(0.8), Some(0.8)],
        "repeats reproduce the cached score"
    );

    // 5 rows scored, but the provider was hit for the 3 distinct texts only —
    // dedupe is the cache, not the model.
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3, "repeats must not reach the provider");

    // One batch call recorded (batch_size 3), per-batch granularity.
    let calls = call_log.snapshot();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].batch_size, 3);
    assert_eq!(calls[0].outcome, CallOutcome::Success);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_degrades_to_null_on_provider_500() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let call_log = Arc::new(AiCallLog::with_defaults());
    let mut graph = build_scoring_graph(runtime_at(&server.uri(), Arc::clone(&call_log)));

    // A parallel non-AI stream that must keep flowing while sentiment degrades.
    graph.register_source_schema(
        "trades".to_string(),
        Arc::new(Schema::new(vec![
            Field::new("sym", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ])),
    );
    graph.add_query(
        "prices".to_string(),
        "SELECT sym, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        None,
    );
    graph
        .take_build_errors()
        .expect("passthrough routes cleanly");

    // Cycle 1: post queued for scoring; the price row flows immediately.
    let mut sources = FxHashMap::default();
    sources.insert(
        Arc::from("posts"),
        vec![posts_batch(&[1], &["bitcoin to the moon"])],
    );
    sources.insert(
        Arc::from("trades"),
        vec![RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("sym", DataType::Utf8, false),
                Field::new("price", DataType::Float64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["BTCUSDT"])),
                Arc::new(Float64Array::from(vec![64000.0])),
            ],
        )
        .unwrap()],
    );
    let cycle1 = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();
    assert!(
        cycle1.contains_key(&(Arc::from("prices") as Arc<str>)),
        "price feed keeps flowing while the provider is down"
    );

    // Poll until the 500s exhaust retries + backoff and the null score is emitted
    // (the pipeline must survive the provider outage and still produce a row).
    let scored = drain_until(&mut graph, "scored").await;
    assert_eq!(
        sentiment_values(&scored),
        vec![None],
        "terminal failure → null score"
    );

    let calls = call_log.snapshot();
    assert!(
        calls
            .iter()
            .any(|c| matches!(c.outcome, CallOutcome::Failure(_))),
        "the failure is recorded in laminar.ai_calls"
    );
}

// A plain INNER equi-join is a stateful processing-time join: it buffers each
// side and matches when the second side arrives, so two windowed views that
// close the same bucket in DIFFERENT cycles (e.g. when an upstream AI operator's
// watermark-hold delays one side) still join — with no watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_matches_buckets_closing_in_different_cycles() {
    // Two hand-built bucketed streams (the shape price_1m / sentiment_1m emit):
    // one row per closed minute, arriving on the bucket key at different times.
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    let price_schema = Arc::new(Schema::new(vec![
        Field::new("bucket", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let sent_schema = Arc::new(Schema::new(vec![
        Field::new("bucket", DataType::Int64, false),
        Field::new("ms", DataType::Float64, false),
    ]));
    graph.register_source_schema("price_b".to_string(), Arc::clone(&price_schema));
    graph.register_source_schema("sent_b".to_string(), Arc::clone(&sent_schema));
    graph.add_query(
        "joined".to_string(),
        "SELECT p.bucket AS bucket, p.price AS price, s.ms AS ms \
         FROM price_b p JOIN sent_b s ON p.bucket = s.bucket"
            .to_string(),
        None,
        None,
        None,
        None,
        None,
    );
    graph
        .take_build_errors()
        .expect("equi-join routes as a per-cycle batch join");

    let price_row = |b: i64, p: f64| {
        RecordBatch::try_new(
            Arc::clone(&price_schema),
            vec![
                Arc::new(Int64Array::from(vec![b])),
                Arc::new(Float64Array::from(vec![p])),
            ],
        )
        .unwrap()
    };
    let sent_row = |b: i64, m: f64| {
        RecordBatch::try_new(
            Arc::clone(&sent_schema),
            vec![
                Arc::new(Int64Array::from(vec![b])),
                Arc::new(Float64Array::from(vec![m])),
            ],
        )
        .unwrap()
    };

    let count = |r: &FxHashMap<Arc<str>, Vec<RecordBatch>>| {
        r.get(&(Arc::from("joined") as Arc<str>))
            .map_or(0, |v| v.iter().map(RecordBatch::num_rows).sum::<usize>())
    };

    // Cycle 1: only the price side closes bucket 1 (the sentiment side is still
    // being scored). The join buffers it and emits nothing yet.
    let mut c1 = FxHashMap::default();
    c1.insert(Arc::from("price_b"), vec![price_row(1, 100.0)]);
    let r1 = graph.execute_cycle(&c1, i64::MAX, None).await.unwrap();
    assert_eq!(count(&r1), 0, "no match until the second side closes");

    // Cycle 2: the sentiment side closes bucket 1 — the buffered price matches.
    let mut c2 = FxHashMap::default();
    c2.insert(Arc::from("sent_b"), vec![sent_row(1, 0.5)]);
    let r2 = graph.execute_cycle(&c2, i64::MAX, None).await.unwrap();
    assert_eq!(count(&r2), 1, "bucket 1 joins across cycles");
}
