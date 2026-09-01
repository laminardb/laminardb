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
            lookup_cache: Arc::new(LookupMemoryCache::with_defaults(0)),
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
async fn late_checkpoint_decode_failure_preserves_replay_state() {
    let source = Arc::new(MapSource {
        rows: FxHashMap::default(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut op = operator_with(LookupJoinType::Inner, source);
    op.replay.push_back((7, stream_batch(&[1], &[Some(10)])));
    let valid = serialize_batch_stream(&stream_batch(&[2], &[Some(20)])).unwrap();
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

#[tokio::test]
async fn replay_respects_active_row_cap_and_holds_its_watermark() {
    let source = Arc::new(MapSource {
        rows: FxHashMap::default(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut op = operator_with(LookupJoinType::Inner, source);
    op.max_in_flight = 2;
    let batch = stream_batch(
        &[1, 2, 3, 4, 5],
        &[Some(10), Some(20), Some(30), Some(40), Some(50)],
    );

    let output = op.process(&[vec![batch]], &[42]).await.unwrap();

    assert!(output.is_empty());
    assert_eq!(op.retained_pending_rows(), 2);
    assert_eq!(
        op.replay
            .iter()
            .map(|(_, batch)| batch.num_rows())
            .sum::<usize>(),
        3
    );
    let frontier = op.output_frontier(InputFrontier {
        watermark: Some(100),
        idle: true,
    });
    assert_eq!(frontier.watermark, Some(42));
    assert!(!frontier.idle);
    assert!(!op.wants_input());
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
async fn run_until_output(op: &mut LookupEnrichOperator, input: RecordBatch) -> Vec<RecordBatch> {
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
    let out = run_until_output(&mut inner, stream_batch(&[100, 101], &[Some(1), Some(9)])).await;
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
            lookup_cache: Arc::new(LookupMemoryCache::with_defaults(0)),
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
