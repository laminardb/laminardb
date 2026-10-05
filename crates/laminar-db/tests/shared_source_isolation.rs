//! Shared-source failure isolation, end to end.
//!
//! Two streams read one source: an intentionally rejected temporal expression and a healthy
//! projection. With `shared_source_isolation` on the
//! healthy sibling keeps producing; with it off the whole shared-source domain faults
//! and starves it.
//!
//! Transient-fault replay lives in the unit test
//! `test_shared_source_isolation_replays_faulted_domain`; this planner rejection is
//! persistent, so this covers only the sibling-survives half.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use laminar_db::{EngineMetrics, FromBatch, LaminarConfig, LaminarDB, TypedSubscription};

#[derive(Clone, Debug)]
struct CapturedBatch(RecordBatch);

impl FromBatch for CapturedBatch {
    fn from_batch(batch: &RecordBatch, row: usize) -> Self {
        Self(batch.slice(row, 1))
    }
    fn from_batch_all(batch: &RecordBatch) -> Vec<Self> {
        (0..batch.num_rows())
            .map(|i| Self(batch.slice(i, 1)))
            .collect()
    }
}

fn drain_rows(sub: &mut TypedSubscription<CapturedBatch>) -> usize {
    let mut rows = 0;
    while let Some(batches) = sub
        .poll()
        .expect("healthy subscription must remain contiguous")
    {
        for cb in batches {
            rows += cb.0.num_rows();
        }
    }
    rows
}

fn make_batch(symbols: &[&str], prices: &[f64], ts_ms: &[i64]) -> RecordBatch {
    let us: Vec<i64> = ts_ms.iter().map(|ms| ms * 1000).collect();
    RecordBatch::try_from_iter_with_nullable(vec![
        (
            "symbol",
            Arc::new(StringArray::from(symbols.to_vec())) as _,
            true,
        ),
        (
            "price",
            Arc::new(Float64Array::from(prices.to_vec())) as _,
            true,
        ),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(us)) as _,
            true,
        ),
    ])
    .unwrap()
}

/// Rows the healthy projection emits while its aggregation sibling faults every cycle.
async fn healthy_rows_with_isolation(isolation: bool) -> usize {
    let dir = tempfile::tempdir().unwrap();
    let config = LaminarConfig {
        storage_dir: Some(dir.path().to_path_buf()),
        shared_source_isolation: isolation,
        // Execute both siblings in one cycle. A time budget could defer the failing sibling
        // and publish healthy rows before the shared domain faults.
        pipeline_query_budget_ns: Some(30_000_000_000),
        ..LaminarConfig::default()
    };

    let db = LaminarDB::open_with_config(config).unwrap();
    let metrics = Arc::new(EngineMetrics::new(&prometheus::Registry::new()));
    db.set_engine_metrics(Arc::clone(&metrics));
    db.execute(
        "CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
    )
    .await
    .unwrap();
    // Healthy: stateless projection sharing `trades`.
    db.execute("CREATE STREAM healthy AS SELECT symbol, price FROM trades")
        .await
        .unwrap();
    // Faulting: non-windowed now() is rejected by the runtime operator unless it is the supported
    // EMIT CHANGES temporal-filter shape.
    db.execute("CREATE STREAM broken AS SELECT symbol, price FROM trades WHERE ts > now()")
        .await
        .unwrap();
    db.start().await.unwrap();

    let mut healthy = db.subscribe::<CapturedBatch>("healthy").await.unwrap();

    let source = db.source_untyped("trades").unwrap();
    // One batch keeps fault replay from interleaving with admission of later test rows.
    source
        .push_arrow(make_batch(&["AAPL"; 20], &[100.0; 20], &[0; 20]))
        .unwrap();

    let processed = tokio::time::timeout(Duration::from_secs(5), async {
        while metrics.pipeline_cycle_errors_total.get() == 0 || metrics.events_ingested.get() < 20 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        processed.is_ok(),
        "both siblings must process the input and observe the fault: errors={}, ingested={}, state={}",
        metrics.pipeline_cycle_errors_total.get(),
        metrics.events_ingested.get(),
        db.pipeline_state(),
    );
    let rows = drain_rows(&mut healthy);
    db.shutdown().await.unwrap();
    rows
}

#[tokio::test]
async fn test_shared_source_isolation_keeps_sibling_alive() {
    let rows = healthy_rows_with_isolation(true).await;
    assert_eq!(
        rows, 20,
        "with isolation on, the healthy projection keeps producing while its \
         shared-source aggregation sibling faults every cycle (got {rows} rows)"
    );
}

#[tokio::test]
async fn test_shared_source_no_isolation_starves_sibling() {
    let rows = healthy_rows_with_isolation(false).await;
    assert_eq!(
        rows, 0,
        "without isolation the shared-source domain faults as a whole, so the \
         healthy projection is starved too (got {rows} rows)"
    );
}
