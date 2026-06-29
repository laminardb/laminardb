//! Shared-source failure isolation, end to end.
//!
//! Two streams read one source. The aggregating one is forced to fault every cycle
//! (a 1-byte operator state limit), the projection one is healthy. With
//! `shared_source_isolation` on, the healthy sibling keeps producing; with it off,
//! the shared-source domain faults as a whole and starves the healthy sibling.
//!
//! Online replay of a *transient* fault is covered by the operator-graph unit test
//! `test_shared_source_isolation_replays_faulted_domain`; a state-limit fault is
//! persistent by design, so this test validates the isolation (sibling-survives) half.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use laminar_db::{FromBatch, LaminarConfig, LaminarDB, TypedSubscription};

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
    while let Some(batches) = sub.poll() {
        for cb in batches {
            rows += cb.0.num_rows();
        }
    }
    rows
}

fn make_batch(symbols: &[&str], prices: &[f64], ts_ms: &[i64]) -> RecordBatch {
    let us: Vec<i64> = ts_ms.iter().map(|ms| ms * 1000).collect();
    RecordBatch::try_from_iter(vec![
        ("symbol", Arc::new(StringArray::from(symbols.to_vec())) as _),
        ("price", Arc::new(Float64Array::from(prices.to_vec())) as _),
        ("ts", Arc::new(TimestampMicrosecondArray::from(us)) as _),
    ])
    .unwrap()
}

/// Run the shared-source scenario and return how many rows the *healthy*
/// projection stream emitted while the aggregation sibling faults every cycle.
async fn healthy_rows_with_isolation(isolation: bool) -> usize {
    let dir = tempfile::tempdir().unwrap();
    let config = LaminarConfig {
        storage_dir: Some(dir.path().to_path_buf()),
        // 1 byte: any aggregate state trips the limit and faults every cycle; a
        // stateless projection stays under it.
        max_state_bytes_per_operator: Some(1),
        shared_source_isolation: isolation,
        ..LaminarConfig::default()
    };

    let db = LaminarDB::open_with_config(config).unwrap();
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
    // Faulting: aggregate state trips the 1-byte limit every cycle.
    db.execute(
        "CREATE STREAM aggy AS SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol",
    )
    .await
    .unwrap();
    db.start().await.unwrap();

    let mut healthy = db.subscribe::<CapturedBatch>("healthy").unwrap();

    let source = db.source_untyped("trades").unwrap();
    for i in 0..20 {
        source
            .push_arrow(make_batch(
                &["AAPL"],
                &[100.0 + f64::from(i)],
                &[i64::from(i) * 1000],
            ))
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(400)).await;
    let rows = drain_rows(&mut healthy);
    db.shutdown().await.unwrap();
    rows
}

#[tokio::test]
async fn test_shared_source_isolation_keeps_sibling_alive() {
    let rows = healthy_rows_with_isolation(true).await;
    assert!(
        rows > 0,
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
