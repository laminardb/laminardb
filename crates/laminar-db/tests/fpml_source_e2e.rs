//! End-to-end: official FpML 5.13 confirmation examples across asset classes -> files source
//! `FORMAT FPML` -> a keyed aggregation, an event-time window, struct and list access on a
//! class profile, and a files sink, across a checkpointed restart.
#![cfg(feature = "fpml")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_db::subscription::{PortalFrame, SubscribeStart};
use laminar_db::{ExecuteResult, LaminarDB};

const CORPUS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../laminar-connectors/tests/fixtures/fpml/fpml-5-13-8-rec-2/confirmation"
);
const CONVERGE: Duration = Duration::from_secs(30);
const DAY_NANOS: i64 = 86_400_000_000_000;

/// `(corpus path, product type, epoch day of the message creation instant)`, in event-time
/// order. Each document holds one trade.
const BEFORE_RESTART: &[(&str, &str, i64)] = &[
    (
        "products/equity-options/eqd-ex01-american-call-stock-long-form.xml",
        "equityOption",
        965_088_000,
    ),
    (
        "products/correlation-swaps/eqcs-ex01-correlation-swap.xml",
        "correlationSwap",
        965_088_000,
    ),
    (
        "business-processes/confirmation/msg-ex17-two-sided-swap-roles-accounts.xml",
        "swap",
        1_143_936_000,
    ),
    (
        "products/credit-derivatives/cd-ex19-cdx-index-option-pred-clearing.xml",
        "creditDefaultSwapOption",
        1_167_955_200,
    ),
    (
        "products/fx-derivatives/fx-ex08-fx-swap.xml",
        "fxSwap",
        1_167_955_200,
    ),
];
const AFTER_RESTART: &[(&str, &str, i64)] = &[
    (
        "products/bond-options/bond-option.xml",
        "bondOption",
        1_167_955_200,
    ),
    (
        "business-processes/execution-advice/msg-ex63-execution-advice-trade-initiation.xml",
        "swap",
        1_185_494_400,
    ),
];

fn sql_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn statements(input: &Path, out: &Path) -> Vec<String> {
    let source = |name: &str, profile: &str, watermark: &str| {
        format!(
            "CREATE SOURCE {name} {watermark} FROM FILES (
                'path' = '{input}',
                'fpml.profile' = '{profile}',
                'poll_interval' = '50ms',
                'stabilisation_delay' = '0s'
             ) FORMAT FPML",
            input = sql_path(input),
        )
    };
    vec![
        source(
            "trades",
            "fpml-5-13-confirmation-trade",
            "(WATERMARK FOR creation_time_utc AS creation_time_utc - INTERVAL '1' SECOND)",
        ),
        source("rates", "fpml-5-13-confirmation-rates", ""),
        "CREATE MATERIALIZED VIEW by_product AS \
         SELECT product_type, COUNT(*) AS trades FROM trades GROUP BY product_type"
            .to_string(),
        "CREATE MATERIALIZED VIEW per_day AS \
         SELECT product_type, tumble(creation_time_utc, INTERVAL '1' DAY) AS window_start, \
                COUNT(*) AS trades \
         FROM trades GROUP BY product_type, tumble(creation_time_utc, INTERVAL '1' DAY)"
            .to_string(),
        "CREATE STREAM swap_legs AS \
         SELECT document_sha256, trade_header['trade_date'] AS trade_date, \
                swap['streams'][1]['payer_party_href'] AS first_payer, \
                swap['streams'][2]['payer_party_href'] AS second_payer \
         FROM rates WHERE product_type = 'swap'"
            .to_string(),
        format!(
            "CREATE SINK legs_out FROM swap_legs INTO FILES ('path' = '{}') FORMAT JSON",
            sql_path(out)
        ),
    ]
}

fn trace() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

async fn open(storage: &Path) -> Arc<LaminarDB> {
    trace();
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .build()
        .await
        .expect("open database")
}

/// Open and register `statements`, waiting for the previous generation's locks.
async fn reopen(storage: &Path, statements: &[String]) -> Arc<LaminarDB> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let db = open(storage).await;
        let mut failure = None;
        for statement in statements {
            if let Err(error) = db.execute(statement).await {
                failure = Some(format!("{statement}: {error}"));
                break;
            }
        }
        match failure {
            None => return db,
            Some(error) if error.contains("LDB-0014") && tokio::time::Instant::now() < deadline => {
                drop(db);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Some(error) => panic!("{error}"),
        }
    }
}

async fn checkpoint(db: &LaminarDB) {
    let result = db.checkpoint().await.unwrap_or_else(|error| {
        panic!(
            "checkpoint: {error}; pipeline {} fault {:?}",
            db.pipeline_state(),
            db.last_fault()
        )
    });
    assert!(result.success, "checkpoint failed: {result:?}");
}

async fn by_product(db: &LaminarDB) -> BTreeMap<String, i64> {
    counts(db, "by_product").await
}

/// A two-column (key, count) view as a map.
async fn counts(db: &LaminarDB, view: &str) -> BTreeMap<String, i64> {
    let ExecuteResult::Query(mut query) = db
        .execute(&format!("SELECT * FROM {view}"))
        .await
        .expect("query")
    else {
        panic!("expected a query result");
    };
    let mut subscription = query.subscribe_raw().expect("subscribe");
    let mut rows = BTreeMap::new();
    // An empty view answers with no batch and closes the result stream.
    let Ok(first) = tokio::time::timeout(CONVERGE, subscription.recv_async())
        .await
        .expect("snapshot in time")
    else {
        return rows;
    };
    let mut batches = vec![first];
    while let Some(batch) = subscription.poll() {
        batches.push(batch);
    }
    for batch in batches {
        let products = strings(&batch, 0);
        let trades = int64s(&batch, 1);
        for (product, count) in products.into_iter().zip(trades) {
            rows.insert(product, count);
        }
    }
    rows
}

fn strings(batch: &RecordBatch, column: usize) -> Vec<String> {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 column");
    (0..array.len())
        .map(|row| array.value(row).to_string())
        .collect()
}

fn int64s(batch: &RecordBatch, column: usize) -> Vec<i64> {
    let cast = arrow::compute::cast(batch.column(column), &arrow::datatypes::DataType::Int64)
        .expect("int64 column");
    let array = cast.as_any().downcast_ref::<Int64Array>().expect("int64");
    (0..array.len()).map(|row| array.value(row)).collect()
}

/// Window starts as epoch nanoseconds, whatever unit the view emits them in.
fn nanos(batch: &RecordBatch, column: usize) -> Vec<i64> {
    let unit = arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None);
    let rescaled = arrow::compute::cast(batch.column(column), &unit).expect("timestamp column");
    let cast = arrow::compute::cast(&rescaled, &arrow::datatypes::DataType::Int64).expect("int64");
    let array = cast.as_any().downcast_ref::<Int64Array>().expect("int64");
    (0..array.len()).map(|row| array.value(row)).collect()
}

fn expected_counts(documents: &[&[(&str, &str, i64)]]) -> BTreeMap<String, i64> {
    let mut counts = BTreeMap::new();
    for (_, product, _) in documents.iter().flat_map(|batch| batch.iter()) {
        *counts.entry((*product).to_string()).or_insert(0) += 1;
    }
    counts
}

async fn wait_by_product(db: &LaminarDB, expected: &BTreeMap<String, i64>) {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let observed = by_product(db).await;
        if &observed == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "by_product {observed:?} never reached {expected:?}; fault {:?}",
            db.last_fault()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Deliver documents one at a time so the watermark advances in event-time order.
async fn deliver(
    db: &LaminarDB,
    input: &Path,
    documents: &[(&str, &str, i64)],
    delivered: &mut Vec<&'static [(&'static str, &'static str, i64)]>,
    all: &'static [(&'static str, &'static str, i64)],
) {
    for (index, (path, _, _)) in documents.iter().enumerate() {
        let bytes = std::fs::read(Path::new(CORPUS).join(path)).expect("corpus document");
        let name = format!(
            "{:02}.xml",
            delivered.iter().map(|batch| batch.len()).sum::<usize>() + index
        );
        std::fs::write(input.join(name), bytes).expect("write document");
        let mut so_far = delivered.clone();
        so_far.push(&all[..=index]);
        wait_by_product(db, &expected_counts(&so_far)).await;
    }
    delivered.push(all);
}

fn sink_rows(out: &Path) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    for entry in std::fs::read_dir(out)
        .expect("sink directory")
        .filter_map(Result::ok)
    {
        let path: PathBuf = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
            continue;
        }
        for line in std::fs::read_to_string(&path).expect("sink file").lines() {
            rows.push(serde_json::from_str(line).expect("json row"));
        }
    }
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn fpml_documents_feed_sql_aggregates_windows_and_a_sink_across_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let storage = dir.path().join("storage");
    let input = dir.path().join("input");
    let out = dir.path().join("out");
    std::fs::create_dir_all(&input).expect("input");
    std::fs::create_dir_all(&out).expect("out");
    let ddl = statements(&input, &out);

    let db = reopen(&storage, &ddl).await;
    let mut windows = db
        .open_subscription("per_day", None, SubscribeStart::Tail)
        .await
        .expect("window subscription");
    db.start().await.expect("start");
    let mut delivered = Vec::new();
    deliver(&db, &input, BEFORE_RESTART, &mut delivered, BEFORE_RESTART).await;

    // Windows a later event has closed: both 2000-08-01 trades and the 2006-04-02 swap.
    let expected: BTreeMap<(String, i64), i64> = BEFORE_RESTART[..3]
        .iter()
        .map(|(_, product, day)| (((*product).to_string(), day * 1_000_000_000), 1))
        .collect();
    let mut seen: BTreeMap<(String, i64), i64> = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + CONVERGE;
    while expected
        .iter()
        .any(|(key, count)| seen.get(key) != Some(count))
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "windows {seen:?} never reached {expected:?}"
        );
        if let Ok(Some(PortalFrame::Batch { batch, .. })) =
            tokio::time::timeout(Duration::from_millis(500), windows.next_frame()).await
        {
            let products = strings(&batch, 0);
            let starts = nanos(&batch, 1);
            let counts = int64s(&batch, 2);
            for ((product, start), count) in products.into_iter().zip(starts).zip(counts) {
                assert_eq!(
                    start % DAY_NANOS,
                    0,
                    "window start {start} is not a UTC day"
                );
                seen.insert((product, start), count);
            }
        }
    }

    checkpoint(&db).await;
    db.shutdown().await.expect("shutdown");
    drop(windows);
    drop(db);

    let db = reopen(&storage, &ddl).await;
    db.start().await.expect("restart");
    deliver(&db, &input, AFTER_RESTART, &mut delivered, AFTER_RESTART).await;
    wait_by_product(&db, &expected_counts(&[BEFORE_RESTART, AFTER_RESTART])).await;
    checkpoint(&db).await;
    db.shutdown().await.expect("shutdown");
    drop(db);

    let legs = sink_rows(&out);
    let mut payers: Vec<(String, String, String)> = legs
        .iter()
        .map(|row| {
            (
                row["trade_date"].as_str().expect("trade_date").to_string(),
                row["first_payer"]
                    .as_str()
                    .expect("first_payer")
                    .to_string(),
                row["second_payer"]
                    .as_str()
                    .expect("second_payer")
                    .to_string(),
            )
        })
        .collect();
    payers.sort();
    payers.dedup();
    assert_eq!(
        payers.len(),
        2,
        "both swaps reach the sink once each: {legs:?}"
    );
}

/// Kafka carries one FpML document per message; the default document profile yields one row each.
#[cfg(feature = "kafka")]
#[tokio::test(flavor = "multi_thread")]
async fn fpml_kafka_messages_feed_sql() {
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::ClientConfig;

    let cluster = rdkafka::mocking::MockCluster::new(1).expect("mock cluster");
    cluster.create_topic("fpml", 1, 1).expect("topic");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .create()
        .expect("producer");
    let documents = [
        "business-processes/clearing/msg-ex10-clearingConfirmed-clearing-performed.xml",
        "business-processes/allocation/msg-ex20-cds-request-allocation.xml",
        "products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml",
    ];
    for path in documents {
        let bytes = std::fs::read(Path::new(CORPUS).join(path)).expect("corpus document");
        producer
            .send(
                FutureRecord::<(), [u8]>::to("fpml")
                    .partition(0)
                    .payload(&bytes),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }

    let dir = tempfile::tempdir().expect("dir");
    let ddl = [
        format!(
            "CREATE SOURCE messages (
                WATERMARK FOR creation_time_utc AS creation_time_utc - INTERVAL '1' SECOND
             ) FROM KAFKA (
                'bootstrap.servers' = '{}',
                'topic' = 'fpml',
                'group.id' = 'fpml-sql',
                'startup.mode' = 'earliest'
             ) FORMAT FPML",
            cluster.bootstrap_servers()
        ),
        "CREATE MATERIALIZED VIEW by_root AS \
         SELECT document_root, COUNT(*) AS documents FROM messages GROUP BY document_root"
            .to_string(),
    ];
    let db = reopen(&dir.path().join("storage"), &ddl).await;
    db.start().await.expect("start");
    let expected = BTreeMap::from([
        ("clearingConfirmed".to_string(), 1),
        ("dataDocument".to_string(), 1),
        ("requestAllocation".to_string(), 1),
    ]);
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let observed = counts(&db, "by_root").await;
        if observed == expected {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "by_root {observed:?} never reached {expected:?}; fault {:?}",
            db.last_fault()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    db.shutdown().await.expect("shutdown");
}
