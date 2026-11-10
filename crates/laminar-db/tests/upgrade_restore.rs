#![allow(clippy::disallowed_types)]
//! A checkpoint written before a dependency upgrade must restore after it: the fixture was written
//! by 0.34.0 (Arrow 58.4.1, DataFusion 53.1.0) and holds keyed-aggregate, open-window,
//! open-session, interval-join and append-view state for the pipeline below.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::util::display::array_value_to_string;
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{ExecuteResult, LaminarConfig, LaminarDB};

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/upgrade/checkpoint_0_34_0"
);
const DDL: &[&str] = &[
    "CREATE SOURCE trades (symbol VARCHAR NOT NULL, price BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
     WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)",
    "CREATE SOURCE quotes (symbol VARCHAR NOT NULL, bid BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
     WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)",
    "CREATE MATERIALIZED VIEW totals AS SELECT symbol, SUM(price) AS total, COUNT(*) AS n, \
     MIN(price) AS lo, MAX(price) AS hi FROM trades GROUP BY symbol",
    "CREATE MATERIALIZED VIEW recent AS SELECT symbol, price FROM trades \
     WITH (retention = '100 rows')",
    "CREATE MATERIALIZED VIEW per_minute AS SELECT symbol, COUNT(*) AS n, SUM(price) AS total, \
     TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket FROM trades \
     GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE) EMIT ON WINDOW CLOSE",
    "CREATE MATERIALIZED VIEW sessions AS SELECT symbol, COUNT(*) AS n, \
     SESSION(ts, INTERVAL '5' SECOND) AS s FROM trades \
     GROUP BY symbol, SESSION(ts, INTERVAL '5' SECOND) EMIT ON WINDOW CLOSE",
    "CREATE MATERIALIZED VIEW matched AS SELECT t.symbol AS symbol, t.price AS price, q.bid AS bid FROM trades t \
     JOIN quotes q ON t.symbol = q.symbol AND q.ts BETWEEN t.ts AND t.ts + INTERVAL '1' MINUTE \
     WITH (retention = '100 rows')",
];

fn config(dir: &Path) -> LaminarConfig {
    LaminarConfig {
        storage_dir: Some(dir.to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: None,
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    }
}

fn trades(rows: &[(&str, i64, i64)]) -> RecordBatch {
    batch("price", rows)
}

fn quotes(rows: &[(&str, i64, i64)]) -> RecordBatch {
    batch("bid", rows)
}

fn batch(value_column: &str, rows: &[(&str, i64, i64)]) -> RecordBatch {
    let symbols: Vec<&str> = rows.iter().map(|row| row.0).collect();
    let values: Vec<i64> = rows.iter().map(|row| row.1).collect();
    let micros: Vec<i64> = rows.iter().map(|row| row.2 * 1_000).collect();
    RecordBatch::try_from_iter_with_nullable(vec![
        ("symbol", Arc::new(StringArray::from(symbols)) as _, false),
        (value_column, Arc::new(Int64Array::from(values)) as _, false),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(micros)) as _,
            false,
        ),
    ])
    .unwrap()
}

async fn pipeline(dir: &Path) -> Arc<LaminarDB> {
    let db = LaminarDB::open_with_config(config(dir)).unwrap();
    for statement in DDL {
        db.execute(statement).await.unwrap();
    }
    db.start().await.unwrap();
    db
}

async fn rows(db: &LaminarDB, sql: &str) -> Vec<Vec<String>> {
    let ExecuteResult::Query(mut query) = db.execute(sql).await.unwrap() else {
        panic!("expected a query result");
    };
    let batches = query.subscribe_raw().unwrap().read_all().await.unwrap();
    let mut rows: Vec<Vec<String>> = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column, row).unwrap())
                    .collect()
            })
        })
        .collect();
    rows.sort();
    rows
}

fn expected(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(ToString::to_string).collect())
        .collect()
}

async fn wait_for<F, Fut>(description: &str, want: Vec<Vec<String>>, read: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Vec<Vec<String>>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let got = read().await;
        if got == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{description}: expected {want:?}, got {got:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

const TOTALS: &str = "SELECT symbol, total, n, lo, hi FROM totals";

#[tokio::test]
async fn checkpoint_written_before_the_upgrade_restores_and_continues() {
    let temp = tempfile::tempdir().unwrap();
    copy_tree(Path::new(FIXTURE), temp.path());
    let db = pipeline(temp.path()).await;

    wait_for(
        "restored aggregate",
        expected(&[
            &["AAPL", "30", "2", "10", "20"],
            &["MSFT", "5", "1", "5", "5"],
        ]),
        || rows(&db, TOTALS),
    )
    .await;
    assert_eq!(
        rows(&db, "SELECT symbol, price FROM recent").await,
        expected(&[&["AAPL", "10"], &["AAPL", "20"], &["MSFT", "5"]]),
        "restored append view"
    );

    db.source_untyped("trades")
        .unwrap()
        .push_arrow(trades(&[("AAPL", 7, 4_000), ("ZZZ", 1, 120_000)]))
        .unwrap();
    db.source_untyped("quotes")
        .unwrap()
        .push_arrow(quotes(&[("AAPL", 99, 2_500)]))
        .unwrap();

    wait_for(
        "aggregate continues from restored accumulators",
        expected(&[
            &["AAPL", "37", "3", "7", "20"],
            &["MSFT", "5", "1", "5", "5"],
            &["ZZZ", "1", "1", "1", "1"],
        ]),
        || rows(&db, TOTALS),
    )
    .await;
    wait_for(
        "open window restored and closed",
        expected(&[&["AAPL", "3", "37"], &["MSFT", "1", "5"]]),
        || rows(&db, "SELECT symbol, n, total FROM per_minute"),
    )
    .await;
    wait_for(
        "open session restored and closed",
        expected(&[&["AAPL", "3"], &["MSFT", "1"]]),
        || rows(&db, "SELECT symbol, n FROM sessions"),
    )
    .await;
    wait_for(
        "interval join matches a restored row",
        expected(&[&["AAPL", "10", "99"], &["AAPL", "20", "99"]]),
        || rows(&db, "SELECT symbol, price, bid FROM matched"),
    )
    .await;
    db.shutdown().await.unwrap();
}
