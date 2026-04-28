//! Differential harness for N-way joins.
//!
//! Oracle: DataFusion batch execution over `MemTable`s. SUT: LaminarDB
//! driving the same SQL through `CREATE STREAM`. Equality: multiset over
//! Arrow row encoding.

#![allow(clippy::disallowed_types)] // tests use std HashMap freely

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use laminar_db::{FromBatch, LaminarDB};
use proptest::prelude::*;
use proptest::test_runner::{Config as PropConfig, TestRng, TestRunner};

// ---------------------------------------------------------------------------
// Drain helpers — wait for sources to clear and the coordinator to advance.
// ---------------------------------------------------------------------------

/// Wait until every source has drained and the coordinator has advanced a
/// couple of cycles past that point — enough for an IntervalJoinOperator
/// (cycle N) to feed its post-projection sink (N+1) and reach the
/// subscription's output queue (N+2).
async fn await_quiescence(
    db: &LaminarDB,
    sources: &[&laminar_db::UntypedSourceHandle],
) {
    const POLL: Duration = Duration::from_millis(20);
    const STAGE_BUDGET: Duration = Duration::from_secs(2);

    let deadline = std::time::Instant::now() + STAGE_BUDGET;
    while std::time::Instant::now() < deadline {
        if sources.iter().all(|h| h.pending() == 0) {
            break;
        }
        tokio::time::sleep(POLL).await;
    }

    let baseline = db.metrics().total_cycles;
    let deadline = std::time::Instant::now() + STAGE_BUDGET;
    while std::time::Instant::now() < deadline
        && db.metrics().total_cycles < baseline + 2
    {
        tokio::time::sleep(POLL).await;
    }
}

fn drain_subscription(sub: &mut laminar_db::TypedSubscription<CapturedBatch>) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    while let Some(batches) = sub.poll() {
        for cb in batches {
            if cb.0.num_rows() > 0 {
                out.push(cb.0);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Subscription wrapper — we want raw batches, not deserialised rows.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct CapturedBatch(RecordBatch);

impl FromBatch for CapturedBatch {
    fn from_batch(batch: &RecordBatch, row: usize) -> Self {
        Self(batch.slice(row, 1))
    }
    // Return one entry per row so `from_batch` is consistent with
    // `from_batch_all`. Callers in this harness only use the latter.
    fn from_batch_all(batch: &RecordBatch) -> Vec<Self> {
        (0..batch.num_rows())
            .map(|i| Self(batch.slice(i, 1)))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Schemas — three sources for both chain and star shapes.
// ---------------------------------------------------------------------------

fn schema_c() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("kc", DataType::Int64, false),
        Field::new("ka", DataType::Int64, false),
        Field::new("xc", DataType::Utf8, false),
    ]))
}

fn make_batch_c(rows: &[(i64, i64, &str)]) -> RecordBatch {
    let kc: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let ka: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let xc: Vec<&str> = rows.iter().map(|r| r.2).collect();
    RecordBatch::try_new(
        schema_c(),
        vec![
            Arc::new(Int64Array::from(kc)),
            Arc::new(Int64Array::from(ka)),
            Arc::new(StringArray::from(xc)),
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Multiset equality over a vector of batches.
// ---------------------------------------------------------------------------

fn batches_to_sorted_rows(
    batches: &[RecordBatch],
    schema: &SchemaRef,
) -> Result<Vec<Vec<u8>>, arrow::error::ArrowError> {
    let fields: Vec<SortField> = schema
        .fields()
        .iter()
        .map(|f| SortField::new(f.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields)?;

    let mut all_rows: Vec<Vec<u8>> = Vec::new();
    for b in batches {
        if b.num_rows() == 0 {
            continue;
        }
        // Re-project to the canonical column order if the SUT batch ordered
        // columns differently. (If schemas already match, this is a no-op.)
        let projected = if b.schema().fields() == schema.fields() {
            b.clone()
        } else {
            let indices: Result<Vec<usize>, _> = schema
                .fields()
                .iter()
                .map(|f| {
                    b.schema().index_of(f.name()).map_err(|_| {
                        arrow::error::ArrowError::SchemaError(format!(
                            "missing column '{}' in SUT output",
                            f.name()
                        ))
                    })
                })
                .collect();
            b.project(&indices?)?
        };
        let cols = projected.columns().to_vec();
        let rows = converter.convert_columns(&cols)?;
        for i in 0..rows.num_rows() {
            all_rows.push(rows.row(i).as_ref().to_vec());
        }
    }
    all_rows.sort();
    Ok(all_rows)
}

/// Format a multiset as a histogram for failure messages.
fn histogram(rows: &[Vec<u8>]) -> BTreeMap<Vec<u8>, usize> {
    let mut h = BTreeMap::new();
    for r in rows {
        *h.entry(r.clone()).or_insert(0) += 1;
    }
    h
}

fn diff_counts(
    oracle: &[Vec<u8>],
    sut: &[Vec<u8>],
) -> (Vec<(Vec<u8>, isize)>, usize, usize) {
    let h_o = histogram(oracle);
    let h_s = histogram(sut);
    let mut diffs: Vec<(Vec<u8>, isize)> = Vec::new();
    for (k, vo) in &h_o {
        let vs = h_s.get(k).copied().unwrap_or(0);
        if *vo != vs {
            diffs.push((k.clone(), *vo as isize - vs as isize));
        }
    }
    for (k, vs) in &h_s {
        if !h_o.contains_key(k) {
            diffs.push((k.clone(), -(*vs as isize)));
        }
    }
    (diffs, oracle.len(), sut.len())
}

// ---------------------------------------------------------------------------
// Output schema for the BETWEEN positive tests.
// ---------------------------------------------------------------------------

fn output_schema_chain_star() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ka", DataType::Int64, false),
        Field::new("xa", DataType::Utf8, false),
        Field::new("xb", DataType::Utf8, false),
        Field::new("xc", DataType::Utf8, false),
    ]))
}

// ---------------------------------------------------------------------------
// Rejection assertions — H1 forbids unbounded multi-way stream-stream joins.
// Equi-equi chain, equi-equi star, and the cross-cycle shape all fall under
// this rule; CREATE STREAM should fail at plan time with a clear message.
// ---------------------------------------------------------------------------

async fn expect_planning_rejection(create_stream_sql: &str) -> String {
    let db = LaminarDB::open().expect("open");
    db.execute("CREATE SOURCE a (ka BIGINT, kb BIGINT, xa VARCHAR)")
        .await
        .expect("ddl a");
    db.execute("CREATE SOURCE b (kb BIGINT, kc BIGINT, xb VARCHAR)")
        .await
        .expect("ddl b");
    db.execute("CREATE SOURCE c (kc BIGINT, ka BIGINT, xc VARCHAR)")
        .await
        .expect("ddl c");

    let err = db.execute(create_stream_sql).await.expect_err(
        "expected planning rejection for unbounded multi-way streaming join",
    );
    format!("{err}")
}

#[tokio::test]
async fn rejects_unbounded_chain() {
    let msg = expect_planning_rejection(
        "CREATE STREAM out AS SELECT a.ka, a.xa, b.xb, c.xc FROM a \
         JOIN b ON a.kb = b.kb JOIN c ON b.kc = c.kc",
    )
    .await;
    assert!(msg.contains("unbounded join"), "got: {msg}");
    assert!(msg.contains("lookup table"), "got: {msg}");
}

#[tokio::test]
async fn rejects_unbounded_star() {
    let msg = expect_planning_rejection(
        "CREATE STREAM out AS SELECT a.ka, a.xa, b.xb, c.xc FROM a \
         JOIN b ON a.kb = b.kb JOIN c ON a.ka = c.ka",
    )
    .await;
    assert!(msg.contains("unbounded join"), "got: {msg}");
}

#[tokio::test]
async fn rejects_unbounded_two_way() {
    let msg = expect_planning_rejection(
        "CREATE STREAM out AS SELECT a.ka, b.xb FROM a JOIN b ON a.kb = b.kb",
    )
    .await;
    assert!(msg.contains("unbounded join"), "got: {msg}");
}

// ===========================================================================
// v2 #1 — BETWEEN-bounded chain. Surfaces audit C1/C2:
// `detect_stream_join_query` matches the first step (a↔b), so
// `IntervalJoinOperator` is built for those two only — c is dropped from the
// operator graph. SUT either errors at projection compile or silently drops
// rows joining through c.
// ===========================================================================

const BETWEEN_CHAIN_SUT_SQL: &str = "SELECT a.ka, a.xa, b.xb, c.xc FROM a \
    JOIN b ON a.kb = b.kb AND b.ts BETWEEN a.ts AND a.ts + INTERVAL '5' SECOND \
    JOIN c ON b.kc = c.kc";

// Oracle: SUT's INTERVAL '5' SECOND lowers to 5000ms in the operator's
// time_bound. Express the same predicate against BIGINT ms columns so
// DataFusion can plan it without TIMESTAMP arithmetic.
const BETWEEN_CHAIN_ORACLE_SQL: &str = "SELECT a.ka, a.xa, b.xb, c.xc FROM a \
    JOIN b ON a.kb = b.kb AND b.ts BETWEEN a.ts AND a.ts + 5000 \
    JOIN c ON b.kc = c.kc";

fn schema_a_ts() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ka", DataType::Int64, false),
        Field::new("kb", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("xa", DataType::Utf8, false),
    ]))
}

fn schema_b_ts() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("kb", DataType::Int64, false),
        Field::new("kc", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("xb", DataType::Utf8, false),
    ]))
}

fn make_a_ts(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    let ka: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let kb: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let xa: Vec<&str> = rows.iter().map(|r| r.3).collect();
    RecordBatch::try_new(
        schema_a_ts(),
        vec![
            Arc::new(Int64Array::from(ka)),
            Arc::new(Int64Array::from(kb)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(xa)),
        ],
    )
    .unwrap()
}

fn make_b_ts(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    let kb: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let kc: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let xb: Vec<&str> = rows.iter().map(|r| r.3).collect();
    RecordBatch::try_new(
        schema_b_ts(),
        vec![
            Arc::new(Int64Array::from(kb)),
            Arc::new(Int64Array::from(kc)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(xb)),
        ],
    )
    .unwrap()
}

async fn run_oracle_ts(
    sql: &str,
    a: Vec<RecordBatch>,
    b: Vec<RecordBatch>,
    c: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, datafusion::error::DataFusionError> {
    let ctx = SessionContext::new();
    ctx.register_table("a", Arc::new(MemTable::try_new(schema_a_ts(), vec![a])?))?;
    ctx.register_table("b", Arc::new(MemTable::try_new(schema_b_ts(), vec![b])?))?;
    ctx.register_table("c", Arc::new(MemTable::try_new(schema_c(), vec![c])?))?;
    let df = ctx.sql(sql).await?;
    df.collect().await
}

async fn run_sut_ts(
    sql: &str,
    a_batches: Vec<RecordBatch>,
    b_batches: Vec<RecordBatch>,
    c_rows: &[(i64, i64, &str)],
) -> Result<Vec<RecordBatch>, String> {
    let db = LaminarDB::open().map_err(|e| format!("open: {e}"))?;

    db.execute(
        "CREATE SOURCE a (ka BIGINT, kb BIGINT, ts BIGINT, xa VARCHAR, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .map_err(|e| format!("ddl a: {e}"))?;
    db.execute(
        "CREATE SOURCE b (kb BIGINT, kc BIGINT, ts BIGINT, xb VARCHAR, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .map_err(|e| format!("ddl b: {e}"))?;
    // c is a lookup table (the natural production shape for an enrichment
    // join on the residual side of an interval-bounded chain).
    db.execute(
        "CREATE LOOKUP TABLE c (kc BIGINT NOT NULL, ka BIGINT, xc VARCHAR, \
         PRIMARY KEY (kc)) WITH ('connector' = 'static')",
    )
    .await
    .map_err(|e| format!("ddl c: {e}"))?;

    // Seed the lookup table. Single multi-row INSERT keeps the planner
    // round-trip count down for proptest cases. String-formatted values
    // are safe here because `xc` comes from the proptest alphabet
    // `[a-z]{1,3}` — do NOT copy this idiom into production seeding code.
    if !c_rows.is_empty() {
        let values: Vec<String> = c_rows
            .iter()
            .map(|(kc, ka, xc)| format!("({kc}, {ka}, '{xc}')"))
            .collect();
        let insert_sql = format!("INSERT INTO c VALUES {}", values.join(", "));
        db.execute(&insert_sql)
            .await
            .map_err(|e| format!("insert c: {e}"))?;
    }

    let stream_sql = format!("CREATE STREAM out AS {sql}");
    db.execute(&stream_sql)
        .await
        .map_err(|e| format!("ddl stream: {e}"))?;

    db.start().await.map_err(|e| format!("start: {e}"))?;

    let mut sub = db
        .subscribe::<CapturedBatch>("out")
        .map_err(|e| format!("subscribe: {e}"))?;

    let h_a = db.source_untyped("a").map_err(|e| format!("h_a: {e}"))?;
    let h_b = db.source_untyped("b").map_err(|e| format!("h_b: {e}"))?;

    for batch in a_batches {
        h_a.push_arrow(batch)
            .map_err(|e| format!("push a: {e}"))?;
    }
    for batch in b_batches {
        h_b.push_arrow(batch)
            .map_err(|e| format!("push b: {e}"))?;
    }

    await_quiescence(&db, &[&h_a, &h_b]).await;
    let collected = drain_subscription(&mut sub);

    db.shutdown().await.map_err(|e| format!("shutdown: {e}"))?;
    Ok(collected)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn smoke_n3_between_chain() {
    // a.ts and b.ts within 5s; b.kc = c.kc to chain.
    let a = [(1_i64, 10_i64, 1_000_i64, "a1"), (2, 20, 2_000, "a2")];
    let b = [
        (10_i64, 100_i64, 1_500_i64, "b1"), // matches a1: 1500-1000=500 <= 5000
        (20, 200, 2_300, "b2"),             // matches a2: 2300-2000=300 <= 5000
    ];
    let c = [(100_i64, 1_i64, "c1"), (200, 2, "c2")];

    let oracle = run_oracle_ts(
        BETWEEN_CHAIN_ORACLE_SQL,
        vec![make_a_ts(&a)],
        vec![make_b_ts(&b)],
        vec![make_batch_c(&c)],
    )
    .await
    .expect("oracle exec");

    let sut_result = run_sut_ts(
        BETWEEN_CHAIN_SUT_SQL,
        vec![make_a_ts(&a)],
        vec![make_b_ts(&b)],
        &c,
    )
    .await;

    let schema = output_schema_chain_star();
    let oracle_rows = batches_to_sorted_rows(&oracle, &schema).expect("oracle rows");

    match sut_result {
        Err(e) => {
            // Capture the SUT failure as the counter-example.
            panic!(
                "[BETWEEN CHAIN N=3 SMOKE] oracle={} rows; SUT failed: {e}",
                oracle_rows.len()
            );
        }
        Ok(sut) => {
            let sut_rows = batches_to_sorted_rows(&sut, &schema).expect("sut rows");
            if oracle_rows != sut_rows {
                let (diffs, no, ns) = diff_counts(&oracle_rows, &sut_rows);
                panic!(
                    "[BETWEEN CHAIN N=3 SMOKE] oracle={no} sut={ns} diffs={} \
                     (positive = oracle has more, negative = sut has more)",
                    diffs.len()
                );
            }
        }
    }
}

fn between_row_strategy() -> impl Strategy<Value = (i64, i64, i64, String)> {
    (0_i64..6, 0_i64..6, 0_i64..10_000, "[a-z]{1,3}")
}

fn between_rows_strategy() -> impl Strategy<Value = Vec<(i64, i64, i64, String)>> {
    proptest::collection::vec(between_row_strategy(), 0..12)
}

#[derive(Debug, Clone)]
struct BetweenCase {
    a: Vec<(i64, i64, i64, String)>,
    b: Vec<(i64, i64, i64, String)>,
    c: Vec<(i64, i64, String)>,
}

fn between_case_strategy() -> impl Strategy<Value = BetweenCase> {
    let c_row = (0_i64..6, 0_i64..6, "[a-z]{1,3}").prop_map(|(k1, k2, s)| (k1, k2, s));
    let c_rows = proptest::collection::vec(c_row, 0..12).prop_map(|rows| {
        // c is materialised as a LOOKUP TABLE keyed on kc; dedupe in the
        // generator so the oracle (plain MemTable, no PK) sees the same
        // multiset the SUT does.
        let mut by_key: BTreeMap<i64, (i64, i64, String)> = BTreeMap::new();
        for row in rows {
            by_key.insert(row.0, row);
        }
        by_key.into_values().collect()
    });
    (between_rows_strategy(), between_rows_strategy(), c_rows)
        .prop_map(|(a, b, c)| BetweenCase { a, b, c })
}

fn block_on_check_between(case: &BetweenCase) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| format!("rt build: {e}"))?;
    rt.block_on(async {
        let a: Vec<_> = case
            .a
            .iter()
            .map(|(k1, k2, t, s)| (*k1, *k2, *t, s.as_str()))
            .collect();
        let b: Vec<_> = case
            .b
            .iter()
            .map(|(k1, k2, t, s)| (*k1, *k2, *t, s.as_str()))
            .collect();
        let c: Vec<_> = case
            .c
            .iter()
            .map(|(k1, k2, s)| (*k1, *k2, s.as_str()))
            .collect();
        let oracle = run_oracle_ts(
            BETWEEN_CHAIN_ORACLE_SQL,
            vec![make_a_ts(&a)],
            vec![make_b_ts(&b)],
            vec![make_batch_c(&c)],
        )
        .await
        .map_err(|e| format!("oracle: {e}"))?;

        let sut = run_sut_ts(
            BETWEEN_CHAIN_SUT_SQL,
            vec![make_a_ts(&a)],
            vec![make_b_ts(&b)],
            &c,
        )
        .await
        .map_err(|e| format!("sut: {e}"))?;

        let schema = output_schema_chain_star();
        let oracle_rows =
            batches_to_sorted_rows(&oracle, &schema).map_err(|e| format!("o rows: {e}"))?;
        let sut_rows =
            batches_to_sorted_rows(&sut, &schema).map_err(|e| format!("s rows: {e}"))?;

        if oracle_rows == sut_rows {
            return Ok(());
        }
        let (diffs, no, ns) = diff_counts(&oracle_rows, &sut_rows);
        Err(format!(
            "oracle={no} sut={ns} diffs={} a={:?} b={:?} c={:?}",
            diffs.len(),
            case.a,
            case.b,
            case.c
        ))
    })
}

/// Deterministic seed shared by every property test in this file.
const PROPTEST_SEED: [u8; 32] = [42; 32];

#[test]
fn property_n3_between_chain() {
    let config = PropConfig {
        cases: 16,
        failure_persistence: None,
        ..PropConfig::default()
    };
    let rng = TestRng::from_seed(proptest::test_runner::RngAlgorithm::ChaCha, &PROPTEST_SEED);
    let mut runner = TestRunner::new_with_rng(config, rng);
    let result = runner.run(&between_case_strategy(), |case| {
        block_on_check_between(&case).map_err(|m| TestCaseError::Fail(m.into()))
    });
    if let Err(e) = result {
        panic!("[BETWEEN CHAIN N=3] property failed: {e}");
    }
}

