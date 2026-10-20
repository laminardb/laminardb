//! Conformance corpus. Each case states its current cluster verdict; a case that must change
//! verdict changes here in the same commit as the planner or runtime change that moves it.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use super::oracle::OutputForm;
use super::source::FixtureSource;
use super::{Case, Verdict};

const PARTITIONS: i64 = 6;
const BATCHES_PER_PARTITION: i64 = 3;
const ROWS_PER_BATCH: i64 = 16;
const KEYS: i64 = 23;
const EVENT_COLUMNS: &str = "(k BIGINT NOT NULL, v BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
     WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)";

fn event_batch(rows: impl Iterator<Item = (i64, i64, i64)>) -> RecordBatch {
    let (mut k, mut v, mut ts) = (Vec::new(), Vec::new(), Vec::new());
    for (key, value, micros) in rows {
        k.push(key);
        v.push(value);
        ts.push(micros);
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(TimestampMicrosecondArray::from(ts)),
        ],
    )
    .unwrap()
}

/// Deterministic input spread over every partition; `seed` decorrelates two sources.
fn events(name: &'static str, seed: i64) -> FixtureSource {
    let partitions = (0..PARTITIONS)
        .map(|partition| {
            (0..BATCHES_PER_PARTITION)
                .map(|batch| {
                    event_batch((0..ROWS_PER_BATCH).map(|row| {
                        let n = (partition * 1_000 + batch * 100 + row) * 7 + seed;
                        let micros = 1_000_000 + (batch * ROWS_PER_BATCH + row) * 10_000;
                        ((n * 31) % KEYS, n % 97, micros)
                    }))
                })
                .collect()
        })
        .collect::<Vec<_>>();
    FixtureSource {
        name,
        columns: EVENT_COLUMNS,
        partitions: partitions.into(),
    }
}

fn case(
    name: &'static str,
    streams: &[&str],
    form: OutputForm,
    ground_truth: Option<&'static str>,
) -> Case {
    Case {
        name,
        sources: vec![events("events", 0)],
        streams: streams.iter().map(|s| (*s).to_string()).collect(),
        observe: "out",
        form,
        ground_truth,
    }
}

async fn verdict(case: Case) -> Verdict {
    match case.run().await {
        Ok(verdict) => verdict,
        Err(bug) => panic!("conformance bug: {bug}"),
    }
}

fn assert_rejected(verdict: Verdict, reason: &str) {
    match verdict {
        Verdict::Rejected(message) => assert!(message.contains(reason), "{message}"),
        other => panic!("expected cluster rejection containing {reason:?}, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projection_filter_is_equal() {
    let case = case(
        "projection_filter",
        &["CREATE STREAM out AS SELECT k, v * 2 AS v2 FROM events WHERE v % 3 = 0"],
        OutputForm::Rows,
        Some("SELECT k, v * 2 AS v2 FROM events WHERE v % 3 = 0"),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_aggregate_changelog_is_equal() {
    let case = case(
        "keyed_aggregate_changelog",
        &[
            "CREATE STREAM out AS SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k \
           EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some("SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k"),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_aggregate_running_state_is_equal() {
    let case = case(
        "keyed_aggregate_running_state",
        &["CREATE STREAM out AS SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k"],
        OutputForm::Upsert(&[0]),
        Some("SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k"),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_aggregate_is_equal() {
    let case = case(
        "global_aggregate",
        &["CREATE STREAM out AS SELECT COUNT(*) AS n, SUM(v) AS s FROM events EMIT CHANGES"],
        OutputForm::Changelog,
        Some("SELECT COUNT(*) AS n, SUM(v) AS s FROM events"),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_float_aggregate_is_equal() {
    let case = case(
        "global_float_aggregate",
        &[
            "CREATE STREAM out AS SELECT SUM(CAST(v AS DOUBLE) / 7) AS s, \
           AVG(CAST(v AS DOUBLE) / 7) AS mean FROM events EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some("SELECT SUM(CAST(v AS DOUBLE) / 7) AS s, AVG(CAST(v AS DOUBLE) / 7) AS mean FROM events"),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_aggregate_over_changelog_stream_is_equal() {
    let case = case(
        "global_aggregate_over_changelog_stream",
        &[
            "CREATE STREAM per_key AS SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k \
           EMIT CHANGES",
            "CREATE STREAM out AS SELECT COUNT(*) AS keys, SUM(n) AS total_rows, SUM(s) AS total \
           FROM per_key EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some(
            "SELECT COUNT(*) AS keys, SUM(n) AS total_rows, SUM(s) AS total FROM \
             (SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k)",
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_over_projection_stream_is_equal() {
    let case = case(
        "aggregate_over_projection_stream",
        &[
            "CREATE STREAM grouped AS SELECT k % 4 AS g, v FROM events",
            "CREATE STREAM out AS SELECT g, SUM(v) AS s FROM grouped GROUP BY g EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some("SELECT k % 4 AS g, SUM(v) AS s FROM events GROUP BY k % 4"),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_over_aggregate_stream_is_equal() {
    let case = case(
        "aggregate_over_aggregate_stream",
        &[
            "CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM events GROUP BY k EMIT CHANGES",
            "CREATE STREAM out AS SELECT n, COUNT(*) AS keys FROM per_key GROUP BY n EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some(
            "SELECT n, COUNT(*) AS keys FROM \
             (SELECT k, COUNT(*) AS n FROM events GROUP BY k) GROUP BY n",
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_aggregate_is_unsupported_everywhere() {
    let case = case(
        "nested_aggregate",
        &["CREATE STREAM out AS SELECT n, COUNT(*) AS keys FROM \
           (SELECT k, COUNT(*) AS n FROM events GROUP BY k) GROUP BY n EMIT CHANGES"],
        OutputForm::Changelog,
        None,
    );
    assert!(matches!(
        verdict(case).await,
        Verdict::UnsupportedEverywhere(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn union_all_is_rejected_in_cluster() {
    let case = Case {
        name: "union_all",
        sources: vec![events("left_events", 0), events("right_events", 5)],
        streams: vec!["CREATE STREAM out AS SELECT k, v FROM left_events \
             UNION ALL SELECT k, v FROM right_events"
            .into()],
        observe: "out",
        form: OutputForm::Rows,
        ground_truth: Some("SELECT k, v FROM left_events UNION ALL SELECT k, v FROM right_events"),
    };
    assert_rejected(verdict(case).await, "single input frontier");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_aggregate_running_state_is_equal() {
    let case = case(
        "global_aggregate_running_state",
        &["CREATE STREAM out AS SELECT COUNT(*) AS n, MIN(v) AS lo, MAX(v) AS hi FROM events"],
        OutputForm::Upsert(&[]),
        Some("SELECT COUNT(*) AS n, MIN(v) AS lo, MAX(v) AS hi FROM events"),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_key_min_max_avg_is_equal() {
    let case = case(
        "composite_key_min_max_avg",
        &[
            "CREATE STREAM out AS SELECT k % 5 AS a, v % 3 AS b, MIN(v) AS lo, MAX(v) AS hi, \
           AVG(v) AS mean FROM events GROUP BY k % 5, v % 3 EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some(
            "SELECT k % 5 AS a, v % 3 AS b, MIN(v) AS lo, MAX(v) AS hi, AVG(v) AS mean \
             FROM events GROUP BY k % 5, v % 3",
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// Without `EMIT CHANGES` the upstream re-emits snapshots that a downstream aggregate would
/// count as new rows; this matched ground truth only when every input landed in one cycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_over_running_state_stream_is_unsupported_everywhere() {
    let case = case(
        "aggregate_over_running_state_stream",
        &[
            "CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM events GROUP BY k",
            "CREATE STREAM out AS SELECT n, COUNT(*) AS keys FROM per_key GROUP BY n EMIT CHANGES",
        ],
        OutputForm::Changelog,
        None,
    );
    assert!(matches!(
        verdict(case).await,
        Verdict::UnsupportedEverywhere(reason) if reason.contains("running-state aggregate")
    ));
}

/// Every partition spans event times 1.000 s to 1.470 s, so the final watermark closes the
/// 100 ms windows that end at or before 1.400 s.
const CLOSED_TUMBLE_TRUTH: &str =
    "SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, \
     COUNT(*) AS n, SUM(v) AS s FROM events \
     WHERE ts < CAST(1400000 AS TIMESTAMP(6)) \
     GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tumble_window_close_is_equal() {
    let case = case(
        "tumble_window_close",
        &[
            "CREATE STREAM out AS SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, \
           COUNT(*) AS n, SUM(v) AS s FROM events \
           GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND) EMIT ON WINDOW CLOSE",
        ],
        OutputForm::Rows,
        Some(CLOSED_TUMBLE_TRUTH),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hop_and_session_windows_are_equal() {
    for (name, window) in [
        (
            "hop_window_final",
            "HOP(ts, INTERVAL '50' MILLISECOND, INTERVAL '100' MILLISECOND)",
        ),
        (
            "session_window_close",
            "SESSION(ts, INTERVAL '5' MILLISECOND)",
        ),
    ] {
        let sql = format!(
            "CREATE STREAM out AS SELECT k % 4 AS g, {window} AS w, COUNT(*) AS n \
             FROM events GROUP BY k % 4, {window} EMIT ON WINDOW CLOSE"
        );
        let case = Case {
            streams: vec![sql],
            ..case(name, &[], OutputForm::Rows, None)
        };
        assert_eq!(verdict(case).await, Verdict::Equal, "{name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_over_stream_is_rejected_in_cluster() {
    let case = case(
        "window_over_stream",
        &[
            "CREATE STREAM filtered AS SELECT k, v, ts FROM events WHERE v > 10",
            "CREATE STREAM out AS SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, \
             COUNT(*) AS n FROM filtered \
             GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND) EMIT ON WINDOW CLOSE",
        ],
        OutputForm::Rows,
        None,
    );
    assert_rejected(verdict(case).await, "direct source");
}

const INTERVAL_JOIN: &str = "SELECT o.k AS k, o.v AS ov, p.v AS pv FROM orders o \
     INNER JOIN payments p ON o.k = p.k \
     AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND";

fn join_case(name: &'static str, streams: Vec<String>, form: OutputForm) -> Case {
    Case {
        name,
        sources: vec![events("orders", 0), events("payments", 3)],
        streams,
        observe: "out",
        form,
        ground_truth: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interval_join_is_equal() {
    let case = Case {
        ground_truth: Some(INTERVAL_JOIN),
        ..join_case(
            "interval_join",
            vec![format!("CREATE STREAM out AS {INTERVAL_JOIN}")],
            OutputForm::Rows,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_then_aggregate_is_equal() {
    let case = Case {
        ground_truth: Some(
            "SELECT k, COUNT(*) AS n, SUM(pv) AS s FROM \
             (SELECT o.k AS k, o.v AS ov, p.v AS pv FROM orders o \
              INNER JOIN payments p ON o.k = p.k \
              AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND) GROUP BY k",
        ),
        ..join_case(
            "join_then_aggregate",
            vec![
                format!("CREATE STREAM joined AS {INTERVAL_JOIN}"),
                "CREATE STREAM out AS SELECT k, COUNT(*) AS n, SUM(pv) AS s FROM joined \
                 GROUP BY k EMIT CHANGES"
                    .into(),
            ],
            OutputForm::Changelog,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fused_join_aggregate_is_unsupported_everywhere() {
    let case = join_case(
        "fused_join_aggregate",
        vec![
            "CREATE STREAM out AS SELECT o.k AS k, COUNT(*) AS n FROM orders o \
              INNER JOIN payments p ON o.k = p.k \
              AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
              GROUP BY o.k EMIT CHANGES"
                .into(),
        ],
        OutputForm::Changelog,
    );
    assert!(matches!(
        verdict(case).await,
        Verdict::UnsupportedEverywhere(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_over_stream_is_unsupported_everywhere() {
    let case = join_case(
        "join_over_stream",
        vec![
            "CREATE STREAM big_orders AS SELECT k, v, ts FROM orders WHERE v > 10".into(),
            "CREATE STREAM out AS SELECT o.k AS k, o.v AS ov, p.v AS pv FROM big_orders o \
             INNER JOIN payments p ON o.k = p.k \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
                .into(),
        ],
        OutputForm::Rows,
    );
    assert!(matches!(
        verdict(case).await,
        Verdict::UnsupportedEverywhere(reason) if reason.contains("directly watermarked source")
    ));
}

/// Join kinds whose output distribution differs (null-extended rows are placed by the side that
/// is present). Differential only: unmatched rows near the final watermark stay open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_semi_and_anti_joins_are_equal() {
    for (name, keyword, projection) in [
        ("left_outer", "LEFT JOIN", "o.k AS ok, o.v AS ov, p.v AS pv"),
        (
            "right_outer",
            "RIGHT JOIN",
            "p.k AS pk, o.v AS ov, p.v AS pv",
        ),
        (
            "full_outer",
            "FULL JOIN",
            "o.k AS ok, p.k AS pk, o.v AS ov, p.v AS pv",
        ),
        ("left_semi", "LEFT SEMI JOIN", "o.k AS ok, o.v AS ov"),
        ("left_anti", "LEFT ANTI JOIN", "o.k AS ok, o.v AS ov"),
        ("right_semi", "RIGHT SEMI JOIN", "p.k AS pk, p.v AS pv"),
        ("right_anti", "RIGHT ANTI JOIN", "p.k AS pk, p.v AS pv"),
    ] {
        let sql = format!(
            "CREATE STREAM out AS SELECT {projection} FROM orders o {keyword} payments p \
             ON o.k = p.k AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
        );
        let case = join_case(name, vec![sql], OutputForm::Rows);
        assert_eq!(verdict(case).await, Verdict::Equal, "{name}");
    }
}
