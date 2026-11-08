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
pub(super) fn events(name: &'static str, seed: i64) -> FixtureSource {
    events_with(name, seed, BATCHES_PER_PARTITION)
}

pub(super) fn events_with(name: &'static str, seed: i64, batches: i64) -> FixtureSource {
    events_in(name, seed, PARTITIONS, batches)
}

/// [`events_with`] over `partitions` partitions; with fewer partitions than nodes, some nodes
/// own none.
pub(super) fn events_in(
    name: &'static str,
    seed: i64,
    partitions: i64,
    batches: i64,
) -> FixtureSource {
    let partitions = (0..partitions)
        .map(|partition| {
            (0..batches)
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
        checkpoint_under_load: false,
    }
}

async fn verdict(case: Case) -> Verdict {
    match case.run().await {
        Ok(verdict) => verdict,
        Err(bug) => panic!("conformance bug: {bug}"),
    }
}

pub(super) fn assert_rejected(verdict: Verdict, reason: &str) {
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

/// Split into an `EMIT CHANGES` stage for the inner aggregate and the outer aggregate over it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_aggregate_is_equal() {
    let case = case(
        "nested_aggregate",
        &["CREATE STREAM out AS SELECT n, COUNT(*) AS keys FROM \
           (SELECT k, COUNT(*) AS n FROM events GROUP BY k) GROUP BY n EMIT CHANGES"],
        OutputForm::Changelog,
        Some(
            "SELECT n, COUNT(*) AS keys FROM \
             (SELECT k, COUNT(*) AS n FROM events GROUP BY k) GROUP BY n",
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// The hot-key remedy: key 0 holds 20 of the 23 keys' rows. The salted stage spreads it over the
/// salt's owners, and the outer stage adds the per-salt counts and sums back up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn salted_two_stage_count_and_sum_are_equal() {
    let case = case(
        "salted_two_stage_count_and_sum",
        &[
            "CREATE STREAM salted AS SELECT k / 20 AS k, v % 8 AS salt, COUNT(*) AS n, \
             SUM(v) AS s FROM events GROUP BY k / 20, v % 8 EMIT CHANGES",
            "CREATE STREAM out AS SELECT k, SUM(n) AS n, SUM(s) AS s FROM salted GROUP BY k \
             EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some("SELECT k / 20 AS k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k / 20"),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// The outer stage takes back each salted extreme the inner stage retracts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn salted_two_stage_min_max_is_equal() {
    let case = case(
        "salted_two_stage_min_max",
        &[
            "CREATE STREAM salted AS SELECT k / 20 AS k, v % 8 AS salt, MIN(v) AS lo, \
             MAX(v) AS hi FROM events GROUP BY k / 20, v % 8 EMIT CHANGES",
            "CREATE STREAM out AS SELECT k, MIN(lo) AS lo, MAX(hi) AS hi FROM salted GROUP BY k \
             EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some("SELECT k / 20 AS k, MIN(v) AS lo, MAX(v) AS hi FROM events GROUP BY k / 20"),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

const PER_KEY_CHANGES: &str = "CREATE STREAM per_key AS SELECT k, COUNT(*) AS n, SUM(v) AS s \
     FROM events GROUP BY k EMIT CHANGES";

/// Each key's update retracts its previous `(n, s)` row, so every group's extreme is retracted
/// as often as it is replaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_min_max_over_changelog_stream_is_equal() {
    let case = case(
        "keyed_min_max_over_changelog_stream",
        &[
            PER_KEY_CHANGES,
            "CREATE STREAM out AS SELECT n, MIN(s) AS lo, MAX(s) AS hi FROM per_key GROUP BY n \
             EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some(
            "SELECT n, MIN(s) AS lo, MAX(s) AS hi FROM \
             (SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k) GROUP BY n",
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// A global MIN or MAX has no one-row partial, so every node sends its rows to the global owner,
/// which folds the COUNT and SUM beside them from the same rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_min_max_over_changelog_stream_is_equal() {
    let case = case(
        "global_min_max_over_changelog_stream",
        &[
            PER_KEY_CHANGES,
            "CREATE STREAM out AS SELECT COUNT(*) AS keys, SUM(s) AS total, MIN(s) AS lo, \
             MAX(s) AS hi FROM per_key EMIT CHANGES",
        ],
        OutputForm::Changelog,
        Some(
            "SELECT COUNT(*) AS keys, SUM(s) AS total, MIN(s) AS lo, MAX(s) AS hi FROM \
             (SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k)",
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

const UNION_ALL: &str =
    "SELECT k, v, ts FROM left_events UNION ALL SELECT k, v, ts FROM right_events";

fn union_case(
    name: &'static str,
    streams: Vec<String>,
    ground_truth: Option<&'static str>,
) -> Case {
    Case {
        name,
        sources: vec![events("left_events", 0), events("right_events", 5)],
        streams,
        observe: "out",
        form: OutputForm::Rows,
        ground_truth,
        checkpoint_under_load: false,
    }
}

/// Each node unions the rows of the partitions it owns, forwarding both sources' event time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn union_all_is_equal() {
    let case = union_case(
        "union_all",
        vec![format!("CREATE STREAM out AS {UNION_ALL}")],
        Some(UNION_ALL),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

const SELF_UNION: &str = "SELECT k, v, ts FROM events WHERE v % 2 = 0 \
     UNION ALL SELECT k, v, ts FROM events WHERE v % 3 = 0";

/// A union that reads one source twice has one input, so cluster `CREATE` admits it; the graph
/// classifies it as stateless and runs it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn union_all_of_one_source_is_equal() {
    let case = case(
        "union_all_of_one_source",
        &[&format!("CREATE STREAM out AS {SELF_UNION}")],
        OutputForm::Rows,
        Some(SELF_UNION),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// A window over a union closes on the lower of its two sources' frontiers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_over_union_all_is_equal() {
    let case = union_case(
        "window_over_union_all",
        vec![
            format!("CREATE STREAM merged AS {UNION_ALL}"),
            "CREATE STREAM out AS SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, \
             COUNT(*) AS n, SUM(v) AS s FROM merged \
             GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND) EMIT ON WINDOW CLOSE"
                .into(),
        ],
        Some(
            "SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, COUNT(*) AS n, \
             SUM(v) AS s FROM (SELECT k, v, ts FROM left_events UNION ALL \
             SELECT k, v, ts FROM right_events) WHERE ts < CAST(1400000 AS TIMESTAMP(6)) \
             GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND)",
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// A keyed aggregate over a union repartitions both sources' rows on its key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_over_union_all_is_equal() {
    let case = Case {
        form: OutputForm::Changelog,
        ..union_case(
            "aggregate_over_union_all",
            vec![
                format!("CREATE STREAM merged AS {UNION_ALL}"),
                "CREATE STREAM out AS SELECT k, COUNT(*) AS n, SUM(v) AS s FROM merged \
                 GROUP BY k EMIT CHANGES"
                    .into(),
            ],
            Some(
                "SELECT k, COUNT(*) AS n, SUM(v) AS s FROM (SELECT k, v, ts FROM left_events \
                 UNION ALL SELECT k, v, ts FROM right_events) GROUP BY k",
            ),
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// An interval join whose right side is a union of two sources.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interval_join_over_union_all_is_equal() {
    let case = Case {
        sources: vec![
            events("orders", 0),
            events("payments", 3),
            events("refunds", 5),
        ],
        ground_truth: Some(
            "SELECT o.k AS k, o.v AS ov, s.v AS sv FROM orders o INNER JOIN \
             (SELECT k, v, ts FROM payments UNION ALL SELECT k, v, ts FROM refunds) s \
             ON o.k = s.k AND s.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND",
        ),
        ..join_case(
            "interval_join_over_union_all",
            vec![
                "CREATE STREAM settled AS SELECT k, v, ts FROM payments \
                 UNION ALL SELECT k, v, ts FROM refunds"
                    .into(),
                "CREATE STREAM out AS SELECT o.k AS k, o.v AS ov, s.v AS sv FROM orders o \
                 INNER JOIN settled s ON o.k = s.k \
                 AND s.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
                    .into(),
            ],
            OutputForm::Rows,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

const UNION_REJECTION: &str =
    "a UNION ALL whose every branch forwards one source event-time column";

/// Without the event-time column a union has no frontier for a consumer to close on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn union_all_without_event_time_is_rejected_in_cluster() {
    let case = union_case(
        "union_all_without_event_time",
        vec!["CREATE STREAM out AS SELECT k, v FROM left_events \
              UNION ALL SELECT k, v FROM right_events"
            .into()],
        Some("SELECT k, v FROM left_events UNION ALL SELECT k, v FROM right_events"),
    );
    assert_rejected(verdict(case).await, UNION_REJECTION);
}

/// A union of interval join streams carries their event time in every mode, but a cluster
/// consumer could close past an idle join's later rows, so cluster mode rejects it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn union_all_over_join_streams_is_rejected_in_cluster() {
    let join = |name: &str, right: &str| {
        format!(
            "CREATE STREAM {name} AS SELECT o.k AS k, o.ts AS ts FROM orders o \
             INNER JOIN {right} r ON o.k = r.k \
             AND r.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
        )
    };
    let case = Case {
        sources: vec![
            events("orders", 0),
            events("payments", 3),
            events("refunds", 5),
        ],
        ..join_case(
            "union_all_over_join_streams",
            vec![
                join("paid", "payments"),
                join("refunded", "refunds"),
                "CREATE STREAM out AS SELECT k, ts FROM paid UNION ALL SELECT k, ts FROM refunded"
                    .into(),
            ],
            OutputForm::Rows,
        )
    };
    assert_rejected(verdict(case).await, UNION_REJECTION);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_remote_exchanges_are_rejected_in_cluster() {
    let case = case(
        "four_remote_exchanges",
        &[
            "CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM events GROUP BY k EMIT CHANGES",
            "CREATE STREAM by_n AS SELECT n, COUNT(*) AS keys FROM per_key GROUP BY n EMIT CHANGES",
            "CREATE STREAM by_keys AS SELECT keys, COUNT(*) AS ns FROM by_n \
             GROUP BY keys EMIT CHANGES",
            "CREATE STREAM out AS SELECT ns, COUNT(*) AS c FROM by_keys GROUP BY ns EMIT CHANGES",
        ],
        OutputForm::Changelog,
        None,
    );
    assert_rejected(verdict(case).await, "4 remote exchanges");
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
pub(super) const CLOSED_TUMBLE_TRUTH: &str =
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
async fn window_over_append_only_stream_is_equal() {
    let case = case(
        "window_over_stream",
        &[
            "CREATE STREAM filtered AS SELECT k, v, ts FROM events WHERE v > 10",
            "CREATE STREAM out AS SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, \
             COUNT(*) AS n FROM filtered \
             GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND) EMIT ON WINDOW CLOSE",
        ],
        OutputForm::Rows,
        Some(
            "SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, COUNT(*) AS n \
             FROM events WHERE v > 10 AND ts < CAST(1400000 AS TIMESTAMP(6)) \
             GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND)",
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

pub(super) const INTERVAL_JOIN: &str = "SELECT o.k AS k, o.v AS ov, p.v AS pv FROM orders o \
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
        checkpoint_under_load: false,
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

/// Three exchanges: both join inputs repartition on `k`, the keyed aggregate is colocated with
/// the join output, and the aggregate over it repartitions on `n`.
fn chained_aggregates_case(name: &'static str) -> Case {
    Case {
        ground_truth: Some(
            "SELECT n, COUNT(*) AS keys FROM \
             (SELECT k, COUNT(*) AS n FROM \
              (SELECT o.k AS k, o.v AS ov, p.v AS pv FROM orders o \
               INNER JOIN payments p ON o.k = p.k \
               AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND) GROUP BY k) \
             GROUP BY n",
        ),
        ..join_case(
            name,
            vec![
                format!("CREATE STREAM joined AS {INTERVAL_JOIN}"),
                "CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM joined \
                 GROUP BY k EMIT CHANGES"
                    .into(),
                "CREATE STREAM out AS SELECT n, COUNT(*) AS keys FROM per_key \
                 GROUP BY n EMIT CHANGES"
                    .into(),
            ],
            OutputForm::Changelog,
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_then_chained_aggregates_is_equal() {
    let case = chained_aggregates_case("join_then_chained_aggregates");
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// Checkpoints taken while rows still cross all three exchanges need more flush waves than an
/// idle pipeline and must still settle within the depth bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loaded_three_exchange_checkpoints_are_equal() {
    const BATCHES: i64 = 200;
    let case = Case {
        sources: vec![
            events_with("orders", 0, BATCHES),
            events_with("payments", 3, BATCHES),
        ],
        checkpoint_under_load: true,
        ..chained_aggregates_case("loaded_three_exchange_checkpoints")
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// Split into a join stage and an aggregate stage colocated with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fused_join_aggregate_is_equal() {
    let case = Case {
        ground_truth: Some(
            "SELECT o.k AS k, COUNT(*) AS n, SUM(p.v) AS s FROM orders o \
             INNER JOIN payments p ON o.k = p.k \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND GROUP BY o.k",
        ),
        ..join_case(
            "fused_join_aggregate",
            vec![
                "CREATE STREAM out AS SELECT o.k AS k, COUNT(*) AS n, SUM(p.v) AS s \
                 FROM orders o INNER JOIN payments p ON o.k = p.k \
                 AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
                 GROUP BY o.k EMIT CHANGES"
                    .into(),
            ],
            OutputForm::Changelog,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interval_join_over_append_only_streams_is_equal() {
    let case = Case {
        ground_truth: Some(
            "SELECT o.k AS k, o.v AS ov, p.v AS pv FROM orders o \
             INNER JOIN payments p ON o.k = p.k \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
             WHERE o.v > 10 AND p.v % 2 = 0",
        ),
        ..join_case(
            "join_over_streams",
            vec![
                "CREATE STREAM big_orders AS SELECT k, v, ts FROM orders WHERE v > 10".into(),
                "CREATE STREAM even_payments AS SELECT k, v, ts FROM payments WHERE v % 2 = 0"
                    .into(),
                "CREATE STREAM out AS SELECT o.k AS k, o.v AS ov, p.v AS pv FROM big_orders o \
                 INNER JOIN even_payments p ON o.k = p.k \
                 AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
                    .into(),
            ],
            OutputForm::Rows,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// An interval join stream that carries its left time column, `ots`.
pub(super) const JOINED: &str = "CREATE STREAM joined AS SELECT o.k AS k, o.ts AS ots, p.v AS pv \
     FROM orders o INNER JOIN payments p ON o.k = p.k \
     AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND";

/// 20 ms windows over `joined`, after the statement's name.
pub(super) const WINDOWS_OVER_JOINED: &str =
    "AS SELECT k % 4 AS g, tumble(ots, INTERVAL '20' MILLISECOND) AS w, \
     COUNT(*) AS n, SUM(pv) AS s FROM joined \
     GROUP BY k % 4, tumble(ots, INTERVAL '20' MILLISECOND) EMIT ON WINDOW CLOSE";

/// The windows over `joined` that close. The join's frontier trails the sources' final watermark
/// (1.469 s) by its 50 ms bound, so 20 ms windows close only up to 1.400 s.
pub(super) const WINDOWS_OVER_JOINED_TRUTH: &str =
    "SELECT k % 4 AS g, tumble(ots, INTERVAL '20' MILLISECOND) AS w, COUNT(*) AS n, \
     SUM(pv) AS s FROM \
     (SELECT o.k AS k, o.ts AS ots, p.v AS pv FROM orders o \
      INNER JOIN payments p ON o.k = p.k \
      AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND) \
     WHERE ots < CAST(1400000 AS TIMESTAMP(6)) \
     GROUP BY k % 4, tumble(ots, INTERVAL '20' MILLISECOND)";

fn window_over_join() -> Vec<String> {
    vec![
        JOINED.to_owned(),
        format!("CREATE STREAM out {WINDOWS_OVER_JOINED}"),
    ]
}

/// A window over an interval join stream, on the join's left time column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_over_interval_join_is_equal() {
    let case = Case {
        ground_truth: Some(WINDOWS_OVER_JOINED_TRUTH),
        ..join_case(
            "window_over_interval_join",
            window_over_join(),
            OutputForm::Rows,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// Checkpoints taken while joined rows cross to their window owners.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loaded_window_over_interval_join_checkpoints_are_equal() {
    const BATCHES: i64 = 200;
    let case = Case {
        sources: vec![
            events_with("orders", 0, BATCHES),
            events_with("payments", 3, BATCHES),
        ],
        checkpoint_under_load: true,
        ..join_case(
            "loaded_window_over_interval_join",
            window_over_join(),
            OutputForm::Rows,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// A window over each join kind's event-time column: the left time for left outer, semi and anti
/// joins, the right time for the right kinds and for an inner join that projects only it.
/// Differential only: unmatched rows near the final watermark stay open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn windows_over_each_join_side_are_equal() {
    for (name, keyword, projection, time) in [
        ("inner_right_time", "INNER JOIN", "p.k AS k, p.ts AS t", "t"),
        ("left_outer", "LEFT JOIN", "o.k AS k, o.ts AS t", "t"),
        ("left_semi", "LEFT SEMI JOIN", "o.k AS k, o.ts AS t", "t"),
        ("left_anti", "LEFT ANTI JOIN", "o.k AS k, o.ts AS t", "t"),
        ("right_outer", "RIGHT JOIN", "p.k AS k, p.ts AS t", "t"),
        ("right_semi", "RIGHT SEMI JOIN", "p.k AS k, p.ts AS t", "t"),
        ("right_anti", "RIGHT ANTI JOIN", "p.k AS k, p.ts AS t", "t"),
    ] {
        let case = join_case(
            name,
            vec![
                format!(
                    "CREATE STREAM joined AS SELECT {projection} FROM orders o {keyword} \
                     payments p ON o.k = p.k \
                     AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
                ),
                format!(
                    "CREATE STREAM out AS SELECT k % 4 AS g, \
                     tumble({time}, INTERVAL '20' MILLISECOND) AS w, COUNT(*) AS n FROM joined \
                     GROUP BY k % 4, tumble({time}, INTERVAL '20' MILLISECOND) \
                     EMIT ON WINDOW CLOSE"
                ),
            ],
            OutputForm::Rows,
        );
        assert_eq!(verdict(case).await, Verdict::Equal, "{name}");
    }
}

/// A full outer join's unmatched rows carry only one side's time, so its output has no event
/// time for a cluster window to close on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_over_full_outer_join_is_rejected_in_cluster() {
    let case = join_case(
        "window_over_full_outer_join",
        vec![
            "CREATE STREAM joined AS SELECT o.k AS k, o.ts AS t FROM orders o FULL JOIN \
             payments p ON o.k = p.k AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
                .into(),
            "CREATE STREAM out AS SELECT tumble(t, INTERVAL '20' MILLISECOND) AS w, \
             COUNT(*) AS n FROM joined GROUP BY tumble(t, INTERVAL '20' MILLISECOND) \
             EMIT ON WINDOW CLOSE"
                .into(),
        ],
        OutputForm::Rows,
    );
    assert_rejected(verdict(case).await, "bounded event-time watermark");
}

/// 100 ms windows rolled up into 200 ms windows. An upstream that carries its start holds its
/// frontier back by its size, to 1.369 s, so the rollup closes only windows before 1.200 s; one
/// that carries its end holds back by its lateness (zero), so the rollup closes windows before
/// 1.400 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn windows_over_windows_are_equal() {
    let tumble = "tumble(ts, INTERVAL '100' MILLISECOND)";
    let tumble_end = "tumble_end(ts, INTERVAL '100' MILLISECOND)";
    for (name, upstream, groups, truth) in [
        (
            "window_over_window_start",
            format!("{tumble} AS w"),
            tumble.to_string(),
            Some(
                "SELECT g, tumble(w, INTERVAL '200' MILLISECOND) AS r, SUM(n) AS n FROM \
                 (SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, COUNT(*) AS n \
                  FROM events WHERE ts < CAST(1400000 AS TIMESTAMP(6)) \
                  GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND)) \
                 WHERE w < CAST(1200000 AS TIMESTAMP(6)) \
                 GROUP BY g, tumble(w, INTERVAL '200' MILLISECOND)",
            ),
        ),
        (
            "window_over_window_end",
            format!("{tumble_end} AS w"),
            format!("{tumble}, {tumble_end}"),
            Some(
                "SELECT g, tumble(w, INTERVAL '200' MILLISECOND) AS r, SUM(n) AS n FROM \
                 (SELECT k % 4 AS g, tumble_end(ts, INTERVAL '100' MILLISECOND) AS w, \
                  COUNT(*) AS n FROM events WHERE ts < CAST(1400000 AS TIMESTAMP(6)) \
                  GROUP BY k % 4, tumble_end(ts, INTERVAL '100' MILLISECOND)) \
                 WHERE w < CAST(1400000 AS TIMESTAMP(6)) \
                 GROUP BY g, tumble(w, INTERVAL '200' MILLISECOND)",
            ),
        ),
        (
            "window_over_hop_start",
            "hop(ts, INTERVAL '50' MILLISECOND, INTERVAL '100' MILLISECOND) AS w".to_string(),
            "hop(ts, INTERVAL '50' MILLISECOND, INTERVAL '100' MILLISECOND)".to_string(),
            None,
        ),
    ] {
        let case = case(
            name,
            &[
                &format!(
                    "CREATE STREAM per_window AS SELECT k % 4 AS g, {upstream}, COUNT(*) AS n \
                     FROM events GROUP BY k % 4, {groups} EMIT ON WINDOW CLOSE"
                ),
                "CREATE STREAM out AS SELECT g, tumble(w, INTERVAL '200' MILLISECOND) AS r, \
                 SUM(n) AS n FROM per_window \
                 GROUP BY g, tumble(w, INTERVAL '200' MILLISECOND) EMIT ON WINDOW CLOSE",
            ],
            OutputForm::Rows,
            truth,
        );
        assert_eq!(verdict(case).await, Verdict::Equal, "{name}");
    }
}

/// A session's start trails its close without bound, so no mode admits a window over it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_over_session_is_unsupported_everywhere() {
    let case = case(
        "window_over_session",
        &[
            "CREATE STREAM sessions AS SELECT k % 4 AS g, \
             SESSION(ts, INTERVAL '5' MILLISECOND) AS s, COUNT(*) AS n FROM events \
             GROUP BY k % 4, SESSION(ts, INTERVAL '5' MILLISECOND) EMIT ON WINDOW CLOSE",
            "CREATE STREAM out AS SELECT g, tumble(s, INTERVAL '200' MILLISECOND) AS r, \
             SUM(n) AS n FROM sessions \
             GROUP BY g, tumble(s, INTERVAL '200' MILLISECOND) EMIT ON WINDOW CLOSE",
        ],
        OutputForm::Rows,
        None,
    );
    assert!(matches!(
        verdict(case).await,
        Verdict::UnsupportedEverywhere(reason) if reason.contains("carries no event-time column")
    ));
}

/// An interval join over an interval join stream, on the stream's event-time column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interval_join_over_interval_join_is_equal() {
    let joined = "SELECT o.k AS k, o.ts AS ots, p.v AS pv FROM orders o \
                  INNER JOIN payments p ON o.k = p.k \
                  AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND";
    let case = Case {
        sources: vec![
            events("orders", 0),
            events("payments", 3),
            events("refunds", 5),
        ],
        ground_truth: Some(
            "SELECT j.k AS k, j.pv AS pv, r.v AS rv FROM \
             (SELECT o.k AS k, o.ts AS ots, p.v AS pv FROM orders o \
              INNER JOIN payments p ON o.k = p.k \
              AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND) j \
             INNER JOIN refunds r ON j.k = r.k \
             AND r.ts BETWEEN j.ots AND j.ots + INTERVAL '30' MILLISECOND",
        ),
        ..join_case(
            "interval_join_over_interval_join",
            vec![
                format!("CREATE STREAM joined AS {joined}"),
                "CREATE STREAM out AS SELECT j.k AS k, j.pv AS pv, r.v AS rv FROM joined j \
                 INNER JOIN refunds r ON j.k = r.k \
                 AND r.ts BETWEEN j.ots AND j.ots + INTERVAL '30' MILLISECOND"
                    .into(),
            ],
            OutputForm::Rows,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// The first `inputs` of `orders`, `payments`, `refunds` and `shipments`.
fn chain_sources(inputs: usize, batches: i64) -> Vec<FixtureSource> {
    [
        ("orders", 0),
        ("payments", 3),
        ("refunds", 5),
        ("shipments", 7),
    ]
    .into_iter()
    .take(inputs)
    .map(|(name, seed)| events_with(name, seed, batches))
    .collect()
}

/// A three-way interval join splits into a join stage and the owner's join over it.
pub(super) const THREE_WAY_JOIN: &str = "SELECT o.k AS k, p.v AS pv, r.v AS rv FROM orders o \
     INNER JOIN payments p ON o.k = p.k \
     AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
     INNER JOIN refunds r ON o.k = r.k \
     AND r.ts BETWEEN o.ts AND o.ts + INTERVAL '30' MILLISECOND";

/// Three remote exchanges: the joins repartition on `k` and then `v`, the aggregate on `r.k`.
pub(super) const THREE_WAY_TOTALS: &str =
    "SELECT r.k AS k, COUNT(*) AS n, SUM(o.v) AS s FROM orders o \
     INNER JOIN payments p ON o.k = p.k \
     AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
     INNER JOIN refunds r ON p.v = r.v \
     AND r.ts BETWEEN o.ts AND o.ts + INTERVAL '30' MILLISECOND \
     GROUP BY r.k";

fn chain_case(name: &'static str, inputs: usize, stream: String, form: OutputForm) -> Case {
    Case {
        sources: chain_sources(inputs, BATCHES_PER_PARTITION),
        ..join_case(name, vec![stream], form)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_way_interval_join_is_equal() {
    let case = Case {
        ground_truth: Some(THREE_WAY_JOIN),
        ..chain_case(
            "three_way_interval_join",
            3,
            format!("CREATE STREAM out AS {THREE_WAY_JOIN}"),
            OutputForm::Rows,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

fn three_way_totals_case(name: &'static str) -> Case {
    Case {
        ground_truth: Some(THREE_WAY_TOTALS),
        ..chain_case(
            name,
            3,
            format!("CREATE STREAM out AS {THREE_WAY_TOTALS} EMIT CHANGES"),
            OutputForm::Changelog,
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_way_join_aggregate_is_equal() {
    let case = three_way_totals_case("three_way_join_aggregate");
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// Checkpoints taken while rows still cross both join stages and the aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loaded_three_way_join_aggregate_checkpoints_are_equal() {
    let case = Case {
        sources: chain_sources(3, 200),
        checkpoint_under_load: true,
        ..three_way_totals_case("loaded_three_way_join_aggregate")
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// A fourth join on another key puts the aggregate four remote exchanges from the sources.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_way_join_aggregate_is_rejected_in_cluster() {
    let case = chain_case(
        "four_way_join_aggregate",
        4,
        "CREATE STREAM out AS SELECT s.v AS v, COUNT(*) AS n FROM orders o \
         INNER JOIN payments p ON o.k = p.k \
         AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
         INNER JOIN refunds r ON p.v = r.v \
         AND r.ts BETWEEN o.ts AND o.ts + INTERVAL '30' MILLISECOND \
         INNER JOIN shipments s ON r.k = s.k \
         AND s.ts BETWEEN o.ts AND o.ts + INTERVAL '20' MILLISECOND \
         GROUP BY s.v EMIT CHANGES"
            .into(),
        OutputForm::Changelog,
    );
    assert_rejected(verdict(case).await, "crosses 4 remote exchanges");
}

/// The first join's stage carries its left time, `o.ts`, so a later join bounded by `p.ts` reads
/// a column whose watermark nothing holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_chain_on_a_time_its_stage_does_not_carry_is_rejected() {
    let case = chain_case(
        "join_chain_on_uncarried_time",
        3,
        "CREATE STREAM out AS SELECT o.k AS k, o.ts AS ots, r.v AS rv FROM orders o \
         INNER JOIN payments p ON o.k = p.k \
         AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
         INNER JOIN refunds r ON o.k = r.k \
         AND r.ts BETWEEN p.ts AND p.ts + INTERVAL '30' MILLISECOND"
            .into(),
        OutputForm::Rows,
    );
    assert!(matches!(
        verdict(case).await,
        Verdict::UnsupportedEverywhere(reason)
            if reason.contains("'out$stage1' must carry an event-time watermark on 'p_ts' but carries 'o_ts'")
    ));
}

/// A window and a join in one statement split into a join stage and the window over it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn windowed_join_is_equal() {
    let case = Case {
        ground_truth: Some(WINDOWS_OVER_JOINED_TRUTH),
        ..join_case(
            "windowed_join",
            vec!["CREATE STREAM out AS SELECT o.k % 4 AS g, \
                 tumble(o.ts, INTERVAL '20' MILLISECOND) AS w, COUNT(*) AS n, SUM(p.v) AS s \
                 FROM orders o INNER JOIN payments p ON o.k = p.k \
                 AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
                 GROUP BY o.k % 4, tumble(o.ts, INTERVAL '20' MILLISECOND) EMIT ON WINDOW CLOSE"
                .into()],
            OutputForm::Rows,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// A three-way join stream named `joined`: its owner runs the second join, on `p.v`, over the
/// first join's stage.
pub(super) const JOINED_CHAIN: &str = "CREATE STREAM joined AS SELECT o.k AS k, o.ts AS ots, \
     p.v AS pv FROM orders o INNER JOIN payments p ON o.k = p.k \
     AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
     INNER JOIN refunds r ON p.v = r.v \
     AND r.ts BETWEEN o.ts AND o.ts + INTERVAL '30' MILLISECOND";

/// The windows over `JOINED_CHAIN` that close. Its frontier trails the final watermark by the
/// first join's 50 ms bound, so the same windows close as over the two-way join.
pub(super) const WINDOWS_OVER_JOINED_CHAIN_TRUTH: &str =
    "SELECT k % 4 AS g, tumble(ots, INTERVAL '20' MILLISECOND) AS w, COUNT(*) AS n, \
     SUM(pv) AS s FROM \
     (SELECT o.k AS k, o.ts AS ots, p.v AS pv FROM orders o \
      INNER JOIN payments p ON o.k = p.k \
      AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
      INNER JOIN refunds r ON p.v = r.v \
      AND r.ts BETWEEN o.ts AND o.ts + INTERVAL '30' MILLISECOND) \
     WHERE ots < CAST(1400000 AS TIMESTAMP(6)) \
     GROUP BY k % 4, tumble(ots, INTERVAL '20' MILLISECOND)";

/// A window over a three-way join stream, three remote exchanges from the sources: the joins
/// repartition on `k` and `v`, the window on `k % 4`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_over_three_way_join_is_equal() {
    let case = Case {
        sources: chain_sources(3, BATCHES_PER_PARTITION),
        ground_truth: Some(WINDOWS_OVER_JOINED_CHAIN_TRUTH),
        ..join_case(
            "window_over_three_way_join",
            vec![
                JOINED_CHAIN.to_owned(),
                format!("CREATE STREAM out {WINDOWS_OVER_JOINED}"),
            ],
            OutputForm::Rows,
        )
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
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

/// The inner window stage emits changes, which cluster windows do not support.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_window_aggregate_is_rejected_in_cluster() {
    let case = case(
        "nested_window_aggregate",
        &["CREATE STREAM out AS SELECT n, COUNT(*) AS c FROM \
           (SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, COUNT(*) AS n \
            FROM events GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND)) \
           GROUP BY n EMIT CHANGES"],
        OutputForm::Changelog,
        Some(
            "SELECT n, COUNT(*) AS c FROM \
             (SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, COUNT(*) AS n \
              FROM events GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND)) GROUP BY n",
        ),
    );
    assert_rejected(verdict(case).await, "EMIT ON WINDOW CLOSE");
}
