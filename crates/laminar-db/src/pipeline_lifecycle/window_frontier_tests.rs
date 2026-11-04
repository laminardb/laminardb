//! Event-time frontiers through windows fed by another window or by an interval join, on the
//! startup-built operator graph with one controlled frontier per cycle.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch, TimestampMillisecondArray};
use arrow::datatypes::DataType;

use super::{FxHashMap, HashMap, LaminarDB};
use crate::engine_metrics::EngineMetrics;
use crate::operator_graph::{InputFrontier, OperatorGraph};

const SECOND: i64 = 1_000;
const MINUTE: i64 = 60 * SECOND;
/// An hour-aligned epoch, so every window size below starts a bucket here.
const T0: i64 = 1_699_999_200_000;

async fn database(ddl: &[&str]) -> (Arc<LaminarDB>, Arc<EngineMetrics>) {
    let db = LaminarDB::open().unwrap();
    let prom = Arc::new(EngineMetrics::new(&prometheus::Registry::new()));
    db.set_engine_metrics(Arc::clone(&prom));
    for statement in ddl {
        db.execute(statement).await.unwrap();
    }
    (db, prom)
}

/// A batch of `source` with each column's values given in milliseconds or as integers.
fn source_batch(db: &LaminarDB, source: &str, columns: &[(&str, &[i64])]) -> RecordBatch {
    let schema = db.catalog.get_source(source).unwrap().schema.clone();
    let arrays = schema
        .fields()
        .iter()
        .map(|field| {
            let values = columns
                .iter()
                .find(|(name, _)| name == field.name())
                .map(|(_, values)| values.to_vec())
                .unwrap();
            let array: Arc<dyn Array> = match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(values)),
                timestamp => {
                    arrow::compute::cast(&TimestampMillisecondArray::from(values), timestamp)
                        .unwrap()
                }
            };
            array
        })
        .collect();
    RecordBatch::try_new(schema, arrays).unwrap()
}

struct Pipeline {
    db: Arc<LaminarDB>,
    graph: OperatorGraph,
    prom: Arc<EngineMetrics>,
}

impl Pipeline {
    async fn new(ddl: &[&str]) -> Self {
        let (db, prom) = database(ddl).await;
        let streams = db.connector_manager.lock().streams().clone();
        let resolved = db
            .resolve_catalog_stream_outputs(std::iter::empty())
            .await
            .unwrap();
        let graph = db
            .build_connector_operator_graph(
                &streams,
                &HashMap::new(),
                &resolved,
                &FxHashMap::default(),
                None,
            )
            .unwrap()
            .initialize_managed_state()
            .await
            .unwrap();
        Self { db, graph, prom }
    }

    fn batch(&self, source: &str, columns: &[(&str, &[i64])]) -> RecordBatch {
        source_batch(&self.db, source, columns)
    }

    /// One cycle in which every source in `frontiers` advances to its watermark.
    async fn cycle(
        &mut self,
        batches: Vec<(&str, RecordBatch)>,
        frontiers: &[(&str, i64)],
    ) -> FxHashMap<Arc<str>, Vec<RecordBatch>> {
        let mut input: FxHashMap<Arc<str>, Vec<RecordBatch>> = FxHashMap::default();
        for (source, batch) in batches {
            input.entry(Arc::from(source)).or_default().push(batch);
        }
        let frontiers = frontiers
            .iter()
            .map(|&(source, watermark)| {
                (
                    Arc::from(source),
                    InputFrontier {
                        watermark: Some(watermark),
                        idle: false,
                    },
                )
            })
            .collect::<FxHashMap<_, _>>();
        let low = frontiers
            .values()
            .filter_map(|frontier| frontier.watermark)
            .min()
            .unwrap_or(i64::MIN);
        self.graph
            .execute_cycle(&input, low, Some(&frontiers))
            .await
            .unwrap()
    }

    fn late_dropped(&self) -> u64 {
        self.prom.window_late_dropped.get()
    }
}

/// `(window marker in ms, value)` rows of `stream`, in emission order.
fn rows(
    outputs: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    stream: &str,
    marker: &str,
    value: &str,
) -> Vec<(i64, i64)> {
    let Some(batches) = outputs.get(stream) else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for batch in batches {
        let markers = arrow::compute::cast(
            batch.column_by_name(marker).unwrap(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .unwrap();
        let markers = markers
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let values = batch
            .column_by_name(value)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| (markers.value(row), values.value(row))));
    }
    rows
}

const SOURCE: &str = "CREATE SOURCE src (ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts)";

fn total(rows: &[(i64, i64)]) -> i64 {
    rows.iter().map(|(_, n)| n).sum()
}

struct Cascade {
    upstream: Vec<(i64, i64)>,
    downstream: Vec<(i64, i64)>,
    late_dropped: u64,
}

impl Cascade {
    /// Every row the upstream window emitted reaches a closed downstream window.
    #[track_caller]
    fn assert_lossless(&self, upstream_rows: i64) {
        assert_eq!(
            (
                total(&self.upstream),
                total(&self.downstream),
                self.late_dropped
            ),
            (upstream_rows, upstream_rows, 0),
            "upstream {:?}, downstream {:?}",
            self.upstream,
            self.downstream
        );
    }
}

/// Feeds one row every 20 s from `T0` for `steps` cycles with the watermark at the row's time,
/// then one cycle that closes every window.
async fn run_cascade(
    pipeline: &mut Pipeline,
    steps: i64,
    (upstream, upstream_marker): (&str, &str),
    (downstream, downstream_marker): (&str, &str),
) -> Cascade {
    let mut cascade = Cascade {
        upstream: Vec::new(),
        downstream: Vec::new(),
        late_dropped: 0,
    };
    let mut watermarks = (0..steps)
        .map(|index| T0 + index * 20 * SECOND)
        .collect::<Vec<_>>();
    watermarks.push(T0 + 24 * 60 * MINUTE);
    for (index, watermark) in watermarks.into_iter().enumerate() {
        let batches = if i64::try_from(index).unwrap() < steps {
            vec![("src", pipeline.batch("src", &[("ts", &[watermark])]))]
        } else {
            Vec::new()
        };
        let outputs = pipeline.cycle(batches, &[("src", watermark)]).await;
        cascade
            .upstream
            .extend(rows(&outputs, upstream, upstream_marker, "n"));
        cascade
            .downstream
            .extend(rows(&outputs, downstream, downstream_marker, "n"));
    }
    cascade.late_dropped = pipeline.late_dropped();
    cascade
}

const PER_MINUTE_START: &str = "CREATE STREAM per_minute AS \
     SELECT tumble(ts, INTERVAL '1' MINUTE) AS w, COUNT(*) AS n FROM src \
     GROUP BY tumble(ts, INTERVAL '1' MINUTE) EMIT ON WINDOW CLOSE";

const PER_MINUTE_END: &str = "CREATE STREAM per_minute AS \
     SELECT tumble_end(ts, INTERVAL '1' MINUTE) AS w, COUNT(*) AS n FROM src \
     GROUP BY tumble(ts, INTERVAL '1' MINUTE), tumble_end(ts, INTERVAL '1' MINUTE) \
     EMIT ON WINDOW CLOSE";

fn tumble_over_per_minute(size: &str) -> String {
    format!(
        "CREATE STREAM rollup AS \
         SELECT tumble(w, INTERVAL {size}) AS r, SUM(n) AS n FROM per_minute \
         GROUP BY tumble(w, INTERVAL {size}) EMIT ON WINDOW CLOSE"
    )
}

#[tokio::test]
async fn smaller_window_over_window_start_keeps_every_row() {
    let downstream = tumble_over_per_minute("'30' SECOND");
    let mut pipeline = Pipeline::new(&[SOURCE, PER_MINUTE_START, &downstream]).await;
    run_cascade(&mut pipeline, 12, ("per_minute", "w"), ("rollup", "r"))
        .await
        .assert_lossless(12);
}

#[tokio::test]
async fn unaligned_window_over_window_start_keeps_every_row() {
    let downstream = tumble_over_per_minute("'90' SECOND");
    let mut pipeline = Pipeline::new(&[SOURCE, PER_MINUTE_START, &downstream]).await;
    run_cascade(&mut pipeline, 18, ("per_minute", "w"), ("rollup", "r"))
        .await
        .assert_lossless(18);
}

#[tokio::test]
async fn aligned_coarser_window_over_window_start_keeps_every_row() {
    let downstream = tumble_over_per_minute("'2' MINUTE");
    let mut pipeline = Pipeline::new(&[SOURCE, PER_MINUTE_START, &downstream]).await;
    run_cascade(&mut pipeline, 18, ("per_minute", "w"), ("rollup", "r"))
        .await
        .assert_lossless(18);
}

#[tokio::test]
async fn smaller_window_over_window_end_keeps_every_row() {
    let downstream = tumble_over_per_minute("'30' SECOND");
    let mut pipeline = Pipeline::new(&[SOURCE, PER_MINUTE_END, &downstream]).await;
    run_cascade(&mut pipeline, 12, ("per_minute", "w"), ("rollup", "r"))
        .await
        .assert_lossless(12);
}

#[tokio::test]
async fn window_over_hop_start_keeps_every_row() {
    let downstream = tumble_over_per_minute("'1' MINUTE");
    let mut pipeline = Pipeline::new(&[
        SOURCE,
        "CREATE STREAM per_minute AS \
         SELECT hop(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) AS w, COUNT(*) AS n FROM src \
         GROUP BY hop(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) EMIT ON WINDOW CLOSE",
        &downstream,
    ])
    .await;
    run_cascade(&mut pipeline, 12, ("per_minute", "w"), ("rollup", "r"))
        .await
        .assert_lossless(24);
}

#[tokio::test]
async fn window_over_hop_end_keeps_every_row() {
    let downstream = tumble_over_per_minute("'30' SECOND");
    let mut pipeline = Pipeline::new(&[
        SOURCE,
        "CREATE STREAM per_minute AS \
         SELECT hop_end(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) AS w, COUNT(*) AS n \
         FROM src GROUP BY hop(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE), \
         hop_end(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) EMIT ON WINDOW CLOSE",
        &downstream,
    ])
    .await;
    run_cascade(&mut pipeline, 12, ("per_minute", "w"), ("rollup", "r"))
        .await
        .assert_lossless(24);
}

#[tokio::test]
async fn window_over_late_window_end_keeps_every_row() {
    let downstream = tumble_over_per_minute("'20' SECOND");
    let upstream = format!("{PER_MINUTE_END} ALLOW LATENESS INTERVAL '30' SECOND");
    let mut pipeline = Pipeline::new(&[SOURCE, &upstream, &downstream]).await;
    run_cascade(&mut pipeline, 12, ("per_minute", "w"), ("rollup", "r"))
        .await
        .assert_lossless(12);
}

/// The upstream's frontier is held only for the boundary it carries: its end when it projects both,
/// so a window on its start could close before the upstream emits into it.
#[tokio::test]
async fn window_over_the_other_boundary_of_a_window_is_rejected() {
    let (db, _) = database(&[
        SOURCE,
        "CREATE STREAM per_minute AS \
         SELECT tumble(ts, INTERVAL '1' MINUTE) AS ws, tumble_end(ts, INTERVAL '1' MINUTE) AS we, \
         COUNT(*) AS n FROM src \
         GROUP BY tumble(ts, INTERVAL '1' MINUTE), tumble_end(ts, INTERVAL '1' MINUTE) \
         EMIT ON WINDOW CLOSE",
    ])
    .await;
    let error = db
        .execute(
            "CREATE STREAM rollup AS SELECT tumble(ws, INTERVAL '30' SECOND) AS r, SUM(n) AS n \
             FROM per_minute GROUP BY tumble(ws, INTERVAL '30' SECOND) EMIT ON WINDOW CLOSE",
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(
            "window 'rollup' over window stream 'per_minute' must window on 'we', the event-time \
             column 'per_minute' carries, not 'ws'"
        ),
        "{error}"
    );
    db.execute(
        "CREATE STREAM rollup AS SELECT tumble(we, INTERVAL '30' SECOND) AS r, SUM(n) AS n \
         FROM per_minute GROUP BY tumble(we, INTERVAL '30' SECOND) EMIT ON WINDOW CLOSE",
    )
    .await
    .unwrap();
}

/// A session's start trails its close without bound, so its output carries no event time.
#[tokio::test]
async fn window_over_a_session_is_rejected() {
    let (db, _) = database(&[
        SOURCE,
        "CREATE STREAM sessions AS SELECT session(ts, INTERVAL '10' SECOND) AS s, \
         COUNT(*) AS n FROM src GROUP BY session(ts, INTERVAL '10' SECOND) EMIT ON WINDOW CLOSE",
    ])
    .await;
    let error = db
        .execute(
            "CREATE STREAM rollup AS SELECT tumble(s, INTERVAL '1' MINUTE) AS r, SUM(n) AS n \
             FROM sessions GROUP BY tumble(s, INTERVAL '1' MINUTE) EMIT ON WINDOW CLOSE",
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("window 'rollup' cannot window over 'sessions'"),
        "{error}"
    );
}

/// One frontier step closes the upstream window and the downstream window its row belongs to;
/// the downstream applies the row before it closes.
#[tokio::test]
async fn window_over_window_applies_rows_before_one_coarse_close() {
    let downstream = tumble_over_per_minute("'30' SECOND");
    let mut pipeline = Pipeline::new(&[SOURCE, PER_MINUTE_START, &downstream]).await;
    let batch = pipeline.batch("src", &[("ts", &[T0 + 10 * SECOND, T0 + 20 * SECOND])]);
    let outputs = pipeline
        .cycle(vec![("src", batch)], &[("src", T0 + 20 * SECOND)])
        .await;
    assert!(rows(&outputs, "rollup", "r", "n").is_empty());
    let outputs = pipeline
        .cycle(Vec::new(), &[("src", T0 + 5 * MINUTE)])
        .await;
    assert_eq!(rows(&outputs, "per_minute", "w", "n"), vec![(T0, 2)]);
    assert_eq!(rows(&outputs, "rollup", "r", "n"), vec![(T0, 2)]);
    assert_eq!(pipeline.late_dropped(), 0);
}

const JOIN_SOURCES: [&str; 2] = [
    "CREATE SOURCE l (k BIGINT NOT NULL, ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts)",
    "CREATE SOURCE r (k BIGINT NOT NULL, ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts)",
];

const JOINED: &str = "CREATE STREAM joined AS \
     SELECT l.k AS k, l.ts AS lts, r.ts AS rts FROM l JOIN r \
     ON l.k = r.k AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND";

async fn window_over_join(column: &str) -> Pipeline {
    let window = format!(
        "CREATE STREAM per_window AS \
         SELECT tumble({column}, INTERVAL '5' SECOND) AS w, COUNT(*) AS n FROM joined \
         GROUP BY tumble({column}, INTERVAL '5' SECOND) EMIT ON WINDOW CLOSE"
    );
    Pipeline::new(&[JOIN_SOURCES[0], JOIN_SOURCES[1], JOINED, &window]).await
}

/// Key `i` has a left row at `T0 + 2s·i` and a right row 8 s later. One side's rows all arrive in
/// the first cycle with its frontier far ahead; the other side's arrive six keys per cycle with
/// the frontier at the batch's latest row, so the first match of each cycle is below the join
/// frontier that the same cycle emits.
async fn run_uneven_join(pipeline: &mut Pipeline, stored: &str, trickled: &str) -> (i64, u64) {
    const KEYS: i64 = 24;
    const BATCH_KEYS: i64 = 6;
    let side_ts =
        |side: &str, key: i64| T0 + 2 * SECOND * key + if side == "r" { 8 * SECOND } else { 0 };
    let far = T0 + 10 * MINUTE;
    let keys = (0..KEYS).collect::<Vec<_>>();
    let stored_ts = keys
        .iter()
        .map(|&key| side_ts(stored, key))
        .collect::<Vec<_>>();
    let mut cycles = vec![(
        vec![(
            stored,
            pipeline.batch(stored, &[("k", &keys), ("ts", &stored_ts)]),
        )],
        [(stored, far), (trickled, T0 - SECOND)],
    )];
    for chunk in keys.chunks(usize::try_from(BATCH_KEYS).unwrap()) {
        let ts = chunk
            .iter()
            .map(|&key| side_ts(trickled, key))
            .collect::<Vec<_>>();
        let batch = pipeline.batch(trickled, &[("k", chunk), ("ts", &ts)]);
        cycles.push((
            vec![(trickled, batch)],
            [(stored, far), (trickled, *ts.last().unwrap())],
        ));
    }
    cycles.push((Vec::new(), [(stored, far), (trickled, far)]));
    let mut emitted = 0;
    for (batches, frontiers) in cycles {
        let outputs = pipeline.cycle(batches, &frontiers).await;
        emitted += total(&rows(&outputs, "per_window", "w", "n"));
    }
    (emitted, pipeline.late_dropped())
}

#[tokio::test]
async fn window_over_interval_join_left_time_keeps_every_row() {
    for (stored, trickled) in [("l", "r"), ("r", "l")] {
        let mut pipeline = window_over_join("lts").await;
        assert_eq!(
            run_uneven_join(&mut pipeline, stored, trickled).await,
            (24, 0),
            "{stored} stored first"
        );
    }
}

#[tokio::test]
async fn window_over_interval_join_right_time_keeps_every_row() {
    for (stored, trickled) in [("l", "r"), ("r", "l")] {
        let mut pipeline = window_over_join("rts").await;
        assert_eq!(
            run_uneven_join(&mut pipeline, stored, trickled).await,
            (24, 0),
            "{stored} stored first"
        );
    }
}

/// A window emitted only after its lateness reaches an append view whose retention is shorter than
/// that lateness: the view follows the window's held frontier, so the rows are not trimmed on
/// arrival.
#[tokio::test]
async fn late_window_view_keeps_rows_emitted_after_its_retention() {
    let (db, prom) = database(&[
        SOURCE,
        "CREATE MATERIALIZED VIEW per_ten AS \
         SELECT tumble(ts, INTERVAL '10' SECOND) AS w, COUNT(*) AS n FROM src \
         GROUP BY tumble(ts, INTERVAL '10' SECOND) \
         EMIT ON WINDOW CLOSE ALLOW LATENESS INTERVAL '1' MINUTE \
         WITH (retention = '5 seconds')",
    ])
    .await;
    db.start().await.unwrap();
    let source = db.source_untyped("src").unwrap();
    let times = [T0 + SECOND, T0 + 2 * SECOND, T0 + 70 * SECOND];
    source
        .push_arrow(source_batch(&db, "src", &[("ts", &times)]))
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let counts = loop {
        let batches = db
            .ctx
            .sql("SELECT n FROM per_ten")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let counts = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                column.values().to_vec()
            })
            .collect::<Vec<_>>();
        if !counts.is_empty() || std::time::Instant::now() > deadline {
            break counts;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(counts, vec![2]);
    assert_eq!(prom.window_late_dropped.get(), 0);
}

/// A statement that splits plans its owner with the statement's `ALLOW LATENESS`, so an owner that
/// cannot apply it rejects the statement instead of dropping the clause.
#[tokio::test]
async fn split_statement_rejects_lateness_its_owner_cannot_apply() {
    let (db, _) = database(&[
        "CREATE SOURCE events (k BIGINT NOT NULL, ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts)",
    ])
    .await;
    let per_group = "CREATE STREAM per_group AS SELECT g, SUM(n) AS n FROM \
                     (SELECT k % 4 AS g, COUNT(*) AS n FROM events GROUP BY k % 4) \
                     GROUP BY g EMIT CHANGES";
    let error = db
        .execute(&format!("{per_group} ALLOW LATENESS INTERVAL '30' SECOND"))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("ALLOW LATENESS requires a TUMBLE"),
        "{error}"
    );
    assert!(db.connector_manager.lock().streams().is_empty());
    db.execute(per_group).await.unwrap();
    assert!(db.connector_manager.lock().streams().len() > 1);
}
