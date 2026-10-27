#![allow(clippy::disallowed_types)]
//! Changelog materialized-view integration tests.
//!
//! Validates that a non-windowed `GROUP BY` MV, which always emits its changelog:
//! 1. serves `SELECT * FROM mv` snapshots equal to a full recompute (the upsert store is
//!    maintained from the operator's dirty-only changelog), and
//! 2. survives checkpoint → restart with a correct snapshot, and
//! 3. feeds downstream consumers of its changelog — chained agg/projection MVs, capability-aware
//!    sinks (upsert/changelog-capable), and SUBSCRIBE (its rows, then weighted changes) — while still
//!    rejecting shapes it can't net (e.g. an arbitrary join, a non-capable sink), and
//! 4. serves `SELECT` from the latest cycle, which a restart can roll back.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{ExecuteResult, LaminarConfig, LaminarDB};

fn config(dir: &std::path::Path) -> LaminarConfig {
    LaminarConfig {
        storage_dir: Some(dir.to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: None, // manual checkpoints only
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    }
}

fn non_recoverable_config(dir: &std::path::Path) -> LaminarConfig {
    let mut config = config(dir);
    config.checkpoint = None;
    config
}

fn batch(ks: &[i64], vs: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable(vec![
        ("k", Arc::new(Int64Array::from(ks.to_vec())) as _, true),
        ("v", Arc::new(Int64Array::from(vs.to_vec())) as _, true),
    ])
    .unwrap()
}

/// Read `SELECT * FROM <mv>` and return `(k, total, cnt)` rows sorted by key.
async fn read_mv(db: &LaminarDB, mv: &str) -> Vec<(i64, i64, i64)> {
    let result = db.execute(&format!("SELECT * FROM {mv}")).await.unwrap();
    let ExecuteResult::Query(mut q) = result else {
        panic!("expected Query result");
    };
    let mut sub = q.subscribe_raw().unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(30), sub.recv_async())
        .await
        .expect("materialized-view query did not produce its snapshot within 30 seconds")
        .expect("materialized-view query disconnected before producing its snapshot");
    let mut rows = Vec::new();
    rows.extend(rows_of(&first));
    for _ in 0..1024 {
        match sub.poll() {
            Some(batch) => rows.extend(rows_of(&batch)),
            None => break,
        }
    }
    rows.sort_unstable();
    rows
}

/// Run `sql` and return its rows as `ncols` i64 columns, sorted.
async fn read_query(db: &LaminarDB, sql: &str, ncols: usize) -> Vec<Vec<i64>> {
    let result = db.execute(sql).await.unwrap();
    let ExecuteResult::Query(mut q) = result else {
        panic!("expected Query result");
    };
    let mut sub = q.subscribe_raw().unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(30), sub.recv_async())
        .await
        .expect("query did not produce its snapshot within 30 seconds")
        .expect("query disconnected before producing its snapshot");
    let mut rows = Vec::new();
    append_i64_rows(&first, ncols, &mut rows);
    for _ in 0..4096 {
        match sub.poll() {
            Some(batch) => append_i64_rows(&batch, ncols, &mut rows),
            None => break,
        }
    }
    rows.sort();
    rows
}

fn append_i64_rows(batch: &RecordBatch, ncols: usize, rows: &mut Vec<Vec<i64>>) {
    for row in 0..batch.num_rows() {
        rows.push(
            (0..ncols)
                .map(|column| {
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row)
                })
                .collect(),
        );
    }
}

/// Parse a plain `agg` batch (`k, total, cnt`) into sorted `(k, total, cnt)` rows.
fn rows_of(b: &RecordBatch) -> Vec<(i64, i64, i64)> {
    let col = |i: usize| b.column(i).as_any().downcast_ref::<Int64Array>().unwrap();
    let (ks, totals, cnts) = (col(0), col(1), col(2));
    let mut rows: Vec<(i64, i64, i64)> = (0..b.num_rows())
        .map(|r| (ks.value(r), totals.value(r), cnts.value(r)))
        .collect();
    rows.sort_unstable();
    rows
}

const SRC: &str = "CREATE SOURCE events (k BIGINT, v BIGINT)";
const MV: &str = "CREATE MATERIALIZED VIEW agg AS \
                  SELECT k, SUM(v) AS total, COUNT(*) AS cnt FROM events GROUP BY k";

#[tokio::test]
async fn incremental_emit_snapshot_matches_full_recompute() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.start().await.unwrap();

    let source = db.source_untyped("events").unwrap();
    // Several batches, each touching a subset of keys, over multiple cycles. The unchanged keys
    // must persist in the upsert store even though only dirty groups are emitted each cycle.
    let batches: Vec<(Vec<i64>, Vec<i64>)> = vec![
        (vec![1, 2, 3], vec![10, 20, 30]),
        (vec![2, 4], vec![5, 40]),
        (vec![1], vec![100]),
        (vec![3, 4, 5], vec![1, 2, 3]),
    ];
    let mut truth: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    for (ks, vs) in &batches {
        for (&k, &v) in ks.iter().zip(vs) {
            let e = truth.entry(k).or_insert((0, 0));
            e.0 += v;
            e.1 += 1;
        }
        source.push_arrow(batch(ks, vs)).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let expected: Vec<(i64, i64, i64)> = truth.into_iter().map(|(k, (s, c))| (k, s, c)).collect();
    assert_eq!(read_mv(&db, "agg").await, expected);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn incremental_emit_survives_checkpoint_restart() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();

    let expected;
    {
        let db = LaminarDB::open_with_config(config(&storage)).unwrap();
        db.execute(SRC).await.unwrap();
        db.execute(MV).await.unwrap();
        db.start().await.unwrap();

        let source = db.source_untyped("events").unwrap();
        source.push_arrow(batch(&[1, 2, 3], &[10, 20, 30])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        source.push_arrow(batch(&[2, 4], &[7, 40])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        expected = read_mv(&db, "agg").await;
        assert_eq!(
            expected,
            vec![(1, 10, 1), (2, 27, 2), (3, 30, 1), (4, 40, 1)]
        );

        let cp = db.checkpoint().await.unwrap();
        assert!(cp.success, "checkpoint must succeed");
        db.shutdown().await.unwrap();
    }

    // Reopen from checkpoint — the upsert snapshot recovers from the manifest.
    {
        let db = LaminarDB::open_with_config(config(&storage)).unwrap();
        db.execute(SRC).await.unwrap();
        db.execute(MV).await.unwrap();
        db.start().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        assert_eq!(
            read_mv(&db, "agg").await,
            expected,
            "snapshot must survive restart"
        );
        db.shutdown().await.unwrap();
    }
}

/// A `SELECT` reads the latest completed cycle, not a committed checkpoint. A restart restores
/// the committed cut, so a row a reader already saw can disappear; an in-memory source cannot
/// replay it.
#[tokio::test]
async fn view_read_can_observe_state_that_a_restart_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();
    {
        let db = LaminarDB::open_with_config(config(&storage)).unwrap();
        db.execute(SRC).await.unwrap();
        db.execute(MV).await.unwrap();
        db.start().await.unwrap();
        let source = db.source_untyped("events").unwrap();

        source.push_arrow(batch(&[1], &[10])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert!(db.checkpoint().await.unwrap().success);
        source.push_arrow(batch(&[2], &[20])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(read_mv(&db, "agg").await, vec![(1, 10, 1), (2, 20, 1)]);
        db.shutdown().await.unwrap();
    }

    let db = LaminarDB::open_with_config(config(&storage)).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(read_mv(&db, "agg").await, vec![(1, 10, 1)]);
    db.shutdown().await.unwrap();
}

/// A counted multiset MV must preserve duplicate rows at the durable cut. After restart, changing
/// one upstream group retracts exactly one copy of the duplicate rather than both copies.
#[tokio::test]
async fn counted_multiset_survives_coordinator_checkpoint_restart_and_retraction() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.execute("CREATE MATERIALIZED VIEW totals AS SELECT total FROM agg")
        .await
        .unwrap();
    db.start().await.unwrap();

    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch(&[1, 2], &[10, 10])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        read_query(&db, "SELECT total FROM totals", 1).await,
        vec![vec![10], vec![10]],
        "the key-dropping projection must retain both equal rows"
    );

    let checkpoint = db.checkpoint().await.unwrap();
    assert!(checkpoint.success, "checkpoint must succeed");
    db.stop_pipeline().await.unwrap();
    db.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(
        read_query(&db, "SELECT total FROM totals", 1).await,
        vec![vec![10], vec![10]],
        "restart must restore the duplicate multiplicity from the committed image"
    );

    db.source_untyped("events")
        .unwrap()
        .push_arrow(batch(&[1], &[5]))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        read_query(&db, "SELECT total FROM totals", 1).await,
        vec![vec![10], vec![15]],
        "the post-restart update must retract exactly one copy of total=10"
    );
    assert_eq!(
        read_mv(&db, "agg").await,
        vec![(1, 15, 2), (2, 10, 1)],
        "the aggregate state feeding the retraction must also resume at the durable cut"
    );
    db.shutdown().await.unwrap();
}

/// A `changelog ⋈ static dimension` enrich join maintains a correct snapshot under UPDATES —
/// the dimension enriches each changelog row and the retraction drops the stale row.
#[tokio::test]
async fn chained_dim_enrich_join_is_correct_under_updates() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(non_recoverable_config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    // Static dimension table (keyed reference table).
    db.execute("CREATE TABLE dim (k BIGINT PRIMARY KEY, label BIGINT)")
        .await
        .unwrap();
    db.execute("INSERT INTO dim VALUES (1, 100), (2, 200)")
        .await
        .unwrap();
    // Enrich join: changelog (agg) ⋈ static dim.
    db.execute(
        "CREATE MATERIALIZED VIEW enriched AS \
         SELECT agg.k, agg.total, dim.label FROM agg JOIN dim ON agg.k = dim.k",
    )
    .await
    .unwrap();
    db.start().await.unwrap();

    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    source.push_arrow(batch(&[1], &[5])).unwrap(); // k=1 total: 10 -> 15
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Enriched snapshot tracks the update (no stale (1,10,100)) and carries the dim column.
    assert_eq!(
        read_query(&db, "SELECT k, total, label FROM enriched", 3).await,
        vec![vec![1, 15, 100], vec![2, 20, 200]]
    );
    db.shutdown().await.unwrap();
}

/// A certified changelog-to-static LEFT join preserves unmatched rows and retracts their
/// previous values when the upstream aggregate changes.
#[tokio::test]
async fn state_backed_left_dim_enrich_preserves_unmatched_rows_under_updates() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(non_recoverable_config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.execute("CREATE TABLE dim (k BIGINT PRIMARY KEY, label BIGINT)")
        .await
        .unwrap();
    db.execute("INSERT INTO dim VALUES (1, 100)").await.unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW enriched_left AS \
         SELECT agg.k, agg.total, dim.label FROM agg LEFT JOIN dim ON agg.k = dim.k",
    )
    .await
    .expect("the exact changelog-to-static LEFT join must be admitted");
    db.start().await.unwrap();

    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    source.push_arrow(batch(&[1, 2], &[5, 7])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert_eq!(
        read_query(
            &db,
            "SELECT k, total, COALESCE(label, -1) FROM enriched_left",
            3,
        )
        .await,
        vec![vec![1, 15, 100], vec![2, 27, -1]],
        "the unmatched key must remain NULL-padded and both keys must retract stale totals"
    );
    db.shutdown().await.unwrap();
}

/// A chained *aggregate* over an incremental MV nets the retraction changelog correctly under
/// UPDATES (the value-correctness gate). `SUM(total)` over `{k1:10→15, k2:20}` = 35.
#[tokio::test]
async fn chained_aggregate_over_incremental_nets_under_updates() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    // Global roll-up and a keyed roll-up, both over the incremental MV's changelog.
    db.execute("CREATE MATERIALIZED VIEW total_sum AS SELECT SUM(total) AS s FROM agg")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW per_key AS SELECT k, SUM(total) AS s FROM agg GROUP BY k")
        .await
        .unwrap();
    db.start().await.unwrap();

    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    source.push_arrow(batch(&[1], &[5])).unwrap(); // k=1 total: 10 -> 15 (an UPDATE)
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    // agg snapshot is correct (k=1 saw two events: cnt=2, sum=15), and the roll-ups net the
    // retraction (no double-count).
    assert_eq!(read_mv(&db, "agg").await, vec![(1, 15, 2), (2, 20, 1)]);
    assert_eq!(
        read_query(&db, "SELECT s FROM total_sum", 1).await,
        vec![vec![35]]
    );
    assert_eq!(
        read_query(&db, "SELECT k, s FROM per_key", 2).await,
        vec![vec![1, 15], vec![2, 20]]
    );
    db.shutdown().await.unwrap();
}

/// A chained projection/filter over an incremental MV maintains a correct snapshot under
/// UPDATES — the retraction drops the stale row (no accumulation).
#[tokio::test]
async fn chained_projection_over_incremental_is_correct_under_updates() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.execute("CREATE MATERIALIZED VIEW proj AS SELECT k, total FROM agg")
        .await
        .unwrap();
    // A filter over the changelog too: only keys whose running total exceeds 12.
    db.execute("CREATE MATERIALIZED VIEW big AS SELECT k, total FROM agg WHERE total > 12")
        .await
        .unwrap();
    db.start().await.unwrap();

    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    source.push_arrow(batch(&[1], &[5])).unwrap(); // k=1 total: 10 -> 15
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    // Projection snapshot tracks the update with NO stale (1,10) row.
    assert_eq!(
        read_query(&db, "SELECT k, total FROM proj", 2).await,
        vec![vec![1, 15], vec![2, 20]]
    );
    // Filter: k=1 crossed 12 (10->15), so it now appears alongside k=2.
    assert_eq!(
        read_query(&db, "SELECT k, total FROM big", 2).await,
        vec![vec![1, 15], vec![2, 20]]
    );
    db.shutdown().await.unwrap();
}

/// Chained aggregates and projections/filters can consume an incremental MV's changelog.
/// Joins are rejected during DDL; sink capabilities are checked before connector activation.
#[tokio::test]
async fn terminality_guard_allows_agg_and_projection_rejects_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();

    // Aggregate and projection/filter chained reads are allowed.
    db.execute("CREATE MATERIALIZED VIEW rollup AS SELECT k, SUM(total) AS s FROM agg GROUP BY k")
        .await
        .expect("chained aggregate is allowed");
    db.execute("CREATE MATERIALIZED VIEW projm AS SELECT k, total FROM agg")
        .await
        .expect("chained projection is allowed (Phase 2)");
    db.execute("CREATE STREAM projs AS SELECT k, total FROM agg")
        .await
        .expect("chained projection stream is allowed (Phase 2)");

    // Stateful joins over a changelog are outside the supported enrichment contract.
    let join = db
        .execute(
            "CREATE MATERIALIZED VIEW j AS SELECT agg.k FROM agg JOIN events ON agg.k = events.k",
        )
        .await;
    assert!(join.is_err(), "join over incremental MV must be rejected");

    // Snapshot reads still work.
    db.start().await.unwrap();
    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(read_mv(&db, "agg").await, vec![(1, 10, 1), (2, 20, 1)]);
    db.shutdown().await.unwrap();
}

/// Files sinks cannot consume incremental retractions and are rejected before preparation.
#[cfg(feature = "files")]
#[tokio::test]
async fn sink_from_incremental_mv_rejects_noncapable_sink_at_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    let error = db
        .execute(&format!(
            "CREATE SINK f FROM agg INTO FILES (path = '{}') FORMAT JSON",
            out.display().to_string().replace('\\', "/")
        ))
        .await
        .expect_err("an append-only sink cannot consume incremental retractions");
    assert!(
        matches!(error, laminar_db::DbError::Config(ref detail) if detail.contains("FullChangelog")),
        "{error}"
    );
    assert!(db.sinks().is_empty());
    assert_eq!(std::fs::read_dir(&out).unwrap().count(), 0);
    db.start().await.unwrap();
    db.shutdown().await.unwrap();
}

/// Startup follows retractions through a stream and requires full changelog sink support.
#[cfg(feature = "files")]
#[tokio::test]
async fn sink_over_changelog_stream_rejects_noncapable_sink_at_start() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.execute("CREATE STREAM s AS SELECT k, total FROM agg")
        .await
        .expect("stream over incremental MV");
    db.execute(&format!(
        "CREATE SINK f FROM s INTO FILES (path = '{}') FORMAT JSON",
        out.display().to_string().replace('\\', "/")
    ))
    .await
    .expect("CREATE SINK over the stream succeeds at DDL");
    assert_eq!(std::fs::read_dir(&out).unwrap().count(), 0);
    let error = db
        .start()
        .await
        .expect_err("an append-only sink cannot consume stream retractions");
    let error = format!("{error:?}");
    assert!(error.contains("LDB-1300"), "{error}");
    assert_eq!(std::fs::read_dir(&out).unwrap().count(), 0);
    db.shutdown().await.ok();
}

/// Stateful joins over changelog-producing materialized views are not part of the single bounded
/// interval-join contract and must fail before catalog registration.
#[tokio::test]
async fn changelog_joins_are_rejected_at_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute("CREATE SOURCE ev_a (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE ev_b (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS total FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS total FROM ev_b GROUP BY k")
        .await
        .unwrap();

    for (name, join) in [("joined_inner", "JOIN"), ("joined_left", "LEFT JOIN")] {
        let ddl = format!(
            "CREATE MATERIALIZED VIEW {name} AS \\
             SELECT a.k FROM agg_a a {join} agg_b b ON a.k = b.k"
        );
        assert!(
            db.execute(&ddl).await.is_err(),
            "{join} changelog join was admitted"
        );
        assert!(db.execute(&format!("SELECT * FROM {name}")).await.is_err());
    }
}

/// SUBSCRIBE to an incremental MV delivers the view's rows at attach, then each cycle's change:
/// an update retracts the key's old row and inserts its new one, and untouched keys are not
/// repeated.
#[tokio::test]
async fn subscribe_to_incremental_mv_delivers_changes() {
    use laminar_db::subscription::{PortalFrame, SubscribeStart};
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.start().await.unwrap();
    let source = db.source_untyped("events").unwrap();

    let mut portal = db
        .open_subscription("agg", None, SubscribeStart::Tail)
        .await
        .unwrap();
    assert!(portal.schema().index_of("__weight").is_ok());

    let mut next_change = async || loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), portal.next_frame())
            .await
            .expect("a frame within the deadline")
            .expect("a portal frame");
        match frame {
            PortalFrame::Batch { batch, .. } => {
                let weights = batch
                    .column_by_name("__weight")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();
                let mut rows: Vec<_> = rows_of(&batch)
                    .into_iter()
                    .zip(weights.values().iter().copied())
                    .collect();
                rows.sort_unstable();
                return rows;
            }
            PortalFrame::Barrier { .. } => {}
            other => panic!("unexpected frame {other:?}"),
        }
    };

    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    let mut inserted = next_change().await;
    inserted.sort_unstable();
    assert_eq!(inserted, vec![((1, 10, 1), 1), ((2, 20, 1), 1)]);

    source.push_arrow(batch(&[1], &[5])).unwrap();
    assert_eq!(next_change().await, vec![((1, 10, 1), -1), ((1, 15, 2), 1)]);
    portal.close();
    db.shutdown().await.unwrap();
}

/// Stop the pipeline after a checkpoint, start it again from the committed cut, and return
/// `sql`'s rows.
async fn restart_and_read(db: &Arc<LaminarDB>, sql: &str, ncols: usize) -> Vec<Vec<i64>> {
    assert!(
        db.checkpoint().await.unwrap().success,
        "checkpoint must succeed"
    );
    db.stop_pipeline().await.unwrap();
    db.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    read_query(db, sql, ncols).await
}

/// A global aggregate view emits its changelog, keeps one row on vnode 0, and restores it from
/// the committed vnode frame.
#[tokio::test]
async fn global_view_survives_checkpoint_restart_and_keeps_updating() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW overall AS \
         SELECT SUM(v) AS total, COUNT(*) AS n, MAX(v) AS peak FROM events",
    )
    .await
    .unwrap();
    db.start().await.unwrap();
    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    source.push_arrow(batch(&[3], &[5])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let sql = "SELECT total, n, peak FROM overall";
    assert_eq!(read_query(&db, sql, 3).await, vec![vec![35, 3, 20]]);

    assert_eq!(restart_and_read(&db, sql, 3).await, vec![vec![35, 3, 20]]);
    db.source_untyped("events")
        .unwrap()
        .push_arrow(batch(&[1], &[1]))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(read_query(&db, sql, 3).await, vec![vec![36, 4, 20]]);
    db.shutdown().await.unwrap();
}

/// A filter over a keyed view keeps the key, so its rows are placed by that key. They survive a
/// restart, and an update that leaves the filter retracts the row.
#[tokio::test]
async fn keyed_filter_view_survives_restart_and_retracts_rows_leaving_the_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.execute("CREATE MATERIALIZED VIEW big AS SELECT k, total FROM agg WHERE total >= 10")
        .await
        .unwrap();
    db.start().await.unwrap();
    db.source_untyped("events")
        .unwrap()
        .push_arrow(batch(&[1, 2, 3], &[10, 5, 30]))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let sql = "SELECT k, total FROM big";
    assert_eq!(
        read_query(&db, sql, 2).await,
        vec![vec![1, 10], vec![3, 30]]
    );

    assert_eq!(
        restart_and_read(&db, sql, 2).await,
        vec![vec![1, 10], vec![3, 30]]
    );
    db.source_untyped("events")
        .unwrap()
        .push_arrow(batch(&[2, 3], &[5, -25]))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        read_query(&db, sql, 2).await,
        vec![vec![1, 10], vec![2, 10]]
    );
    db.shutdown().await.unwrap();
}

/// A running pipeline without checkpoints drops a changelog view with its result node and admits
/// a new view under the same name, which starts empty.
#[tokio::test]
async fn a_dropped_changelog_view_can_be_created_again_while_running() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(non_recoverable_config(dir.path())).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.start().await.unwrap();
    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(read_mv(&db, "agg").await, vec![(1, 10, 1), (2, 20, 1)]);

    db.execute("DROP MATERIALIZED VIEW agg").await.unwrap();
    db.execute(MV).await.unwrap();
    source.push_arrow(batch(&[3], &[30])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(read_mv(&db, "agg").await, vec![(3, 30, 1)]);
    db.shutdown().await.unwrap();
}

/// View slices take the configured vnode count, and restore checks frames against it.
#[tokio::test]
async fn views_restore_under_a_non_default_vnode_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::builder()
        .storage_dir(dir.path())
        .checkpoint(StreamCheckpointConfig {
            interval_ms: None,
            ..StreamCheckpointConfig::default()
        })
        .key_groups(laminar_core::state::KeyGroupCount::try_from(16u16).unwrap())
        .build()
        .await
        .unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.start().await.unwrap();
    db.source_untyped("events")
        .unwrap()
        .push_arrow(batch(&[1, 2, 3, 1], &[10, 20, 30, 5]))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let sql = "SELECT k, total, cnt FROM agg";
    let expected = vec![vec![1, 15, 2], vec![2, 20, 1], vec![3, 30, 1]];
    assert_eq!(read_query(&db, sql, 3).await, expected);
    assert_eq!(restart_and_read(&db, sql, 3).await, expected);
    db.shutdown().await.unwrap();
}
