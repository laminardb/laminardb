#![allow(clippy::disallowed_types)]
//! Incremental running-state aggregate integration tests.
//!
//! Validates that a terminal non-windowed `GROUP BY` MV under `incremental_emit`:
//! 1. serves `SELECT * FROM mv` snapshots equal to a full recompute (the upsert store is
//!    maintained from the operator's dirty-only changelog), and
//! 2. survives checkpoint → restart with a correct snapshot, and
//! 3. feeds downstream consumers of its changelog — chained agg/projection MVs, capability-aware
//!    sinks (upsert/changelog-capable), and SUBSCRIBE (plain consolidated snapshots) — while still
//!    rejecting shapes it can't net (e.g. an arbitrary join, a non-capable sink).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{ExecuteResult, LaminarConfig, LaminarDB};

fn config(dir: &std::path::Path, incremental: bool) -> LaminarConfig {
    LaminarConfig {
        storage_dir: Some(dir.to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: None, // manual checkpoints only
            incremental_emit: incremental,
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    }
}

fn batch(ks: &[i64], vs: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("k", Arc::new(Int64Array::from(ks.to_vec())) as _),
        ("v", Arc::new(Int64Array::from(vs.to_vec())) as _),
    ])
    .unwrap()
}

/// Read `SELECT * FROM <mv>` and return `(k, total, cnt)` rows sorted by key.
async fn read_mv(db: &LaminarDB, mv: &str) -> Vec<(i64, i64, i64)> {
    let result = db.execute(&format!("SELECT * FROM {mv}")).await.unwrap();
    let ExecuteResult::Query(mut q) = result else {
        panic!("expected Query result");
    };
    tokio::task::yield_now().await;
    let mut sub = q.subscribe_raw().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut rows = Vec::new();
    for _ in 0..1024 {
        match sub.poll() {
            Some(b) => {
                let col = |i: usize| {
                    b.column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .clone()
                };
                let (ks, totals, cnts) = (col(0), col(1), col(2));
                for r in 0..b.num_rows() {
                    rows.push((ks.value(r), totals.value(r), cnts.value(r)));
                }
            }
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
    tokio::task::yield_now().await;
    let mut sub = q.subscribe_raw().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut rows = Vec::new();
    for _ in 0..4096 {
        match sub.poll() {
            Some(b) => {
                for r in 0..b.num_rows() {
                    let row: Vec<i64> = (0..ncols)
                        .map(|c| {
                            b.column(c)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(r)
                        })
                        .collect();
                    rows.push(row);
                }
            }
            None => break,
        }
    }
    rows.sort();
    rows
}

/// Parse a plain `agg` batch (`k, total, cnt`) into sorted `(k, total, cnt)` rows.
fn rows_of(b: &RecordBatch) -> Vec<(i64, i64, i64)> {
    let col = |i: usize| {
        b.column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone()
    };
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
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
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
        let db = LaminarDB::open_with_config(config(&storage, true)).unwrap();
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
        db.close();
    }

    // Reopen from checkpoint — the upsert snapshot recovers from the manifest.
    {
        let db = LaminarDB::open_with_config(config(&storage, true)).unwrap();
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

/// A `changelog ⋈ static dimension` enrich join maintains a correct snapshot under UPDATES —
/// the dimension enriches each changelog row and the retraction drops the stale row.
#[tokio::test]
async fn chained_dim_enrich_join_is_correct_under_updates() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
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

/// A chained *aggregate* over an incremental MV nets the retraction changelog correctly under
/// UPDATES (the value-correctness gate). `SUM(total)` over `{k1:10→15, k2:20}` = 35.
#[tokio::test]
async fn chained_aggregate_over_incremental_nets_under_updates() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
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
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
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

/// Guard: chained aggregates AND simple projections/filters over an incremental MV are allowed
/// (they net the changelog); a complex shape (join) is rejected. Sinks are no longer rejected at
/// DDL — capability is enforced at pipeline start (see `sink_from_incremental_mv_*`).
#[tokio::test]
async fn terminality_guard_allows_agg_and_projection_rejects_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
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

    // A join over the changelog (complex shape) is rejected — not yet supported.
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

/// A sink from an incremental MV is no longer rejected at DDL, but a connector that can neither
/// upsert nor handle changelog records (the `files` sink) is rejected at pipeline start with
/// `[LDB-1300]` — never silently dropping the changelog's retractions.
#[cfg(feature = "files")]
#[tokio::test]
async fn sink_from_incremental_mv_rejects_noncapable_sink_at_start() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    // Guard relaxed: the sink DDL itself now succeeds over an incremental MV.
    db.execute(&format!(
        "CREATE SINK f FROM agg INTO FILES (path = '{}', format = 'json')",
        out.display().to_string().replace('\\', "/")
    ))
    .await
    .expect("CREATE SINK over an incremental MV now succeeds at DDL");
    // Capability is enforced at start: the files sink can't consume a changelog.
    let started = db.start().await;
    let err = format!(
        "{:?}",
        started.expect_err("a non-capable sink over an incremental MV must be rejected at start")
    );
    assert!(err.contains("LDB-1300"), "expected LDB-1300, got: {err}");
    db.shutdown().await.ok();
}

/// The capability check fires ONLY for incremental MVs: a full-emit (non-incremental) aggregate
/// still feeds a plain append-only files sink, so the check does not over-reject.
#[cfg(feature = "files")]
#[tokio::test]
async fn sink_from_nonincremental_mv_allows_noncapable_sink() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), false)).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.execute(&format!(
        "CREATE SINK f FROM agg INTO FILES (path = '{}', format = 'json')",
        out.display().to_string().replace('\\', "/")
    ))
    .await
    .expect("CREATE SINK");
    db.start()
        .await
        .expect("a full-emit aggregate feeding a plain sink must start fine");
    db.shutdown().await.ok();
}

/// An inner `changelog ⋈ changelog` IVM join over TWO incremental MVs maintains a correct snapshot
/// under UPDATES on BOTH sides — the δA⋈B + A⋈δB netting drops stale joined rows.
#[tokio::test]
async fn incremental_join_over_two_changelogs_nets_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    db.execute("CREATE SOURCE ev_a (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE ev_b (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS total FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS wtotal FROM ev_b GROUP BY k")
        .await
        .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW joined AS \
         SELECT a.k, a.total, b.wtotal FROM agg_a a JOIN agg_b b ON a.k = b.k",
    )
    .await
    .expect("inner changelog ⋈ changelog join is allowed (Stage 3b)");
    db.start().await.unwrap();

    let sa = db.source_untyped("ev_a").unwrap();
    let sb = db.source_untyped("ev_b").unwrap();
    // Seed both sides for k=1,2.
    sa.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    sb.push_arrow(batch(&[1, 2], &[100, 200])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    // Update LEFT k=1 (total 10->15) and RIGHT k=2 (wtotal 200->250).
    sa.push_arrow(batch(&[1], &[5])).unwrap();
    sb.push_arrow(batch(&[2], &[50])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // No stale (1,10,100) or (2,20,200); both updates tracked.
    assert_eq!(
        read_query(&db, "SELECT k, total, wtotal FROM joined", 3).await,
        vec![vec![1, 15, 100], vec![2, 20, 250]]
    );
    db.shutdown().await.unwrap();
}

/// An incremental join survives checkpoint → restart: the post-restart UPDATE only nets correctly
/// if the operator's per-side Z-set state was restored (else the stale joined row persists).
#[tokio::test]
async fn incremental_join_survives_checkpoint_restart() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();

    const DDL: [&str; 5] = [
        "CREATE SOURCE ev_a (k BIGINT, v BIGINT)",
        "CREATE SOURCE ev_b (k BIGINT, v BIGINT)",
        "CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS total FROM ev_a GROUP BY k",
        "CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS wtotal FROM ev_b GROUP BY k",
        "CREATE MATERIALIZED VIEW joined AS \
         SELECT a.k, a.total, b.wtotal FROM agg_a a JOIN agg_b b ON a.k = b.k",
    ];

    {
        let db = LaminarDB::open_with_config(config(&storage, true)).unwrap();
        for stmt in DDL {
            db.execute(stmt).await.unwrap();
        }
        db.start().await.unwrap();
        let sa = db.source_untyped("ev_a").unwrap();
        let sb = db.source_untyped("ev_b").unwrap();
        sa.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
        sb.push_arrow(batch(&[1, 2], &[100, 200])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            read_query(&db, "SELECT k, total, wtotal FROM joined", 3).await,
            vec![vec![1, 10, 100], vec![2, 20, 200]]
        );
        let cp = db.checkpoint().await.unwrap();
        assert!(cp.success, "checkpoint must succeed");
        db.close();
    }

    {
        let db = LaminarDB::open_with_config(config(&storage, true)).unwrap();
        for stmt in DDL {
            db.execute(stmt).await.unwrap();
        }
        db.start().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        // Snapshot restored.
        assert_eq!(
            read_query(&db, "SELECT k, total, wtotal FROM joined", 3).await,
            vec![vec![1, 10, 100], vec![2, 20, 200]],
            "join MV snapshot must survive restart"
        );
        // UPDATE left k=1 (total 10->15). Netting the retraction of the stale (1,10,100) requires
        // the restored right-side state (k=1: wtotal=100) to still match.
        let sa = db.source_untyped("ev_a").unwrap();
        sa.push_arrow(batch(&[1], &[5])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert_eq!(
            read_query(&db, "SELECT k, total, wtotal FROM joined", 3).await,
            vec![vec![1, 15, 100], vec![2, 20, 200]],
            "post-restart update must net (no stale (1,10,100)) — proves side-state was restored"
        );
        db.shutdown().await.unwrap();
    }
}

/// A LEFT outer `changelog ⋈ changelog` join NULL-pads unmatched left rows and tracks the
/// pad↔inner transition as right matches come and go.
#[tokio::test]
async fn left_outer_incremental_join_nullpads_unmatched_left() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    db.execute("CREATE SOURCE ev_a (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE ev_b (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS total FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS wtotal FROM ev_b GROUP BY k")
        .await
        .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW joined AS \
         SELECT a.k, a.total, b.wtotal FROM agg_a a LEFT JOIN agg_b b ON a.k = b.k",
    )
    .await
    .expect("inner+LEFT changelog join allowed (Stage 3b Slice 2)");
    db.start().await.unwrap();

    let sa = db.source_untyped("ev_a").unwrap();
    let sb = db.source_untyped("ev_b").unwrap();
    // Left has k=1,2; right matches only k=1. k=2 must show NULL wtotal.
    sa.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    sb.push_arrow(batch(&[1], &[100])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    // wtotal is nullable; COALESCE the NULL to -1 to read it as an i64 column.
    assert_eq!(
        read_query(&db, "SELECT k, total, COALESCE(wtotal, -1) FROM joined", 3).await,
        vec![vec![1, 10, 100], vec![2, 20, -1]]
    );

    // Right k=2 arrives → its NULL-pad retracts, the inner row appears.
    sb.push_arrow(batch(&[2], &[200])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(
        read_query(&db, "SELECT k, total, COALESCE(wtotal, -1) FROM joined", 3).await,
        vec![vec![1, 10, 100], vec![2, 20, 200]]
    );
    db.shutdown().await.unwrap();
}

/// Guard: inner and LEFT `changelog ⋈ changelog` joins are allowed; a RIGHT/outer join and a
/// `changelog ⋈ source` join stay rejected.
#[tokio::test]
async fn incremental_join_guard_allows_inner_left_rejects_right_and_source() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    db.execute("CREATE SOURCE ev_a (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE ev_b (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS total FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS wtotal FROM ev_b GROUP BY k")
        .await
        .unwrap();

    db.execute(
        "CREATE MATERIALIZED VIEW j AS \
         SELECT a.k, a.total, b.wtotal FROM agg_a a JOIN agg_b b ON a.k = b.k",
    )
    .await
    .expect("inner changelog join allowed");
    db.execute(
        "CREATE MATERIALIZED VIEW jl AS \
         SELECT a.k, a.total, b.wtotal FROM agg_a a LEFT JOIN agg_b b ON a.k = b.k",
    )
    .await
    .expect("LEFT changelog join allowed (Slice 2)");

    let right = db
        .execute(
            "CREATE MATERIALIZED VIEW jr AS \
             SELECT a.k FROM agg_a a RIGHT JOIN agg_b b ON a.k = b.k",
        )
        .await;
    assert!(
        right.is_err(),
        "RIGHT join over changelogs is a later slice"
    );

    let src = db
        .execute(
            "CREATE MATERIALIZED VIEW js AS \
             SELECT agg_a.k FROM agg_a JOIN ev_a ON agg_a.k = ev_a.k",
        )
        .await;
    assert!(src.is_err(), "changelog ⋈ source join is rejected");
}

/// Multi-way A⋈B⋈C as chained pairwise IVM joins (Stage 3b S5): an intermediate join MV is itself a
/// changelog that feeds the next join. Updates on ALL THREE sides must net through the chain.
#[tokio::test]
async fn multiway_incremental_join_chained_pairwise() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    for s in ["ev_a", "ev_b", "ev_c"] {
        db.execute(&format!("CREATE SOURCE {s} (k BIGINT, v BIGINT)"))
            .await
            .unwrap();
    }
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS ta FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS tb FROM ev_b GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_c AS SELECT k, SUM(v) AS tc FROM ev_c GROUP BY k")
        .await
        .unwrap();
    // Intermediate ab = agg_a ⋈ agg_b, then abc = ab ⋈ agg_c — the join MV `ab` feeds the next join.
    db.execute(
        "CREATE MATERIALIZED VIEW ab AS \
         SELECT a.k, a.ta, b.tb FROM agg_a a JOIN agg_b b ON a.k = b.k",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW abc AS \
         SELECT ab.k, ab.ta, ab.tb, c.tc FROM ab JOIN agg_c c ON ab.k = c.k",
    )
    .await
    .expect("chained pairwise multi-way join allowed");
    db.start().await.unwrap();

    let (sa, sb, sc) = (
        db.source_untyped("ev_a").unwrap(),
        db.source_untyped("ev_b").unwrap(),
        db.source_untyped("ev_c").unwrap(),
    );
    sa.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    sb.push_arrow(batch(&[1, 2], &[100, 200])).unwrap();
    sc.push_arrow(batch(&[1, 2], &[1000, 2000])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    // Update one column on each side: ta[k1] 10->15, tb[k2] 200->250, tc[k1] 1000->1500.
    sa.push_arrow(batch(&[1], &[5])).unwrap();
    sb.push_arrow(batch(&[2], &[50])).unwrap();
    sc.push_arrow(batch(&[1], &[500])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    // Every update nets through the two-level chain; no stale rows.
    assert_eq!(
        read_query(&db, "SELECT k, ta, tb, tc FROM abc", 4).await,
        vec![vec![1, 15, 100, 1500], vec![2, 20, 250, 2000]]
    );
    db.shutdown().await.unwrap();
}

/// A join projection with a duplicate OUTPUT column name (same-named column from both sides) must be
/// rejected — otherwise the operator binds the join key and the projected column to different physical
/// columns, and it compounds when the MV feeds a downstream join.
#[tokio::test]
async fn incremental_join_rejects_duplicate_output_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    db.execute("CREATE SOURCE ev_a (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE ev_b (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS t FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS t FROM ev_b GROUP BY k")
        .await
        .unwrap();
    // Both sides project `t` → duplicate output name → rejected.
    let bad = db
        .execute(
            "CREATE MATERIALIZED VIEW dup AS \
             SELECT a.t, b.t FROM agg_a a JOIN agg_b b ON a.k = b.k",
        )
        .await;
    assert!(
        bad.is_err(),
        "duplicate output column name must be rejected"
    );
}

/// A SINGLE-statement 3-way join `FROM a JOIN b JOIN c` is decomposed into a hidden intermediate MV +
/// a rewritten 2-way final, producing the same result as the explicit chained-pairwise form.
#[tokio::test]
async fn single_statement_multiway_join_decomposes() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    for s in ["ev_a", "ev_b", "ev_c"] {
        db.execute(&format!("CREATE SOURCE {s} (k BIGINT, v BIGINT)"))
            .await
            .unwrap();
    }
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS ta FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS tb FROM ev_b GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_c AS SELECT k, SUM(v) AS tc FROM ev_c GROUP BY k")
        .await
        .unwrap();
    // ONE statement, three tables — decomposed into __ivm_abc_0 (= agg_a ⋈ agg_b) then abc (⋈ agg_c).
    db.execute(
        "CREATE MATERIALIZED VIEW abc AS \
         SELECT a.k, a.ta, b.tb, c.tc \
         FROM agg_a a JOIN agg_b b ON a.k = b.k JOIN agg_c c ON b.k = c.k",
    )
    .await
    .expect("single-statement 3-way join accepted + decomposed");
    db.start().await.unwrap();

    let (sa, sb, sc) = (
        db.source_untyped("ev_a").unwrap(),
        db.source_untyped("ev_b").unwrap(),
        db.source_untyped("ev_c").unwrap(),
    );
    sa.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    sb.push_arrow(batch(&[1, 2], &[100, 200])).unwrap();
    sc.push_arrow(batch(&[1, 2], &[1000, 2000])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    sa.push_arrow(batch(&[1], &[5])).unwrap();
    sb.push_arrow(batch(&[2], &[50])).unwrap();
    sc.push_arrow(batch(&[1], &[500])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    assert_eq!(
        read_query(&db, "SELECT k, ta, tb, tc FROM abc", 4).await,
        vec![vec![1, 15, 100, 1500], vec![2, 20, 250, 2000]]
    );
    db.shutdown().await.unwrap();
}

/// Dropping a single-statement multi-way join cascades to its hidden intermediate (upstream) MVs.
#[tokio::test]
async fn single_statement_multiway_drop_cascades_intermediate() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    for s in ["ev_a", "ev_b", "ev_c"] {
        db.execute(&format!("CREATE SOURCE {s} (k BIGINT, v BIGINT)"))
            .await
            .unwrap();
    }
    for (m, c) in [("agg_a", "ev_a"), ("agg_b", "ev_b"), ("agg_c", "ev_c")] {
        db.execute(&format!(
            "CREATE MATERIALIZED VIEW {m} AS SELECT k, SUM(v) AS t FROM {c} GROUP BY k"
        ))
        .await
        .unwrap();
    }
    db.execute(
        "CREATE MATERIALIZED VIEW abc AS \
         SELECT a.k, a.t AS at, b.t AS bt, c.t AS ct \
         FROM agg_a a JOIN agg_b b ON a.k = b.k JOIN agg_c c ON b.k = c.k",
    )
    .await
    .unwrap();
    db.start().await.unwrap();

    assert!(
        db.execute("SELECT * FROM __ivm_abc_0").await.is_ok(),
        "hidden intermediate exists before drop"
    );
    db.execute("DROP MATERIALIZED VIEW abc").await.unwrap();
    assert!(
        db.execute("SELECT * FROM __ivm_abc_0").await.is_err(),
        "intermediate dropped with the parent"
    );
    db.shutdown().await.unwrap();
}

/// A multi-way join whose participant is a non-incremental source is rejected (not decomposed).
#[tokio::test]
async fn single_statement_multiway_rejects_non_incremental_participant() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    for s in ["ev_a", "ev_b", "ev_c"] {
        db.execute(&format!("CREATE SOURCE {s} (k BIGINT, v BIGINT)"))
            .await
            .unwrap();
    }
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS ta FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS tb FROM ev_b GROUP BY k")
        .await
        .unwrap();
    // ev_c is a raw source (not incremental) -> the whole 3-way is rejected.
    let bad = db
        .execute(
            "CREATE MATERIALIZED VIEW abc AS \
             SELECT a.k, a.ta, b.tb FROM agg_a a JOIN agg_b b ON a.k = b.k JOIN ev_c c ON b.k = c.k",
        )
        .await;
    assert!(
        bad.is_err(),
        "a source participant in a multi-way join is rejected"
    );
}

/// A STAR multi-way join: the third table joins the FIRST (agg_a), not the preceding one. The
/// decomposition's own qualifier-aware ON analysis carries agg_a's key into the intermediate so the
/// later step can reference it (analyze_joins' immediate-previous model could not).
#[tokio::test]
async fn single_statement_multiway_star_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    for s in ["ev_a", "ev_b", "ev_c"] {
        db.execute(&format!("CREATE SOURCE {s} (k BIGINT, v BIGINT)"))
            .await
            .unwrap();
    }
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS ta FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS tb FROM ev_b GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_c AS SELECT k, SUM(v) AS tc FROM ev_c GROUP BY k")
        .await
        .unwrap();
    // Star: `JOIN agg_c c ON a.k = c.k` (references agg_a, two relations back), not `b.k = c.k`.
    db.execute(
        "CREATE MATERIALIZED VIEW abc AS \
         SELECT a.k, a.ta, b.tb, c.tc \
         FROM agg_a a JOIN agg_b b ON a.k = b.k JOIN agg_c c ON a.k = c.k",
    )
    .await
    .expect("star 3-way join accepted");
    db.start().await.unwrap();

    let (sa, sb, sc) = (
        db.source_untyped("ev_a").unwrap(),
        db.source_untyped("ev_b").unwrap(),
        db.source_untyped("ev_c").unwrap(),
    );
    sa.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    sb.push_arrow(batch(&[1, 2], &[100, 200])).unwrap();
    sc.push_arrow(batch(&[1, 2], &[1000, 2000])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    sc.push_arrow(batch(&[2], &[500])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    assert_eq!(
        read_query(&db, "SELECT k, ta, tb, tc FROM abc", 4).await,
        vec![vec![1, 10, 100, 1000], vec![2, 20, 200, 2500]]
    );
    db.shutdown().await.unwrap();
}

/// Like `read_query` but tolerates NULLs (LEFT-join pads), returning `None` for a NULL cell.
async fn read_query_nullable(db: &LaminarDB, sql: &str, ncols: usize) -> Vec<Vec<Option<i64>>> {
    let result = db.execute(sql).await.unwrap();
    let ExecuteResult::Query(mut q) = result else {
        panic!("expected Query result");
    };
    tokio::task::yield_now().await;
    let mut sub = q.subscribe_raw().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut rows = Vec::new();
    for _ in 0..4096 {
        match sub.poll() {
            Some(b) => {
                for r in 0..b.num_rows() {
                    let row: Vec<Option<i64>> = (0..ncols)
                        .map(|c| {
                            let col = b.column(c).as_any().downcast_ref::<Int64Array>().unwrap();
                            (!col.is_null(r)).then(|| col.value(r))
                        })
                        .collect();
                    rows.push(row);
                }
            }
            None => break,
        }
    }
    rows.sort();
    rows
}

/// A single-statement 4-way join (2 hidden intermediates) nets correctly under updates on every side.
#[tokio::test]
async fn single_statement_multiway_4way_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    for s in ["ev_a", "ev_b", "ev_c", "ev_d"] {
        db.execute(&format!("CREATE SOURCE {s} (k BIGINT, v BIGINT)"))
            .await
            .unwrap();
    }
    for (m, c, col) in [
        ("agg_a", "ev_a", "ta"),
        ("agg_b", "ev_b", "tb"),
        ("agg_c", "ev_c", "tc"),
        ("agg_d", "ev_d", "td"),
    ] {
        db.execute(&format!(
            "CREATE MATERIALIZED VIEW {m} AS SELECT k, SUM(v) AS {col} FROM {c} GROUP BY k"
        ))
        .await
        .unwrap();
    }
    db.execute(
        "CREATE MATERIALIZED VIEW abcd AS \
         SELECT a.k, a.ta, b.tb, c.tc, d.td \
         FROM agg_a a JOIN agg_b b ON a.k = b.k JOIN agg_c c ON b.k = c.k JOIN agg_d d ON c.k = d.k",
    )
    .await
    .expect("single-statement 4-way join accepted");
    db.start().await.unwrap();

    let src = |n: &str| db.source_untyped(n).unwrap();
    src("ev_a").push_arrow(batch(&[1], &[10])).unwrap();
    src("ev_b").push_arrow(batch(&[1], &[100])).unwrap();
    src("ev_c").push_arrow(batch(&[1], &[1000])).unwrap();
    src("ev_d").push_arrow(batch(&[1], &[10000])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    src("ev_d").push_arrow(batch(&[1], &[5000])).unwrap(); // td 10000 -> 15000
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    assert_eq!(
        read_query(&db, "SELECT k, ta, tb, tc, td FROM abcd", 5).await,
        vec![vec![1, 10, 100, 1000, 15000]]
    );
    db.shutdown().await.unwrap();
}

/// A single-statement chain with a LEFT step: a key present up to the LEFT boundary but with no
/// right match is NULL-padded through the decomposed final join.
#[tokio::test]
async fn single_statement_multiway_left_step() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    for s in ["ev_a", "ev_b", "ev_c"] {
        db.execute(&format!("CREATE SOURCE {s} (k BIGINT, v BIGINT)"))
            .await
            .unwrap();
    }
    db.execute("CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS ta FROM ev_a GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS tb FROM ev_b GROUP BY k")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg_c AS SELECT k, SUM(v) AS tc FROM ev_c GROUP BY k")
        .await
        .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW abc AS \
         SELECT a.k, a.ta, b.tb, c.tc \
         FROM agg_a a JOIN agg_b b ON a.k = b.k LEFT JOIN agg_c c ON b.k = c.k",
    )
    .await
    .expect("LEFT step in a multi-way chain accepted");
    db.start().await.unwrap();

    // Keys 1,2 on a+b; agg_c only for key 1 → key 2's tc is NULL.
    db.source_untyped("ev_a")
        .unwrap()
        .push_arrow(batch(&[1, 2], &[10, 20]))
        .unwrap();
    db.source_untyped("ev_b")
        .unwrap()
        .push_arrow(batch(&[1, 2], &[100, 200]))
        .unwrap();
    db.source_untyped("ev_c")
        .unwrap()
        .push_arrow(batch(&[1], &[1000]))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    assert_eq!(
        read_query_nullable(&db, "SELECT k, ta, tb, tc FROM abc", 4).await,
        vec![
            vec![Some(1), Some(10), Some(100), Some(1000)],
            vec![Some(2), Some(20), Some(200), None],
        ]
    );
    db.shutdown().await.unwrap();
}

/// SUBSCRIBE to an incremental MV is allowed (was `[LDB-1300]`) and delivers PLAIN rows: a Tail
/// subscriber is seeded with the current snapshot, and a subsequent change arrives as a fresh
/// consolidated snapshot — never the raw `__weight` changelog.
#[tokio::test]
async fn subscribe_to_incremental_mv_delivers_plain_snapshot() {
    use laminar_db::subscription::{PortalFrame, SubscribeStart};
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.start().await.unwrap();
    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Previously rejected; now returns a portal.
    let mut portal = db
        .open_subscription("agg", None, SubscribeStart::Tail)
        .await
        .expect("SUBSCRIBE to an incremental MV is now allowed");
    // The wire schema is the plain MV schema — no `__weight`.
    assert!(
        portal.schema().index_of("__weight").is_err(),
        "SUBSCRIBE wire schema must be plain rows"
    );

    // Seeded with the current snapshot (k1=10, k2=20), as plain rows.
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), portal.next_frame())
        .await
        .expect("a frame within the deadline")
        .expect("a portal frame");
    let PortalFrame::Batch(b) = frame else {
        panic!("expected a Batch frame, got {frame:?}");
    };
    assert!(
        b.schema().index_of("__weight").is_err(),
        "snapshot batch must be plain rows (no __weight)"
    );
    assert_eq!(rows_of(&b), vec![(1, 10, 1), (2, 20, 1)], "seeded snapshot");

    // A change arrives as a fresh consolidated snapshot (k1: 10 -> 15).
    source.push_arrow(batch(&[1], &[5])).unwrap();
    let mut saw_update = false;
    for _ in 0..20 {
        match tokio::time::timeout(std::time::Duration::from_secs(3), portal.next_frame()).await {
            Ok(Some(PortalFrame::Batch(b))) => {
                if rows_of(&b).iter().any(|&(k, t, _)| k == 1 && t == 15) {
                    saw_update = true;
                    break;
                }
            }
            Ok(Some(_)) => {} // Barrier/Lagged — keep polling
            Ok(None) | Err(_) => break,
        }
    }
    assert!(
        saw_update,
        "subscriber must receive the updated snapshot k1=15"
    );
    portal.close();
    db.shutdown().await.unwrap();
}
