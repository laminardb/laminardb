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
        incremental_emit: incremental,
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: None, // manual checkpoints only
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    }
}

fn non_recoverable_config(dir: &std::path::Path, incremental: bool) -> LaminarConfig {
    let mut config = config(dir, incremental);
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
        db.shutdown().await.unwrap();
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

/// A counted multiset MV must preserve duplicate rows at the durable cut. After restart, changing
/// one upstream group retracts exactly one copy of the duplicate rather than both copies.
#[tokio::test]
async fn counted_multiset_survives_coordinator_checkpoint_restart_and_retraction() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
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
    let db = LaminarDB::open_with_config(non_recoverable_config(dir.path(), true)).unwrap();
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
    let db = LaminarDB::open_with_config(non_recoverable_config(dir.path(), true)).unwrap();
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
        "CREATE SINK f FROM agg INTO FILES (path = '{}') FORMAT JSON",
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
        "CREATE SINK f FROM agg INTO FILES (path = '{}') FORMAT JSON",
        out.display().to_string().replace('\\', "/")
    ))
    .await
    .expect("CREATE SINK");
    db.start()
        .await
        .expect("a full-emit aggregate feeding a plain sink must start fine");
    db.shutdown().await.ok();
}

/// The capability check follows the changelog through a stream: a non-capable sink over a STREAM
/// that projects an incremental MV is rejected at start too, not just a direct incremental-MV input.
#[cfg(feature = "files")]
#[tokio::test]
async fn sink_over_changelog_stream_rejects_noncapable_sink_at_start() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
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
    let started = db.start().await;
    let err = format!(
        "{:?}",
        started.expect_err("a non-capable sink over a changelog stream must be rejected at start")
    );
    assert!(err.contains("LDB-1300"), "expected LDB-1300, got: {err}");
    db.shutdown().await.ok();
}

/// Stateful joins over changelog-producing materialized views are not part of the single bounded
/// interval-join contract and must fail before catalog registration.
#[tokio::test]
async fn changelog_joins_are_rejected_at_ddl() {
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

/// SUBSCRIBE to an incremental MV delivers plain consolidated snapshots for updates emitted after
/// a Tail subscriber attaches, never the raw `__weight` changelog.
#[tokio::test]
async fn subscribe_to_incremental_mv_delivers_plain_snapshot() {
    use laminar_db::subscription::{PortalFrame, SubscribeStart};
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path(), true)).unwrap();
    db.execute(SRC).await.unwrap();
    db.execute(MV).await.unwrap();
    db.start().await.unwrap();
    let source = db.source_untyped("events").unwrap();

    let mut portal = db
        .open_subscription("agg", None, SubscribeStart::Tail)
        .await
        .expect("SUBSCRIBE to an incremental MV is now allowed");
    // The wire schema is the plain MV schema — no `__weight`.
    assert!(
        portal.schema().index_of("__weight").is_err(),
        "SUBSCRIBE wire schema must be plain rows"
    );

    source.push_arrow(batch(&[1, 2], &[10, 20])).unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), portal.next_frame())
        .await
        .expect("a frame within the deadline")
        .expect("a portal frame");
    let PortalFrame::Batch { batch: b, .. } = frame else {
        panic!("expected a Batch frame, got {frame:?}");
    };
    assert!(
        b.schema().index_of("__weight").is_err(),
        "snapshot batch must be plain rows (no __weight)"
    );
    assert_eq!(
        rows_of(&b),
        vec![(1, 10, 1), (2, 20, 1)],
        "post-attach snapshot"
    );

    // A change arrives as a fresh consolidated snapshot (k1: 10 -> 15).
    source.push_arrow(batch(&[1], &[5])).unwrap();
    let mut saw_update = false;
    for _ in 0..20 {
        match tokio::time::timeout(std::time::Duration::from_secs(3), portal.next_frame()).await {
            Ok(Some(PortalFrame::Batch { batch: b, .. })) => {
                if rows_of(&b).iter().any(|&(k, t, _)| k == 1 && t == 15) {
                    saw_update = true;
                    break;
                }
            }
            Ok(Some(PortalFrame::Barrier { .. })) => {}
            Ok(Some(PortalFrame::Lagged(n))) => panic!("subscriber lagged by {n} messages"),
            Ok(Some(PortalFrame::Error { message })) => {
                panic!("subscription failed: {message}")
            }
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
