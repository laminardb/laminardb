#![cfg(feature = "state-tier")]
//! Single-node tier-backed IVM changelog⋈changelog join: demotion correctness + restart durability
//! (A1-emit 3b-S4.6). Mirrors `single_node_group_demotion.rs` but exercises the two-sided join
//! operator. The join's state (both side Z-sets) is the largest consumer, so a tiny memory budget
//! reliably demotes its keys; an extra round re-touches them to drive fetch-on-access promotion.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use laminar_core::state::{NodeId, ObjectStoreBackend, VnodeRegistry};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{ExecuteResult, LaminarDB};
use object_store::local::LocalFileSystem;

const VNODES: u32 = 64;
const KEYS: i64 = 300;

fn kv_batch(keys: &[i64], v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let vals: Vec<i64> = keys.iter().map(|_| v).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap()
}

async fn open_join(
    ckpt: &std::path::Path,
    state_dir: &std::path::Path,
    tier_dir: &std::path::Path,
    budget_bytes: usize,
    group_demotion: bool,
    left_outer: bool,
) -> LaminarDB {
    let store = Arc::new(LocalFileSystem::new_with_prefix(state_dir).unwrap());
    let backend = Arc::new(ObjectStoreBackend::new(store, "node-0", VNODES));
    let registry = Arc::new(VnodeRegistry::new(VNODES));
    registry.set_assignment((0..VNODES).map(|_| NodeId(0)).collect::<Vec<_>>().into());

    let db = LaminarDB::builder()
        .storage_dir(ckpt)
        .checkpoint(StreamCheckpointConfig {
            interval_ms: None,
            incremental_emit: true,
            ..StreamCheckpointConfig::default()
        })
        .state_backend(backend)
        .vnode_registry(registry)
        .state_tier_dir(tier_dir)
        .state_memory_budget_bytes(budget_bytes)
        .state_tier_group_demotion(group_demotion)
        .build()
        .await
        .unwrap();
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
    let join_kind = if left_outer { "LEFT JOIN" } else { "JOIN" };
    db.execute(&format!(
        "CREATE MATERIALIZED VIEW joined AS \
         SELECT a.k, a.total, b.wtotal FROM agg_a a {join_kind} agg_b b ON a.k = b.k"
    ))
    .await
    .unwrap();
    db
}

/// `SELECT k, total, wtotal FROM joined` as `k -> (total, wtotal?)` (wtotal is NULL for an unmatched
/// LEFT-join key).
async fn read_joined(db: &LaminarDB) -> BTreeMap<i64, (i64, Option<i64>)> {
    let result = db
        .execute("SELECT k, total, wtotal FROM joined")
        .await
        .unwrap();
    let ExecuteResult::Query(mut q) = result else {
        panic!("expected Query");
    };
    tokio::task::yield_now().await;
    let mut sub = q.subscribe_raw().unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let mut out = BTreeMap::new();
    for _ in 0..8192 {
        match sub.poll() {
            Some(b) => {
                let col = |i: usize| {
                    b.column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .clone()
                };
                let (k, total, wtotal) = (col(0), col(1), col(2));
                for r in 0..b.num_rows() {
                    let w = (!wtotal.is_null(r)).then(|| wtotal.value(r));
                    out.insert(k.value(r), (total.value(r), w));
                }
            }
            None => break,
        }
    }
    out
}

fn all_keys() -> Vec<i64> {
    (0..KEYS).collect()
}
fn first_half() -> Vec<i64> {
    (0..KEYS / 2).collect()
}

/// One deterministic workload run; returns the joined snapshot + the demotion count.
/// `left_outer=true` seeds the RIGHT side for only the first half → second-half keys NULL-pad.
async fn run(group_demotion: bool, left_outer: bool) -> (BTreeMap<i64, (i64, Option<i64>)>, u64) {
    let dir = tempfile::tempdir().unwrap();
    let ckpt = dir.path().join("ckpt");
    let state_dir = dir.path().join("state");
    let tier_dir = dir.path().join("tier");
    std::fs::create_dir_all(&state_dir).unwrap();
    // Tiny budget forces demotion across operators (the join is the largest); a big one is the oracle.
    let budget = if group_demotion { 4096 } else { 64 << 20 };
    let db = open_join(
        &ckpt,
        &state_dir,
        &tier_dir,
        budget,
        group_demotion,
        left_outer,
    )
    .await;
    db.start().await.unwrap();
    let sa = db.source_untyped("ev_a").unwrap();
    let sb = db.source_untyped("ev_b").unwrap();
    let right_keys = if left_outer { first_half() } else { all_keys() };

    // Three rounds over all keys with idle gaps so a demoting run sheds idle join keys.
    for _ in 0..3 {
        sa.push_arrow(kv_batch(&all_keys(), 1)).unwrap();
        sb.push_arrow(kv_batch(&right_keys, 10)).unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    // Checkpoint marks join keys clean (a demotion prerequisite), then wait for demotion to fire.
    assert!(db.checkpoint().await.unwrap().success);
    if group_demotion {
        let deadline = Instant::now() + Duration::from_secs(60);
        while db.tier_metrics().demote_total == 0 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                Instant::now() < deadline,
                "demotion never fired: {:?}",
                db.tier_metrics()
            );
        }
    }
    // Extra round over the first half on the LEFT side — re-touching demoted join keys promotes them
    // back (fetch-on-access), bumping `total` to 4. Poll until key 0 settles.
    sa.push_arrow(kv_batch(&first_half(), 1)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut map = read_joined(&db).await;
    while map.get(&0).map(|(t, _)| *t) != Some(4) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        map = read_joined(&db).await;
    }
    let demotes = db.tier_metrics().demote_total;
    db.shutdown().await.unwrap();
    (map, demotes)
}

/// Headline gate: an inner changelog⋈changelog join run with demotion ON (tiny budget) must produce
/// the exact joined snapshot of the no-demotion oracle — demote/promote must not change values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_join_demotion_matches_oracle() {
    let (oracle, _) = run(false, false).await;
    let (demoted, demotes) = run(true, false).await;

    assert!(
        demotes > 0,
        "demotion never fired — the path was not exercised"
    );
    assert_eq!(oracle.len() as i64, KEYS, "oracle holds every key");
    assert_eq!(oracle.get(&0), Some(&(4, Some(30))), "first-half key");
    assert_eq!(
        oracle.get(&(KEYS - 1)),
        Some(&(3, Some(30))),
        "second-half key"
    );
    assert_eq!(
        demoted, oracle,
        "join demotion changed values vs the no-demotion oracle"
    );
}

/// §5.2 in the integrated path: a LEFT join where second-half keys never match (NULL-pad). Their
/// cold blob encodes an absent right side, so demote→promote must keep them NULL-padded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn left_outer_incremental_join_demotion_matches_oracle() {
    let (oracle, _) = run(false, true).await;
    let (demoted, demotes) = run(true, true).await;

    assert!(demotes > 0, "demotion never fired");
    assert_eq!(
        oracle.get(&0),
        Some(&(4, Some(30))),
        "matched first-half key"
    );
    assert_eq!(
        oracle.get(&(KEYS - 1)),
        Some(&(3, None)),
        "unmatched second-half key NULL-padded"
    );
    assert_eq!(
        demoted, oracle,
        "LEFT-join demotion changed values vs the no-demotion oracle"
    );
}

/// Restart durability: demoted join keys must recover from the durable cold-only partials (the tier
/// is wiped on restart). A post-restart update on a demoted key nets correctly only if BOTH side
/// Z-sets were restored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_join_demotion_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let ckpt = dir.path().join("ckpt");
    let state_dir = dir.path().join("state");
    let tier_dir = dir.path().join("tier");
    std::fs::create_dir_all(&state_dir).unwrap();

    {
        let db = open_join(&ckpt, &state_dir, &tier_dir, 4096, true, false).await;
        db.start().await.unwrap();
        let sa = db.source_untyped("ev_a").unwrap();
        let sb = db.source_untyped("ev_b").unwrap();
        for _ in 0..3 {
            sa.push_arrow(kv_batch(&all_keys(), 1)).unwrap();
            sb.push_arrow(kv_batch(&all_keys(), 10)).unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        assert!(db.checkpoint().await.unwrap().success);
        let deadline = Instant::now() + Duration::from_secs(60);
        while db.tier_metrics().demote_total == 0 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(Instant::now() < deadline, "demotion never fired");
        }
        // Checkpoint again so demoted join keys are written to durable cold-only partials.
        assert!(db.checkpoint().await.unwrap().success);
        let pre = read_joined(&db).await;
        assert_eq!(pre.len() as i64, KEYS, "all keys present pre-restart");
        assert!(
            pre.values().all(|&(t, w)| t == 3 && w == Some(30)),
            "every joined key is (3, 30) pre-restart"
        );
        db.close();
    }

    // Reopen from the checkpoint: demoted keys recover from cold-only partials, resident from the
    // manifest. The tier dir is reused but wiped on open — recovery rides the durable partials.
    {
        let db = open_join(&ckpt, &state_dir, &tier_dir, 4096, true, false).await;
        db.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let sa = db.source_untyped("ev_a").unwrap();
        // Bump every key's LEFT total 3 -> 4. A recovered join key nets to (4, 30); a lost one would
        // be (1, NULL) or missing.
        sa.push_arrow(kv_batch(&all_keys(), 1)).unwrap();

        let deadline = Instant::now() + Duration::from_secs(45);
        let mut after = read_joined(&db).await;
        while (0..KEYS).any(|k| after.get(&k) != Some(&(4, Some(30)))) && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(250)).await;
            after = read_joined(&db).await;
        }
        let lost: Vec<i64> = (0..KEYS)
            .filter(|k| after.get(k) != Some(&(4, Some(30))))
            .collect();
        assert!(
            lost.is_empty(),
            "join keys lost or wrong after restart (want (4,30)): {:?} (sample={:?})",
            &lost[..lost.len().min(10)],
            after.iter().take(5).collect::<Vec<_>>()
        );
        db.shutdown().await.unwrap();
    }
}

async fn open_multiway3(
    ckpt: &std::path::Path,
    state_dir: &std::path::Path,
    tier_dir: &std::path::Path,
) -> LaminarDB {
    let store = Arc::new(LocalFileSystem::new_with_prefix(state_dir).unwrap());
    let backend = Arc::new(ObjectStoreBackend::new(store, "node-0", VNODES));
    let registry = Arc::new(VnodeRegistry::new(VNODES));
    registry.set_assignment((0..VNODES).map(|_| NodeId(0)).collect::<Vec<_>>().into());
    let db = LaminarDB::builder()
        .storage_dir(ckpt)
        .checkpoint(StreamCheckpointConfig {
            interval_ms: None,
            incremental_emit: true,
            ..StreamCheckpointConfig::default()
        })
        .state_backend(backend)
        .vnode_registry(registry)
        .state_tier_dir(tier_dir)
        .state_memory_budget_bytes(4096)
        .state_tier_group_demotion(true)
        .build()
        .await
        .unwrap();
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
    // Single-statement 3-way join (decomposed into __ivm_abc_0 + abc; both demote/recover).
    db.execute(
        "CREATE MATERIALIZED VIEW abc AS \
         SELECT a.k, a.t AS ta, b.t AS tb, c.t AS tc \
         FROM agg_a a JOIN agg_b b ON a.k = b.k JOIN agg_c c ON b.k = c.k",
    )
    .await
    .unwrap();
    db
}

async fn read_abc(db: &LaminarDB) -> BTreeMap<i64, (i64, i64, i64)> {
    let result = db.execute("SELECT k, ta, tb, tc FROM abc").await.unwrap();
    let ExecuteResult::Query(mut q) = result else {
        panic!("expected Query");
    };
    tokio::task::yield_now().await;
    let mut sub = q.subscribe_raw().unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let mut out = BTreeMap::new();
    for _ in 0..8192 {
        match sub.poll() {
            Some(b) => {
                let col = |i: usize| {
                    b.column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .clone()
                };
                let (k, ta, tb, tc) = (col(0), col(1), col(2), col(3));
                for r in 0..b.num_rows() {
                    out.insert(k.value(r), (ta.value(r), tb.value(r), tc.value(r)));
                }
            }
            None => break,
        }
    }
    out
}

/// A SINGLE-statement 3-way join under demotion (both the hidden intermediate and the final join
/// operator shed cold keys) survives checkpoint -> restart: a post-restart update nets correctly only
/// if both operators' side Z-sets recovered from their cold-only partials.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_statement_multiway_demotion_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let ckpt = dir.path().join("ckpt");
    let state_dir = dir.path().join("state");
    let tier_dir = dir.path().join("tier");
    std::fs::create_dir_all(&state_dir).unwrap();

    {
        let db = open_multiway3(&ckpt, &state_dir, &tier_dir).await;
        db.start().await.unwrap();
        let all: Vec<i64> = (0..KEYS).collect();
        for _ in 0..3 {
            db.source_untyped("ev_a")
                .unwrap()
                .push_arrow(kv_batch(&all, 1))
                .unwrap();
            db.source_untyped("ev_b")
                .unwrap()
                .push_arrow(kv_batch(&all, 10))
                .unwrap();
            db.source_untyped("ev_c")
                .unwrap()
                .push_arrow(kv_batch(&all, 100))
                .unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        assert!(db.checkpoint().await.unwrap().success);
        let deadline = Instant::now() + Duration::from_secs(60);
        while db.tier_metrics().demote_total == 0 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(Instant::now() < deadline, "demotion never fired");
        }
        assert!(db.checkpoint().await.unwrap().success); // write demoted keys to cold-only partials
        let pre = read_abc(&db).await;
        assert_eq!(pre.len() as i64, KEYS, "all keys joined pre-restart");
        assert!(
            pre.values().all(|&(a, b, c)| a == 3 && b == 30 && c == 300),
            "every key is (3,30,300) pre-restart"
        );
        db.close();
    }

    {
        let db = open_multiway3(&ckpt, &state_dir, &tier_dir).await;
        db.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let all: Vec<i64> = (0..KEYS).collect();
        db.source_untyped("ev_a")
            .unwrap()
            .push_arrow(kv_batch(&all, 1))
            .unwrap(); // ta 3 -> 4
        let deadline = Instant::now() + Duration::from_secs(45);
        let mut after = read_abc(&db).await;
        while (0..KEYS).any(|k| after.get(&k) != Some(&(4, 30, 300))) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(250)).await;
            after = read_abc(&db).await;
        }
        let lost: Vec<i64> = (0..KEYS)
            .filter(|k| after.get(k) != Some(&(4, 30, 300)))
            .collect();
        assert!(
            lost.is_empty(),
            "keys lost/wrong after restart (want (4,30,300)): {:?}",
            &lost[..lost.len().min(10)]
        );
        db.shutdown().await.unwrap();
    }
}
