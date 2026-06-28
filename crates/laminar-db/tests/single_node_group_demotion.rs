#![cfg(feature = "state-tier")]
//! Single-node v2 GROUP demotion + restart durability.
//!
//! With `state_tier_group_demotion`, idle aggregate groups are demoted to the cold tier. The tier
//! is wiped on restart, so demoted groups must reach the durable per-vnode partials: the coordinator
//! writes them as a COLD-ONLY partial that recovery merges additively on top of the whole-node
//! manifest's resident groups. This test proves a demoted group's count survives a checkpoint →
//! restart (re-feeding it continues from the recovered count, rather than rebuilding from zero).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use laminar_core::state::{NodeId, ObjectStoreBackend, VnodeRegistry};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{ExecuteResult, LaminarDB};
use object_store::local::LocalFileSystem;

const VNODES: u32 = 64;
const KEYS: i64 = 200;
const REPEATS: i64 = 3;

fn key_batch(keys: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys.to_vec()))]).unwrap()
}

async fn open(
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
            interval_ms: None,      // manual checkpoints
            incremental_emit: true, // changelog agg + queryable Upsert snapshot
            ..StreamCheckpointConfig::default()
        })
        .state_backend(backend)
        .vnode_registry(registry)
        .state_tier_dir(tier_dir)
        .state_memory_budget_bytes(2048) // tiny — forces group demotion
        .state_tier_group_demotion(true)
        .build()
        .await
        .unwrap();
    db.execute("CREATE SOURCE events (k BIGINT)").await.unwrap();
    db.execute("CREATE MATERIALIZED VIEW counts AS SELECT k, COUNT(*) AS n FROM events GROUP BY k")
        .await
        .unwrap();
    db
}

/// `SELECT k, n FROM counts` as a sorted `k -> n` map.
async fn read_counts(db: &LaminarDB) -> BTreeMap<i64, i64> {
    let result = db.execute("SELECT k, n FROM counts").await.unwrap();
    let ExecuteResult::Query(mut q) = result else {
        panic!("expected Query");
    };
    tokio::task::yield_now().await;
    let mut sub = q.subscribe_raw().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut out = BTreeMap::new();
    for _ in 0..4096 {
        match sub.poll() {
            Some(b) => {
                let col = |i: usize| {
                    b.column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .clone()
                };
                let (k, n) = (col(0), col(1));
                for r in 0..b.num_rows() {
                    out.insert(k.value(r), n.value(r));
                }
            }
            None => break,
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_group_demotion_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let ckpt = dir.path().join("ckpt");
    let state_dir = dir.path().join("state");
    let tier_dir = dir.path().join("tier");
    std::fs::create_dir_all(&state_dir).unwrap();

    let expected: BTreeMap<i64, i64>;
    {
        let db = open(&ckpt, &state_dir, &tier_dir).await;
        db.start().await.unwrap();
        let source = db.source_untyped("events").unwrap();

        // Each key counted REPEATS times → counts > 1 so a recovered count is distinguishable from
        // a from-scratch rebuild.
        for _ in 0..REPEATS {
            source
                .push_arrow(key_batch(&(0..KEYS).collect::<Vec<_>>()))
                .unwrap();
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        // Drain the backlog so groups go idle+clean, then checkpoint to set the clean baseline and
        // seed the per-vnode delta-tracking count.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(db.checkpoint().await.unwrap().success);

        // Idle + over-budget → the maintenance pass demotes groups to the tier.
        let deadline = Instant::now() + Duration::from_secs(60);
        while db.tier_metrics().demote_total == 0 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(
                Instant::now() < deadline,
                "group demotion never fired: {:?}",
                db.tier_metrics()
            );
        }

        // Checkpoint AGAIN — now the demoted groups must be written to durable cold-only partials.
        assert!(db.checkpoint().await.unwrap().success);

        expected = read_counts(&db).await;
        assert_eq!(expected.len() as i64, KEYS, "all keys present pre-restart");
        assert!(
            expected.values().all(|&n| n == REPEATS),
            "each key counted {REPEATS} times"
        );
        db.close();
    }

    // Reopen from the checkpoint — demoted groups recover from the cold-only partials, resident
    // groups from the manifest.
    {
        let db = open(&ckpt, &state_dir, &tier_dir).await;
        db.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Feed every key once more. A RECOVERED group continues from its count (→ REPEATS+1); a
        // LOST/rebuilt group would restart at 1.
        let source = db.source_untyped("events").unwrap();
        source
            .push_arrow(key_batch(&(0..KEYS).collect::<Vec<_>>()))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;

        let after = read_counts(&db).await;
        let lost: Vec<i64> = (0..KEYS)
            .filter(|k| after.get(k).copied() != Some(REPEATS + 1))
            .collect();
        assert!(
            lost.is_empty(),
            "groups lost or rebuilt-from-zero after restart (expected count {}): {:?} (sample after={:?})",
            REPEATS + 1,
            &lost[..lost.len().min(10)],
            after.iter().take(5).collect::<Vec<_>>()
        );
        db.shutdown().await.unwrap();
    }
}
