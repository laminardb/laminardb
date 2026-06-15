#![cfg(feature = "state-tier")]
//! Single-node cold-tier demotion + promotion, with no cluster controller.
//!
//! Exercises the path where a durable state backend and a single-owner vnode
//! registry are configured but there is no controller or row shuffle. The tier
//! takes its vnode count from the registry (not the shuffle config), so
//! per-vnode capture and demotion run without a cluster. Before the vnode count
//! was decoupled from the shuffle config, a node without a controller could
//! never demote (the tier stayed disabled), so `demote_total` would never move.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use laminar_core::state::{NodeId, ObjectStoreBackend, VnodeRegistry};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::LaminarDB;
use object_store::local::LocalFileSystem;

const VNODES: u32 = 64;

fn key_batch(keys: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys.to_vec()))]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_demotes_and_promotes_without_a_controller() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let tier_dir = dir.path().join("tier");
    std::fs::create_dir_all(&state_dir).unwrap();

    let store = Arc::new(LocalFileSystem::new_with_prefix(&state_dir).unwrap());
    let backend = Arc::new(ObjectStoreBackend::new(store, "node-0", VNODES));

    // Single-owner registry: all vnodes owned by this node, no controller. This
    // is the documented single-instance path — the durability gate wires the
    // coordinator from it, and the tier takes its vnode count from it.
    let registry = Arc::new(VnodeRegistry::new(VNODES));
    registry.set_assignment((0..VNODES).map(|_| NodeId(0)).collect::<Vec<_>>().into());

    let db = LaminarDB::builder()
        .storage_dir(dir.path().join("ckpt"))
        .checkpoint(StreamCheckpointConfig {
            interval_ms: None, // drive checkpoints manually below
            ..StreamCheckpointConfig::default()
        })
        .state_backend(backend)
        .vnode_registry(registry)
        .state_tier_dir(&tier_dir)
        .state_memory_budget_bytes(4096) // tiny — forces demotion under load
        .build()
        .await
        .unwrap();

    db.execute("CREATE SOURCE events (k BIGINT)").await.unwrap();
    db.execute(
        "CREATE STREAM counts AS SELECT k, COUNT(*) AS n FROM events GROUP BY k EMIT CHANGES",
    )
    .await
    .unwrap();
    db.start().await.unwrap();

    // The tier opens its fjall dir only when the activation gate passes — so its
    // presence proves the single-node tier was enabled without a controller.
    assert!(
        tier_dir.join("db").exists(),
        "single-node tier should be enabled (no controller required)"
    );

    let source = db.source_untyped("events").unwrap();

    // Build aggregate state over the 4 KiB budget: 1000 distinct keys.
    for batch in 0..2i64 {
        let start = batch * 500;
        source
            .push_arrow(key_batch(&(start..start + 500).collect::<Vec<_>>()))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Drain the ingest backlog so every vnode goes idle (demotion only sheds
    // vnodes clean since the last capture — a still-draining backlog keeps them
    // dirty), then checkpoint to set the clean baseline + per-vnode slices.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(db.checkpoint().await.unwrap().success);

    // With no more input, the maintenance pass sees clean, over-budget vnodes
    // and sheds them to the tier.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if db.tier_metrics().demote_total > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "demotion never fired on a single node: {:?}",
            db.tier_metrics()
        );
    }
    let demoted = db.tier_metrics();
    assert!(demoted.demote_total > 0, "expected effective demotions");
    assert!(
        demoted.resident_bytes > 0 && demoted.resident_slices > 0,
        "tier should hold demoted slices: {demoted:?}"
    );

    // Re-feed earlier keys — now demoted — so a row lands on a cold vnode and
    // must promote it back (a fetch), not silently rebuild it (which would lose
    // the prior count).
    source
        .push_arrow(key_batch(&(0..200).collect::<Vec<_>>()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if db.tier_metrics().fetch_total > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "promotion never fired on a single node: {:?}",
            db.tier_metrics()
        );
    }
    assert!(
        db.tier_metrics().fetch_total > 0,
        "expected promotion fetches"
    );

    db.shutdown().await.unwrap();
}
