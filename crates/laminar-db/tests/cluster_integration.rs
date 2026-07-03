#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)]
//! Unified cluster integration tests.

#[path = "common/cluster_harness.rs"]
mod cluster_harness;

#[path = "common/mod.rs"]
mod common;

mod durable_backend_gate {
    use std::sync::Arc;

    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::state::{InProcessBackend, NodeId, VnodeRegistry};
    use laminar_db::LaminarDB;
    use tokio::sync::watch;

    // Cluster mode must refuse to start on a non-durable (in-process) state backend: a peer
    // could not recover a dead node's vnodes from another node's local memory.
    #[tokio::test]
    async fn cluster_start_rejects_non_durable_backend() {
        let self_id = NodeId(1);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
        let (_tx, rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv, None, rx));

        let db = LaminarDB::builder()
            .cluster_controller(controller)
            .state_backend(Arc::new(InProcessBackend::new(4)))
            .vnode_registry(Arc::new(VnodeRegistry::new(4)))
            .build()
            .await
            .expect("build succeeds; durability is enforced at start");

        let err = db
            .start()
            .await
            .expect_err("cluster start must reject a non-durable backend");
        assert!(
            err.to_string().contains("LDB-0011"),
            "expected LDB-0011 durable-backend error, got: {err}"
        );
    }
}

mod smoke {
    use std::collections::HashSet;
    use std::time::Duration;
    use tokio::time::sleep;

    use super::cluster_harness::{
        input_batch, manifest_epoch, pick_keys_per_owner, read_mv_sums, ClusterEngineHarness,
    };

    const VNODE_COUNT: u32 = 4;
    const N_NODES: usize = 2;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn happy_path_eight_keys_correct_sums() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .try_init();

        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];

        let key_buckets = {
            let leader = &harness.nodes[leader_idx];
            let follower = &harness.nodes[follower_idx];
            let owners = vec![
                (leader.instance_id, leader.owned_vnodes()),
                (follower.instance_id, follower.owned_vnodes()),
            ];
            pick_keys_per_owner(VNODE_COUNT, &owners, 4)
                .expect("pick_keys_per_owner: search range too small")
        };
        let all_keys: Vec<i64> = key_buckets
            .iter()
            .flat_map(|(_, ks)| ks.iter().copied())
            .collect();
        assert_eq!(all_keys.len(), 8, "want 4 keys per owner");

        for node in &harness.nodes {
            node.db
                .execute("CREATE SOURCE src (key BIGINT, value BIGINT)")
                .await
                .expect("CREATE SOURCE");
            node.db
                .execute(
                    "CREATE MATERIALIZED VIEW sums AS \
                     SELECT key, SUM(value) AS total FROM src GROUP BY key",
                )
                .await
                .expect("CREATE MATERIALIZED VIEW");
        }
        harness.start_all().await;

        let leader_node = &harness.nodes[leader_idx];
        let follower_node = &harness.nodes[follower_idx];
        let src = leader_node
            .db
            .source_untyped("src")
            .expect("source_untyped on leader");
        src.push_arrow(input_batch(&all_keys)).expect("push_arrow");

        let result = leader_node
            .db
            .checkpoint()
            .await
            .expect("leader checkpoint");
        assert!(
            result.success,
            "leader checkpoint failed: {:?}",
            result.error,
        );

        sleep(Duration::from_millis(500)).await;

        let leader_rows = read_mv_sums(&leader_node.db, "sums").await;
        let follower_rows = read_mv_sums(&follower_node.db, "sums").await;

        assert!(
            !follower_rows.is_empty(),
            "follower MV is empty — shuffle didn't deliver any partials. \
             leader_rows={leader_rows:?}",
        );

        let leader_keys: HashSet<i64> = leader_rows.iter().map(|(k, _)| *k).collect();
        let follower_keys: HashSet<i64> = follower_rows.iter().map(|(k, _)| *k).collect();
        assert!(
            leader_keys.is_disjoint(&follower_keys),
            "key appears on both nodes: leader={leader_keys:?} follower={follower_keys:?}",
        );

        let mut union: Vec<(i64, i64)> = leader_rows
            .iter()
            .chain(follower_rows.iter())
            .copied()
            .collect();
        union.sort_by_key(|(k, _)| *k);

        let mut expected: Vec<(i64, i64)> = all_keys.iter().map(|&k| (k, k * 10)).collect();
        expected.sort_by_key(|(k, _)| *k);

        assert_eq!(
            union, expected,
            "union of MVs does not match input:\n got  {union:?}\n want {expected:?}",
        );

        let leader_epoch = manifest_epoch(&leader_node.db).await;
        let follower_epoch = manifest_epoch(&follower_node.db).await;
        assert!(
            leader_epoch.abs_diff(follower_epoch) <= 1,
            "manifest epoch drift > 1: leader={leader_epoch} follower={follower_epoch}",
        );

        harness.shutdown().await;
    }
}

mod failures {
    use std::time::Duration;
    use tokio::time::sleep;

    use super::cluster_harness::{
        input_batch, pick_keys_per_owner, read_mv_sums, ClusterEngineHarness,
    };

    const VNODE_COUNT: u32 = 4;
    const N_NODES: usize = 2;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn split_brain_write_partial_rejected() {
        use bytes::Bytes;
        use laminar_core::state::{ObjectStoreBackend, StateBackend, StateBackendError};
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;

        let dir = tempfile::tempdir().expect("tempdir");
        let store: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"));

        let fresh = ObjectStoreBackend::new(std::sync::Arc::clone(&store), "leader", 4);
        let stale = ObjectStoreBackend::new(std::sync::Arc::clone(&store), "ex-leader", 4);

        fresh.set_authoritative_version(3);
        fresh
            .write_partial(0, 1, 3, Bytes::from_static(b"fresh"))
            .await
            .expect("fresh write at current version");

        stale.set_authoritative_version(3);
        let err = stale
            .write_partial(0, 1, 2, Bytes::from_static(b"stale"))
            .await
            .expect_err("stale write must be rejected");
        match err {
            StateBackendError::StaleVersion {
                caller,
                authoritative,
            } => {
                assert_eq!(caller, 2);
                assert_eq!(authoritative, 3);
            }
            other => panic!("expected StaleVersion, got {other:?}"),
        }

        let got = fresh.read_partial(0, 1).await.unwrap().unwrap();
        assert_eq!(&got[..], b"fresh");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn assignment_snapshot_unifies_cluster_view() {
        let harness_a =
            super::cluster_harness::ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        let assignment_a: Vec<super::cluster_harness::NodeIdView> = harness_a
            .nodes
            .iter()
            .map(|n| (n.instance_id, n.owned_vnodes()))
            .collect();
        let (shared_dir, _cp_dirs) = harness_a.shutdown_keep_dirs().await;

        let cp_dirs2: Vec<_> = (0..N_NODES).map(|_| tempfile::tempdir().unwrap()).collect();
        let harness_b = super::cluster_harness::ClusterEngineHarness::spawn_with_dirs(
            N_NODES,
            VNODE_COUNT,
            shared_dir,
            cp_dirs2,
            None,
            None,
        )
        .await;
        let assignment_b: Vec<super::cluster_harness::NodeIdView> = harness_b
            .nodes
            .iter()
            .map(|n| (n.instance_id, n.owned_vnodes()))
            .collect();

        assert_eq!(assignment_a, assignment_b);
        harness_b.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn checkpoint_records_durable_commit_marker() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        let leader = &harness.nodes[harness.leader_idx()];
        let result = leader.db.checkpoint().await.expect("leader checkpoint");
        assert!(result.success, "leader checkpoint: {:?}", result.error);

        assert!(
            leader
                .decision_store
                .is_committed(result.epoch)
                .await
                .expect("marker read"),
            "commit marker must exist for the just-completed epoch",
        );
        for idx in harness.follower_idxs() {
            assert!(harness.nodes[idx]
                .decision_store
                .is_committed(result.epoch)
                .await
                .unwrap());
        }

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn start_wires_backend_fence_from_snapshot() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

        for node in &harness.nodes {
            assert_eq!(node.state_backend.authoritative_version(), 0);
        }

        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        for node in &harness.nodes {
            let registry_v = node.vnode_registry.assignment_version();
            let backend_v = node.state_backend.authoritative_version();
            assert!(registry_v > 0);
            assert_eq!(backend_v, registry_v);
        }

        use bytes::Bytes;
        use laminar_core::state::StateBackendError;
        let node = &harness.nodes[0];
        let authoritative = node.state_backend.authoritative_version();
        let stale_caller = authoritative - 1;
        let err = node
            .state_backend
            .write_partial(0, 9_999, stale_caller, Bytes::from_static(b"stale"))
            .await
            .expect_err("stale write must be rejected by the live fence");
        match err {
            StateBackendError::StaleVersion {
                caller,
                authoritative: got,
            } => {
                assert_eq!(caller, stale_caller);
                assert_eq!(got, authoritative);
            }
            other => panic!("expected StaleVersion, got {other:?}"),
        }

        harness.shutdown().await;
    }

    async fn setup_query(db: &laminar_db::LaminarDB) {
        db.execute("CREATE SOURCE src (key BIGINT, value BIGINT)")
            .await
            .expect("CREATE SOURCE");
        db.execute(
            "CREATE MATERIALIZED VIEW sums AS \
             SELECT key, SUM(value) AS total FROM src GROUP BY key",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW");
    }

    async fn union_sums(harness: &ClusterEngineHarness) -> Vec<(i64, i64)> {
        let mut out = Vec::new();
        for node in &harness.nodes {
            out.extend(read_mv_sums(&node.db, "sums").await);
        }
        out
    }

    /// A hard crash sheds the dead node's vnodes to the survivor, which rehydrates their
    /// checkpointed state and takes over their keys. Rows in flight at the crash are lost —
    /// the at-least-once failover window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn crash_sheds_vnodes_to_survivor() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];

        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        let owners = vec![
            (
                harness.nodes[leader_idx].instance_id,
                harness.nodes[leader_idx].owned_vnodes(),
            ),
            (
                harness.nodes[follower_idx].instance_id,
                harness.nodes[follower_idx].owned_vnodes(),
            ),
        ];
        let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 4)
            .expect("pick_keys_per_owner: search range too small");
        let follower_keys = key_buckets[1].1.clone();

        let phase_a: Vec<i64> = key_buckets
            .iter()
            .flat_map(|(_, ks)| ks.iter().copied())
            .collect();
        let src = harness.nodes[leader_idx]
            .db
            .source_untyped("src")
            .expect("source_untyped");
        src.push_arrow(input_batch(&phase_a)).expect("push phase_a");
        harness.nodes[leader_idx]
            .db
            .checkpoint()
            .await
            .expect("leader checkpoint phase_a");
        sleep(Duration::from_millis(500)).await;

        let pre_crash = union_sums(&harness).await;
        assert_eq!(
            pre_crash.len(),
            8,
            "want 8 rows total; duplicates here would signal a key on >1 node",
        );

        let crashed_runtime = harness.nodes.swap_remove(follower_idx);
        let crashed_node = harness.cluster.nodes.swap_remove(follower_idx);
        drop(crashed_runtime);
        crashed_node.crash().await;

        // Wait for rotation to hand every vnode to the survivor.
        // Detection is time-based (phi-accrual Suspected flip →
        // debounce → rotation → rehydration), so a fixed sleep flakes
        // under parallel test load.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while harness.nodes[0].owned_vnodes().len() < VNODE_COUNT as usize {
            assert!(
                std::time::Instant::now() < deadline,
                "survivor never acquired the crashed node's vnodes",
            );
            sleep(Duration::from_millis(200)).await;
        }

        let phase_c = input_batch(&follower_keys);
        let src = harness.nodes[0]
            .db
            .source_untyped("src")
            .expect("source_untyped on surviving leader");
        src.push_arrow(phase_c).expect("push phase_c");

        // Rotation handed the crashed node's vnodes to the survivor,
        // which rehydrated their phase-A state and processed phase C
        // for them — so the survivor now serves EVERY key, and the
        // crashed node's keys total phase A + phase C (`input_batch`
        // pushes value = key*10). Asserting TOTALS, not just presence:
        // a lost rehydration would still show the key (phase C creates
        // the group) but with only phase C's contribution.
        let mut expected: std::collections::HashMap<i64, i64> =
            key_buckets[0].1.iter().map(|&k| (k, k * 10)).collect();
        for &k in &follower_keys {
            expected.insert(k, k * 10 * 2);
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let got: std::collections::HashMap<i64, i64> =
                read_mv_sums(&harness.nodes[0].db, "sums")
                    .await
                    .into_iter()
                    .collect();
            if got == expected {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "survivor never served all recovered totals: got {got:?}, want {expected:?}",
            );
            sleep(Duration::from_millis(200)).await;
        }

        harness.shutdown().await;
    }

    // Delta chain is the PRIMARY aggregate checkpoint; the manifest has none, so the
    // survivor recovers the crashed node's keys from the per-vnode chain; doubled totals prove it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delta_primary_crash_rehydrates_aggregate_from_chain() {
        let mut harness = ClusterEngineHarness::spawn_delta(N_NODES, VNODE_COUNT, 2).await;
        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        let owners = vec![
            (
                harness.nodes[leader_idx].instance_id,
                harness.nodes[leader_idx].owned_vnodes(),
            ),
            (
                harness.nodes[follower_idx].instance_id,
                harness.nodes[follower_idx].owned_vnodes(),
            ),
        ];
        let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 4).expect("pick keys");
        let follower_keys = key_buckets[1].1.clone();
        let phase_a: Vec<i64> = key_buckets
            .iter()
            .flat_map(|(_, ks)| ks.iter().copied())
            .collect();

        harness.nodes[leader_idx]
            .db
            .source_untyped("src")
            .expect("src")
            .push_arrow(input_batch(&phase_a))
            .expect("push phase_a");
        harness.nodes[leader_idx]
            .db
            .checkpoint()
            .await
            .expect("checkpoint phase_a");
        sleep(Duration::from_millis(500)).await;

        let crashed_runtime = harness.nodes.swap_remove(follower_idx);
        let crashed_node = harness.cluster.nodes.swap_remove(follower_idx);
        drop(crashed_runtime);
        crashed_node.crash().await;

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while harness.nodes[0].owned_vnodes().len() < VNODE_COUNT as usize {
            assert!(
                std::time::Instant::now() < deadline,
                "survivor never acquired the crashed node's vnodes",
            );
            sleep(Duration::from_millis(200)).await;
        }

        harness.nodes[0]
            .db
            .source_untyped("src")
            .expect("src on survivor")
            .push_arrow(input_batch(&follower_keys))
            .expect("push phase_c");

        let mut expected: std::collections::HashMap<i64, i64> =
            key_buckets[0].1.iter().map(|&k| (k, k * 10)).collect();
        for &k in &follower_keys {
            expected.insert(k, k * 10 * 2);
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let got: std::collections::HashMap<i64, i64> =
                read_mv_sums(&harness.nodes[0].db, "sums")
                    .await
                    .into_iter()
                    .collect();
            if got == expected {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "delta_primary crash recovery totals wrong: got {got:?}, want {expected:?}",
            );
            sleep(Duration::from_millis(200)).await;
        }
        harness.shutdown().await;
    }

    // Graceful full-cluster restart, delta_primary: the manifest holds no aggregate state, so each
    // node rehydrates its OWN vnodes from the chain (start_inner staging); re-fed keys double.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delta_primary_aggregate_survives_graceful_restart() {
        let mut harness = ClusterEngineHarness::spawn_delta(N_NODES, VNODE_COUNT, 2).await;
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        let owners: Vec<_> = harness
            .nodes
            .iter()
            .map(|n| (n.instance_id, n.owned_vnodes()))
            .collect();
        let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 3).expect("pick keys");
        let keys: Vec<i64> = key_buckets
            .iter()
            .flat_map(|(_, ks)| ks.iter().copied())
            .collect();

        let leader = harness.leader_idx();
        harness.nodes[leader]
            .db
            .source_untyped("src")
            .expect("src")
            .push_arrow(input_batch(&keys))
            .expect("push batch1");
        harness.nodes[leader]
            .db
            .checkpoint()
            .await
            .expect("checkpoint batch1");
        sleep(Duration::from_millis(500)).await;

        let baseline: std::collections::HashMap<i64, i64> =
            union_sums(&harness).await.into_iter().collect();
        for &k in &keys {
            assert_eq!(
                baseline.get(&k),
                Some(&(k * 10)),
                "baseline total for key {k}"
            );
        }

        // Graceful full-cluster restart, delta_primary still on.
        let (shared, cp_dirs) = harness.shutdown_keep_dirs().await;
        let mut harness = ClusterEngineHarness::spawn_with_dirs(
            N_NODES,
            VNODE_COUNT,
            shared,
            cp_dirs,
            Some(2),
            None,
        )
        .await;
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        // Re-feed the same keys: doubled totals iff the accumulators recovered from the chain.
        let leader = harness.leader_idx();
        harness.nodes[leader]
            .db
            .source_untyped("src")
            .expect("src")
            .push_arrow(input_batch(&keys))
            .expect("push batch2");

        let expected: std::collections::HashMap<i64, i64> =
            keys.iter().map(|&k| (k, k * 10 * 2)).collect();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let got: std::collections::HashMap<i64, i64> = union_sums(&harness)
                .await
                .into_iter()
                .filter(|(k, _)| keys.contains(k))
                .collect();
            if got == expected {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "delta_primary graceful-restart totals wrong: got {got:?}, want {expected:?}",
            );
            sleep(Duration::from_millis(200)).await;
        }
        harness.shutdown().await;
    }

    /// Exact-value correctness under cluster GROUP demotion: a deterministic keyed SUM across a
    /// 2-node cluster with a tiny per-node budget (idle groups demote to cold, re-touched ones
    /// promote back) must match the analytic per-key totals — the exact-value guarantee the kill-9
    /// soaks omit, since they assert only demote/promote counters + EO density. Keys span both
    /// owners so the demote↔promote path runs under cross-node shuffle + barrier alignment.
    #[cfg(feature = "state-tier")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_group_demotion_preserves_aggregate_values() {
        use std::collections::HashMap;
        use std::time::Instant;

        // chain_max=2 enables the delta chain (group demotion needs it); 2 KiB/node forces demotion.
        let mut harness =
            ClusterEngineHarness::spawn_delta_tier(N_NODES, VNODE_COUNT, 2, 2048).await;
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        let owners: Vec<_> = harness
            .nodes
            .iter()
            .map(|n| (n.instance_id, n.owned_vnodes()))
            .collect();
        let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 96).expect("pick keys");
        let all_keys: Vec<i64> = key_buckets
            .iter()
            .flat_map(|(_, ks)| ks.iter().copied())
            .collect();
        let leader = harness.leader_idx();

        // Three rounds over every key with idle gaps (so clean groups become demotable), then a
        // checkpoint to mark them clean — the demotion precondition + delta-tracking seed.
        const ROUNDS: i64 = 3;
        for _ in 0..ROUNDS {
            harness.nodes[leader]
                .db
                .source_untyped("src")
                .expect("src")
                .push_arrow(input_batch(&all_keys))
                .expect("push round");
            sleep(Duration::from_millis(150)).await;
        }
        harness.nodes[leader]
            .db
            .checkpoint()
            .await
            .expect("checkpoint");

        let demotes = |h: &ClusterEngineHarness| -> u64 {
            h.nodes
                .iter()
                .map(|n| n.db.tier_metrics().demote_total)
                .sum()
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        while demotes(&harness) == 0 {
            assert!(
                Instant::now() < deadline,
                "cluster group demotion never fired (tier never shed an idle group)",
            );
            sleep(Duration::from_millis(200)).await;
        }

        // Re-touch the first half once more → demoted groups promote back (fetch-on-access) and bump.
        let half: Vec<i64> = all_keys.iter().take(all_keys.len() / 2).copied().collect();
        harness.nodes[leader]
            .db
            .source_untyped("src")
            .expect("src")
            .push_arrow(input_batch(&half))
            .expect("push promote round");

        // value = key*10; fed ROUNDS times, plus once more for the re-touched half.
        let mut expected: HashMap<i64, i64> =
            all_keys.iter().map(|&k| (k, k * 10 * ROUNDS)).collect();
        for &k in &half {
            *expected.get_mut(&k).expect("half ⊆ all_keys") += k * 10;
        }

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let got: HashMap<i64, i64> = union_sums(&harness).await.into_iter().collect();
            if got == expected {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cluster group demotion changed aggregate values (demotes={}): {:?}",
                demotes(&harness),
                expected
                    .iter()
                    .filter(|(k, v)| got.get(k) != Some(v))
                    .map(|(k, v)| (*k, *v, got.get(k).copied()))
                    .take(10)
                    .collect::<Vec<_>>(),
            );
            sleep(Duration::from_millis(200)).await;
        }
        assert!(
            demotes(&harness) > 0,
            "demote/promote path must be exercised"
        );
        harness.shutdown().await;
    }

    /// Non-chain group demotion (no delta chain — the whole-node manifest is authoritative and
    /// demoted groups fold into cold-only partials) must recover EXACT values across a full cluster
    /// restart. A resident∩cold overlap at capture would double-count here (additive merge).
    #[cfg(feature = "state-tier")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_group_demotion_nonchain_survives_restart() {
        use std::collections::HashMap;
        use std::time::Instant;
        use tempfile::TempDir;

        let shared = tempfile::tempdir().expect("shared state dir");
        let cp_dirs: Vec<TempDir> = (0..N_NODES)
            .map(|_| tempfile::tempdir().expect("cp dir"))
            .collect();
        let mut harness = ClusterEngineHarness::spawn_with_dirs(
            N_NODES,
            VNODE_COUNT,
            shared,
            cp_dirs,
            None,
            Some(2048),
        )
        .await;
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        let owners: Vec<_> = harness
            .nodes
            .iter()
            .map(|n| (n.instance_id, n.owned_vnodes()))
            .collect();
        let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 96).expect("pick keys");
        let all_keys: Vec<i64> = key_buckets
            .iter()
            .flat_map(|(_, ks)| ks.iter().copied())
            .collect();
        let leader = harness.leader_idx();
        let demotes = |h: &ClusterEngineHarness| -> u64 {
            h.nodes
                .iter()
                .map(|n| n.db.tier_metrics().demote_total)
                .sum()
        };

        for _ in 0..3 {
            harness.nodes[leader]
                .db
                .source_untyped("src")
                .expect("src")
                .push_arrow(input_batch(&all_keys))
                .expect("push");
            sleep(Duration::from_millis(150)).await;
        }
        harness.nodes[leader]
            .db
            .checkpoint()
            .await
            .expect("checkpoint");
        let deadline = Instant::now() + Duration::from_secs(60);
        while demotes(&harness) == 0 {
            assert!(
                Instant::now() < deadline,
                "non-chain group demotion never fired"
            );
            sleep(Duration::from_millis(200)).await;
        }
        // Checkpoint AGAIN so the demoted groups are captured into durable cold-only partials.
        harness.nodes[leader]
            .db
            .checkpoint()
            .await
            .expect("checkpoint2");
        let expected: HashMap<i64, i64> = all_keys.iter().map(|&k| (k, k * 10 * 3)).collect();

        // Graceful full-cluster restart (tier wiped; recover from the durable manifest + partials).
        let (shared, cp_dirs) = harness.shutdown_keep_dirs().await;
        let mut harness = ClusterEngineHarness::spawn_with_dirs(
            N_NODES,
            VNODE_COUNT,
            shared,
            cp_dirs,
            None,
            Some(2048),
        )
        .await;
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        // No re-feed: the recovered values must equal the pre-restart totals — never doubled.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let got: HashMap<i64, i64> = union_sums(&harness)
                .await
                .into_iter()
                .filter(|(k, _)| expected.contains_key(k))
                .collect();
            let bad: Vec<(i64, i64, i64)> = expected
                .iter()
                .filter(|(k, &e)| got.get(k).copied() != Some(e))
                .map(|(k, &e)| (*k, e, got.get(k).copied().unwrap_or(-1)))
                .take(10)
                .collect();
            if bad.is_empty() && got.len() == expected.len() {
                break;
            }
            if Instant::now() > deadline {
                let overlap: u64 = harness
                    .nodes
                    .iter()
                    .map(|n| n.db.tier_metrics().overlap_total)
                    .sum();
                panic!(
                    "non-chain cluster demotion recovered wrong values (overlap_total={overlap}) \
                     [(key, expected, got)]: {bad:?}",
                );
            }
            sleep(Duration::from_millis(200)).await;
        }
        harness.shutdown().await;
    }

    /// A crashed node's DEMOTED groups must survive failover. With the cold tier on, the follower
    /// demotes idle groups — their durable home is the per-vnode delta chain, NOT the node-local tier
    /// (which dies with the process). When the follower crashes, the survivor acquires its vnodes and
    /// must rehydrate those demoted groups from the chain. Re-feeding the dead node's keys doubles
    /// their totals iff the prior (demoted) counts recovered; a lost demoted group would instead show
    /// only the re-fed value.
    #[cfg(feature = "state-tier")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_demoted_groups_survive_crash_failover() {
        use std::collections::HashMap;
        use std::time::Instant;

        let mut harness =
            ClusterEngineHarness::spawn_delta_tier(N_NODES, VNODE_COUNT, 2, 2048).await;
        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        let owners = vec![
            (
                harness.nodes[leader_idx].instance_id,
                harness.nodes[leader_idx].owned_vnodes(),
            ),
            (
                harness.nodes[follower_idx].instance_id,
                harness.nodes[follower_idx].owned_vnodes(),
            ),
        ];
        let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 96).expect("pick keys");
        let leader_keys = key_buckets[0].1.clone();
        let follower_keys = key_buckets[1].1.clone();
        let phase_a: Vec<i64> = leader_keys.iter().chain(&follower_keys).copied().collect();

        // Feed everything, checkpoint (clean baseline + delta seed), then wait for the FOLLOWER to
        // demote idle groups; checkpoint again so the demoted state is captured in the durable chain.
        harness.nodes[leader_idx]
            .db
            .source_untyped("src")
            .expect("src")
            .push_arrow(input_batch(&phase_a))
            .expect("push phase_a");
        harness.nodes[leader_idx]
            .db
            .checkpoint()
            .await
            .expect("checkpoint phase_a");
        let deadline = Instant::now() + Duration::from_secs(60);
        while harness.nodes[follower_idx].db.tier_metrics().demote_total == 0 {
            assert!(
                Instant::now() < deadline,
                "follower never demoted a group before the crash",
            );
            sleep(Duration::from_millis(200)).await;
        }
        harness.nodes[leader_idx]
            .db
            .checkpoint()
            .await
            .expect("checkpoint post-demote");
        sleep(Duration::from_millis(300)).await;

        // Crash the follower.
        let crashed_runtime = harness.nodes.swap_remove(follower_idx);
        let crashed_node = harness.cluster.nodes.swap_remove(follower_idx);
        drop(crashed_runtime);
        crashed_node.crash().await;

        // Survivor acquires the crashed node's vnodes.
        let deadline = Instant::now() + Duration::from_secs(30);
        while harness.nodes[0].owned_vnodes().len() < VNODE_COUNT as usize {
            assert!(
                Instant::now() < deadline,
                "survivor never acquired the crashed node's vnodes",
            );
            sleep(Duration::from_millis(200)).await;
        }

        // Re-feed the dead node's keys on the survivor → totals double iff the demoted groups
        // recovered from the chain (a lost demoted group would show only this re-fed value).
        harness.nodes[0]
            .db
            .source_untyped("src")
            .expect("src on survivor")
            .push_arrow(input_batch(&follower_keys))
            .expect("push phase_c");

        let mut expected: HashMap<i64, i64> = leader_keys.iter().map(|&k| (k, k * 10)).collect();
        for &k in &follower_keys {
            expected.insert(k, k * 10 * 2);
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let got: HashMap<i64, i64> = read_mv_sums(&harness.nodes[0].db, "sums")
                .await
                .into_iter()
                .collect();
            if got == expected {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "demoted-group crash-failover totals wrong: {:?}",
                expected
                    .iter()
                    .filter(|(k, v)| got.get(k) != Some(v))
                    .map(|(k, v)| (*k, *v, got.get(k).copied()))
                    .take(10)
                    .collect::<Vec<_>>(),
            );
            sleep(Duration::from_millis(200)).await;
        }
        harness.shutdown().await;
    }

    /// Lose-then-REACQUIRE of a vnode whose aggregate group was demoted to the cold tier must not
    /// double-count on re-acquire and must not lose the group. Node A
    /// (follower) owns V, demotes idle groups and durably folds them into V's delta chain; V then
    /// moves to B (leader) and back to A. On revoke, `drop_vnodes` must purge A's resident AND cold
    /// tracking for V before the additive `merge_groups` rehydrates the chain — otherwise re-acquire
    /// merges the chain onto A's stale state and doubles its totals. Read-back is on A (the final
    /// owner) only, isolating the operator-state property from any cross-node MV cleanup concern.
    #[cfg(feature = "state-tier")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_demoted_group_survives_lose_then_reacquire() {
        use std::collections::BTreeMap;
        use std::sync::Arc;
        use std::time::Instant;

        use laminar_core::cluster::control::{AssignmentSnapshotStore, RotateOutcome};
        use laminar_core::state::NodeId;

        let mut harness =
            ClusterEngineHarness::spawn_delta_tier(N_NODES, VNODE_COUNT, 2, 2048).await;
        let leader_idx = harness.leader_idx(); // B: temporarily gains V
        let follower_idx = harness.follower_idxs()[0]; // A: holds V, demotes, loses then reacquires
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        let owners = vec![
            (
                harness.nodes[leader_idx].instance_id,
                harness.nodes[leader_idx].owned_vnodes(),
            ),
            (
                harness.nodes[follower_idx].instance_id,
                harness.nodes[follower_idx].owned_vnodes(),
            ),
        ];
        let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 96).expect("pick keys");
        let leader_keys = key_buckets[0].1.clone();
        let follower_keys = key_buckets[1].1.clone();
        let phase_a: Vec<i64> = leader_keys.iter().chain(&follower_keys).copied().collect();

        // K is a non-zero follower key (so a doubled total is detectable); V is the vnode it hashes to.
        let k: i64 = *follower_keys
            .iter()
            .find(|&&x| x != 0)
            .expect("a non-zero follower key");
        let v: u32 = super::cluster_harness::vnode_for_key(k, VNODE_COUNT);
        assert!(
            harness.nodes[follower_idx].owned_vnodes().contains(&v),
            "precondition: follower (A) owns V={v} for K={k}",
        );
        let node_a = NodeId(harness.nodes[follower_idx].instance_id.0);
        let node_b = NodeId(harness.nodes[leader_idx].instance_id.0);
        let store: Arc<AssignmentSnapshotStore> =
            Arc::clone(&harness.nodes[0].assignment_snapshot_store);

        // Seed durable demoted state: feed all keys with idle gaps → checkpoint (clean baseline +
        // delta seed) → wait for A to demote → checkpoint AGAIN so the cold-only partial folds into
        // V's chain. Without the second checkpoint, re-acquire rehydrates nothing → loss, not a fix.
        const ROUNDS: i64 = 3;
        for _ in 0..ROUNDS {
            harness.nodes[leader_idx]
                .db
                .source_untyped("src")
                .expect("src")
                .push_arrow(input_batch(&phase_a))
                .expect("push round");
            sleep(Duration::from_millis(150)).await;
        }
        harness.nodes[leader_idx]
            .db
            .checkpoint()
            .await
            .expect("checkpoint phase_a");
        let deadline = Instant::now() + Duration::from_secs(60);
        while harness.nodes[follower_idx].db.tier_metrics().demote_total == 0 {
            assert!(
                Instant::now() < deadline,
                "follower (A) never demoted a group before the rotation",
            );
            sleep(Duration::from_millis(200)).await;
        }
        harness.nodes[leader_idx]
            .db
            .checkpoint()
            .await
            .expect("checkpoint post-demote");
        sleep(Duration::from_millis(300)).await;

        // (1) LOSE: move ONLY V from A -> B, preserving every other vnode->owner mapping.
        let seed = store.load().await.unwrap().unwrap();
        let mut vnodes: BTreeMap<u32, NodeId> = seed
            .to_vnode_vec(VNODE_COUNT)
            .into_iter()
            .enumerate()
            .map(|(i, owner)| (i as u32, owner))
            .collect();
        vnodes.insert(v, node_b);
        let moved = seed.next(vnodes);
        let v_moved = moved.version;
        assert!(matches!(
            store.save_if_version(&moved, seed.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !harness
            .nodes
            .iter()
            .all(|n| n.vnode_registry.assignment_version() >= v_moved)
        {
            assert!(
                Instant::now() < deadline,
                "nodes never adopted the A->B move"
            );
            sleep(Duration::from_millis(100)).await;
        }
        assert!(
            harness.nodes[follower_idx]
                .owned_vnodes()
                .iter()
                .all(|&x| x != v),
            "A must drop V",
        );
        // Poll (don't fixed-sleep) until A's compute thread drains the staged revoke — apply_revoked_vnodes
        // empties pending_revoke while dropping V's state — so the reacquire exercises drop-then-rehydrate.
        let deadline = Instant::now() + Duration::from_secs(5);
        while harness.nodes[follower_idx].db.pending_revoke_vnode_count() > 0 {
            assert!(
                Instant::now() < deadline,
                "A never drained the pending revoke of V",
            );
            sleep(Duration::from_millis(50)).await;
        }

        // (2) REACQUIRE: move V back B -> A (exercises the revoked-state-drop-then-rehydrate path).
        let seed2 = store.load().await.unwrap().unwrap();
        let mut vnodes2: BTreeMap<u32, NodeId> = seed2
            .to_vnode_vec(VNODE_COUNT)
            .into_iter()
            .enumerate()
            .map(|(i, owner)| (i as u32, owner))
            .collect();
        vnodes2.insert(v, node_a);
        let back = seed2.next(vnodes2);
        let v_back = back.version;
        assert!(matches!(
            store.save_if_version(&back, seed2.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !harness
            .nodes
            .iter()
            .all(|n| n.vnode_registry.assignment_version() >= v_back)
        {
            assert!(
                Instant::now() < deadline,
                "nodes never adopted the B->A move-back"
            );
            sleep(Duration::from_millis(100)).await;
        }
        assert!(
            harness.nodes[follower_idx].owned_vnodes().contains(&v),
            "A must re-acquire V",
        );
        // Let A run cycles so apply_rehydrated_vnodes folds the durable chain onto now-empty V.
        sleep(Duration::from_millis(600)).await;

        // (3) Re-feed K once → shuffles to V's owner (A); A re-emits its rehydrated changelog.
        harness.nodes[leader_idx]
            .db
            .source_untyped("src")
            .expect("src")
            .push_arrow(input_batch(&[k]))
            .expect("push K re-feed");

        // K fed ROUNDS times pre-rotation + once after re-acquire. A double-count (chain merged onto
        // un-dropped state) shows ~2x; a lost group shows < expected.
        let expected_k = k * 10 * (ROUNDS + 1);
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let got_k: i64 = read_mv_sums(&harness.nodes[follower_idx].db, "sums")
                .await
                .into_iter()
                .filter(|(kk, _)| *kk == k)
                .map(|(_, t)| t)
                .sum();
            if got_k == expected_k {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "K total after lose-then-reacquire wrong: got {got_k}, want {expected_k} \
                 (double-count or loss across the rotation)",
            );
            sleep(Duration::from_millis(200)).await;
        }

        // A sibling key on a vnode that never moved must be unchanged (single-counted).
        let sibling: i64 = *leader_keys
            .iter()
            .find(|&&x| x != 0)
            .expect("a non-zero leader key");
        let sib_total: i64 = read_mv_sums(&harness.nodes[leader_idx].db, "sums")
            .await
            .into_iter()
            .filter(|(kk, _)| *kk == sibling)
            .map(|(_, t)| t)
            .sum();
        assert_eq!(
            sib_total,
            sibling * 10 * ROUNDS,
            "untouched sibling key changed across the rotation",
        );

        harness.shutdown().await;
    }

    /// A rebalance MOVE (vnode A->B, no move-back) must RETRACT the moved groups from the
    /// LOSING node's incremental MV snapshot. Otherwise A keeps materializing K forever while B also
    /// materializes it, so a distributed (union) read double-counts K. Reads the union across both
    /// nodes and asserts K appears exactly once at the full total, and that A dropped K locally.
    #[cfg(feature = "state-tier")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rebalance_move_retracts_moved_group_from_losing_node() {
        use std::collections::BTreeMap;
        use std::sync::Arc;
        use std::time::Instant;

        use laminar_core::cluster::control::{AssignmentSnapshotStore, RotateOutcome};
        use laminar_core::state::NodeId;

        let mut harness =
            ClusterEngineHarness::spawn_delta_tier(N_NODES, VNODE_COUNT, 2, 2048).await;
        let leader_idx = harness.leader_idx(); // B: gains V
        let follower_idx = harness.follower_idxs()[0]; // A: holds V, then loses it
        for node in &harness.nodes {
            setup_query(&node.db).await;
        }
        harness.start_all().await;

        let owners = vec![
            (
                harness.nodes[leader_idx].instance_id,
                harness.nodes[leader_idx].owned_vnodes(),
            ),
            (
                harness.nodes[follower_idx].instance_id,
                harness.nodes[follower_idx].owned_vnodes(),
            ),
        ];
        let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 96).expect("pick keys");
        let leader_keys = key_buckets[0].1.clone();
        let follower_keys = key_buckets[1].1.clone();
        let phase: Vec<i64> = leader_keys.iter().chain(&follower_keys).copied().collect();

        let k: i64 = *follower_keys
            .iter()
            .find(|&&x| x != 0)
            .expect("a non-zero follower key");
        let v: u32 = super::cluster_harness::vnode_for_key(k, VNODE_COUNT);
        assert!(
            harness.nodes[follower_idx].owned_vnodes().contains(&v),
            "precondition: A owns V={v} for K={k}",
        );
        let node_b = NodeId(harness.nodes[leader_idx].instance_id.0);
        let store: Arc<AssignmentSnapshotStore> =
            Arc::clone(&harness.nodes[0].assignment_snapshot_store);

        // Feed K (and siblings) then checkpoint so B can rehydrate V's aggregate state on acquire.
        const ROUNDS: i64 = 3;
        for _ in 0..ROUNDS {
            harness.nodes[leader_idx]
                .db
                .source_untyped("src")
                .expect("src")
                .push_arrow(input_batch(&phase))
                .expect("push round");
            sleep(Duration::from_millis(150)).await;
        }
        harness.nodes[leader_idx]
            .db
            .checkpoint()
            .await
            .expect("checkpoint baseline");
        sleep(Duration::from_millis(300)).await;

        // Baseline: K materialized exactly once at ROUNDS*k*10 (on A, its owner) across the union.
        let baseline = k * 10 * ROUNDS;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let rows = union_sums(&harness).await;
            let n = rows.iter().filter(|(kk, _)| *kk == k).count();
            let tot: i64 = rows.iter().filter(|(kk, _)| *kk == k).map(|(_, t)| t).sum();
            if n == 1 && tot == baseline {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "baseline K wrong: rows={rows:?} want one row total {baseline}",
            );
            sleep(Duration::from_millis(200)).await;
        }

        // MOVE V: A -> B (one way), preserving every other vnode->owner mapping.
        let seed = store.load().await.unwrap().unwrap();
        let mut vnodes: BTreeMap<u32, NodeId> = seed
            .to_vnode_vec(VNODE_COUNT)
            .into_iter()
            .enumerate()
            .map(|(i, owner)| (i as u32, owner))
            .collect();
        vnodes.insert(v, node_b);
        let moved = seed.next(vnodes);
        let v_moved = moved.version;
        assert!(matches!(
            store.save_if_version(&moved, seed.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !harness
            .nodes
            .iter()
            .all(|n| n.vnode_registry.assignment_version() >= v_moved)
        {
            assert!(Instant::now() < deadline, "nodes never adopted A->B move");
            sleep(Duration::from_millis(100)).await;
        }
        assert!(
            harness.nodes[follower_idx]
                .owned_vnodes()
                .iter()
                .all(|&x| x != v),
            "A must drop V",
        );
        // Let A drain apply_revoked_vnodes (stash the retraction) and B stage the rehydrated chain.
        sleep(Duration::from_millis(600)).await;

        // Push another round: K routes to V's new owner (B), and A's retained vnodes still receive
        // rows so A's operator advances its watermark and flushes the stashed retraction.
        harness.nodes[leader_idx]
            .db
            .source_untyped("src")
            .expect("src")
            .push_arrow(input_batch(&phase))
            .expect("push post-move round");

        // K2 is a sibling follower key on a DIFFERENT vnode than V — one A keeps. It proves the
        // retraction is surgical (only V's groups leave A), and since A never rehydrates, its total
        // settles promptly (unlike a leader key on B, which races B's acquire-rehydrate cycle).
        let k2: i64 = *follower_keys
            .iter()
            .find(|&&x| {
                x != 0 && x != k && super::cluster_harness::vnode_for_key(x, VNODE_COUNT) != v
            })
            .expect("a second follower key on a vnode other than V");

        // Settle everything together on one consistent snapshot: A dropped K (the retraction), the
        // union carries K once at the full rehydrated+new total, and A still owns K2 at its full
        // total. Without the retraction, A keeps K and the union double-counts it.
        let expected_k = k * 10 * (ROUNDS + 1);
        let expected_k2 = k2 * 10 * (ROUNDS + 1);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let a_rows = read_mv_sums(&harness.nodes[follower_idx].db, "sums").await;
            let a_has_k = a_rows.iter().any(|(kk, _)| *kk == k);
            let a_has_k2 = a_rows.iter().any(|(kk, _)| *kk == k2);
            let rows = union_sums(&harness).await;
            let kn = rows.iter().filter(|(kk, _)| *kk == k).count();
            let kt: i64 = rows.iter().filter(|(kk, _)| *kk == k).map(|(_, t)| t).sum();
            let k2n = rows.iter().filter(|(kk, _)| *kk == k2).count();
            let k2t: i64 = rows
                .iter()
                .filter(|(kk, _)| *kk == k2)
                .map(|(_, t)| t)
                .sum();
            if !a_has_k && kn == 1 && kt == expected_k && a_has_k2 && k2n == 1 && k2t == expected_k2
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "after A->B move: A_has_K={a_has_k} K(rows={kn},tot={kt},want {expected_k}) \
                 A_has_K2={a_has_k2} K2(rows={k2n},tot={k2t},want {expected_k2}); rows={rows:?}",
            );
            sleep(Duration::from_millis(250)).await;
        }

        harness.shutdown().await;
    }

    /// [LDB-3006] An incremental changelog join is single-node only; creating one in a multi-node
    /// cluster must be rejected at DDL rather than silently producing per-node-partitioned (wrong)
    /// results. Covers both the two-way form and the single-statement multi-way decomposition.
    #[cfg(feature = "state-tier")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn incremental_join_rejected_in_multi_node_cluster() {
        let harness = ClusterEngineHarness::spawn_delta_tier(N_NODES, VNODE_COUNT, 2, 2048).await;
        let leader = &harness.nodes[harness.leader_idx()].db;
        leader
            .execute("CREATE SOURCE ev_a (k BIGINT, v BIGINT)")
            .await
            .expect("src a");
        leader
            .execute("CREATE SOURCE ev_b (k BIGINT, v BIGINT)")
            .await
            .expect("src b");
        leader
            .execute(
                "CREATE MATERIALIZED VIEW agg_a AS SELECT k, SUM(v) AS ta FROM ev_a GROUP BY k",
            )
            .await
            .expect("agg a");
        leader
            .execute(
                "CREATE MATERIALIZED VIEW agg_b AS SELECT k, SUM(v) AS tb FROM ev_b GROUP BY k",
            )
            .await
            .expect("agg b");

        let two_way = leader
            .execute(
                "CREATE MATERIALIZED VIEW j AS \
                 SELECT a.k, a.ta, b.tb FROM agg_a a JOIN agg_b b ON a.k = b.k",
            )
            .await;
        let err = format!(
            "{:?}",
            two_way.expect_err("two-way incremental join must be rejected in a cluster")
        );
        assert!(err.contains("LDB-3006"), "expected LDB-3006, got: {err}");

        // Single-statement multi-way decomposition is also rejected: each hidden 2-way intermediate
        // hits the same guard, so the CREATE fails (and the atomic unwind removes any intermediate).
        leader
            .execute("CREATE SOURCE ev_c (k BIGINT, v BIGINT)")
            .await
            .expect("src c");
        leader
            .execute(
                "CREATE MATERIALIZED VIEW agg_c AS SELECT k, SUM(v) AS tc FROM ev_c GROUP BY k",
            )
            .await
            .expect("agg c");
        let multi_way = leader
            .execute(
                "CREATE MATERIALIZED VIEW jm AS \
                 SELECT a.k, a.ta, b.tb, c.tc FROM agg_a a \
                 JOIN agg_b b ON a.k = b.k JOIN agg_c c ON b.k = c.k",
            )
            .await;
        let merr = format!(
            "{:?}",
            multi_way.expect_err("multi-way incremental join must be rejected in a cluster")
        );
        assert!(merr.contains("LDB-3006"), "expected LDB-3006, got: {merr}");

        harness.shutdown().await;
    }
}

mod rebalance {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use laminar_core::cluster::control::{AssignmentSnapshotStore, RotateOutcome};
    use laminar_core::state::NodeId;

    use super::cluster_harness::ClusterEngineHarness;

    const VNODE_COUNT: u32 = 4;
    const N_NODES: usize = 2;
    const POLL_DEADLINE: Duration = Duration::from_secs(5);

    async fn wait_for<F: Fn() -> bool>(predicate: F, what: &str) {
        let deadline = Instant::now() + POLL_DEADLINE;
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_watcher_adopts_direct_rotation() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        harness.start_all().await;

        let store: Arc<AssignmentSnapshotStore> =
            Arc::clone(&harness.nodes[0].assignment_snapshot_store);
        let seed = store.load().await.unwrap().unwrap();

        let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
        let mut vnodes = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            vnodes.insert(v, leader);
        }
        let next = seed.next(vnodes);
        let expected = next.version;
        assert!(matches!(
            store.save_if_version(&next, seed.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));

        wait_for(
            || {
                harness
                    .nodes
                    .iter()
                    .all(|n| n.vnode_registry.assignment_version() >= expected)
            },
            "every node to adopt the new version",
        )
        .await;

        for node in &harness.nodes {
            let backend_v = node.state_backend.authoritative_version();
            let registry_v = node.vnode_registry.assignment_version();
            assert!(backend_v >= expected);
            assert_eq!(backend_v, registry_v);
        }
        assert_eq!(
            harness.nodes[harness.leader_idx()].owned_vnodes().len(),
            VNODE_COUNT as usize,
        );

        harness.shutdown().await;
    }

    /// Barrier-aligned handoff: the draining phase marks to-be-lost vnodes draining (source pauses
    /// them for a clean cut) without changing ownership; the commit phase rotates and clears it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_watcher_handles_draining_phase() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        harness.start_all().await;

        let store: Arc<AssignmentSnapshotStore> =
            Arc::clone(&harness.nodes[0].assignment_snapshot_store);
        let seed = store.load().await.unwrap().unwrap();

        let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
        let follower_idx = harness.follower_idxs()[0];
        let lost = harness.nodes[follower_idx].owned_vnodes();
        assert!(
            !lost.is_empty(),
            "test needs the follower to own vnodes to drain"
        );

        // Everything moves to the leader; the follower loses all its vnodes.
        let mut vnodes = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            vnodes.insert(v, leader);
        }

        // Draining snapshot: the follower marks its lost vnodes draining, but ownership
        // (the registry version) does not change.
        let pre_version = harness.nodes[follower_idx]
            .vnode_registry
            .assignment_version();
        let mut drain = seed.next(vnodes.clone());
        drain.draining = true;
        assert!(matches!(
            store.save_if_version(&drain, seed.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));
        wait_for(
            || {
                lost.iter()
                    .all(|&v| harness.nodes[follower_idx].vnode_registry.is_draining(v))
            },
            "follower marks its lost vnodes draining",
        )
        .await;
        assert_eq!(
            harness.nodes[follower_idx]
                .vnode_registry
                .assignment_version(),
            pre_version,
            "drain phase must not change ownership",
        );

        // Committed snapshot: ownership rotates and the drain clears.
        let commit = drain.next(vnodes);
        let expected = commit.version;
        assert!(matches!(
            store.save_if_version(&commit, drain.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));
        wait_for(
            || {
                harness
                    .nodes
                    .iter()
                    .all(|n| n.vnode_registry.assignment_version() >= expected)
            },
            "every node adopts the committed snapshot",
        )
        .await;
        for node in &harness.nodes {
            for v in 0..VNODE_COUNT {
                assert!(
                    !node.vnode_registry.is_draining(v),
                    "draining must clear on commit",
                );
            }
        }
        assert_eq!(
            harness.nodes[harness.leader_idx()].owned_vnodes().len(),
            VNODE_COUNT as usize,
        );

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_rotation_picks_single_winner() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        harness.start_all().await;

        let store: Arc<AssignmentSnapshotStore> =
            Arc::clone(&harness.nodes[0].assignment_snapshot_store);
        let seed = store.load().await.unwrap().unwrap();

        let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
        let follower = NodeId(harness.nodes[harness.follower_idxs()[0]].instance_id.0);
        let mut a_map = BTreeMap::new();
        let mut b_map = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            a_map.insert(v, leader);
            b_map.insert(v, follower);
        }
        let (a, b) = (seed.next(a_map), seed.next(b_map));

        let (ra, rb) = tokio::join!(
            store.save_if_version(&a, seed.version),
            store.save_if_version(&b, seed.version),
        );
        let outcomes = [ra.unwrap(), rb.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|o| matches!(o, RotateOutcome::Rotated))
                .count(),
            1,
        );

        let stored = store.load().await.unwrap().unwrap();
        assert_eq!(stored.version, seed.version + 1);
        for outcome in &outcomes {
            if let RotateOutcome::Conflict(current) = outcome {
                assert_eq!(current, &stored);
            }
        }

        wait_for(
            || {
                harness
                    .nodes
                    .iter()
                    .all(|n| n.vnode_registry.assignment_version() >= stored.version)
            },
            "nodes to adopt the CAS winner",
        )
        .await;

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_rotation_attempt_rejected_by_cas() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        harness.start_all().await;

        let store: Arc<AssignmentSnapshotStore> =
            Arc::clone(&harness.nodes[0].assignment_snapshot_store);
        let seed = store.load().await.unwrap().unwrap();

        let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
        let mut m = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            m.insert(v, leader);
        }
        let next = seed.next(m);
        assert!(matches!(
            store.save_if_version(&next, seed.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));

        let mut stale_map = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            stale_map.insert(v, NodeId(99));
        }
        let stale = seed.next(stale_map);
        match store.save_if_version(&stale, seed.version).await.unwrap() {
            RotateOutcome::Conflict(current) => assert_eq!(current, next),
            RotateOutcome::Rotated => panic!("stale rotation must not succeed"),
        }

        wait_for(
            || {
                harness
                    .nodes
                    .iter()
                    .all(|n| n.vnode_registry.assignment_version() >= next.version)
            },
            "nodes to adopt the legitimate rotation",
        )
        .await;
        for node in &harness.nodes {
            for v in 0..VNODE_COUNT {
                assert_ne!(node.vnode_registry.owner(v).0, 99);
            }
        }

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rebalance_rehydrates_acquired_vnode_state() {
        use bytes::Bytes;

        const SEED_EPOCH: u64 = 50;

        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        harness.start_all().await;

        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];

        let follower_vnodes = harness.nodes[follower_idx].owned_vnodes();
        assert!(!follower_vnodes.is_empty(), "follower must own some vnodes");

        let backend = Arc::clone(&harness.nodes[leader_idx].state_backend);
        let version = backend.authoritative_version();
        let all_vnodes: Vec<u32> = (0..VNODE_COUNT).collect();
        for &v in &all_vnodes {
            backend
                .write_partial(v, SEED_EPOCH, version, Bytes::from(format!("vnode-{v}")))
                .await
                .expect("seed write_partial");
        }
        assert!(
            backend
                .epoch_complete(SEED_EPOCH, &all_vnodes, &[])
                .await
                .expect("seal seed epoch"),
            "seed epoch must seal once every vnode partial is present",
        );

        let store: Arc<AssignmentSnapshotStore> =
            Arc::clone(&harness.nodes[0].assignment_snapshot_store);
        let seed = store.load().await.unwrap().unwrap();
        let leader = NodeId(harness.nodes[leader_idx].instance_id.0);
        let mut m = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            m.insert(v, leader);
        }
        let rotated = seed.next(m);
        assert!(matches!(
            store.save_if_version(&rotated, seed.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));

        wait_for(
            || {
                harness.nodes[leader_idx]
                    .vnode_registry
                    .assignment_version()
                    >= rotated.version
            },
            "leader to adopt the rotation",
        )
        .await;

        wait_for(
            || {
                let staged = harness.nodes[leader_idx].db.rehydrated_vnode_state();
                follower_vnodes.iter().all(|v| staged.contains_key(v))
            },
            "leader to stage rehydrated state for acquired vnodes",
        )
        .await;

        let staged = harness.nodes[leader_idx].db.rehydrated_vnode_state();
        let staged_keys: std::collections::BTreeSet<u32> = staged.keys().copied().collect();
        let expected: std::collections::BTreeSet<u32> = follower_vnodes.iter().copied().collect();
        assert_eq!(
            staged_keys, expected,
            "leader must rehydrate exactly its newly-acquired vnodes",
        );
        for &v in &follower_vnodes {
            let entry = staged.get(&v).expect("acquired vnode staged");
            assert_eq!(entry.epoch, SEED_EPOCH);
            assert_eq!(
                entry.chain.len(),
                1,
                "simple seed resolves to a single-link chain"
            );
            assert_eq!(&entry.chain[0][..], format!("vnode-{v}").as_bytes());
        }

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn checkpoint_after_rotation_carries_new_version() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

        for node in &harness.nodes {
            node.db
                .execute("CREATE SOURCE src (key BIGINT, value BIGINT)")
                .await
                .expect("CREATE SOURCE");
            node.db
                .execute(
                    "CREATE MATERIALIZED VIEW sums AS \
                     SELECT key, SUM(value) AS total FROM src GROUP BY key",
                )
                .await
                .expect("CREATE MATERIALIZED VIEW");
        }
        harness.start_all().await;

        let store: Arc<AssignmentSnapshotStore> =
            Arc::clone(&harness.nodes[0].assignment_snapshot_store);
        let seed = store.load().await.unwrap().unwrap();

        let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
        let mut m = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            m.insert(v, leader);
        }
        let rotated = seed.next(m);
        assert!(matches!(
            store.save_if_version(&rotated, seed.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));

        wait_for(
            || {
                harness
                    .nodes
                    .iter()
                    .all(|n| n.vnode_registry.assignment_version() >= rotated.version)
            },
            "adoption of rotated snapshot",
        )
        .await;

        let result = harness.nodes[harness.leader_idx()]
            .db
            .checkpoint()
            .await
            .expect("checkpoint");
        assert!(
            result.success,
            "post-rotation checkpoint: {:?}",
            result.error
        );

        assert!(harness.nodes[harness.leader_idx()]
            .decision_store
            .is_committed(result.epoch)
            .await
            .unwrap());

        harness.shutdown().await;
    }
}

mod two_pc {
    use std::sync::Arc;
    use std::time::Duration;

    use laminar_core::cluster::control::{BarrierAnnouncement, CheckpointDecisionStore, Phase};
    use laminar_core::cluster::testing::MiniCluster;
    use laminar_core::state::{
        owned_vnodes, InProcessBackend, NodeId, StateBackend, VnodeRegistry,
    };
    use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;
    use laminar_db::checkpoint_coordinator::{
        CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
    };
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use tempfile::TempDir;

    const CONVERGENCE: Duration = Duration::from_secs(5);

    async fn make_coord(
        dir: &std::path::Path,
        backend: Arc<InProcessBackend>,
        vnodes: Vec<u32>,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) -> CheckpointCoordinator {
        let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        coord.set_state_backend(backend);
        coord.set_vnode_set(vnodes);
        coord.set_cluster_controller(controller);
        coord
    }

    fn make_decision_store(dir: &TempDir) -> Arc<CheckpointDecisionStore> {
        let os: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(dir.path())
                .expect("LocalFileSystem for decision store"),
        );
        Arc::new(CheckpointDecisionStore::new(os))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_node_leader_commits_follower_mirrors() {
        let cluster = MiniCluster::spawn(2).await;
        cluster
            .wait_for_convergence(CONVERGENCE)
            .await
            .expect("cluster converges");

        let (leader_node, follower_node) = if cluster.nodes[0].controller.is_leader() {
            (&cluster.nodes[0], &cluster.nodes[1])
        } else {
            (&cluster.nodes[1], &cluster.nodes[0])
        };

        let backend = Arc::new(InProcessBackend::new(4));
        let registry = VnodeRegistry::new(4);
        registry.set_assignment(
            vec![
                NodeId(leader_node.instance_id.0),
                NodeId(leader_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
            ]
            .into(),
        );

        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let decision_dir = tempfile::tempdir().unwrap();
        let decision_store = make_decision_store(&decision_dir);

        let mut leader_coord = make_coord(
            leader_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(leader_node.instance_id.0)),
            Arc::clone(&leader_node.controller),
        )
        .await;
        leader_coord.set_decision_store(Arc::clone(&decision_store));
        let mut follower_coord = make_coord(
            follower_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
            Arc::clone(&follower_node.controller),
        )
        .await;
        follower_coord.set_decision_store(Arc::clone(&decision_store));

        let ann = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };

        let follower_handle = tokio::spawn(async move {
            follower_coord
                .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(15))
                .await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let leader_result = leader_coord
            .checkpoint(CheckpointRequest::default())
            .await
            .expect("leader checkpoint call");
        assert!(
            leader_result.success,
            "leader checkpoint must succeed: {:?}",
            leader_result.error
        );

        let committed = follower_handle
            .await
            .expect("follower task join")
            .expect("follower checkpoint Result");
        assert!(committed, "follower must commit on leader's Commit");

        for v in 0..4 {
            assert!(backend.read_partial(v, 1).await.unwrap().is_some());
        }

        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_records_commit_decision_before_announce() {
        let cluster = MiniCluster::spawn(2).await;
        cluster.wait_for_convergence(CONVERGENCE).await.unwrap();
        let (leader_node, follower_node) = if cluster.nodes[0].controller.is_leader() {
            (&cluster.nodes[0], &cluster.nodes[1])
        } else {
            (&cluster.nodes[1], &cluster.nodes[0])
        };

        let backend = Arc::new(InProcessBackend::new(4));
        let registry = VnodeRegistry::new(4);
        registry.set_assignment(
            vec![
                NodeId(leader_node.instance_id.0),
                NodeId(leader_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
            ]
            .into(),
        );

        let decision_dir = tempfile::tempdir().unwrap();
        let decision_store = make_decision_store(&decision_dir);

        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let mut leader_coord = make_coord(
            leader_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(leader_node.instance_id.0)),
            Arc::clone(&leader_node.controller),
        )
        .await;
        leader_coord.set_decision_store(Arc::clone(&decision_store));
        let mut follower_coord = make_coord(
            follower_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
            Arc::clone(&follower_node.controller),
        )
        .await;
        follower_coord.set_decision_store(Arc::clone(&decision_store));

        let ann = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let follower_handle = tokio::spawn(async move {
            follower_coord
                .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(15))
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let leader_result = leader_coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(
            leader_result.success,
            "leader checkpoint: {:?}",
            leader_result.error
        );
        let _ = follower_handle.await.unwrap().unwrap();

        assert!(
            decision_store.is_committed(1).await.unwrap(),
            "leader must record commit marker before announcing Commit",
        );

        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn follower_timeout_commits_when_decision_recorded() {
        let cluster = MiniCluster::spawn(2).await;
        cluster.wait_for_convergence(CONVERGENCE).await.unwrap();
        let follower_node = if cluster.nodes[0].controller.is_leader() {
            &cluster.nodes[1]
        } else {
            &cluster.nodes[0]
        };

        let backend = Arc::new(InProcessBackend::new(4));
        let registry = VnodeRegistry::new(4);
        registry.set_assignment(vec![NodeId(follower_node.instance_id.0); 4].into());

        let decision_dir = tempfile::tempdir().unwrap();
        let decision_store = make_decision_store(&decision_dir);
        decision_store.record_committed(42).await.unwrap();

        let follower_dir = tempfile::tempdir().unwrap();
        let mut follower_coord = make_coord(
            follower_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
            Arc::clone(&follower_node.controller),
        )
        .await;
        follower_coord.set_decision_store(Arc::clone(&decision_store));

        let ann = BarrierAnnouncement {
            epoch: 42,
            checkpoint_id: 100,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let committed = follower_coord
            .follower_checkpoint(
                CheckpointRequest::default(),
                ann,
                Duration::from_millis(500),
            )
            .await
            .unwrap();

        assert!(committed);
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn follower_timeout_rolls_back_when_no_decision() {
        let cluster = MiniCluster::spawn(2).await;
        cluster.wait_for_convergence(CONVERGENCE).await.unwrap();
        let follower_node = if cluster.nodes[0].controller.is_leader() {
            &cluster.nodes[1]
        } else {
            &cluster.nodes[0]
        };

        let backend = Arc::new(InProcessBackend::new(4));
        let registry = VnodeRegistry::new(4);
        registry.set_assignment(vec![NodeId(follower_node.instance_id.0); 4].into());

        let decision_dir = tempfile::tempdir().unwrap();
        let decision_store = make_decision_store(&decision_dir);

        let follower_dir = tempfile::tempdir().unwrap();
        let mut follower_coord = make_coord(
            follower_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
            Arc::clone(&follower_node.controller),
        )
        .await;
        follower_coord.set_decision_store(Arc::clone(&decision_store));

        let ann = BarrierAnnouncement {
            epoch: 99,
            checkpoint_id: 200,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let committed = follower_coord
            .follower_checkpoint(
                CheckpointRequest::default(),
                ann,
                Duration::from_millis(500),
            )
            .await
            .unwrap();
        assert!(!committed);
        cluster.shutdown().await;
    }
}

mod minio {
    use std::sync::Arc;
    use std::time::Duration;

    use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
    use laminar_core::cluster::testing::MiniCluster;
    use laminar_core::state::{
        owned_vnodes, rendezvous_assignment, NodeId, ObjectStoreBackend, VnodeRegistry,
    };
    use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;
    use laminar_db::checkpoint_coordinator::{
        CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
    };
    use object_store::ObjectStoreExt;

    use super::common::{minio_endpoint, minio_store};

    const CONVERGENCE: Duration = Duration::from_secs(5);

    async fn make_coord(
        dir: &std::path::Path,
        backend: Arc<ObjectStoreBackend>,
        vnodes: Vec<u32>,
        gate_vnodes: Vec<u32>,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) -> CheckpointCoordinator {
        let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        coord.set_state_backend(backend);
        coord.set_vnode_set(vnodes);
        coord.set_gate_vnode_set(gate_vnodes);
        coord.set_cluster_controller(controller);
        coord
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_node_minio_leader_commits_follower_mirrors() {
        if minio_endpoint().is_none() {
            eprintln!("skipping: MinIO not reachable at 127.0.0.1:19000");
            return;
        }
        let cluster = MiniCluster::spawn(2).await;
        cluster
            .wait_for_convergence(CONVERGENCE)
            .await
            .expect("cluster converges");

        let (leader_node, follower_node) = if cluster.nodes[0].controller.is_leader() {
            (&cluster.nodes[0], &cluster.nodes[1])
        } else {
            (&cluster.nodes[1], &cluster.nodes[0])
        };

        let bucket = format!(
            "laminar-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let store = minio_store(&bucket).await;

        let leader_backend = Arc::new(ObjectStoreBackend::new(
            Arc::clone(&store),
            leader_node.instance_id.0.to_string(),
            4,
        ));
        let follower_backend = Arc::new(ObjectStoreBackend::new(
            Arc::clone(&store),
            follower_node.instance_id.0.to_string(),
            4,
        ));

        let registry = VnodeRegistry::new(4);
        let peers = [
            NodeId(leader_node.instance_id.0),
            NodeId(follower_node.instance_id.0),
        ];
        registry.set_assignment(rendezvous_assignment(4, &peers));

        let leader_owned = owned_vnodes(&registry, NodeId(leader_node.instance_id.0));
        let follower_owned = owned_vnodes(&registry, NodeId(follower_node.instance_id.0));
        assert_eq!(leader_owned.len() + follower_owned.len(), 4);
        let full = (0..4).collect::<Vec<_>>();

        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let mut leader_coord = make_coord(
            leader_dir.path(),
            leader_backend,
            leader_owned,
            full.clone(),
            Arc::clone(&leader_node.controller),
        )
        .await;
        let mut follower_coord = make_coord(
            follower_dir.path(),
            follower_backend,
            follower_owned,
            full,
            Arc::clone(&follower_node.controller),
        )
        .await;

        let ann = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };

        let follower_handle = tokio::spawn(async move {
            follower_coord
                .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(20))
                .await
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        let leader_result = leader_coord
            .checkpoint(CheckpointRequest::default())
            .await
            .expect("leader checkpoint");
        assert!(
            leader_result.success,
            "leader checkpoint over MinIO must succeed: {:?}",
            leader_result.error,
        );

        let committed = follower_handle.await.expect("join").expect("follower");
        assert!(committed, "follower must commit on leader's Commit");

        for v in 0..4 {
            let path = object_store::path::Path::from(format!("epoch=1/vnode={v}/partial.bin"));
            let meta = store.head(&path).await;
            assert!(meta.is_ok(), "missing partial for vnode {v}: {meta:?}");
        }
        let commit_path = object_store::path::Path::from("epoch=1/_COMMIT");
        assert!(
            store.head(&commit_path).await.is_ok(),
            "missing epoch=1/_COMMIT marker on MinIO",
        );

        cluster.shutdown().await;
    }

    /// Coordinated-commit descriptors written by two nodes to shared MinIO seal
    /// the leader's gate only when both are present, and the leader reads both
    /// back for the designated committer to aggregate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_node_coordinated_descriptors_aggregate_on_leader() {
        use bytes::Bytes;
        use laminar_core::state::StateBackend as _;

        if minio_endpoint().is_none() {
            eprintln!("skipping: MinIO not reachable at 127.0.0.1:19000");
            return;
        }
        let bucket = format!(
            "laminar-coord-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let store = minio_store(&bucket).await;
        let node1 = ObjectStoreBackend::new(Arc::clone(&store), "1".to_string(), 4);
        let node2 = ObjectStoreBackend::new(Arc::clone(&store), "2".to_string(), 4);

        let full = [0u32, 1, 2, 3];
        let required = ["node=1/sink=s".to_string(), "node=2/sink=s".to_string()];

        // Node 1 writes its vnode slice and its commit descriptor.
        for v in [0u32, 1] {
            node1
                .write_partial(v, 1, 0, Bytes::from_static(b"a"))
                .await
                .unwrap();
        }
        node1
            .write_commit_descriptor(1, "node=1/sink=s", 0, Bytes::from_static(b"d1"))
            .await
            .unwrap();

        // Leader cannot seal yet — node 2's partials and descriptor are missing.
        assert!(!node1.epoch_complete(1, &full, &required).await.unwrap());

        // Node 2 writes its slice and descriptor to the same bucket.
        for v in [2u32, 3] {
            node2
                .write_partial(v, 1, 0, Bytes::from_static(b"b"))
                .await
                .unwrap();
        }
        node2
            .write_commit_descriptor(1, "node=2/sink=s", 0, Bytes::from_static(b"d2"))
            .await
            .unwrap();

        // Now the leader seals: all partials and both descriptors are durable.
        assert!(node1.epoch_complete(1, &full, &required).await.unwrap());

        // The leader reads both nodes' descriptors for the committer to aggregate.
        let mut descriptors = node1.read_commit_descriptors(1).await.unwrap();
        descriptors.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            descriptors,
            vec![
                ("node=1/sink=s".to_string(), Bytes::from_static(b"d1")),
                ("node=2/sink=s".to_string(), Bytes::from_static(b"d2")),
            ]
        );
    }
}
