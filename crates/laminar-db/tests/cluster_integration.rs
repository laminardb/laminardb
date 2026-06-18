#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)]
//! Unified cluster integration tests.

#[path = "common/cluster_harness.rs"]
mod cluster_harness;

#[path = "common/mod.rs"]
mod common;

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

    /// A hard crash sheds the dead node's vnodes to the survivor,
    /// which rehydrates their checkpointed state and takes over their
    /// keys (rows in flight at the crash are lost — the at-least-once
    /// failover window). Before dead-node rotation engaged, the
    /// survivor kept only its own keys and the cluster could not
    /// commit at all until the node returned.
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
            assert_eq!(&entry.bytes[..], format!("vnode-{v}").as_bytes());
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
