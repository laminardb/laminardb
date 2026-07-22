#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)]
//! Unified cluster integration tests.

#[path = "common/cluster_harness.rs"]
mod cluster_harness;

#[path = "common/minio.rs"]
mod common;

fn test_assignment_fence(
    cluster: &laminar_core::cluster::testing::MiniCluster,
    registry: &laminar_core::state::VnodeRegistry,
) -> laminar_core::checkpoint::CheckpointAssignmentFence {
    let snapshot = registry.versioned_snapshot();
    let owners = snapshot
        .owners()
        .iter()
        .map(|owner| owner.0)
        .collect::<Vec<_>>();
    let owner_set = owners
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut participants = cluster
        .nodes
        .iter()
        .filter(|node| owner_set.contains(&node.instance_id.0))
        .map(|node| laminar_core::checkpoint::CheckpointParticipant {
            node_id: node.instance_id.0,
            boot_incarnation: node.controller.recovery_incarnation(),
        })
        .collect::<Vec<_>>();
    participants.sort_unstable_by_key(|participant| participant.node_id);
    laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
        snapshot.version(),
        &owners,
        participants,
    )
    .unwrap()
}

mod durable_backend_gate {
    use std::sync::Arc;

    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv, LeaseDeadline};
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
        let controller = Arc::new(ClusterController::new(self_id, Arc::clone(&kv), None, rx));
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(30),
            )))
            .expect("install the process authority deadline required by cluster construction");

        let checkpoint_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let participant = laminar_core::checkpoint::CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: controller.recovery_incarnation(),
        };
        let verified_namespaces =
            laminar_core::cluster::control::prove_shared_object_store_namespaces(
                participant,
                &[participant],
                kv,
                Arc::clone(&checkpoint_store),
                Arc::clone(&checkpoint_store),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap();
        let err = LaminarDB::builder()
            .cluster_controller(controller)
            .verified_cluster_namespaces(verified_namespaces)
            .state_backend(Arc::new(InProcessBackend::new(4)))
            .vnode_registry(Arc::new(VnodeRegistry::new(4)))
            .build()
            .await
            .expect_err("cluster construction must reject a non-durable backend");
        assert!(
            err.to_string().contains("LDB-0011"),
            "expected LDB-0011 durable-backend error, got: {err}"
        );
    }
}

mod failures {
    use super::cluster_harness::ClusterEngineHarness;

    const VNODE_COUNT: u32 = 4;
    const N_NODES: usize = 2;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn split_brain_write_partial_rejected() {
        use bytes::Bytes;
        use laminar_core::state::{
            CheckpointAttempt, ObjectStoreBackend, StateBackend, StateBackendError,
        };
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;

        let dir = tempfile::tempdir().expect("tempdir");
        let store: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"));

        let fresh = ObjectStoreBackend::cluster_shared(std::sync::Arc::clone(&store), "leader", 4);
        let stale =
            ObjectStoreBackend::cluster_shared(std::sync::Arc::clone(&store), "ex-leader", 4);

        fresh.set_authoritative_version(3);
        let attempt = CheckpointAttempt::new(1, 1);
        fresh
            .write_partial(attempt, 0, 3, Bytes::from_static(b"fresh"))
            .await
            .expect("fresh write at current version");

        stale.set_authoritative_version(3);
        let err = stale
            .write_partial(attempt, 0, 2, Bytes::from_static(b"stale"))
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

        let got = fresh.read_partial(attempt, 0).await.unwrap().unwrap();
        assert_eq!(&got[..], b"fresh");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn assignment_snapshot_unifies_cluster_view() {
        let mut harness_a =
            super::cluster_harness::ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        setup_stateless_query(&harness_a).await;
        harness_a.start_all().await;
        let checkpoint = harness_a.nodes[harness_a.leader_idx()]
            .db
            .checkpoint()
            .await
            .expect("checkpoint the assignment before restarting its process owners");
        assert!(
            checkpoint.success,
            "assignment checkpoint failed: {:?}",
            checkpoint.error,
        );
        let assignment_a: Vec<super::cluster_harness::NodeIdView> = harness_a
            .nodes
            .iter()
            .map(|n| (n.instance_id, n.owned_vnodes()))
            .collect();
        let (shared_dir, checkpoint_dirs, control_store) = harness_a.shutdown_keep_dirs().await;

        let mut harness_b = super::cluster_harness::ClusterEngineHarness::spawn_with_dirs(
            N_NODES,
            VNODE_COUNT,
            shared_dir,
            checkpoint_dirs,
            control_store,
        )
        .await;
        harness_b.start_all().await;
        let assignment_b: Vec<super::cluster_harness::NodeIdView> = harness_b
            .nodes
            .iter()
            .map(|n| (n.instance_id, n.owned_vnodes()))
            .collect();

        assert_eq!(assignment_a, assignment_b);
        harness_b.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn zero_vnode_workers_start_idle_without_joining_assignment_quorum() {
        let mut harness = ClusterEngineHarness::spawn(3, 1).await;
        setup_stateless_query(&harness).await;
        tokio::time::timeout(std::time::Duration::from_secs(20), harness.start_all())
            .await
            .expect("idle workers must not block cluster startup");

        let snapshot = harness.nodes[0]
            .assignment_snapshot_store
            .load()
            .await
            .unwrap()
            .unwrap();
        let owner = snapshot.to_vnode_vec(1).unwrap()[0];
        assert_eq!(snapshot.participants.len(), 1);
        assert_eq!(snapshot.participants[0].node_id, owner.0);

        for node in &harness.nodes {
            let owns_vnode = node.instance_id == owner;
            assert_eq!(node.owned_vnodes().len(), if owns_vnode { 1 } else { 0 });
            assert_eq!(node.db.cluster_intake_fenced(), !owns_vnode);
            if !owns_vnode {
                assert_eq!(node.shuffle_sender.assignment_version(), 0);
                assert_eq!(node.shuffle_receiver.assignment_version(), 0);
            }
        }

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sealed_materialized_view_manifest_is_rejected_by_every_node_after_restart() {
        use laminar_core::cluster::control::{
            CatalogManifest, CatalogManifestEntry, CatalogObjectKind,
        };

        const SOURCE_DDL: &str = "CREATE SOURCE src (key BIGINT, value BIGINT)";
        const VIEW_DDL: &str =
            "CREATE MATERIALIZED VIEW totals AS SELECT key, SUM(value) FROM src GROUP BY key";

        let harness = ClusterEngineHarness::spawn(3, VNODE_COUNT).await;
        let manifest = CatalogManifest::new(vec![
            CatalogManifestEntry {
                canonical_name: "src".into(),
                kind: CatalogObjectKind::Source,
                ddl: SOURCE_DDL.into(),
            },
            CatalogManifestEntry {
                canonical_name: "totals".into(),
                kind: CatalogObjectKind::MaterializedView,
                ddl: VIEW_DDL.into(),
            },
        ])
        .expect("fault manifest is structurally valid");
        let leader = harness.leader_idx();
        let proof = harness.cluster.nodes[leader]
            .controller
            .capture_catalog_bootstrap_proof()
            .expect("active durable leader proof");
        harness
            .catalog_manifest_store
            .seal(&manifest, &proof)
            .await
            .expect("inject structurally valid but unsupported catalog manifest");

        assert_cluster_nodes_reject_materialized_manifest(&harness).await;
        let (shared_dir, checkpoint_dirs, control_store) = harness.shutdown_keep_dirs().await;

        let restarted = ClusterEngineHarness::spawn_with_dirs(
            3,
            VNODE_COUNT,
            shared_dir,
            checkpoint_dirs,
            control_store,
        )
        .await;
        assert_eq!(
            restarted
                .catalog_manifest_store
                .load()
                .await
                .expect("load injected manifest"),
            Some(manifest),
        );
        assert_cluster_nodes_reject_materialized_manifest(&restarted).await;
        restarted.shutdown().await;
    }

    async fn assert_cluster_nodes_reject_materialized_manifest(harness: &ClusterEngineHarness) {
        for node in &harness.nodes {
            let error = node
                .db
                .start()
                .await
                .expect_err("cluster materialized-view manifest must fail closed");
            assert!(error.to_string().contains("LDB-4007"), "{error}");
            assert!(
                node.db.sources().is_empty(),
                "failed replay left source residue on node {}",
                node.instance_id.0,
            );
            assert!(
                node.db.materialized_views().is_empty(),
                "failed replay left materialized state on node {}",
                node.instance_id.0,
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn checkpoint_records_durable_commit_marker() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        setup_stateless_query(&harness).await;
        harness.start_all().await;

        let leader = &harness.nodes[harness.leader_idx()];
        let result = leader.db.checkpoint().await.expect("leader checkpoint");
        assert!(result.success, "leader checkpoint: {:?}", result.error);

        for node in &harness.cluster.nodes {
            let outcome = node
                .controller
                .checkpoint_authority()
                .expect("cluster checkpoint authority")
                .cluster_outcome(result.epoch)
                .await
                .expect("cluster outcome read")
                .expect("commit outcome must exist before checkpoint success");
            assert!(outcome.is_commit());
            assert_eq!(outcome.checkpoint_id, result.checkpoint_id);
        }

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn start_wires_backend_fence_from_snapshot() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

        for node in &harness.nodes {
            assert_eq!(node.state_backend.authoritative_version(), 0);
        }

        setup_stateless_query(&harness).await;
        harness.start_all().await;

        for node in &harness.nodes {
            let registry_v = node.vnode_registry.assignment_version();
            let backend_v = node.state_backend.authoritative_version();
            assert!(registry_v > 0);
            assert_eq!(backend_v, registry_v);
        }

        use bytes::Bytes;
        use laminar_core::state::{CheckpointAttempt, StateBackendError};
        let node = &harness.nodes[0];
        let authoritative = node.state_backend.authoritative_version();
        let stale_caller = authoritative - 1;
        let err = node
            .state_backend
            .write_partial(
                CheckpointAttempt::new(9_999, 9_999),
                0,
                stale_caller,
                Bytes::from_static(b"stale"),
            )
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

    async fn setup_stateless_query(harness: &ClusterEngineHarness) {
        harness
            .bootstrap_catalog(&[
                super::cluster_harness::TEST_SOURCE_DDL,
                "CREATE STREAM projected AS SELECT key, value FROM src",
            ])
            .await
            .expect("bootstrap cluster catalog");
    }
}

mod rebalance {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use laminar_core::checkpoint::{CheckpointParticipant, LeaderProof};
    use laminar_core::cluster::control::{
        AssignmentDrainDecision, AssignmentDrainVerdict, AssignmentSnapshot,
        AssignmentSnapshotStore, RotateOutcome,
    };
    use laminar_core::state::{CheckpointAttempt, NodeId};

    use super::cluster_harness::{AggregateObservation, ClusterEngineHarness};

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

    fn successor_participants(
        predecessor: &AssignmentSnapshot,
        vnodes: &BTreeMap<u32, NodeId>,
    ) -> Vec<CheckpointParticipant> {
        let owners = vnodes
            .values()
            .map(|owner| owner.0)
            .collect::<BTreeSet<_>>();
        let participants = predecessor
            .participants
            .iter()
            .copied()
            .filter(|participant| owners.contains(&participant.node_id))
            .collect::<Vec<_>>();
        assert_eq!(
            participants.len(),
            owners.len(),
            "every successor owner must retain its exact live process incarnation",
        );
        participants
    }

    fn canonical_successor(
        predecessor: &AssignmentSnapshot,
        vnodes: BTreeMap<u32, NodeId>,
    ) -> AssignmentSnapshot {
        let participants = successor_participants(predecessor, &vnodes);
        predecessor
            .next_for_participants(vnodes, participants)
            .expect("canonical participant-aware successor")
    }

    async fn stop_placement_drivers(harness: &mut ClusterEngineHarness) {
        for node in &mut harness.nodes {
            let placement_driver = node
                .rebalance_tasks
                .pop()
                .expect("cluster harness starts one placement driver after each watcher");
            placement_driver.abort();
            let _ = placement_driver.await;
        }
    }

    async fn begin_planned_rotation(
        harness: &mut ClusterEngineHarness,
        vnodes: BTreeMap<u32, NodeId>,
    ) -> (AssignmentSnapshot, LeaderProof) {
        stop_placement_drivers(harness).await;
        let leader_idx = harness.leader_idx();
        let store = Arc::clone(&harness.nodes[leader_idx].assignment_snapshot_store);
        let predecessor = store
            .load()
            .await
            .expect("load predecessor assignment")
            .expect("predecessor assignment");
        let participants = successor_participants(&predecessor, &vnodes);
        let leader_proof = harness.cluster.nodes[leader_idx]
            .controller
            .capture_leader_proof()
            .expect("planned rotation requires the exact live leader grant");
        let draining = predecessor
            .next_draining(vnodes, participants, leader_proof.clone())
            .expect("canonical planned rotation");
        assert!(matches!(
            store
                .save_if_version(&draining, predecessor.version)
                .await
                .expect("publish draining assignment"),
            RotateOutcome::Rotated,
        ));

        let transition = draining
            .drain_transition
            .as_ref()
            .expect("draining assignment carries its exact transition")
            .clone();
        tokio::time::timeout(POLL_DEADLINE, async {
            loop {
                match harness.cluster.nodes[leader_idx]
                    .controller
                    .drain_ack_quorum_reached(&transition)
                    .await
                {
                    Ok(true) => break,
                    Ok(false) => tokio::time::sleep(Duration::from_millis(50)).await,
                    Err(error) => panic!("drain acknowledgement audit failed: {error}"),
                }
            }
        })
        .await
        .expect("every predecessor must acknowledge the planned source cut");

        (draining, leader_proof)
    }

    async fn authorize_planned_rotation(
        harness: &ClusterEngineHarness,
        draining: &AssignmentSnapshot,
        leader_proof: &LeaderProof,
    ) -> AssignmentSnapshot {
        let transition = draining
            .drain_transition
            .as_ref()
            .expect("draining assignment carries its exact transition");
        let decision = AssignmentDrainDecision::new(
            transition,
            leader_proof.clone(),
            AssignmentDrainVerdict::Commit,
        )
        .expect("canonical planned-rotation decision");
        harness.cluster.nodes[harness.leader_idx()]
            .controller
            .checkpoint_authority()
            .expect("cluster checkpoint authority")
            .record_assignment_drain_decision(leader_proof, decision)
            .await
            .expect("record immutable planned-rotation decision");
        draining
            .committed_target()
            .expect("materialize committed planned rotation")
    }

    async fn finalize_planned_rotation(
        harness: &ClusterEngineHarness,
        draining: &AssignmentSnapshot,
        committed: &AssignmentSnapshot,
    ) {
        let store = &harness.nodes[harness.leader_idx()].assignment_snapshot_store;
        assert!(matches!(
            store
                .finalize_drain(draining, committed)
                .await
                .expect("finalize planned rotation"),
            RotateOutcome::Rotated,
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dead_aggregate_owner_advances_to_a_successor_recovery_quorum() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, 1).await;
        harness
            .bootstrap_catalog(&[
                super::cluster_harness::TEST_SOURCE_DDL,
                "CREATE STREAM totals AS SELECT SUM(value) AS total FROM src EMIT CHANGES",
                super::cluster_harness::TEST_AGGREGATE_SINK_DDL,
            ])
            .await
            .expect("bootstrap stateful cluster catalog");
        harness.start_all().await;

        let leader = harness.leader_idx();
        let store = Arc::clone(&harness.nodes[leader].assignment_snapshot_store);
        let predecessor = store.load().await.unwrap().unwrap();
        let aggregate_owner = predecessor
            .vnodes
            .get(&0)
            .copied()
            .expect("global aggregate state is assigned through vnode zero");
        let failed_index = harness
            .nodes
            .iter()
            .position(|node| node.instance_id == aggregate_owner)
            .expect("aggregate owner belongs to the running harness");

        harness.source_log.append(&[(1, 10), (2, 20)]);
        let expected_observation = AggregateObservation {
            node_id: aggregate_owner,
            total: 30,
            weight: 1,
        };
        if tokio::time::timeout(POLL_DEADLINE, async {
            while !harness
                .sink_state
                .observations()
                .contains(&expected_observation)
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_err()
        {
            panic!(
                "aggregate owner did not publish {expected_observation:?}; observations={:?}; nodes={:?}",
                harness.sink_state.observations(),
                harness
                    .nodes
                    .iter()
                    .map(|node| (
                        node.instance_id,
                        node.owned_vnodes(),
                        node.source_state.polls(),
                        node.source_state.last_checkpoint_cursor(),
                        node.db.cluster_intake_fenced(),
                        node.db.pipeline_state(),
                        node.db.last_fault(),
                    ))
                    .collect::<Vec<_>>(),
            );
        }

        let committed = harness.nodes[leader]
            .db
            .checkpoint()
            .await
            .expect("seed cluster checkpoint");
        assert!(
            committed.success,
            "seed cluster checkpoint failed: {:?}",
            committed.error
        );
        let committed_attempt = CheckpointAttempt::new(committed.epoch, committed.checkpoint_id);
        assert_eq!(
            harness.nodes[failed_index]
                .source_state
                .last_checkpoint_cursor(),
            2,
            "the committed cut must own both scripted input rows"
        );
        let observations_at_commit = harness.sink_state.observations();
        assert!(
            observations_at_commit
                .iter()
                .all(|observation| observation.node_id == aggregate_owner),
            "only the vnode owner may consume the scripted split before failover: {observations_at_commit:?}"
        );
        let mut baseline_relation = std::collections::BTreeMap::<i64, i64>::new();
        for observation in &observations_at_commit {
            *baseline_relation.entry(observation.total).or_default() += observation.weight;
        }
        baseline_relation.retain(|_, weight| *weight != 0);
        assert_eq!(
            baseline_relation,
            std::collections::BTreeMap::from([(30, 1)]),
            "the committed aggregate relation must contain exactly one total 30"
        );
        assert!(
            harness.nodes[failed_index].source_state.polls() > 0,
            "the vnode-zero owner must be the process that consumed the scripted split"
        );
        for (index, node) in harness.nodes.iter().enumerate() {
            if index != failed_index {
                assert_eq!(
                    node.source_state.polls(),
                    0,
                    "a non-owner source process must never poll the shared split"
                );
            }
        }

        harness.source_log.block_resumes();
        let survivor_index = (0..harness.nodes.len())
            .find(|index| *index != failed_index)
            .expect("two-node harness has one survivor");
        let resume_starts_before_failure =
            harness.nodes[survivor_index].source_state.resume_starts();
        let stopped = harness.fail_node_runtime(failed_index).await;
        assert_eq!(stopped, aggregate_owner);
        assert_eq!(harness.nodes.len(), 1);
        let survivor_id = harness.nodes[0].instance_id;
        let survivor_db = Arc::clone(&harness.nodes[0].db);
        let survivor_registry = Arc::clone(&harness.nodes[0].vnode_registry);
        let survivor_source_state = Arc::clone(&harness.nodes[0].source_state);
        let successor_version = predecessor.version + 1;

        tokio::time::timeout(Duration::from_secs(45), async {
            while survivor_source_state.resume_starts() <= resume_starts_before_failure {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("successor source must install its durable cursor before intake opens");
        assert_eq!(
            survivor_source_state.last_resume_cursor(),
            2,
            "successor must resume after the committed input, not replay from zero"
        );
        assert!(
            survivor_db.cluster_intake_fenced(),
            "recovery must retain the source-intake fence while Resume is blocked"
        );
        assert!(
            harness.cluster.nodes[0].controller.is_recovering(),
            "the controller must remain in recovery while Resume is blocked"
        );
        let polls_while_fenced = survivor_source_state.polls();
        assert_eq!(
            harness.sink_state.observations(),
            observations_at_commit,
            "a fenced successor must not emit aggregate output"
        );

        harness.source_log.release_resumes();
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                let durable = store.load().await.unwrap().unwrap();
                let recovered = durable.version == successor_version
                    && !durable.draining
                    && durable.participants.len() == 1
                    && durable.participants[0].node_id == survivor_id.0
                    && durable.vnodes.values().all(|owner| *owner == survivor_id)
                    && survivor_registry.assignment_version() == successor_version
                    && harness.cluster.nodes[0]
                        .controller
                        .checkpoint_assignment_fence(successor_version)
                        .is_some_and(|fence| {
                            fence.participants == durable.participants
                                && fence.matches_owner_map(
                                    &durable
                                        .vnodes
                                        .values()
                                        .map(|owner| owner.0)
                                        .collect::<Vec<_>>(),
                                )
                        })
                    && harness.cluster.nodes[0].controller.is_leader()
                    && !harness.cluster.nodes[0].controller.is_recovering()
                    && !survivor_db.cluster_intake_fenced()
                    && survivor_source_state.polls() > polls_while_fenced;
                if recovered {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("successor owners must restore and resume serving");

        let durable = store.load().await.unwrap().unwrap();
        let survivor_controller = &harness.cluster.nodes[0].controller;
        let decision = survivor_controller
            .checkpoint_authority()
            .expect("cluster checkpoint authority")
            .assignment_recovery_decision(successor_version)
            .await
            .expect("recovery decision read")
            .expect("successor must have a durable recovery decision");
        assert_eq!(
            decision.predecessor,
            predecessor.assignment_fence().unwrap()
        );
        assert_eq!(decision.target, durable.assignment_fence().unwrap());
        assert_eq!(
            store
                .load_recovery_proposal(&decision.proposal)
                .await
                .expect("staged recovery proposal"),
            durable
        );
        assert_eq!(decision.removed_process_fences.len(), 1);
        let removed = &decision.removed_process_fences[0];
        assert_eq!(removed.predecessor.node, aggregate_owner);
        assert_eq!(
            removed.predecessor.owner,
            predecessor
                .participants
                .iter()
                .find(|participant| participant.node_id == aggregate_owner.0)
                .expect("failed owner belongs to predecessor roster")
                .boot_incarnation
        );
        assert!(
            survivor_controller
                .verify_process_lease_fence(
                    removed,
                    tokio::time::Instant::now() + Duration::from_secs(2),
                )
                .await
                .expect("failed-owner process fence verification"),
            "the recovery decision must retain a verifiable takeover certificate"
        );

        let assignment = survivor_registry.versioned_snapshot();
        assert_eq!(
            assignment.source_handoff_attempt(),
            Some(committed_attempt),
            "new ownership must be restored from the last committed cluster cut"
        );
        harness.source_log.append(&[(3, 7)]);
        if tokio::time::timeout(POLL_DEADLINE, async {
            loop {
                let observations = harness.sink_state.observations();
                if observations.contains(&AggregateObservation {
                    node_id: survivor_id,
                    total: 30,
                    weight: -1,
                }) && observations.contains(&AggregateObservation {
                    node_id: survivor_id,
                    total: 37,
                    weight: 1,
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_err()
        {
            panic!(
                "successor did not retract restored total 30 and publish continued total 37; observations={:?}; polls={}; cursor={}; restoring={}; intake_fenced={}; state={:?}; fault={:?}",
                harness.sink_state.observations(),
                survivor_source_state.polls(),
                survivor_source_state.last_checkpoint_cursor(),
                survivor_registry.is_restoring(0),
                survivor_db.cluster_intake_fenced(),
                survivor_db.pipeline_state(),
                survivor_db.last_fault(),
            );
        }

        let post_recovery = survivor_db
            .checkpoint()
            .await
            .expect("checkpoint after successor recovery");
        assert!(
            post_recovery.success,
            "post-recovery checkpoint failed: {:?}",
            post_recovery.error
        );
        assert!(post_recovery.epoch > committed.epoch);
        assert_eq!(
            survivor_source_state.last_checkpoint_cursor(),
            3,
            "post-recovery checkpoint must advance the connector-local cursor once"
        );
        let observations = harness.sink_state.observations();
        assert_eq!(
            &observations[observations_at_commit.len()..],
            &[
                AggregateObservation {
                    node_id: survivor_id,
                    total: 30,
                    weight: -1,
                },
                AggregateObservation {
                    node_id: survivor_id,
                    total: 37,
                    weight: 1,
                },
            ],
            "the post-barrier tail must contain no replay or stale-owner write"
        );

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_watcher_adopts_direct_rotation() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        harness
            .bootstrap_catalog(&[
                super::cluster_harness::TEST_SOURCE_DDL,
                "CREATE STREAM totals AS SELECT SUM(value) AS total FROM src EMIT CHANGES",
                super::cluster_harness::TEST_AGGREGATE_SINK_DDL,
            ])
            .await
            .expect("bootstrap cluster catalog");
        harness.start_all().await;

        let old_owner = harness.nodes[0].vnode_registry.owner(0);
        let target_owner = harness
            .nodes
            .iter()
            .map(|node| node.instance_id)
            .find(|node| *node != old_owner)
            .expect("two-node harness has a distinct transfer target");
        harness.source_log.append(&[(1, 10), (2, 20)]);
        wait_for(
            || {
                harness
                    .sink_state
                    .observations()
                    .contains(&AggregateObservation {
                        node_id: old_owner,
                        total: 30,
                        weight: 1,
                    })
            },
            "predecessor aggregate output",
        )
        .await;

        let mut vnodes = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            vnodes.insert(v, target_owner);
        }
        let (draining, leader_proof) = begin_planned_rotation(&mut harness, vnodes).await;
        let cut = harness.nodes[harness.leader_idx()]
            .db
            .checkpoint()
            .await
            .expect("checkpoint the acknowledged predecessor cut");
        assert!(cut.success, "pre-rotation checkpoint: {:?}", cut.error);
        let observations_before_transfer = harness.sink_state.observations();
        let next = authorize_planned_rotation(&harness, &draining, &leader_proof).await;
        let expected = next.version;
        finalize_planned_rotation(&harness, &draining, &next).await;

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
        wait_for(
            || {
                harness
                    .nodes
                    .iter()
                    .find(|node| node.instance_id == target_owner)
                    .is_some_and(|node| !node.vnode_registry.is_restoring(0))
            },
            "transfer target to apply the restored vnode baseline",
        )
        .await;
        assert_eq!(
            harness.sink_state.observations(),
            observations_before_transfer,
            "physical ownership movement must not change the logical stream"
        );

        harness.source_log.append(&[(3, 7)]);
        wait_for(
            || {
                let observations = harness.sink_state.observations();
                observations.contains(&AggregateObservation {
                    node_id: target_owner,
                    total: 30,
                    weight: -1,
                }) && observations.contains(&AggregateObservation {
                    node_id: target_owner,
                    total: 37,
                    weight: 1,
                })
            },
            "transferred aggregate to continue from its restored baseline",
        )
        .await;
        let post_rotation = harness.nodes[harness.leader_idx()]
            .db
            .checkpoint()
            .await
            .expect("checkpoint after transferred input");
        assert!(
            post_rotation.success,
            "post-rotation checkpoint: {:?}",
            post_rotation.error
        );
        let committed = harness.cluster.nodes[harness.leader_idx()]
            .controller
            .checkpoint_authority()
            .expect("cluster checkpoint authority")
            .cluster_outcome(post_rotation.epoch)
            .await
            .expect("cluster outcome read")
            .expect("post-rotation checkpoint outcome");
        assert!(committed.is_commit());
        assert_eq!(
            committed
                .assignment_fence
                .as_ref()
                .map(|fence| fence.assignment_version),
            Some(expected),
            "post-rotation checkpoint must certify the adopted assignment generation"
        );
        assert_eq!(
            &harness.sink_state.observations()[observations_before_transfer.len()..],
            &[
                AggregateObservation {
                    node_id: target_owner,
                    total: 30,
                    weight: -1,
                },
                AggregateObservation {
                    node_id: target_owner,
                    total: 37,
                    weight: 1,
                },
            ],
            "the transfer and committed source handoff must produce one logical continuation"
        );

        for node in &harness.nodes {
            let backend_v = node.state_backend.authoritative_version();
            let registry_v = node.vnode_registry.assignment_version();
            assert!(backend_v >= expected);
            assert_eq!(backend_v, registry_v);
        }
        assert_eq!(
            harness
                .nodes
                .iter()
                .find(|node| node.instance_id == target_owner)
                .expect("transfer target remains live")
                .owned_vnodes()
                .len(),
            VNODE_COUNT as usize,
        );
        let mut relation = BTreeMap::<i64, i64>::new();
        for observation in harness.sink_state.observations() {
            *relation.entry(observation.total).or_default() += observation.weight;
        }
        relation.retain(|_, weight| *weight != 0);
        assert_eq!(relation, BTreeMap::from([(37, 1)]));

        harness.shutdown().await;
    }

    /// Barrier-aligned handoff: the draining phase holds every predecessor source at one frontier
    /// without changing ownership; the commit phase rotates and releases the cut.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_watcher_handles_draining_phase() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        harness
            .bootstrap_catalog(&[
                super::cluster_harness::TEST_SOURCE_DDL,
                "CREATE STREAM projected AS SELECT key, value FROM src",
            ])
            .await
            .expect("bootstrap cluster catalog");
        harness.start_all().await;
        // This test drives the durable assignment head directly. Retain the watchers under test
        // and stop the independent placement drivers so they cannot abort the injected drain.
        stop_placement_drivers(&mut harness).await;

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

        // Draining snapshot: every predecessor process adopts the global source-cut transition,
        // but ownership (the registry version) does not change.
        let pre_version = harness.nodes[follower_idx]
            .vnode_registry
            .assignment_version();
        let leader_proof = harness.cluster.nodes[harness.leader_idx()]
            .controller
            .capture_leader_proof()
            .expect("test leader must hold the durable lease");
        let leader_participant = seed
            .participants
            .iter()
            .find(|participant| participant.node_id == leader.0)
            .copied()
            .expect("leader belongs to the predecessor roster");
        let drain = seed
            .next_draining(
                vnodes.clone(),
                vec![leader_participant],
                leader_proof.clone(),
            )
            .unwrap();
        assert!(matches!(
            store.save_if_version(&drain, seed.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));
        let transition = drain.drain_transition.clone();
        wait_for(
            || {
                harness.cluster.nodes[follower_idx]
                    .controller
                    .checkpoint_drain_transition()
                    == transition
            },
            "follower adopts the global source-cut transition",
        )
        .await;
        let exact_transition = transition
            .as_ref()
            .expect("draining assignment has a transition");
        tokio::time::timeout(POLL_DEADLINE, async {
            loop {
                match harness.cluster.nodes[harness.leader_idx()]
                    .controller
                    .drain_ack_quorum_reached(exact_transition)
                    .await
                {
                    Ok(true) => break,
                    Ok(false) => tokio::time::sleep(Duration::from_millis(50)).await,
                    Err(error) => panic!("drain acknowledgement audit failed: {error}"),
                }
            }
        })
        .await
        .expect("every predecessor watcher must acknowledge the global source cut");
        assert_eq!(
            harness.nodes[follower_idx]
                .vnode_registry
                .assignment_version(),
            pre_version,
            "drain phase must not change ownership",
        );

        let checkpoint = harness.nodes[harness.leader_idx()]
            .db
            .checkpoint()
            .await
            .expect("pre-rotation checkpoint");
        assert!(
            checkpoint.success,
            "pre-rotation checkpoint must consume the held FIFO barriers: {:?}",
            checkpoint.error
        );
        let leader_idx = harness.leader_idx();
        let held_polls = harness.nodes[leader_idx].source_state.polls();
        assert_eq!(harness.nodes[leader_idx].source_state.drain_finishes(), 0);
        let watcher = harness.nodes[leader_idx].rebalance_tasks.remove(0);
        watcher.abort();
        let _ = watcher.await;

        // Committed snapshot: ownership rotates and the drain clears.
        let commit = drain.committed_target().unwrap();
        let expected = commit.version;
        let decision = laminar_core::cluster::control::AssignmentDrainDecision::new(
            drain.drain_transition.as_ref().unwrap(),
            leader_proof.clone(),
            laminar_core::cluster::control::AssignmentDrainVerdict::Commit,
        )
        .unwrap();
        harness.cluster.nodes[harness.leader_idx()]
            .controller
            .checkpoint_authority()
            .unwrap()
            .record_assignment_drain_decision(&leader_proof, decision)
            .await
            .unwrap();
        assert!(matches!(
            store.finalize_drain(&drain, &commit).await.unwrap(),
            RotateOutcome::Rotated,
        ));
        let restarted_watcher = laminar_db::rebalance::spawn_snapshot_watcher(
            Arc::clone(&harness.nodes[leader_idx].db),
            Arc::clone(&harness.nodes[leader_idx].assignment_snapshot_store),
            Arc::clone(&harness.nodes[leader_idx].vnode_registry),
            harness.nodes[leader_idx].rebalance_shutdown.clone(),
            laminar_db::rebalance::RebalanceConfig::test_defaults(),
            Some(Arc::clone(&harness.cluster.nodes[leader_idx].controller)),
        );
        harness.nodes[leader_idx]
            .rebalance_tasks
            .push(restarted_watcher);
        let adoption_deadline = Instant::now() + POLL_DEADLINE;
        while Instant::now() < adoption_deadline
            && !harness
                .nodes
                .iter()
                .all(|node| node.vnode_registry.assignment_version() >= expected)
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let observed_versions: Vec<_> = harness
            .nodes
            .iter()
            .map(|node| node.vnode_registry.assignment_version())
            .collect();
        assert!(
            observed_versions.iter().all(|version| *version >= expected),
            "every node must adopt committed assignment {expected}; observed {observed_versions:?}"
        );
        wait_for(
            || {
                harness
                    .cluster
                    .nodes
                    .iter()
                    .all(|node| node.controller.checkpoint_drain_transition().is_none())
            },
            "every watcher resolves the global source cut",
        )
        .await;
        wait_for(
            || {
                harness.nodes[leader_idx].source_state.drain_finishes() == 1
                    && harness.nodes[leader_idx].source_state.polls() > held_polls
                    && !harness.nodes[leader_idx].db.cluster_intake_fenced()
            },
            "fresh leader watcher to resolve durable source history before reopening intake",
        )
        .await;
        let drain_resolutions = harness.nodes[leader_idx].source_state.drain_resolutions();
        assert_eq!(drain_resolutions.len(), 1);
        assert_eq!(
            drain_resolutions[0].outcome,
            laminar_connectors::connector::SourceDrainOutcome::Commit,
            "the connector must observe the durable handoff outcome"
        );
        assert_eq!(
            harness.nodes[harness.leader_idx()].owned_vnodes().len(),
            VNODE_COUNT as usize,
        );

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_rotation_picks_single_winner() {
        let harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

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
        let (a, b) = (
            canonical_successor(&seed, a_map),
            canonical_successor(&seed, b_map),
        );

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
                assert_eq!(current.as_ref(), &stored);
            }
        }

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_rotation_attempt_rejected_by_cas() {
        let harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

        let store: Arc<AssignmentSnapshotStore> =
            Arc::clone(&harness.nodes[0].assignment_snapshot_store);
        let seed = store.load().await.unwrap().unwrap();

        let winner = NodeId(
            seed.participants
                .first()
                .expect("seed must certify the cluster participants")
                .node_id,
        );
        let stale_owner = NodeId(
            seed.participants
                .iter()
                .find(|participant| participant.node_id != winner.0)
                .expect("test requires a second certified participant")
                .node_id,
        );
        let mut m = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            m.insert(v, winner);
        }
        let next = canonical_successor(&seed, m);
        assert!(matches!(
            store.save_if_version(&next, seed.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));

        let mut stale_map = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            stale_map.insert(v, stale_owner);
        }
        let stale = canonical_successor(&seed, stale_map);
        match store.save_if_version(&stale, seed.version).await.unwrap() {
            RotateOutcome::Conflict(current) => assert_eq!(*current, next),
            RotateOutcome::Rotated => panic!("stale rotation must not succeed"),
        }
        assert_eq!(store.load().await.unwrap().unwrap(), next);

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rebalance_rehydrates_acquired_vnode_state() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

        // Drive the production checkpoint path so the fixture contains a participant-ready
        // inventory and durable decision as well as valid encoded vnode partials. A bare seal is
        // only prepared state and must never be an assignment handoff cut.
        harness
            .bootstrap_catalog(&[
                super::cluster_harness::TEST_SOURCE_DDL,
                "CREATE STREAM projected AS SELECT key, value FROM src",
            ])
            .await
            .expect("bootstrap cluster catalog");
        harness.start_all().await;

        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];

        let follower_vnodes = harness.nodes[follower_idx].owned_vnodes();
        assert!(!follower_vnodes.is_empty(), "follower must own some vnodes");

        let leader = NodeId(harness.nodes[leader_idx].instance_id.0);
        let mut target = BTreeMap::new();
        for vnode in 0..VNODE_COUNT {
            target.insert(vnode, leader);
        }
        let (draining, leader_proof) = begin_planned_rotation(&mut harness, target).await;

        let checkpoint = harness.nodes[leader_idx]
            .db
            .checkpoint()
            .await
            .expect("decision-bound handoff checkpoint");
        assert!(
            checkpoint.success,
            "handoff checkpoint failed: {:?}",
            checkpoint.error
        );
        let outcome = harness.cluster.nodes[leader_idx]
            .controller
            .checkpoint_authority()
            .expect("cluster checkpoint authority")
            .cluster_outcome(checkpoint.epoch)
            .await
            .expect("read handoff outcome")
            .expect("handoff checkpoint must have a durable outcome");
        assert_eq!(outcome.checkpoint_id, checkpoint.checkpoint_id);

        let seed_attempt = CheckpointAttempt::new(checkpoint.epoch, checkpoint.checkpoint_id);
        let inventory = harness.nodes[leader_idx]
            .state_backend
            .checkpoint_seal_inventory(seed_attempt)
            .await
            .expect("read handoff seal")
            .expect("decided checkpoint must have an exact seal");
        let all_vnodes: Vec<u32> = (0..VNODE_COUNT).collect();
        assert_eq!(inventory.attempt, seed_attempt);
        assert_eq!(inventory.required_vnodes, all_vnodes);
        assert_eq!(inventory.assignment_fence, outcome.assignment_fence);

        for node in &mut harness.nodes {
            for watcher in std::mem::take(&mut node.rebalance_tasks) {
                watcher.abort();
                let _ = watcher.await;
            }
        }
        let rotated = authorize_planned_rotation(&harness, &draining, &leader_proof).await;
        finalize_planned_rotation(&harness, &draining, &rotated).await;
        let adoption = harness.nodes[leader_idx]
            .db
            .adopt_assignment_snapshot(
                rotated.clone(),
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await
            .expect("adopt decision-bound assignment handoff");

        assert!(adoption.adopted);
        assert_eq!(adoption.version, rotated.version);
        let acquired: std::collections::BTreeSet<u32> =
            adoption.newly_acquired.iter().copied().collect();
        let expected: std::collections::BTreeSet<u32> = follower_vnodes.iter().copied().collect();
        assert_eq!(
            acquired, expected,
            "leader must acquire exactly the follower's prior vnodes",
        );
        assert_eq!(adoption.rehydrated, follower_vnodes.len());
        assert_eq!(adoption.rehydration_epoch, Some(checkpoint.epoch));

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn checkpoint_after_rotation_carries_new_version() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

        harness
            .bootstrap_catalog(&[
                super::cluster_harness::TEST_SOURCE_DDL,
                "CREATE STREAM projected AS SELECT key, value FROM src",
            ])
            .await
            .expect("bootstrap cluster catalog");
        harness.start_all().await;

        let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
        let mut m = BTreeMap::new();
        for v in 0..VNODE_COUNT {
            m.insert(v, leader);
        }
        let (draining, leader_proof) = begin_planned_rotation(&mut harness, m).await;
        let handoff = harness.nodes[harness.leader_idx()]
            .db
            .checkpoint()
            .await
            .expect("checkpoint the acknowledged predecessor cut");
        assert!(
            handoff.success,
            "handoff checkpoint failed: {:?}",
            handoff.error
        );
        let rotated = authorize_planned_rotation(&harness, &draining, &leader_proof).await;
        finalize_planned_rotation(&harness, &draining, &rotated).await;

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

        let leader_idx = harness.leader_idx();
        let outcome = harness.cluster.nodes[leader_idx]
            .controller
            .checkpoint_authority()
            .expect("cluster checkpoint authority")
            .cluster_outcome(result.epoch)
            .await
            .expect("cluster outcome read")
            .expect("post-rotation checkpoint outcome");
        assert!(outcome.is_commit());
        assert_eq!(
            outcome
                .assignment_fence
                .as_ref()
                .map(|fence| fence.assignment_version),
            Some(rotated.version),
            "post-rotation checkpoint must certify the adopted assignment generation",
        );

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn source_assignment_cut_survives_cluster_restart() {
        let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
        harness
            .bootstrap_catalog(&[
                super::cluster_harness::TEST_SOURCE_DDL,
                "CREATE STREAM projected AS SELECT key, value FROM src",
            ])
            .await
            .expect("bootstrap cluster catalog");
        harness.start_all().await;

        let first = harness.nodes[harness.leader_idx()]
            .db
            .checkpoint()
            .await
            .expect("initial cluster checkpoint");
        assert!(first.success, "initial checkpoint: {:?}", first.error);

        let predecessor = harness.nodes[0]
            .assignment_snapshot_store
            .load()
            .await
            .expect("load durable predecessor")
            .expect("durable predecessor assignment");
        let predecessor_fence = predecessor
            .assignment_fence()
            .expect("canonical predecessor assignment");
        let first_outcome = harness.cluster.nodes[harness.leader_idx()]
            .controller
            .checkpoint_authority()
            .expect("cluster checkpoint authority")
            .cluster_outcome(first.epoch)
            .await
            .expect("initial cluster outcome read")
            .expect("initial checkpoint outcome");
        assert_eq!(
            first_outcome.assignment_fence,
            Some(predecessor_fence.clone())
        );

        let (shared_dir, checkpoint_dirs, control_store) = harness.shutdown_keep_dirs().await;
        let mut restarted = ClusterEngineHarness::spawn_with_dirs(
            N_NODES,
            VNODE_COUNT,
            shared_dir,
            checkpoint_dirs,
            control_store,
        )
        .await;
        assert_eq!(
            restarted.nodes[0]
                .assignment_snapshot_store
                .load()
                .await
                .expect("load assignment after process respawn")
                .expect("durable assignment after process respawn"),
            predecessor,
            "process construction must not advance the durable assignment head"
        );
        restarted.start_all().await;

        let successor = restarted.nodes[0]
            .assignment_snapshot_store
            .load()
            .await
            .expect("load durable restart successor")
            .expect("durable restart successor");
        assert_eq!(successor.version, predecessor.version + 1);
        assert_eq!(successor.vnodes, predecessor.vnodes);
        assert!(!successor.draining);

        let owner_ids = successor
            .vnodes
            .values()
            .map(|owner| owner.0)
            .collect::<std::collections::BTreeSet<_>>();
        let mut replacement_roster = restarted
            .cluster
            .nodes
            .iter()
            .filter(|node| owner_ids.contains(&node.instance_id.0))
            .map(|node| laminar_core::checkpoint::CheckpointParticipant {
                node_id: node.instance_id.0,
                boot_incarnation: node.controller.recovery_incarnation(),
            })
            .collect::<Vec<_>>();
        replacement_roster.sort_unstable_by_key(|participant| participant.node_id);
        assert_eq!(successor.participants, replacement_roster);
        assert!(
            successor
                .participants
                .iter()
                .zip(&predecessor.participants)
                .all(|(replacement, prior)| {
                    replacement.node_id == prior.node_id
                        && replacement.boot_incarnation != prior.boot_incarnation
                }),
            "every predecessor owner must be replaced by its current process incarnation"
        );

        let successor_fence = successor
            .assignment_fence()
            .expect("canonical restart successor");
        let leader_idx = restarted.leader_idx();
        let authority = restarted.cluster.nodes[leader_idx]
            .controller
            .checkpoint_authority()
            .expect("cluster checkpoint authority");
        let decision = authority
            .assignment_recovery_decision(successor.version)
            .await
            .expect("restart recovery decision read")
            .expect("authorized restart recovery decision");
        assert_eq!(decision.predecessor, predecessor_fence);
        assert_eq!(decision.target, successor_fence);
        assert_eq!(
            restarted.nodes[leader_idx]
                .assignment_snapshot_store
                .load_recovery_proposal(&decision.proposal)
                .await
                .expect("load authorized restart proposal"),
            successor
        );

        for (runtime, cluster_node) in restarted.nodes.iter().zip(&restarted.cluster.nodes) {
            assert_eq!(
                runtime.vnode_registry.assignment_version(),
                successor.version
            );
            assert_eq!(
                cluster_node
                    .controller
                    .checkpoint_assignment_fence(successor.version),
                Some(successor_fence.clone())
            );
            let active = successor_fence.contains(runtime.instance_id.0);
            let active_version = if active { successor.version } else { 0 };
            let active_digest = active.then_some(successor_fence.digest());
            assert_eq!(runtime.shuffle_sender.assignment_version(), active_version);
            assert_eq!(
                runtime.shuffle_receiver.assignment_version(),
                active_version
            );
            assert_eq!(
                runtime.shuffle_sender.active_assignment_digest(),
                active_digest
            );
            assert_eq!(
                runtime.shuffle_receiver.active_assignment_digest(),
                active_digest
            );
        }

        let second = restarted.nodes[restarted.leader_idx()]
            .db
            .checkpoint()
            .await
            .expect("checkpoint after recovery");
        assert!(second.success, "recovered checkpoint: {:?}", second.error);
        assert!(second.epoch > first.epoch);

        restarted.shutdown().await;
    }
}

mod two_pc {
    use super::cluster_harness::ClusterEngineHarness;
    use std::sync::Arc;
    use std::time::Duration;

    use laminar_core::cluster::control::{
        BarrierAnnouncement, CheckpointAssignmentFence, CheckpointDecisionStore, LeaderLeaseStore,
        Phase,
    };
    use laminar_core::state::{
        owned_vnodes, CheckpointAttempt, InProcessBackend, NodeId, StateBackend, VnodeRegistry,
    };
    use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;
    use laminar_db::checkpoint_coordinator::{
        CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
    };
    use object_store::ObjectStore;

    fn certified_request(assignment_fence: &CheckpointAssignmentFence) -> CheckpointRequest {
        CheckpointRequest {
            assignment_fence: Some(assignment_fence.clone()),
            ..CheckpointRequest::default()
        }
    }

    fn checkpoint_authority(harness: &ClusterEngineHarness) -> Arc<LeaderLeaseStore> {
        let authority = harness.cluster.nodes[0]
            .controller
            .checkpoint_authority()
            .expect("cluster checkpoint authority");
        for node in &harness.cluster.nodes[1..] {
            let peer_authority = node
                .controller
                .checkpoint_authority()
                .expect("peer checkpoint authority");
            assert!(
                Arc::ptr_eq(&authority, &peer_authority),
                "all participants must use the exact shared checkpoint authority",
            );
        }
        authority
    }

    fn live_leader_proof(
        controller: &laminar_core::cluster::control::ClusterController,
    ) -> laminar_core::checkpoint::LeaderProof {
        let proof = controller
            .capture_leader_proof()
            .expect("durable leader proof must be live");
        assert!(controller.proof_is_live(&proof));
        proof
    }

    async fn make_coord(
        dir: &std::path::Path,
        backend: Arc<InProcessBackend>,
        vnodes: Vec<u32>,
        assignment_fence: &CheckpointAssignmentFence,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        decision_store: Arc<CheckpointDecisionStore>,
    ) -> CheckpointCoordinator {
        let key_group_count =
            laminar_core::state::KeyGroupCount::try_from(backend.key_group_capacity()).unwrap();
        let store = Box::new(
            FileSystemCheckpointStore::new(dir)
                .with_key_group_count(key_group_count)
                .with_participant_id(controller.instance_id().0),
        );
        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        coord.set_state_backend(backend).unwrap();
        coord.set_vnode_set(vnodes);
        coord.set_assignment_version(assignment_fence.assignment_version);
        controller.publish_checkpoint_assignment_fence(Some(assignment_fence.clone()));
        coord.set_cluster_controller(controller);
        coord
            .bind_durable_decision_store(decision_store)
            .await
            .unwrap();
        coord
    }

    fn make_decision_store(store: Arc<dyn ObjectStore>) -> Arc<CheckpointDecisionStore> {
        Arc::new(CheckpointDecisionStore::new(store))
    }

    async fn create_test_recovery_capsule(
        store: &CheckpointDecisionStore,
        checkpoint_id: u64,
        fence: &CheckpointAssignmentFence,
    ) -> laminar_core::checkpoint::RecoveryCapsuleRef {
        use laminar_core::checkpoint::{
            ClusterRecoveryCapsule, ParticipantRecoveryRef, PipelineIdentity,
            CLUSTER_RECOVERY_CAPSULE_VERSION,
        };

        fn digest(byte: u8) -> String {
            format!("{byte:02x}").repeat(32)
        }

        let deployment_id = store.load_or_create_deployment_id().await.unwrap();
        let portable_state_sha256 = digest(9);
        let participants = fence
            .participant_ids()
            .into_iter()
            .map(|participant_id| ParticipantRecoveryRef {
                participant_id,
                readiness_sha256: digest(3),
                manifest_sha256: digest(4),
                portable_state_sha256: portable_state_sha256.clone(),
            })
            .collect();
        let capsule = ClusterRecoveryCapsule {
            version: CLUSTER_RECOVERY_CAPSULE_VERSION,
            attempt: CheckpointAttempt::new(checkpoint_id, checkpoint_id),
            deployment_id,
            pipeline_identity: PipelineIdentity::empty(),
            assignment_fence: fence.clone(),
            seal_inventory_sha256: digest(2),
            participants,
            source_offsets: Default::default(),
            source_metadata: Default::default(),
            source_assignment_versions: Default::default(),
            source_watermarks: Default::default(),
            cluster_watermark: laminar_core::checkpoint::CheckpointWatermark::Uninitialized,
            recovery_watermark_frontier: None,
            portable_state_sha256,
        };
        store.create_recovery_capsule(&capsule).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_node_leader_commits_follower_mirrors() {
        let harness = ClusterEngineHarness::spawn(2, 4).await;
        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];
        let leader_node = &harness.cluster.nodes[leader_idx];
        let follower_node = &harness.cluster.nodes[follower_idx];
        let authority = checkpoint_authority(&harness);

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
        let decision_store = make_decision_store(harness.control_store());
        let fence = super::test_assignment_fence(&harness.cluster, &registry);

        let mut leader_coord = make_coord(
            leader_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(leader_node.instance_id.0)),
            &fence,
            Arc::clone(&leader_node.controller),
            Arc::clone(&decision_store),
        )
        .await;
        leader_coord.set_gate_vnode_set((0..registry.vnode_count()).collect());
        let mut follower_coord = make_coord(
            follower_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
            &fence,
            Arc::clone(&follower_node.controller),
            Arc::clone(&decision_store),
        )
        .await;

        let leader_proof = live_leader_proof(&leader_node.controller);
        let ann = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(leader_proof),
            phase: Phase::Prepare,
            flags: 0,
        };

        let follower_request = certified_request(&fence);
        let follower_handle = tokio::spawn(async move {
            follower_coord
                .follower_checkpoint(follower_request, ann, Duration::from_secs(15))
                .await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let leader_result = leader_coord
            .checkpoint(certified_request(&fence))
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

        let attempt = CheckpointAttempt::new(leader_result.epoch, leader_result.checkpoint_id);
        for v in 0..4 {
            assert!(backend.read_partial(attempt, v).await.unwrap().is_some());
        }
        let outcome = authority
            .cluster_outcome(leader_result.epoch)
            .await
            .expect("cluster outcome read")
            .expect("committed cluster outcome");
        assert!(outcome.is_commit());
        assert_eq!(outcome.checkpoint_id, leader_result.checkpoint_id);
        assert_eq!(outcome.assignment_fence.as_ref(), Some(&fence));

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_records_commit_decision_before_announce() {
        let harness = ClusterEngineHarness::spawn(2, 4).await;
        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];
        let leader_node = &harness.cluster.nodes[leader_idx];
        let follower_node = &harness.cluster.nodes[follower_idx];
        let authority = checkpoint_authority(&harness);

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

        let decision_store = make_decision_store(harness.control_store());
        let fence = super::test_assignment_fence(&harness.cluster, &registry);

        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let mut leader_coord = make_coord(
            leader_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(leader_node.instance_id.0)),
            &fence,
            Arc::clone(&leader_node.controller),
            Arc::clone(&decision_store),
        )
        .await;
        leader_coord.set_gate_vnode_set((0..registry.vnode_count()).collect());
        let mut follower_coord = make_coord(
            follower_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
            &fence,
            Arc::clone(&follower_node.controller),
            Arc::clone(&decision_store),
        )
        .await;

        let leader_proof = live_leader_proof(&leader_node.controller);
        let ann = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(leader_proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        let follower_request = certified_request(&fence);
        let follower_handle = tokio::spawn(async move {
            follower_coord
                .follower_checkpoint(follower_request, ann, Duration::from_secs(15))
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let leader_result = leader_coord
            .checkpoint(certified_request(&fence))
            .await
            .unwrap();
        assert!(
            leader_result.success,
            "leader checkpoint: {:?}",
            leader_result.error
        );
        let _ = follower_handle.await.unwrap().unwrap();

        let outcome = authority
            .cluster_outcome(leader_result.epoch)
            .await
            .expect("cluster outcome read")
            .expect("leader must record Commit before the follower can observe it");
        assert!(outcome.is_commit());
        assert_eq!(outcome.checkpoint_id, leader_result.checkpoint_id);
        assert_eq!(outcome.assignment_fence.as_ref(), Some(&fence));

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn follower_timeout_commits_when_decision_recorded() {
        const CHECKPOINT_ID: u64 = 42;

        let harness = ClusterEngineHarness::spawn(2, 4).await;
        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];
        let leader_node = &harness.cluster.nodes[leader_idx];
        let follower_node = &harness.cluster.nodes[follower_idx];
        let authority = checkpoint_authority(&harness);

        let backend = Arc::new(InProcessBackend::new(4));
        let registry = VnodeRegistry::new(4);
        registry.set_assignment(
            vec![
                NodeId(leader_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
            ]
            .into(),
        );

        let decision_store = make_decision_store(harness.control_store());
        let fence = super::test_assignment_fence(&harness.cluster, &registry);
        let leader_proof = live_leader_proof(&leader_node.controller);
        let recovery_capsule =
            create_test_recovery_capsule(decision_store.as_ref(), CHECKPOINT_ID, &fence).await;
        authority
            .record_cluster_outcome(
                &leader_proof,
                CHECKPOINT_ID,
                CHECKPOINT_ID,
                fence.clone(),
                laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
                Some(recovery_capsule),
            )
            .await
            .unwrap();

        let follower_dir = tempfile::tempdir().unwrap();
        let mut follower_coord = make_coord(
            follower_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
            &fence,
            Arc::clone(&follower_node.controller),
            Arc::clone(&decision_store),
        )
        .await;

        let ann = BarrierAnnouncement {
            epoch: CHECKPOINT_ID,
            checkpoint_id: CHECKPOINT_ID,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(leader_proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        let committed = follower_coord
            .follower_checkpoint(certified_request(&fence), ann, Duration::from_millis(500))
            .await
            .unwrap();

        assert!(committed);
        let outcome = authority
            .cluster_outcome(CHECKPOINT_ID)
            .await
            .expect("cluster outcome read")
            .expect("pre-recorded cluster outcome");
        assert!(outcome.is_commit());
        assert_eq!(outcome.checkpoint_id, CHECKPOINT_ID);
        assert_eq!(outcome.assignment_fence.as_ref(), Some(&fence));
        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn follower_timeout_stays_in_doubt_when_no_decision() {
        const CHECKPOINT_ID: u64 = 99;

        let harness = ClusterEngineHarness::spawn(2, 4).await;
        let leader_idx = harness.leader_idx();
        let follower_idx = harness.follower_idxs()[0];
        let leader_node = &harness.cluster.nodes[leader_idx];
        let follower_node = &harness.cluster.nodes[follower_idx];
        let authority = checkpoint_authority(&harness);

        let backend = Arc::new(InProcessBackend::new(4));
        let registry = VnodeRegistry::new(4);
        registry.set_assignment(
            vec![
                NodeId(leader_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
                NodeId(follower_node.instance_id.0),
            ]
            .into(),
        );

        let decision_store = make_decision_store(harness.control_store());
        let fence = super::test_assignment_fence(&harness.cluster, &registry);

        let follower_dir = tempfile::tempdir().unwrap();
        let mut follower_coord = make_coord(
            follower_dir.path(),
            backend.clone(),
            owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
            &fence,
            Arc::clone(&follower_node.controller),
            Arc::clone(&decision_store),
        )
        .await;

        let leader_proof = live_leader_proof(&leader_node.controller);
        let ann = BarrierAnnouncement {
            epoch: CHECKPOINT_ID,
            checkpoint_id: CHECKPOINT_ID,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(leader_proof),
            phase: Phase::Prepare,
            flags: 0,
        };
        let error = follower_coord
            .follower_checkpoint(certified_request(&fence), ann, Duration::from_millis(500))
            .await
            .expect_err("a prepared follower must not guess abort without a durable decision");
        assert!(
            error.to_string().contains("[LDB-6046]"),
            "unexpected in-doubt error: {error}"
        );
        assert!(
            authority
                .cluster_outcome(CHECKPOINT_ID)
                .await
                .expect("cluster outcome read")
                .is_none(),
            "the follower must not synthesize a terminal cluster outcome",
        );
        harness.shutdown().await;
    }
}

mod minio {
    use super::cluster_harness::{ClusterEngineHarness, TEST_SOURCE_DDL};
    use std::sync::Arc;
    use std::time::Duration;

    use laminar_core::cluster::control::{
        BarrierAnnouncement, CheckpointAssignmentFence, CheckpointDecisionStore, Phase,
    };
    use laminar_core::state::{
        owned_vnodes, rendezvous_assignment, CheckpointAttempt, NodeId, ObjectStoreBackend,
        StateBackend, VnodeRegistry,
    };
    use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;
    use laminar_db::checkpoint_coordinator::{
        CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
    };
    use object_store::ObjectStoreExt;

    use super::common::{minio_endpoint, minio_store};

    fn unique_bucket(prefix: &str) -> String {
        format!("{prefix}-{}", uuid::Uuid::new_v4())
    }

    fn certified_request(
        controller: &laminar_core::cluster::control::ClusterController,
        assignment_version: u64,
    ) -> CheckpointRequest {
        CheckpointRequest {
            assignment_fence: controller.checkpoint_assignment_fence(assignment_version),
            ..CheckpointRequest::default()
        }
    }

    async fn make_coord(
        dir: &std::path::Path,
        backend: Arc<ObjectStoreBackend>,
        vnodes: Vec<u32>,
        gate_vnodes: Vec<u32>,
        assignment_fence: &CheckpointAssignmentFence,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        decision_store: Arc<CheckpointDecisionStore>,
    ) -> CheckpointCoordinator {
        let key_group_count =
            laminar_core::state::KeyGroupCount::try_from(backend.key_group_capacity()).unwrap();
        let store = Box::new(
            FileSystemCheckpointStore::new(dir)
                .with_key_group_count(key_group_count)
                .with_participant_id(controller.instance_id().0),
        );
        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        coord.set_state_backend(backend).unwrap();
        coord.set_vnode_set(vnodes);
        coord.set_gate_vnode_set(gate_vnodes);
        coord.set_assignment_version(assignment_fence.assignment_version);
        controller.publish_checkpoint_assignment_fence(Some(assignment_fence.clone()));
        coord.set_cluster_controller(controller);
        coord
            .bind_durable_decision_store(decision_store)
            .await
            .unwrap();
        coord
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_node_minio_leader_commits_follower_mirrors() {
        if minio_endpoint().is_none() {
            eprintln!("skipping: MinIO not reachable at 127.0.0.1:19000");
            return;
        }
        let bucket = unique_bucket("laminar-test");
        let store = minio_store(&bucket).await;
        let harness =
            ClusterEngineHarness::spawn_with_control_store(2, 4, Arc::clone(&store)).await;
        let cluster = &harness.cluster;

        let (leader_node, follower_node) = if cluster.nodes[0].controller.is_leader() {
            (&cluster.nodes[0], &cluster.nodes[1])
        } else {
            (&cluster.nodes[1], &cluster.nodes[0])
        };

        let decision_store = Arc::new(CheckpointDecisionStore::new(Arc::clone(&store)));

        let leader_backend = Arc::new(ObjectStoreBackend::cluster_shared(
            Arc::clone(&store),
            leader_node.instance_id.0.to_string(),
            4,
        ));
        let follower_backend = Arc::new(ObjectStoreBackend::cluster_shared(
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
        let fence = super::test_assignment_fence(cluster, &registry);

        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let mut leader_coord = make_coord(
            leader_dir.path(),
            leader_backend,
            leader_owned,
            full.clone(),
            &fence,
            Arc::clone(&leader_node.controller),
            Arc::clone(&decision_store),
        )
        .await;
        let mut follower_coord = make_coord(
            follower_dir.path(),
            follower_backend,
            follower_owned,
            full,
            &fence,
            Arc::clone(&follower_node.controller),
            decision_store,
        )
        .await;

        let assignment_version = registry.assignment_version();
        let ann = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            assignment_fence: leader_node
                .controller
                .checkpoint_assignment_fence(assignment_version),
            leader_proof: Some(
                leader_node
                    .controller
                    .capture_leader_proof()
                    .expect("durable leader proof must be live"),
            ),
            phase: Phase::Prepare,
            flags: 0,
        };

        let follower_request = certified_request(&follower_node.controller, assignment_version);
        let follower_handle = tokio::spawn(async move {
            follower_coord
                .follower_checkpoint(follower_request, ann, Duration::from_secs(20))
                .await
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        let leader_result = leader_coord
            .checkpoint(certified_request(
                &leader_node.controller,
                assignment_version,
            ))
            .await
            .expect("leader checkpoint");
        assert!(
            leader_result.success,
            "leader checkpoint over MinIO must succeed: {:?}",
            leader_result.error,
        );

        let committed = follower_handle.await.expect("join").expect("follower");
        assert!(committed, "follower must commit on leader's Commit");

        let attempt = CheckpointAttempt::new(leader_result.epoch, leader_result.checkpoint_id);
        for v in 0..4 {
            let path = object_store::path::Path::from(format!(
                "state-v2/epoch={}/checkpoint={}/vnode={v}/partial.bin",
                attempt.epoch, attempt.checkpoint_id
            ));
            let meta = store.head(&path).await;
            assert!(meta.is_ok(), "missing partial for vnode {v}: {meta:?}");
        }
        let seal_path = object_store::path::Path::from(format!(
            "state-v2/epoch={}/checkpoint={}/_SEAL",
            attempt.epoch, attempt.checkpoint_id
        ));
        assert!(
            store.head(&seal_path).await.is_ok(),
            "missing exact-attempt _SEAL marker on MinIO",
        );

        harness.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_control_state_survives_fresh_minio_client_restart() {
        if minio_endpoint().is_none() {
            eprintln!("skipping: MinIO not reachable at 127.0.0.1:19000");
            return;
        }
        let bucket = unique_bucket("laminar-restart");
        let mut harness =
            ClusterEngineHarness::spawn_with_control_store(2, 4, minio_store(&bucket).await).await;
        harness
            .bootstrap_catalog(&[
                TEST_SOURCE_DDL,
                "CREATE STREAM projected AS SELECT key, value FROM src",
            ])
            .await
            .expect("bootstrap cluster catalog");
        harness.start_all().await;

        let first = harness.nodes[harness.leader_idx()]
            .db
            .checkpoint()
            .await
            .expect("checkpoint before restart");
        assert!(
            first.success,
            "checkpoint before restart: {:?}",
            first.error
        );
        let predecessor = harness.nodes[0]
            .assignment_snapshot_store
            .load()
            .await
            .expect("load assignment before restart")
            .expect("assignment before restart");
        let predecessor_fence = predecessor
            .assignment_fence()
            .expect("canonical assignment before restart");
        let first_certified = harness.cluster.nodes[harness.leader_idx()]
            .controller
            .checkpoint_authority()
            .expect("checkpoint authority before restart")
            .cluster_outcome_with_recovery_capsule(first.epoch)
            .await
            .expect("load certified checkpoint before restart")
            .expect("certified checkpoint before restart");
        assert!(first_certified.0.is_commit());
        assert!(first_certified.1.is_some());

        let (shared_dir, checkpoint_dirs, old_control_store) = harness.shutdown_keep_dirs().await;
        drop(old_control_store);
        let fresh_control_store = minio_store(&bucket).await;
        let mut restarted = ClusterEngineHarness::spawn_with_dirs(
            2,
            4,
            shared_dir,
            checkpoint_dirs,
            fresh_control_store,
        )
        .await;
        assert_eq!(
            restarted.nodes[0]
                .assignment_snapshot_store
                .load()
                .await
                .expect("load assignment through fresh MinIO client")
                .expect("durable assignment after restart"),
            predecessor,
            "process construction must recover the persisted assignment head",
        );
        let recovered_first = restarted.cluster.nodes[0]
            .controller
            .checkpoint_authority()
            .expect("checkpoint authority through fresh MinIO client")
            .cluster_outcome_with_recovery_capsule(first.epoch)
            .await
            .expect("load certified checkpoint through fresh MinIO client")
            .expect("certified checkpoint after restart");
        assert_eq!(recovered_first, first_certified);
        restarted.start_all().await;

        let leader_idx = restarted.leader_idx();
        let successor = restarted.nodes[leader_idx]
            .assignment_snapshot_store
            .load()
            .await
            .expect("load restart successor through fresh MinIO client")
            .expect("restart successor assignment");
        assert_eq!(successor.version, predecessor.version + 1);
        assert_eq!(successor.vnodes, predecessor.vnodes);
        let successor_fence = successor
            .assignment_fence()
            .expect("canonical restart successor");
        let authority = restarted.cluster.nodes[leader_idx]
            .controller
            .checkpoint_authority()
            .expect("checkpoint authority after restart");
        let recovery_decision = authority
            .assignment_recovery_decision(successor.version)
            .await
            .expect("load restart recovery decision through fresh MinIO client")
            .expect("durable restart recovery decision");
        assert_eq!(recovery_decision.predecessor, predecessor_fence);
        assert_eq!(recovery_decision.target, successor_fence);

        let second = restarted.nodes[leader_idx]
            .db
            .checkpoint()
            .await
            .expect("checkpoint after restart");
        assert!(
            second.success,
            "checkpoint after restart: {:?}",
            second.error
        );
        assert!(second.epoch > first.epoch);
        let (outcome, capsule) = authority
            .cluster_outcome_with_recovery_capsule(second.epoch)
            .await
            .expect("read checkpoint outcome through fresh MinIO client")
            .expect("durable checkpoint outcome after restart");
        assert!(outcome.is_commit());
        assert_eq!(outcome.checkpoint_id, second.checkpoint_id);
        let capsule = capsule.expect("durable recovery capsule after restart");
        assert_eq!(capsule.attempt.epoch, second.epoch);
        assert_eq!(capsule.attempt.checkpoint_id, second.checkpoint_id);
        assert_eq!(capsule.assignment_fence, successor_fence);

        restarted.shutdown().await;
    }

    /// Coordinated-commit descriptors written by two nodes to shared MinIO seal
    /// the leader's gate only when both are present. The designated committer reads
    /// the exact keys bound into that seal rather than listing the descriptor prefix.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_node_coordinated_descriptors_aggregate_on_leader() {
        use bytes::Bytes;
        use laminar_core::checkpoint::{
            CheckpointAssignmentFence, CheckpointParticipant, LeaderProof, LeaderProofOwner,
        };
        use laminar_core::state::StateBackend as _;

        if minio_endpoint().is_none() {
            eprintln!("skipping: MinIO not reachable at 127.0.0.1:19000");
            return;
        }
        let bucket = unique_bucket("laminar-coord");
        let store = minio_store(&bucket).await;
        let node1 = ObjectStoreBackend::cluster_shared(Arc::clone(&store), "1".to_string(), 4);
        let node2 = ObjectStoreBackend::cluster_shared(Arc::clone(&store), "2".to_string(), 4);

        let full = [0u32, 1, 2, 3];
        let required = ["node=1/sink=s".to_string(), "node=2/sink=s".to_string()];
        let attempt = CheckpointAttempt::new(1, 1);
        let boot1 = uuid::Uuid::from_u128(1);
        let boot2 = uuid::Uuid::from_u128(2);
        let fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[1, 1, 2, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: boot1,
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: boot2,
                },
            ],
        )
        .unwrap();
        let proof = LeaderProof {
            owner: LeaderProofOwner {
                node_id: 1,
                boot_id: boot1,
                process_term: 1,
            },
            fencing_token: 1,
        };
        node1.set_authoritative_version(1);
        node2.set_authoritative_version(1);

        // Node 1 writes its vnode slice and its commit descriptor.
        for v in [0u32, 1] {
            node1
                .write_certified_partial(attempt, v, &fence, 1, Bytes::from_static(b"a"))
                .await
                .unwrap();
        }
        node1
            .write_certified_commit_descriptor(
                attempt,
                "node=1/sink=s",
                &fence,
                1,
                &proof,
                Bytes::from_static(b"d1"),
            )
            .await
            .unwrap();

        // Leader cannot seal yet — node 2's partials and descriptor are missing.
        assert!(!node1
            .seal_checkpoint(attempt, Some(&fence), &full, &required)
            .await
            .unwrap());

        // Node 2 writes its slice and descriptor to the same bucket.
        for v in [2u32, 3] {
            node2
                .write_certified_partial(attempt, v, &fence, 2, Bytes::from_static(b"b"))
                .await
                .unwrap();
        }
        node2
            .write_certified_commit_descriptor(
                attempt,
                "node=2/sink=s",
                &fence,
                2,
                &proof,
                Bytes::from_static(b"d2"),
            )
            .await
            .unwrap();

        // Now the leader seals: all partials and both descriptors are durable.
        assert!(node1
            .seal_checkpoint(attempt, Some(&fence), &full, &required)
            .await
            .unwrap());

        let inventory = node1
            .checkpoint_seal_inventory(attempt)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inventory.required_descriptors, required);
        let descriptor1 = node1
            .read_commit_descriptor(attempt, "node=1/sink=s")
            .await
            .unwrap()
            .expect("node 1 descriptor named by seal");
        let descriptor2 = node1
            .read_commit_descriptor(attempt, "node=2/sink=s")
            .await
            .unwrap()
            .expect("node 2 descriptor named by seal");
        assert_eq!(
            (descriptor1, descriptor2),
            (Bytes::from_static(b"d1"), Bytes::from_static(b"d2"))
        );
    }
}
