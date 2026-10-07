use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use laminar_core::checkpoint::{CheckpointParticipant, PipelineIdentity};
use laminar_core::cluster::control::{
    ClusterController, ClusterKv, InMemoryKv, ProcessLeaseAuthority, ProcessLeaseConfig,
    ProcessLeaseManager, ProcessLeaseOutcome,
};
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{KeyGroupCount, NodeId, VnodeRegistry};
use uuid::Uuid;

use super::*;
use crate::operator_graph::{GraphOperator, InputFrontier, OperatorCheckpoint, OperatorGraph};
use crate::process_function::tests::{
    activity_rows, descriptor, input_batch, materialize, AccountActivity,
};
use crate::process_function::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessFunctionDescriptor,
    ProcessFunctionRegistration, ProcessHandler,
};
use crate::vnode_transition_staging::InstalledVnodeStateBinding;

const TTL: Duration = Duration::from_secs(3);

struct Fixture {
    authority: Arc<ProcessLeaseAuthority>,
    controller: Arc<ClusterController>,
    scope: ClusterShuffleConfig,
    binding: InstalledVnodeStateBinding,
    lease_manager: Option<ProcessLeaseManager>,
}

impl Fixture {
    async fn new(boot: Uuid, version: u64, recovery: u64, ttl: Duration) -> Self {
        let authority = Arc::new(
            ProcessLeaseAuthority::new(Arc::new(object_store::memory::InMemory::new()), ttl)
                .unwrap(),
        );
        Self::acquire(authority, boot, version, recovery, ttl).await
    }

    async fn acquire(
        authority: Arc<ProcessLeaseAuthority>,
        boot: Uuid,
        version: u64,
        recovery: u64,
        ttl: Duration,
    ) -> Self {
        let node = NodeId(7);
        let fence = CheckpointAssignmentFence::from_owner_map(
            version,
            &[7; 4],
            vec![CheckpointParticipant {
                node_id: 7,
                boot_incarnation: boot,
            }],
        )
        .unwrap();
        Self::for_assignment(authority, node, fence, [7; 4], recovery, ttl).await
    }

    async fn for_assignment(
        authority: Arc<ProcessLeaseAuthority>,
        node: NodeId,
        fence: CheckpointAssignmentFence,
        owners: [u64; 4],
        recovery: u64,
        ttl: Duration,
    ) -> Self {
        let boot = fence.participant_incarnation(node.0).unwrap();
        let store = authority.store_for(node);
        let started = Instant::now();
        let ProcessLeaseOutcome::Acquired(lease) = store.try_acquire(boot, 0).await.unwrap() else {
            panic!("fixture must acquire its actual stable-node lease");
        };
        let manager = ProcessLeaseManager::new(
            store,
            boot,
            ProcessLeaseConfig {
                ttl,
                renew_interval: ttl / 2,
            },
            started,
            &lease,
        )
        .unwrap();
        let deadline = manager.deadline();
        let kv = Arc::new(InMemoryKv::new(node));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery_kv: Arc<dyn ClusterKv> = kv;
        let (_members, members) = tokio::sync::watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            control,
            recovery_kv,
            None,
            members,
            boot,
        ));
        controller
            .set_process_lease_authority(Arc::clone(&authority))
            .unwrap();
        controller
            .set_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        controller
            .publish_leased_recovery_incarnation(&lease)
            .await
            .unwrap();
        let registry = Arc::new(VnodeRegistry::single_owner(4, node));
        registry.set_assignment_and_version(
            Arc::from(owners.iter().copied().map(NodeId).collect::<Vec<_>>()),
            fence.assignment_version,
        );
        let sender = Arc::new(ShuffleSender::new(node.0, boot));
        let receiver = Arc::new(
            ShuffleReceiver::bind(node.0, "127.0.0.1:0".parse().unwrap(), boot)
                .await
                .unwrap(),
        );
        sender
            .bind_process_lease_deadline_pair(&receiver, deadline)
            .unwrap();
        sender.install_assignment_fence(&fence, &owners).unwrap();
        receiver.install_assignment_fence(&fence, &owners).unwrap();
        sender.set_recovery_gen(recovery);
        receiver.set_recovery_gen(recovery);
        Self {
            authority,
            controller,
            scope: ClusterShuffleConfig {
                registry,
                sender,
                receiver,
                self_id: node,
                topology: None,
            },
            binding: InstalledVnodeStateBinding::new(fence, PipelineIdentity::empty()).unwrap(),
            lease_manager: Some(manager),
        }
    }

    fn bind_operator(&self, operator: &mut ProcessFunctionOperator) {
        if matches!(operator.execution, ProcessExecution::Local) {
            operator
                .require_cluster_execution(
                    "activity",
                    tokio::runtime::Handle::current(),
                    self.scope.self_id,
                )
                .unwrap();
        }
        operator
            .bind_startup_assignment(
                self.binding.assignment(),
                &self
                    .scope
                    .registry
                    .versioned_snapshot()
                    .owners()
                    .iter()
                    .enumerate()
                    .filter_map(|(vnode, owner)| {
                        (*owner == self.scope.self_id)
                            .then_some(u32::try_from(vnode).expect("fixture vnode domain fits u32"))
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        operator
            .bind_process_execution_authority(
                &self.scope,
                self.controller.process_lease_deadline().unwrap(),
            )
            .unwrap();
    }

    fn graph(&self, binding: ProcessFunctionDescriptor, handler: ProcessHandler) -> OperatorGraph {
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.set_query_budget_ns(5_000_000_000);
        graph.set_key_group_count(KeyGroupCount::try_from(4_u16).unwrap());
        graph.set_pipeline_identity(PipelineIdentity::empty());
        graph.set_cluster_shuffle(self.scope.clone());
        graph.set_runtime_handle(tokio::runtime::Handle::current());
        graph.register_source_schema("events".into(), Arc::clone(&binding.input_schema));
        graph
            .add_process_function(&ProcessFunctionRegistration {
                output_name: "activity".into(),
                source_name: "events".into(),
                descriptor: binding,
                handler,
            })
            .unwrap();
        graph
    }

    #[cfg(feature = "process-remote")]
    async fn takeover(&self, boot: Uuid, version: u64, recovery: u64) -> Self {
        let store = self.authority.store_for(NodeId(7));
        let observation = store
            .observe_rival(&store.load().await.unwrap().unwrap())
            .unwrap();
        tokio::time::sleep(TTL + Duration::from_millis(20)).await;
        assert!(matches!(
            store.try_takeover(boot, &observation, 1).await.unwrap(),
            ProcessLeaseOutcome::Acquired(_)
        ));
        Self::acquire(Arc::clone(&self.authority), boot, version, recovery, TTL).await
    }
}

fn frontier(watermark: i64) -> [InputFrontier; 1] {
    [InputFrontier {
        watermark: Some(watermark),
        idle: false,
    }]
}

fn state_image(operator: &ProcessFunctionOperator) -> serde_json::Value {
    let state = operator
        .state
        .iter()
        .flat_map(|slot| slot.iter())
        .collect::<BTreeMap<_, _>>();
    serde_json::to_value((
        state.into_iter().collect::<Vec<_>>(),
        &operator.due,
        operator.live_bytes,
        operator.key_count,
        operator.timer_count,
        operator.next_timer_generation,
        operator.watermark_us,
    ))
    .unwrap()
}

#[tokio::test]
async fn graph_routes_single_owner_input_to_canonical_vnodes_and_preserves_key_order() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
    // Exercise the assignment hooks separately from database bootstrap.
    let mut graph = fixture
        .graph(
            descriptor(),
            ProcessHandler::Native(Arc::new(AccountActivity)),
        )
        .initialize_managed_state()
        .await
        .unwrap()
        .bind_startup_assignment(&fixture.binding, &fixture.controller)
        .unwrap();
    let input = input_batch(&[
        ("b", 3, 100_000),
        ("a", 7, 100_000),
        ("b", 5, 101_000),
        ("c", 1, 102_000),
    ]);
    let expected = laminar_core::shuffle::row_vnodes(&input, &[0], 4).unwrap();
    let output = graph
        .execute_cycle(
            &rustc_hash::FxHashMap::from_iter([(Arc::from("events"), vec![input.clone()])]),
            102,
            None,
        )
        .await
        .unwrap();
    let rows = activity_rows(&output["activity"]);
    assert!(rows.iter().any(|row| row.0 == "b" && row.2 == 8));
    let (whole, vnodes) = materialize(graph.capture_state(u64::MAX).unwrap());
    let codec = PartitionKeyCodecV1::try_new([arrow_schema::DataType::Utf8]).unwrap();
    let keys = codec
        .encode_columns(&[Arc::clone(input.column(0))])
        .unwrap();
    for (key, vnode) in keys.iter().zip(expected) {
        let frame = vnodes
            .iter()
            .find(|(name, slot, _)| name == "activity" && *slot == vnode)
            .unwrap();
        let decoded: super::super::VnodeFrame = serde_json::from_slice(&frame.2).unwrap();
        assert!(decoded
            .entries
            .iter()
            .any(|(encoded, _)| encoded == key.data()));
    }
    let (restored, count) = fixture
        .graph(
            descriptor(),
            ProcessHandler::Native(Arc::new(AccountActivity)),
        )
        .restore_state_frames(&whole, &vnodes, 4)
        .unwrap();
    assert_eq!(count, 5);
    let mut restored = restored
        .bind_startup_assignment(&fixture.binding, &fixture.controller)
        .unwrap();
    let output = restored
        .execute_cycle(&rustc_hash::FxHashMap::default(), 113, None)
        .await
        .unwrap();
    let mut totals = activity_rows(&output["activity"])
        .into_iter()
        .map(|row| (row.0, row.2))
        .collect::<Vec<_>>();
    totals.sort_unstable();
    assert_eq!(
        totals,
        vec![("a".into(), 7), ("b".into(), 8), ("c".into(), 1)]
    );
}

#[tokio::test]
async fn cluster_process_cannot_execute_before_authority_binding_or_transport_activation() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
    let mut operator =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    operator
        .require_cluster_execution("activity", tokio::runtime::Handle::current(), NodeId(7))
        .unwrap();
    let before = state_image(&operator);
    assert!(operator
        .process_with_frontiers(&[vec![input_batch(&[("a", 1, 100_000)])]], &frontier(100))
        .await
        .unwrap_err()
        .is_shuffle_not_ready());
    fixture.scope.sender.suspend_assignment_fence();
    fixture.scope.receiver.suspend_assignment_fence();
    operator
        .bind_startup_assignment(fixture.binding.assignment(), &[0, 1, 2, 3])
        .unwrap();
    operator
        .bind_process_execution_authority(
            &fixture.scope,
            fixture.controller.process_lease_deadline().unwrap(),
        )
        .unwrap();
    assert!(operator
        .process_with_frontiers(&[vec![input_batch(&[("a", 1, 100_000)])]], &frontier(100))
        .await
        .unwrap_err()
        .requires_pipeline_recovery());
    assert_eq!(state_image(&operator), before);
    fixture
        .scope
        .sender
        .install_assignment_fence(fixture.binding.assignment(), &[7; 4])
        .unwrap();
    fixture
        .scope
        .receiver
        .install_assignment_fence(fixture.binding.assignment(), &[7; 4])
        .unwrap();
    assert_eq!(
        operator
            .process_with_frontiers(&[vec![input_batch(&[("a", 1, 100_000)])]], &frontier(100))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn transfer_rejects_saturated_single_owner_frontier_without_changing_state() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
    let mut operator =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    fixture.bind_operator(&mut operator);
    operator
        .process_with_frontiers(&[], &frontier(i64::MAX))
        .await
        .unwrap();
    let before = state_image(&operator);
    let target = CheckpointAssignmentFence::from_owner_map(
        8,
        &[7; 4],
        fixture.binding.assignment().participants.clone(),
    )
    .unwrap();
    let error = operator
        .prepare_vnode_transition(crate::operator_graph::ManagedVnodeTransition {
            predecessor: fixture.binding.assignment(),
            target: &target,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &[],
            whole_restores: &[],
            mode: crate::operator_graph::ManagedVnodeTransitionMode::Live,
        })
        .unwrap_err();
    assert!(error.to_string().contains("exact frontier"), "{error}");
    assert_eq!(state_image(&operator), before);
    assert!(operator.vnode_transition.is_idle());
}

#[tokio::test]
async fn native_process_rejects_lost_lease_before_input_and_after_handler_without_mutation() {
    struct FencingActivity(Arc<LeaseDeadline>);
    impl NativeProcessFunction for FencingActivity {
        fn invoke(
            &self,
            activations: &[ProcessActivation],
        ) -> Result<Vec<ProcessActivationResult>, DbError> {
            let results = AccountActivity.invoke(activations)?;
            self.0.fence();
            Ok(results)
        }
    }
    for during_handler in [false, true] {
        let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
        let deadline = fixture.controller.process_lease_deadline().unwrap();
        let mut operator =
            ProcessFunctionOperator::new(descriptor(), Arc::new(FencingActivity(deadline)), 4)
                .unwrap();
        fixture.bind_operator(&mut operator);
        let before = state_image(&operator);
        if !during_handler {
            fixture.controller.fence_process_lease();
        }
        let error = operator
            .process_with_frontiers(&[vec![input_batch(&[("a", 999, 100_000)])]], &frontier(100))
            .await
            .unwrap_err();
        assert!(error.requires_pipeline_recovery(), "{error}");
        assert_eq!(state_image(&operator), before);
        assert!(operator
            .activation_sequences
            .iter()
            .all(|sequence| *sequence == 0));
    }
}

#[tokio::test]
async fn native_process_rejects_natural_lease_expiry_before_timer_progress() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, Duration::from_millis(250)).await;
    let mut operator =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    fixture.bind_operator(&mut operator);
    operator
        .process_with_frontiers(&[vec![input_batch(&[("a", 1, 100_000)])]], &frontier(100))
        .await
        .unwrap();
    let before = state_image(&operator);
    tokio::time::timeout(
        Duration::from_secs(2),
        fixture
            .controller
            .process_lease_deadline()
            .unwrap()
            .wait_until_expired(),
    )
    .await
    .unwrap();
    assert!(operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap_err()
        .requires_pipeline_recovery());
    assert_eq!(state_image(&operator), before);
}

#[tokio::test]
async fn routing_rejects_input_and_temporary_budget_overflow_before_state_application() {
    for temporary in [false, true] {
        let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
        let mut binding = descriptor();
        if !temporary {
            binding.limits.max_input_rows = 1;
        }
        let mut operator =
            ProcessFunctionOperator::new(binding, Arc::new(AccountActivity), 4).unwrap();
        fixture.bind_operator(&mut operator);
        if temporary {
            operator.set_managed_state_budget(1);
        }
        let before = state_image(&operator);
        assert!(operator
            .process_with_frontiers(
                &[vec![input_batch(&[("a", 1, 100_000), ("b", 2, 100_000)])]],
                &frontier(100)
            )
            .await
            .unwrap_err()
            .requires_pipeline_halt());
        assert_eq!(state_image(&operator), before);
    }
}

#[tokio::test]
async fn admitted_multi_owner_graph_still_requires_startup_authority() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
    let owners = [7, 7, 8, 8];
    let fence = CheckpointAssignmentFence::from_owner_map(
        8,
        &owners,
        vec![
            CheckpointParticipant {
                node_id: 7,
                boot_incarnation: Uuid::from_u128(7),
            },
            CheckpointParticipant {
                node_id: 8,
                boot_incarnation: Uuid::from_u128(8),
            },
        ],
    )
    .unwrap();
    fixture
        .scope
        .registry
        .set_assignment_and_version(Arc::from(owners.map(NodeId)), 8);
    fixture
        .scope
        .sender
        .install_assignment_fence(&fence, &owners)
        .unwrap();
    fixture
        .scope
        .receiver
        .install_assignment_fence(&fence, &owners)
        .unwrap();
    let binding = InstalledVnodeStateBinding::new(fence, PipelineIdentity::empty()).unwrap();
    let mut unbound = fixture
        .graph(
            descriptor(),
            ProcessHandler::Native(Arc::new(AccountActivity)),
        )
        .initialize_managed_state()
        .await
        .unwrap();
    let input = rustc_hash::FxHashMap::from_iter([(
        Arc::from("events"),
        vec![input_batch(&[("a", 1, 100_000)])],
    )]);
    assert!(unbound
        .execute_cycle(&input, 100, None)
        .await
        .unwrap()
        .is_empty());
    assert!(unbound.has_deferred_work());
    assert!(!unbound.checkpoint_is_quiescent());
    let admitted = fixture
        .graph(
            descriptor(),
            ProcessHandler::Native(Arc::new(AccountActivity)),
        )
        .initialize_managed_state()
        .await
        .unwrap();
    let _bound_graph = admitted
        .bind_startup_assignment(&binding, &fixture.controller)
        .unwrap();
}

#[cfg(feature = "process-remote")]
mod remote;

mod sequencing;
mod shuffle;
