//! Bind a private startup image to its verified assignment before compute owns it.

use std::sync::Arc;

use laminar_core::cluster::control::ClusterController;

use crate::operator::capability::ManagedStateContract;

use super::authority::VnodeTransitionAuthoritySnapshot;
use super::{DbError, InstalledVnodeStateBinding, OperatorGraph};

impl OperatorGraph {
    pub(crate) fn bind_startup_assignment(
        mut self,
        binding: &InstalledVnodeStateBinding,
        controller: &ClusterController,
    ) -> Result<Self, DbError> {
        // COMPAT: SQL and source-only graphs use their existing startup/transport lifecycle.
        if !self.nodes.iter().any(|node| {
            !node.removed
                && node.capability.managed_state == Some(ManagedStateContract::ProcessFunctionV1)
        }) {
            return Ok(self);
        }
        self.ensure_execution_not_poisoned()?;
        if self.execution_started || self.has_pending_vnode_transition() {
            return Err(DbError::Checkpoint(
                "assignment binding requires an unexecuted startup graph without staged vnode work"
                    .into(),
            ));
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint("startup assignment binding requires cluster shuffle".into())
        })?;
        config.ensure_topology_current()?;
        let pipeline = self.pipeline_identity.as_ref().ok_or_else(|| {
            DbError::Checkpoint("startup assignment binding has no pipeline identity".into())
        })?;
        let assignment = binding.assignment();
        if !binding.matches(assignment, pipeline)
            || assignment.vnode_count != u32::from(self.key_group_count)
        {
            return Err(DbError::Checkpoint(
                "startup assignment binding differs from the graph pipeline or vnode domain".into(),
            ));
        }
        let process = controller
            .try_live_local_process_authority_identity()
            .map_err(DbError::Checkpoint)?;
        if process.participant.node_id != config.self_id.0
            || assignment.participant_incarnation(config.self_id.0)
                != Some(process.participant.boot_incarnation)
        {
            return Err(DbError::Checkpoint(
                "startup assignment certifies another local process incarnation".into(),
            ));
        }
        let authority = VnodeTransitionAuthoritySnapshot::capture_startup(config, assignment)?;
        let deadline = controller.process_lease_deadline().ok_or_else(|| {
            DbError::Checkpoint("process execution has no shared lease deadline".into())
        })?;
        let owned = authority
            .assignment
            .owners()
            .iter()
            .enumerate()
            .filter_map(|(vnode, owner)| {
                (*owner == authority.self_id)
                    .then_some(u32::try_from(vnode).expect("vnode domain fits u32"))
            })
            .collect::<Vec<_>>();
        for node in self.nodes.iter_mut().filter(|node| {
            !node.removed
                && node.capability.managed_state == Some(ManagedStateContract::ProcessFunctionV1)
        }) {
            node.operator.bind_startup_assignment(assignment, &owned)?;
            node.operator
                .bind_process_execution_authority(config, Arc::clone(&deadline))?;
        }
        // Hooks may fail or run arbitrary trusted native code. A partial image is dropped on
        // error; readiness is published only after the same lease and transport are revalidated.
        authority.revalidate_for_publication()?;
        config.ensure_topology_current()?;
        if controller
            .try_live_local_process_authority_identity()
            .map_err(DbError::Checkpoint)?
            != process
        {
            return Err(DbError::Checkpoint(
                "local process authority changed during startup assignment binding".into(),
            ));
        }
        self.whole_restore_open = false;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::RecordBatch;
    use async_trait::async_trait;
    use laminar_core::checkpoint::{
        CheckpointAssignmentFence, CheckpointParticipant, PipelineIdentity,
    };
    use laminar_core::cluster::control::{
        ClusterKv, InMemoryKv, LeaseDeadline, ProcessLeaseAuthority, ProcessLeaseOutcome,
    };
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{KeyGroupCount, NodeId, VnodeRegistry};
    use parking_lot::Mutex;
    use uuid::Uuid;

    use super::*;
    use crate::operator::capability::OperatorCapability;
    use crate::operator::sql_query::ClusterShuffleConfig;
    use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

    type Bindings = Arc<Mutex<Vec<(CheckpointAssignmentFence, Vec<u32>)>>>;

    struct BindingProbe {
        bindings: Bindings,
        lose_lease: Option<Arc<ClusterController>>,
    }

    #[async_trait]
    impl GraphOperator for BindingProbe {
        fn cluster_capability(&self) -> OperatorCapability {
            let mut capability = OperatorCapability::test_vnode_state();
            capability.managed_state = Some(ManagedStateContract::ProcessFunctionV1);
            capability
        }

        fn bind_startup_assignment(
            &mut self,
            assignment: &CheckpointAssignmentFence,
            owned: &[u32],
        ) -> Result<(), DbError> {
            self.bindings
                .lock()
                .push((assignment.clone(), owned.to_vec()));
            if let Some(controller) = &self.lose_lease {
                controller.fence_process_lease();
            }
            Ok(())
        }

        async fn process(
            &mut self,
            _: &[Vec<RecordBatch>],
            _: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }
    }

    struct Fixture {
        controller: Arc<ClusterController>,
        scope: ClusterShuffleConfig,
        binding: InstalledVnodeStateBinding,
    }

    impl Fixture {
        async fn new(
            authority: Arc<ProcessLeaseAuthority>,
            boot: Uuid,
            now_ms: i64,
            version: u64,
        ) -> Self {
            let node = NodeId(7);
            let ProcessLeaseOutcome::Acquired(lease) = authority
                .store_for(node)
                .try_acquire(boot, now_ms)
                .await
                .unwrap()
            else {
                panic!("test process must acquire its stable-node lease");
            };
            let kv = Arc::new(InMemoryKv::new(node));
            let control: Arc<dyn ClusterKv> = kv.clone();
            let recovery: Arc<dyn ClusterKv> = kv;
            let (_members, members) = tokio::sync::watch::channel(Vec::new());
            let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
                node, control, recovery, None, members, boot,
            ));
            controller.set_process_lease_authority(authority).unwrap();
            let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
            controller
                .set_process_lease_deadline(Arc::clone(&deadline))
                .unwrap();
            controller
                .publish_leased_recovery_incarnation(&lease)
                .await
                .unwrap();
            let assignment = CheckpointAssignmentFence::from_owner_map(
                version,
                &[7; 4],
                vec![CheckpointParticipant {
                    node_id: 7,
                    boot_incarnation: boot,
                }],
            )
            .unwrap();
            let registry = Arc::new(VnodeRegistry::single_owner(4, node));
            registry.set_assignment_and_version(Arc::from([node; 4]), version);
            let sender = Arc::new(ShuffleSender::new(7, boot));
            let receiver = Arc::new(
                ShuffleReceiver::bind(7, "127.0.0.1:0".parse().unwrap(), boot)
                    .await
                    .unwrap(),
            );
            sender
                .bind_process_lease_deadline_pair(&receiver, deadline)
                .unwrap();
            assert!(sender
                .install_assignment_fence(&assignment, &[7; 4])
                .unwrap());
            assert!(receiver
                .install_assignment_fence(&assignment, &[7; 4])
                .unwrap());
            Self {
                controller,
                scope: ClusterShuffleConfig {
                    registry,
                    sender,
                    receiver,
                    self_id: node,
                    topology: None,
                },
                binding: InstalledVnodeStateBinding::new(assignment, PipelineIdentity::empty())
                    .unwrap(),
            }
        }

        fn graph(&self, bindings: Bindings, lose_lease: bool) -> OperatorGraph {
            let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
            graph.set_key_group_count(KeyGroupCount::try_from(4_u16).unwrap());
            graph.set_pipeline_identity(PipelineIdentity::empty());
            graph.set_cluster_shuffle(self.scope.clone());
            graph
                .place_operator_node(
                    "binding",
                    Box::new(BindingProbe {
                        bindings,
                        lose_lease: lose_lease.then(|| Arc::clone(&self.controller)),
                    }),
                    1,
                )
                .unwrap();
            graph
        }
    }

    fn authority() -> Arc<ProcessLeaseAuthority> {
        Arc::new(
            ProcessLeaseAuthority::new(
                Arc::new(object_store::memory::InMemory::new()),
                Duration::from_secs(60),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn startup_binding_uses_live_process_and_exact_local_roster() {
        let fixture = Fixture::new(authority(), Uuid::from_u128(7), 0, 7).await;
        let bindings = Bindings::default();
        let graph = fixture
            .graph(Arc::clone(&bindings), false)
            .bind_startup_assignment(&fixture.binding, &fixture.controller)
            .unwrap();
        assert_eq!(
            *bindings.lock(),
            vec![(fixture.binding.assignment().clone(), vec![0, 1, 2, 3])]
        );
        assert!(graph.restore_state_frames(&[], &[], 4).is_err());
    }

    #[tokio::test]
    async fn startup_binding_leaves_graphs_without_process_state_to_existing_lifecycle() {
        let fixture = Fixture::new(authority(), Uuid::from_u128(7), 0, 7).await;
        fixture.controller.fence_process_lease();
        let graph = OperatorGraph::new(laminar_sql::create_session_context())
            .bind_startup_assignment(&fixture.binding, &fixture.controller)
            .unwrap();
        assert!(graph.whole_restore_open);
        assert!(graph.cluster_shuffle.is_none());
    }

    #[tokio::test]
    async fn startup_binding_accepts_fenced_transport_before_intake_activation() {
        let fixture = Fixture::new(authority(), Uuid::from_u128(7), 0, 7).await;
        fixture.scope.sender.suspend_assignment_fence();
        fixture.scope.receiver.suspend_assignment_fence();
        // Live transfer keeps its active-certificate requirement.
        assert!(VnodeTransitionAuthoritySnapshot::capture(
            &fixture.scope,
            fixture.binding.assignment()
        )
        .is_err());
        let bindings = Bindings::default();
        fixture
            .graph(Arc::clone(&bindings), false)
            .bind_startup_assignment(&fixture.binding, &fixture.controller)
            .unwrap();
        assert_eq!(bindings.lock().len(), 1);
        assert_eq!(fixture.scope.sender.assignment_version(), 0);
        assert_eq!(fixture.scope.receiver.assignment_version(), 0);
    }

    #[tokio::test]
    async fn startup_binding_rejects_foreign_pipeline_or_vnode_domain_before_hooks() {
        let fixture = Fixture::new(authority(), Uuid::from_u128(7), 0, 7).await;
        let bindings = Bindings::default();
        let mut graph = fixture.graph(Arc::clone(&bindings), false);
        graph.key_group_count = KeyGroupCount::try_from(8_u16).unwrap();
        assert!(graph
            .bind_startup_assignment(&fixture.binding, &fixture.controller)
            .is_err());
        let mut graph = fixture.graph(Arc::clone(&bindings), false);
        let mut foreign = PipelineIdentity::empty();
        foreign.sha256 = "0".repeat(64);
        graph.set_pipeline_identity(foreign);
        assert!(graph
            .bind_startup_assignment(&fixture.binding, &fixture.controller)
            .is_err());
        assert!(bindings.lock().is_empty());
    }

    #[tokio::test]
    async fn startup_binding_rejects_process_loss_before_and_during_hooks() {
        for during_hooks in [false, true] {
            let fixture = Fixture::new(authority(), Uuid::from_u128(7), 0, 7).await;
            let bindings = Bindings::default();
            let graph = fixture.graph(Arc::clone(&bindings), during_hooks);
            if !during_hooks {
                fixture.controller.fence_process_lease();
            }
            let error = graph
                .bind_startup_assignment(&fixture.binding, &fixture.controller)
                .err()
                .unwrap();
            assert!(error.to_string().contains("not live"), "{error}");
            assert_eq!(bindings.lock().len(), usize::from(during_hooks));
        }
    }

    #[tokio::test]
    async fn startup_binding_rejects_stale_transport_without_calling_hooks() {
        let fixture = Fixture::new(authority(), Uuid::from_u128(7), 0, 7).await;
        let bindings = Bindings::default();
        let graph = fixture.graph(Arc::clone(&bindings), false);
        let target = CheckpointAssignmentFence::from_owner_map(
            8,
            &[7; 4],
            fixture.binding.assignment().participants.clone(),
        )
        .unwrap();
        fixture
            .scope
            .receiver
            .install_assignment_fence(&target, &[7; 4])
            .unwrap();
        assert!(graph
            .bind_startup_assignment(&fixture.binding, &fixture.controller)
            .is_err());
        assert!(bindings.lock().is_empty());
    }

    #[tokio::test]
    async fn restarted_process_requires_its_new_boot_and_assignment() {
        let authority = Arc::new(
            ProcessLeaseAuthority::new(
                Arc::new(object_store::memory::InMemory::new()),
                Duration::from_millis(20),
            )
            .unwrap(),
        );
        let predecessor = Fixture::new(Arc::clone(&authority), Uuid::from_u128(7), 0, 7).await;
        predecessor.controller.fence_process_lease();
        let store = authority.store_for(NodeId(7));
        let observation = store
            .observe_rival(&store.load().await.unwrap().unwrap())
            .unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(matches!(
            store
                .try_takeover(Uuid::from_u128(8), &observation, 25)
                .await
                .unwrap(),
            ProcessLeaseOutcome::Acquired(_)
        ));
        let replacement = Fixture::new(authority, Uuid::from_u128(8), 26, 8).await;
        assert_eq!(
            replacement
                .controller
                .try_live_local_process_authority_identity()
                .unwrap()
                .process_term,
            2
        );
        let bindings = Bindings::default();
        assert!(replacement
            .graph(Arc::clone(&bindings), false)
            .bind_startup_assignment(&predecessor.binding, &replacement.controller)
            .is_err());
        assert!(bindings.lock().is_empty());
        replacement
            .graph(Arc::clone(&bindings), false)
            .bind_startup_assignment(&replacement.binding, &replacement.controller)
            .unwrap();
        assert_eq!(
            *bindings.lock(),
            vec![(replacement.binding.assignment().clone(), vec![0, 1, 2, 3])]
        );
    }
}
