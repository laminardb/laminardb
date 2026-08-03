use super::*;

struct DrainingCaptureFailureOperator {
    live_rows: Arc<std::sync::atomic::AtomicUsize>,
    fail_whole_capture: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(feature = "cluster")]
    fail_vnode_capture: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for DrainingCaptureFailureOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::test_vnode_state()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
        if self
            .fail_whole_capture
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.live_rows
                .store(0, std::sync::atomic::Ordering::Release);
            return Err(DbError::Checkpoint(
                "injected failure after draining whole-operator state".into(),
            ));
        }
        Ok(None)
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_by_vnode(
        &mut self,
        required_vnodes: &[u32],
        _vnode_count: u32,
    ) -> Result<
        Option<std::collections::HashMap<u32, crate::checkpoint_coordinator::StagedSlice>>,
        DbError,
    > {
        if self
            .fail_vnode_capture
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.live_rows
                .store(0, std::sync::atomic::Ordering::Release);
            return Err(DbError::Checkpoint(
                "injected failure after draining vnode state".into(),
            ));
        }
        Ok(Some(
            required_vnodes
                .iter()
                .map(|vnode| {
                    (
                        *vnode,
                        crate::checkpoint_coordinator::StagedSlice::Bytes(
                            bytes::Bytes::from_static(b"test-vnode-state"),
                        ),
                    )
                })
                .collect(),
        ))
    }
}

struct DrainingCaptureOperator {
    live_rows: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for DrainingCaptureOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
        let rows = self.live_rows.swap(0, std::sync::atomic::Ordering::AcqRel);
        Ok(Some(crate::operator_graph::OperatorCheckpoint {
            data: rows.to_le_bytes().to_vec(),
        }))
    }
}

struct PendingGraphPassOperator {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
}

#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for PendingGraphPassOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        std::future::pending().await
    }

    fn checkpoint(&mut self) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

#[cfg(feature = "cluster")]
struct FollowerCheckpointEvidenceOperator;

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for FollowerCheckpointEvidenceOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
        Ok(Some(crate::operator_graph::OperatorCheckpoint {
            data: b"follower-whole-operator-state".to_vec(),
        }))
    }
}

#[cfg(feature = "cluster")]
struct CheckpointRotationFenceAuditOperator {
    fence: Arc<tokio::sync::RwLock<()>>,
    whole_capture_observed: Arc<std::sync::atomic::AtomicBool>,
    vnode_capture_observed: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for CheckpointRotationFenceAuditOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::test_vnode_state()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
        if Arc::clone(&self.fence).try_write_owned().is_ok() {
            return Err(DbError::Checkpoint(
                "whole-state capture escaped the assignment rotation fence".into(),
            ));
        }
        self.whole_capture_observed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(None)
    }

    fn checkpoint_by_vnode(
        &mut self,
        required_vnodes: &[u32],
        _vnode_count: u32,
    ) -> Result<
        Option<std::collections::HashMap<u32, crate::checkpoint_coordinator::StagedSlice>>,
        DbError,
    > {
        if Arc::clone(&self.fence).try_write_owned().is_ok() {
            return Err(DbError::Checkpoint(
                "vnode-state capture escaped the assignment rotation fence".into(),
            ));
        }
        self.vnode_capture_observed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(Some(
            required_vnodes
                .iter()
                .map(|vnode| {
                    (
                        *vnode,
                        crate::checkpoint_coordinator::StagedSlice::Bytes(
                            bytes::Bytes::from_static(b"fence-audit-state"),
                        ),
                    )
                })
                .collect(),
        ))
    }
}

#[cfg(feature = "cluster")]
type CheckpointRotationFenceAudit = (
    Arc<tokio::sync::RwLock<()>>,
    Arc<std::sync::atomic::AtomicBool>,
    Arc<std::sync::atomic::AtomicBool>,
);

#[cfg(feature = "cluster")]
fn install_checkpoint_rotation_fence_audit(
    callback: &mut ConnectorPipelineCallback,
) -> CheckpointRotationFenceAudit {
    let fence = Arc::new(tokio::sync::RwLock::new(()));
    let whole_capture_observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let vnode_capture_observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    callback
        .graph
        .set_rotation_execution_fence(Arc::clone(&fence));
    callback.graph.set_test_vnode_count(1);
    callback.graph.push_test_node(
        "checkpoint-rotation-fence-audit",
        Box::new(CheckpointRotationFenceAuditOperator {
            fence: Arc::clone(&fence),
            whole_capture_observed: Arc::clone(&whole_capture_observed),
            vnode_capture_observed: Arc::clone(&vnode_capture_observed),
        }),
    );
    (fence, whole_capture_observed, vnode_capture_observed)
}

#[cfg(feature = "cluster")]
async fn assignment_writer_after_checkpoint_tail_handoff<F>(
    route: &str,
    mut checkpoint: std::pin::Pin<&mut F>,
    checkpoint_in_flight: &std::sync::atomic::AtomicU64,
    rotation_fence: &Arc<tokio::sync::RwLock<()>>,
) -> tokio::sync::OwnedRwLockWriteGuard<()>
where
    F: std::future::Future + ?Sized,
{
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if checkpoint_in_flight.load(std::sync::atomic::Ordering::Acquire) == 1 {
                break;
            }
            tokio::select! {
                _ = checkpoint.as_mut() => {
                    panic!("{route} completed before its durable tail blocked")
                }
                () = tokio::task::yield_now() => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{route} did not hand off its durable tail"));

    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::select! {
            guard = Arc::clone(rotation_fence).write_owned() => guard,
            _ = checkpoint.as_mut() => {
                panic!("{route} completed before the blocked tail released")
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{route} retained the rotation token in its durable tail"))
}

fn empty_callback_fixture() -> ConnectorPipelineCallback {
    let (_sink_event_tx, sink_event_rx) =
        laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(1);
    let (checkpoint_complete_tx, _checkpoint_complete_rx) =
        crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(1);
    let mv_store = crate::mv_store::MvStore::new();
    let mv_store_has_any = mv_store.has_any_handle();
    ConnectorPipelineCallback {
        graph: crate::operator_graph::OperatorGraph::new(SessionContext::new()),
        stream_entries: Vec::new(),
        sinks: Vec::new(),
        owned_sink_handles: Arc::new(parking_lot::Mutex::new(Vec::new())),
        watermark_states: FxHashMap::default(),
        source_entries_for_wm: FxHashMap::default(),
        source_ids: FxHashMap::default(),
        source_name_arcs: FxHashMap::default(),
        source_wms_buf: FxHashMap::default(),
        tracker: None,
        prom: Arc::new(crate::engine_metrics::EngineMetrics::new(
            &prometheus::Registry::new(),
        )),
        #[cfg(feature = "cluster")]
        checkpoint_barrier_timings: Arc::new(
            crate::checkpoint_timing::CheckpointBarrierTimingLedger::new(),
        ),
        pipeline_watermark: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
        coordinator: Arc::new(tokio::sync::Mutex::new(None)),
        table_store: Arc::new(parking_lot::RwLock::new(
            crate::table_store::TableStore::new(),
        )),
        mv_store: Arc::new(parking_lot::RwLock::new(mv_store)),
        mv_store_has_any,
        filter_ctx: SessionContext::new(),
        compiled_sink_filters: Vec::new(),
        pending_sink_filter_compiles: 0,
        delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
        serialization_timeout: Duration::from_secs(1),
        checkpoint_state_cap_bytes: 512 * 1024 * 1024,
        checkpoint_serialization_gate: Arc::new(tokio::sync::Semaphore::new(1)),
        checkpoint_timeout: Duration::from_secs(1),
        checkpoint_cleanup_timeout: Duration::from_secs(1),
        sink_event_rx,
        sink_timed_out: false,
        sink_fault: None,
        checkpoint_fault: Arc::new(parking_lot::Mutex::new(None)),
        last_checkpoint_admission_failure: None,
        checkpoint_admission_recovering: false,
        shutdown_signal: Arc::new(tokio::sync::Notify::new()),
        #[cfg(feature = "cluster")]
        cluster_controller: None,
        #[cfg(feature = "cluster")]
        shuffle_delivery_loss_incidents: None,
        #[cfg(feature = "cluster")]
        shuffle_recovered_delivery_loss_incidents: None,
        #[cfg(feature = "cluster")]
        shuffle_delivery_loss_incidents_seen: 0,
        #[cfg(feature = "cluster")]
        vnode_registry: None,
        #[cfg(feature = "cluster")]
        reconciled_source_handoff_version: None,
        #[cfg(feature = "cluster")]
        follower_tail: Arc::default(),
        #[cfg(feature = "cluster")]
        barrier_injectors: Vec::new(),
        #[cfg(feature = "cluster")]
        pending_follower_checkpoint: None,
        #[cfg(feature = "cluster")]
        checkpoint_leader_proofs: FxHashMap::default(),
        subscription_registry: Arc::new(crate::subscription::SubscriptionRegistry::new()),
        named_stream_names: rustc_hash::FxHashSet::default(),
        checkpoint_complete_tx,
        checkpoint_tail_tasks: tokio::task::JoinSet::new(),
        checkpoint_in_flight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        #[cfg(feature = "cluster")]
        delta_rebase_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        #[cfg(feature = "cluster")]
        last_vnode_capture_epoch: None,
        epoch_allocator: None,
        #[cfg(feature = "cluster")]
        quorum_timeout: Duration::from_secs(1),
        checkpoint_committable_sinks: false,
        intake_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    }
}

#[tokio::test]
async fn cancelled_graph_pass_is_a_sticky_pipeline_fault() {
    let mut callback = empty_callback_fixture();
    let (entered_tx, mut entered_rx) = tokio::sync::oneshot::channel();
    callback.graph.push_test_node(
        "pending-graph-pass",
        Box::new(PendingGraphPassOperator {
            entered: Some(entered_tx),
        }),
    );
    let sources = FxHashMap::default();
    let mut cycle = Box::pin(crate::pipeline::PipelineCallback::execute_cycle(
        &mut callback,
        &sources,
        i64::MIN,
    ));
    tokio::select! {
        entered = &mut entered_rx => entered.expect("pending operator dropped its signal"),
        _ = &mut cycle => panic!("graph pass completed before cancellation"),
        () = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("graph pass did not reach the pending operator")
        }
    }
    drop(cycle);

    let first = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
        .expect("the callback must expose graph-generation poison");
    assert!(first.contains("cancelled or panicked"), "{first}");
    assert_eq!(
        crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback),
        Some(first),
        "taking the supervisor fault must not clear the graph-generation fence"
    );
}

#[test]
fn checkpoint_admission_failure_reporting_is_edge_deduplicated() {
    let mut callback = empty_callback_fixture();
    assert!(callback.mark_checkpoint_admission_failure("authority unavailable"));
    assert!(!callback.mark_checkpoint_admission_failure("authority unavailable"));
    assert!(callback.mark_checkpoint_admission_failure("assignment unavailable"));
    assert!(callback.observe_checkpoint_recovery_state(true));
    assert!(callback.mark_checkpoint_admission_failure("assignment unavailable"));
    assert!(callback.observe_checkpoint_recovery_state(true));
    assert!(!callback.mark_checkpoint_admission_failure("assignment unavailable"));
    assert!(!callback.observe_checkpoint_recovery_state(false));
    assert!(callback.mark_checkpoint_admission_failure("assignment unavailable"));
}

#[tokio::test]
async fn zero_cycle_barrier_is_not_suppressed_by_process_metrics() {
    let mut callback = empty_callback_fixture();
    callback.sink_timed_out = true;
    let mut source_checkpoint = SourceCheckpoint::new();
    source_checkpoint.set_offset("seq", "4096");
    let mut source_checkpoints = FxHashMap::default();
    source_checkpoints.insert("gen".to_string(), source_checkpoint);

    assert_eq!(callback.prom.cycles.get(), 0);
    let outcome = crate::pipeline::PipelineCallback::checkpoint_with_barrier(
        &mut callback,
        source_checkpoints,
        CheckpointAttempt::new(38, 38),
        std::time::Instant::now(),
        None,
    )
    .await;

    assert!(matches!(
        outcome,
        crate::pipeline::BarrierOutcome::Skipped(
            crate::pipeline::SkipReason::PreservingReplayWindowAfterSinkTimeout
        )
    ));
    assert!(!callback.sink_timed_out);
}

#[cfg(feature = "cluster")]
struct ClusterCallbackFixture {
    callback: ConnectorPipelineCallback,
    source: Arc<crate::catalog::SourceEntry>,
}

#[cfg(feature = "cluster")]
fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

#[cfg(feature = "cluster")]
fn local_controller_with_kv() -> (
    Arc<laminar_core::cluster::control::ClusterController>,
    Arc<laminar_core::cluster::control::InMemoryKv>,
) {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo};

    let node_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(node_id));
    let control_kv: Arc<dyn ClusterKv> = kv.clone();
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    (
        Arc::new(laminar_core::cluster::control::ClusterController::new(
            node_id, control_kv, None, members_rx,
        )),
        kv,
    )
}

#[cfg(feature = "cluster")]
fn local_controller() -> Arc<laminar_core::cluster::control::ClusterController> {
    local_controller_with_kv().0
}

#[cfg(feature = "cluster")]
struct AuthoritativeLocalLeader {
    controller: Arc<laminar_core::cluster::control::ClusterController>,
    fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    proof: laminar_core::checkpoint::LeaderProof,
}

#[cfg(feature = "cluster")]
impl AuthoritativeLocalLeader {
    fn prepare(
        &self,
        attempt: CheckpointAttempt,
    ) -> laminar_core::cluster::control::BarrierAnnouncement {
        use laminar_core::cluster::control::Phase;

        certified_barrier(
            attempt,
            self.fence.clone(),
            self.proof.clone(),
            Phase::Prepare,
        )
    }
}

#[cfg(feature = "cluster")]
async fn authoritative_local_leader(
    control_kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
) -> AuthoritativeLocalLeader {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::{
        ClusterController, LeaderLeaseOwner, LeaderLeaseStore, LeaseDeadline, LeaseOutcome,
        ProcessLeaseAuthority, ProcessLeaseOutcome,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo};

    let node = NodeId(1);
    let boot = "00000000-0000-0000-0000-000000000001".parse().unwrap();
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&control_kv),
        control_kv,
        None,
        members_rx,
        boot,
    ));
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(30)));
    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();

    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(
            Arc::new(object_store::memory::InMemory::new()),
            Duration::from_secs(30),
        )
        .unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
        .store_for(node)
        .try_acquire(boot, 0)
        .await
        .unwrap()
    else {
        panic!("test leader must acquire its stable-node process lease");
    };
    controller
        .set_process_lease_authority(process_authority)
        .unwrap();
    controller
        .publish_leased_recovery_incarnation(&process_lease)
        .await
        .unwrap();

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let authority = Arc::new(LeaderLeaseStore::new(backing, 1_000));
    let owner = LeaderLeaseOwner {
        node,
        boot,
        process_term: process_lease.term,
    };
    let LeaseOutcome::Acquired(grant) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("test leader must acquire its durable term");
    };
    let (_grant_tx, grant_rx) = tokio::sync::watch::channel(Some(grant));
    controller
        .set_leader_lease_watch(grant_rx, owner, Arc::clone(&deadline))
        .unwrap();
    controller.set_leader_lease_store(authority);
    controller.install_local_leader_proof_provider();
    controller
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &process_lease)
        .await
        .unwrap();

    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[node.0],
        vec![CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: boot,
        }],
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let proof = controller.capture_leader_proof().unwrap();
    AuthoritativeLocalLeader {
        controller,
        fence,
        proof,
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn leader_only_prepare_quorum_admits_exact_aligned() {
    use crate::checkpoint_coordinator::{CheckpointCoordinator, PrepareQuorum};
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;

    let kv = Arc::new(InMemoryKv::new(NodeId(1)));
    let control_kv: Arc<dyn ClusterKv> = kv.clone();
    let leader = authoritative_local_leader(control_kv).await;
    let attempt = CheckpointAttempt::new(7, 7);
    let prepare = leader.prepare(attempt);
    leader
        .controller
        .announce_prepare_barrier(&prepare, Duration::from_secs(1))
        .await
        .unwrap();

    let (watermark, participants) = CheckpointCoordinator::run_prepare_quorum(
        &leader.controller,
        Duration::from_secs(1),
        PrepareQuorum::new(
            attempt,
            CheckpointWatermark::Active(100),
            &leader.fence,
            &leader.proof,
            false,
        ),
    )
    .await
    .unwrap();
    assert_eq!(watermark, CheckpointWatermark::Active(100));
    assert!(participants.is_empty());

    let aligned = BarrierAnnouncement {
        phase: Phase::Aligned,
        ..prepare
    };
    leader.controller.announce_barrier(&aligned).await.unwrap();
    let durable = kv.read_from(NodeId(1), ANNOUNCEMENT_KEY).await.unwrap();
    assert_eq!(
        serde_json::from_str::<BarrierAnnouncement>(&durable).unwrap(),
        aligned
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn source_less_leader_holds_rotation_fence_through_whole_and_vnode_capture() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeId;
    use laminar_core::state::VnodeRegistry;

    let node = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(node));
    let control_kv: Arc<dyn ClusterKv> = kv;
    let leader = authoritative_local_leader(control_kv).await;
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    registry.set_assignment_and_version(vec![node].into(), leader.fence.assignment_version);

    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&leader.controller));
    callback.vnode_registry = Some(registry);
    let (rotation_fence, whole_capture_observed, vnode_capture_observed) =
        install_checkpoint_rotation_fence_audit(&mut callback);
    let attempt = CheckpointAttempt::new(9, 9);
    let expected_assignment_version = leader.fence.assignment_version;
    let expected_assignment_digest = leader.fence.digest();
    let expected_process = leader
        .controller
        .try_live_local_process_authority_identity()
        .unwrap();
    callback
        .checkpoint_leader_proofs
        .insert(attempt, leader.proof);

    let outcome = crate::pipeline::PipelineCallback::checkpoint_with_barrier(
        &mut callback,
        FxHashMap::default(),
        attempt,
        std::time::Instant::now(),
        Some(leader.fence),
    )
    .await;

    assert!(matches!(outcome, crate::pipeline::BarrierOutcome::Async));
    assert!(whole_capture_observed.load(std::sync::atomic::Ordering::Acquire));
    assert!(vnode_capture_observed.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        callback
            .prom
            .checkpoint_pipeline_stall_duration
            .get_sample_count(),
        1
    );
    let timing = callback
        .checkpoint_barrier_timings
        .snapshot_after(0, 1)
        .unwrap();
    assert_eq!(timing.recording_loss_count, 0);
    assert_eq!(timing.records.len(), 1);
    let record = timing.records[0];
    assert_eq!(record.process, expected_process);
    assert_eq!(record.attempt, attempt);
    assert_eq!(
        record.role,
        crate::checkpoint_timing::CheckpointBarrierRole::Leader
    );
    assert_eq!(record.assignment_version, expected_assignment_version);
    assert_eq!(record.assignment_digest, expected_assignment_digest);
    assert!(record.durable_tail_handoff);
    assert!(record.local_barrier_ns <= record.pipeline_stall_ns);
    assert!(
        Arc::clone(&rotation_fence).try_write_owned().is_ok(),
        "the capture token must be released before the durable tail runs"
    );
    callback.checkpoint_tail_tasks.abort_all();
}

#[cfg(feature = "cluster")]
fn local_follower_prepare(
    controller: &laminar_core::cluster::control::ClusterController,
    attempt: CheckpointAttempt,
) -> (
    laminar_core::cluster::control::BarrierAnnouncement,
    CertifiedCheckpointAttempt,
) {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::Phase;

    let boot = controller.recovery_incarnation();
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[controller.instance_id().0],
        vec![CheckpointParticipant {
            node_id: controller.instance_id().0,
            boot_incarnation: boot,
        }],
    )
    .unwrap();
    let announcement = certified_barrier(
        attempt,
        fence,
        test_leader_proof(controller.instance_id().0, boot, 1, 1),
        Phase::Prepare,
    );
    let identity = ConnectorPipelineCallback::certified_announcement(&announcement).unwrap();
    (announcement, identity)
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn authoritative_follower_abort_cleanup_is_local_and_role_stable() {
    use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
    use laminar_core::cluster::control::{ClusterKv, LeaseDeadline, ACK_KEY};

    let (controller, kv) = local_controller_with_kv();
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    controller.set_active(true);
    assert!(
        controller.is_leader(),
        "fixture must model follower promotion"
    );

    let attempt = CheckpointAttempt::new(23, 23);
    let (announcement, identity) = local_follower_prepare(&controller, attempt);
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&controller));
    callback.pending_follower_checkpoint = Some(announcement.clone());
    assert_eq!(
        callback.follower_tail.reserve(identity),
        Ok(FollowerAdmission::Reserved)
    );

    let (release_tx, _release_rx) = tokio::sync::watch::channel(None);
    let control = crate::pipeline::callback::SourceBarrierControl::new(
        CheckpointBarrierInjector::new(),
        release_tx,
    );
    let barrier = CheckpointBarrier {
        checkpoint_id: attempt.checkpoint_id,
        epoch: attempt.epoch,
        flags: announcement.flags,
    };
    assert!(control.trigger(barrier));
    callback.barrier_injectors.push(control.clone());

    let sentinel = "pre-existing-ack".to_string();
    kv.seed(controller.instance_id(), ACK_KEY, sentinel.clone());
    crate::pipeline::PipelineCallback::resolve_authoritative_follower_abort(&mut callback, attempt)
        .unwrap();

    assert!(callback.pending_follower_checkpoint.is_none());
    assert!(callback.follower_tail.in_flight().is_empty());
    assert!(
        control.can_trigger(),
        "the unclaimed source command must be cancelled"
    );
    assert_eq!(
        kv.read_from(controller.instance_id(), ACK_KEY).await,
        Some(sentinel),
        "authoritative cleanup must not publish a negative acknowledgement"
    );
}

#[cfg(feature = "cluster")]
#[test]
fn authoritative_follower_abort_cleanup_rejects_identity_mismatch() {
    use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};

    let controller = local_controller();
    let attempt = CheckpointAttempt::new(24, 24);
    let (announcement, identity) = local_follower_prepare(&controller, attempt);
    let mut callback = empty_callback_fixture();
    callback.pending_follower_checkpoint = Some(announcement.clone());
    assert_eq!(
        callback.follower_tail.reserve(identity.clone()),
        Ok(FollowerAdmission::Reserved)
    );
    let (release_tx, _release_rx) = tokio::sync::watch::channel(None);
    let control = crate::pipeline::callback::SourceBarrierControl::new(
        CheckpointBarrierInjector::new(),
        release_tx,
    );
    assert!(control.trigger(CheckpointBarrier {
        checkpoint_id: attempt.checkpoint_id,
        epoch: attempt.epoch,
        flags: announcement.flags,
    }));
    callback.barrier_injectors.push(control.clone());

    let error = crate::pipeline::PipelineCallback::resolve_authoritative_follower_abort(
        &mut callback,
        CheckpointAttempt::new(attempt.epoch + 1, attempt.checkpoint_id + 1),
    )
    .unwrap_err();

    assert!(error.contains("does not match pending"), "{error}");
    assert!(callback.pending_follower_checkpoint.is_some());
    assert_eq!(callback.follower_tail.in_flight(), vec![identity]);
    assert!(
        !control.can_trigger(),
        "a mismatched cleanup cannot release the command"
    );
    assert!(callback.checkpoint_fault.lock().is_some());
}

#[cfg(feature = "cluster")]
#[test]
fn authoritative_follower_abort_cleanup_keeps_command_when_reservation_is_missing() {
    use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};

    let controller = local_controller();
    let attempt = CheckpointAttempt::new(25, 25);
    let (announcement, _identity) = local_follower_prepare(&controller, attempt);
    let mut callback = empty_callback_fixture();
    callback.pending_follower_checkpoint = Some(announcement.clone());
    let (release_tx, _release_rx) = tokio::sync::watch::channel(None);
    let control = crate::pipeline::callback::SourceBarrierControl::new(
        CheckpointBarrierInjector::new(),
        release_tx,
    );
    assert!(control.trigger(CheckpointBarrier {
        checkpoint_id: attempt.checkpoint_id,
        epoch: attempt.epoch,
        flags: announcement.flags,
    }));
    callback.barrier_injectors.push(control.clone());

    let error = crate::pipeline::PipelineCallback::resolve_authoritative_follower_abort(
        &mut callback,
        attempt,
    )
    .unwrap_err();

    assert!(error.contains("no reserved identity"), "{error}");
    assert!(callback.pending_follower_checkpoint.is_some());
    assert!(
        !control.can_trigger(),
        "a failed reservation transition cannot release the source command"
    );
    assert!(callback.checkpoint_fault.lock().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_rejection_validates_identity_before_cancelling_source_command() {
    use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
    use laminar_core::cluster::control::{ClusterKv, ACK_KEY};

    let (controller, kv) = local_controller_with_kv();
    let attempt = CheckpointAttempt::new(29, 29);
    let (mut announcement, _identity) = local_follower_prepare(&controller, attempt);
    announcement.leader_proof = None;
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&controller));
    callback.pending_follower_checkpoint = Some(announcement.clone());
    let (release_tx, _release_rx) = tokio::sync::watch::channel(None);
    let control = crate::pipeline::callback::SourceBarrierControl::new(
        CheckpointBarrierInjector::new(),
        release_tx,
    );
    assert!(control.trigger(CheckpointBarrier {
        checkpoint_id: attempt.checkpoint_id,
        epoch: attempt.epoch,
        flags: announcement.flags,
    }));
    callback.barrier_injectors.push(control.clone());

    let error = crate::pipeline::PipelineCallback::cancel_source_barrier_attempt(
        &mut callback,
        attempt,
        "injected rejection",
    )
    .await
    .unwrap_err();

    assert!(error.contains("lost its certified identity"), "{error}");
    assert!(callback.pending_follower_checkpoint.is_some());
    assert!(
        !control.can_trigger(),
        "an uncertified attempt cannot mutate the source command"
    );
    assert_eq!(kv.read_from(controller.instance_id(), ACK_KEY).await, None);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn source_less_immediate_follower_holds_rotation_fence_through_capture() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, LeaseDeadline, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use laminar_core::state::VnodeRegistry;

    let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let controller = Arc::new(controller);
    let fence = assignment_fence(19, &[1, 7]);
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let registry = Arc::new(VnodeRegistry::new_unassigned(2));
    registry.set_assignment_and_version(vec![NodeId(1), NodeId(7)].into(), 19);
    let attempt = CheckpointAttempt::new(30, 30);
    let announcement = BarrierAnnouncement {
        epoch: attempt.epoch,
        checkpoint_id: attempt.checkpoint_id,
        assignment_fence: Some(fence),
        leader_proof: Some(leader_proof(1)),
        phase: Phase::Prepare,
        flags: 0,
    };
    kv.seed(
        leader_id,
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&announcement).unwrap(),
    );

    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);
    callback.vnode_registry = Some(registry);
    callback.checkpoint_committable_sinks = true;
    let (rotation_fence, whole_capture_observed, vnode_capture_observed) =
        install_checkpoint_rotation_fence_audit(&mut callback);
    let checkpoint_in_flight = Arc::clone(&callback.checkpoint_in_flight);
    let coordinator = Arc::clone(&callback.coordinator);
    let coordinator_guard = coordinator.lock().await;

    let checkpoint = crate::pipeline::PipelineCallback::service_checkpoint_control(
        &mut callback,
        FxHashMap::default(),
    );
    tokio::pin!(checkpoint);
    let assignment_writer = assignment_writer_after_checkpoint_tail_handoff(
        "immediate follower",
        checkpoint.as_mut(),
        &checkpoint_in_flight,
        &rotation_fence,
    )
    .await;
    assert!(whole_capture_observed.load(std::sync::atomic::Ordering::Acquire));
    assert!(vnode_capture_observed.load(std::sync::atomic::Ordering::Acquire));

    drop(coordinator_guard);
    let outcome = tokio::time::timeout(Duration::from_secs(1), &mut checkpoint)
        .await
        .expect("immediate follower tail did not finish after coordinator release");
    assert!(matches!(
        outcome,
        crate::pipeline::CheckpointControlOutcome::Started {
            attempt: observed,
            captured: true,
        } if observed == attempt
    ));
    drop(assignment_writer);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn retained_follower_capture_keeps_ownership_after_promotion() {
    use laminar_core::cluster::control::{LeaseDeadline, Phase};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let controller = local_controller();
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    controller.set_active(true);
    assert!(
        controller.is_leader(),
        "fixture must model follower promotion"
    );
    let registry = Arc::new(VnodeRegistry::single_owner(1, NodeId(1)));
    let fence = local_assignment_fence(&controller, registry.assignment_version());
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let mut fixture = cluster_callback_fixture(registry, Arc::clone(&controller), None, None);
    fixture.callback.checkpoint_committable_sinks = true;
    let (rotation_fence, whole_capture_observed, vnode_capture_observed) =
        install_checkpoint_rotation_fence_audit(&mut fixture.callback);

    let attempt = CheckpointAttempt::new(26, 26);
    let announcement = certified_barrier(
        attempt,
        fence,
        test_leader_proof(
            controller.instance_id().0,
            controller.recovery_incarnation(),
            1,
            1,
        ),
        Phase::Prepare,
    );
    let identity = ConnectorPipelineCallback::certified_announcement(&announcement).unwrap();
    assert_eq!(
        fixture.callback.follower_tail.reserve(identity),
        Ok(FollowerAdmission::Reserved)
    );
    fixture.callback.pending_follower_checkpoint = Some(announcement);
    let checkpoint_in_flight = Arc::clone(&fixture.callback.checkpoint_in_flight);
    let coordinator = Arc::clone(&fixture.callback.coordinator);
    let coordinator_guard = coordinator.lock().await;

    let mut checkpoint = Box::pin(crate::pipeline::PipelineCallback::checkpoint_with_barrier(
        &mut fixture.callback,
        FxHashMap::default(),
        attempt,
        std::time::Instant::now(),
        None,
    ));
    let assignment_writer = assignment_writer_after_checkpoint_tail_handoff(
        "deferred follower",
        checkpoint.as_mut(),
        &checkpoint_in_flight,
        &rotation_fence,
    )
    .await;
    assert!(whole_capture_observed.load(std::sync::atomic::Ordering::Acquire));
    assert!(vnode_capture_observed.load(std::sync::atomic::Ordering::Acquire));

    drop(coordinator_guard);
    let outcome = tokio::time::timeout(Duration::from_secs(1), &mut checkpoint)
        .await
        .expect("deferred follower tail did not finish after coordinator release");
    assert!(matches!(outcome, crate::pipeline::BarrierOutcome::Async));
    drop(assignment_writer);
    drop(checkpoint);
    assert!(fixture.callback.pending_follower_checkpoint.is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn promoted_follower_faults_on_retained_attempt_mismatch() {
    use laminar_core::cluster::control::LeaseDeadline;

    let controller = local_controller();
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    controller.set_active(true);
    assert!(
        controller.is_leader(),
        "fixture must model follower promotion"
    );
    let retained = CheckpointAttempt::new(27, 27);
    let (announcement, _identity) = local_follower_prepare(&controller, retained);
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);
    callback.pending_follower_checkpoint = Some(announcement);

    let outcome = crate::pipeline::PipelineCallback::checkpoint_with_barrier(
        &mut callback,
        FxHashMap::default(),
        CheckpointAttempt::new(28, 28),
        std::time::Instant::now(),
        None,
    )
    .await;

    assert!(matches!(outcome, crate::pipeline::BarrierOutcome::Failed));
    assert!(callback.pending_follower_checkpoint.is_some());
    assert!(callback
        .checkpoint_fault
        .lock()
        .as_deref()
        .is_some_and(|error| error.contains("does not match source barrier")));
}

#[cfg(feature = "cluster")]
fn test_leader_proof(
    node_id: u64,
    boot_id: uuid::Uuid,
    process_term: u64,
    fencing_token: u64,
) -> laminar_core::cluster::control::LeaderProof {
    laminar_core::cluster::control::LeaderProof {
        owner: laminar_core::checkpoint::LeaderProofOwner {
            node_id,
            boot_id,
            process_term,
        },
        fencing_token,
    }
}

#[cfg(feature = "cluster")]
fn certified_barrier(
    attempt: CheckpointAttempt,
    assignment_fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_proof: laminar_core::cluster::control::LeaderProof,
    phase: laminar_core::cluster::control::Phase,
) -> laminar_core::cluster::control::BarrierAnnouncement {
    laminar_core::cluster::control::BarrierAnnouncement {
        epoch: attempt.epoch,
        checkpoint_id: attempt.checkpoint_id,
        assignment_fence: Some(assignment_fence),
        leader_proof: Some(leader_proof),
        phase,
        flags: 0,
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reserve_attempt_uses_durable_order_after_unannounced_leader_crash() {
    let (controller, _kv) = local_controller_with_kv();
    let abandoned = CheckpointAttempt::canonical(7);
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let initial =
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(&object_store));
    // The former leader reserves its identity before publication and crashes. There is no
    // gossip record to reconstruct, so the fixed durable counter must be authoritative.
    for expected in 1..=abandoned.checkpoint_id {
        assert_eq!(
            initial.allocate_checkpoint_id_at_least(1).await.unwrap(),
            expected
        );
    }
    let restarted =
        Arc::new(laminar_core::checkpoint_decision::CheckpointDecisionStore::new(object_store));
    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(
        laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(dir.path()),
    );
    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        store,
    )
    .await
    .unwrap();
    coordinator
        .bind_durable_decision_store(restarted)
        .await
        .unwrap();

    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);
    callback.epoch_allocator = Some(coordinator.epoch_allocator());
    let reserved = callback
        .reserve_attempt(std::time::Instant::now())
        .await
        .unwrap();

    assert_eq!(reserved, CheckpointAttempt::canonical(8));
    assert_eq!(
        reserved.relation_to(abandoned),
        CheckpointAttemptRelation::Newer
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn expired_process_lease_rejects_checkpoint_graph_drain() {
    let controller = local_controller();
    controller
        .set_process_lease_deadline(Arc::new(
            laminar_core::cluster::control::LeaseDeadline::fenced(),
        ))
        .unwrap();
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);

    let error = callback
        .drain_checkpoint_edges_until_inner(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect_err("a fenced process must not drain checkpoint graph work");

    assert!(
        error.to_string().contains("process lease expired"),
        "{error}"
    );
    assert!(callback
        .checkpoint_fault
        .lock()
        .as_deref()
        .is_some_and(|reason| reason.contains("process lease expired")));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn expired_process_lease_rejects_sink_work_before_polling_it() {
    let controller = local_controller();
    controller
        .set_process_lease_deadline(Arc::new(
            laminar_core::cluster::control::LeaseDeadline::fenced(),
        ))
        .unwrap();
    let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let future_polled = Arc::clone(&polled);

    let error = await_sink_publication(
        Some(controller.as_ref()),
        None,
        "sink enqueue",
        async move {
            future_polled.store(true, std::sync::atomic::Ordering::Release);
        },
    )
    .await
    .expect_err("a fenced process must not poll sink work");

    assert!(error.contains("process lease expired"), "{error}");
    assert!(!polled.load(std::sync::atomic::Ordering::Acquire));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_interrupts_blocked_sink_admission() {
    let controller = local_controller();
    controller
        .set_process_lease_deadline(Arc::new(
            laminar_core::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
        ))
        .unwrap();
    let fencing_controller = Arc::clone(&controller);
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        fencing_controller.fence_process_lease();
    });

    let error = tokio::time::timeout(
        Duration::from_secs(1),
        await_sink_publication(
            Some(controller.as_ref()),
            None,
            "sink enqueue",
            std::future::pending::<()>(),
        ),
    )
    .await
    .expect("lease loss did not wake blocked sink admission")
    .expect_err("lease loss must reject blocked sink admission");

    assert!(error.contains("process lease expired"), "{error}");
}

#[tokio::test]
async fn expired_checkpoint_deadline_rejects_an_already_quiescent_graph() {
    let mut callback = empty_callback_fixture();

    let error = callback
        .drain_checkpoint_edges_until_inner(tokio::time::Instant::now())
        .await
        .expect_err("the final quiescent pass must not overrun the attempt deadline");

    assert!(error.to_string().contains("end-to-end deadline"), "{error}");
    assert!(callback
        .checkpoint_fault
        .lock()
        .as_deref()
        .is_some_and(|reason| reason.contains("end-to-end deadline")));
}

#[tokio::test]
async fn rejected_sink_filter_faults_replay_guaranteed_publication() {
    use laminar_connectors::connector::{SinkConsistency, SinkInputMode, SinkTopology};

    let mut callback = empty_callback_fixture();
    let (event_tx, event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    callback.sink_event_rx = event_rx;
    let contract = SinkContract::new(
        SinkConsistency::DurableAtLeastOnce,
        SinkTopology::MultiWriter,
        SinkInputMode::AppendOnly,
    );
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "filtered".into(),
        sink_id: Arc::from("filtered"),
        connector: Box::new(laminar_connectors::testing::MockSinkConnector::new()),
        contract,
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: Duration::from_secs(5),
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    callback.sinks.push((
        "filtered".into(),
        handle.clone(),
        Some("(".into()),
        "input".into(),
        contract,
    ));
    callback.pending_sink_filter_compiles = 1;
    let mut results = FxHashMap::default();
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    results.insert(
        Arc::from("input"),
        vec![RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
        )
        .unwrap()],
    );

    let error = crate::pipeline::PipelineCallback::write_to_sinks(&mut callback, &results, None)
        .await
        .expect_err("at-least-once publication must reject an invalid sink filter");

    assert!(
        error.to_string().contains("filter compilation failed"),
        "{error}"
    );
    assert!(matches!(
        callback.compiled_sink_filters.as_slice(),
        [SinkFilter::Rejected]
    ));
    assert!(callback
        .sink_fault
        .as_deref()
        .is_some_and(|reason| reason.contains("filter compilation failed")));
    handle.close().await.unwrap();
}

#[cfg(feature = "cluster")]
fn local_assignment_fence(
    controller: &laminar_core::cluster::control::ClusterController,
    version: u64,
) -> laminar_core::checkpoint::CheckpointAssignmentFence {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    CheckpointAssignmentFence::from_owner_map(
        version,
        &[1],
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: controller.recovery_incarnation(),
        }],
    )
    .unwrap()
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn callback_publishes_prepare_directly_before_checkpoint_work() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::barrier::BARRIER_ADDR_KEY;
    use laminar_core::cluster::control::{
        ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner, LeaderLeaseStore,
        LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority, ProcessLeaseOutcome, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use laminar_core::state::VnodeRegistry;

    let member = |id| NodeInfo {
        id,
        name: format!("node-{}", id.0),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let leader_id = NodeId(1);
    let follower_id = NodeId(2);
    let leader_kv = Arc::new(InMemoryKv::new(leader_id));
    let follower_kv = Arc::new(InMemoryKv::new(follower_id));
    let leader_control_kv: Arc<dyn ClusterKv> = leader_kv.clone();
    let follower_control_kv: Arc<dyn ClusterKv> = follower_kv.clone();
    let (_leader_members_tx, leader_members_rx) =
        tokio::sync::watch::channel(vec![member(follower_id)]);
    let (_follower_members_tx, follower_members_rx) =
        tokio::sync::watch::channel(vec![member(leader_id)]);
    let leader = Arc::new(ClusterController::new(
        leader_id,
        leader_control_kv,
        None,
        leader_members_rx,
    ));
    let follower = Arc::new(ClusterController::new(
        follower_id,
        follower_control_kv,
        None,
        follower_members_rx,
    ));

    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(
            Arc::new(object_store::memory::InMemory::new()),
            Duration::from_secs(30),
        )
        .unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(leader_process) = process_authority
        .store_for(leader_id)
        .try_acquire(leader.recovery_incarnation(), 0)
        .await
        .unwrap()
    else {
        panic!("test leader must acquire its stable-node process lease");
    };
    let ProcessLeaseOutcome::Acquired(follower_process) = process_authority
        .store_for(follower_id)
        .try_acquire(follower.recovery_incarnation(), 0)
        .await
        .unwrap()
    else {
        panic!("test follower must acquire its stable-node process lease");
    };
    let leader_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(30)));
    let follower_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(30)));
    leader
        .set_process_lease_deadline(Arc::clone(&leader_deadline))
        .unwrap();
    follower
        .set_process_lease_deadline(follower_deadline)
        .unwrap();
    leader
        .set_process_lease_authority(Arc::clone(&process_authority))
        .unwrap();
    follower
        .set_process_lease_authority(process_authority)
        .unwrap();
    leader
        .publish_leased_recovery_incarnation(&leader_process)
        .await
        .unwrap();
    follower
        .publish_leased_recovery_incarnation(&follower_process)
        .await
        .unwrap();

    let authority = Arc::new(LeaderLeaseStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1_000,
    ));
    let owner = LeaderLeaseOwner {
        node: leader_id,
        boot: leader.recovery_incarnation(),
        process_term: leader_process.term,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("test leader must acquire its first durable term");
    };
    let (_lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease));
    leader
        .set_leader_lease_watch(lease_rx, owner, leader_deadline)
        .unwrap();
    leader.set_leader_lease_store(Arc::clone(&authority));
    leader.install_local_leader_proof_provider();
    follower.set_leader_lease_store(authority);

    follower
        .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
        .await
        .unwrap();
    leader
        .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
        .await
        .unwrap();
    let follower_advertisement = follower_kv
        .read_from(follower_id, BARRIER_ADDR_KEY)
        .await
        .unwrap();
    leader_kv.seed(follower_id, BARRIER_ADDR_KEY, follower_advertisement);

    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[leader_id.0, follower_id.0],
        vec![
            CheckpointParticipant {
                node_id: leader_id.0,
                boot_incarnation: leader.recovery_incarnation(),
            },
            CheckpointParticipant {
                node_id: follower_id.0,
                boot_incarnation: follower.recovery_incarnation(),
            },
        ],
    )
    .unwrap();
    let registry = Arc::new(VnodeRegistry::new_unassigned(2));
    registry.set_assignment_and_version(vec![leader_id, follower_id].into(), 1);
    leader.publish_checkpoint_assignment_fence(Some(fence.clone()));

    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&leader));
    callback.vnode_registry = Some(registry);
    callback.checkpoint_timeout = Duration::from_secs(1);
    callback.quorum_timeout = Duration::from_millis(200);

    let expired = CheckpointAttempt::new(1, 1);
    let error = crate::pipeline::PipelineCallback::publish_checkpoint_prepare(
        &mut callback,
        expired,
        std::time::Instant::now() - Duration::from_secs(2),
        Some(fence.clone()),
    )
    .await
    .expect_err("an exhausted attempt must not publish Prepare");
    assert!(error.contains("no remaining quorum window"), "{error}");
    assert!(!callback.checkpoint_leader_proofs.contains_key(&expired));
    assert!(leader_kv
        .read_from(leader_id, ANNOUNCEMENT_KEY)
        .await
        .is_none());

    let attempt = CheckpointAttempt::new(2, 2);
    let expected = certified_barrier(
        attempt,
        fence.clone(),
        leader.capture_leader_proof().unwrap(),
        laminar_core::cluster::control::Phase::Prepare,
    );
    crate::pipeline::PipelineCallback::publish_checkpoint_prepare(
        &mut callback,
        attempt,
        std::time::Instant::now(),
        Some(fence),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while follower.checkpoint_prepare_received_at(&expected).is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the callback did not start direct Prepare delivery");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_capture_request_includes_whole_operator_graph_state() {
    let controller = local_controller();
    let assignment_fence = local_assignment_fence(&controller, 17);
    let mut callback = empty_callback_fixture();
    callback.graph.push_test_node(
        "follower-checkpoint-evidence",
        Box::new(FollowerCheckpointEvidenceOperator),
    );

    // This is the request builder used by both the no-source-barrier CaptureNow branch and
    // deferred source-barrier capture.
    let (mut request, operator_state) = callback
        .build_follower_checkpoint_request_until(
            &assignment_fence,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    assert!(
        request.operator_states.is_empty(),
        "aligned capture must defer serialization to the durable tail"
    );
    request.operator_states = operator_state
        .serialize_until(
            callback.checkpoint_state_cap_bytes,
            callback.serialization_timeout,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap()
        .accept_for_test();

    assert_eq!(request.assignment_fence.as_ref(), Some(&assignment_fence));
    let bytes = request
        .operator_states
        .get("operator_graph")
        .expect("whole operator graph must be present in the follower request");
    let graph_checkpoint =
        rkyv::from_bytes::<crate::operator_graph::GraphCheckpoint, rkyv::rancor::Error>(bytes)
            .unwrap();
    assert_eq!(
        graph_checkpoint
            .operators
            .get("follower-checkpoint-evidence")
            .map(Vec::as_slice),
        Some(b"follower-whole-operator-state".as_slice())
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_capture_request_rejects_an_expired_deadline() {
    let controller = local_controller();
    let assignment_fence = local_assignment_fence(&controller, 18);
    let mut callback = empty_callback_fixture();

    let error = callback
        .build_follower_checkpoint_request_until(
            &assignment_fence,
            tokio::time::Instant::now() - Duration::from_millis(1),
        )
        .err()
        .expect("an expired deadline must reject follower capture");

    assert_eq!(
        error,
        "follower operator-state capture exhausted the checkpoint deadline"
    );
}

#[cfg(feature = "cluster")]
fn committed_source_handoff(
    assignment_fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    source_watermark: Option<i64>,
    cluster_watermark: CheckpointWatermark,
    recovery_watermark_frontier: Option<i64>,
) -> Arc<laminar_core::checkpoint::CommittedSourceHandoff> {
    use std::collections::BTreeMap;

    use laminar_core::checkpoint::{
        ClusterRecoveryCapsule, ParticipantRecoveryRef, PipelineIdentity,
        CLUSTER_RECOVERY_CAPSULE_VERSION, PIPELINE_IDENTITY_VERSION,
    };

    let source_watermarks = source_watermark
        .map(|watermark| BTreeMap::from([("orders".to_string(), watermark)]))
        .unwrap_or_default();
    let portable_state_sha256 = digest(6);
    let capsule = ClusterRecoveryCapsule {
        version: CLUSTER_RECOVERY_CAPSULE_VERSION,
        attempt: CheckpointAttempt::canonical(7),
        deployment_id: "00000000-0000-0000-0000-000000000007".into(),
        pipeline_identity: PipelineIdentity {
            canonical_version: PIPELINE_IDENTITY_VERSION,
            sha256: digest(1),
        },
        vnode_restore_contract: crate::cluster_recovery_capsule::vnode_restore_contract_for_test(
            assignment_fence.vnode_count,
        ),
        participants: vec![ParticipantRecoveryRef {
            participant_id: 1,
            readiness_sha256: digest(3),
            manifest_sha256: digest(4),
            portable_state_sha256: portable_state_sha256.clone(),
        }],
        assignment_fence,
        seal_inventory_sha256: digest(2),
        source_offsets: BTreeMap::from([("orders".into(), BTreeMap::new())]),
        source_metadata: BTreeMap::from([("orders".into(), BTreeMap::new())]),
        source_assignment_versions: BTreeMap::new(),
        source_watermarks,
        cluster_watermark,
        recovery_watermark_frontier,
        portable_state_sha256,
    };
    Arc::new(laminar_core::checkpoint::CommittedSourceHandoff::try_from(&capsule).unwrap())
}

#[cfg(feature = "cluster")]
fn cluster_callback_fixture(
    registry: Arc<laminar_core::state::VnodeRegistry>,
    controller: Arc<laminar_core::cluster::control::ClusterController>,
    reconciled_source_handoff_version: Option<u64>,
    startup_watermark: Option<i64>,
) -> ClusterCallbackFixture {
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use laminar_core::streaming::BackpressureStrategy;

    let catalog = crate::catalog::SourceCatalog::new(16, BackpressureStrategy::Block);
    let source = catalog
        .register_source(
            "orders",
            Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )])),
            vec![],
            Some("ts".into()),
            Some(Duration::ZERO),
            None,
            None,
        )
        .unwrap();
    source
        .source
        .restore_watermark_for_recovery(startup_watermark.unwrap_or(i64::MIN));

    let mut generator = laminar_core::time::BoundedOutOfOrdernessGenerator::new(0);
    laminar_core::time::WatermarkGenerator::restore_watermark_for_recovery(
        &mut generator,
        startup_watermark.unwrap_or(i64::MIN),
    );
    let watermark_states = FxHashMap::from_iter([(
        "orders".into(),
        SourceWatermarkState {
            extractor: laminar_core::time::EventTimeExtractor::from_column("ts"),
            generator: Box::new(generator),
            column: "ts".into(),
        },
    )]);
    let source_entries_for_wm = FxHashMap::from_iter([("orders".into(), Arc::clone(&source))]);
    let source_ids = FxHashMap::from_iter([("orders".into(), 0)]);
    let source_name_arcs = FxHashMap::from_iter([(0, Arc::<str>::from("orders"))]);
    let mut tracker = laminar_core::time::WatermarkTracker::new(1);
    tracker
        .restore_for_recovery(&[startup_watermark], &[false], startup_watermark)
        .unwrap();

    let (_sink_event_tx, sink_event_rx) =
        laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(1);
    let (checkpoint_complete_tx, _checkpoint_complete_rx) =
        crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(1);
    let mv_store = crate::mv_store::MvStore::new();
    let mv_store_has_any = mv_store.has_any_handle();
    let pipeline_watermark = Arc::new(std::sync::atomic::AtomicI64::new(
        startup_watermark.unwrap_or(i64::MIN),
    ));

    ClusterCallbackFixture {
        callback: ConnectorPipelineCallback {
            graph: crate::operator_graph::OperatorGraph::new(SessionContext::new()),
            stream_entries: Vec::new(),
            sinks: Vec::new(),
            owned_sink_handles: Arc::new(parking_lot::Mutex::new(Vec::new())),
            watermark_states,
            source_entries_for_wm,
            source_ids,
            source_name_arcs,
            source_wms_buf: FxHashMap::default(),
            tracker: Some(tracker),
            prom: Arc::new(crate::engine_metrics::EngineMetrics::new(
                &prometheus::Registry::new(),
            )),
            checkpoint_barrier_timings: Arc::new(
                crate::checkpoint_timing::CheckpointBarrierTimingLedger::new(),
            ),
            pipeline_watermark,
            coordinator: Arc::new(tokio::sync::Mutex::new(None)),
            table_store: Arc::new(parking_lot::RwLock::new(
                crate::table_store::TableStore::new(),
            )),
            mv_store: Arc::new(parking_lot::RwLock::new(mv_store)),
            mv_store_has_any,
            filter_ctx: SessionContext::new(),
            compiled_sink_filters: Vec::new(),
            pending_sink_filter_compiles: 0,
            delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
            serialization_timeout: Duration::from_secs(1),
            checkpoint_state_cap_bytes: 512 * 1024 * 1024,
            checkpoint_serialization_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            checkpoint_timeout: Duration::from_secs(1),
            checkpoint_cleanup_timeout: Duration::from_secs(1),
            sink_event_rx,
            sink_timed_out: false,
            sink_fault: None,
            checkpoint_fault: Arc::new(parking_lot::Mutex::new(None)),
            last_checkpoint_admission_failure: None,
            checkpoint_admission_recovering: false,
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            cluster_controller: Some(controller),
            shuffle_delivery_loss_incidents: None,
            shuffle_recovered_delivery_loss_incidents: None,
            shuffle_delivery_loss_incidents_seen: 0,
            vnode_registry: Some(registry),
            reconciled_source_handoff_version,
            follower_tail: Arc::default(),
            barrier_injectors: Vec::new(),
            pending_follower_checkpoint: None,
            checkpoint_leader_proofs: FxHashMap::default(),
            subscription_registry: Arc::new(crate::subscription::SubscriptionRegistry::new()),
            named_stream_names: rustc_hash::FxHashSet::default(),
            checkpoint_complete_tx,
            checkpoint_tail_tasks: tokio::task::JoinSet::new(),
            checkpoint_in_flight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            delta_rebase_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            last_vnode_capture_epoch: None,
            epoch_allocator: None,
            quorum_timeout: Duration::from_secs(1),
            checkpoint_committable_sinks: false,
            intake_gate: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        },
        source,
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_checkpoint_control_exposes_expired_process_authority() {
    use laminar_core::cluster::control::LeaseDeadline;

    let controller = local_controller();
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::fenced()))
        .unwrap();
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);

    let outcome = crate::pipeline::PipelineCallback::service_checkpoint_control(
        &mut callback,
        FxHashMap::default(),
    )
    .await;

    let crate::pipeline::CheckpointControlOutcome::AdmissionFailed { error } = outcome else {
        panic!("expired follower authority must be an explicit admission failure");
    };
    assert!(error.contains("cluster process lease expired"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_checkpoint_control_exposes_handoff_reconciliation_failure() {
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::state::{NodeId, VnodeRegistry};

    let controller = local_controller();
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    let fence = local_assignment_fence(&controller, 1);
    registry.set_assignment_and_version_with_source_handoff(
        vec![NodeId(1)].into(),
        1,
        committed_source_handoff(
            fence.clone(),
            Some(1_000),
            CheckpointWatermark::Active(1_000),
            Some(1_000),
        ),
    );
    controller.publish_checkpoint_assignment_fence(Some(fence));
    let mut fixture = cluster_callback_fixture(registry, controller, None, Some(1_000));
    fixture
        .callback
        .source_name_arcs
        .insert(0, Arc::from("missing"));

    let outcome = crate::pipeline::PipelineCallback::service_checkpoint_control(
        &mut fixture.callback,
        FxHashMap::default(),
    )
    .await;

    let crate::pipeline::CheckpointControlOutcome::AdmissionFailed { error } = outcome else {
        panic!("handoff reconciliation failure must not be reported as idle");
    };
    assert!(error.contains("no watermark state"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn rebuilt_callback_does_not_replay_a_carried_old_source_handoff() {
    use laminar_core::state::{NodeId, VnodeRegistry};

    let controller = local_controller();
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    let version_one_fence = local_assignment_fence(&controller, 1);
    registry.set_assignment_and_version_with_source_handoff(
        vec![NodeId(1)].into(),
        1,
        committed_source_handoff(
            version_one_fence,
            Some(1_000),
            CheckpointWatermark::Active(1_000),
            Some(1_000),
        ),
    );
    registry.set_assignment_and_version_carrying_source_handoff(vec![NodeId(1)].into(), 2);

    let version_two_fence = local_assignment_fence(&controller, 2);
    controller.publish_checkpoint_assignment_fence(Some(version_two_fence));
    let mut fixture = cluster_callback_fixture(
        Arc::clone(&registry),
        Arc::clone(&controller),
        Some(1),
        Some(2_000),
    );

    fixture
        .callback
        .reconcile_source_handoff_watermarks()
        .unwrap();

    assert_eq!(fixture.callback.reconciled_source_handoff_version, Some(1));
    assert_eq!(
        fixture
            .callback
            .tracker
            .as_ref()
            .unwrap()
            .source_watermark(0),
        Some(2_000)
    );
    assert_eq!(
        fixture.callback.watermark_states["orders"]
            .generator
            .current_watermark(),
        2_000
    );
    assert_eq!(fixture.source.source.current_watermark(), 2_000);
    assert_eq!(
        fixture
            .callback
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire),
        2_000
    );

    let version_three_fence = local_assignment_fence(&controller, 3);
    registry.set_assignment_and_version_with_source_handoff(
        vec![NodeId(1)].into(),
        3,
        committed_source_handoff(
            version_three_fence.clone(),
            Some(2_500),
            CheckpointWatermark::Active(2_500),
            Some(2_500),
        ),
    );
    controller.publish_checkpoint_assignment_fence(Some(version_three_fence));

    fixture
        .callback
        .reconcile_source_handoff_watermarks()
        .unwrap();

    assert_eq!(fixture.callback.reconciled_source_handoff_version, Some(3));
    assert_eq!(
        fixture
            .callback
            .tracker
            .as_ref()
            .unwrap()
            .source_watermark(0),
        Some(2_500)
    );
    assert_eq!(
        fixture.callback.watermark_states["orders"]
            .generator
            .current_watermark(),
        2_500
    );
    assert_eq!(fixture.source.source.current_watermark(), 2_500);
    assert_eq!(
        fixture
            .callback
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire),
        2_500
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn active_handoff_frontier_fills_a_missing_source_watermark() {
    use arrow_array::TimestampMillisecondArray;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let controller = local_controller();
    controller.publish_cluster_min_watermark(1_500);
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    let fence = local_assignment_fence(&controller, 1);
    registry.set_assignment_and_version_with_source_handoff(
        vec![NodeId(1)].into(),
        1,
        committed_source_handoff(
            fence.clone(),
            None,
            CheckpointWatermark::Active(1_500),
            Some(1_500),
        ),
    );
    controller.publish_checkpoint_assignment_fence(Some(fence));
    let mut fixture =
        cluster_callback_fixture(registry, Arc::clone(&controller), None, Some(2_000));

    fixture
        .callback
        .reconcile_source_handoff_watermarks()
        .unwrap();

    let tracker = fixture.callback.tracker.as_ref().unwrap();
    assert_eq!(tracker.source_watermark(0), Some(1_500));
    assert_eq!(tracker.current_watermark().unwrap().timestamp(), 1_500);
    assert_eq!(
        fixture.callback.watermark_states["orders"]
            .generator
            .current_watermark(),
        1_500
    );
    assert_eq!(fixture.source.source.current_watermark(), 1_500);
    assert_eq!(
        fixture
            .callback
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire),
        1_500
    );

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )])),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            1_400, 1_700,
        ]))],
    )
    .unwrap();
    let retained =
        crate::pipeline::PipelineCallback::filter_late_rows(&fixture.callback, "orders", &batch)
            .expect("the post-frontier row must survive");
    assert_eq!(retained.num_rows(), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn idle_handoff_restores_its_retained_frontier_on_a_fresh_controller() {
    use laminar_core::state::{NodeId, VnodeRegistry};

    let controller = local_controller();
    assert_eq!(controller.cluster_min_watermark(), None);
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    let fence = local_assignment_fence(&controller, 1);
    registry.set_assignment_and_version_with_source_handoff(
        vec![NodeId(1)].into(),
        1,
        committed_source_handoff(fence.clone(), None, CheckpointWatermark::Idle, Some(1_500)),
    );
    controller.publish_checkpoint_assignment_fence(Some(fence));
    let mut fixture =
        cluster_callback_fixture(registry, Arc::clone(&controller), None, Some(2_000));

    fixture
        .callback
        .reconcile_source_handoff_watermarks()
        .unwrap();

    let tracker = fixture.callback.tracker.as_ref().unwrap();
    assert_eq!(tracker.source_watermark(0), Some(1_500));
    assert_eq!(tracker.current_watermark().unwrap().timestamp(), 1_500);
    assert!(tracker.is_idle(0));
    assert_eq!(
        fixture.callback.watermark_states["orders"]
            .generator
            .current_watermark(),
        1_500
    );
    assert_eq!(fixture.source.source.current_watermark(), 1_500);
    assert_eq!(
        fixture
            .callback
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire),
        1_500
    );
    assert_eq!(
        controller.cluster_min_watermark(),
        None,
        "reconciliation must use the durable handoff frontier, not controller residue"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_capture_rejects_an_installed_unreconciled_source_handoff() {
    use laminar_core::state::{NodeId, VnodeRegistry};

    let controller = local_controller();
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    let fence = local_assignment_fence(&controller, 1);
    registry.set_assignment_and_version_with_source_handoff(
        vec![NodeId(1)].into(),
        1,
        committed_source_handoff(
            fence.clone(),
            Some(1_500),
            CheckpointWatermark::Active(1_500),
            Some(1_500),
        ),
    );
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let fixture = cluster_callback_fixture(registry, controller, None, None);

    let error = fixture
        .callback
        .validate_checkpoint_assignment(Some(&fence))
        .unwrap_err();

    assert!(error.contains("[LDB-6055]"), "unexpected error: {error}");
    assert!(
        error.contains("source handoff was not installed before checkpoint capture"),
        "unexpected error: {error}"
    );
}

#[test]
fn checkpoint_watermark_distinguishes_idle_from_uninitialized_inputs() {
    assert_eq!(
        classify_checkpoint_watermark(0, 0, i64::MIN),
        CheckpointWatermark::Idle
    );
    assert_eq!(
        classify_checkpoint_watermark(2, 0, 10),
        CheckpointWatermark::Idle
    );
    assert_eq!(
        classify_checkpoint_watermark(2, 2, i64::MIN),
        CheckpointWatermark::Uninitialized
    );
    assert_eq!(
        classify_checkpoint_watermark(2, 1, 10),
        CheckpointWatermark::Active(10)
    );
}

#[cfg(feature = "cluster")]
fn assignment_fence(
    version: u64,
    participants: &[u64],
) -> laminar_core::checkpoint::CheckpointAssignmentFence {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    let participants = participants
        .iter()
        .map(|node_id| CheckpointParticipant {
            node_id: *node_id,
            boot_incarnation: format!("00000000-0000-0000-0000-{node_id:012x}")
                .parse()
                .unwrap(),
        })
        .collect::<Vec<_>>();
    let owners = participants
        .iter()
        .map(|participant| participant.node_id)
        .collect::<Vec<_>>();
    CheckpointAssignmentFence::from_owner_map(version, &owners, participants).unwrap()
}

#[cfg(feature = "cluster")]
fn leader_proof(fencing_token: u64) -> laminar_core::cluster::control::LeaderProof {
    laminar_core::cluster::control::LeaderProof {
        owner: laminar_core::cluster::control::LeaderProofOwner {
            node_id: 1,
            boot_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            process_term: 1,
        },
        fencing_token,
    }
}

#[cfg(feature = "cluster")]
#[test]
fn allocator_epoch_jump_requires_full_vnode_capture() {
    assert!(vnode_capture_requires_full_rebase(None, 1));
    assert!(!vnode_capture_requires_full_rebase(Some(7), 8));
    assert!(vnode_capture_requires_full_rebase(Some(7), 1_000));
    assert!(vnode_capture_requires_full_rebase(Some(u64::MAX), 0));
}

#[tokio::test]
async fn destructive_operator_capture_failure_faults_at_least_once_runtime() {
    let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
    let fail_whole_capture = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut callback = empty_callback_fixture();
    callback.graph.push_test_node(
        "draining-capture",
        Box::new(DrainingCaptureFailureOperator {
            live_rows: Arc::clone(&live_rows),
            fail_whole_capture,
            #[cfg(feature = "cluster")]
            fail_vnode_capture: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }),
    );

    let error = callback
        .capture_and_serialize_operator_state()
        .await
        .unwrap_err();

    assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 0);
    assert!(error.contains("recovery from the last committed checkpoint is required"));
    let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
        .expect("mutable capture failure must become a runtime fault");
    assert_eq!(fault, error);
}

#[tokio::test]
async fn failure_after_destructive_operator_capture_faults_runtime() {
    let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
    let mut callback = empty_callback_fixture();
    callback.checkpoint_state_cap_bytes = 1;
    callback.graph.push_test_node(
        "draining-capture",
        Box::new(DrainingCaptureOperator {
            live_rows: Arc::clone(&live_rows),
        }),
    );

    let error = callback
        .capture_and_serialize_operator_state()
        .await
        .unwrap_err();

    assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 0);
    assert!(error.contains("recovery from the last committed checkpoint is required"));
    assert!(error.contains("staged-state cap"));
    let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
        .expect("a post-capture failure must become a runtime fault");
    assert_eq!(fault, error);
}

#[tokio::test]
async fn busy_serialization_gate_rejects_synchronously_before_mutating_operator_state() {
    let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
    let mut callback = empty_callback_fixture();
    callback.serialization_timeout = Duration::from_secs(1);
    callback.graph.push_test_node(
        "draining-capture",
        Box::new(DrainingCaptureOperator {
            live_rows: Arc::clone(&live_rows),
        }),
    );
    let held_permit = Arc::clone(&callback.checkpoint_serialization_gate)
        .acquire_owned()
        .await
        .unwrap();

    let error = match callback
        .capture_operator_state_until(tokio::time::Instant::now() + Duration::from_secs(10))
    {
        Ok(_) => panic!("a busy serialization gate must reject synchronously"),
        Err(error) => error,
    };

    assert!(error.contains("[LDB-6017]"), "{error}");
    assert!(error.contains("still active"), "{error}");
    assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 17);
    assert!(
        crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback).is_none(),
        "serialization contention must reject before mutable capture"
    );
    drop(held_permit);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn busy_serialization_gate_does_not_hold_assignment_writer_until_timeout() {
    let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
    let mut callback = empty_callback_fixture();
    callback.serialization_timeout = Duration::from_secs(1);
    callback.graph.push_test_node(
        "draining-capture",
        Box::new(DrainingCaptureOperator {
            live_rows: Arc::clone(&live_rows),
        }),
    );
    let rotation_fence = Arc::new(tokio::sync::RwLock::new(()));
    callback
        .graph
        .set_rotation_execution_fence(Arc::clone(&rotation_fence));
    let held_permit = Arc::clone(&callback.checkpoint_serialization_gate)
        .acquire_owned()
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let capture_guard = callback
        .checkpoint_capture_rotation_guard_until(None, deadline)
        .await
        .unwrap()
        .expect("configured callback must acquire its rotation token");

    let error = match callback.capture_operator_state_until(deadline) {
        Ok(_) => panic!("serialization contention must not wait under the rotation token"),
        Err(error) => error,
    };
    assert!(error.contains("[LDB-6017]"), "{error}");
    assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 17);
    assert!(
        crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback).is_none(),
        "contention rejects before mutable capture and must not create a second fault"
    );

    let mut assignment_writer = Box::pin(Arc::clone(&rotation_fence).write_owned());
    assert!(matches!(
        futures::poll!(&mut assignment_writer),
        std::task::Poll::Pending
    ));
    drop(capture_guard);
    let assignment_writer = tokio::time::timeout(Duration::from_secs(1), assignment_writer)
        .await
        .expect("assignment publication remained blocked after capture rejection");
    drop(assignment_writer);
    drop(held_permit);
}

#[tokio::test]
async fn dropping_serialized_destructive_image_before_commit_faults_runtime() {
    let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
    let mut callback = empty_callback_fixture();
    callback.graph.push_test_node(
        "draining-capture",
        Box::new(DrainingCaptureOperator {
            live_rows: Arc::clone(&live_rows),
        }),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let capture = callback.capture_operator_state_until(deadline).unwrap();
    let budget = encoded_operator_state_budget(
        callback.checkpoint_state_cap_bytes,
        capture.estimated_bytes(),
        0,
    )
    .unwrap();
    let serialized = capture
        .serialize_until(budget, callback.serialization_timeout, deadline)
        .await
        .unwrap();

    assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 0);
    drop(serialized);
    let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
        .expect("an uncommitted destructive image must require recovery");
    assert!(fault.contains("recovery from the last committed checkpoint is required"));
}

#[cfg(feature = "cluster")]
#[test]
fn destructive_vnode_capture_failure_faults_runtime() {
    let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(23));
    let mut callback = empty_callback_fixture();
    callback.graph.set_test_vnode_count(4);
    callback.graph.push_test_node(
        "draining-vnode-capture",
        Box::new(DrainingCaptureFailureOperator {
            live_rows: Arc::clone(&live_rows),
            fail_whole_capture: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            fail_vnode_capture: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }),
    );

    let error = callback.capture_vnode_states(41).unwrap_err();

    assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 0);
    assert!(error.contains("recovery from the last committed checkpoint is required"));
    let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
        .expect("mutable vnode capture failure must become a runtime fault");
    assert_eq!(fault, error);
}

#[cfg(feature = "cluster")]
#[test]
fn shuffle_delivery_loss_incidents_are_forgiven_only_after_recovery_completion() {
    let mut seen = 0;

    assert_eq!(
        observe_unrecovered_delivery_loss_incidents(2, 0, &mut seen, true),
        None,
        "an active rewind must not re-fault the replacement callback"
    );
    assert_eq!(seen, 0, "an incomplete rewind must not hide the loss");

    assert_eq!(
        observe_unrecovered_delivery_loss_incidents(2, 2, &mut seen, false),
        None,
        "the completed rewind covers its captured loss cutoff"
    );
    assert_eq!(seen, 2);

    assert_eq!(
        observe_unrecovered_delivery_loss_incidents(3, 2, &mut seen, false),
        Some(1),
        "loss after the captured cutoff still requires recovery"
    );
    assert_eq!(seen, 3);
}

#[test]
fn ambiguous_decision_faults_at_least_once_pipeline() {
    let result = crate::checkpoint_coordinator::CheckpointResult {
        success: false,
        checkpoint_id: 7,
        epoch: 7,
        duration: std::time::Duration::ZERO,
        error: Some("decision outcome is in doubt".into()),
        failure_disposition: Some(
            crate::checkpoint_coordinator::CheckpointFailureDisposition::RequiresRecovery,
        ),
    };

    assert!(checkpoint_failure_requires_pipeline_fault(&result, false));
}
use crate::error::DbError;

#[test]
fn source_checkpoint_map_materializes_offsets_and_metadata() {
    use laminar_connectors::checkpoint::PersistentOffset;

    let mut inventory = PersistentOffset::new("[", ",", "]");
    inventory.push_fragment(r#""first.parquet""#);
    inventory.push_fragment(r#""second.parquet""#);
    let mut files = SourceCheckpoint::new();
    files.set_offset("row", "17");
    files.set_persistent_offset("manifest", inventory);
    files.set_metadata("connector", "file");
    files.set_metadata("schema_sha256", "abc123");

    let mut partitioned = SourceCheckpoint::new();
    partitioned.set_offset("orders:0", "42");
    let mut snapshots = FxHashMap::default();
    snapshots.insert("files".to_string(), files.clone());
    snapshots.insert("orders".to_string(), partitioned);

    let materialized = materialize_source_checkpoint_map(snapshots);
    let files_durable = materialized.get("files").expect("files checkpoint");
    assert_eq!(
        files_durable.offsets.get("manifest").map(String::as_str),
        Some(r#"["first.parquet","second.parquet"]"#)
    );
    assert_eq!(
        files_durable.offsets.get("row").map(String::as_str),
        Some("17")
    );
    assert_eq!(
        files_durable
            .metadata
            .get("schema_sha256")
            .map(String::as_str),
        Some("abc123")
    );
    assert_eq!(
        materialized
            .get("orders")
            .and_then(|checkpoint| checkpoint.offsets.get("orders:0"))
            .map(String::as_str),
        Some("42")
    );
}

#[tokio::test]
async fn source_checkpoint_materialization_rejects_expired_deadline() {
    let attempt = CheckpointAttempt::new(7, 7);
    let result = materialize_source_checkpoints_until(
        FxHashMap::default(),
        attempt,
        tokio::time::Instant::now(),
    )
    .await;
    let Err(error) = result else {
        panic!("an expired absolute deadline must reject materialization");
    };
    assert!(error.contains("checkpoint 7 epoch 7"));
    assert!(error.contains("before source-offset materialization"));
}

#[test]
fn mv_state_is_retained_when_graph_has_no_snapshot() {
    let mv_bytes = bytes::Bytes::from_static(b"materialized-view-state");
    let states =
        combine_operator_checkpoint_states(None, [("mv:test_view".to_string(), mv_bytes.clone())]);

    assert_eq!(states.get("mv:test_view"), Some(&mv_bytes));
    assert!(!states.contains_key("operator_graph"));
}

#[tokio::test]
async fn test_backpressure_fail_notifies_shutdown() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let err = DbError::BackpressureFail("downstream of 'q'".into());
    let mapped = ConnectorPipelineCallback::map_graph_error(&err, &notify);
    assert!(
        matches!(&mapped, CycleError::Halt(m) if m.contains("Backpressure fail")),
        "unexpected: {mapped:?}"
    );

    tokio::time::timeout(Duration::from_millis(50), notify.notified())
        .await
        .expect("shutdown should have been notified");
}

#[tokio::test]
async fn terminal_shuffle_routing_halts_and_notifies_shutdown() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let error = DbError::ShuffleTerminal("oversized routed row".into());

    let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);

    assert!(matches!(
        &mapped,
        CycleError::Halt(message) if message.contains("oversized routed row")
    ));
    tokio::time::timeout(Duration::from_millis(50), notify.notified())
        .await
        .expect("terminal routing must notify shutdown");
}

#[tokio::test]
async fn managed_state_budget_exhaustion_halts_and_notifies_shutdown() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let error = DbError::ManagedStateBudgetExceeded {
        context: "operator 'agg' record processing".into(),
        accounted_bytes: 257,
        limit_bytes: 256,
    };

    let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);

    assert!(matches!(
        &mapped,
        CycleError::Halt(message)
            if message.contains(laminar_core::error_codes::MANAGED_STATE_BUDGET_EXCEEDED)
                && message.contains("257")
                && message.contains("256")
    ));
    tokio::time::timeout(Duration::from_millis(50), notify.notified())
        .await
        .expect("managed-state exhaustion must notify shutdown");
}

#[tokio::test]
async fn retractable_extremum_checkpoint_budget_exhaustion_is_terminal() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let error = DbError::RetractableExtremumCheckpointBudgetExceeded {
        context: "aggregate 'agg' delta capture".into(),
        charged_bytes: 1_048_577,
        limit_bytes: 1_048_576,
    };

    assert_eq!(
        error.code(),
        laminar_core::error_codes::RETRACTABLE_EXTREMUM_CHECKPOINT_BUDGET_EXCEEDED
    );
    assert!(error.requires_pipeline_halt());
    assert!(!error.requires_pipeline_recovery());
    assert!(!error.is_transient());

    let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);
    assert!(matches!(
        &mapped,
        CycleError::Halt(message)
            if message.contains(
                laminar_core::error_codes::RETRACTABLE_EXTREMUM_CHECKPOINT_BUDGET_EXCEEDED
            ) && message.contains("1048577")
                && message.contains("1048576")
    ));
    tokio::time::timeout(Duration::from_millis(50), notify.notified())
        .await
        .expect("retractable-extremum checkpoint exhaustion must notify shutdown");
}

#[tokio::test]
async fn partial_shuffle_send_requires_recovery_without_shutdown() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let error = DbError::ShufflePartialSend("peer accepted an earlier frame".into());

    let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);

    assert!(matches!(
        &mapped,
        CycleError::Recovery(message) if message.contains("peer accepted an earlier frame")
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), notify.notified())
            .await
            .is_err(),
        "recovery is coordinator-owned and must not signal terminal shutdown"
    );
}

#[tokio::test]
async fn indeterminate_stateful_apply_requires_recovery_without_shutdown() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let error = DbError::StatefulOperatorPartialApply(
        "aggregate state may have changed before acknowledgement".into(),
    );

    let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);

    assert!(matches!(
        &mapped,
        CycleError::Recovery(message)
            if message.contains("state may have changed before acknowledgement")
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), notify.notified())
            .await
            .is_err(),
        "coordinated recovery, rather than terminal shutdown, owns the fresh-state restart"
    );
}

#[tokio::test]
async fn checkpoint_drain_preserves_terminal_shuffle_halt() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let error = DbError::ShuffleTerminal("invalid routing structure".into());

    let mapped = ConnectorPipelineCallback::map_checkpoint_drain_error(&error, &notify);

    assert!(matches!(
        &mapped,
        CycleError::Halt(message)
            if message.contains("checkpoint graph drain halted")
                && message.contains("invalid routing structure")
    ));
    tokio::time::timeout(Duration::from_millis(50), notify.notified())
        .await
        .expect("checkpoint drain terminal error must notify shutdown");
}

#[tokio::test]
async fn test_non_backpressure_error_does_not_notify() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let err = DbError::Pipeline("unrelated".into());
    let mapped = ConnectorPipelineCallback::map_graph_error(&err, &notify);
    assert!(
        matches!(mapped, CycleError::Fatal(_)),
        "non-Fail errors must classify as Fatal"
    );

    let got = tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
    assert!(got.is_err(), "non-Fail errors must not trigger shutdown");
}

#[tokio::test]
async fn vnode_rehydration_checkpoint_error_requires_whole_graph_recovery() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let error = DbError::Checkpoint("vnode apply failed after a partial graph swap".into());

    let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);

    assert!(matches!(mapped, CycleError::Recovery(_)));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), notify.notified())
            .await
            .is_err(),
        "the coordinator owns recovery; this is not a graceful shutdown"
    );
}

#[tokio::test]
async fn reserved_attempt_cleanup_deadline_includes_coordinator_lock() {
    let coordinator = tokio::sync::Mutex::new(None);
    let lock = coordinator.lock().await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
    let started = std::time::Instant::now();

    let error = cleanup_reserved_attempt_until(
        &coordinator,
        CheckpointAttempt::new(7, 7),
        "injected admission failure".into(),
        None,
        None,
        deadline,
    )
    .await
    .unwrap_err();

    assert!(error.contains("cleanup exceeded its end-to-end deadline"));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "coordinator lock contention must not refresh or bypass the cleanup deadline"
    );
    drop(lock);
}

/// Rejected must never pass rows through.
#[test]
fn rejected_filter_dispatches_to_drop_not_passthrough() {
    let filters = [SinkFilter::Rejected];
    let dispatch = match filters.first().cloned() {
        Some(SinkFilter::Compiled(phys)) => SinkFilterDispatch::Compiled(phys),
        Some(SinkFilter::Rejected) => SinkFilterDispatch::Rejected,
        Some(SinkFilter::Pending) | None => SinkFilterDispatch::None,
    };
    assert!(
        matches!(dispatch, SinkFilterDispatch::Rejected),
        "Rejected filter must map to Rejected dispatch (drop), not None (passthrough)"
    );
}

/// Pending / absent → no filter (compilation runs before the dispatch loop).
#[test]
fn pending_and_absent_filters_dispatch_to_passthrough() {
    for filter in [Some(SinkFilter::Pending), None] {
        let dispatch = match filter.clone() {
            Some(SinkFilter::Compiled(phys)) => SinkFilterDispatch::Compiled(phys),
            Some(SinkFilter::Rejected) => SinkFilterDispatch::Rejected,
            Some(SinkFilter::Pending) | None => SinkFilterDispatch::None,
        };
        assert!(matches!(dispatch, SinkFilterDispatch::None));
    }
}

/// A clustered runtime cannot finalize event time before its first
/// globally committed frontier.
#[cfg(feature = "cluster")]
#[test]
fn cap_source_watermarks_freezes_before_first_cluster_commit() {
    let mut wms: FxHashMap<Arc<str>, i64> = FxHashMap::default();
    wms.insert(Arc::from("a"), 1_000);
    wms.insert(Arc::from("b"), 500);

    ConnectorPipelineCallback::cap_source_watermarks_by_cluster_min(&mut wms, None);

    assert_eq!(wms.get(&Arc::<str>::from("a")).copied(), Some(i64::MIN));
    assert_eq!(wms.get(&Arc::<str>::from("b")).copied(), Some(i64::MIN));
}

/// When a cluster-wide minimum is published, sources that have
/// advanced past it get pulled back to it; sources at or below
/// the cap are left alone (cap must not push watermarks forward).
#[cfg(feature = "cluster")]
#[test]
fn cap_source_watermarks_lowers_only_sources_above_cluster_min() {
    let mut wms: FxHashMap<Arc<str>, i64> = FxHashMap::default();
    wms.insert(Arc::from("ahead"), 2_000);
    wms.insert(Arc::from("at"), 1_500);
    wms.insert(Arc::from("behind"), 800);

    ConnectorPipelineCallback::cap_source_watermarks_by_cluster_min(&mut wms, Some(1_500));

    assert_eq!(
        wms.get(&Arc::<str>::from("ahead")).copied(),
        Some(1_500),
        "source above cluster min must be capped down",
    );
    assert_eq!(
        wms.get(&Arc::<str>::from("at")).copied(),
        Some(1_500),
        "source at cluster min unchanged",
    );
    assert_eq!(
        wms.get(&Arc::<str>::from("behind")).copied(),
        Some(800),
        "source below cluster min must NOT be advanced by the cap",
    );
}

#[cfg(feature = "cluster")]
#[test]
fn committed_cluster_watermark_keeps_replayed_rows_from_a_faster_local_frontier() {
    use arrow_array::TimestampMillisecondArray;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )])),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            1_400, 1_700,
        ]))],
    )
    .unwrap();
    let effective = ConnectorPipelineCallback::cap_watermark_by_cluster_min(2_000, Some(1_500));
    let retained = filter_late_rows(&batch, "ts", effective)
        .unwrap()
        .expect("the row after the committed frontier must survive replay");

    assert_eq!(effective, 1_500);
    assert_eq!(retained.num_rows(), 1);
}

#[cfg(feature = "cluster")]
fn follower_identity(epoch: u64, checkpoint_id: u64, digest: u8) -> CertifiedCheckpointAttempt {
    CertifiedCheckpointAttempt {
        attempt: CheckpointAttempt::new(epoch, checkpoint_id),
        assignment_digest: [digest; 32],
        leader_proof: leader_proof(1),
    }
}

/// Failure releases an attempt for an exact retry but permanently binds that retained epoch
/// to its first checkpoint/certificate identity.
#[cfg(feature = "cluster")]
#[test]
fn follower_admission_allows_only_exact_retry_after_failure() {
    let state = FollowerTailState::default();
    let exact = follower_identity(5, 5, 1);
    let wrong_checkpoint = follower_identity(5, 6, 1);
    let wrong_certificate = follower_identity(5, 5, 2);
    let mut wrong_authority = exact.clone();
    wrong_authority.leader_proof = leader_proof(2);

    assert_eq!(
        state.reserve(exact.clone()),
        Ok(FollowerAdmission::Reserved)
    );
    assert_eq!(state.reserve(exact.clone()), Ok(FollowerAdmission::Covered));
    assert!(state.reserve(wrong_checkpoint.clone()).is_err());
    assert!(state.reserve(wrong_certificate.clone()).is_err());
    assert!(state.reserve(wrong_authority.clone()).is_err());

    assert_eq!(state.finish(&exact, false), Ok(()));
    assert_eq!(
        state.reserve(exact.clone()),
        Ok(FollowerAdmission::Reserved)
    );
    assert!(state.reserve(wrong_checkpoint).is_err());
    assert!(state.reserve(wrong_certificate).is_err());
    assert!(state.reserve(wrong_authority).is_err());

    assert_eq!(state.finish(&exact, false), Ok(()));
    let newer = follower_identity(6, 6, 1);
    assert_eq!(
        state.reserve(newer.clone()),
        Ok(FollowerAdmission::Reserved)
    );
    assert_eq!(state.finish(&newer, false), Ok(()));
    assert_eq!(
        state.reserve(exact),
        Ok(FollowerAdmission::Covered),
        "an exact retry becomes stale after a newer epoch is observed"
    );
}

/// An atomic commit leaves no admission window in which the same attempt can be reserved a
/// second time: admission sees either the active reservation or its committed terminal.
#[cfg(feature = "cluster")]
#[test]
fn follower_commit_and_admission_are_atomic() {
    let state = Arc::new(FollowerTailState::default());
    let identity = follower_identity(8, 8, 3);
    assert_eq!(
        state.reserve(identity.clone()),
        Ok(FollowerAdmission::Reserved)
    );

    let start = Arc::new(std::sync::Barrier::new(3));
    let finishing_state = Arc::clone(&state);
    let finishing_start = Arc::clone(&start);
    let finishing_identity = identity.clone();
    let finish = std::thread::spawn(move || {
        finishing_start.wait();
        finishing_state.finish(&finishing_identity, true)
    });
    let admitting_state = Arc::clone(&state);
    let admitting_start = Arc::clone(&start);
    let admitting_identity = identity.clone();
    let admission = std::thread::spawn(move || {
        admitting_start.wait();
        admitting_state.reserve(admitting_identity)
    });
    start.wait();

    assert_eq!(finish.join().unwrap(), Ok(()));
    assert_eq!(admission.join().unwrap(), Ok(FollowerAdmission::Covered));
    assert_eq!(state.committed(), Some(identity));
    assert!(state.in_flight().is_empty());
}

/// Multiple durable tails retain independent slots; a newer admission never overwrites an
/// older tail, and either tail may finish first.
#[cfg(feature = "cluster")]
#[test]
fn follower_tail_tracks_multiple_in_flight_identities() {
    let state = FollowerTailState::default();
    let five = follower_identity(5, 5, 1);
    let seven = follower_identity(7, 7, 1);

    assert_eq!(state.reserve(five.clone()), Ok(FollowerAdmission::Reserved));
    assert_eq!(
        state.reserve(seven.clone()),
        Ok(FollowerAdmission::Reserved)
    );
    assert_eq!(state.in_flight(), vec![five.clone(), seven.clone()]);

    assert_eq!(state.finish(&seven, true), Ok(()));
    assert_eq!(state.in_flight(), vec![five.clone()]);
    assert_eq!(state.finish(&five, true), Ok(()));
    assert!(state.in_flight().is_empty());
    assert_eq!(
        state.committed(),
        Some(seven),
        "an older tail finishing late must not regress committed identity"
    );
}

#[cfg(feature = "cluster")]
#[test]
fn follower_committed_highwater_rejects_conflicting_attempt_dimensions() {
    let state = FollowerTailState::default();
    let committed = follower_identity(10, 10, 1);
    assert_eq!(
        state.reserve(committed.clone()),
        Ok(FollowerAdmission::Reserved)
    );
    assert_eq!(state.finish(&committed, true), Ok(()));

    for conflicting in [
        follower_identity(10, 11, 1),
        follower_identity(11, 10, 1),
        follower_identity(9, 11, 1),
        follower_identity(11, 9, 1),
    ] {
        let error = state.reserve(conflicting).unwrap_err();
        assert!(error.contains("conflicting checkpoint"), "{error}");
    }
    assert_eq!(state.committed(), Some(committed));
    assert!(state.in_flight().is_empty());
}

#[cfg(feature = "cluster")]
#[test]
fn follower_rejected_terminal_keeps_its_admission_fence() {
    let terminal = follower_identity(11, 11, 1);
    for committed in [follower_identity(10, 12, 1), follower_identity(11, 11, 2)] {
        let state = FollowerTailState::default();
        assert_eq!(
            state.reserve(terminal.clone()),
            Ok(FollowerAdmission::Reserved)
        );
        state.progress.lock().committed = Some(committed.clone());

        let error = state.finish(&terminal, true).unwrap_err();
        assert!(error.contains("conflicting checkpoint"), "{error}");
        assert_eq!(state.in_flight(), vec![terminal.clone()]);
        assert_eq!(state.committed(), Some(committed));
    }
}

/// A terminal carrying another certificate for the same epoch/checkpoint cannot release or
/// commit the reserved identity.
#[cfg(feature = "cluster")]
#[test]
fn follower_terminal_rejects_same_attempt_with_different_certificate() {
    let state = FollowerTailState::default();
    let exact = follower_identity(9, 9, 1);
    let wrong_certificate = follower_identity(9, 9, 2);

    assert_eq!(
        state.reserve(exact.clone()),
        Ok(FollowerAdmission::Reserved)
    );
    assert!(state.finish(&wrong_certificate, true).is_err());
    assert_eq!(state.in_flight(), vec![exact.clone()]);
    assert_eq!(state.committed(), None);
    assert_eq!(state.finish(&exact, true), Ok(()));
    assert_eq!(state.committed(), Some(exact));
}

/// Missing/outage/conflict errors are all in-doubt after prepare and must retain the exact
/// admission identity until recovery observes an immutable outcome.
#[cfg(feature = "cluster")]
#[test]
fn follower_in_doubt_completion_retains_in_flight_identity() {
    let state = FollowerTailState::default();
    let identity = follower_identity(10, 10, 4);
    assert_eq!(
        state.reserve(identity.clone()),
        Ok(FollowerAdmission::Reserved)
    );

    let missing = Err(DbError::Checkpoint(
        "follower outcome remained unresolved".into(),
    ));
    assert_eq!(state.finish_resolved(&identity, &missing), Ok(None));
    assert_eq!(state.in_flight(), vec![identity.clone()]);

    let read_error = Err(DbError::Checkpoint(
        "durable outcome store read failed".into(),
    ));
    assert_eq!(state.finish_resolved(&identity, &read_error), Ok(None));
    assert_eq!(state.in_flight(), vec![identity.clone()]);

    let abort = Ok(false);
    assert_eq!(state.finish_resolved(&identity, &abort), Ok(Some(false)));
    assert!(state.in_flight().is_empty());
}

#[cfg(feature = "cluster")]
fn resume_identity(
    epoch: u64,
    checkpoint_id: u64,
) -> (
    laminar_core::cluster::control::CheckpointAssignmentFence,
    CertifiedCheckpointAttempt,
) {
    let fence = assignment_fence(1, &[1, 7]);
    let identity = CertifiedCheckpointAttempt {
        attempt: CheckpointAttempt::new(epoch, checkpoint_id),
        assignment_digest: fence.digest(),
        leader_proof: leader_proof(1),
    };
    (fence, identity)
}

/// Build a follower-side controller whose `current_leader()` is a
/// seeded peer, for resume-gate tests. The caller holds the
/// returned membership sender alive for the test's duration.
#[cfg(feature = "cluster")]
async fn gate_controller() -> (
    Arc<laminar_core::cluster::control::InMemoryKv>,
    laminar_core::cluster::control::ClusterController,
    laminar_core::cluster::discovery::NodeId,
    tokio::sync::watch::Sender<Vec<laminar_core::cluster::discovery::NodeInfo>>,
    Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
) {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};

    let leader_id = NodeId(1);
    let follower_id = NodeId(7);
    let kv = Arc::new(InMemoryKv::new(follower_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let leader_info = NodeInfo {
        id: leader_id,
        name: "leader".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let follower_info = NodeInfo {
        id: follower_id,
        name: "follower".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (tx, rx) = tokio::sync::watch::channel(vec![leader_info, follower_info]);
    let controller = ClusterController::new_with_recovery_incarnation(
        follower_id,
        Arc::clone(&kv_trait),
        kv_trait,
        None,
        rx,
        "00000000-0000-0000-0000-000000000007".parse().unwrap(),
    );
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = Arc::new(
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(&backing)),
    );
    let lease_store = Arc::new(laminar_core::cluster::control::LeaderLeaseStore::new(
        backing, 1_000,
    ));
    lease_store
        .begin_new_term(
            &laminar_core::cluster::control::LeaderLeaseOwner {
                node: leader_id,
                boot: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                process_term: 1,
            },
            0,
        )
        .await
        .unwrap();
    controller.set_leader_lease_store(lease_store);
    (kv, controller, leader_id, tx, decision_store)
}

#[cfg(feature = "cluster")]
async fn install_callback_shuffle(
    callback: &mut ConnectorPipelineCallback,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
) -> (
    Arc<laminar_core::shuffle::ShuffleSender>,
    Arc<laminar_core::shuffle::ShuffleReceiver>,
) {
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let receiver = Arc::new(
        ShuffleReceiver::bind(
            7,
            "127.0.0.1:0".parse().unwrap(),
            "00000000-0000-0000-0000-000000000007".parse().unwrap(),
        )
        .await
        .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(
        1,
        "00000000-0000-0000-0000-000000000001".parse().unwrap(),
    ));
    let process_lease = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
    receiver
        .install_process_lease_deadline(Arc::clone(&process_lease))
        .unwrap();
    sender
        .install_process_lease_deadline(process_lease)
        .unwrap();
    receiver.install_assignment_fence(fence, &[1, 7]).unwrap();
    sender.install_assignment_fence(fence, &[1, 7]).unwrap();
    sender.register_peer(7, receiver.local_addr());
    callback
        .graph
        .set_key_group_count(laminar_core::state::KeyGroupCount::try_from(2_u16).unwrap());
    callback
        .graph
        .set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
            registry: Arc::new(VnodeRegistry::single_owner(2, NodeId(7))),
            sender: Arc::clone(&sender),
            receiver: Arc::clone(&receiver),
            self_id: NodeId(7),
        });
    (sender, receiver)
}

#[cfg(feature = "cluster")]
async fn send_callback_shuffle_barrier(
    sender: &laminar_core::shuffle::ShuffleSender,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    attempt: CheckpointAttempt,
) {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::shuffle::ShuffleMessage;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![9]))],
    )
    .unwrap();
    sender
        .send_to(7, &ShuffleMessage::checkpointed("held".into(), 1, batch))
        .await
        .unwrap();
    sender
        .fan_out_barrier(
            &[7],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            fence,
        )
        .await
        .unwrap();
}

#[cfg(feature = "cluster")]
async fn stage_callback_shuffle_barrier(
    sender: &laminar_core::shuffle::ShuffleSender,
    receiver: &laminar_core::shuffle::ShuffleReceiver,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    attempt: CheckpointAttempt,
) {
    send_callback_shuffle_barrier(sender, fence, attempt).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while !receiver.has_staged_checkpoint_barriers() {
            assert!(receiver.drain_checkpointed_data_for("absent").is_empty());
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shuffle barrier did not enter the holdover");
}

#[cfg(feature = "cluster")]
async fn await_callback_shuffle_data(
    receiver: &laminar_core::shuffle::ShuffleReceiver,
    stage: &str,
) -> Vec<laminar_core::shuffle::ReceivedBatch> {
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let batches = receiver.drain_checkpointed_data_for(stage);
            if !batches.is_empty() {
                return batches;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    result.unwrap_or_else(|_| {
        panic!(
            "shuffle data did not become drainable: assignment={}, staged_barrier={}, loss={}",
            receiver.assignment_version(),
            receiver.has_staged_checkpoint_barriers(),
            receiver
                .delivery_loss_incidents()
                .load(std::sync::atomic::Ordering::Acquire)
        )
    })
}

#[cfg(feature = "cluster")]
fn install_pending_follower_attempt(
    callback: &mut ConnectorPipelineCallback,
    announcement: laminar_core::cluster::control::BarrierAnnouncement,
) -> crate::pipeline::callback::SourceBarrierControl {
    use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};

    let identity = ConnectorPipelineCallback::certified_announcement(&announcement).unwrap();
    assert_eq!(
        callback.follower_tail.reserve(identity),
        Ok(FollowerAdmission::Reserved)
    );
    let attempt = CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id);
    let (release_tx, _release_rx) = tokio::sync::watch::channel(None);
    let control = crate::pipeline::callback::SourceBarrierControl::new(
        CheckpointBarrierInjector::new(),
        release_tx,
    );
    assert!(control.trigger(CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch)));
    callback.barrier_injectors.push(control.clone());
    callback.pending_follower_checkpoint = Some(announcement);
    control
}

#[cfg(feature = "cluster")]
async fn record_gate_abort(
    controller: &laminar_core::cluster::control::ClusterController,
    attempt: CheckpointAttempt,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
) {
    let authority = controller.checkpoint_authority().unwrap();
    let proof = authority.load().await.unwrap().unwrap().proof();
    authority
        .record_cluster_outcome(
            &proof,
            attempt.epoch,
            attempt.checkpoint_id,
            fence.clone(),
            laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_rejection_publishes_negative_ack_and_cleans_local_attempt() {
    use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
    use laminar_core::cluster::control::{
        BarrierAck, BarrierAnnouncement, ClusterKv, Phase, ACK_KEY,
    };

    let (kv, controller, _leader_id, _members_tx, _decision_store) = gate_controller().await;
    let controller = Arc::new(controller);
    let fence = assignment_fence(19, &[1, 7]);
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let attempt = CheckpointAttempt::new(30, 30);
    let announcement = BarrierAnnouncement {
        epoch: attempt.epoch,
        checkpoint_id: attempt.checkpoint_id,
        assignment_fence: Some(fence.clone()),
        leader_proof: Some(leader_proof(1)),
        phase: Phase::Prepare,
        flags: 0,
    };
    let identity = ConnectorPipelineCallback::certified_announcement(&announcement).unwrap();
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&controller));
    callback.pending_follower_checkpoint = Some(announcement);
    assert_eq!(
        callback.follower_tail.reserve(identity),
        Ok(FollowerAdmission::Reserved)
    );
    let (release_tx, _release_rx) = tokio::sync::watch::channel(None);
    let control = crate::pipeline::callback::SourceBarrierControl::new(
        CheckpointBarrierInjector::new(),
        release_tx,
    );
    assert!(control.trigger(CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch)));
    callback.barrier_injectors.push(control.clone());
    let reason = "injected follower capture failure";

    crate::pipeline::PipelineCallback::cancel_source_barrier_attempt(
        &mut callback,
        attempt,
        reason,
    )
    .await
    .unwrap();

    let encoded = kv
        .read_from(controller.instance_id(), ACK_KEY)
        .await
        .expect("follower rejection must publish a negative acknowledgement");
    let acknowledgement: BarrierAck = serde_json::from_str(&encoded).unwrap();
    assert_eq!(acknowledgement.epoch, attempt.epoch);
    assert_eq!(acknowledgement.checkpoint_id, attempt.checkpoint_id);
    assert_eq!(acknowledgement.assignment_digest, Some(fence.digest()));
    assert!(!acknowledgement.ok);
    assert_eq!(acknowledgement.error.as_deref(), Some(reason));
    assert!(callback.pending_follower_checkpoint.is_none());
    assert!(callback.follower_tail.in_flight().is_empty());
    assert!(control.can_trigger());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_follower_rejection_accepts_exact_abort_or_newer_terminal() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

    for dominated_by_successor in [false, true] {
        let (_kv, controller, _leader_id, _members_tx, _decision_store) = gate_controller().await;
        let controller = Arc::new(controller);
        let fence = assignment_fence(19, &[1, 7]);
        let attempt = CheckpointAttempt::new(30, 30);
        let mut callback = empty_callback_fixture();
        callback.cluster_controller = Some(Arc::clone(&controller));
        let (sender, receiver) = install_callback_shuffle(&mut callback, &fence).await;
        stage_callback_shuffle_barrier(&sender, &receiver, &fence, attempt).await;

        if dominated_by_successor {
            let successor = CheckpointAttempt::new(31, 31);
            let successor_fence = assignment_fence(20, &[1, 7]);
            record_gate_abort(&controller, successor, &successor_fence).await;
        } else {
            record_gate_abort(&controller, attempt, &fence).await;
        }
        let announcement = BarrierAnnouncement {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(leader_proof(1)),
            phase: Phase::Prepare,
            flags: 0,
        };
        let control = install_pending_follower_attempt(&mut callback, announcement);

        crate::pipeline::PipelineCallback::cancel_source_barrier_attempt(
            &mut callback,
            attempt,
            "shuffle scope cancelled",
        )
        .await
        .unwrap();

        assert!(callback.pending_follower_checkpoint.is_none());
        assert!(callback.follower_tail.in_flight().is_empty());
        assert!(control.can_trigger());
        assert!(callback.checkpoint_fault.lock().is_none());
        assert!(!receiver.has_staged_checkpoint_barriers());
        let held = await_callback_shuffle_data(&receiver, "held").await;
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].batch().num_rows(), 1);
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn live_leader_durably_aborts_shuffle_follower_nack_before_retirement() {
    use laminar_core::checkpoint_decision::CheckpointVerdict;
    use laminar_core::cluster::control::barrier::BARRIER_ADDR_KEY;
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, LeaseDeadline, Phase,
        ProcessLease,
    };
    use laminar_core::cluster::discovery::{NodeInfo, NodeMetadata, NodeState};

    let (follower_kv, follower, leader_id, follower_members_tx, decision_store) =
        gate_controller().await;
    let authority = follower.checkpoint_authority().unwrap();
    let leader_grant = authority.load().await.unwrap().unwrap();
    let leader_proof = leader_grant.proof();

    let follower = Arc::new(follower);
    let follower_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
    follower
        .set_process_lease_deadline(Arc::clone(&follower_deadline))
        .unwrap();
    let follower_lease = ProcessLease {
        node: follower.instance_id(),
        owner: follower.recovery_incarnation(),
        term: 1,
        seq: 1,
        expires_at_ms: i64::MAX,
    };
    follower
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &follower_lease)
        .await
        .unwrap();

    let leader_kv = Arc::new(InMemoryKv::new(leader_id));
    let leader_control: Arc<dyn ClusterKv> = leader_kv.clone();
    let follower_info = NodeInfo {
        id: follower.instance_id(),
        name: "follower".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (leader_members_tx, leader_members_rx) = tokio::sync::watch::channel(vec![follower_info]);
    let leader = Arc::new(ClusterController::new_with_recovery_incarnation(
        leader_id,
        Arc::clone(&leader_control),
        leader_control,
        None,
        leader_members_rx,
        leader_grant.owner.boot,
    ));
    leader.set_leader_lease_store(Arc::clone(&authority));
    let leader_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
    leader
        .set_process_lease_deadline(Arc::clone(&leader_deadline))
        .unwrap();
    let (leader_grant_tx, leader_grant_rx) =
        tokio::sync::watch::channel(Some(leader_grant.clone()));
    leader
        .set_leader_lease_watch(
            leader_grant_rx,
            leader_grant.owner.clone(),
            Arc::clone(&leader_deadline),
        )
        .unwrap();
    leader.install_local_leader_proof_provider();
    let leader_process_lease = ProcessLease {
        node: leader_id,
        owner: leader_grant.owner.boot,
        term: leader_grant.owner.process_term,
        seq: 1,
        expires_at_ms: i64::MAX,
    };
    leader
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &leader_process_lease)
        .await
        .unwrap();
    let follower_endpoint = follower_kv
        .read_from(follower.instance_id(), BARRIER_ADDR_KEY)
        .await
        .expect("follower control endpoint must be advertised");
    leader_kv.seed(follower.instance_id(), BARRIER_ADDR_KEY, follower_endpoint);
    let leader_endpoint = leader_kv
        .read_from(leader_id, BARRIER_ADDR_KEY)
        .await
        .expect("leader control endpoint must be advertised");
    follower_kv.seed(leader_id, BARRIER_ADDR_KEY, leader_endpoint);

    let fence = assignment_fence(19, &[leader_id.0, follower.instance_id().0]);
    leader.publish_checkpoint_assignment_fence(Some(fence.clone()));
    follower.publish_checkpoint_assignment_fence(Some(fence.clone()));
    assert_eq!(leader.capture_leader_proof().as_ref(), Some(&leader_proof));

    let attempt = CheckpointAttempt::new(30, 30);
    let announcement = BarrierAnnouncement {
        epoch: attempt.epoch,
        checkpoint_id: attempt.checkpoint_id,
        assignment_fence: Some(fence.clone()),
        leader_proof: Some(leader_proof.clone()),
        phase: Phase::Prepare,
        flags: 0,
    };
    let mut follower_callback = empty_callback_fixture();
    follower_callback.cluster_controller = Some(Arc::clone(&follower));
    let (sender, receiver) = install_callback_shuffle(&mut follower_callback, &fence).await;
    stage_callback_shuffle_barrier(&sender, &receiver, &fence, attempt).await;
    let control = install_pending_follower_attempt(&mut follower_callback, announcement.clone());

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let store = Box::new(
        laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(
            checkpoint_dir.path(),
        ),
    );
    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        store,
    )
    .await
    .unwrap();
    coordinator
        .bind_durable_decision_store(Arc::clone(&decision_store))
        .await
        .unwrap();
    coordinator.set_cluster_controller(Arc::clone(&leader));
    coordinator.set_assignment_version(fence.assignment_version);
    let coordinator = Arc::new(tokio::sync::Mutex::new(Some(coordinator)));
    let (complete_tx, _complete_rx) =
        crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(1);
    let in_flight = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut request = crate::checkpoint_coordinator::CheckpointRequest::default();
    request.assignment_fence = Some(fence.clone());
    let tail = LeaderTail {
        _in_flight: EpochInFlightGuard::claim(&in_flight),
        coordinator,
        complete_tx,
        request,
        operator_state: None,
        operator_state_encoded_budget: 0,
        mutable_operator_capture_guard: None,
        vnode_states: Default::default(),
        fan_out: FxHashMap::default(),
        local_watermark: CheckpointWatermark::Uninitialized,
        attempt,
        attempt_started: std::time::Instant::now(),
        checkpoint_timeout: Duration::from_secs(2),
        serialization_timeout: Duration::from_secs(1),
        checkpoint_cleanup_timeout: Duration::from_secs(1),
        fault_on_failure: false,
        checkpoint_fault: Arc::new(parking_lot::Mutex::new(None)),
        controller: Some(Arc::clone(&leader)),
        leader_proof: Some(leader_proof),
        quorum_timeout: Duration::from_secs(1),
        delta_rebase_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };

    leader
        .announce_prepare_barrier(&announcement, Duration::from_secs(1))
        .await
        .unwrap();
    {
        let cancellation = crate::pipeline::PipelineCallback::cancel_source_barrier_attempt(
            &mut follower_callback,
            attempt,
            "shuffle scope cancelled",
        );
        tokio::pin!(cancellation);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut cancellation)
                .await
                .is_err(),
            "a follower NACK must remain fenced without a durable terminal outcome"
        );
        assert!(authority
            .cluster_attempt_settlement(attempt)
            .await
            .unwrap()
            .is_none());
        assert!(receiver.has_staged_checkpoint_barriers());
        assert!(!control.can_trigger());

        let leader_tail = ConnectorPipelineCallback::prepare_leader_quorum(
            &tail,
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
        let (quorum, cancellation_result) = tokio::join!(leader_tail, &mut cancellation);
        assert!(
            quorum.is_none(),
            "the follower NACK must reject leader quorum"
        );
        cancellation_result.unwrap();
    }

    let outcome = authority
        .cluster_attempt_settlement(attempt)
        .await
        .unwrap()
        .expect("the live leader must durably settle the rejected attempt");
    assert_eq!(outcome.epoch, attempt.epoch);
    assert_eq!(outcome.checkpoint_id, attempt.checkpoint_id);
    assert_eq!(outcome.verdict, CheckpointVerdict::Abort);
    assert_eq!(outcome.assignment_fence.as_ref(), Some(&fence));
    assert!(follower_callback.pending_follower_checkpoint.is_none());
    assert!(follower_callback.follower_tail.in_flight().is_empty());
    assert!(control.can_trigger());
    assert!(!receiver.has_staged_checkpoint_barriers());

    drop((leader_grant_tx, leader_members_tx, follower_members_tx));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_follower_rejection_does_not_trust_abort_hint_without_outcome() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
    let controller = Arc::new(controller);
    let fence = assignment_fence(19, &[1, 7]);
    let attempt = CheckpointAttempt::new(30, 30);
    let mut callback = empty_callback_fixture();
    callback.checkpoint_cleanup_timeout = Duration::from_millis(30);
    callback.cluster_controller = Some(Arc::clone(&controller));
    let _ = install_callback_shuffle(&mut callback, &fence).await;
    let mut announcement = BarrierAnnouncement {
        epoch: attempt.epoch,
        checkpoint_id: attempt.checkpoint_id,
        assignment_fence: Some(fence),
        leader_proof: Some(leader_proof(1)),
        phase: Phase::Prepare,
        flags: 0,
    };
    let control = install_pending_follower_attempt(&mut callback, announcement.clone());
    announcement.phase = Phase::Abort;
    kv.seed(
        leader_id,
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&announcement).unwrap(),
    );

    let error = crate::pipeline::PipelineCallback::cancel_source_barrier_attempt(
        &mut callback,
        attempt,
        "shuffle scope cancelled",
    )
    .await
    .unwrap_err();

    assert!(error.contains("no verified durable settlement"), "{error}");
    assert!(callback.pending_follower_checkpoint.is_some());
    assert!(!control.can_trigger());
    assert!(callback.checkpoint_fault.lock().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_follower_rejection_faults_on_exact_durable_commit() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

    let (_kv, controller, _leader_id, _members_tx, decision_store) = gate_controller().await;
    let controller = Arc::new(controller);
    let fence = assignment_fence(19, &[1, 7]);
    let attempt = CheckpointAttempt::new(30, 30);
    record_gate_commit(&controller, &decision_store, attempt, &fence).await;
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&controller));
    let _ = install_callback_shuffle(&mut callback, &fence).await;
    let control = install_pending_follower_attempt(
        &mut callback,
        BarrierAnnouncement {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            assignment_fence: Some(fence),
            leader_proof: Some(leader_proof(1)),
            phase: Phase::Prepare,
            flags: 0,
        },
    );

    let error = crate::pipeline::PipelineCallback::cancel_source_barrier_attempt(
        &mut callback,
        attempt,
        "shuffle scope cancelled",
    )
    .await
    .unwrap_err();

    assert!(error.contains("durable Commit"), "{error}");
    assert!(callback.pending_follower_checkpoint.is_some());
    assert!(!control.can_trigger());
    assert!(callback.checkpoint_fault.lock().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn terminal_reconciliation_retires_barrier_without_local_prepare_admission() {
    use laminar_core::cluster::control::LeaseDeadline;

    let (_kv, controller, _leader_id, _members_tx, _decision_store) = gate_controller().await;
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let controller = Arc::new(controller);
    let fence = assignment_fence(19, &[1, 7]);
    let attempt = CheckpointAttempt::new(30, 30);
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&controller));
    let (sender, receiver) = install_callback_shuffle(&mut callback, &fence).await;
    send_callback_shuffle_barrier(&sender, &fence, attempt).await;
    assert!(!receiver.has_staged_checkpoint_barriers());
    record_gate_abort(&controller, attempt, &fence).await;

    let outcome = crate::pipeline::PipelineCallback::service_checkpoint_control(
        &mut callback,
        FxHashMap::default(),
    )
    .await;

    assert!(matches!(
        outcome,
        crate::pipeline::CheckpointControlOutcome::Idle
    ));
    assert!(callback.pending_follower_checkpoint.is_none());
    assert!(callback.checkpoint_fault.lock().is_none());
    assert!(!receiver.has_staged_checkpoint_barriers());
    let held = await_callback_shuffle_data(&receiver, "held").await;
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].batch().num_rows(), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn barrier_retirement_rejects_noncanonical_attempt() {
    let (_kv, _controller, _leader_id, _members_tx, _decision_store) = gate_controller().await;
    let fence = assignment_fence(19, &[1, 7]);
    let mut callback = empty_callback_fixture();
    let (_sender, receiver) = install_callback_shuffle(&mut callback, &fence).await;
    receiver
        .retire_checkpoint_barriers(CheckpointAttempt::new(40, 40), fence.digest())
        .unwrap();

    let error = callback
        .retire_shuffle_checkpoint_barriers(CheckpointAttempt::new(41, 399), Some(fence.digest()))
        .unwrap_err();

    assert!(error.contains("canonical checkpoint ID"), "{error}");
    assert!(callback.checkpoint_fault.lock().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_rejection_keeps_source_fenced_when_ack_publication_fails() {
    use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterKv, LeaseDeadline, Phase, ACK_KEY,
    };

    let (kv, controller, _leader_id, _members_tx, _decision_store) = gate_controller().await;
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::fenced()))
        .unwrap();
    let controller = Arc::new(controller);
    let fence = assignment_fence(20, &[1, 7]);
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let attempt = CheckpointAttempt::new(31, 31);
    let announcement = BarrierAnnouncement {
        epoch: attempt.epoch,
        checkpoint_id: attempt.checkpoint_id,
        assignment_fence: Some(fence),
        leader_proof: Some(leader_proof(1)),
        phase: Phase::Prepare,
        flags: 0,
    };
    let identity = ConnectorPipelineCallback::certified_announcement(&announcement).unwrap();
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&controller));
    callback.pending_follower_checkpoint = Some(announcement);
    assert_eq!(
        callback.follower_tail.reserve(identity.clone()),
        Ok(FollowerAdmission::Reserved)
    );
    let (release_tx, _release_rx) = tokio::sync::watch::channel(None);
    let control = crate::pipeline::callback::SourceBarrierControl::new(
        CheckpointBarrierInjector::new(),
        release_tx,
    );
    assert!(control.trigger(CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch)));
    callback.barrier_injectors.push(control.clone());

    let error = crate::pipeline::PipelineCallback::cancel_source_barrier_attempt(
        &mut callback,
        attempt,
        "injected rejection",
    )
    .await
    .unwrap_err();

    assert!(error.contains("negative acknowledgement"), "{error}");
    assert!(callback.pending_follower_checkpoint.is_some());
    assert_eq!(callback.follower_tail.in_flight(), vec![identity]);
    assert!(
        !control.can_trigger(),
        "an unpublished rejection cannot reopen source intake"
    );
    assert_eq!(kv.read_from(controller.instance_id(), ACK_KEY).await, None);
    assert!(callback.checkpoint_fault.lock().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_rejection_keeps_source_fenced_when_process_lease_expires_during_settlement() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterKv, LeaseDeadline, Phase, ACK_KEY,
    };

    let (kv, controller, _leader_id, _members_tx, _decision_store) = gate_controller().await;
    let process_lease = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
    controller
        .set_process_lease_deadline(Arc::clone(&process_lease))
        .unwrap();
    let controller = Arc::new(controller);
    let fence = assignment_fence(20, &[1, 7]);
    let attempt = CheckpointAttempt::new(31, 31);
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&controller));
    let (sender, receiver) = install_callback_shuffle(&mut callback, &fence).await;
    stage_callback_shuffle_barrier(&sender, &receiver, &fence, attempt).await;
    let control = install_pending_follower_attempt(
        &mut callback,
        BarrierAnnouncement {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            assignment_fence: Some(fence),
            leader_proof: Some(leader_proof(1)),
            phase: Phase::Prepare,
            flags: 0,
        },
    );

    let error = {
        let cancellation = crate::pipeline::PipelineCallback::cancel_source_barrier_attempt(
            &mut callback,
            attempt,
            "injected rejection",
        );
        tokio::pin!(cancellation);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                tokio::select! {
                    result = &mut cancellation => {
                        panic!("cancellation completed before settlement lease loss: {result:?}");
                    }
                    () = tokio::time::sleep(Duration::from_millis(5)) => {
                        if kv.read_from(controller.instance_id(), ACK_KEY).await.is_some() {
                            break;
                        }
                    }
                }
            }
        })
        .await
        .expect("follower rejection did not publish its negative acknowledgement");

        process_lease.fence();
        tokio::time::timeout(Duration::from_secs(1), &mut cancellation)
            .await
            .expect("process lease loss did not stop durable settlement")
            .unwrap_err()
    };

    assert!(error.contains("lost process authority"), "{error}");
    assert!(callback.pending_follower_checkpoint.is_some());
    assert!(!control.can_trigger());
    assert!(receiver.has_staged_checkpoint_barriers());
    assert!(callback.checkpoint_fault.lock().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn mismatched_observed_assignment_sends_exact_negative_prepare_ack() {
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::cluster::control::{
        BarrierAck, BarrierAnnouncement, ClusterKv, Phase, ACK_KEY, ANNOUNCEMENT_KEY,
    };

    let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
    let announced_fence = assignment_fence(17, &[1, 7]);
    let local_fence = CheckpointAssignmentFence::from_owner_map(
        17,
        &[7, 1],
        announced_fence.participants.clone(),
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(local_fence));
    let announcement = BarrierAnnouncement {
        epoch: 20,
        checkpoint_id: 20,
        assignment_fence: Some(announced_fence.clone()),
        leader_proof: Some(leader_proof(1)),
        phase: Phase::Prepare,
        flags: 0,
    };
    kv.seed(
        leader_id,
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&announcement).unwrap(),
    );

    let mut callback = empty_callback_fixture();
    let admission = callback.admit_follower_prepare(&controller).await;
    let FollowerPrepareAdmission::Failed { attempt, error } = admission else {
        panic!("a mismatched local assignment must reject follower admission");
    };
    assert_eq!(attempt, CheckpointAttempt::new(20, 20));
    assert!(error.contains("follower assignment differs"), "{error}");

    let encoded = kv
        .read_from(controller.instance_id(), ACK_KEY)
        .await
        .expect("assignment rejection must publish a prompt negative acknowledgement");
    let acknowledgement: BarrierAck = serde_json::from_str(&encoded).unwrap();
    assert_eq!(acknowledgement.epoch, 20);
    assert_eq!(acknowledgement.checkpoint_id, 20);
    assert_eq!(
        acknowledgement.assignment_digest,
        Some(announced_fence.digest())
    );
    assert!(!acknowledgement.ok);
    assert_eq!(acknowledgement.error.as_deref(), Some(error.as_str()));
}

#[cfg(feature = "cluster")]
async fn gate_recovery_capsule(
    decision_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    attempt: CheckpointAttempt,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
) -> laminar_core::checkpoint::RecoveryCapsuleRef {
    use laminar_core::checkpoint::{
        ClusterRecoveryCapsule, ParticipantRecoveryRef, PipelineIdentity,
        CLUSTER_RECOVERY_CAPSULE_VERSION,
    };

    let capsule = ClusterRecoveryCapsule {
        version: CLUSTER_RECOVERY_CAPSULE_VERSION,
        attempt,
        deployment_id: decision_store.load_or_create_deployment_id().await.unwrap(),
        pipeline_identity: PipelineIdentity::empty(),
        assignment_fence: fence.clone(),
        seal_inventory_sha256: digest(1),
        vnode_restore_contract: crate::cluster_recovery_capsule::vnode_restore_contract_for_test(
            fence.vnode_count,
        ),
        participants: fence
            .participant_ids()
            .into_iter()
            .map(|participant_id| ParticipantRecoveryRef {
                participant_id,
                readiness_sha256: digest(2),
                manifest_sha256: digest(3),
                portable_state_sha256: digest(4),
            })
            .collect(),
        source_offsets: Default::default(),
        source_metadata: Default::default(),
        source_assignment_versions: Default::default(),
        source_watermarks: Default::default(),
        cluster_watermark: CheckpointWatermark::Active(42),
        recovery_watermark_frontier: Some(42),
        portable_state_sha256: digest(4),
    };
    decision_store
        .create_recovery_capsule(&capsule)
        .await
        .unwrap()
}

#[cfg(feature = "cluster")]
async fn record_gate_commit(
    controller: &laminar_core::cluster::control::ClusterController,
    decision_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    attempt: CheckpointAttempt,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
) {
    let recovery_capsule = gate_recovery_capsule(decision_store, attempt, fence).await;
    let authority = controller.checkpoint_authority().unwrap();
    let proof = authority.load().await.unwrap().unwrap().proof();
    authority
        .record_cluster_outcome(
            &proof,
            attempt.epoch,
            attempt.checkpoint_id,
            fence.clone(),
            laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
            Some(recovery_capsule),
        )
        .await
        .unwrap();
}

/// The resume gate releases on the leader's `Aligned` announcement.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn aligned_resume_gate_releases_on_aligned() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
    let (fence, identity) = resume_identity(3, 3);
    let aligned = serde_json::to_string(&BarrierAnnouncement {
        epoch: 3,
        checkpoint_id: 3,
        assignment_fence: Some(fence.clone()),
        leader_proof: Some(identity.leader_proof.clone()),
        phase: Phase::Aligned,
        flags: 0,
    })
    .unwrap();
    kv.seed(leader_id, ANNOUNCEMENT_KEY, aligned);

    tokio::time::timeout(
        Duration::from_secs(2),
        ConnectorPipelineCallback::wait_for_aligned_resume(
            true,
            &controller,
            identity,
            &fence,
            std::time::Duration::from_secs(3),
        ),
    )
    .await
    .expect("gate must release on Aligned")
    .expect("Aligned certificate must validate");
    assert_eq!(
        controller.cluster_min_watermark(),
        None,
        "Aligned may still abort and cannot advance the recovery-safe watermark"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn exact_commit_resume_gate_publishes_the_recovery_safe_watermark() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (kv, controller, leader_id, _members_tx, decision_store) = gate_controller().await;
    let (fence, identity) = resume_identity(3, 3);
    record_gate_commit(
        &controller,
        decision_store.as_ref(),
        identity.attempt,
        &fence,
    )
    .await;
    kv.seed(
        leader_id,
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 3,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(identity.leader_proof.clone()),
            phase: Phase::Commit,
            flags: 0,
        })
        .unwrap(),
    );

    ConnectorPipelineCallback::wait_for_aligned_resume(
        true,
        &controller,
        identity,
        &fence,
        Duration::from_secs(1),
    )
    .await
    .expect("exact Commit must release the resume gate");
    assert_eq!(
        controller.cluster_min_watermark(),
        Some(42),
        "the immutable capsule owns the recovery-safe watermark"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn successor_abort_releases_the_resume_gate_without_publishing_a_watermark() {
    use laminar_core::checkpoint_decision::{CheckpointVerdict, RecordOutcomeResult};
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
    let (prepared_fence, identity) = resume_identity(3, 3);
    let successor_fence = assignment_fence(2, &[1, 7]);
    let authority = controller.checkpoint_authority().unwrap();
    let owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: leader_id,
        boot: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
        process_term: 1,
    };
    let laminar_core::cluster::control::LeaseOutcome::Acquired(successor) =
        authority.begin_new_term(&owner, 1).await.unwrap()
    else {
        panic!("the incumbent process must rotate its leader term");
    };
    assert!(matches!(
        authority
            .record_cluster_outcome(
                &successor.proof(),
                3,
                3,
                successor_fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap(),
        RecordOutcomeResult::Created(_)
    ));
    kv.seed(
        leader_id,
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 3,
            assignment_fence: Some(successor_fence),
            leader_proof: Some(successor.proof()),
            phase: Phase::Abort,
            flags: 0,
        })
        .unwrap(),
    );

    tokio::time::timeout(
        Duration::from_secs(2),
        ConnectorPipelineCallback::wait_for_aligned_resume(
            true,
            &controller,
            identity,
            &prepared_fence,
            Duration::from_secs(1),
        ),
    )
    .await
    .expect("successor Abort must release the old leader's resume gate")
    .expect("durable outcome validation owns successor Abort authority");
    assert_eq!(
        controller.cluster_min_watermark(),
        None,
        "Abort cannot publish a recovery-safe watermark"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn compacted_abort_still_settles_the_newer_terminal_wait() {
    use laminar_core::checkpoint_decision::CheckpointVerdict;

    let (_kv, controller, _leader_id, _members_tx, _decision_store) = gate_controller().await;
    let authority = controller.checkpoint_authority().unwrap();
    let proof = authority.load().await.unwrap().unwrap().proof();
    let fence = assignment_fence(1, &[1, 7]);
    for epoch in 1..=80 {
        authority
            .record_cluster_outcome(
                &proof,
                epoch,
                epoch,
                fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }

    let compacted = CheckpointAttempt::new(1, 1);
    assert!(authority
        .cluster_outcome(compacted.epoch)
        .await
        .unwrap()
        .is_none());
    ConnectorPipelineCallback::wait_for_newer_terminal_outcome(
        &controller,
        compacted,
        tokio::time::Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("a newer durable settlement must close a compacted Abort");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn terminal_hint_without_an_outcome_does_not_release_the_resume_gate() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    for phase in [Phase::Commit, Phase::Abort] {
        let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
        let (prepared_fence, identity) = resume_identity(3, 3);
        let terminal_fence = if phase == Phase::Commit {
            prepared_fence.clone()
        } else {
            assignment_fence(2, &[1, 7])
        };
        kv.seed(
            leader_id,
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&BarrierAnnouncement {
                epoch: 3,
                checkpoint_id: 3,
                assignment_fence: Some(terminal_fence),
                leader_proof: Some(leader_proof(if phase == Phase::Commit { 1 } else { 2 })),
                phase,
                flags: 0,
            })
            .unwrap(),
        );

        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            ConnectorPipelineCallback::wait_for_aligned_resume(
                true,
                &controller,
                identity,
                &prepared_fence,
                Duration::from_secs(1),
            ),
        )
        .await;
        assert!(
            outcome.is_err(),
            "a {phase:?} hint cannot reopen shuffle without immutable authority"
        );
        assert_eq!(controller.cluster_min_watermark(), None);
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn aligned_resume_gate_timeout_fails_closed() {
    let (_kv, controller, _leader_id, _members_tx, _decision_store) = gate_controller().await;
    let (fence, identity) = resume_identity(3, 3);
    let error = ConnectorPipelineCallback::wait_for_aligned_resume(
        true,
        &controller,
        identity,
        &fence,
        Duration::from_millis(20),
    )
    .await
    .expect_err("missing Aligned must not reopen the pipeline");
    assert!(error.contains("pipeline remains fenced"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn aligned_resume_gate_rejects_wrong_attempt_or_certificate() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    for wrong in [
        BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 4,
            assignment_fence: Some(resume_identity(3, 3).0),
            leader_proof: None,
            phase: Phase::Abort,
            flags: 0,
        },
        BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 3,
            assignment_fence: Some(assignment_fence(2, &[1, 7])),
            leader_proof: Some(leader_proof(1)),
            phase: Phase::Aligned,
            flags: 0,
        },
        BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 3,
            assignment_fence: Some(resume_identity(3, 3).0),
            leader_proof: Some(leader_proof(2)),
            phase: Phase::Aligned,
            flags: 0,
        },
    ] {
        let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
        let (expected_fence, identity) = resume_identity(3, 3);
        kv.seed(
            leader_id,
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&wrong).unwrap(),
        );

        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            ConnectorPipelineCallback::wait_for_aligned_resume(
                true,
                &controller,
                identity,
                &expected_fence,
                std::time::Duration::from_secs(3),
            ),
        )
        .await;
        assert!(
            !matches!(outcome, Ok(Ok(()))),
            "wrong exact identity must not release the shuffle resume gate"
        );
        assert_eq!(
            controller.cluster_min_watermark(),
            None,
            "rejected identity must not advance the cluster watermark"
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn resume_gate_does_not_observe_commit_from_another_assignment() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (kv, controller, leader_id, _members_tx, decision_store) = gate_controller().await;
    let (prepared_fence, identity) = resume_identity(3, 3);
    record_gate_commit(
        &controller,
        decision_store.as_ref(),
        identity.attempt,
        &prepared_fence,
    )
    .await;
    kv.seed(
        leader_id,
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 3,
            assignment_fence: Some(assignment_fence(2, &[1, 7])),
            leader_proof: Some(leader_proof(2)),
            phase: Phase::Commit,
            flags: 0,
        })
        .unwrap(),
    );

    let outcome = tokio::time::timeout(
        Duration::from_millis(100),
        ConnectorPipelineCallback::wait_for_aligned_resume(
            true,
            &controller,
            identity,
            &prepared_fence,
            Duration::from_secs(3),
        ),
    )
    .await;
    assert!(
        outcome.is_err(),
        "a Commit from another assignment must remain unobserved"
    );
    assert_eq!(controller.cluster_min_watermark(), None);
}

/// A newer epoch's announcement supersedes the awaited one
/// (latest-wins observation can overwrite Aligned/Commit).
#[cfg(feature = "cluster")]
#[tokio::test]
async fn aligned_resume_gate_releases_on_newer_epoch() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
    let (fence, identity) = resume_identity(3, 3);
    let successor_fence = assignment_fence(2, &[1, 7]);
    controller.publish_checkpoint_assignment_fence(Some(successor_fence.clone()));
    let newer = serde_json::to_string(&BarrierAnnouncement {
        epoch: 4,
        checkpoint_id: 4,
        assignment_fence: Some(successor_fence),
        leader_proof: Some(leader_proof(1)),
        phase: Phase::Aligned,
        flags: 0,
    })
    .unwrap();
    kv.seed(leader_id, ANNOUNCEMENT_KEY, newer);

    tokio::time::timeout(
        Duration::from_secs(2),
        ConnectorPipelineCallback::wait_for_aligned_resume(
            true,
            &controller,
            identity,
            &fence,
            std::time::Duration::from_secs(3),
        ),
    )
    .await
    .expect("gate must release when a newer epoch is announced")
    .expect("newer-epoch release must preserve the awaited certificate");
    assert_eq!(
        controller.cluster_min_watermark(),
        None,
        "a newer epoch may release the gate but cannot publish its watermark here"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn newer_reversible_phase_requires_successor_alignment_authority() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    for announcement in [
        BarrierAnnouncement {
            epoch: 4,
            checkpoint_id: 4,
            assignment_fence: Some(assignment_fence(2, &[1, 7])),
            leader_proof: Some(leader_proof(1)),
            phase: Phase::Prepare,
            flags: 0,
        },
        BarrierAnnouncement {
            epoch: 4,
            checkpoint_id: 4,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Aligned,
            flags: 0,
        },
        BarrierAnnouncement {
            epoch: 4,
            checkpoint_id: 4,
            assignment_fence: Some(assignment_fence(2, &[2, 7])),
            leader_proof: Some(leader_proof(1)),
            phase: Phase::Aligned,
            flags: 0,
        },
        BarrierAnnouncement {
            epoch: 4,
            checkpoint_id: 4,
            assignment_fence: Some(assignment_fence(2, &[1, 7])),
            leader_proof: None,
            phase: Phase::Aligned,
            flags: 0,
        },
    ] {
        let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
        let (fence, identity) = resume_identity(3, 3);
        kv.seed(
            leader_id,
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&announcement).unwrap(),
        );
        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            ConnectorPipelineCallback::wait_for_aligned_resume(
                true,
                &controller,
                identity,
                &fence,
                Duration::from_secs(1),
            ),
        )
        .await;
        assert!(
            !matches!(outcome, Ok(Ok(()))),
            "newer {:?} without successor alignment authority released the gate",
            announcement.phase
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn newer_aligned_requires_the_local_successor_assignment() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    for (successor_fence, publish, expected) in [
        (
            assignment_fence(2, &[1, 7]),
            false,
            "no locally certified assignment",
        ),
        (assignment_fence(2, &[1, 2]), true, "excludes this process"),
    ] {
        let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
        let (fence, identity) = resume_identity(3, 3);
        if publish {
            controller.publish_checkpoint_assignment_fence(Some(successor_fence.clone()));
        }
        kv.seed(
            leader_id,
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&BarrierAnnouncement {
                epoch: 4,
                checkpoint_id: 4,
                assignment_fence: Some(successor_fence),
                leader_proof: Some(leader_proof(1)),
                phase: Phase::Aligned,
                flags: 0,
            })
            .unwrap(),
        );

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            ConnectorPipelineCallback::wait_for_aligned_resume(
                true,
                &controller,
                identity,
                &fence,
                Duration::from_secs(1),
            ),
        )
        .await
        .expect("the successor announcement must be observed")
        .expect_err("an uncertified successor assignment must keep the old pipeline fenced");
        assert!(error.contains(expected), "{error}");
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn newer_terminal_hint_requires_an_immutable_outcome() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    for phase in [Phase::Commit, Phase::Abort] {
        let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
        let (fence, identity) = resume_identity(3, 3);
        kv.seed(
            leader_id,
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&BarrierAnnouncement {
                epoch: 4,
                checkpoint_id: 4,
                assignment_fence: Some(assignment_fence(2, &[1, 7])),
                leader_proof: Some(leader_proof(1)),
                phase,
                flags: 0,
            })
            .unwrap(),
        );
        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            ConnectorPipelineCallback::wait_for_aligned_resume(
                true,
                &controller,
                identity,
                &fence,
                Duration::from_secs(1),
            ),
        )
        .await;
        assert!(
            outcome.is_err(),
            "newer {phase:?} hint released the gate without immutable authority"
        );
        assert_eq!(controller.cluster_min_watermark(), None);
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn newer_durable_abort_releases_the_resume_gate() {
    use laminar_core::checkpoint_decision::CheckpointVerdict;
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
    let (fence, identity) = resume_identity(3, 3);
    let successor_fence = assignment_fence(2, &[1, 7]);
    let authority = controller.checkpoint_authority().unwrap();
    let proof = authority.load().await.unwrap().unwrap().proof();
    authority
        .record_cluster_outcome(
            &proof,
            4,
            4,
            successor_fence.clone(),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    kv.seed(
        leader_id,
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&BarrierAnnouncement {
            epoch: 4,
            checkpoint_id: 4,
            assignment_fence: Some(successor_fence),
            leader_proof: Some(proof),
            phase: Phase::Abort,
            flags: 0,
        })
        .unwrap(),
    );

    tokio::time::timeout(
        Duration::from_secs(2),
        ConnectorPipelineCallback::wait_for_aligned_resume(
            true,
            &controller,
            identity,
            &fence,
            Duration::from_secs(1),
        ),
    )
    .await
    .expect("durable newer Abort must release the old gate")
    .unwrap();
    assert_eq!(controller.cluster_min_watermark(), None);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn newer_durable_commit_does_not_publish_an_unapplied_watermark() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (kv, controller, leader_id, _members_tx, decision_store) = gate_controller().await;
    let (fence, identity) = resume_identity(3, 3);
    let successor = CheckpointAttempt::new(4, 4);
    let successor_fence = assignment_fence(2, &[1, 2]);
    record_gate_commit(&controller, &decision_store, successor, &successor_fence).await;
    let proof = controller
        .checkpoint_authority()
        .unwrap()
        .load()
        .await
        .unwrap()
        .unwrap()
        .proof();
    kv.seed(
        leader_id,
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&BarrierAnnouncement {
            epoch: successor.epoch,
            checkpoint_id: successor.checkpoint_id,
            assignment_fence: Some(successor_fence),
            leader_proof: Some(proof),
            phase: Phase::Commit,
            flags: 0,
        })
        .unwrap(),
    );

    tokio::time::timeout(
        Duration::from_secs(2),
        ConnectorPipelineCallback::wait_for_aligned_resume(
            true,
            &controller,
            identity,
            &fence,
            Duration::from_secs(1),
        ),
    )
    .await
    .expect("a durable newer Commit must supersede the old gate")
    .unwrap();
    assert_eq!(
        controller.cluster_min_watermark(),
        None,
        "an old gate cannot install state from a newer cut that this process did not apply"
    );
}

/// Without a cross-node shuffle there is no in-flight-row invariant
/// to protect — the gate is a no-op even with no announcement.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn aligned_resume_gate_skips_without_shuffle() {
    let (_kv, controller, _leader_id, _members_tx, _decision_store) = gate_controller().await;
    let (fence, identity) = resume_identity(3, 3);
    tokio::time::timeout(
        Duration::from_millis(100),
        ConnectorPipelineCallback::wait_for_aligned_resume(
            false,
            &controller,
            identity,
            &fence,
            std::time::Duration::from_secs(3),
        ),
    )
    .await
    .expect("gate must be a no-op without a cluster shuffle")
    .expect("no-shuffle gate must accept a valid certificate");
}

/// Retention is bounded and fails closed when every retained identity is active. Once one tail
/// finishes, its inactive slot can be evicted for a newer epoch.
#[cfg(feature = "cluster")]
#[test]
fn follower_identity_history_is_bounded() {
    let state = FollowerTailState::default();
    for epoch in 1..=MAX_RETAINED_FOLLOWER_IDENTITIES as u64 {
        assert_eq!(
            state.reserve(follower_identity(epoch, epoch, 1)),
            Ok(FollowerAdmission::Reserved)
        );
    }
    assert!(
        state
            .reserve(follower_identity(
                MAX_RETAINED_FOLLOWER_IDENTITIES as u64 + 1,
                MAX_RETAINED_FOLLOWER_IDENTITIES as u64 + 1,
                1,
            ))
            .is_err(),
        "all-active capacity must fail closed"
    );

    let oldest = follower_identity(1, 1, 1);
    assert_eq!(state.finish(&oldest, false), Ok(()));
    let newest = follower_identity(
        MAX_RETAINED_FOLLOWER_IDENTITIES as u64 + 1,
        MAX_RETAINED_FOLLOWER_IDENTITIES as u64 + 1,
        1,
    );
    assert_eq!(state.reserve(newest), Ok(FollowerAdmission::Reserved));
    assert_eq!(state.in_flight().len(), MAX_RETAINED_FOLLOWER_IDENTITIES);
}
