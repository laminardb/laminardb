use super::*;

#[cfg(feature = "cluster")]
fn memory_checkpoint_store() -> Box<dyn laminar_core::checkpoint::CheckpointStore> {
    Box::new(laminar_core::checkpoint::ObjectStoreCheckpointStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "",
    ))
}

struct DrainingCaptureFailureOperator {
    live_rows: Arc<std::sync::atomic::AtomicUsize>,
    fail_whole_capture: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(feature = "cluster")]
    fail_vnode_capture: Arc<std::sync::atomic::AtomicBool>,
}

struct TerminalCaptureFailureOperator;

#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for TerminalCaptureFailureOperator {
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
        Err(DbError::ShuffleTerminal(
            "injected permanent checkpoint routing failure".into(),
        ))
    }
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
    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        _vnode_count: u32,
        _max_capture_bytes: u64,
    ) -> Result<Option<Vec<crate::operator_graph::CapturedVnodeState>>, DbError> {
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
                .map(|vnode| crate::operator_graph::CapturedVnodeState {
                    vnode: *vnode,
                    state: Some(crate::operator_graph::StateFrameCapture::encoded_static(
                        b"test-vnode-state",
                    )),
                })
                .collect(),
        ))
    }
}

struct DrainingCaptureOperator {
    live_rows: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(feature = "cluster")]
struct UnchangedVnodeCaptureOperator;

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for UnchangedVnodeCaptureOperator {
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
        Ok(None)
    }

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        _vnode_count: u32,
        _max_capture_bytes: u64,
    ) -> Result<Option<Vec<crate::operator_graph::CapturedVnodeState>>, DbError> {
        Ok(Some(
            required_vnodes
                .iter()
                .map(|vnode| crate::operator_graph::CapturedVnodeState {
                    vnode: *vnode,
                    state: None,
                })
                .collect(),
        ))
    }
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

struct ExternallyRunnableDrainOperator {
    pending: Arc<std::sync::atomic::AtomicBool>,
    runnable: Arc<std::sync::atomic::AtomicBool>,
    process_calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for ExternallyRunnableDrainOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.process_calls
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if self.runnable.load(std::sync::atomic::Ordering::Acquire) {
            self.pending
                .store(false, std::sync::atomic::Ordering::Release);
        }
        Ok(Vec::new())
    }

    fn checkpoint_drain_pending(&self) -> bool {
        self.pending.load(std::sync::atomic::Ordering::Acquire)
    }

    fn checkpoint(&mut self) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn deferred_work_is_runnable(&self) -> bool {
        self.runnable.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[cfg(feature = "cluster")]
struct CompletionPolledDrainOperator {
    pending: Arc<std::sync::atomic::AtomicBool>,
    completion_ready: Arc<std::sync::atomic::AtomicBool>,
    process_calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for CompletionPolledDrainOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.process_calls
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if self
            .completion_ready
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.pending
                .store(false, std::sync::atomic::Ordering::Release);
        }
        Ok(Vec::new())
    }

    fn checkpoint_drain_pending(&self) -> bool {
        self.pending.load(std::sync::atomic::Ordering::Acquire)
    }

    fn checkpoint(&mut self) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn deferred_work_is_runnable(&self) -> bool {
        // Matches pending shuffle sends: their task completion is a wake that must be polled by
        // another operator pass, but the retained send itself is not continuously runnable.
        false
    }
}

#[cfg(feature = "cluster")]
struct SnapshotableAlignedReplayDrainOperator {
    checkpoint_drain_pending: Arc<std::sync::atomic::AtomicBool>,
    aligned_replay_pending: Arc<std::sync::atomic::AtomicBool>,
    process_calls: Arc<std::sync::atomic::AtomicUsize>,
    clear_aligned_on_process: bool,
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl crate::operator_graph::GraphOperator for SnapshotableAlignedReplayDrainOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        assert!(inputs.iter().all(Vec::is_empty));
        self.process_calls
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.checkpoint_drain_pending
            .store(false, std::sync::atomic::Ordering::Release);
        if self.clear_aligned_on_process {
            self.aligned_replay_pending
                .store(false, std::sync::atomic::Ordering::Release);
        }
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn checkpoint_aligned_replay_pending(&self) -> bool {
        self.aligned_replay_pending
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn checkpoint_drain_pending(&self) -> bool {
        self.checkpoint_drain_pending
            .load(std::sync::atomic::Ordering::Acquire)
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

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        _vnode_count: u32,
        _max_capture_bytes: u64,
    ) -> Result<Option<Vec<crate::operator_graph::CapturedVnodeState>>, DbError> {
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
                .map(|vnode| crate::operator_graph::CapturedVnodeState {
                    vnode: *vnode,
                    state: Some(crate::operator_graph::StateFrameCapture::encoded_static(
                        b"fence-audit-state",
                    )),
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
        checkpoint_source_names: Vec::new(),
        source_frontiers_buf: FxHashMap::default(),
        #[cfg(feature = "cluster")]
        committed_source_watermarks_snapshot: Arc::new(FxHashMap::default()),
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
        pipeline_halt: None,
        last_checkpoint_admission_failure: None,
        checkpoint_admission_recovering: false,
        shutdown_signal: Arc::new(tokio::sync::Notify::new()),
        #[cfg(feature = "cluster")]
        cluster_controller: None,
        #[cfg(feature = "cluster")]
        assignment_adoption_lock: Arc::new(tokio::sync::Mutex::new(())),
        #[cfg(feature = "cluster")]
        shuffle_delivery_loss_incidents: None,
        #[cfg(feature = "cluster")]
        shuffle_recovered_delivery_loss_incidents: None,
        #[cfg(feature = "cluster")]
        shuffle_delivery_loss_incidents_seen: 0,
        #[cfg(feature = "cluster")]
        vnode_registry: None,
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
        checkpoint_tail_runtime: tokio::runtime::Handle::current(),
        checkpoint_tail_tasks: tokio::task::JoinSet::new(),
        checkpoint_in_flight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        full_vnode_capture_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        epoch_allocator: None,
        #[cfg(feature = "cluster")]
        quorum_timeout: Duration::from_secs(1),
        checkpoint_committable_sinks: false,
        intake_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_initial_sink_fence_is_reserved_for_checkpoint_committable_sinks() {
    let mut callback = empty_callback_fixture();
    assert!(
        !callback.initial_checkpoint_sink_fence_required(),
        "a cluster ALO pipeline already has the mandatory post-fixed-point FIFO fence"
    );

    callback.checkpoint_committable_sinks = true;
    assert!(
        callback.initial_checkpoint_sink_fence_required(),
        "EO or mixed sinks must retain both checkpoint fences"
    );
}

#[cfg(not(feature = "cluster"))]
#[tokio::test]
async fn noncluster_checkpoint_retains_its_only_sink_fence() {
    let callback = empty_callback_fixture();
    assert!(callback.initial_checkpoint_sink_fence_required());
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

#[tokio::test]
async fn checkpoint_admission_failure_reporting_is_edge_deduplicated() {
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
    callback.checkpoint_source_names = vec!["gen".into()];
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
        tokio::time::Instant::now() + Duration::from_secs(5),
        laminar_core::checkpoint::flags::NONE,
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
    assignment_store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
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

    async fn begin_drain(
        &self,
    ) -> (
        laminar_core::cluster::control::AssignmentSnapshot,
        laminar_core::checkpoint::AssignmentDrainTransition,
    ) {
        let predecessor = self.assignment_store.load().await.unwrap().unwrap();
        let draining = predecessor
            .next_draining(
                std::collections::BTreeMap::from([(
                    0,
                    laminar_core::cluster::discovery::NodeId(1),
                )]),
                predecessor.participants.clone(),
                self.proof.clone(),
            )
            .unwrap();
        assert!(matches!(
            self.assignment_store
                .save_if_version(&draining, predecessor.version)
                .await
                .unwrap(),
            laminar_core::cluster::control::RotateOutcome::Rotated
        ));
        let transition = draining.drain_transition.clone().unwrap();
        self.controller
            .publish_checkpoint_drain_transition(Some(transition.clone()));
        (draining, transition)
    }
}

#[cfg(feature = "cluster")]
async fn authoritative_local_leader(
    control_kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
) -> AuthoritativeLocalLeader {
    use laminar_core::checkpoint::CheckpointParticipant;
    use laminar_core::cluster::control::{
        ClusterController, LeaderLeaseOwner, LeaderLeaseStore, LeaseDeadline, LeaseOutcome,
        ProcessLeaseAuthority, ProcessLeaseOutcome,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo};

    let node = NodeId(1);
    let boot = "00000000-0000-0000-0000-000000000001".parse().unwrap();
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let assignment_store = Arc::new(
        laminar_core::cluster::control::AssignmentSnapshotStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )),
    );
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&control_kv),
        control_kv,
        Some(Arc::clone(&assignment_store)),
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
    controller.set_leader_lease_store(Arc::clone(&authority));
    controller.install_local_leader_proof_provider();
    controller
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &process_lease)
        .await
        .unwrap();

    let assignment = laminar_core::cluster::control::AssignmentSnapshot::empty()
        .next_for_participants(
            std::collections::BTreeMap::from([(0, node)]),
            vec![CheckpointParticipant {
                node_id: node.0,
                boot_incarnation: boot,
            }],
        )
        .unwrap();
    assignment_store.save_if_absent(&assignment).await.unwrap();
    let fence = assignment.assignment_fence().unwrap();
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let proof = controller.capture_leader_proof().unwrap();
    AuthoritativeLocalLeader {
        controller,
        assignment_store,
        authority,
        fence,
        proof,
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn handoff_admission_waits_for_exact_quorum_and_unfinalized_head() {
    use crate::pipeline::CheckpointAssignmentAdmission;
    use laminar_core::cluster::control::{
        AssignmentDrainDecision, ClusterKv, InMemoryKv, RecordAssignmentDrainDecisionResult,
    };
    use laminar_core::cluster::discovery::NodeId;
    use laminar_core::state::VnodeRegistry;

    let kv = Arc::new(InMemoryKv::new(NodeId(1)));
    let control_kv: Arc<dyn ClusterKv> = kv;
    let leader = authoritative_local_leader(control_kv).await;
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    registry.set_assignment_and_version(vec![NodeId(1)].into(), 1);
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&leader.controller));
    callback.vnode_registry = Some(registry);
    let assignment_lock = Arc::clone(&callback.assignment_adoption_lock);

    let (_draining, transition) = leader.begin_drain().await;
    match crate::pipeline::PipelineCallback::checkpoint_assignment_for_admission(
        &mut callback,
        tokio::time::Instant::now() + Duration::from_secs(5),
    )
    .await
    {
        CheckpointAssignmentAdmission::Deferred(reason) => {
            assert!(reason.contains("not HANDOFF-ready"), "{reason}");
        }
        _ => panic!("an active pre-quorum drain must defer before attempt reservation"),
    }
    assert!(
        Arc::clone(&assignment_lock).try_lock_owned().is_ok(),
        "a deferred admission must release assignment serialization"
    );

    leader
        .controller
        .announce_drain_ack(&transition)
        .await
        .unwrap();
    let (assignment_fence, flags) =
        match crate::pipeline::PipelineCallback::checkpoint_assignment_for_admission(
            &mut callback,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        {
            CheckpointAssignmentAdmission::Ready {
                assignment_fence,
                flags,
                assignment_guard,
            } => {
                assert!(assignment_guard.is_some());
                assert!(
                    Arc::clone(&assignment_lock).try_lock_owned().is_err(),
                    "a ready admission must retain assignment serialization"
                );
                drop(assignment_guard);
                assert!(Arc::clone(&assignment_lock).try_lock_owned().is_ok());
                (assignment_fence, flags)
            }
            _ => panic!("the exact one-process receipt quorum and drain head must admit HANDOFF"),
        };
    assert_eq!(assignment_fence, Some(leader.fence.clone()));
    assert_eq!(flags, laminar_core::checkpoint::flags::HANDOFF);

    assert!(matches!(
        leader
            .authority
            .record_assignment_drain_decision(
                &leader.proof,
                AssignmentDrainDecision::abort(&transition, leader.proof.clone()).unwrap(),
            )
            .await
            .unwrap(),
        RecordAssignmentDrainDecisionResult::Created(_)
    ));
    let attempt = CheckpointAttempt::new(41, 41);
    let error = crate::pipeline::PipelineCallback::publish_checkpoint_prepare(
        &mut callback,
        attempt,
        std::time::Instant::now(),
        tokio::time::Instant::now() + Duration::from_secs(5),
        flags,
        assignment_fence,
    )
    .await
    .expect_err("a terminalized drain must fail the Prepare-time HANDOFF re-audit");
    assert!(error.contains("not HANDOFF-ready"), "{error}");
    assert!(!callback.checkpoint_leader_proofs.contains_key(&attempt));
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

    let (watermark, participants, handoff_replay_pending) =
        CheckpointCoordinator::run_prepare_quorum(
            &leader.controller,
            Duration::from_secs(1),
            PrepareQuorum::new(
                attempt,
                CheckpointWatermark::Active(100),
                &leader.fence,
                &leader.proof,
                laminar_core::checkpoint::flags::NONE,
            ),
        )
        .await
        .unwrap();
    assert_eq!(watermark, CheckpointWatermark::Active(100));
    assert!(participants.is_empty());
    assert!(!handoff_replay_pending);

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
        tokio::time::Instant::now() + Duration::from_secs(5),
        laminar_core::checkpoint::flags::NONE,
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
#[tokio::test]
async fn checkpoint_tail_settlement_waits_for_terminal_task() {
    let mut callback = empty_callback_fixture();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    callback.spawn_checkpoint_tail(async move {
        started_tx.send(()).unwrap();
        release_rx.await.unwrap();
    });
    started_rx.await.unwrap();

    {
        let settlement =
            crate::pipeline::PipelineCallback::settle_checkpoint_tail_tasks(&mut callback);
        tokio::pin!(settlement);
        tokio::select! {
            biased;
            result = &mut settlement => {
                panic!("unfinished checkpoint tail detached during settlement: {result:?}");
            }
            () = tokio::task::yield_now() => {}
        }
        release_tx.send(()).unwrap();
        settlement.await.unwrap();
    }

    assert!(callback.checkpoint_tail_tasks.is_empty());
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
#[tokio::test]
async fn authoritative_follower_abort_cleanup_rejects_identity_mismatch() {
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
#[tokio::test]
async fn authoritative_follower_abort_cleanup_keeps_command_when_reservation_is_missing() {
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
    let outcome = tokio::time::timeout(Duration::from_secs(1), &mut checkpoint)
        .await
        .expect("immediate follower did not return after spawning its durable tail");
    assert_eq!(
        checkpoint_in_flight.load(std::sync::atomic::Ordering::Acquire),
        1,
        "the spawned follower tail must retain exact-attempt ownership"
    );
    let assignment_writer = tokio::time::timeout(
        Duration::from_secs(1),
        Arc::clone(&rotation_fence).write_owned(),
    )
    .await
    .expect("immediate follower retained the rotation token after capture");
    assert!(whole_capture_observed.load(std::sync::atomic::Ordering::Acquire));
    assert!(vnode_capture_observed.load(std::sync::atomic::Ordering::Acquire));
    assert!(matches!(
        outcome,
        crate::pipeline::CheckpointControlOutcome::Started {
            attempt: observed,
            captured: true,
            flags: laminar_core::checkpoint::flags::NONE,
        } if observed == attempt
    ));
    drop(coordinator_guard);
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
    let mut fixture = cluster_callback_fixture(registry, Arc::clone(&controller), None);
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

    let source_checkpoints =
        FxHashMap::from_iter([("orders".to_string(), SourceCheckpoint::new())]);
    let mut checkpoint = Box::pin(crate::pipeline::PipelineCallback::checkpoint_with_barrier(
        &mut fixture.callback,
        source_checkpoints,
        attempt,
        std::time::Instant::now(),
        tokio::time::Instant::now() + Duration::from_secs(5),
        laminar_core::checkpoint::flags::NONE,
        None,
    ));
    let outcome = tokio::time::timeout(Duration::from_secs(1), &mut checkpoint)
        .await
        .expect("deferred follower did not return after spawning its durable tail");
    assert_eq!(
        checkpoint_in_flight.load(std::sync::atomic::Ordering::Acquire),
        1,
        "the spawned deferred tail must retain exact-attempt ownership"
    );
    let assignment_writer = tokio::time::timeout(
        Duration::from_secs(1),
        Arc::clone(&rotation_fence).write_owned(),
    )
    .await
    .expect("deferred follower retained the rotation token after capture");
    assert!(whole_capture_observed.load(std::sync::atomic::Ordering::Acquire));
    assert!(vnode_capture_observed.load(std::sync::atomic::Ordering::Acquire));
    assert!(matches!(outcome, crate::pipeline::BarrierOutcome::Async));
    drop(coordinator_guard);
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
        tokio::time::Instant::now() + Duration::from_secs(5),
        laminar_core::checkpoint::flags::NONE,
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
    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        memory_checkpoint_store(),
    )
    .unwrap();
    coordinator
        .bind_durable_decision_store(restarted)
        .await
        .unwrap();

    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);
    callback.epoch_allocator = Some(coordinator.epoch_allocator());
    let reserved = callback
        .reserve_attempt(tokio::time::Instant::now())
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
async fn checkpoint_graph_drain_does_not_pollute_normal_cycle_metrics() {
    let mut callback = empty_callback_fixture();
    callback.graph.set_metrics(Arc::clone(&callback.prom));
    let pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let runnable = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let process_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    callback.graph.push_test_node(
        "immediately-runnable-checkpoint-drain",
        Box::new(ExternallyRunnableDrainOperator {
            pending,
            runnable,
            process_calls: Arc::clone(&process_calls),
        }),
    );

    assert_eq!(callback.prom.cycles.get(), 0);
    assert_eq!(callback.prom.cycle_duration.get_sample_count(), 0);
    callback
        .drain_checkpoint_edges_until_inner(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 1);
    assert_eq!(callback.prom.cycles.get(), 0);
    assert_eq!(callback.prom.cycle_duration.get_sample_count(), 0);
    assert_eq!(callback.prom.cycle_execute_duration.get_sample_count(), 0);
    assert_eq!(
        callback.prom.cycle_output_store_duration.get_sample_count(),
        0
    );
    assert_eq!(
        callback.prom.cycle_sink_enqueue_duration.get_sample_count(),
        0
    );
    assert_eq!(
        callback
            .prom
            .operator_process_duration
            .with_label_values(&["immediately-runnable-checkpoint-drain", "checkpoint_drain"])
            .get_sample_count(),
        1
    );
    assert_eq!(
        callback
            .prom
            .operator_process_duration
            .with_label_values(&["immediately-runnable-checkpoint-drain", "normal"])
            .get_sample_count(),
        0
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn handoff_graph_drain_consumes_snapshotable_aligned_replay_and_reports_activity() {
    let mut callback = empty_callback_fixture();
    let checkpoint_drain_pending = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let aligned_replay_pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let process_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    callback.graph.push_test_node(
        "handoff-aligned-replay",
        Box::new(SnapshotableAlignedReplayDrainOperator {
            checkpoint_drain_pending,
            aligned_replay_pending: Arc::clone(&aligned_replay_pending),
            process_calls: Arc::clone(&process_calls),
            clear_aligned_on_process: true,
        }),
    );

    let active = callback
        .drain_handoff_edges_until_inner(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect("handoff drain must execute retained aligned replay");

    assert!(active);
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 1);
    assert!(!aligned_replay_pending.load(std::sync::atomic::Ordering::Acquire));
    assert!(callback.graph.handoff_is_quiescent());
    assert!(!callback
        .drain_handoff_edges_until_inner(tokio::time::Instant::now() + Duration::from_secs(1),)
        .await
        .unwrap());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn handoff_graph_drain_deadline_bounds_rotation_fence_wait() {
    let mut callback = empty_callback_fixture();
    let checkpoint_drain_pending = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let aligned_replay_pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    callback.graph.push_test_node(
        "handoff-drain-held-rotation-fence",
        Box::new(SnapshotableAlignedReplayDrainOperator {
            checkpoint_drain_pending,
            aligned_replay_pending,
            process_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            clear_aligned_on_process: true,
        }),
    );
    let rotation_fence = Arc::new(tokio::sync::RwLock::new(()));
    callback
        .graph
        .set_rotation_execution_fence(Arc::clone(&rotation_fence));
    let held_writer = rotation_fence.write_owned().await;

    let error = callback
        .drain_handoff_edges_until_inner(tokio::time::Instant::now() + Duration::from_millis(20))
        .await
        .expect_err("the absolute attempt deadline must bound a blocked graph pass");

    assert!(
        error.to_string().contains("absolute attempt deadline"),
        "{error}"
    );
    assert!(callback
        .checkpoint_fault
        .lock()
        .as_deref()
        .is_some_and(|reason| reason.contains("absolute attempt deadline")));
    drop(held_writer);
}

#[cfg(feature = "cluster")]
#[test]
fn shuffle_flush_terminal_after_wave_zero_staging_requires_recovery() {
    let pristine = crate::operator_graph::ShuffleFlushWaveOutcome {
        outcome: crate::operator_graph::ShuffleAlignmentOutcome::Aborted,
        peer_activity: false,
        graph_state_staged: false,
    };
    assert!(!ConnectorPipelineCallback::shuffle_flush_attempt_advanced(
        0, false, pristine
    ));

    let staged = crate::operator_graph::ShuffleFlushWaveOutcome {
        graph_state_staged: true,
        ..pristine
    };
    assert!(ConnectorPipelineCallback::shuffle_flush_attempt_advanced(
        0, false, staged
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_graph_drain_accepts_snapshotable_aligned_replay() {
    let mut callback = empty_callback_fixture();
    let checkpoint_drain_pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let aligned_replay_pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let process_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    callback.graph.push_test_node(
        "snapshotable-aligned-replay",
        Box::new(SnapshotableAlignedReplayDrainOperator {
            checkpoint_drain_pending: Arc::clone(&checkpoint_drain_pending),
            aligned_replay_pending: Arc::clone(&aligned_replay_pending),
            process_calls: Arc::clone(&process_calls),
            clear_aligned_on_process: false,
        }),
    );

    assert!(!callback.graph.checkpoint_is_quiescent());
    callback
        .drain_checkpoint_edges_until_inner(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect("snapshotable aligned replay must not fault an ordinary checkpoint drain");

    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 1);
    assert!(!checkpoint_drain_pending.load(std::sync::atomic::Ordering::Acquire));
    assert!(aligned_replay_pending.load(std::sync::atomic::Ordering::Acquire));
    assert!(callback.graph.checkpoint_is_quiescent());
    assert!(
        !callback.graph.handoff_is_quiescent(),
        "retained aligned replay remains a handoff blocker"
    );
    assert!(callback.checkpoint_fault.lock().is_none());

    aligned_replay_pending.store(false, std::sync::atomic::Ordering::Release);
    assert!(callback.graph.handoff_is_quiescent());
}

#[tokio::test(start_paused = true)]
async fn checkpoint_graph_drain_waits_for_externally_blocked_work() {
    let mut callback = empty_callback_fixture();
    let pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let runnable = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let process_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    callback.graph.push_test_node(
        "externally-runnable-drain",
        Box::new(ExternallyRunnableDrainOperator {
            pending,
            runnable: Arc::clone(&runnable),
            process_calls: Arc::clone(&process_calls),
        }),
    );

    let drain = tokio::spawn(async move {
        callback
            .drain_checkpoint_edges_until_inner(
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
    });
    for _ in 0..16 {
        if process_calls.load(std::sync::atomic::Ordering::Acquire) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 1);
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 1);

    tokio::time::advance(
        crate::pipeline::streaming_coordinator::IDLE_TIMEOUT - Duration::from_millis(1),
    )
    .await;
    tokio::task::yield_now().await;
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 1);

    runnable.store(true, std::sync::atomic::Ordering::Release);
    tokio::time::advance(Duration::from_millis(1)).await;
    drain.await.unwrap().unwrap();
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 2);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_graph_drain_repolls_nonrunnable_completion_after_shuffle_wake() {
    let mut callback = empty_callback_fixture();
    let fence = assignment_fence(1, &[1, 7]);
    let (_sender, receiver) = install_callback_shuffle(&mut callback, &fence).await;
    let work_ready = receiver.work_ready_notify();
    let pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let completion_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let process_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    callback.graph.push_test_node(
        "completion-polled-checkpoint-drain",
        Box::new(CompletionPolledDrainOperator {
            pending,
            completion_ready: Arc::clone(&completion_ready),
            process_calls: Arc::clone(&process_calls),
        }),
    );

    let drain = tokio::spawn(async move {
        callback
            .drain_checkpoint_edges_until_inner(
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await
    });
    for _ in 0..64 {
        if process_calls.load(std::sync::atomic::Ordering::Acquire) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 1);

    completion_ready.store(true, std::sync::atomic::Ordering::Release);
    work_ready.notify_one();
    tokio::time::timeout(Duration::from_secs(1), drain)
        .await
        .expect("shuffle completion wake was consumed without repolling the graph")
        .unwrap()
        .unwrap();
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 2);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn checkpoint_graph_drain_repolls_nonrunnable_completion_after_idle_fallback() {
    let mut callback = empty_callback_fixture();
    let pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let completion_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let process_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    callback.graph.push_test_node(
        "fallback-polled-checkpoint-drain",
        Box::new(CompletionPolledDrainOperator {
            pending,
            completion_ready: Arc::clone(&completion_ready),
            process_calls: Arc::clone(&process_calls),
        }),
    );
    let fence = assignment_fence(1, &[1, 7]);
    let (_sender, _receiver) = install_callback_shuffle(&mut callback, &fence).await;

    let drain = tokio::spawn(async move {
        callback
            .drain_checkpoint_edges_until_inner(
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await
    });
    for _ in 0..64 {
        if process_calls.load(std::sync::atomic::Ordering::Acquire) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 1);

    completion_ready.store(true, std::sync::atomic::Ordering::Release);
    tokio::time::advance(crate::pipeline::streaming_coordinator::IDLE_TIMEOUT).await;
    drain.await.unwrap().unwrap();
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Acquire), 2);
}

struct WriteCountingSink {
    writes: Arc<std::sync::atomic::AtomicUsize>,
    schema: arrow_schema::SchemaRef,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for WriteCountingSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(laminar_connectors::connector::WriteResult::new(
            batch.num_rows(),
            batch.get_array_memory_size() as u64,
        ))
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }
}

struct BatchRecordingSink {
    batches: Arc<parking_lot::Mutex<Vec<RecordBatch>>>,
    schema: arrow_schema::SchemaRef,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for BatchRecordingSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        self.batches.lock().push(batch.clone());
        Ok(laminar_connectors::connector::WriteResult::new(
            batch.num_rows(),
            batch.get_array_memory_size() as u64,
        ))
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }
}

fn spawn_batch_recording_sink(
    name: &str,
    contract: SinkContract,
    schema: arrow_schema::SchemaRef,
    event_tx: laminar_core::streaming::Producer<crate::sink_task::SinkEvent>,
) -> (
    crate::sink_task::SinkTaskHandle,
    Arc<parking_lot::Mutex<Vec<RecordBatch>>>,
) {
    let batches = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: name.into(),
        sink_id: Arc::from(name),
        connector: Box::new(BatchRecordingSink {
            batches: Arc::clone(&batches),
            schema,
        }),
        contract,
        requires_recovery_on_error: contract.is_checkpoint_committable(),
        channel_capacity: 128,
        flush_interval: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    (handle, batches)
}

fn recorded_i64_values(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .expect("recorded test values must be Int64")
                .values()
                .iter()
                .copied()
        })
        .collect()
}

#[tokio::test]
async fn sink_publication_preserves_preflighted_batch_boundaries() {
    use laminar_connectors::connector::{SinkConsistency, SinkInputMode, SinkTopology};

    const BATCH_COUNT: usize = 40;
    let mut callback = empty_callback_fixture();
    let (event_tx, event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    callback.sink_event_rx = event_rx;
    let append_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::AppendOnly,
    );
    let boundary_sensitive_append_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::AppendOnly,
    );
    let upsert_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::KeyedUpsert,
    );
    let changelog_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::FullChangelog,
    );
    let committable_contract = SinkContract::new(
        SinkConsistency::CheckpointCommittable,
        SinkTopology::MultiWriter,
        SinkInputMode::AppendOnly,
    );
    let plain_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let weighted_schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            arrow_schema::DataType::Int64,
            false,
        ),
    ]));

    let (append_a, append_a_batches) = spawn_batch_recording_sink(
        "append-a",
        append_contract,
        Arc::clone(&plain_schema),
        event_tx.clone(),
    );
    let (append_b, append_b_batches) = spawn_batch_recording_sink(
        "append-b",
        append_contract,
        Arc::clone(&plain_schema),
        event_tx.clone(),
    );
    let (upsert, upsert_batches) = spawn_batch_recording_sink(
        "upsert",
        upsert_contract,
        Arc::clone(&plain_schema),
        event_tx.clone(),
    );
    let (boundary_sensitive, boundary_sensitive_batches) = spawn_batch_recording_sink(
        "boundary-sensitive",
        boundary_sensitive_append_contract,
        Arc::clone(&plain_schema),
        event_tx.clone(),
    );
    let (changelog, changelog_batches) = spawn_batch_recording_sink(
        "changelog",
        changelog_contract,
        Arc::clone(&weighted_schema),
        event_tx.clone(),
    );
    let (committable, committable_batches) = spawn_batch_recording_sink(
        "committable",
        committable_contract,
        Arc::clone(&plain_schema),
        event_tx,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    committable.begin_epoch_until(7, deadline).await.unwrap();
    let admission = committable.begun_epoch_admission(7).unwrap();
    committable.publish_open_epoch(admission).unwrap();

    callback.sinks.extend([
        (
            "append-a".into(),
            append_a.clone(),
            None,
            "plain".into(),
            append_contract,
            false,
        ),
        (
            "append-b".into(),
            append_b.clone(),
            None,
            "plain".into(),
            append_contract,
            false,
        ),
        (
            "boundary-sensitive".into(),
            boundary_sensitive.clone(),
            None,
            "plain".into(),
            boundary_sensitive_append_contract,
            false,
        ),
        (
            "upsert".into(),
            upsert.clone(),
            None,
            "plain".into(),
            upsert_contract,
            false,
        ),
        (
            "changelog".into(),
            changelog.clone(),
            None,
            "weighted".into(),
            changelog_contract,
            true,
        ),
        (
            "committable".into(),
            committable.clone(),
            None,
            "plain".into(),
            committable_contract,
            false,
        ),
    ]);
    let batch_values = 0..i64::try_from(BATCH_COUNT).expect("batch count fits in i64");
    let plain = batch_values
        .clone()
        .map(|value| {
            RecordBatch::try_new(
                Arc::clone(&plain_schema),
                vec![Arc::new(arrow_array::Int64Array::from(vec![value]))],
            )
            .unwrap()
        })
        .collect();
    let weighted = batch_values
        .clone()
        .map(|value| {
            RecordBatch::try_new(
                Arc::clone(&weighted_schema),
                vec![
                    Arc::new(arrow_array::Int64Array::from(vec![value])),
                    Arc::new(arrow_array::Int64Array::from(vec![1])),
                ],
            )
            .unwrap()
        })
        .collect();
    let mut results = FxHashMap::default();
    results.insert(Arc::from("plain"), plain);
    results.insert(Arc::from("weighted"), weighted);

    crate::pipeline::PipelineCallback::write_to_sinks(&mut callback, &results, None)
        .await
        .unwrap();
    for handle in [
        &append_a,
        &append_b,
        &boundary_sensitive,
        &upsert,
        &changelog,
        &committable,
    ] {
        handle.sync().await.unwrap();
    }

    let append_a_written = append_a_batches.lock().clone();
    let append_b_written = append_b_batches.lock().clone();
    assert_eq!(append_a_written.len(), BATCH_COUNT);
    assert_eq!(append_b_written.len(), BATCH_COUNT);
    assert_eq!(
        recorded_i64_values(&append_a_written),
        batch_values.collect::<Vec<_>>()
    );
    assert_eq!(
        recorded_i64_values(&append_b_written),
        recorded_i64_values(&append_a_written)
    );
    assert_eq!(boundary_sensitive_batches.lock().len(), BATCH_COUNT);
    assert_eq!(upsert_batches.lock().len(), BATCH_COUNT);
    assert_eq!(changelog_batches.lock().len(), BATCH_COUNT);
    assert_eq!(committable_batches.lock().len(), BATCH_COUNT);

    for handle in [
        append_a,
        append_b,
        boundary_sensitive,
        upsert,
        changelog,
        committable,
    ] {
        handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn weighted_input_faults_best_effort_before_any_sink_write() {
    use laminar_connectors::connector::{
        DeliveryGuarantee, SinkConsistency, SinkInputMode, SinkTopology,
    };

    let mut callback = empty_callback_fixture();
    callback.delivery_guarantee = DeliveryGuarantee::BestEffort;
    let (event_tx, event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    callback.sink_event_rx = event_rx;
    let append_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::AppendOnly,
    );
    let changelog_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::FullChangelog,
    );
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            arrow_schema::DataType::Int64,
            false,
        ),
    ]));
    let changelog_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let changelog_handle =
        crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: "changelog".into(),
            sink_id: Arc::from("changelog"),
            connector: Box::new(WriteCountingSink {
                writes: Arc::clone(&changelog_writes),
                schema: Arc::clone(&schema),
            }),
            contract: changelog_contract,
            requires_recovery_on_error: false,
            channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_secs(5),
            write_timeout: Duration::from_secs(1),
            event_tx: event_tx.clone(),
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
    let append_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let append_handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "append".into(),
        sink_id: Arc::from("append"),
        connector: Box::new(WriteCountingSink {
            writes: Arc::clone(&append_writes),
            schema: Arc::clone(&schema),
        }),
        contract: append_contract,
        requires_recovery_on_error: false,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: Duration::from_secs(5),
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    callback.sinks.push((
        "changelog".into(),
        changelog_handle.clone(),
        None,
        "input".into(),
        changelog_contract,
        true,
    ));
    callback.sinks.push((
        "append".into(),
        append_handle.clone(),
        None,
        "input".into(),
        append_contract,
        false,
    ));
    let weighted = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![7])),
            Arc::new(arrow_array::Int64Array::from(vec![-1])),
        ],
    )
    .unwrap();
    let mut results = FxHashMap::default();
    results.insert(Arc::from("input"), vec![weighted]);

    let error = crate::pipeline::PipelineCallback::write_to_sinks(&mut callback, &results, None)
        .await
        .expect_err("weighted input must fault an append-only sink in best-effort mode");

    assert!(matches!(
        error,
        crate::pipeline::CycleError::Recovery(ref reason)
            if reason.contains("FullChangelog")
    ));
    tokio::task::yield_now().await;
    assert_eq!(
        changelog_writes.load(std::sync::atomic::Ordering::Acquire),
        0,
        "all-sink preflight must reject before a valid sibling sink writes"
    );
    assert_eq!(append_writes.load(std::sync::atomic::Ordering::Acquire), 0);
    changelog_handle.close().await.unwrap();
    append_handle.close().await.unwrap();
}

#[tokio::test]
async fn admitted_changelog_missing_weight_faults_before_any_sibling_write() {
    use laminar_connectors::connector::{
        DeliveryGuarantee, SinkConsistency, SinkInputMode, SinkTopology,
    };

    let mut callback = empty_callback_fixture();
    callback.delivery_guarantee = DeliveryGuarantee::BestEffort;
    let (event_tx, event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    callback.sink_event_rx = event_rx;
    let append_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::AppendOnly,
    );
    let full_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::FullChangelog,
    );
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let sibling_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sibling_handle =
        crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: "plain-sibling".into(),
            sink_id: Arc::from("plain-sibling"),
            connector: Box::new(WriteCountingSink {
                writes: Arc::clone(&sibling_writes),
                schema: Arc::clone(&schema),
            }),
            contract: append_contract,
            requires_recovery_on_error: false,
            channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_secs(5),
            write_timeout: Duration::from_secs(1),
            event_tx: event_tx.clone(),
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
    let changelog_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let changelog_handle =
        crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: "missing-weight".into(),
            sink_id: Arc::from("missing-weight"),
            connector: Box::new(WriteCountingSink {
                writes: Arc::clone(&changelog_writes),
                schema: Arc::clone(&schema),
            }),
            contract: full_contract,
            requires_recovery_on_error: false,
            channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_secs(5),
            write_timeout: Duration::from_secs(1),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
    callback.sinks.push((
        "plain-sibling".into(),
        sibling_handle.clone(),
        None,
        "plain".into(),
        append_contract,
        false,
    ));
    callback.sinks.push((
        "missing-weight".into(),
        changelog_handle.clone(),
        None,
        "changes".into(),
        full_contract,
        true,
    ));
    let plain = RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::Int64Array::from(vec![7]))],
    )
    .unwrap();
    let mut results = FxHashMap::default();
    results.insert(Arc::from("plain"), vec![plain.clone()]);
    results.insert(Arc::from("changes"), vec![plain]);

    let error = crate::pipeline::PipelineCallback::write_to_sinks(&mut callback, &results, None)
        .await
        .expect_err("an admitted changelog must not silently publish a plain batch");
    assert!(matches!(
        error,
        crate::pipeline::CycleError::Recovery(ref reason) if reason.contains("missing")
    ));
    tokio::task::yield_now().await;
    assert_eq!(sibling_writes.load(std::sync::atomic::Ordering::Acquire), 0);
    assert_eq!(
        changelog_writes.load(std::sync::atomic::Ordering::Acquire),
        0
    );
    sibling_handle.close().await.unwrap();
    changelog_handle.close().await.unwrap();
}

#[tokio::test]
async fn weighted_filter_preflight_is_atomic_across_batches_and_sinks() {
    use laminar_connectors::connector::{
        DeliveryGuarantee, SinkConsistency, SinkInputMode, SinkTopology,
    };

    let mut callback = empty_callback_fixture();
    callback.delivery_guarantee = DeliveryGuarantee::BestEffort;
    let (event_tx, event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    callback.sink_event_rx = event_rx;
    let full_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::FullChangelog,
    );
    let append_contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::AppendOnly,
    );
    let plain_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let int_schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            arrow_schema::DataType::Int64,
            false,
        ),
    ]));
    let text_schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("value", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            arrow_schema::DataType::Int64,
            false,
        ),
    ]));
    let filtered_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let filtered_handle =
        crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: "filtered".into(),
            sink_id: Arc::from("filtered"),
            connector: Box::new(WriteCountingSink {
                writes: Arc::clone(&filtered_writes),
                schema: Arc::clone(&int_schema),
            }),
            contract: full_contract,
            requires_recovery_on_error: false,
            channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_secs(5),
            write_timeout: Duration::from_secs(1),
            event_tx: event_tx.clone(),
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
    let sibling_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sibling_handle =
        crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: "sibling".into(),
            sink_id: Arc::from("sibling"),
            connector: Box::new(WriteCountingSink {
                writes: Arc::clone(&sibling_writes),
                schema: Arc::clone(&plain_schema),
            }),
            contract: append_contract,
            requires_recovery_on_error: false,
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
        filtered_handle.clone(),
        Some("value > 0".into()),
        "changes".into(),
        full_contract,
        true,
    ));
    callback.sinks.push((
        "sibling".into(),
        sibling_handle.clone(),
        None,
        "plain".into(),
        append_contract,
        false,
    ));
    callback.pending_sink_filter_compiles = 1;
    let first = RecordBatch::try_new(
        int_schema,
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![7])),
            Arc::new(arrow_array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        text_schema,
        vec![
            Arc::new(arrow_array::StringArray::from(vec!["bad-type"])),
            Arc::new(arrow_array::Int64Array::from(vec![-1])),
        ],
    )
    .unwrap();
    let mut results = FxHashMap::default();
    results.insert(Arc::from("changes"), vec![first, second]);
    results.insert(
        Arc::from("plain"),
        (0..40)
            .map(|value| {
                RecordBatch::try_new(
                    Arc::clone(&plain_schema),
                    vec![Arc::new(arrow_array::Int64Array::from(vec![value]))],
                )
                .unwrap()
            })
            .collect(),
    );

    let error = crate::pipeline::PipelineCallback::write_to_sinks(&mut callback, &results, None)
        .await
        .expect_err("a later filter-evaluation error must abort the whole publication");
    assert!(
        error.to_string().contains("filter application failed"),
        "{error}"
    );
    tokio::task::yield_now().await;
    assert_eq!(
        filtered_writes.load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert_eq!(sibling_writes.load(std::sync::atomic::Ordering::Acquire), 0);
    filtered_handle.close().await.unwrap();
    sibling_handle.close().await.unwrap();
}

#[tokio::test]
async fn weighted_sink_filters_reject_volatility_and_weight_references_before_write() {
    use laminar_connectors::connector::{
        DeliveryGuarantee, SinkConsistency, SinkInputMode, SinkTopology,
    };

    let contract = SinkContract::new(
        SinkConsistency::Ephemeral,
        SinkTopology::MultiWriter,
        SinkInputMode::FullChangelog,
    );
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            arrow_schema::DataType::Int64,
            false,
        ),
    ]));
    let weighted = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![7])),
            Arc::new(arrow_array::Int64Array::from(vec![-1])),
        ],
    )
    .unwrap();

    for (filter, expected) in [
        ("random() > 0.5", "not replay-immutable"),
        ("__WEIGHT > 0", "must not reference"),
    ] {
        let mut callback = empty_callback_fixture();
        callback.delivery_guarantee = DeliveryGuarantee::BestEffort;
        let (event_tx, event_rx) = laminar_core::streaming::channel::channel::<
            crate::sink_task::SinkEvent,
        >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
        callback.sink_event_rx = event_rx;
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: "filtered-changelog".into(),
            sink_id: Arc::from("filtered-changelog"),
            connector: Box::new(WriteCountingSink {
                writes: Arc::clone(&writes),
                schema: Arc::clone(&schema),
            }),
            contract,
            requires_recovery_on_error: false,
            channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_secs(5),
            write_timeout: Duration::from_secs(1),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
        callback.sinks.push((
            "filtered-changelog".into(),
            handle.clone(),
            Some(filter.into()),
            "input".into(),
            contract,
            true,
        ));
        callback.pending_sink_filter_compiles = 1;
        let mut results = FxHashMap::default();
        results.insert(Arc::from("input"), vec![weighted.clone()]);

        for (attempt, expected) in [("initial", expected), ("cached", "recovery-required")] {
            let error =
                crate::pipeline::PipelineCallback::write_to_sinks(&mut callback, &results, None)
                    .await
                    .expect_err(
                        "unsafe weighted filter must fault even under best-effort delivery",
                    );

            assert!(
                error.to_string().contains(expected),
                "{filter} {attempt}: {error}"
            );
            assert!(matches!(
                callback.compiled_sink_filters.as_slice(),
                [SinkFilter::Rejected]
            ));
            tokio::task::yield_now().await;
            assert_eq!(writes.load(std::sync::atomic::Ordering::Acquire), 0);
        }
        handle.close().await.unwrap();
    }
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
        false,
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

    let checkpoint_objects: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = Arc::new(
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(
            &checkpoint_objects,
        )),
    );
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    let authority = Arc::new(LeaderLeaseStore::new(checkpoint_objects, 1_000));
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
    follower.set_leader_lease_store(Arc::clone(&authority));

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
    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        memory_checkpoint_store(),
    )
    .unwrap();
    coordinator
        .bind_durable_decision_store(Arc::clone(&decision_store))
        .await
        .unwrap();
    coordinator
        .bind_pipeline_identity(laminar_core::checkpoint::PipelineIdentity::empty())
        .unwrap();
    coordinator.set_cluster_controller(Arc::clone(&leader));
    callback.coordinator = Arc::new(tokio::sync::Mutex::new(Some(coordinator)));

    let expired = CheckpointAttempt::new(1, 1);
    let error = crate::pipeline::PipelineCallback::publish_checkpoint_prepare(
        &mut callback,
        expired,
        std::time::Instant::now() - Duration::from_secs(2),
        tokio::time::Instant::now(),
        laminar_core::checkpoint::flags::NONE,
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

    let blocked_attempt = CheckpointAttempt::new(1, 1);
    let blocked_inventory = laminar_core::checkpoint_decision::CheckpointArtifactInventory {
        deployment_id,
        pipeline_identity: laminar_core::checkpoint::PipelineIdentity::empty(),
        attempt: blocked_attempt,
        assignment_fence: Some(fence.clone()),
    };
    let blocking_proof = leader.capture_leader_proof().unwrap();
    authority
        .begin_cluster_checkpoint_artifacts(&blocking_proof, blocked_inventory)
        .await
        .unwrap();
    let rejected = CheckpointAttempt::new(2, 2);
    let error = crate::pipeline::PipelineCallback::publish_checkpoint_prepare(
        &mut callback,
        rejected,
        std::time::Instant::now(),
        tokio::time::Instant::now() + Duration::from_secs(5),
        laminar_core::checkpoint::flags::NONE,
        Some(fence.clone()),
    )
    .await
    .expect_err("conflicting artifact inventory must prevent Prepare publication");
    assert!(error.contains("artifact admission"), "{error}");
    assert!(leader_kv
        .read_from(leader_id, ANNOUNCEMENT_KEY)
        .await
        .is_none());
    authority
        .record_cluster_outcome(
            &blocking_proof,
            blocked_attempt.epoch,
            blocked_attempt.checkpoint_id,
            fence.clone(),
            laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    {
        let mut coordinator = callback.coordinator.lock().await;
        assert!(coordinator
            .as_mut()
            .unwrap()
            .settle_cluster_checkpoint_artifacts_until(
                &blocking_proof,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap());
    }

    let attempt = CheckpointAttempt::new(3, 3);
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
        tokio::time::Instant::now() + Duration::from_secs(5),
        laminar_core::checkpoint::flags::NONE,
        Some(fence),
    )
    .await
    .unwrap();
    assert_eq!(
        authority
            .cluster_checkpoint_artifacts()
            .await
            .unwrap()
            .map(|inventory| inventory.attempt),
        Some(attempt)
    );
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
            laminar_core::checkpoint::flags::NONE,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    assert!(
        request.state_frames.is_empty(),
        "aligned capture must defer serialization to the durable tail"
    );
    request.state_frames = operator_state
        .serialize_until(
            callback.checkpoint_state_cap_bytes,
            callback.serialization_timeout,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap()
        .accept_for_test();

    assert_eq!(request.assignment_fence.as_ref(), Some(&assignment_fence));
    let frame = request
        .state_frames
        .iter()
        .find(|frame| {
            frame.key
                == laminar_core::checkpoint::StateFrameKey::OperatorWhole {
                    operator_id: "graph:follower-checkpoint-evidence".into(),
                }
        })
        .expect("the operator frame must be present in the follower request");
    assert_eq!(
        frame.state.as_deref(),
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
            laminar_core::checkpoint::flags::NONE,
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
fn cluster_callback_fixture(
    registry: Arc<laminar_core::state::VnodeRegistry>,
    controller: Arc<laminar_core::cluster::control::ClusterController>,
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
        SourceWatermarkState::new(
            laminar_core::time::EventTimeExtractor::from_column("ts"),
            Box::new(generator),
            "ts".into(),
        ),
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
            checkpoint_source_names: vec!["orders".into()],
            source_frontiers_buf: FxHashMap::default(),
            committed_source_watermarks_snapshot: Arc::new(FxHashMap::default()),
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
            pipeline_halt: None,
            last_checkpoint_admission_failure: None,
            checkpoint_admission_recovering: false,
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            cluster_controller: Some(controller),
            assignment_adoption_lock: Arc::new(tokio::sync::Mutex::new(())),
            shuffle_delivery_loss_incidents: None,
            shuffle_recovered_delivery_loss_incidents: None,
            shuffle_delivery_loss_incidents_seen: 0,
            vnode_registry: Some(registry),
            follower_tail: Arc::default(),
            barrier_injectors: Vec::new(),
            pending_follower_checkpoint: None,
            checkpoint_leader_proofs: FxHashMap::default(),
            subscription_registry: Arc::new(crate::subscription::SubscriptionRegistry::new()),
            named_stream_names: rustc_hash::FxHashSet::default(),
            checkpoint_complete_tx,
            checkpoint_tail_runtime: tokio::runtime::Handle::current(),
            checkpoint_tail_tasks: tokio::task::JoinSet::new(),
            checkpoint_in_flight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            full_vnode_capture_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            epoch_allocator: None,
            quorum_timeout: Duration::from_secs(1),
            checkpoint_committable_sinks: false,
            intake_gate: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        },
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
async fn terminal_operator_capture_populates_halt_without_recovery_fault() {
    let mut callback = empty_callback_fixture();
    callback
        .graph
        .push_test_node("terminal-capture", Box::new(TerminalCaptureFailureOperator));

    let error = callback
        .capture_operator_state_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .err()
        .expect("terminal operator capture must reject the checkpoint");

    assert!(
        error.contains("injected permanent checkpoint routing failure"),
        "{error}"
    );
    assert_eq!(
        crate::pipeline::PipelineCallback::take_pipeline_halt(&mut callback).as_deref(),
        Some(error.as_str())
    );
    assert!(
        crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback).is_none(),
        "a deterministic capture error must not request checkpoint recovery"
    );
    tokio::time::timeout(
        Duration::from_millis(50),
        callback.shutdown_signal.notified(),
    )
    .await
    .expect("terminal capture must wake the coordinator");
}

#[tokio::test]
async fn failure_after_destructive_operator_capture_faults_runtime() {
    let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
    let mut callback = empty_callback_fixture();
    callback.checkpoint_state_cap_bytes =
        256 + 128 + u64::try_from("draining-capture".len()).unwrap();
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
    assert!(error.contains("capture headroom"), "{error}");
    let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
        .expect("a post-capture failure must become a runtime fault");
    assert_eq!(fault, error);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn unchanged_vnode_capture_does_not_arm_the_mutable_capture_guard() {
    let mut callback = empty_callback_fixture();
    callback.graph.set_test_vnode_count(2);
    callback
        .graph
        .push_test_node("unchanged-vnodes", Box::new(UnchangedVnodeCaptureOperator));

    let capture = callback
        .capture_operator_state_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .unwrap();
    drop(capture);

    assert!(crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback).is_none());
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

#[cfg(feature = "cluster")]
#[tokio::test]
async fn clustered_capture_without_rotation_fence_cannot_claim_portability() {
    let mut callback = empty_callback_fixture();
    let fence = assignment_fence(1, &[1, 7]);

    let error = callback
        .checkpoint_capture_rotation_guard_until(
            Some(&fence),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect_err("a clustered capture must own a graph assignment-rotation fence");

    assert!(
        error.contains("no graph assignment-rotation fence"),
        "{error}"
    );
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
    let serialized = capture
        .serialize_until(
            callback.checkpoint_state_cap_bytes,
            callback.serialization_timeout,
            deadline,
        )
        .await
        .unwrap();

    assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 0);
    drop(serialized);
    let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
        .expect("an uncommitted destructive image must require recovery");
    assert!(fault.contains("recovery from the last committed checkpoint is required"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn destructive_vnode_capture_failure_faults_runtime() {
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

    let error = callback
        .capture_operator_state_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .err()
        .expect("vnode capture failure must reject the checkpoint");

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

#[tokio::test]
async fn unclassified_checkpoint_tail_error_faults_only_durable_delivery() {
    for durable_delivery in [false, true] {
        let (complete_tx, _complete_rx) =
            crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(1);
        let in_flight = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let checkpoint_fault = Arc::new(parking_lot::Mutex::new(None));
        let full_vnode_capture_needed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut tail = LeaderTail {
            in_flight: EpochInFlightGuard::claim(
                &in_flight,
                &checkpoint_fault,
                CheckpointAttempt::canonical(7),
                std::iter::empty(),
            )
            .unwrap(),
            coordinator: Arc::new(tokio::sync::Mutex::new(None)),
            complete_tx,
            request: crate::checkpoint_coordinator::CheckpointRequest::default(),
            operator_state: None,
            operator_state_staged_cap_bytes: 0,
            mutable_operator_capture_guard: None,
            fan_out: FxHashMap::default(),
            local_watermark: CheckpointWatermark::Uninitialized,
            handoff_replay_pending: false,
            attempt: CheckpointAttempt::canonical(7),
            attempt_started: std::time::Instant::now(),
            attempt_deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            checkpoint_timeout: Duration::from_secs(1),
            serialization_timeout: Duration::from_secs(1),
            checkpoint_cleanup_timeout: Duration::from_secs(1),
            fault_on_retryable_failure: false,
            fault_on_unclassified_error: durable_delivery,
            checkpoint_fault: Arc::clone(&checkpoint_fault),
            #[cfg(feature = "cluster")]
            controller: None,
            #[cfg(feature = "cluster")]
            leader_proof: None,
            full_vnode_capture_needed: Arc::clone(&full_vnode_capture_needed),
        };

        ConnectorPipelineCallback::handle_leader_result(
            &mut tail,
            Err(DbError::Checkpoint(
                "injected unclassified tail error".into(),
            )),
        )
        .await;

        assert_eq!(checkpoint_fault.lock().is_some(), durable_delivery);
        assert!(full_vnode_capture_needed.load(std::sync::atomic::Ordering::SeqCst));
    }
}

#[test]
fn durable_checkpoint_requires_the_exact_source_roster() {
    let expected = vec!["orders".to_string(), "payments".to_string()];
    let mut checkpoints = FxHashMap::default();
    checkpoints.insert("orders".to_string(), SourceCheckpoint::new());
    checkpoints.insert("payments".to_string(), SourceCheckpoint::new());
    assert!(validate_durable_source_checkpoint_roster(&expected, &checkpoints).is_ok());

    checkpoints.remove("payments");
    let missing = validate_durable_source_checkpoint_roster(&expected, &checkpoints).unwrap_err();
    assert!(missing.contains("payments"));

    checkpoints.insert("payments".to_string(), SourceCheckpoint::new());
    checkpoints.insert("unknown".to_string(), SourceCheckpoint::new());
    let unexpected =
        validate_durable_source_checkpoint_roster(&expected, &checkpoints).unwrap_err();
    assert!(unexpected.contains("unknown"));
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
async fn terminal_record_error_halts_and_notifies_shutdown() {
    use crate::pipeline::CycleError;
    let notify = Arc::new(tokio::sync::Notify::new());
    let error = DbError::PipelineTerminal("timestamp is outside the supported range".into());

    let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);

    assert!(matches!(
        &mapped,
        CycleError::Halt(message) if message.contains("timestamp is outside the supported range")
    ));
    tokio::time::timeout(Duration::from_millis(50), notify.notified())
        .await
        .expect("terminal record error must notify shutdown");
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

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_alignment_retains_terminal_disposition_out_of_band() {
    let mut callback = empty_callback_fixture();
    let error = DbError::ShuffleTerminal("invalid routing structure".into());
    let expected = error.to_string();

    callback.record_checkpoint_alignment_error(&error);

    assert_eq!(
        crate::pipeline::PipelineCallback::take_pipeline_halt(&mut callback).as_deref(),
        Some(expected.as_str())
    );
    assert!(callback.checkpoint_fault.lock().is_none());
    tokio::time::timeout(
        Duration::from_millis(50),
        callback.shutdown_signal.notified(),
    )
    .await
    .expect("checkpoint alignment terminal error must wake the coordinator");
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
        laminar_core::checkpoint::flags::NONE,
        None,
        None,
        deadline,
        crate::checkpoint_coordinator::SinkEpochPublication::Immediate,
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

#[tokio::test]
async fn reserved_attempt_failure_is_reported_only_after_cleanup_resolves() {
    let attempt = CheckpointAttempt::canonical(17);
    let coordinator = Arc::new(tokio::sync::Mutex::new(None));
    let coordinator_lock = coordinator.lock().await;
    let (complete_tx, complete_rx) =
        crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(1);
    let in_flight = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let checkpoint_fault = Arc::new(parking_lot::Mutex::new(None));
    let full_vnode_capture_needed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tail = LeaderTail {
        in_flight: EpochInFlightGuard::claim(
            &in_flight,
            &checkpoint_fault,
            attempt,
            std::iter::empty(),
        )
        .unwrap(),
        coordinator: Arc::clone(&coordinator),
        complete_tx,
        request: crate::checkpoint_coordinator::CheckpointRequest::default(),
        operator_state: None,
        operator_state_staged_cap_bytes: 0,
        mutable_operator_capture_guard: None,
        fan_out: FxHashMap::default(),
        local_watermark: CheckpointWatermark::Uninitialized,
        handoff_replay_pending: false,
        attempt,
        attempt_started: std::time::Instant::now(),
        attempt_deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        checkpoint_timeout: Duration::from_secs(2),
        serialization_timeout: Duration::from_secs(1),
        checkpoint_cleanup_timeout: Duration::from_secs(1),
        fault_on_retryable_failure: false,
        fault_on_unclassified_error: false,
        checkpoint_fault: Arc::clone(&checkpoint_fault),
        #[cfg(feature = "cluster")]
        controller: None,
        #[cfg(feature = "cluster")]
        leader_proof: None,
        full_vnode_capture_needed: Arc::clone(&full_vnode_capture_needed),
    };

    let failure = tokio::spawn(async move {
        fail_reserved_leader_attempt(
            &mut tail,
            "injected admission failure".into(),
            "reserved-attempt cleanup".into(),
        )
        .await;
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !full_vnode_capture_needed.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failure task must reach reserved-attempt cleanup");
    tokio::task::yield_now().await;

    assert!(matches!(
        complete_rx.try_recv(),
        Err(crossfire::TryRecvError::Empty)
    ));
    assert!(checkpoint_fault.lock().is_none());

    // Let cleanup acquire the coordinator and classify the missing reservation owner.
    drop(coordinator_lock);
    tokio::time::timeout(Duration::from_secs(1), failure)
        .await
        .expect("bounded reserved-attempt cleanup must settle")
        .expect("failure task must not panic");

    let completion = complete_rx.recv().await.unwrap();
    let crate::pipeline::CheckpointCompletion::Failed {
        attempt: completed_attempt,
        error,
    } = completion
    else {
        panic!("reserved attempt must report failure");
    };
    assert_eq!(completed_attempt, attempt);
    assert!(error.contains("injected admission failure"));
    assert!(error.contains("cleanup incomplete; recovery required"));
    assert!(checkpoint_fault
        .lock()
        .as_deref()
        .is_some_and(|fault| fault.contains("cleanup incomplete; recovery required")));
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

#[tokio::test]
async fn tracker_refresh_preserves_watermark_idleness_and_uninitialized_state() {
    let mut callback = empty_callback_fixture();
    callback.source_name_arcs = FxHashMap::from_iter([
        (0, Arc::from("active")),
        (1, Arc::from("idle")),
        (2, Arc::from("uninitialized")),
    ]);
    let mut tracker = laminar_core::time::WatermarkTracker::new(3);
    tracker.update_source(0, 1_000);
    tracker.update_source(1, 500);
    tracker.mark_idle(1);
    tracker.mark_idle(2);
    callback.tracker = Some(tracker);

    callback.refresh_source_frontiers();

    assert_eq!(
        callback.source_frontiers_buf["active"],
        InputFrontier {
            watermark: Some(1_000),
            idle: false,
        }
    );
    assert_eq!(
        callback.source_frontiers_buf["idle"],
        InputFrontier {
            watermark: Some(500),
            idle: true,
        }
    );
    assert_eq!(
        callback.source_frontiers_buf["uninitialized"],
        InputFrontier {
            watermark: None,
            idle: true,
        }
    );
}

/// A clustered runtime cannot finalize event time before its first
/// globally committed frontier.
#[cfg(feature = "cluster")]
#[test]
fn cap_source_frontiers_freezes_before_first_cluster_commit() {
    let mut frontiers: FxHashMap<Arc<str>, InputFrontier> = FxHashMap::default();
    frontiers.insert(
        Arc::from("a"),
        InputFrontier {
            watermark: Some(1_000),
            idle: false,
        },
    );
    frontiers.insert(
        Arc::from("b"),
        InputFrontier {
            watermark: Some(500),
            idle: true,
        },
    );

    ConnectorPipelineCallback::cap_source_frontiers_by_cluster_min(&mut frontiers, None);

    assert_eq!(frontiers["a"].watermark, None);
    assert_eq!(frontiers["b"].watermark, None);
    assert!(frontiers["b"].idle);
}

/// When a cluster-wide minimum is published, sources that have
/// advanced past it get pulled back to it; sources at or below
/// the cap are left alone (cap must not push watermarks forward).
#[cfg(feature = "cluster")]
#[test]
fn cap_source_frontiers_lowers_only_sources_above_cluster_min() {
    let mut frontiers: FxHashMap<Arc<str>, InputFrontier> =
        [("ahead", 2_000), ("at", 1_500), ("behind", 800)]
            .into_iter()
            .map(|(name, watermark)| {
                (
                    Arc::from(name),
                    InputFrontier {
                        watermark: Some(watermark),
                        idle: false,
                    },
                )
            })
            .collect();

    ConnectorPipelineCallback::cap_source_frontiers_by_cluster_min(&mut frontiers, Some(1_500));

    assert_eq!(
        frontiers["ahead"].watermark,
        Some(1_500),
        "source above cluster min must be capped down",
    );
    assert_eq!(
        frontiers["at"].watermark,
        Some(1_500),
        "source at cluster min unchanged",
    );
    assert_eq!(
        frontiers["behind"].watermark,
        Some(800),
        "source below cluster min must NOT be advanced by the cap",
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn source_admission_pins_its_committed_frontier_for_the_whole_cycle() {
    use arrow_array::TimestampMillisecondArray;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use laminar_core::checkpoint::ChannelProgress;

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

    let channel = |source_name: &str, watermark: i64| ChannelProgress {
        participant_id: 1,
        source_name: source_name.into(),
        input_channel: vec![0],
        watermark: Some(watermark),
        idle: false,
    };
    let controller = local_controller();
    controller
        .publish_committed_channel_progress(&[
            channel("orders", 1_500),
            channel("unrelated_slow_source", 100),
        ])
        .unwrap();
    assert_eq!(controller.cluster_min_watermark(), Some(100));

    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(Arc::clone(&controller));
    let mut generator = laminar_core::time::BoundedOutOfOrdernessGenerator::new(0);
    laminar_core::time::WatermarkGenerator::restore_watermark_for_recovery(&mut generator, 2_000);
    callback.watermark_states.insert(
        "orders".into(),
        SourceWatermarkState::new(
            laminar_core::time::EventTimeExtractor::from_column("ts"),
            Box::new(generator),
            "ts".into(),
        ),
    );
    callback
        .pipeline_watermark
        .store(2_000, std::sync::atomic::Ordering::Release);
    crate::pipeline::PipelineCallback::pin_source_frontiers_for_new_cycle(&mut callback).unwrap();

    // A concurrent durable tail may publish the successor while this cycle is being folded.
    controller
        .publish_committed_channel_progress(&[
            channel("orders", 1_800),
            channel("unrelated_slow_source", 200),
        ])
        .unwrap();
    let retained = crate::pipeline::PipelineCallback::filter_late_rows(&callback, "orders", &batch)
        .unwrap()
        .expect("the row after the cycle-pinned source frontier must survive replay");
    assert_eq!(retained.num_rows(), 1);

    crate::pipeline::PipelineCallback::pin_source_frontiers_for_new_cycle(&mut callback).unwrap();
    assert!(
        crate::pipeline::PipelineCallback::filter_late_rows(&callback, "orders", &batch,)
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn idle_source_admission_uses_the_pinned_committed_cut() {
    use arrow_array::TimestampMillisecondArray;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use laminar_core::checkpoint::ChannelProgress;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )])),
        vec![Arc::new(TimestampMillisecondArray::from(vec![500, 1_000]))],
    )
    .unwrap();
    let channel = |source_name: &str, watermark: i64| ChannelProgress {
        participant_id: 1,
        source_name: source_name.into(),
        input_channel: vec![0],
        watermark: Some(watermark),
        idle: false,
    };
    let controller = local_controller();
    controller
        .publish_committed_channel_progress(&[
            channel("orders", 900),
            channel("unrelated_slow_source", 100),
        ])
        .unwrap();

    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);
    callback.source_ids.insert("orders".into(), 0);
    let mut tracker = laminar_core::time::WatermarkTracker::new(1);
    tracker.update_source(0, 100);
    tracker.mark_idle(0);
    callback.tracker = Some(tracker);
    let mut generator = laminar_core::time::BoundedOutOfOrdernessGenerator::new(0);
    laminar_core::time::WatermarkGenerator::restore_watermark_for_recovery(&mut generator, 100);
    callback.watermark_states.insert(
        "orders".into(),
        SourceWatermarkState::new(
            laminar_core::time::EventTimeExtractor::from_column("ts"),
            Box::new(generator),
            "ts".into(),
        ),
    );
    callback
        .pipeline_watermark
        .store(1_500, std::sync::atomic::Ordering::Release);
    crate::pipeline::PipelineCallback::pin_source_frontiers_for_new_cycle(&mut callback).unwrap();

    let retained = crate::pipeline::PipelineCallback::filter_late_rows(&callback, "orders", &batch)
        .unwrap()
        .expect("the row at or beyond the idle source's committed cut must survive");
    assert_eq!(retained.num_rows(), 1);
    assert_eq!(
        retained
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        1_000
    );

    callback.tracker.as_mut().unwrap().update_source(0, 100);
    callback
        .pipeline_watermark
        .store(100, std::sync::atomic::Ordering::Release);
    let retained = crate::pipeline::PipelineCallback::filter_late_rows(&callback, "orders", &batch)
        .unwrap()
        .expect("active replay must retain rows above its lower local frontier");
    assert_eq!(retained.num_rows(), 2);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn empty_idle_source_gaining_a_channel_activates_at_the_pinned_committed_cut() {
    use laminar_core::checkpoint::{ChannelProgress, SINGLETON_WATERMARK_CHANNEL};
    use std::time::Duration;

    let channel = |source_name: &str, watermark: i64| ChannelProgress {
        participant_id: 1,
        source_name: source_name.into(),
        input_channel: vec![0],
        watermark: Some(watermark),
        idle: false,
    };
    let controller = local_controller();
    controller
        .publish_committed_channel_progress(&[
            channel("orders", 900),
            channel("unrelated_slow_source", 100),
        ])
        .unwrap();

    let mut state = SourceWatermarkState::new(
        laminar_core::time::EventTimeExtractor::from_column("ts"),
        Box::new(laminar_core::time::BoundedOutOfOrdernessGenerator::new(0)),
        "ts".into(),
    )
    .with_input_channels(
        Duration::ZERO,
        0,
        None,
        FxHashMap::default(),
        Some(Arc::from([])),
    );
    state
        .install_input_channels(Some(Arc::from([])), i64::MIN)
        .unwrap();
    assert_eq!(state.install_committed_watermark_floor(900), Some(900));

    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);
    callback.source_ids.insert("orders".into(), 0);
    callback.source_name_arcs.insert(0, Arc::from("orders"));
    callback.checkpoint_source_names = vec!["orders".into()];
    let mut tracker = laminar_core::time::WatermarkTracker::new(1);
    tracker.update_source(0, 900);
    tracker.mark_idle(0);
    callback.tracker = Some(tracker);
    callback.watermark_states.insert("orders".into(), state);
    crate::pipeline::PipelineCallback::pin_source_frontiers_for_new_cycle(&mut callback).unwrap();

    let request = callback.build_checkpoint_request().unwrap();
    assert_eq!(request.channel_progress.len(), 1);
    let marker = &request.channel_progress[0];
    assert_eq!(marker.source_name, "orders");
    assert_eq!(marker.input_channel.as_slice(), SINGLETON_WATERMARK_CHANNEL);
    assert_eq!(marker.watermark, Some(900));
    assert!(marker.idle);

    crate::pipeline::PipelineCallback::reconcile_source_input_channels(
        &mut callback,
        "orders",
        Some(Arc::from([b"p0".to_vec()])),
    )
    .unwrap();
    let progress = callback.watermark_states["orders"]
        .input_channel_progress()
        .unwrap()
        .unwrap();
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].watermark, Some(900));
    assert!(!progress[0].idle);
}

#[tokio::test]
async fn local_empty_partition_inventory_emits_its_logical_watermark_marker() {
    use laminar_core::checkpoint::SINGLETON_WATERMARK_CHANNEL;
    use std::time::Duration;

    let empty_inventory: Arc<[Vec<u8>]> = Arc::from(Vec::<Vec<u8>>::new());
    let mut state = SourceWatermarkState::new(
        laminar_core::time::EventTimeExtractor::from_column("ts"),
        Box::new(laminar_core::time::BoundedOutOfOrdernessGenerator::new(0)),
        "ts".into(),
    )
    .with_input_channels(
        Duration::ZERO,
        0,
        None,
        FxHashMap::default(),
        Some(Arc::clone(&empty_inventory)),
    );
    state
        .install_input_channels(Some(empty_inventory), i64::MIN)
        .unwrap();
    assert_eq!(state.install_committed_watermark_floor(900), Some(900));

    let mut callback = empty_callback_fixture();
    callback.source_ids.insert("orders".into(), 0);
    callback.source_name_arcs.insert(0, Arc::from("orders"));
    callback.checkpoint_source_names = vec!["orders".into()];
    callback.watermark_states.insert("orders".into(), state);
    let mut tracker = laminar_core::time::WatermarkTracker::new(1);
    tracker.update_source(0, 900);
    tracker.mark_idle(0);
    callback.tracker = Some(tracker);

    let request = callback.build_checkpoint_request().unwrap();
    assert_eq!(request.channel_progress.len(), 1);
    let marker = &request.channel_progress[0];
    assert_eq!(marker.source_name, "orders");
    assert_eq!(marker.input_channel.as_slice(), SINGLETON_WATERMARK_CHANNEL);
    assert_eq!(marker.watermark, Some(900));
    assert!(marker.idle);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cold_empty_successor_rehydrates_the_callback_source_pin() {
    use laminar_core::checkpoint::{
        ChannelProgress, CheckpointScope, CommittedCheckpointIndex, CommittedParticipantRef,
        ConnectorCheckpoint, PipelineIdentity, COMMITTED_CHECKPOINT_INDEX_VERSION,
    };
    use laminar_core::state::LOCAL_NODE_ID;
    use std::{collections::BTreeMap, time::Duration};

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let warm_store =
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(&backing));
    let deployment_id = warm_store.load_or_create_deployment_id().await.unwrap();
    let participant = || CommittedParticipantRef {
        participant_id: LOCAL_NODE_ID.0,
        manifest_len: 1,
        manifest_sha256: digest(1),
        node_data_len: 0,
        node_data_sha256: digest(2),
    };
    let predecessor = CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: deployment_id.clone(),
        pipeline_identity: PipelineIdentity::empty(),
        epoch: 1,
        checkpoint_id: 1,
        scope: CheckpointScope::Local,
        vnode_count: 1,
        assignment_fence: None,
        reassignment_portable: false,
        predecessor: None,
        participants: vec![participant()],
        source_names: vec!["orders".into()],
        source_offsets: BTreeMap::from([(
            "orders".into(),
            ConnectorCheckpoint {
                input_channels: Some(vec![b"p0".to_vec()]),
                ..ConnectorCheckpoint::default()
            },
        )]),
        channel_progress: vec![ChannelProgress {
            participant_id: LOCAL_NODE_ID.0,
            source_name: "orders".into(),
            input_channel: b"p0".to_vec(),
            watermark: Some(900),
            idle: false,
        }],
        source_watermarks: BTreeMap::from([("orders".into(), 900)]),
        checkpoint_watermark: Some(900),
    };
    let predecessor_ref = warm_store
        .create_committed_checkpoint(&predecessor)
        .await
        .unwrap();
    let mut successor = predecessor.clone();
    successor.epoch = 2;
    successor.checkpoint_id = 2;
    successor.predecessor = Some(predecessor_ref);
    successor
        .source_offsets
        .get_mut("orders")
        .unwrap()
        .input_channels = Some(Vec::new());
    successor.channel_progress.clear();
    successor.checkpoint_watermark = None;
    successor.validate_predecessor_index(&predecessor).unwrap();
    let successor_ref = warm_store
        .create_committed_checkpoint(&successor)
        .await
        .unwrap();
    drop(warm_store);

    let cold_store = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(backing);
    let restored = cold_store
        .load_committed_checkpoint(&successor_ref)
        .await
        .unwrap();
    let restored_predecessor = cold_store
        .load_committed_checkpoint(restored.predecessor.as_ref().unwrap())
        .await
        .unwrap();
    restored
        .validate_predecessor_index(&restored_predecessor)
        .unwrap();
    let retained = restored.effective_source_watermarks().unwrap();
    assert!(restored.channel_progress.is_empty());
    assert_eq!(retained.get("orders"), Some(&900));

    let controller = local_controller();
    controller
        .replace_recovered_checkpoint_progress(&restored.channel_progress, &retained)
        .unwrap();
    let mut state = SourceWatermarkState::new(
        laminar_core::time::EventTimeExtractor::from_column("ts"),
        Box::new(laminar_core::time::BoundedOutOfOrdernessGenerator::new(0)),
        "ts".into(),
    )
    .with_input_channels(
        Duration::ZERO,
        0,
        None,
        FxHashMap::default(),
        Some(Arc::from([])),
    );
    state
        .install_input_channels(Some(Arc::from([])), i64::MIN)
        .unwrap();
    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);
    callback.source_ids.insert("orders".into(), 0);
    let mut tracker = laminar_core::time::WatermarkTracker::new(1);
    tracker.mark_idle(0);
    callback.tracker = Some(tracker);
    callback.watermark_states.insert("orders".into(), state);

    crate::pipeline::PipelineCallback::pin_source_frontiers_for_new_cycle(&mut callback).unwrap();
    assert_eq!(
        callback.committed_source_watermarks_snapshot.get("orders"),
        Some(&900)
    );
    crate::pipeline::PipelineCallback::reconcile_source_input_channels(
        &mut callback,
        "orders",
        Some(Arc::from([b"p0".to_vec()])),
    )
    .unwrap();
    let progress = callback.watermark_states["orders"]
        .input_channel_progress()
        .unwrap()
        .unwrap();
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].watermark, Some(900));
    assert!(!progress[0].idle);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn idle_watermark_tick_retains_the_pinned_committed_cut() {
    use laminar_core::checkpoint::ChannelProgress;
    use std::time::Duration;

    let channel = |source_name: &str, watermark: i64| ChannelProgress {
        participant_id: 1,
        source_name: source_name.into(),
        input_channel: vec![0],
        watermark: Some(watermark),
        idle: false,
    };
    let committed_cut = i64::MAX - 1;
    let controller = local_controller();
    controller
        .publish_committed_channel_progress(&[
            channel("orders", committed_cut),
            channel("unrelated_slow_source", 100),
        ])
        .unwrap();

    let mut state = SourceWatermarkState::new(
        laminar_core::time::EventTimeExtractor::from_column("ts"),
        Box::new(laminar_core::time::BoundedOutOfOrdernessGenerator::new(0)),
        "ts".into(),
    )
    .with_input_channels(
        Duration::ZERO,
        0,
        Some(Duration::ZERO),
        FxHashMap::default(),
        None,
    );
    state
        .install_input_channels(Some(Arc::from([b"p0".to_vec()])), 500)
        .unwrap();

    let mut callback = empty_callback_fixture();
    callback.cluster_controller = Some(controller);
    callback.source_ids.insert("orders".into(), 0);
    callback.source_name_arcs.insert(0, Arc::from("orders"));
    let mut tracker = laminar_core::time::WatermarkTracker::new(1);
    tracker.update_source(0, 500);
    callback.tracker = Some(tracker);
    callback.watermark_states.insert("orders".into(), state);
    crate::pipeline::PipelineCallback::pin_source_frontiers_for_new_cycle(&mut callback).unwrap();

    crate::pipeline::PipelineCallback::tick_idle_watermark(&mut callback);
    let tracker = callback.tracker.as_ref().unwrap();
    assert_eq!(tracker.source_watermark(0), Some(committed_cut));
    assert!(tracker.is_idle(0));
    let progress = callback.watermark_states["orders"]
        .input_channel_progress()
        .unwrap()
        .unwrap();
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].watermark, Some(committed_cut));
    assert!(progress[0].idle);
}

#[cfg(feature = "cluster")]
fn follower_identity(epoch: u64, checkpoint_id: u64, digest: u8) -> CertifiedCheckpointAttempt {
    CertifiedCheckpointAttempt {
        attempt: CheckpointAttempt::new(epoch, checkpoint_id),
        assignment_digest: [digest; 32],
        flags: laminar_core::checkpoint::flags::NONE,
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
        flags: laminar_core::checkpoint::flags::NONE,
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
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let follower_info = NodeInfo {
        id: follower_id,
        name: "follower".into(),
        rpc_address: String::new(),
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
struct PreparedQuorumControllers {
    leader: Arc<laminar_core::cluster::control::ClusterController>,
    follower: Arc<laminar_core::cluster::control::ClusterController>,
    fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_proof: laminar_core::cluster::control::LeaderProof,
    _follower_members_tx:
        tokio::sync::watch::Sender<Vec<laminar_core::cluster::discovery::NodeInfo>>,
    _leader_members_tx: tokio::sync::watch::Sender<Vec<laminar_core::cluster::discovery::NodeInfo>>,
    _leader_grant_tx:
        tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>>,
}

#[cfg(feature = "cluster")]
async fn prepared_quorum_controllers() -> PreparedQuorumControllers {
    use laminar_core::cluster::control::barrier::BARRIER_ADDR_KEY;
    use laminar_core::cluster::control::{
        ClusterController, ClusterKv, InMemoryKv, LeaseDeadline, ProcessLease,
    };
    use laminar_core::cluster::discovery::{NodeInfo, NodeMetadata, NodeState};

    let (follower_kv, follower, leader_id, follower_members_tx, _decision_store) =
        gate_controller().await;
    let authority = follower.checkpoint_authority().unwrap();
    let leader_grant = authority.load().await.unwrap().unwrap();
    let leader_proof = leader_grant.proof();

    let follower = Arc::new(follower);
    follower
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    follower
        .start_leased_barrier_server(
            "127.0.0.1:0".parse().unwrap(),
            None,
            &ProcessLease {
                node: follower.instance_id(),
                owner: follower.recovery_incarnation(),
                term: 1,
                seq: 1,
                expires_at_ms: i64::MAX,
            },
        )
        .await
        .unwrap();

    let leader_kv = Arc::new(InMemoryKv::new(leader_id));
    let leader_control: Arc<dyn ClusterKv> = leader_kv.clone();
    let (leader_members_tx, leader_members_rx) = tokio::sync::watch::channel(vec![NodeInfo {
        id: follower.instance_id(),
        name: "follower".into(),
        rpc_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    }]);
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
        .set_leader_lease_watch(leader_grant_rx, leader_grant.owner.clone(), leader_deadline)
        .unwrap();
    leader.install_local_leader_proof_provider();
    leader
        .start_leased_barrier_server(
            "127.0.0.1:0".parse().unwrap(),
            None,
            &ProcessLease {
                node: leader_id,
                owner: leader_grant.owner.boot,
                term: leader_grant.owner.process_term,
                seq: 1,
                expires_at_ms: i64::MAX,
            },
        )
        .await
        .unwrap();

    let follower_endpoint = follower_kv
        .read_from(follower.instance_id(), BARRIER_ADDR_KEY)
        .await
        .unwrap();
    leader_kv.seed(follower.instance_id(), BARRIER_ADDR_KEY, follower_endpoint);
    let leader_endpoint = leader_kv
        .read_from(leader_id, BARRIER_ADDR_KEY)
        .await
        .unwrap();
    follower_kv.seed(leader_id, BARRIER_ADDR_KEY, leader_endpoint);

    let fence = assignment_fence(19, &[leader_id.0, follower.instance_id().0]);
    leader.publish_checkpoint_assignment_fence(Some(fence.clone()));
    follower.publish_checkpoint_assignment_fence(Some(fence.clone()));
    assert_eq!(leader.capture_leader_proof().as_ref(), Some(&leader_proof));

    PreparedQuorumControllers {
        leader,
        follower,
        fence,
        leader_proof,
        _follower_members_tx: follower_members_tx,
        _leader_members_tx: leader_members_tx,
        _leader_grant_tx: leader_grant_tx,
    }
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn captured_ack_can_arrive_after_thirty_seconds_before_attempt_deadline() {
    use laminar_core::cluster::control::{BarrierAck, BarrierAckDisposition, Phase};

    let controllers = prepared_quorum_controllers().await;
    let attempt = CheckpointAttempt::new(41, 41);
    let flags = laminar_core::checkpoint::flags::NONE;
    let prepare = certified_barrier(
        attempt,
        controllers.fence.clone(),
        controllers.leader_proof.clone(),
        Phase::Prepare,
    );
    let retry_window = Duration::from_secs(3);
    let attempt_window = Duration::from_secs(40);
    let deadline = tokio::time::Instant::now() + attempt_window;
    controllers
        .leader
        .announce_prepare_barrier_until(&prepare, deadline, retry_window)
        .await
        .unwrap();

    let prepared = ConnectorPipelineCallback::wait_for_capture_quorum_until(
        &controllers.leader,
        deadline,
        attempt_window,
        crate::checkpoint_coordinator::PrepareQuorum::new(
            attempt,
            CheckpointWatermark::Active(101),
            &controllers.fence,
            &controllers.leader_proof,
            flags,
        ),
    );
    tokio::pin!(prepared);
    tokio::select! {
        result = &mut prepared => panic!("capture quorum completed before its follower ack: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    tokio::time::advance(Duration::from_secs(31)).await;
    controllers
        .follower
        .ack_barrier(&BarrierAck {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            assignment_digest: Some(controllers.fence.digest()),
            flags,
            disposition: BarrierAckDisposition::Captured,
            error: None,
            watermark: CheckpointWatermark::Active(91),
        })
        .await
        .unwrap();

    // The fan-out intentionally uses real loopback gRPC. Resume wall-clock time after proving
    // that the task survived 31 virtual seconds so Tokio cannot auto-advance the remaining retry
    // timers past real socket I/O and the exact attempt deadline.
    tokio::time::resume();

    let (watermark, participants, replay_pending) = prepared
        .await
        .expect("an immutable follower capture must satisfy quorum before the exact deadline");
    assert_eq!(watermark, CheckpointWatermark::Active(91));
    assert_eq!(participants, vec![controllers.follower.instance_id()]);
    assert!(!replay_pending);
    assert!(tokio::time::Instant::now() < deadline);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn capture_quorum_fails_at_the_exact_attempt_deadline() {
    use laminar_core::cluster::control::Phase;

    let controllers = prepared_quorum_controllers().await;
    let attempt = CheckpointAttempt::new(42, 42);
    let prepare = certified_barrier(
        attempt,
        controllers.fence.clone(),
        controllers.leader_proof.clone(),
        Phase::Prepare,
    );
    let retry_window = Duration::from_secs(3);
    let attempt_window = Duration::from_secs(8);
    let started = tokio::time::Instant::now();
    let deadline = started + attempt_window;
    controllers
        .leader
        .announce_prepare_barrier_until(&prepare, deadline, retry_window)
        .await
        .unwrap();

    let prepared = ConnectorPipelineCallback::wait_for_capture_quorum_until(
        &controllers.leader,
        deadline,
        attempt_window,
        crate::checkpoint_coordinator::PrepareQuorum::new(
            attempt,
            CheckpointWatermark::Active(101),
            &controllers.fence,
            &controllers.leader_proof,
            laminar_core::checkpoint::flags::NONE,
        ),
    );
    tokio::pin!(prepared);
    tokio::select! {
        result = &mut prepared => panic!("capture quorum completed before its deadline: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    tokio::time::advance(retry_window + Duration::from_secs(1)).await;
    tokio::select! {
        result = &mut prepared => panic!("legacy retry window ended the attempt early: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    tokio::time::advance(attempt_window - (retry_window + Duration::from_secs(1))).await;

    let error = prepared
        .await
        .expect_err("missing Captured acknowledgement must fail at the attempt deadline");
    assert!(
        error.contains("timed out") || error.contains("end-to-end checkpoint deadline"),
        "{error}"
    );
    assert_eq!(tokio::time::Instant::now(), deadline);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn aligned_resume_gate_survives_the_legacy_retry_window() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (kv, controller, leader_id, _members_tx, _decision_store) = gate_controller().await;
    let (fence, identity) = resume_identity(43, 43);
    let attempt_window = Duration::from_secs(20);
    let attempt_deadline = tokio::time::Instant::now() + attempt_window;
    let aligned = ConnectorPipelineCallback::wait_for_aligned_resume_until(
        true,
        &controller,
        identity.clone(),
        &fence,
        attempt_deadline,
    );
    tokio::pin!(aligned);
    tokio::select! {
        result = &mut aligned => panic!("resume gate completed before Aligned: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    tokio::time::advance(Duration::from_secs(12)).await;
    kv.seed(
        leader_id,
        ANNOUNCEMENT_KEY,
        serde_json::to_string(&BarrierAnnouncement {
            epoch: identity.attempt.epoch,
            checkpoint_id: identity.attempt.checkpoint_id,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(identity.leader_proof),
            phase: Phase::Aligned,
            flags: identity.flags,
        })
        .unwrap(),
    );

    tokio::time::timeout(Duration::from_secs(1), &mut aligned)
        .await
        .expect("Aligned observation must complete before the exact attempt deadline")
        .expect("the resume gate must outlive the old ten-second derived timeout");
    assert!(tokio::time::Instant::now() < attempt_deadline);
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
        BarrierAck, BarrierAckDisposition, BarrierAnnouncement, ClusterKv, Phase, ACK_KEY,
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
    assert_eq!(acknowledgement.flags, laminar_core::checkpoint::flags::NONE);
    assert_eq!(acknowledgement.disposition, BarrierAckDisposition::Failed);
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
#[test]
fn live_leader_durably_aborts_shuffle_follower_nack_before_retirement() {
    const TEST_THREAD_STACK_BYTES: usize = 4 * 1024 * 1024;

    let test = std::thread::Builder::new()
        .name("live-leader-shuffle-nack-test".into())
        .stack_size(TEST_THREAD_STACK_BYTES)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("live-leader shuffle NACK test runtime must build");
            runtime.block_on(
                live_leader_durably_aborts_shuffle_follower_nack_before_retirement_body(),
            );
        })
        .expect("live-leader shuffle NACK test thread must spawn");
    if let Err(panic) = test.join() {
        std::panic::resume_unwind(panic);
    }
}

#[cfg(feature = "cluster")]
async fn live_leader_durably_aborts_shuffle_follower_nack_before_retirement_body() {
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

    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        memory_checkpoint_store(),
    )
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
    let checkpoint_fault = Arc::new(parking_lot::Mutex::new(None));
    let mut request = crate::checkpoint_coordinator::CheckpointRequest::default();
    request.assignment_fence = Some(fence.clone());
    request.reassignment_portable = true;
    let mut tail = LeaderTail {
        in_flight: EpochInFlightGuard::claim(
            &in_flight,
            &checkpoint_fault,
            attempt,
            std::iter::empty(),
        )
        .unwrap(),
        coordinator,
        complete_tx,
        request,
        operator_state: None,
        operator_state_staged_cap_bytes: 0,
        mutable_operator_capture_guard: None,
        fan_out: FxHashMap::default(),
        local_watermark: CheckpointWatermark::Uninitialized,
        handoff_replay_pending: false,
        attempt,
        attempt_started: std::time::Instant::now(),
        attempt_deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        checkpoint_timeout: Duration::from_secs(2),
        serialization_timeout: Duration::from_secs(1),
        checkpoint_cleanup_timeout: Duration::from_secs(1),
        fault_on_retryable_failure: false,
        fault_on_unclassified_error: true,
        checkpoint_fault,
        controller: Some(Arc::clone(&leader)),
        leader_proof: Some(leader_proof),
        full_vnode_capture_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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

        let leader_tail = ConnectorPipelineCallback::capture_leader_quorum(
            &mut tail,
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
        BarrierAck, BarrierAckDisposition, BarrierAnnouncement, ClusterKv, Phase, ACK_KEY,
        ANNOUNCEMENT_KEY,
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
    assert_eq!(acknowledgement.flags, laminar_core::checkpoint::flags::NONE);
    assert_eq!(acknowledgement.disposition, BarrierAckDisposition::Failed);
    assert_eq!(acknowledgement.error.as_deref(), Some(error.as_str()));
}

#[cfg(feature = "cluster")]
async fn gate_committed_checkpoint(
    decision_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    attempt: CheckpointAttempt,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
) -> laminar_core::checkpoint::CommittedCheckpointIndex {
    use laminar_core::checkpoint::{
        ChannelProgress, CheckpointScope, CommittedCheckpointIndex, CommittedParticipantRef,
        ConnectorCheckpoint, PipelineIdentity, COMMITTED_CHECKPOINT_INDEX_VERSION,
        SINGLETON_WATERMARK_CHANNEL,
    };

    let participants = fence
        .participant_ids()
        .into_iter()
        .map(|participant_id| CommittedParticipantRef {
            participant_id,
            manifest_len: 1,
            manifest_sha256: digest(3),
            node_data_len: 0,
            node_data_sha256: digest(4),
        })
        .collect::<Vec<_>>();
    let channel_progress = participants
        .iter()
        .map(|participant| ChannelProgress {
            participant_id: participant.participant_id,
            source_name: "source".into(),
            input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
            watermark: Some(42),
            idle: false,
        })
        .collect();
    CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: decision_store.load_or_create_deployment_id().await.unwrap(),
        pipeline_identity: PipelineIdentity::empty(),
        epoch: attempt.epoch,
        checkpoint_id: attempt.checkpoint_id,
        scope: CheckpointScope::Cluster,
        vnode_count: u16::try_from(fence.vnode_count).unwrap(),
        assignment_fence: Some(fence.clone()),
        reassignment_portable: true,
        predecessor: None,
        participants,
        source_names: vec!["source".into()],
        source_offsets: std::collections::BTreeMap::from([(
            "source".into(),
            ConnectorCheckpoint::default(),
        )]),
        channel_progress,
        source_watermarks: std::collections::BTreeMap::from([("source".into(), 42)]),
        checkpoint_watermark: Some(42),
    }
}

#[cfg(feature = "cluster")]
async fn record_gate_commit(
    controller: &laminar_core::cluster::control::ClusterController,
    decision_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    attempt: CheckpointAttempt,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
) {
    let index = gate_committed_checkpoint(decision_store, attempt, fence).await;
    let authority = controller.checkpoint_authority().unwrap();
    let proof = authority.load().await.unwrap().unwrap().proof();
    crate::rebalance::admit_cluster_checkpoint_artifacts_for_test(&authority, &proof, &index).await;
    let committed_checkpoint = decision_store
        .create_committed_checkpoint(&index)
        .await
        .unwrap();
    authority
        .record_cluster_outcome(
            &proof,
            attempt.epoch,
            attempt.checkpoint_id,
            fence.clone(),
            laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
            Some(committed_checkpoint),
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
        "the committed checkpoint owns the recovery-safe watermark"
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
