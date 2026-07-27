use super::*;
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

fn test_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(Float64Array::from(vec![150.0, 2800.0])),
            Arc::new(Int64Array::from(vec![1000, 2000])),
        ],
    )
    .unwrap()
}

#[cfg(feature = "cluster")]
#[test]
fn default_operator_rejects_checkpointed_shuffle() {
    let mut operator = SourcePassthrough;

    let error = operator
        .stage_checkpointed_shuffle(
            "unadmitted-join-stage",
            RetainedBatch::local(test_batch()),
            0,
        )
        .expect_err("operators without an admitted shuffle path must fail closed");

    assert!(error
        .to_string()
        .contains("does not accept checkpointed shuffle stage"));
}

struct RestoreProbe(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait]
impl GraphOperator for RestoreProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

struct RestoreFailureProbe {
    restores: Arc<std::sync::atomic::AtomicUsize>,
    drops: Arc<std::sync::atomic::AtomicUsize>,
    fail: bool,
}

impl Drop for RestoreFailureProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl GraphOperator for RestoreFailureProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        self.restores
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.fail {
            Err(DbError::Pipeline("injected late restore failure".into()))
        } else {
            Ok(())
        }
    }
}

#[cfg(feature = "cluster")]
struct RestoredReplayWatermarkProbe {
    replay_watermark: Option<i64>,
    processed: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "cluster")]
#[async_trait]
impl GraphOperator for RestoredReplayWatermarkProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if self.replay_watermark.is_none() {
            return Ok(Vec::new());
        }
        assert!(inputs.is_empty(), "replay-only cycle accepted new input");
        self.replay_watermark = None;
        self.processed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(vec![test_batch()])
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(self.replay_watermark.map(|watermark| OperatorCheckpoint {
            data: watermark.to_le_bytes().to_vec(),
        }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let encoded: [u8; 8] = checkpoint
            .data
            .try_into()
            .map_err(|_| DbError::Checkpoint("invalid replay-watermark probe checkpoint".into()))?;
        self.replay_watermark = Some(i64::from_le_bytes(encoded));
        Ok(())
    }

    fn watermark_hold(&self) -> Option<i64> {
        self.replay_watermark
    }

    fn restored_output_watermark(&self) -> Option<i64> {
        self.replay_watermark
    }

    fn wants_input(&self) -> bool {
        self.replay_watermark.is_none()
    }
}

#[test]
fn whole_graph_restore_rejects_old_abi_before_mutation() {
    let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.allocate_node(GraphNode::new(
        Arc::from("present"),
        Box::new(RestoreProbe(Arc::clone(&restores))),
        1,
    ));
    let mut operators = OperatorStateMap::new();
    operators.insert("present".into(), vec![1]);

    let error = graph
        .restore_state(&GraphCheckpoint {
            version: GRAPH_CHECKPOINT_VERSION - 1,
            operators,
        })
        .err()
        .expect("old graph ABI must fail");

    assert!(error.to_string().contains("[LDB-6043]"), "{error}");
    assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn whole_graph_checkpoint_serialization_enforces_its_byte_budget() {
    let mut operators = OperatorStateMap::new();
    operators.insert("stateful".into(), vec![42; 4_096]);
    let checkpoint = GraphCheckpoint {
        version: GRAPH_CHECKPOINT_VERSION,
        operators,
    };
    let encoded = OperatorGraph::serialize_checkpoint_bounded(&checkpoint, u64::MAX).unwrap();
    let restored = rkyv::from_bytes::<GraphCheckpoint, rkyv::rancor::Error>(&encoded).unwrap();
    assert_eq!(restored.version, GRAPH_CHECKPOINT_VERSION);
    assert_eq!(restored.operators["stateful"], vec![42; 4_096]);

    let error = OperatorGraph::serialize_checkpoint_bounded(
        &checkpoint,
        u64::try_from(encoded.len() - 1).unwrap(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("byte budget"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restored_replay_seeds_and_holds_output_watermark_through_final_emission() {
    let donor_processed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut donor = OperatorGraph::new(laminar_sql::create_session_context());
    donor.push_test_node(
        "replay",
        Box::new(RestoredReplayWatermarkProbe {
            replay_watermark: Some(42),
            processed: donor_processed,
        }),
    );
    let checkpoint = donor.snapshot_state().unwrap().unwrap();
    let encoded = OperatorGraph::serialize_checkpoint_bounded(&checkpoint, u64::MAX).unwrap();

    let processed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut target = OperatorGraph::new(laminar_sql::create_session_context());
    target.push_test_node(
        "replay",
        Box::new(RestoredReplayWatermarkProbe {
            replay_watermark: None,
            processed: Arc::clone(&processed),
        }),
    );
    let (mut restored, count) = target.restore_from_bytes(&encoded).unwrap();
    assert_eq!(count, 1);
    assert_eq!(restored.output_watermarks[0], 42);

    let mut results = FxHashMap::default();
    restored
        .execute_single_operator(0, 100, &mut results)
        .await
        .unwrap();
    assert!(processed.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        restored.output_watermarks[0], 42,
        "the replay-only emission cycle must not advance past its restored watermark"
    );

    restored
        .execute_single_operator(0, 100, &mut results)
        .await
        .unwrap();
    assert_eq!(
        restored.output_watermarks[0], 100,
        "the next input-accepting cycle may advance after replay drains"
    );
}

#[test]
fn whole_graph_restore_rejects_missing_operator_before_mutation() {
    let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.allocate_node(GraphNode::new(
        Arc::from("present"),
        Box::new(RestoreProbe(Arc::clone(&restores))),
        1,
    ));
    let mut operators = OperatorStateMap::new();
    operators.insert("present".into(), vec![1]);
    operators.insert("missing".into(), vec![2]);

    let error = graph
        .restore_state(&GraphCheckpoint {
            version: GRAPH_CHECKPOINT_VERSION,
            operators,
        })
        .err()
        .expect("missing operator must fail");

    assert!(error.to_string().contains("missing operator(s): missing"));
    assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn whole_graph_restore_closes_before_first_execution_cycle() {
    let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.allocate_node(GraphNode::new(
        Arc::from("present"),
        Box::new(RestoreProbe(Arc::clone(&restores))),
        1,
    ));
    graph
        .execute_cycle(&FxHashMap::default(), i64::MIN, None)
        .await
        .unwrap();
    let operators = [("present".to_string(), vec![1])].into_iter().collect();

    let error = graph
        .restore_state(&GraphCheckpoint {
            version: GRAPH_CHECKPOINT_VERSION,
            operators,
        })
        .err()
        .expect("restore after execution must fail");

    assert!(error
        .to_string()
        .contains("before the first execution cycle"));
    assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn late_restore_failure_consumes_and_drops_partial_graph() {
    let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    for (name, fail) in [("first", false), ("second", true)] {
        graph.allocate_node(GraphNode::new(
            Arc::from(name),
            Box::new(RestoreFailureProbe {
                restores: Arc::clone(&restores),
                drops: Arc::clone(&drops),
                fail,
            }),
            1,
        ));
    }
    let operators = [
        ("first".to_string(), vec![1]),
        ("second".to_string(), vec![2]),
    ]
    .into_iter()
    .collect();

    let error = graph
        .restore_state(&GraphCheckpoint {
            version: GRAPH_CHECKPOINT_VERSION,
            operators,
        })
        .err()
        .expect("late restore fault must fail the graph");

    assert!(matches!(error, DbError::Checkpoint(_)));
    assert!(error.requires_pipeline_recovery());
    assert!(error.to_string().contains("second"), "{error}");
    assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn stateless_operator_rejects_unexpected_checkpoint_state() {
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.allocate_node(GraphNode::new(
        Arc::from("source"),
        Box::new(SourcePassthrough),
        1,
    ));
    let operators = [("source".to_string(), vec![1])].into_iter().collect();

    let error = graph
        .restore_state(&GraphCheckpoint {
            version: GRAPH_CHECKPOINT_VERSION,
            operators,
        })
        .err()
        .expect("stateless operator state must be rejected");

    assert!(error
        .to_string()
        .contains("does not accept checkpoint state"));
    assert!(error.requires_pipeline_recovery());
}

/// Records the batches handed to `stage_checkpointed_shuffle`.
#[cfg(feature = "cluster")]
struct RecordingOperator(Arc<parking_lot::Mutex<Vec<RetainedBatch>>>);

#[cfg(feature = "cluster")]
#[async_trait]
impl GraphOperator for RecordingOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }
    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
    fn stage_checkpointed_shuffle(
        &mut self,
        _stage: &str,
        batch: RetainedBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        self.0.lock().push(batch);
        Ok(())
    }
}

#[cfg(feature = "cluster")]
struct RecordingVnodeRestoreOperator {
    applied: Arc<parking_lot::Mutex<Vec<u32>>>,
    failure_on_vnode: Option<(u32, &'static str)>,
}

#[cfg(feature = "cluster")]
#[async_trait]
impl GraphOperator for RecordingVnodeRestoreOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }

    fn apply_vnode_chain(
        &mut self,
        vnode: u32,
        _base: &[u8],
        _deltas: &[&[u8]],
    ) -> Result<(), DbError> {
        if let Some((failed_vnode, message)) = self.failure_on_vnode {
            if failed_vnode == vnode {
                return Err(DbError::Pipeline(message.into()));
            }
        }
        self.applied.lock().push(vnode);
        Ok(())
    }
}

#[cfg(feature = "cluster")]
struct RestoringRosterProbe {
    label: &'static str,
    registry: Arc<laminar_core::state::VnodeRegistry>,
    observations: Arc<parking_lot::Mutex<Vec<(&'static str, u32, Vec<u32>)>>>,
}

#[cfg(feature = "cluster")]
#[async_trait]
impl GraphOperator for RestoringRosterProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn apply_vnode_chain(
        &mut self,
        vnode: u32,
        _base: &[u8],
        _deltas: &[&[u8]],
    ) -> Result<(), DbError> {
        self.observations
            .lock()
            .push((self.label, vnode, self.registry.restoring_vnodes()));
        Ok(())
    }
}

#[cfg(feature = "cluster")]
fn encoded_vnode_partial(partial: &crate::vnode_partial::VnodePartial) -> bytes::Bytes {
    bytes::Bytes::from(partial.encode().expect("encode test vnode partial"))
}

#[cfg(feature = "cluster")]
fn sealed_test_parent(
    attempt: laminar_core::state::CheckpointAttempt,
) -> crate::vnode_partial::SealedVnodeParentLink {
    crate::vnode_partial::SealedVnodeParentLink::new(
        attempt,
        &laminar_core::state::SealedVnodePartial {
            vnode: 0,
            assignment_version: 0,
            writer: None,
            payload_len: 0,
            payload_sha256: "00".repeat(32),
        },
    )
    .expect("valid test parent attestation")
}

#[cfg(feature = "cluster")]
type TestVnodeRevokeHandle = Arc<parking_lot::Mutex<Option<crate::db::StagedVnodeRevocation>>>;

#[cfg(feature = "cluster")]
fn target_scoped_revoke_handle(vnodes: impl IntoIterator<Item = u32>) -> TestVnodeRevokeHandle {
    let vnodes = vnodes.into_iter().collect::<FxHashSet<_>>();
    Arc::new(parking_lot::Mutex::new(Some(
        crate::db::StagedVnodeRevocation::target_scoped_for_test(vnodes)
            .expect("test revoke roster must be nonempty"),
    )))
}

#[cfg(feature = "cluster")]
fn staged_revoke_vnodes(handle: &TestVnodeRevokeHandle) -> Option<FxHashSet<u32>> {
    handle.lock().as_ref().map(|staged| staged.vnodes().clone())
}

#[cfg(feature = "cluster")]
struct VnodeTransitionHarness {
    graph: OperatorGraph,
    registry: Arc<laminar_core::state::VnodeRegistry>,
    staged: Arc<parking_lot::Mutex<std::collections::HashMap<u32, crate::db::RehydratedVnode>>>,
}

#[cfg(feature = "cluster")]
async fn vnode_transition_harness(
    vnode_count: u32,
    restoring: &[u32],
    chains: Vec<(u32, Vec<bytes::Bytes>)>,
) -> VnodeTransitionHarness {
    use laminar_core::state::NodeId;

    vnode_transition_harness_for_assignment(
        vec![NodeId(1); vnode_count as usize],
        restoring,
        chains,
    )
    .await
}

#[cfg(feature = "cluster")]
async fn vnode_transition_harness_for_assignment(
    owners: Vec<laminar_core::state::NodeId>,
    restoring: &[u32],
    chains: Vec<(u32, Vec<bytes::Bytes>)>,
) -> VnodeTransitionHarness {
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let self_id = NodeId(1);
    let vnode_count = u32::try_from(owners.len()).unwrap();
    let registry = Arc::new(VnodeRegistry::new_unassigned(vnode_count));
    registry.set_assignment_and_version(owners.clone().into(), 1);
    registry.mark_restoring(restoring);
    let receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
            .await
            .expect("bind test shuffle receiver"),
    );
    let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
    let process_deadline = Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
        std::time::Duration::from_secs(60),
    ));
    receiver
        .install_process_lease_deadline(Arc::clone(&process_deadline))
        .unwrap();
    sender
        .install_process_lease_deadline(process_deadline)
        .unwrap();
    let owner_ids: Vec<u64> = owners.iter().map(|owner| owner.0).collect();
    let participant_ids = owners
        .iter()
        .map(|owner| owner.0)
        .filter(|owner| *owner != 0)
        .collect::<std::collections::BTreeSet<_>>();
    let participants = participant_ids
        .into_iter()
        .map(|node_id| laminar_core::checkpoint::CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        })
        .collect();
    let fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &owner_ids,
        participants,
    )
    .unwrap();
    receiver
        .install_assignment_fence(&fence, &owner_ids)
        .unwrap();
    sender.install_assignment_fence(&fence, &owner_ids).unwrap();

    let mut graph = test_graph();
    graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
        registry: Arc::clone(&registry),
        sender,
        receiver,
        self_id,
    });
    let staged = Arc::new(parking_lot::Mutex::new(
        chains
            .into_iter()
            .map(|(vnode, chain)| {
                (
                    vnode,
                    crate::db::RehydratedVnode {
                        attempt: laminar_core::state::CheckpointAttempt::canonical(7),
                        chain,
                    },
                )
            })
            .collect(),
    ));
    graph.set_rehydration_handle(Arc::clone(&staged));
    graph.set_rotation_execution_fence(Arc::new(tokio::sync::RwLock::new(())));
    VnodeTransitionHarness {
        graph,
        registry,
        staged,
    }
}

#[cfg(feature = "cluster")]
struct FinalOwnerExitHarness {
    graph: OperatorGraph,
    registry: Arc<laminar_core::state::VnodeRegistry>,
    sender: Arc<laminar_core::shuffle::ShuffleSender>,
    receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    revoked: TestVnodeRevokeHandle,
}

#[cfg(feature = "cluster")]
async fn final_owner_exit_harness(endpoint_incarnation: uuid::Uuid) -> FinalOwnerExitHarness {
    use laminar_core::checkpoint::{
        AssignmentDrainTransition, CheckpointAssignmentFence, CheckpointParticipant, LeaderProof,
        LeaderProofOwner,
    };
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let self_id = NodeId(1);
    let predecessor_incarnation = uuid::Uuid::from_u128(1);
    let target_incarnation = uuid::Uuid::from_u128(2);
    let predecessor_owners = [self_id];
    let target_owners = [NodeId(2)];
    let predecessor_participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: predecessor_incarnation,
    };
    let predecessor =
        CheckpointAssignmentFence::from_owner_map(1, &[self_id.0], vec![predecessor_participant])
            .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        2,
        &[2],
        vec![CheckpointParticipant {
            node_id: 2,
            boot_incarnation: target_incarnation,
        }],
    )
    .unwrap();
    let transition = AssignmentDrainTransition::new(
        predecessor.clone(),
        target,
        LeaderProof {
            owner: LeaderProofOwner {
                node_id: 2,
                boot_id: target_incarnation,
                process_term: 1,
            },
            fencing_token: 1,
        },
    )
    .unwrap();
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    registry.set_assignment_and_version(target_owners.to_vec().into(), 2);
    let receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), endpoint_incarnation)
            .await
            .expect("bind final-owner-exit test receiver"),
    );
    let sender = Arc::new(ShuffleSender::new(1, endpoint_incarnation));
    let process_deadline = Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
        std::time::Duration::from_secs(60),
    ));
    receiver
        .install_process_lease_deadline(Arc::clone(&process_deadline))
        .unwrap();
    sender
        .install_process_lease_deadline(process_deadline)
        .unwrap();
    if endpoint_incarnation == predecessor_incarnation {
        receiver
            .install_assignment_fence(&predecessor, &[self_id.0])
            .unwrap();
        sender
            .install_assignment_fence(&predecessor, &[self_id.0])
            .unwrap();
        receiver.invalidate_assignment_fence();
        sender.invalidate_assignment_fence();
    }

    let revoked = Arc::new(parking_lot::Mutex::new(Some(
        crate::db::StagedVnodeRevocation::committed_final_owner_exit_for_test(
            transition,
            1,
            &predecessor_owners,
            2,
            &target_owners,
            predecessor_participant,
        )
        .unwrap(),
    )));
    let mut graph = test_graph();
    graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
        registry: Arc::clone(&registry),
        sender: Arc::clone(&sender),
        receiver: Arc::clone(&receiver),
        self_id,
    });
    graph.set_vnode_revocation_handle(Arc::clone(&revoked));
    graph.set_rehydration_handle(Arc::new(parking_lot::Mutex::new(
        std::collections::HashMap::new(),
    )));
    graph.set_rotation_execution_fence(Arc::new(tokio::sync::RwLock::new(())));
    FinalOwnerExitHarness {
        graph,
        registry,
        sender,
        receiver,
        revoked,
    }
}

#[cfg(feature = "cluster")]
struct AlignmentHarness {
    graph: OperatorGraph,
    local_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    remote_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    remote_sender: laminar_core::shuffle::ShuffleSender,
    fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    recorded: Arc<parking_lot::Mutex<Vec<RetainedBatch>>>,
}

#[cfg(feature = "cluster")]
async fn alignment_harness() -> AlignmentHarness {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let registry = Arc::new(VnodeRegistry::new(2));
    registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
    let assignment_version = registry.assignment_version();
    let fence = CheckpointAssignmentFence::from_owner_map(
        assignment_version,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ],
    )
    .unwrap();
    let local_receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
            .await
            .unwrap(),
    );
    let remote_receiver = Arc::new(
        ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
            .await
            .unwrap(),
    );
    let local_sender = ShuffleSender::new(1, uuid::Uuid::from_u128(1));
    local_sender.register_peer(2, remote_receiver.local_addr());
    let remote_sender = ShuffleSender::new(2, uuid::Uuid::from_u128(2));
    remote_sender.register_peer(1, local_receiver.local_addr());
    let local_process_deadline = Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
        std::time::Duration::from_secs(60),
    ));
    local_receiver
        .install_process_lease_deadline(Arc::clone(&local_process_deadline))
        .unwrap();
    local_sender
        .install_process_lease_deadline(local_process_deadline)
        .unwrap();
    let remote_process_deadline = Arc::new(
        laminar_core::cluster::control::LeaseDeadline::live_for(std::time::Duration::from_secs(60)),
    );
    remote_receiver
        .install_process_lease_deadline(Arc::clone(&remote_process_deadline))
        .unwrap();
    remote_sender
        .install_process_lease_deadline(remote_process_deadline)
        .unwrap();
    local_receiver
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();
    remote_receiver
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();
    local_sender
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();
    remote_sender
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();

    let recorded = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.push_test_node("out", Box::new(RecordingOperator(Arc::clone(&recorded))));
    graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
        registry,
        sender: Arc::new(local_sender),
        receiver: Arc::clone(&local_receiver),
        self_id: NodeId(1),
    });
    AlignmentHarness {
        graph,
        local_receiver,
        remote_receiver,
        remote_sender,
        fence,
        recorded,
    }
}

#[cfg(feature = "cluster")]
struct ThreeNodeAlignmentHarness {
    graph: OperatorGraph,
    local_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    _peer_two_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    waiting_peer_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    peer_two_sender: laminar_core::shuffle::ShuffleSender,
    peer_three_sender: laminar_core::shuffle::ShuffleSender,
    fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    recorded: Arc<parking_lot::Mutex<Vec<RetainedBatch>>>,
}

#[cfg(feature = "cluster")]
async fn three_node_alignment_harness() -> ThreeNodeAlignmentHarness {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let registry = Arc::new(VnodeRegistry::new(3));
    registry.set_assignment(vec![NodeId(1), NodeId(2), NodeId(3)].into());
    let fence = CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &[1, 2, 3],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
            CheckpointParticipant {
                node_id: 3,
                boot_incarnation: uuid::Uuid::from_u128(3),
            },
        ],
    )
    .unwrap();
    let local_receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
            .await
            .unwrap(),
    );
    let peer_two_receiver = Arc::new(
        ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
            .await
            .unwrap(),
    );
    let waiting_peer_receiver = Arc::new(
        ShuffleReceiver::bind(3, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(3))
            .await
            .unwrap(),
    );
    let local_process_deadline = Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
        std::time::Duration::from_secs(60),
    ));
    local_receiver
        .install_process_lease_deadline(Arc::clone(&local_process_deadline))
        .unwrap();
    let peer_two_process_deadline = Arc::new(
        laminar_core::cluster::control::LeaseDeadline::live_for(std::time::Duration::from_secs(60)),
    );
    peer_two_receiver
        .install_process_lease_deadline(Arc::clone(&peer_two_process_deadline))
        .unwrap();
    let peer_three_process_deadline = Arc::new(
        laminar_core::cluster::control::LeaseDeadline::live_for(std::time::Duration::from_secs(60)),
    );
    waiting_peer_receiver
        .install_process_lease_deadline(Arc::clone(&peer_three_process_deadline))
        .unwrap();
    for receiver in [&local_receiver, &peer_two_receiver, &waiting_peer_receiver] {
        receiver
            .install_assignment_fence(&fence, &[1, 2, 3])
            .unwrap();
    }

    let local_sender = ShuffleSender::new(1, uuid::Uuid::from_u128(1));
    local_sender.register_peer(2, peer_two_receiver.local_addr());
    local_sender.register_peer(3, waiting_peer_receiver.local_addr());
    local_sender
        .install_process_lease_deadline(local_process_deadline)
        .unwrap();
    local_sender
        .install_assignment_fence(&fence, &[1, 2, 3])
        .unwrap();
    let peer_two_sender = ShuffleSender::new(2, uuid::Uuid::from_u128(2));
    peer_two_sender.register_peer(1, local_receiver.local_addr());
    peer_two_sender.register_peer(3, waiting_peer_receiver.local_addr());
    peer_two_sender
        .install_process_lease_deadline(peer_two_process_deadline)
        .unwrap();
    peer_two_sender
        .install_assignment_fence(&fence, &[1, 2, 3])
        .unwrap();
    let peer_three_sender = ShuffleSender::new(3, uuid::Uuid::from_u128(3));
    peer_three_sender.register_peer(1, local_receiver.local_addr());
    peer_three_sender.register_peer(2, peer_two_receiver.local_addr());
    peer_three_sender
        .install_process_lease_deadline(peer_three_process_deadline)
        .unwrap();
    peer_three_sender
        .install_assignment_fence(&fence, &[1, 2, 3])
        .unwrap();

    let recorded = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.push_test_node("out", Box::new(RecordingOperator(Arc::clone(&recorded))));
    graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
        registry,
        sender: Arc::new(local_sender),
        receiver: Arc::clone(&local_receiver),
        self_id: NodeId(1),
    });

    ThreeNodeAlignmentHarness {
        graph,
        local_receiver,
        _peer_two_receiver: peer_two_receiver,
        waiting_peer_receiver,
        peer_two_sender,
        peer_three_sender,
        fence,
        recorded,
    }
}

#[cfg(feature = "cluster")]
async fn stage_peer_two_data_and_barrier(
    harness: &ThreeNodeAlignmentHarness,
    attempt: laminar_core::state::CheckpointAttempt,
) -> RecordBatch {
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::shuffle::ShuffleMessage;

    let batch = test_batch();
    harness
        .peer_two_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("out".into(), 0, batch.clone()),
        )
        .await
        .unwrap();
    harness
        .peer_two_sender
        .fan_out_barrier(
            &[1, 3],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let _ = harness
            .local_receiver
            .drain_checkpointed_data_for("__alignment_probe");
        let barriers = harness.local_receiver.drain_staged_barriers();
        if !barriers.is_empty() {
            for barrier in barriers {
                harness.local_receiver.stash_barrier(barrier);
            }
            return batch;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "peer data and barrier did not reach the holdover"
        );
        tokio::task::yield_now().await;
    }
}

#[cfg(feature = "cluster")]
async fn stage_peer_two_data_barrier_data(
    harness: &ThreeNodeAlignmentHarness,
    attempt: laminar_core::state::CheckpointAttempt,
) -> (RecordBatch, RecordBatch) {
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::shuffle::ShuffleMessage;

    let before_barrier = test_batch();
    let after_barrier = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(vec!["MSFT", "NVDA"])),
            Arc::new(Float64Array::from(vec![420.0, 125.0])),
            Arc::new(Int64Array::from(vec![3000, 4000])),
        ],
    )
    .unwrap();
    harness
        .peer_two_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("out".into(), 0, before_barrier.clone()),
        )
        .await
        .unwrap();
    harness
        .peer_two_sender
        .fan_out_barrier(
            &[1, 3],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();
    harness
        .peer_two_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("out".into(), 0, after_barrier.clone()),
        )
        .await
        .unwrap();

    // Reproduce the normal drainer splitting a queued data/barrier/data sequence: the first
    // batch and barrier enter holdovers, while the post-barrier batch remains on the live queue.
    let stage_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let _ = harness
            .local_receiver
            .drain_checkpointed_data_for("__alignment_probe");
        let barriers = harness.local_receiver.drain_staged_barriers();
        if !barriers.is_empty() {
            for barrier in barriers {
                harness.local_receiver.stash_barrier(barrier);
            }
            break;
        }
        assert!(
            tokio::time::Instant::now() < stage_deadline,
            "remote barrier did not reach the staged holdover"
        );
        tokio::task::yield_now().await;
    }
    (before_barrier, after_barrier)
}

#[cfg(feature = "cluster")]
async fn alignment_abort_controller(
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    attempt: laminar_core::state::CheckpointAttempt,
    durable: bool,
) -> Arc<laminar_core::cluster::control::ClusterController> {
    alignment_abort_controller_with_announcement(fence, attempt, durable, true).await
}

#[cfg(feature = "cluster")]
async fn alignment_abort_controller_with_announcement(
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    attempt: laminar_core::state::CheckpointAttempt,
    durable: bool,
    announce: bool,
) -> Arc<laminar_core::cluster::control::ClusterController> {
    use laminar_core::checkpoint_decision::CheckpointVerdict;
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner,
        LeaderLeaseStore, LeaseDeadline, LeaseOutcome, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};

    let node_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(node_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let info = |id| NodeInfo {
        id: NodeId(id),
        name: format!("node-{id}"),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![info(1), info(2), info(3)]);
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node_id,
        Arc::clone(&kv_trait),
        kv_trait,
        None,
        members_rx,
        uuid::Uuid::from_u128(1),
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();
    controller.set_active(true);
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let authority = Arc::new(LeaderLeaseStore::new(backing, 1_000));
    let owner = LeaderLeaseOwner {
        node: node_id,
        boot: uuid::Uuid::from_u128(1),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty alignment authority must grant leadership");
    };
    let proof = lease.proof();
    if durable {
        authority
            .record_cluster_outcome(
                &proof,
                attempt.epoch,
                attempt.checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }
    controller.set_leader_lease_store(authority);
    if announce {
        kv.seed(
            node_id,
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&BarrierAnnouncement {
                epoch: attempt.epoch,
                checkpoint_id: attempt.checkpoint_id,
                assignment_fence: Some(fence.clone()),
                leader_proof: Some(proof),
                phase: Phase::Abort,
                flags: 0,
            })
            .unwrap(),
        );
    }
    controller
}

/// A peer ships a row + its exact-attempt barrier; alignment retains the row as channel state
/// before completing the certified distributed cut.
#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn align_shuffle_barriers_retains_peer_rows_then_aligns_exact_attempt() {
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::shuffle::ShuffleMessage;
    use laminar_core::state::CheckpointAttempt;

    let mut harness = alignment_harness().await;
    let attempt = CheckpointAttempt::new(70, 70);

    let batch = test_batch();
    harness
        .remote_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("out".into(), 0, batch.clone()),
        )
        .await
        .unwrap();
    harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();

    harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .unwrap();

    let received = harness.remote_receiver.recv().await.unwrap();
    assert_eq!(received.peer(), 1);
    assert_eq!(received.assignment_digest(), Some(harness.fence.digest()));
    assert!(matches!(
        received.message(),
        ShuffleMessage::Barrier(barrier)
            if barrier.epoch == attempt.epoch
                && barrier.checkpoint_id == attempt.checkpoint_id
    ));

    let got = harness.recorded.lock();
    assert_eq!(
        got.len(),
        1,
        "peer's pre-barrier row retained by the operator"
    );
    assert_eq!(got[0].num_rows(), batch.num_rows());
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_scope_cancellation_preserves_holdover_for_the_next_attempt() {
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::state::CheckpointAttempt;

    let mut harness = three_node_alignment_harness().await;
    let cancelled = CheckpointAttempt::new(70, 70);
    let retained = stage_peer_two_data_and_barrier(&harness, cancelled).await;
    let sender = Arc::clone(
        &harness
            .graph
            .cluster_shuffle_config()
            .expect("cluster shuffle")
            .sender,
    );
    let live_peer_three = harness.waiting_peer_receiver.local_addr();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    sender.register_peer(3, listener.local_addr().unwrap());
    let accepted = Arc::new(tokio::sync::Notify::new());
    let stalled_peer = {
        let accepted = Arc::clone(&accepted);
        tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            accepted.notify_one();
            std::future::pending::<()>().await;
        })
    };
    let outcome = {
        let alignment = harness.graph.align_shuffle_barriers(
            cancelled,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        );
        tokio::pin!(alignment);
        tokio::select! {
            () = accepted.notified() => {}
            result = &mut alignment => panic!("alignment completed before scope cancellation: {result:?}"),
        }

        sender.suspend_assignment_fence();
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut alignment)
            .await
            .expect("scope cancellation did not release barrier fan-out")
            .unwrap()
    };
    assert_eq!(
        outcome,
        ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging
    );
    assert!(
        harness.recorded.lock().is_empty(),
        "cancelled alignment staged checkpoint holdover"
    );

    harness
        .local_receiver
        .retire_checkpoint_barriers(cancelled, harness.fence.digest())
        .unwrap();
    sender.register_peer(3, live_peer_three);
    assert!(sender
        .install_assignment_fence(&harness.fence, &[1, 2, 3])
        .unwrap());
    let successor = CheckpointAttempt::new(71, 71);
    harness
        .peer_two_sender
        .fan_out_barrier(
            &[1, 3],
            CheckpointBarrier::new(successor.checkpoint_id, successor.epoch),
            &harness.fence,
        )
        .await
        .unwrap();
    harness
        .peer_three_sender
        .fan_out_barrier(
            &[1, 2],
            CheckpointBarrier::new(successor.checkpoint_id, successor.epoch),
            &harness.fence,
        )
        .await
        .unwrap();

    assert_eq!(
        harness
            .graph
            .align_shuffle_barriers(
                successor,
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                None,
            )
            .await
            .unwrap(),
        ShuffleAlignmentOutcome::Aligned
    );
    let recorded = harness.recorded.lock();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].num_rows(), retained.num_rows());
    stalled_peer.abort();
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receiver_scope_suspension_preserves_holdover_before_graph_staging() {
    use laminar_core::state::CheckpointAttempt;

    let mut harness = three_node_alignment_harness().await;
    let cancelled = CheckpointAttempt::new(70, 70);
    let retained = stage_peer_two_data_and_barrier(&harness, cancelled).await;
    harness.local_receiver.suspend_assignment_fence();

    let outcome = harness
        .graph
        .align_shuffle_barriers(
            cancelled,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging
    );
    assert!(harness.recorded.lock().is_empty());

    assert!(harness
        .local_receiver
        .install_assignment_fence(&harness.fence, &[1, 2, 3])
        .unwrap());
    harness
        .local_receiver
        .retire_checkpoint_barriers(cancelled, harness.fence.digest())
        .unwrap();
    let preserved = harness
        .local_receiver
        .drain_checkpointed_holdover()
        .unwrap();
    assert_eq!(preserved.len(), 1);
    assert_eq!(preserved[0].0, "out");
    assert_eq!(preserved[0].1.batch().num_rows(), retained.num_rows());
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_rejects_staged_data_barrier_data_sequence() {
    use laminar_core::state::CheckpointAttempt;

    let mut harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(70, 70);
    let (before_barrier, _after_barrier) =
        stage_peer_two_data_barrier_data(&harness, attempt).await;

    let error = harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .expect_err("data behind an observed peer barrier must fail the checkpoint");
    assert!(error.to_string().contains("checkpoint barrier"), "{error}");
    assert!(
        error.requires_pipeline_recovery(),
        "destructive alignment failure must rewind the pipeline"
    );
    let retained = harness.recorded.lock();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].num_rows(), before_barrier.num_rows());
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_retains_resumed_peer_data_on_durable_abort() {
    use laminar_core::state::CheckpointAttempt;

    let mut harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(80, 80);
    let (before_barrier, after_barrier) = stage_peer_two_data_barrier_data(&harness, attempt).await;
    let controller = alignment_abort_controller(&harness.fence, attempt, true).await;

    let outcome = harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            Some(controller.as_ref()),
        )
        .await
        .expect("an exact durable Abort must end pre-capture alignment cleanly");

    assert_eq!(outcome, ShuffleAlignmentOutcome::Aborted);
    let mut retained: Vec<_> = harness
        .recorded
        .lock()
        .iter()
        .map(|batch| batch.batch().clone())
        .collect();
    assert!(
        matches!(retained.len(), 1 | 2),
        "the pre-barrier batch must be staged before Abort"
    );
    if retained.len() == 1 {
        let receiver_owned = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let batches = harness.local_receiver.drain_checkpointed_data_for("out");
                if !batches.is_empty() {
                    break batches;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("post-barrier batch remained in flight after Abort");
        retained.extend(
            receiver_owned
                .into_iter()
                .map(|batch| batch.batch().clone()),
        );
    }
    assert_eq!(
        retained.len(),
        2,
        "the graph and receiver must jointly own each batch exactly once after Abort"
    );
    assert_eq!(retained[0], before_barrier);
    assert_eq!(retained[1], after_barrier);
    assert!(
        harness
            .local_receiver
            .drain_checkpointed_data_for("out")
            .is_empty(),
        "post-barrier batch was duplicated in receiver ownership"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_audits_durable_abort_when_announcement_is_lost() {
    use laminar_core::state::CheckpointAttempt;

    let mut harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(90, 90);
    let retained = stage_peer_two_data_and_barrier(&harness, attempt).await;
    let controller =
        alignment_abort_controller_with_announcement(&harness.fence, attempt, true, false).await;

    let outcome = harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            Some(controller.as_ref()),
        )
        .await
        .expect("the periodic authority audit must observe an Abort without gossip");

    assert_eq!(outcome, ShuffleAlignmentOutcome::Aborted);
    let recorded = harness.recorded.lock();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].num_rows(), retained.num_rows());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_alignment_does_not_trust_abort_hint_without_durable_outcome() {
    use laminar_core::state::CheckpointAttempt;

    let harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(90, 90);
    let controller = alignment_abort_controller(&harness.fence, attempt, false).await;

    let hint = OperatorGraph::wait_for_shuffle_alignment_terminal_hint(
        Some(controller.as_ref()),
        attempt,
        None,
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    )
    .await
    .unwrap()
    .expect("Abort announcement must wake alignment");
    assert_eq!(hint.epoch, attempt.epoch);
    assert_eq!(hint.checkpoint_id, attempt.checkpoint_id);
    assert_eq!(hint.phase, laminar_core::cluster::control::Phase::Abort);
    assert_eq!(
        OperatorGraph::audit_shuffle_alignment_settlement(
            Some(controller.as_ref()),
            attempt,
            &harness.fence,
        )
        .await
        .unwrap(),
        None
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_alignment_rejects_abort_with_a_different_assignment_certificate() {
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::state::CheckpointAttempt;

    let harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(90, 90);
    let other_fence = CheckpointAssignmentFence::from_owner_map(
        harness.fence.assignment_version,
        &[1, 3, 2],
        harness.fence.participants.clone(),
    )
    .unwrap();
    let controller = alignment_abort_controller(&other_fence, attempt, true).await;

    let error = OperatorGraph::audit_shuffle_alignment_settlement(
        Some(controller.as_ref()),
        attempt,
        &harness.fence,
    )
    .await
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("different assignment certificate"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_sender_rejects_wrong_epoch_for_same_checkpoint_id() {
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::state::CheckpointAttempt;

    let harness = alignment_harness().await;
    let expected = CheckpointAttempt::new(70, 70);
    let error = harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(expected.checkpoint_id, 8),
            &harness.fence,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        error.to_string().contains("canonical checkpoint ID"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[test]
fn shuffle_attempt_comparison_rejects_all_conflicting_orders() {
    use laminar_core::state::CheckpointAttempt;

    let expected = CheckpointAttempt::new(70, 70);
    for observed in [
        CheckpointAttempt::new(69, 71),
        CheckpointAttempt::new(71, 69),
        CheckpointAttempt::new(70, 69),
        CheckpointAttempt::new(70, 71),
        CheckpointAttempt::new(69, 70),
        CheckpointAttempt::new(71, 70),
    ] {
        assert!(
            OperatorGraph::compare_shuffle_attempts(expected, observed).is_err(),
            "mixed attempt order must fail: {observed:?}"
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_alignment_rejects_newer_durable_terminal_without_announcement() {
    use laminar_core::state::CheckpointAttempt;

    let attempt = CheckpointAttempt::new(70, 70);
    let newer = CheckpointAttempt::new(71, 71);
    let harness = three_node_alignment_harness().await;
    let controller =
        alignment_abort_controller_with_announcement(&harness.fence, newer, true, false).await;
    let error = OperatorGraph::audit_shuffle_alignment_settlement(
        Some(controller.as_ref()),
        attempt,
        &harness.fence,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("superseded by durable terminal"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_rejects_wrong_assignment_digest() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier};
    use laminar_core::state::CheckpointAttempt;

    let harness = alignment_harness().await;
    let wrong_fence = CheckpointAssignmentFence::from_owner_map(
        harness.fence.assignment_version,
        &[2, 1],
        harness.fence.participants.clone(),
    )
    .unwrap();
    let attempt = CheckpointAttempt::new(70, 70);
    let error = harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &wrong_fence,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("assignment roster"), "{error}");
    assert!(harness.recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_rejects_changed_local_assignment_scope() {
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::state::CheckpointAttempt;

    let mut harness = alignment_harness().await;
    let next = CheckpointAssignmentFence::from_owner_map(
        harness.fence.assignment_version + 1,
        &[1, 2],
        harness.fence.participants.clone(),
    )
    .unwrap();
    let cfg = harness.graph.cluster_shuffle_config().unwrap();
    cfg.sender.install_assignment_fence(&next, &[1, 2]).unwrap();
    cfg.receiver
        .install_assignment_fence(&next, &[1, 2])
        .unwrap();
    let error = harness
        .graph
        .align_shuffle_barriers(
            CheckpointAttempt::new(70, 70),
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("assignment differs"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_transition_discards_staged_pre_recovery_barrier() {
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::state::CheckpointAttempt;

    let harness = alignment_harness().await;
    let attempt = CheckpointAttempt::new(70, 70);
    harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();
    let old = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        harness.local_receiver.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    harness.local_receiver.stash_barrier(old);
    harness.local_receiver.set_recovery_gen(1);
    harness.remote_receiver.set_recovery_gen(1);
    harness
        .graph
        .cluster_shuffle_config()
        .unwrap()
        .sender
        .set_recovery_gen(1);

    assert!(harness.local_receiver.drain_staged_barriers().is_empty());
    assert!(harness.remote_receiver.drain_staged_barriers().is_empty());
    assert!(harness.recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_fails_closed_on_unknown_stage() {
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::shuffle::ShuffleMessage;
    use laminar_core::state::CheckpointAttempt;

    let mut harness = alignment_harness().await;
    let attempt = CheckpointAttempt::new(70, 70);
    harness
        .remote_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("missing".into(), 0, test_batch()),
        )
        .await
        .unwrap();
    harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();
    let error = harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("unknown or removed stage"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_uses_supplied_absolute_deadline() {
    use laminar_core::state::CheckpointAttempt;

    let mut harness = alignment_harness().await;
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        harness.graph.align_shuffle_barriers(
            CheckpointAttempt::new(70, 70),
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_millis(30),
            None,
        ),
    )
    .await
    .expect("alignment ignored its supplied deadline")
    .unwrap_err();
    assert!(error.to_string().contains("absolute deadline"), "{error}");
}

#[test]
fn test_source_passthrough() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut op = SourcePassthrough;
        let batch = test_batch();
        let result = op.process(&[vec![batch.clone()]], &[0]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    });
}

#[test]
fn test_graph_construction() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "q1".to_string(),
        "SELECT symbol, price FROM trades WHERE price > 100".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    assert_eq!(graph.nodes.len(), 2); // source "trades" + query "q1"
    assert_eq!(graph.edges.len(), 1); // trades → q1
    assert!(graph.source_map.contains_key("trades"));
    assert!(graph.output_map.contains_key("q1"));
}

#[test]
fn test_cascading_queries() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "q1".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q2".to_string(),
        "SELECT symbol FROM q1 WHERE price > 100".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // source "trades" + query "q1" + query "q2" = 3 nodes
    assert_eq!(graph.nodes.len(), 3);
    // trades → q1, q1 → q2 = 2 edges
    assert_eq!(graph.edges.len(), 2);
    assert!(graph.depends_on_stream.contains(&2)); // q2 depends on q1
}

#[test]
fn test_topo_order() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    // Add in reverse dependency order
    graph.add_query(
        "q2".to_string(),
        "SELECT * FROM q1".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    graph.compute_topo_order();

    // Find positions in topo order
    let q1_pos = graph
        .topo_order
        .iter()
        .position(|&id| &*graph.nodes[id].name == "q1");
    let q2_pos = graph
        .topo_order
        .iter()
        .position(|&id| &*graph.nodes[id].name == "q2");

    // q1 should appear before q2 (but note: q2 was added first and created
    // a source node "q1" which gets the first edge; the real q1 query node
    // doesn't have that edge. This test mainly verifies no panics.)
    assert!(q1_pos.is_some());
    assert!(q2_pos.is_some());
}

#[test]
fn test_remove_query() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        true,
    );
    assert!(graph.output_map.contains_key("q1"));
    let original_node = graph.output_map["q1"];
    graph.ensure_live_provider("q1", &test_schema());
    let temporal_config = TemporalJoinTranslatorConfig {
        stream_table: "trades".to_string(),
        table_name: "versions".to_string(),
        stream_key_column: "symbol".to_string(),
        table_key_column: "symbol".to_string(),
        stream_time_column: "ts".to_string(),
        table_version_column: "valid_from".to_string(),
        semantics: "event_time".to_string(),
        join_type: "inner".to_string(),
    };
    graph
        .temporal_configs
        .push(("q1".to_string(), temporal_config.clone()));

    graph.remove_query("q1");
    assert!(!graph.output_map.contains_key("q1"));
    assert!(graph.nodes[1].removed); // node 0 = source, node 1 = q1
    assert!(!graph.incremental_tables.contains("q1"));
    assert!(graph.temporal_configs.is_empty());
    assert!(!graph.live_handles.contains_key("q1"));

    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let replacement_node = graph.output_map["q1"];
    assert_eq!(replacement_node, original_node);
    assert_eq!(
        graph
            .nodes
            .iter()
            .filter(|node| !node.removed && &*node.name == "q1")
            .count(),
        1
    );
    assert!(!graph.incremental_tables.contains("q1"));
    graph.ensure_live_provider("q1", &test_schema());
    assert!(graph.live_handles.contains_key("q1"));

    graph.incremental_tables.insert("metadata_only".to_string());
    graph
        .temporal_configs
        .push(("metadata_only".to_string(), temporal_config));
    graph.ensure_live_provider("metadata_only", &test_schema());
    assert!(!graph.output_map.contains_key("metadata_only"));
    graph.remove_query("metadata_only");
    assert!(!graph.incremental_tables.contains("metadata_only"));
    assert!(graph
        .temporal_configs
        .iter()
        .all(|(query_name, _)| query_name != "metadata_only"));
    assert!(!graph.live_handles.contains_key("metadata_only"));
}

#[test]
fn rejected_control_add_removes_all_query_artifacts() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);
    graph
        .build_errors
        .push(DbError::Pipeline("forced admission rejection".into()));

    let mutation = Arc::new(crate::pipeline::ControlMutation::new());
    let (reply, mut result) = tokio::sync::oneshot::channel();
    let message = crate::pipeline::ControlMsg::add_stream(
        "rejected".to_string(),
        "SELECT * FROM events".to_string(),
        None,
        None,
        None,
        None,
        true,
        reply,
        Arc::clone(&mutation),
    );
    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);
    let error = result
        .try_recv()
        .expect("control result must be sent synchronously")
        .unwrap_err();

    assert_eq!(
        mutation.state(),
        crate::pipeline::ControlMutationState::Cancelled
    );
    assert!(matches!(error, DbError::Pipeline(_)));
    assert!(!graph.output_map.contains_key("rejected"));
    assert!(!graph.incremental_tables.contains("rejected"));
    assert!(!graph.live_handles.contains_key("rejected"));
    assert!(graph
        .temporal_configs
        .iter()
        .all(|(query_name, _)| query_name != "rejected"));
    let rejected_nodes: FxHashSet<_> = graph
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| &*node.name == "rejected")
        .map(|(id, node)| {
            assert!(node.removed);
            id
        })
        .collect();
    assert!(!rejected_nodes.is_empty());
    assert!(graph.edges.iter().all(
        |edge| !rejected_nodes.contains(&edge.source) && !rejected_nodes.contains(&edge.target)
    ));
    assert!(graph.nodes.iter().all(|node| node
        .output_routes
        .iter()
        .all(|(target, _)| !rejected_nodes.contains(target))));
}

#[test]
fn repeated_live_control_create_drop_reuses_graph_slots() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    for _ in 0..128 {
        let create_mutation = Arc::new(crate::pipeline::ControlMutation::new());
        let (create_reply, mut create_result) = tokio::sync::oneshot::channel();
        crate::pipeline_callback::apply_control_to_graph(
            &mut graph,
            crate::pipeline::ControlMsg::add_stream(
                "churn".to_string(),
                "SELECT * FROM events".to_string(),
                None,
                None,
                None,
                None,
                false,
                create_reply,
                Arc::clone(&create_mutation),
            ),
        );
        create_result
            .try_recv()
            .expect("CREATE acknowledgement must be synchronous")
            .unwrap();
        assert_eq!(
            create_mutation.state(),
            crate::pipeline::ControlMutationState::Applied
        );

        let drop_mutation = Arc::new(crate::pipeline::ControlMutation::new());
        let (drop_reply, mut drop_result) = tokio::sync::oneshot::channel();
        crate::pipeline_callback::apply_control_to_graph(
            &mut graph,
            crate::pipeline::ControlMsg::drop_streams(
                vec!["churn".to_string()],
                drop_reply,
                Arc::clone(&drop_mutation),
            ),
        );
        drop_result
            .try_recv()
            .expect("DROP acknowledgement must be synchronous")
            .unwrap();
        assert_eq!(
            drop_mutation.state(),
            crate::pipeline::ControlMutationState::Applied
        );
    }

    assert_eq!(
        graph.nodes.len(),
        2,
        "one source slot plus one reusable query slot"
    );
    assert_eq!(graph.free_node_ids.len(), 1);
    assert!(graph.edges.is_empty());
}

#[tokio::test]
async fn test_execute_cycle_basic() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "filtered".to_string(),
        "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let batch = test_batch();
    let mut source_batches = FxHashMap::default();
    source_batches.insert(Arc::from("trades"), vec![batch]);

    let results = graph
        .execute_cycle(&source_batches, i64::MAX, None)
        .await
        .unwrap();
    assert!(results.contains_key("filtered"));
    let filtered = &results[&Arc::from("filtered") as &Arc<str>];
    // Only GOOG (price=2800) passes the filter
    let total_rows: usize = filtered.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

// --- AI routing ---

struct PosProvider;

#[async_trait]
impl crate::ai::InferenceProvider for PosProvider {
    async fn infer_batch(
        &self,
        request: crate::ai::InferenceRequest,
    ) -> Result<crate::ai::InferenceResponse, crate::ai::ProviderError> {
        Ok(crate::ai::InferenceResponse {
            outputs: crate::ai::InferenceOutputs::Text(vec![
                "pos".to_string();
                request.inputs.len()
            ]),
            usage: crate::ai::Usage::ZERO,
        })
    }
    fn name(&self) -> &'static str {
        "pos"
    }
}

fn stub_ai_runtime() -> Arc<crate::ai::AiRuntime> {
    use crate::ai::{ModelBackend, ModelEntry, ModelRegistry, Task};
    let mut registry = ModelRegistry::new();
    registry
        .register(ModelEntry {
            id: "m".into(),
            tasks: vec![Task::Classify],
            backend: ModelBackend::Remote {
                provider: "p".into(),
                model: "stub-model".into(),
            },
        })
        .unwrap();
    let providers = [(
        "p".to_string(),
        Arc::new(PosProvider) as Arc<dyn crate::ai::InferenceProvider>,
    )];
    Arc::new(crate::ai::AiRuntime::new(
        registry,
        providers,
        None,
        Arc::new(crate::ai::AiResultCache::with_defaults()),
        Arc::new(crate::ai::AiCallLog::with_defaults()),
    ))
}

fn docs_batch() -> RecordBatch {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["great quarter"])),
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_routing_enriches_rows() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);
    graph.set_ai_runtime(stub_ai_runtime(), tokio::runtime::Handle::current());
    graph.register_source_schema("docs".to_string(), docs_batch().schema());

    graph.add_query(
        "labeled".to_string(),
        "SELECT id, ai_classify(text, model => 'm', labels => ARRAY['pos','neg']) AS label \
         FROM docs"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph
        .take_build_errors()
        .expect("AI query should route cleanly");

    // Cycle 1: the row misses the cache and is handed to the worker.
    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("docs"), vec![docs_batch()]);
    let _ = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();

    // Let the off-thread worker finish, then drain on a later cycle.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let empty = FxHashMap::default();
    let results = graph.execute_cycle(&empty, i64::MAX, None).await.unwrap();

    let out = &results[&(Arc::from("labeled") as Arc<str>)];
    let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "the enriched row should be emitted");
    // Output schema is the residual projection: (id, label).
    let batch = out.iter().find(|b| b.num_rows() > 0).unwrap();
    let label = batch
        .column(batch.schema().index_of("label").unwrap())
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(label.value(0), "pos");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_routing_unknown_model_fails_at_build() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);
    graph.set_ai_runtime(stub_ai_runtime(), tokio::runtime::Handle::current());

    graph.add_query(
        "bad".to_string(),
        "SELECT ai_classify(text, model => 'ghost', labels => ARRAY['a']) AS label FROM docs"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    assert!(
        graph.take_build_errors().is_err(),
        "unknown model must fail"
    );
}

/// End-to-end through the real graph: `ai_sentiment` lifts to the AI
/// operator, the worker scores on Ring 1, and the emitted column is a
/// numeric `Float64`, not a label.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_sentiment_emits_a_double_score() {
    use crate::ai::{
        AiCallLog, AiResultCache, AiRuntime, InferenceOutputs, InferenceProvider, InferenceRequest,
        InferenceResponse, ModelBackend, ModelEntry, ModelRegistry, ProviderError, Task, Usage,
    };

    struct ScoreProvider;
    #[async_trait::async_trait]
    impl InferenceProvider for ScoreProvider {
        async fn infer_batch(
            &self,
            req: InferenceRequest,
        ) -> Result<InferenceResponse, ProviderError> {
            // A compliant sentiment model replies with a bare number.
            Ok(InferenceResponse {
                outputs: InferenceOutputs::Text(vec!["0.8".to_string(); req.inputs.len()]),
                usage: Usage::ZERO,
            })
        }
        fn name(&self) -> &'static str {
            "score"
        }
    }

    let mut registry = ModelRegistry::new();
    registry
        .register(ModelEntry {
            id: "m".into(),
            tasks: vec![Task::Sentiment],
            backend: ModelBackend::Remote {
                provider: "p".into(),
                model: "stub".into(),
            },
        })
        .unwrap();
    let call_log = Arc::new(AiCallLog::with_defaults());
    let runtime = Arc::new(AiRuntime::new(
        registry,
        [(
            "p".to_string(),
            Arc::new(ScoreProvider) as Arc<dyn InferenceProvider>,
        )],
        None,
        Arc::new(AiResultCache::with_defaults()),
        Arc::clone(&call_log),
    ));

    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);
    graph.set_ai_runtime(runtime, tokio::runtime::Handle::current());
    graph.register_source_schema("docs".to_string(), docs_batch().schema());

    graph.add_query(
        "scored".to_string(),
        "SELECT id, ai_sentiment(text, model => 'm') AS sentiment FROM docs".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph
        .take_build_errors()
        .expect("ai_sentiment should route cleanly");

    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("docs"), vec![docs_batch()]);
    let _ = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let results = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .unwrap();

    let out = &results[&(Arc::from("scored") as Arc<str>)];
    let batch = out.iter().find(|b| b.num_rows() > 0).expect("a scored row");
    let col = batch.column(batch.schema().index_of("sentiment").unwrap());
    let scores = col
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("sentiment is a Float64 score, not a label");
    assert!((scores.value(0) - 0.8).abs() < 1e-9);
    assert_eq!(
        call_log.total_recorded(),
        1,
        "the call is in laminar.ai_calls"
    );
}

#[tokio::test]
async fn test_execute_cycle_empty_source() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    // Register schema so the graph can create empty placeholder tables
    graph.register_source_schema("trades".to_string(), test_schema());

    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let source_batches = FxHashMap::default();
    let results = graph
        .execute_cycle(&source_batches, i64::MAX, None)
        .await
        .unwrap();
    // No source data → empty results (or no entry)
    let total: usize = results
        .get("q1")
        .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
    assert_eq!(total, 0);
}

#[tokio::test]
async fn test_fan_out() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "q1".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q2".to_string(),
        "SELECT symbol FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let batch = test_batch();
    let mut source_batches = FxHashMap::default();
    source_batches.insert(Arc::from("trades"), vec![batch]);

    let results = graph
        .execute_cycle(&source_batches, i64::MAX, None)
        .await
        .unwrap();
    assert!(results.contains_key("q1"));
    assert!(results.contains_key("q2"));
}

#[test]
fn test_checkpoint_empty() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    // No state yet → None
    let cp = graph.snapshot_state().unwrap();
    assert!(cp.is_none());
}

#[tokio::test]
async fn test_temporal_filter_checkpoint_restore_through_graph() {
    use laminar_sql::parser::EmitClause;
    // test_batch(): ts is Int64 epoch-ms — AAPL@1000, GOOG@2000.
    let sql = "SELECT * FROM trades WHERE ts > now() - INTERVAL '10' SECOND";
    let mut g1 = test_graph();
    g1.add_query(
        "recent".into(),
        sql.into(),
        Some(EmitClause::Changes),
        None,
        None,
        None,
        false,
    );
    let mut src = FxHashMap::default();
    src.insert(Arc::from("trades"), vec![test_batch()]);
    // Frontier 5000ms: both rows are members (exit 11000/12000) ⇒ +1,+1.
    let r = g1.execute_cycle(&src, 5_000, None).await.unwrap();
    assert_eq!(total_rows(&r, "recent"), 2);

    // Snapshot + restore through the real GraphCheckpoint/rkyv path.
    let cp = g1.snapshot_state().unwrap().expect("buffered state");
    let bytes = OperatorGraph::serialize_checkpoint_bounded(&cp, u64::MAX).unwrap();
    let mut g2 = test_graph();
    g2.add_query(
        "recent".into(),
        sql.into(),
        Some(EmitClause::Changes),
        None,
        None,
        None,
        false,
    );
    let (restored_graph, restored) = g2.restore_from_bytes(&bytes).unwrap();
    let mut g2 = restored_graph;
    assert_eq!(restored, 1);

    // Advancing to 11000ms ages out AAPL@1000 (exit 11000, strict `>`)
    // but not GOOG@2000 (exit 12000): exactly one -1, nothing lost.
    let empty = FxHashMap::default();
    let r = g2.execute_cycle(&empty, 11_000, None).await.unwrap();
    let batches = r.get("recent").expect("recent output");
    let mut wts = Vec::new();
    for b in batches {
        let w = b
            .column(b.schema().index_of("__weight").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ts = b
            .column(b.schema().index_of("ts").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            wts.push((w.value(i), ts.value(i)));
        }
    }
    assert_eq!(
        wts,
        vec![(-1, 1000)],
        "only AAPL@1000 ages out post-restore"
    );

    // Re-advancing to the same frontier must not double-retract.
    let r = g2.execute_cycle(&empty, 11_000, None).await.unwrap();
    assert_eq!(total_rows(&r, "recent"), 0);
}

struct DelayOperator;

#[async_trait]
impl GraphOperator for DelayOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}

struct SignalThenPendingOperator {
    entered: Option<tokio::sync::oneshot::Sender<(usize, Option<f64>)>>,
}

fn asof_probe_observation(inputs: &[Vec<RecordBatch>]) -> (usize, Option<f64>) {
    let batches = inputs.iter().flat_map(|port| port.iter());
    let rows = batches.clone().map(RecordBatch::num_rows).sum();
    let bid = batches
        .filter_map(|batch| batch.column_by_name("bid"))
        .filter_map(|column| column.as_any().downcast_ref::<Float64Array>())
        .find_map(|column| (!column.is_empty()).then(|| column.value(0)));
    (rows, bid)
}

#[async_trait]
impl GraphOperator for SignalThenPendingOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(asof_probe_observation(inputs));
        }
        std::future::pending().await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

struct PanicAfterInputOperator(Arc<parking_lot::Mutex<Option<(usize, Option<f64>)>>>);

#[async_trait]
impl GraphOperator for PanicAfterInputOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        *self.0.lock() = Some(asof_probe_observation(inputs));
        panic!("injected panic after stateful upstream output");
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

/// Helper: total row count from result batches.
fn total_rows(results: &FxHashMap<Arc<str>, Vec<RecordBatch>>, key: &str) -> usize {
    results
        .get(key)
        .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum())
}

/// Creates a graph with streaming functions registered and generous budget.
fn test_graph() -> OperatorGraph {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);
    // Debug builds are slow — use a generous budget for tests.
    graph.set_query_budget_ns(5_000_000_000); // 5 seconds
    graph
}

fn asof_execution_test_graph() -> OperatorGraph {
    let mut graph = test_graph();
    let config = laminar_sql::translator::AsofJoinTranslatorConfig {
        left_table: "trades".to_string(),
        right_table: "quotes".to_string(),
        key_column: "symbol".to_string(),
        left_time_column: "trade_ts".to_string(),
        right_time_column: "quote_ts".to_string(),
        direction: laminar_sql::parser::join_parser::AsofSqlDirection::Backward,
        tolerance: None,
        join_type: laminar_sql::translator::AsofSqlJoinType::Left,
    };
    let operator =
        crate::operator::asof_join::AsofJoinOperator::new("asof", config, None, graph.ctx.clone());
    let asof = graph
        .place_operator_node("asof", Box::new(operator), 2)
        .unwrap();
    let trades = graph.ensure_source_node("trades");
    let quotes = graph.ensure_source_node("quotes");
    graph.add_edge(trades, asof, 0);
    graph.add_edge(quotes, asof, 1);
    graph.output_map.insert(Arc::from("asof"), asof);
    graph.topo_dirty = true;
    graph
}

fn append_asof_downstream_probe(graph: &mut OperatorGraph, operator: Box<dyn GraphOperator>) {
    let asof = *graph.output_map.get("asof").expect("ASOF output node");
    let probe = graph
        .place_operator_node("asof_probe", operator, 1)
        .unwrap();
    graph.add_edge(asof, probe, 0);
    graph.topo_dirty = true;
}

fn asof_trade_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("trade_ts", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["AAPL"])),
            Arc::new(Int64Array::from(vec![25])),
        ],
    )
    .unwrap()
}

fn asof_quote_batch(quote_ts: i64, bid: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("quote_ts", DataType::Int64, false),
        Field::new("bid", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["AAPL"])),
            Arc::new(Int64Array::from(vec![quote_ts])),
            Arc::new(Float64Array::from(vec![bid])),
        ],
    )
    .unwrap()
}

fn asof_sources(trades: bool, quote: Option<(i64, f64)>) -> FxHashMap<Arc<str>, Vec<RecordBatch>> {
    let mut sources = FxHashMap::default();
    if trades {
        sources.insert(Arc::from("trades"), vec![asof_trade_batch()]);
    }
    if let Some((quote_ts, bid)) = quote {
        sources.insert(Arc::from("quotes"), vec![asof_quote_batch(quote_ts, bid)]);
    }
    sources
}

fn assert_graph_execution_poison(error: &DbError) {
    let DbError::StatefulOperatorPartialApply(reason) = error else {
        panic!("expected graph execution poison, got {error}");
    };
    assert!(reason.contains("cancelled or panicked"), "{reason}");
    assert!(reason.contains("last committed checkpoint"), "{reason}");
}

struct AlwaysFailOperator;

#[async_trait]
impl GraphOperator for AlwaysFailOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Err(DbError::Pipeline("injected operator failure".into()))
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}

struct TerminalShuffleOperator;

#[async_trait]
impl GraphOperator for TerminalShuffleOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Err(DbError::ShuffleTerminal(
            "injected permanent routing failure".into(),
        ))
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}

fn terminal_shuffle_graph(query_budget_ns: u64) -> OperatorGraph {
    let mut graph = test_graph();
    graph.set_query_budget_ns(query_budget_ns);
    graph.set_shared_source_isolation(true, usize::MAX);
    let source = graph.ensure_source_node("trades");
    let terminal = graph
        .place_operator_node("terminal", Box::new(TerminalShuffleOperator), 1)
        .unwrap();
    let healthy = graph
        .place_operator_node("healthy", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source, terminal, 0);
    graph.add_edge(source, healthy, 0);
    graph.output_map.insert(Arc::from("terminal"), terminal);
    graph.output_map.insert(Arc::from("healthy"), healthy);
    graph.topo_dirty = true;
    graph
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn committed_final_owner_exit_runs_only_on_the_control_path() {
    struct RevokeOnlyProbe {
        process_calls: Arc<std::sync::atomic::AtomicUsize>,
        revoked: Arc<parking_lot::Mutex<Vec<FxHashSet<u32>>>>,
    }

    #[async_trait]
    impl GraphOperator for RevokeOnlyProbe {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            self.process_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.revoked.lock().push(revoked.clone());
            Ok(())
        }
    }

    let FinalOwnerExitHarness {
        mut graph,
        sender,
        receiver,
        revoked,
        ..
    } = final_owner_exit_harness(uuid::Uuid::from_u128(1)).await;
    let process_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_revokes = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "revoke-only",
        Box::new(RevokeOnlyProbe {
            process_calls: Arc::clone(&process_calls),
            revoked: Arc::clone(&observed_revokes),
        }),
    );

    let normal_error = graph
        .complete_staged_vnode_transition()
        .expect_err("the normal transition path must reject final-owner-exit authority");
    assert!(
        normal_error.to_string().contains("control-only"),
        "{normal_error}"
    );
    assert!(observed_revokes.lock().is_empty());
    assert!(revoked.lock().is_some());

    let normal_error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("normal execution must not consume final-owner-exit authority");
    assert!(normal_error.is_shuffle_not_ready(), "{normal_error}");
    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(observed_revokes.lock().is_empty());
    assert!(revoked.lock().is_some());

    assert!(graph
        .complete_pending_vnode_transition()
        .await
        .expect("control-only final-owner-exit cleanup should complete"));

    assert_eq!(process_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(
        &*observed_revokes.lock(),
        &[[0u32].into_iter().collect::<FxHashSet<_>>()]
    );
    assert!(revoked.lock().is_none());
    assert_eq!(sender.assignment_version(), 0);
    assert_eq!(receiver.assignment_version(), 0);
    assert!(sender.active_assignment_digest().is_none());
    assert!(receiver.active_assignment_digest().is_none());
    assert!(graph.execution_poison_reason().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn final_owner_exit_rejects_process_mismatch_and_active_transport_before_callbacks() {
    struct RevokeCounter(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl GraphOperator for RevokeCounter {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    let mut mismatched = final_owner_exit_harness(uuid::Uuid::from_u128(9)).await;
    let mismatch_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    mismatched.graph.push_test_node(
        "revoke-probe",
        Box::new(RevokeCounter(Arc::clone(&mismatch_calls))),
    );
    let error = mismatched
        .graph
        .complete_pending_vnode_transition()
        .await
        .expect_err("a different process incarnation must not clean predecessor state");
    assert!(error.to_string().contains("process identity"), "{error}");
    assert_eq!(mismatch_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(mismatched.revoked.lock().is_some());
    assert!(mismatched.graph.execution_poison_reason().is_none());

    let mut active = final_owner_exit_harness(uuid::Uuid::from_u128(1)).await;
    let active_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    active.graph.push_test_node(
        "revoke-probe",
        Box::new(RevokeCounter(Arc::clone(&active_calls))),
    );
    let active_fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
        3,
        &[1],
        vec![laminar_core::checkpoint::CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        }],
    )
    .unwrap();
    active
        .sender
        .install_assignment_fence(&active_fence, &[1])
        .unwrap();
    active
        .receiver
        .install_assignment_fence(&active_fence, &[1])
        .unwrap();
    let error = active
        .graph
        .complete_pending_vnode_transition()
        .await
        .expect_err("active transport must block final-owner-exit cleanup");
    assert!(error.to_string().contains("inactive shuffle"), "{error}");
    assert_eq!(active_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(active.revoked.lock().is_some());
    assert!(active.graph.execution_poison_reason().is_none());

    let mut unfenced = final_owner_exit_harness(uuid::Uuid::from_u128(1)).await;
    let unfenced_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    unfenced.graph.push_test_node(
        "revoke-probe",
        Box::new(RevokeCounter(Arc::clone(&unfenced_calls))),
    );
    unfenced.graph.rotation_execution_fence = None;
    let error = unfenced
        .graph
        .complete_pending_vnode_transition()
        .await
        .expect_err("final-owner-exit cleanup requires the rotation execution fence");
    assert!(
        error.to_string().contains("rotation execution fence"),
        "{error}"
    );
    assert_eq!(unfenced_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(unfenced.revoked.lock().is_some());
    assert!(unfenced.graph.execution_poison_reason().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn final_owner_exit_rejects_restore_work_before_revoke_callbacks() {
    struct RevokeCounter(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl GraphOperator for RevokeCounter {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    let mut staged = final_owner_exit_harness(uuid::Uuid::from_u128(1)).await;
    let staged_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    staged.graph.push_test_node(
        "revoke-probe",
        Box::new(RevokeCounter(Arc::clone(&staged_calls))),
    );
    staged
        .graph
        .rehydrated_vnode_state
        .as_ref()
        .unwrap()
        .lock()
        .insert(
            0,
            crate::db::RehydratedVnode {
                attempt: laminar_core::state::CheckpointAttempt::canonical(7),
                chain: vec![bytes::Bytes::from_static(b"unexpected-restore")],
            },
        );
    let error = staged
        .graph
        .complete_pending_vnode_transition()
        .await
        .expect_err("final-owner-exit cleanup cannot mix restore state");
    assert!(
        error.to_string().contains("staged vnode restore"),
        "{error}"
    );
    assert_eq!(staged_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(staged.revoked.lock().is_some());

    let mut restoring = final_owner_exit_harness(uuid::Uuid::from_u128(1)).await;
    let restoring_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    restoring.graph.push_test_node(
        "revoke-probe",
        Box::new(RevokeCounter(Arc::clone(&restoring_calls))),
    );
    restoring.registry.mark_restoring(&[0]);
    let error = restoring
        .graph
        .complete_pending_vnode_transition()
        .await
        .expect_err("final-owner-exit cleanup cannot mix lifecycle restore state");
    assert!(error.to_string().contains("restoring vnodes"), "{error}");
    assert_eq!(
        restoring_calls.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(restoring.revoked.lock().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn final_owner_exit_callback_failure_poisons_and_retains_authority() {
    struct FailingRevoke;

    #[async_trait]
    impl GraphOperator for FailingRevoke {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            Err(DbError::Pipeline(
                "injected final-owner-exit failure".into(),
            ))
        }
    }

    let mut harness = final_owner_exit_harness(uuid::Uuid::from_u128(1)).await;
    harness
        .graph
        .push_test_node("failing-revoke", Box::new(FailingRevoke));

    let error = harness
        .graph
        .complete_pending_vnode_transition()
        .await
        .expect_err("an indeterminate final-owner-exit callback must poison the graph");

    assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
    assert!(error.to_string().contains("injected final-owner-exit"));
    assert!(harness.revoked.lock().is_some());
    assert!(harness.graph.execution_poison_reason().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn final_owner_exit_restore_drift_after_callback_poisons_and_retains_authority() {
    struct RestoringRevoke {
        registry: Arc<laminar_core::state::VnodeRegistry>,
        staged: Arc<parking_lot::Mutex<std::collections::HashMap<u32, crate::db::RehydratedVnode>>>,
    }

    #[async_trait]
    impl GraphOperator for RestoringRevoke {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.registry.mark_restoring(&[0]);
            self.staged.lock().insert(
                0,
                crate::db::RehydratedVnode {
                    attempt: laminar_core::state::CheckpointAttempt::canonical(7),
                    chain: vec![bytes::Bytes::from_static(b"injected-restore")],
                },
            );
            Ok(())
        }
    }

    let mut harness = final_owner_exit_harness(uuid::Uuid::from_u128(1)).await;
    let staged = Arc::clone(
        harness
            .graph
            .rehydrated_vnode_state
            .as_ref()
            .expect("final-owner-exit harness must install restore staging"),
    );
    harness.graph.push_test_node(
        "restoring-revoke",
        Box::new(RestoringRevoke {
            registry: Arc::clone(&harness.registry),
            staged: Arc::clone(&staged),
        }),
    );

    let error = harness
        .graph
        .complete_pending_vnode_transition()
        .await
        .expect_err("restore drift after callbacks must poison the graph");

    assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
    assert!(error.to_string().contains("restore lifecycle"), "{error}");
    assert!(harness.registry.any_restoring());
    assert!(!staged.lock().is_empty());
    assert!(harness.revoked.lock().is_some());
    assert!(harness.graph.execution_poison_reason().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn final_owner_exit_transport_drift_after_callback_poisons_and_retains_authority() {
    struct ActivatingRevoke {
        receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
        fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    }

    #[async_trait]
    impl GraphOperator for ActivatingRevoke {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.receiver
                .install_assignment_fence(&self.fence, &[1])
                .map_err(|error| DbError::Pipeline(error.to_string()))?;
            Ok(())
        }
    }

    let mut harness = final_owner_exit_harness(uuid::Uuid::from_u128(1)).await;
    let conflicting = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
        3,
        &[1],
        vec![laminar_core::checkpoint::CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        }],
    )
    .unwrap();
    harness.graph.push_test_node(
        "activating-revoke",
        Box::new(ActivatingRevoke {
            receiver: Arc::clone(&harness.receiver),
            fence: conflicting,
        }),
    );

    let error = harness
        .graph
        .complete_pending_vnode_transition()
        .await
        .expect_err("transport activation after revoke must poison the graph");

    assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
    assert!(error.to_string().contains("became active"), "{error}");
    assert!(harness.revoked.lock().is_some());
    assert!(harness.graph.execution_poison_reason().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn final_owner_exit_assignment_drift_after_callback_poisons_and_retains_authority() {
    struct ReassigningRevoke(Arc<laminar_core::state::VnodeRegistry>);

    #[async_trait]
    impl GraphOperator for ReassigningRevoke {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.0
                .set_assignment_and_version(vec![laminar_core::state::NodeId(2)].into(), 3);
            Ok(())
        }
    }

    let mut harness = final_owner_exit_harness(uuid::Uuid::from_u128(1)).await;
    harness.graph.push_test_node(
        "reassigning-revoke",
        Box::new(ReassigningRevoke(Arc::clone(&harness.registry))),
    );

    let error = harness
        .graph
        .complete_pending_vnode_transition()
        .await
        .expect_err("assignment drift after revoke must poison the graph");

    assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
    assert!(error.to_string().contains("assignment changed"), "{error}");
    assert!(harness.revoked.lock().is_some());
    assert!(harness.graph.execution_poison_reason().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn successful_revoke_batch_consumes_handle() {
    let VnodeTransitionHarness { mut graph, .. } = vnode_transition_harness_for_assignment(
        vec![
            laminar_core::state::NodeId(2),
            laminar_core::state::NodeId(1),
        ],
        &[],
        Vec::new(),
    )
    .await;
    let handle = target_scoped_revoke_handle([0]);
    graph.set_vnode_revocation_handle(Arc::clone(&handle));
    graph.complete_staged_vnode_transition().unwrap();
    assert!(
        handle.lock().is_none(),
        "the revoke handle is consumed only after the complete batch succeeds",
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn out_of_range_revoke_is_rejected_before_callbacks() {
    struct RevokeCounter(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl GraphOperator for RevokeCounter {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    let VnodeTransitionHarness { mut graph, .. } =
        vnode_transition_harness(1, &[], Vec::new()).await;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    graph.push_test_node("revoke-probe", Box::new(RevokeCounter(Arc::clone(&calls))));
    let handle = target_scoped_revoke_handle([7]);
    graph.set_vnode_revocation_handle(Arc::clone(&handle));

    let error = graph.complete_staged_vnode_transition().unwrap_err();

    assert!(error.to_string().contains("outside the pinned assignment"));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(
        staged_revoke_vnodes(&handle),
        Some([7u32].into_iter().collect::<FxHashSet<u32>>())
    );
    assert!(graph.execution_poison_reason().is_none());
}

#[cfg(feature = "cluster")]
#[test]
fn revoke_without_cluster_scope_is_rejected_before_callbacks() {
    struct RevokeCounter(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl GraphOperator for RevokeCounter {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = test_graph();
    graph.push_test_node("revoke-probe", Box::new(RevokeCounter(Arc::clone(&calls))));
    let handle = target_scoped_revoke_handle([1]);
    graph.set_vnode_revocation_handle(Arc::clone(&handle));

    let error = graph.complete_staged_vnode_transition().unwrap_err();

    assert!(error.to_string().contains("no cluster ownership scope"));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(
        staged_revoke_vnodes(&handle),
        Some([1u32].into_iter().collect::<FxHashSet<u32>>())
    );
    assert!(graph.execution_poison_reason().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn vnode_revoke_failure_faults_and_retains_pending_work() {
    struct RevokeProbe {
        label: &'static str,
        calls: Arc<parking_lot::Mutex<Vec<&'static str>>>,
        failure: Option<&'static str>,
    }

    #[async_trait]
    impl GraphOperator for RevokeProbe {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.calls.lock().push(self.label);
            if let Some(message) = self.failure {
                return Err(DbError::Pipeline(message.into()));
            }
            Ok(())
        }
    }

    let VnodeTransitionHarness { mut graph, .. } = vnode_transition_harness_for_assignment(
        vec![
            laminar_core::state::NodeId(2),
            laminar_core::state::NodeId(1),
        ],
        &[],
        Vec::new(),
    )
    .await;
    let calls = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "z-revoke-failure",
        Box::new(RevokeProbe {
            label: "failure",
            calls: Arc::clone(&calls),
            failure: Some("injected vnode revoke failure"),
        }),
    );
    graph.push_test_node(
        "a-revoke-success",
        Box::new(RevokeProbe {
            label: "success",
            calls: Arc::clone(&calls),
            failure: None,
        }),
    );
    let handle = target_scoped_revoke_handle([0]);
    graph.set_vnode_revocation_handle(Arc::clone(&handle));

    let error = graph.complete_staged_vnode_transition().unwrap_err();
    assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
    assert!(error.to_string().contains("z-revoke-failure"));
    assert_eq!(&*calls.lock(), &["success", "failure"]);
    assert_eq!(
        staged_revoke_vnodes(&handle),
        Some([0u32].into_iter().collect::<FxHashSet<u32>>())
    );
    assert!(graph.execution_poison_reason().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_capture_guard_excludes_assignment_publication() {
    let mut graph = test_graph();
    let fence = Arc::new(tokio::sync::RwLock::new(()));
    graph.set_rotation_execution_fence(Arc::clone(&fence));
    let writer = Arc::clone(&fence).write_owned().await;

    let mut capture = Box::pin(graph.checkpoint_rotation_guard_until(
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut capture)
            .await
            .is_err(),
        "capture must wait while assignment publication owns the write fence"
    );
    drop(writer);

    let reader = capture
        .await
        .expect("capture should acquire the released rotation fence")
        .expect("configured graph should return a rotation token");
    assert!(
        Arc::clone(&fence).try_write_owned().is_err(),
        "assignment publication must remain excluded through mutable capture"
    );
    drop(reader);
    assert!(Arc::clone(&fence).try_write_owned().is_ok());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_quiescence_requires_staged_vnode_transitions_to_apply() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("global".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        ..
    } = vnode_transition_harness(1, &[0], vec![(0, vec![encoded_vnode_partial(&partial)])]).await;
    let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "global",
        Box::new(RecordingVnodeRestoreOperator {
            applied: Arc::clone(&applied),
            failure_on_vnode: None,
        }),
    );

    assert!(
        !graph.checkpoint_is_quiescent(),
        "staged acquire state must enter the checkpoint drain loop"
    );
    assert!(matches!(
        graph.snapshot_state(),
        Err(DbError::Checkpoint(_))
    ));
    assert!(matches!(
        graph.snapshot_state_by_vnode(),
        Err(DbError::Checkpoint(_))
    ));
    graph
        .execute_checkpoint_drain_cycle(i64::MAX, None)
        .await
        .expect("checkpoint drain should apply staged acquire state");
    assert_eq!(&*applied.lock(), &[0]);
    assert!(graph.checkpoint_is_quiescent());
    assert!(graph.snapshot_state().unwrap().is_none());
    assert!(graph.snapshot_state_by_vnode().unwrap().is_empty());

    let VnodeTransitionHarness {
        graph: mut revoke_graph,
        ..
    } = vnode_transition_harness_for_assignment(
        vec![
            laminar_core::state::NodeId(2),
            laminar_core::state::NodeId(1),
        ],
        &[],
        Vec::new(),
    )
    .await;
    let revoked = target_scoped_revoke_handle([0]);
    revoke_graph.set_vnode_revocation_handle(Arc::clone(&revoked));
    assert!(
        !revoke_graph.checkpoint_is_quiescent(),
        "staged revoke state must enter the checkpoint drain loop"
    );
    assert!(matches!(
        revoke_graph.snapshot_state(),
        Err(DbError::Checkpoint(_))
    ));
    assert!(matches!(
        revoke_graph.snapshot_state_by_vnode(),
        Err(DbError::Checkpoint(_))
    ));
    revoke_graph
        .execute_checkpoint_drain_cycle(i64::MAX, None)
        .await
        .expect("checkpoint drain should apply staged revoke state");
    assert!(revoked.lock().is_none());
    assert!(revoke_graph.checkpoint_is_quiescent());

    registry.set_assignment(vec![laminar_core::state::NodeId(1)].into());
    assert!(
        !graph.checkpoint_is_quiescent(),
        "capture must wait for graph execution to validate the new assignment"
    );
    assert!(matches!(
        graph.snapshot_state(),
        Err(DbError::Checkpoint(_))
    ));
    assert!(matches!(
        graph.snapshot_state_by_vnode(),
        Err(DbError::Checkpoint(_))
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn corrupt_rehydration_chain_faults_and_keeps_vnode_restoring() {
    let VnodeTransitionHarness {
        mut graph,
        registry,
        ..
    } = vnode_transition_harness(
        1,
        &[0],
        vec![(0, vec![bytes::Bytes::from_static(b"not-rkyv")])],
    )
    .await;

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("corrupt vnode state must fault the cycle");
    let message = error.to_string();
    assert!(message.contains("[LDB-6051]"), "{message}");
    assert!(message.contains("link 0"), "{message}");
    assert!(message.contains("corrupt"), "{message}");
    assert!(
        registry.is_restoring(0),
        "a corrupt chain must not activate the vnode"
    );
    assert_eq!(graph.last_execution_assignment_version(), None);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn execution_assignment_is_not_published_when_transport_scope_is_stale() {
    let VnodeTransitionHarness {
        mut graph,
        registry,
        ..
    } = vnode_transition_harness(1, &[0], vec![(0, Vec::new())]).await;
    registry.set_assignment(vec![laminar_core::state::NodeId(1)].into());

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("stale shuffle transport scope must reject the cycle");

    assert!(matches!(error, DbError::ShuffleNotReady(_)));
    assert_eq!(graph.last_execution_assignment_version(), None);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn rehydration_delta_without_full_base_is_rejected_before_callbacks() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: Vec::new(),
        base: Some(sealed_test_parent(
            laminar_core::state::CheckpointAttempt::new(1, 1),
        )),
        deltas: vec![("agg".to_string(), vec![1])],
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        ..
    } = vnode_transition_harness(1, &[0], vec![(0, vec![encoded_vnode_partial(&partial)])]).await;
    let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "agg",
        Box::new(RecordingVnodeRestoreOperator {
            applied: Arc::clone(&applied),
            failure_on_vnode: None,
        }),
    );

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("a delta-only chain must fault the cycle");
    let message = error.to_string();
    assert!(message.contains("no FULL base"), "{message}");
    assert!(message.contains("agg"), "{message}");
    assert!(applied.lock().is_empty(), "invalid state was applied");
    assert!(registry.is_restoring(0));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn duplicate_operator_in_one_partial_is_rejected_before_callbacks() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".to_string(), vec![1]), ("agg".to_string(), vec![2])],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        ..
    } = vnode_transition_harness(1, &[0], vec![(0, vec![encoded_vnode_partial(&partial)])]).await;
    let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "agg",
        Box::new(RecordingVnodeRestoreOperator {
            applied: Arc::clone(&applied),
            failure_on_vnode: None,
        }),
    );

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("ambiguous legacy participant entries must fail closed");

    assert!(error.to_string().contains("repeats operator 'agg'"));
    assert!(applied.lock().is_empty());
    assert!(registry.is_restoring(0));
    assert!(graph.execution_poison_reason().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn operator_without_vnode_restore_contract_cannot_silently_accept_state() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("stateless".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        ..
    } = vnode_transition_harness(1, &[0], vec![(0, vec![encoded_vnode_partial(&partial)])]).await;
    graph.push_test_node("stateless", Box::new(SourcePassthrough));

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("the trait default must reject unowned vnode state");

    assert!(error
        .to_string()
        .contains("operator does not accept vnode checkpoint state for vnode 0"));
    assert!(registry.is_restoring(0));
    assert!(graph.execution_poison_reason().is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn missing_rehydration_operator_is_rejected_before_callbacks() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![
            ("present".to_string(), vec![1]),
            ("ghost".to_string(), vec![2]),
        ],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        ..
    } = vnode_transition_harness(1, &[0], vec![(0, vec![encoded_vnode_partial(&partial)])]).await;
    let present_applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "present",
        Box::new(RecordingVnodeRestoreOperator {
            applied: Arc::clone(&present_applied),
            failure_on_vnode: None,
        }),
    );

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("topology drift must fault the cycle");
    let message = error.to_string();
    assert!(message.contains("missing operator"), "{message}");
    assert!(message.contains("ghost"), "{message}");
    assert!(
        present_applied.lock().is_empty(),
        "validation must finish before any operator is mutated"
    );
    assert!(registry.is_restoring(0));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn later_vnode_structural_failure_runs_no_callbacks_and_retains_batch() {
    struct TransitionCallbackProbe(Arc<parking_lot::Mutex<Vec<&'static str>>>);

    #[async_trait]
    impl GraphOperator for TransitionCallbackProbe {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.0.lock().push("revoke");
            Ok(())
        }

        fn apply_vnode_chain(
            &mut self,
            _vnode: u32,
            _base: &[u8],
            _deltas: &[&[u8]],
        ) -> Result<(), DbError> {
            self.0.lock().push("restore");
            Ok(())
        }
    }

    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness_for_assignment(
        vec![
            laminar_core::state::NodeId(1),
            laminar_core::state::NodeId(1),
            laminar_core::state::NodeId(2),
        ],
        &[0, 1],
        vec![
            (1, vec![bytes::Bytes::from_static(b"not-rkyv")]),
            (0, vec![encoded_vnode_partial(&partial)]),
        ],
    )
    .await;
    let callbacks = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "agg",
        Box::new(TransitionCallbackProbe(Arc::clone(&callbacks))),
    );
    let revoked = target_scoped_revoke_handle([2]);
    graph.set_vnode_revocation_handle(Arc::clone(&revoked));

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("later structural corruption must reject the complete batch");
    let message = error.to_string();
    assert!(message.contains("vnode 1"), "{message}");
    assert!(message.contains("corrupt"), "{message}");
    assert!(callbacks.lock().is_empty(), "preflight ran a callback");
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);
    assert_eq!(staged.lock().len(), 2, "preflight consumed recovery input");
    assert_eq!(
        staged_revoke_vnodes(&revoked),
        Some([2u32].into_iter().collect::<FxHashSet<_>>())
    );
    assert!(graph.execution_poison_reason().is_none());
    assert_eq!(graph.last_execution_assignment_version(), None);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn mixed_checkpoint_attempts_run_no_callbacks_and_retain_batch() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let encoded = encoded_vnode_partial(&partial);
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(
        2,
        &[0, 1],
        vec![(0, vec![encoded.clone()]), (1, vec![encoded])],
    )
    .await;
    staged
        .lock()
        .get_mut(&1)
        .expect("second vnode is staged")
        .attempt = laminar_core::state::CheckpointAttempt::new(7, 8);
    let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "agg",
        Box::new(RecordingVnodeRestoreOperator {
            applied: Arc::clone(&applied),
            failure_on_vnode: None,
        }),
    );

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("one transition cannot mix committed checkpoint attempts");

    assert!(error
        .to_string()
        .contains("one canonical checkpoint attempt"));
    assert!(
        applied.lock().is_empty(),
        "attempt validation ran a callback"
    );
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);
    assert_eq!(
        staged.lock().len(),
        2,
        "attempt validation consumed staging"
    );
    assert!(graph.execution_poison_reason().is_none());
    assert_eq!(graph.last_execution_assignment_version(), None);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn exact_owned_restoring_roster_is_required_before_callbacks() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(2, &[0, 1], vec![(0, vec![encoded_vnode_partial(&partial)])])
        .await;
    let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "agg",
        Box::new(RecordingVnodeRestoreOperator {
            applied: Arc::clone(&applied),
            failure_on_vnode: None,
        }),
    );

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("missing owned vnode state must reject the complete batch");

    assert!(error.to_string().contains("exact owned/restoring roster"));
    assert!(applied.lock().is_empty());
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);
    assert_eq!(staged.lock().len(), 1);
    assert!(graph.execution_poison_reason().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn owned_non_restoring_staged_vnode_is_rejected_before_callbacks() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let encoded = encoded_vnode_partial(&partial);
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(
        2,
        &[0],
        vec![(0, vec![encoded.clone()]), (1, vec![encoded])],
    )
    .await;
    let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "agg",
        Box::new(RecordingVnodeRestoreOperator {
            applied: Arc::clone(&applied),
            failure_on_vnode: None,
        }),
    );

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("an owned staged vnode outside the restoring roster must reject the batch");

    assert!(error.to_string().contains("exact owned/restoring roster"));
    assert!(applied.lock().is_empty());
    assert_eq!(registry.restoring_vnodes(), vec![0]);
    assert_eq!(staged.lock().len(), 2);
    assert!(graph.execution_poison_reason().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn currently_owned_revoke_requires_matching_restore() {
    struct RevokeProbe(Arc<parking_lot::Mutex<Vec<u32>>>);

    #[async_trait]
    impl GraphOperator for RevokeProbe {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            self.0.lock().extend(revoked);
            Ok(())
        }
    }

    let VnodeTransitionHarness {
        mut graph,
        registry,
        ..
    } = vnode_transition_harness(1, &[], Vec::new()).await;
    let callbacks = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node("probe", Box::new(RevokeProbe(Arc::clone(&callbacks))));
    let revoked = target_scoped_revoke_handle([0]);
    graph.set_vnode_revocation_handle(Arc::clone(&revoked));

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("owned state cannot be dropped without an authoritative restoring image");

    assert!(error
        .to_string()
        .contains("currently owned revoked vnodes [0]"));
    assert!(callbacks.lock().is_empty());
    assert_eq!(
        staged_revoke_vnodes(&revoked),
        Some([0u32].into_iter().collect::<FxHashSet<_>>())
    );
    assert!(!registry.any_restoring());
    assert!(graph.execution_poison_reason().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn zero_link_rehydration_cannot_activate_vnode() {
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(1, &[0], vec![(0, Vec::new())]).await;

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("a staged vnode without a durable chain must fail closed");

    assert!(error.to_string().contains("chain has no links"));
    assert!(registry.is_restoring(0));
    assert_eq!(staged.lock().len(), 1);
    assert!(graph.execution_poison_reason().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn encoded_empty_full_rehydration_activates_vnode() {
    let empty_full = crate::vnode_partial::VnodePartial {
        operators: Vec::new(),
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(1, &[0], vec![(0, vec![encoded_vnode_partial(&empty_full)])])
        .await;

    graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect("one encoded empty FULL is a valid durable empty vnode");

    assert!(!registry.is_restoring(0));
    assert!(staged.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn later_restore_callback_failure_poisons_and_retains_complete_transition() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(
        2,
        &[0, 1],
        vec![
            (1, vec![encoded_vnode_partial(&partial)]),
            (0, vec![encoded_vnode_partial(&partial)]),
        ],
    )
    .await;
    let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "agg",
        Box::new(RecordingVnodeRestoreOperator {
            applied: Arc::clone(&applied),
            failure_on_vnode: Some((1, "injected vnode apply failure")),
        }),
    );
    let revoked = target_scoped_revoke_handle([0]);
    graph.set_vnode_revocation_handle(Arc::clone(&revoked));

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("an indeterminate restore callback must poison the graph generation");
    let message = error.to_string();
    assert!(message.contains("restore callback for vnode 1 operator 'agg'"));
    assert!(message.contains("injected vnode apply failure"));
    assert_eq!(&*applied.lock(), &[0], "the first callback did mutate");
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);
    assert_eq!(staged.lock().len(), 2);
    assert_eq!(
        staged_revoke_vnodes(&revoked),
        Some([0u32].into_iter().collect::<FxHashSet<_>>())
    );
    assert!(graph.execution_poison_reason().is_some());

    let Err(snapshot_error) = graph.snapshot_state() else {
        panic!("the poisoned graph unexpectedly allowed a whole-state snapshot");
    };
    assert_graph_execution_poison(&snapshot_error);
    let Err(vnode_snapshot_error) = graph.snapshot_state_by_vnode() else {
        panic!("the poisoned graph unexpectedly allowed a vnode snapshot");
    };
    assert_graph_execution_poison(&vnode_snapshot_error);
    let retry_error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("the poisoned graph must not retry retained callbacks");
    assert_graph_execution_poison(&retry_error);
    assert_eq!(&*applied.lock(), &[0]);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn transport_certificate_change_after_callback_poisons_before_activation() {
    struct TransportInvalidatingOperator {
        receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
        applied: Arc<parking_lot::Mutex<Vec<u32>>>,
    }

    #[async_trait]
    impl GraphOperator for TransportInvalidatingOperator {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn apply_vnode_chain(
            &mut self,
            vnode: u32,
            _base: &[u8],
            _deltas: &[&[u8]],
        ) -> Result<(), DbError> {
            self.applied.lock().push(vnode);
            self.receiver.invalidate_assignment_fence();
            Ok(())
        }
    }

    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(1, &[0], vec![(0, vec![encoded_vnode_partial(&partial)])]).await;
    let receiver = Arc::clone(&graph.cluster_shuffle.as_ref().unwrap().receiver);
    let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "agg",
        Box::new(TransportInvalidatingOperator {
            receiver,
            applied: Arc::clone(&applied),
        }),
    );
    let revoked = target_scoped_revoke_handle([0]);
    graph.set_vnode_revocation_handle(Arc::clone(&revoked));

    let error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("transport authority drift after mutation must poison the graph");

    assert!(error
        .to_string()
        .contains("shuffle assignment certificate changed"));
    assert_eq!(&*applied.lock(), &[0]);
    assert!(registry.is_restoring(0));
    assert_eq!(staged.lock().len(), 1);
    assert_eq!(
        staged_revoke_vnodes(&revoked),
        Some([0u32].into_iter().collect::<FxHashSet<_>>())
    );
    assert!(graph.execution_poison_reason().is_some());
    assert_eq!(graph.last_execution_assignment_version(), None);
    let retry_error = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect_err("transport drift must poison same-graph retry");
    assert_graph_execution_poison(&retry_error);
    assert_eq!(&*applied.lock(), &[0]);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn revoke_then_reacquire_overlap_drops_before_restore() {
    struct RevokeRestoreProbe(Arc<parking_lot::Mutex<Vec<&'static str>>>);

    #[async_trait]
    impl GraphOperator for RevokeRestoreProbe {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn drop_owned_vnodes(&mut self, revoked: &FxHashSet<u32>) -> Result<(), DbError> {
            assert_eq!(revoked, &[0u32].into_iter().collect::<FxHashSet<_>>());
            self.0.lock().push("revoke");
            Ok(())
        }

        fn apply_vnode_chain(
            &mut self,
            vnode: u32,
            _base: &[u8],
            _deltas: &[&[u8]],
        ) -> Result<(), DbError> {
            assert_eq!(vnode, 0);
            self.0.lock().push("restore");
            Ok(())
        }
    }

    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(1, &[0], vec![(0, vec![encoded_vnode_partial(&partial)])]).await;
    let events = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node("agg", Box::new(RevokeRestoreProbe(Arc::clone(&events))));
    let revoked = target_scoped_revoke_handle([0]);
    graph.set_vnode_revocation_handle(Arc::clone(&revoked));

    graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect("rapid revoke/reacquire overlap should repair in drop-then-restore order");

    assert_eq!(&*events.lock(), &["revoke", "restore"]);
    assert!(!registry.is_restoring(0));
    assert!(staged.lock().is_empty());
    assert!(revoked.lock().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn acquire_then_lose_before_completion_clears_unowned_restoring_state() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::state::NodeId;

    let empty_full = crate::vnode_partial::VnodePartial {
        operators: Vec::new(),
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(2, &[1], vec![(1, vec![encoded_vnode_partial(&empty_full)])])
        .await;
    registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
    let owners = [1, 2];
    let fence = CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &owners,
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ],
    )
    .unwrap();
    let config = graph.cluster_shuffle.as_ref().unwrap();
    config
        .sender
        .install_assignment_fence(&fence, &owners)
        .unwrap();
    config
        .receiver
        .install_assignment_fence(&fence, &owners)
        .unwrap();

    graph
        .complete_staged_vnode_transition()
        .expect("the pinned owner map may discard an obsolete unowned acquire");

    assert!(staged.lock().is_empty());
    assert!(!registry.is_restoring(1));
    assert!(!registry.any_restoring());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn successful_rehydration_delays_all_activation_until_callbacks_finish() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![
            ("left".to_string(), vec![1]),
            ("right".to_string(), vec![2]),
        ],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(
        2,
        &[0, 1],
        vec![
            (1, vec![encoded_vnode_partial(&partial)]),
            (0, vec![encoded_vnode_partial(&partial)]),
        ],
    )
    .await;
    let observations = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "right",
        Box::new(RestoringRosterProbe {
            label: "right",
            registry: Arc::clone(&registry),
            observations: Arc::clone(&observations),
        }),
    );
    graph.push_test_node(
        "left",
        Box::new(RestoringRosterProbe {
            label: "left",
            registry: Arc::clone(&registry),
            observations: Arc::clone(&observations),
        }),
    );

    graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .expect("complete vnode batch should apply");

    assert_eq!(
        &*observations.lock(),
        &[
            ("left", 0, vec![0, 1]),
            ("right", 0, vec![0, 1]),
            ("left", 1, vec![0, 1]),
            ("right", 1, vec![0, 1]),
        ],
        "vnode and operator callbacks are canonical and activation is delayed"
    );
    assert!(registry.restoring_vnodes().is_empty());
    assert!(staged.lock().is_empty());
    assert_eq!(
        graph.last_execution_assignment_version(),
        Some(registry.assignment_version())
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn control_only_completion_applies_transition_without_source_cycle() {
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".to_string(), vec![1])],
        base: None,
        deltas: Vec::new(),
    };
    let VnodeTransitionHarness {
        mut graph,
        registry,
        staged,
    } = vnode_transition_harness(1, &[0], vec![(0, vec![encoded_vnode_partial(&partial)])]).await;
    let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
    graph.push_test_node(
        "agg",
        Box::new(RecordingVnodeRestoreOperator {
            applied: Arc::clone(&applied),
            failure_on_vnode: None,
        }),
    );

    assert!(graph.complete_pending_vnode_transition().await.unwrap());
    assert_eq!(&*applied.lock(), &[0]);
    assert!(staged.lock().is_empty());
    assert!(!registry.any_restoring());
    assert_eq!(
        graph.last_execution_assignment_version(),
        Some(registry.assignment_version())
    );
    assert!(!graph.complete_pending_vnode_transition().await.unwrap());
}

#[test]
fn test_node_domains_disjoint_queries_separate() {
    let mut graph = test_graph();
    graph.register_source_schema("trades_a".to_string(), test_schema());
    graph.register_source_schema("trades_b".to_string(), test_schema());
    graph.add_query(
        "qa".to_string(),
        "SELECT symbol FROM trades_a".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "qb".to_string(),
        "SELECT symbol FROM trades_b".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.compute_topo_order();

    assert_eq!(
        graph.domain_count, 2,
        "disjoint-source queries are separate domains"
    );
    let a = graph.source_map.get("trades_a").copied().unwrap();
    let b = graph.source_map.get("trades_b").copied().unwrap();
    assert_ne!(graph.node_domain[a], graph.node_domain[b]);
}

#[test]
fn test_node_domains_shared_source_joined() {
    let mut graph = test_graph();
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "qa".to_string(),
        "SELECT symbol FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "qb".to_string(),
        "SELECT price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.compute_topo_order();

    assert_eq!(
        graph.domain_count, 1,
        "queries sharing a source recover together"
    );
}

#[test]
fn test_node_domains_shared_source_isolated() {
    let mut graph = test_graph();
    graph.set_shared_source_isolation(true, usize::MAX);
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "qa".to_string(),
        "SELECT symbol FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "qb".to_string(),
        "SELECT price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.compute_topo_order();

    assert_eq!(
        graph.domain_count, 2,
        "isolation splits shared-source queries into separate domains"
    );
    let qa = graph.find_node("qa").unwrap();
    let qb = graph.find_node("qb").unwrap();
    assert_ne!(graph.node_domain[qa], graph.node_domain[qb]);
    let src = graph.source_map.get("trades").copied().unwrap();
    assert_eq!(
        graph.node_domain[src],
        usize::MAX,
        "an isolated source is not a failure domain of its own"
    );
}

// A fault in one query sharing a source must not sink a sibling reading the same source: the
// healthy query still emits, and the shared source is held back because it feeds the faulted domain.

#[tokio::test]
async fn terminal_shuffle_bypasses_main_failure_domain_isolation() {
    let mut graph = terminal_shuffle_graph(u64::MAX);

    let error = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .expect_err("terminal routing must abort before isolating one domain");

    assert!(matches!(error, DbError::ShuffleTerminal(_)));
    assert!(!graph.take_cycle_failures().0);
}

#[tokio::test]
async fn terminal_shuffle_bypasses_deferred_failure_domain_isolation() {
    let mut graph = terminal_shuffle_graph(0);

    let error = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .expect_err("a deferred terminal routing failure must abort the cycle");

    assert!(matches!(error, DbError::ShuffleTerminal(_)));
    assert!(!graph.take_cycle_failures().0);
}

#[tokio::test]
async fn test_execute_cycle_isolates_shared_source_sibling() {
    let mut graph = test_graph();
    graph.set_shared_source_isolation(true, usize::MAX);
    let source_node = graph.ensure_source_node("trades");
    let failing = graph
        .place_operator_node("failing", Box::new(AlwaysFailOperator), 1)
        .unwrap();
    let healthy = graph
        .place_operator_node("healthy", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source_node, failing, 0);
    graph.add_edge(source_node, healthy, 0);
    graph.output_map.insert(Arc::from("failing"), failing);
    graph.output_map.insert(Arc::from("healthy"), healthy);
    graph.topo_dirty = true;

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let results = graph
        .execute_cycle(&source, i64::MAX, None)
        .await
        .expect("the healthy sibling keeps the cycle Ok though they share a source");

    assert_eq!(
        total_rows(&results, "healthy"),
        2,
        "healthy sibling emitted despite sharing the faulted source"
    );
    assert_eq!(
        total_rows(&results, "failing"),
        0,
        "faulted domain emitted nothing"
    );

    let (any_failed, failed_sources) = graph.take_cycle_failures();
    assert!(any_failed);
    assert!(
        failed_sources.contains(&Arc::from("trades")),
        "the shared source is held back: it feeds the faulted domain"
    );
}

// A transient fault in one shared-source query replays from the preserved input on the next
// cycle (cycle-1 rows + cycle-2 rows), while the healthy sibling only sees new rows.
#[tokio::test]
async fn test_shared_source_isolation_replays_faulted_domain() {
    struct ReplayTestOp {
        fail_once: bool,
        has_failed: bool,
    }
    #[async_trait]
    impl GraphOperator for ReplayTestOp {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            if self.fail_once && !self.has_failed {
                self.has_failed = true;
                return Err(DbError::Pipeline("transient fault".into()));
            }
            Ok(inputs.first().cloned().unwrap_or_default())
        }
        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }
        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }
    }

    let mut graph = test_graph();
    graph.set_shared_source_isolation(true, usize::MAX);
    let src = graph.ensure_source_node("trades");
    let a = graph
        .place_operator_node(
            "a",
            Box::new(ReplayTestOp {
                fail_once: true,
                has_failed: false,
            }),
            1,
        )
        .unwrap();
    graph.add_edge(src, a, 0);
    graph.output_map.insert(Arc::from("a"), a);
    let b = graph
        .place_operator_node(
            "b",
            Box::new(ReplayTestOp {
                fail_once: false,
                has_failed: false,
            }),
            1,
        )
        .unwrap();
    graph.add_edge(src, b, 0);
    graph.output_map.insert(Arc::from("b"), b);
    graph.topo_dirty = true;

    let mut cycle1 = FxHashMap::default();
    cycle1.insert(Arc::from("trades"), vec![test_batch()]);
    let r1 = graph
        .execute_cycle(&cycle1, i64::MAX, None)
        .await
        .expect("healthy sibling keeps cycle 1 Ok");
    assert_eq!(total_rows(&r1, "b"), 2, "healthy sibling emitted cycle 1");
    assert_eq!(
        total_rows(&r1, "a"),
        0,
        "faulted op emitted nothing cycle 1"
    );
    let (_, failed) = graph.take_cycle_failures();
    assert!(failed.contains(&Arc::from("trades")));

    let mut cycle2 = FxHashMap::default();
    cycle2.insert(Arc::from("trades"), vec![test_batch()]);
    let r2 = graph
        .execute_cycle(&cycle2, i64::MAX, None)
        .await
        .expect("cycle 2 Ok");
    assert_eq!(
        total_rows(&r2, "a"),
        4,
        "faulted op replays preserved cycle-1 rows plus new cycle-2 rows"
    );
    assert_eq!(
        total_rows(&r2, "b"),
        2,
        "healthy sibling sees only new rows (no replay)"
    );
    let (any_failed2, _) = graph.take_cycle_failures();
    assert!(!any_failed2, "no fault on the replay cycle");
}

// A fatal error in one disjoint query must not sink the sibling query: the healthy domain
// still produces output, and only the faulted domain's source is held back from committing.
#[tokio::test]
async fn test_execute_cycle_isolates_failed_domain() {
    let mut graph = test_graph();
    let source_a = graph.ensure_source_node("trades_a");
    let source_b = graph.ensure_source_node("trades_b");
    let failing = graph
        .place_operator_node("failing", Box::new(AlwaysFailOperator), 1)
        .unwrap();
    let healthy = graph
        .place_operator_node("filtered", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source_a, failing, 0);
    graph.add_edge(source_b, healthy, 0);
    graph.output_map.insert(Arc::from("failing"), failing);
    graph.output_map.insert(Arc::from("filtered"), healthy);
    graph.topo_dirty = true;

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades_a"), vec![test_batch()]);
    source.insert(Arc::from("trades_b"), vec![test_batch()]);

    let results = graph
        .execute_cycle(&source, i64::MAX, None)
        .await
        .expect("a healthy sibling domain keeps the cycle Ok");

    assert_eq!(
        total_rows(&results, "filtered"),
        2,
        "healthy domain emitted"
    );
    assert_eq!(
        total_rows(&results, "failing"),
        0,
        "faulted domain emitted nothing"
    );

    let (any_failed, failed_sources) = graph.take_cycle_failures();
    assert!(any_failed);
    assert!(failed_sources.contains(&Arc::from("trades_a")));
    assert!(!failed_sources.contains(&Arc::from("trades_b")));
}

#[tokio::test]
async fn test_og_compiled_projection() {
    // Non-aggregate projection-only query should compile to PhysicalExpr
    let mut graph = test_graph();
    graph.add_query(
        "projected".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // First cycle triggers lazy init
    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "projected"), 2); // Both rows projected

    // Second cycle reuses compiled path (no SQL overhead)
    let r2 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r2, "projected"), 2);
}

#[tokio::test]
async fn test_og_compiled_fallback_on_type_mismatch() {
    // WHERE price > 200 has Float64 > Int64 type mismatch that
    // DataFusion's create_physical_expr doesn't coerce. Compiled
    // path should fall back to CachedPlan transparently.
    let mut graph = test_graph();
    graph.add_query(
        "filtered".to_string(),
        "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "filtered"), 1); // Only GOOG passes
}

#[tokio::test]
async fn test_og_aggregate_incremental() {
    // GROUP BY should route through IncrementalAggState
    let mut graph = test_graph();
    graph.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // Cycle 1
    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "agg"), 2); // AAPL + GOOG groups

    // Cycle 2: running totals accumulate
    let r2 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    let agg_batches = &r2[&Arc::from("agg") as &Arc<str>];
    assert_eq!(total_rows(&r2, "agg"), 2); // Still 2 groups

    // Verify accumulation: AAPL should be 150+150=300
    let price_col = agg_batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let symbol_col = agg_batches[0]
        .column_by_name("symbol")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..agg_batches[0].num_rows() {
        match symbol_col.value(i) {
            "AAPL" => assert!((price_col.value(i) - 300.0).abs() < f64::EPSILON),
            "GOOG" => assert!((price_col.value(i) - 5600.0).abs() < f64::EPSILON),
            other => panic!("unexpected symbol: {other}"),
        }
    }
}

#[tokio::test]
async fn test_og_cascading() {
    // Query A feeds Query B through intermediate LiveSourceProvider
    let mut graph = test_graph();
    graph.add_query(
        "step1".to_string(),
        "SELECT symbol, price * 2 AS doubled FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "step2".to_string(),
        "SELECT symbol, doubled FROM step1 WHERE doubled > 400".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    // step1: AAPL=300, GOOG=5600 (2 rows)
    assert_eq!(total_rows(&r, "step1"), 2);
    // step2: only GOOG=5600 passes WHERE doubled > 400
    assert_eq!(total_rows(&r, "step2"), 1);
}

#[test]
fn test_og_rejects_unbounded_diamond_fanin() {
    let mut graph = test_graph();
    graph.add_query(
        "high".to_string(),
        "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "low".to_string(),
        "SELECT symbol, price FROM trades WHERE price <= 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "combined".to_string(),
        "SELECT h.symbol, h.price FROM high h INNER JOIN low l ON h.symbol = l.symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let error = graph.take_build_errors().unwrap_err();
    assert!(error.to_string().contains("unbounded join"));
    assert!(!graph.has_query("combined"));
}

#[test]
fn test_og_rejects_generic_cross_join_fallback() {
    let mut graph = test_graph();
    graph.add_query(
        "crossed".to_string(),
        "SELECT l.symbol FROM trades l CROSS JOIN trades r".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let error = graph.take_build_errors().unwrap_err();
    assert!(error.to_string().contains("could not be planned"));
    assert!(!graph.has_query("crossed"));
}

#[tokio::test]
async fn test_og_budget_exhaustion() {
    // With a tiny budget (1 ns), only the first operator runs
    let mut graph = test_graph();
    graph.set_query_budget_ns(1); // 1 ns budget — effectively skip after first

    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q2".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();

    // With 1ns budget, not all queries should produce output
    let produced = r.len();
    assert!(
        produced < 2,
        "with 1ns budget, at most one query should run"
    );
}

#[tokio::test]
async fn test_og_budget_deferred_forward_progress() {
    // With a 1ns budget, only the first operator runs in the main loop.
    // The deferred execution pass must guarantee every operator eventually
    // processes its input within N cycles (N = number of deferred operators).
    let mut graph = test_graph();
    graph.set_query_budget_ns(1); // forces break after first operator

    // Add 5 independent queries — all read from "trades"
    for i in 0..5 {
        graph.add_query(
            format!("q{i}"),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
    }

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // Run enough cycles for all 5 operators to get their turn via
    // deferred execution (1 main + 1 deferred per cycle = 5 cycles).
    let mut produced = FxHashSet::default();
    for _ in 0..5 {
        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        for key in r.keys() {
            produced.insert(key.to_string());
        }
    }

    assert_eq!(
        produced.len(),
        5,
        "all 5 operators should produce output within 5 cycles, got: {produced:?}"
    );
}

#[tokio::test]
async fn checkpoint_drain_bypasses_query_budget_and_emits_each_row_once() {
    let mut graph = test_graph();
    // This root runs before the source and makes the near-zero budget deterministic.
    graph
        .place_operator_node("delay", Box::new(DelayOperator), 1)
        .unwrap();
    let source = graph.ensure_source_node("trades");
    let middle = graph
        .place_operator_node("middle", Box::new(SourcePassthrough), 1)
        .unwrap();
    let output = graph
        .place_operator_node("output", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source, middle, 0);
    graph.add_edge(middle, output, 0);
    graph.output_map.insert(Arc::from("output"), output);
    graph.topo_dirty = true;
    graph.set_query_budget_ns(1);

    let batch = test_batch();
    let expected_edge_bytes = batch.get_array_memory_size();
    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("trades"), vec![batch]);

    let normal = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&normal, "output"), 0);
    let (any_deferred, deferred_sources) = graph.take_cycle_deferrals();
    assert!(any_deferred);
    assert!(deferred_sources.contains(&Arc::from("trades")));
    assert_eq!(
        graph.checkpoint_pending_input_bytes(),
        expected_edge_bytes,
        "normal budget deferral leaves the source row batch on the middle edge"
    );

    let mut emitted_symbols = Vec::new();
    for _ in 0..3 {
        let mut drained = graph
            .execute_checkpoint_drain_cycle(i64::MAX, None)
            .await
            .unwrap();
        for output_batch in drained.remove("output").unwrap_or_default() {
            let symbols = output_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            emitted_symbols
                .extend((0..output_batch.num_rows()).map(|row| symbols.value(row).to_string()));
        }
        if graph.checkpoint_is_quiescent() {
            break;
        }
    }

    assert_eq!(graph.checkpoint_pending_input_bytes(), 0);
    assert!(graph.checkpoint_is_quiescent());
    assert_eq!(emitted_symbols, ["AAPL", "GOOG"]);

    let after_quiescence = graph
        .execute_checkpoint_drain_cycle(i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(
        total_rows(&after_quiescence, "output"),
        0,
        "a drained edge is not replayed"
    );
}

#[tokio::test]
async fn checkpoint_drain_accounting_includes_deferred_source_ports() {
    let mut graph = test_graph();
    graph
        .place_operator_node("delay", Box::new(DelayOperator), 1)
        .unwrap();
    let source_a = graph.ensure_source_node("source_a");
    let source_b = graph.ensure_source_node("source_b");
    let output_a = graph
        .place_operator_node("output_a", Box::new(SourcePassthrough), 1)
        .unwrap();
    let output_b = graph
        .place_operator_node("output_b", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source_a, output_a, 0);
    graph.add_edge(source_b, output_b, 0);
    graph.output_map.insert(Arc::from("output_a"), output_a);
    graph.output_map.insert(Arc::from("output_b"), output_b);
    graph.topo_dirty = true;
    graph.set_query_budget_ns(1);

    let batch = test_batch();
    let batch_bytes = batch.get_array_memory_size();
    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("source_a"), vec![batch.clone()]);
    sources.insert(Arc::from("source_b"), vec![batch]);

    let normal = graph.execute_cycle(&sources, 10, None).await.unwrap();
    assert!(normal.is_empty());
    assert_eq!(graph.input_bufs[source_b][0].len(), 1);
    assert_eq!(
        graph.checkpoint_pending_input_bytes(),
        batch_bytes.saturating_mul(2),
        "one routed edge and one budget-deferred source port are both accounted"
    );
    assert!(!graph.checkpoint_is_quiescent());

    let drained = graph
        .execute_checkpoint_drain_cycle(10, None)
        .await
        .unwrap();
    assert_eq!(total_rows(&drained, "output_a"), 2);
    assert_eq!(total_rows(&drained, "output_b"), 2);
    assert!(graph.checkpoint_is_quiescent());
}

#[tokio::test]
async fn checkpoint_drain_quiescence_detects_zero_byte_row_batch() {
    let mut graph = test_graph();
    let source = graph.ensure_source_node("empty_schema_source");
    let output = graph
        .place_operator_node("output", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source, output, 0);
    graph.output_map.insert(Arc::from("output"), output);
    graph.topo_dirty = true;

    let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(3));
    let zero_byte_rows =
        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), Vec::new(), &options).unwrap();
    assert_eq!(zero_byte_rows.num_rows(), 3);
    assert_eq!(zero_byte_rows.get_array_memory_size(), 0);
    prefill_port(&mut graph, output, 0, vec![zero_byte_rows]);

    assert_eq!(graph.checkpoint_pending_input_bytes(), 0);
    assert!(!graph.checkpoint_is_quiescent());

    let drained = graph
        .execute_checkpoint_drain_cycle(10, None)
        .await
        .unwrap();
    assert_eq!(total_rows(&drained, "output"), 3);
    assert!(graph.checkpoint_is_quiescent());
}

#[tokio::test]
async fn checkpoint_drain_does_not_poll_unrelated_aggregate_branch() {
    let mut graph = test_graph();
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut trades = FxHashMap::default();
    trades.insert(Arc::from("trades"), vec![test_batch()]);
    let initial = graph.execute_cycle(&trades, 10, None).await.unwrap();
    assert_eq!(total_rows(&initial, "agg"), 2);
    assert!(graph.checkpoint_is_quiescent());

    let other_source = graph.ensure_source_node("other");
    let other_output = graph
        .place_operator_node("other_output", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(other_source, other_output, 0);
    graph
        .output_map
        .insert(Arc::from("other_output"), other_output);
    graph.topo_dirty = true;
    prefill_port(&mut graph, other_source, 0, vec![test_batch()]);

    let drained = graph
        .execute_checkpoint_drain_cycle(10, None)
        .await
        .unwrap();
    assert_eq!(total_rows(&drained, "other_output"), 2);
    assert_eq!(
        total_rows(&drained, "agg"),
        0,
        "the unchanged aggregate branch must not re-emit during another branch's drain"
    );
    assert!(graph.checkpoint_is_quiescent());
}

#[tokio::test]
async fn checkpoint_drain_failure_or_no_progress_preserves_pending_edges() {
    struct PausedOperator;

    #[async_trait]
    impl GraphOperator for PausedOperator {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            assert!(inputs.is_empty(), "paused operator must not accept input");
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }

        fn wants_input(&self) -> bool {
            false
        }
    }

    let mut graph = test_graph();
    let source = graph.ensure_source_node("trades");
    let middle = graph
        .place_operator_node("middle", Box::new(SourcePassthrough), 1)
        .unwrap();
    let paused = graph
        .place_operator_node("paused", Box::new(PausedOperator), 1)
        .unwrap();
    graph.add_edge(source, middle, 0);
    graph.add_edge(middle, paused, 0);
    graph.topo_dirty = true;
    prefill_port(&mut graph, middle, 0, vec![test_batch()]);
    prefill_port(&mut graph, paused, 0, vec![test_batch()]);
    graph.set_max_input_buf_batches(1);

    let pending_before = graph.checkpoint_pending_input_bytes();
    assert_eq!(pending_before, 2 * test_batch().get_array_memory_size());
    assert!(!graph.checkpoint_is_quiescent());

    graph.set_backpressure_policy(BackpressurePolicy::Fail);
    let error = graph
        .execute_checkpoint_drain_cycle(i64::MAX, None)
        .await
        .expect_err("the checkpoint drain must preserve Fail backpressure semantics");
    assert!(matches!(error, DbError::BackpressureFail(_)));
    assert!(
        graph.execution_poison_reason().is_none(),
        "an explicit returned error must retain its disposition without looking like cancellation"
    );
    assert_eq!(graph.checkpoint_pending_input_bytes(), pending_before);
    assert!(!graph.checkpoint_is_quiescent());

    graph.set_backpressure_policy(BackpressurePolicy::Backpressure);
    graph
        .execute_checkpoint_drain_cycle(i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(
        graph.checkpoint_pending_input_bytes(),
        pending_before,
        "a gated/paused drain cycle must not clear pending edge buffers"
    );
    assert_eq!(
        graph.output_watermarks[paused],
        i64::MIN,
        "an operator that declined buffered input must not advance its output watermark"
    );
    assert!(!graph.checkpoint_is_quiescent());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cancellation_while_waiting_for_rotation_does_not_poison_graph() {
    let mut graph = test_graph();
    let fence = Arc::new(tokio::sync::RwLock::new(()));
    graph.set_rotation_execution_fence(Arc::clone(&fence));
    let rotation = fence.write().await;
    let empty = FxHashMap::default();
    let mut cycle = Box::pin(graph.execute_cycle(&empty, i64::MIN, None));

    assert!(
        matches!(futures::poll!(&mut cycle), std::task::Poll::Pending),
        "the graph cycle must be waiting behind vnode rotation"
    );
    drop(cycle);
    drop(rotation);

    assert!(graph.execution_poison_reason().is_none());
    graph
        .execute_cycle(&empty, i64::MIN, None)
        .await
        .expect("no state or graph input was admitted before the rotation fence");
}

#[tokio::test]
async fn cancelled_stateful_cycle_poison_requires_fresh_graph_restore() {
    let mut graph = asof_execution_test_graph();
    graph
        .execute_cycle(&asof_sources(false, Some((10, 10.0))), i64::MIN, None)
        .await
        .unwrap();
    let checkpoint = graph
        .snapshot_state()
        .unwrap()
        .expect("ASOF right state should be checkpointed");
    let checkpoint = OperatorGraph::serialize_checkpoint_bounded(&checkpoint, u64::MAX).unwrap();

    let (entered_tx, mut entered_rx) = tokio::sync::oneshot::channel();
    append_asof_downstream_probe(
        &mut graph,
        Box::new(SignalThenPendingOperator {
            entered: Some(entered_tx),
        }),
    );
    let replay = asof_sources(true, Some((20, 20.0)));
    let mut cycle = Box::pin(graph.execute_cycle(&replay, i64::MIN, None));
    let observation = tokio::select! {
        entered = &mut entered_rx => entered.expect("pending probe dropped its signal"),
        result = &mut cycle => panic!("cycle completed before cancellation: {result:?}"),
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            panic!("stateful output did not reach the pending probe")
        }
    };
    assert_eq!(
        observation,
        (1, Some(20.0)),
        "the cancelled pass must route the newly admitted ASOF quote"
    );
    drop(cycle);

    let snapshot_error = match graph.snapshot_state() {
        Err(error) => error,
        Ok(_) => panic!("cancelled graph generation accepted a checkpoint"),
    };
    assert_graph_execution_poison(&snapshot_error);
    #[cfg(feature = "cluster")]
    {
        let vnode_error = graph
            .snapshot_state_by_vnode()
            .expect_err("cancelled graph generation accepted a vnode checkpoint");
        assert_graph_execution_poison(&vnode_error);
    }

    let execution_error = graph
        .execute_cycle(&FxHashMap::default(), i64::MIN, None)
        .await
        .expect_err("cancelled graph generation executed again");
    assert_graph_execution_poison(&execution_error);
    let drain_error = graph
        .execute_checkpoint_drain_cycle(i64::MIN, None)
        .await
        .expect_err("cancelled graph generation entered checkpoint drain");
    assert_graph_execution_poison(&drain_error);

    let (mut restored, restored_operators) = asof_execution_test_graph()
        .restore_from_bytes(&checkpoint)
        .unwrap();
    assert_eq!(restored_operators, 1);
    let output = restored
        .execute_cycle(&replay, i64::MIN, None)
        .await
        .unwrap();
    let batches = output.get("asof").expect("replayed ASOF output");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let bid = batches[0]
        .column_by_name("bid")
        .expect("ASOF bid")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 bid");
    assert_eq!(
        bid.value(0),
        20.0,
        "fresh prior-cut restore must observe the newer replayed quote"
    );
}

#[tokio::test]
async fn caught_stateful_cycle_panic_poison_prevents_graph_reuse() {
    use futures::FutureExt as _;

    let mut graph = asof_execution_test_graph();
    graph
        .execute_cycle(&asof_sources(false, Some((10, 10.0))), i64::MIN, None)
        .await
        .unwrap();
    let observation = Arc::new(parking_lot::Mutex::new(None));
    append_asof_downstream_probe(
        &mut graph,
        Box::new(PanicAfterInputOperator(Arc::clone(&observation))),
    );
    let replay = asof_sources(true, Some((20, 20.0)));

    let panic = std::panic::AssertUnwindSafe(graph.execute_cycle(&replay, i64::MIN, None))
        .catch_unwind()
        .await;
    assert!(panic.is_err(), "the downstream probe must panic");
    assert_eq!(*observation.lock(), Some((1, Some(20.0))));

    let snapshot_error = match graph.snapshot_state() {
        Err(error) => error,
        Ok(_) => panic!("panicked graph generation accepted a checkpoint"),
    };
    assert_graph_execution_poison(&snapshot_error);
    let execution_error = graph
        .execute_cycle(&FxHashMap::default(), i64::MIN, None)
        .await
        .expect_err("panicked graph generation executed again");
    assert_graph_execution_poison(&execution_error);
}

#[tokio::test]
async fn test_og_checkpoint_roundtrip_aggregate() {
    // Aggregate state should survive checkpoint + restore
    let mut graph = test_graph();
    graph.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // Cycle 1: build up state
    let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();

    // Snapshot
    let cp = graph
        .snapshot_state()
        .unwrap()
        .expect("aggregate should have state");
    let bytes = OperatorGraph::serialize_checkpoint_bounded(&cp, u64::MAX).unwrap();

    // Create a new graph with same query and restore
    let mut graph2 = test_graph();
    graph2.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let (restored_graph, restored) = graph2.restore_from_bytes(&bytes).unwrap();
    let mut graph2 = restored_graph;
    assert!(restored > 0, "should restore at least one operator");

    // New input is applied on top of the authoritative restored image.
    let r = graph2.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "agg"), 2);
}

#[tokio::test]
async fn test_og_aggregate_empty_source_emits_state() {
    // Aggregate queries should emit running state even with no new input
    let mut graph = test_graph();
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // First cycle with data
    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "agg"), 2);

    // Second cycle with no data — should still emit accumulated state
    let empty_source = FxHashMap::default();
    let r2 = graph
        .execute_cycle(&empty_source, i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(total_rows(&r2, "agg"), 2);
}

#[tokio::test]
async fn test_og_reverse_order_cascading() {
    // Queries added in reverse dependency order (q2 before q1).
    // q2 creates a SourcePassthrough placeholder for "q1". When q1 is
    // added, it replaces the placeholder in place so q2's existing edge
    // automatically receives q1's real output.
    let mut graph = test_graph();
    graph.add_query(
        "q2".to_string(),
        "SELECT symbol FROM q1 WHERE price > 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q1".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // "q1" should NOT be in source_map (it was replaced with a real query)
    assert!(
        !graph.source_map.contains_key("q1"),
        "q1 placeholder should be replaced, not in source_map"
    );
    assert!(graph.output_map.contains_key("q1"));
    assert!(graph.output_map.contains_key("q2"));

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "q1"), 2); // AAPL + GOOG
    assert_eq!(total_rows(&r, "q2"), 1); // Only GOOG (price=2800 > 200)
}

#[tokio::test]
async fn test_temporal_probe_through_graph() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    let trades_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let market_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("mts", DataType::Int64, false),
        Field::new("mprice", DataType::Float64, false),
    ]));

    graph.register_source_schema("trades".to_string(), trades_schema.clone());
    graph.register_source_schema("market_data".to_string(), market_schema);

    graph.add_query(
        "probed".to_string(),
        "SELECT t.symbol, p.offset_ms, mprice \
         FROM trades t \
         TEMPORAL PROBE JOIN market_data m ON (symbol) \
         TIMESTAMPS (ts, mts) LIST (0s, 5s) AS p"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // Cycle 1: inject both sides, watermark=102k (only offset=0 resolves)
    let trades = RecordBatch::try_new(
        trades_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["AAPL"])),
            Arc::new(Int64Array::from(vec![100_000])),
            Arc::new(Float64Array::from(vec![152.5])),
        ],
    )
    .unwrap();
    let market = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("mts", DataType::Int64, false),
            Field::new("mprice", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["AAPL", "AAPL"])),
            Arc::new(Int64Array::from(vec![100_000, 105_000])),
            Arc::new(Float64Array::from(vec![150.0, 155.0])),
        ],
    )
    .unwrap();

    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("trades"), vec![trades]);
    sources.insert(Arc::from("market_data"), vec![market]);

    let r1 = graph.execute_cycle(&sources, 102_000, None).await.unwrap();
    let rows1 = total_rows(&r1, "probed");
    assert_eq!(rows1, 1, "only offset=0 should resolve at watermark=102k");

    // Cycle 2: no new data, advance watermark past offset=5000 (probe_ts=105000)
    let empty = FxHashMap::default();
    let r2 = graph.execute_cycle(&empty, 110_000, None).await.unwrap();
    let rows2 = total_rows(&r2, "probed");
    assert_eq!(rows2, 1, "offset=5000 should resolve at watermark=110k");
}

#[test]
fn test_pressure_zero_when_cap_disabled() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(0); // unlimited
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    // Push some data into the source buffer
    if let Some(&node_id) = graph.source_map.get("trades") {
        prefill_port(&mut graph, node_id, 0, vec![test_batch(); 10]);
    }
    assert!((graph.input_buf_pressure() - 0.0).abs() < f64::EPSILON);
}

#[test]
fn test_pressure_reflects_fill_ratio() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(100);
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    // Fill source buffer to 50% of cap
    if let Some(&node_id) = graph.source_map.get("trades") {
        prefill_port(&mut graph, node_id, 0, vec![test_batch(); 50]);
    }
    assert!((graph.input_buf_pressure() - 0.5).abs() < f64::EPSILON);
}

#[test]
fn test_pressure_clamped_at_one() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(10);
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    // Overfill the buffer beyond cap — pressure clamps at 1.0.
    if let Some(&node_id) = graph.source_map.get("trades") {
        prefill_port(&mut graph, node_id, 0, vec![test_batch(); 20]);
    }
    assert!((graph.input_buf_pressure() - 1.0).abs() < f64::EPSILON);
}

#[test]
fn test_pressure_empty_graph() {
    let graph = test_graph();
    assert!((graph.input_buf_pressure() - 0.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_credit_gate_defers_producer_when_downstream_full() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(4);

    // Two queries chained via an intermediate stream: the first projects
    // `trades`, the second reads from the first. The gate should skip the
    // first when the second's input port is full.
    graph.add_query(
        "proj".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "downstream".to_string(),
        "SELECT symbol FROM proj".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // Find the downstream node id and pre-fill its input buffer at cap,
    // simulating a slow consumer.
    let downstream_id = *graph.output_map.get("downstream").unwrap();
    prefill_port(&mut graph, downstream_id, 0, vec![test_batch(); 4]);

    let proj_id = *graph.output_map.get("proj").unwrap();
    assert!(
        graph.is_downstream_at_capacity(proj_id),
        "proj's downstream should register as at capacity"
    );

    // Run a cycle with trade input. proj must be deferred because its
    // downstream is full — so proj's output_bufs should still hold its
    // source input, and downstream's input should not grow.
    let before_len = graph.input_bufs[downstream_id][0].len();
    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);
    let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(
        graph.input_bufs[downstream_id][0].len(),
        before_len,
        "deferred producer must not have extended a full downstream buffer"
    );
}

// Replacing a SourcePassthrough placeholder must also clear source_node_ids,
// otherwise the node keeps its source-class flag and output_watermarks is
// never advanced — downstream TUMBLE windows never close.
#[tokio::test]
async fn test_placeholder_replacement_clears_source_classification() {
    let mut graph = test_graph();

    // Register the downstream query FIRST — its SQL references
    // `derived`, which triggers an `ensure_source_node("derived")` and
    // seeds `source_node_ids` with the placeholder.
    graph.add_query(
        "aggregate".to_string(),
        "SELECT symbol, SUM(price) AS total FROM derived GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // Now register `derived` — this replaces the placeholder.
    graph.add_query(
        "derived".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let derived_id = *graph.output_map.get("derived").unwrap();
    assert!(
        !graph.source_node_ids.contains(&derived_id),
        "real operator node must not be classified as a source after \
         placeholder replacement (blocks output_watermarks updates)"
    );
}

#[tokio::test]
async fn test_source_inputs_accumulate_when_deferred() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(2);
    graph.add_query(
        "sink".to_string(),
        "SELECT symbol FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // Pre-fill sink's input at cap. Because sink has no downstream, sink
    // will still run this cycle — so to keep trades deferred across a
    // second cycle we keep the cap threshold tight and re-fill sink each
    // cycle, simulating a continuous slow-consumer scenario.
    let sink_id = *graph.output_map.get("sink").unwrap();
    let source_id = *graph.source_map.get("trades").unwrap();
    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // Cycle 1: sink's input pre-filled to cap, trades deferred, trades
    // input extended by 1.
    prefill_port(&mut graph, sink_id, 0, vec![test_batch(); 2]);
    let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(
        graph.input_bufs[source_id][0].len(),
        1,
        "deferred source must accumulate its input buffer"
    );

    // Cycle 2: re-fill sink to cap so trades stays deferred; trades input
    // must grow from 1 to 2 (extend, not clone_from).
    prefill_port(&mut graph, sink_id, 0, vec![test_batch(); 2]);
    let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(
        graph.input_bufs[source_id][0].len(),
        2,
        "source input must accumulate across deferred cycles"
    );
}

/// Regression test: LEFT JOIN between a streaming source and a
/// `ReferenceTableProvider` (lookup table) must work across multiple
/// cycles without panicking. Before the fix, `RepartitionExec` in the
/// cached physical plan had consumed internal channels on the first
/// cycle, causing `"partition not used yet"` on the second.
#[tokio::test]
async fn test_lookup_left_join_multi_cycle() {
    use crate::table_store::TableStore;

    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);

    // Register a lookup table via ReferenceTableProvider
    let lookup_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("company_name", DataType::Utf8, true),
    ]));
    let ts = Arc::new(parking_lot::RwLock::new(TableStore::new()));
    {
        let mut store = ts.write();
        store
            .create_table("instruments", lookup_schema.clone(), "symbol")
            .unwrap();
        let batch = RecordBatch::try_new(
            lookup_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(StringArray::from(vec!["Apple Inc.", "Alphabet"])),
            ],
        )
        .unwrap();
        store.upsert("instruments", &batch).unwrap();
    }
    let provider = crate::table_provider::ReferenceTableProvider::new(
        "instruments".to_string(),
        lookup_schema,
        ts,
    );
    ctx.register_table("instruments", Arc::new(provider))
        .unwrap();

    let mut graph = OperatorGraph::new(ctx);
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.set_reference_tables(["instruments".to_string()].into_iter().collect());

    graph.add_query(
        "enriched".to_string(),
        "SELECT t.symbol, t.price, i.company_name \
         FROM trades t LEFT JOIN instruments i ON t.symbol = i.symbol"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let batch = test_batch(); // AAPL + GOOG
    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![batch.clone()]);

    // Cycle 1
    let r1 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    let rows1: usize = r1
        .get("enriched")
        .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
    assert_eq!(rows1, 2, "cycle 1 should produce 2 joined rows");

    // Cycle 2 — this panicked before the fix
    source.insert(Arc::from("trades"), vec![batch]);
    let r2 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    let rows2: usize = r2
        .get("enriched")
        .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
    assert_eq!(rows2, 2, "cycle 2 should also produce 2 joined rows");
}

#[tokio::test]
async fn test_self_join_prefilter_end_to_end() {
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::TimeUnit;

    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    graph.register_source_schema("events".to_string(), Arc::clone(&schema));

    graph.add_query(
        "joined".to_string(),
        "SELECT p.key, p.type, a.type \
         FROM events p \
         JOIN events a ON p.key = a.key \
         AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
         WHERE p.type = 'A' AND a.type = 'B'"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // source + 2 filter nodes + join operator = 4
    assert!(
        graph.nodes.len() >= 4,
        "expected 4+ nodes, got {}",
        graph.nodes.len()
    );

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["k1", "k1", "k1", "k1"])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "B"])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1000, 2000, 3000, 4000,
            ])),
        ],
    )
    .unwrap();

    let mut source = FxHashMap::default();
    source.insert(Arc::from("events"), vec![batch.clone()]);

    // First cycle seeds the join buffers; second cycle produces matches
    // when buffered left (type=A) rows see right (type=B) rows. Keep the
    // watermark below the rows so the first cycle does not close their interval.
    let _ = graph.execute_cycle(&source, 0, None).await.unwrap();

    source.clear();
    source.insert(Arc::from("events"), vec![batch]);
    let results = graph.execute_cycle(&source, 0, None).await.unwrap();

    let total_rows: usize = results
        .get("joined")
        .map_or(0, |batches| batches.iter().map(|b| b.num_rows()).sum());

    assert!(
        total_rows > 0,
        "should produce matches from prefiltered self-join"
    );
}

fn prefill_port(graph: &mut OperatorGraph, node: usize, port: usize, batches: Vec<RecordBatch>) {
    let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    graph.input_bufs[node][port] = batches;
    graph.input_buf_bytes[node][port] = bytes;
}

fn producer_consumer_graph(policy: BackpressurePolicy, cap: usize) -> (OperatorGraph, usize) {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(cap);
    graph.set_backpressure_policy(policy);
    graph.add_query(
        "producer".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "consumer".to_string(),
        "SELECT symbol FROM producer".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let consumer_id = *graph.output_map.get("consumer").unwrap();
    prefill_port(&mut graph, consumer_id, 0, vec![test_batch(); cap]);
    (graph, consumer_id)
}

fn trades_source() -> FxHashMap<Arc<str>, Vec<RecordBatch>> {
    let mut s = FxHashMap::default();
    s.insert(Arc::from("trades"), vec![test_batch()]);
    s
}

#[tokio::test]
async fn test_backpressure_policy_defers_without_shedding() {
    let (mut graph, consumer_id) = producer_consumer_graph(BackpressurePolicy::Backpressure, 2);
    let _ = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(
        graph.input_bufs[consumer_id][0].len(),
        2,
        "consumer input stays at cap — producer must have been deferred"
    );
}

#[tokio::test]
async fn test_shed_oldest_policy_drops_rows_and_increments_counter() {
    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    let (mut graph, consumer_id) = producer_consumer_graph(BackpressurePolicy::ShedOldest, 2);
    graph.set_metrics(Arc::clone(&prom));

    let _ = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .unwrap();

    assert!(graph.input_bufs[consumer_id][0].len() <= 2);
    assert!(
        prom.shed_records_total
            .with_label_values(&["consumer"])
            .get()
            > 0,
        "shed_records_total should have incremented"
    );
}

#[tokio::test]
async fn test_fail_policy_returns_error_at_cap() {
    let (mut graph, _) = producer_consumer_graph(BackpressurePolicy::Fail, 2);
    let err = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .expect_err("Fail policy must return an error at capacity");
    assert!(
        matches!(err, DbError::BackpressureFail(_)),
        "expected DbError::BackpressureFail, got {err:?}"
    );
}

#[tokio::test]
async fn test_byte_budget_gates_capacity() {
    let mut graph = test_graph();
    graph.set_max_input_buf_bytes(Some(1));
    graph.add_query(
        "producer".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "consumer".to_string(),
        "SELECT symbol FROM producer".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let consumer_id = *graph.output_map.get("consumer").unwrap();
    prefill_port(&mut graph, consumer_id, 0, vec![test_batch()]);

    let producer_id = *graph.output_map.get("producer").unwrap();
    assert!(graph.is_downstream_at_capacity(producer_id));
}
