use super::*;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use laminar_connectors::connector::{
    CoordinatedCommitter as CommitterTrait, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, WriteResult,
};
use laminar_connectors::error::ConnectorError;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::CheckpointWatermark;
#[cfg(feature = "cluster")]
use laminar_core::state::ObjectStoreBackend;
use laminar_core::state::{InProcessBackend, StateBackend};
use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt, PutPayload};
use parking_lot::Mutex;

#[cfg(feature = "cluster")]
use crate::cluster_recovery_capsule::{
    participant_ready_key, ParticipantReady, PARTICIPANT_READY_VERSION,
};
use crate::sink_task::{SinkTaskConfig, SinkTaskHandle, DEFAULT_CHANNEL_CAPACITY};

type Recorded = Arc<Mutex<Vec<CoordinatedCommitBatch>>>;
type ExternalCursor = Arc<Mutex<Option<CoordinatedCommitCursor>>>;
const TEST_SINK_ID: &str = "external";

#[test]
fn prune_floor_rejects_epoch_exhaustion() {
    let error = checked_committer_floor_after(u64::MAX).unwrap_err();
    assert!(matches!(error, DbError::Checkpoint(_)));
    assert!(error.to_string().contains("epoch space exhausted"));
    assert_eq!(
        checked_committer_floor_after(u64::MAX - 1).unwrap(),
        u64::MAX
    );
}

struct RecordingSink {
    schema: SchemaRef,
    recorded: Recorded,
    committed: ExternalCursor,
}

#[async_trait::async_trait]
impl SinkConnector for RecordingSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(0, 0))
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
    fn as_coordinated_committer(&self) -> Option<&dyn CommitterTrait> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl CommitterTrait for RecordingSink {
    async fn commit_aggregated(
        &self,
        batch: CoordinatedCommitBatch,
        _context: laminar_connectors::connector::CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        *self.committed.lock() = Some(CoordinatedCommitCursor {
            checkpoint_id: batch.target.checkpoint_id,
            fencing_token: batch.fencing_token,
        });
        self.recorded.lock().push(batch);
        Ok(())
    }

    async fn committed_cursor(
        &self,
        _namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
        Ok(*self.committed.lock())
    }
}

fn spawn_recording_sink_with_cursor(
    recorded: Recorded,
    committed: ExternalCursor,
) -> SinkTaskHandle {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _rx) =
        laminar_core::streaming::channel::channel(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    SinkTaskHandle::spawn(SinkTaskConfig {
        name: TEST_SINK_ID.into(),
        sink_id: Arc::from(TEST_SINK_ID),
        connector: Box::new(RecordingSink {
            schema,
            recorded,
            committed,
        }),
        contract: SinkContract::new(
            SinkConsistency::CheckpointCommittable,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    })
}

fn spawn_recording_sink(recorded: Recorded) -> SinkTaskHandle {
    spawn_recording_sink_with_cursor(recorded, Arc::new(Mutex::new(None)))
}

fn external_cursor(checkpoint_id: u64, fencing_token: u64) -> ExternalCursor {
    Arc::new(Mutex::new(Some(CoordinatedCommitCursor {
        checkpoint_id,
        fencing_token,
    })))
}

fn identity() -> PipelineIdentity {
    PipelineIdentity::empty()
}

fn deployment_id() -> String {
    "018f0000-0000-7000-8000-000000000001".into()
}

fn namespace() -> CoordinatedCommitNamespace {
    CoordinatedCommitNamespace::try_new(identity(), deployment_id(), TEST_SINK_ID).unwrap()
}

#[cfg(feature = "cluster")]
fn descriptor_object_path(attempt: CheckpointAttempt, key: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "state-v2/epoch={}/checkpoint={}/commit/{key}",
        attempt.epoch, attempt.checkpoint_id
    ))
}

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

fn leader_proof(
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    node_id: u64,
    process_term: u64,
    fencing_token: u64,
) -> laminar_core::checkpoint::LeaderProof {
    laminar_core::checkpoint::LeaderProof {
        owner: laminar_core::checkpoint::LeaderProofOwner {
            node_id,
            boot_id: fence
                .participant_incarnation(node_id)
                .expect("test leader belongs to the assignment certificate"),
            process_term,
        },
        fencing_token,
    }
}

async fn seal<B: StateBackend>(
    backend: &Arc<B>,
    attempt: CheckpointAttempt,
    markers: &[(u64, Option<&[u8]>)],
) {
    seal_with_fence(backend, attempt, markers, &[], None, None).await;
}

async fn seal_with_fence<B: StateBackend>(
    backend: &Arc<B>,
    attempt: CheckpointAttempt,
    markers: &[(u64, Option<&[u8]>)],
    readiness_participants: &[u64],
    assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
    leader_proof: Option<&laminar_core::checkpoint::LeaderProof>,
) {
    assert_eq!(
        assignment_fence.is_some(),
        leader_proof.is_some(),
        "cluster test descriptors require their exact outcome leader proof"
    );
    let assignment_version = assignment_fence.map_or(0, |fence| fence.assignment_version);
    backend.set_authoritative_version(assignment_version);
    let namespace = namespace();
    let mut keys = Vec::new();
    let mut required_vnodes = Vec::new();
    let vnode_owners = assignment_fence.map_or_else(Vec::new, |fence| {
        let owners = fence.participant_ids();
        assert!(
            fence.matches_owner_map(&owners),
            "test assignment helper uses one vnode per participant"
        );
        owners
    });
    if let Some(fence) = assignment_fence {
        for (vnode, &owner) in vnode_owners.iter().enumerate() {
            let vnode = u32::try_from(vnode).unwrap();
            let payload = Bytes::from_static(b"test-vnode-state");
            backend
                .write_certified_partial(
                    attempt,
                    vnode,
                    fence,
                    owner,
                    laminar_core::state::VnodePartialLineage::root(payload.len() as u64),
                    payload,
                )
                .await
                .unwrap();
            required_vnodes.push(vnode);
        }
    }
    for &(participant_id, payload) in markers {
        let key = descriptor_key(&namespace, participant_id);
        let marker = encode_prepared_marker(&namespace, attempt, participant_id, payload).unwrap();
        match (assignment_fence, leader_proof) {
            (Some(fence), Some(proof)) => backend
                .write_certified_commit_descriptor(
                    attempt,
                    &key,
                    fence,
                    participant_id,
                    proof,
                    Bytes::from(marker),
                )
                .await
                .unwrap(),
            (None, None) => backend
                .write_commit_descriptor(attempt, &key, Bytes::from(marker))
                .await
                .unwrap(),
            _ => unreachable!("descriptor provenance shape was checked above"),
        }
        keys.push(key);
    }
    #[cfg(feature = "cluster")]
    for &participant_id in readiness_participants {
        let ready_key = participant_ready_key(participant_id);
        let ready = ParticipantReady {
            version: PARTICIPANT_READY_VERSION,
            attempt,
            participant_id,
            assignment_fence: assignment_fence
                .expect("readiness requires an assignment fence")
                .clone(),
            deployment_id: deployment_id(),
            pipeline_identity: identity(),
            vnode_restore_limits: crate::cluster_recovery_capsule::vnode_restore_limits_for_test(
                assignment_fence
                    .expect("readiness requires an assignment fence")
                    .vnode_count,
            ),
            owned_vnodes: vnode_owners
                .iter()
                .enumerate()
                .filter_map(|(vnode, owner)| {
                    (*owner == participant_id).then(|| u32::try_from(vnode).unwrap())
                })
                .collect(),
            source_offsets: Default::default(),
            source_metadata: Default::default(),
            source_assignment_versions: Default::default(),
            source_watermarks: Default::default(),
            local_watermark: CheckpointWatermark::Uninitialized,
            manifest_sha256: format!("{participant_id:064x}"),
            portable_state_sha256: identity().sha256,
        };
        backend
            .write_certified_commit_descriptor(
                attempt,
                &ready_key,
                assignment_fence.expect("readiness requires an assignment fence"),
                participant_id,
                leader_proof.expect("readiness requires an exact leader proof"),
                Bytes::from(canonical_json_bytes(&ready).unwrap()),
            )
            .await
            .unwrap();
        keys.push(ready_key);
    }
    #[cfg(not(feature = "cluster"))]
    let _ = readiness_participants;
    keys.sort_unstable();
    assert!(backend
        .seal_checkpoint(attempt, assignment_fence, &required_vnodes, &keys)
        .await
        .unwrap());
}

async fn decisions_on(
    store: Arc<dyn ObjectStore>,
) -> Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore> {
    let identity_json = format!(
        r#"{{"version":2,"id":"{}","allocator_mode":"native_cas","checkpoint_id":0,"allocation_id":"018f0000-0000-7000-8000-000000000002"}}"#,
        deployment_id()
    );
    store
        .put(
            &object_store::path::Path::from("checkpoint-deployment/identity.json"),
            PutPayload::from_bytes(Bytes::from(identity_json)),
        )
        .await
        .unwrap();
    Arc::new(laminar_core::checkpoint_decision::CheckpointDecisionStore::new(store))
}

async fn decisions() -> Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore> {
    decisions_on(Arc::new(InMemory::new())).await
}

#[cfg(feature = "cluster")]
struct ClusterDecisions {
    backing: Arc<dyn ObjectStore>,
    capsules: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    controller: Arc<laminar_core::cluster::control::ClusterController>,
    owner: laminar_core::cluster::control::LeaderLeaseOwner,
    proof: laminar_core::checkpoint::LeaderProof,
}

#[cfg(feature = "cluster")]
impl std::ops::Deref for ClusterDecisions {
    type Target = laminar_core::checkpoint_decision::CheckpointDecisionStore;

    fn deref(&self) -> &Self::Target {
        &self.capsules
    }
}

#[cfg(feature = "cluster")]
fn cluster_controller(
    authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    owner: &laminar_core::cluster::control::LeaderLeaseOwner,
    lease: laminar_core::cluster::control::LeaderLease,
) -> Arc<laminar_core::cluster::control::ClusterController> {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv, LeaseDeadline};
    use tokio::sync::watch;

    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(owner.node));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(owner.node, kv, None, members_rx));
    controller.set_leader_lease_store(authority);
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let (_lease_tx, lease_rx) = watch::channel(Some(lease));
    controller
        .set_leader_lease_watch(
            lease_rx,
            owner.clone(),
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
        )
        .unwrap();
    controller
}

#[cfg(feature = "cluster")]
async fn cluster_decisions(
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_id: u64,
) -> ClusterDecisions {
    use laminar_core::cluster::control::{LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome};

    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let capsules = decisions_on(Arc::clone(&backing)).await;
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));
    let owner = LeaderLeaseOwner {
        node: laminar_core::cluster::discovery::NodeId(leader_id),
        boot: fence
            .participant_incarnation(leader_id)
            .expect("test leader belongs to the assignment certificate"),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        unreachable!("fresh test authority must grant its first lease")
    };
    let proof = lease.proof();
    let controller = cluster_controller(Arc::clone(&authority), &owner, lease);
    ClusterDecisions {
        backing,
        capsules,
        authority,
        controller,
        owner,
        proof,
    }
}

#[cfg(feature = "cluster")]
async fn advance_cluster_term(
    decisions: &mut ClusterDecisions,
    next_owner: laminar_core::cluster::control::LeaderLeaseOwner,
) {
    use laminar_core::cluster::control::LeaseOutcome;

    let current = decisions.authority.load().await.unwrap().unwrap();
    let observation = decisions
        .authority
        .observe_rival(&next_owner, &current)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let LeaseOutcome::Acquired(lease) = decisions
        .authority
        .try_takeover(&next_owner, &observation, 0)
        .await
        .unwrap()
    else {
        panic!("replacement test term must acquire")
    };
    decisions.proof = lease.proof();
    decisions.controller = cluster_controller(Arc::clone(&decisions.authority), &next_owner, lease);
    decisions.owner = next_owner;
}

async fn record_local_commit(
    store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    checkpoint_id: u64,
) {
    store
        .record_outcome(
            checkpoint_id,
            checkpoint_id,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            None,
        )
        .await
        .unwrap();
}

async fn record_local_abort(
    store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    checkpoint_id: u64,
) {
    store
        .record_outcome(
            checkpoint_id,
            checkpoint_id,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
}

#[cfg(feature = "cluster")]
async fn record_cluster_commit<B: StateBackend>(
    store: &ClusterDecisions,
    backend: &Arc<B>,
    checkpoint_id: u64,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
) {
    record_cluster_commit_with_inventory_digest(store, backend, checkpoint_id, fence, None).await;
}

#[cfg(feature = "cluster")]
async fn record_cluster_commit_with_inventory_digest<B: StateBackend>(
    store: &ClusterDecisions,
    backend: &Arc<B>,
    checkpoint_id: u64,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    seal_inventory_sha256: Option<String>,
) {
    let attempt = CheckpointAttempt::canonical(checkpoint_id);
    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .expect("cluster test commit has an exact seal");
    let mut readiness = Vec::new();
    for participant_id in fence.participant_ids() {
        let key = participant_ready_key(participant_id);
        let bytes = backend
            .read_commit_descriptor(attempt, &key)
            .await
            .unwrap()
            .expect("cluster test readiness descriptor exists");
        let ready = serde_json::from_slice::<ParticipantReady>(&bytes).unwrap();
        readiness.push((key, ready));
    }
    let mut capsule = assemble_capsule(
        &inventory,
        readiness,
        crate::cluster_recovery_capsule::declared_vnode_restore_contract_for_test(&inventory),
        &deployment_id(),
        &identity(),
        CheckpointWatermark::Uninitialized,
        None,
    )
    .unwrap();
    // One negative test deliberately binds the outcome to a different assignment. Preserve
    // that fixture while deriving every normal capsule from its exact readiness descriptors.
    capsule.assignment_fence = fence.clone();
    if let Some(seal_inventory_sha256) = seal_inventory_sha256 {
        capsule.seal_inventory_sha256 = seal_inventory_sha256;
    }
    let capsule_ref = store.create_recovery_capsule(&capsule).await.unwrap();
    store
        .authority
        .record_cluster_outcome(
            &store.proof,
            checkpoint_id,
            checkpoint_id,
            fence.clone(),
            CheckpointVerdict::Commit,
            Some(capsule_ref),
        )
        .await
        .unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn batches_sealed_epochs_into_one_commit() {
    let backend = Arc::new(InProcessBackend::new(2));
    let first = CheckpointAttempt::canonical(1);
    let second = CheckpointAttempt::canonical(2);
    let fence = assignment_fence(3, &[7, 9]);
    let decisions = cluster_decisions(&fence, 7).await;
    seal_with_fence(
        &backend,
        first,
        &[(7, Some(b"e1")), (9, None)],
        &[7, 9],
        Some(&fence),
        Some(&decisions.proof),
    )
    .await;
    seal_with_fence(
        &backend,
        second,
        &[(7, Some(b"e2")), (9, None)],
        &[7, 9],
        Some(&fence),
        Some(&decisions.proof),
    )
    .await;

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    record_cluster_commit(&decisions, &backend, 1, &fence).await;
    record_cluster_commit(&decisions, &backend, 2, &fence).await;
    let floor = Arc::new(AtomicU64::new(0));
    let mut committer = CoordinatedCommitter::new(
        Arc::clone(&backend) as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::clone(&floor),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    committer.commit_ready().await.unwrap();

    let batches = recorded.lock().clone();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].namespace, namespace());
    assert_eq!(
        batches[0].expected_predecessor,
        CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        }
    );
    assert_eq!(batches[0].fencing_token, 1);
    assert_eq!(batches[0].target, second);
    assert_eq!(batches[0].entries.len(), 4);
    assert_eq!(
        batches[0]
            .entries
            .iter()
            .map(|entry| (entry.attempt, entry.participant_id, entry.payload.clone()))
            .collect::<Vec<_>>(),
        vec![
            (first, 7, Some(b"e1".to_vec())),
            (first, 9, None),
            (second, 7, Some(b"e2".to_vec())),
            (second, 9, None),
        ]
    );
    assert_eq!(floor.load(Ordering::Acquire), 3);

    // A second pass with no new sealed epochs is a no-op (cursor advanced).
    committer.commit_ready().await.unwrap();
    assert_eq!(recorded.lock().len(), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn split_batches_use_their_flushed_targets_authority() {
    let backend = Arc::new(InProcessBackend::new(1));
    let attempts = [
        CheckpointAttempt::canonical(1),
        CheckpointAttempt::canonical(2),
        CheckpointAttempt::canonical(3),
    ];
    let tokens = [1, 2, 3];
    let fence = assignment_fence(3, &[7]);
    let mut outcomes = cluster_decisions(&fence, 7).await;
    for (index, attempt) in attempts.into_iter().enumerate() {
        assert_eq!(outcomes.proof.fencing_token, tokens[index]);
        seal_with_fence(
            &backend,
            attempt,
            &[(7, Some(b"payload"))],
            &[7],
            Some(&fence),
            Some(&outcomes.proof),
        )
        .await;
        record_cluster_commit(&outcomes, &backend, attempt.checkpoint_id, &fence).await;
        if index + 1 < attempts.len() {
            let next_owner = laminar_core::cluster::control::LeaderLeaseOwner {
                node: outcomes.owner.node,
                boot: outcomes.owner.boot,
                process_term: outcomes.owner.process_term + 1,
            };
            advance_cluster_term(&mut outcomes, next_owner).await;
        }
    }
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    let committer = CoordinatedCommitter::new(
        Arc::clone(&backend) as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle.clone())],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&outcomes.controller)))
    .with_decision_store(Some(Arc::clone(&outcomes.capsules)));
    let inventory = committer.load_commit_inventory().await.unwrap();

    let cursor = committer
        .commit_sealed_with_limits(
            &handle,
            TEST_SINK_ID,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            &inventory.attempts,
            attempts[2],
            &inventory.bindings,
            MAX_COORDINATED_COMMIT_BATCH_BYTES,
            1,
        )
        .await
        .unwrap();

    assert_eq!(
        cursor,
        CoordinatedCommitCursor {
            checkpoint_id: 3,
            fencing_token: 3,
        }
    );
    let batches = recorded.lock();
    assert_eq!(batches.len(), 3);
    assert_eq!(
        batches
            .iter()
            .map(|batch| (
                batch.expected_predecessor,
                batch.target.checkpoint_id,
                batch.fencing_token,
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                CoordinatedCommitCursor {
                    checkpoint_id: 0,
                    fencing_token: 0,
                },
                1,
                1,
            ),
            (
                CoordinatedCommitCursor {
                    checkpoint_id: 1,
                    fencing_token: 1,
                },
                2,
                2,
            ),
            (
                CoordinatedCommitCursor {
                    checkpoint_id: 2,
                    fencing_token: 2,
                },
                3,
                3,
            ),
        ]
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn live_cluster_cursor_must_match_its_outcome_authority() {
    let backend = Arc::new(InProcessBackend::new(1));
    let attempt = CheckpointAttempt::canonical(1);
    let fence = assignment_fence(3, &[7]);
    let mut outcomes = cluster_decisions(&fence, 7).await;
    let next_owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: outcomes.owner.node,
        boot: outcomes.owner.boot,
        process_term: outcomes.owner.process_term + 1,
    };
    advance_cluster_term(&mut outcomes, next_owner).await;
    seal_with_fence(
        &backend,
        attempt,
        &[(7, Some(b"payload"))],
        &[7],
        Some(&fence),
        Some(&outcomes.proof),
    )
    .await;
    record_cluster_commit(&outcomes, &backend, attempt.checkpoint_id, &fence).await;
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink_with_cursor(
        Arc::clone(&recorded),
        external_cursor(attempt.checkpoint_id, 1),
    );
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&outcomes.controller)))
    .with_decision_store(Some(Arc::clone(&outcomes.capsules)));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("does not match authoritative token 2"));
    assert!(recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn external_commit_rejects_capsule_bound_to_another_seal_inventory() {
    let backend = Arc::new(InProcessBackend::new(2));
    let attempt = CheckpointAttempt::canonical(1);
    let fence = assignment_fence(3, &[7, 9]);
    let decisions = cluster_decisions(&fence, 7).await;
    seal_with_fence(
        &backend,
        attempt,
        &[(7, Some(b"payload")), (9, None)],
        &[7, 9],
        Some(&fence),
        Some(&decisions.proof),
    )
    .await;

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    record_cluster_commit_with_inventory_digest(
        &decisions,
        &backend,
        attempt.checkpoint_id,
        &fence,
        Some("ff".repeat(32)),
    )
    .await;
    let mut committer = CoordinatedCommitter::new(
        Arc::clone(&backend) as Arc<dyn StateBackend>,
        vec![(
            TEST_SINK_ID.into(),
            spawn_recording_sink(Arc::clone(&recorded)),
        )],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("recovery capsule seal inventory digest does not match"));
    assert!(recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn current_leader_finishes_commit_selected_by_predecessor_proof() {
    let backend = Arc::new(InProcessBackend::new(2));
    let attempt = CheckpointAttempt::canonical(1);
    let fence = assignment_fence(3, &[7, 9]);
    let mut outcomes = cluster_decisions(&fence, 7).await;
    seal_with_fence(
        &backend,
        attempt,
        &[(7, Some(b"old-leader")), (9, None)],
        &[7, 9],
        Some(&fence),
        Some(&outcomes.proof),
    )
    .await;

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    // The immutable outcome is certified by predecessor node 7. Node 9 is the current
    // designated committer and must finish it without asking whether proof 7 is still live.
    record_cluster_commit(&outcomes, &backend, 1, &fence).await;
    let successor = laminar_core::cluster::control::LeaderLeaseOwner {
        node: laminar_core::cluster::discovery::NodeId(9),
        boot: fence.participant_incarnation(9).unwrap(),
        process_term: 1,
    };
    advance_cluster_term(&mut outcomes, successor).await;
    assert!(outcomes.controller.is_leader());

    let mut committer = CoordinatedCommitter::new(
        Arc::clone(&backend) as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&outcomes.controller)))
    .with_decision_store(Some(Arc::clone(&outcomes.capsules)));

    committer.commit_ready().await.unwrap();

    let batches = recorded.lock();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].target, attempt);
}

#[tokio::test]
async fn skips_abort_outcome_with_partial_descriptor() {
    let backend = Arc::new(InProcessBackend::new(2));
    let first = CheckpointAttempt::canonical(1);
    let abandoned = CheckpointAttempt::canonical(2);
    let third = CheckpointAttempt::canonical(3);
    seal(&backend, first, &[(0, Some(b"e1"))]).await;
    // Epoch 2 wrote a descriptor but durably selected abort (and was never sealed).
    let namespace = namespace();
    let orphan_key = descriptor_key(&namespace, 0);
    let orphan = encode_prepared_marker(&namespace, abandoned, 0, Some(b"orphan")).unwrap();
    backend
        .write_commit_descriptor(abandoned, &orphan_key, Bytes::from(orphan))
        .await
        .unwrap();
    seal(&backend, third, &[(0, Some(b"e3"))]).await;

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    let decisions = decisions().await;
    record_local_commit(&decisions, 1).await;
    record_local_abort(&decisions, 2).await;
    record_local_commit(&decisions, 3).await;
    let mut committer = CoordinatedCommitter::new(
        Arc::clone(&backend) as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_decision_store(Some(decisions));

    committer.commit_ready().await.unwrap();

    // Epochs 1 and 3 batch into one commit keyed by 3; epoch 2's aborted
    // descriptor must not enter seal validation or external commit.
    let batches = recorded.lock().clone();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].fencing_token, 1);
    assert_eq!(batches[0].target, third);
    assert_eq!(
        batches[0]
            .entries
            .iter()
            .filter_map(|entry| entry.payload.as_deref())
            .collect::<Vec<_>>(),
        vec![b"e1".as_slice(), b"e3".as_slice()]
    );
}

/// On restart/failover a fresh committer seeds its cursor from the sink's
/// external commit state and must not re-commit already-committed epochs.
#[tokio::test]
async fn restart_resumes_from_exact_external_cursor() {
    let backend = Arc::new(InProcessBackend::new(2));
    let first_attempt = CheckpointAttempt::canonical(1);
    let second_attempt = CheckpointAttempt::canonical(2);
    seal(&backend, first_attempt, &[(0, Some(b"e1"))]).await;
    seal(&backend, second_attempt, &[(0, Some(b"e2"))]).await;

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    let decisions = decisions().await;
    record_local_commit(&decisions, first_attempt.checkpoint_id).await;
    record_local_commit(&decisions, second_attempt.checkpoint_id).await;

    let mut first = CoordinatedCommitter::new(
        Arc::clone(&backend) as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle.clone())],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_decision_store(Some(Arc::clone(&decisions)));
    first.commit_ready().await.unwrap();
    {
        let batches = recorded.lock();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].target, second_attempt);
        assert_eq!(
            batches[0]
                .entries
                .iter()
                .map(|entry| entry.attempt)
                .collect::<Vec<_>>(),
            vec![first_attempt, second_attempt]
        );
        assert!(batches[0]
            .entries
            .iter()
            .all(|entry| entry.attempt.is_canonical()));
    }

    // Fresh committer (restart) over the same sink — seeds from committed_through.
    let mut restarted = CoordinatedCommitter::new(
        Arc::clone(&backend) as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_decision_store(Some(decisions));
    restarted.commit_ready().await.unwrap();
    assert_eq!(
        recorded.lock().len(),
        1,
        "restart must not re-commit already-committed epochs"
    );
}

#[tokio::test]
async fn live_local_cursor_uses_the_fixed_local_authority() {
    let backend = Arc::new(InProcessBackend::new(1));
    let attempt = CheckpointAttempt::canonical(1);
    seal(&backend, attempt, &[(0, Some(b"payload"))]).await;
    let outcomes = decisions().await;
    record_local_commit(&outcomes, attempt.checkpoint_id).await;
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink_with_cursor(
        Arc::clone(&recorded),
        external_cursor(attempt.checkpoint_id, 2),
    );
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_decision_store(Some(outcomes));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("does not match authoritative token 1"));
    assert!(recorded.lock().is_empty());
}

#[tokio::test]
async fn outcome_gc_anchor_is_cursor_continuity_only() {
    let backend = Arc::new(InProcessBackend::new(1));
    let anchor = CheckpointAttempt::canonical(1);
    let aborted = CheckpointAttempt::canonical(2);
    let live = CheckpointAttempt::canonical(3);
    seal(&backend, anchor, &[(0, Some(b"e1"))]).await;
    seal(&backend, aborted, &[(0, Some(b"must-not-commit"))]).await;
    seal(&backend, live, &[(0, Some(b"e3"))]).await;

    let outcomes = decisions().await;
    record_local_commit(&outcomes, anchor.checkpoint_id).await;
    record_local_abort(&outcomes, aborted.checkpoint_id).await;
    record_local_commit(&outcomes, live.checkpoint_id).await;
    assert_eq!(outcomes.prune_outcomes_before(3).await.unwrap(), 3);
    let boundary = outcomes.outcome_retention_boundary().await.unwrap();
    assert_eq!(boundary.committed_checkpoint_id, Some(anchor.checkpoint_id));
    assert_eq!(boundary.highest_closed_epoch, Some(aborted.epoch));

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink_with_cursor(
        Arc::clone(&recorded),
        external_cursor(anchor.checkpoint_id, 1),
    );
    let mut committer = CoordinatedCommitter::new(
        Arc::clone(&backend) as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_decision_store(Some(outcomes));

    committer.commit_ready().await.unwrap();

    let batches = recorded.lock();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].expected_predecessor,
        CoordinatedCommitCursor {
            checkpoint_id: anchor.checkpoint_id,
            fencing_token: 1,
        }
    );
    assert_eq!(batches[0].target, live);
    assert_eq!(batches[0].entries.len(), 1);
    assert_eq!(batches[0].entries[0].attempt, live);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn terminal_compaction_preserves_every_lagging_commit_for_external_publication() {
    let backend = Arc::new(InProcessBackend::new(1));
    let fence = assignment_fence(3, &[7]);
    let decisions = cluster_decisions(&fence, 7).await;
    let commits = [1_u64, 3, 20, 40, 60, 80].map(CheckpointAttempt::canonical);
    let anchor = commits[0];
    let live_commits = &commits[1..];

    for epoch in 1..=80 {
        let attempt = CheckpointAttempt::canonical(epoch);
        if commits.contains(&attempt) {
            seal_with_fence(
                &backend,
                attempt,
                &[(7, Some(b"live"))],
                &[7],
                Some(&fence),
                Some(&decisions.proof),
            )
            .await;
            record_cluster_commit(&decisions, &backend, epoch, &fence).await;
        } else {
            decisions
                .authority
                .record_cluster_outcome(
                    &decisions.proof,
                    epoch,
                    epoch,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }
        if epoch == 3 {
            assert_eq!(
                decisions
                    .authority
                    .prune_cluster_outcomes_before(&decisions.proof, 3, |_| async { Ok(()) })
                    .await
                    .unwrap(),
                3
            );
        }
    }

    let boundary = decisions
        .authority
        .cluster_outcome_retention_boundary()
        .await
        .unwrap();
    assert_eq!(boundary.artifact_before_epoch, 3);
    assert!(
        boundary.terminal_before_epoch > boundary.artifact_before_epoch,
        "automatic terminal compaction must not advance artifact retention"
    );
    assert!(decisions
        .authority
        .cluster_outcome(4)
        .await
        .unwrap()
        .is_none());
    let retained_commits = decisions
        .authority
        .cluster_outcomes()
        .await
        .unwrap()
        .into_iter()
        .filter(CheckpointOutcome::is_commit)
        .map(|outcome| CheckpointAttempt::canonical(outcome.checkpoint_id))
        .collect::<Vec<_>>();
    assert_eq!(retained_commits.as_slice(), live_commits);

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink_with_cursor(
        Arc::clone(&recorded),
        external_cursor(anchor.checkpoint_id, decisions.proof.fencing_token),
    );
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    committer.commit_ready().await.unwrap();

    let batches = recorded.lock();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].expected_predecessor,
        CoordinatedCommitCursor {
            checkpoint_id: anchor.checkpoint_id,
            fencing_token: decisions.proof.fencing_token,
        }
    );
    assert_eq!(batches[0].target, *live_commits.last().unwrap());
    let submitted_commits = batches[0]
        .entries
        .iter()
        .map(|entry| entry.attempt)
        .collect::<Vec<_>>();
    assert_eq!(submitted_commits.as_slice(), live_commits);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn compacted_cluster_cursor_must_match_anchor_authority() {
    let backend = Arc::new(InProcessBackend::new(1));
    let anchor = CheckpointAttempt::canonical(1);
    let live = CheckpointAttempt::canonical(3);
    let fence = assignment_fence(3, &[7]);
    let mut decisions = cluster_decisions(&fence, 7).await;
    seal_with_fence(
        &backend,
        anchor,
        &[(7, Some(b"payload"))],
        &[7],
        Some(&fence),
        Some(&decisions.proof),
    )
    .await;
    record_cluster_commit(&decisions, &backend, anchor.checkpoint_id, &fence).await;
    let next_owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: decisions.owner.node,
        boot: decisions.owner.boot,
        process_term: decisions.owner.process_term + 1,
    };
    advance_cluster_term(&mut decisions, next_owner).await;
    seal_with_fence(
        &backend,
        live,
        &[(7, Some(b"payload"))],
        &[7],
        Some(&fence),
        Some(&decisions.proof),
    )
    .await;
    record_cluster_commit(&decisions, &backend, live.checkpoint_id, &fence).await;
    assert_eq!(
        decisions
            .authority
            .prune_cluster_outcomes_before(&decisions.proof, live.epoch, |_| async { Ok(()) })
            .await
            .unwrap(),
        live.epoch
    );

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink_with_cursor(
        Arc::clone(&recorded),
        external_cursor(anchor.checkpoint_id, 2),
    );
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("compacted external cursor checkpoint 1 fencing token 2"));
    assert!(error.to_string().contains("authoritative token 1"));
    assert!(recorded.lock().is_empty());
}

#[tokio::test]
async fn tampered_marker_checksum_fails_without_external_commit() {
    let backend = Arc::new(InProcessBackend::new(1));
    let attempt = CheckpointAttempt::canonical(4);
    let namespace = namespace();
    let key = descriptor_key(&namespace, 0);
    let mut marker = encode_prepared_marker(&namespace, attempt, 0, Some(b"original")).unwrap();
    *marker.last_mut().unwrap() = b'!';
    backend
        .write_commit_descriptor(attempt, &key, Bytes::from(marker))
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(attempt, None, &[], &[key])
        .await
        .unwrap());

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    let decisions = decisions().await;
    record_local_commit(&decisions, 4).await;
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_decision_store(Some(decisions));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error.to_string().contains("checksum mismatch"));
    assert!(recorded.lock().is_empty());
}

#[tokio::test]
async fn local_outcome_rejects_a_cluster_assignment_seal() {
    let backend = Arc::new(InProcessBackend::new(1));
    let attempt = CheckpointAttempt::canonical(4);
    let fence = assignment_fence(4, &[1]);
    let proof = leader_proof(&fence, 1, 1, 1);
    seal_with_fence(
        &backend,
        attempt,
        &[(1, Some(b"payload"))],
        &[],
        Some(&fence),
        Some(&proof),
    )
    .await;
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    let decisions = decisions().await;
    record_local_commit(&decisions, 4).await;
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_decision_store(Some(decisions));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error.to_string().contains("cluster assignment certificate"));
    assert!(recorded.lock().is_empty());
}

#[tokio::test]
async fn caught_up_history_still_rejects_a_different_deployment() {
    let backend = Arc::new(InProcessBackend::new(1));
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink_with_cursor(Arc::clone(&recorded), external_cursor(4, 1));
    let decisions = decisions().await;
    record_local_commit(&decisions, 4).await;
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        "018f0000-0000-7000-8000-000000000099".into(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_decision_store(Some(decisions));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("does not match committer deployment"));
    assert!(recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_committer_ignores_forged_standalone_outcome_key() {
    let fence = assignment_fence(3, &[7]);
    let decisions = cluster_decisions(&fence, 7).await;
    let malformed = serde_json::json!({
        "version": 2,
        "scope": "cluster",
        "epoch": 4,
        "checkpoint_id": 4,
        "deployment_id": deployment_id(),
        "assignment_fence": null,
        "leader_proof": {
            "owner": {
                "node_id": 7,
                "boot_id": "00000000-0000-0000-0000-000000000007",
                "process_term": 1
            },
            "fencing_token": 1
        },
        "recovery_capsule": null,
        "verdict": "commit"
    });
    decisions
        .backing
        .put(
            &object_store::path::Path::from("checkpoint-outcomes/epoch=4/outcome"),
            PutPayload::from_bytes(Bytes::from(serde_json::to_vec(&malformed).unwrap())),
        )
        .await
        .unwrap();

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    let mut committer = CoordinatedCommitter::new(
        Arc::new(InProcessBackend::new(1)) as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    committer.commit_ready().await.unwrap();
    assert!(recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_commit_rejects_marker_written_by_another_certified_participant() {
    let backend = Arc::new(InProcessBackend::new(2));
    let attempt = CheckpointAttempt::canonical(5);
    let fence = assignment_fence(4, &[7, 9]);
    let decisions = cluster_decisions(&fence, 7).await;
    backend.set_authoritative_version(fence.assignment_version);
    let payload = Bytes::from_static(b"test-vnode-state");
    backend
        .write_certified_partial(
            attempt,
            0,
            &fence,
            7,
            laminar_core::state::VnodePartialLineage::root(payload.len() as u64),
            payload.clone(),
        )
        .await
        .unwrap();
    backend
        .write_certified_partial(
            attempt,
            1,
            &fence,
            9,
            laminar_core::state::VnodePartialLineage::root(payload.len() as u64),
            payload,
        )
        .await
        .unwrap();

    let namespace = namespace();
    let marker_key = descriptor_key(&namespace, 7);
    let marker = encode_prepared_marker(&namespace, attempt, 7, Some(b"payload")).unwrap();
    backend
        .write_certified_commit_descriptor(
            attempt,
            &marker_key,
            &fence,
            9,
            &decisions.proof,
            Bytes::from(marker),
        )
        .await
        .unwrap();
    let mut keys = vec![marker_key];
    for participant_id in [7, 9] {
        let ready_key = participant_ready_key(participant_id);
        let ready = ParticipantReady {
            version: PARTICIPANT_READY_VERSION,
            attempt,
            participant_id,
            assignment_fence: fence.clone(),
            deployment_id: deployment_id(),
            pipeline_identity: identity(),
            vnode_restore_limits: crate::cluster_recovery_capsule::vnode_restore_limits_for_test(
                fence.vnode_count,
            ),
            owned_vnodes: match participant_id {
                7 => vec![0],
                9 => vec![1],
                _ => unreachable!("test readiness participant belongs to the assignment"),
            },
            source_offsets: Default::default(),
            source_metadata: Default::default(),
            source_assignment_versions: Default::default(),
            source_watermarks: Default::default(),
            local_watermark: CheckpointWatermark::Uninitialized,
            manifest_sha256: format!("{participant_id:064x}"),
            portable_state_sha256: identity().sha256,
        };
        backend
            .write_certified_commit_descriptor(
                attempt,
                &ready_key,
                &fence,
                participant_id,
                &decisions.proof,
                Bytes::from(canonical_json_bytes(&ready).unwrap()),
            )
            .await
            .unwrap();
        keys.push(ready_key);
    }
    keys.sort_unstable();
    assert!(backend
        .seal_checkpoint(attempt, Some(&fence), &[0, 1], &keys)
        .await
        .unwrap());
    record_cluster_commit(&decisions, &backend, attempt.checkpoint_id, &fence).await;

    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(
            TEST_SINK_ID.into(),
            spawn_recording_sink(Arc::clone(&recorded)),
        )],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("was not written by participant 7"));
    assert!(recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn external_commit_rejects_participants_outside_the_outcome() {
    let backend = Arc::new(InProcessBackend::new(2));
    let attempt = CheckpointAttempt::canonical(5);
    let fence = assignment_fence(4, &[7, 9]);
    let decisions = cluster_decisions(&fence, 7).await;
    seal_with_fence(
        &backend,
        attempt,
        &[(7, Some(b"payload"))],
        &[7, 9],
        Some(&fence),
        Some(&decisions.proof),
    )
    .await;
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    record_cluster_commit(&decisions, &backend, 5, &fence).await;
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("do not match outcome participants"));
    assert!(recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn external_commit_rejects_deleted_sealed_readiness() {
    let raw = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = raw.clone();
    let backend = Arc::new(ObjectStoreBackend::cluster_shared(store, "node-7", 2));
    let attempt = CheckpointAttempt::canonical(5);
    let fence = assignment_fence(4, &[7, 9]);
    let decisions = cluster_decisions(&fence, 7).await;
    seal_with_fence(
        &backend,
        attempt,
        &[(7, Some(b"payload")), (9, None)],
        &[7, 9],
        Some(&fence),
        Some(&decisions.proof),
    )
    .await;
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    record_cluster_commit(&decisions, &backend, 5, &fence).await;
    let deleted_key = participant_ready_key(9);
    raw.delete(&descriptor_object_path(attempt, &deleted_key))
        .await
        .unwrap();
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("sealed participant readiness marker"));
    assert!(recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn external_commit_rejects_mutated_sealed_readiness() {
    let raw = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = raw.clone();
    let backend = Arc::new(ObjectStoreBackend::cluster_shared(store, "node-7", 2));
    let attempt = CheckpointAttempt::canonical(5);
    let fence = assignment_fence(4, &[7, 9]);
    let decisions = cluster_decisions(&fence, 7).await;
    seal_with_fence(
        &backend,
        attempt,
        &[(7, Some(b"payload")), (9, None)],
        &[7, 9],
        Some(&fence),
        Some(&decisions.proof),
    )
    .await;
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    record_cluster_commit(&decisions, &backend, 5, &fence).await;

    let mutated_key = participant_ready_key(9);
    let mut mutated = serde_json::from_slice::<ParticipantReady>(
        &backend
            .read_commit_descriptor(attempt, &mutated_key)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    mutated.manifest_sha256 = "ff".repeat(32);
    raw.delete(&descriptor_object_path(attempt, &mutated_key))
        .await
        .unwrap();
    backend
        .write_certified_commit_descriptor(
            attempt,
            &mutated_key,
            &fence,
            9,
            &decisions.proof,
            Bytes::from(canonical_json_bytes(&mutated).unwrap()),
        )
        .await
        .unwrap();

    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(
            TEST_SINK_ID.into(),
            spawn_recording_sink(Arc::clone(&recorded)),
        )],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("attestation does not match the checkpoint seal"));
    assert!(recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn external_commit_rejects_a_different_outcome_assignment() {
    let backend = Arc::new(InProcessBackend::new(1));
    let attempt = CheckpointAttempt::canonical(6);
    let sealed_fence = assignment_fence(4, &[7]);
    let decision_fence = assignment_fence(5, &[7]);
    let decisions = cluster_decisions(&decision_fence, 7).await;
    seal_with_fence(
        &backend,
        attempt,
        &[(7, Some(b"payload"))],
        &[7],
        Some(&sealed_fence),
        Some(&decisions.proof),
    )
    .await;
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    record_cluster_commit(&decisions, &backend, 6, &decision_fence).await;
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
    .with_decision_store(Some(Arc::clone(&decisions.capsules)));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("assignment certificate does not match"));
    assert!(recorded.lock().is_empty());
}

#[tokio::test]
async fn durable_commit_outcome_without_exact_seal_fails_closed() {
    let backend = Arc::new(InProcessBackend::new(1));
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_recording_sink(Arc::clone(&recorded));
    let decisions = decisions().await;
    record_local_commit(&decisions, 4).await;
    let lag = Arc::new(AtomicU64::new(7));
    let lag_known = Arc::new(AtomicBool::new(true));
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_lag_state(
        Arc::clone(&lag),
        Arc::clone(&lag_known),
        Arc::new(tokio::sync::Notify::new()),
    )
    .with_decision_store(Some(decisions));

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error.to_string().contains("has no exact state seal"));
    assert!(!lag_known.load(Ordering::Acquire));
    assert_eq!(lag.load(Ordering::Acquire), 7);
    assert!(recorded.lock().is_empty());
}

#[tokio::test]
async fn live_external_cursor_rollback_fails_closed() {
    let backend = Arc::new(InProcessBackend::new(1));
    let attempt = CheckpointAttempt::canonical(1);
    seal(&backend, attempt, &[(0, Some(b"e1"))]).await;
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let cursor: ExternalCursor = Arc::new(Mutex::new(None));
    let handle = spawn_recording_sink_with_cursor(Arc::clone(&recorded), Arc::clone(&cursor));
    let decisions = decisions().await;
    record_local_commit(&decisions, 1).await;
    let mut committer = CoordinatedCommitter::new(
        backend as Arc<dyn StateBackend>,
        vec![(TEST_SINK_ID.into(), handle)],
        identity(),
        deployment_id(),
        Arc::new(AtomicU64::new(0)),
    )
    .with_decision_store(Some(decisions));

    committer.commit_ready().await.unwrap();
    assert_eq!(
        *cursor.lock(),
        Some(CoordinatedCommitCursor {
            checkpoint_id: attempt.checkpoint_id,
            fencing_token: 1,
        })
    );
    *cursor.lock() = None;

    let error = committer.commit_ready().await.unwrap_err();
    assert!(error.to_string().contains("rolled back from 1 to 0"));
    assert_eq!(recorded.lock().len(), 1);
}
