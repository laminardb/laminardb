use super::super::controller::{RecoveryFault, RecoveryRound};
use super::super::snapshot::AssignmentSnapshot;
use super::*;
use async_trait::async_trait;
use futures::StreamExt as FuturesStreamExt;
use object_store::memory::InMemory;
use std::collections::BTreeMap;

fn owner(node: u64, boot: u128, process_term: u64) -> LeaderLeaseOwner {
    LeaderLeaseOwner {
        node: NodeId(node),
        boot: Uuid::from_u128(boot),
        process_term,
    }
}

async fn accept_recovery_artifacts(_: CheckpointOutcome) -> Result<(), String> {
    Ok(())
}

fn process(owner: &LeaderLeaseOwner) -> ProcessLease {
    ProcessLease {
        node: owner.node,
        owner: owner.boot,
        term: owner.process_term,
        seq: 1,
        expires_at_ms: 1,
    }
}

async fn assert_store_contract_probe_prefix_empty(store: &dyn ObjectStore) {
    let mut entries = store.list(Some(&OsPath::from(STORE_CONTRACT_PROBE_PREFIX)));
    assert!(FuturesStreamExt::next(&mut entries).await.is_none());
}

fn store(ttl_ms: i64) -> LeaderLeaseStore {
    LeaderLeaseStore::new(Arc::new(InMemory::new()), ttl_ms)
}

fn handoff_checkpoint(epoch: u64) -> CommittedCheckpointRef {
    CommittedCheckpointRef {
        epoch,
        checkpoint_id: epoch,
        sha256: "0".repeat(64),
        len: 1,
    }
}

fn drain_decision_for_test(
    transition: &AssignmentDrainTransition,
    proof: LeaderProof,
    verdict: AssignmentDrainVerdict,
) -> Result<AssignmentDrainDecision, String> {
    match verdict {
        AssignmentDrainVerdict::Commit => AssignmentDrainDecision::commit(
            transition,
            proof,
            handoff_checkpoint(transition.target.assignment_version),
        ),
        AssignmentDrainVerdict::Abort => AssignmentDrainDecision::abort(transition, proof),
    }
}

#[test]
fn live_authority_link_budget_is_exact() {
    let mut traversed = 0;
    for _ in 0..MAX_LIVE_AUTHORITY_LINKS {
        assert!(consume_live_authority_link(&mut traversed));
    }
    assert!(!consume_live_authority_link(&mut traversed));
    assert_eq!(traversed, MAX_LIVE_AUTHORITY_LINKS);
}

#[test]
fn outcome_link_requires_one_canonical_checkpoint_identity() {
    assert!(OutcomeLink {
        sequence: 1,
        epoch: 7,
        checkpoint_id: 7,
    }
    .validate()
    .is_ok());

    for link in [
        OutcomeLink {
            sequence: 0,
            epoch: 7,
            checkpoint_id: 7,
        },
        OutcomeLink {
            sequence: 1,
            epoch: 0,
            checkpoint_id: 0,
        },
        OutcomeLink {
            sequence: 1,
            epoch: 7,
            checkpoint_id: 8,
        },
    ] {
        assert!(matches!(link.validate(), Err(LeaseError::Invalid(_))));
    }

    let mut record = bare_authority_record(&owner(1, 1, 1), 1);
    record.outcome_head = Some(OutcomeLink {
        sequence: 1,
        epoch: 7,
        checkpoint_id: 8,
    });
    assert!(matches!(record.validate(), Err(LeaseError::Invalid(_))));
}

#[test]
fn prune_latch_guard_releases_on_every_drop_path() {
    let latch = Arc::new(AtomicBool::new(true));
    {
        let _guard = PruneLatchGuard(Arc::clone(&latch));
    }
    assert!(!latch.load(Ordering::Acquire));
}

fn assignment_fence(owner: &LeaderLeaseOwner) -> CheckpointAssignmentFence {
    CheckpointAssignmentFence::from_owner_map(
        1,
        &[owner.node.0],
        vec![crate::checkpoint::CheckpointParticipant {
            node_id: owner.node.0,
            boot_incarnation: owner.boot,
        }],
    )
    .unwrap()
}

fn recovery_fault_publisher(
    node_id: u64,
    boot_incarnation: u128,
    process_term: u64,
) -> RecoveryFaultPublisher {
    RecoveryFaultPublisher {
        participant: crate::checkpoint::CheckpointParticipant {
            node_id,
            boot_incarnation: Uuid::from_u128(boot_incarnation),
        },
        process_term,
    }
}

fn owner_recovery_fault_publisher(owner: &LeaderLeaseOwner) -> RecoveryFaultPublisher {
    RecoveryFaultPublisher {
        participant: crate::checkpoint::CheckpointParticipant {
            node_id: owner.node.0,
            boot_incarnation: owner.boot,
        },
        process_term: owner.process_term,
    }
}

async fn recovery_release_terminal(
    store: &LeaderLeaseStore,
    lease: &LeaderLease,
    generation: u64,
    epoch: u64,
) -> RecoveryAnnouncement {
    let inventory = store.recovery_fault_inventory().await.unwrap();
    assert!(!inventory.faults().is_empty());
    let round = RecoveryRound::new(
        generation,
        lease.proof(),
        assignment_fence(&lease.owner),
        Vec::new(),
        inventory.revision(),
        inventory.faults().to_vec(),
    )
    .unwrap();
    RecoveryAnnouncement {
        round,
        phase: RecoverPhase::ReleaseCommitted { epoch },
    }
}

async fn recovery_release_terminal_after_owner_fault(
    store: &LeaderLeaseStore,
    lease: &LeaderLease,
    generation: u64,
    epoch: u64,
) -> RecoveryAnnouncement {
    assert_eq!(
        store
            .record_recovery_fault(owner_recovery_fault_publisher(&lease.owner), generation,)
            .await
            .unwrap(),
        RecordRecoveryFaultResult::Active
    );
    recovery_release_terminal(store, lease, generation, epoch).await
}

async fn commit_recovery_release(
    store: &LeaderLeaseStore,
    lease: &LeaderLease,
    terminal: &RecoveryAnnouncement,
) -> RecoveryReleaseTerminalRef {
    let reference = store
        .stage_recovery_release_terminal(terminal)
        .await
        .unwrap();
    assert_eq!(
        store
            .record_recovery_release_commit(&lease.proof(), reference.clone())
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::Created(reference.clone())
    );
    reference
}

fn assignment_drain_transition(
    owner: &LeaderLeaseOwner,
    leader_proof: LeaderProof,
) -> AssignmentDrainTransition {
    assignment_drain_transition_at(owner, leader_proof, 2)
}

fn assignment_drain_transition_at(
    owner: &LeaderLeaseOwner,
    leader_proof: LeaderProof,
    target_version: u64,
) -> AssignmentDrainTransition {
    assert!(target_version > 1);
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        target_version - 1,
        &[owner.node.0],
        vec![crate::checkpoint::CheckpointParticipant {
            node_id: owner.node.0,
            boot_incarnation: owner.boot,
        }],
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        target_version,
        &[owner.node.0],
        predecessor.participants.clone(),
    )
    .unwrap();
    AssignmentDrainTransition::new(predecessor, target, leader_proof).unwrap()
}

async fn assignment_recovery_decision(
    store: &LeaderLeaseStore,
    predecessor_version: u64,
    predecessor_processes: &[LeaderLeaseOwner],
    target_processes: &[LeaderLeaseOwner],
    leader_proof: LeaderProof,
    updated_at_ms: i64,
) -> AssignmentRecoveryDecision {
    assert!(!predecessor_processes.is_empty());
    assert!(!target_processes.is_empty());
    assert!(predecessor_processes
        .windows(2)
        .all(|pair| pair[0].node.0 < pair[1].node.0));
    assert!(target_processes
        .windows(2)
        .all(|pair| pair[0].node.0 < pair[1].node.0));
    let vnode_count = predecessor_processes.len().max(target_processes.len());
    let predecessor_owners: Vec<_> = (0..vnode_count)
        .map(|index| {
            predecessor_processes[index % predecessor_processes.len()]
                .node
                .0
        })
        .collect();
    let target_owners: Vec<_> = (0..vnode_count)
        .map(|index| target_processes[index % target_processes.len()].node.0)
        .collect();
    let participants = |processes: &[LeaderLeaseOwner]| {
        processes
            .iter()
            .map(|process| crate::checkpoint::CheckpointParticipant {
                node_id: process.node.0,
                boot_incarnation: process.boot,
            })
            .collect::<Vec<_>>()
    };
    let snapshots = AssignmentSnapshotStore::new(Arc::clone(&store.store));
    let mut durable_version = snapshots
        .load()
        .await
        .unwrap()
        .map_or(0, |snapshot| snapshot.version);
    while durable_version < predecessor_version {
        let version = durable_version.checked_add(1).unwrap();
        let snapshot = AssignmentSnapshot {
            version,
            partitioning_abi_version: crate::state::PARTITIONING_ABI_VERSION,
            vnodes: predecessor_owners
                .iter()
                .copied()
                .enumerate()
                .map(|(vnode, node)| (u32::try_from(vnode).unwrap(), NodeId(node)))
                .collect(),
            participants: participants(predecessor_processes),
            updated_at_ms: i64::try_from(version).unwrap(),
            draining: false,
            drain_transition: None,
        };
        if version == 1 {
            let _ = snapshots.save_if_absent(&snapshot).await.unwrap();
        } else {
            let _ = snapshots
                .save_if_version(&snapshot, durable_version)
                .await
                .unwrap();
        }
        durable_version = snapshots.load().await.unwrap().unwrap().version;
    }
    let predecessor_snapshot = snapshots.load().await.unwrap().unwrap();
    assert_eq!(predecessor_snapshot.version, predecessor_version);
    let predecessor = predecessor_snapshot.assignment_fence().unwrap();
    assert_eq!(
        predecessor,
        CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
            &predecessor_owners,
            participants(predecessor_processes),
        )
        .unwrap()
    );
    let target_version = predecessor_version.checked_add(1).unwrap();
    let target_snapshot = AssignmentSnapshot {
        version: target_version,
        partitioning_abi_version: crate::state::PARTITIONING_ABI_VERSION,
        vnodes: target_owners
            .iter()
            .copied()
            .enumerate()
            .map(|(vnode, node)| (u32::try_from(vnode).unwrap(), NodeId(node)))
            .collect(),
        participants: participants(target_processes),
        updated_at_ms,
        draining: false,
        drain_transition: None,
    };
    let target = target_snapshot.assignment_fence().unwrap();
    let proposal = snapshots
        .stage_recovery_proposal(&target_snapshot)
        .await
        .unwrap();
    let removed_process_fences = predecessor_processes
        .iter()
        .enumerate()
        .filter(|(_, process)| target.participant_incarnation(process.node.0) != Some(process.boot))
        .map(|(index, process)| {
            let predecessor = ProcessLease {
                node: process.node,
                owner: process.boot,
                term: process.process_term,
                seq: u64::try_from(index).unwrap().saturating_add(10),
                expires_at_ms: 1,
            };
            let successor_owner = target
                .participant_incarnation(process.node.0)
                .unwrap_or_else(|| Uuid::from_u128(10_000 + u128::from(process.node.0)));
            let successor = ProcessLease {
                node: process.node,
                owner: successor_owner,
                term: predecessor.term.checked_add(1).unwrap(),
                seq: predecessor.seq.checked_add(1).unwrap(),
                expires_at_ms: 2,
            };
            ProcessLeaseFence::new(predecessor, successor).unwrap()
        })
        .collect();
    let recovery_checkpoint =
        recovery_checkpoint_for_test(store, &leader_proof, &predecessor).await;
    AssignmentRecoveryDecision::new(
        predecessor,
        target,
        proposal,
        removed_process_fences,
        recovery_checkpoint,
        leader_proof,
    )
    .unwrap()
}

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn committed_checkpoint_path(reference: &CommittedCheckpointRef) -> OsPath {
    OsPath::from(format!(
        "committed-checkpoints/epoch={:020}/checkpoint={:020}/sha256={}",
        reference.epoch, reference.checkpoint_id, reference.sha256
    ))
}

async fn committed_checkpoint(
    store: &LeaderLeaseStore,
    fence: &CheckpointAssignmentFence,
    epoch: u64,
    checkpoint_id: u64,
) -> CommittedCheckpointRef {
    assert_eq!(epoch, checkpoint_id, "test checkpoint must be canonical");
    committed_checkpoint_with_predecessor(store, fence, checkpoint_id, 9, None).await
}

async fn committed_checkpoint_with_predecessor(
    store: &LeaderLeaseStore,
    fence: &CheckpointAssignmentFence,
    checkpoint_id: u64,
    variant: u8,
    predecessor: Option<CommittedCheckpointRef>,
) -> CommittedCheckpointRef {
    let decisions = CheckpointDecisionStore::new(Arc::clone(&store.store));
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let index = CommittedCheckpointIndex {
        version: crate::checkpoint::COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id,
        pipeline_identity: crate::checkpoint::PipelineIdentity::empty(),
        epoch: checkpoint_id,
        checkpoint_id,
        scope: CheckpointScope::Cluster,
        vnode_count: u16::try_from(fence.vnode_count).unwrap(),
        assignment_fence: Some(fence.clone()),
        reassignment_portable: true,
        predecessor,
        participants: fence
            .participants
            .iter()
            .map(|participant| crate::checkpoint::CommittedParticipantRef {
                participant_id: participant.node_id,
                manifest_len: 1,
                manifest_sha256: digest(variant),
                node_data_len: 0,
                node_data_sha256: digest(variant.wrapping_add(1)),
            })
            .collect(),
        source_names: Vec::new(),
        source_offsets: std::collections::BTreeMap::new(),
        channel_progress: Vec::new(),
        source_watermarks: std::collections::BTreeMap::new(),
        checkpoint_watermark: None,
    };
    decisions.create_committed_checkpoint(&index).await.unwrap()
}

async fn begin_checkpoint_artifacts(
    store: &LeaderLeaseStore,
    proof: &LeaderProof,
    fence: &CheckpointAssignmentFence,
    checkpoint_id: u64,
) -> CheckpointArtifactInventory {
    let inventory = checkpoint_artifact_inventory(store, fence, checkpoint_id).await;
    store
        .begin_cluster_checkpoint_artifacts(proof, inventory)
        .await
        .unwrap()
}

async fn checkpoint_artifact_inventory(
    store: &LeaderLeaseStore,
    fence: &CheckpointAssignmentFence,
    checkpoint_id: u64,
) -> CheckpointArtifactInventory {
    CheckpointArtifactInventory {
        deployment_id: CheckpointDecisionStore::new(Arc::clone(&store.store))
            .load_or_create_deployment_id()
            .await
            .unwrap(),
        pipeline_identity: crate::checkpoint::PipelineIdentity::empty(),
        attempt: crate::checkpoint::CheckpointAttempt::canonical(checkpoint_id),
        assignment_fence: Some(fence.clone()),
        sink_artifact_intent_protocol: true,
    }
}

async fn record_commit(
    store: &LeaderLeaseStore,
    proof: &LeaderProof,
    fence: &CheckpointAssignmentFence,
    epoch: u64,
    checkpoint_id: u64,
) -> RecordOutcomeResult {
    try_record_commit(store, proof, fence, epoch, checkpoint_id)
        .await
        .unwrap()
}

async fn try_record_commit(
    store: &LeaderLeaseStore,
    proof: &LeaderProof,
    fence: &CheckpointAssignmentFence,
    epoch: u64,
    checkpoint_id: u64,
) -> Result<RecordOutcomeResult, ClusterCheckpointAuthorityError> {
    assert_eq!(epoch, checkpoint_id, "test outcome must be canonical");
    let predecessor = store
        .highest_cluster_committed_outcome()
        .await
        .unwrap()
        .and_then(|outcome| outcome.committed_checkpoint);
    begin_checkpoint_artifacts(store, proof, fence, checkpoint_id).await;
    let committed_checkpoint =
        committed_checkpoint_with_predecessor(store, fence, checkpoint_id, 9, predecessor).await;
    store
        .record_cluster_outcome(
            proof,
            epoch,
            checkpoint_id,
            fence.clone(),
            CheckpointVerdict::Commit,
            Some(committed_checkpoint),
        )
        .await
}

async fn recovery_checkpoint_for_test(
    store: &LeaderLeaseStore,
    proof: &LeaderProof,
    predecessor: &CheckpointAssignmentFence,
) -> CommittedCheckpointRef {
    if let Some(reference) = store
        .assignment_handoff_checkpoint(predecessor)
        .await
        .unwrap()
    {
        return reference;
    }
    if let Some(outcome) = store.highest_cluster_committed_outcome().await.unwrap() {
        if outcome.assignment_fence.as_ref() == Some(predecessor) {
            return outcome.committed_checkpoint.unwrap();
        }
    }
    let checkpoint_id = store
        .highest_cluster_terminal_outcome()
        .await
        .unwrap()
        .map_or(1, |outcome| outcome.checkpoint_id.checked_add(1).unwrap());
    match record_commit(store, proof, predecessor, checkpoint_id, checkpoint_id).await {
        RecordOutcomeResult::Created(outcome) | RecordOutcomeResult::Unchanged(outcome) => {
            outcome.committed_checkpoint.unwrap()
        }
        RecordOutcomeResult::Conflict { winner } => {
            panic!("unexpected recovery checkpoint winner: {winner:?}")
        }
    }
}

async fn committed_drain_decision_for_test(
    store: &LeaderLeaseStore,
    transition: &AssignmentDrainTransition,
    proof: LeaderProof,
    checkpoint_id: u64,
) -> AssignmentDrainDecision {
    let outcome = match record_commit(
        store,
        &proof,
        &transition.predecessor,
        checkpoint_id,
        checkpoint_id,
    )
    .await
    {
        RecordOutcomeResult::Created(outcome) | RecordOutcomeResult::Unchanged(outcome) => outcome,
        RecordOutcomeResult::Conflict { winner } => {
            panic!("unexpected checkpoint winner: {winner:?}")
        }
    };
    AssignmentDrainDecision::commit(transition, proof, outcome.committed_checkpoint.unwrap())
        .unwrap()
}

async fn disable_history_pruning_for_test(store: &LeaderLeaseStore) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.prune_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    store.prune_running.store(true, Ordering::Release);
}

#[tokio::test]
async fn exact_active_recovery_fault_retry_is_idempotent() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);

    assert_eq!(
        store.record_recovery_fault(publisher, 7).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let admitted = store.load_record().await.unwrap().unwrap();
    assert_eq!(
        store.record_recovery_fault(publisher, 7).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );

    assert_eq!(store.load_record().await.unwrap().unwrap(), admitted);
    assert_eq!(
        store.recovery_fault_inventory().await.unwrap().faults(),
        &[RecoveryFault {
            reporter: incumbent.node,
            sequence: 2,
            disposition: RecoveryFaultDisposition::Recoverable,
        }]
    );
}

#[tokio::test]
async fn legacy_recoverable_fault_authority_and_release_bytes_remain_canonical() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);
    store.record_recovery_fault(publisher, 1).await.unwrap();

    let record = store.load_record().await.unwrap().unwrap();
    let encoded_record = encode_authority_record(&record).unwrap();
    assert!(!String::from_utf8_lossy(&encoded_record).contains("disposition"));
    assert!(!String::from_utf8_lossy(&encoded_record).contains("subscription_cleanup_commit"));
    let decoded_record: LeaderAuthorityRecord = serde_json::from_slice(&encoded_record).unwrap();
    assert_eq!(
        encode_authority_record(&decoded_record).unwrap(),
        encoded_record
    );
    assert_eq!(store.load_record().await.unwrap().unwrap(), record);

    let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
    let (encoded_terminal, reference) = encode_recovery_release_terminal(&terminal).unwrap();
    assert!(!String::from_utf8_lossy(&encoded_terminal).contains("disposition"));
    let decoded_terminal: RecoveryAnnouncement = serde_json::from_slice(&encoded_terminal).unwrap();
    let (reencoded_terminal, reencoded_reference) =
        encode_recovery_release_terminal(&decoded_terminal).unwrap();
    assert_eq!(reencoded_terminal, encoded_terminal);
    assert_eq!(reencoded_reference, reference);
}

#[tokio::test]
async fn terminal_fault_upgrade_is_sticky_and_blocks_release_commit() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);
    assert_eq!(
        store.record_recovery_fault(publisher, 7).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let recoverable = store.recovery_fault_inventory().await.unwrap();
    let release = recovery_release_terminal(&store, &lease, 1, 0).await;
    let reference = store
        .stage_recovery_release_terminal(&release)
        .await
        .unwrap();

    assert_eq!(
        store
            .record_recovery_fault_with_disposition(
                publisher,
                7,
                RecoveryFaultDisposition::Terminal,
            )
            .await
            .unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let terminal = store.recovery_fault_inventory().await.unwrap();
    assert!(terminal.has_terminal_fault());
    assert!(terminal.revision() > recoverable.revision());
    assert!(terminal.faults()[0].sequence > recoverable.faults()[0].sequence);

    assert_eq!(
        store.record_recovery_fault(publisher, 8).await.unwrap(),
        RecordRecoveryFaultResult::TerminalFenceActive
    );
    let replacement = recovery_fault_publisher(1, 2, 2);
    assert_eq!(
        store.record_recovery_fault(replacement, 1).await.unwrap(),
        RecordRecoveryFaultResult::TerminalFenceActive
    );
    assert_eq!(store.recovery_fault_inventory().await.unwrap(), terminal);
    assert_eq!(
        store
            .record_recovery_release_commit(&lease.proof(), reference)
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::FaultsChanged
    );
}

#[tokio::test]
async fn ambiguous_recovery_fault_create_reconciles_without_a_duplicate_sequence() {
    let (raw, store) = ambiguous_once_at(1_000, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);

    assert_eq!(
        store.record_recovery_fault(publisher, 7).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    assert!(raw
        .did_return_ambiguous
        .load(std::sync::atomic::Ordering::Acquire));
    let admitted = store.load_record().await.unwrap().unwrap();
    assert_eq!(admitted.lease.seq, 2);
    assert_eq!(admitted.recovery_fault_revision, 2);
    assert_eq!(admitted.recovery_fault_slots.len(), 1);
    assert_eq!(admitted.recovery_fault_slots[0].fault_sequence, 2);

    assert_eq!(
        store.record_recovery_fault(publisher, 7).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    assert_eq!(store.load_record().await.unwrap().unwrap(), admitted);
}

#[tokio::test]
async fn exact_recovery_fault_retry_observes_a_terminal_bound_tombstone() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);
    assert_eq!(
        store.record_recovery_fault(publisher, 7).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
    commit_recovery_release(&store, &lease, &terminal).await;
    let tombstone = store.load_record().await.unwrap().unwrap();
    assert_eq!(tombstone.recovery_fault_revision, tombstone.lease.seq);
    assert!(!tombstone.recovery_fault_slots[0].active);
    let mut stale_revision = tombstone.clone();
    stale_revision.recovery_fault_revision -= 1;
    assert!(stale_revision.validate().is_err());
    let mut active_slot = tombstone.clone();
    active_slot.recovery_fault_slots[0].active = true;
    assert!(active_slot.validate().is_err());
    let mut advanced_lease = tombstone.lease.clone();
    advanced_lease.seq += 1;
    let mut detached_revision = tombstone.preserve_with_lease(advanced_lease);
    detached_revision.recovery_fault_revision = detached_revision.lease.seq;
    assert!(detached_revision.validate().is_err());
    assert!(store
        .authorize_recovery_release(publisher, &terminal)
        .await
        .unwrap());
    assert!(!store
        .authorize_recovery_release(recovery_fault_publisher(1, 2, 2), &terminal)
        .await
        .unwrap());
    assert_eq!(store.load_record().await.unwrap().unwrap(), tombstone);

    assert_eq!(
        store.record_recovery_fault(publisher, 7).await.unwrap(),
        RecordRecoveryFaultResult::AlreadyCleared
    );
    assert_eq!(store.load_record().await.unwrap().unwrap(), tombstone);
    assert!(store
        .recovery_fault_inventory()
        .await
        .unwrap()
        .faults()
        .is_empty());
}

#[tokio::test]
async fn a_new_fault_blocks_authorization_from_the_previous_release() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let released = owner_recovery_fault_publisher(&incumbent);
    assert_eq!(
        store.record_recovery_fault(released, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
    commit_recovery_release(&store, &lease, &terminal).await;

    let newer = recovery_fault_publisher(2, 2, 1);
    assert_eq!(
        store.record_recovery_fault(newer, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    assert!(!store
        .authorize_recovery_release(released, &terminal)
        .await
        .unwrap());
}

#[tokio::test]
async fn recovery_release_retains_only_exact_stopped_fault_publishers() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let evidence = recovery_fault_publisher(2, 2, 1);
    let unavailable = recovery_fault_publisher(3, 3, 1);
    assert_eq!(
        store.record_recovery_fault(evidence, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    assert_eq!(
        store.record_recovery_fault(unavailable, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let inventory = store.recovery_fault_inventory().await.unwrap();
    let round = RecoveryRound::new(
        1,
        lease.proof(),
        assignment_fence(&incumbent),
        vec![evidence.participant],
        inventory.revision(),
        inventory.faults().to_vec(),
    )
    .unwrap();
    let terminal = RecoveryAnnouncement {
        round,
        phase: RecoverPhase::ReleaseCommitted { epoch: 0 },
    };

    commit_recovery_release(&store, &lease, &terminal).await;
    let head = store.load_record().await.unwrap().unwrap();
    assert_eq!(head.recovery_fault_slots.len(), 1);
    assert_eq!(head.recovery_fault_slots[0].publisher, evidence);
    assert!(!head.recovery_fault_slots[0].active);
    assert!(store
        .authorize_recovery_release(evidence, &terminal)
        .await
        .unwrap());
    assert!(!store
        .authorize_recovery_release(unavailable, &terminal)
        .await
        .unwrap());
}

#[tokio::test]
async fn older_same_boot_recovery_fault_is_covered_by_the_newer_request() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);
    assert_eq!(
        store.record_recovery_fault(publisher, 2).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let newer = store.load_record().await.unwrap().unwrap();

    assert_eq!(
        store.record_recovery_fault(publisher, 1).await.unwrap(),
        RecordRecoveryFaultResult::CoveredByNewerRequest
    );
    assert_eq!(store.load_record().await.unwrap().unwrap(), newer);
}

#[tokio::test]
async fn lower_term_recovery_fault_is_superseded() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let current = recovery_fault_publisher(1, 2, 2);
    assert_eq!(
        store.record_recovery_fault(current, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let admitted = store.load_record().await.unwrap().unwrap();

    assert_eq!(
        store
            .record_recovery_fault(recovery_fault_publisher(1, 1, 1), u64::MAX)
            .await
            .unwrap(),
        RecordRecoveryFaultResult::Superseded
    );
    assert_eq!(store.load_record().await.unwrap().unwrap(), admitted);
}

#[tokio::test]
async fn higher_term_recovery_fault_replaces_the_stable_node_slot() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let first = recovery_fault_publisher(1, 1, 1);
    let replacement = recovery_fault_publisher(1, 2, 2);
    assert_eq!(
        store.record_recovery_fault(first, 10).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );

    assert_eq!(
        store.record_recovery_fault(replacement, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let head = store.load_record().await.unwrap().unwrap();
    assert_eq!(head.recovery_fault_slots.len(), 1);
    assert_eq!(head.recovery_fault_slots[0].publisher, replacement);
    assert_eq!(head.recovery_fault_slots[0].request_sequence, 1);
    assert_eq!(head.recovery_fault_slots[0].fault_sequence, 3);
    assert!(head.recovery_fault_slots[0].active);
    assert_eq!(
        store.record_recovery_fault(first, u64::MAX).await.unwrap(),
        RecordRecoveryFaultResult::Superseded
    );
}

#[tokio::test]
async fn same_term_different_boot_recovery_fault_is_rejected() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    assert_eq!(
        store
            .record_recovery_fault(recovery_fault_publisher(1, 1, 1), 1)
            .await
            .unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let admitted = store.load_record().await.unwrap().unwrap();

    let error = store
        .record_recovery_fault(recovery_fault_publisher(1, 2, 1), 2)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ClusterCheckpointAuthorityError::Authority(LeaseError::Invalid(message))
            if message.contains("two recovery fault publishers")
    ));
    assert_eq!(store.load_record().await.unwrap().unwrap(), admitted);
}

#[tokio::test]
async fn recovery_release_authorization_rejects_uncommitted_and_stale_terminals() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);
    assert_eq!(
        store.record_recovery_fault(publisher, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let first = recovery_release_terminal(&store, &lease, 1, 0).await;
    let first_reference = store.stage_recovery_release_terminal(&first).await.unwrap();

    assert!(!store
        .authorize_recovery_release(publisher, &first)
        .await
        .unwrap());
    assert_eq!(
        store
            .record_recovery_release_commit(&lease.proof(), first_reference.clone())
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::Created(first_reference)
    );
    assert!(store
        .authorize_recovery_release(publisher, &first)
        .await
        .unwrap());

    assert_eq!(
        store.record_recovery_fault(publisher, 2).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let second = recovery_release_terminal(&store, &lease, 2, 0).await;
    commit_recovery_release(&store, &lease, &second).await;

    assert!(!store
        .authorize_recovery_release(publisher, &first)
        .await
        .unwrap());
    assert!(store
        .authorize_recovery_release(publisher, &second)
        .await
        .unwrap());
}

#[tokio::test]
async fn fault_report_before_recovery_release_cas_returns_faults_changed() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);
    assert_eq!(
        store.record_recovery_fault(publisher, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
    let reference = store
        .stage_recovery_release_terminal(&terminal)
        .await
        .unwrap();

    assert_eq!(
        store.record_recovery_fault(publisher, 2).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    assert_eq!(
        store
            .record_recovery_release_commit(&lease.proof(), reference)
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::FaultsChanged
    );
    assert_eq!(
        store.recovery_fault_inventory().await.unwrap().faults(),
        &[RecoveryFault {
            reporter: incumbent.node,
            sequence: 3,
            disposition: RecoveryFaultDisposition::Recoverable,
        }]
    );
    assert_eq!(
        store.latest_recovery_release_terminal().await.unwrap(),
        None
    );
}

#[tokio::test]
async fn recovery_release_cas_before_fault_report_preserves_both_facts() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);
    assert_eq!(
        store.record_recovery_fault(publisher, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
    let reference = commit_recovery_release(&store, &lease, &terminal).await;

    assert_eq!(
        store.record_recovery_fault(publisher, 2).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    assert_eq!(
        store.latest_recovery_release_terminal().await.unwrap(),
        Some(terminal.clone())
    );
    assert_eq!(
        store.recovery_fault_inventory().await.unwrap().faults(),
        &[RecoveryFault {
            reporter: incumbent.node,
            sequence: 4,
            disposition: RecoveryFaultDisposition::Recoverable,
        }]
    );
    assert!(!store
        .authorize_recovery_release(publisher, &terminal)
        .await
        .unwrap());
    assert_eq!(
        store
            .record_recovery_release_commit(&lease.proof(), reference.clone())
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::Unchanged(reference)
    );
}

#[tokio::test]
async fn recovery_admission_snapshot_retries_when_faults_change_during_terminal_read() {
    let inner = Arc::new(InMemory::new());
    let object_store: Arc<dyn ObjectStore> = inner.clone();
    let setup = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = setup.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let initial = setup.recovery_admission_snapshot().await.unwrap();
    assert_eq!(initial.committed_release(), None);
    assert!(initial.fault_inventory().faults().is_empty());
    assert!(setup
        .recovery_admission_is_current(&initial, &lease.proof())
        .await
        .unwrap());
    let publisher = owner_recovery_fault_publisher(&incumbent);
    let terminal = recovery_release_terminal_after_owner_fault(&setup, &lease, 7, 4).await;
    let reference = commit_recovery_release(&setup, &lease, &terminal).await;
    let settled = setup.recovery_admission_snapshot().await.unwrap();
    assert_eq!(settled.committed_release(), Some(&terminal));
    assert!(settled.fault_inventory().faults().is_empty());
    assert!(setup
        .recovery_admission_is_current(&settled, &lease.proof())
        .await
        .unwrap());

    let current = setup.load_record().await.unwrap().unwrap();
    let sequence = current.lease.seq + 1;
    let mut changed = current.preserve_with_lease(LeaderLease {
        seq: sequence,
        renewal_sequence: current.lease.renewal_sequence,
        token: current.lease.token,
        owner: current.lease.owner.clone(),
        expires_at_ms: current.lease.expires_at_ms,
        catalog_manifest: current.lease.catalog_manifest.clone(),
    });
    let slot = AuthorityRecoveryFaultSlot {
        publisher,
        request_sequence: 8,
        fault_sequence: sequence,
        disposition: RecoveryFaultDisposition::Recoverable,
        active: true,
    };
    match changed
        .recovery_fault_slots
        .binary_search_by_key(&publisher.participant.node_id, |slot| {
            slot.publisher.participant.node_id
        }) {
        Ok(index) => changed.recovery_fault_slots[index] = slot,
        Err(index) => changed.recovery_fault_slots.insert(index, slot),
    }
    changed.recovery_fault_revision = sequence;
    changed.validate().unwrap();
    let changed_path = lease_path(changed.lease.seq);
    let changed_body = encode_authority_record(&changed).unwrap();

    let terminal_path = recovery_release_terminal_path(&reference);
    let (raw, store) = replacing_once_on_get(
        1_000,
        object_store,
        terminal_path.clone(),
        changed_path,
        changed_body,
        false,
    );
    let current = store.recovery_admission_snapshot().await.unwrap();

    assert!(raw.did_replace.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(raw.get_count(&terminal_path), 2);
    assert_eq!(current.committed_release(), Some(&terminal));
    assert_eq!(current.authority_sequence, changed.lease.seq);
    assert_eq!(current.fault_inventory().revision(), changed.lease.seq);
    assert_eq!(
        current.fault_inventory().faults(),
        &[RecoveryFault {
            reporter: incumbent.node,
            sequence: changed.lease.seq,
            disposition: RecoveryFaultDisposition::Recoverable,
        }]
    );

    raw.clear_get_counts();
    assert!(!store
        .recovery_admission_is_current(&settled, &lease.proof())
        .await
        .unwrap());
    assert!(!store
        .recovery_admission_is_current(&current, &lease.proof())
        .await
        .unwrap());
    assert_eq!(raw.get_count(&terminal_path), 0);
}

#[tokio::test]
async fn recovery_admission_revalidation_rejects_leader_takeover() {
    let store = store(1);
    let incumbent = owner(1, 1, 1);
    let rival = owner(2, 2, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let terminal = recovery_release_terminal_after_owner_fault(&store, &lease, 7, 4).await;
    commit_recovery_release(&store, &lease, &terminal).await;
    let snapshot = store.recovery_admission_snapshot().await.unwrap();
    assert!(store
        .recovery_admission_is_current(&snapshot, &lease.proof())
        .await
        .unwrap());

    let observed = store.load().await.unwrap().unwrap();
    let observation = LeaderLeaseObservation {
        lease: observed,
        started: Instant::now()
            .checked_sub(Duration::from_millis(2))
            .unwrap(),
    };
    let LeaseOutcome::Acquired(takeover) =
        store.try_takeover(&rival, &observation, 2).await.unwrap()
    else {
        panic!("expired incumbent must be replaced");
    };

    let after = store.recovery_admission_snapshot().await.unwrap();
    assert_eq!(after.committed_release(), snapshot.committed_release());
    assert_eq!(after.fault_inventory(), snapshot.fault_inventory());
    assert!(!store
        .recovery_admission_is_current(&snapshot, &lease.proof())
        .await
        .unwrap());
    assert!(store
        .recovery_admission_is_current(&snapshot, &takeover.proof())
        .await
        .unwrap());
}

#[tokio::test]
async fn recovery_release_compacts_tombstones_without_reusing_fault_sequences() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let first = recovery_fault_publisher(2, 2, 1);
    assert_eq!(
        store.record_recovery_fault(first, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let first_sequence = store.recovery_fault_inventory().await.unwrap().faults()[0].sequence;
    let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
    commit_recovery_release(&store, &lease, &terminal).await;

    for offset in 0..=MAX_RECOVERY_FAULT_SLOTS {
        let ordinal = u64::try_from(offset).unwrap();
        let publisher = recovery_fault_publisher(10_000 + ordinal, 10_000 + u128::from(ordinal), 1);
        assert_eq!(
            store.record_recovery_fault(publisher, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let terminal = recovery_release_terminal(&store, &lease, 2 + ordinal, 0).await;
        commit_recovery_release(&store, &lease, &terminal).await;
        let head = store.load_record().await.unwrap().unwrap();
        assert!(head.recovery_fault_slots.is_empty());
        assert_eq!(head.recovery_fault_revision, head.lease.seq);
    }

    assert_eq!(
        store.record_recovery_fault(first, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let retried = store.recovery_fault_inventory().await.unwrap();
    assert_eq!(retried.faults().len(), 1);
    assert!(retried.faults()[0].sequence > first_sequence);
}

#[tokio::test]
async fn full_unavailable_fault_inventory_is_compacted_before_new_admission() {
    let incumbent = owner(1, 1, 1);
    let sequence = u64::try_from(MAX_RECOVERY_FAULT_SLOTS).unwrap() + 1;
    let lease = LeaderLease {
        seq: sequence,
        renewal_sequence: 1,
        token: 1,
        owner: incumbent.clone(),
        expires_at_ms: i64::MAX,
        catalog_manifest: None,
    };
    let mut record = LeaderAuthorityRecord::initial(lease.clone());
    record.recovery_fault_revision = sequence;
    record.recovery_fault_slots = (0..MAX_RECOVERY_FAULT_SLOTS)
        .map(|index| {
            let ordinal = u64::try_from(index).unwrap() + 2;
            AuthorityRecoveryFaultSlot {
                publisher: recovery_fault_publisher(ordinal, u128::from(ordinal), 1),
                request_sequence: 1,
                fault_sequence: ordinal,
                disposition: RecoveryFaultDisposition::Recoverable,
                active: true,
            }
        })
        .collect();
    let store = store(1_000);
    store
        .store
        .put(
            &lease_path(sequence),
            PutPayload::from(encode_authority_record(&record).unwrap()),
        )
        .await
        .unwrap();
    let inventory = store.recovery_fault_inventory().await.unwrap();
    let terminal = RecoveryAnnouncement {
        round: RecoveryRound::new(
            1,
            lease.proof(),
            assignment_fence(&incumbent),
            Vec::new(),
            inventory.revision(),
            inventory.faults().to_vec(),
        )
        .unwrap(),
        phase: RecoverPhase::ReleaseCommitted { epoch: 0 },
    };

    commit_recovery_release(&store, &lease, &terminal).await;
    let settled = store.load_record().await.unwrap().unwrap();
    assert!(settled.recovery_fault_slots.is_empty());
    assert_eq!(settled.recovery_fault_revision, settled.lease.seq);
    assert!(settled.validate().is_ok());

    let unseen = recovery_fault_publisher(sequence + 1, u128::from(sequence + 1), 1);
    assert_eq!(
        store.record_recovery_fault(unseen, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let admitted = store.recovery_fault_inventory().await.unwrap();
    assert_eq!(admitted.faults().len(), 1);
    assert!(admitted.faults()[0].sequence > sequence);
}

#[test]
fn recovery_fault_slot_capacity_preserves_authority_headroom() {
    fn authority_with_fault_slots(slot_count: usize, wide_values: bool) -> LeaderAuthorityRecord {
        let sequence = if wide_values {
            u64::MAX
        } else {
            u64::try_from(slot_count.max(1)).unwrap()
        };
        let mut record = LeaderAuthorityRecord::initial(LeaderLease {
            seq: sequence,
            renewal_sequence: 1,
            token: 1,
            owner: owner(1, 1, 1),
            expires_at_ms: i64::MAX,
            catalog_manifest: None,
        });
        record.recovery_fault_revision = sequence;
        record.recovery_fault_slots = (0..slot_count)
            .map(|index| {
                let ordinal = u64::try_from(index).unwrap() + 1;
                let node_id = if wide_values {
                    u64::MAX - u64::try_from(slot_count - 1 - index).unwrap()
                } else {
                    ordinal
                };
                AuthorityRecoveryFaultSlot {
                    publisher: RecoveryFaultPublisher {
                        participant: crate::checkpoint::CheckpointParticipant {
                            node_id,
                            boot_incarnation: if wide_values {
                                Uuid::from_u128(
                                    u128::MAX - u128::try_from(slot_count - 1 - index).unwrap(),
                                )
                            } else {
                                Uuid::from_u128(u128::from(ordinal))
                            },
                        },
                        process_term: sequence,
                    },
                    request_sequence: sequence,
                    fault_sequence: if wide_values {
                        sequence - u64::try_from(slot_count - 1 - index).unwrap()
                    } else {
                        ordinal
                    },
                    disposition: RecoveryFaultDisposition::Recoverable,
                    active: true,
                }
            })
            .collect();
        record
    }

    let canonical_roster = authority_with_fault_slots(MAX_CHECKPOINT_PARTICIPANTS, true);
    let slot_bound = authority_with_fault_slots(MAX_RECOVERY_FAULT_SLOTS, true);
    for record in [&canonical_roster, &slot_bound] {
        record.validate().unwrap();
        let encoded = encode_authority_record(record).unwrap();
        let encoded_len = u64::try_from(encoded.len()).unwrap();
        assert!(
            encoded_len + RECOVERY_FAULT_AUTHORITY_HEADROOM_BYTES <= MAX_AUTHORITY_RECORD_BYTES,
            "{} fault slots encoded to {encoded_len} bytes",
            record.recovery_fault_slots.len()
        );
    }

    let overflow = authority_with_fault_slots(MAX_RECOVERY_FAULT_SLOTS + 1, true);
    assert!(overflow.validate().is_err());

    let mut future_fault = authority_with_fault_slots(1, false);
    future_fault.lease.seq = 2;
    future_fault.recovery_fault_slots[0].fault_sequence = 2;
    assert!(future_fault.validate().is_err());

    let mut orphaned_revision = LeaderAuthorityRecord::initial(LeaderLease {
        seq: 2,
        renewal_sequence: 1,
        token: 1,
        owner: owner(1, 1, 1),
        expires_at_ms: 1,
        catalog_manifest: None,
    });
    orphaned_revision.recovery_fault_revision = 2;
    assert!(orphaned_revision.validate().is_err());
}

#[tokio::test]
async fn exact_owner_renews_without_advancing_token() {
    let store = store(1_000);
    let owner = owner(1, 1, 4);
    let LeaseOutcome::Acquired(first) = store.begin_new_term(&owner, 10).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let LeaseOutcome::Acquired(second) = store.renew_exact(&owner, first.token, 500).await.unwrap()
    else {
        panic!("exact owner must renew");
    };
    assert_eq!((first.seq, first.renewal_sequence, first.token), (1, 1, 1));
    assert_eq!(
        (second.seq, second.renewal_sequence, second.token),
        (2, 2, 1)
    );
}

#[tokio::test]
async fn acquisition_new_term_and_takeover_advance_the_renewal_sequence() {
    let store = store(10);
    let incumbent = owner(1, 1, 4);
    let rival = owner(2, 2, 1);
    let LeaseOutcome::Acquired(first) = store.begin_new_term(&incumbent, 10).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let LeaseOutcome::Acquired(renewed) = store
        .renew_exact(&incumbent, first.token, 20)
        .await
        .unwrap()
    else {
        panic!("exact owner must renew");
    };
    let LeaseOutcome::Acquired(new_term) = store.begin_new_term(&incumbent, 30).await.unwrap()
    else {
        panic!("same owner must begin a new authority term");
    };
    let observation = store.observe_rival(&rival, &new_term).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let LeaseOutcome::Acquired(takeover) =
        store.try_takeover(&rival, &observation, 40).await.unwrap()
    else {
        panic!("expired rival must be replaced");
    };

    assert_eq!((first.seq, first.renewal_sequence, first.token), (1, 1, 1));
    assert_eq!(
        (renewed.seq, renewed.renewal_sequence, renewed.token),
        (2, 2, 1)
    );
    assert_eq!(
        (new_term.seq, new_term.renewal_sequence, new_term.token),
        (3, 3, 2)
    );
    assert_eq!(
        (
            takeover.seq,
            takeover.renewal_sequence,
            takeover.token,
            takeover.owner,
        ),
        (4, 4, 3, rival)
    );
}

#[test]
fn lease_validation_rejects_an_invalid_renewal_sequence() {
    let mut lease = LeaderLease {
        seq: 2,
        renewal_sequence: 0,
        token: 1,
        owner: owner(1, 1, 1),
        expires_at_ms: 1,
        catalog_manifest: None,
    };
    assert!(lease.validate().is_err());
    lease.renewal_sequence = 3;
    assert!(lease.validate().is_err());
    lease.renewal_sequence = 2;
    assert!(lease.validate().is_ok());
}

#[tokio::test]
async fn exact_renewal_rejects_a_missing_or_newer_authority_term() {
    let empty = store(1_000);
    let owner = owner(1, 1, 4);
    assert!(empty.renew_exact(&owner, 1, 10).await.is_err());

    let store = store(1_000);
    let LeaseOutcome::Acquired(first) = store.begin_new_term(&owner, 10).await.unwrap() else {
        panic!("empty authority must be acquired");
    };
    let LeaseOutcome::Acquired(new_term) = store.begin_new_term(&owner, 20).await.unwrap() else {
        panic!("same-owner reacquisition must rotate its term");
    };
    let error = store
        .renew_exact(&owner, first.token, 30)
        .await
        .unwrap_err();
    assert!(matches!(error, LeaseError::Fenced(_)));
    assert!(new_term.token > first.token);
}

#[tokio::test]
async fn fast_rival_clock_cannot_steal() {
    let store = store(30);
    let incumbent = owner(1, 1, 1);
    let rival = owner(2, 2, 1);
    store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap();
    let LeaseOutcome::Held(current) = store
        .acquire_or_renew_current_term_for_test(&rival, i64::MAX - 30)
        .await
        .unwrap()
    else {
        panic!("wall time must not authorize a takeover");
    };
    let observation = store.observe_rival(&rival, &current).unwrap();
    let LeaseOutcome::Held(_) = store
        .try_takeover(&rival, &observation, i64::MAX - 30)
        .await
        .unwrap()
    else {
        panic!("a full local observation is mandatory");
    };
}

#[tokio::test]
async fn renewal_invalidates_observation_despite_backward_owner_clock() {
    let store = store(20);
    let incumbent = owner(1, 1, 1);
    let rival = owner(2, 2, 1);
    store
        .acquire_or_renew_current_term_for_test(&incumbent, 10_000)
        .await
        .unwrap();
    let LeaseOutcome::Held(first) = store
        .acquire_or_renew_current_term_for_test(&rival, 0)
        .await
        .unwrap()
    else {
        panic!("rival must observe the incumbent");
    };
    let observation = store.observe_rival(&rival, &first).unwrap();
    store
        .acquire_or_renew_current_term_for_test(&incumbent, -10_000)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    let LeaseOutcome::Held(current) = store.try_takeover(&rival, &observation, 0).await.unwrap()
    else {
        panic!("renewal must invalidate the old observation");
    };
    assert_eq!(current.seq, 2);
    assert_eq!(current.renewal_sequence, 2);
    assert_eq!(current.owner, incumbent);
}

#[tokio::test]
async fn recovery_fault_append_does_not_reset_takeover_observation() {
    let store = store(60);
    let incumbent = owner(1, 1, 1);
    let rival = owner(2, 2, 1);
    let LeaseOutcome::Acquired(first) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("initial leader acquisition");
    };
    let observation = store.observe_rival(&rival, &first).unwrap();

    tokio::time::sleep(Duration::from_millis(35)).await;
    assert_eq!(
        store
            .record_recovery_fault(owner_recovery_fault_publisher(&incumbent), 1)
            .await
            .unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let appended = store.load().await.unwrap().unwrap();
    assert_eq!(appended.seq, 2);
    assert_eq!(appended.renewal_sequence, first.renewal_sequence);
    tokio::time::sleep(Duration::from_millis(30)).await;

    let LeaseOutcome::Acquired(takeover) =
        store.try_takeover(&rival, &observation, 1).await.unwrap()
    else {
        panic!("recovery fault append reset the takeover observation");
    };
    assert_eq!(takeover.seq, 3);
    assert_eq!(takeover.renewal_sequence, 2);
    assert_eq!(takeover.owner, rival);
}

#[tokio::test]
async fn repeated_recovery_fault_contention_cannot_starve_an_expired_takeover() {
    let (raw, store) = blocking_once_at(10, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let rival = owner(2, 2, 1);
    let LeaseOutcome::Acquired(first) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("initial leader acquisition");
    };
    let publisher = owner_recovery_fault_publisher(&incumbent);
    let observation = store.observe_rival(&rival, &first).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    let takeover_store = Arc::clone(&store);
    let takeover_owner = rival.clone();
    let takeover = tokio::spawn(async move {
        takeover_store
            .try_takeover(&takeover_owner, &observation, 1)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    assert_eq!(
        store.record_recovery_fault(publisher, 1).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    assert_eq!(
        store.record_recovery_fault(publisher, 2).await.unwrap(),
        RecordRecoveryFaultResult::Active
    );
    let contending_head = store.load().await.unwrap().unwrap();
    assert_eq!(contending_head.seq, 3);
    assert_eq!(contending_head.renewal_sequence, 1);
    raw.release.add_permits(1);

    let LeaseOutcome::Acquired(replacement) =
        tokio::time::timeout(Duration::from_secs(1), takeover)
            .await
            .expect("takeover was starved by recovery fault appends")
            .unwrap()
            .unwrap()
    else {
        panic!("unchanged liveness identity did not retry after CAS contention");
    };
    assert_eq!(replacement.seq, 4);
    assert_eq!(replacement.renewal_sequence, 2);
    assert_eq!(replacement.owner, rival);
}

#[tokio::test]
async fn unchanged_liveness_observation_is_required_for_takeover() {
    let store = store(15);
    let incumbent = owner(1, 1, 1);
    let rival = owner(2, 2, 1);
    store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap();
    let LeaseOutcome::Held(current) = store
        .acquire_or_renew_current_term_for_test(&rival, 0)
        .await
        .unwrap()
    else {
        panic!("rival must be held");
    };
    let observation = store.observe_rival(&rival, &current).unwrap();
    assert!(matches!(
        store.try_takeover(&rival, &observation, 0).await.unwrap(),
        LeaseOutcome::Held(_)
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let LeaseOutcome::Acquired(lease) = store.try_takeover(&rival, &observation, 0).await.unwrap()
    else {
        panic!("unchanged rival may be replaced after a full TTL");
    };
    assert_eq!(
        (lease.seq, lease.renewal_sequence, lease.token, lease.owner,),
        (2, 2, 2, rival)
    );
}

#[tokio::test]
async fn same_node_new_boot_is_a_rival_and_advances_token() {
    let store = store(10);
    let old = owner(7, 1, 3);
    let replacement = owner(7, 2, 4);
    store
        .acquire_or_renew_current_term_for_test(&old, 0)
        .await
        .unwrap();
    let LeaseOutcome::Held(current) = store
        .acquire_or_renew_current_term_for_test(&replacement, 0)
        .await
        .unwrap()
    else {
        panic!("new boot cannot renew an old boot's token");
    };
    let observation = store.observe_rival(&replacement, &current).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let LeaseOutcome::Acquired(lease) = store
        .try_takeover(&replacement, &observation, 0)
        .await
        .unwrap()
    else {
        panic!("replacement must acquire");
    };
    assert_eq!(lease.token, 2);
    assert_eq!(lease.owner, replacement);
}

#[tokio::test]
async fn two_racers_have_one_winner() {
    let (raw, store) = blocking_store_at(1_000, lease_path(1));
    let left_owner = owner(1, 1, 1);
    let right_owner = owner(2, 2, 1);
    let left_store = Arc::clone(&store);
    let left = tokio::spawn(async move {
        left_store
            .acquire_or_renew_current_term_for_test(&left_owner, 0)
            .await
    });
    let right_store = Arc::clone(&store);
    let right = tokio::spawn(async move {
        right_store
            .acquire_or_renew_current_term_for_test(&right_owner, 0)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    raw.release.add_permits(2);
    let (left, right) = tokio::join!(left, right);
    let left = left.unwrap().unwrap();
    let right = right.unwrap().unwrap();
    assert_eq!(
        usize::from(matches!(left, LeaseOutcome::Acquired(_)))
            + usize::from(matches!(right, LeaseOutcome::Acquired(_))),
        1
    );
    let durable = store.load().await.unwrap().unwrap();
    assert!(matches!(
        (&left, &right),
        (LeaseOutcome::Acquired(winner), LeaseOutcome::Held(held))
            | (LeaseOutcome::Held(held), LeaseOutcome::Acquired(winner))
            if winner == &durable && held == &durable
    ));
}

#[tokio::test]
async fn exact_renewal_is_fenced_when_a_rival_wins_its_cas_sequence() {
    let (raw, store) = blocking_once_at(10, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let rival = owner(2, 2, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        panic!("initial leader acquisition");
    };
    let observation = store.observe_rival(&rival, &first).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let renewing_store = Arc::clone(&store);
    let renewing_owner = incumbent.clone();
    let renewal = tokio::spawn(async move {
        renewing_store
            .renew_exact(&renewing_owner, first.token, 1)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let LeaseOutcome::Acquired(replacement) =
        store.try_takeover(&rival, &observation, 2).await.unwrap()
    else {
        panic!("observed rival must win the blocked renewal sequence");
    };
    raw.release.add_permits(1);
    let error = renewal.await.unwrap().unwrap_err();

    assert!(matches!(error, LeaseError::Fenced(_)));
    assert_eq!(store.load().await.unwrap(), Some(replacement));
}

#[tokio::test]
async fn shared_local_filesystem_rejects_authority_head_cas() {
    let temp = tempfile::tempdir().unwrap();
    let filesystem: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp.path()).unwrap());
    let store = LeaderLeaseStore::new(filesystem, 1_000);
    let owner = owner(1, 1, 1);
    assert!(matches!(
        store
            .acquire_or_renew_current_term_for_test(&owner, 0)
            .await
            .unwrap(),
        LeaseOutcome::Acquired(LeaderLease { seq: 1, .. })
    ));
    let error = store
        .acquire_or_renew_current_term_for_test(&owner, 1)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("PutMode::Update"), "{error}");
    assert_eq!(
        read_authority_head_pointer(store.store.as_ref())
            .await
            .unwrap()
            .unwrap()
            .pointer
            .sequence,
        1
    );
    assert!(read_authority_record(store.store.as_ref(), 2)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn store_contract_probe_accepts_in_memory_and_cleans_up() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);

    store
        .verify_store_contract(Duration::from_secs(1))
        .await
        .unwrap();

    assert_store_contract_probe_prefix_empty(object_store.as_ref()).await;
}

#[tokio::test]
async fn store_contract_probe_rejects_local_filesystem_and_cleans_up() {
    let temp = tempfile::tempdir().unwrap();
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp.path()).unwrap());
    let store = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);

    let error = store
        .verify_store_contract(Duration::from_secs(1))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("PutMode::Update"), "{error}");
    assert_store_contract_probe_prefix_empty(object_store.as_ref()).await;
}

#[tokio::test]
async fn store_contract_probe_rejects_update_as_overwrite_and_cleans_up() {
    let faulty = Arc::new(ContractFaultStore::new(ContractFault::UpdateAsOverwrite));
    let object_store: Arc<dyn ObjectStore> = faulty.clone();
    let store = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);

    let error = store
        .verify_store_contract(Duration::from_secs(1))
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("accepted a stale conditional update"),
        "{error}"
    );
    assert_eq!(
        faulty
            .update_count
            .load(std::sync::atomic::Ordering::Acquire),
        2
    );
    assert_store_contract_probe_prefix_empty(object_store.as_ref()).await;
}

#[tokio::test]
async fn store_contract_probe_rejects_versionless_get_before_update_and_cleans_up() {
    let faulty = Arc::new(ContractFaultStore::new(ContractFault::VersionlessGet));
    let object_store: Arc<dyn ObjectStore> = faulty.clone();
    let store = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);

    let error = store
        .verify_store_contract(Duration::from_secs(1))
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("returned neither ETag nor version"),
        "{error}"
    );
    assert_eq!(
        faulty
            .update_count
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert_store_contract_probe_prefix_empty(object_store.as_ref()).await;
}

#[tokio::test]
async fn concurrent_store_contract_probes_are_isolated_and_clean_up() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);
    let second = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);
    let third = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);
    let fourth = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);

    let results = tokio::join!(
        first.verify_store_contract(Duration::from_secs(1)),
        second.verify_store_contract(Duration::from_secs(1)),
        third.verify_store_contract(Duration::from_secs(1)),
        fourth.verify_store_contract(Duration::from_secs(1)),
    );

    results.0.unwrap();
    results.1.unwrap();
    results.2.unwrap();
    results.3.unwrap();
    assert_store_contract_probe_prefix_empty(object_store.as_ref()).await;
}

#[tokio::test]
async fn renewal_history_pruning_has_a_reader_grace_period() {
    let store = store(1);
    let owner = owner(1, 1, 1);
    for now in 0..8 {
        assert!(matches!(
            store
                .acquire_or_renew_current_term_for_test(&owner, now)
                .await
                .unwrap(),
            LeaseOutcome::Acquired(_)
        ));
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(matches!(
        store
            .acquire_or_renew_current_term_for_test(&owner, 9)
            .await
            .unwrap(),
        LeaseOutcome::Acquired(_)
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if store.list_seqs().await.unwrap() == vec![8, 9] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.load().await.unwrap().unwrap().seq, 9);
}

#[tokio::test]
async fn prune_never_deletes_records_newer_than_its_head_snapshot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for sequence in 1..=3 {
        object_store
            .put(
                &lease_path(sequence),
                PutPayload::from(Bytes::from_static(b"x")),
            )
            .await
            .unwrap();
    }
    let retained = BTreeSet::from([2, 3]);
    let snapshot_head_sequence = *retained.last().unwrap();

    object_store
        .put(
            &lease_path(snapshot_head_sequence + 1),
            PutPayload::from(Bytes::from_static(b"x")),
        )
        .await
        .unwrap();

    let (candidates, exhausted) =
        LeaderLeaseStore::prune_candidates(&object_store, &retained, snapshot_head_sequence, 0)
            .await
            .unwrap();
    assert!(exhausted);
    assert_eq!(candidates, vec![lease_path(1)]);
}

#[derive(Clone, Copy)]
enum ContractFault {
    VersionlessGet,
    UpdateAsOverwrite,
}

struct ContractFaultStore {
    inner: Arc<dyn ObjectStore>,
    fault: ContractFault,
    update_count: std::sync::atomic::AtomicUsize,
}

impl ContractFaultStore {
    fn new(fault: ContractFault) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            fault,
            update_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl std::fmt::Debug for ContractFaultStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContractFaultStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ContractFaultStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ContractFaultStore")
    }
}

#[async_trait]
impl ObjectStore for ContractFaultStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        mut options: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if matches!(&options.mode, PutMode::Update(_)) {
            self.update_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            if matches!(self.fault, ContractFault::UpdateAsOverwrite) {
                options.mode = PutMode::Overwrite;
            }
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &OsPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let mut result = self.inner.get_opts(location, options).await?;
        if matches!(self.fault, ContractFault::VersionlessGet) {
            result.meta.e_tag = None;
            result.meta.version = None;
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct BlockingStore {
    inner: Arc<dyn ObjectStore>,
    blocked_path: OsPath,
    block_put: bool,
    block_once: bool,
    block_after_put: bool,
    block_get_once: bool,
    did_block: std::sync::atomic::AtomicBool,
    ambiguous_path: Option<OsPath>,
    did_return_ambiguous: std::sync::atomic::AtomicBool,
    replacement_on_get: Option<(OsPath, Bytes, bool)>,
    did_replace: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    get_counts: Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>>,
    put_counts: Arc<std::sync::Mutex<std::collections::BTreeMap<(String, &'static str), u64>>>,
    list_count: std::sync::atomic::AtomicU64,
}

impl BlockingStore {
    fn clear_get_counts(&self) {
        self.get_counts.lock().unwrap().clear();
    }

    fn get_count(&self, location: &OsPath) -> u64 {
        self.get_counts
            .lock()
            .unwrap()
            .get(location.as_ref())
            .copied()
            .unwrap_or(0)
    }

    fn get_count_prefix(&self, prefix: &str) -> u64 {
        self.get_counts
            .lock()
            .unwrap()
            .iter()
            .filter(|(location, _)| location.starts_with(prefix))
            .map(|(_, count)| *count)
            .sum()
    }

    fn put_count(&self, location: &OsPath, mode: &'static str) -> u64 {
        self.put_counts
            .lock()
            .unwrap()
            .get(&(location.to_string(), mode))
            .copied()
            .unwrap_or(0)
    }

    fn list_count(&self) -> u64 {
        self.list_count.load(std::sync::atomic::Ordering::Acquire)
    }

    fn clear_authority_io_counts(&self) {
        self.clear_get_counts();
        self.put_counts.lock().unwrap().clear();
        self.list_count
            .store(0, std::sync::atomic::Ordering::Release);
    }
}

impl std::fmt::Debug for BlockingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlockingStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for BlockingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BlockingStore")
    }
}

#[async_trait]
impl ObjectStore for BlockingStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let mode = match &options.mode {
            PutMode::Overwrite => "overwrite",
            PutMode::Create => "create",
            PutMode::Update(_) => "update",
        };
        {
            let mut put_counts = self.put_counts.lock().unwrap();
            *put_counts.entry((location.to_string(), mode)).or_default() += 1;
        }
        let should_block = self.block_put
            && location == &self.blocked_path
            && (!self.block_once
                || !self
                    .did_block
                    .swap(true, std::sync::atomic::Ordering::AcqRel));
        if should_block && !self.block_after_put {
            self.entered.add_permits(1);
            let permit =
                self.release
                    .acquire()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "BlockingStore",
                        source: Box::new(error),
                    })?;
            permit.forget();
        }
        let result = self.inner.put_opts(location, payload, options).await;
        if should_block && self.block_after_put {
            self.entered.add_permits(1);
            let permit =
                self.release
                    .acquire()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "BlockingStore",
                        source: Box::new(error),
                    })?;
            permit.forget();
        }
        if result.is_ok()
            && self.ambiguous_path.as_ref() == Some(location)
            && !self
                .did_return_ambiguous
                .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(object_store::Error::Generic {
                store: "BlockingStore",
                source: Box::new(std::io::Error::other("injected ambiguous create response")),
            });
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &OsPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        {
            let mut get_counts = self.get_counts.lock().unwrap();
            *get_counts.entry(location.to_string()).or_default() += 1;
        }
        if self.block_get_once
            && location == &self.blocked_path
            && !self
                .did_block
                .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.entered.add_permits(1);
            let permit =
                self.release
                    .acquire()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "BlockingStore",
                        source: Box::new(error),
                    })?;
            permit.forget();
        }
        if location == &self.blocked_path
            && !self
                .did_replace
                .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            if let Some((replacement_path, replacement, remove_blocked)) = &self.replacement_on_get
            {
                self.inner
                    .put_opts(
                        replacement_path,
                        PutPayload::from(replacement.clone()),
                        PutOptions {
                            mode: PutMode::Create,
                            ..PutOptions::default()
                        },
                    )
                    .await?;
                if *remove_blocked {
                    self.inner.delete(location).await?;
                }
            }
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.list_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn blocking_store_at(
    ttl_ms: i64,
    blocked_path: OsPath,
) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    let raw = Arc::new(BlockingStore {
        inner: Arc::new(InMemory::new()),
        blocked_path,
        block_put: true,
        block_once: false,
        block_after_put: false,
        block_get_once: false,
        did_block: std::sync::atomic::AtomicBool::new(false),
        ambiguous_path: None,
        did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
        replacement_on_get: None,
        did_replace: std::sync::atomic::AtomicBool::new(false),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        list_count: std::sync::atomic::AtomicU64::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
    (raw, authority)
}

fn blocking_get_once_at(
    ttl_ms: i64,
    blocked_path: OsPath,
) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    blocking_get_once_with_inner(ttl_ms, Arc::new(InMemory::new()), blocked_path)
}

fn blocking_get_once_with_inner(
    ttl_ms: i64,
    inner: Arc<dyn ObjectStore>,
    blocked_path: OsPath,
) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    let raw = Arc::new(BlockingStore {
        inner,
        blocked_path,
        block_put: false,
        block_once: true,
        block_after_put: false,
        block_get_once: true,
        did_block: std::sync::atomic::AtomicBool::new(false),
        ambiguous_path: None,
        did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
        replacement_on_get: None,
        did_replace: std::sync::atomic::AtomicBool::new(false),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        list_count: std::sync::atomic::AtomicU64::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
    (raw, authority)
}

fn replacing_once_on_get(
    ttl_ms: i64,
    inner: Arc<dyn ObjectStore>,
    blocked_path: OsPath,
    replacement_path: OsPath,
    replacement: Bytes,
    remove_blocked: bool,
) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    let raw = Arc::new(BlockingStore {
        inner,
        blocked_path,
        block_put: true,
        block_once: true,
        block_after_put: false,
        block_get_once: false,
        did_block: std::sync::atomic::AtomicBool::new(false),
        ambiguous_path: None,
        did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
        replacement_on_get: Some((replacement_path, replacement, remove_blocked)),
        did_replace: std::sync::atomic::AtomicBool::new(false),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        list_count: std::sync::atomic::AtomicU64::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
    (raw, authority)
}

fn blocking_once_at(
    ttl_ms: i64,
    blocked_path: OsPath,
) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    let raw = Arc::new(BlockingStore {
        inner: Arc::new(InMemory::new()),
        blocked_path,
        block_put: true,
        block_once: true,
        block_after_put: false,
        block_get_once: false,
        did_block: std::sync::atomic::AtomicBool::new(false),
        ambiguous_path: None,
        did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
        replacement_on_get: None,
        did_replace: std::sync::atomic::AtomicBool::new(false),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        list_count: std::sync::atomic::AtomicU64::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
    (raw, authority)
}

#[cfg(feature = "cluster")]
fn delayed_response_once_at(
    ttl_ms: i64,
    blocked_path: OsPath,
) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    delayed_response_once_at_with_ambiguity(ttl_ms, blocked_path, false)
}

fn delayed_ambiguous_response_once_at(
    ttl_ms: i64,
    blocked_path: OsPath,
) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    delayed_response_once_at_with_ambiguity(ttl_ms, blocked_path, true)
}

fn delayed_response_once_at_with_ambiguity(
    ttl_ms: i64,
    blocked_path: OsPath,
    ambiguous: bool,
) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    let raw = Arc::new(BlockingStore {
        inner: Arc::new(InMemory::new()),
        ambiguous_path: ambiguous.then(|| blocked_path.clone()),
        blocked_path,
        block_put: true,
        block_once: true,
        block_after_put: true,
        block_get_once: false,
        did_block: std::sync::atomic::AtomicBool::new(false),
        did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
        replacement_on_get: None,
        did_replace: std::sync::atomic::AtomicBool::new(false),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        list_count: std::sync::atomic::AtomicU64::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
    (raw, authority)
}

fn ambiguous_once_at(
    ttl_ms: i64,
    ambiguous_path: OsPath,
) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    let raw = Arc::new(BlockingStore {
        inner: Arc::new(InMemory::new()),
        blocked_path: OsPath::from("control/never-block"),
        block_put: true,
        block_once: true,
        block_after_put: false,
        block_get_once: false,
        did_block: std::sync::atomic::AtomicBool::new(false),
        ambiguous_path: Some(ambiguous_path),
        did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
        replacement_on_get: None,
        did_replace: std::sync::atomic::AtomicBool::new(false),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        list_count: std::sync::atomic::AtomicU64::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
    (raw, authority)
}

fn bare_authority_record(owner: &LeaderLeaseOwner, sequence: u64) -> LeaderAuthorityRecord {
    LeaderAuthorityRecord::initial(LeaderLease {
        seq: sequence,
        renewal_sequence: sequence,
        token: 1,
        owner: owner.clone(),
        expires_at_ms: 1_000,
        catalog_manifest: None,
    })
}

async fn seed_authority_record(raw: &BlockingStore, record: &LeaderAuthorityRecord) {
    raw.inner
        .put_opts(
            &lease_path(record.lease.seq),
            PutPayload::from(encode_authority_record(record).unwrap()),
            PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            },
        )
        .await
        .unwrap();
}

async fn seed_authority_head(raw: &BlockingStore, sequence: u64) {
    raw.inner
        .put_opts(
            &authority_head_path(),
            PutPayload::from(encode_authority_head_pointer(sequence).unwrap()),
            PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn missing_head_discovers_once_repairs_successor_and_healthy_reads_never_list() {
    let (empty_raw, empty) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-empty-authority-head"),
    );
    assert!(empty.load().await.unwrap().is_none());
    assert_eq!(empty_raw.list_count(), 1);
    assert_eq!(empty_raw.put_count(&authority_head_path(), "create"), 0);

    let (raw, store) = delayed_ambiguous_response_once_at(1_000, authority_head_path());
    let incumbent = owner(1, 1, 1);
    let stale = bare_authority_record(&incumbent, 1);
    let retained_previous = bare_authority_record(&incumbent, 6);
    let retained_head = bare_authority_record(&incumbent, 7);
    let orphan_successor = bare_authority_record(&incumbent, 8);
    seed_authority_record(&raw, &stale).await;
    seed_authority_record(&raw, &retained_previous).await;
    seed_authority_record(&raw, &retained_head).await;
    raw.clear_authority_io_counts();

    let recovery = {
        let store = Arc::clone(&store);
        tokio::spawn(async move { store.load().await })
    };
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    seed_authority_record(&raw, &orphan_successor).await;
    raw.release.add_permits(1);

    assert_eq!(
        recovery.await.unwrap().unwrap(),
        Some(orphan_successor.lease.clone())
    );
    assert!(raw
        .did_return_ambiguous
        .load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(raw.list_count(), 1);
    assert_eq!(raw.put_count(&authority_head_path(), "create"), 1);
    assert_eq!(raw.put_count(&authority_head_path(), "update"), 1);
    assert_eq!(
        read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap()
            .pointer
            .sequence,
        8
    );

    raw.clear_authority_io_counts();
    assert_eq!(store.load().await.unwrap(), Some(orphan_successor.lease));
    assert!(store.cluster_outcome(1).await.unwrap().is_none());
    assert_eq!(raw.list_count(), 0);
    assert_eq!(raw.put_count(&authority_head_path(), "create"), 0);
    assert_eq!(raw.put_count(&authority_head_path(), "update"), 0);
    assert_eq!(raw.get_count(&authority_head_path()), 2);
    assert_eq!(raw.get_count(&lease_path(8)), 2);
    assert_eq!(raw.get_count(&lease_path(9)), 2);
}

#[tokio::test]
async fn pointer_update_without_a_native_version_fails_before_writing() {
    let (raw, store) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-versionless-authority-head"),
    );
    let first = bare_authority_record(&owner(1, 1, 1), 1);
    seed_authority_record(&raw, &first).await;
    seed_authority_head(&raw, 1).await;
    let before = read_authority_head_pointer(raw.inner.as_ref())
        .await
        .unwrap()
        .unwrap();
    let versionless = VersionedAuthorityHeadPointer {
        pointer: before.pointer,
        update_version: UpdateVersion {
            e_tag: None,
            version: None,
        },
    };
    raw.clear_authority_io_counts();

    let error = store
        .publish_authority_head(2, Some(&versionless))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("did not provide a native conditional update version"),
        "{error}"
    );
    assert_eq!(raw.put_count(&authority_head_path(), "update"), 0);
    assert_eq!(
        read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap()
            .pointer,
        before.pointer
    );
}

#[test]
fn same_sequence_authority_heads_have_unique_nonce_bodies() {
    let first = encode_authority_head_pointer(7).unwrap();
    let second = encode_authority_head_pointer(7).unwrap();
    let first_pointer: AuthorityHeadPointer = serde_json::from_slice(&first).unwrap();
    let second_pointer: AuthorityHeadPointer = serde_json::from_slice(&second).unwrap();

    assert_eq!(first_pointer.sequence, second_pointer.sequence);
    assert_ne!(first_pointer.nonce, second_pointer.nonce);
    assert_ne!(first, second);
}

#[tokio::test]
async fn record_before_pointer_crash_is_repaired_without_listing() {
    let (raw, store) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-record-before-pointer"),
    );
    let incumbent = owner(1, 1, 1);
    let first = bare_authority_record(&incumbent, 1);
    let second = first.preserve_with_lease(LeaderLease {
        seq: 2,
        renewal_sequence: 2,
        token: 1,
        owner: incumbent,
        expires_at_ms: 2_000,
        catalog_manifest: None,
    });
    seed_authority_record(&raw, &first).await;
    seed_authority_head(&raw, 1).await;
    seed_authority_record(&raw, &second).await;
    raw.clear_authority_io_counts();

    assert_eq!(store.load().await.unwrap(), Some(second.lease));
    assert_eq!(raw.list_count(), 0);
    assert_eq!(raw.put_count(&authority_head_path(), "update"), 1);
    assert_eq!(
        read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap()
            .pointer
            .sequence,
        2
    );
}

#[tokio::test]
async fn repaired_head_returns_a_coherent_snapshot_without_chasing_newer_orphans() {
    let (raw, store) = delayed_response_once_at(1_000, authority_head_path());
    let incumbent = owner(1, 1, 1);
    let first = bare_authority_record(&incumbent, 1);
    let second = bare_authority_record(&incumbent, 2);
    let third = bare_authority_record(&incumbent, 3);
    let fourth = bare_authority_record(&incumbent, 4);
    seed_authority_record(&raw, &first).await;
    seed_authority_head(&raw, 1).await;
    seed_authority_record(&raw, &second).await;
    raw.clear_authority_io_counts();

    let reader = {
        let store = Arc::clone(&store);
        tokio::spawn(async move { store.load().await })
    };
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let second_pointer = read_authority_head_pointer(raw.inner.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second_pointer.pointer.sequence, 2);
    seed_authority_record(&raw, &third).await;
    seed_authority_record(&raw, &fourth).await;
    raw.inner
        .put_opts(
            &authority_head_path(),
            PutPayload::from(encode_authority_head_pointer(3).unwrap()),
            PutOptions {
                mode: PutMode::Update(second_pointer.update_version),
                ..PutOptions::default()
            },
        )
        .await
        .unwrap();
    raw.release.add_permits(1);

    assert_eq!(reader.await.unwrap().unwrap(), Some(third.lease));
    assert_eq!(
        read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap()
            .pointer
            .sequence,
        3
    );
    assert!(raw.inner.get(&lease_path(4)).await.is_ok());
    assert_eq!(raw.put_count(&authority_head_path(), "update"), 1);
    assert_eq!(raw.list_count(), 0);
}

#[tokio::test]
async fn stalled_reader_retries_when_the_pointer_target_was_pruned() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (raw, store) = blocking_get_once_with_inner(1_000, inner, lease_path(1));
    let incumbent = owner(1, 1, 1);
    let first = bare_authority_record(&incumbent, 1);
    let second = bare_authority_record(&incumbent, 2);
    let third = bare_authority_record(&incumbent, 3);
    seed_authority_record(&raw, &first).await;
    seed_authority_head(&raw, 1).await;
    let reader = {
        let store = Arc::clone(&store);
        tokio::spawn(async move { store.load().await })
    };
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    seed_authority_record(&raw, &second).await;
    seed_authority_record(&raw, &third).await;
    raw.inner.delete(&authority_head_path()).await.unwrap();
    seed_authority_head(&raw, 3).await;
    raw.inner.delete(&lease_path(1)).await.unwrap();
    raw.release.add_permits(1);

    assert_eq!(reader.await.unwrap().unwrap(), Some(third.lease));
    assert_eq!(raw.list_count(), 0);
}

#[tokio::test]
async fn applied_but_ambiguous_pointer_update_is_reconciled() {
    let (raw, store) = ambiguous_once_at(1_000, authority_head_path());
    let incumbent = owner(1, 1, 1);
    let first = bare_authority_record(&incumbent, 1);
    seed_authority_record(&raw, &first).await;
    seed_authority_head(&raw, 1).await;
    store.prune_running.store(true, Ordering::Release);
    raw.clear_authority_io_counts();

    let LeaseOutcome::Acquired(renewed) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 1)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(renewed.seq, 2);
    assert!(raw
        .did_return_ambiguous
        .load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(raw.put_count(&authority_head_path(), "update"), 1);
    assert_eq!(raw.list_count(), 0);
    assert_eq!(
        read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap()
            .pointer
            .sequence,
        2
    );
}

#[tokio::test]
async fn healthy_renewal_reuses_the_loaded_head_without_extra_reads_or_listing() {
    let (raw, store) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-healthy-authority-append"),
    );
    let incumbent = owner(1, 1, 1);
    let first = bare_authority_record(&incumbent, 1);
    seed_authority_record(&raw, &first).await;
    seed_authority_head(&raw, 1).await;
    store.prune_running.store(true, Ordering::Release);
    raw.clear_authority_io_counts();

    let LeaseOutcome::Acquired(renewed) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 1)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(renewed.seq, 2);
    assert_eq!(raw.get_count(&authority_head_path()), 1);
    assert_eq!(raw.get_count(&lease_path(1)), 1);
    assert_eq!(raw.get_count(&lease_path(2)), 1);
    assert_eq!(raw.put_count(&lease_path(2), "create"), 1);
    assert_eq!(raw.put_count(&authority_head_path(), "update"), 1);
    assert_eq!(raw.list_count(), 0);
}

#[tokio::test]
async fn contenders_and_stale_repair_cannot_regress_the_pointer() {
    let (raw, store) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-pointer-contenders"),
    );
    let incumbent = owner(1, 1, 1);
    let first = bare_authority_record(&incumbent, 1);
    seed_authority_record(&raw, &first).await;
    seed_authority_head(&raw, 1).await;
    store.prune_running.store(true, Ordering::Release);
    let stale = read_authority_head_pointer(raw.inner.as_ref())
        .await
        .unwrap()
        .unwrap();
    let first_candidate = first.preserve_with_lease(LeaderLease {
        seq: 2,
        renewal_sequence: 2,
        token: 1,
        owner: incumbent.clone(),
        expires_at_ms: 2_000,
        catalog_manifest: None,
    });
    let second_candidate = first.preserve_with_lease(LeaderLease {
        expires_at_ms: 3_000,
        ..first_candidate.lease.clone()
    });
    let expected = store
        .load_published_authority_head()
        .await
        .unwrap()
        .unwrap();
    raw.clear_authority_io_counts();

    let (first_result, second_result) = tokio::join!(
        store.create_authority_record(Some(&expected), &first_candidate),
        store.create_authority_record(Some(&expected), &second_candidate)
    );
    let first_result = first_result.unwrap();
    let second_result = second_result.unwrap();
    assert_eq!(
        usize::from(matches!(&first_result, AuthorityCreateOutcome::Created))
            + usize::from(matches!(&second_result, AuthorityCreateOutcome::Created)),
        1
    );
    assert_eq!(
        usize::from(matches!(
            &first_result,
            AuthorityCreateOutcome::Contended(_)
        )) + usize::from(matches!(
            &second_result,
            AuthorityCreateOutcome::Contended(_)
        )),
        1
    );

    let winner = store
        .load_published_authority_head()
        .await
        .unwrap()
        .unwrap();
    let third = winner.record.preserve_with_lease(LeaderLease {
        seq: 3,
        renewal_sequence: 3,
        token: 1,
        owner: incumbent,
        expires_at_ms: 4_000,
        catalog_manifest: None,
    });
    assert!(matches!(
        store
            .create_authority_record(Some(&winner), &third)
            .await
            .unwrap(),
        AuthorityCreateOutcome::Created
    ));
    let before_stale = read_authority_head_pointer(raw.inner.as_ref())
        .await
        .unwrap()
        .unwrap()
        .pointer;
    assert_eq!(
        store.publish_authority_head(2, Some(&stale)).await.unwrap(),
        3
    );
    let after_stale = read_authority_head_pointer(raw.inner.as_ref())
        .await
        .unwrap()
        .unwrap()
        .pointer;
    assert_eq!(after_stale, before_stale);
    assert_eq!(after_stale.sequence, 3);
    assert_eq!(raw.list_count(), 0);
}

#[tokio::test]
async fn stalled_writer_recreating_a_pruned_sequence_is_contended() {
    let (raw, store) = blocking_store_at(1_000, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let first = bare_authority_record(&incumbent, 1);
    seed_authority_record(&raw, &first).await;
    seed_authority_head(&raw, 1).await;
    store.prune_running.store(true, Ordering::Release);
    let stale_candidate = first.preserve_with_lease(LeaderLease {
        seq: 2,
        renewal_sequence: 2,
        token: 1,
        owner: incumbent.clone(),
        expires_at_ms: 2_000,
        catalog_manifest: None,
    });
    let stale_retry = stale_candidate.clone();
    let stale_expected = store
        .load_published_authority_head()
        .await
        .unwrap()
        .unwrap();
    let stale_store = Arc::clone(&store);
    let stalled = tokio::spawn(async move {
        stale_store
            .create_authority_record(Some(&stale_expected), &stale_candidate)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let winner = first.preserve_with_lease(LeaderLease {
        seq: 2,
        renewal_sequence: 2,
        token: 1,
        owner: incumbent.clone(),
        expires_at_ms: 3_000,
        catalog_manifest: None,
    });
    seed_authority_record(&raw, &winner).await;
    let first_pointer = read_authority_head_pointer(raw.inner.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .publish_authority_head(2, Some(&first_pointer))
            .await
            .unwrap(),
        2
    );
    let third = winner.preserve_with_lease(LeaderLease {
        seq: 3,
        renewal_sequence: 3,
        token: 1,
        owner: incumbent,
        expires_at_ms: 4_000,
        catalog_manifest: None,
    });
    seed_authority_record(&raw, &third).await;
    let second_pointer = read_authority_head_pointer(raw.inner.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .publish_authority_head(3, Some(&second_pointer))
            .await
            .unwrap(),
        3
    );
    raw.inner.delete(&lease_path(2)).await.unwrap();

    raw.release.add_permits(1);
    let result = stalled.await.unwrap().unwrap();
    let AuthorityCreateOutcome::Contended(current) = result else {
        panic!("stale recreated sequence must be classified as contention");
    };
    assert_eq!(current.lease.seq, 3);
    assert_eq!(
        read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap()
            .pointer
            .sequence,
        3
    );
    assert!(read_authority_record(raw.inner.as_ref(), 2)
        .await
        .unwrap()
        .is_some());
    let current = store
        .load_published_authority_head()
        .await
        .unwrap()
        .unwrap();
    let AuthorityCreateOutcome::Contended(current) = store
        .create_authority_record(Some(&current), &stale_retry)
        .await
        .unwrap()
    else {
        panic!("stale exact residue must remain contended on retry");
    };
    assert_eq!(current.lease.seq, 3);
}

#[tokio::test]
async fn malformed_or_ahead_pointer_fails_closed_without_list_or_put() {
    let (malformed_raw, malformed_store) =
        blocking_store_at(1_000, OsPath::from("control/never-block-malformed-pointer"));
    malformed_raw
        .inner
        .put(
            &authority_head_path(),
            PutPayload::from(Bytes::from_static(b"{\"version\":1,\"sequence\":1}")),
        )
        .await
        .unwrap();
    malformed_raw.clear_authority_io_counts();
    assert!(malformed_store.load().await.is_err());
    assert_eq!(malformed_raw.list_count(), 0);
    assert_eq!(malformed_raw.put_count(&authority_head_path(), "create"), 0);
    assert_eq!(malformed_raw.put_count(&authority_head_path(), "update"), 0);

    let (ahead_raw, ahead_store) =
        blocking_store_at(1_000, OsPath::from("control/never-block-ahead-pointer"));
    let incumbent = owner(1, 1, 1);
    seed_authority_record(&ahead_raw, &bare_authority_record(&incumbent, 1)).await;
    seed_authority_head(&ahead_raw, 2).await;
    ahead_raw.clear_authority_io_counts();
    let error = ahead_store.load().await.unwrap_err();
    assert!(error.to_string().contains("points ahead"), "{error}");
    assert_eq!(ahead_raw.list_count(), 0);
    assert_eq!(ahead_raw.put_count(&authority_head_path(), "create"), 0);
    assert_eq!(ahead_raw.put_count(&authority_head_path(), "update"), 0);
}

#[tokio::test]
async fn pruning_lists_once_and_preserves_pointer_and_recent_records() {
    let (raw, store) = blocking_store_at(1_000, OsPath::from("control/never-block-pointer-prune"));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(_) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    disable_history_pruning_for_test(&store).await;
    for now in 1..5 {
        assert!(matches!(
            store
                .acquire_or_renew_current_term_for_test(&incumbent, now)
                .await
                .unwrap(),
            LeaseOutcome::Acquired(_)
        ));
    }
    raw.clear_authority_io_counts();

    LeaderLeaseStore::prune_history(&store.store, 0)
        .await
        .unwrap();
    assert_eq!(raw.list_count(), 1);
    assert!(raw.inner.get(&authority_head_path()).await.is_ok());
    assert!(raw.inner.get(&lease_path(4)).await.is_ok());
    assert!(raw.inner.get(&lease_path(5)).await.is_ok());
    assert_eq!(store.load().await.unwrap().unwrap().seq, 5);
    assert_eq!(raw.list_count(), 1);
}

fn catalog(name: &str) -> CatalogManifest {
    CatalogManifest::new(vec![super::super::CatalogManifestEntry {
        canonical_name: name.to_owned(),
        kind: crate::catalog::CatalogObjectKind::Source,
        catalog_generation: 1,
        ddl: format!("CREATE SOURCE {name} (id BIGINT)"),
    }])
    .unwrap()
}

#[tokio::test]
async fn committed_recovery_release_survives_renewal_and_takeover() {
    let store = Arc::new(store(1));
    let incumbent = owner(1, 11, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
    let reference = store
        .stage_recovery_release_terminal(&terminal)
        .await
        .unwrap();
    assert!(matches!(
        store
            .record_recovery_release_commit(&proof, reference.clone())
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::Created(_)
    ));
    let LeaseOutcome::Acquired(renewed) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 1)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let successor = owner(2, 22, 1);
    let observation = store.observe_rival(&successor, &renewed).unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let LeaseOutcome::Acquired(_) = store
        .try_takeover(&successor, &observation, 10)
        .await
        .unwrap()
    else {
        panic!("successor must acquire the expired authority");
    };

    assert_eq!(
        store.latest_recovery_release_terminal().await.unwrap(),
        Some(terminal)
    );
    assert!(matches!(
        store
            .record_recovery_release_commit(&proof, reference)
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::Unchanged(_)
    ));
}

#[tokio::test]
async fn takeover_before_recovery_release_append_fences_the_old_commit() {
    let (raw, store) = blocking_once_at(10, lease_path(3));
    let incumbent = owner(1, 11, 1);
    let successor = owner(2, 22, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
    let reference = store
        .stage_recovery_release_terminal(&terminal)
        .await
        .unwrap();
    let observation = store.observe_rival(&successor, &first).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    let committing = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            store
                .record_recovery_release_commit(&proof, reference)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(matches!(
        store
            .try_takeover(&successor, &observation, 20)
            .await
            .unwrap(),
        LeaseOutcome::Acquired(_)
    ));
    raw.release.add_permits(1);
    assert!(matches!(
        committing.await.unwrap(),
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));
    assert_eq!(
        store.latest_recovery_release_terminal().await.unwrap(),
        None
    );
}

#[tokio::test]
async fn ambiguous_recovery_release_create_reconciles_the_exact_winner() {
    let (raw, store) = ambiguous_once_at(1_000, lease_path(3));
    let incumbent = owner(1, 11, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
    let reference = store
        .stage_recovery_release_terminal(&terminal)
        .await
        .unwrap();
    assert!(matches!(
        store
            .record_recovery_release_commit(&first.proof(), reference)
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::Created(_)
    ));
    assert!(raw
        .did_return_ambiguous
        .load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        store.latest_recovery_release_terminal().await.unwrap(),
        Some(terminal)
    );
}

#[tokio::test]
async fn recovery_release_generation_has_one_exact_winner() {
    let store = store(1_000);
    let incumbent = owner(1, 11, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let first_terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
    let first_reference = store
        .stage_recovery_release_terminal(&first_terminal)
        .await
        .unwrap();
    store
        .record_recovery_release_commit(&proof, first_reference)
        .await
        .unwrap();

    let mut divergent = first_terminal.clone();
    divergent.phase = RecoverPhase::ReleaseCommitted { epoch: 5 };
    let divergent_reference = store
        .stage_recovery_release_terminal(&divergent)
        .await
        .unwrap();
    assert!(matches!(
        store
            .record_recovery_release_commit(&proof, divergent_reference)
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::Conflict { .. }
    ));

    let newer = recovery_release_terminal_after_owner_fault(&store, &first, 8, 5).await;
    let newer_reference = store.stage_recovery_release_terminal(&newer).await.unwrap();
    assert!(matches!(
        store
            .record_recovery_release_commit(&proof, newer_reference)
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::Created(_)
    ));
    assert_eq!(
        store.latest_recovery_release_terminal().await.unwrap(),
        Some(newer)
    );
}

#[tokio::test]
async fn retained_recovery_release_blob_is_revalidated_on_every_read() {
    let raw = Arc::new(InMemory::new());
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let store = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);
    let incumbent = owner(1, 11, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
    let (encoded, expected_reference) = encode_recovery_release_terminal(&terminal).unwrap();
    let reference = store
        .stage_recovery_release_terminal(&terminal)
        .await
        .unwrap();
    assert_eq!(reference, expected_reference);
    store
        .record_recovery_release_commit(&first.proof(), reference.clone())
        .await
        .unwrap();

    let path = recovery_release_terminal_path(&reference);
    raw.delete(&path).await.unwrap();
    let missing = store
        .latest_recovery_release_terminal()
        .await
        .unwrap_err()
        .to_string();
    assert!(missing.contains("is missing"), "{missing}");

    raw.put(&path, PutPayload::from(encoded)).await.unwrap();
    assert_eq!(
        store.latest_recovery_release_terminal().await.unwrap(),
        Some(terminal)
    );
    raw.put(&path, PutPayload::from(Bytes::from_static(b"broken")))
        .await
        .unwrap();
    let corrupt = store
        .latest_recovery_release_terminal()
        .await
        .unwrap_err()
        .to_string();
    assert!(corrupt.contains("bytes, expected"), "{corrupt}");
}

#[tokio::test]
async fn existing_invalid_release_blob_is_a_validation_conflict() {
    let raw = Arc::new(InMemory::new());
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let store = LeaderLeaseStore::new(object_store, 1_000);
    let incumbent = owner(1, 11, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
    let (_, reference) = encode_recovery_release_terminal(&terminal).unwrap();
    raw.put(
        &recovery_release_terminal_path(&reference),
        PutPayload::from(Bytes::from_static(b"invalid")),
    )
    .await
    .unwrap();

    assert!(matches!(
        store.stage_recovery_release_terminal(&terminal).await,
        Err(ClusterCheckpointAuthorityError::Authority(
            LeaseError::Invalid(_)
        ))
    ));
}

#[tokio::test]
async fn pruning_retains_latest_release_admission_and_collects_orphan_blobs() {
    let raw = Arc::new(InMemory::new());
    let object_store: Arc<dyn ObjectStore> = raw.clone();
    let store = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);
    let incumbent = owner(1, 11, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
    let retained = store
        .stage_recovery_release_terminal(&terminal)
        .await
        .unwrap();
    let mut orphan = terminal.clone();
    orphan.phase = RecoverPhase::ReleaseCommitted { epoch: 5 };
    let orphan = store
        .stage_recovery_release_terminal(&orphan)
        .await
        .unwrap();
    store
        .record_recovery_release_commit(&first.proof(), retained.clone())
        .await
        .unwrap();
    for now in 1..=4 {
        assert!(matches!(
            store
                .acquire_or_renew_current_term_for_test(&incumbent, now)
                .await
                .unwrap(),
            LeaseOutcome::Acquired(_)
        ));
    }

    LeaderLeaseStore::prune_history(&object_store, 0)
        .await
        .unwrap();
    let sequences = store.list_seqs().await.unwrap();
    assert!(sequences.contains(&3), "{sequences:?}");
    assert!(raw
        .get(&recovery_release_terminal_path(&retained))
        .await
        .is_ok());
    assert!(matches!(
        raw.get(&recovery_release_terminal_path(&orphan)).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(
        store.latest_recovery_release_terminal().await.unwrap(),
        Some(terminal)
    );
}

#[test]
fn replacement_term_may_abort_but_cannot_commit_an_existing_drain() {
    let incumbent = owner(1, 1, 1);
    let incumbent_lease = LeaderLease {
        seq: 1,
        renewal_sequence: 1,
        token: 1,
        owner: incumbent.clone(),
        expires_at_ms: 1,
        catalog_manifest: None,
    };
    let transition = assignment_drain_transition(&incumbent, incumbent_lease.proof());
    let replacement = LeaderLease {
        seq: 2,
        renewal_sequence: 2,
        token: 2,
        owner: owner(2, 2, 1),
        expires_at_ms: 2,
        catalog_manifest: None,
    }
    .proof();

    assert!(drain_decision_for_test(
        &transition,
        replacement.clone(),
        AssignmentDrainVerdict::Commit,
    )
    .is_err());
    assert!(
        drain_decision_for_test(&transition, replacement, AssignmentDrainVerdict::Abort,).is_ok()
    );
}

#[tokio::test]
async fn assignment_recovery_requires_exact_sorted_removals_and_matching_proposal() {
    let store = store(1_000);
    let incumbent = owner(1, 11, 1);
    let failed_two = owner(2, 22, 1);
    let failed_three = owner(3, 33, 1);
    let LeaseOutcome::Acquired(lease) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = lease.proof();
    let decision = assignment_recovery_decision(
        &store,
        1,
        &[incumbent.clone(), failed_two, failed_three],
        std::slice::from_ref(&incumbent),
        proof.clone(),
        1,
    )
    .await;
    assert!(decision.validate().is_ok());
    let authority_seq_before_invalid_attempts = store.load().await.unwrap().unwrap().seq;

    let mut missing = decision.clone();
    missing.removed_process_fences.pop();
    assert!(missing.validate().is_err());

    let mut unsorted = decision.clone();
    unsorted.removed_process_fences.swap(0, 1);
    assert!(unsorted.validate().is_err());

    let mut wrong_version = decision.clone();
    wrong_version.proposal.version = wrong_version.proposal.version.checked_add(1).unwrap();
    assert!(wrong_version.validate().is_err());

    let mut wrong_predecessor = decision.clone();
    wrong_predecessor.predecessor.assignment_digest[0] ^= 1;
    assert!(wrong_predecessor.validate().is_ok());
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&proof, wrong_predecessor)
            .await,
        Err(ClusterCheckpointAuthorityError::Authority(
            LeaseError::Invalid(_)
        ))
    ));

    let mut wrong_target = decision;
    wrong_target.target.assignment_digest[0] ^= 1;
    assert!(wrong_target.validate().is_ok());
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&proof, wrong_target)
            .await,
        Err(ClusterCheckpointAuthorityError::Authority(
            LeaseError::Invalid(_)
        ))
    ));
    assert_eq!(
        store.load().await.unwrap().unwrap().seq,
        authority_seq_before_invalid_attempts,
        "invalid recovery decisions must not advance the authority sequence"
    );
}

#[tokio::test]
async fn recovery_requires_the_current_cut_and_pins_it_until_the_target_commits() {
    let store = store(1_000);
    let incumbent = owner(1, 11, 1);
    let failed = owner(2, 22, 1);
    let LeaseOutcome::Acquired(lease) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = lease.proof();
    let stale = assignment_recovery_decision(
        &store,
        1,
        &[incumbent.clone(), failed.clone()],
        std::slice::from_ref(&incumbent),
        proof.clone(),
        1,
    )
    .await;
    let current = match record_commit(&store, &proof, &stale.predecessor, 2, 2).await {
        RecordOutcomeResult::Created(outcome) => outcome.committed_checkpoint.unwrap(),
        outcome => panic!("unexpected recovery checkpoint result: {outcome:?}"),
    };
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&proof, stale)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));

    let recovery = assignment_recovery_decision(
        &store,
        1,
        &[incumbent.clone(), failed],
        std::slice::from_ref(&incumbent),
        proof.clone(),
        2,
    )
    .await;
    assert_eq!(recovery.recovery_checkpoint, current);
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&proof, recovery.clone())
            .await
            .unwrap(),
        RecordAssignmentRecoveryDecisionResult::Created(_)
    ));
    assert_eq!(
        store
            .assignment_handoff_checkpoint(&recovery.target)
            .await
            .unwrap(),
        Some(current)
    );

    assert!(matches!(
        record_commit(&store, &proof, &recovery.target, 3, 3).await,
        RecordOutcomeResult::Created(_)
    ));
    assert_eq!(
        store
            .assignment_handoff_checkpoint(&recovery.target)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn competing_assignment_recoveries_have_one_same_version_winner() {
    let (raw, store) = blocking_store_at(1_000, lease_path(4));
    let incumbent = owner(1, 11, 1);
    let failed = owner(2, 22, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let left = assignment_recovery_decision(
        &store,
        1,
        &[incumbent.clone(), failed.clone()],
        std::slice::from_ref(&incumbent),
        proof.clone(),
        1,
    )
    .await;
    let right = assignment_recovery_decision(
        &store,
        1,
        &[incumbent.clone(), failed],
        std::slice::from_ref(&incumbent),
        proof.clone(),
        2,
    )
    .await;
    assert_ne!(left.proposal, right.proposal);

    let left_store = Arc::clone(&store);
    let left_proof = proof.clone();
    let left_task = tokio::spawn(async move {
        left_store
            .record_assignment_recovery_decision(&left_proof, left)
            .await
    });
    let right_store = Arc::clone(&store);
    let right_proof = proof.clone();
    let right_task = tokio::spawn(async move {
        right_store
            .record_assignment_recovery_decision(&right_proof, right)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    raw.release.add_permits(2);

    let (left_result, right_result) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(left_task, right_task)
    })
    .await
    .unwrap();
    let left_result = left_result.unwrap().unwrap();
    let right_result = right_result.unwrap().unwrap();
    assert_eq!(
        usize::from(matches!(
            &left_result,
            RecordAssignmentRecoveryDecisionResult::Created(_)
        )) + usize::from(matches!(
            &right_result,
            RecordAssignmentRecoveryDecisionResult::Created(_)
        )),
        1
    );
    assert_eq!(
        usize::from(matches!(
            &left_result,
            RecordAssignmentRecoveryDecisionResult::Conflict { .. }
        )) + usize::from(matches!(
            &right_result,
            RecordAssignmentRecoveryDecisionResult::Conflict { .. }
        )),
        1
    );
    let durable = store
        .assignment_recovery_decision(2)
        .await
        .unwrap()
        .unwrap();
    let snapshots = AssignmentSnapshotStore::new(Arc::clone(&store.store));
    let _ = store.materialize_assignment_recovery(2).await.unwrap();
    assert_eq!(snapshots.load().await.unwrap().unwrap().version, 2);
    assert_eq!(
        store
            .record_assignment_recovery_decision(&proof, durable.clone())
            .await
            .unwrap(),
        RecordAssignmentRecoveryDecisionResult::Unchanged(durable)
    );
}

#[tokio::test]
async fn authorized_recovery_supersedes_a_delayed_same_version_drain_write() {
    let store = store(1_000);
    let incumbent = owner(1, 11, 1);
    let failed = owner(2, 22, 1);
    let LeaseOutcome::Acquired(lease) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = lease.proof();
    let recovery = assignment_recovery_decision(
        &store,
        1,
        &[incumbent.clone(), failed.clone()],
        std::slice::from_ref(&incumbent),
        proof.clone(),
        1,
    )
    .await;
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&proof, recovery.clone())
            .await
            .unwrap(),
        RecordAssignmentRecoveryDecisionResult::Created(_)
    ));

    let snapshots = AssignmentSnapshotStore::new(Arc::clone(&store.store));
    let predecessor = snapshots.load().await.unwrap().unwrap();
    let delayed_drain = predecessor
        .next_draining(
            BTreeMap::from([(0, failed.node), (1, incumbent.node)]),
            predecessor.participants.clone(),
            proof,
        )
        .unwrap();
    assert!(matches!(
        snapshots
            .save_if_version(&delayed_drain, predecessor.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));
    assert_eq!(snapshots.load().await.unwrap(), Some(delayed_drain));

    assert!(matches!(
        store.materialize_assignment_recovery(2).await.unwrap(),
        RotateOutcome::Rotated
    ));
    let authorized = snapshots
        .load_recovery_proposal(&recovery.proposal)
        .await
        .unwrap();
    assert_eq!(snapshots.load().await.unwrap(), Some(authorized));
    assert_eq!(snapshots.load_drain_transition(2).await.unwrap(), None);
}

#[tokio::test]
async fn drain_and_recovery_share_one_ordered_retention_chain() {
    let store = store(1_000);
    let incumbent = owner(1, 11, 1);
    let failed = owner(2, 22, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let participants = vec![
        crate::checkpoint::CheckpointParticipant {
            node_id: incumbent.node.0,
            boot_incarnation: incumbent.boot,
        },
        crate::checkpoint::CheckpointParticipant {
            node_id: failed.node.0,
            boot_incarnation: failed.boot,
        },
    ];
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        1,
        &[incumbent.node.0, failed.node.0],
        participants.clone(),
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        2,
        &[incumbent.node.0, failed.node.0],
        participants,
    )
    .unwrap();
    let transition = AssignmentDrainTransition::new(predecessor, target, proof.clone()).unwrap();
    let drain = committed_drain_decision_for_test(&store, &transition, proof.clone(), 1).await;
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, drain.clone())
            .await
            .unwrap(),
        RecordAssignmentDrainDecisionResult::Created(_)
    ));

    let losing_recovery = assignment_recovery_decision(
        &store,
        1,
        &[incumbent.clone(), failed.clone()],
        std::slice::from_ref(&incumbent),
        proof.clone(),
        2,
    )
    .await;
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&proof, losing_recovery)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));

    let recovery = assignment_recovery_decision(
        &store,
        2,
        &[incumbent.clone(), failed],
        std::slice::from_ref(&incumbent),
        proof.clone(),
        3,
    )
    .await;
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&proof, recovery.clone())
            .await
            .unwrap(),
        RecordAssignmentRecoveryDecisionResult::Created(_)
    ));
    let propagated_pin = store
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .assignment_handoff_pin
        .unwrap();
    assert_eq!(propagated_pin.target, recovery.target);
    assert_eq!(
        Some(&propagated_pin.checkpoint),
        drain.handoff_checkpoint.as_ref()
    );

    let losing_drain_transition = assignment_drain_transition_at(&incumbent, proof.clone(), 3);
    let losing_drain = drain_decision_for_test(
        &losing_drain_transition,
        proof.clone(),
        AssignmentDrainVerdict::Commit,
    )
    .unwrap();
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, losing_drain)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));

    let head = store.load_record().await.unwrap().unwrap();
    assert!(matches!(
        head.assignment_decision,
        Some(AuthorityAssignmentDecision::Recovery(ref durable)) if durable == &recovery
    ));
    let previous = head.previous_assignment_decision.unwrap();
    assert_eq!(previous.target_version, 2);
    let previous_record = read_authority_record(store.store.as_ref(), previous.sequence)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        previous_record.assignment_decision,
        Some(AuthorityAssignmentDecision::Drain(ref durable)) if durable == &drain
    ));
    assert!(matches!(
        store.assignment_drain_decision(3).await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
    assert!(matches!(
        store.assignment_recovery_decision(2).await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));

    assert_eq!(
        store
            .prune_assignment_drain_decisions_before(&proof, 3)
            .await
            .unwrap(),
        3
    );
    let floor = store
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .assignment_decision_floor
        .unwrap();
    assert!(matches!(
        floor.terminal_anchor,
        Some(AuthorityAssignmentDecision::Drain(anchor)) if anchor == drain
    ));
    assert_eq!(
        store.assignment_recovery_decision(3).await.unwrap(),
        Some(recovery)
    );
}

#[tokio::test]
async fn takeover_fences_a_delayed_assignment_recovery_decision() {
    // Establishing the portable predecessor checkpoint advances the authority through artifact
    // admission and Commit. The recovery decision is therefore the fourth authority record.
    let (raw, store) = blocking_once_at(10, lease_path(4));
    let incumbent = owner(1, 11, 1);
    let successor = owner(1, 12, 2);
    let failed = owner(2, 22, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let old_proof = first.proof();
    let old_decision = assignment_recovery_decision(
        &store,
        1,
        &[incumbent.clone(), failed.clone()],
        std::slice::from_ref(&incumbent),
        old_proof.clone(),
        1,
    )
    .await;
    let observation = store.observe_rival(&successor, &first).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    let delayed_store = Arc::clone(&store);
    let delayed_proof = old_proof.clone();
    let delayed_task = tokio::spawn(async move {
        delayed_store
            .record_assignment_recovery_decision(&delayed_proof, old_decision)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let LeaseOutcome::Acquired(takeover) = store
        .try_takeover(&successor, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("successor must win the authority sequence");
    };
    raw.release.add_permits(1);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), delayed_task)
            .await
            .unwrap()
            .unwrap(),
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));

    let takeover_proof = takeover.proof();
    let winner = assignment_recovery_decision(
        &store,
        1,
        &[incumbent, failed],
        std::slice::from_ref(&successor),
        takeover_proof.clone(),
        2,
    )
    .await;
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&takeover_proof, winner.clone())
            .await
            .unwrap(),
        RecordAssignmentRecoveryDecisionResult::Created(_)
    ));
    assert_eq!(
        store.assignment_recovery_decision(2).await.unwrap(),
        Some(winner)
    );
}

#[tokio::test]
async fn competing_assignment_drain_decisions_have_one_immutable_winner() {
    let (raw, store) = blocking_store_at(1_000, lease_path(4));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let transition = assignment_drain_transition(&incumbent, proof.clone());
    let commit =
        committed_drain_decision_for_test(store.as_ref(), &transition, proof.clone(), 1).await;
    let abort =
        drain_decision_for_test(&transition, proof.clone(), AssignmentDrainVerdict::Abort).unwrap();

    let commit_store = Arc::clone(&store);
    let commit_proof = proof.clone();
    let commit_task = tokio::spawn(async move {
        commit_store
            .record_assignment_drain_decision(&commit_proof, commit)
            .await
    });
    let abort_store = Arc::clone(&store);
    let abort_proof = proof.clone();
    let abort_task = tokio::spawn(async move {
        abort_store
            .record_assignment_drain_decision(&abort_proof, abort)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    raw.release.add_permits(2);

    let (commit_result, abort_result) = tokio::join!(commit_task, abort_task);
    let commit_result = commit_result.unwrap().unwrap();
    let abort_result = abort_result.unwrap().unwrap();
    assert_eq!(
        usize::from(matches!(
            &commit_result,
            RecordAssignmentDrainDecisionResult::Created(_)
        )) + usize::from(matches!(
            &abort_result,
            RecordAssignmentDrainDecisionResult::Created(_)
        )),
        1
    );
    assert_eq!(
        usize::from(matches!(
            &commit_result,
            RecordAssignmentDrainDecisionResult::Conflict { .. }
        )) + usize::from(matches!(
            &abort_result,
            RecordAssignmentDrainDecisionResult::Conflict { .. }
        )),
        1
    );
    let durable = store.assignment_drain_decision(2).await.unwrap().unwrap();
    let retry = store
        .record_assignment_drain_decision(&proof, durable.clone())
        .await
        .unwrap();
    assert_eq!(
        retry,
        RecordAssignmentDrainDecisionResult::Unchanged(durable)
    );
}

#[tokio::test]
async fn takeover_fences_delayed_drain_commit_and_can_abort_the_transition() {
    // Establishing the portable handoff checkpoint advances the authority through artifact
    // admission and Commit. The drain decision is therefore the fourth authority record.
    let (raw, store) = blocking_once_at(10, lease_path(4));
    let incumbent = owner(1, 1, 1);
    let successor = owner(2, 2, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let old_proof = first.proof();
    let transition = assignment_drain_transition(&incumbent, old_proof.clone());
    let observation = store.observe_rival(&successor, &first).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    let delayed =
        committed_drain_decision_for_test(store.as_ref(), &transition, old_proof.clone(), 1).await;
    let delayed_store = Arc::clone(&store);
    let delayed_proof = old_proof.clone();
    let delayed_task = tokio::spawn(async move {
        delayed_store
            .record_assignment_drain_decision(&delayed_proof, delayed)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let LeaseOutcome::Acquired(takeover) = store
        .try_takeover(&successor, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("successor must win the authority sequence");
    };
    raw.release.add_permits(1);
    assert!(matches!(
        delayed_task.await.unwrap(),
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));

    let takeover_proof = takeover.proof();
    let abort = drain_decision_for_test(
        &transition,
        takeover_proof.clone(),
        AssignmentDrainVerdict::Abort,
    )
    .unwrap();
    assert!(matches!(
        store
            .record_assignment_drain_decision(&takeover_proof, abort)
            .await
            .unwrap(),
        RecordAssignmentDrainDecisionResult::Created(_)
    ));
    assert_eq!(
        store
            .assignment_drain_decision(2)
            .await
            .unwrap()
            .unwrap()
            .verdict,
        AssignmentDrainVerdict::Abort
    );
}

#[tokio::test]
async fn assignment_drain_floor_compacts_history_and_rejects_stale_versions() {
    let store = store(1);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    for target_version in 2..=5 {
        let transition = assignment_drain_transition_at(&incumbent, proof.clone(), target_version);
        let decision =
            drain_decision_for_test(&transition, proof.clone(), AssignmentDrainVerdict::Abort)
                .unwrap();
        assert!(matches!(
            store
                .record_assignment_drain_decision(&proof, decision)
                .await
                .unwrap(),
            RecordAssignmentDrainDecisionResult::Created(_)
        ));
    }

    let head = store.load_record().await.unwrap().unwrap();
    let mut by_target_version = std::collections::BTreeMap::new();
    let mut link = head.assignment_decision_head;
    while let Some(current) = link {
        by_target_version.insert(current.target_version, current.sequence);
        link = read_authority_record(store.store.as_ref(), current.sequence)
            .await
            .unwrap()
            .unwrap()
            .previous_assignment_decision;
    }

    assert_eq!(
        store
            .prune_assignment_drain_decisions_before(&proof, 4)
            .await
            .unwrap(),
        4
    );
    let floor = store
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .assignment_decision_floor
        .unwrap();
    assert_eq!(floor.before_target_version, 4);
    assert_eq!(floor.terminal_anchor.unwrap().target_version(), 3);
    let stale_inventory =
        checkpoint_artifact_inventory(&store, &assignment_fence(&incumbent), 1).await;
    assert!(matches!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, stale_inventory)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("below durable assignment-decision floor")
    ));
    assert!(matches!(
        store.assignment_drain_decision(3).await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
    for target_version in [4, 5] {
        assert_eq!(
            store
                .assignment_drain_decision(target_version)
                .await
                .unwrap()
                .unwrap()
                .target_version(),
            target_version
        );
    }

    let stale_transition = assignment_drain_transition_at(&incumbent, proof.clone(), 3);
    let stale = drain_decision_for_test(
        &stale_transition,
        proof.clone(),
        AssignmentDrainVerdict::Commit,
    )
    .unwrap();
    assert!(matches!(
        store.record_assignment_drain_decision(&proof, stale).await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = store
                .acquire_or_renew_current_term_for_test(&incumbent, 10)
                .await
                .unwrap();
            let mut compacted_absent = true;
            for target_version in [2, 3] {
                if read_authority_record(store.store.as_ref(), by_target_version[&target_version])
                    .await
                    .unwrap()
                    .is_some()
                {
                    compacted_absent = false;
                    break;
                }
            }
            if compacted_absent {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();

    for target_version in [4, 5] {
        assert!(
            read_authority_record(store.store.as_ref(), by_target_version[&target_version])
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test]
async fn assignment_drain_floor_rejects_a_rewritten_anchor_link() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    for target_version in [2, 4] {
        let transition = assignment_drain_transition_at(&incumbent, proof.clone(), target_version);
        let decision =
            drain_decision_for_test(&transition, proof.clone(), AssignmentDrainVerdict::Abort)
                .unwrap();
        store
            .record_assignment_drain_decision(&proof, decision)
            .await
            .unwrap();
    }
    store
        .prune_assignment_drain_decisions_before(&proof, 4)
        .await
        .unwrap();

    let mut corrupt = store.load_record().await.unwrap().unwrap();
    corrupt
        .assignment_decision_floor
        .as_mut()
        .unwrap()
        .terminal_anchor_link
        .as_mut()
        .unwrap()
        .sequence += 1;
    store
        .store
        .put(
            &lease_path(corrupt.lease.seq),
            PutPayload::from(Bytes::from(serde_json::to_vec(&corrupt).unwrap())),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.assignment_drain_decision(4).await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
}

#[tokio::test]
async fn delayed_cluster_decision_is_fenced_when_takeover_wins_next_sequence() {
    let (raw, store) = blocking_once_at(10, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let successor = owner(2, 2, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let observation = store.observe_rival(&successor, &first).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    let decision_store = Arc::clone(&store);
    let decision = tokio::spawn(async move {
        decision_store
            .record_cluster_outcome(&proof, 1, 1, fence, CheckpointVerdict::Abort, None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let LeaseOutcome::Acquired(takeover) = store
        .try_takeover(&successor, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("successor must win the unblocked next sequence");
    };
    assert_eq!(takeover.owner, successor);
    raw.release.add_permits(1);
    assert!(matches!(
        decision.await.unwrap(),
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));
    assert!(store.cluster_outcomes().await.unwrap().is_empty());
}

#[tokio::test]
async fn assignment_decisions_prevent_predecessor_checkpoint_artifacts_from_reopening() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let failed = owner(2, 2, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        unreachable!()
    };
    let proof = lease.proof();
    let transition = assignment_drain_transition(&incumbent, proof.clone());
    let drain = committed_drain_decision_for_test(&store, &transition, proof.clone(), 1).await;
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, drain)
            .await
            .unwrap(),
        RecordAssignmentDrainDecisionResult::Created(_)
    ));

    let predecessor = checkpoint_artifact_inventory(&store, &transition.predecessor, 2).await;
    assert!(matches!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, predecessor)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("consumed")
    ));
    let target = checkpoint_artifact_inventory(&store, &transition.target, 2).await;
    assert_eq!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, target.clone())
            .await
            .unwrap(),
        target
    );

    let recovery_store = LeaderLeaseStore::new(Arc::new(InMemory::new()), 1_000);
    let LeaseOutcome::Acquired(recovery_lease) =
        recovery_store.begin_new_term(&incumbent, 0).await.unwrap()
    else {
        unreachable!()
    };
    let recovery_proof = recovery_lease.proof();
    let recovery = assignment_recovery_decision(
        &recovery_store,
        1,
        &[incumbent.clone(), failed],
        std::slice::from_ref(&incumbent),
        recovery_proof.clone(),
        1,
    )
    .await;
    assert!(matches!(
        recovery_store
            .record_assignment_recovery_decision(&recovery_proof, recovery.clone())
            .await
            .unwrap(),
        RecordAssignmentRecoveryDecisionResult::Created(_)
    ));
    let predecessor =
        checkpoint_artifact_inventory(&recovery_store, &recovery.predecessor, 2).await;
    assert!(matches!(
        recovery_store
            .begin_cluster_checkpoint_artifacts(&recovery_proof, predecessor)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("consumed")
    ));
    let target = checkpoint_artifact_inventory(&recovery_store, &recovery.target, 2).await;
    assert_eq!(
        recovery_store
            .begin_cluster_checkpoint_artifacts(&recovery_proof, target.clone())
            .await
            .unwrap(),
        target
    );
}

#[tokio::test]
async fn drain_abort_only_reopens_its_materialized_rollback_assignment() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let peer = owner(2, 2, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        unreachable!()
    };
    let proof = lease.proof();
    let participants = vec![
        crate::checkpoint::CheckpointParticipant {
            node_id: incumbent.node.0,
            boot_incarnation: incumbent.boot,
        },
        crate::checkpoint::CheckpointParticipant {
            node_id: peer.node.0,
            boot_incarnation: peer.boot,
        },
    ];
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        1,
        &[incumbent.node.0, peer.node.0],
        participants.clone(),
    )
    .unwrap();
    let proposed_target = CheckpointAssignmentFence::from_owner_map(
        2,
        &[peer.node.0, incumbent.node.0],
        participants,
    )
    .unwrap();
    let transition =
        AssignmentDrainTransition::new(predecessor.clone(), proposed_target.clone(), proof.clone())
            .unwrap();
    let abort = AssignmentDrainDecision::abort(&transition, proof.clone()).unwrap();
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, abort)
            .await
            .unwrap(),
        RecordAssignmentDrainDecisionResult::Created(_)
    ));

    let proposed = checkpoint_artifact_inventory(&store, &proposed_target, 1).await;
    assert!(matches!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, proposed)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("materialized")
    ));
    let mut rollback = predecessor;
    rollback.assignment_version = proposed_target.assignment_version;
    let rollback_inventory = checkpoint_artifact_inventory(&store, &rollback, 1).await;
    assert_eq!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, rollback_inventory.clone())
            .await
            .unwrap(),
        rollback_inventory
    );
}

#[tokio::test]
async fn active_predecessor_artifacts_block_drain_settlement_but_not_recovery() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let peer = owner(2, 2, 1);
    let reincarnated = owner(1, 11, 2);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        unreachable!()
    };
    let proof = lease.proof();
    let transition = assignment_drain_transition(&incumbent, proof.clone());
    let handoff = match record_commit(&store, &proof, &transition.predecessor, 1, 1).await {
        RecordOutcomeResult::Created(outcome) => outcome.committed_checkpoint.unwrap(),
        outcome => panic!("unexpected handoff checkpoint result: {outcome:?}"),
    };
    let active = begin_checkpoint_artifacts(&store, &proof, &transition.predecessor, 2).await;

    let commit = AssignmentDrainDecision::commit(&transition, proof.clone(), handoff).unwrap();
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, commit)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("cannot overtake checkpoint")
    ));
    let abort = AssignmentDrainDecision::abort(&transition, proof.clone()).unwrap();
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, abort)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("cannot overtake checkpoint")
    ));
    let mismatched_participants = vec![
        crate::checkpoint::CheckpointParticipant {
            node_id: incumbent.node.0,
            boot_incarnation: incumbent.boot,
        },
        crate::checkpoint::CheckpointParticipant {
            node_id: peer.node.0,
            boot_incarnation: peer.boot,
        },
    ];
    let mismatched_predecessor = CheckpointAssignmentFence::from_owner_map(
        1,
        &[incumbent.node.0, peer.node.0],
        mismatched_participants.clone(),
    )
    .unwrap();
    let mismatched_target = CheckpointAssignmentFence::from_owner_map(
        2,
        &[peer.node.0, incumbent.node.0],
        mismatched_participants,
    )
    .unwrap();
    let mismatched_transition =
        AssignmentDrainTransition::new(mismatched_predecessor, mismatched_target, proof.clone())
            .unwrap();
    let mismatched_abort =
        AssignmentDrainDecision::abort(&mismatched_transition, proof.clone()).unwrap();
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, mismatched_abort)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("conflicts with predecessor version")
    ));

    let recovery = assignment_recovery_decision(
        &store,
        1,
        std::slice::from_ref(&incumbent),
        std::slice::from_ref(&reincarnated),
        proof.clone(),
        1,
    )
    .await;
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&proof, recovery)
            .await
            .unwrap(),
        RecordAssignmentRecoveryDecisionResult::Created(_)
    ));
    assert_eq!(
        store.cluster_checkpoint_artifacts().await.unwrap(),
        Some(active.clone())
    );
    assert!(matches!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, active)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("consumed")
    ));
}

#[tokio::test]
async fn contended_identical_artifact_admission_rechecks_a_recovery_decision() {
    // The recovery checkpoint advances authority through artifact admission and Commit, so the
    // delayed successor admission below contends at the fourth authority record.
    let (raw, store) = blocking_once_at(1_000, lease_path(4));
    let incumbent = owner(1, 1, 1);
    let failed = owner(2, 2, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        unreachable!()
    };
    let proof = lease.proof();
    let recovery = assignment_recovery_decision(
        store.as_ref(),
        1,
        &[incumbent.clone(), failed],
        std::slice::from_ref(&incumbent),
        proof.clone(),
        1,
    )
    .await;
    let inventory = checkpoint_artifact_inventory(store.as_ref(), &recovery.predecessor, 2).await;

    let delayed_store = Arc::clone(&store);
    let delayed_proof = proof.clone();
    let delayed_inventory = inventory.clone();
    let delayed = tokio::spawn(async move {
        delayed_store
            .begin_cluster_checkpoint_artifacts(&delayed_proof, delayed_inventory)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    assert_eq!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, inventory.clone())
            .await
            .unwrap(),
        inventory
    );
    assert!(matches!(
        store
            .record_assignment_recovery_decision(&proof, recovery)
            .await
            .unwrap(),
        RecordAssignmentRecoveryDecisionResult::Created(_)
    ));
    raw.release.add_permits(1);

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), delayed)
            .await
            .unwrap()
            .unwrap(),
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("consumed")
    ));
    assert_eq!(
        store.cluster_checkpoint_artifacts().await.unwrap(),
        Some(inventory)
    );
}

#[tokio::test]
async fn cluster_checkpoint_artifacts_block_successors_and_survive_abort_until_cleanup() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    let inventory = begin_checkpoint_artifacts(&store, &proof, &fence, 1).await;
    assert_eq!(
        store.cluster_checkpoint_artifacts().await.unwrap(),
        Some(inventory.clone())
    );
    assert_eq!(
        store.cluster_checkpoint_artifact_admission().await.unwrap(),
        Some((inventory.clone(), proof.clone()))
    );

    let mut successor = inventory.clone();
    successor.attempt = crate::checkpoint::CheckpointAttempt::canonical(2);
    assert!(matches!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, successor)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));

    store
        .record_cluster_outcome(&proof, 1, 1, fence.clone(), CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    assert_eq!(
        store.cluster_checkpoint_artifacts().await.unwrap(),
        Some(inventory.clone())
    );
    assert!(matches!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, inventory.clone())
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("unresolved or inherited")
    ));
    store
        .finish_cluster_checkpoint_artifact_cleanup(&proof, &inventory)
        .await
        .unwrap();
    store
        .finish_cluster_checkpoint_artifact_cleanup(&proof, &inventory)
        .await
        .unwrap();
    assert!(store
        .cluster_checkpoint_artifacts()
        .await
        .unwrap()
        .is_none());

    record_commit(&store, &proof, &fence, 2, 2).await;
    assert!(store
        .cluster_checkpoint_artifacts()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn delayed_artifact_admission_cannot_reopen_a_durable_abort() {
    let (raw, store) = blocking_once_at(1_000, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    let inventory = CheckpointArtifactInventory {
        deployment_id: CheckpointDecisionStore::new(Arc::clone(&store.store))
            .load_or_create_deployment_id()
            .await
            .unwrap(),
        pipeline_identity: crate::checkpoint::PipelineIdentity::empty(),
        attempt: crate::checkpoint::CheckpointAttempt::canonical(1),
        assignment_fence: Some(fence.clone()),
        sink_artifact_intent_protocol: true,
    };

    let delayed_store = Arc::clone(&store);
    let delayed_proof = proof.clone();
    let delayed_inventory = inventory.clone();
    let delayed = tokio::spawn(async move {
        delayed_store
            .begin_cluster_checkpoint_artifacts(&delayed_proof, delayed_inventory)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    assert_eq!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, inventory.clone())
            .await
            .unwrap(),
        inventory
    );
    store
        .record_cluster_outcome(&proof, 1, 1, fence, CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    raw.release.add_permits(1);

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), delayed)
            .await
            .unwrap()
            .unwrap(),
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
    assert_eq!(
        store.cluster_checkpoint_artifacts().await.unwrap(),
        Some(inventory)
    );
}

#[tokio::test]
async fn takeover_can_only_abort_the_exact_active_checkpoint_inventory() {
    let store = store(10);
    let incumbent = owner(1, 1, 1);
    let successor = owner(2, 2, 1);
    let LeaseOutcome::Acquired(first) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    let inventory = begin_checkpoint_artifacts(&store, &proof, &fence, 1).await;
    let observation = store.observe_rival(&successor, &first).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let LeaseOutcome::Acquired(takeover) = store
        .try_takeover(&successor, &observation, 20)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let takeover_proof = takeover.proof();
    assert!(matches!(
        store
            .begin_cluster_checkpoint_artifacts(&takeover_proof, inventory.clone())
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("outside the assignment fence")
    ));
    let committed = committed_checkpoint(&store, &fence, 1, 1).await;
    assert!(matches!(
        store
            .record_cluster_outcome(
                &takeover_proof,
                1,
                1,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(committed),
            )
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("requires its leader proof in the assignment fence")
    ));
    store
        .record_cluster_outcome(&takeover_proof, 1, 1, fence, CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    assert!(matches!(
        store
            .finish_cluster_checkpoint_artifact_cleanup(&proof, &inventory)
            .await,
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));
    store
        .finish_cluster_checkpoint_artifact_cleanup(&takeover_proof, &inventory)
        .await
        .unwrap();
}

#[tokio::test]
async fn delayed_cluster_decision_retries_after_renewal_wins_next_sequence() {
    let (raw, store) = blocking_once_at(1_000, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    let decision_store = Arc::clone(&store);
    let decision = tokio::spawn(async move {
        decision_store
            .record_cluster_outcome(&proof, 1, 1, fence, CheckpointVerdict::Abort, None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let LeaseOutcome::Acquired(renewal) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 1)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(renewal.seq, 2);
    raw.release.add_permits(1);
    assert!(matches!(
        decision.await.unwrap().unwrap(),
        RecordOutcomeResult::Created(_)
    ));
    assert_eq!(store.load().await.unwrap().unwrap().seq, 3);
    assert_eq!(store.cluster_outcomes().await.unwrap().len(), 1);
}

#[tokio::test]
async fn delayed_cluster_decision_retries_after_catalog_seal_wins_next_sequence() {
    let (raw, store) = blocking_once_at(1_000, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    let decision_store = Arc::clone(&store);
    let decision_proof = proof.clone();
    let decision = tokio::spawn(async move {
        decision_store
            .record_cluster_outcome(&decision_proof, 1, 1, fence, CheckpointVerdict::Abort, None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let manifest = catalog("events");
    assert_eq!(
        store.seal_catalog(&proof, &manifest).await.unwrap(),
        CatalogSealOutcome::Created
    );
    raw.release.add_permits(1);
    assert!(matches!(
        decision.await.unwrap().unwrap(),
        RecordOutcomeResult::Created(_)
    ));
    let head = store.load().await.unwrap().unwrap();
    assert_eq!(head.seq, 3);
    let reference = head.catalog_manifest.expect("catalog seal must survive");
    assert_eq!(
        store.load_catalog_manifest(&reference).await.unwrap(),
        manifest
    );
    assert_eq!(store.cluster_outcomes().await.unwrap().len(), 1);
}

#[tokio::test]
async fn ambiguous_cluster_decision_reconciles_exact_canonical_winner() {
    let (raw, store) = ambiguous_once_at(1_000, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let result = store
        .record_cluster_outcome(
            &first.proof(),
            1,
            1,
            assignment_fence(&incumbent),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    assert!(raw
        .did_return_ambiguous
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(matches!(result, RecordOutcomeResult::Unchanged(_)));
    let outcomes = store.cluster_outcomes().await.unwrap();
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
            .collect::<Vec<_>>(),
        vec![(1, 1)]
    );
}

#[tokio::test]
async fn ambiguous_cluster_decision_compacted_before_reconciliation_fails_closed() {
    let (raw, store) = delayed_ambiguous_response_once_at(1_000, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    disable_history_pruning_for_test(&store).await;
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    let delayed_store = Arc::clone(&store);
    let delayed_proof = proof.clone();
    let delayed_fence = fence.clone();
    let delayed = tokio::spawn(async move {
        delayed_store
            .record_cluster_outcome(
                &delayed_proof,
                1,
                1,
                delayed_fence,
                CheckpointVerdict::Abort,
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    for epoch in 2..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap() {
        store
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
    let boundary = store.cluster_outcome_retention_boundary().await.unwrap();
    assert!(boundary.terminal_before_epoch > 1);
    assert!(store
        .cluster_outcomes()
        .await
        .unwrap()
        .iter()
        .all(|outcome| outcome.epoch != 1));

    raw.release.add_permits(1);
    let error = delayed.await.unwrap().unwrap_err();
    assert!(raw
        .did_return_ambiguous
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(
        matches!(
            error,
            ClusterCheckpointAuthorityError::Decision(DecisionError::Conflict(_))
        ),
        "{error}"
    );
    assert_eq!(
        read_authority_record(raw.as_ref(), 2)
            .await
            .unwrap()
            .unwrap()
            .checkpoint_outcome
            .unwrap()
            .epoch,
        1
    );
}

#[tokio::test]
async fn exact_cluster_outcome_bounds_latest_future_and_older_reads() {
    let (raw, store) = blocking_once_at(
        1_000,
        OsPath::from("control/never-block-exact-outcome-reads"),
    );
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    for epoch in 1..=4 {
        store
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
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.prune_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let deployment_path = OsPath::from("checkpoint-deployment/identity.json");

    raw.clear_get_counts();
    let latest = store.cluster_outcome(4).await.unwrap().unwrap();
    assert_eq!((latest.epoch, latest.checkpoint_id), (4, 4));
    assert_eq!(raw.get_count(&lease_path(5)), 1);
    assert_eq!(raw.get_count(&lease_path(4)), 0);
    assert_eq!(raw.get_count(&deployment_path), 0);

    raw.clear_get_counts();
    assert!(store.cluster_outcome(5).await.unwrap().is_none());
    assert_eq!(raw.get_count(&lease_path(5)), 1);
    assert_eq!(raw.get_count(&lease_path(4)), 0);
    assert_eq!(raw.get_count(&deployment_path), 0);

    raw.clear_get_counts();
    let older = store.cluster_outcome(2).await.unwrap().unwrap();
    assert_eq!((older.epoch, older.checkpoint_id), (2, 2));
    assert_eq!(raw.get_count(&lease_path(5)), 1);
    assert_eq!(raw.get_count(&lease_path(4)), 0);
    assert_eq!(raw.get_count(&lease_path(3)), 0);
    assert_eq!(raw.get_count(&lease_path(2)), 0);
    assert_eq!(raw.get_count(&deployment_path), 0);

    let restarted = LeaderLeaseStore::new(raw.clone(), 1_000);
    raw.clear_get_counts();
    let cold_older = restarted.cluster_outcome(2).await.unwrap().unwrap();
    assert_eq!((cold_older.epoch, cold_older.checkpoint_id), (2, 2));
    assert_eq!(raw.get_count(&lease_path(5)), 1);
    assert_eq!(raw.get_count(&lease_path(4)), 1);
    assert_eq!(raw.get_count(&lease_path(3)), 1);
    assert_eq!(raw.get_count(&lease_path(2)), 0);
    assert_eq!(raw.get_count(&deployment_path), 1);

    record_commit(store.as_ref(), &proof, &fence, 5, 5).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.prune_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let recovery_reference = store
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .checkpoint_outcome
        .unwrap()
        .committed_checkpoint
        .unwrap();
    raw.clear_get_counts();
    let (committed, checkpoint) = store
        .cluster_outcome_with_committed_checkpoint(5)
        .await
        .unwrap()
        .unwrap();
    assert!(committed.is_commit());
    assert_eq!(checkpoint.unwrap().epoch, 5);
    assert_eq!(
        raw.get_count(&lease_path(6)),
        0,
        "the explicit record read immediately above seeds the exact authority-head cache"
    );
    assert_eq!(raw.get_count(&deployment_path), 1);
    assert_eq!(
        raw.get_count(&committed_checkpoint_path(&recovery_reference)),
        1
    );
}

#[tokio::test]
async fn exact_cluster_outcome_retries_a_disappearing_admission() {
    let (raw, store) = blocking_once_at(
        1_000,
        OsPath::from("control/never-block-exact-outcome-retry"),
    );
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    for epoch in 1..=3 {
        store
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
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.prune_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    raw.inner.delete(&lease_path(3)).await.unwrap();
    raw.clear_get_counts();
    let restarted = LeaderLeaseStore::new(raw.clone(), 1_000);

    assert!(matches!(
        restarted.cluster_outcome(1).await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::InventoryChanged(_)
        ))
    ));
    assert_eq!(raw.get_count(&lease_path(4)), 3);
    assert_eq!(raw.get_count(&lease_path(3)), 3);
    assert_eq!(
        raw.get_count(&OsPath::from("checkpoint-deployment/identity.json")),
        0
    );
}

#[tokio::test]
async fn exact_cluster_outcome_rejects_a_rewritten_immutable_link() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    store
        .record_cluster_outcome(&proof, 1, 1, fence.clone(), CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    store
        .acquire_or_renew_current_term_for_test(&incumbent, 1)
        .await
        .unwrap();
    store
        .record_cluster_outcome(&proof, 3, 3, fence, CheckpointVerdict::Abort, None)
        .await
        .unwrap();

    let mut corrupt = store.load_record().await.unwrap().unwrap();
    corrupt.previous_outcome = Some(OutcomeLink {
        sequence: 3,
        epoch: 1,
        checkpoint_id: 1,
    });
    store
        .store
        .put(
            &lease_path(corrupt.lease.seq),
            PutPayload::from(encode_authority_record(&corrupt).unwrap()),
        )
        .await
        .unwrap();
    let restarted = LeaderLeaseStore::new(Arc::clone(&store.store), 1_000);

    assert!(matches!(
        restarted.cluster_outcome(1).await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
}

#[tokio::test]
async fn outcome_audit_rejects_a_commit_chain_link_to_abort() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    disable_history_pruning_for_test(&store).await;
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    record_commit(&store, &proof, &fence, 1, 1).await;
    store
        .record_cluster_outcome(&proof, 2, 2, fence.clone(), CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    record_commit(&store, &proof, &fence, 3, 3).await;

    let mut corrupt = store.load_record().await.unwrap().unwrap();
    corrupt.previous_commit = Some(OutcomeLink {
        sequence: corrupt.lease.seq - 1,
        epoch: 2,
        checkpoint_id: 2,
    });
    store
        .store
        .put(
            &lease_path(corrupt.lease.seq),
            PutPayload::from(encode_authority_record(&corrupt).unwrap()),
        )
        .await
        .unwrap();
    *store.outcome_audit_cache.lock() = None;

    assert!(matches!(
        store.highest_cluster_terminal_outcome().await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
}

#[tokio::test]
async fn cluster_attempt_settlement_returns_exact_or_newer_closure() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    store
        .record_cluster_outcome(&proof, 1, 1, fence.clone(), CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    store
        .record_cluster_outcome(&proof, 3, 3, fence, CheckpointVerdict::Abort, None)
        .await
        .unwrap();

    let exact = store
        .cluster_attempt_settlement(crate::checkpoint::CheckpointAttempt::canonical(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((exact.epoch, exact.checkpoint_id), (1, 1));
    let newer_closure = store
        .cluster_attempt_settlement(crate::checkpoint::CheckpointAttempt::canonical(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((newer_closure.epoch, newer_closure.checkpoint_id), (3, 3));
    assert!(store
        .cluster_attempt_settlement(crate::checkpoint::CheckpointAttempt::canonical(4))
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        store
            .cluster_attempt_settlement(crate::checkpoint::CheckpointAttempt::new(2, 35))
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
}

#[tokio::test]
async fn cluster_outcome_audit_cache_reuses_unchanged_head_and_reaudits_changed_head() {
    let (raw, store) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-outcome-audit-cache"),
    );
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    store
        .record_cluster_outcome(&proof, 1, 1, fence.clone(), CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    store
        .record_cluster_outcome(&proof, 2, 2, fence.clone(), CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.prune_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    *store.outcome_audit_cache.lock() = None;
    raw.clear_get_counts();
    let exact = store
        .cluster_attempt_settlement(crate::checkpoint::CheckpointAttempt::canonical(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((exact.epoch, exact.checkpoint_id), (1, 1));
    assert_eq!(raw.get_count(&lease_path(2)), 1);

    raw.clear_get_counts();
    let highest = store
        .highest_cluster_terminal_outcome()
        .await
        .unwrap()
        .unwrap();
    assert_eq!((highest.epoch, highest.checkpoint_id), (2, 2));
    assert_eq!(raw.get_count(&lease_path(3)), 1);
    assert_eq!(raw.get_count(&lease_path(2)), 0);

    let external = LeaderLeaseStore::new(raw.clone(), 1_000);
    external
        .record_cluster_outcome(&proof, 3, 3, fence, CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while external.prune_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    raw.clear_get_counts();
    let highest = store
        .highest_cluster_terminal_outcome()
        .await
        .unwrap()
        .unwrap();
    assert_eq!((highest.epoch, highest.checkpoint_id), (3, 3));
    assert_eq!(raw.get_count(&lease_path(3)), 1);
    assert_eq!(raw.get_count(&lease_path(2)), 1);
}

#[tokio::test]
async fn concurrent_cold_outcome_audits_read_each_history_link_once() {
    let (raw, store) = blocking_get_once_at(1_000, lease_path(3));
    raw.did_block.store(true, Ordering::Release);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    for checkpoint_id in 1..=3 {
        store
            .record_cluster_outcome(
                &proof,
                checkpoint_id,
                checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.prune_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    *store.outcome_audit_cache.lock() = None;
    raw.clear_get_counts();
    raw.did_block.store(false, Ordering::Release);
    let first_store = Arc::clone(&store);
    let first_audit =
        tokio::spawn(async move { first_store.highest_cluster_terminal_outcome().await });
    raw.entered.acquire().await.unwrap().forget();

    let mut followers = Vec::new();
    for _ in 0..16 {
        let follower = Arc::clone(&store);
        followers.push(tokio::spawn(async move {
            follower.highest_cluster_terminal_outcome().await
        }));
    }
    tokio::task::yield_now().await;
    raw.release.add_permits(1);

    assert_eq!(first_audit.await.unwrap().unwrap().unwrap().epoch, 3);
    for follower in followers {
        assert_eq!(follower.await.unwrap().unwrap().unwrap().epoch, 3);
    }
    assert_eq!(raw.get_count(&lease_path(3)), 1);
    assert_eq!(raw.get_count(&lease_path(2)), 1);
}

#[tokio::test]
async fn failed_outcome_audit_is_not_retained() {
    let (raw, store) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-failed-outcome-audit"),
    );
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    for checkpoint_id in 1..=2 {
        store
            .record_cluster_outcome(
                &proof,
                checkpoint_id,
                checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.prune_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let path = lease_path(2);
    let saved = raw.inner.get(&path).await.unwrap().bytes().await.unwrap();
    raw.inner.delete(&path).await.unwrap();
    *store.outcome_audit_cache.lock() = None;
    assert!(matches!(
        store.highest_cluster_terminal_outcome().await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::InventoryChanged(_)
        ))
    ));

    raw.inner.put(&path, PutPayload::from(saved)).await.unwrap();
    raw.clear_get_counts();
    assert_eq!(
        store
            .highest_cluster_terminal_outcome()
            .await
            .unwrap()
            .unwrap()
            .epoch,
        2
    );
    assert_eq!(raw.get_count(&path), 1);
}

#[tokio::test]
async fn repeated_cluster_outcome_appends_do_not_reaudit_history() {
    let (raw, store) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-outcome-hot-appends"),
    );
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.prune_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    store.prune_running.store(true, Ordering::Release);
    raw.clear_get_counts();

    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    for epoch in 1..=8 {
        store
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

    let counts = (1..=9)
        .map(|sequence| raw.get_count(&lease_path(sequence)))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 4, 4, 4, 4, 4, 4, 4, 2]);
    assert_eq!(raw.get_count(&authority_head_path()), 16);
    store.prune_running.store(false, Ordering::Release);
}

#[tokio::test]
async fn all_abort_history_compacts_without_advancing_artifact_retention() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    disable_history_pruning_for_test(&store).await;
    let empty = store.cluster_outcome_retention_boundary().await.unwrap();
    assert_eq!(
        (empty.artifact_before_epoch, empty.terminal_before_epoch),
        (0, 0)
    );
    assert!(empty.committed_anchor.is_none());

    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    for epoch in 1..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap() {
        store
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

    let boundary = store.cluster_outcome_retention_boundary().await.unwrap();
    assert_eq!(boundary.artifact_before_epoch, 0);
    assert!(boundary.terminal_before_epoch > 1);
    assert!(boundary.committed_anchor.is_none());
    let terminal_anchor = boundary.terminal_anchor.unwrap();
    assert!(!terminal_anchor.is_commit());
    assert_eq!(terminal_anchor.epoch + 1, boundary.terminal_before_epoch);
    LeaderLeaseStore::prune_history(&store.store, 0)
        .await
        .unwrap();
    let restarted = LeaderLeaseStore::new(Arc::clone(&store.store), 1_000);
    assert!(restarted
        .highest_cluster_committed_outcome()
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        restarted
            .cluster_outcome(terminal_anchor.epoch)
            .await
            .unwrap(),
        Some(terminal_anchor.clone())
    );

    let compacted_attempt = restarted
        .cluster_attempt_settlement(crate::checkpoint::CheckpointAttempt::canonical(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        compacted_attempt.epoch,
        u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap()
    );
    let exact_anchor = restarted
        .cluster_attempt_settlement(crate::checkpoint::CheckpointAttempt::new(
            terminal_anchor.epoch,
            terminal_anchor.checkpoint_id,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact_anchor, terminal_anchor);
}

#[tokio::test]
async fn history_compaction_retains_lagged_commit_inventory() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    disable_history_pruning_for_test(&store).await;
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    record_commit(&store, &proof, &fence, 1, 1).await;
    for epoch in 2..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap() {
        store
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

    let boundary = store.cluster_outcome_retention_boundary().await.unwrap();
    assert_eq!(boundary.artifact_before_epoch, 0);
    assert!(boundary.terminal_before_epoch > 1);
    assert!(boundary.committed_anchor.is_none());
    assert!(boundary.terminal_anchor.as_ref().unwrap().epoch > 1);
    LeaderLeaseStore::prune_history(&store.store, 0)
        .await
        .unwrap();
    let restarted = LeaderLeaseStore::new(Arc::clone(&store.store), 1_000);
    assert_eq!(
        restarted
            .highest_cluster_committed_outcome()
            .await
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
}

#[tokio::test]
async fn next_commit_is_rejected_at_the_live_commit_capacity_before_sequence_creation() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    disable_history_pruning_for_test(&store).await;
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    begin_checkpoint_artifacts(&store, &proof, &fence, 1).await;
    let first_checkpoint = committed_checkpoint(&store, &fence, 1, 1).await;
    store
        .record_cluster_outcome(
            &proof,
            1,
            1,
            fence.clone(),
            CheckpointVerdict::Commit,
            Some(first_checkpoint),
        )
        .await
        .unwrap();

    let maximum = u64::try_from(MAX_LIVE_AUTHORITY_LINKS).unwrap();
    let authority_before_epoch = maximum
        .checked_sub(u64::try_from(OUTCOME_HISTORY_RETAINED_LINKS).unwrap())
        .and_then(|epoch| epoch.checked_add(1))
        .unwrap();
    let terminal_anchor_epoch = authority_before_epoch - 1;
    let mut current = store.load_record().await.unwrap().unwrap();
    let template = current.checkpoint_outcome.clone().unwrap();
    let mut terminal_anchor = None;
    let mut terminal_anchor_link = None;
    for epoch in 2..=maximum {
        let sequence = current.lease.seq.checked_add(1).unwrap();
        let checkpoint_id = epoch;
        let mut outcome = template.clone();
        outcome.epoch = epoch;
        outcome.checkpoint_id = checkpoint_id;
        let reference = outcome.committed_checkpoint.as_mut().unwrap();
        reference.epoch = epoch;
        reference.checkpoint_id = checkpoint_id;
        let link = OutcomeLink {
            sequence,
            epoch,
            checkpoint_id,
        };
        let mut lease = current.lease.clone();
        lease.seq = sequence;
        let mut next = current.preserve_with_lease(lease);
        next.checkpoint_outcome = Some(outcome.clone());
        next.previous_outcome = current.outcome_head;
        next.outcome_head = Some(link);
        next.previous_commit = current.commit_head;
        next.commit_head = Some(link);
        store
            .store
            .put(
                &lease_path(sequence),
                PutPayload::from(encode_authority_record(&next).unwrap()),
            )
            .await
            .unwrap();
        if epoch == terminal_anchor_epoch {
            terminal_anchor = Some(outcome);
            terminal_anchor_link = Some(link);
        }
        current = next;
    }

    let floor = AuthorityOutcomeFloor {
        deployment_id: template.deployment_id,
        artifact_before_epoch: 0,
        authority_before_epoch,
        terminal_anchor,
        terminal_anchor_link,
        committed_anchor: None,
        committed_anchor_link: None,
    };
    let floor_sequence = current.lease.seq.checked_add(1).unwrap();
    let mut floor_lease = current.lease.clone();
    floor_lease.seq = floor_sequence;
    let mut floor_head = current.preserve_with_lease(floor_lease);
    floor_head.outcome_floor = Some(floor);
    store
        .store
        .put(
            &lease_path(floor_sequence),
            PutPayload::from(encode_authority_record(&floor_head).unwrap()),
        )
        .await
        .unwrap();
    store
        .store
        .put(
            &authority_head_path(),
            PutPayload::from(encode_authority_head_pointer(floor_sequence).unwrap()),
        )
        .await
        .unwrap();
    *store.outcome_audit_cache.lock() = None;

    let next_epoch = maximum.checked_add(1).unwrap();
    let next_checkpoint_id = next_epoch;
    let predecessor = current
        .checkpoint_outcome
        .as_ref()
        .and_then(|outcome| outcome.committed_checkpoint.clone());
    begin_checkpoint_artifacts(&store, &proof, &fence, next_checkpoint_id).await;
    let next_checkpoint =
        committed_checkpoint_with_predecessor(&store, &fence, next_checkpoint_id, 9, predecessor)
            .await;
    let before = store.load_record().await.unwrap().unwrap();
    let error = store
        .record_cluster_outcome(
            &proof,
            next_epoch,
            next_checkpoint_id,
            fence,
            CheckpointVerdict::Commit,
            Some(next_checkpoint),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ClusterCheckpointAuthorityError::Decision(DecisionError::Conflict(message))
            if message.contains("live Commit retention reached")
    ));
    assert!(
        read_authority_record(store.store.as_ref(), before.lease.seq + 1)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.load_record().await.unwrap().unwrap(), before);
}

#[tokio::test]
async fn hot_history_compaction_uses_the_cached_anchor_link() {
    let (raw, store) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-hot-history-compaction"),
    );
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    disable_history_pruning_for_test(&store).await;
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    for epoch in 1..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER).unwrap() {
        store
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

    raw.clear_get_counts();
    let next_epoch = u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap();
    store
        .record_cluster_outcome(
            &proof,
            next_epoch,
            next_epoch,
            fence,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        raw.get_count(&authority_head_path()),
        4,
        "hot compaction must use a fixed number of pointer reads"
    );
    assert_eq!(
        raw.get_count_prefix(LEASE_PREFIX),
        8,
        "hot compaction must use only bounded head and successor reads"
    );
}

#[tokio::test]
async fn restarted_authority_compacts_before_append_with_bounded_terminal_reads() {
    let (raw, store) = blocking_store_at(
        1_000,
        OsPath::from("control/never-block-restarted-history-compaction"),
    );
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    disable_history_pruning_for_test(&store).await;
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    let second_trigger = OUTCOME_HISTORY_COMPACTION_TRIGGER * 2 - OUTCOME_HISTORY_RETAINED_LINKS;
    for epoch in 1..=u64::try_from(second_trigger).unwrap() {
        store
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

    let restarted = LeaderLeaseStore::new(raw.clone(), 1_000);
    restarted.prune_running.store(true, Ordering::Release);
    raw.clear_get_counts();
    let next_epoch = u64::try_from(second_trigger + 1).unwrap();
    restarted
        .record_cluster_outcome(
            &proof,
            next_epoch,
            next_epoch,
            fence,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();

    let head = restarted.load_record().await.unwrap().unwrap();
    let snapshot = restarted
        .cached_audited_cluster_outcomes_from(&head)
        .await
        .unwrap();
    assert!(snapshot.terminal_links.len() <= OUTCOME_HISTORY_COMPACTION_TRIGGER);
    assert_eq!(
        raw.get_count(&authority_head_path()),
        5,
        "cold compaction must use a fixed number of pointer reads"
    );
    assert_eq!(
        raw.get_count_prefix(LEASE_PREFIX),
        u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 10).unwrap(),
        "cold compaction must perform exactly one bounded authority-chain audit"
    );
    assert!(head.outcome_floor.as_ref().unwrap().authority_before_epoch > 1);
}

#[tokio::test]
async fn older_concurrent_outcome_audit_cannot_replace_a_newer_cache_entry() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let setup = Arc::new(LeaderLeaseStore::new(Arc::clone(&inner), 1_000));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = setup
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    for checkpoint_id in 1..=2 {
        setup
            .record_cluster_outcome(
                &proof,
                checkpoint_id,
                checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }

    let (raw, store) = blocking_get_once_with_inner(1_000, inner, lease_path(3));
    store.prune_running.store(true, Ordering::Release);
    let old_store = Arc::clone(&store);
    let old_audit = tokio::spawn(async move {
        old_store
            .cluster_attempt_settlement(crate::checkpoint::CheckpointAttempt::canonical(1))
            .await
    });
    raw.entered.acquire().await.unwrap().forget();

    store
        .record_cluster_outcome(&proof, 3, 3, fence, CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    let newest = store
        .highest_cluster_terminal_outcome()
        .await
        .unwrap()
        .unwrap();
    assert_eq!((newest.epoch, newest.checkpoint_id), (3, 3));

    raw.release.add_permits(1);
    let old = old_audit.await.unwrap().unwrap().unwrap();
    assert_eq!((old.epoch, old.checkpoint_id), (1, 1));
    {
        let cache = store.outcome_audit_cache.lock();
        let cached = cache.as_ref().expect("newer audit must remain cached");
        assert_eq!(cached.authority_sequence, 4);
        assert_eq!(
            cached.snapshot.outcomes.last().map(|outcome| outcome.epoch),
            Some(3)
        );
    }

    raw.clear_get_counts();
    assert_eq!(
        store
            .highest_cluster_terminal_outcome()
            .await
            .unwrap()
            .unwrap()
            .epoch,
        3
    );
    assert_eq!(raw.get_count(&lease_path(4)), 1);
    assert_eq!(raw.get_count(&lease_path(3)), 0);
}

#[tokio::test]
async fn cluster_decision_rejects_noncanonical_attempt_before_mutation() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let before = store.load_record().await.unwrap().unwrap();

    assert!(matches!(
        store
            .record_cluster_outcome(
                &first.proof(),
                1,
                10,
                assignment_fence(&incumbent),
                CheckpointVerdict::Abort,
                None,
            )
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
    assert_eq!(store.load_record().await.unwrap().unwrap(), before);
    assert!(matches!(
        store
            .store
            .head(&OsPath::from("checkpoint-deployment/identity.json"))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn cluster_commit_rejects_a_fork_from_the_authoritative_commit_head() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    record_commit(&store, &proof, &fence, 1, 1).await;
    begin_checkpoint_artifacts(&store, &proof, &fence, 2).await;
    let fork = committed_checkpoint(&store, &fence, 2, 2).await;
    let before = store.load_record().await.unwrap().unwrap();

    assert!(matches!(
        store
            .record_cluster_outcome(
                &proof,
                2,
                2,
                fence,
                CheckpointVerdict::Commit,
                Some(fork),
            )
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(message)
        )) if message.contains("does not extend the authoritative Commit head")
    ));
    assert_eq!(store.load_record().await.unwrap().unwrap(), before);
}

#[tokio::test]
async fn cluster_commit_extends_the_same_head_after_an_intervening_abort() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let fence = assignment_fence(&incumbent);
    begin_checkpoint_artifacts(&store, &proof, &fence, 1).await;
    let first_checkpoint = committed_checkpoint(&store, &fence, 1, 1).await;
    store
        .record_cluster_outcome(
            &proof,
            1,
            1,
            fence.clone(),
            CheckpointVerdict::Commit,
            Some(first_checkpoint.clone()),
        )
        .await
        .unwrap();
    store
        .record_cluster_outcome(&proof, 2, 2, fence.clone(), CheckpointVerdict::Abort, None)
        .await
        .unwrap();
    begin_checkpoint_artifacts(&store, &proof, &fence, 3).await;
    let next_checkpoint =
        committed_checkpoint_with_predecessor(&store, &fence, 3, 9, Some(first_checkpoint)).await;

    assert!(matches!(
        store
            .record_cluster_outcome(
                &proof,
                3,
                3,
                fence,
                CheckpointVerdict::Commit,
                Some(next_checkpoint.clone()),
            )
            .await
            .unwrap(),
        RecordOutcomeResult::Created(outcome)
            if outcome.committed_checkpoint == Some(next_checkpoint)
    ));
}

#[tokio::test]
async fn cluster_decision_rejects_foreign_owner_and_fencing_token() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let fence = assignment_fence(&incumbent);
    let mut wrong_token = first.proof();
    wrong_token.fencing_token += 1;
    assert!(matches!(
        store
            .record_cluster_outcome(
                &wrong_token,
                1,
                1,
                fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await,
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));
    let foreign = LeaderProof {
        owner: owner(2, 2, 1).proof_owner(),
        fencing_token: first.token,
    };
    assert!(matches!(
        store
            .record_cluster_outcome(&foreign, 1, 1, fence, CheckpointVerdict::Abort, None,)
            .await,
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));
    assert!(store.cluster_outcomes().await.unwrap().is_empty());
}

#[tokio::test]
async fn delayed_catalog_seal_is_fenced_when_takeover_wins_the_sequence() {
    let (raw, store) = blocking_once_at(10, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let successor = owner(2, 2, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let observation = store.observe_rival(&successor, &first).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    let proof = first.proof();
    let manifest = catalog("events");
    let seal_store = Arc::clone(&store);
    let seal_manifest = manifest.clone();
    let seal = tokio::spawn(async move { seal_store.seal_catalog(&proof, &seal_manifest).await });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let LeaseOutcome::Acquired(takeover) = store
        .try_takeover(&successor, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("successor must win the unblocked create-only sequence");
    };
    assert_eq!(takeover.owner, successor);
    assert!(takeover.catalog_manifest.is_none());
    raw.release.add_permits(1);
    assert!(matches!(
        seal.await.unwrap(),
        Err(CatalogManifestError::Fenced)
    ));
    assert!(store
        .load()
        .await
        .unwrap()
        .unwrap()
        .catalog_manifest
        .is_none());
}

#[tokio::test]
async fn delayed_catalog_seal_retries_after_same_term_renewal_wins_the_sequence() {
    let (raw, store) = blocking_once_at(1_000, lease_path(2));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = first.proof();
    let manifest = catalog("events");
    let seal_store = Arc::clone(&store);
    let seal_manifest = manifest.clone();
    let seal = tokio::spawn(async move { seal_store.seal_catalog(&proof, &seal_manifest).await });
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let LeaseOutcome::Acquired(renewal) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 1)
        .await
        .unwrap()
    else {
        panic!("same owner must renew through the unblocked sequence");
    };
    assert_eq!(renewal.seq, 2);
    assert!(renewal.catalog_manifest.is_none());
    raw.release.add_permits(1);
    assert_eq!(seal.await.unwrap().unwrap(), CatalogSealOutcome::Created);
    let sealed = store.load().await.unwrap().unwrap();
    assert_eq!(sealed.seq, 3);
    let reference = sealed
        .catalog_manifest
        .expect("catalog reference must be sealed");
    assert_eq!(
        store.load_catalog_manifest(&reference).await.unwrap(),
        manifest
    );
}

#[tokio::test]
async fn takeover_preserves_a_catalog_sealed_before_it() {
    let store = store(10);
    let incumbent = owner(1, 1, 1);
    let successor = owner(2, 2, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let manifest = catalog("events");
    assert_eq!(
        store.seal_catalog(&first.proof(), &manifest).await.unwrap(),
        CatalogSealOutcome::Created
    );
    let sealed = store.load().await.unwrap().unwrap();
    let sealed_reference = sealed
        .catalog_manifest
        .clone()
        .expect("catalog reference must be sealed");
    let observation = store.observe_rival(&successor, &sealed).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let LeaseOutcome::Acquired(takeover) = store
        .try_takeover(&successor, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("successor must acquire after a full observation");
    };
    assert_eq!(takeover.catalog_manifest, Some(sealed_reference.clone()));
    assert_eq!(
        store
            .load_catalog_manifest(&sealed_reference)
            .await
            .unwrap(),
        manifest
    );
}

#[tokio::test]
async fn renewals_copy_only_the_bounded_catalog_reference() {
    let store = store(1_000);
    let owner = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&owner, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let manifest = CatalogManifest::new(vec![super::super::CatalogManifestEntry {
        canonical_name: "events".into(),
        kind: crate::catalog::CatalogObjectKind::Source,
        catalog_generation: 1,
        ddl: format!(
            "CREATE SOURCE events FROM GENERATOR ('description' = '{}')",
            "x".repeat(100_000)
        ),
    }])
    .unwrap();
    store.seal_catalog(&first.proof(), &manifest).await.unwrap();

    let LeaseOutcome::Acquired(renewed) = store
        .acquire_or_renew_current_term_for_test(&owner, 1)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let reference = renewed
        .catalog_manifest
        .clone()
        .expect("renewal must retain the catalog reference");
    assert!(serde_json::to_vec(&renewed).unwrap().len() < 512);
    assert_eq!(
        store.load_catalog_manifest(&reference).await.unwrap(),
        manifest
    );
}

#[tokio::test]
async fn preexisting_manifest_blob_must_match_exact_content() {
    let store = store(1_000);
    let owner = owner(1, 1, 1);
    let LeaseOutcome::Acquired(first) = store
        .acquire_or_renew_current_term_for_test(&owner, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let manifest = catalog("events");
    let (_, reference) = manifest.encode_and_reference().unwrap();
    store
        .store
        .put(
            &reference.object_path(),
            PutPayload::from(Bytes::from_static(b"corrupt")),
        )
        .await
        .unwrap();

    assert!(matches!(
        store.seal_catalog(&first.proof(), &manifest).await,
        Err(CatalogManifestError::Invalid(_))
    ));
    assert!(store
        .load()
        .await
        .unwrap()
        .unwrap()
        .catalog_manifest
        .is_none());
}

#[cfg(feature = "cluster")]
fn blocking_store(ttl_ms: i64) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
    blocking_store_at(ttl_ms, lease_path(2))
}

#[cfg(feature = "cluster")]
async fn wait_for_lease(lease: &mut watch::Receiver<Option<LeaderLease>>) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while lease.borrow_and_update().is_none() {
            lease.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[cfg(feature = "cluster")]
fn candidacy_channel(
    eligible: bool,
) -> (
    watch::Sender<LeaderCandidacy>,
    watch::Receiver<LeaderCandidacy>,
) {
    watch::channel(LeaderCandidacy::initial(eligible))
}

#[cfg(feature = "cluster")]
fn set_candidacy(candidate: &watch::Sender<LeaderCandidacy>, eligible: bool) {
    candidate.send_modify(|current| {
        *current = current
            .transition(eligible)
            .expect("leader candidacy generation");
    });
}

#[cfg(feature = "cluster")]
#[test]
fn withdrawal_rotates_an_expired_never_published_deadline_generation() {
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(
        Arc::new(store(100)),
        &process(&owner),
        LeaderLeaseConfig {
            ttl: Duration::from_millis(100),
            renew_interval: Duration::from_millis(20),
        },
    )
    .unwrap();
    let expired = manager.deadline();
    expired.extend(Duration::from_nanos(1));
    while expired.is_live() {
        std::hint::spin_loop();
    }

    assert!(!manager.withdraw());
    let fresh = manager.deadline();
    assert!(!Arc::ptr_eq(&expired, &fresh));
    expired.extend(Duration::from_secs(1));
    assert!(!expired.is_live());
    fresh.extend(Duration::from_secs(1));
    assert!(fresh.is_live());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn supervised_manager_exit_signals_process_authority_loss() {
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(
        Arc::new(store(100)),
        &process(&owner),
        LeaderLeaseConfig {
            ttl: Duration::from_millis(100),
            renew_interval: Duration::from_millis(20),
        },
    )
    .unwrap();
    let (candidate_tx, candidate_rx) = candidacy_channel(true);
    drop(candidate_tx);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let unexpected_exit = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn_supervised(shutdown, candidate_rx, unexpected_exit.clone());

    tokio::time::timeout(Duration::from_secs(1), unexpected_exit.cancelled())
        .await
        .expect("unsupervised manager exit did not fence process authority");
    task.await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn supervised_manager_intentional_shutdown_does_not_signal_authority_loss() {
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(
        Arc::new(store(100)),
        &process(&owner),
        LeaderLeaseConfig {
            ttl: Duration::from_millis(100),
            renew_interval: Duration::from_millis(20),
        },
    )
    .unwrap();
    let mut lease = manager.lease_watch();
    let (_candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let unexpected_exit = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn_supervised(shutdown.clone(), candidate_rx, unexpected_exit.clone());
    wait_for_lease(&mut lease).await;

    shutdown.cancel();
    task.await.unwrap();
    assert!(!unexpected_exit.is_cancelled());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn supervised_manager_abort_fences_the_published_grant_before_signalling_loss() {
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(
        Arc::new(store(1_000)),
        &process(&owner),
        LeaderLeaseConfig {
            ttl: Duration::from_secs(1),
            renew_interval: Duration::from_millis(100),
        },
    )
    .unwrap();
    let mut lease = manager.lease_watch();
    let deadline = manager.deadline_watch();
    let (_candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let unexpected_exit = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn_supervised(shutdown, candidate_rx, unexpected_exit.clone());
    wait_for_lease(&mut lease).await;
    let published_deadline = deadline.borrow().clone();

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(unexpected_exit.is_cancelled());
    assert!(lease.borrow().is_none());
    assert!(!published_deadline.is_live());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn supervised_manager_forced_shutdown_fences_without_reporting_unexpected_loss() {
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(
        Arc::new(store(1_000)),
        &process(&owner),
        LeaderLeaseConfig {
            ttl: Duration::from_secs(1),
            renew_interval: Duration::from_millis(100),
        },
    )
    .unwrap();
    let mut lease = manager.lease_watch();
    let deadline = manager.deadline_watch();
    let (_candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let unexpected_exit = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn_supervised(shutdown.clone(), candidate_rx, unexpected_exit.clone());
    wait_for_lease(&mut lease).await;
    let published_deadline = deadline.borrow().clone();

    shutdown.cancel();
    task.abort();
    let _ = task.await;
    assert!(!unexpected_exit.is_cancelled());
    assert!(lease.borrow().is_none());
    assert!(!published_deadline.is_live());
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn delayed_durable_acquisition_response_rotates_after_attempt_deadline() {
    let ttl = Duration::from_millis(40);
    let (raw, store) = delayed_response_once_at(40, lease_path(1));
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(
        Arc::clone(&store),
        &process(&owner),
        LeaderLeaseConfig {
            ttl,
            renew_interval: Duration::from_millis(5),
        },
    )
    .unwrap();
    let old_deadline = manager.deadline();
    let deadline = manager.deadline_watch();
    let mut lease = manager.lease_watch();
    let (_candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn(shutdown.clone(), candidate_rx);

    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let first = store
        .load()
        .await
        .unwrap()
        .expect("durable first term must exist before its delayed response");
    assert_eq!(first.owner, owner);
    let stale_proof = first.proof();

    tokio::time::timeout(ttl + Duration::from_secs(1), async {
        loop {
            if lease
                .borrow_and_update()
                .as_ref()
                .is_some_and(|current| current.token > first.token)
            {
                break;
            }
            lease.changed().await.unwrap();
        }
    })
    .await
    .expect("manager did not replace the ambiguous first term after its local deadline");
    let current = lease.borrow().clone().expect("rotated local leader grant");
    let current_deadline = deadline.borrow().clone();
    assert!(!old_deadline.is_live());
    assert!(!Arc::ptr_eq(&old_deadline, &current_deadline));
    assert!(current_deadline.is_live());
    assert!(current.token > first.token);
    assert_eq!(store.load().await.unwrap().unwrap().token, current.token);
    assert!(!lease_grants_proof(
        &Some(current),
        &owner,
        &current_deadline,
        &stale_proof,
    ));
    assert!(!task.is_finished());

    raw.release.add_permits(1);
    shutdown.cancel();
    task.await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn candidacy_loss_interrupts_hung_renewal_and_withdraws_the_grant() {
    let (raw, store) = blocking_store(80);
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(
        Arc::clone(&store),
        &process(&owner),
        LeaderLeaseConfig {
            ttl: Duration::from_millis(80),
            renew_interval: Duration::from_millis(10),
        },
    )
    .unwrap();
    let old_deadline = manager.deadline();
    let deadline = manager.deadline_watch();
    let mut lease = manager.lease_watch();
    let (candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn(shutdown.clone(), candidate_rx);
    wait_for_lease(&mut lease).await;
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    set_candidacy(&candidate_tx, false);
    tokio::time::timeout(Duration::from_millis(40), lease.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(lease.borrow().is_none());
    let current_deadline = deadline.borrow().clone();
    assert!(!old_deadline.is_live());
    assert!(!Arc::ptr_eq(&old_deadline, &current_deadline));
    assert!(!current_deadline.is_live());
    raw.release.add_permits(1);
    shutdown.cancel();
    task.await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn candidacy_reacquisition_rotates_the_durable_fencing_token() {
    let owner = owner(1, 1, 1);
    let store = Arc::new(store(500));
    let manager = LeaderLeaseManager::new(
        Arc::clone(&store),
        &process(&owner),
        LeaderLeaseConfig {
            ttl: Duration::from_millis(500),
            renew_interval: Duration::from_millis(20),
        },
    )
    .unwrap();
    let old_deadline = manager.deadline();
    let deadline = manager.deadline_watch();
    let mut lease = manager.lease_watch();
    let (candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn(shutdown.clone(), candidate_rx);

    wait_for_lease(&mut lease).await;
    let first = lease.borrow().clone().expect("initial leader grant");
    let stale_proof = first.proof();
    set_candidacy(&candidate_tx, false);
    tokio::time::timeout(Duration::from_secs(1), async {
        while lease.borrow_and_update().is_some() {
            lease.changed().await.unwrap();
        }
    })
    .await
    .expect("candidacy loss did not withdraw the local grant");
    assert!(!lease_grants_proof(
        &lease.borrow().clone(),
        &owner,
        &old_deadline,
        &stale_proof,
    ));

    set_candidacy(&candidate_tx, true);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if lease
                .borrow_and_update()
                .as_ref()
                .is_some_and(|current| current.token > first.token)
            {
                break;
            }
            lease.changed().await.unwrap();
        }
    })
    .await
    .expect("candidacy reacquisition did not publish a new fencing token");
    let reacquired = lease.borrow().clone().expect("reacquired leader grant");
    assert!(reacquired.token > first.token);
    assert_eq!(
        store
            .load()
            .await
            .unwrap()
            .expect("durable leader grant")
            .token,
        reacquired.token
    );
    let current_deadline = deadline.borrow().clone();
    assert!(!old_deadline.is_live());
    assert!(!Arc::ptr_eq(&old_deadline, &current_deadline));
    assert!(current_deadline.is_live());
    let current_proof = reacquired.proof();
    assert!(lease_grants_proof(
        &Some(reacquired.clone()),
        &owner,
        &current_deadline,
        &current_proof,
    ));
    assert!(!lease_grants_proof(
        &Some(reacquired),
        &owner,
        &current_deadline,
        &stale_proof,
    ));

    shutdown.cancel();
    task.await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "current_thread")]
async fn coalesced_candidacy_loss_still_rotates_the_fencing_token() {
    let owner = owner(1, 2, 1);
    let store = Arc::new(store(500));
    let manager = LeaderLeaseManager::new(
        Arc::clone(&store),
        &process(&owner),
        LeaderLeaseConfig {
            ttl: Duration::from_millis(500),
            renew_interval: Duration::from_millis(20),
        },
    )
    .unwrap();
    let old_deadline = manager.deadline();
    let deadline = manager.deadline_watch();
    let mut lease = manager.lease_watch();
    let (candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn(shutdown.clone(), candidate_rx);
    wait_for_lease(&mut lease).await;
    let first = lease.borrow().clone().expect("initial leader grant");
    let stale_proof = first.proof();

    // No await between these updates: the receiver observes only the final eligible value.
    set_candidacy(&candidate_tx, false);
    set_candidacy(&candidate_tx, true);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if lease
                .borrow_and_update()
                .as_ref()
                .is_some_and(|current| current.token > first.token)
            {
                break;
            }
            lease.changed().await.unwrap();
        }
    })
    .await
    .expect("coalesced candidacy loss reused the old fencing token");
    let current = lease.borrow().clone().expect("rotated leader grant");
    let current_deadline = deadline.borrow().clone();
    assert!(!old_deadline.is_live());
    assert!(!Arc::ptr_eq(&old_deadline, &current_deadline));
    assert!(current_deadline.is_live());
    assert!(!lease_grants_proof(
        &Some(current),
        &owner,
        &current_deadline,
        &stale_proof,
    ));

    shutdown.cancel();
    task.await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn hung_renewal_withdraws_then_reacquires_with_a_new_fencing_token() {
    let ttl = Duration::from_millis(200);
    let (raw, store) = blocking_store(200);
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(
        Arc::clone(&store),
        &process(&owner),
        LeaderLeaseConfig {
            ttl,
            renew_interval: Duration::from_millis(20),
        },
    )
    .unwrap();
    let old_deadline = manager.deadline();
    let deadline = manager.deadline_watch();
    let mut lease = manager.lease_watch();
    let (candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn(shutdown.clone(), candidate_rx);
    wait_for_lease(&mut lease).await;
    let first = lease.borrow().clone().expect("initial leader grant");
    let stale_proof = first.proof();
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    tokio::time::timeout(Duration::from_millis(400), async {
        while lease.borrow_and_update().is_some() {
            lease.changed().await.unwrap();
        }
    })
    .await
    .expect("expired renewal did not withdraw the local grant");
    assert!(lease.borrow().is_none());
    assert!(!old_deadline.is_live());
    assert!(!task.is_finished());

    assert!(
        tokio::time::timeout(ttl / 2, raw.entered.acquire())
            .await
            .is_err(),
        "manager rotated the old fencing token before its settlement grace elapsed"
    );
    set_candidacy(&candidate_tx, false);
    tokio::task::yield_now().await;
    set_candidacy(&candidate_tx, true);
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(ttl / 4, raw.entered.acquire())
            .await
            .is_err(),
        "candidacy churn erased the outstanding settlement grace"
    );
    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .expect("manager did not retry the durable term after withdrawal")
        .unwrap()
        .forget();
    raw.release.add_permits(1);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if lease
                .borrow_and_update()
                .as_ref()
                .is_some_and(|current| current.token > first.token)
            {
                break;
            }
            lease.changed().await.unwrap();
        }
    })
    .await
    .expect("manager did not reacquire with a rotated fencing token");
    let current = lease.borrow().clone().expect("reacquired leader grant");
    let current_deadline = deadline.borrow().clone();
    assert!(!Arc::ptr_eq(&old_deadline, &current_deadline));
    assert!(current_deadline.is_live());
    assert!(current.token > first.token);
    assert_eq!(store.load().await.unwrap().unwrap().token, current.token);
    assert!(!lease_grants_proof(
        &Some(current),
        &owner,
        &current_deadline,
        &stale_proof,
    ));

    shutdown.cancel();
    task.await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn expired_local_grant_preserves_old_proof_commit_before_token_rotation() {
    let ttl = Duration::from_millis(200);
    // Initial lease is sequence 1 and artifact admission is sequence 2. Block the renewal's
    // sequence-3 create; once that cancelled writer lapses, the old-proof Commit may use seq 3.
    let (raw, store) = blocking_once_at(200, lease_path(3));
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(
        Arc::clone(&store),
        &process(&owner),
        LeaderLeaseConfig {
            ttl,
            renew_interval: Duration::from_millis(20),
        },
    )
    .unwrap();
    let old_deadline = manager.deadline();
    let deadline = manager.deadline_watch();
    let mut lease = manager.lease_watch();
    let (_candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn(shutdown.clone(), candidate_rx);
    wait_for_lease(&mut lease).await;
    let first = lease.borrow().clone().expect("initial leader grant");
    let proof = first.proof();
    let fence = assignment_fence(&owner);
    begin_checkpoint_artifacts(&store, &proof, &fence, 1).await;
    let committed = committed_checkpoint(&store, &fence, 1, 1).await;

    tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
        .await
        .expect("renewal did not enter the blocked authority create")
        .unwrap()
        .forget();
    tokio::time::timeout(ttl + Duration::from_millis(1), async {
        while lease.borrow_and_update().is_some() {
            lease.changed().await.unwrap();
        }
    })
    .await
    .expect("expired renewal did not withdraw the local grant");
    assert!(!old_deadline.is_live());

    tokio::time::advance(ttl / 2).await;
    assert!(lease.borrow().is_none());
    let outcome = store
        .record_cluster_outcome(
            &proof,
            1,
            1,
            fence,
            CheckpointVerdict::Commit,
            Some(committed.clone()),
        )
        .await
        .expect("old-proof Commit must remain legal during settlement grace");
    assert!(matches!(outcome, RecordOutcomeResult::Created(_)));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if lease
                .borrow_and_update()
                .as_ref()
                .is_some_and(|current| current.token > first.token)
            {
                break;
            }
            lease.changed().await.unwrap();
        }
    })
    .await
    .expect("manager did not rotate after the settlement grace");
    let current = lease.borrow().clone().expect("rotated leader grant");
    let current_deadline = deadline.borrow().clone();
    assert!(current_deadline.is_live());
    assert!(current.token > first.token);
    assert_eq!(
        store
            .highest_cluster_committed_outcome()
            .await
            .unwrap()
            .and_then(|outcome| outcome.committed_checkpoint),
        Some(committed)
    );
    assert!(!lease_grants_proof(
        &Some(current),
        &owner,
        &current_deadline,
        &proof,
    ));

    raw.release.add_permits(1);
    shutdown.cancel();
    task.await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shutdown_clears_published_grant_and_fences() {
    let config = LeaderLeaseConfig {
        ttl: Duration::from_millis(100),
        renew_interval: Duration::from_millis(20),
    };
    let owner = owner(1, 1, 1);
    let manager = LeaderLeaseManager::new(Arc::new(store(100)), &process(&owner), config).unwrap();
    let deadline = manager.deadline();
    let mut lease = manager.lease_watch();
    let (_candidate_tx, candidate_rx) = candidacy_channel(true);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = manager.spawn(shutdown.clone(), candidate_rx);
    wait_for_lease(&mut lease).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_millis(50), task)
        .await
        .unwrap()
        .unwrap();
    assert!(lease.borrow().is_none());
    assert!(!deadline.is_live());
}

#[test]
fn grant_requires_exact_owner_and_live_deadline() {
    let expected = owner(1, 1, 1);
    let lease = Some(LeaderLease {
        seq: 1,
        renewal_sequence: 1,
        token: 1,
        owner: expected.clone(),
        expires_at_ms: i64::MIN,
        catalog_manifest: None,
    });
    let deadline = LeaseDeadline::live_for(Duration::from_secs(1));
    assert!(lease_grants_leadership(&lease, &expected, &deadline));
    assert!(!lease_grants_leadership(&lease, &owner(1, 2, 2), &deadline));
    deadline.fence();
    assert!(!lease_grants_leadership(&lease, &expected, &deadline));
}

async fn artifact_cleanup_chain(
    ttl_ms: i64,
    commits: u64,
) -> (
    Arc<LeaderLeaseStore>,
    LeaderLeaseOwner,
    LeaderProof,
    CheckpointAssignmentFence,
    Vec<CommittedCheckpointRef>,
) {
    let store = Arc::new(store(ttl_ms));
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = lease.proof();
    let fence = assignment_fence(&incumbent);
    let mut references = Vec::new();
    for checkpoint_id in 1..=commits {
        let outcome =
            match record_commit(store.as_ref(), &proof, &fence, checkpoint_id, checkpoint_id).await
            {
                RecordOutcomeResult::Created(outcome) | RecordOutcomeResult::Unchanged(outcome) => {
                    outcome
                }
                RecordOutcomeResult::Conflict { winner } => {
                    panic!("unexpected checkpoint winner: {winner:?}")
                }
            };
        references.push(outcome.committed_checkpoint.unwrap());
    }
    (store, incumbent, proof, fence, references)
}

#[tokio::test]
async fn drain_abort_requires_a_matching_cut_and_pins_it_until_rollback_commits() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = lease.proof();
    let transition = assignment_drain_transition(&incumbent, proof.clone());
    assert!(matches!(
        record_commit(&store, &proof, &transition.target, 1, 1).await,
        RecordOutcomeResult::Created(_)
    ));
    let abort = AssignmentDrainDecision::abort(&transition, proof.clone()).unwrap();
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, abort.clone())
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));

    let checkpoint = match record_commit(&store, &proof, &transition.predecessor, 2, 2).await {
        RecordOutcomeResult::Created(outcome) => outcome.committed_checkpoint.unwrap(),
        outcome => panic!("unexpected rollback checkpoint result: {outcome:?}"),
    };
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, abort)
            .await
            .unwrap(),
        RecordAssignmentDrainDecisionResult::Created(_)
    ));
    let rollback = LeaderLeaseStore::aborted_handoff_target(&transition).unwrap();
    assert_eq!(
        store
            .assignment_handoff_checkpoint(&rollback)
            .await
            .unwrap(),
        Some(checkpoint)
    );

    assert!(matches!(
        record_commit(&store, &proof, &rollback, 3, 3).await,
        RecordOutcomeResult::Created(_)
    ));
    assert_eq!(
        store
            .assignment_handoff_checkpoint(&rollback)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn drain_commit_rejects_a_handoff_that_is_not_the_current_commit() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = lease.proof();
    let transition = assignment_drain_transition(&incumbent, proof.clone());
    let first = match record_commit(&store, &proof, &transition.predecessor, 1, 1).await {
        RecordOutcomeResult::Created(outcome) => outcome.committed_checkpoint.unwrap(),
        outcome => panic!("unexpected first checkpoint result: {outcome:?}"),
    };
    assert!(matches!(
        record_commit(&store, &proof, &transition.predecessor, 2, 2).await,
        RecordOutcomeResult::Created(_)
    ));
    let decision = AssignmentDrainDecision::commit(&transition, proof.clone(), first).unwrap();
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, decision)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
}

#[tokio::test]
async fn handoff_pin_rejects_stale_commits_and_releases_on_a_target_commit() {
    let store = store(1_000);
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store
        .acquire_or_renew_current_term_for_test(&incumbent, 0)
        .await
        .unwrap()
    else {
        unreachable!()
    };
    let proof = lease.proof();
    let transition = assignment_drain_transition(&incumbent, proof.clone());
    let handoff = match record_commit(&store, &proof, &transition.predecessor, 1, 1).await {
        RecordOutcomeResult::Created(outcome) => outcome.committed_checkpoint.unwrap(),
        outcome => panic!("unexpected handoff checkpoint result: {outcome:?}"),
    };
    let decision =
        AssignmentDrainDecision::commit(&transition, proof.clone(), handoff.clone()).unwrap();
    assert!(matches!(
        store
            .record_assignment_drain_decision(&proof, decision)
            .await
            .unwrap(),
        RecordAssignmentDrainDecisionResult::Created(_)
    ));
    assert_eq!(
        store
            .assignment_handoff_checkpoint(&transition.target)
            .await
            .unwrap()
            .as_ref(),
        Some(&handoff)
    );

    let rejected_inventory =
        checkpoint_artifact_inventory(&store, &transition.predecessor, 2).await;
    assert!(matches!(
        store
            .begin_cluster_checkpoint_artifacts(&proof, rejected_inventory)
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
    assert!(store
        .cluster_checkpoint_artifacts()
        .await
        .unwrap()
        .is_none());

    let target = match record_commit(&store, &proof, &transition.target, 3, 3).await {
        RecordOutcomeResult::Created(outcome) => outcome.committed_checkpoint.unwrap(),
        outcome => panic!("unexpected target checkpoint result: {outcome:?}"),
    };
    assert_eq!(
        store
            .assignment_handoff_checkpoint(&transition.target)
            .await
            .unwrap(),
        None
    );
    let cleanup = store
        .begin_cluster_artifact_cleanup(&proof, target, accept_recovery_artifacts)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cleanup.current, handoff);
}

#[tokio::test]
async fn cluster_artifact_cleanup_advances_only_through_durable_phases() {
    let (store, _incumbent, proof, _fence, references) = artifact_cleanup_chain(1_000, 3).await;
    let cursor = store
        .begin_cluster_artifact_cleanup(&proof, references[2].clone(), accept_recovery_artifacts)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.protected, references[2]);
    assert_eq!(cursor.current, references[1]);
    assert_eq!(cursor.next.as_ref(), Some(&references[0]));
    assert_eq!(cursor.stop_before, None);
    assert_eq!(cursor.participant_ids, vec![1]);
    assert_eq!(cursor.phase, ClusterArtifactCleanupPhase::DeleteData);
    assert_eq!(
        store
            .cluster_outcome_retention_boundary()
            .await
            .unwrap()
            .artifact_before_epoch,
        3
    );

    let identical = store
        .begin_cluster_artifact_cleanup(&proof, references[2].clone(), |_| async {
            Err("identical begin must not repeat preflight".into())
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(identical, cursor);

    let metadata = store
        .mark_cluster_artifact_data_deleted(&proof, &cursor)
        .await
        .unwrap();
    assert_eq!(metadata.phase, ClusterArtifactCleanupPhase::DeleteMetadata);
    assert_eq!(
        store
            .mark_cluster_artifact_data_deleted(&proof, &cursor)
            .await
            .unwrap(),
        metadata
    );
    let oldest = store
        .mark_cluster_artifact_metadata_deleted(&proof, &metadata)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(oldest.current, references[0]);
    assert_eq!(oldest.next, None);
    assert_eq!(oldest.phase, ClusterArtifactCleanupPhase::DeleteData);
    let oldest_metadata = store
        .mark_cluster_artifact_data_deleted(&proof, &oldest)
        .await
        .unwrap();
    assert!(store
        .mark_cluster_artifact_metadata_deleted(&proof, &oldest_metadata)
        .await
        .unwrap()
        .is_none());
    assert!(store.cluster_artifact_cleanup().await.unwrap().is_none());
    assert!(store
        .begin_cluster_artifact_cleanup(&proof, references[2].clone(), accept_recovery_artifacts,)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn cluster_artifact_cleanup_resumes_at_the_previous_floor_boundary() {
    let (store, _incumbent, proof, fence, mut references) = artifact_cleanup_chain(1_000, 3).await;
    let first = store
        .begin_cluster_artifact_cleanup(&proof, references[2].clone(), accept_recovery_artifacts)
        .await
        .unwrap()
        .unwrap();
    let first = store
        .mark_cluster_artifact_data_deleted(&proof, &first)
        .await
        .unwrap();
    let oldest = store
        .mark_cluster_artifact_metadata_deleted(&proof, &first)
        .await
        .unwrap()
        .unwrap();
    let oldest = store
        .mark_cluster_artifact_data_deleted(&proof, &oldest)
        .await
        .unwrap();
    assert!(store
        .mark_cluster_artifact_metadata_deleted(&proof, &oldest)
        .await
        .unwrap()
        .is_none());

    for checkpoint_id in 4..=5 {
        let RecordOutcomeResult::Created(outcome) =
            record_commit(store.as_ref(), &proof, &fence, checkpoint_id, checkpoint_id).await
        else {
            panic!("new checkpoint must be created")
        };
        references.push(outcome.committed_checkpoint.unwrap());
    }
    let next_segment = store
        .begin_cluster_artifact_cleanup(&proof, references[4].clone(), accept_recovery_artifacts)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next_segment.current, references[3]);
    assert_eq!(next_segment.next.as_ref(), Some(&references[2]));
    assert_eq!(next_segment.stop_before.as_ref(), Some(&references[1]));

    let next_segment = store
        .mark_cluster_artifact_data_deleted(&proof, &next_segment)
        .await
        .unwrap();
    let prior_protected = store
        .mark_cluster_artifact_metadata_deleted(&proof, &next_segment)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prior_protected.current, references[2]);
    assert_eq!(prior_protected.next.as_ref(), Some(&references[1]));
    let prior_protected = store
        .mark_cluster_artifact_data_deleted(&proof, &prior_protected)
        .await
        .unwrap();
    assert!(store
        .mark_cluster_artifact_metadata_deleted(&proof, &prior_protected)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn cluster_artifact_cleanup_preflight_failure_publishes_nothing() {
    let (store, _incumbent, proof, _fence, references) = artifact_cleanup_chain(1_000, 3).await;
    assert!(matches!(
        store
            .begin_cluster_artifact_cleanup(&proof, references[2].clone(), |_| async {
                Err("protected manifests are unavailable".into())
            })
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
    assert!(store.cluster_artifact_cleanup().await.unwrap().is_none());
    assert_eq!(
        store
            .cluster_outcome_retention_boundary()
            .await
            .unwrap()
            .artifact_before_epoch,
        0
    );
}

#[tokio::test]
async fn cluster_artifact_cleanup_is_preserved_and_fenced_across_takeover() {
    let (store, incumbent, proof, _fence, references) = artifact_cleanup_chain(2, 3).await;
    let cursor = store
        .begin_cluster_artifact_cleanup(&proof, references[2].clone(), accept_recovery_artifacts)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        store
            .begin_cluster_artifact_cleanup(
                &proof,
                references[1].clone(),
                accept_recovery_artifacts,
            )
            .await,
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));

    let successor = owner(2, 2, 1);
    let current_lease = store.load().await.unwrap().unwrap();
    assert_eq!(current_lease.owner, incumbent);
    let observation = store.observe_rival(&successor, &current_lease).unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let LeaseOutcome::Acquired(successor_lease) = store
        .try_takeover(&successor, &observation, 10)
        .await
        .unwrap()
    else {
        panic!("successor must acquire after a full observation")
    };
    assert_eq!(
        store.cluster_artifact_cleanup().await.unwrap().as_ref(),
        Some(&cursor)
    );
    assert!(matches!(
        store
            .mark_cluster_artifact_data_deleted(&proof, &cursor)
            .await,
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));
    let metadata = store
        .mark_cluster_artifact_data_deleted(&successor_lease.proof(), &cursor)
        .await
        .unwrap();
    assert_eq!(metadata.phase, ClusterArtifactCleanupPhase::DeleteMetadata);
}
