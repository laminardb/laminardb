use super::*;
use crate::checkpoint::PipelineIdentity;
use crate::cluster::control::topology::*;
use crate::cluster::control::{
    LocalProcessAuthorityIdentity, ProcessLeaseAuthority, ProcessLeaseStore,
};

pub(super) fn processes(authority: &LeaderLeaseStore) -> ProcessLeaseAuthority {
    ProcessLeaseAuthority::new(authority.store.clone(), Duration::from_secs(30)).unwrap()
}

pub(super) async fn fixture(
    authority: &LeaderLeaseStore,
) -> (
    LeaderLease,
    AssignmentSnapshotStore,
    TopologyAdmissionPlan,
    CatalogManifest,
) {
    let (lease, assignments, mut plan, target) = topology_admission::fixture(authority).await;
    let parent = authority
        .load_catalog_manifest(&plan.parent_manifest)
        .await
        .unwrap();
    let deployment = authority
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .topology_baseline
        .unwrap()
        .deployment_id;
    let mut objects = target
        .entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let preserved = index < parent.entries.len();
            ClusterTopologyObjectPlan {
                name: entry.canonical_name.clone(),
                kind: entry.kind,
                catalog_generation: entry.catalog_generation,
                transition: if preserved {
                    ClusterTopologyObjectTransition::Preserve
                } else {
                    ClusterTopologyObjectTransition::AddFutureOnly
                },
                initialization: if preserved {
                    TopologyInitialization::PreserveExactCut
                } else {
                    TopologyInitialization::ResolveSourcePositionsOnce
                },
                definition_sha256: format!("{:x}", Sha256::digest(entry.ddl.as_bytes())),
                compatibility_sha256: "1".repeat(64),
                dependencies: Vec::new(),
                schema_sha256: None,
                managed_state_contract: None,
            }
        })
        .collect::<Vec<_>>();
    objects.sort_by(|left, right| left.name.cmp(&right.name));
    let mut descriptor = ClusterTopologyValidation {
        validation_format_version: 3,
        scope: TopologyValidationScope::LocalCandidatePlan,
        deployment_id: deployment,
        parent_version: plan.expected_parent,
        target_version: plan.expected_parent.successor().unwrap(),
        parent_manifest: plan.parent_manifest.clone(),
        target_manifest: plan.target_manifest.clone(),
        parent_pipeline: PipelineIdentity::empty(),
        target_pipeline: PipelineIdentity {
            canonical_version: 7,
            sha256: "2".repeat(64),
        },
        environment_sha256: "3".repeat(64),
        compatibility_sha256: String::new(),
        statements: target.entries[parent.entries.len()..]
            .iter()
            .map(|entry| entry.ddl.clone())
            .collect(),
        objects,
        requires_processing_pause: true,
        required_before_activation: vec![
            TopologyActivationRequirement::ParticipantPlanAgreement,
            TopologyActivationRequirement::ReconciledCheckpointCut,
            TopologyActivationRequirement::DurableInitializationAndProgress,
            TopologyActivationRequirement::ObservedActorRetirement,
            TopologyActivationRequirement::AtomicTargetCommit,
            TopologyActivationRequirement::InstalledTargetRelease,
        ],
    };
    descriptor.compatibility_sha256 = descriptor.descriptor_digest().unwrap();
    plan.protocol_version = TOPOLOGY_PREPARATION_PROTOCOL_VERSION;
    plan.compatibility = Some(
        authority
            .stage_topology_compatibility(&descriptor)
            .await
            .unwrap(),
    );
    (lease, assignments, plan, target)
}

async fn identity(
    authority: &LeaderLeaseStore,
    participant: crate::checkpoint::CheckpointParticipant,
) -> LocalProcessAuthorityIdentity {
    let process =
        ProcessLeaseStore::new(authority.store.clone(), NodeId(participant.node_id), 30_000);
    let lease = match process
        .try_acquire(participant.boot_incarnation, 0)
        .await
        .unwrap()
    {
        crate::cluster::control::ProcessLeaseOutcome::Acquired(lease) => lease,
        other @ crate::cluster::control::ProcessLeaseOutcome::Held(_) => {
            panic!("test process cannot acquire lease: {other:?}")
        }
    };
    LocalProcessAuthorityIdentity {
        participant,
        process_term: lease.term,
    }
}

pub(super) async fn prepare_all(
    authority: &LeaderLeaseStore,
    assignments: &AssignmentSnapshotStore,
    plan: &TopologyAdmissionPlan,
    operation: &TopologyAdmissionStatus,
) -> TopologyAdmissionStatus {
    let descriptor = authority
        .load_topology_compatibility(plan.compatibility.as_ref().unwrap())
        .await
        .unwrap();
    let mut status = operation.clone();
    for participant in &plan.assignment.participants {
        let identity = identity(authority, *participant).await;
        status = authority
            .certify_topology_participant(
                assignments,
                &processes(authority),
                plan.operation_id,
                &operation.plan,
                identity,
                TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
                &descriptor,
            )
            .await
            .unwrap();
    }
    status
}

#[tokio::test]
async fn public_topology_plan_requires_complete_protocol_before_cut_and_gates_old_formats() {
    let authority = store(30_000);
    let (lease, assignments, mut plan, target) = fixture(&authority).await;
    plan.protocol_version = TOPOLOGY_SUBMISSION_PROTOCOL_VERSION;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    assert_eq!(
        authority.load_record().await.unwrap().unwrap().version,
        TOPOLOGY_SUBMISSION_RECORD_VERSION
    );
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    assert!(authority
        .begin_topology_checkpoint_cut(
            &lease.proof(),
            &assignments,
            &processes(&authority),
            admitted.operation_id,
            &admitted.plan,
            inventory.clone()
        )
        .await
        .is_err());
    let descriptor = authority
        .load_topology_compatibility(plan.compatibility.as_ref().unwrap())
        .await
        .unwrap();
    let process = identity(&authority, plan.assignment.participants[0]).await;
    assert!(authority
        .certify_topology_participant(
            &assignments,
            &processes(&authority),
            plan.operation_id,
            &admitted.plan,
            process,
            TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
            &descriptor
        )
        .await
        .is_err());
    let complete = authority
        .certify_topology_participant(
            &assignments,
            &processes(&authority),
            plan.operation_id,
            &admitted.plan,
            process,
            TOPOLOGY_SUBMISSION_PROTOCOL_VERSION,
            &descriptor,
        )
        .await
        .unwrap();
    assert!(complete
        .preparation
        .as_ref()
        .unwrap()
        .complete_sequence
        .is_some());
    let mut head = authority.load_record().await.unwrap().unwrap();
    head.version = TOPOLOGY_RECOVERY_RECORD_VERSION;
    assert!(head.validate().is_err());
    let cut = authority
        .begin_topology_checkpoint_cut(
            &lease.proof(),
            &assignments,
            &processes(&authority),
            admitted.operation_id,
            &admitted.plan,
            inventory,
        )
        .await
        .unwrap();
    assert_eq!(cut.phase, TopologyAdmissionPhase::Quiescing);
}

#[tokio::test]
async fn complete_roster_is_required_and_certificates_survive_reopen_and_abort() {
    let authority = store(30_000);
    let (lease, assignments, mut plan, target) = fixture(&authority).await;
    let prior = assignments.load().await.unwrap().unwrap();
    let mut participants = prior.participants.clone();
    participants.push(crate::checkpoint::CheckpointParticipant {
        node_id: 2,
        boot_incarnation: Uuid::from_u128(22),
    });
    let next = prior
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&[lease.owner.node, NodeId(2)]),
            participants,
        )
        .unwrap();
    assignments
        .save_if_version(&next, prior.version)
        .await
        .unwrap();
    plan.assignment = next.assignment_fence().unwrap();
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    assert!(matches!(
        authority
            .begin_topology_checkpoint_cut(
                &lease.proof(),
                &assignments,
                &processes(&authority),
                plan.operation_id,
                &admitted.plan,
                inventory.clone()
            )
            .await,
        Err(TopologyError::Conflict(_))
    ));
    let descriptor = authority
        .load_topology_compatibility(plan.compatibility.as_ref().unwrap())
        .await
        .unwrap();
    let first = identity(&authority, plan.assignment.participants[0]).await;
    let partial = authority
        .certify_topology_participant(
            &assignments,
            &processes(&authority),
            plan.operation_id,
            &admitted.plan,
            first,
            2,
            &descriptor,
        )
        .await
        .unwrap();
    assert_eq!(partial.phase, TopologyAdmissionPhase::Preparing);
    assert!(partial
        .preparation
        .as_ref()
        .unwrap()
        .complete_sequence
        .is_none());
    assert!(matches!(
        authority
            .begin_topology_checkpoint_cut(
                &lease.proof(),
                &assignments,
                &processes(&authority),
                plan.operation_id,
                &admitted.plan,
                inventory.clone()
            )
            .await,
        Err(TopologyError::Conflict(_))
    ));
    assert_eq!(
        authority
            .certify_topology_participant(
                &assignments,
                &processes(&authority),
                plan.operation_id,
                &admitted.plan,
                first,
                2,
                &descriptor
            )
            .await
            .unwrap(),
        partial
    );
    let complete = prepare_all(&authority, &assignments, &plan, &admitted).await;
    assert_eq!(complete.preparation.as_ref().unwrap().certificates.len(), 2);
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    assert_eq!(
        reopened
            .topology_operation_status(plan.operation_id)
            .await
            .unwrap(),
        Some(complete.clone())
    );
    let aborted = reopened
        .abort_topology_plan(&lease.proof(), plan.operation_id, &admitted.plan)
        .await
        .unwrap();
    assert_eq!(aborted.preparation, complete.preparation);
    assert!(matches!(
        reopened
            .certify_topology_participant(
                &assignments,
                &processes(&authority),
                plan.operation_id,
                &admitted.plan,
                first,
                2,
                &descriptor
            )
            .await,
        Err(TopologyError::Fenced)
    ));
    assert_eq!(
        reopened.load().await.unwrap().unwrap().catalog_manifest,
        Some(plan.parent_manifest.clone())
    );
}

#[tokio::test]
async fn legacy_reservations_are_readable_but_cannot_start_new_uncertified_cuts() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = topology_admission::fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    assert!(matches!(
        authority
            .begin_topology_checkpoint_cut(
                &lease.proof(),
                &assignments,
                &processes(&authority),
                plan.operation_id,
                &admitted.plan,
                inventory
            )
            .await,
        Err(TopologyError::Protocol(_))
    ));
    assert_eq!(
        authority
            .topology_operation_status(plan.operation_id)
            .await
            .unwrap(),
        Some(admitted)
    );
}

#[tokio::test]
async fn divergence_capabilities_and_stale_process_are_rejected_without_append() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let descriptor = authority
        .load_topology_compatibility(plan.compatibility.as_ref().unwrap())
        .await
        .unwrap();
    let current = identity(&authority, plan.assignment.participants[0]).await;
    let before = authority.load().await.unwrap().unwrap().seq;
    let mut changed = descriptor.clone();
    changed.environment_sha256 = "4".repeat(64);
    changed.compatibility_sha256 = changed.descriptor_digest().unwrap();
    assert!(matches!(
        authority
            .certify_topology_participant(
                &assignments,
                &processes(&authority),
                plan.operation_id,
                &admitted.plan,
                current,
                2,
                &changed
            )
            .await,
        Err(TopologyError::Conflict(_))
    ));
    assert!(matches!(
        authority
            .certify_topology_participant(
                &assignments,
                &processes(&authority),
                plan.operation_id,
                &admitted.plan,
                current,
                1,
                &descriptor
            )
            .await,
        Err(TopologyError::Protocol(_))
    ));
    let stale = LocalProcessAuthorityIdentity {
        process_term: current.process_term + 1,
        ..current
    };
    assert!(matches!(
        authority
            .certify_topology_participant(
                &assignments,
                &processes(&authority),
                plan.operation_id,
                &admitted.plan,
                stale,
                2,
                &descriptor
            )
            .await,
        Err(TopologyError::Fenced)
    ));
    assert_eq!(authority.load().await.unwrap().unwrap().seq, before);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn lost_certificate_response_and_cancellation_resolve_the_original_append() {
    let (raw, authority) = delayed_ambiguous_response_once_at(30_000, lease_path(5));
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let descriptor = authority
        .load_topology_compatibility(plan.compatibility.as_ref().unwrap())
        .await
        .unwrap();
    let process = identity(&authority, plan.assignment.participants[0]).await;
    let task_authority = authority.clone();
    let task_assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let task_plan = admitted.plan.clone();
    let task_descriptor = descriptor.clone();
    let operation = plan.operation_id;
    let task = tokio::spawn(async move {
        task_authority
            .certify_topology_participant(
                &task_assignments,
                &processes(&task_authority),
                operation,
                &task_plan,
                process,
                2,
                &task_descriptor,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let recovered = reopened
        .topology_operation_status(operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recovered.preparation.as_ref().unwrap().complete_sequence,
        Some(5)
    );
    assert_eq!(
        reopened
            .certify_topology_participant(
                &assignments,
                &processes(&authority),
                operation,
                &admitted.plan,
                process,
                2,
                &descriptor
            )
            .await
            .unwrap(),
        recovered
    );
}

#[tokio::test]
async fn descriptor_and_certificate_damage_fail_closed_and_pruning_retains_anchors() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let prepared = prepare_all(&authority, &assignments, &plan, &admitted).await;
    for step in 0..8 {
        authority
            .renew_exact(&lease.owner, lease.token, step)
            .await
            .unwrap();
    }
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    assert_eq!(
        authority
            .topology_operation_status(plan.operation_id)
            .await
            .unwrap(),
        Some(prepared.clone())
    );
    let sequence = prepared.preparation.as_ref().unwrap().certificates[0].authority_sequence;
    authority.store.delete(&lease_path(sequence)).await.unwrap();
    assert!(authority
        .topology_operation_status(plan.operation_id)
        .await
        .is_err());
}

#[tokio::test(start_paused = true)]
async fn certificate_deadline_before_create_keeps_original_planned_evidence() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(5));
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let descriptor = authority
        .load_topology_compatibility(plan.compatibility.as_ref().unwrap())
        .await
        .unwrap();
    let identity = identity(&authority, plan.assignment.participants[0]).await;
    let task_authority = Arc::clone(&authority);
    let expected_plan = admitted.plan.clone();
    let operation = plan.operation_id;
    let task = tokio::spawn(async move {
        task_authority
            .certify_topology_participant(
                &assignments,
                &processes(&task_authority),
                operation,
                &expected_plan,
                identity,
                2,
                &descriptor,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    tokio::time::advance(Duration::from_secs(16)).await;
    assert!(matches!(task.await.unwrap(), Err(TopologyError::Contended)));
    assert_eq!(
        authority
            .topology_operation_status(operation)
            .await
            .unwrap(),
        Some(admitted)
    );
}

#[tokio::test]
async fn leader_change_before_certificate_create_fences_the_delayed_old_process() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(5));
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let descriptor = authority
        .load_topology_compatibility(plan.compatibility.as_ref().unwrap())
        .await
        .unwrap();
    let identity = identity(&authority, plan.assignment.participants[0]).await;
    let task_authority = Arc::clone(&authority);
    let expected_plan = admitted.plan.clone();
    let operation = plan.operation_id;
    let task = tokio::spawn(async move {
        task_authority
            .certify_topology_participant(
                &assignments,
                &processes(&task_authority),
                operation,
                &expected_plan,
                identity,
                2,
                &descriptor,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    authority.begin_new_term(&lease.owner, 1).await.unwrap();
    raw.release.add_permits(1);
    assert!(matches!(task.await.unwrap(), Err(TopologyError::Fenced)));
    let status = authority
        .topology_operation_status(operation)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        status.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::LeaderChanged
        }
    ));
    assert_eq!(status.preparation, admitted.preparation);
}

#[tokio::test]
async fn descriptor_digest_bounds_missing_blob_and_rewritten_certificate_fail_closed() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let reference = plan.compatibility.as_ref().unwrap();
    let mut descriptor = authority
        .load_topology_compatibility(reference)
        .await
        .unwrap();
    descriptor.objects[0].definition_sha256 = "9".repeat(64);
    assert!(descriptor.encode_and_reference().is_err());
    descriptor.compatibility_sha256 = descriptor.descriptor_digest().unwrap();
    descriptor.objects[0].name = "x".repeat(1024 * 1024);
    assert!(authority
        .stage_topology_compatibility(&descriptor)
        .await
        .is_err());
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let prepared = prepare_all(&authority, &assignments, &plan, &admitted).await;
    let mut forgotten = prepared.clone();
    forgotten.preparation.as_mut().unwrap().certificates.clear();
    assert!(prepared
        .validate_successor(&forgotten, prepared.status_sequence + 1)
        .is_err());
    let mut replaced = prepared.clone();
    replaced.preparation.as_mut().unwrap().certificates[0].process_term += 1;
    assert!(prepared
        .validate_successor(&replaced, prepared.status_sequence + 1)
        .is_err());
    let mut downgraded = authority.load_record().await.unwrap().unwrap();
    downgraded.version = TOPOLOGY_CUT_RECORD_VERSION;
    assert!(downgraded.validate().is_err());
    let path = OsPath::from(format!(
        "control/topology-compatibility/v1/{}.json",
        reference.sha256
    ));
    authority.store.delete(&path).await.unwrap();
    assert!(authority
        .topology_operation_status(plan.operation_id)
        .await
        .is_err());
}
