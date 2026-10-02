use super::*;

fn topology_operation() -> crate::cluster::control::TopologyOperationId {
    Uuid::from_u128(42).try_into().unwrap()
}

pub(super) async fn topology_adoption_fixture(
    store: &LeaderLeaseStore,
) -> (LeaderLease, CatalogManifestRef, String) {
    let incumbent = owner(1, 1, 1);
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
        panic!("fresh authority must admit the leader");
    };
    let manifest = catalog("events");
    store.seal_catalog(&lease.proof(), &manifest).await.unwrap();
    let (_, reference) = manifest.encode_and_reference().unwrap();
    let deployment = CheckpointDecisionStore::new(store.store.clone())
        .load_or_create_deployment_id()
        .await
        .unwrap();
    (lease, reference, deployment)
}

#[tokio::test(start_paused = true)]
async fn topology_status_deadline_cancels_blocked_read_without_writes() {
    use crate::cluster::control::{TopologyCatalogState, TopologyError};
    let authority = store(30_000);
    let (_, reference, _) = topology_adoption_fixture(&authority).await;
    let before = authority.load().await.unwrap().unwrap();
    let (raw, blocked) =
        blocking_get_once_with_inner(30_000, authority.store.clone(), reference.object_path());
    let status_authority = blocked.clone();
    let read = tokio::spawn(async move { status_authority.topology_catalog_state().await });
    raw.entered.acquire().await.unwrap().forget();
    tokio::time::advance(Duration::from_secs(16)).await;
    assert!(matches!(
        read.await.unwrap(),
        Err(TopologyError::ReadTimedOut)
    ));
    assert!(raw.put_counts.lock().unwrap().is_empty());
    assert_eq!(authority.load().await.unwrap().unwrap(), before);
    assert_eq!(
        blocked.topology_catalog_state().await.unwrap(),
        TopologyCatalogState::LegacySealed {
            manifest: reference
        }
    );
}

#[tokio::test]
async fn topology_adoption_racing_renewal_preserves_winner_and_retries_exact_operation() {
    use crate::cluster::control::{TopologyAdoptionOutcome, TopologyCatalogState};
    let (raw, authority) = blocking_once_at(30_000, lease_path(3));
    let (lease, reference, deployment) = topology_adoption_fixture(&authority).await;
    let proof = lease.proof();
    let task_authority = authority.clone();
    let task = tokio::spawn(async move {
        task_authority
            .adopt_legacy_topology(&proof, topology_operation(), &reference, &deployment)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let LeaseOutcome::Acquired(renewed) = authority
        .renew_exact(&lease.owner, lease.token, 1)
        .await
        .unwrap()
    else {
        panic!("renewal must win the blocked append");
    };
    assert_eq!(renewed.seq, 3);
    raw.release.add_permits(1);
    let TopologyAdoptionOutcome::Created(baseline) = task.await.unwrap().unwrap() else {
        panic!("adoption must retry through the renewal");
    };
    assert_eq!(baseline.authority_sequence, 4);
    assert_eq!(
        authority.load().await.unwrap().unwrap().renewal_sequence,
        renewed.renewal_sequence
    );
    assert_eq!(
        authority.topology_catalog_state().await.unwrap(),
        TopologyCatalogState::Versioned {
            baseline,
            committed: None
        }
    );
}

#[tokio::test]
async fn topology_adoption_before_term_loss_is_fenced_without_upgrade() {
    use crate::cluster::control::{TopologyCatalogState, TopologyError};
    let (raw, authority) = blocking_once_at(30_000, lease_path(3));
    let (lease, reference, deployment) = topology_adoption_fixture(&authority).await;
    let task_authority = authority.clone();
    let proof = lease.proof();
    let task_reference = reference.clone();
    let task = tokio::spawn(async move {
        task_authority
            .adopt_legacy_topology(&proof, topology_operation(), &task_reference, &deployment)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let LeaseOutcome::Acquired(replacement) =
        authority.begin_new_term(&lease.owner, 1).await.unwrap()
    else {
        panic!("replacement candidacy must rotate this process's leader term");
    };
    assert!(replacement.token > lease.token);
    raw.release.add_permits(1);
    assert!(matches!(task.await.unwrap(), Err(TopologyError::Fenced)));
    assert_eq!(
        authority.topology_catalog_state().await.unwrap(),
        TopologyCatalogState::LegacySealed {
            manifest: reference
        }
    );
}

#[tokio::test]
async fn topology_adoption_resolves_lost_record_and_head_responses() {
    use crate::cluster::control::{TopologyAdoptionOutcome, TopologyCatalogState};
    for path in [lease_path(3), authority_head_path()] {
        let (raw, authority) = ambiguous_once_at(30_000, path);
        let (lease, reference, deployment) = topology_adoption_fixture(&authority).await;
        raw.did_return_ambiguous
            .store(false, std::sync::atomic::Ordering::Release);
        let outcome = authority
            .adopt_legacy_topology(
                &lease.proof(),
                topology_operation(),
                &reference,
                &deployment,
            )
            .await
            .unwrap();
        let (TopologyAdoptionOutcome::Created(baseline)
        | TopologyAdoptionOutcome::Existing(baseline)) = outcome;
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            authority.topology_catalog_state().await.unwrap(),
            TopologyCatalogState::Versioned {
                baseline,
                committed: None
            }
        );
        assert_eq!(authority.load().await.unwrap().unwrap().seq, 3);
        assert_eq!(raw.put_count(&lease_path(3), "create"), 1);
    }
}

#[tokio::test]
async fn topology_adoption_cancelled_after_durable_create_is_recoverable() {
    use crate::cluster::control::{TopologyAdoptionOutcome, TopologyCatalogState};
    let (raw, authority) = delayed_response_once_at_with_ambiguity(30_000, lease_path(3), false);
    let (lease, reference, deployment) = topology_adoption_fixture(&authority).await;
    let proof = lease.proof();
    let task_authority = authority.clone();
    let task_reference = reference.clone();
    let task_deployment = deployment.clone();
    let task_proof = proof.clone();
    let task = tokio::spawn(async move {
        task_authority
            .adopt_legacy_topology(
                &task_proof,
                topology_operation(),
                &task_reference,
                &task_deployment,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let restarted = LeaderLeaseStore::new(raw.clone(), 30_000);
    let TopologyCatalogState::Versioned {
        baseline,
        committed: None,
    } = restarted.topology_catalog_state().await.unwrap()
    else {
        panic!("durable append must be recovered independently of the cancelled request");
    };
    assert_eq!(
        restarted
            .adopt_legacy_topology(&proof, topology_operation(), &reference, &deployment)
            .await
            .unwrap(),
        TopologyAdoptionOutcome::Existing(baseline)
    );
    assert_eq!(raw.put_count(&lease_path(3), "create"), 1);
}

#[tokio::test]
async fn topology_adoption_cancelled_before_create_leaves_legacy_authority() {
    use crate::cluster::control::TopologyCatalogState;
    let (raw, authority) = blocking_once_at(30_000, lease_path(3));
    let (lease, reference, deployment) = topology_adoption_fixture(&authority).await;
    let task_authority = authority.clone();
    let task_reference = reference.clone();
    let task = tokio::spawn(async move {
        task_authority
            .adopt_legacy_topology(
                &lease.proof(),
                topology_operation(),
                &task_reference,
                &deployment,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), raw.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        authority.topology_catalog_state().await.unwrap(),
        TopologyCatalogState::LegacySealed {
            manifest: reference
        }
    );
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 2);
}

#[tokio::test]
async fn topology_adoption_preserves_unresolved_checkpoint_and_commit_chain() {
    use crate::cluster::control::{TopologyAdoptionOutcome, TopologyCatalogState};
    let authority = store(30_000);
    let (lease, reference, deployment) = topology_adoption_fixture(&authority).await;
    let proof = lease.proof();
    let fence = assignment_fence(&lease.owner);
    record_commit(&authority, &proof, &fence, 1, 1).await;
    let original_commit = authority
        .highest_cluster_committed_outcome()
        .await
        .unwrap()
        .unwrap();
    let pending = begin_checkpoint_artifacts(&authority, &proof, &fence, 2).await;
    let before = authority.load_record().await.unwrap().unwrap();
    let TopologyAdoptionOutcome::Created(baseline) = authority
        .adopt_legacy_topology(&proof, topology_operation(), &reference, &deployment)
        .await
        .unwrap()
    else {
        panic!("first adoption must create");
    };
    let after = authority.load_record().await.unwrap().unwrap();
    assert_eq!(after, {
        let mut expected = before.preserve_with_lease(after.lease.clone());
        expected.version = TOPOLOGY_AUTHORITY_RECORD_VERSION;
        expected.topology_baseline = Some(baseline.clone());
        expected
    });
    assert_eq!(
        authority.cluster_checkpoint_artifacts().await.unwrap(),
        Some(pending)
    );
    assert_eq!(
        authority.highest_cluster_committed_outcome().await.unwrap(),
        Some(original_commit)
    );
    record_commit(&authority, &proof, &fence, 2, 2).await;
    assert_eq!(
        authority
            .highest_cluster_committed_outcome()
            .await
            .unwrap()
            .unwrap()
            .epoch,
        2
    );
    assert_eq!(
        authority.topology_catalog_state().await.unwrap(),
        TopologyCatalogState::Versioned {
            baseline,
            committed: None
        }
    );
}

#[tokio::test]
async fn topology_adoption_record_is_retained_and_format_cannot_downgrade() {
    use crate::cluster::control::{TopologyAdoptionOutcome, TopologyCatalogState};
    let authority = store(30_000);
    let (lease, reference, deployment) = topology_adoption_fixture(&authority).await;
    let old_head = authority
        .load_published_authority_head()
        .await
        .unwrap()
        .unwrap();
    let mut frozen_old_candidate = old_head
        .record
        .preserve_with_lease(old_head.record.lease.clone());
    frozen_old_candidate.lease.seq += 1;
    let TopologyAdoptionOutcome::Created(baseline) = authority
        .adopt_legacy_topology(
            &lease.proof(),
            topology_operation(),
            &reference,
            &deployment,
        )
        .await
        .unwrap()
    else {
        panic!("first adoption must create");
    };
    // An old writer frozen before the upgrade cannot win the same create-only successor.
    assert!(matches!(
        authority
            .create_authority_record(Some(&old_head), &frozen_old_candidate)
            .await
            .unwrap(),
        AuthorityCreateOutcome::Contended(_)
    ));
    for now in 1..8 {
        authority
            .renew_exact(&lease.owner, lease.token, now)
            .await
            .unwrap();
    }
    let current = authority
        .load_published_authority_head()
        .await
        .unwrap()
        .unwrap();
    let mut downgrade = current
        .record
        .preserve_with_lease(current.record.lease.clone());
    downgrade.lease.seq += 1;
    downgrade.version = AUTHORITY_RECORD_VERSION;
    downgrade.topology_baseline = None;
    assert!(matches!(
        authority
            .create_authority_record(Some(&current), &downgrade)
            .await,
        Err(LeaseError::Invalid(_))
    ));
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    assert!(
        read_authority_record(authority.store.as_ref(), baseline.authority_sequence)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        authority.topology_catalog_state().await.unwrap(),
        TopologyCatalogState::Versioned {
            baseline,
            committed: None
        }
    );
}
