use super::*;
use crate::cluster::control::topology::LegacyTopologyBaseline;
use crate::cluster::control::{TopologyCatalogState, TopologyError};

async fn acquire(store: &LeaderLeaseStore) -> LeaderLease {
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(&owner(1, 1, 1), 0).await.unwrap()
    else {
        panic!("fresh authority must admit the leader");
    };
    lease
}

async fn sealed_baseline(store: &LeaderLeaseStore) -> LegacyTopologyBaseline {
    let TopologyCatalogState::Versioned {
        baseline,
        committed: None,
    } = store.topology_catalog_state().await.unwrap()
    else {
        panic!("a sealed catalog must be topology one");
    };
    baseline
}

pub(super) async fn sealed_topology_fixture(
    store: &LeaderLeaseStore,
) -> (LeaderLease, LegacyTopologyBaseline) {
    let lease = acquire(store).await;
    store
        .seal_catalog(&lease.proof(), &catalog("events"))
        .await
        .unwrap();
    (lease, sealed_baseline(store).await)
}

#[tokio::test(start_paused = true)]
async fn topology_status_deadline_cancels_blocked_read_without_writes() {
    let authority = store(30_000);
    let (_, baseline) = sealed_topology_fixture(&authority).await;
    let before = authority.load().await.unwrap().unwrap();
    let (raw, blocked) = blocking_get_once_with_inner(
        30_000,
        authority.store.clone(),
        baseline.manifest.object_path(),
    );
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
    assert_eq!(sealed_baseline(&blocked).await, baseline);
}

#[tokio::test]
async fn catalog_seal_racing_renewal_preserves_winner_and_binds_its_own_sequence() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(2));
    let lease = acquire(&authority).await;
    let proof = lease.proof();
    let task_authority = authority.clone();
    let task = tokio::spawn(async move {
        task_authority
            .seal_catalog(&proof, &catalog("events"))
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
    assert_eq!(renewed.seq, 2);
    raw.release.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap(), CatalogSealOutcome::Created);
    let baseline = sealed_baseline(&authority).await;
    assert_eq!(baseline.authority_sequence, 3);
    assert_eq!(
        authority.load().await.unwrap().unwrap().renewal_sequence,
        renewed.renewal_sequence
    );
}

#[tokio::test]
async fn catalog_seal_before_term_loss_is_fenced_without_baseline() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(2));
    let lease = acquire(&authority).await;
    let task_authority = authority.clone();
    let proof = lease.proof();
    let task = tokio::spawn(async move {
        task_authority
            .seal_catalog(&proof, &catalog("events"))
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
    assert!(matches!(
        task.await.unwrap(),
        Err(CatalogManifestError::Fenced)
    ));
    assert_eq!(
        authority.topology_catalog_state().await.unwrap(),
        TopologyCatalogState::Uninitialized
    );
}

#[tokio::test]
async fn catalog_seal_resolves_lost_record_and_head_responses() {
    for path in [lease_path(2), authority_head_path()] {
        let (raw, authority) = ambiguous_once_at(30_000, path);
        let lease = acquire(&authority).await;
        raw.did_return_ambiguous
            .store(false, std::sync::atomic::Ordering::Release);
        authority
            .seal_catalog(&lease.proof(), &catalog("events"))
            .await
            .unwrap();
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(sealed_baseline(&authority).await.authority_sequence, 2);
        assert_eq!(authority.load().await.unwrap().unwrap().seq, 2);
        assert_eq!(raw.put_count(&lease_path(2), "create"), 1);
    }
}

#[tokio::test]
async fn catalog_seal_cancelled_after_durable_create_is_recoverable() {
    let (raw, authority) = delayed_response_once_at_with_ambiguity(30_000, lease_path(2), false);
    let lease = acquire(&authority).await;
    let proof = lease.proof();
    let task_authority = authority.clone();
    let task_proof = proof.clone();
    let task = tokio::spawn(async move {
        task_authority
            .seal_catalog(&task_proof, &catalog("events"))
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
    let baseline = sealed_baseline(&restarted).await;
    assert_eq!(baseline.authority_sequence, 2);
    assert_eq!(
        restarted
            .seal_catalog(&proof, &catalog("events"))
            .await
            .unwrap(),
        CatalogSealOutcome::ExistingIdentical
    );
    assert_eq!(sealed_baseline(&restarted).await, baseline);
    assert_eq!(raw.put_count(&lease_path(2), "create"), 1);
}

#[tokio::test]
async fn catalog_seal_cancelled_before_create_leaves_authority_unsealed() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(2));
    let lease = acquire(&authority).await;
    let task_authority = authority.clone();
    let task = tokio::spawn(async move {
        task_authority
            .seal_catalog(&lease.proof(), &catalog("events"))
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
        TopologyCatalogState::Uninitialized
    );
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 1);
}

#[tokio::test]
async fn unsealed_to_sealed_append_requires_baseline_but_history_stays_readable() {
    let authority = store(30_000);
    let lease = acquire(&authority).await;
    let manifest = catalog("events");
    let (encoded, reference) = manifest.encode_and_reference().unwrap();
    authority
        .ensure_catalog_manifest_blob(&encoded, &reference)
        .await
        .unwrap();
    let head = authority
        .load_published_authority_head()
        .await
        .unwrap()
        .unwrap();
    let mut sealed_lease = lease.clone();
    sealed_lease.seq += 1;
    sealed_lease.catalog_manifest = Some(reference.clone());
    let unversioned = head.record.preserve_with_lease(sealed_lease);
    assert!(matches!(
        authority
            .create_authority_record(Some(&head), &unversioned)
            .await,
        Err(LeaseError::Invalid(_))
    ));
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 1);

    // Records sealed before topology versioning remain valid history, but cannot be given a
    // baseline afterwards and are not a startable catalog.
    unversioned.validate().unwrap();
    let mut late_baseline = unversioned.preserve_with_lease(LeaderLease {
        seq: unversioned.lease.seq + 1,
        ..unversioned.lease.clone()
    });
    late_baseline.version = TOPOLOGY_AUTHORITY_RECORD_VERSION;
    late_baseline.topology_baseline = Some(LegacyTopologyBaseline {
        protocol_version: crate::cluster::control::TOPOLOGY_PROTOCOL_VERSION,
        topology_version: crate::cluster::control::TopologyVersion::LEGACY_BASELINE,
        manifest: reference.clone(),
        deployment_id: Uuid::from_u128(7).to_string(),
        operation_id: Uuid::from_u128(8).try_into().unwrap(),
        authority_sequence: unversioned.lease.seq + 1,
    });
    late_baseline.validate().unwrap();
    assert!(matches!(
        unversioned.validate_topology_successor(&late_baseline),
        Err(LeaseError::Invalid(_))
    ));
    let (raw, legacy) = blocking_once_at(30_000, OsPath::from("control/never-block"));
    let mut record = bare_authority_record(&lease.owner, 1);
    record.lease.catalog_manifest = Some(reference);
    seed_authority_record(&raw, &record).await;
    seed_authority_head(&raw, 1).await;
    assert_eq!(legacy.load().await.unwrap().unwrap(), record.lease);
    assert!(matches!(
        legacy.topology_catalog_state().await,
        Err(TopologyError::Invalid(_))
    ));
}

#[tokio::test]
async fn topology_baseline_record_is_retained_and_format_cannot_downgrade() {
    let authority = store(30_000);
    let lease = acquire(&authority).await;
    let old_head = authority
        .load_published_authority_head()
        .await
        .unwrap()
        .unwrap();
    let mut frozen_old_candidate = old_head
        .record
        .preserve_with_lease(old_head.record.lease.clone());
    frozen_old_candidate.lease.seq += 1;
    assert_eq!(
        authority
            .seal_catalog(&lease.proof(), &catalog("events"))
            .await
            .unwrap(),
        CatalogSealOutcome::Created
    );
    let baseline = sealed_baseline(&authority).await;
    // An unsealed writer frozen before the seal cannot win the same create-only successor.
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
    assert_eq!(sealed_baseline(&authority).await, baseline);
}
