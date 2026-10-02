use super::*;
use crate::cluster::control::LocalProcessAuthorityIdentity;

async fn restore_input(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    participant: usize,
) -> Result<crate::cluster::control::TopologyRestoreInput, TopologyError> {
    let status = authority
        .topology_operation_status(fixture.operation.operation_id)
        .await?
        .unwrap();
    let certificate = &status.preparation.as_ref().unwrap().certificates[participant];
    authority
        .topology_restore_input(
            &fixture.assignments,
            &topology_preparation::processes(authority),
            fixture.operation.operation_id,
            LocalProcessAuthorityIdentity {
                participant: certificate.participant,
                process_term: certificate.process_term,
            },
        )
        .await
}

#[tokio::test]
async fn topology_restore_requires_published_root_and_exact_complete_cut() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    assert!(restore_input(&authority, &fixture, 0).await.is_err());
    let staged = fixture.stage(&authority).await.unwrap();
    let before = authority.load_record().await.unwrap();
    for participant in 0..2 {
        let input = restore_input(&authority, &fixture, participant)
            .await
            .unwrap();
        assert_eq!(input.operation(), &staged);
        assert_eq!(input.checkpoint(), &fixture.index);
        assert_eq!(input.descriptor(), &fixture.descriptor);
        assert_eq!(input.owned_vnodes(), &[u32::try_from(participant).unwrap()]);
        assert_eq!(
            input.checkpoint().pipeline_identity,
            fixture.descriptor.parent_pipeline
        );
        assert_ne!(
            input.checkpoint().pipeline_identity,
            fixture.descriptor.target_pipeline
        );
        input
            .root()
            .validate_restore_cut(
                input.operation(),
                input.descriptor(),
                input.checkpoint(),
                &fixture.manifests,
            )
            .unwrap();
    }
    assert_eq!(authority.load_record().await.unwrap(), before);
    assert_eq!(
        authority
            .load()
            .await
            .unwrap()
            .unwrap()
            .catalog_manifest
            .unwrap(),
        fixture.descriptor.parent_manifest
    );
}

#[tokio::test]
async fn topology_restore_rejects_unknown_incarnation_and_term() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    fixture.stage(&authority).await.unwrap();
    let input = restore_input(&authority, &fixture, 0).await.unwrap();
    let mut identities = vec![input.process(); 3];
    identities[0].participant.node_id = 99;
    identities[1].participant.boot_incarnation = Uuid::from_u128(999);
    identities[2].process_term += 1;
    for identity in identities {
        assert!(matches!(
            authority
                .topology_restore_input(
                    &fixture.assignments,
                    &topology_preparation::processes(&authority),
                    fixture.operation.operation_id,
                    identity
                )
                .await,
            Err(TopologyError::Fenced)
        ));
    }
}

#[tokio::test]
async fn topology_restore_rejects_abort_and_replacement_leader() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    fixture.stage(&authority).await.unwrap();
    let input = restore_input(&authority, &fixture, 0).await.unwrap();
    assert!(matches!(
        authority
            .begin_new_term(&fixture.lease.owner, 1)
            .await
            .unwrap(),
        LeaseOutcome::Acquired(_)
    ));
    assert!(authority
        .topology_restore_input(
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            fixture.operation.operation_id,
            input.process()
        )
        .await
        .is_err());
    let status = authority
        .topology_operation_status(fixture.operation.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        status.phase,
        TopologyAdmissionPhase::Aborted { .. }
    ));
    assert_eq!(status.migration_root, input.operation().migration_root);
    assert_eq!(status.cut, input.operation().cut);
}

#[tokio::test]
async fn topology_restore_recomputes_every_state_and_subscription_requirement() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    fixture.stage(&authority).await.unwrap();
    let input = restore_input(&authority, &fixture, 0).await.unwrap();
    let mut roots = vec![input.root().clone(); 3];
    roots[0].subscriptions[0].frontiers[0].through_sequence = PartitionSequence::FIRST;
    roots[1].preserved_objects[0].compatibility_sha256 = digest(9);
    roots[2].subscriptions.clear();
    for root in roots {
        assert!(root
            .validate_restore_cut(
                input.operation(),
                input.descriptor(),
                input.checkpoint(),
                &fixture.manifests
            )
            .is_err());
    }
}

#[tokio::test]
async fn topology_restore_reads_sealed_new_source_without_reinitialization() {
    let authority = store(30_000);
    let fixture = fixture_with_sources(&authority, true).await;
    let initialized = source_initialization::position(&fixture.descriptor, 91);
    authority
        .stage_topology_migration_root_with_initialization(
            &fixture.lease.proof(),
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            &fixture.store,
            fixture.operation.operation_id,
            &fixture.operation.plan,
            |_, _| async { Ok(vec![initialized.clone()]) },
        )
        .await
        .unwrap();
    for participant in 0..2 {
        let input = restore_input(&authority, &fixture, participant)
            .await
            .unwrap();
        assert_eq!(
            input.root().source_initializations,
            vec![initialized.clone()]
        );
        assert_eq!(
            input.root().source_initializations[0]
                .checkpoint
                .source_assignment_version,
            None
        );
    }
}
