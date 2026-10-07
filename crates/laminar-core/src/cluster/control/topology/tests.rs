use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::{ObjectStoreExt, PutPayload};

use super::*;
use crate::checkpoint_decision::CheckpointDecisionStore;
use crate::cluster::control::{
    CatalogManifest, CatalogManifestEntry, CatalogManifestStore, CatalogObjectKind,
    LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome,
};
use crate::cluster::discovery::NodeId;

struct Fixture {
    backing: Arc<InMemory>,
    authority: Arc<LeaderLeaseStore>,
    catalog: CatalogManifestStore,
    proof: crate::checkpoint::LeaderProof,
    manifest: CatalogManifestRef,
    deployment: String,
}

fn operation(value: u128) -> TopologyOperationId {
    Uuid::from_u128(value).try_into().unwrap()
}

async fn fixture() -> Fixture {
    let backing = Arc::new(InMemory::new());
    let authority = Arc::new(LeaderLeaseStore::new(backing.clone(), 30_000));
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: Uuid::from_u128(1),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("fresh authority must admit the leader");
    };
    let catalog = CatalogManifestStore::new(authority.clone());
    let inventory = CatalogManifest::new(vec![CatalogManifestEntry {
        schema_binding: None,
        canonical_name: "events".into(),
        kind: CatalogObjectKind::Source,
        catalog_generation: 1,
        ddl: "CREATE SOURCE events (k BIGINT)".into(),
    }])
    .unwrap();
    catalog.seal(&inventory, &lease.proof()).await.unwrap();
    let (_, manifest) = inventory.encode_and_reference().unwrap();
    let deployment = CheckpointDecisionStore::new(backing.clone())
        .load_or_create_deployment_id()
        .await
        .unwrap();
    Fixture {
        backing,
        authority,
        catalog,
        proof: lease.proof(),
        manifest,
        deployment,
    }
}

async fn adopt(f: &Fixture, id: u128) -> TopologyAdoptionOutcome {
    f.catalog
        .adopt_legacy_topology(&f.proof, operation(id), &f.manifest, &f.deployment)
        .await
        .unwrap()
}

#[test]
fn identifiers_reject_zero_and_overflow_on_decode_and_allocation() {
    assert!(TopologyVersion::new(0).is_err());
    assert!(serde_json::from_str::<TopologyVersion>("0").is_err());
    assert_eq!(
        TopologyVersion::LEGACY_BASELINE.successor().unwrap().get(),
        2
    );
    assert!(TopologyVersion::new(u64::MAX).unwrap().successor().is_err());
    assert!(TopologyOperationId::try_from(Uuid::nil()).is_err());
    assert!(serde_json::from_str::<TopologyOperationId>(
        "\"00000000-0000-0000-0000-000000000000\""
    )
    .is_err());
    let id = operation(2);
    assert_eq!(
        serde_json::from_str::<TopologyOperationId>(&serde_json::to_string(&id).unwrap()).unwrap(),
        id
    );
}

#[tokio::test]
async fn missing_metadata_is_uninitialized_or_legacy_never_a_version() {
    let empty = Arc::new(InMemory::new());
    let authority = LeaderLeaseStore::new(empty.clone(), 30_000);
    assert_eq!(
        authority.topology_catalog_state().await.unwrap(),
        TopologyCatalogState::Uninitialized
    );
    assert!(CheckpointDecisionStore::new(empty)
        .load_deployment_id()
        .await
        .unwrap()
        .is_none());

    let f = fixture().await;
    assert_eq!(
        f.catalog.topology_state().await.unwrap(),
        TopologyCatalogState::LegacySealed {
            manifest: f.manifest
        }
    );
}

#[tokio::test]
async fn adoption_preserves_exact_legacy_bytes_and_checkpoint_allocator() {
    let f = fixture().await;
    let original_blob = f.backing.get(&f.manifest.object_path()).await.unwrap();
    let original_bytes = original_blob.bytes().await.unwrap();
    let original_lease = f.authority.load().await.unwrap().unwrap();
    let original_path = object_store::path::Path::from(format!(
        "control/leader-lease/v{:016}.json",
        original_lease.seq
    ));
    let original_authority = f
        .backing
        .get(&original_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let decisions = CheckpointDecisionStore::new(f.backing.clone());
    let first_id = decisions
        .allocate_checkpoint_id_at_least(123)
        .await
        .unwrap();

    let TopologyAdoptionOutcome::Created(baseline) = adopt(&f, 2).await else {
        panic!("first adoption must create the baseline");
    };
    assert_eq!(baseline.manifest, f.manifest);
    assert_eq!(baseline.manifest.version, 1); // Serialization version is preserved.
    assert_eq!(baseline.topology_version, TopologyVersion::LEGACY_BASELINE);
    assert_eq!(baseline.authority_sequence, original_lease.seq + 1);
    assert_eq!(
        f.backing
            .get(&f.manifest.object_path())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        original_bytes
    );
    assert_eq!(
        f.backing
            .get(&original_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        original_authority
    );
    assert_eq!(
        decisions.load_deployment_id().await.unwrap(),
        Some(f.deployment)
    );
    assert!(decisions.allocate_checkpoint_id().await.unwrap() > first_id);
}

#[tokio::test]
async fn concurrent_adoptions_and_identical_retry_return_original_winner() {
    let f = fixture().await;
    let (left, right) = tokio::join!(
        f.catalog
            .adopt_legacy_topology(&f.proof, operation(2), &f.manifest, &f.deployment),
        f.catalog
            .adopt_legacy_topology(&f.proof, operation(3), &f.manifest, &f.deployment)
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, TopologyAdoptionOutcome::Created(_)))
            .count(),
        1
    );
    let TopologyCatalogState::Versioned {
        baseline,
        committed: None,
    } = f.catalog.topology_state().await.unwrap()
    else {
        panic!("adoption must be visible");
    };
    for outcome in outcomes {
        let (TopologyAdoptionOutcome::Created(value) | TopologyAdoptionOutcome::Existing(value)) =
            outcome;
        assert_eq!(value, baseline);
    }
    let before = f.authority.load().await.unwrap().unwrap();
    assert_eq!(
        adopt(&f, baseline.operation_id.get().as_u128()).await,
        TopologyAdoptionOutcome::Existing(baseline)
    );
    assert_eq!(f.authority.load().await.unwrap().unwrap(), before);
}

#[tokio::test]
async fn stale_proof_or_changed_parent_deployment_fails_without_append() {
    let f = fixture().await;
    let before = f.authority.load().await.unwrap().unwrap();
    let mut stale = f.proof.clone();
    stale.fencing_token += 1;
    assert!(matches!(
        f.catalog
            .adopt_legacy_topology(&stale, operation(2), &f.manifest, &f.deployment)
            .await,
        Err(TopologyError::Fenced)
    ));
    let mut wrong_manifest = f.manifest.clone();
    wrong_manifest.sha256 = "0".repeat(64);
    assert!(matches!(
        f.catalog
            .adopt_legacy_topology(&f.proof, operation(2), &wrong_manifest, &f.deployment)
            .await,
        Err(TopologyError::Conflict(_))
    ));
    assert!(matches!(
        f.catalog
            .adopt_legacy_topology(
                &f.proof,
                operation(2),
                &f.manifest,
                &Uuid::from_u128(3).to_string()
            )
            .await,
        Err(TopologyError::Conflict(_))
    ));
    assert_eq!(f.authority.load().await.unwrap().unwrap(), before);
}

#[tokio::test]
async fn adoption_survives_renewal_and_fresh_store_reconstruction() {
    let f = fixture().await;
    let TopologyAdoptionOutcome::Created(baseline) = adopt(&f, 2).await else {
        panic!("first adoption must create");
    };
    let owner = f.authority.load().await.unwrap().unwrap().owner;
    let LeaseOutcome::Acquired(renewed) = f
        .authority
        .renew_exact(&owner, f.proof.fencing_token, 1)
        .await
        .unwrap()
    else {
        panic!("current proof must renew");
    };
    assert_eq!(renewed.catalog_manifest, Some(f.manifest));
    let restarted = CatalogManifestStore::new(Arc::new(LeaderLeaseStore::new(f.backing, 30_000)));
    assert_eq!(
        restarted.topology_state().await.unwrap(),
        TopologyCatalogState::Versioned {
            baseline,
            committed: None
        }
    );
    assert_eq!(
        restarted.load().await.unwrap().unwrap().entries[0].canonical_name,
        "events"
    );
}

#[tokio::test]
async fn damaged_adoption_anchor_or_deployment_fails_closed_without_identity_creation() {
    for damage in 0..4 {
        let f = fixture().await;
        let TopologyAdoptionOutcome::Created(baseline) = adopt(&f, 2).await else {
            panic!("first adoption must create");
        };
        let lease = f.authority.load().await.unwrap().unwrap();
        f.authority
            .renew_exact(&lease.owner, lease.token, 1)
            .await
            .unwrap();
        let deployment_path = object_store::path::Path::from("checkpoint-deployment/identity.json");
        let anchor_path = object_store::path::Path::from(format!(
            "control/leader-lease/v{:016}.json",
            baseline.authority_sequence
        ));
        let damaged_path = if damage < 2 {
            deployment_path
        } else {
            anchor_path
        };
        match damage {
            0 | 2 => f.backing.delete(&damaged_path).await.unwrap(),
            _ => {
                f.backing
                    .put(&damaged_path, PutPayload::from(vec![b'x'; 100]))
                    .await
                    .unwrap();
            }
        }
        let before = f.authority.load().await.unwrap().unwrap();
        let restarted =
            CatalogManifestStore::new(Arc::new(LeaderLeaseStore::new(f.backing.clone(), 30_000)));
        assert!(restarted.topology_state().await.is_err());
        assert!(restarted.load().await.is_err());
        assert_eq!(f.authority.load().await.unwrap().unwrap(), before);
        if damage == 0 || damage == 2 {
            assert!(matches!(
                f.backing.get(&damaged_path).await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
    }
}

#[tokio::test]
async fn malformed_missing_or_oversized_manifest_never_adopts() {
    for damaged in [
        None,
        Some(vec![b'x'; 8 * 1024 * 1024 + 1]),
        Some(vec![b'x'; 100]),
    ] {
        let f = fixture().await;
        let before = f.authority.load().await.unwrap().unwrap();
        match damaged {
            None => f.backing.delete(&f.manifest.object_path()).await.unwrap(),
            Some(bytes) => {
                f.backing
                    .put(&f.manifest.object_path(), PutPayload::from(bytes))
                    .await
                    .unwrap();
            }
        }
        assert!(f
            .catalog
            .adopt_legacy_topology(&f.proof, operation(2), &f.manifest, &f.deployment)
            .await
            .is_err());
        assert_eq!(f.authority.load().await.unwrap().unwrap(), before);
    }
}

#[tokio::test]
async fn unknown_protocol_or_nonbaseline_version_is_rejected() {
    let f = fixture().await;
    let TopologyAdoptionOutcome::Created(mut baseline) = adopt(&f, 2).await else {
        panic!("first adoption must create");
    };
    baseline.protocol_version += 1;
    assert!(matches!(
        baseline.validate(),
        Err(TopologyError::Protocol(_))
    ));
    baseline.protocol_version = TOPOLOGY_PROTOCOL_VERSION;
    baseline.topology_version = TopologyVersion::new(2).unwrap();
    assert!(matches!(
        baseline.validate(),
        Err(TopologyError::Invalid(_))
    ));
}
