use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::{ObjectStoreExt, PutPayload};

use super::*;
use crate::checkpoint_decision::CheckpointDecisionStore;
use crate::cluster::control::{
    CatalogManifest, CatalogManifestEntry, CatalogManifestStore, CatalogObjectKind,
    CatalogSealOutcome, LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome,
};
use crate::cluster::discovery::NodeId;

struct Fixture {
    backing: Arc<InMemory>,
    authority: Arc<LeaderLeaseStore>,
    catalog: CatalogManifestStore,
    proof: crate::checkpoint::LeaderProof,
    baseline: TopologyBaseline,
}

fn operation(value: u128) -> TopologyOperationId {
    Uuid::from_u128(value).try_into().unwrap()
}

fn inventory(name: &str) -> CatalogManifest {
    CatalogManifest::new(vec![CatalogManifestEntry {
        schema_binding: None,
        canonical_name: name.into(),
        kind: CatalogObjectKind::Source,
        catalog_generation: 1,
        ddl: format!("CREATE SOURCE {name} (k BIGINT)"),
    }])
    .unwrap()
}

async fn unsealed() -> (
    Arc<InMemory>,
    Arc<LeaderLeaseStore>,
    crate::checkpoint::LeaderProof,
) {
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
    (backing, authority, lease.proof())
}

async fn fixture() -> Fixture {
    let (backing, authority, proof) = unsealed().await;
    let catalog = CatalogManifestStore::new(authority.clone());
    assert_eq!(
        catalog.seal(&inventory("events"), &proof).await.unwrap(),
        CatalogSealOutcome::Created
    );
    let TopologyCatalogState::Versioned {
        baseline,
        committed: None,
    } = catalog.topology_state().await.unwrap()
    else {
        panic!("the seal must write topology one");
    };
    Fixture {
        backing,
        authority,
        catalog,
        proof,
        baseline,
    }
}

#[test]
fn identifiers_reject_zero_and_overflow_on_decode_and_allocation() {
    assert!(TopologyVersion::new(0).is_err());
    assert!(serde_json::from_str::<TopologyVersion>("0").is_err());
    assert_eq!(TopologyVersion::BASELINE.successor().unwrap().get(), 2);
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
async fn fresh_namespace_is_uninitialized_until_the_seal_writes_topology_one() {
    let (backing, authority, proof) = unsealed().await;
    let catalog = CatalogManifestStore::new(authority.clone());
    assert_eq!(
        catalog.topology_state().await.unwrap(),
        TopologyCatalogState::Uninitialized
    );
    let decisions = CheckpointDecisionStore::new(backing.clone());
    assert!(decisions.load_deployment_id().await.unwrap().is_none());
    let mut stale = proof.clone();
    stale.fencing_token += 1;
    assert!(matches!(
        catalog.seal(&inventory("events"), &stale).await,
        Err(CatalogManifestError::Fenced)
    ));
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 1);
    assert!(decisions.load_deployment_id().await.unwrap().is_none());

    catalog.seal(&inventory("events"), &proof).await.unwrap();
    let state = catalog.topology_state().await.unwrap();
    assert_eq!(state.committed_version(), Some(TopologyVersion::BASELINE));
    let TopologyCatalogState::Versioned { baseline, .. } = state else {
        panic!("the seal must write topology one");
    };
    let (_, reference) = inventory("events").encode_and_reference().unwrap();
    assert_eq!(baseline.manifest, reference);
    assert_eq!(baseline.manifest.version, 1);
    assert_eq!(baseline.protocol_version, TOPOLOGY_PROTOCOL_VERSION);
    assert_eq!(
        baseline.authority_sequence,
        authority.load().await.unwrap().unwrap().seq
    );
    assert_eq!(
        decisions.load_deployment_id().await.unwrap(),
        Some(baseline.deployment_id)
    );
    decisions.allocate_checkpoint_id().await.unwrap();
}

#[tokio::test]
async fn identical_reseal_never_appends_and_a_different_inventory_conflicts() {
    let f = fixture().await;
    let before = f.authority.load().await.unwrap().unwrap();
    assert_eq!(
        f.catalog
            .seal(&inventory("events"), &f.proof)
            .await
            .unwrap(),
        CatalogSealOutcome::ExistingIdentical
    );
    assert!(matches!(
        f.catalog.seal(&inventory("other"), &f.proof).await,
        Err(CatalogManifestError::Conflict)
    ));
    assert_eq!(f.authority.load().await.unwrap().unwrap(), before);
    assert_eq!(
        f.catalog.topology_state().await.unwrap(),
        TopologyCatalogState::Versioned {
            baseline: f.baseline,
            committed: None
        }
    );
}

#[tokio::test]
async fn baseline_survives_renewal_and_fresh_store_reconstruction() {
    let f = fixture().await;
    let owner = f.authority.load().await.unwrap().unwrap().owner;
    let LeaseOutcome::Acquired(renewed) = f
        .authority
        .renew_exact(&owner, f.proof.fencing_token, 1)
        .await
        .unwrap()
    else {
        panic!("current proof must renew");
    };
    assert_eq!(renewed.catalog_manifest, Some(f.baseline.manifest.clone()));
    let restarted = CatalogManifestStore::new(Arc::new(LeaderLeaseStore::new(f.backing, 30_000)));
    assert_eq!(
        restarted.topology_state().await.unwrap(),
        TopologyCatalogState::Versioned {
            baseline: f.baseline,
            committed: None
        }
    );
    assert_eq!(
        restarted.load().await.unwrap().unwrap().entries[0].canonical_name,
        "events"
    );
}

#[tokio::test]
async fn damaged_baseline_anchor_or_deployment_fails_closed_without_identity_creation() {
    for damage in 0..4 {
        let f = fixture().await;
        let lease = f.authority.load().await.unwrap().unwrap();
        f.authority
            .renew_exact(&lease.owner, lease.token, 1)
            .await
            .unwrap();
        let deployment_path = object_store::path::Path::from("checkpoint-deployment/identity.json");
        let anchor_path = object_store::path::Path::from(format!(
            "control/leader-lease/v{:016}.json",
            f.baseline.authority_sequence
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
async fn malformed_missing_or_oversized_manifest_fails_closed() {
    let (backing, authority, proof) = unsealed().await;
    let (_, reference) = inventory("events").encode_and_reference().unwrap();
    backing
        .put(&reference.object_path(), PutPayload::from(vec![b'x'; 100]))
        .await
        .unwrap();
    assert!(CatalogManifestStore::new(authority.clone())
        .seal(&inventory("events"), &proof)
        .await
        .is_err());
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 1);
    assert!(CheckpointDecisionStore::new(backing)
        .load_deployment_id()
        .await
        .unwrap()
        .is_none());

    for damaged in [
        None,
        Some(vec![b'x'; 8 * 1024 * 1024 + 1]),
        Some(vec![b'x'; 100]),
    ] {
        let f = fixture().await;
        let before = f.authority.load().await.unwrap().unwrap();
        let path = f.baseline.manifest.object_path();
        match damaged {
            None => f.backing.delete(&path).await.unwrap(),
            Some(bytes) => {
                f.backing.put(&path, PutPayload::from(bytes)).await.unwrap();
            }
        }
        assert!(f.catalog.topology_state().await.is_err());
        assert!(f
            .catalog
            .seal(&inventory("events"), &f.proof)
            .await
            .is_err());
        assert_eq!(f.authority.load().await.unwrap().unwrap(), before);
    }
}

#[tokio::test]
async fn unknown_protocol_or_nonbaseline_version_is_rejected() {
    let mut baseline = fixture().await.baseline;
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

#[test]
fn an_object_owns_its_stream_frame_its_result_slices_and_its_stages() {
    let object = |name: &str, kind| ClusterTopologyObjectPlan {
        name: name.into(),
        kind,
        catalog_generation: 1,
        transition: ClusterTopologyObjectTransition::Preserve,
        initialization: TopologyInitialization::PreserveExactCut,
        definition_sha256: String::new(),
        compatibility_sha256: String::new(),
        dependencies: Vec::new(),
        schema_sha256: None,
        managed_state_contract: Some("contract".into()),
    };
    let view = object("counts", CatalogObjectKind::MaterializedView);
    assert!(view.owns_state_frame("graph:counts"));
    assert!(view.owns_state_frame("graph:counts::view"));
    assert!(view.owns_state_frame("graph:counts$stage1"));
    assert!(!view.owns_state_frame("graph:counts$stage1::view"));
    assert!(!view.owns_state_frame("counts::view"));
    let stream = object("counts", CatalogObjectKind::Stream);
    assert!(stream.owns_state_frame("graph:counts"));
    assert!(stream.owns_state_frame("graph:counts$stage2"));
    assert!(!stream.owns_state_frame("graph:counts$stage"));
    assert!(!stream.owns_state_frame("graph:other$stage1"));
    assert!(!stream.owns_state_frame("graph:counts::view"));
    assert!(!object("counts", CatalogObjectKind::Source).owns_state_frame("graph:counts"));
}

#[test]
fn only_a_new_view_over_a_preserved_stream_or_view_seeds() {
    let plan = |name: &str, kind, transition, dependencies: &[&str]| ClusterTopologyObjectPlan {
        name: name.into(),
        kind,
        catalog_generation: 1,
        transition,
        initialization: TopologyInitialization::SeedFromUpstreamAtCut,
        definition_sha256: String::new(),
        compatibility_sha256: String::new(),
        dependencies: dependencies.iter().map(|&name| name.into()).collect(),
        schema_sha256: None,
        managed_state_contract: Some("view_result_v1".into()),
    };
    let preserved = ClusterTopologyObjectTransition::Preserve;
    let added = ClusterTopologyObjectTransition::AddFutureOnly;
    let objects = [
        plan("totals", CatalogObjectKind::Stream, preserved, &[]),
        plan(
            "counts",
            CatalogObjectKind::MaterializedView,
            preserved,
            &[],
        ),
        plan("trades", CatalogObjectKind::Source, preserved, &[]),
        plan("fresh", CatalogObjectKind::Stream, added, &[]),
    ];
    let view = |dependency: &str| {
        plan(
            "v",
            CatalogObjectKind::MaterializedView,
            added,
            &[dependency],
        )
    };
    assert!(view("totals").seeds_from_preserved_upstream(&objects));
    assert!(view("counts").seeds_from_preserved_upstream(&objects));
    assert!(!view("trades").seeds_from_preserved_upstream(&objects));
    assert!(!view("fresh").seeds_from_preserved_upstream(&objects));
    let stream = plan("s", CatalogObjectKind::Stream, added, &["totals"]);
    assert!(!stream.seeds_from_preserved_upstream(&objects));
    let mut stateless = view("totals");
    stateless.managed_state_contract = None;
    assert!(!stateless.seeds_from_preserved_upstream(&objects));
}
