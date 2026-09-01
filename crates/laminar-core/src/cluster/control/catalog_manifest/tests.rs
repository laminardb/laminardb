use super::*;
use bytes::Bytes;
use object_store::{memory::InMemory, ObjectStoreExt, PutPayload};
use uuid::Uuid;

async fn store() -> (CatalogManifestStore, Arc<InMemory>, LeaderProof) {
    let backing = Arc::new(InMemory::new());
    let authority = Arc::new(LeaderLeaseStore::new(backing.clone(), 1_000));
    let owner = super::super::LeaderLeaseOwner {
        node: crate::cluster::discovery::NodeId(1),
        boot: Uuid::from_u128(1),
        process_term: 1,
    };
    let super::super::LeaseOutcome::Acquired(lease) =
        authority.begin_new_term(&owner, 0).await.unwrap()
    else {
        unreachable!()
    };
    (CatalogManifestStore::new(authority), backing, lease.proof())
}

fn entry(name: &str) -> CatalogManifestEntry {
    CatalogManifestEntry {
        canonical_name: name.to_string(),
        kind: CatalogObjectKind::Source,
        catalog_generation: 1,
        ddl: format!("CREATE SOURCE {name} (k BIGINT)"),
    }
}

#[tokio::test]
async fn empty_inventory_is_a_real_sealed_manifest() {
    let (store, _, proof) = store().await;
    assert!(store.load().await.unwrap().is_none());
    assert_eq!(
        store
            .seal(&CatalogManifest::default(), &proof)
            .await
            .unwrap(),
        CatalogSealOutcome::Created
    );
    assert_eq!(
        store.load().await.unwrap(),
        Some(CatalogManifest::default())
    );
}

#[tokio::test]
async fn concurrent_identical_seal_is_idempotent() {
    let (store, _, proof) = store().await;
    let store = Arc::new(store);
    let manifest = CatalogManifest::new(vec![entry("events")]).unwrap();
    let (left, right) = tokio::join!(store.seal(&manifest, &proof), store.seal(&manifest, &proof));
    let outcomes = [left.unwrap(), right.unwrap()];
    assert!(outcomes.contains(&CatalogSealOutcome::Created));
    assert!(outcomes.contains(&CatalogSealOutcome::ExistingIdentical));
    assert_eq!(store.load().await.unwrap(), Some(manifest));
}

#[tokio::test]
async fn divergent_second_inventory_fails_closed() {
    let (store, _, proof) = store().await;
    let winner = CatalogManifest::new(vec![entry("winner")]).unwrap();
    let loser = CatalogManifest::new(vec![entry("loser")]).unwrap();
    store.seal(&winner, &proof).await.unwrap();
    assert!(matches!(
        store.seal(&loser, &proof).await,
        Err(CatalogManifestError::Conflict)
    ));
    assert_eq!(store.load().await.unwrap(), Some(winner));
}

#[test]
fn duplicate_or_noncanonical_entries_are_rejected() {
    let duplicate = vec![entry("events"), entry("events")];
    assert!(matches!(
        CatalogManifest::new(duplicate),
        Err(CatalogManifestError::Invalid(_))
    ));
    assert!(matches!(
        CatalogManifest::new(vec![entry(" events")]),
        Err(CatalogManifestError::Invalid(_))
    ));
}

#[test]
fn object_generation_is_durable_nonzero_and_backward_decodable() {
    let mut zero = entry("events");
    zero.catalog_generation = 0;
    assert!(matches!(
        CatalogManifest::new(vec![zero]),
        Err(CatalogManifestError::Invalid(_))
    ));

    let first = CatalogManifest::new(vec![entry("events")]).unwrap();
    let mut recreated_entry = entry("events");
    recreated_entry.catalog_generation = 2;
    let recreated = CatalogManifest::new(vec![recreated_entry]).unwrap();
    assert_ne!(
        first.encode_and_reference().unwrap().1,
        recreated.encode_and_reference().unwrap().1
    );

    let legacy = br#"{"entries":[{"canonical_name":"events","kind":"source","ddl":"CREATE SOURCE events (k BIGINT)"}]}"#;
    let decoded: CatalogManifest = serde_json::from_slice(legacy).unwrap();
    assert_eq!(decoded.entries[0].catalog_generation, 1);
    decoded.validate().unwrap();
}

#[tokio::test]
async fn legacy_generation_one_manifest_loads_through_the_sealed_blob_path() {
    let (store, backing, proof) = store().await;
    let legacy = Bytes::from_static(
        br#"{"entries":[{"canonical_name":"events","kind":"source","ddl":"CREATE SOURCE events (k BIGINT)"}]}"#,
    );
    let manifest: CatalogManifest = serde_json::from_slice(&legacy).unwrap();

    store.seal(&manifest, &proof).await.unwrap();
    let (_, reference) = manifest.encode_and_reference().unwrap();
    let stored = backing
        .get(&reference.object_path())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(stored, legacy);
    assert_eq!(store.load().await.unwrap(), Some(manifest));
}

#[test]
fn manifest_bounds_are_enforced_before_durable_writes() {
    let too_many = (0..=MAX_CATALOG_MANIFEST_ENTRIES)
        .map(|index| entry(&format!("source_{index}")))
        .collect();
    assert!(matches!(
        CatalogManifest::new(too_many),
        Err(CatalogManifestError::Invalid(_))
    ));

    let oversized = CatalogManifest::new(vec![CatalogManifestEntry {
        canonical_name: "events".into(),
        kind: CatalogObjectKind::Source,
        catalog_generation: 1,
        ddl: "x".repeat(MAX_CATALOG_MANIFEST_BYTES),
    }]);
    assert!(matches!(oversized, Err(CatalogManifestError::Invalid(_))));
}

#[tokio::test]
async fn missing_sealed_manifest_blob_fails_closed() {
    let (store, backing, proof) = store().await;
    let manifest = CatalogManifest::new(vec![entry("events")]).unwrap();
    let (_, reference) = manifest.encode_and_reference().unwrap();
    store.seal(&manifest, &proof).await.unwrap();
    backing.delete(&reference.object_path()).await.unwrap();

    let error = store.load().await.unwrap_err();
    assert!(matches!(&error, CatalogManifestError::Invalid(_)));
    assert!(error.to_string().contains("is missing"));
}

#[tokio::test]
async fn tampered_sealed_manifest_blob_fails_closed() {
    let (store, backing, proof) = store().await;
    let manifest = CatalogManifest::new(vec![entry("events")]).unwrap();
    let (mut encoded, reference) = manifest.encode_and_reference().unwrap();
    store.seal(&manifest, &proof).await.unwrap();
    let offset = encoded
        .windows(b"events".len())
        .position(|window| window == b"events")
        .unwrap();
    encoded[offset] = b'x';
    backing
        .put(
            &reference.object_path(),
            PutPayload::from(Bytes::from(encoded)),
        )
        .await
        .unwrap();

    let error = store.load().await.unwrap_err();
    assert!(matches!(&error, CatalogManifestError::Invalid(_)));
    assert!(error
        .to_string()
        .contains("does not match its sealed reference"));
}
