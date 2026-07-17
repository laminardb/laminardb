//! Catalog inventory sealed in the append-only leader authority.

use std::collections::HashSet;
use std::fmt::Write;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::checkpoint::LeaderProof;

use super::{LeaderLeaseStore, LeaseError};

pub use crate::catalog::CatalogObjectKind;

pub(super) const CATALOG_MANIFEST_FORMAT_VERSION: u16 = 1;
pub(super) const MAX_CATALOG_MANIFEST_ENTRIES: usize = 4_096;
pub(super) const MAX_CATALOG_MANIFEST_BYTES: usize = 8 * 1024 * 1024;

const CATALOG_MANIFEST_PREFIX: &str = "control/catalog-manifest/v1/";

fn validate_entries(entries: &[CatalogManifestEntry]) -> Result<(), CatalogManifestError> {
    if entries.len() > MAX_CATALOG_MANIFEST_ENTRIES {
        return Err(CatalogManifestError::Invalid(format!(
            "catalog manifest has {} entries; maximum is {MAX_CATALOG_MANIFEST_ENTRIES}",
            entries.len()
        )));
    }
    let mut names = HashSet::with_capacity(entries.len());
    for entry in entries {
        if entry.canonical_name.is_empty() || entry.canonical_name.trim() != entry.canonical_name {
            return Err(CatalogManifestError::Invalid(format!(
                "catalog manifest has a non-canonical name {:?}",
                entry.canonical_name
            )));
        }
        if entry.ddl.trim().is_empty() {
            return Err(CatalogManifestError::Invalid(format!(
                "catalog manifest entry '{}' has empty DDL",
                entry.canonical_name
            )));
        }
        if !names.insert(entry.canonical_name.as_str()) {
            return Err(CatalogManifestError::Invalid(format!(
                "catalog manifest repeats canonical name '{}'",
                entry.canonical_name
            )));
        }
    }
    Ok(())
}

/// One catalog object's defining DDL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogManifestEntry {
    /// Canonical catalog identifier.
    pub canonical_name: String,
    /// Exact namespace owner.
    pub kind: CatalogObjectKind,
    /// Exact DDL text replayed on every node.
    pub ddl: String,
}

/// The complete ordered catalog sealed for one cluster control namespace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogManifest {
    /// DDL entries in dependency-safe creation order. An empty inventory is valid and sealed.
    pub entries: Vec<CatalogManifestEntry>,
}

impl CatalogManifest {
    /// Construct and validate a complete inventory.
    ///
    /// # Errors
    /// Rejects empty/non-canonical names, empty DDL, duplicate identifiers, or an inventory that
    /// exceeds the durable cardinality or encoded-size limits.
    pub fn new(entries: Vec<CatalogManifestEntry>) -> Result<Self, CatalogManifestError> {
        let manifest = Self { entries };
        manifest.encode_and_reference()?;
        Ok(manifest)
    }

    pub(super) fn validate(&self) -> Result<(), CatalogManifestError> {
        validate_entries(&self.entries)?;
        Ok(())
    }

    pub(super) fn encode_and_reference(
        &self,
    ) -> Result<(Vec<u8>, CatalogManifestRef), CatalogManifestError> {
        self.validate()?;
        let encoded = serde_json::to_vec(self)?;
        if encoded.len() > MAX_CATALOG_MANIFEST_BYTES {
            return Err(CatalogManifestError::Invalid(format!(
                "encoded catalog manifest is {} bytes; maximum is {MAX_CATALOG_MANIFEST_BYTES}",
                encoded.len()
            )));
        }
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| {
            CatalogManifestError::Invalid("encoded catalog manifest length overflow".into())
        })?;
        let entry_count = u32::try_from(self.entries.len()).map_err(|_| {
            CatalogManifestError::Invalid("catalog manifest entry count overflow".into())
        })?;
        let digest = Sha256::digest(&encoded);
        let mut sha256 = String::with_capacity(64);
        for byte in digest {
            write!(&mut sha256, "{byte:02x}").expect("writing to a String cannot fail");
        }
        let reference = CatalogManifestRef {
            version: CATALOG_MANIFEST_FORMAT_VERSION,
            sha256,
            encoded_len,
            entry_count,
        };
        reference.validate()?;
        Ok((encoded, reference))
    }
}

/// Small immutable reference carried by every leader-lease renewal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogManifestRef {
    /// Canonical manifest encoding version.
    pub version: u16,
    /// Lowercase hexadecimal SHA-256 of the exact encoded manifest.
    pub sha256: String,
    /// Exact encoded object length.
    pub encoded_len: u64,
    /// Exact inventory cardinality.
    pub entry_count: u32,
}

impl CatalogManifestRef {
    pub(super) fn validate(&self) -> Result<(), CatalogManifestError> {
        if self.version != CATALOG_MANIFEST_FORMAT_VERSION {
            return Err(CatalogManifestError::Invalid(format!(
                "unsupported catalog manifest version {}",
                self.version
            )));
        }
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CatalogManifestError::Invalid(
                "catalog manifest SHA-256 must be 64 lowercase hexadecimal characters".into(),
            ));
        }
        if self.encoded_len == 0
            || self.encoded_len
                > u64::try_from(MAX_CATALOG_MANIFEST_BYTES)
                    .expect("catalog manifest byte limit fits u64")
        {
            return Err(CatalogManifestError::Invalid(format!(
                "catalog manifest encoded length {} is outside 1..={MAX_CATALOG_MANIFEST_BYTES}",
                self.encoded_len
            )));
        }
        if !matches!(
            usize::try_from(self.entry_count),
            Ok(count) if count <= MAX_CATALOG_MANIFEST_ENTRIES
        ) {
            return Err(CatalogManifestError::Invalid(format!(
                "catalog manifest entry count {} exceeds {MAX_CATALOG_MANIFEST_ENTRIES}",
                self.entry_count
            )));
        }
        Ok(())
    }

    pub(super) fn object_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{CATALOG_MANIFEST_PREFIX}{}.json", self.sha256))
    }
}

/// Result of attempting to seal the immutable inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogSealOutcome {
    /// This caller appended the first lease record carrying the inventory.
    Created,
    /// The authority already carries the exact same canonical inventory.
    ExistingIdentical,
}

/// Catalog view over the same append-only authority used for leader fencing.
pub struct CatalogManifestStore {
    authority: Arc<LeaderLeaseStore>,
}

impl std::fmt::Debug for CatalogManifestStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogManifestStore")
            .finish_non_exhaustive()
    }
}

/// Errors loading or sealing the catalog inventory.
#[derive(Debug, thiserror::Error)]
pub enum CatalogManifestError {
    /// Shared leader authority failed.
    #[error("leader lease authority: {0}")]
    Authority(#[from] LeaseError),
    /// JSON serialization or decoding failure.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The stored or proposed inventory is malformed.
    #[error("invalid sealed catalog manifest: {0}")]
    Invalid(String),
    /// Another writer sealed a different complete inventory.
    #[error("cluster catalog is already sealed with a different inventory")]
    Conflict,
    /// The supplied proof no longer owns the durable leader term.
    #[error("catalog seal was fenced by a different durable leader term")]
    Fenced,
}

impl CatalogManifestStore {
    /// Share the exact append-only authority used by the leader lease manager.
    #[must_use]
    pub fn new(authority: Arc<LeaderLeaseStore>) -> Self {
        Self { authority }
    }

    /// Load the sealed catalog, or `None` before the first successful seal.
    ///
    /// # Errors
    /// Fails on object-store I/O, malformed JSON, or an invalid inventory.
    pub async fn load(&self) -> Result<Option<CatalogManifest>, CatalogManifestError> {
        let Some(reference) = self
            .authority
            .load()
            .await?
            .and_then(|lease| lease.catalog_manifest)
        else {
            return Ok(None);
        };
        self.authority
            .load_catalog_manifest(&reference)
            .await
            .map(Some)
    }

    /// CAS-append the first inventory under an exact leader proof.
    ///
    /// A concurrent exact inventory is idempotent. Any different winner fails closed.
    ///
    /// # Errors
    /// Fails for an invalid proposal, divergent winner, or object-store I/O.
    pub async fn seal(
        &self,
        manifest: &CatalogManifest,
        proof: &LeaderProof,
    ) -> Result<CatalogSealOutcome, CatalogManifestError> {
        self.authority.seal_catalog(proof, manifest).await
    }
}

#[cfg(test)]
mod tests {
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
        let (left, right) =
            tokio::join!(store.seal(&manifest, &proof), store.seal(&manifest, &proof));
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
}
