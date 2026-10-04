//! Catalog inventory sealed in the append-only leader authority.

use std::fmt::Write;
use std::sync::Arc;

use ahash::AHashSet;
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
    let mut names = AHashSet::with_capacity(entries.len());
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
        if entry.catalog_generation == 0 {
            return Err(CatalogManifestError::Invalid(format!(
                "catalog manifest entry '{}' has a zero object generation",
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
    /// Durable catalog-object incarnation. The initial sealed inventory uses generation one.
    #[serde(
        default = "initial_catalog_generation",
        skip_serializing_if = "is_initial_catalog_generation"
    )]
    pub catalog_generation: u64,
    /// Exact DDL text replayed on every node.
    pub ddl: String,
}

const fn initial_catalog_generation() -> u64 {
    1
}

const fn is_initial_catalog_generation(generation: &u64) -> bool {
    *generation == initial_catalog_generation()
}

/// The complete ordered catalog sealed for one cluster control namespace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogManifest {
    /// DDL entries in dependency-safe creation order. An empty inventory is valid and sealed.
    pub entries: Vec<CatalogManifestEntry>,
}

impl CatalogManifest {
    /// Compute the canonical content reference without publishing or changing authority.
    ///
    /// # Errors
    /// Rejects a malformed or oversized inventory.
    pub fn reference(&self) -> Result<CatalogManifestRef, CatalogManifestError> {
        self.encode_and_reference().map(|(_, reference)| reference)
    }

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
    pub(crate) fn validate(&self) -> Result<(), CatalogManifestError> {
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
    /// Read the definitive payload-bound status of a reserved topology request.
    ///
    /// # Errors
    /// Fails on unavailable/corrupt evidence or the bounded read deadline.
    pub async fn operation_status(
        &self,
        operation_id: super::topology::TopologyOperationId,
    ) -> Result<Option<super::topology::TopologyAdmissionStatus>, super::topology::TopologyError>
    {
        self.authority.topology_operation_status(operation_id).await
    }

    /// Share the exact append-only authority used by the leader lease manager.
    #[must_use]
    pub fn new(authority: Arc<LeaderLeaseStore>) -> Self {
        Self { authority }
    }

    /// Read explicit logical topology metadata from the same catalog/leader authority.
    ///
    /// # Errors
    /// Fails closed when the authority or referenced catalog/deployment is invalid.
    pub async fn topology_state(
        &self,
    ) -> Result<super::topology::TopologyCatalogState, super::topology::TopologyError> {
        self.authority.topology_catalog_state().await
    }

    /// Load the catalog and its topology metadata from one immutable authority snapshot.
    ///
    /// # Errors
    /// Fails closed on missing/corrupt content or inconsistent adoption/deployment authority.
    pub async fn load_with_topology(
        &self,
    ) -> Result<
        Option<(CatalogManifest, super::topology::TopologyCatalogState)>,
        super::topology::TopologyError,
    > {
        self.authority.catalog_with_topology().await
    }

    /// Load the exact parent catalog while checking the current committed inventory.
    ///
    /// # Errors
    /// Rejects changed authority, invalid retained evidence or the bounded read deadline.
    pub async fn parent_topology_catalog(
        &self,
        expected_current: &CatalogManifestRef,
    ) -> Result<Option<CatalogManifest>, super::topology::TopologyError> {
        self.authority
            .parent_topology_catalog(expected_current)
            .await
    }

    /// Load the complete adopted startup catalog while checking the exact current inventory.
    /// Removed objects remain startup assertions and are never recreated.
    ///
    /// # Errors
    /// Rejects changed authority, invalid retained evidence or the bounded read deadline.
    pub async fn original_topology_catalog(
        &self,
        expected_current: &CatalogManifestRef,
    ) -> Result<Option<CatalogManifest>, super::topology::TopologyError> {
        self.authority
            .original_topology_catalog(expected_current)
            .await
    }

    /// Read object names retired by the bounded, retained committed topology journal.
    ///
    /// # Errors
    /// Rejects invalid retained evidence, changed committed authority or the read deadline.
    pub async fn retired_topology_names(
        &self,
    ) -> Result<std::collections::BTreeSet<String>, super::topology::TopologyError> {
        self.authority.retired_topology_names().await
    }

    /// Read the latest retired incarnation of each name for safe future-only recreation.
    ///
    /// # Errors
    /// Rejects invalid retained evidence, changed committed authority or the read deadline.
    pub async fn retired_topology_generations(
        &self,
    ) -> Result<std::collections::BTreeMap<String, u64>, super::topology::TopologyError> {
        self.authority.retired_topology_generations().await
    }

    /// Explicitly adopt a sealed legacy inventory without changing the processing graph.
    ///
    /// Requires a coordinated binary upgrade; this is not runtime migration admission.
    ///
    /// # Errors
    /// Rejects stale leader proof, divergent reference/deployment, invalid data or authority I/O.
    pub async fn adopt_legacy_topology(
        &self,
        proof: &LeaderProof,
        operation_id: super::topology::TopologyOperationId,
        expected_manifest: &CatalogManifestRef,
        expected_deployment: &str,
    ) -> Result<super::topology::TopologyAdoptionOutcome, super::topology::TopologyError> {
        self.authority
            .adopt_legacy_topology(proof, operation_id, expected_manifest, expected_deployment)
            .await
    }

    /// Load the sealed catalog, or `None` before the first successful seal.
    ///
    /// # Errors
    /// Fails on object-store I/O, malformed JSON, or an invalid inventory.
    pub async fn load(&self) -> Result<Option<CatalogManifest>, CatalogManifestError> {
        self.load_with_topology()
            .await
            .map(|snapshot| snapshot.map(|(manifest, _)| manifest))
            .map_err(|error| match error {
                super::topology::TopologyError::Authority(error) => {
                    CatalogManifestError::Authority(error)
                }
                super::topology::TopologyError::Catalog(error) => error,
                error => CatalogManifestError::Invalid(error.to_string()),
            })
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
        Box::pin(self.authority.seal_catalog(proof, manifest)).await
    }
}

#[cfg(test)]
mod tests;
