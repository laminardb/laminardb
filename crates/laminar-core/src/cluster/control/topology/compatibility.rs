//! Deterministic candidate definitions shared by local compilation and durable preparation.

use super::{TopologyError, TopologyVersion};
use crate::checkpoint::PipelineIdentity;
use crate::cluster::control::{CatalogManifest, CatalogManifestRef, CatalogObjectKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const MAX_TOPOLOGY_COMPATIBILITY_BYTES: usize = 1024 * 1024;

/// What a successful validation proves. Participant agreement and runtime authorization follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TopologyValidationScope {
    /// This binary compiled a compatible candidate without changing the active catalog or actors.
    LocalCandidatePlan,
}

/// Conservative operation classification for the initial additive planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ClusterTopologyObjectTransition {
    /// Same incarnation, definition, dependency closure, schema and managed-state contract.
    Preserve,
    /// New object, activated at an explicitly persisted future-only boundary.
    AddFutureOnly,
}

/// Required initialization semantics. These are requirements, never concrete cut positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TopologyInitialization {
    /// Retain the exact reconciled cut's state, timers, watermarks and source/output progress.
    PreserveExactCut,
    /// Process only target-generation input after the cut, without historical replay or backfill.
    FutureOnlyAtCut,
    /// Resolve concrete latest source/partition positions once at the cut and persist before commit.
    ResolveSourcePositionsOnce,
}

/// Authorization still needed before this local plan can activate a target graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TopologyActivationRequirement {
    /// Every required exact owner/evidence process must validate the same plan and protocol.
    ParticipantPlanAgreement,
    /// Establish and reconcile one exact old-topology checkpoint cut.
    ReconciledCheckpointCut,
    /// Persist source/channel positions, state mapping and output/replay frontiers.
    DurableInitializationAndProgress,
    /// Observe superseded actors' terminal completion and fence staged target output.
    ObservedActorRetirement,
    /// Commit the target manifest, cut and mapping atomically in shared authority.
    AtomicTargetCommit,
    /// Install the committed graph and consume an owner-complete coordinated Release.
    InstalledTargetRelease,
}

/// One catalog-bound entry of the local compatibility descriptor, sorted by canonical name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTopologyObjectPlan {
    /// Stable catalog name, never an optimizer node index.
    pub name: String,
    /// Typed catalog namespace owner.
    pub kind: CatalogObjectKind,
    /// Incarnation retained from the parent, or one for a never-before-created additive name.
    pub catalog_generation: u64,
    /// Supported local classification.
    pub transition: ClusterTopologyObjectTransition,
    /// Required activation semantics; no scalar offset is invented during validation.
    pub initialization: TopologyInitialization,
    /// Hash of the same resolved definition used by strict pipeline identity.
    pub definition_sha256: String,
    /// Hash binding identity, schema, ABI, capability, connector contract and dependency closure.
    pub compatibility_sha256: String,
    /// Sorted direct catalog dependencies, with their identities transitively bound by the hash.
    pub dependencies: Vec<String>,
    /// Resolved Arrow schema hash, absent for sinks.
    pub schema_sha256: Option<String>,
    /// Versioned codec name of a managed operator, absent for stateless/source/sink objects.
    pub managed_state_contract: Option<String>,
}

/// Effect-free candidate validation. This is neither a durable admission nor an activation receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTopologyValidation {
    /// Deterministic local descriptor format, independent of catalog and topology versions.
    pub validation_format_version: u16,
    /// Exact scope of the returned evidence.
    pub scope: TopologyValidationScope,
    /// Existing control/checkpoint deployment identity.
    pub deployment_id: String,
    /// Expected authoritative parent.
    pub parent_version: TopologyVersion,
    /// Proposed exact successor; not a committed version.
    pub target_version: TopologyVersion,
    /// Exact durable parent bytes and inventory.
    pub parent_manifest: CatalogManifestRef,
    /// Candidate inventory reference, computed without writing the blob or a catalog head.
    pub target_manifest: CatalogManifestRef,
    /// Unmodified strict recovery identity for the parent graph.
    pub parent_pipeline: PipelineIdentity,
    /// Full strict identity for the changed graph; ordinary parent checkpoints will not match it.
    pub target_pipeline: PipelineIdentity,
    /// Global state/routing/delivery ABI and config shared by every object descriptor.
    pub environment_sha256: String,
    /// Deterministic descriptor digest. Participants must agree on this before admission advances.
    pub compatibility_sha256: String,
    /// Preserved and additive objects with explicit state and initialization requirements.
    pub objects: Vec<ClusterTopologyObjectPlan>,
    /// A processing pause is required for the implemented old-topology cut contract.
    pub requires_processing_pause: bool,
    /// Evidence this validation does not supply; the public mutation path remains guarded.
    pub required_before_activation: Vec<TopologyActivationRequirement>,
}

/// Immutable canonical candidate report. The report is separate from authority renewals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyCompatibilityRef {
    /// SHA-256 of the complete canonical report, including the definition descriptor digest.
    pub sha256: String,
    /// Exact report length, bounded before reading or decoding.
    pub encoded_len: u64,
}

pub(super) fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl TopologyCompatibilityRef {
    pub(crate) fn validate(&self) -> Result<(), TopologyError> {
        if !is_digest(&self.sha256)
            || self.encoded_len == 0
            || self.encoded_len > MAX_TOPOLOGY_COMPATIBILITY_BYTES as u64
        {
            return Err(TopologyError::Invalid(
                "invalid compatibility reference".into(),
            ));
        }
        Ok(())
    }
}

impl ClusterTopologyValidation {
    /// Recompute the existing descriptor digest from its explicit immutable fields.
    ///
    /// # Errors
    /// Returns a typed encoding failure; this never writes authority.
    pub fn descriptor_digest(&self) -> Result<String, TopologyError> {
        let bytes = serde_json::to_vec(&(
            "laminardb-topology-compatibility-v1",
            self.validation_format_version,
            &self.deployment_id,
            self.parent_version,
            self.target_version,
            &self.parent_manifest,
            &self.target_manifest,
            &self.parent_pipeline,
            &self.target_pipeline,
            &self.environment_sha256,
            &self.objects,
        ))
        .map_err(|error| TopologyError::Invalid(error.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    /// Validate canonical structure and bind every object to the exact ordered catalog inventories.
    /// Compilation/connector semantics are independently checked by each DB participant.
    ///
    /// # Errors
    /// Rejects malformed, unsupported, divergent or oversized evidence.
    pub fn validate_catalogs(
        &self,
        parent: &CatalogManifest,
        target: &CatalogManifest,
    ) -> Result<(), TopologyError> {
        if parent.reference()? != self.parent_manifest
            || target.reference()? != self.target_manifest
            || target.entries.len() <= parent.entries.len()
            || !target.entries.starts_with(&parent.entries)
            || target.entries.len() != self.objects.len()
        {
            return Err(TopologyError::Invalid(
                "compatibility descriptor differs from additive catalog inventories".into(),
            ));
        }
        self.encode_and_reference()?;
        for (index, entry) in target.entries.iter().enumerate() {
            let object = self
                .objects
                .binary_search_by(|object| object.name.cmp(&entry.canonical_name))
                .ok()
                .map(|index| &self.objects[index])
                .ok_or_else(|| {
                    TopologyError::Invalid(
                        "catalog object is absent from compatibility descriptor".into(),
                    )
                })?;
            let transition = if index < parent.entries.len() {
                ClusterTopologyObjectTransition::Preserve
            } else {
                ClusterTopologyObjectTransition::AddFutureOnly
            };
            if object.kind != entry.kind
                || object.catalog_generation != entry.catalog_generation
                || object.transition != transition
            {
                return Err(TopologyError::Invalid(
                    "compatibility object incarnation/classification differs from catalog".into(),
                ));
            }
        }
        Ok(())
    }

    /// Canonical bounded report encoding used by all admission and participant paths.
    ///
    /// # Errors
    /// Rejects unsupported or noncanonical descriptor fields and size limits.
    pub fn encode_and_reference(
        &self,
    ) -> Result<(Vec<u8>, TopologyCompatibilityRef), TopologyError> {
        let deployment = uuid::Uuid::parse_str(&self.deployment_id)
            .map_err(|_| TopologyError::Invalid("invalid descriptor deployment".into()))?;
        self.parent_manifest.validate()?;
        self.target_manifest.validate()?;
        if self.validation_format_version != 1
            || deployment.is_nil()
            || deployment.to_string() != self.deployment_id
            || self.parent_version.successor()? != self.target_version
            || self.parent_manifest == self.target_manifest
            || !self.parent_pipeline.is_canonical()
            || !self.target_pipeline.is_canonical()
            || self.parent_pipeline == self.target_pipeline
            || !is_digest(&self.environment_sha256)
            || self.objects.is_empty()
            || self.objects.len() > 256
            || !self
                .objects
                .windows(2)
                .all(|pair| pair[0].name < pair[1].name)
            || !self.requires_processing_pause
            || self.required_before_activation
                != [
                    TopologyActivationRequirement::ParticipantPlanAgreement,
                    TopologyActivationRequirement::ReconciledCheckpointCut,
                    TopologyActivationRequirement::DurableInitializationAndProgress,
                    TopologyActivationRequirement::ObservedActorRetirement,
                    TopologyActivationRequirement::AtomicTargetCommit,
                    TopologyActivationRequirement::InstalledTargetRelease,
                ]
        {
            return Err(TopologyError::Invalid(
                "unsupported or noncanonical candidate descriptor".into(),
            ));
        }
        for object in &self.objects {
            let initialization = match (object.transition, object.kind) {
                (ClusterTopologyObjectTransition::Preserve, _) => {
                    TopologyInitialization::PreserveExactCut
                }
                (_, CatalogObjectKind::Source) => {
                    TopologyInitialization::ResolveSourcePositionsOnce
                }
                _ => TopologyInitialization::FutureOnlyAtCut,
            };
            if object.name.is_empty()
                || object.name.len() > 256
                || object.catalog_generation == 0
                || !is_digest(&object.definition_sha256)
                || !is_digest(&object.compatibility_sha256)
                || object
                    .schema_sha256
                    .as_ref()
                    .is_some_and(|hash| !is_digest(hash))
                || object.initialization != initialization
                || object.dependencies.len() > 256
                || !object.dependencies.windows(2).all(|pair| pair[0] < pair[1])
                || object.dependencies.iter().any(|name| {
                    name == &object.name
                        || self
                            .objects
                            .binary_search_by(|obj| obj.name.cmp(name))
                            .is_err()
                })
                || object
                    .managed_state_contract
                    .as_ref()
                    .is_some_and(|contract| {
                        contract.is_empty()
                            || contract.len() > 128
                            || object.kind != CatalogObjectKind::Stream
                            || object.transition != ClusterTopologyObjectTransition::Preserve
                    })
            {
                return Err(TopologyError::Invalid(
                    "invalid compatibility object mapping".into(),
                ));
            }
        }
        if self.descriptor_digest()? != self.compatibility_sha256 {
            return Err(TopologyError::Invalid(
                "candidate descriptor digest differs from its fields".into(),
            ));
        }
        let bytes =
            serde_json::to_vec(self).map_err(|error| TopologyError::Invalid(error.to_string()))?;
        let reference = TopologyCompatibilityRef {
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            encoded_len: bytes.len() as u64,
        };
        reference.validate()?;
        Ok((bytes, reference))
    }
}
