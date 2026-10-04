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

/// Supported catalog-object transitions at an exact checkpoint cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ClusterTopologyObjectTransition {
    /// Same incarnation, schema and state contract; any replacement is compiler-certified.
    Preserve,
    /// New object, activated at an explicitly persisted future-only boundary.
    AddFutureOnly,
    /// Retire an existing source, stream or sink after its cut and actor termination are settled.
    Remove,
}

/// Required initialization semantics. These are requirements, never concrete cut positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TopologyInitialization {
    /// Retain the exact reconciled cut's state, timers, watermarks and source/output progress.
    PreserveExactCut,
    /// Process only target-generation input after the cut, without historical replay or backfill.
    FutureOnlyAtCut,
    /// Initialize new managed state empty at the exact cut; consume only subsequent input.
    /// This never authorizes resetting a preserved operator or a target checkpoint image.
    EmptyManagedStateAtCut,
    /// Resolve concrete latest source/partition positions once at the cut and persist before commit.
    ResolveSourcePositionsOnce,
    /// Settle old-generation state/progress/effects at the cut and omit the object from the target.
    RetireAtCut,
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
    /// Incarnation retained from the parent or newly allocated for future-only activation.
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
    /// Exact submitted DDL, including removals absent from the target inventory. Binds retries
    /// and participant compilation to the same ordered payload.
    pub statements: Vec<String>,
    /// Parent/target union sorted by name and generation. Explicit reset has two mappings:
    /// retirement of the parent incarnation and future-only activation of its successor.
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
            "laminardb-topology-compatibility-v3",
            self.validation_format_version,
            &self.deployment_id,
            self.parent_version,
            self.target_version,
            &self.parent_manifest,
            &self.target_manifest,
            &self.parent_pipeline,
            &self.target_pipeline,
            &self.environment_sha256,
            &self.statements,
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
        {
            return Err(TopologyError::Invalid(
                "compatibility descriptor differs from the exact catalog inventories".into(),
            ));
        }
        self.encode_and_reference()?;
        let mut expected = std::collections::BTreeMap::new();
        let mut retained = Vec::new();
        for entry in &parent.entries {
            let transition = match target
                .entries
                .iter()
                .find(|target| target.canonical_name == entry.canonical_name)
            {
                Some(target) if target.kind == entry.kind
                    && target.catalog_generation == entry.catalog_generation => {
                    retained.push(target.clone());
                    ClusterTopologyObjectTransition::Preserve
                }
                replacement if matches!(
                    entry.kind,
                    CatalogObjectKind::Source | CatalogObjectKind::Stream | CatalogObjectKind::Sink
                ) && replacement.is_none_or(|new| new.kind == entry.kind
                    && Some(new.catalog_generation) == entry.catalog_generation.checked_add(1)) =>
                {
                    ClusterTopologyObjectTransition::Remove
                }
                _ => return Err(TopologyError::Invalid(
                    "target changes an incarnation without an explicit retirement/successor mapping"
                        .into(),
                )),
            };
            expected.insert(
                (entry.canonical_name.as_str(), entry.catalog_generation),
                (entry, transition),
            );
        }
        if !target.entries.starts_with(&retained) {
            return Err(TopologyError::Invalid(
                "target reordered the retained parent inventory".into(),
            ));
        }
        for entry in &target.entries {
            expected
                .entry((entry.canonical_name.as_str(), entry.catalog_generation))
                .or_insert((entry, ClusterTopologyObjectTransition::AddFutureOnly));
        }
        if expected.len() != self.objects.len() {
            return Err(TopologyError::Invalid(
                "compatibility descriptor omitted or added a catalog mapping".into(),
            ));
        }
        for object in &self.objects {
            let (entry, transition) = expected
                .get(&(object.name.as_str(), object.catalog_generation))
                .ok_or_else(|| {
                    TopologyError::Invalid(
                        "compatibility object is absent from both catalogs".into(),
                    )
                })?;
            if object.kind != entry.kind
                || object.catalog_generation != entry.catalog_generation
                || object.transition != *transition
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
        let sql_bytes = self
            .statements
            .iter()
            .try_fold(0_usize, |total, sql| total.checked_add(sql.len()));
        if self.validation_format_version != 3
            || deployment.is_nil()
            || deployment.to_string() != self.deployment_id
            || self.parent_version.successor()? != self.target_version
            || self.parent_manifest == self.target_manifest
            || !self.parent_pipeline.is_canonical()
            || !self.target_pipeline.is_canonical()
            || self.parent_pipeline == self.target_pipeline
            || !is_digest(&self.environment_sha256)
            || self.statements.is_empty()
            || self.statements.len() > 64
            || self.statements.iter().any(|sql| sql.trim().is_empty())
            || sql_bytes.is_none_or(|bytes| bytes > 256 * 1024)
            || self.objects.is_empty()
            || self.objects.len() > 256
            || !self.objects.windows(2).all(|pair| {
                (&pair[0].name, pair[0].catalog_generation)
                    < (&pair[1].name, pair[1].catalog_generation)
            })
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
                (
                    ClusterTopologyObjectTransition::Remove,
                    CatalogObjectKind::Source | CatalogObjectKind::Stream | CatalogObjectKind::Sink,
                ) => TopologyInitialization::RetireAtCut,
                (ClusterTopologyObjectTransition::Remove, _) => {
                    return Err(TopologyError::Unsupported(
                        "only sources, streams and sinks have a certified removal contract".into(),
                    ))
                }
                (_, CatalogObjectKind::Source) => {
                    TopologyInitialization::ResolveSourcePositionsOnce
                }
                (_, CatalogObjectKind::Stream) if object.managed_state_contract.is_some() => {
                    TopologyInitialization::EmptyManagedStateAtCut
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
                        || !self.objects.iter().any(|dependency| {
                            &dependency.name == name
                                && if object.transition == ClusterTopologyObjectTransition::Remove {
                                    dependency.transition
                                        != ClusterTopologyObjectTransition::AddFutureOnly
                                } else {
                                    dependency.transition != ClusterTopologyObjectTransition::Remove
                                }
                        })
                })
                || object
                    .managed_state_contract
                    .as_ref()
                    .is_some_and(|contract| {
                        contract.is_empty()
                            || contract.len() > 128
                            || object.kind != CatalogObjectKind::Stream
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
