//! Exact-cut restore and retirement requirements. Staging grants no target authority.

use super::{
    ClusterTopologyObjectTransition, ClusterTopologyValidation, TopologyAdmissionPhase,
    TopologyAdmissionPlan, TopologyAdmissionStatus, TopologyCompatibilityRef, TopologyCutCommit,
    TopologyError, TopologyOperationId, TopologyPlanRef, TopologySourceInitialization,
};
use crate::checkpoint::{
    canonical_json_bytes, checkpoint_manifest_bytes, merge_node_subscription_manifests,
    CheckpointManifest, CommittedCheckpointIndex, OutputDistributionCertificate, PartitionFrontier,
    StateFrameKey,
};
use crate::cluster::control::CatalogObjectKind;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const MAX_TOPOLOGY_ROOT_BYTES: u64 = 1024 * 1024;
/// Aggregate manifest metadata admitted while staging a root; state/output payloads are not read.
pub const MAX_TOPOLOGY_ROOT_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

/// Same catalog incarnation and ABI mapped to the same checkpoint state slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyPreservedObject {
    /// Canonical catalog identity, never a traversal index.
    pub name: String,
    /// Source, stream or sink namespace.
    pub kind: CatalogObjectKind,
    /// Exact unchanged incarnation.
    pub catalog_generation: u64,
    /// Independently certified definition/schema/codec/dependency contract.
    pub compatibility_sha256: String,
    /// Exact existing graph frame identifier for streams; sources/sinks use the cut's inventories.
    pub state_operator_id: Option<String>,
}

/// Unchanged subscription identity and exclusive sequence vector at the old cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySubscriptionRoot {
    /// Original certificate and stream generation. Historical segments retain this certificate.
    pub parent_certificate: OutputDistributionCertificate,
    /// Required target certificate, differing only in the complete pipeline identity.
    /// Target installation must explicitly bind this preserved generation instead of deriving
    /// a new one from the changed whole-graph hash. This requirement is not an output permit.
    pub target_certificate: OutputDistributionCertificate,
    /// Exact next sequence in every output partition; never reset to the first sequence.
    pub frontiers: Vec<PartitionFrontier>,
}

/// Immutable target restore/initialization requirements, retained by one authority append.
/// All state ranges, source/snapshot/channel progress, sink decisions and replay segment references
/// remain in the exact old checkpoint. No historical manifest or state payload is rewritten.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyMigrationRoot {
    /// Root encoding, separate from logical topology, catalog and checkpoint versions.
    pub format_version: u16,
    /// Exact admitted request.
    pub operation_id: TopologyOperationId,
    /// Immutable admission payload.
    pub plan: TopologyPlanRef,
    /// Complete participant-certified compatibility descriptor.
    pub compatibility: TopologyCompatibilityRef,
    /// Definitive old-topology checkpoint and its shared Commit append.
    pub cut: TopologyCutCommit,
    /// Sorted exact identity mappings for every unchanged object.
    pub preserved_objects: Vec<TopologyPreservedObject>,
    /// Sorted new objects. Streams/sinks receive only target input; sources use their sealed cursors.
    pub future_only_objects: Vec<String>,
    /// Sorted preserved subscription incarnations and publication frontiers.
    pub subscriptions: Vec<TopologySubscriptionRoot>,
    /// Sorted new-source positions, sealed once for this exact operation and cut.
    /// Empty format-1 roots retain their original canonical bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_initializations: Vec<TopologySourceInitialization>,
}

/// Small content reference carried by authority renewals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyMigrationRootRef {
    /// SHA-256 of the exact canonical root body.
    pub sha256: String,
    /// Exact length, bounded before reading or decoding.
    pub encoded_len: u64,
}

impl TopologyMigrationRootRef {
    pub(crate) fn validate(&self) -> Result<(), TopologyError> {
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.encoded_len == 0
            || self.encoded_len > MAX_TOPOLOGY_ROOT_BYTES
        {
            return Err(TopologyError::Invalid(
                "invalid migration root reference".into(),
            ));
        }
        Ok(())
    }
}

/// Durable staging evidence. Neither this binding nor `CutPrepared` commits the target catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyMigrationRootBinding {
    /// Immutable requirements derived from the certified descriptor and exact cut.
    pub root: TopologyMigrationRootRef,
    /// Shared append that first pinned the root.
    pub authority_sequence: u64,
}

impl TopologyMigrationRoot {
    /// Rebuild the exact requirements from checksummed historical manifests before target restore.
    /// This does not rewrite their parent identity or grant installation/output authority.
    ///
    /// # Errors
    /// Rejects missing, divergent or reordered state/progress/subscription mappings.
    pub fn validate_restore_cut(
        &self,
        operation: &TopologyAdmissionStatus,
        descriptor: &ClusterTopologyValidation,
        index: &CommittedCheckpointIndex,
        manifests: &[CheckpointManifest],
    ) -> Result<(), TopologyError> {
        let rebuilt = Self::build_with_sources(
            operation,
            descriptor,
            index,
            manifests,
            self.source_initializations.clone(),
        )?;
        if rebuilt != *self {
            return Err(TopologyError::Invalid(
                "restore metadata differs from the sealed migration root".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn object_mappings(
        descriptor: &ClusterTopologyValidation,
    ) -> Result<(Vec<TopologyPreservedObject>, Vec<String>), TopologyError> {
        let mut preserved = Vec::new();
        let mut future = Vec::new();
        for object in &descriptor.objects {
            match object.transition {
                ClusterTopologyObjectTransition::Preserve => {
                    preserved.push(TopologyPreservedObject {
                        name: object.name.clone(),
                        kind: object.kind,
                        catalog_generation: object.catalog_generation,
                        compatibility_sha256: object.compatibility_sha256.clone(),
                        state_operator_id: (object.kind == CatalogObjectKind::Stream)
                            .then(|| format!("graph:{}", object.name)),
                    });
                }
                ClusterTopologyObjectTransition::AddFutureOnly => {
                    if object.managed_state_contract.is_some()
                        && object.initialization
                            != super::TopologyInitialization::EmptyManagedStateAtCut
                    {
                        return Err(TopologyError::Unsupported(
                            "new managed state requires an explicit empty-state cut contract"
                                .into(),
                        ));
                    }
                    future.push(object.name.clone());
                }
                ClusterTopologyObjectTransition::Remove => {
                    if !matches!(
                        object.kind,
                        CatalogObjectKind::Source
                            | CatalogObjectKind::Stream
                            | CatalogObjectKind::Sink
                    ) || object.initialization != super::TopologyInitialization::RetireAtCut
                    {
                        return Err(TopologyError::Unsupported(
                            "removed objects must retire at the checkpoint cut".into(),
                        ));
                    }
                }
            }
        }
        Ok((preserved, future))
    }

    pub(crate) fn build(
        operation: &TopologyAdmissionStatus,
        descriptor: &ClusterTopologyValidation,
        index: &CommittedCheckpointIndex,
        manifests: &[CheckpointManifest],
    ) -> Result<Self, TopologyError> {
        Self::build_with_sources(operation, descriptor, index, manifests, Vec::new())
    }

    pub(crate) fn build_with_sources(
        operation: &TopologyAdmissionStatus,
        descriptor: &ClusterTopologyValidation,
        index: &CommittedCheckpointIndex,
        manifests: &[CheckpointManifest],
        source_initializations: Vec<TopologySourceInitialization>,
    ) -> Result<Self, TopologyError> {
        let cut = operation
            .cut
            .as_ref()
            .and_then(|cut| cut.committed.as_ref())
            .ok_or_else(|| {
                TopologyError::Conflict("root staging requires a definitive old cut".into())
            })?;
        if !matches!(
            operation.phase,
            TopologyAdmissionPhase::CutPrepared
                | TopologyAdmissionPhase::Committed
                | TopologyAdmissionPhase::Activating
                | TopologyAdmissionPhase::Active
        ) || index
            .encode_and_reference()
            .map_err(TopologyError::Invalid)?
            .1
            != cut.checkpoint
            || index.pipeline_identity != descriptor.parent_pipeline
            || index.deployment_id != descriptor.deployment_id
        {
            return Err(TopologyError::Conflict(
                "root does not bind the certified prepared cut".into(),
            ));
        }
        let encoded = manifests
            .iter()
            .map(checkpoint_manifest_bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TopologyError::Invalid(e.to_string()))?;
        let views = manifests
            .iter()
            .zip(&encoded)
            .map(|(m, b)| (m, b.as_slice()))
            .collect::<Vec<_>>();
        index
            .validate_participant_manifests(&views)
            .map_err(TopologyError::Invalid)?;
        let (preserved_objects, future_only_objects) = Self::object_mappings(descriptor)?;
        Self::validate_parent_inventory(index, manifests, descriptor)?;
        Self::validate_parent_state(manifests, descriptor)?;
        let subscriptions = Self::subscription_mappings(index, manifests, descriptor)?;
        let root = Self {
            format_version: if source_initializations.is_empty() {
                1
            } else {
                2
            },
            operation_id: operation.operation_id,
            plan: operation.plan.clone(),
            compatibility: operation
                .preparation
                .as_ref()
                .ok_or_else(|| TopologyError::Protocol("root has no preparation".into()))?
                .compatibility
                .clone(),
            cut: cut.clone(),
            preserved_objects,
            future_only_objects,
            subscriptions,
            source_initializations,
        };
        root.validate_source_mappings(descriptor)?;
        root.encode_and_reference()?;
        Ok(root)
    }

    fn validate_parent_state(
        manifests: &[CheckpointManifest],
        descriptor: &ClusterTopologyValidation,
    ) -> Result<(), TopologyError> {
        for manifest in manifests {
            for frame in &manifest.state_frames {
                let (operator_id, vnode) = match &frame.key {
                    StateFrameKey::OperatorWhole { operator_id } => (operator_id, false),
                    StateFrameKey::Vnode { operator_id, .. } => (operator_id, true),
                };
                let object = descriptor
                    .objects
                    .iter()
                    .find(|o| {
                        o.kind == CatalogObjectKind::Stream
                            && o.transition != ClusterTopologyObjectTransition::AddFutureOnly
                            && operator_id.strip_prefix("graph:") == Some(o.name.as_str())
                    })
                    .ok_or_else(|| {
                        TopologyError::Unsupported(format!(
                            "cut frame '{operator_id}' has no certified parent state mapping"
                        ))
                    })?;
                if vnode && object.managed_state_contract.is_none() {
                    return Err(TopologyError::Invalid(format!(
                        "unmanaged stream '{}' has vnode state",
                        object.name
                    )));
                }
            }
        }
        for object in descriptor.objects.iter().filter(|o| {
            o.transition != ClusterTopologyObjectTransition::AddFutureOnly
                && o.managed_state_contract.is_some()
        }) {
            if !manifests.iter().any(|m| {
                m.state_frames.iter().any(|frame| {
                    matches!(&frame.key, StateFrameKey::Vnode { operator_id, .. }
                    if operator_id.strip_prefix("graph:") == Some(object.name.as_str()))
                })
            }) {
                return Err(TopologyError::Invalid(format!(
                    "managed stream '{}' has no parent vnode state; a complete cut is required",
                    object.name
                )));
            }
        }
        Ok(())
    }

    fn subscription_mappings(
        index: &CommittedCheckpointIndex,
        manifests: &[CheckpointManifest],
        descriptor: &ClusterTopologyValidation,
    ) -> Result<Vec<TopologySubscriptionRoot>, TopologyError> {
        let subscription_views = manifests
            .iter()
            .filter_map(|m| {
                m.subscription_output
                    .as_ref()
                    .map(|s| (s, m.owned_vnodes.as_slice()))
            })
            .collect::<Vec<_>>();
        let merged = if subscription_views.is_empty() {
            Vec::new()
        } else {
            merge_node_subscription_manifests(
                index.epoch,
                index.checkpoint_id,
                index
                    .assignment_fence
                    .as_ref()
                    .ok_or_else(|| TopologyError::Invalid("root has no assignment".into()))?,
                &subscription_views,
            )
            .map_err(|e| TopologyError::Invalid(e.to_string()))?
        };
        let mut subscriptions = Vec::with_capacity(merged.len());
        for stream in merged {
            let parent_certificate = stream.manifest.distribution_certificate;
            let object = descriptor
                .objects
                .iter()
                .find(|o| {
                    o.name == parent_certificate.stream_id
                        && o.kind == CatalogObjectKind::Stream
                        && o.transition != ClusterTopologyObjectTransition::AddFutureOnly
                })
                .ok_or_else(|| {
                    TopologyError::Invalid("subscription has no parent stream mapping".into())
                })?;
            if object.catalog_generation != parent_certificate.catalog_generation
                || object.schema_sha256.as_deref()
                    != Some(parent_certificate.schema_fingerprint.to_hex().as_str())
                || parent_certificate.final_operator_id != format!("stream:{}", object.name)
            {
                return Err(TopologyError::Invalid(
                    "subscription incarnation/schema/operator differs from the descriptor".into(),
                ));
            }
            if object.transition == ClusterTopologyObjectTransition::Remove {
                continue;
            }
            let mut target_certificate = parent_certificate.clone();
            target_certificate.pipeline_identity = descriptor.target_pipeline.clone();
            subscriptions.push(TopologySubscriptionRoot {
                parent_certificate,
                target_certificate,
                frontiers: stream.manifest.frontiers,
            });
        }
        Ok(subscriptions)
    }

    fn validate_parent_inventory(
        index: &CommittedCheckpointIndex,
        manifests: &[CheckpointManifest],
        descriptor: &ClusterTopologyValidation,
    ) -> Result<(), TopologyError> {
        let names = |kind| {
            descriptor
                .objects
                .iter()
                .filter(|o| {
                    o.kind == kind && o.transition != ClusterTopologyObjectTransition::AddFutureOnly
                })
                .map(|o| o.name.clone())
                .collect::<Vec<_>>()
        };
        if index.source_names != names(CatalogObjectKind::Source)
            || manifests
                .iter()
                .any(|m| m.sink_names != names(CatalogObjectKind::Sink))
        {
            return Err(TopologyError::Invalid(
                "cut source/sink inventory differs from the complete parent catalog".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_binding(
        &self,
        operation: &TopologyAdmissionStatus,
        plan: &TopologyAdmissionPlan,
        descriptor: &ClusterTopologyValidation,
    ) -> Result<(), TopologyError> {
        self.validate_source_mappings(descriptor)?;
        let (preserved, future) = Self::object_mappings(descriptor)?;
        if self.operation_id != operation.operation_id
            || self.plan != operation.plan
            || plan.compatibility.as_ref() != Some(&self.compatibility)
            || operation
                .cut
                .as_ref()
                .and_then(|cut| cut.committed.as_ref())
                != Some(&self.cut)
            || self.preserved_objects != preserved
            || self.future_only_objects != future
            || self.subscriptions.iter().any(|s| {
                s.parent_certificate.pipeline_identity != descriptor.parent_pipeline
                    || s.target_certificate.pipeline_identity != descriptor.target_pipeline
            })
        {
            return Err(TopologyError::Invalid(
                "migration root differs from its immutable plan and cut".into(),
            ));
        }
        let key_groups = crate::state::KeyGroupCount::try_from(plan.assignment.vnode_count)
            .map_err(|e| TopologyError::Invalid(e.to_string()))?;
        for subscription in &self.subscriptions {
            let certificate = &subscription.parent_certificate;
            certificate
                .validate(key_groups)
                .map_err(|e| TopologyError::Invalid(e.to_string()))?;
            subscription
                .target_certificate
                .validate(key_groups)
                .map_err(|e| TopologyError::Invalid(e.to_string()))?;
            let object = descriptor
                .objects
                .iter()
                .find(|o| {
                    o.name == certificate.stream_id
                        && o.kind == CatalogObjectKind::Stream
                        && o.transition == ClusterTopologyObjectTransition::Preserve
                })
                .ok_or_else(|| {
                    TopologyError::Invalid("root subscription has no preserved object".into())
                })?;
            if certificate.catalog_generation != object.catalog_generation
                || object.schema_sha256.as_deref()
                    != Some(certificate.schema_fingerprint.to_hex().as_str())
                || certificate.final_operator_id != format!("stream:{}", object.name)
                || subscription.frontiers.len()
                    != usize::from(certificate.distribution.partition_count())
                || subscription
                    .frontiers
                    .iter()
                    .any(|f| !certificate.distribution.contains(f.partition))
            {
                return Err(TopologyError::Invalid("root subscription does not preserve its certified incarnation/schema/partition vector".into()));
            }
        }
        Ok(())
    }

    fn validate_source_mappings(
        &self,
        descriptor: &ClusterTopologyValidation,
    ) -> Result<(), TopologyError> {
        let sources = descriptor
            .objects
            .iter()
            .filter(|object| {
                object.kind == CatalogObjectKind::Source
                    && object.transition == ClusterTopologyObjectTransition::AddFutureOnly
            })
            .collect::<Vec<_>>();
        if sources.len() != self.source_initializations.len() {
            return Err(TopologyError::Unsupported(
                "every new source requires concrete connector positions sealed once at the cut"
                    .into(),
            ));
        }
        for (object, initialization) in sources.iter().zip(&self.source_initializations) {
            initialization.validate()?;
            if initialization.name != object.name
                || initialization.catalog_generation != object.catalog_generation
                || initialization.compatibility_sha256 != object.compatibility_sha256
            {
                return Err(TopologyError::Invalid(
                    "source initialization differs from its certified catalog incarnation".into(),
                ));
            }
        }
        Ok(())
    }

    /// Canonical bounded body for durable storage. This encodes requirements, never authority.
    ///
    /// # Errors
    /// Rejects malformed identities, unordered mappings or a body above 1 MiB.
    pub fn encode_and_reference(
        &self,
    ) -> Result<(Vec<u8>, TopologyMigrationRootRef), TopologyError> {
        self.plan.validate()?;
        self.compatibility.validate()?;
        self.cut
            .checkpoint
            .validate()
            .map_err(TopologyError::Invalid)?;
        if !matches!(
            (self.format_version, self.source_initializations.is_empty()),
            (1, true) | (2, false)
        ) || self.cut.authority_sequence == 0
            || self.preserved_objects.len() + self.future_only_objects.len() > 256
            || !self
                .preserved_objects
                .windows(2)
                .all(|p| p[0].name < p[1].name)
            || !self.future_only_objects.windows(2).all(|p| p[0] < p[1])
            || self.subscriptions.len() > self.preserved_objects.len()
            || self.source_initializations.len() > self.future_only_objects.len()
            || !self
                .source_initializations
                .windows(2)
                .all(|p| p[0].name < p[1].name)
            || !self
                .subscriptions
                .windows(2)
                .all(|p| p[0].parent_certificate.stream_id < p[1].parent_certificate.stream_id)
        {
            return Err(TopologyError::Invalid(
                "invalid migration root shape".into(),
            ));
        }
        for initialization in &self.source_initializations {
            initialization.validate()?;
            if self
                .future_only_objects
                .binary_search(&initialization.name)
                .is_err()
            {
                return Err(TopologyError::Invalid(
                    "source initialization has no future object".into(),
                ));
            }
        }
        for subscription in &self.subscriptions {
            let mut expected = subscription.parent_certificate.clone();
            expected.pipeline_identity = subscription.target_certificate.pipeline_identity.clone();
            if expected != subscription.target_certificate
                || subscription.frontiers.is_empty()
                || !subscription
                    .frontiers
                    .windows(2)
                    .all(|p| p[0].partition < p[1].partition)
            {
                return Err(TopologyError::Invalid(
                    "root cannot change subscription identity or rewind its sequence vector".into(),
                ));
            }
        }
        let bytes =
            canonical_json_bytes(self).map_err(|e| TopologyError::Invalid(e.to_string()))?;
        let reference = TopologyMigrationRootRef {
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            encoded_len: bytes.len() as u64,
        };
        reference.validate()?;
        Ok((bytes, reference))
    }
}
