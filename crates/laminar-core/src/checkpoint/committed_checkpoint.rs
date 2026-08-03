//! Immutable global index for one committed checkpoint cut.

#![allow(clippy::disallowed_types)] // cold-path canonical checkpoint metadata

use std::collections::BTreeMap;

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{
    classify_channel_progress, ChannelProgress, CheckpointAssignmentFence, CheckpointManifest,
    ConnectorCheckpoint, PipelineIdentity,
};
use crate::state::{KeyGroupCount, LOCAL_NODE_ID};

/// Current committed-checkpoint index format.
pub const COMMITTED_CHECKPOINT_INDEX_VERSION: u32 = 1;

/// Hard bound on one canonical committed-checkpoint index.
pub const MAX_COMMITTED_CHECKPOINT_INDEX_BYTES: usize = 8 * 1024 * 1024;

/// Recovery domain covered by a committed checkpoint.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointScope {
    /// One process owns the complete vnode domain.
    Local,
    /// An assignment-fenced participant set owns the vnode domain.
    Cluster,
}

/// Event-time position of a complete checkpoint cut.
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "state", content = "watermark_ms")]
pub enum CheckpointWatermark {
    /// No active input has established a frontier.
    #[default]
    Uninitialized,
    /// Every channel in the cut is explicitly idle.
    Idle,
    /// Minimum watermark across active initialized channels.
    Active(i64),
}

impl CheckpointWatermark {
    /// Fold two participant watermarks into a safe cluster status.
    #[must_use]
    pub const fn cluster_min(self, other: Self) -> Self {
        match (self, other) {
            (Self::Uninitialized, _) | (_, Self::Uninitialized) => Self::Uninitialized,
            (Self::Idle, Self::Idle) => Self::Idle,
            (Self::Idle, Self::Active(value)) | (Self::Active(value), Self::Idle) => {
                Self::Active(value)
            }
            (Self::Active(left), Self::Active(right)) => {
                Self::Active(if left < right { left } else { right })
            }
        }
    }

    /// Numeric frontier for an active cut.
    #[must_use]
    pub const fn active_value(self) -> Option<i64> {
        match self {
            Self::Active(value) => Some(value),
            Self::Uninitialized | Self::Idle => None,
        }
    }

    /// Reject the reserved uninitialized sentinel.
    ///
    /// # Errors
    /// Returns an error when an active watermark uses the reserved sentinel.
    pub fn validate(self) -> Result<(), String> {
        if self == Self::Active(i64::MIN) {
            return Err("active checkpoint watermark cannot use the uninitialized sentinel".into());
        }
        Ok(())
    }
}

/// Exact immutable participant objects admitted into a committed cut.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedParticipantRef {
    /// Stable participant node identifier.
    pub participant_id: u64,
    /// Exact persisted manifest length.
    pub manifest_len: u64,
    /// SHA-256 of the exact persisted manifest bytes.
    pub manifest_sha256: String,
    /// Exact immutable node-data object length.
    pub node_data_len: u64,
    /// SHA-256 of the complete immutable node-data object.
    pub node_data_sha256: String,
}

impl CommittedParticipantRef {
    /// Bind an already validated manifest and its exact persisted bytes.
    ///
    /// # Errors
    /// Returns an error if the manifest bytes do not encode the supplied manifest.
    pub fn from_manifest(
        manifest: &CheckpointManifest,
        persisted_manifest: &[u8],
    ) -> Result<Self, String> {
        let decoded: CheckpointManifest = serde_json::from_slice(persisted_manifest)
            .map_err(|error| format!("participant manifest is not valid JSON: {error}"))?;
        if decoded != *manifest {
            return Err("persisted participant manifest differs from the supplied manifest".into());
        }
        let reference = Self {
            participant_id: manifest.participant_id,
            manifest_len: u64::try_from(persisted_manifest.len())
                .map_err(|_| "participant manifest length overflowed".to_owned())?,
            manifest_sha256: sha256_hex(persisted_manifest),
            node_data_len: manifest.node_data.object_length,
            node_data_sha256: manifest.node_data.sha256.clone(),
        };
        reference.validate()?;
        Ok(reference)
    }

    /// Validate the reference encoding.
    ///
    /// # Errors
    /// Returns an error when an identifier, length, or digest is invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.participant_id == 0 {
            return Err("committed participant ID cannot be zero".into());
        }
        if self.manifest_len == 0 {
            return Err("committed participant manifest cannot be empty".into());
        }
        validate_digest("participant manifest", &self.manifest_sha256)?;
        validate_digest("participant node data", &self.node_data_sha256)?;
        Ok(())
    }

    /// Verify one loaded manifest against this exact reference.
    ///
    /// # Errors
    /// Returns an error when the manifest or its bytes differ from this reference.
    pub fn verify_manifest(
        &self,
        manifest: &CheckpointManifest,
        persisted_manifest: &[u8],
    ) -> Result<(), String> {
        self.validate()?;
        if u64::try_from(persisted_manifest.len()).unwrap_or(u64::MAX) != self.manifest_len
            || sha256_hex(persisted_manifest) != self.manifest_sha256
        {
            return Err(format!(
                "participant {} manifest length or digest differs from the committed index",
                self.participant_id
            ));
        }
        let decoded: CheckpointManifest = serde_json::from_slice(persisted_manifest)
            .map_err(|error| format!("participant manifest is not valid JSON: {error}"))?;
        if decoded != *manifest {
            return Err(format!(
                "participant {} decoded manifest differs from its persisted bytes",
                self.participant_id
            ));
        }
        if manifest.participant_id != self.participant_id
            || manifest.node_data.chunk.participant_id != self.participant_id
            || manifest.node_data.object_length != self.node_data_len
            || manifest.node_data.sha256 != self.node_data_sha256
        {
            return Err(format!(
                "participant {} manifest does not match its committed object reference",
                self.participant_id
            ));
        }
        Ok(())
    }
}

/// Content-addressed reference carried by a terminal Commit outcome.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedCheckpointRef {
    /// Exact epoch encoded by the index and its object path.
    pub epoch: u64,
    /// Exact checkpoint ID encoded by the index and its object path.
    pub checkpoint_id: u64,
    /// SHA-256 of the exact canonical index body.
    pub sha256: String,
    /// Exact canonical index body length.
    pub len: u64,
}

impl CommittedCheckpointRef {
    /// Validate the reference encoding.
    ///
    /// # Errors
    /// Returns an error when the attempt, digest, or encoded length is invalid.
    pub fn validate(&self) -> Result<(), String> {
        validate_attempt(
            self.epoch,
            self.checkpoint_id,
            "committed checkpoint reference",
        )?;
        validate_digest("committed checkpoint", &self.sha256)?;
        if self.len == 0
            || self.len > u64::try_from(MAX_COMMITTED_CHECKPOINT_INDEX_BYTES).unwrap_or(u64::MAX)
        {
            return Err(format!(
                "committed checkpoint index length {} is outside 1..={MAX_COMMITTED_CHECKPOINT_INDEX_BYTES}",
                self.len
            ));
        }
        Ok(())
    }
}

/// Canonical global recovery index selected by one Commit outcome.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedCheckpointIndex {
    /// Exact index format.
    pub version: u32,
    /// Durable deployment incarnation owning the cut.
    pub deployment_id: String,
    /// Logical pipeline and state ABI identity.
    pub pipeline_identity: PipelineIdentity,
    /// Terminal epoch.
    pub epoch: u64,
    /// Exact checkpoint ID; equal to `epoch`.
    pub checkpoint_id: u64,
    /// Recovery domain covered by the cut.
    pub scope: CheckpointScope,
    /// Exact vnode domain used by every participant manifest.
    pub vnode_count: u16,
    /// Exact cluster assignment; absent in local mode.
    pub assignment_fence: Option<CheckpointAssignmentFence>,
    /// Direct prior committed index used for explicit, LIST-free retention traversal.
    pub predecessor: Option<CommittedCheckpointRef>,
    /// Canonically sorted exact participant objects.
    pub participants: Vec<CommittedParticipantRef>,
    /// Complete merged connector source cut.
    pub source_offsets: BTreeMap<String, ConnectorCheckpoint>,
    /// Complete merged per-channel event-time progress.
    pub channel_progress: Vec<ChannelProgress>,
    /// Safe event-time frontier at this cut.
    pub checkpoint_watermark: Option<i64>,
}

impl CommittedCheckpointIndex {
    /// Validate canonical shape and cross-field invariants.
    ///
    /// # Errors
    /// Returns an error when the index is not a complete canonical checkpoint cut.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != COMMITTED_CHECKPOINT_INDEX_VERSION {
            return Err(format!(
                "unsupported committed checkpoint index version {}; expected {COMMITTED_CHECKPOINT_INDEX_VERSION}",
                self.version
            ));
        }
        validate_attempt(self.epoch, self.checkpoint_id, "committed checkpoint index")?;
        validate_deployment(&self.deployment_id)?;
        if !self.pipeline_identity.is_canonical() {
            return Err("committed checkpoint pipeline identity is not canonical".into());
        }
        KeyGroupCount::try_from(u32::from(self.vnode_count))
            .map_err(|_| "committed checkpoint vnode count is invalid".to_owned())?;

        if self.participants.is_empty() {
            return Err("committed checkpoint has no participants".into());
        }
        for participant in &self.participants {
            participant.validate()?;
        }
        if !self
            .participants
            .windows(2)
            .all(|pair| pair[0].participant_id < pair[1].participant_id)
        {
            return Err(
                "committed participants must be sorted and unique by participant ID".into(),
            );
        }

        match (self.scope, self.assignment_fence.as_ref()) {
            (CheckpointScope::Local, None)
                if self.participants.len() == 1
                    && self.participants[0].participant_id == LOCAL_NODE_ID.0 => {}
            (CheckpointScope::Local, None) => {
                return Err("local committed checkpoint requires only LOCAL_NODE_ID".into());
            }
            (CheckpointScope::Local, Some(_)) => {
                return Err("local committed checkpoint cannot carry an assignment fence".into());
            }
            (CheckpointScope::Cluster, Some(fence)) => {
                if !fence.is_canonical() || fence.vnode_count != u32::from(self.vnode_count) {
                    return Err(
                        "cluster committed checkpoint has an invalid assignment fence".into(),
                    );
                }
                let actual: Vec<_> = self
                    .participants
                    .iter()
                    .map(|participant| participant.participant_id)
                    .collect();
                if actual != fence.participant_ids() {
                    return Err(
                        "committed participants do not exactly cover the assignment fence".into(),
                    );
                }
            }
            (CheckpointScope::Cluster, None) => {
                return Err("cluster committed checkpoint requires an assignment fence".into());
            }
        }

        if let Some(predecessor) = &self.predecessor {
            predecessor.validate()?;
            if predecessor.epoch >= self.epoch || predecessor.checkpoint_id >= self.checkpoint_id {
                return Err("committed checkpoint predecessor must be strictly older".into());
            }
        }

        validate_sources(&self.source_offsets, self.assignment_fence.as_ref())?;
        validate_channel_progress(&self.channel_progress, self.checkpoint_watermark)?;
        if self.channel_progress.iter().any(|channel| {
            self.participants
                .binary_search_by_key(&channel.participant_id, |participant| {
                    participant.participant_id
                })
                .is_err()
        }) {
            return Err("channel progress names a participant outside the committed cut".into());
        }
        Ok(())
    }

    /// Validate metadata continuity for an explicitly loaded predecessor index.
    ///
    /// # Errors
    /// Returns an error when either index is invalid or continuity is broken.
    pub fn validate_predecessor_index(
        &self,
        predecessor: &CommittedCheckpointIndex,
    ) -> Result<(), String> {
        self.validate()?;
        predecessor.validate()?;
        let expected = self
            .predecessor
            .as_ref()
            .ok_or_else(|| "committed checkpoint does not declare a predecessor".to_owned())?;
        let (_, actual) = predecessor.encode_and_reference()?;
        if &actual != expected
            || predecessor.deployment_id != self.deployment_id
            || predecessor.pipeline_identity != self.pipeline_identity
            || predecessor.scope != self.scope
            || predecessor.vnode_count != self.vnode_count
        {
            return Err("committed checkpoint predecessor breaks recovery continuity".into());
        }
        Ok(())
    }

    /// Verify exact participant manifest bytes and complete, exclusive vnode ownership.
    ///
    /// # Errors
    /// Returns an error when the manifests do not exactly represent this committed cut.
    pub fn validate_participant_manifests(
        &self,
        manifests: &[(&CheckpointManifest, &[u8])],
    ) -> Result<(), String> {
        self.validate()?;
        if manifests.len() != self.participants.len() {
            return Err("participant manifest count differs from the committed index".into());
        }

        let key_group_count = KeyGroupCount::try_from(u32::from(self.vnode_count))
            .map_err(|_| "committed checkpoint vnode count is invalid".to_owned())?;
        let mut owners = vec![None; usize::from(self.vnode_count)];
        let mut source_names = None;
        let mut sink_names = None;
        for ((manifest, encoded), reference) in manifests.iter().zip(&self.participants) {
            reference.verify_manifest(manifest, encoded)?;
            let errors = manifest.validate(key_group_count);
            if let Some(error) = errors.first() {
                return Err(format!(
                    "participant {} manifest is invalid: {error}",
                    reference.participant_id
                ));
            }
            if manifest.checkpoint_id != self.checkpoint_id
                || manifest.epoch != self.epoch
                || manifest.deployment_id != self.deployment_id
                || manifest.pipeline_identity != self.pipeline_identity
                || manifest.vnode_count != self.vnode_count
                || manifest.assignment_fence != self.assignment_fence
            {
                return Err(format!(
                    "participant {} manifest belongs to a different checkpoint cut",
                    reference.participant_id
                ));
            }

            match source_names {
                Some(expected) if expected != manifest.source_names.as_slice() => {
                    return Err(
                        "participant manifests disagree on the registered source topology".into(),
                    );
                }
                None => source_names = Some(manifest.source_names.as_slice()),
                Some(_) => {}
            }
            match sink_names {
                Some(expected) if expected != manifest.sink_names.as_slice() => {
                    return Err(
                        "participant manifests disagree on the registered sink topology".into(),
                    );
                }
                None => sink_names = Some(manifest.sink_names.as_slice()),
                Some(_) => {}
            }
            for vnode in &manifest.owned_vnodes {
                let owner = owners
                    .get_mut(usize::from(*vnode))
                    .ok_or_else(|| format!("manifest owns out-of-range vnode {vnode}"))?;
                if owner.replace(manifest.participant_id).is_some() {
                    return Err(format!(
                        "vnode {vnode} is owned by more than one participant"
                    ));
                }
            }
        }
        if owners.iter().any(Option::is_none) {
            return Err("participant manifests do not exactly cover the vnode domain".into());
        }
        if let Some(fence) = &self.assignment_fence {
            let owner_map = owners.into_iter().flatten().collect::<Vec<_>>();
            if !fence.matches_owner_map(&owner_map) {
                return Err(
                    "committed manifest vnode owners do not match the assignment fence".into(),
                );
            }
        }
        Ok(())
    }

    /// Encode the canonical index and derive its exact content reference.
    ///
    /// # Errors
    /// Returns an error when the index is invalid, cannot be encoded, or exceeds its bound.
    pub fn encode_and_reference(&self) -> Result<(Vec<u8>, CommittedCheckpointRef), String> {
        self.validate()?;
        let encoded = canonical_json_bytes(self).map_err(|error| error.to_string())?;
        if encoded.len() > MAX_COMMITTED_CHECKPOINT_INDEX_BYTES {
            return Err(format!(
                "committed checkpoint index is {} bytes; maximum is {MAX_COMMITTED_CHECKPOINT_INDEX_BYTES}",
                encoded.len()
            ));
        }
        let reference = CommittedCheckpointRef {
            epoch: self.epoch,
            checkpoint_id: self.checkpoint_id,
            sha256: sha256_hex(&encoded),
            len: u64::try_from(encoded.len())
                .map_err(|_| "committed checkpoint index length overflowed".to_owned())?,
        };
        reference.validate()?;
        Ok((encoded, reference))
    }
}

fn validate_sources(
    sources: &BTreeMap<String, ConnectorCheckpoint>,
    fence: Option<&CheckpointAssignmentFence>,
) -> Result<(), String> {
    for (source, checkpoint) in sources {
        validate_name("source", source)?;
        for key in checkpoint.offsets.keys() {
            validate_name("source offset key", key)?;
        }
        for key in checkpoint.metadata.keys() {
            validate_name("source metadata key", key)?;
        }
        if let (Some(version), Some(fence)) = (checkpoint.source_assignment_version, fence) {
            if version.get() != fence.assignment_version {
                return Err(format!(
                    "source '{source}' assignment version is {}; expected {}",
                    version, fence.assignment_version
                ));
            }
        }
    }
    Ok(())
}

fn validate_channel_progress(
    channels: &[ChannelProgress],
    checkpoint_watermark: Option<i64>,
) -> Result<(), String> {
    for channel in channels {
        if channel.participant_id == 0 {
            return Err("channel progress participant ID cannot be zero".into());
        }
        validate_name("channel", &channel.channel_id)?;
    }
    if !channels.windows(2).all(|pair| {
        (&pair[0].participant_id, &pair[0].channel_id)
            < (&pair[1].participant_id, &pair[1].channel_id)
    }) {
        return Err(
            "channel progress must be sorted and unique by participant and channel ID".into(),
        );
    }
    let classification = classify_channel_progress(channels)?;
    if checkpoint_watermark != classification.active_value() {
        return Err("checkpoint watermark does not match channel progress".into());
    }
    Ok(())
}

fn validate_attempt(epoch: u64, checkpoint_id: u64, kind: &str) -> Result<(), String> {
    if epoch == 0 || checkpoint_id != epoch {
        return Err(format!(
            "{kind} must use one nonzero canonical checkpoint ID"
        ));
    }
    Ok(())
}

fn validate_deployment(value: &str) -> Result<(), String> {
    let deployment = uuid::Uuid::parse_str(value)
        .map_err(|error| format!("committed checkpoint deployment identity is invalid: {error}"))?;
    if deployment.is_nil() || deployment.to_string() != value {
        return Err(
            "committed checkpoint deployment identity must be canonical and non-nil".into(),
        );
    }
    Ok(())
}

fn validate_name(kind: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.trim() != value {
        return Err(format!(
            "committed checkpoint has a non-canonical {kind} name"
        ));
    }
    Ok(())
}

fn validate_digest(kind: &str, value: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{kind} SHA-256 must be 64 lowercase hexadecimal characters"
        ));
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Serialize)]
#[serde(untagged)]
enum CanonicalJsonValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

impl From<serde_json::Value> for CanonicalJsonValue {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(value) => Self::Bool(value),
            serde_json::Value::Number(value) => Self::Number(value),
            serde_json::Value::String(value) => Self::String(value),
            serde_json::Value::Array(values) => {
                Self::Array(values.into_iter().map(Self::from).collect())
            }
            serde_json::Value::Object(values) => Self::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Self::from(value)))
                    .collect(),
            ),
        }
    }
}

/// Serialize JSON with recursively ordered object keys.
///
/// # Errors
/// Returns an error when the value cannot be represented as JSON.
pub fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    serde_json::to_vec(&CanonicalJsonValue::from(value))
}

/// SHA-256 of canonical JSON, encoded as lowercase hexadecimal.
///
/// # Errors
/// Returns an error when the value cannot be represented as JSON.
pub fn canonical_json_sha256<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    canonical_json_bytes(value).map(|bytes| sha256_hex(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{checkpoint_manifest_bytes, checkpoint_sha256, CheckpointParticipant};

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn local_index() -> CommittedCheckpointIndex {
        CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: uuid::Uuid::new_v4().to_string(),
            pipeline_identity: PipelineIdentity::empty(),
            epoch: 7,
            checkpoint_id: 7,
            scope: CheckpointScope::Local,
            vnode_count: 4,
            assignment_fence: None,
            predecessor: None,
            participants: vec![CommittedParticipantRef {
                participant_id: LOCAL_NODE_ID.0,
                manifest_len: 100,
                manifest_sha256: digest(1),
                node_data_len: 0,
                node_data_sha256: digest(2),
            }],
            source_offsets: BTreeMap::new(),
            channel_progress: vec![ChannelProgress {
                participant_id: LOCAL_NODE_ID.0,
                channel_id: "source/orders".into(),
                watermark: Some(42),
                idle: false,
            }],
            checkpoint_watermark: Some(42),
        }
    }

    fn cluster_cut() -> (CommittedCheckpointIndex, Vec<CheckpointManifest>) {
        let key_groups = KeyGroupCount::try_from(2_u16).unwrap();
        let participants = vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ];
        let fence = CheckpointAssignmentFence::from_owner_map(1, &[1, 2], participants).unwrap();
        let deployment_id = uuid::Uuid::from_u128(3).to_string();
        let manifests = [1_u64, 2]
            .into_iter()
            .enumerate()
            .map(|(vnode, participant_id)| {
                let mut manifest = CheckpointManifest::new_with_key_group_count(7, 7, key_groups);
                manifest.bind_participant(participant_id);
                manifest.deployment_id = deployment_id.clone();
                manifest.assignment_fence = Some(fence.clone());
                manifest.owned_vnodes = vec![u16::try_from(vnode).unwrap()];
                manifest.source_names = vec!["source".into()];
                manifest.sink_names = vec!["sink".into()];
                manifest.node_data.sha256 = checkpoint_sha256(b"");
                manifest
            })
            .collect::<Vec<_>>();
        let mut index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id,
            pipeline_identity: PipelineIdentity::empty(),
            epoch: 7,
            checkpoint_id: 7,
            scope: CheckpointScope::Cluster,
            vnode_count: 2,
            assignment_fence: Some(fence),
            predecessor: None,
            participants: Vec::new(),
            source_offsets: BTreeMap::new(),
            channel_progress: Vec::new(),
            checkpoint_watermark: None,
        };
        bind_manifests(&mut index, &manifests);
        (index, manifests)
    }

    fn bind_manifests(
        index: &mut CommittedCheckpointIndex,
        manifests: &[CheckpointManifest],
    ) -> Vec<Vec<u8>> {
        let encoded = manifests
            .iter()
            .map(|manifest| checkpoint_manifest_bytes(manifest).unwrap())
            .collect::<Vec<_>>();
        index.participants = manifests
            .iter()
            .zip(&encoded)
            .map(|(manifest, bytes)| {
                CommittedParticipantRef::from_manifest(manifest, bytes).unwrap()
            })
            .collect();
        encoded
    }

    fn validate_manifests(
        index: &CommittedCheckpointIndex,
        manifests: &[CheckpointManifest],
        encoded: &[Vec<u8>],
    ) -> Result<(), String> {
        let views = manifests
            .iter()
            .zip(encoded)
            .map(|(manifest, bytes)| (manifest, bytes.as_slice()))
            .collect::<Vec<_>>();
        index.validate_participant_manifests(&views)
    }

    #[test]
    fn local_index_requires_the_single_local_participant() {
        let mut index = local_index();
        assert!(index.validate().is_ok());
        index.participants[0].participant_id = 9;
        assert!(index.validate().is_err());
    }

    #[test]
    fn reference_binds_exact_canonical_bytes() {
        let index = local_index();
        let (bytes, reference) = index.encode_and_reference().unwrap();
        assert_eq!(reference.len, bytes.len() as u64);
        assert_eq!(reference.sha256, sha256_hex(&bytes));

        let mut changed = index;
        changed.checkpoint_watermark = None;
        changed.channel_progress[0].watermark = None;
        let (_, changed_reference) = changed.encode_and_reference().unwrap();
        assert_ne!(reference.sha256, changed_reference.sha256);
    }

    #[test]
    fn entirely_uninitialized_channels_have_no_watermark() {
        let mut index = local_index();
        index.channel_progress[0].watermark = None;
        assert!(index.validate().is_err());
        index.checkpoint_watermark = None;
        assert!(index.validate().is_ok());
    }

    #[test]
    fn participant_vnode_owners_must_match_the_assignment_fence() {
        let (mut index, mut manifests) = cluster_cut();
        manifests[0].owned_vnodes = vec![1];
        manifests[1].owned_vnodes = vec![0];
        let encoded = bind_manifests(&mut index, &manifests);

        let error = validate_manifests(&index, &manifests, &encoded).unwrap_err();
        assert!(error.contains("vnode owners do not match the assignment fence"));
    }

    #[test]
    fn participant_source_and_sink_inventories_must_match() {
        let (mut index, mut manifests) = cluster_cut();
        manifests[1].source_names = vec!["other-source".into()];
        let encoded = bind_manifests(&mut index, &manifests);
        let error = validate_manifests(&index, &manifests, &encoded).unwrap_err();
        assert!(error.contains("source topology"));

        manifests[1].source_names = manifests[0].source_names.clone();
        manifests[1].sink_names = vec!["other-sink".into()];
        let encoded = bind_manifests(&mut index, &manifests);
        let error = validate_manifests(&index, &manifests, &encoded).unwrap_err();
        assert!(error.contains("sink topology"));
    }
}
