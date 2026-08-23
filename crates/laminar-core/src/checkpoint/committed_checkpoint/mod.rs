//! Immutable global index for one committed checkpoint cut.

#![allow(clippy::disallowed_types)] // cold-path canonical checkpoint metadata

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{
    channel_progress_frontiers_by_source, classify_channel_progress,
    merge_node_subscription_manifests, ChannelProgress, CheckpointAssignmentFence,
    CheckpointManifest, ConnectorCheckpoint, PipelineIdentity, SINGLETON_WATERMARK_CHANNEL,
};
use crate::state::{KeyGroupCount, LOCAL_NODE_ID};

/// Current committed-checkpoint index format.
pub const COMMITTED_CHECKPOINT_INDEX_VERSION: u32 = 4;
const LEGACY_COMMITTED_CHECKPOINT_INDEX_VERSION: u32 = 3;

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
    /// Whether every participant captured a cut portable across vnode reassignment.
    pub reassignment_portable: bool,
    /// Direct prior committed index used for explicit, LIST-free retention traversal.
    pub predecessor: Option<CommittedCheckpointRef>,
    /// Canonically sorted exact participant objects.
    pub participants: Vec<CommittedParticipantRef>,
    /// Canonically sorted registered source names.
    pub source_names: Vec<String>,
    /// Complete merged connector source cut.
    pub source_offsets: BTreeMap<String, ConnectorCheckpoint>,
    /// Complete merged per-channel event-time progress.
    pub channel_progress: Vec<ChannelProgress>,
    /// Monotonic decision frontier retained for every source, including sources whose current
    /// physical input-channel inventory is empty.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub source_watermarks: BTreeMap<String, i64>,
    /// Safe event-time frontier at this cut.
    pub checkpoint_watermark: Option<i64>,
}

impl CommittedCheckpointIndex {
    /// Validate canonical shape and cross-field invariants.
    ///
    /// # Errors
    /// Returns an error when the index is not a complete canonical checkpoint cut.
    pub fn validate(&self) -> Result<(), String> {
        if !matches!(
            self.version,
            LEGACY_COMMITTED_CHECKPOINT_INDEX_VERSION | COMMITTED_CHECKPOINT_INDEX_VERSION
        ) {
            return Err(format!(
                "unsupported committed checkpoint index version {}; expected {} or {COMMITTED_CHECKPOINT_INDEX_VERSION}",
                self.version, LEGACY_COMMITTED_CHECKPOINT_INDEX_VERSION
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
                    && self.participants[0].participant_id == LOCAL_NODE_ID.0
                    && !self.reassignment_portable => {}
            (CheckpointScope::Local, None) if self.reassignment_portable => {
                return Err(
                    "local committed checkpoint cannot claim vnode reassignment portability".into(),
                );
            }
            (CheckpointScope::Local, None) => {
                return Err("local committed checkpoint requires only LOCAL_NODE_ID".into());
            }
            (CheckpointScope::Local, Some(_)) => {
                return Err("local committed checkpoint cannot carry an assignment fence".into());
            }
            (CheckpointScope::Cluster, Some(fence)) => {
                if !self.reassignment_portable {
                    return Err(
                        "cluster committed checkpoint must be portable across vnode reassignment"
                            .into(),
                    );
                }
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

        validate_source_topology(&self.source_names, &self.source_offsets)?;
        validate_sources(&self.source_offsets, self.assignment_fence.as_ref())?;
        validate_channel_progress(
            &self.source_names,
            &self.channel_progress,
            self.checkpoint_watermark,
        )?;
        if self.version == LEGACY_COMMITTED_CHECKPOINT_INDEX_VERSION {
            if !self.source_watermarks.is_empty() {
                return Err("legacy committed checkpoint cannot carry source watermarks".into());
            }
        } else {
            validate_source_watermarks(
                &self.source_names,
                &self.channel_progress,
                &self.source_watermarks,
            )?;
            if self.predecessor.is_none() {
                let exact = channel_progress_frontiers_by_source(&self.channel_progress)?
                    .into_iter()
                    .filter_map(|(source, frontier)| {
                        frontier.map(|frontier| (source.to_owned(), frontier))
                    })
                    .collect::<BTreeMap<_, _>>();
                if self.source_watermarks != exact {
                    return Err(
                        "initial committed source watermarks do not exactly match channel progress"
                            .into(),
                    );
                }
            }
        }
        validate_channel_rosters(&self.source_offsets, &self.channel_progress)?;
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
        if self.version < predecessor.version {
            return Err(
                "committed checkpoint index version regresses across its predecessor".into(),
            );
        }
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
            || predecessor.source_names != self.source_names
        {
            return Err("committed checkpoint predecessor breaks recovery continuity".into());
        }
        if self.version == COMMITTED_CHECKPOINT_INDEX_VERSION {
            let mut expected = predecessor
                .effective_source_watermarks()?
                .into_iter()
                .filter(|(source, _)| self.source_names.binary_search(source).is_ok())
                .collect::<BTreeMap<_, _>>();
            for (source, frontier) in channel_progress_frontiers_by_source(&self.channel_progress)?
            {
                let Some(frontier) = frontier else {
                    continue;
                };
                if expected
                    .get(source)
                    .is_some_and(|predecessor| *predecessor > frontier)
                {
                    return Err(format!(
                        "committed source watermark for '{source}' regresses across its predecessor"
                    ));
                }
                expected.insert(source.to_owned(), frontier);
            }
            if self.source_watermarks != expected {
                return Err(
                    "committed source watermarks do not exactly continue their predecessor".into(),
                );
            }
        }
        Ok(())
    }

    /// Return the source-keyed decision cuts represented by this index.
    ///
    /// Version 3 encoded only physical channel progress, so its effective map is derived. Version
    /// 4 carries the cumulative map explicitly so an empty current inventory retains its cut.
    ///
    /// # Errors
    /// Returns an error when channel progress contains an invalid watermark sentinel.
    pub fn effective_source_watermarks(&self) -> Result<BTreeMap<String, i64>, String> {
        if self.version == LEGACY_COMMITTED_CHECKPOINT_INDEX_VERSION {
            return channel_progress_frontiers_by_source(&self.channel_progress).map(|frontiers| {
                frontiers
                    .into_iter()
                    .filter_map(|(source, frontier)| {
                        frontier.map(|frontier| (source.to_owned(), frontier))
                    })
                    .collect()
            });
        }
        Ok(self.source_watermarks.clone())
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
                || manifest.reassignment_portable != self.reassignment_portable
            {
                return Err(format!(
                    "participant {} manifest belongs to a different checkpoint cut",
                    reference.participant_id
                ));
            }

            if manifest.source_names != self.source_names {
                return Err(
                    "participant manifest source topology differs from the committed index".into(),
                );
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
            let subscription_manifests = manifests
                .iter()
                .filter_map(|(manifest, _)| manifest.subscription_output.as_ref())
                .collect::<Vec<_>>();
            if !subscription_manifests.is_empty() {
                if subscription_manifests.len() != manifests.len() {
                    return Err(
                        "participant manifests do not agree on subscription output presence".into(),
                    );
                }
                merge_node_subscription_manifests(
                    self.epoch,
                    self.checkpoint_id,
                    fence,
                    &subscription_manifests,
                )
                .map_err(|error| format!("committed subscription output is invalid: {error}"))?;
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

fn validate_source_topology(
    source_names: &[String],
    sources: &BTreeMap<String, ConnectorCheckpoint>,
) -> Result<(), String> {
    for source in source_names {
        validate_name("source", source)?;
    }
    if !source_names.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err("committed source names must be sorted and unique".into());
    }
    if let Some(source) = sources
        .keys()
        .find(|source| source_names.binary_search(source).is_err())
    {
        return Err(format!(
            "source offset '{source}' is absent from the committed source topology"
        ));
    }
    Ok(())
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
        if let Some(channels) = &checkpoint.input_channels {
            if channels.iter().any(Vec::is_empty)
                || !channels.windows(2).all(|pair| pair[0] < pair[1])
            {
                return Err(format!(
                    "source '{source}' input channels must contain non-empty, sorted unique identities"
                ));
            }
            if channels
                .iter()
                .any(|channel| channel == SINGLETON_WATERMARK_CHANNEL)
            {
                return Err(format!(
                    "source '{source}' input channels contain the reserved logical watermark channel"
                ));
            }
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
    source_names: &[String],
    channels: &[ChannelProgress],
    checkpoint_watermark: Option<i64>,
) -> Result<(), String> {
    for channel in channels {
        if channel.participant_id == 0 {
            return Err("channel progress participant ID cannot be zero".into());
        }
        validate_name("channel source", &channel.source_name)?;
        if source_names.binary_search(&channel.source_name).is_err() {
            return Err(format!(
                "channel progress source '{}' is absent from the committed source topology",
                channel.source_name
            ));
        }
        if channel.input_channel.is_empty() {
            return Err("channel progress input channel cannot be empty".into());
        }
    }
    if !channels.windows(2).all(|pair| {
        (
            &pair[0].participant_id,
            &pair[0].source_name,
            &pair[0].input_channel,
        ) < (
            &pair[1].participant_id,
            &pair[1].source_name,
            &pair[1].input_channel,
        )
    }) {
        return Err(
            "channel progress must be sorted and unique by participant, source, and input channel"
                .into(),
        );
    }
    let mut logical_sources = BTreeSet::new();
    let mut physical_sources = BTreeSet::new();
    let mut physical_channels = BTreeSet::new();
    for channel in channels {
        if channel.input_channel == SINGLETON_WATERMARK_CHANNEL {
            logical_sources.insert((channel.participant_id, channel.source_name.as_str()));
            continue;
        }
        physical_sources.insert((channel.participant_id, channel.source_name.as_str()));
        if !physical_channels.insert((&channel.source_name, &channel.input_channel)) {
            return Err(format!(
                "source '{}' input channel is owned by multiple participants",
                channel.source_name
            ));
        }
    }
    if let Some((participant_id, source)) = logical_sources.intersection(&physical_sources).next() {
        return Err(format!(
            "channel progress participant {participant_id} source '{source}' mixes logical and physical input channels"
        ));
    }
    let classification = classify_channel_progress(channels)?;
    if checkpoint_watermark != classification.active_value() {
        return Err("checkpoint watermark does not match channel progress".into());
    }
    Ok(())
}

fn validate_channel_rosters(
    sources: &BTreeMap<String, ConnectorCheckpoint>,
    channels: &[ChannelProgress],
) -> Result<(), String> {
    for (source, checkpoint) in sources {
        let Some(expected) = checkpoint.input_channels.as_ref() else {
            continue;
        };
        let mut actual = channels
            .iter()
            .filter(|channel| {
                channel.source_name == *source
                    && channel.input_channel != SINGLETON_WATERMARK_CHANNEL
            })
            .map(|channel| channel.input_channel.as_slice())
            .collect::<Vec<_>>();
        if actual.is_empty() {
            continue;
        }
        actual.sort_unstable();
        if !actual.into_iter().eq(expected.iter().map(Vec::as_slice)) {
            return Err(format!(
                "source '{source}' input channels do not match its channel progress roster"
            ));
        }
    }
    Ok(())
}

fn validate_source_watermarks(
    source_names: &[String],
    channels: &[ChannelProgress],
    source_watermarks: &BTreeMap<String, i64>,
) -> Result<(), String> {
    for (source, watermark) in source_watermarks {
        validate_name("source watermark", source)?;
        if source_names.binary_search(source).is_err() {
            return Err(format!(
                "source watermark '{source}' is absent from the committed source topology"
            ));
        }
        if *watermark == i64::MIN {
            return Err(format!(
                "source watermark '{source}' uses the reserved uninitialized value"
            ));
        }
    }
    for (source, current) in channel_progress_frontiers_by_source(channels)? {
        let Some(current) = current else {
            continue;
        };
        if source_watermarks.get(source) != Some(&current) {
            return Err(format!(
                "source watermark '{source}' does not match its exact channel progress"
            ));
        }
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
mod tests;
