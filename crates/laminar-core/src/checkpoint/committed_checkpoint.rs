//! Immutable global index for one committed checkpoint cut.

#![allow(clippy::disallowed_types)] // cold-path canonical checkpoint metadata

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{
    channel_progress_frontiers_by_source, classify_channel_progress, ChannelProgress,
    CheckpointAssignmentFence, CheckpointManifest, ConnectorCheckpoint, PipelineIdentity,
    SINGLETON_WATERMARK_CHANNEL,
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
    for (source, current) in
        channel_progress_frontiers_by_source(channels).map_err(|error| error.to_string())?
    {
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
            reassignment_portable: false,
            predecessor: None,
            participants: vec![CommittedParticipantRef {
                participant_id: LOCAL_NODE_ID.0,
                manifest_len: 100,
                manifest_sha256: digest(1),
                node_data_len: 0,
                node_data_sha256: digest(2),
            }],
            source_names: vec!["source".into()],
            source_offsets: BTreeMap::from([(
                "source".into(),
                ConnectorCheckpoint {
                    input_channels: Some(vec![b"orders".to_vec()]),
                    ..ConnectorCheckpoint::default()
                },
            )]),
            channel_progress: vec![ChannelProgress {
                participant_id: LOCAL_NODE_ID.0,
                source_name: "source".into(),
                input_channel: b"orders".to_vec(),
                watermark: Some(42),
                idle: false,
            }],
            source_watermarks: BTreeMap::from([("source".into(), 42)]),
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
                manifest.reassignment_portable = true;
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
            reassignment_portable: true,
            predecessor: None,
            participants: Vec::new(),
            source_names: vec!["source".into()],
            source_offsets: BTreeMap::from([("source".into(), ConnectorCheckpoint::default())]),
            channel_progress: Vec::new(),
            source_watermarks: BTreeMap::new(),
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
    fn exact_scope_requires_reassignment_portability() {
        let mut local = local_index();
        local.reassignment_portable = true;
        assert!(local
            .validate()
            .unwrap_err()
            .contains("local committed checkpoint cannot claim"));

        let (mut cluster, _) = cluster_cut();
        cluster.reassignment_portable = false;
        assert!(cluster
            .validate()
            .unwrap_err()
            .contains("cluster committed checkpoint must be portable"));
    }

    #[test]
    fn version_three_index_remains_canonical_and_version_two_fails_closed() {
        let mut index = local_index();
        index.version = 2;
        assert!(index
            .validate()
            .unwrap_err()
            .contains("unsupported committed checkpoint index version 2"));

        let mut v3_shape = serde_json::to_value(local_index()).unwrap();
        let object = v3_shape.as_object_mut().unwrap();
        object.insert("version".into(), serde_json::Value::from(3));
        object.remove("source_watermarks");
        let v3_bytes = canonical_json_bytes(&v3_shape).unwrap();
        let restored: CommittedCheckpointIndex = serde_json::from_slice(&v3_bytes).unwrap();
        restored.validate().unwrap();
        assert_eq!(
            restored.effective_source_watermarks().unwrap()["source"],
            42
        );
        assert_eq!(canonical_json_bytes(&restored).unwrap(), v3_bytes);

        let mut impossible_v3 = restored;
        impossible_v3.source_watermarks.insert("source".into(), 42);
        assert!(impossible_v3
            .validate()
            .unwrap_err()
            .contains("legacy committed checkpoint cannot carry"));
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
        changed.source_watermarks.clear();
        let (_, changed_reference) = changed.encode_and_reference().unwrap();
        assert_ne!(reference.sha256, changed_reference.sha256);
    }

    #[test]
    fn empty_source_inventory_retains_the_exact_predecessor_decision() {
        let predecessor = local_index();
        let (_, predecessor_ref) = predecessor.encode_and_reference().unwrap();
        let mut successor = predecessor.clone();
        successor.epoch = 8;
        successor.checkpoint_id = 8;
        successor.predecessor = Some(predecessor_ref);
        successor.channel_progress.clear();
        successor.checkpoint_watermark = None;

        successor.validate().unwrap();
        successor.validate_predecessor_index(&predecessor).unwrap();
        assert_eq!(successor.source_watermarks["source"], 42);

        successor.source_watermarks.insert("source".into(), 43);
        assert!(successor
            .validate_predecessor_index(&predecessor)
            .unwrap_err()
            .contains("do not exactly continue"));
    }

    #[test]
    fn legacy_successor_cannot_erase_a_version_four_source_cut() {
        let predecessor = local_index();
        let (_, predecessor_ref) = predecessor.encode_and_reference().unwrap();
        let mut successor = predecessor.clone();
        successor.version = LEGACY_COMMITTED_CHECKPOINT_INDEX_VERSION;
        successor.epoch = 8;
        successor.checkpoint_id = 8;
        successor.predecessor = Some(predecessor_ref);
        successor.source_watermarks.clear();

        successor.validate().unwrap();
        assert!(successor
            .validate_predecessor_index(&predecessor)
            .unwrap_err()
            .contains("version regresses"));
    }

    #[test]
    fn predecessor_continuity_rejects_a_source_topology_change() {
        let predecessor = local_index();
        let (_, predecessor_ref) = predecessor.encode_and_reference().unwrap();
        let mut successor = predecessor.clone();
        successor.epoch = 8;
        successor.checkpoint_id = 8;
        successor.predecessor = Some(predecessor_ref);
        successor.source_names = vec!["other-source".into()];
        let offset = successor.source_offsets.remove("source").unwrap();
        successor
            .source_offsets
            .insert("other-source".into(), offset);
        successor.channel_progress[0].source_name = "other-source".into();
        let watermark = successor.source_watermarks.remove("source").unwrap();
        successor
            .source_watermarks
            .insert("other-source".into(), watermark);

        successor.validate().unwrap();
        assert!(successor
            .validate_predecessor_index(&predecessor)
            .unwrap_err()
            .contains("breaks recovery continuity"));
    }

    #[test]
    fn initial_index_cannot_invent_a_channel_less_source_decision() {
        let mut index = local_index();
        index.channel_progress.clear();
        index.checkpoint_watermark = None;

        assert!(index
            .validate()
            .unwrap_err()
            .contains("initial committed source watermarks"));
    }

    #[test]
    fn entirely_uninitialized_channels_have_no_watermark() {
        let mut index = local_index();
        index.channel_progress[0].watermark = None;
        assert!(index.validate().is_err());
        index.checkpoint_watermark = None;
        index.source_watermarks.clear();
        assert!(index.validate().is_ok());
    }

    #[test]
    fn physical_input_channel_has_one_cluster_owner() {
        let (mut index, _) = cluster_cut();
        index.channel_progress = vec![
            ChannelProgress {
                participant_id: 1,
                source_name: "source".into(),
                input_channel: b"partition-0".to_vec(),
                watermark: Some(42),
                idle: false,
            },
            ChannelProgress {
                participant_id: 2,
                source_name: "source".into(),
                input_channel: b"partition-0".to_vec(),
                watermark: Some(42),
                idle: false,
            },
        ];
        index.checkpoint_watermark = Some(42);

        let error = index.validate().unwrap_err();
        assert!(error.contains("owned by multiple participants"));
    }

    #[test]
    fn empty_participant_marker_can_share_a_source_with_remote_physical_channels() {
        let (mut index, mut manifests) = cluster_cut();
        index
            .source_offsets
            .get_mut("source")
            .unwrap()
            .input_channels = Some(vec![b"partition-0".to_vec()]);
        index.channel_progress = vec![
            ChannelProgress {
                participant_id: 1,
                source_name: "source".into(),
                input_channel: b"partition-0".to_vec(),
                watermark: Some(42),
                idle: false,
            },
            ChannelProgress {
                participant_id: 2,
                source_name: "source".into(),
                input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
                watermark: Some(41),
                idle: true,
            },
        ];
        index.source_watermarks.insert("source".into(), 42);
        index.checkpoint_watermark = Some(42);
        manifests[0].source_offsets.insert(
            "source".into(),
            ConnectorCheckpoint {
                input_channels: Some(vec![b"partition-0".to_vec()]),
                ..ConnectorCheckpoint::default()
            },
        );
        manifests[0].channel_progress = vec![index.channel_progress[0].clone()];
        manifests[0].checkpoint_watermark = Some(42);
        manifests[1].source_offsets.insert(
            "source".into(),
            ConnectorCheckpoint {
                input_channels: Some(Vec::new()),
                ..ConnectorCheckpoint::default()
            },
        );
        manifests[1].channel_progress = vec![index.channel_progress[1].clone()];
        let encoded = bind_manifests(&mut index, &manifests);
        index.validate().unwrap();
        validate_manifests(&index, &manifests, &encoded).unwrap();

        index.channel_progress = vec![
            ChannelProgress {
                participant_id: 1,
                source_name: "source".into(),
                input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
                watermark: Some(41),
                idle: true,
            },
            ChannelProgress {
                participant_id: 1,
                source_name: "source".into(),
                input_channel: b"partition-0".to_vec(),
                watermark: Some(42),
                idle: false,
            },
        ];
        let error = index.validate().unwrap_err();
        assert!(error.contains("participant 1"), "{error}");
        assert!(error.contains("mixes logical and physical"), "{error}");
    }

    #[test]
    fn source_input_channels_match_merged_progress() {
        let mut index = local_index();
        index
            .source_offsets
            .get_mut("source")
            .unwrap()
            .input_channels = Some(vec![b"different".to_vec()]);

        let error = index.validate().unwrap_err();
        assert!(error.contains("channel progress roster"));

        index
            .source_offsets
            .get_mut("source")
            .unwrap()
            .input_channels = Some(vec![SINGLETON_WATERMARK_CHANNEL.to_vec()]);
        let error = index.validate().unwrap_err();
        assert!(error.contains("reserved logical watermark channel"));
    }

    #[test]
    fn logical_singleton_is_participant_local_and_requires_a_known_source() {
        let (mut index, _) = cluster_cut();
        index
            .source_offsets
            .get_mut("source")
            .unwrap()
            .input_channels = Some(vec![b"partition-0".to_vec(), b"partition-1".to_vec()]);
        index.channel_progress = vec![
            ChannelProgress {
                participant_id: 1,
                source_name: "source".into(),
                input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
                watermark: Some(42),
                idle: false,
            },
            ChannelProgress {
                participant_id: 2,
                source_name: "source".into(),
                input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
                watermark: Some(41),
                idle: false,
            },
        ];
        index.source_watermarks.insert("source".into(), 41);
        index.checkpoint_watermark = Some(41);
        assert!(index.validate().is_ok());
        index.source_offsets.clear();
        assert!(index.validate().is_ok());

        index.channel_progress[0].source_name = "missing".into();
        let error = index.validate().unwrap_err();
        assert!(error.contains("absent from the committed source topology"));
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
    fn every_cluster_manifest_must_bind_the_portability_proof() {
        let (mut index, mut manifests) = cluster_cut();
        manifests[1].reassignment_portable = false;
        let encoded = bind_manifests(&mut index, &manifests);

        let error = validate_manifests(&index, &manifests, &encoded).unwrap_err();
        assert!(error.contains("must be proven portable across vnode reassignment"));
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
