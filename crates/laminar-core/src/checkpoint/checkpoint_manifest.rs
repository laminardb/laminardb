//! Versioned checkpoint manifest.

#![allow(clippy::disallowed_types)] // cold path: manifest serialization

use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU32, NonZeroU64};

use sha2::{Digest, Sha256};

use crate::checkpoint::assignment::CheckpointAssignmentFence;
use crate::state::{
    KeyGroupCount, DEFAULT_KEY_GROUP_COUNT, LOCAL_NODE_ID, PARTITIONING_ABI_VERSION,
};

/// Current checkpoint manifest format. Every other version is rejected.
pub const CHECKPOINT_MANIFEST_VERSION: u32 = 9;

/// Canonical pipeline-identity payload version.
pub const PIPELINE_IDENTITY_VERSION: u16 = 6;

/// Runtime envelope used for prepared sink descriptors.
pub const PREPARED_SINK_DESCRIPTOR_VERSION: u16 = 1;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// SHA-256 identity of the logical pipeline and recovery-state ABI.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct PipelineIdentity {
    /// Version of the canonical payload format.
    pub canonical_version: u16,
    /// Exactly 64 lowercase hexadecimal characters.
    pub sha256: String,
}

impl PipelineIdentity {
    /// Identity of an empty canonical payload.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            canonical_version: PIPELINE_IDENTITY_VERSION,
            sha256: EMPTY_SHA256.into(),
        }
    }

    pub(crate) fn validation_error(&self) -> Option<String> {
        if self.canonical_version != PIPELINE_IDENTITY_VERSION {
            return Some(format!(
                "unsupported pipeline identity version {}; expected {PIPELINE_IDENTITY_VERSION}",
                self.canonical_version
            ));
        }
        (!is_sha256(&self.sha256))
            .then(|| "pipeline identity must be 64 lowercase hexadecimal characters".into())
    }

    /// Whether this identity uses the current canonical version and digest encoding.
    #[must_use]
    pub(crate) fn is_canonical(&self) -> bool {
        self.validation_error().is_none()
    }
}

/// Stable identity of one immutable node data object.
#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
#[serde(deny_unknown_fields)]
pub struct StateChunkId {
    /// Node that wrote the object.
    pub participant_id: u64,
    /// Checkpoint that created the object.
    pub checkpoint_id: u64,
}

/// A byte range within an immutable node data object.
#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
#[serde(deny_unknown_fields)]
pub struct ByteRange {
    /// First byte in the range.
    pub offset: u64,
    /// Number of bytes in the range.
    pub length: u64,
}

impl ByteRange {
    /// Return the exclusive range end, or `None` on overflow.
    #[must_use]
    pub fn end(self) -> Option<u64> {
        self.offset.checked_add(self.length)
    }
}

/// The one data object written by this participant for this checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeDataObject {
    /// Identity used to derive the provider-neutral object path.
    pub chunk: StateChunkId,
    /// Exact object length.
    pub object_length: u64,
    /// SHA-256 of the complete object.
    pub sha256: String,
}

/// State-frame identity. Whole-operator metadata and vnode-keyed state are distinct.
#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateFrameKey {
    /// Non-vnode state such as graph progress, channel watermarks, idleness, and timers.
    OperatorWhole {
        /// Stable operator identifier.
        operator_id: String,
    },
    /// State owned by one vnode of an operator.
    Vnode {
        /// Stable operator identifier.
        operator_id: String,
        /// Vnode within the manifest's partition domain.
        vnode: u16,
    },
}

/// A checksummed state payload in the current or a prior node data object.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StateFrame {
    /// Logical state slot restored from this payload.
    pub key: StateFrameKey,
    /// Object containing the payload.
    pub chunk: StateChunkId,
    /// Exact payload range.
    pub range: ByteRange,
    /// SHA-256 of only this frame.
    pub sha256: String,
}

/// Opaque phase-one sink descriptor stored in the current node data object.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PreparedSinkDescriptor {
    /// Stable registered sink name.
    pub sink_name: String,
    /// Runtime descriptor-envelope version.
    pub format_version: u16,
    /// `None` and an explicit empty range have different connector semantics.
    pub payload: Option<ByteRange>,
    /// Presence-domain-separated SHA-256 of the optional payload.
    pub sha256: String,
}

/// Watermark and idleness state for one stable input channel.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChannelProgress {
    /// Participant owning this channel state.
    pub participant_id: u64,
    /// Stable registered source name.
    pub source_name: String,
    /// Reserved logical singleton or opaque physical input-channel identity.
    pub input_channel: Vec<u8>,
    /// Watermark when initialized; `None` before the channel emits one.
    pub watermark: Option<i64>,
    /// Whether the channel is excluded from the active watermark minimum.
    pub idle: bool,
}

/// Metadata and exact logical reference count for an older immutable object.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReferencedStateChunk {
    /// Prior object identity. The object path is derived without listing.
    pub chunk: StateChunkId,
    /// Exact immutable object length.
    pub object_length: u64,
    /// SHA-256 of the complete object.
    pub sha256: String,
    /// Number of state frames in this manifest that reference the object. GC aggregates this
    /// over the bounded committed manifest/index inventory; it never discovers references by
    /// listing objects.
    pub ref_count: NonZeroU32,
}

/// A point-in-time snapshot of one participant's pipeline state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CheckpointManifest {
    /// Exact manifest format version.
    pub version: u32,
    /// Unique, monotonically increasing checkpoint ID; must equal `epoch`.
    pub checkpoint_id: u64,
    /// Persisted protocol epoch; must equal `checkpoint_id`.
    pub epoch: u64,
    /// Creation time in milliseconds since the Unix epoch.
    pub timestamp_ms: u64,
    /// Node owning this manifest and its current data object.
    pub participant_id: u64,
    /// Per-source connector offsets.
    pub source_offsets: HashMap<String, ConnectorCheckpoint>,
    /// Canonically sorted registered source names.
    pub source_names: Vec<String>,
    /// Canonically sorted registered sink names.
    pub sink_names: Vec<String>,
    /// Deterministic logical topology and state ABI identity.
    pub pipeline_identity: PipelineIdentity,
    /// Create-once checkpoint namespace incarnation.
    pub deployment_id: String,
    /// Durable key encoding, hashing, and key-group mapping contract.
    pub partitioning_abi_version: u16,
    /// Virtual partition count.
    pub vnode_count: u16,
    /// Exact cluster assignment generation and participant fence; local manifests use `None`.
    pub assignment_fence: Option<CheckpointAssignmentFence>,
    /// Whether this cut can be restored under a different cluster vnode assignment.
    ///
    /// Cluster manifests must carry an affirmative, capture-time proof. Local manifests cannot
    /// claim reassignment portability because they have no assignment-fenced recovery domain.
    pub reassignment_portable: bool,
    /// Canonically sorted vnodes whose state inventory this participant supplies.
    pub owned_vnodes: Vec<u16>,
    /// Derived checkpoint watermark retained with the exact cut.
    pub checkpoint_watermark: Option<i64>,
    /// Canonically ordered per-channel watermarks and idle/uninitialized states.
    pub channel_progress: Vec<ChannelProgress>,
    /// The only data object written by this node for this checkpoint.
    pub node_data: NodeDataObject,
    /// Canonically ordered complete logical state inventory.
    pub state_frames: Vec<StateFrame>,
    /// Canonically ordered phase-one sink descriptor inventory.
    pub prepared_sinks: Vec<PreparedSinkDescriptor>,
    /// Canonically ordered older objects retained by `state_frames`.
    pub referenced_chunks: Vec<ReferencedStateChunk>,
}

/// Errors found during manifest validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestValidationError {
    /// Human-readable description of the issue.
    pub message: String,
}

impl std::fmt::Display for ManifestValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl CheckpointManifest {
    /// Create a prepared manifest with the common vnode topology.
    #[must_use]
    pub fn new(checkpoint_id: u64, epoch: u64) -> Self {
        Self::new_with_key_group_count(checkpoint_id, epoch, DEFAULT_KEY_GROUP_COUNT)
    }

    /// Create a prepared manifest with an explicit vnode topology.
    #[must_use]
    pub fn new_with_key_group_count(
        checkpoint_id: u64,
        epoch: u64,
        key_group_count: KeyGroupCount,
    ) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            version: CHECKPOINT_MANIFEST_VERSION,
            checkpoint_id,
            epoch,
            timestamp_ms,
            participant_id: LOCAL_NODE_ID.0,
            source_offsets: HashMap::new(),
            source_names: Vec::new(),
            sink_names: Vec::new(),
            pipeline_identity: PipelineIdentity::empty(),
            deployment_id: String::new(),
            partitioning_abi_version: PARTITIONING_ABI_VERSION,
            vnode_count: key_group_count.get(),
            assignment_fence: None,
            reassignment_portable: false,
            owned_vnodes: (0..key_group_count.get()).collect(),
            checkpoint_watermark: None,
            channel_progress: Vec::new(),
            node_data: NodeDataObject {
                chunk: StateChunkId {
                    participant_id: LOCAL_NODE_ID.0,
                    checkpoint_id,
                },
                object_length: 0,
                sha256: EMPTY_SHA256.into(),
            },
            state_frames: Vec::new(),
            prepared_sinks: Vec::new(),
            referenced_chunks: Vec::new(),
        }
    }

    /// Bind the manifest and current data-object identity to one participant.
    pub fn bind_participant(&mut self, participant_id: u64) {
        self.participant_id = participant_id;
        self.node_data.chunk.participant_id = participant_id;
    }

    /// Validate the exact v9 recovery contract.
    #[must_use]
    pub fn validate(
        &self,
        expected_key_group_count: KeyGroupCount,
    ) -> Vec<ManifestValidationError> {
        let mut errors = Vec::new();
        let mut error = |message| errors.push(ManifestValidationError { message });

        if self.version != CHECKPOINT_MANIFEST_VERSION {
            error(format!(
                "unsupported manifest version {}; expected {CHECKPOINT_MANIFEST_VERSION}",
                self.version
            ));
        }
        if self.partitioning_abi_version != PARTITIONING_ABI_VERSION {
            error(format!(
                "partitioning ABI mismatch: checkpoint has {}, runtime expects {PARTITIONING_ABI_VERSION}",
                self.partitioning_abi_version
            ));
        }
        if self.checkpoint_id == 0 || self.epoch != self.checkpoint_id {
            error("checkpoint attempt must use one nonzero canonical checkpoint ID".into());
        }
        if self.timestamp_ms == 0 {
            error("timestamp_ms must be nonzero".into());
        }
        if let Some(message) = self.pipeline_identity.validation_error() {
            error(message);
        }
        let valid_deployment = uuid::Uuid::parse_str(&self.deployment_id)
            .is_ok_and(|id| !id.is_nil() && id.to_string() == self.deployment_id);
        if !valid_deployment {
            error("deployment_id must be a canonical non-nil UUID".into());
        }
        if self.vnode_count != expected_key_group_count.get() {
            error(format!(
                "vnode_count mismatch: checkpoint has {}, runtime expects {expected_key_group_count}",
                self.vnode_count
            ));
        }
        if self.participant_id == 0 {
            error("participant_id must be nonzero".into());
        }
        if self.owned_vnodes.is_empty() {
            error("owned_vnodes must not be empty".into());
        } else if !self.owned_vnodes.windows(2).all(|pair| pair[0] < pair[1]) {
            error("owned_vnodes must be strictly ordered and unique".into());
        }
        if self
            .owned_vnodes
            .iter()
            .any(|vnode| *vnode >= self.vnode_count)
        {
            error("owned_vnodes contains a vnode outside the manifest domain".into());
        }
        if let Some(fence) = &self.assignment_fence {
            if fence.vnode_count != u32::from(self.vnode_count)
                || fence.partitioning_abi_version != self.partitioning_abi_version
                || !fence.contains(self.participant_id)
            {
                error(
                    "assignment_fence does not cover this manifest topology and participant".into(),
                );
            }
            if !self.reassignment_portable {
                error(
                    "a cluster manifest must be proven portable across vnode reassignment".into(),
                );
            }
        } else {
            let owns_complete_domain = self.owned_vnodes.len() == usize::from(self.vnode_count)
                && self.owned_vnodes.iter().copied().eq(0..self.vnode_count);
            if self.participant_id != LOCAL_NODE_ID.0 || !owns_complete_domain {
                error("a local manifest must use LOCAL_NODE_ID and own every vnode".into());
            }
            if self.reassignment_portable {
                error("a local manifest cannot claim vnode reassignment portability".into());
            }
        }
        validate_sorted_unique("source_names", &self.source_names, &mut error);
        validate_sorted_unique("sink_names", &self.sink_names, &mut error);
        if !self.channel_progress.windows(2).all(|pair| {
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
            error(
                "channel_progress must be strictly ordered by participant, source, and input channel"
                    .into(),
            );
        }
        let mut channel_modes = BTreeMap::new();
        for channel in &self.channel_progress {
            if channel.participant_id != self.participant_id {
                error("channel_progress participant_id must match the manifest participant".into());
            }
            if channel.source_name.is_empty() {
                error("channel_progress source_name must not be empty".into());
            } else if self
                .source_names
                .binary_search(&channel.source_name)
                .is_err()
            {
                error(format!(
                    "channel_progress source '{}' not in source_names",
                    channel.source_name
                ));
            }
            if channel.input_channel.is_empty() {
                error("channel_progress input_channel must not be empty".into());
            }
            let modes = channel_modes
                .entry(channel.source_name.as_str())
                .or_insert((false, false));
            if channel.input_channel == super::SINGLETON_WATERMARK_CHANNEL {
                modes.0 = true;
            } else {
                modes.1 = true;
            }
        }
        for (source, (logical, physical)) in channel_modes {
            if logical && physical {
                error(format!(
                    "channel_progress source '{source}' mixes logical and physical input channels"
                ));
            }
        }
        match super::classify_channel_progress(&self.channel_progress) {
            Ok(classification) if self.checkpoint_watermark != classification.active_value() => {
                error("checkpoint_watermark does not match channel progress".into());
            }
            Err(message) => error(message),
            Ok(_) => {}
        }
        for name in self.source_offsets.keys() {
            if self.source_names.binary_search(name).is_err() {
                error(format!(
                    "source_offsets contains '{name}' not in source_names"
                ));
            }
        }
        for (name, checkpoint) in &self.source_offsets {
            if let Some(channels) = &checkpoint.input_channels {
                if channels.iter().any(Vec::is_empty)
                    || !channels.windows(2).all(|pair| pair[0] < pair[1])
                {
                    error(format!(
                        "source '{name}' input_channels must contain non-empty, strictly ordered unique identities"
                    ));
                }
                if channels
                    .iter()
                    .any(|channel| channel == super::SINGLETON_WATERMARK_CHANNEL)
                {
                    error(format!(
                        "source '{name}' input_channels contains the reserved logical watermark channel"
                    ));
                }
                let mut progress = self
                    .channel_progress
                    .iter()
                    .filter(|channel| channel.source_name == *name)
                    .map(|channel| channel.input_channel.as_slice())
                    .collect::<Vec<_>>();
                if !progress.is_empty()
                    && !progress
                        .iter()
                        .all(|channel| *channel == super::SINGLETON_WATERMARK_CHANNEL)
                {
                    progress.sort_unstable();
                    if !progress.into_iter().eq(channels.iter().map(Vec::as_slice)) {
                        error(format!(
                            "source '{name}' input_channels do not match its channel_progress roster"
                        ));
                    }
                }
            }
        }

        let current = self.node_data.chunk;
        if current.participant_id != self.participant_id
            || current.checkpoint_id != self.checkpoint_id
        {
            error("node_data chunk must match the manifest participant and checkpoint".into());
        }
        if !is_sha256(&self.node_data.sha256) {
            error("node_data digest must be lowercase SHA-256".into());
        }

        let referenced_by_id = self
            .referenced_chunks
            .iter()
            .map(|reference| (reference.chunk, reference))
            .collect::<HashMap<_, _>>();
        if referenced_by_id.len() != self.referenced_chunks.len()
            || !self
                .referenced_chunks
                .windows(2)
                .all(|pair| pair[0].chunk < pair[1].chunk)
        {
            error("referenced_chunks must be strictly ordered and unique".into());
        }
        for reference in &self.referenced_chunks {
            if reference.chunk == current
                || reference.chunk.checkpoint_id >= self.checkpoint_id
                || reference.chunk.checkpoint_id == 0
            {
                error(format!(
                    "referenced chunk {:?} is not an older immutable object",
                    reference.chunk
                ));
            }
            if !is_sha256(&reference.sha256) {
                error(format!(
                    "referenced chunk {:?} digest must be lowercase SHA-256",
                    reference.chunk
                ));
            }
        }

        let mut prior_counts = HashMap::<StateChunkId, u32>::new();
        let mut current_ranges = Vec::new();
        if !self
            .state_frames
            .windows(2)
            .all(|pair| pair[0].key < pair[1].key)
        {
            error("state_frames must be strictly ordered by logical key".into());
        }
        for frame in &self.state_frames {
            if let StateFrameKey::Vnode { vnode, .. } = &frame.key {
                if *vnode >= self.vnode_count {
                    error(format!(
                        "state frame vnode {vnode} is outside the vnode domain"
                    ));
                }
                if self.owned_vnodes.binary_search(vnode).is_err() {
                    error(format!(
                        "state frame vnode {vnode} is not owned by participant {}",
                        self.participant_id
                    ));
                }
            }
            if frame.range.length == 0 {
                error(format!("state frame {:?} has an empty payload", frame.key));
            }
            if !is_sha256(&frame.sha256) {
                error(format!(
                    "state frame {:?} digest must be lowercase SHA-256",
                    frame.key
                ));
            }
            let object_length = if frame.chunk == current {
                current_ranges.push((frame.range, format!("state frame {:?}", frame.key)));
                Some(self.node_data.object_length)
            } else {
                let reference = referenced_by_id.get(&frame.chunk).copied();
                if let Some(reference) = reference {
                    let count = prior_counts.entry(frame.chunk).or_default();
                    *count = count.saturating_add(1);
                    Some(reference.object_length)
                } else {
                    error(format!(
                        "state frame {:?} references an untracked chunk {:?}",
                        frame.key, frame.chunk
                    ));
                    None
                }
            };
            validate_range(frame.range, object_length, "state frame", &mut error);
        }
        for reference in &self.referenced_chunks {
            let actual = prior_counts.get(&reference.chunk).copied().unwrap_or(0);
            if actual != reference.ref_count.get() {
                error(format!(
                    "referenced chunk {:?} declares ref_count {}, but {actual} state frames reference it",
                    reference.chunk,
                    reference.ref_count
                ));
            }
        }

        if !self
            .prepared_sinks
            .windows(2)
            .all(|pair| pair[0].sink_name < pair[1].sink_name)
        {
            error("prepared_sinks must be strictly ordered by sink_name".into());
        }
        for sink in &self.prepared_sinks {
            if sink.sink_name.is_empty() {
                error("prepared sink name must not be empty".into());
            }
            if self.sink_names.binary_search(&sink.sink_name).is_err() {
                error(format!(
                    "prepared sink '{}' is not in sink_names",
                    sink.sink_name
                ));
            }
            if sink.format_version != PREPARED_SINK_DESCRIPTOR_VERSION {
                error(format!(
                    "prepared sink '{}' format_version must be {PREPARED_SINK_DESCRIPTOR_VERSION}",
                    sink.sink_name,
                ));
            }
            if !is_sha256(&sink.sha256) {
                error(format!(
                    "prepared sink '{}' digest must be lowercase SHA-256",
                    sink.sink_name
                ));
            }
            match sink.payload {
                Some(range) => {
                    validate_range(
                        range,
                        Some(self.node_data.object_length),
                        "prepared sink payload",
                        &mut error,
                    );
                    current_ranges.push((range, format!("prepared sink '{}'", sink.sink_name)));
                }
                None if sink.sha256 != checkpoint_descriptor_sha256(None) => error(format!(
                    "prepared sink '{}' without a payload has the wrong domain-separated digest",
                    sink.sink_name
                )),
                None => {}
            }
        }

        let mut expected_offset = 0_u64;
        for (range, owner) in current_ranges {
            if range.offset != expected_offset {
                error(format!(
                    "{owner} starts at {}, expected {expected_offset} in the canonical node object",
                    range.offset
                ));
            }
            if let Some(end) = range.end() {
                expected_offset = end;
            }
        }
        if expected_offset != self.node_data.object_length {
            error(format!(
                "current frame and sink ranges cover {expected_offset} bytes, but node_data declares {}",
                self.node_data.object_length
            ));
        }

        errors
    }
}

/// Connector-owned offset map stored at the exact checkpoint cut.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConnectorCheckpoint {
    /// Connector-specific offsets.
    pub offsets: HashMap<String, String>,
    /// Connector metadata required to interpret the offsets.
    pub metadata: HashMap<String, String>,
    /// Canonically ordered opaque identities of the input channels owned by this cut.
    pub input_channels: Option<Vec<Vec<u8>>>,
    /// Source-assignment version owning this cut, when applicable.
    pub source_assignment_version: Option<NonZeroU64>,
}

impl ConnectorCheckpoint {
    /// Create an empty connector checkpoint.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a checkpoint from an offset map.
    #[must_use]
    pub fn with_offsets(offsets: HashMap<String, String>) -> Self {
        Self {
            offsets,
            ..Self::default()
        }
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_sorted_unique(field: &str, values: &[String], error: &mut impl FnMut(String)) {
    if !values.windows(2).all(|pair| pair[0] < pair[1]) {
        error(format!("{field} must be strictly ordered and unique"));
    }
}

fn validate_range(
    range: ByteRange,
    object_length: Option<u64>,
    owner: &str,
    error: &mut impl FnMut(String),
) {
    let Some(end) = range.end() else {
        error(format!("{owner} byte range overflows"));
        return;
    };
    if object_length.is_some_and(|length| end > length) {
        error(format!("{owner} byte range ends beyond its object"));
    }
}

/// SHA-256 helper used by capture and focused format tests.
#[must_use]
pub fn checkpoint_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Domain-separated digest for an optional prepared sink descriptor.
#[must_use]
pub fn checkpoint_descriptor_sha256(payload: Option<&[u8]>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"laminardb-prepared-sink-descriptor-v1\0");
    match payload {
        None => hasher.update([0]),
        Some(bytes) => {
            hasher.update([1]);
            hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
            hasher.update(bytes);
        }
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_manifest(id: u64) -> CheckpointManifest {
        let mut manifest = CheckpointManifest::new_with_key_group_count(
            id,
            id,
            KeyGroupCount::try_from(1_u16).unwrap(),
        );
        manifest.deployment_id = uuid::Uuid::from_u128(1).to_string();
        manifest.source_names = vec!["source".into()];
        manifest.sink_names = vec!["sink".into()];
        manifest.node_data.sha256 = checkpoint_sha256(b"");
        manifest
    }

    fn channel(id: &str, watermark: Option<i64>, idle: bool) -> ChannelProgress {
        ChannelProgress {
            participant_id: LOCAL_NODE_ID.0,
            source_name: "source".into(),
            input_channel: id.as_bytes().to_vec(),
            watermark,
            idle,
        }
    }

    #[test]
    fn active_uninitialized_channel_withholds_the_watermark() {
        let channels = vec![channel("a", Some(20), false), channel("b", None, false)];
        assert_eq!(
            crate::checkpoint::classify_channel_progress(&channels),
            Ok(crate::checkpoint::CheckpointWatermark::Uninitialized)
        );

        let mut manifest = valid_manifest(1);
        manifest.channel_progress = channels;
        assert!(manifest
            .validate(KeyGroupCount::try_from(1_u16).unwrap())
            .is_empty());
        manifest.checkpoint_watermark = Some(20);
        assert!(manifest
            .validate(KeyGroupCount::try_from(1_u16).unwrap())
            .iter()
            .any(|error| error.message.contains("does not match channel progress")));
    }

    #[test]
    fn idle_uninitialized_channel_is_excluded() {
        let channels = vec![channel("a", Some(20), false), channel("b", None, true)];
        assert_eq!(
            crate::checkpoint::classify_channel_progress(&channels),
            Ok(crate::checkpoint::CheckpointWatermark::Active(20))
        );

        let mut manifest = valid_manifest(1);
        manifest.channel_progress = channels;
        manifest.checkpoint_watermark = Some(20);
        assert!(manifest
            .validate(KeyGroupCount::try_from(1_u16).unwrap())
            .is_empty());
    }

    #[test]
    fn all_idle_channels_have_no_watermark() {
        let channels = vec![channel("a", None, true), channel("b", None, true)];
        assert_eq!(
            crate::checkpoint::classify_channel_progress(&[]),
            Ok(crate::checkpoint::CheckpointWatermark::Idle)
        );
        assert_eq!(
            crate::checkpoint::classify_channel_progress(&channels),
            Ok(crate::checkpoint::CheckpointWatermark::Idle)
        );
        let retained = vec![channel("a", Some(10), true), channel("b", Some(20), true)];
        assert_eq!(
            crate::checkpoint::channel_progress_frontier(&retained),
            Ok(Some(20))
        );

        let mut manifest = valid_manifest(1);
        manifest.channel_progress = channels;
        assert!(manifest
            .validate(KeyGroupCount::try_from(1_u16).unwrap())
            .is_empty());
        manifest.checkpoint_watermark = Some(20);
        assert!(manifest
            .validate(KeyGroupCount::try_from(1_u16).unwrap())
            .iter()
            .any(|error| error.message.contains("does not match channel progress")));

        let invalid = vec![channel("idle", Some(i64::MIN), true)];
        assert!(crate::checkpoint::classify_channel_progress(&invalid).is_err());
    }

    #[test]
    fn v9_round_trip_carries_portability_channels_ranges_sinks_and_prior_chunk_refs() {
        let mut manifest = valid_manifest(9);
        manifest.source_offsets.insert(
            "source".into(),
            ConnectorCheckpoint {
                input_channels: Some(vec![b"partition-0".to_vec(), b"partition-1".to_vec()]),
                ..ConnectorCheckpoint::default()
            },
        );
        let prior = StateChunkId {
            participant_id: 2,
            checkpoint_id: 8,
        };
        let current_data = b"ew";
        manifest.node_data.object_length = current_data.len() as u64;
        manifest.node_data.sha256 = checkpoint_sha256(current_data);
        manifest.state_frames = vec![
            StateFrame {
                key: StateFrameKey::OperatorWhole {
                    operator_id: "graph".into(),
                },
                chunk: prior,
                range: ByteRange {
                    offset: 0,
                    length: 3,
                },
                sha256: checkpoint_sha256(b"old"),
            },
            StateFrame {
                key: StateFrameKey::Vnode {
                    operator_id: "join".into(),
                    vnode: 0,
                },
                chunk: manifest.node_data.chunk,
                range: ByteRange {
                    offset: 0,
                    length: 2,
                },
                sha256: checkpoint_sha256(b"ew"),
            },
        ];
        manifest.referenced_chunks.push(ReferencedStateChunk {
            chunk: prior,
            object_length: 3,
            sha256: checkpoint_sha256(b"old"),
            ref_count: NonZeroU32::new(1).unwrap(),
        });
        manifest.prepared_sinks.push(PreparedSinkDescriptor {
            sink_name: "sink".into(),
            format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
            payload: Some(ByteRange {
                offset: 2,
                length: 0,
            }),
            sha256: checkpoint_descriptor_sha256(Some(b"")),
        });

        let one = KeyGroupCount::try_from(1_u16).unwrap();
        assert!(manifest.validate(one).is_empty());
        let encoded = serde_json::to_vec(&manifest).unwrap();
        let restored: CheckpointManifest = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(restored, manifest);
        assert!(!restored.reassignment_portable);
    }

    #[test]
    fn previous_manifest_versions_are_not_accepted() {
        let mut manifest = valid_manifest(1);
        manifest.version = 8;
        let errors = manifest.validate(KeyGroupCount::try_from(1_u16).unwrap());
        assert!(errors
            .iter()
            .any(|error| error.message.contains("unsupported manifest version 8")));

        let mut json = serde_json::to_value(valid_manifest(1)).unwrap();
        json.as_object_mut().unwrap().remove("node_data");
        assert!(serde_json::from_value::<CheckpointManifest>(json).is_err());

        let mut v8_shape = serde_json::to_value(valid_manifest(1)).unwrap();
        let object = v8_shape.as_object_mut().unwrap();
        object.insert("version".into(), serde_json::Value::from(8));
        object.remove("reassignment_portable");
        assert!(serde_json::from_value::<CheckpointManifest>(v8_shape).is_err());
    }

    #[test]
    fn local_manifest_cannot_claim_reassignment_portability() {
        let mut manifest = valid_manifest(1);
        manifest.reassignment_portable = true;

        let errors = manifest.validate(KeyGroupCount::try_from(1_u16).unwrap());
        assert!(errors.iter().any(|error| {
            error
                .message
                .contains("local manifest cannot claim vnode reassignment portability")
        }));
    }

    #[test]
    fn validation_rejects_noncanonical_or_mismatched_input_channels() {
        let mut manifest = valid_manifest(1);
        manifest.source_offsets.insert(
            "source".into(),
            ConnectorCheckpoint {
                input_channels: Some(vec![b"partition-1".to_vec(), b"partition-0".to_vec()]),
                ..ConnectorCheckpoint::default()
            },
        );

        assert!(manifest
            .validate(KeyGroupCount::try_from(1_u16).unwrap())
            .iter()
            .any(|error| error.message.contains("input_channels")));

        manifest
            .source_offsets
            .get_mut("source")
            .unwrap()
            .input_channels = Some(vec![crate::checkpoint::SINGLETON_WATERMARK_CHANNEL.to_vec()]);
        assert!(manifest
            .validate(KeyGroupCount::try_from(1_u16).unwrap())
            .iter()
            .any(|error| error.message.contains("reserved logical watermark channel")));

        manifest
            .source_offsets
            .get_mut("source")
            .unwrap()
            .input_channels = Some(vec![b"partition-0".to_vec(), b"partition-1".to_vec()]);
        manifest.channel_progress = vec![channel("partition-0", Some(1), false)];
        manifest.checkpoint_watermark = Some(1);
        assert!(manifest
            .validate(KeyGroupCount::try_from(1_u16).unwrap())
            .iter()
            .any(|error| error.message.contains("channel_progress roster")));
    }

    #[test]
    fn logical_singleton_is_independent_of_the_connector_roster() {
        let mut manifest = valid_manifest(1);
        manifest.source_offsets.insert(
            "source".into(),
            ConnectorCheckpoint {
                input_channels: Some(vec![b"partition-0".to_vec(), b"partition-1".to_vec()]),
                ..ConnectorCheckpoint::default()
            },
        );
        manifest.channel_progress = vec![ChannelProgress {
            participant_id: LOCAL_NODE_ID.0,
            source_name: "source".into(),
            input_channel: crate::checkpoint::SINGLETON_WATERMARK_CHANNEL.to_vec(),
            watermark: Some(1),
            idle: false,
        }];
        manifest.checkpoint_watermark = Some(1);

        let one = KeyGroupCount::try_from(1_u16).unwrap();
        assert!(manifest.validate(one).is_empty());

        manifest.channel_progress[0].source_name = "missing".into();
        assert!(manifest
            .validate(one)
            .iter()
            .any(|error| error.message.contains("not in source_names")));
    }

    #[test]
    fn validation_rejects_gaps_bad_refcounts_and_out_of_bounds_vnodes() {
        let mut manifest = valid_manifest(3);
        let current_data = b"xta";
        manifest.node_data.object_length = current_data.len() as u64;
        manifest.node_data.sha256 = checkpoint_sha256(current_data);
        manifest.state_frames.push(StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "join".into(),
                vnode: manifest.vnode_count,
            },
            chunk: manifest.node_data.chunk,
            range: ByteRange {
                offset: 1,
                length: 2,
            },
            sha256: checkpoint_sha256(b"ta"),
        });

        let errors = manifest.validate(KeyGroupCount::try_from(1_u16).unwrap());
        assert!(errors.iter().any(|error| error.message.contains("outside")));
        assert!(errors
            .iter()
            .any(|error| error.message.contains("starts at 1, expected 0")));
    }

    #[test]
    fn absent_and_empty_sink_descriptors_remain_distinct() {
        let mut absent = valid_manifest(2);
        absent.prepared_sinks.push(PreparedSinkDescriptor {
            sink_name: "sink".into(),
            format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
            payload: None,
            sha256: checkpoint_descriptor_sha256(None),
        });
        let mut empty = absent.clone();
        empty.prepared_sinks[0].payload = Some(ByteRange {
            offset: 0,
            length: 0,
        });
        empty.prepared_sinks[0].sha256 = checkpoint_descriptor_sha256(Some(b""));

        let one = KeyGroupCount::try_from(1_u16).unwrap();
        assert!(absent.validate(one).is_empty());
        assert!(empty.validate(one).is_empty());
        assert_ne!(
            absent.prepared_sinks[0].sha256,
            empty.prepared_sinks[0].sha256
        );
        assert_ne!(
            serde_json::to_vec(&absent).unwrap(),
            serde_json::to_vec(&empty).unwrap()
        );
    }
}
