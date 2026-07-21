//! Canonical, content-addressed recovery image for a committed cluster checkpoint.

use std::{collections::BTreeMap, num::NonZeroU64};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::checkpoint::{
    CheckpointAssignmentFence, ConnectorCheckpoint, PipelineIdentity, PIPELINE_IDENTITY_VERSION,
};
use crate::state::CheckpointAttempt;

/// Current canonical cluster recovery-capsule format.
pub const CLUSTER_RECOVERY_CAPSULE_VERSION: u32 = 5;

/// One participant's event-time position at a checkpoint cut.
///
/// An uninitialized participant must block watermark advancement. An explicitly idle participant
/// has no input that can currently hold back the cut and is excluded from the active minimum.
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "state", content = "watermark_ms")]
pub enum CheckpointWatermark {
    /// At least one required input has not established an event-time frontier.
    #[default]
    Uninitialized,
    /// Every required input is explicitly idle.
    Idle,
    /// Minimum active input watermark for this participant.
    Active(i64),
}

impl CheckpointWatermark {
    /// Fold two required participants into a safe cluster watermark state.
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

    /// Active watermark value, if this participant has active inputs.
    #[must_use]
    pub const fn active_value(self) -> Option<i64> {
        match self {
            Self::Active(value) => Some(value),
            Self::Uninitialized | Self::Idle => None,
        }
    }

    /// Reject the reserved uninitialized sentinel as an active watermark.
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

/// Immutable runtime state for one source at a committed checkpoint cut.
///
/// This type is deliberately not serializable. The durable authority remains
/// [`ClusterRecoveryCapsule`]; this is its validated runtime projection.
#[derive(Debug, PartialEq, Eq)]
pub struct SourceHandoffState {
    checkpoint: ConnectorCheckpoint,
    watermark_ms: Option<i64>,
}

impl SourceHandoffState {
    /// Complete connector-defined offsets and metadata plus its provider-neutral assignment cut.
    #[must_use]
    pub const fn checkpoint(&self) -> &ConnectorCheckpoint {
        &self.checkpoint
    }

    /// Event-time watermark at the committed cut, when this source had one.
    #[must_use]
    pub const fn watermark(&self) -> Option<i64> {
        self.watermark_ms
    }
}

/// Validated source recovery state bound to one committed cluster cut.
///
/// The object is immutable and intended to be shared as one `Arc` with an
/// assignment publication. It is not a second durable or serialized format.
#[derive(Debug, PartialEq, Eq)]
pub struct CommittedSourceHandoff {
    attempt: CheckpointAttempt,
    assignment_version: u64,
    sources: BTreeMap<String, SourceHandoffState>,
    cluster_watermark: CheckpointWatermark,
    recovery_watermark_frontier: Option<i64>,
}

impl CommittedSourceHandoff {
    /// Exact committed checkpoint attempt supplying this handoff.
    #[must_use]
    pub const fn attempt(&self) -> CheckpointAttempt {
        self.attempt
    }

    /// Assignment version sealed by the committed checkpoint.
    #[must_use]
    pub const fn checkpoint_assignment_version(&self) -> u64 {
        self.assignment_version
    }

    /// Explicit cluster event-time status at the committed cut.
    #[must_use]
    pub const fn cluster_watermark(&self) -> CheckpointWatermark {
        self.cluster_watermark
    }

    /// Durable numeric event-time frontier restored with the committed cut.
    ///
    /// An idle cut can retain the last active frontier even though it has no
    /// currently active input. An uninitialized cut has no recovery frontier.
    #[must_use]
    pub const fn recovery_watermark_frontier(&self) -> Option<i64> {
        self.recovery_watermark_frontier
    }

    /// State for `source`, or `None` when the committed cut did not contain it.
    #[must_use]
    pub fn source(&self, source: &str) -> Option<&SourceHandoffState> {
        self.sources.get(source)
    }

    /// Iterate the complete source cut in canonical source-name order.
    #[must_use]
    pub fn sources(
        &self,
    ) -> impl ExactSizeIterator<Item = (&str, &SourceHandoffState)> + DoubleEndedIterator {
        self.sources
            .iter()
            .map(|(name, state)| (name.as_str(), state))
    }

    /// Number of sources captured in the committed cut.
    #[must_use]
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    /// Whether the committed cut contains no sources.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
}

/// Hard bound on the exact canonical capsule body.
pub const MAX_RECOVERY_CAPSULE_BYTES: usize = 8 * 1024 * 1024;

/// Content-addressed reference carried by a durable checkpoint outcome.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryCapsuleRef {
    /// Exact epoch encoded by the capsule and its object path.
    pub epoch: u64,
    /// Exact checkpoint attempt encoded by the capsule and its object path.
    pub checkpoint_id: u64,
    /// Lowercase hexadecimal SHA-256 of the exact canonical JSON body.
    pub sha256: String,
    /// Exact canonical JSON body length.
    pub len: u64,
}

impl RecoveryCapsuleRef {
    /// Validate the persisted reference format.
    ///
    /// # Errors
    /// Returns a description when the digest or encoded length is not canonical.
    pub fn validate(&self) -> Result<(), String> {
        if !CheckpointAttempt::new(self.epoch, self.checkpoint_id).is_canonical() {
            return Err(
                "recovery capsule reference must use one nonzero canonical checkpoint ID".into(),
            );
        }
        validate_digest("recovery capsule", &self.sha256)?;
        if self.len == 0 || self.len > u64::try_from(MAX_RECOVERY_CAPSULE_BYTES).unwrap_or(u64::MAX)
        {
            return Err(format!(
                "recovery capsule length {} is outside 1..={MAX_RECOVERY_CAPSULE_BYTES}",
                self.len
            ));
        }
        Ok(())
    }
}

/// Exact participant artifacts admitted into a cluster recovery image.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantRecoveryRef {
    /// Stable participant node identifier.
    pub participant_id: u64,
    /// SHA-256 of the participant's canonical readiness record.
    pub readiness_sha256: String,
    /// SHA-256 of the participant's normalized prepared manifest.
    pub manifest_sha256: String,
    /// SHA-256 of the participant's portable non-vnode state image.
    pub portable_state_sha256: String,
}

impl ParticipantRecoveryRef {
    fn validate(&self) -> Result<(), String> {
        if self.participant_id == 0 {
            return Err("recovery capsule participant ID cannot be 0".into());
        }
        validate_digest("participant readiness", &self.readiness_sha256)?;
        validate_digest("participant manifest", &self.manifest_sha256)?;
        validate_digest("participant portable state", &self.portable_state_sha256)?;
        Ok(())
    }
}

/// Canonical global recovery image selected by one cluster Commit outcome.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterRecoveryCapsule {
    /// Capsule payload format.
    pub version: u32,
    /// Exact checkpoint attempt represented by this image.
    pub attempt: CheckpointAttempt,
    /// Durable deployment incarnation that owns the image.
    pub deployment_id: String,
    /// Exact logical pipeline and recovery-state ABI.
    pub pipeline_identity: PipelineIdentity,
    /// Exact vnode assignment and process roster covered by the image.
    pub assignment_fence: CheckpointAssignmentFence,
    /// SHA-256 of the canonical state-backend seal inventory.
    pub seal_inventory_sha256: String,
    /// Participant artifacts, sorted by participant ID and exactly covering the fence.
    pub participants: Vec<ParticipantRecoveryRef>,
    /// Complete per-source connector offsets in canonical map order.
    pub source_offsets: BTreeMap<String, BTreeMap<String, String>>,
    /// Complete per-source connector metadata in canonical map order.
    pub source_metadata: BTreeMap<String, BTreeMap<String, String>>,
    /// Assignment version captured by each partitioned source, in canonical source-name order.
    ///
    /// The map may be sparse because runtime topology determines which sources require an
    /// assignment cut. Every populated version must match `assignment_fence`.
    pub source_assignment_versions: BTreeMap<String, NonZeroU64>,
    /// Per-source event-time watermarks in canonical map order.
    pub source_watermarks: BTreeMap<String, i64>,
    /// Cluster-wide event-time state at this exact cut.
    pub cluster_watermark: CheckpointWatermark,
    /// Durable numeric event-time frontier restored with this cut.
    ///
    /// This equals the active watermark for an active cut, is absent for an
    /// uninitialized cut, and may retain the last active value for an idle cut.
    pub recovery_watermark_frontier: Option<i64>,
    /// SHA-256 of the canonical portable non-vnode state image.
    pub portable_state_sha256: String,
}

impl ClusterRecoveryCapsule {
    /// Validate all canonical and cross-record invariants.
    ///
    /// # Errors
    /// Returns a description when the capsule cannot name one exact cluster recovery image.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != CLUSTER_RECOVERY_CAPSULE_VERSION {
            return Err(format!(
                "unsupported recovery capsule version {}; expected {CLUSTER_RECOVERY_CAPSULE_VERSION}",
                self.version
            ));
        }
        if !self.attempt.is_canonical() {
            return Err(
                "recovery capsule attempt must use one nonzero canonical checkpoint ID".into(),
            );
        }

        let deployment = uuid::Uuid::parse_str(&self.deployment_id)
            .map_err(|error| format!("recovery capsule deployment identity is invalid: {error}"))?;
        if deployment.is_nil() || deployment.to_string() != self.deployment_id {
            return Err(
                "recovery capsule deployment identity must be a canonical non-nil UUID".into(),
            );
        }

        if self.pipeline_identity.canonical_version != PIPELINE_IDENTITY_VERSION {
            return Err(format!(
                "unsupported recovery capsule pipeline identity version {}; expected {PIPELINE_IDENTITY_VERSION}",
                self.pipeline_identity.canonical_version
            ));
        }
        validate_digest("pipeline identity", &self.pipeline_identity.sha256)?;

        if !self.assignment_fence.is_canonical() {
            return Err("recovery capsule assignment fence is not canonical".into());
        }
        validate_digest("checkpoint seal inventory", &self.seal_inventory_sha256)?;
        validate_digest("portable state", &self.portable_state_sha256)?;

        if self.participants.is_empty() {
            return Err("recovery capsule has no participants".into());
        }
        for participant in &self.participants {
            participant.validate()?;
            if participant.portable_state_sha256 != self.portable_state_sha256 {
                return Err(format!(
                    "recovery capsule participant {} has divergent portable state",
                    participant.participant_id
                ));
            }
        }
        if self
            .participants
            .windows(2)
            .any(|pair| pair[0].participant_id >= pair[1].participant_id)
        {
            return Err(
                "recovery capsule participants must be sorted and unique by participant ID".into(),
            );
        }
        let participant_ids: Vec<u64> = self
            .participants
            .iter()
            .map(|participant| participant.participant_id)
            .collect();
        if participant_ids != self.assignment_fence.participant_ids() {
            return Err(
                "recovery capsule participants do not exactly cover its assignment fence".into(),
            );
        }
        validate_source_maps(&self.source_offsets, &self.source_metadata)?;
        for (source, assignment_version) in &self.source_assignment_versions {
            validate_name("source assignment version", source)?;
            if !self.source_offsets.contains_key(source) {
                return Err(format!(
                    "recovery capsule source assignment version names unknown source '{source}'"
                ));
            }
            if assignment_version.get() != self.assignment_fence.assignment_version {
                return Err(format!(
                    "recovery capsule source assignment version for '{source}' is {}; expected {}",
                    assignment_version, self.assignment_fence.assignment_version
                ));
            }
        }
        for (source, watermark) in &self.source_watermarks {
            validate_name("source watermark", source)?;
            if !self.source_offsets.contains_key(source) {
                return Err(format!(
                    "recovery capsule source watermark '{source}' has no connector checkpoint"
                ));
            }
            if *watermark == i64::MIN {
                return Err(format!(
                    "recovery capsule source watermark '{source}' uses the uninitialized sentinel"
                ));
            }
        }
        self.cluster_watermark.validate()?;
        match self.cluster_watermark {
            CheckpointWatermark::Active(active) => {
                if self.recovery_watermark_frontier != Some(active) {
                    return Err(
                        "active recovery capsule watermark must equal its recovery frontier".into(),
                    );
                }
            }
            CheckpointWatermark::Uninitialized => {
                if self.recovery_watermark_frontier.is_some() {
                    return Err(
                        "uninitialized recovery capsule watermark cannot have a recovery frontier"
                            .into(),
                    );
                }
            }
            CheckpointWatermark::Idle => {}
        }
        if let Some(frontier) = self.recovery_watermark_frontier {
            if frontier == i64::MIN {
                return Err(
                    "recovery capsule watermark frontier cannot use the uninitialized sentinel"
                        .into(),
                );
            }
            if self
                .source_watermarks
                .values()
                .any(|source_watermark| *source_watermark < frontier)
            {
                return Err(
                    "recovery capsule watermark frontier exceeds a source watermark".into(),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn encode_and_reference(&self) -> Result<(Vec<u8>, RecoveryCapsuleRef), String> {
        self.validate()?;
        let encoded = canonical_json_bytes(self).map_err(|error| error.to_string())?;
        if encoded.len() > MAX_RECOVERY_CAPSULE_BYTES {
            return Err(format!(
                "encoded recovery capsule is {} bytes; maximum is {MAX_RECOVERY_CAPSULE_BYTES}",
                encoded.len()
            ));
        }
        let reference = RecoveryCapsuleRef {
            epoch: self.attempt.epoch,
            checkpoint_id: self.attempt.checkpoint_id,
            sha256: sha256_hex(&encoded),
            len: u64::try_from(encoded.len())
                .map_err(|_| "recovery capsule length overflow".to_string())?,
        };
        reference.validate()?;
        Ok((encoded, reference))
    }
}

impl TryFrom<&ClusterRecoveryCapsule> for CommittedSourceHandoff {
    type Error = String;

    fn try_from(capsule: &ClusterRecoveryCapsule) -> Result<Self, Self::Error> {
        capsule.validate()?;

        let mut sources = BTreeMap::new();
        for (source, offsets) in &capsule.source_offsets {
            let metadata = capsule.source_metadata.get(source).ok_or_else(|| {
                format!("recovery capsule source '{source}' is missing connector metadata")
            })?;
            sources.insert(
                source.clone(),
                SourceHandoffState {
                    checkpoint: ConnectorCheckpoint {
                        offsets: offsets
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect(),
                        metadata: metadata
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect(),
                        source_assignment_version: capsule
                            .source_assignment_versions
                            .get(source)
                            .copied(),
                    },
                    watermark_ms: capsule.source_watermarks.get(source).copied(),
                },
            );
        }

        Ok(Self {
            attempt: capsule.attempt,
            assignment_version: capsule.assignment_fence.assignment_version,
            sources,
            cluster_watermark: capsule.cluster_watermark,
            recovery_watermark_frontier: capsule.recovery_watermark_frontier,
        })
    }
}

fn validate_source_maps(
    offsets: &BTreeMap<String, BTreeMap<String, String>>,
    metadata: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<(), String> {
    if offsets.keys().ne(metadata.keys()) {
        return Err(
            "recovery capsule source offsets and metadata must cover the same sources".into(),
        );
    }
    for (source, source_offsets) in offsets {
        validate_name("source", source)?;
        for key in source_offsets.keys() {
            validate_name("source offset key", key)?;
        }
        for key in metadata
            .get(source)
            .expect("source key sets were compared above")
            .keys()
        {
            validate_name("source metadata key", key)?;
        }
    }
    Ok(())
}

fn validate_name(kind: &str, name: &str) -> Result<(), String> {
    if name.is_empty() || name.trim() != name {
        return Err(format!("recovery capsule has a non-canonical {kind} name"));
    }
    Ok(())
}

fn validate_digest(kind: &str, digest: &str) -> Result<(), String> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "recovery capsule {kind} SHA-256 must be 64 lowercase hexadecimal characters"
        ));
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
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

/// Serialize any JSON-compatible value with recursively sorted object keys.
///
/// This is the canonical digest input for manifests and readiness records containing `HashMap`s.
///
/// # Errors
/// Returns a JSON error when `value` cannot be represented as JSON.
pub fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    serde_json::to_vec(&CanonicalJsonValue::from(value))
}

/// SHA-256 of [`canonical_json_bytes`], encoded as lowercase hexadecimal.
///
/// # Errors
/// Returns a JSON error when `value` cannot be represented as JSON.
pub fn canonical_json_sha256<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    canonical_json_bytes(value).map(|bytes| sha256_hex(&bytes))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::checkpoint::CheckpointParticipant;

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn capsule() -> ClusterRecoveryCapsule {
        let assignment_fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[2, 9, 2],
            vec![
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(22),
                },
                CheckpointParticipant {
                    node_id: 9,
                    boot_incarnation: uuid::Uuid::from_u128(99),
                },
            ],
        )
        .unwrap();
        ClusterRecoveryCapsule {
            version: CLUSTER_RECOVERY_CAPSULE_VERSION,
            attempt: CheckpointAttempt::canonical(10),
            deployment_id: uuid::Uuid::from_u128(500).to_string(),
            pipeline_identity: PipelineIdentity {
                canonical_version: PIPELINE_IDENTITY_VERSION,
                sha256: digest(1),
            },
            assignment_fence,
            seal_inventory_sha256: digest(2),
            participants: vec![
                ParticipantRecoveryRef {
                    participant_id: 2,
                    readiness_sha256: digest(3),
                    manifest_sha256: digest(4),
                    portable_state_sha256: digest(9),
                },
                ParticipantRecoveryRef {
                    participant_id: 9,
                    readiness_sha256: digest(6),
                    manifest_sha256: digest(7),
                    portable_state_sha256: digest(9),
                },
            ],
            source_offsets: BTreeMap::from([(
                "events".into(),
                BTreeMap::from([("partition:0".into(), "41".into())]),
            )]),
            source_metadata: BTreeMap::from([(
                "events".into(),
                BTreeMap::from([("topic".into(), "events".into())]),
            )]),
            source_assignment_versions: BTreeMap::from([(
                "events".into(),
                NonZeroU64::new(7).unwrap(),
            )]),
            source_watermarks: BTreeMap::from([("events".into(), 1_000)]),
            cluster_watermark: CheckpointWatermark::Active(900),
            recovery_watermark_frontier: Some(900),
            portable_state_sha256: digest(9),
        }
    }

    #[test]
    fn canonical_json_digest_ignores_hash_map_insertion_order() {
        let mut first = HashMap::new();
        first.insert("z", HashMap::from([("b", 2), ("a", 1)]));
        first.insert("a", HashMap::from([("d", 4), ("c", 3)]));

        let mut second = HashMap::new();
        second.insert("a", HashMap::from([("c", 3), ("d", 4)]));
        second.insert("z", HashMap::from([("a", 1), ("b", 2)]));

        assert_eq!(
            canonical_json_bytes(&first).unwrap(),
            canonical_json_bytes(&second).unwrap()
        );
        assert_eq!(
            canonical_json_sha256(&first).unwrap(),
            canonical_json_sha256(&second).unwrap()
        );
    }

    #[test]
    fn capsule_requires_exact_sorted_fence_roster() {
        let valid = capsule();
        valid.validate().unwrap();

        let mut noncanonical_attempt = valid.clone();
        noncanonical_attempt.attempt.checkpoint_id += 1;
        assert!(noncanonical_attempt
            .validate()
            .unwrap_err()
            .contains("one nonzero canonical checkpoint ID"));

        let mut previous_version = valid.clone();
        previous_version.version = 4;
        assert!(previous_version
            .validate()
            .unwrap_err()
            .contains("unsupported recovery capsule version 4"));

        let mut reordered = valid.clone();
        reordered.participants.reverse();
        assert!(reordered
            .validate()
            .unwrap_err()
            .contains("sorted and unique"));

        let mut incomplete = valid;
        incomplete.participants.pop();
        assert!(incomplete.validate().unwrap_err().contains("exactly cover"));
    }

    #[test]
    fn encoded_reference_binds_exact_canonical_body() {
        let capsule = capsule();
        let (encoded, reference) = capsule.encode_and_reference().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["version"], CLUSTER_RECOVERY_CAPSULE_VERSION);
        assert!(json.get("bootstrap_manifest_participant_id").is_none());
        assert!(json.get("cluster_min_watermark").is_none());
        assert_eq!(json["cluster_watermark"]["state"], "active");
        assert_eq!(json["cluster_watermark"]["watermark_ms"], 900);
        assert_eq!(json["recovery_watermark_frontier"], 900);
        assert_eq!(json["source_assignment_versions"]["events"], 7);
        assert_eq!(reference.len, u64::try_from(encoded.len()).unwrap());
        assert_eq!(reference.sha256, sha256_hex(&encoded));
        reference.validate().unwrap();

        let mut noncanonical_reference = reference;
        noncanonical_reference.checkpoint_id += 1;
        assert!(noncanonical_reference
            .validate()
            .unwrap_err()
            .contains("one nonzero canonical checkpoint ID"));
    }

    #[test]
    fn runtime_handoff_preserves_the_complete_committed_source_cut() {
        let capsule = capsule();
        let handoff = CommittedSourceHandoff::try_from(&capsule).unwrap();

        assert_eq!(handoff.attempt(), CheckpointAttempt::canonical(10));
        assert_eq!(handoff.checkpoint_assignment_version(), 7);
        assert_eq!(
            handoff.cluster_watermark(),
            CheckpointWatermark::Active(900)
        );
        assert_eq!(handoff.recovery_watermark_frontier(), Some(900));
        assert_eq!(handoff.source_count(), 1);
        assert!(!handoff.is_empty());

        let events = handoff.source("events").unwrap();
        assert_eq!(
            events
                .checkpoint()
                .offsets
                .get("partition:0")
                .map(String::as_str),
            Some("41")
        );
        assert_eq!(
            events
                .checkpoint()
                .metadata
                .get("topic")
                .map(String::as_str),
            Some("events")
        );
        assert_eq!(
            events.checkpoint().source_assignment_version,
            NonZeroU64::new(7)
        );
        assert_eq!(events.watermark(), Some(1_000));
        assert!(handoff.source("missing").is_none());
    }

    #[test]
    fn source_assignment_versions_are_sparse_and_fenced() {
        let mut sparse = capsule();
        sparse.source_assignment_versions.clear();
        sparse.validate().unwrap();
        let sparse_handoff = CommittedSourceHandoff::try_from(&sparse).unwrap();
        assert_eq!(
            sparse_handoff
                .source("events")
                .unwrap()
                .checkpoint()
                .source_assignment_version,
            None
        );

        let mut unknown = capsule();
        unknown.source_assignment_versions.insert(
            "missing".into(),
            NonZeroU64::new(unknown.assignment_fence.assignment_version).unwrap(),
        );
        assert!(unknown
            .validate()
            .unwrap_err()
            .contains("unknown source 'missing'"));

        let mut mismatched = capsule();
        mismatched
            .source_assignment_versions
            .insert("events".into(), NonZeroU64::new(8).unwrap());
        assert!(mismatched
            .validate()
            .unwrap_err()
            .contains("for 'events' is 8; expected 7"));
    }

    #[test]
    fn malformed_digest_and_source_metadata_fail_closed() {
        let mut bad_digest = capsule();
        bad_digest.participants[0].manifest_sha256 = "AB".repeat(32);
        assert!(bad_digest.validate().is_err());

        let mut missing_metadata = capsule();
        missing_metadata.source_metadata.clear();
        assert!(missing_metadata
            .validate()
            .unwrap_err()
            .contains("same sources"));
        assert!(CommittedSourceHandoff::try_from(&missing_metadata).is_err());

        let mut divergent_state = capsule();
        divergent_state.participants[1].portable_state_sha256 = digest(0xaa);
        assert!(divergent_state
            .validate()
            .unwrap_err()
            .contains("divergent portable state"));
    }

    #[test]
    fn capsule_preserves_non_active_watermark_states() {
        for (watermark, frontier) in [
            (CheckpointWatermark::Uninitialized, None),
            (CheckpointWatermark::Idle, None),
            (CheckpointWatermark::Idle, Some(800)),
        ] {
            let mut capsule = capsule();
            capsule.cluster_watermark = watermark;
            capsule.recovery_watermark_frontier = frontier;
            let (encoded, _) = capsule.encode_and_reference().unwrap();
            let decoded: ClusterRecoveryCapsule = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(decoded.cluster_watermark, watermark);
            assert_eq!(decoded.recovery_watermark_frontier, frontier);
            let handoff = CommittedSourceHandoff::try_from(&decoded).unwrap();
            assert_eq!(handoff.cluster_watermark(), watermark);
            assert_eq!(handoff.recovery_watermark_frontier(), frontier);
        }
    }

    #[test]
    fn recovery_watermark_status_and_frontier_must_agree() {
        let mut reserved = capsule();
        reserved.cluster_watermark = CheckpointWatermark::Active(i64::MIN);
        reserved.recovery_watermark_frontier = Some(i64::MIN);
        assert!(reserved
            .validate()
            .unwrap_err()
            .contains("uninitialized sentinel"));

        let mut missing = capsule();
        missing.recovery_watermark_frontier = None;
        assert!(missing
            .validate()
            .unwrap_err()
            .contains("must equal its recovery frontier"));

        let mut mismatched = capsule();
        mismatched.recovery_watermark_frontier = Some(899);
        assert!(mismatched
            .validate()
            .unwrap_err()
            .contains("must equal its recovery frontier"));

        let mut uninitialized = capsule();
        uninitialized.cluster_watermark = CheckpointWatermark::Uninitialized;
        assert!(uninitialized
            .validate()
            .unwrap_err()
            .contains("cannot have a recovery frontier"));

        let mut idle_reserved = capsule();
        idle_reserved.cluster_watermark = CheckpointWatermark::Idle;
        idle_reserved.recovery_watermark_frontier = Some(i64::MIN);
        assert!(idle_reserved
            .validate()
            .unwrap_err()
            .contains("uninitialized sentinel"));
    }

    #[test]
    fn recovery_watermark_frontier_must_not_exceed_sources() {
        let mut ahead = capsule();
        ahead.cluster_watermark = CheckpointWatermark::Idle;
        ahead.recovery_watermark_frontier = Some(1_001);
        assert!(ahead
            .validate()
            .unwrap_err()
            .contains("frontier exceeds a source watermark"));

        let mut at_source = capsule();
        at_source.cluster_watermark = CheckpointWatermark::Active(1_000);
        at_source.recovery_watermark_frontier = Some(1_000);
        at_source.validate().unwrap();
    }
}
