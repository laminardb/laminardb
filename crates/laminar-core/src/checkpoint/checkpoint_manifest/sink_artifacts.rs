//! Checkpoint-owned sink intents and phase-one descriptor validation.

use sha2::{Digest, Sha256};

use super::{ByteRange, CheckpointManifest, PREPARED_SINK_DESCRIPTOR_VERSION};

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

/// Recovery evidence persisted before a checkpoint-committable sink may write.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PreparedSinkArtifactIntent {
    /// Stable registered sink name.
    pub sink_name: String,
    /// Runtime intent-envelope version.
    pub format_version: u16,
    /// Connector intent bytes in the current node data object.
    pub payload: Option<ByteRange>,
    /// Presence-domain-separated SHA-256 of the optional payload.
    pub sha256: String,
}

/// SHA-256 for one optional sink artifact intent.
#[must_use]
pub fn checkpoint_artifact_intent_sha256(payload: Option<&[u8]>) -> String {
    let mut digest = Sha256::new();
    digest.update(b"laminardb/checkpoint-sink-artifact-intent/v1\0");
    match payload {
        Some(payload) => {
            digest.update([1]);
            digest.update(payload);
        }
        None => digest.update([0]),
    }
    format!("{:x}", digest.finalize())
}

pub(super) fn validate_sink_artifacts(
    manifest: &CheckpointManifest,
    current_ranges: &mut Vec<(ByteRange, String)>,
    error: &mut impl FnMut(String),
) {
    if !manifest.sink_artifact_intents.is_empty() {
        validate_intent_roster(manifest, error);
        for intent in &manifest.sink_artifact_intents {
            validate_entry(
                "sink artifact intent",
                &intent.sink_name,
                intent.format_version,
                intent.payload,
                &intent.sha256,
                checkpoint_artifact_intent_sha256,
                manifest,
                current_ranges,
                error,
            );
        }
    }
    if !manifest
        .prepared_sinks
        .windows(2)
        .all(|pair| pair[0].sink_name < pair[1].sink_name)
    {
        error("prepared_sinks must be strictly ordered by sink_name".into());
    }
    for sink in &manifest.prepared_sinks {
        validate_entry(
            "prepared sink",
            &sink.sink_name,
            sink.format_version,
            sink.payload,
            &sink.sha256,
            super::checkpoint_descriptor_sha256,
            manifest,
            current_ranges,
            error,
        );
    }
}

fn validate_intent_roster(manifest: &CheckpointManifest, error: &mut impl FnMut(String)) {
    if !manifest
        .sink_artifact_intents
        .windows(2)
        .all(|pair| pair[0].sink_name < pair[1].sink_name)
    {
        error("sink_artifact_intents must be strictly ordered by sink_name".into());
    }
    let intent_names = manifest
        .sink_artifact_intents
        .iter()
        .map(|intent| intent.sink_name.as_str());
    let prepared_names = manifest
        .prepared_sinks
        .iter()
        .map(|sink| sink.sink_name.as_str());
    if !intent_names.eq(prepared_names) {
        error("sink artifact intents must name every prepared sink exactly once".into());
    }
}

#[allow(clippy::too_many_arguments)] // One generic envelope check keeps both domains identical.
fn validate_entry(
    label: &str,
    sink_name: &str,
    format_version: u16,
    payload: Option<ByteRange>,
    sha256: &str,
    absent_digest: fn(Option<&[u8]>) -> String,
    manifest: &CheckpointManifest,
    current_ranges: &mut Vec<(ByteRange, String)>,
    error: &mut impl FnMut(String),
) {
    if sink_name.is_empty() {
        error(format!("{label} name must not be empty"));
    }
    if manifest
        .sink_names
        .binary_search_by(|candidate| candidate.as_str().cmp(sink_name))
        .is_err()
    {
        error(format!("{label} '{sink_name}' is not in sink_names"));
    }
    if format_version != PREPARED_SINK_DESCRIPTOR_VERSION {
        error(format!(
            "{label} '{sink_name}' format_version must be {PREPARED_SINK_DESCRIPTOR_VERSION}"
        ));
    }
    if !super::is_sha256(sha256) {
        error(format!(
            "{label} '{sink_name}' digest must be lowercase SHA-256"
        ));
    }
    match payload {
        Some(range) => {
            super::validate_range(range, Some(manifest.node_data.object_length), label, error);
            current_ranges.push((range, format!("{label} '{sink_name}'")));
        }
        None if sha256 != absent_digest(None) => error(format!(
            "{label} '{sink_name}' without a payload has the wrong domain-separated digest"
        )),
        None => {}
    }
}
