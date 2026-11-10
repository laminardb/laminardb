//! Append-only materialized-view logs. Each checkpoint packs every view's new segments into one
//! view-log object per participant; later checkpoints reference a segment while its view's
//! retention keeps it. The log is not owner state and never enters a state-frame inventory.

use std::collections::HashMap;

use super::{
    is_sha256, validate_range, ByteRange, CheckpointManifest, ReferencedStateChunk, StateChunkId,
};

/// Most segments one manifest may retain across every view log. The bound keeps the manifest
/// within its encoded-size limit; compaction merges a log's segments, so their number follows
/// the log's bytes rather than the checkpoints that wrote it.
pub const MAX_VIEW_LOG_SEGMENTS: usize = 32_768;

/// Most segments one cluster cut retains across all of its participants. Cleanup and topology
/// migration load a cut's manifests, and a migration root beside its first target cut, within
/// 16 MiB together; at about 285 encoded bytes a segment, two cuts at this budget stay near
/// 13 MiB and leave room for state frames.
pub const MAX_CLUSTER_VIEW_LOG_SEGMENTS: usize = 24_576;

/// Most segments one manifest may retain when its cut has `participants` participants: in a
/// cluster an even share of [`MAX_CLUSTER_VIEW_LOG_SEGMENTS`], and never more than
/// [`MAX_VIEW_LOG_SEGMENTS`].
#[must_use]
pub fn view_log_segment_limit(participants: Option<usize>) -> usize {
    participants.map_or(MAX_VIEW_LOG_SEGMENTS, |participants| {
        MAX_VIEW_LOG_SEGMENTS.min(MAX_CLUSTER_VIEW_LOG_SEGMENTS / participants.max(1))
    })
}

/// Inclusive event-time range of one segment's rows.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EventTimeBounds {
    /// Earliest event time in the segment.
    pub min: i64,
    /// Latest event time in the segment.
    pub max: i64,
}

/// One immutable run of rows in a view-log partition.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ViewLogSegment {
    /// Log partition within the manifest's vnode domain.
    pub partition: u16,
    /// Position of the segment in its view's log; later segments hold later rows.
    pub sequence: u64,
    /// View-log object holding the payload.
    pub chunk: StateChunkId,
    /// Exact payload range within that object.
    pub range: ByteRange,
    /// SHA-256 of only this payload.
    pub sha256: String,
    /// Exact number of rows.
    pub rows: u64,
    /// Event-time range of the rows; `None` when the view has no event-time column.
    pub event_time: Option<EventTimeBounds>,
}

/// One append-only view's retained log at this cut.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ViewLog {
    /// Catalog name of the view.
    pub view: String,
    /// Sequence of the view's next segment; every retained segment is below it.
    pub next_sequence: u64,
    /// Greatest event-time frontier the view had reached on this participant at this cut. In a
    /// local cut an event-time retention keeps exactly the rows at or after this frontier minus
    /// its interval; a cluster cut uses the committed index's view frontier instead.
    pub frontier: Option<i64>,
    /// Whether every input of the view was idle at this cut. An idle participant's frontier
    /// does not hold back the view's committed cluster frontier.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub idle: bool,
    /// Retained segments ordered by partition, then sequence.
    pub segments: Vec<ViewLogSegment>,
}

pub(super) fn validate_view_logs(manifest: &CheckpointManifest, error: &mut impl FnMut(String)) {
    let current = manifest.view_log.as_ref().map(|object| object.chunk);
    if let Some(object) = &manifest.view_log {
        if object.chunk != manifest.node_data.chunk {
            error("view_log chunk must match the manifest participant and checkpoint".into());
        }
        if object.object_length == 0 || !is_sha256(&object.sha256) {
            error("view_log must be a non-empty object with a lowercase SHA-256 digest".into());
        }
    }
    let referenced = referenced_view_logs(manifest, error);
    if !manifest
        .view_logs
        .windows(2)
        .all(|pair| pair[0].view < pair[1].view)
    {
        error("view_logs must be strictly ordered and unique by view".into());
    }

    let mut segment_count = 0_usize;
    let mut prior_counts = HashMap::<StateChunkId, u32>::new();
    let mut current_ranges = Vec::new();
    for log in &manifest.view_logs {
        if log.view.is_empty() {
            error("view log names an empty view".into());
        }
        if !log.segments.windows(2).all(|pair| {
            (pair[0].partition, pair[0].sequence) < (pair[1].partition, pair[1].sequence)
        }) {
            error(format!(
                "view log '{}' segments must be strictly ordered by partition and sequence",
                log.view
            ));
        }
        segment_count = segment_count.saturating_add(log.segments.len());
        for segment in &log.segments {
            let owner = format!(
                "view log '{}' partition {} segment {}",
                log.view, segment.partition, segment.sequence
            );
            validate_segment(manifest, log, segment, &owner, error);
            let object_length = if Some(segment.chunk) == current {
                current_ranges.push((segment.range, owner.clone()));
                manifest
                    .view_log
                    .as_ref()
                    .map(|object| object.object_length)
            } else if let Some(reference) = referenced.get(&segment.chunk) {
                let count = prior_counts.entry(segment.chunk).or_default();
                *count = count.saturating_add(1);
                Some(reference.object_length)
            } else {
                error(format!(
                    "{owner} references an untracked view-log object {:?}",
                    segment.chunk
                ));
                None
            };
            validate_range(segment.range, object_length, &owner, error);
        }
    }
    let limit = view_log_segment_limit(
        manifest
            .assignment_fence
            .as_ref()
            .map(|fence| fence.participants.len()),
    );
    if segment_count > limit {
        error(format!(
            "view logs retain {segment_count} segments; this manifest admits at most {limit}"
        ));
    }
    for reference in &manifest.referenced_view_logs {
        let actual = prior_counts.get(&reference.chunk).copied().unwrap_or(0);
        if actual != reference.ref_count.get() {
            error(format!(
                "referenced view log {:?} declares ref_count {}, but {actual} segments reference it",
                reference.chunk, reference.ref_count
            ));
        }
    }
    validate_current_layout(manifest, current_ranges, error);
}

fn referenced_view_logs<'a>(
    manifest: &'a CheckpointManifest,
    error: &mut impl FnMut(String),
) -> HashMap<StateChunkId, &'a ReferencedStateChunk> {
    let references = &manifest.referenced_view_logs;
    if !references
        .windows(2)
        .all(|pair| pair[0].chunk < pair[1].chunk)
    {
        error("referenced_view_logs must be strictly ordered and unique".into());
    }
    // A cluster participant may list another participant's segments after adopting the log of
    // one that left; a local cut has one participant.
    let cluster = manifest.assignment_fence.is_some();
    for reference in references {
        if (!cluster && reference.chunk.participant_id != manifest.participant_id)
            || reference.chunk.checkpoint_id == 0
            || reference.chunk.checkpoint_id >= manifest.checkpoint_id
        {
            error(format!(
                "referenced view log {:?} is not an older object this participant may reference",
                reference.chunk
            ));
        }
        if !is_sha256(&reference.sha256) {
            error(format!(
                "referenced view log {:?} digest must be lowercase SHA-256",
                reference.chunk
            ));
        }
    }
    references
        .iter()
        .map(|reference| (reference.chunk, reference))
        .collect()
}

fn validate_segment(
    manifest: &CheckpointManifest,
    log: &ViewLog,
    segment: &ViewLogSegment,
    owner: &str,
    error: &mut impl FnMut(String),
) {
    if segment.partition >= manifest.vnode_count {
        error(format!("{owner} is outside the vnode domain"));
    }
    if segment.sequence >= log.next_sequence {
        error(format!(
            "{owner} is not below the log's next sequence {}",
            log.next_sequence
        ));
    }
    if segment.rows == 0 || segment.range.length == 0 {
        error(format!("{owner} is empty"));
    }
    if !is_sha256(&segment.sha256) {
        error(format!("{owner} digest must be lowercase SHA-256"));
    }
    if segment
        .event_time
        .is_some_and(|bounds| bounds.min > bounds.max)
    {
        error(format!("{owner} event-time bounds are reversed"));
    }
}

fn validate_current_layout(
    manifest: &CheckpointManifest,
    current_ranges: Vec<(ByteRange, String)>,
    error: &mut impl FnMut(String),
) {
    let Some(object) = &manifest.view_log else {
        return;
    };
    let mut expected_offset = 0_u64;
    for (range, owner) in current_ranges {
        if range.offset != expected_offset {
            error(format!(
                "{owner} starts at {}, expected {expected_offset} in the view-log object",
                range.offset
            ));
        }
        if let Some(end) = range.end() {
            expected_offset = end;
        }
    }
    if expected_offset != object.object_length {
        error(format!(
            "current view-log segments cover {expected_offset} bytes, but view_log declares {}",
            object.object_length
        ));
    }
}
