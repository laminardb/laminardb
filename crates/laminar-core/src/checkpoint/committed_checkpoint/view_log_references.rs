//! View-log segments across the participants of one cut. A participant that adopts the log of
//! one that left lists the leaver's segments by reference, so a cut is checked as a whole: no
//! segment is listed twice, and no departed segment is lost.

use std::collections::btree_map::Entry;

use super::{BTreeMap, CheckpointManifest};
use crate::checkpoint::{ByteRange, StateChunkId};

/// Every view-log segment of a cut is listed once, and every participant that writes or
/// references a view-log object agrees on its length and digest.
///
/// # Errors
/// Returns an error naming the first segment listed twice or the first object participants
/// disagree on.
pub fn validate_cut_view_logs<'a>(
    manifests: impl IntoIterator<Item = &'a CheckpointManifest>,
) -> Result<(), String> {
    let mut objects = BTreeMap::<StateChunkId, (u64, &str)>::new();
    let mut segments = BTreeMap::<(StateChunkId, ByteRange), &str>::new();
    for manifest in manifests {
        let written = manifest
            .view_log
            .iter()
            .map(|object| (object.chunk, object.object_length, object.sha256.as_str()));
        let referenced = manifest
            .referenced_view_logs
            .iter()
            .map(|object| (object.chunk, object.object_length, object.sha256.as_str()));
        for (chunk, length, sha256) in written.chain(referenced) {
            match objects.entry(chunk) {
                Entry::Vacant(entry) => {
                    entry.insert((length, sha256));
                }
                Entry::Occupied(entry) if *entry.get() != (length, sha256) => {
                    return Err(format!(
                        "participants disagree on the length or digest of view-log object {chunk:?}"
                    ));
                }
                Entry::Occupied(_) => {}
            }
        }
        for log in &manifest.view_logs {
            for segment in &log.segments {
                if segments
                    .insert((segment.chunk, segment.range), &log.view)
                    .is_some()
                {
                    return Err(format!(
                        "view-log object {:?} bytes {}+{} are listed twice in one cut",
                        segment.chunk, segment.range.offset, segment.range.length
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Every segment the departed participants' view logs held at the predecessor cut is listed
/// exactly once, by the same view, in this cut.
///
/// # Errors
/// Returns an error naming the first departed segment this cut lost or lists twice.
pub fn validate_departed_view_logs<'a>(
    departed: impl IntoIterator<Item = &'a CheckpointManifest>,
    cut: impl IntoIterator<Item = &'a CheckpointManifest>,
) -> Result<(), String> {
    let mut listed = BTreeMap::<(&str, StateChunkId, ByteRange, &str), u32>::new();
    for log in cut.into_iter().flat_map(|manifest| &manifest.view_logs) {
        for segment in &log.segments {
            let key = (
                log.view.as_str(),
                segment.chunk,
                segment.range,
                segment.sha256.as_str(),
            );
            *listed.entry(key).or_default() += 1;
        }
    }
    for manifest in departed {
        for log in &manifest.view_logs {
            for segment in &log.segments {
                let key = (
                    log.view.as_str(),
                    segment.chunk,
                    segment.range,
                    segment.sha256.as_str(),
                );
                let count = listed.get(&key).copied().unwrap_or(0);
                if count != 1 {
                    return Err(format!(
                        "participant {} left, and view '{}' lists its segment {} of object \
                         {:?} {count} times instead of once",
                        manifest.participant_id, log.view, segment.sequence, segment.chunk
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::checkpoint::{
        checkpoint_sha256, NodeDataObject, ReferencedStateChunk, ViewLog, ViewLogSegment,
    };

    const OBJECT: &[u8] = b"rows of participant one";

    fn chunk(participant_id: u64, checkpoint_id: u64) -> StateChunkId {
        StateChunkId {
            participant_id,
            checkpoint_id,
        }
    }

    fn segment(chunk: StateChunkId, sequence: u64) -> ViewLogSegment {
        ViewLogSegment {
            partition: 0,
            sequence,
            chunk,
            range: ByteRange {
                offset: sequence * 4,
                length: 4,
            },
            sha256: checkpoint_sha256(&[u8::try_from(sequence).unwrap()]),
            rows: 1,
            event_time: None,
        }
    }

    /// Participant `participant` at checkpoint 5, listing `segments` of view `v`; segments of
    /// another checkpoint's object are references.
    fn manifest(participant: u64, segments: Vec<ViewLogSegment>) -> CheckpointManifest {
        let mut manifest = CheckpointManifest::new(5, 5);
        manifest.participant_id = participant;
        let own = chunk(participant, 5);
        let mut referenced = BTreeMap::<StateChunkId, u32>::new();
        for segment in segments.iter().filter(|segment| segment.chunk != own) {
            *referenced.entry(segment.chunk).or_default() += 1;
        }
        manifest.view_log = segments
            .iter()
            .any(|segment| segment.chunk == own)
            .then(|| NodeDataObject {
                chunk: own,
                object_length: 64,
                sha256: checkpoint_sha256(OBJECT),
            });
        manifest.referenced_view_logs = referenced
            .into_iter()
            .map(|(chunk, count)| ReferencedStateChunk {
                chunk,
                object_length: 64,
                sha256: checkpoint_sha256(OBJECT),
                ref_count: NonZeroU32::new(count).unwrap(),
            })
            .collect();
        manifest.view_logs = vec![ViewLog {
            view: "v".into(),
            next_sequence: 8,
            frontier: None,
            idle: false,
            segments,
        }];
        manifest
    }

    #[test]
    fn a_segment_is_listed_once_and_objects_agree_across_participants() {
        let old = chunk(1, 3);
        let first = manifest(1, vec![segment(old, 0)]);
        let second = manifest(2, vec![segment(old, 1)]);
        validate_cut_view_logs([&first, &second]).unwrap();

        let twice = manifest(2, vec![segment(old, 0)]);
        assert!(validate_cut_view_logs([&first, &twice])
            .unwrap_err()
            .contains("listed twice"));

        let mut disagreeing = second;
        disagreeing.referenced_view_logs[0].object_length = 65;
        assert!(validate_cut_view_logs([&first, &disagreeing])
            .unwrap_err()
            .contains("disagree"));
    }

    #[test]
    fn a_departed_participants_segments_are_listed_exactly_once() {
        let leaver_object = chunk(3, 4);
        let leaver = manifest(
            3,
            vec![segment(leaver_object, 0), segment(leaver_object, 1)],
        );
        let adopter = manifest(
            1,
            vec![
                segment(chunk(1, 5), 2),
                segment(leaver_object, 0),
                segment(leaver_object, 1),
            ],
        );
        let other = manifest(2, vec![segment(chunk(2, 5), 3)]);
        validate_departed_view_logs([&leaver], [&adopter, &other]).unwrap();

        let lost = manifest(1, vec![segment(leaver_object, 0)]);
        assert!(validate_departed_view_logs([&leaver], [&lost, &other])
            .unwrap_err()
            .contains("0 times"));

        let doubled = manifest(2, vec![segment(leaver_object, 1)]);
        assert!(validate_departed_view_logs([&leaver], [&adopter, &doubled])
            .unwrap_err()
            .contains("2 times"));

        let mut renamed = adopter;
        renamed.view_logs[0].view = "other".into();
        assert!(validate_departed_view_logs([&leaver], [&renamed]).is_err());
    }
}
