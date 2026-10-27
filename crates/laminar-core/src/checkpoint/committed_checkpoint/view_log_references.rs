//! View-log segments across the participants of one cut. A participant that adopts the log of
//! one that left lists the leaver's segments by reference, so a cut is checked as a whole: no
//! segment is listed twice, and no departed segment is lost.

use std::collections::btree_map::Entry;

use super::{BTreeMap, CheckpointManifest};
use crate::checkpoint::{
    view_log_segment_limit, ByteRange, CheckpointAssignmentFence, StateChunkId, ViewLogSegment,
};

/// One segment a survivor adopts from a participant that left, with the view-log object that
/// holds its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptedSegment {
    /// The segment as the departed participant listed it.
    pub segment: ViewLogSegment,
    /// Exact length of the object holding the segment.
    pub object_length: u64,
    /// SHA-256 of that whole object.
    pub object_sha256: String,
}

/// The segments of one view a survivor adopts, in the departed log's order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptedViewLog {
    /// Catalog name of the view.
    pub view: String,
    /// Adopted segments.
    pub segments: Vec<AdoptedSegment>,
}

/// The view logs `adopter` adopts when the participants of `cut` that `target` lacks leave.
///
/// Every participant derives the same plan from the same cut and target. Each departed log goes
/// whole to the survivor (a participant of both) holding the fewest segments so far, ties to
/// the lowest node id, in order of departed participant and then view.
///
/// # Errors
/// Returns an error when departed logs have no survivor to adopt them, when a survivor would
/// hold more segments than its share of the cluster budget, or when a departed segment's object
/// is untracked.
pub fn adopted_view_logs(
    cut: &[CheckpointManifest],
    target: &CheckpointAssignmentFence,
    adopter: u64,
) -> Result<Vec<AdoptedViewLog>, String> {
    plan_adoption(cut, target, Some(adopter))
}

/// Whether the survivors of `target` can adopt the logs of the participants of `cut` it lacks.
///
/// # Errors
/// Returns the error [`adopted_view_logs`] would return to any survivor.
pub fn check_view_log_adoption(
    cut: &[CheckpointManifest],
    target: &CheckpointAssignmentFence,
) -> Result<(), String> {
    plan_adoption(cut, target, None).map(drop)
}

fn plan_adoption(
    cut: &[CheckpointManifest],
    target: &CheckpointAssignmentFence,
    adopter: Option<u64>,
) -> Result<Vec<AdoptedViewLog>, String> {
    let limit = view_log_segment_limit(Some(target.participants.len()));
    let mut manifests: Vec<&CheckpointManifest> = cut.iter().collect();
    manifests.sort_unstable_by_key(|manifest| manifest.participant_id);
    let (survivors, departed): (Vec<_>, Vec<_>) = manifests
        .into_iter()
        .partition(|manifest| target.contains(manifest.participant_id));
    let mut load: BTreeMap<u64, usize> = survivors
        .iter()
        .map(|manifest| (manifest.participant_id, segment_count(manifest)))
        .collect();
    let mut logs = Vec::new();
    for manifest in departed {
        for log in manifest
            .view_logs
            .iter()
            .filter(|log| !log.segments.is_empty())
        {
            let (&chosen, held) = load
                .iter_mut()
                .min_by_key(|(id, held)| (**held, **id))
                .ok_or_else(|| {
                    format!(
                        "participant {} left with view logs, and no surviving participant can \
                         adopt them",
                        manifest.participant_id
                    )
                })?;
            *held = held.saturating_add(log.segments.len());
            if *held > limit {
                return Err(format!(
                    "participant {chosen} would hold {held} view-log segments after adopting \
                     view '{}' from participant {}; its share of the cluster budget is {limit}",
                    log.view, manifest.participant_id
                ));
            }
            if adopter != Some(chosen) {
                continue;
            }
            let segments = log
                .segments
                .iter()
                .map(|segment| adopted_segment(manifest, segment))
                .collect::<Result<_, _>>()?;
            logs.push(AdoptedViewLog {
                view: log.view.clone(),
                segments,
            });
        }
    }
    Ok(logs)
}

fn segment_count(manifest: &CheckpointManifest) -> usize {
    manifest
        .view_logs
        .iter()
        .map(|log| log.segments.len())
        .sum()
}

fn adopted_segment(
    manifest: &CheckpointManifest,
    segment: &ViewLogSegment,
) -> Result<AdoptedSegment, String> {
    let written = manifest
        .view_log
        .iter()
        .map(|object| (object.chunk, object.object_length, &object.sha256));
    let referenced = manifest
        .referenced_view_logs
        .iter()
        .map(|object| (object.chunk, object.object_length, &object.sha256));
    let (_, object_length, object_sha256) = written
        .chain(referenced)
        .find(|(chunk, _, _)| *chunk == segment.chunk)
        .ok_or_else(|| {
            format!(
                "participant {} lists view-log segment {} of untracked object {:?}",
                manifest.participant_id, segment.sequence, segment.chunk
            )
        })?;
    Ok(AdoptedSegment {
        segment: segment.clone(),
        object_length,
        object_sha256: object_sha256.clone(),
    })
}

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

    fn fence(participants: &[u64]) -> CheckpointAssignmentFence {
        let owners: Vec<u64> = participants.iter().copied().cycle().take(4).collect();
        CheckpointAssignmentFence::from_owner_map(
            9,
            &owners,
            participants
                .iter()
                .map(|&node_id| crate::checkpoint::CheckpointParticipant {
                    node_id,
                    boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
                })
                .collect(),
        )
        .unwrap()
    }

    fn with_view(mut manifest: CheckpointManifest, view: &str) -> CheckpointManifest {
        manifest.view_logs[0].view = view.into();
        manifest
    }

    #[test]
    fn each_departed_log_goes_whole_to_the_least_loaded_survivor() {
        let leaver = chunk(3, 4);
        let mut departed = manifest(3, vec![segment(leaver, 0), segment(leaver, 1)]);
        let mut second = with_view(manifest(3, vec![segment(leaver, 2)]), "w");
        departed.view_logs.append(&mut second.view_logs);
        // Survivor 1 holds two segments, survivor 2 one.
        let one = manifest(1, vec![segment(chunk(1, 5), 0), segment(chunk(1, 5), 1)]);
        let two = manifest(2, vec![segment(chunk(2, 5), 0)]);
        let cut = [one, two, departed];
        let target = fence(&[1, 2]);

        // `v` (two segments) goes to survivor 2, which then holds three, so `w` goes to 1.
        let to_two = adopted_view_logs(&cut, &target, 2).unwrap();
        assert_eq!(to_two.len(), 1);
        assert_eq!(to_two[0].view, "v");
        assert_eq!(to_two[0].segments.len(), 2);
        assert_eq!(to_two[0].segments[0].object_length, 64);
        let to_one = adopted_view_logs(&cut, &target, 1).unwrap();
        assert_eq!(to_one[0].view, "w");
        assert_eq!(to_one[0].segments[0].segment, segment(leaver, 2));

        // The same cut in another order makes the same plan.
        let reordered = [cut[2].clone(), cut[0].clone(), cut[1].clone()];
        assert_eq!(adopted_view_logs(&reordered, &target, 2).unwrap(), to_two);

        // A participant joining in the target adopts nothing.
        assert!(adopted_view_logs(&cut, &fence(&[1, 2, 4]), 4)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn departed_logs_need_a_survivor_with_room() {
        let leaver = chunk(3, 4);
        let cut = [manifest(3, vec![segment(leaver, 0)])];
        assert!(adopted_view_logs(&cut, &fence(&[4]), 4)
            .unwrap_err()
            .contains("no surviving participant"));

        // Two survivors share the cluster budget, and both already hold their whole share.
        let share = view_log_segment_limit(Some(2));
        let mut first = manifest(1, vec![segment(chunk(1, 5), 0)]);
        first.view_logs[0].segments = vec![segment(chunk(1, 5), 0); share];
        let mut second = manifest(2, vec![segment(chunk(2, 5), 0)]);
        second.view_logs[0].segments = vec![segment(chunk(2, 5), 0); share];
        let cut = [first, second, manifest(3, vec![segment(leaver, 0)])];
        let error = check_view_log_adoption(&cut, &fence(&[1, 2])).unwrap_err();
        assert!(error.contains("share of the cluster budget"), "{error}");
        assert_eq!(
            adopted_view_logs(&cut, &fence(&[1, 2]), 2).unwrap_err(),
            error
        );
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
