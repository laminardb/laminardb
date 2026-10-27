//! A view as a cluster subscription receives it from committed checkpoints: its rows at one cut,
//! then its change from each cut to the next. A changelog view's change is the rows of each
//! vnode whose frame changed, netted against the earlier frame. An append view's change is the
//! segments each participant wrote at the later cut: those in its own view-log object at or
//! after the sequence its log had reached at the earlier cut. That excludes segments adopted from
//! a departed participant, which stay in that participant's objects, and merged segments, which
//! take the sequence of their first input.

use arrow::array::RecordBatch;
use laminar_core::checkpoint::{StateChunkId, ViewLog, ViewLogSegment};

use super::artifacts::{Artifact, EMPTY_FRAME_SHA256};
use super::{CommittedReadError, CommittedViews, PinnedCut, ViewShape};
use crate::error::DbError;
use crate::mv_store::changes::{row_diff, subscription_schema, with_unit_weights};
use crate::mv_store::frames::decode_view_frame;
use crate::mv_store::{MvStorageMode, ViewKey};

impl PinnedCut {
    /// View `shape`'s rows at this cut, as its subscribers first receive them.
    pub(crate) async fn subscription_rows(
        &self,
        views: &CommittedViews,
        shape: &ViewShape,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let rows = self.read_view(views, shape, &[]).await?;
        if matches!(shape.mode, MvStorageMode::AppendLog { .. }) {
            return Ok(rows);
        }
        let schema = subscription_schema(&shape.schema, &shape.mode);
        rows.iter()
            .map(|rows| with_unit_weights(rows, std::sync::Arc::clone(&schema)))
            .collect()
    }

    /// The change to view `shape` from `earlier`, the committed cut directly before this one.
    pub(crate) async fn changes_since(
        &self,
        earlier: &Self,
        views: &CommittedViews,
        shape: &ViewShape,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let read = async {
            match &shape.mode {
                MvStorageMode::Changelog { key, key_groups } => {
                    let vnode_count = u32::from(key_groups.get());
                    self.frame_changes(earlier, views, shape, key, vnode_count)
                        .await
                }
                MvStorageMode::AppendLog { .. } => self.appended_since(earlier, views, shape).await,
            }
        };
        tokio::time::timeout_at(tokio::time::Instant::from_std(self.deadline), read)
            .await
            .map_err(|_| CommittedReadError::DeadlineExceeded { epoch: self.epoch })?
    }

    async fn frame_changes(
        &self,
        earlier: &Self,
        views: &CommittedViews,
        shape: &ViewShape,
        key: &ViewKey,
        vnode_count: u32,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut older = earlier.view_frames(shape, key, vnode_count)?;
        let mut before = Vec::new();
        let mut after = Vec::new();
        for (vnode, frame) in self.view_frames(shape, key, vnode_count)? {
            let old = older.remove(&vnode);
            if old.as_ref().map(|old| old.sha256) != Some(frame.sha256) {
                before.push(old);
                after.push(Some(frame));
            }
        }
        let before = earlier
            .load_frames(views, shape, vnode_count, before)
            .await?;
        let after = self.load_frames(views, shape, vnode_count, after).await?;
        let schema = subscription_schema(&shape.schema, &shape.mode);
        let mut changes = Vec::new();
        for (older, newer) in before.iter().zip(&after) {
            changes.extend(row_diff(&schema, older, newer)?);
        }
        Ok(changes)
    }

    /// The rows of each of `frames`, in order: none for an absent or empty frame.
    async fn load_frames(
        &self,
        views: &CommittedViews,
        shape: &ViewShape,
        vnode_count: u32,
        frames: Vec<Option<Artifact<'_>>>,
    ) -> Result<Vec<Vec<RecordBatch>>, DbError> {
        let mut rows = vec![Vec::new(); frames.len()];
        let (positions, wanted): (Vec<_>, Vec<_>) = frames
            .into_iter()
            .enumerate()
            .filter_map(|(position, frame)| Some((position, frame?)))
            .filter(|(_, frame)| frame.sha256 != EMPTY_FRAME_SHA256.as_str())
            .unzip();
        let loaded = self
            .load(views, &shape.schema, &wanted, |frame, bytes| {
                let vnode = u32::try_from(frame.position).unwrap_or(u32::MAX);
                decode_view_frame(shape, vnode, vnode_count, bytes).map(|batch| vec![batch])
            })
            .await?;
        for (position, loaded) in positions.into_iter().zip(loaded) {
            rows[position] = loaded.to_vec();
        }
        Ok(rows)
    }

    async fn appended_since(
        &self,
        earlier: &Self,
        views: &CommittedViews,
        shape: &ViewShape,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut wanted = Vec::new();
        let mut found = false;
        for manifest in self.manifests.iter() {
            let Some(log) = super::artifacts::view_log(manifest, &shape.name) else {
                continue;
            };
            found = true;
            let reached = earlier
                .manifests
                .iter()
                .filter(|previous| previous.participant_id == manifest.participant_id)
                .find_map(|previous| super::artifacts::view_log(previous, &shape.name))
                .map_or(0, |previous| previous.next_sequence);
            let own = manifest.view_log.as_ref().map(|object| object.chunk);
            for segment in written(log, own, reached) {
                wanted.push(super::artifacts::segment_artifact(manifest, segment)?);
            }
        }
        if !found {
            return Err(CommittedReadError::ViewNotCommitted {
                view: shape.name.clone(),
                epoch: self.epoch,
            }
            .into());
        }
        let rows = self
            .load(views, &shape.schema, &wanted, |segment, bytes| {
                super::artifacts::decode_segment(
                    &shape.name,
                    &shape.schema,
                    segment.position,
                    bytes,
                )
            })
            .await?;
        Ok(rows.iter().flat_map(|rows| rows.iter().cloned()).collect())
    }
}

/// The segments of `log` its participant wrote at this cut: in the view-log object `own` it wrote
/// there, at or after the sequence `reached` its log had reached at the cut before.
fn written(
    log: &ViewLog,
    own: Option<StateChunkId>,
    reached: u64,
) -> impl Iterator<Item = &ViewLogSegment> {
    log.segments
        .iter()
        .filter(move |segment| Some(segment.chunk) == own && segment.sequence >= reached)
}

#[cfg(test)]
mod tests {
    use laminar_core::checkpoint::ByteRange;

    use super::*;

    fn segment(participant_id: u64, checkpoint_id: u64, sequence: u64) -> ViewLogSegment {
        ViewLogSegment {
            partition: 0,
            sequence,
            chunk: StateChunkId {
                participant_id,
                checkpoint_id,
            },
            range: ByteRange {
                offset: 0,
                length: 1,
            },
            sha256: String::new(),
            rows: 1,
            event_time: None,
        }
    }

    /// At checkpoint 9 participant 7 merged its segments 0 and 1 into one at sequence 0, adopted
    /// a departed participant's segment at sequence 3, and wrote its new rows at sequence 4. Only
    /// the new rows were written there.
    #[test]
    fn merged_and_adopted_segments_were_not_written_at_the_cut() {
        let log = ViewLog {
            view: "v".into(),
            next_sequence: 5,
            frontier: None,
            idle: false,
            segments: vec![
                segment(7, 9, 0),
                segment(7, 4, 2),
                segment(8, 6, 3),
                segment(7, 9, 4),
            ],
        };
        let own = Some(segment(7, 9, 0).chunk);
        let sequences: Vec<_> = written(&log, own, 3)
            .map(|segment| segment.sequence)
            .collect();
        assert_eq!(sequences, [4]);
        // A participant that wrote no view-log object at the cut wrote no segment there.
        assert_eq!(written(&log, None, 0).count(), 0);
    }
}
