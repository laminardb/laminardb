//! Vnode transitions of changelog views in a cluster: live reassignment and failed-owner
//! adoption move a view's slices with the vnodes that own them. Preparation decodes the
//! acquired vnodes' committed frames into a private image; publication swaps them in and
//! empties the revoked vnodes under the store write lock, at the graph's authority cut.

use rustc_hash::FxHashSet;

use super::{MvEntry, MvStore};
use crate::error::DbError;

/// One changelog view's prepared vnode transition.
pub(crate) struct ViewTransition {
    view: String,
    revoked: Vec<u32>,
    acquired: Vec<u32>,
    staged: MvEntry,
}

impl MvStore {
    /// Decode the frames of the vnodes `view` acquires, and check that the view stays within
    /// its limits once it also releases `revoked`. The live store is not touched.
    pub(crate) fn prepare_view_transition<'a>(
        &self,
        view: &str,
        vnode_count: u32,
        revoked: &FxHashSet<u32>,
        acquired: impl IntoIterator<Item = (u32, &'a [u8])>,
    ) -> Result<ViewTransition, DbError> {
        let mut restored = self.restore_target(view)?;
        let mut acquired_vnodes = Vec::new();
        for (vnode, state) in acquired {
            if revoked.contains(&vnode) || acquired_vnodes.contains(&vnode) {
                return Err(DbError::Checkpoint(format!(
                    "view '{view}' transition names vnode {vnode} twice"
                )));
            }
            restored.restore_vnode(vnode, vnode_count, state)?;
            acquired_vnodes.push(vnode);
        }
        let mut revoked: Vec<u32> = revoked.iter().copied().collect();
        revoked.sort_unstable();
        let live = self
            .entries
            .get(view)
            .ok_or_else(|| DbError::Checkpoint(format!("view '{view}' is not registered")))?;
        let released: Vec<u32> = revoked.iter().chain(&acquired_vnodes).copied().collect();
        let (rows, bytes) = live.exchanged_charge(&released, restored.entry());
        self.limits.validate(view, rows, bytes)?;
        Ok(ViewTransition {
            view: view.to_owned(),
            revoked,
            acquired: acquired_vnodes,
            staged: restored.into_entry(),
        })
    }

    pub(crate) fn publish_view_transition(&mut self, mut transition: ViewTransition) {
        let Some(entry) = self.entries.get_mut(&transition.view) else {
            return;
        };
        if let (Some(live), Some(staged)) =
            (entry.upsert.as_mut(), transition.staged.upsert.as_mut())
        {
            live.exchange_slices(&transition.revoked, staged, &transition.acquired);
            entry.approx_bytes = live.approx_bytes;
        }
        if let (Some(live), Some(staged)) =
            (entry.multiset.as_mut(), transition.staged.multiset.as_mut())
        {
            live.exchange_slices(&transition.revoked, staged, &transition.acquired);
            entry.approx_bytes = live.approx_bytes;
        }
    }
}

impl MvEntry {
    /// The rows and bytes this view would hold after its `released` slices are replaced by
    /// `staged`'s.
    fn exchanged_charge(&self, released: &[u32], staged: &Self) -> (usize, usize) {
        let (rows, bytes, (released_rows, released_bytes)) = match (&self.upsert, &self.multiset) {
            (Some(upsert), _) => (
                upsert.rows,
                upsert.approx_bytes,
                upsert.slices_charge(released),
            ),
            (_, Some(multiset)) => (
                multiset.row_count(),
                multiset.approx_bytes,
                multiset.slices_charge(released),
            ),
            (None, None) => (0, 0, (0, 0)),
        };
        let (added_rows, added_bytes) = match (&staged.upsert, &staged.multiset) {
            (Some(upsert), _) => (upsert.rows, upsert.approx_bytes),
            (_, Some(multiset)) => (multiset.row_count(), multiset.approx_bytes),
            (None, None) => (0, 0),
        };
        (
            rows.saturating_sub(released_rows)
                .saturating_add(added_rows),
            bytes
                .saturating_sub(released_bytes)
                .saturating_add(added_bytes),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::KeyGroupCount;

    use super::*;
    use crate::mv_store::{MvStorageMode, ViewKey};

    const VNODES: u32 = 4;

    /// A keyed view holding keys `1..=keys` with `n = scale × k`.
    fn counts(keys: i64, scale: i64) -> MvStore {
        let mut store = MvStore::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("n", DataType::Int64, true),
        ]));
        store
            .create_mv(
                "counts",
                schema,
                MvStorageMode::Changelog {
                    key: ViewKey::Keyed(vec![0]),
                    key_groups: KeyGroupCount::try_from(4_u16).unwrap(),
                },
            )
            .unwrap();
        let keys: Vec<i64> = (1..=keys).collect();
        let column = |values: Vec<i64>| Arc::new(Int64Array::from(values)) as ArrayRef;
        let changes = RecordBatch::try_from_iter(vec![
            ("k", column(keys.clone())),
            ("n", column(keys.iter().map(|k| k * scale).collect())),
            (
                laminar_core::changelog::WEIGHT_COLUMN,
                column(vec![1; keys.len()]),
            ),
        ])
        .unwrap();
        store.update("counts", &changes);
        store
    }

    fn rows(store: &MvStore) -> Vec<(i64, i64)> {
        let batch = store.to_record_batch("counts").unwrap().unwrap();
        let column = |index: usize| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone()
        };
        let (keys, counts) = (column(0), column(1));
        let mut rows: Vec<_> = (0..keys.len())
            .map(|row| (keys.value(row), counts.value(row)))
            .collect();
        rows.sort_unstable();
        rows
    }

    fn frame(frames: &[(u32, bytes::Bytes)], vnode: u32) -> (u32, &[u8]) {
        let (_, frame) = frames.iter().find(|(held, _)| *held == vnode).unwrap();
        (vnode, frame.as_ref())
    }

    #[test]
    fn a_transition_empties_revoked_vnodes_and_adopts_the_donor_frames() {
        let mut live = counts(40, 10);
        let live_frames = live.full_view_frames("counts");
        let donor_frames = counts(40, 100).full_view_frames("counts");
        let revoked = FxHashSet::from_iter([0]);
        let transition = live
            .prepare_view_transition("counts", VNODES, &revoked, [frame(&donor_frames, 1)])
            .unwrap();
        live.publish_view_transition(transition);

        let expected_frames: Vec<_> = live_frames
            .iter()
            .filter(|(vnode, _)| *vnode >= 2)
            .chain(donor_frames.iter().filter(|(vnode, _)| *vnode == 1))
            .cloned()
            .collect();
        let expected = live
            .image_from_view_frames("counts", &expected_frames)
            .unwrap();
        assert_eq!(rows(&live), rows(&expected));
        assert!(rows(&live).len() < 40);
        assert_eq!(live.total_bytes(), expected.total_bytes());

        // The next capture writes every owned vnode; vnode 0 holds nothing.
        let captured = live
            .capture_view_vnodes("counts", &[1, 2, 3], VNODES, u64::MAX)
            .unwrap();
        assert_eq!(
            captured.iter().map(|state| state.vnode).collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    #[test]
    fn a_transition_fails_before_publication_past_the_limits_or_naming_a_vnode_twice() {
        let mut live = counts(40, 10);
        live.limits.rows = 40;
        let donor_frames = counts(400, 100).full_view_frames("counts");
        let none = FxHashSet::default();
        assert!(live
            .prepare_view_transition("counts", VNODES, &none, [frame(&donor_frames, 1)])
            .is_err());
        let twice = [frame(&donor_frames, 2), frame(&donor_frames, 2)];
        assert!(live
            .prepare_view_transition("counts", VNODES, &none, twice)
            .is_err());
        assert_eq!(rows(&live), rows(&counts(40, 10)));
    }
}
