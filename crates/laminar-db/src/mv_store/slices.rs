//! Per-vnode slices of a changelog view, placed by the partition-key ABI and tracked for dirty
//! checkpoint capture.

use std::num::NonZeroU32;

use laminar_core::state::{KeyGroupCount, PartitionKeyCodecV1};

use crate::error::DbError;

/// A view's rows split by vnode. Writers go through [`Self::slot_mut`], which records the vnode
/// as changed since the last capture.
pub(super) struct VnodeSlices<T> {
    slots: Box<[T]>,
    dirty: Box<[bool]>,
    dirty_roster: Vec<u32>,
    full_capture_required: bool,
}

impl<T: Default> VnodeSlices<T> {
    pub(super) fn new(key_groups: KeyGroupCount) -> Self {
        let count = usize::from(key_groups.get());
        Self {
            slots: std::iter::repeat_with(T::default).take(count).collect(),
            dirty: vec![false; count].into_boxed_slice(),
            dirty_roster: Vec::new(),
            // No committed predecessor holds this view's frames yet.
            full_capture_required: true,
        }
    }
}

/// The vnode owning a row whose placement columns encode to `encoded`; the same mapping the
/// aggregate applies to its group key.
pub(super) fn vnode_for_encoded(encoded: &[u8], vnode_count: NonZeroU32) -> u32 {
    if vnode_count.get() == 1 {
        0
    } else {
        PartitionKeyCodecV1::vnode_for_encoded(encoded, vnode_count)
    }
}

impl<T> VnodeSlices<T> {
    pub(super) fn vnode_count(&self) -> NonZeroU32 {
        let count = u32::try_from(self.slots.len()).expect("key-group count fits u32");
        NonZeroU32::new(count).expect("key-group count is nonzero")
    }

    pub(super) fn vnode_for(&self, encoded: &[u8]) -> u32 {
        vnode_for_encoded(encoded, self.vnode_count())
    }

    pub(super) fn slot(&self, vnode: u32) -> &T {
        &self.slots[vnode as usize]
    }

    pub(super) fn slot_mut(&mut self, vnode: u32) -> &mut T {
        let dirty = &mut self.dirty[vnode as usize];
        if !*dirty {
            *dirty = true;
            self.dirty_roster.push(vnode);
        }
        &mut self.slots[vnode as usize]
    }

    pub(super) fn slots(&self) -> impl Iterator<Item = (u32, &T)> {
        (0u32..).zip(self.slots.iter())
    }

    pub(super) fn require_full_capture(&mut self) {
        self.full_capture_required = true;
    }

    /// The canonical vnodes the next frame capture must encode, then the dirty set is consumed.
    /// A capture that fails afterwards must call [`Self::require_full_capture`].
    pub(super) fn take_capture_roster(
        &mut self,
        required: &[u32],
        vnode_count: u32,
        is_empty: impl Fn(&T) -> bool,
    ) -> Result<Vec<u32>, DbError> {
        if vnode_count != self.vnode_count().get() {
            return Err(DbError::Checkpoint(format!(
                "view key-group count mismatch: state={}, requested={vnode_count}",
                self.vnode_count()
            )));
        }
        if required.windows(2).any(|pair| pair[0] >= pair[1])
            || required.last().is_some_and(|vnode| *vnode >= vnode_count)
        {
            return Err(DbError::Checkpoint(format!(
                "view received a non-canonical vnode roster {required:?}"
            )));
        }
        let roster = if self.full_capture_required {
            if let Some((unowned, _)) = self
                .slots()
                .find(|(vnode, slot)| !is_empty(slot) && required.binary_search(vnode).is_err())
            {
                return Err(DbError::Checkpoint(format!(
                    "view retained rows for unowned vnode {unowned}"
                )));
            }
            required.to_vec()
        } else {
            let mut dirty = std::mem::take(&mut self.dirty_roster);
            dirty.sort_unstable();
            if let Some(&unowned) = dirty
                .iter()
                .find(|vnode| required.binary_search(vnode).is_err())
            {
                self.dirty_roster = dirty;
                return Err(DbError::Checkpoint(format!(
                    "view changed rows for unowned vnode {unowned}"
                )));
            }
            dirty
        };
        for vnode in self.dirty_roster.drain(..) {
            self.dirty[vnode as usize] = false;
        }
        for &vnode in &roster {
            self.dirty[vnode as usize] = false;
        }
        self.full_capture_required = false;
        Ok(roster)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slices(count: u16) -> VnodeSlices<Vec<u8>> {
        VnodeSlices::new(KeyGroupCount::try_from(count).unwrap())
    }

    #[test]
    fn the_first_capture_is_full_and_later_captures_take_only_changed_vnodes() {
        let mut slices = slices(4);
        slices.slot_mut(2).push(1);
        assert_eq!(
            slices
                .take_capture_roster(&[0, 1, 2, 3], 4, Vec::is_empty)
                .unwrap(),
            vec![0, 1, 2, 3]
        );
        assert!(slices
            .take_capture_roster(&[0, 1, 2, 3], 4, Vec::is_empty)
            .unwrap()
            .is_empty());
        slices.slot_mut(3).push(1);
        slices.slot_mut(1).push(1);
        slices.slot_mut(3).push(2);
        assert_eq!(
            slices
                .take_capture_roster(&[0, 1, 2, 3], 4, Vec::is_empty)
                .unwrap(),
            vec![1, 3]
        );
        slices.require_full_capture();
        assert_eq!(
            slices
                .take_capture_roster(&[0, 1, 2, 3], 4, Vec::is_empty)
                .unwrap(),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn a_capture_rejects_rows_outside_the_required_roster() {
        let mut slices = slices(4);
        slices.slot_mut(3).push(1);
        assert!(slices
            .take_capture_roster(&[0, 1], 4, Vec::is_empty)
            .is_err());
        assert!(slices
            .take_capture_roster(&[0, 1, 2, 3], 8, Vec::is_empty)
            .is_err());
        assert!(slices
            .take_capture_roster(&[1, 0], 4, Vec::is_empty)
            .is_err());
    }
}
