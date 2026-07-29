use ahash::{AHashMap, AHashSet};
use datafusion_common::ScalarValue;
use laminar_core::state::KeyGroupCount;

use super::GroupEntry;
use crate::error::DbError;

/// Complete mutable working set owned by one aggregate vnode.
///
/// The box containing this value is the unit of rebalance publication. Keeping logical state,
/// changelog state, and checkpoint bookkeeping under the same owner prevents a vnode transition
/// from publishing only part of a group's state.
#[derive(Default)]
pub(super) struct AggregateVnodeState {
    pub(super) groups: AHashMap<arrow::row::OwnedRow, GroupEntry>,
    pub(super) last_emitted: AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
    pub(super) emit_dirty_keys: AHashSet<arrow::row::OwnedRow>,
    pub(super) checkpoint_dirty_keys: AHashSet<arrow::row::OwnedRow>,
    pub(super) last_emitted_dirty_keys: AHashSet<arrow::row::OwnedRow>,
    #[cfg(feature = "cluster")]
    pub(super) delta_chain_len: Option<u32>,
    #[cfg(feature = "cluster")]
    pub(super) force_full_rebase: bool,
}

/// Fixed vnode address space selected from an aggregate's immutable routing identity.
pub(super) struct AggregateVnodeSlots {
    slots: Box<[Option<Box<AggregateVnodeState>>]>,
    active_vnodes: Vec<u32>,
    resident_group_count: usize,
}

/// Mutable iterator over the sorted active roster without scanning the fixed slot address space.
pub(super) struct ActiveVnodeIterMut<'a> {
    remaining_slots: &'a mut [Option<Box<AggregateVnodeState>>],
    active_vnodes: std::slice::Iter<'a, u32>,
    next_slot_index: usize,
}

impl<'a> Iterator for ActiveVnodeIterMut<'a> {
    type Item = (u32, &'a mut AggregateVnodeState);

    fn next(&mut self) -> Option<Self::Item> {
        let vnode = *self.active_vnodes.next()?;
        let slot_index = vnode as usize;
        let relative_index = slot_index
            .checked_sub(self.next_slot_index)
            .expect("aggregate active vnode roster must be sorted and unique");
        let remaining_slots = std::mem::take(&mut self.remaining_slots);
        let (_, from_active) = remaining_slots.split_at_mut(relative_index);
        let (active, remaining_slots) = from_active
            .split_first_mut()
            .expect("aggregate active vnode must be inside the fixed slot table");
        self.remaining_slots = remaining_slots;
        self.next_slot_index = slot_index + 1;
        Some((
            vnode,
            active
                .as_deref_mut()
                .expect("aggregate active vnode roster must reference a resident slot"),
        ))
    }
}

impl AggregateVnodeSlots {
    pub(super) fn try_new(key_group_count: KeyGroupCount) -> Result<Self, DbError> {
        let slot_count = usize::from(key_group_count.get());
        let mut slots = Vec::new();
        slots.try_reserve_exact(slot_count).map_err(|error| {
            DbError::Pipeline(format!(
                "aggregate vnode table could not reserve {slot_count} slots: {error}"
            ))
        })?;
        slots.resize_with(slot_count, || None);
        let mut active_vnodes = Vec::new();
        // This bounded topology reserve keeps later active-roster insertion from reallocating;
        // emit/checkpoint hot paths iterate only the sorted active roster.
        active_vnodes
            .try_reserve_exact(slot_count)
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "aggregate active vnode roster could not reserve {slot_count} entries: {error}"
                ))
            })?;
        Ok(Self {
            slots: slots.into_boxed_slice(),
            active_vnodes,
            resident_group_count: 0,
        })
    }

    #[inline]
    pub(super) fn get(&self, vnode: u32) -> Option<&AggregateVnodeState> {
        self.slots.get(vnode as usize).and_then(Option::as_deref)
    }

    #[inline]
    pub(super) fn get_mut(&mut self, vnode: u32) -> Option<&mut AggregateVnodeState> {
        self.slots
            .get_mut(vnode as usize)
            .and_then(Option::as_deref_mut)
    }

    #[inline]
    pub(super) fn get_or_insert(&mut self, vnode: u32) -> &mut AggregateVnodeState {
        let slot_index = vnode as usize;
        if self.slots[slot_index].is_none() {
            let insertion = self
                .active_vnodes
                .binary_search(&vnode)
                .expect_err("an absent aggregate slot must not be in the active roster");
            let state = Box::new(AggregateVnodeState::default());
            self.active_vnodes.insert(insertion, vnode);
            self.slots[slot_index] = Some(state);
        }
        self.slots[slot_index].as_deref_mut().unwrap()
    }

    #[inline]
    pub(super) const fn resident_group_count(&self) -> usize {
        self.resident_group_count
    }

    #[inline]
    pub(super) fn increment_resident_groups(&mut self) {
        self.resident_group_count = self
            .resident_group_count
            .checked_add(1)
            .expect("aggregate resident group count was preflighted");
    }

    #[inline]
    #[cfg(feature = "cluster")]
    pub(super) fn set_resident_group_count(&mut self, count: usize) {
        self.resident_group_count = count;
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (u32, &AggregateVnodeState)> + Clone {
        self.active_vnodes.iter().map(|&vnode| {
            (
                vnode,
                self.slots[vnode as usize]
                    .as_deref()
                    .expect("aggregate active vnode roster must reference a resident slot"),
            )
        })
    }

    pub(super) fn iter_mut(&mut self) -> ActiveVnodeIterMut<'_> {
        ActiveVnodeIterMut {
            remaining_slots: &mut self.slots,
            active_vnodes: self.active_vnodes.iter(),
            next_slot_index: 0,
        }
    }

    #[cfg(feature = "cluster")]
    pub(super) fn active_vnodes(&self) -> &[u32] {
        &self.active_vnodes
    }

    #[cfg(feature = "cluster")]
    pub(super) fn swap_active_vnodes(&mut self, replacement: &mut Vec<u32>) {
        std::mem::swap(&mut self.active_vnodes, replacement);
    }

    #[inline]
    #[cfg(feature = "cluster")]
    pub(super) fn replace_for_publication(
        &mut self,
        vnode: u32,
        replacement: Option<Box<AggregateVnodeState>>,
    ) -> Option<Box<AggregateVnodeState>> {
        std::mem::replace(&mut self.slots[vnode as usize], replacement)
    }

    #[cfg(all(feature = "cluster", test))]
    pub(super) fn remove(&mut self, vnode: u32) -> Option<Box<AggregateVnodeState>> {
        let removed = self.slots[vnode as usize].take();
        if removed.is_some() {
            let roster_index = self
                .active_vnodes
                .binary_search(&vnode)
                .expect("a resident aggregate slot must be in the active roster");
            self.active_vnodes.remove(roster_index);
        }
        removed
    }

    #[cfg(test)]
    pub(super) fn active_vnodes_for_test(&self) -> &[u32] {
        &self.active_vnodes
    }
}
