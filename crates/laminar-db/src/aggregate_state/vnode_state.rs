use datafusion_common::ScalarValue;
use laminar_core::state::KeyGroupCount;
use rustc_hash::{FxHashMap, FxHashSet};

use super::accounting::{
    logical_collection_element_usage, logical_collection_spare_capacity_usage,
    owned_row_payload_usage, retained_changelog_vector_element_usage, topology_element_usage,
    vnode_inline_usage, AggregateStateUsage,
};
use super::GroupEntry;
use crate::error::DbError;

/// Complete mutable working set owned by one aggregate vnode.
///
/// The box containing this value is the unit of rebalance publication. Keeping logical state,
/// changelog state, and checkpoint bookkeeping under the same owner prevents a vnode transition
/// from publishing only part of a group's state.
pub(super) struct AggregateVnodeState {
    pub(super) groups: FxHashMap<arrow::row::OwnedRow, GroupEntry>,
    pub(super) last_emitted: FxHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
    pub(super) emit_dirty_keys: FxHashSet<arrow::row::OwnedRow>,
    pub(super) checkpoint_dirty_keys: FxHashSet<arrow::row::OwnedRow>,
    pub(super) last_emitted_dirty_keys: FxHashSet<arrow::row::OwnedRow>,
    usage: AggregateStateUsage,
}

/// Fixed vnode address space selected from an aggregate's immutable routing identity.
pub(super) struct AggregateVnodeSlots {
    slots: Box<[Option<Box<AggregateVnodeState>>]>,
    active_vnodes: Vec<u32>,
    resident_group_count: usize,
    fixed_topology_usage: AggregateStateUsage,
}

impl Default for AggregateVnodeState {
    fn default() -> Self {
        Self {
            groups: FxHashMap::default(),
            last_emitted: FxHashMap::default(),
            emit_dirty_keys: FxHashSet::default(),
            checkpoint_dirty_keys: FxHashSet::default(),
            last_emitted_dirty_keys: FxHashSet::default(),
            usage: vnode_inline_usage::<AggregateVnodeState>(1),
        }
    }
}

impl AggregateVnodeState {
    fn hash_set_spare_usage(values: &FxHashSet<arrow::row::OwnedRow>) -> AggregateStateUsage {
        logical_collection_spare_capacity_usage::<arrow::row::OwnedRow>(
            values.capacity(),
            values.len(),
        )
    }

    pub(super) fn collection_spare_usage(&self) -> AggregateStateUsage {
        logical_collection_spare_capacity_usage::<(arrow::row::OwnedRow, GroupEntry)>(
            self.groups.capacity(),
            self.groups.len(),
        )
        .saturating_add(logical_collection_spare_capacity_usage::<(
            arrow::row::OwnedRow,
            Vec<ScalarValue>,
        )>(
            self.last_emitted.capacity(), self.last_emitted.len()
        ))
        .saturating_add(Self::hash_set_spare_usage(&self.emit_dirty_keys))
        .saturating_add(Self::hash_set_spare_usage(&self.checkpoint_dirty_keys))
        .saturating_add(Self::hash_set_spare_usage(&self.last_emitted_dirty_keys))
    }

    pub(super) fn reconcile_collection_spare_usage(&mut self, previous: AggregateStateUsage) {
        self.usage = self
            .usage
            .saturating_sub(previous)
            .saturating_add(self.collection_spare_usage());
    }

    pub(super) fn try_from_recovered(
        groups: FxHashMap<arrow::row::OwnedRow, GroupEntry>,
        last_emitted: FxHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
        mark_emit_dirty: bool,
    ) -> Result<Self, (&'static str, std::collections::TryReserveError)> {
        let mut state = Self {
            groups,
            last_emitted,
            ..Self::default()
        };
        if mark_emit_dirty {
            state
                .emit_dirty_keys
                .try_reserve(state.groups.len())
                .map_err(|error| ("changelog dirty keys", error))?;
            state.emit_dirty_keys.extend(state.groups.keys().cloned());
        }
        // Reconcile once, after all fallible reservations and retained collections are complete.
        state.refresh_usage();
        Ok(state)
    }

    pub(super) fn group_usage(
        key: &arrow::row::OwnedRow,
        entry: &GroupEntry,
    ) -> AggregateStateUsage {
        logical_collection_element_usage::<(arrow::row::OwnedRow, GroupEntry)>(1)
            .saturating_add(owned_row_payload_usage(key, 1))
            .saturating_add(entry.accounted_accumulator_usage())
    }

    fn emitted_usage(key: &arrow::row::OwnedRow, values: &Vec<ScalarValue>) -> AggregateStateUsage {
        logical_collection_element_usage::<(arrow::row::OwnedRow, Vec<ScalarValue>)>(1)
            .saturating_add(owned_row_payload_usage(key, 1))
            .saturating_add(retained_changelog_vector_element_usage(values))
    }

    fn dirty_key_usage(key: &arrow::row::OwnedRow) -> AggregateStateUsage {
        logical_collection_element_usage::<arrow::row::OwnedRow>(1)
            .saturating_add(owned_row_payload_usage(key, 1))
    }

    pub(super) fn add_usage(&mut self, usage: AggregateStateUsage) {
        self.usage = self.usage.saturating_add(usage);
    }

    pub(super) fn reconcile_accumulator_usage(
        &mut self,
        previous: AggregateStateUsage,
        current: AggregateStateUsage,
    ) {
        self.usage = self.usage.saturating_sub(previous).saturating_add(current);
    }

    pub(super) fn insert_emit_dirty_key(&mut self, key: arrow::row::OwnedRow) {
        let previous_spare = self.collection_spare_usage();
        let usage = Self::dirty_key_usage(&key);
        if self.emit_dirty_keys.insert(key) {
            self.usage = self.usage.saturating_add(usage);
        }
        self.reconcile_collection_spare_usage(previous_spare);
    }

    pub(super) fn insert_checkpoint_dirty_key(&mut self, key: arrow::row::OwnedRow) {
        let previous_spare = self.collection_spare_usage();
        let usage = Self::dirty_key_usage(&key);
        if self.checkpoint_dirty_keys.insert(key) {
            self.usage = self.usage.saturating_add(usage);
        }
        self.reconcile_collection_spare_usage(previous_spare);
    }

    pub(super) fn insert_last_emitted_dirty_key(&mut self, key: arrow::row::OwnedRow) {
        let previous_spare = self.collection_spare_usage();
        let usage = Self::dirty_key_usage(&key);
        if self.last_emitted_dirty_keys.insert(key) {
            self.usage = self.usage.saturating_add(usage);
        }
        self.reconcile_collection_spare_usage(previous_spare);
    }

    pub(super) fn clear_checkpoint_dirty_keys(&mut self) {
        let previous_spare = self.collection_spare_usage();
        let released = self
            .checkpoint_dirty_keys
            .iter()
            .fold(AggregateStateUsage::default(), |usage, key| {
                usage.saturating_add(Self::dirty_key_usage(key))
            });
        self.checkpoint_dirty_keys.clear();
        self.usage = self.usage.saturating_sub(released);
        self.reconcile_collection_spare_usage(previous_spare);
    }

    pub(super) fn clear_last_emitted_dirty_keys(&mut self) {
        let previous_spare = self.collection_spare_usage();
        let released = self
            .last_emitted_dirty_keys
            .iter()
            .fold(AggregateStateUsage::default(), |usage, key| {
                usage.saturating_add(Self::dirty_key_usage(key))
            });
        self.last_emitted_dirty_keys.clear();
        self.usage = self.usage.saturating_sub(released);
        self.reconcile_collection_spare_usage(previous_spare);
    }

    pub(super) fn replace_emit_dirty_keys_after_attempt(
        &mut self,
        mut dirty: FxHashSet<arrow::row::OwnedRow>,
        emission_succeeded: bool,
    ) {
        let previous_spare = Self::hash_set_spare_usage(&dirty);
        if emission_succeeded {
            let released = dirty
                .iter()
                .fold(AggregateStateUsage::default(), |usage, key| {
                    usage.saturating_add(Self::dirty_key_usage(key))
                });
            dirty.clear();
            self.usage = self.usage.saturating_sub(released);
        }
        let current_spare = Self::hash_set_spare_usage(&dirty);
        self.usage = self
            .usage
            .saturating_sub(previous_spare)
            .saturating_add(current_spare);
        self.emit_dirty_keys = dirty;
    }

    pub(super) fn insert_last_emitted(
        &mut self,
        key: arrow::row::OwnedRow,
        values: Vec<ScalarValue>,
    ) {
        let previous_spare = self.collection_spare_usage();
        let new_usage = Self::emitted_usage(&key, &values);
        if let Some((resident_key, previous_values)) = self.last_emitted.get_key_value(&key) {
            let previous_usage = Self::emitted_usage(resident_key, previous_values);
            self.usage = self
                .usage
                .saturating_sub(previous_usage)
                .saturating_add(new_usage);
        } else {
            self.usage = self.usage.saturating_add(new_usage);
        }
        self.last_emitted.insert(key, values);
        self.reconcile_collection_spare_usage(previous_spare);
    }

    fn recompute_usage(&self) -> AggregateStateUsage {
        let mut usage = vnode_inline_usage::<AggregateVnodeState>(1)
            .saturating_add(self.collection_spare_usage());
        for (key, entry) in &self.groups {
            usage = usage.saturating_add(Self::group_usage(key, entry));
        }
        for (key, values) in &self.last_emitted {
            usage = usage.saturating_add(Self::emitted_usage(key, values));
        }
        for keys in [
            &self.emit_dirty_keys,
            &self.checkpoint_dirty_keys,
            &self.last_emitted_dirty_keys,
        ] {
            for key in keys {
                usage = usage.saturating_add(Self::dirty_key_usage(key));
            }
        }
        usage
    }

    pub(super) fn refresh_usage(&mut self) {
        for entry in self.groups.values_mut() {
            entry.refresh_accumulator_usage();
        }
        self.usage = self.recompute_usage();
    }

    #[inline]
    pub(super) const fn usage(&self) -> AggregateStateUsage {
        self.usage
    }

    #[cfg(test)]
    pub(super) fn cached_usage_matches_structural_recompute(&self) -> bool {
        self.usage == self.recompute_usage()
    }
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
        let fixed_topology_usage = topology_element_usage::<AggregateVnodeSlots>(1).saturating_add(
            topology_element_usage::<Option<Box<AggregateVnodeState>>>(slots.len()),
        );
        Ok(Self {
            slots: slots.into_boxed_slice(),
            active_vnodes,
            resident_group_count: 0,
            fixed_topology_usage,
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

    pub(super) fn accounted_usage(&self) -> AggregateStateUsage {
        let topology = self
            .fixed_topology_usage
            .saturating_add(topology_element_usage::<u32>(self.active_vnodes.capacity()));
        self.iter().fold(topology, |usage, (_, state)| {
            usage.saturating_add(state.usage())
        })
    }

    #[cfg(test)]
    pub(super) fn cached_usage_matches_structural_recompute(&self) -> bool {
        self.iter()
            .all(|(_, state)| state.cached_usage_matches_structural_recompute())
    }

    pub(super) fn active_vnodes(&self) -> &[u32] {
        &self.active_vnodes
    }

    pub(super) fn swap_active_vnodes(&mut self, replacement: &mut Vec<u32>) {
        std::mem::swap(&mut self.active_vnodes, replacement);
    }

    #[inline]
    pub(super) fn replace_for_publication(
        &mut self,
        vnode: u32,
        replacement: Option<Box<AggregateVnodeState>>,
    ) -> Option<Box<AggregateVnodeState>> {
        std::mem::replace(&mut self.slots[vnode as usize], replacement)
    }

    #[cfg(test)]
    pub(super) fn active_vnodes_for_test(&self) -> &[u32] {
        &self.active_vnodes
    }
}
