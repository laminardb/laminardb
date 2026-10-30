//! Total and per-vnode accounting of core window state: the live vnode states plus the
//! fixed slot table, rosters and reusable scratch every cycle keeps.

use rustc_hash::{FxBuildHasher, FxHashMap};

use super::{CoreWindowState, CoreWindowVnodeState};
use crate::operator_graph::VnodeStateLoad;

impl CoreWindowState {
    fn scratch_nogroup_bytes(scratch: &FxHashMap<i64, Vec<u32>>) -> usize {
        scratch
            .capacity()
            .saturating_mul(std::mem::size_of::<(i64, Vec<u32>)>())
            .saturating_add(scratch.values().fold(0_usize, |bytes, rows| {
                bytes.saturating_add(rows.capacity().saturating_mul(std::mem::size_of::<u32>()))
            }))
    }

    fn scratch_grouped_bytes(scratch: &FxHashMap<(u32, i64, u32), Vec<u32>>) -> usize {
        scratch
            .capacity()
            .saturating_mul(std::mem::size_of::<((u32, i64, u32), Vec<u32>)>())
            .saturating_add(scratch.values().fold(0_usize, |bytes, rows| {
                bytes.saturating_add(rows.capacity().saturating_mul(std::mem::size_of::<u32>()))
            }))
    }

    fn scratch_group_keys_bytes(
        keys: &indexmap::IndexSet<arrow::row::OwnedRow, FxBuildHasher>,
    ) -> usize {
        keys.capacity()
            .saturating_mul(std::mem::size_of::<arrow::row::OwnedRow>())
            .saturating_add(keys.iter().fold(0_usize, |bytes, key| {
                bytes.saturating_add(key.as_ref().len())
            }))
    }

    fn window_group_counts_bytes(counts: &FxHashMap<i64, usize>) -> usize {
        counts
            .capacity()
            .saturating_mul(std::mem::size_of::<(i64, usize)>())
    }

    #[must_use]
    pub(crate) fn accounted_state_bytes(&self) -> usize {
        self.active_vnodes
            .iter()
            .filter_map(|vnode| self.vnode_states[*vnode as usize].as_deref())
            .fold(0_usize, |bytes, state| {
                bytes
                    .saturating_add(std::mem::size_of::<CoreWindowVnodeState>())
                    .saturating_add(state.accounted_state_bytes)
            })
            .saturating_add(Self::scratch_nogroup_bytes(&self.scratch_nogroup))
            .saturating_add(Self::scratch_grouped_bytes(&self.scratch_grouped))
            .saturating_add(Self::scratch_group_keys_bytes(&self.scratch_group_keys))
            .saturating_add(Self::window_group_counts_bytes(&self.window_group_counts))
            .saturating_add(
                self.vnode_states
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<Box<CoreWindowVnodeState>>>()),
            )
            .saturating_add(
                self.active_vnodes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.active_vnode_positions
                    .len()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add(
                self.checkpoint_dirty_vnodes
                    .len()
                    .saturating_mul(std::mem::size_of::<bool>()),
            )
            .saturating_add(
                self.checkpoint_dirty_vnode_roster
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.vnode_applied_rows
                    .len()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
    }

    /// Each resident vnode's window state. Groups count each key once per open window or
    /// session.
    pub(crate) fn vnode_loads(&self) -> Vec<VnodeStateLoad> {
        self.active_vnodes
            .iter()
            .filter_map(|&vnode| {
                let state = self.vnode_states[vnode as usize].as_deref()?;
                let groups = state
                    .windows
                    .values()
                    .map(FxHashMap::len)
                    .fold(state.session_groups.len(), usize::saturating_add);
                Some(VnodeStateLoad {
                    vnode,
                    state_bytes: std::mem::size_of::<CoreWindowVnodeState>()
                        .saturating_add(state.accounted_state_bytes),
                    groups,
                    applied_rows: self.vnode_applied_rows[vnode as usize],
                })
            })
            .collect()
    }

    pub(super) fn count_applied_rows(&mut self, vnode: u32, rows: usize) {
        let applied = &mut self.vnode_applied_rows[vnode as usize];
        *applied = applied.saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
    }
}
