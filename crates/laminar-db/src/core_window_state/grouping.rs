//! A window batch's rows grouped by key and window, with each distinct key encoded and owned
//! once per batch rather than once per row.

use std::hash::BuildHasher;

use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, Row};
use hashbrown::hash_table::{Entry, HashTable};
use rustc_hash::FxBuildHasher;

use super::{CoreWindowAssigner, CoreWindowState, NULL_TIMESTAMP};
use crate::aggregate_state::{AggregateInput, EncodedKeys, IncrementalAggState};
use crate::error::DbError;

const UNNUMBERED: u32 = u32::MAX;

fn key_hash(bytes: &[u8]) -> u64 {
    FxBuildHasher.hash_one(bytes)
}

/// Numbers a batch's distinct group keys in the order [`BatchKeyIds::number`] first sees them,
/// comparing encoded bytes, and keeps each key's vnode.
#[derive(Default)]
pub(super) struct BatchKeyIds {
    /// Ids of keys encoded once per row, by key hash.
    by_hash: HashTable<u32>,
    /// The id of each distinct value's key, for keys encoded once per value.
    by_value: Vec<u32>,
    /// Each id's index in the encoded rows.
    rows: Vec<u32>,
    vnodes: Vec<u32>,
}

impl BatchKeyIds {
    pub(super) fn reset(&mut self, keys: &EncodedKeys) {
        self.by_hash.clear();
        self.by_value.clear();
        self.rows.clear();
        self.vnodes.clear();
        if let EncodedKeys::PerGroup { keys, .. } = keys {
            self.by_value.resize(keys.num_rows(), UNNUMBERED);
        }
    }

    /// `row`'s key id and vnode. A key not seen before takes the next id and its vnode from
    /// `vnode_of`. A batch holds at most `u32::MAX` rows.
    #[allow(clippy::cast_possible_truncation)]
    pub(super) fn number(
        &mut self,
        keys: &EncodedKeys,
        row: usize,
        vnode_of: impl FnOnce(&[u8]) -> u32,
    ) -> (u32, u32) {
        let next = self.rows.len() as u32;
        let index = match keys {
            EncodedKeys::PerGroup { group_of_row, .. } => {
                let value = group_of_row[row];
                let id = &mut self.by_value[value as usize];
                if *id != UNNUMBERED {
                    return (*id, self.vnodes[*id as usize]);
                }
                *id = next;
                value
            }
            EncodedKeys::PerRow(rows) => {
                let bytes = rows.row(row).data();
                let indices = &self.rows;
                let key_of = |id: &u32| rows.row(indices[*id as usize] as usize).data();
                match self.by_hash.entry(
                    key_hash(bytes),
                    |id| key_of(id) == bytes,
                    |id| key_hash(key_of(id)),
                ) {
                    Entry::Occupied(entry) => {
                        let id = *entry.get();
                        return (id, self.vnodes[id as usize]);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(next);
                        row as u32
                    }
                }
            }
        };
        self.rows.push(index);
        let vnode = vnode_of(self.key(keys, next).data());
        self.vnodes.push(vnode);
        (next, vnode)
    }

    pub(super) fn key<'k>(&self, keys: &'k EncodedKeys, id: u32) -> Row<'k> {
        let index = self.rows[id as usize] as usize;
        match keys {
            EncodedKeys::PerRow(rows) => rows.row(index),
            EncodedKeys::PerGroup { keys, .. } => keys.row(index),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn retained_bytes(&self) -> usize {
        self.by_hash
            .capacity()
            .saturating_add(self.by_value.capacity())
            .saturating_add(self.rows.capacity())
            .saturating_add(self.vnodes.capacity())
            .saturating_mul(std::mem::size_of::<u32>())
    }
}

impl CoreWindowState {
    pub(super) fn encode_group_keys(&self, batch: &RecordBatch) -> Result<EncodedKeys, DbError> {
        if let Some(grouper) = &self.key_grouper {
            return grouper.encode(batch.column(0), &self.row_converter);
        }
        self.row_converter
            .convert_columns(&batch.columns()[..self.num_group_cols])
            .map(EncodedKeys::PerRow)
            .map_err(|error| DbError::Pipeline(format!("row conversion: {error}")))
    }

    pub(super) fn update_batch_grouped(
        &mut self,
        batch: &RecordBatch,
        ts_array: &[i64],
        vnode_hint: Option<u32>,
    ) -> Result<(), DbError> {
        let keys = self.encode_group_keys(batch)?;
        let inputs =
            AggregateInput::resolve_all(batch, &self.agg_specs, &self.aggregate_templates, None)?;

        let mut grouped = std::mem::take(&mut self.scratch_grouped);
        let mut key_ids = std::mem::take(&mut self.scratch_key_ids);
        grouped.clear();
        key_ids.reset(&keys);

        for (row_idx, &ts_ms) in ts_array.iter().enumerate() {
            if ts_ms == NULL_TIMESTAMP {
                continue;
            }
            let (gid, vnode) = key_ids.number(&keys, row_idx, |key| self.vnode_for_group_key(key));
            if vnode_hint.is_some_and(|hint| hint != vnode) {
                return Err(DbError::Pipeline(format!(
                    "Core window batch routed to vnode {} contains a key for vnode {vnode}",
                    vnode_hint.expect("checked as Some")
                )));
            }
            #[allow(clippy::cast_possible_truncation)]
            let idx = row_idx as u32;
            match &self.assigner {
                CoreWindowAssigner::Tumbling(a) => {
                    let ws = a
                        .try_assign(ts_ms)
                        .map_err(|error| {
                            DbError::PipelineTerminal(format!(
                                "Core tumbling window assignment failed: {error}"
                            ))
                        })?
                        .start;
                    if self.is_window_closed(ws) {
                        self.record_late_drop(1);
                        continue;
                    }
                    grouped.entry((vnode, ws, gid)).or_default().push(idx);
                }
                CoreWindowAssigner::Hopping(a) => {
                    let windows = a.try_iter_windows(ts_ms).map_err(|error| {
                        DbError::PipelineTerminal(format!(
                            "Core hopping window assignment failed: {error}"
                        ))
                    })?;
                    for wid in windows {
                        if self.is_window_closed(wid.start) {
                            self.record_late_drop(1);
                            continue;
                        }
                        grouped
                            .entry((vnode, wid.start, gid))
                            .or_default()
                            .push(idx);
                    }
                }
                CoreWindowAssigner::Session { .. } => unreachable!("handled above"),
            }
        }

        // Each key is owned once per batch and moved into the last window that inserts it.
        let mut windows_left = vec![0_u32; key_ids.len()];
        for &(_, _, gid) in grouped.keys() {
            windows_left[gid as usize] += 1;
        }
        let mut owned_keys: Vec<Option<OwnedRow>> = vec![None; key_ids.len()];
        for (&(vnode, window_start, gid), indices) in &grouped {
            self.count_applied_rows(vnode, indices.len());
            windows_left[gid as usize] -= 1;
            let row_key =
                owned_keys[gid as usize].get_or_insert_with(|| key_ids.key(&keys, gid).owned());
            let resident = self.vnode_states[vnode as usize]
                .as_ref()
                .and_then(|state| state.windows.get(&window_start))
                .is_some_and(|groups| groups.contains_key(row_key));
            if !resident {
                let groups = self.window_group_counts.get(&window_start).copied();
                self.admit_group_count(groups.unwrap_or(0).saturating_add(1), || {
                    format!("window {window_start} record processing")
                })?;
                let key = if windows_left[gid as usize] == 0 {
                    owned_keys[gid as usize].take().expect("owned above")
                } else {
                    row_key.clone()
                };
                let mut accs = self.aggregate_templates.clone();
                let update =
                    IncrementalAggState::update_concrete_accumulators(&mut accs, &inputs, indices);
                self.insert_fixed_group(vnode, window_start, key, accs);
                update?;
                self.mark_checkpoint_vnode_dirty(vnode);
                continue;
            }

            let state = self.vnode_states[vnode as usize]
                .as_deref_mut()
                .expect("Core window vnode must exist");
            let (previous_accumulator_bytes, update, current_accumulator_bytes) = {
                let accs = state
                    .windows
                    .get_mut(&window_start)
                    .and_then(|groups| groups.get_mut(row_key))
                    .expect("Core window group must exist");
                let previous = Self::accumulator_vector_bytes(accs);
                let update =
                    IncrementalAggState::update_concrete_accumulators(accs, &inputs, indices);
                (previous, update, Self::accumulator_vector_bytes(accs))
            };
            state.accounted_state_bytes = state
                .accounted_state_bytes
                .saturating_sub(previous_accumulator_bytes)
                .saturating_add(current_accumulator_bytes);
            update?;
            self.mark_checkpoint_vnode_dirty(vnode);
        }

        self.scratch_grouped = grouped;
        self.scratch_key_ids = key_ids;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow::row::{RowConverter, SortField};

    use super::*;
    use crate::aggregate_state::FixedWidthKeyGrouper;

    /// Numbers `rows` of `key` and checks each id against first-appearance order over the
    /// row-encoded keys, as an insertion-ordered set numbers them, each id's key bytes, and one vnode call per distinct key.
    fn assert_numbered_in_first_appearance_order(key: ArrayRef, rows: &[usize]) {
        let data_type = key.data_type().clone();
        let converter = RowConverter::new(vec![SortField::new(data_type.clone())]).unwrap();
        let per_row = converter.convert_columns(&[Arc::clone(&key)]).unwrap();
        let keys = match FixedWidthKeyGrouper::for_key(&[data_type]) {
            Some(grouper) => grouper.encode(&key, &converter).unwrap(),
            None => EncodedKeys::PerRow(converter.convert_columns(&[key]).unwrap()),
        };
        let mut expected: Vec<&[u8]> = Vec::new();
        let mut ids = BatchKeyIds::default();
        ids.reset(&keys);
        let vnode_calls = Cell::new(0);
        for &row in rows {
            let (id, vnode) = ids.number(&keys, row, |bytes| {
                vnode_calls.set(vnode_calls.get() + 1);
                u32::try_from(bytes.len()).unwrap()
            });
            let bytes = per_row.row(row).data();
            let expected_id = expected.iter().position(|seen| *seen == bytes);
            let expected_id = expected_id.unwrap_or_else(|| {
                expected.push(bytes);
                expected.len() - 1
            });
            assert_eq!(id as usize, expected_id);
            assert_eq!(ids.key(&keys, id).data(), per_row.row(row).data());
            assert_eq!(vnode as usize, per_row.row(row).data().len());
        }
        assert_eq!(ids.len(), expected.len());
        assert_eq!(vnode_calls.get(), expected.len());
    }

    #[test]
    fn values_grouped_once_are_numbered_in_first_appearance_order() {
        let values = (0..40).map(|row| (row % 7 != 3).then_some((row * 5) % 9));
        let key: ArrayRef = Arc::new(values.collect::<Int64Array>());
        let keys = FixedWidthKeyGrouper::for_key(&[DataType::Int64])
            .unwrap()
            .encode(
                &key,
                &RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap(),
            )
            .unwrap();
        assert!(matches!(keys, EncodedKeys::PerGroup { .. }));
        let rows: Vec<usize> = (0..40).filter(|row| row % 4 != 1).collect();
        assert_numbered_in_first_appearance_order(key, &rows);
    }

    #[test]
    fn keys_encoded_per_row_are_numbered_in_first_appearance_order() {
        let key: ArrayRef = Arc::new(StringArray::from(vec![
            Some("b"),
            None,
            Some("a"),
            Some("b"),
            Some(""),
            None,
            Some("a"),
        ]));
        assert_numbered_in_first_appearance_order(key, &[1, 0, 3, 2, 4, 5, 6]);
    }

    #[test]
    fn reset_starts_numbering_again() {
        let key: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let keys = EncodedKeys::PerRow(converter.convert_columns(&[key]).unwrap());
        let mut ids = BatchKeyIds::default();
        ids.reset(&keys);
        assert_eq!(ids.number(&keys, 1, |_| 7), (0, 7));
        ids.reset(&keys);
        assert_eq!(ids.number(&keys, 0, |_| 9), (0, 9));
        assert_eq!(ids.number(&keys, 1, |_| 8), (1, 8));
        assert_eq!(ids.len(), 2);
    }
}
