//! One input batch's rows grouped by encoded group key.

use std::hash::{Hash, Hasher};

use arrow::array::Int64Array;
use arrow::row::{OwnedRow, Row, Rows};
use rustc_hash::FxHashMap;

use crate::error::DbError;

/// A group key borrowed from a batch, for looking up a resident group without allocating its
/// `OwnedRow`.
pub(super) struct EncodedKey<'a>(pub(super) &'a [u8]);

impl Hash for EncodedKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // `OwnedRow` hashes its bytes as a slice, length prefix included; `write` would miss.
        self.0.hash(state);
    }
}

impl hashbrown::Equivalent<OwnedRow> for EncodedKey<'_> {
    fn equivalent(&self, key: &OwnedRow) -> bool {
        self.0 == key.as_ref()
    }
}

/// A batch's groups in first-appearance order, with all row indices in one buffer. Each group's
/// rows stay in batch order, so accumulators see the same sequence as a per-group scan.
pub(super) struct BatchGroups<'a> {
    keys: Vec<Row<'a>>,
    input_weights: Vec<i64>,
    /// Exclusive end of each group's rows in `rows`; a group starts where the previous one ends.
    ends: Vec<u32>,
    rows: Vec<u32>,
}

impl<'a> BatchGroups<'a> {
    pub(super) fn try_new(
        rows: &'a Rows,
        input_weights: Option<&Int64Array>,
    ) -> Result<Self, DbError> {
        let num_rows = u32::try_from(rows.num_rows()).map_err(|_| {
            DbError::Pipeline(format!(
                "aggregate batch has {} rows; row indices are limited to u32",
                rows.num_rows()
            ))
        })?;
        let estimated_groups = (rows.num_rows() / 4).max(16);
        let mut group_ids: FxHashMap<Row<'a>, u32> =
            FxHashMap::with_capacity_and_hasher(estimated_groups, rustc_hash::FxBuildHasher);
        let mut keys = Vec::with_capacity(estimated_groups);
        let mut weights: Vec<i64> = Vec::with_capacity(estimated_groups);
        let mut ends: Vec<u32> = Vec::with_capacity(estimated_groups);
        let mut group_of_row = Vec::with_capacity(rows.num_rows());
        let mut group_count = 0;
        for row_idx in 0..num_rows {
            let row = rows.row(row_idx as usize);
            let group = *group_ids.entry(row).or_insert(group_count);
            if group == group_count {
                group_count += 1;
                keys.push(row);
                weights.push(0);
                ends.push(0);
            }
            let row_weight = input_weights.map_or(1, |column| column.value(row_idx as usize));
            let weight = &mut weights[group as usize];
            *weight = weight
                .checked_add(row_weight)
                .ok_or_else(|| DbError::Pipeline("aggregate input weight overflow".into()))?;
            ends[group as usize] += 1;
            group_of_row.push(group);
        }

        // Counts become start offsets, and the stable scatter moves each start to its group's end.
        let mut start = 0;
        for slot in &mut ends {
            let count = *slot;
            *slot = start;
            start += count;
        }
        let mut sorted = vec![0; group_of_row.len()];
        for (row_idx, group) in (0..num_rows).zip(group_of_row) {
            let slot = &mut ends[group as usize];
            sorted[*slot as usize] = row_idx;
            *slot += 1;
        }
        Ok(Self {
            keys,
            input_weights: weights,
            ends,
            rows: sorted,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Each group's key, row indices, and summed input weight.
    pub(super) fn iter(&self) -> impl Iterator<Item = (Row<'a>, &[u32], i64)> + '_ {
        let mut start = 0;
        self.keys
            .iter()
            .zip(&self.input_weights)
            .zip(&self.ends)
            .map(move |((key, weight), &end)| {
                let rows = &self.rows[start as usize..end as usize];
                start = end;
                (*key, rows, *weight)
            })
    }
}

#[cfg(test)]
mod tests {
    use std::hash::BuildHasher;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::DataType;
    use arrow::row::{RowConverter, SortField};

    use super::*;

    fn encode(keys: Vec<Option<&str>>) -> Rows {
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let column: ArrayRef = Arc::new(StringArray::from(keys));
        converter.convert_columns(&[column]).unwrap()
    }

    #[test]
    fn encoded_key_hashes_and_matches_like_its_owned_row() {
        let rows = encode(vec![Some("a"), Some("ab"), None]);
        let hasher = rustc_hash::FxBuildHasher;
        let mut groups: hashbrown::HashMap<OwnedRow, usize, _> =
            hashbrown::HashMap::with_hasher(hasher);
        for row_idx in 0..rows.num_rows() {
            let row = rows.row(row_idx);
            assert_eq!(
                hasher.hash_one(EncodedKey(row.data())),
                hasher.hash_one(row.owned())
            );
            groups.insert(row.owned(), row_idx);
        }
        for row_idx in 0..rows.num_rows() {
            assert_eq!(
                groups.get(&EncodedKey(rows.row(row_idx).data())),
                Some(&row_idx)
            );
        }
        let other = encode(vec![Some("b")]);
        assert_eq!(groups.get(&EncodedKey(other.row(0).data())), None);
    }

    #[test]
    fn groups_keep_first_appearance_and_batch_row_order() {
        let rows = encode(vec![
            Some("b"),
            Some("a"),
            Some("b"),
            None,
            Some("a"),
            Some("b"),
        ]);
        let weights = Int64Array::from(vec![1, 2, 3, 4, 5, -6]);
        let groups = BatchGroups::try_new(&rows, Some(&weights)).unwrap();
        let grouped: Vec<_> = groups
            .iter()
            .map(|(key, rows, weight)| (key.owned(), rows.to_vec(), weight))
            .collect();
        assert_eq!(groups.len(), 3);
        assert_eq!(
            grouped,
            vec![
                (rows.row(0).owned(), vec![0, 2, 5], -2),
                (rows.row(1).owned(), vec![1, 4], 7),
                (rows.row(3).owned(), vec![3], 4),
            ]
        );

        let unweighted = BatchGroups::try_new(&rows, None).unwrap();
        let weights: Vec<i64> = unweighted.iter().map(|(_, _, weight)| weight).collect();
        assert_eq!(weights, vec![3, 2, 1]);
    }

    #[test]
    fn group_weight_overflow_rejects_the_batch() {
        let rows = encode(vec![Some("a"), Some("a")]);
        let weights = Int64Array::from(vec![i64::MAX, 1]);
        let Err(DbError::Pipeline(message)) = BatchGroups::try_new(&rows, Some(&weights)) else {
            panic!("an overflowing group weight must reject the batch");
        };
        assert_eq!(message, "aggregate input weight overflow");
    }
}
