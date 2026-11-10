//! Resident groups of one vnode, probed with the key hash a batch computes once per row.

use std::hash::BuildHasher;

use arrow::row::{OwnedRow, Row};
use hashbrown::hash_table::{self, HashTable};
use rustc_hash::FxBuildHasher;

use super::GroupEntry;

fn key_hash(bytes: &[u8]) -> u64 {
    FxBuildHasher.hash_one(bytes)
}

/// An encoded group key with its hash.
#[derive(Clone, Copy)]
pub(super) struct GroupKey<'a> {
    hash: u64,
    row: Row<'a>,
}

impl<'a> GroupKey<'a> {
    pub(super) fn new(row: Row<'a>) -> Self {
        Self {
            hash: key_hash(row.data()),
            row,
        }
    }

    pub(super) fn hash(self) -> u64 {
        self.hash
    }

    pub(super) fn bytes(self) -> &'a [u8] {
        self.row.data()
    }

    pub(super) fn owned(self) -> OwnedRow {
        self.row.owned()
    }

    fn matches(self, resident: &OwnedRow) -> bool {
        resident.as_ref() == self.bytes()
    }
}

/// Resident groups of one vnode by encoded key.
#[derive(Default)]
pub(super) struct GroupMap(HashTable<(OwnedRow, GroupEntry)>);

impl GroupMap {
    fn rehash((key, _): &(OwnedRow, GroupEntry)) -> u64 {
        key_hash(key.as_ref())
    }

    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub(super) fn try_reserve(
        &mut self,
        additional: usize,
    ) -> Result<(), hashbrown::TryReserveError> {
        self.0.try_reserve(additional, Self::rehash)
    }

    pub(super) fn get(&self, key: GroupKey<'_>) -> Option<&GroupEntry> {
        self.0
            .find(key.hash, |(resident, _)| key.matches(resident))
            .map(|(_, entry)| entry)
    }

    pub(super) fn get_mut(&mut self, key: GroupKey<'_>) -> Option<&mut GroupEntry> {
        self.0
            .find_mut(key.hash, |(resident, _)| key.matches(resident))
            .map(|(_, entry)| entry)
    }

    /// Inserts a group whose key the caller has already looked up and found absent.
    pub(super) fn insert_new(
        &mut self,
        key: GroupKey<'_>,
        entry: GroupEntry,
    ) -> &(OwnedRow, GroupEntry) {
        debug_assert!(self.get(key).is_none());
        self.0
            .insert_unique(key.hash, (key.owned(), entry), Self::rehash)
            .into_mut()
    }

    /// Inserts `entry` under `key`, returning the entry it replaced.
    pub(super) fn insert(&mut self, key: OwnedRow, entry: GroupEntry) -> Option<GroupEntry> {
        let probe = GroupKey::new(key.row());
        match self.0.entry(
            probe.hash,
            |(resident, _)| probe.matches(resident),
            Self::rehash,
        ) {
            hash_table::Entry::Occupied(mut occupied) => {
                Some(std::mem::replace(&mut occupied.get_mut().1, entry))
            }
            hash_table::Entry::Vacant(vacant) => {
                vacant.insert((key, entry));
                None
            }
        }
    }

    pub(super) fn remove_entry(&mut self, key: &OwnedRow) -> Option<(OwnedRow, GroupEntry)> {
        let probe = GroupKey::new(key.row());
        let occupied = self
            .0
            .find_entry(probe.hash, |(resident, _)| probe.matches(resident))
            .ok()?;
        Some(occupied.remove().0)
    }

    pub(super) fn iter(&self) -> hash_table::Iter<'_, (OwnedRow, GroupEntry)> {
        self.0.iter()
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &OwnedRow> {
        self.0.iter().map(|(key, _)| key)
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut GroupEntry> {
        self.0.iter_mut().map(|(_, entry)| entry)
    }
}

impl<'a> IntoIterator for &'a GroupMap {
    type Item = &'a (OwnedRow, GroupEntry);
    type IntoIter = hash_table::Iter<'a, (OwnedRow, GroupEntry)>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::DataType;
    use arrow::row::{RowConverter, Rows, SortField};

    use super::*;

    fn encode(keys: Vec<Option<&str>>) -> Rows {
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let column: ArrayRef = Arc::new(StringArray::from(keys));
        converter.convert_columns(&[column]).unwrap()
    }

    fn entry(input_weight: i64) -> GroupEntry {
        GroupEntry::new(Vec::new(), input_weight, 0)
    }

    #[test]
    fn batch_keys_find_owned_groups_by_their_bytes() {
        let rows = encode(vec![Some("a"), Some("ab"), None]);
        let mut groups = GroupMap::default();
        for row_idx in 0..rows.num_rows() {
            let key = GroupKey::new(rows.row(row_idx));
            assert_eq!(key.hash(), GroupKey::new(key.owned().row()).hash());
            groups.insert_new(key, entry(i64::try_from(row_idx).unwrap()));
        }
        for row_idx in 0..rows.num_rows() {
            let found = groups.get(GroupKey::new(rows.row(row_idx)));
            assert_eq!(
                found.map(|group| group.input_weight),
                Some(i64::try_from(row_idx).unwrap())
            );
        }
        let other = encode(vec![Some("b")]);
        assert!(groups.get(GroupKey::new(other.row(0))).is_none());

        let replaced = groups.insert(rows.row(1).owned(), entry(9));
        assert_eq!(replaced.map(|group| group.input_weight), Some(1));
        assert_eq!(groups.len(), 3);
        let (key, removed) = groups.remove_entry(&rows.row(1).owned()).unwrap();
        assert_eq!((key, removed.input_weight), (rows.row(1).owned(), 9));
        assert!(groups.get(GroupKey::new(rows.row(1))).is_none());
        assert!(groups.get(GroupKey::new(rows.row(0))).is_some());
        assert!(groups.insert(rows.row(1).owned(), entry(1)).is_none());
    }
}
