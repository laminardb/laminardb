//! What happens to a streamed row during the copy, and the merge of streamed changes into a
//! fetched chunk that is not emitted yet.

use std::collections::HashMap;

use crate::error::ConnectorError;

use super::super::{RowImage, RowValues};

/// Where a streamed row's primary key lies relative to the copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyClass {
    /// At or below the bound: its copied state is already emitted.
    Copied,
    /// In the held chunk's range; `visible` when the chunk's snapshot already includes the
    /// row's transaction.
    Held { visible: bool },
    /// Above every fetched range: a later chunk copies it.
    Uncopied,
}

/// What the drain does with one streamed row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowAction {
    Emit,
    Drop,
    Merge,
    MergeAndEmit,
}

/// The row actions of the copy. Upsert tombstones are emitted in every class, so a restart
/// inside the copy that re-reads a range still deletes rows an earlier attempt wrote.
pub(crate) fn row_action(class: KeyClass, image: RowImage) -> RowAction {
    match (class, image) {
        (KeyClass::Copied, _) => RowAction::Emit,
        (KeyClass::Held { visible: false }, RowImage::Tombstone) => RowAction::MergeAndEmit,
        (KeyClass::Held { visible: false }, _) => RowAction::Merge,
        (KeyClass::Held { visible: true } | KeyClass::Uncopied, RowImage::Tombstone) => {
            RowAction::Emit
        }
        (KeyClass::Held { visible: true } | KeyClass::Uncopied, _) => RowAction::Drop,
    }
}

/// The primary-key texts of `values` at `key_columns`, length-prefixed into one map key.
///
/// # Errors
/// Returns an error when a key column is `NULL`.
pub(crate) fn encode_key(
    values: &RowValues,
    key_columns: &[usize],
) -> Result<Box<[u8]>, ConnectorError> {
    let mut key = Vec::new();
    for &column in key_columns {
        let text = values
            .get(column)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                ConnectorError::ReadError("PostgreSQL CDC row has a NULL primary-key column".into())
            })?;
        let length = u32::try_from(text.len()).map_err(|_| {
            ConnectorError::ReadError("PostgreSQL CDC primary-key value is too long".into())
        })?;
        key.extend_from_slice(&length.to_be_bytes());
        key.extend_from_slice(text);
    }
    Ok(key.into_boxed_slice())
}

/// Rows of a fetched chunk, kept in `SELECT` order with merged inserts appended, until the chunk
/// is emitted or abandoned.
#[derive(Default)]
pub(crate) struct HeldRows {
    rows: Vec<Option<(RowValues, usize)>>,
    positions: HashMap<Box<[u8]>, usize>,
    bytes: usize,
}

impl HeldRows {
    /// Retained bytes of the rows held now.
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Apply one row: a fetched row or an invisible streamed change in the chunk's range.
    ///
    /// # Errors
    /// Fails closed when a change contradicts the held state: a retraction of a row image that is
    /// not held byte for byte, or an insertion of a key already held. Both mean the stream and
    /// the chunk disagree about the table.
    pub(crate) fn apply(
        &mut self,
        image: RowImage,
        key: Box<[u8]>,
        values: RowValues,
        bytes: usize,
    ) -> Result<(), ConnectorError> {
        let held = self.positions.get(&key).copied();
        match (image, held) {
            (RowImage::Put, Some(index)) => {
                self.remove(index);
                self.place(index, key, values, bytes);
            }
            (RowImage::Put | RowImage::Weighted(1), None) => self.push(key, values, bytes),
            (RowImage::Tombstone, Some(index)) => {
                self.remove(index);
                self.positions.remove(&key);
            }
            (RowImage::Tombstone, None) => {}
            (RowImage::Weighted(-1), Some(index))
                if self.rows[index]
                    .as_ref()
                    .is_some_and(|(held, _)| *held == values) =>
            {
                self.remove(index);
                self.positions.remove(&key);
            }
            (RowImage::Weighted(weight), _) => {
                return Err(ConnectorError::Internal(format!(
                    "PostgreSQL CDC snapshot merge found a {weight:+} change that contradicts the \
                     held chunk (the key is {}held, or its image differs)",
                    if held.is_some() { "" } else { "not " }
                )));
            }
        }
        Ok(())
    }

    /// The held rows in order from slot `start`, each with the slot after it.
    pub(crate) fn rows_from(
        &self,
        start: usize,
    ) -> impl Iterator<Item = (usize, &RowValues, usize)> {
        self.rows
            .get(start..)
            .unwrap_or_default()
            .iter()
            .zip(start + 1..)
            .filter_map(|(row, next)| row.as_ref().map(|(values, bytes)| (next, values, *bytes)))
    }

    fn remove(&mut self, index: usize) {
        if let Some((_, bytes)) = self.rows[index].take() {
            self.bytes = self.bytes.saturating_sub(bytes);
        }
    }

    fn place(&mut self, index: usize, key: Box<[u8]>, values: RowValues, bytes: usize) {
        self.rows[index] = Some((values, bytes));
        self.positions.insert(key, index);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    fn push(&mut self, key: Box<[u8]>, values: RowValues, bytes: usize) {
        self.rows.push(None);
        self.place(self.rows.len() - 1, key, values, bytes);
    }
}
