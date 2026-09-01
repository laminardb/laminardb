//! Shared key decoding + result realignment for on-demand lookup sources.
//!
//! Every [`LookupSource`](crate::lookup::source::LookupSource) has the same
//! shape around its backend-specific fetch: the operator hands keys as
//! `RowConverter`-encoded bytes, the source fetches matching rows in arbitrary
//! order, and each fetched row is matched back to its key by re-encoding its
//! primary-key columns with the *same* converter. [`KeyAligner`] owns that
//! converter and performs the decode + realign so each source only writes its
//! fetch.

use std::sync::Arc;

use arrow::row::{RowConverter, SortField};
use arrow_array::{ArrayRef, RecordBatch};
use rustc_hash::FxHashMap;

use crate::lookup::source::LookupError;

/// Decodes opaque lookup keys and realigns fetched rows to the input key order.
pub struct KeyAligner {
    converter: RowConverter,
    pk_columns: Vec<String>,
}

impl KeyAligner {
    /// Build an aligner from the primary-key sort fields and column names
    /// (same order).
    ///
    /// # Errors
    ///
    /// Returns `LookupError::Internal` if the key is empty or the converter
    /// cannot be built for the given sort fields.
    pub fn new(
        pk_sort_fields: Vec<SortField>,
        pk_columns: Vec<String>,
    ) -> Result<Self, LookupError> {
        if pk_columns.is_empty() {
            return Err(LookupError::Internal(
                "primary_key_columns must not be empty".into(),
            ));
        }
        let converter = RowConverter::new(pk_sort_fields)
            .map_err(|e| LookupError::Internal(format!("row converter: {e}")))?;
        Ok(Self {
            converter,
            pk_columns,
        })
    }

    /// The primary-key column names, in key order.
    #[must_use]
    pub fn pk_columns(&self) -> &[String] {
        &self.pk_columns
    }

    /// Decode opaque key bytes into the primary-key columns (column-major: one
    /// array per PK column, each `keys.len()` long) for building a source
    /// filter.
    ///
    /// # Errors
    ///
    /// Returns `LookupError::Internal` if the bytes cannot be decoded.
    pub fn decode_keys(&self, keys: &[&[u8]]) -> Result<Vec<ArrayRef>, LookupError> {
        let parser = self.converter.parser();
        let parsed = keys.iter().map(|k| parser.parse(k));
        self.converter
            .convert_rows(parsed)
            .map_err(|e| LookupError::Internal(format!("decode keys: {e}")))
    }

    /// Realign fetched rows to the input key order. Each fetched row is matched
    /// to its key by re-encoding its PK columns. Duplicate input keys each
    /// resolve to their own single-row slice; duplicate fetched keys are
    /// rejected, and misses are `None`.
    ///
    /// # Errors
    ///
    /// Returns `LookupError::Internal` if a PK column is absent from a fetched
    /// batch, cannot be re-encoded, or occurs more than once in the fetched rows.
    pub fn align(
        &self,
        keys: &[&[u8]],
        fetched: &[RecordBatch],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        let mut index: FxHashMap<Vec<u8>, (usize, usize)> = FxHashMap::default();
        for (batch_idx, batch) in fetched.iter().enumerate() {
            if batch.num_rows() == 0 {
                continue;
            }
            let pk_cols = self
                .pk_columns
                .iter()
                .map(|name| {
                    let idx = batch.schema().index_of(name).map_err(|_| {
                        LookupError::Internal(format!("pk column not found in result: {name}"))
                    })?;
                    Ok(Arc::clone(batch.column(idx)))
                })
                .collect::<Result<Vec<ArrayRef>, LookupError>>()?;
            let rows = self
                .converter
                .convert_columns(&pk_cols)
                .map_err(|e| LookupError::Internal(format!("encode result keys: {e}")))?;
            for row in 0..batch.num_rows() {
                if index
                    .insert(rows.row(row).as_ref().to_vec(), (batch_idx, row))
                    .is_some()
                {
                    return Err(LookupError::Internal(
                        "lookup source returned multiple rows for one key".into(),
                    ));
                }
            }
        }
        Ok(keys
            .iter()
            .map(|key| index.get(*key).map(|&(bi, row)| fetched[bi].slice(row, 1)))
            .collect())
    }
}

#[cfg(test)]
mod tests;
