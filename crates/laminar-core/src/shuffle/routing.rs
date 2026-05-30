//! Row→vnode routing shared by the cluster shuffle paths (the aggregate
//! row-shuffle, the lookup-enrich key-shuffle, and `ClusterRepartitionExec`).
//! Hashing matches [`crate::state::vnode_for_key`] so every stage agrees on a
//! key's vnode.

use std::sync::Arc;

use arrow::compute::take;
use arrow::row::{RowConverter, SortField};
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};

use crate::state::key_hash;

/// Vnode for each row, hashing `columns` (by index) with the engine's
/// `arrow-row` + xxh3 encoding. `columns` must be non-empty.
///
/// # Panics
/// Panics if `columns` holds an out-of-range index or the columns cannot be
/// row-encoded — both internal-invariant violations, not input errors.
#[must_use]
pub fn row_vnodes(batch: &RecordBatch, columns: &[usize], vnode_count: u32) -> Vec<u32> {
    let cols: Vec<ArrayRef> = columns
        .iter()
        .map(|&i| Arc::clone(batch.column(i)))
        .collect();
    let fields: Vec<SortField> = cols
        .iter()
        .map(|c| SortField::new(c.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).expect("row converter");
    let rows = converter.convert_columns(&cols).expect("convert rows");
    (0..batch.num_rows())
        .map(|r| {
            #[allow(clippy::cast_possible_truncation)]
            let v = (key_hash(rows.row(r).as_ref()) % u64::from(vnode_count)) as u32;
            v
        })
        .collect()
}

/// The sub-batch of `batch` whose `row_vnodes[i] == target`, or `None` if no
/// row maps to `target`.
///
/// # Panics
/// Panics if the `take` or rebuild fails — only on an internal-invariant
/// violation (the indices are derived from `batch` itself).
#[must_use]
pub fn slice_batch_by_vnode(
    batch: &RecordBatch,
    row_vnodes: &[u32],
    target: u32,
) -> Option<RecordBatch> {
    let indices: UInt32Array = row_vnodes
        .iter()
        .enumerate()
        .filter_map(|(i, &v)| (v == target).then(|| u32::try_from(i).ok()).flatten())
        .collect();
    if indices.is_empty() {
        return None;
    }
    let cols: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None).expect("take"))
        .collect();
    Some(RecordBatch::try_new(batch.schema(), cols).expect("rebuild"))
}
