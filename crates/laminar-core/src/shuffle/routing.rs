//! Row→vnode routing shared by the cluster shuffle paths (the aggregate
//! row-shuffle, the lookup-enrich key-shuffle, and `ClusterRepartitionExec`).
//! Hashing matches [`crate::state::vnode_for_key`] so every stage agrees on a
//! key's vnode.

use std::sync::Arc;

use arrow::compute::take;
use arrow::row::{RowConverter, SortField};
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use rustc_hash::FxHashMap;

use crate::state::{key_hash, NodeId, VnodeRegistry};

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

/// Slice batch by vnodes in a single pass, returning a vector of (vnode, `RecordBatch`).
/// Avoids the $O(V \times R)$ loop and allocations of vnode-by-vnode slicing.
///
/// # Panics
/// Panics if `take` or rebuild fails — only on internal-invariant violations.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn slice_batch_by_vnodes(batch: &RecordBatch, row_vnodes: &[u32]) -> Vec<(u32, RecordBatch)> {
    if batch.num_rows() == 0 {
        return Vec::new();
    }
    let mut groups: FxHashMap<u32, Vec<u32>> = FxHashMap::default();
    for (i, &v) in row_vnodes.iter().enumerate() {
        groups.entry(v).or_default().push(i as u32);
    }
    let mut slices = Vec::with_capacity(groups.len());
    for (vnode, indices_vec) in groups {
        let indices = UInt32Array::from(indices_vec);
        let cols: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).expect("take"))
            .collect();
        if let Ok(slice) = RecordBatch::try_new(batch.schema(), cols) {
            slices.push((vnode, slice));
        }
    }
    slices
}

/// Slices a `RecordBatch` by targets.
/// Remote rows are grouped by `NodeId`, and the metadata column `__laminar_vnode` is appended to the batch.
/// Local rows are grouped by vnode.
///
/// Returns (`local_slices`, `remote_slices`).
///
/// # Panics
/// Panics if `take` or rebuild fails — only on internal-invariant violations.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn slice_batch_by_targets(
    batch: &RecordBatch,
    row_vnodes: &[u32],
    registry: &VnodeRegistry,
    self_id: NodeId,
) -> (FxHashMap<u32, RecordBatch>, FxHashMap<NodeId, RecordBatch>) {
    if batch.num_rows() == 0 {
        return (FxHashMap::default(), FxHashMap::default());
    }

    let mut local_groups: FxHashMap<u32, Vec<u32>> = FxHashMap::default();
    let mut remote_groups: FxHashMap<NodeId, (Vec<u32>, Vec<u32>)> = FxHashMap::default();

    for (row_idx, &vnode) in row_vnodes.iter().enumerate() {
        let owner = registry.owner(vnode);
        if owner == self_id {
            local_groups.entry(vnode).or_default().push(row_idx as u32);
        } else if !owner.is_unassigned() {
            let entry = remote_groups.entry(owner).or_default();
            entry.0.push(row_idx as u32);
            entry.1.push(vnode);
        }
    }

    let mut local_slices = FxHashMap::default();
    for (vnode, indices_vec) in local_groups {
        let indices = UInt32Array::from(indices_vec);
        let cols: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).expect("take"))
            .collect();
        if let Ok(slice) = RecordBatch::try_new(batch.schema(), cols) {
            local_slices.insert(vnode, slice);
        }
    }

    let mut remote_slices = FxHashMap::default();
    for (node_id, (indices_vec, vnodes_vec)) in remote_groups {
        let indices = UInt32Array::from(indices_vec);
        let mut cols: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).expect("take"))
            .collect();
        let vnode_col = Arc::new(UInt32Array::from(vnodes_vec)) as ArrayRef;
        cols.push(vnode_col);

        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(arrow_schema::Field::new(
            "__laminar_vnode",
            arrow_schema::DataType::UInt32,
            false,
        )));
        let extended_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        ));

        if let Ok(slice) = RecordBatch::try_new(extended_schema, cols) {
            remote_slices.insert(node_id, slice);
        }
    }

    (local_slices, remote_slices)
}
