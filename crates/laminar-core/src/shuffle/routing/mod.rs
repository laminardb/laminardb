//! Row-to-vnode routing shared by checkpointed cluster shuffle paths.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::compute::take;
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};

use crate::state::partition_key::{PartitionKeyCodecV1Builder, MAX_PARTITION_KEY_COLUMNS};
use crate::state::{NodeId, PartitionKeyCodecError, PartitionKeyCodecV1, VnodeAssignmentSnapshot};

/// Target decoded size for one routed batch. This is intentionally below the hard receiver bound
/// so schema/allocator variance does not turn ordinary skew into a transport failure.
pub const ROUTE_TARGET_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// Hard decoded size admitted for one logical shuffle batch.
pub const ROUTE_MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
/// Row-count bound prevents tiny or zero-width rows from producing unbounded logical frames.
pub const ROUTE_MAX_BATCH_ROWS: usize = 65_536;

/// Logical Arrow bytes referenced by this slice, independent of backing-buffer capacity or owner.
/// This is the stable bound for IPC content; transport reservations account the retained backing
/// allocation separately.
///
/// # Errors
///
/// Returns an Arrow compute error when buffer-size accounting overflows.
pub fn logical_batch_bytes(batch: &RecordBatch) -> Result<usize, arrow_schema::ArrowError> {
    batch.columns().iter().try_fold(0usize, |total, column| {
        let data = column.to_data();
        let bytes = data
            .get_slice_memory_size()?
            .checked_add(variadic_buffer_bytes(&data)?)
            .ok_or_else(|| {
                arrow_schema::ArrowError::ComputeError(
                    "logical shuffle batch size overflow".to_string(),
                )
            })?;
        total.checked_add(bytes).ok_or_else(|| {
            arrow_schema::ArrowError::ComputeError(
                "logical shuffle batch size overflow".to_string(),
            )
        })
    })
}

fn variadic_buffer_bytes(
    data: &arrow::array::ArrayData,
) -> Result<usize, arrow_schema::ArrowError> {
    let own = if matches!(
        data.data_type(),
        arrow_schema::DataType::Utf8View | arrow_schema::DataType::BinaryView
    ) {
        data.buffers()
            .iter()
            .skip(1)
            .try_fold(0usize, |total, buffer| {
                total.checked_add(buffer.len()).ok_or_else(|| {
                    arrow_schema::ArrowError::ComputeError(
                        "variadic Arrow buffer size overflow".to_string(),
                    )
                })
            })?
    } else {
        0
    };
    data.child_data().iter().try_fold(own, |total, child| {
        total
            .checked_add(variadic_buffer_bytes(child)?)
            .ok_or_else(|| {
                arrow_schema::ArrowError::ComputeError(
                    "nested variadic Arrow buffer size overflow".to_string(),
                )
            })
    })
}

/// A local vnode slice in deterministic vnode order.
#[derive(Debug, Clone)]
pub struct LocalRoute {
    /// Exact local vnode.
    pub vnode: u32,
    /// Rows for that vnode, preserving their input order.
    pub batch: RecordBatch,
    /// Original input-row indices, aligned one-for-one with `batch`.
    pub source_rows: Arc<[u32]>,
}

/// One bounded owner-coalesced remote batch.
#[derive(Debug, Clone)]
pub struct RemoteRoute {
    /// Certified remote owner.
    pub owner: NodeId,
    /// Ascending, duplicate-free vnode set represented by `batch`.
    pub routed_vnodes: Arc<[u32]>,
    /// Rows for this owner, preserving their input order.
    pub batch: RecordBatch,
}

/// Complete, conserving routing plan staged before any remote send.
#[derive(Debug, Clone, Default)]
pub struct CheckpointRoutePlan {
    /// Local chunks, ordered by vnode and then input row.
    pub local: Vec<LocalRoute>,
    /// Remote chunks, ordered by owner and then input row.
    pub remote: Vec<RemoteRoute>,
}

/// A routing failure. No plan is returned, so callers cannot partially send or silently drop rows.
#[derive(Debug, thiserror::Error)]
pub enum ShuffleRoutingError {
    /// Keyed routing requires at least one key column.
    #[error("shuffle routing requires at least one key column")]
    EmptyKey,
    /// Modulo routing is undefined for an empty vnode space.
    #[error("shuffle routing requires a nonzero vnode count")]
    EmptyVnodeSpace,
    /// A resolved key index no longer exists in the batch schema.
    #[error("shuffle key column {index} is outside the {columns}-column batch")]
    KeyColumnOutOfRange {
        /// Requested zero-based index.
        index: usize,
        /// Available columns.
        columns: usize,
    },
    /// ABI v1 admits only scalar, non-floating key types.
    #[error("shuffle key column {index} has unsupported partition type {data_type}")]
    UnsupportedKeyType {
        /// Requested zero-based index.
        index: usize,
        /// Rejected Arrow type.
        data_type: arrow_schema::DataType,
    },
    /// A bounded partition-key ABI invariant failed before row encoding.
    #[error("shuffle partition-key contract: {0}")]
    PartitionKeyContract(PartitionKeyCodecError),
    /// Arrow row encoding or slicing failed.
    #[error("shuffle Arrow routing: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// Routing metadata must cover every input row exactly once.
    #[error("shuffle route has {vnodes} vnode entries for {rows} rows")]
    RowVnodeCardinality {
        /// Input row count.
        rows: usize,
        /// Supplied route count.
        vnodes: usize,
    },
    /// `UInt32` take indices cannot represent this input cardinality.
    #[error("shuffle batch has {rows} rows; maximum supported is {}", u32::MAX)]
    TooManyRows {
        /// Input row count.
        rows: usize,
    },
    /// The route references a vnode outside the pinned assignment.
    #[error("shuffle vnode {vnode} is outside the {vnode_count}-vnode assignment")]
    VnodeOutOfRange {
        /// Invalid vnode.
        vnode: u32,
        /// Pinned assignment cardinality.
        vnode_count: usize,
    },
    /// Formation/rebalance has not assigned this vnode yet.
    #[error("shuffle vnode {vnode} is unassigned")]
    UnassignedVnode {
        /// Vnode awaiting an owner.
        vnode: u32,
    },
    /// A single row cannot be split further and would always fail receiver admission.
    #[error("shuffle row for vnode {vnode} occupies {bytes} decoded bytes; hard limit is {limit}")]
    OversizedRow {
        /// Row's routed vnode.
        vnode: u32,
        /// Decoded Arrow bytes.
        bytes: usize,
        /// Hard decoded limit.
        limit: usize,
    },
}

impl ShuffleRoutingError {
    /// Whether retrying after assignment convergence can succeed without changing the input.
    #[must_use]
    pub const fn is_not_ready(&self) -> bool {
        matches!(self, Self::UnassignedVnode { .. })
    }
}

/// Hash each row with the engine's canonical Arrow-row and xxh3 encoding.
///
/// # Errors
/// Returns a structural or Arrow encoding error instead of panicking on invalid resolved columns.
pub fn row_vnodes(
    batch: &RecordBatch,
    columns: &[usize],
    vnode_count: u32,
) -> Result<Vec<u32>, ShuffleRoutingError> {
    if columns.is_empty() {
        return Err(ShuffleRoutingError::EmptyKey);
    }
    if vnode_count == 0 {
        return Err(ShuffleRoutingError::EmptyVnodeSpace);
    }
    if columns.len() > MAX_PARTITION_KEY_COLUMNS {
        return Err(ShuffleRoutingError::PartitionKeyContract(
            PartitionKeyCodecError::TooManyKeyColumns {
                count: columns.len(),
                limit: MAX_PARTITION_KEY_COLUMNS,
            },
        ));
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut builder = PartitionKeyCodecV1Builder::with_capacity(columns.len());
    for &index in columns {
        let column = batch.columns().get(index).cloned().ok_or(
            ShuffleRoutingError::KeyColumnOutOfRange {
                index,
                columns: batch.num_columns(),
            },
        )?;
        builder
            .push(column.data_type().clone())
            .map_err(|error| match error {
                PartitionKeyCodecError::EmptyKeySchema => ShuffleRoutingError::EmptyKey,
                PartitionKeyCodecError::UnsupportedKeyType { data_type, .. } => {
                    ShuffleRoutingError::UnsupportedKeyType { index, data_type }
                }
                PartitionKeyCodecError::Arrow(error) => ShuffleRoutingError::Arrow(error),
                other => ShuffleRoutingError::PartitionKeyContract(other),
            })?;
        cols.push(column);
    }
    let codec = builder.finish().map_err(|error| match error {
        PartitionKeyCodecError::EmptyKeySchema => ShuffleRoutingError::EmptyKey,
        PartitionKeyCodecError::UnsupportedKeyType { index, data_type } => {
            ShuffleRoutingError::UnsupportedKeyType {
                index: columns[index],
                data_type,
            }
        }
        PartitionKeyCodecError::Arrow(error) => ShuffleRoutingError::Arrow(error),
        other => ShuffleRoutingError::PartitionKeyContract(other),
    })?;
    let rows = codec.encode_columns(&cols)?;
    let vnode_count =
        std::num::NonZeroU32::new(vnode_count).ok_or(ShuffleRoutingError::EmptyVnodeSpace)?;
    Ok((0..batch.num_rows())
        .map(|row| PartitionKeyCodecV1::vnode_for_encoded(rows.row(row).as_ref(), vnode_count))
        .collect())
}

/// Build a complete local/remote plan from one caller-pinned assignment.
///
/// Every input row is assigned exactly once or the whole call fails. Remote batches carry vnode
/// ownership out-of-band; user schemas, including a user field named `__laminar_vnode`, are never
/// rewritten.
///
/// # Errors
/// Returns before producing a plan for malformed metadata, unassigned/out-of-range vnodes, Arrow
/// slicing failures, or a single row above the receiver's hard decoded-memory bound.
pub fn route_checkpointed_batch(
    batch: &RecordBatch,
    row_vnodes: &[u32],
    assignment: &VnodeAssignmentSnapshot,
    self_id: NodeId,
) -> Result<CheckpointRoutePlan, ShuffleRoutingError> {
    if row_vnodes.len() != batch.num_rows() {
        return Err(ShuffleRoutingError::RowVnodeCardinality {
            rows: batch.num_rows(),
            vnodes: row_vnodes.len(),
        });
    }
    if batch.num_rows() > usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
        return Err(ShuffleRoutingError::TooManyRows {
            rows: batch.num_rows(),
        });
    }
    if batch.num_rows() == 0 {
        return Ok(CheckpointRoutePlan::default());
    }

    let mut local_groups: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    let mut remote_groups: BTreeMap<NodeId, Vec<(u32, u32)>> = BTreeMap::new();
    for (row, &vnode) in row_vnodes.iter().enumerate() {
        let owner = assignment
            .owners()
            .get(usize::try_from(vnode).unwrap_or(usize::MAX))
            .copied()
            .ok_or(ShuffleRoutingError::VnodeOutOfRange {
                vnode,
                vnode_count: assignment.owners().len(),
            })?;
        if owner.is_unassigned() {
            return Err(ShuffleRoutingError::UnassignedVnode { vnode });
        }
        let row = u32::try_from(row).map_err(|_| ShuffleRoutingError::TooManyRows {
            rows: batch.num_rows(),
        })?;
        if owner == self_id {
            local_groups.entry(vnode).or_default().push(row);
        } else {
            remote_groups.entry(owner).or_default().push((row, vnode));
        }
    }

    let mut plan = CheckpointRoutePlan::default();
    for (vnode, indices) in local_groups {
        for (range, slice) in bounded_slices(batch, &indices, vnode)? {
            plan.local.push(LocalRoute {
                vnode,
                batch: slice,
                source_rows: Arc::from(&indices[range]),
            });
        }
    }
    for (owner, rows) in remote_groups {
        let indices: Vec<u32> = rows.iter().map(|(row, _)| *row).collect();
        for (range, slice) in bounded_slices(batch, &indices, rows[0].1)? {
            let mut routed_vnodes: Vec<u32> = rows[range].iter().map(|(_, vnode)| *vnode).collect();
            routed_vnodes.sort_unstable();
            routed_vnodes.dedup();
            plan.remote.push(RemoteRoute {
                owner,
                routed_vnodes: routed_vnodes.into(),
                batch: slice,
            });
        }
    }
    debug_assert_eq!(
        plan.local
            .iter()
            .map(|route| route.batch.num_rows())
            .sum::<usize>()
            + plan
                .remote
                .iter()
                .map(|route| route.batch.num_rows())
                .sum::<usize>(),
        batch.num_rows()
    );
    Ok(plan)
}

fn bounded_slices(
    batch: &RecordBatch,
    indices: &[u32],
    vnode: u32,
) -> Result<Vec<(std::ops::Range<usize>, RecordBatch)>, ShuffleRoutingError> {
    let estimated_row_bytes = logical_batch_bytes(batch)?
        .div_ceil(batch.num_rows())
        .max(1);
    let initial_rows =
        ROUTE_MAX_BATCH_ROWS.min((ROUTE_TARGET_BATCH_BYTES / estimated_row_bytes).max(1));
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < indices.len() {
        let mut end = (start + initial_rows).min(indices.len());
        loop {
            let slice = take_rows(batch, &indices[start..end])?;
            let bytes = logical_batch_bytes(&slice)?;
            let rows = end - start;
            if bytes <= ROUTE_TARGET_BATCH_BYTES || rows == 1 {
                if bytes > ROUTE_MAX_BATCH_BYTES {
                    return Err(ShuffleRoutingError::OversizedRow {
                        vnode,
                        bytes,
                        limit: ROUTE_MAX_BATCH_BYTES,
                    });
                }
                chunks.push((start..end, slice));
                start = end;
                break;
            }
            end = start + (rows / 2).max(1);
        }
    }
    Ok(chunks)
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, ShuffleRoutingError> {
    if indices.len() == batch.num_rows() && batch.get_array_memory_size() <= ROUTE_MAX_BATCH_BYTES {
        debug_assert!(indices
            .iter()
            .enumerate()
            .all(|(offset, &index)| u32::try_from(offset).ok() == Some(index)));
        return Ok(batch.clone());
    }

    let indices = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column, &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[cfg(test)]
mod tests;
