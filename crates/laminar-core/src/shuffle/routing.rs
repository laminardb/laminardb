//! Row-to-vnode routing shared by checkpointed cluster shuffle paths.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::compute::take;
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_row::{RowConverter, SortField};

use crate::state::{key_hash, NodeId, VnodeAssignmentSnapshot};

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
pub(crate) fn logical_batch_bytes(batch: &RecordBatch) -> Result<usize, arrow_schema::ArrowError> {
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
    let cols: Vec<ArrayRef> = columns
        .iter()
        .map(|&index| {
            let column = batch.columns().get(index).cloned().ok_or(
                ShuffleRoutingError::KeyColumnOutOfRange {
                    index,
                    columns: batch.num_columns(),
                },
            )?;
            if !is_supported_key_type(column.data_type()) {
                return Err(ShuffleRoutingError::UnsupportedKeyType {
                    index,
                    data_type: column.data_type().clone(),
                });
            }
            Ok(column)
        })
        .collect::<Result<_, _>>()?;
    let fields: Vec<SortField> = cols
        .iter()
        .map(|column| SortField::new(column.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields)?;
    let rows = converter.convert_columns(&cols)?;
    (0..batch.num_rows())
        .map(|row| {
            u32::try_from(key_hash(rows.row(row).as_ref()) % u64::from(vnode_count))
                .map_err(|_| ShuffleRoutingError::EmptyVnodeSpace)
        })
        .collect()
}

fn is_supported_key_type(data_type: &arrow_schema::DataType) -> bool {
    match data_type {
        // Dictionary indices are an encoding detail. Hash the hydrated scalar
        // value, but apply the same ABI gate recursively to that value type.
        arrow_schema::DataType::Dictionary(indices, values) => {
            matches!(
                indices.as_ref(),
                arrow_schema::DataType::Int8
                    | arrow_schema::DataType::Int16
                    | arrow_schema::DataType::Int32
                    | arrow_schema::DataType::Int64
                    | arrow_schema::DataType::UInt8
                    | arrow_schema::DataType::UInt16
                    | arrow_schema::DataType::UInt32
                    | arrow_schema::DataType::UInt64
            ) && is_supported_key_type(values)
        }
        // Run-end encoding is also representation-level, but is excluded until
        // equivalence with plain arrays is frozen by vectors.
        arrow_schema::DataType::RunEndEncoded(_, _) => false,
        data_type => !data_type.is_floating() && !data_type.is_nested(),
    }
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
        for (_, slice) in bounded_slices(batch, &indices, vnode)? {
            plan.local.push(LocalRoute {
                vnode,
                batch: slice,
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
    let indices = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column, &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use arrow_array::types::{Int16Type, Int8Type};
    use arrow_array::{
        Array as _, DictionaryArray, Float64Array, Int16Array, Int64Array, Int8Array, RecordBatch,
        StringArray, StringViewArray,
    };
    use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};

    use super::*;
    use crate::state::VnodeRegistry;

    fn values(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    #[test]
    fn route_plan_uses_pinned_assignment_and_conserves_rows() {
        let registry = VnodeRegistry::single_owner(2, NodeId(1));
        let pinned = registry.versioned_snapshot();
        registry.set_assignment(Arc::from([NodeId(2), NodeId(2)]));

        let plan =
            route_checkpointed_batch(&values(&[10, 20]), &[0, 1], &pinned, NodeId(1)).unwrap();

        assert_eq!(pinned.version(), 1);
        assert_eq!(registry.assignment_version(), 2);
        assert_eq!(plan.local.len(), 2);
        assert!(plan.remote.is_empty());
        assert_eq!(
            plan.local
                .iter()
                .map(|route| route.batch.num_rows())
                .sum::<usize>(),
            2
        );
    }

    #[test]
    fn unassigned_vnode_fails_before_any_plan_is_returned() {
        let registry = VnodeRegistry::single_owner(2, NodeId(1));
        registry.set_assignment(Arc::from([NodeId(1), NodeId::UNASSIGNED]));
        let assignment = registry.versioned_snapshot();

        let error = route_checkpointed_batch(&values(&[10, 20]), &[0, 1], &assignment, NodeId(1))
            .unwrap_err();

        assert!(error.is_not_ready());
    }

    #[test]
    fn remote_routes_preserve_user_vnode_named_field() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__laminar_vnode", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["user-a", "user-b"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let registry = VnodeRegistry::single_owner(2, NodeId(2));

        let plan =
            route_checkpointed_batch(&batch, &[0, 1], &registry.versioned_snapshot(), NodeId(1))
                .unwrap();

        assert_eq!(plan.remote.len(), 1);
        assert_eq!(plan.remote[0].routed_vnodes.as_ref(), &[0, 1]);
        assert_eq!(plan.remote[0].batch.schema(), schema);
        assert_eq!(plan.remote[0].batch.num_columns(), 2);
    }

    #[test]
    fn large_owner_group_is_split_below_the_decoded_bound() {
        let payload = "x".repeat(1024);
        let rows = ROUTE_TARGET_BATCH_BYTES / payload.len() + 512;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![payload; rows]))],
        )
        .unwrap();
        let registry = VnodeRegistry::single_owner(1, NodeId(2));
        let plan = route_checkpointed_batch(
            &batch,
            &vec![0; rows],
            &registry.versioned_snapshot(),
            NodeId(1),
        )
        .unwrap();

        assert!(plan.remote.len() > 1);
        assert_eq!(
            plan.remote
                .iter()
                .map(|route| route.batch.num_rows())
                .sum::<usize>(),
            rows
        );
        assert!(plan.remote.iter().all(|route| {
            route.batch.num_rows() <= ROUTE_MAX_BATCH_ROWS
                && logical_batch_bytes(&route.batch).unwrap() <= ROUTE_MAX_BATCH_BYTES
        }));
    }

    #[test]
    fn string_view_variadic_buffers_count_toward_the_hard_bound() {
        let value = "x".repeat(129);
        let array = StringViewArray::from_iter_values(std::iter::repeat_n(
            value.as_str(),
            ROUTE_MAX_BATCH_ROWS,
        ));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8View,
                false,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap();

        assert!(logical_batch_bytes(&batch).unwrap() > ROUTE_MAX_BATCH_BYTES);
    }

    #[test]
    fn narrow_slices_of_shared_backing_use_referenced_bytes() {
        let backing = Int64Array::from(vec![1; 2_000_000]);
        let column_count = 256;
        let fields = (0..column_count)
            .map(|index| Field::new(format!("c{index}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        let columns = (0..column_count)
            .map(|index| Arc::new(backing.slice(index, 1)) as arrow_array::ArrayRef)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        assert!(batch.get_array_memory_size() > ROUTE_MAX_BATCH_BYTES);
        assert_eq!(
            logical_batch_bytes(&batch).unwrap(),
            column_count * std::mem::size_of::<i64>()
        );
    }

    #[test]
    fn mixed_routes_have_deterministic_owner_order_and_conserve_input_order() {
        let registry = VnodeRegistry::single_owner(4, NodeId(1));
        registry.set_assignment(Arc::from([NodeId(2), NodeId(1), NodeId(2), NodeId(3)]));
        let batch = values(&[10, 20, 30, 40, 50, 60]);

        let plan = route_checkpointed_batch(
            &batch,
            &[2, 0, 3, 2, 1, 0],
            &registry.versioned_snapshot(),
            NodeId(1),
        )
        .unwrap();

        assert_eq!(plan.local.len(), 1);
        assert_eq!(plan.local[0].vnode, 1);
        assert_eq!(
            plan.local[0]
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[50]
        );
        assert_eq!(
            plan.remote
                .iter()
                .map(|route| route.owner)
                .collect::<Vec<_>>(),
            vec![NodeId(2), NodeId(3)]
        );
        assert_eq!(plan.remote[0].routed_vnodes.as_ref(), &[0, 2]);
        assert_eq!(
            plan.remote[0]
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10, 20, 40, 60]
        );
    }

    #[test]
    fn single_oversized_row_is_terminal_before_a_plan_is_returned() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![
                "x".repeat(ROUTE_MAX_BATCH_BYTES + 1)
            ]))],
        )
        .unwrap();
        let registry = VnodeRegistry::single_owner(1, NodeId(2));

        let error =
            route_checkpointed_batch(&batch, &[0], &registry.versioned_snapshot(), NodeId(1))
                .unwrap_err();

        assert!(matches!(error, ShuffleRoutingError::OversizedRow { .. }));
        assert!(!error.is_not_ready());
    }

    #[test]
    fn row_hashing_rejects_invalid_dimensions_without_panicking() {
        let batch = values(&[1]);
        assert!(matches!(
            row_vnodes(&batch, &[], 1),
            Err(ShuffleRoutingError::EmptyKey)
        ));
        assert!(matches!(
            row_vnodes(&batch, &[0], 0),
            Err(ShuffleRoutingError::EmptyVnodeSpace)
        ));
        assert!(matches!(
            row_vnodes(&batch, &[1], 1),
            Err(ShuffleRoutingError::KeyColumnOutOfRange { .. })
        ));
    }

    #[test]
    fn partitioning_abi_v1_arrow_row_golden_vectors() {
        const KEY_GROUPS: u32 = 257;

        let strings = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![
                None,
                Some(""),
                Some("alpha"),
                Some("snowman-☃"),
            ]))],
        )
        .unwrap();
        let string_groups = row_vnodes(&strings, &[0], KEY_GROUPS).unwrap();

        let integers = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(-1),
                None,
                Some(0),
                Some(1),
                Some(i64::MAX),
            ]))],
        )
        .unwrap();
        let integer_groups = row_vnodes(&integers, &[0], KEY_GROUPS).unwrap();

        let composite = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant", DataType::Utf8, true),
                Field::new("account", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("a"), None, None])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(1), None])),
            ],
        )
        .unwrap();
        let composite_groups = row_vnodes(&composite, &[0, 1], KEY_GROUPS).unwrap();

        let dictionary =
            DictionaryArray::<Int8Type>::from_iter([Some("alpha"), None, Some(""), Some("alpha")]);
        let dictionary = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "key",
                dictionary.data_type().clone(),
                true,
            )])),
            vec![Arc::new(dictionary)],
        )
        .unwrap();
        let dictionary_groups = row_vnodes(&dictionary, &[0], KEY_GROUPS).unwrap();

        assert_eq!(
            (
                string_groups,
                integer_groups,
                composite_groups,
                dictionary_groups,
            ),
            (
                vec![211, 224, 44, 94],
                vec![111, 202, 114, 90, 180, 32],
                vec![26, 208, 118, 52],
                vec![44, 211, 224, 44],
            )
        );
    }

    #[test]
    fn partitioning_abi_v1_rejects_non_scalar_and_floating_point_keys() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "key",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![0.0, -0.0, f64::NAN]))],
        )
        .unwrap();

        assert!(matches!(
            row_vnodes(&batch, &[0], 257),
            Err(ShuffleRoutingError::UnsupportedKeyType {
                index: 0,
                data_type: DataType::Float64,
            })
        ));

        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let nested = [
            DataType::List(Arc::clone(&item)),
            DataType::ListView(Arc::clone(&item)),
            DataType::LargeList(Arc::clone(&item)),
            DataType::LargeListView(Arc::clone(&item)),
            DataType::FixedSizeList(Arc::clone(&item), 2),
            DataType::Struct(vec![Field::new("item", DataType::Int64, true)].into()),
            DataType::Union(
                UnionFields::try_new([0], [Field::new("item", DataType::Int64, true)]).unwrap(),
                UnionMode::Sparse,
            ),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ];
        assert!(nested
            .iter()
            .all(|data_type| !is_supported_key_type(data_type)));
        assert!(!is_supported_key_type(&DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Float64),
        )));
        assert!(!is_supported_key_type(&DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::List(item)),
        )));
        assert!(!is_supported_key_type(&DataType::Dictionary(
            Box::new(DataType::Float64),
            Box::new(DataType::Utf8),
        )));
    }

    #[test]
    fn dictionary_encoding_does_not_change_partitioning() {
        const KEY_GROUPS: u32 = 257;

        let int8 = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(1), Some(0)]),
            Arc::new(StringArray::from(vec!["alpha", ""])),
        )
        .unwrap();
        let int16 = DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(vec![Some(2), None, Some(0), Some(2)]),
            Arc::new(StringArray::from(vec!["", "unused", "alpha"])),
        )
        .unwrap();

        let int8 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "key",
                int8.data_type().clone(),
                true,
            )])),
            vec![Arc::new(int8)],
        )
        .unwrap();
        let int16 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "key",
                int16.data_type().clone(),
                true,
            )])),
            vec![Arc::new(int16)],
        )
        .unwrap();

        let int8_groups = row_vnodes(&int8, &[0], KEY_GROUPS).unwrap();
        let int16_groups = row_vnodes(&int16, &[0], KEY_GROUPS).unwrap();
        assert_eq!(int8_groups, vec![44, 211, 224, 44]);
        assert_eq!(int16_groups, int8_groups);
    }
}
