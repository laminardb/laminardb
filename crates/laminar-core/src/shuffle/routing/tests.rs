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
fn take_rows_reuses_bounded_identity_and_compacts_retained_backing() {
    let batch = values(&[10, 20, 30, 40]);
    let identity = take_rows(&batch, &[0, 1, 2, 3]).unwrap();
    let contiguous = take_rows(&batch, &[1, 2]).unwrap();

    let backing = Int64Array::from(vec![1; 2_000_000]);
    let sliced = RecordBatch::try_new(
        batch.schema(),
        vec![Arc::new(backing.slice(1, 1)) as ArrayRef],
    )
    .unwrap();
    assert!(sliced.get_array_memory_size() > ROUTE_MAX_BATCH_BYTES);
    let compacted_identity = take_rows(&sliced, &[0]).unwrap();

    let original = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let identity_values = identity
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let contiguous_values = contiguous
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sliced_values = sliced
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let compacted_values = compacted_identity
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(identity_values.values(), &[10, 20, 30, 40]);
    assert_eq!(contiguous_values.values(), &[20, 30]);
    assert!(Arc::ptr_eq(batch.column(0), identity.column(0)));
    assert_eq!(
        identity_values.values().as_ptr(),
        original.values().as_ptr()
    );
    assert_ne!(
        contiguous_values.values().as_ptr(),
        original.values()[1..].as_ptr()
    );
    assert_ne!(
        compacted_values.values().as_ptr(),
        sliced_values.values().as_ptr()
    );
    assert!(compacted_identity.get_array_memory_size() <= ROUTE_MAX_BATCH_BYTES);
}

#[test]
fn route_plan_uses_pinned_assignment_and_conserves_rows() {
    let registry = VnodeRegistry::single_owner(2, NodeId(1));
    let pinned = registry.versioned_snapshot();
    registry.set_assignment(Arc::from([NodeId(2), NodeId(2)]));

    let plan = route_checkpointed_batch(&values(&[10, 20]), &[0, 1], &pinned, NodeId(1)).unwrap();

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

    let error =
        route_checkpointed_batch(&values(&[10, 20]), &[0, 1], &assignment, NodeId(1)).unwrap_err();

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

    let plan = route_checkpointed_batch(&batch, &[0, 1], &registry.versioned_snapshot(), NodeId(1))
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
    assert_eq!(plan.local[0].source_rows.as_ref(), &[4]);
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

    let error = route_checkpointed_batch(&batch, &[0], &registry.versioned_snapshot(), NodeId(1))
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
    let over_wide = vec![0; MAX_PARTITION_KEY_COLUMNS + 1];
    assert!(matches!(
        row_vnodes(&batch, &over_wide, 1),
        Err(ShuffleRoutingError::PartitionKeyContract(
            PartitionKeyCodecError::TooManyKeyColumns { .. }
        ))
    ));
}

#[test]
fn partitioning_abi_v2_arrow_row_golden_vectors() {
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
fn partitioning_abi_v2_rejects_non_scalar_and_floating_point_keys() {
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

    assert!(matches!(
        row_vnodes(&batch, &[0, 1], 257),
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
        .all(|data_type| PartitionKeyCodecV1::try_new([data_type.clone()]).is_err()));
    assert!(PartitionKeyCodecV1::try_new([DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::Float64),
    )])
    .is_err());
    assert!(PartitionKeyCodecV1::try_new([DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::List(item)),
    )])
    .is_err());
    assert!(PartitionKeyCodecV1::try_new([DataType::Dictionary(
        Box::new(DataType::Float64),
        Box::new(DataType::Utf8),
    )])
    .is_err());
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
