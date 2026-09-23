use std::collections::BTreeMap;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int64Array, Int8Array, ListArray, StringArray,
    StringViewArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Int64Type, Int8Type, Schema};

use super::*;

fn batch(ids: &[i64], values: &[&str]) -> RecordBatch {
    with_payload(ids, Arc::new(StringArray::from(values.to_vec())))
}

fn with_payload(ids: &[i64], payload: ArrayRef) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", payload.data_type().clone(), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(ids.to_vec())), payload],
    )
    .unwrap()
}

fn store(batch: &RecordBatch, max_rows: usize, max_bytes: usize) -> TableStore {
    let mut store = TableStore::from_config(&crate::LaminarConfig {
        reference_table_max_rows: max_rows,
        reference_table_max_bytes: max_bytes,
        ..Default::default()
    });
    store.create_table("t", batch.schema(), "id").unwrap();
    store
}

fn used(store: &TableStore, name: &str) -> usize {
    store.tables[name].rows.retained_bytes()
}

fn contents(store: &TableStore, name: &str) -> BTreeMap<i64, String> {
    let batch = store.to_record_batch(name).unwrap().unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| (ids.value(i), values.value(i).into()))
        .collect()
}

fn checkpoint(store: &TableStore) -> bytes::Bytes {
    store
        .capture_checkpoint(u64::MAX)
        .unwrap()
        .unwrap()
        .encode(u64::MAX)
        .unwrap()
        .0
}

#[test]
fn growth_rejection_preserves_all_rows_bytes_and_readiness() {
    let initial = batch(&[1, 2], &["one", "two"]);
    let mut store = store(&initial, 2, usize::MAX);
    store.upsert("t", &initial).unwrap();
    store.set_ready("t", true);
    let before = contents(&store, "t");
    let bytes = used(&store, "t");
    assert!(matches!(
        store.upsert("t", &batch(&[1, 3], &["changed", "new"])),
        Err(DbError::ReferenceTableQuotaExceeded {
            rows: 3,
            max_rows: 2,
            ..
        })
    ));
    assert_eq!(contents(&store, "t"), before);
    assert_eq!(used(&store, "t"), bytes);
    assert!(store.is_ready("t"));
}

#[test]
fn duplicate_upserts_charge_only_final_keys_and_use_last_value() {
    let input = batch(&[1, 1, 1], &["first", "second", "last"]);
    let mut store = store(&input, 1, usize::MAX);
    assert_eq!(store.upsert("t", &input).unwrap(), 3);
    assert_eq!(contents(&store, "t"), BTreeMap::from([(1, "last".into())]));
    let bytes = used(&store, "t");
    store.limits.bytes = bytes;
    store.upsert("t", &input).unwrap();
    assert_eq!(used(&store, "t"), bytes);
    let mut candidate = store.prepare_snapshot("t", &[]).unwrap();
    store
        .extend_snapshot(&mut candidate, &input.slice(0, 1))
        .unwrap();
    assert!(store
        .extend_snapshot(&mut candidate, &input.slice(1, 1))
        .is_err());
    store.install_prepared_snapshots(vec![candidate]).unwrap();
    assert_eq!(contents(&store, "t"), BTreeMap::from([(1, "first".into())]));
}

#[test]
fn wide_encoded_keys_and_payloads_cannot_exceed_byte_limit() {
    let wide = "w".repeat(65_536);
    let keys = Arc::new(StringArray::from(vec![wide.as_str()]));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let input = RecordBatch::try_new(schema, vec![keys]).unwrap();
    let mut store = store(&input, 10, usize::MAX);
    store.upsert("t", &input).unwrap();
    assert!(used(&store, "t") > input.column(0).get_buffer_memory_size() + wide.len());
    store.limits.bytes = used(&store, "t") - 1;
    assert!(matches!(
        store.upsert("t", &input),
        Err(DbError::ReferenceTableQuotaExceeded { .. })
    ));
    assert_eq!(store.table_row_count("t"), 1);
}

#[test]
fn byte_preflight_credits_replaced_storage_but_preserves_surviving_allocations() {
    let large = "x".repeat(256 * 1024);
    let original = batch(&[1, 2, 3], &[&large, &large, &large]);
    let mut store = store(&original, 3, usize::MAX);
    store.upsert("t", &original).unwrap();
    let capture = store.capture_checkpoint(u64::MAX).unwrap().unwrap();
    store.upsert("t", &batch(&[2, 3], &["b", "c"])).unwrap();
    assert!(used(&store, "t") >= original.column(1).get_buffer_memory_size());
    assert!(store
        .capture_checkpoint(u64::try_from(used(&store, "t")).unwrap() - 1)
        .is_err());
    let replacement = batch(&[1], &["a"]);
    store.upsert("t", &replacement).unwrap();
    assert!(used(&store, "t") < 16 * 1024);
    let bytes = used(&store, "t");
    store.limits.bytes = bytes;
    assert!(matches!(
        store.upsert("t", &original),
        Err(DbError::ReferenceTableQuotaExceeded { .. })
    ));
    assert_eq!(used(&store, "t"), bytes);
    assert_eq!(contents(&store, "t")[&1], "a");
    // A pinned checkpoint owns its older buffers independently of the live quota.
    let mut recovered = self::store(&original, 3, usize::MAX);
    recovered
        .restore_checkpoint(&capture.encode(u64::MAX).unwrap().0)
        .unwrap();
    assert_eq!(contents(&recovered, "t")[&1], large);
    let empty = store.prepare_snapshot("t", &[]).unwrap();
    store.install_prepared_snapshots(vec![empty]).unwrap();
    assert_eq!(used(&store, "t"), 0);
    assert_eq!(store.table_row_count("t"), 0);
}

#[test]
fn replacement_at_exact_byte_limit_credits_the_retired_allocation() {
    let original = batch(&[1], &[&"x".repeat(4096)]);
    let mut store = store(&original, 1, usize::MAX);
    store.upsert("t", &original).unwrap();
    store.limits.bytes = used(&store, "t");
    let replacement = batch(&[1], &[&"y".repeat(4096)]);
    assert_ne!(
        original.column(1).to_data().buffers()[1].data_ptr(),
        replacement.column(1).to_data().buffers()[1].data_ptr()
    );
    store.upsert("t", &replacement).unwrap();
    assert_eq!(used(&store, "t"), store.limits.bytes);
    assert_eq!(contents(&store, "t")[&1], "y".repeat(4096));
}

#[test]
fn shared_buffers_are_charged_once_across_columns_and_admitted_batches() {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(4096); 2]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("left", DataType::Utf8, false),
        Field::new("right", DataType::Utf8, false),
    ]));
    let shared = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            values.clone(),
            values.clone(),
        ],
    )
    .unwrap();
    let separate = RecordBatch::try_new(
        schema,
        vec![
            shared.column(0).clone(),
            values.clone(),
            Arc::new(StringArray::from(vec!["x".repeat(4096); 2])),
        ],
    )
    .unwrap();
    let mut together = store(&shared, 2, usize::MAX);
    together.upsert("t", &shared).unwrap();
    let mut split = store(&shared, 2, usize::MAX);
    split.upsert("t", &shared.slice(0, 1)).unwrap();
    split.upsert("t", &shared.slice(1, 1)).unwrap();
    assert_eq!(used(&together, "t"), used(&split, "t"));
    let mut unshared = store(&separate, 2, usize::MAX);
    unshared.upsert("t", &separate).unwrap();
    assert!(used(&unshared, "t") >= used(&together, "t") + values.get_buffer_memory_size());
}

#[test]
fn spare_arrow_capacity_is_retained_by_one_slice() {
    let mut ids = Vec::<i64>::with_capacity(32_768);
    ids.push(1);
    let ids: ArrayRef = Arc::new(Int64Array::from(ids));
    let capacity = ids.get_buffer_memory_size();
    let input = with_payload(&[1], ids);
    assert!(capacity >= 256 * 1024);
    let mut store = store(&input, 1, capacity - 1);
    assert!(matches!(
        store.upsert("t", &input),
        Err(DbError::ReferenceTableQuotaExceeded { .. })
    ));
    assert_eq!(used(&store, "t"), 0);
}

#[test]
fn nested_dictionary_and_view_payloads_retain_backing_storage() {
    let large = "x".repeat(65_536);
    let string: ArrayRef = Arc::new(StringArray::from(vec![large.as_str(), "small"]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::new(
            vec![1_i64, 2].into(),
            Some(arrow::buffer::NullBuffer::new(
                arrow::buffer::BooleanBuffer::new(
                    arrow::buffer::Buffer::from_vec(vec![1_u8; 65_536]),
                    0,
                    2,
                ),
            )),
        )),
        string.clone(),
        Arc::new(StringViewArray::from(vec![large.as_str(), "small"])),
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 1]), string.clone())
                .unwrap(),
        ),
        Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("value", DataType::Utf8, true)),
            string,
        )])),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(7); 8192]),
            Some(vec![Some(1)]),
        ])),
    ];
    for array in arrays {
        let capacity = array.get_buffer_memory_size();
        let input = with_payload(&[1, 2], array).slice(1, 1);
        let mut store = store(&input, 1, usize::MAX);
        store.upsert("t", &input).unwrap();
        let bytes = used(&store, "t");
        assert!(bytes >= capacity, "{:?}", input.schema());
        store.limits.bytes = bytes - 1;
        assert!(matches!(
            store.prepare_snapshot("t", &[input]),
            Err(DbError::ReferenceTableQuotaExceeded { .. })
        ));
        assert_eq!(used(&store, "t"), bytes);
    }
}

#[test]
fn external_allocation_extent_is_charged_even_for_an_empty_slice() {
    let original = batch(&[1], &["old"]);
    let mut store = store(&original, 10, 4095);
    store.upsert("t", &original).unwrap();
    let external = arrow::buffer::Buffer::from(bytes::Bytes::from(vec![b'x'; 4096]));
    assert_eq!(external.capacity(), 4096);
    for length in [0_i32, 1] {
        let length_usize = usize::try_from(length).unwrap();
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0_i32, length].into());
        let payload = StringArray::new(offsets, external.slice_with_length(0, length_usize), None);
        let input = with_payload(&[1], Arc::new(payload));
        assert!(matches!(
            store.upsert("t", &input),
            Err(DbError::ReferenceTableQuotaExceeded { .. })
        ));
        assert_eq!(contents(&store, "t")[&1], "old");
    }
}

#[test]
fn reduced_install_limit_rejects_the_entire_prepared_inventory() {
    let initial = batch(&[1], &["old"]);
    let mut store = store(&initial, 10, usize::MAX);
    store.create_table("u", initial.schema(), "id").unwrap();
    for name in ["t", "u"] {
        store.upsert(name, &initial).unwrap();
    }
    store.set_ready("t", true);
    let snapshots = vec![
        store
            .prepare_snapshot("t", &[batch(&[2], &["new"])])
            .unwrap(),
        store
            .prepare_snapshot("u", &[batch(&[3, 4], &["three", "four"])])
            .unwrap(),
    ];
    store.limits.rows = 1;
    assert!(matches!(
        store.install_prepared_snapshots(snapshots),
        Err(DbError::ReferenceTableQuotaExceeded { .. })
    ));
    for name in ["t", "u"] {
        assert_eq!(contents(&store, name)[&1], "old");
    }
    assert!(store.is_ready("t"));
    assert!(!store.is_ready("u"));
}

#[test]
fn restore_row_and_byte_limits_preserve_complete_live_inventory() {
    let initial = batch(&[1], &["old"]);
    let mut source = store(&initial, 10, usize::MAX);
    source.create_table("u", initial.schema(), "id").unwrap();
    source.upsert("t", &batch(&[2], &["new"])).unwrap();
    source
        .upsert("u", &batch(&[3, 4], &[&"x".repeat(65_536), "four"]))
        .unwrap();
    let encoded = checkpoint(&source);
    for (rows, bytes) in [(1, usize::MAX), (10, 8192)] {
        let mut target = store(&initial, rows, bytes);
        target.create_table("u", initial.schema(), "id").unwrap();
        for name in ["t", "u"] {
            target.upsert(name, &initial).unwrap();
        }
        target.set_ready("t", true);
        let before = [used(&target, "t"), used(&target, "u")];
        assert!(matches!(
            target.restore_checkpoint(&encoded),
            Err(DbError::ReferenceTableQuotaExceeded { .. })
        ));
        for name in ["t", "u"] {
            assert_eq!(contents(&target, name)[&1], "old");
        }
        assert_eq!([used(&target, "t"), used(&target, "u")], before);
        assert!(target.is_ready("t"));
        assert!(!target.is_ready("u"));
    }
}

#[test]
fn repeated_updates_match_an_independent_ordered_map_and_release_on_refresh() {
    let initial = batch(&[], &[]);
    let mut store = store(&initial, 32, 48 * 1024);
    let mut model = BTreeMap::new();
    let mut random = 9_u64;
    for step in 0..160 {
        random = random
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let id = i64::try_from((random >> 32) % 40).unwrap();
        let value = if step % 11 == 0 {
            "x".repeat(65_536)
        } else {
            step.to_string()
        };
        let before = used(&store, "t");
        match store.upsert("t", &batch(&[id, id], &["overwritten", &value])) {
            Ok(_) => {
                model.insert(id, value);
            }
            Err(DbError::ReferenceTableQuotaExceeded { .. }) => {
                assert_eq!(used(&store, "t"), before);
            }
            Err(error) => panic!("unexpected error: {error}"),
        }
        assert_eq!(contents(&store, "t"), model);
        assert!(used(&store, "t") <= store.limits.bytes);
    }
    let snapshot = store.prepare_snapshot("t", &[]).unwrap();
    store.install_prepared_snapshots(vec![snapshot]).unwrap();
    assert_eq!(used(&store, "t"), 0);
    assert!(store.drop_table("t"));
}
