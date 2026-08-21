use std::sync::Arc;

use super::*;
use arrow::array::{Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, true),
    ]))
}

fn make_batch(ids: &[i32], names: &[&str], prices: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Float64Array::from(prices.to_vec())),
        ],
    )
    .unwrap()
}

fn checkpoint_bytes(store: &TableStore) -> bytes::Bytes {
    store
        .capture_checkpoint(u64::MAX)
        .unwrap()
        .expect("non-empty table inventory")
        .encode(u64::MAX)
        .unwrap()
        .0
}

fn rewrite_archive(
    bytes: &[u8],
    mutate: impl FnOnce(&mut ReferenceTableCheckpointArchive),
) -> Vec<u8> {
    let mut archive =
        rkyv::from_bytes::<ReferenceTableCheckpointArchive, rkyv::rancor::Error>(bytes).unwrap();
    mutate(&mut archive);
    rkyv::to_bytes::<rkyv::rancor::Error>(&archive)
        .unwrap()
        .to_vec()
}

#[test]
fn test_create_table_validates_pk() {
    let mut store = TableStore::new();
    let result = store.create_table("t", test_schema(), "id");
    assert!(result.is_ok());
    assert!(store.has_table("t"));
}

#[test]
fn test_create_table_rejects_missing_pk() {
    let mut store = TableStore::new();
    let result = store.create_table("t", test_schema(), "nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_create_table_rejects_nullable_pk() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let mut store = TableStore::new();
    let result = store.create_table("t", schema, "id");
    assert!(matches!(result, Err(DbError::InvalidOperation(_))));
    assert!(!store.has_table("t"));
}

#[test]
fn test_create_table_rejects_duplicate() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    let result = store.create_table("t", test_schema(), "id");
    assert!(matches!(result, Err(DbError::TableAlreadyExists(_))));
}

#[test]
fn test_upsert_and_scan() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();

    let batch = make_batch(&[1], &["Widget"], &[9.99]);
    let count = store.upsert("t", &batch).unwrap();
    assert_eq!(count, 1);
    assert_eq!(store.table_row_count("t"), 1);

    let row = store.to_record_batch("t").unwrap().unwrap();
    assert_eq!(row.num_rows(), 1);
    let names = row
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Widget");
}

#[test]
fn test_upsert_multiple_rows() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();

    let batch = make_batch(&[1, 2, 3], &["A", "B", "C"], &[1.0, 2.0, 3.0]);
    let count = store.upsert("t", &batch).unwrap();
    assert_eq!(count, 3);
    assert_eq!(store.table_row_count("t"), 3);
}

#[test]
fn test_upsert_schema_mismatch_is_atomic() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    store
        .upsert("t", &make_batch(&[1], &["original"], &[1.0]))
        .unwrap();

    let incompatible_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let incompatible = RecordBatch::try_new(
        incompatible_schema,
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["bad"])),
        ],
    )
    .unwrap();
    assert!(matches!(
        store.upsert("t", &incompatible),
        Err(DbError::SchemaMismatch(_))
    ));

    let snapshot = store.to_record_batch("t").unwrap().unwrap();
    assert_eq!(snapshot.num_rows(), 1);
    assert_eq!(store.table_row_count("t"), 1);
}

#[test]
fn upsert_rejects_null_in_non_nullable_column_without_mutation() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    store
        .upsert("t", &make_batch(&[1], &["original"], &[1.0]))
        .unwrap();
    // SAFETY: field count, data types, and array lengths match. The deliberately invalid
    // nullability contract models a corrupted or foreign Arrow producer.
    let invalid = unsafe {
        RecordBatch::new_unchecked(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(vec![2, 3])),
                Arc::new(StringArray::from(vec![Some("valid"), None])),
                Arc::new(Float64Array::from(vec![2.0, 3.0])),
            ],
            2,
        )
    };

    let error = store.upsert("t", &invalid).unwrap_err();
    assert!(error.to_string().contains("non-nullable column 'name'"));
    assert_eq!(store.table_row_count("t"), 1);
    let snapshot = store.to_record_batch("t").unwrap().unwrap();
    let names = snapshot
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "original");
}

#[test]
fn test_upsert_overwrites_existing() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();

    let batch1 = make_batch(&[1], &["Old"], &[1.0]);
    store.upsert("t", &batch1).unwrap();

    let batch2 = make_batch(&[1], &["New"], &[2.0]);
    store.upsert("t", &batch2).unwrap();

    assert_eq!(store.table_row_count("t"), 1);
    let row = store.to_record_batch("t").unwrap().unwrap();
    let names = row
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "New");
}

#[test]
fn test_table_names_and_counts() {
    let mut store = TableStore::new();
    assert!(store.table_names().is_empty());

    store.create_table("a", test_schema(), "id").unwrap();
    store.create_table("b", test_schema(), "id").unwrap();

    let mut names = store.table_names();
    names.sort();
    assert_eq!(names, vec!["a", "b"]);
    assert!(store.has_table("a"));
    assert!(!store.has_table("c"));
}

#[test]
fn test_to_record_batch() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();

    // Empty table returns empty batch
    let batch = store.to_record_batch("t").unwrap().unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema(), test_schema());

    // With data
    store
        .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
        .unwrap();
    let batch = store.to_record_batch("t").unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);

    // Missing table
    assert!(store.to_record_batch("nosuch").unwrap().is_none());
}

#[test]
fn test_drop_table() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    assert!(store.drop_table("t"));
    assert!(!store.has_table("t"));
    assert!(!store.drop_table("t"));
}

#[test]
fn test_ready_flag() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    assert!(!store.is_ready("t"));

    store.set_ready("t", true);
    assert!(store.is_ready("t"));

    store.set_ready("t", false);
    assert!(!store.is_ready("t"));
}

#[test]
fn test_connector_tracking() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    assert!(store.connector("t").is_none());

    store.set_connector("t", "kafka");
    assert_eq!(store.connector("t"), Some("kafka"));
}

#[test]
fn test_row_count_tracks_upserts() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    assert_eq!(store.table_row_count("t"), 0);

    store
        .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
        .unwrap();
    assert_eq!(store.table_row_count("t"), 2);

    // Upsert existing key — count should not increase
    store
        .upsert("t", &make_batch(&[1], &["X"], &[9.0]))
        .unwrap();
    assert_eq!(store.table_row_count("t"), 2);
}

#[test]
fn prepared_snapshot_rejects_duplicate_keys_across_batches_without_mutation() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    store
        .upsert("t", &make_batch(&[9], &["stale"], &[9.0]))
        .unwrap();
    let before = checkpoint_bytes(&store);
    let batches = vec![
        make_batch(&[1], &["first"], &[1.0]),
        make_batch(&[1], &["duplicate"], &[2.0]),
    ];

    let error = store
        .prepare_snapshot("t", &batches)
        .err()
        .expect("duplicate keys must reject the prepared snapshot");

    assert!(
        error.to_string().contains("duplicate primary keys"),
        "{error}"
    );
    assert_eq!(checkpoint_bytes(&store), before);
    assert!(!store.is_ready("t"));
}

#[test]
fn one_invalid_prepared_table_prevents_every_replacement() {
    let mut target = TableStore::new();
    target.create_table("a", test_schema(), "id").unwrap();
    target.create_table("b", test_schema(), "id").unwrap();
    target
        .upsert("a", &make_batch(&[10], &["old-a"], &[10.0]))
        .unwrap();
    target
        .upsert("b", &make_batch(&[20], &["old-b"], &[20.0]))
        .unwrap();
    let prepared = vec![
        target
            .prepare_snapshot("a", &[make_batch(&[1], &["new-a"], &[1.0])])
            .unwrap(),
        target
            .prepare_snapshot("b", &[make_batch(&[2], &["new-b"], &[2.0])])
            .unwrap(),
    ];

    let incompatible_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("description", DataType::Utf8, false),
    ]));
    assert!(target.drop_table("b"));
    target
        .create_table("b", incompatible_schema.clone(), "id")
        .unwrap();
    let old_b = RecordBatch::try_new(
        incompatible_schema,
        vec![
            Arc::new(Int32Array::from(vec![20])),
            Arc::new(StringArray::from(vec!["old-b"])),
        ],
    )
    .unwrap();
    target.upsert("b", &old_b).unwrap();
    let before = checkpoint_bytes(&target);

    let error = target.install_prepared_snapshots(prepared).unwrap_err();

    assert!(matches!(error, DbError::SchemaMismatch(_)));
    assert_eq!(checkpoint_bytes(&target), before);
    assert!(!target.is_ready("a"));
    assert!(!target.is_ready("b"));
}

#[test]
fn empty_prepared_snapshot_clears_stale_rows_and_marks_ready() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    store
        .upsert("t", &make_batch(&[9], &["stale"], &[9.0]))
        .unwrap();

    let prepared = store.prepare_snapshot("t", &[]).unwrap();
    store.install_prepared_snapshots(vec![prepared]).unwrap();

    assert_eq!(store.table_row_count("t"), 0);
    assert_eq!(store.to_record_batch("t").unwrap().unwrap().num_rows(), 0);
    assert!(store.is_ready("t"));
}

#[test]
fn successful_multi_table_snapshot_install_is_atomic_and_ready() {
    let mut store = TableStore::new();
    store.create_table("a", test_schema(), "id").unwrap();
    store.create_table("b", test_schema(), "id").unwrap();
    let prepared = vec![
        store
            .prepare_snapshot(
                "a",
                &[
                    make_batch(&[1], &["a-1"], &[1.0]),
                    make_batch(&[2], &["a-2"], &[2.0]),
                ],
            )
            .unwrap(),
        store
            .prepare_snapshot("b", &[make_batch(&[3], &["b-3"], &[3.0])])
            .unwrap(),
    ];

    store.install_prepared_snapshots(prepared).unwrap();

    assert_eq!(store.table_row_count("a"), 2);
    assert_eq!(store.table_row_count("b"), 1);
    assert!(store.is_ready("a"));
    assert!(store.is_ready("b"));
}

#[test]
fn duplicate_prepared_table_names_fail_without_mutation() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    store
        .upsert("t", &make_batch(&[9], &["stale"], &[9.0]))
        .unwrap();
    let before = checkpoint_bytes(&store);
    let prepared = vec![
        store
            .prepare_snapshot("t", &[make_batch(&[1], &["first"], &[1.0])])
            .unwrap(),
        store
            .prepare_snapshot("t", &[make_batch(&[2], &["second"], &[2.0])])
            .unwrap(),
    ];

    let error = store.install_prepared_snapshots(prepared).unwrap_err();

    assert!(error.to_string().contains("duplicated"), "{error}");
    assert_eq!(checkpoint_bytes(&store), before);
    assert!(!store.is_ready("t"));
}

#[test]
fn checkpoint_round_trip_is_deterministic_and_marks_complete_inventory_ready() {
    let mut source = TableStore::new();
    source.create_table("b", test_schema(), "id").unwrap();
    source.create_table("a", test_schema(), "id").unwrap();
    source
        .upsert("a", &make_batch(&[2, 1], &["A2", "A1"], &[2.0, 1.0]))
        .unwrap();
    source
        .upsert("b", &make_batch(&[4, 3], &["B4", "B3"], &[4.0, 3.0]))
        .unwrap();

    let first = checkpoint_bytes(&source);
    let second = checkpoint_bytes(&source);
    assert_eq!(first, second, "checkpoint bytes must be canonical");

    let mut restored = TableStore::new();
    restored.create_table("a", test_schema(), "id").unwrap();
    restored.create_table("b", test_schema(), "id").unwrap();
    restored
        .upsert("a", &make_batch(&[99], &["stale"], &[99.0]))
        .unwrap();
    restored
        .upsert("b", &make_batch(&[98], &["stale"], &[98.0]))
        .unwrap();

    assert!(restored.restore_checkpoint(&first).unwrap());
    assert_eq!(restored.table_row_count("a"), 2);
    assert_eq!(restored.table_row_count("b"), 2);
    assert!(restored.is_ready("a"));
    assert!(restored.is_ready("b"));
    assert_eq!(checkpoint_bytes(&restored), first);
}

#[test]
fn checkpoint_capture_cap_rejection_preserves_live_state() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    store
        .upsert("t", &make_batch(&[1, 2], &["first", "second"], &[1.0, 2.0]))
        .unwrap();
    let before = checkpoint_bytes(&store);
    let estimated_bytes = store.checkpoint_capture_estimated_bytes().unwrap();
    assert!(estimated_bytes > 0);

    let error = store
        .capture_checkpoint(estimated_bytes - 1)
        .err()
        .expect("capture above the remaining checkpoint budget must fail");

    assert!(error.to_string().contains("staged-state budget"), "{error}");
    assert_eq!(store.table_row_count("t"), 2);
    assert_eq!(checkpoint_bytes(&store), before);
}

#[test]
fn checkpoint_encoding_enforces_the_worker_remaining_budget() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    store
        .upsert("t", &make_batch(&[1], &["value"], &[1.0]))
        .unwrap();
    let before = checkpoint_bytes(&store);
    let capture = store
        .capture_checkpoint(u64::MAX)
        .unwrap()
        .expect("non-empty table inventory");
    assert_eq!(
        capture.estimated_bytes(),
        store.checkpoint_capture_estimated_bytes().unwrap()
    );

    let error = capture
        .encode(u64::try_from(before.len()).unwrap() - 1)
        .unwrap_err();

    assert!(error.to_string().contains("staged-state budget"), "{error}");
    assert_eq!(store.table_row_count("t"), 1);
    assert_eq!(checkpoint_bytes(&store), before);
}

#[test]
fn checkpoint_capture_is_a_point_in_time_image() {
    let mut store = TableStore::new();
    store.create_table("t", test_schema(), "id").unwrap();
    store
        .upsert("t", &make_batch(&[1], &["old"], &[1.0]))
        .unwrap();
    let capture = store
        .capture_checkpoint(u64::MAX)
        .unwrap()
        .expect("non-empty table inventory");

    store
        .upsert("t", &make_batch(&[1], &["new"], &[2.0]))
        .unwrap();
    let (encoded, retained_bytes) = capture.encode(u64::MAX).unwrap();
    assert!(retained_bytes >= encoded.len() as u64);
    let mut restored = TableStore::new();
    restored.create_table("t", test_schema(), "id").unwrap();
    restored.restore_checkpoint(&encoded).unwrap();

    let restored_batch = restored.to_record_batch("t").unwrap().unwrap();
    let restored_names = restored_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(restored_names.value(0), "old");
    let live_batch = store.to_record_batch("t").unwrap().unwrap();
    let live_names = live_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(live_names.value(0), "new");
}

#[test]
fn checkpoint_chunks_large_tables_and_restores_every_row() {
    let row_count = REFERENCE_TABLE_CHECKPOINT_CHUNK_ROWS + 904;
    let ids: Vec<i32> = (0..i32::try_from(row_count).unwrap()).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let prices: Vec<f64> = ids.iter().map(|id| f64::from(*id)).collect();

    let mut source = TableStore::new();
    source.create_table("t", test_schema(), "id").unwrap();
    source
        .upsert("t", &make_batch(&ids, &name_refs, &prices))
        .unwrap();
    let checkpoint = checkpoint_bytes(&source);

    let archive =
        rkyv::from_bytes::<ReferenceTableCheckpointArchive, rkyv::rancor::Error>(&checkpoint)
            .unwrap();
    let batches = decode_checkpoint_batches("t", &archive.tables[0].ipc, row_count).unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows(), REFERENCE_TABLE_CHECKPOINT_CHUNK_ROWS);
    assert_eq!(batches[1].num_rows(), 904);

    let mut restored = TableStore::new();
    restored.create_table("t", test_schema(), "id").unwrap();
    assert!(restored.restore_checkpoint(&checkpoint).unwrap());
    assert_eq!(restored.table_row_count("t"), row_count);
    assert_eq!(checkpoint_bytes(&restored), checkpoint);
}

#[test]
fn empty_inventory_emits_no_checkpoint_state() {
    assert!(TableStore::new().capture_checkpoint(0).unwrap().is_none());
}

#[test]
fn restore_accepts_an_unaligned_checkpoint_buffer() {
    let mut source = TableStore::new();
    source.create_table("a", test_schema(), "id").unwrap();
    source
        .upsert("a", &make_batch(&[1], &["value"], &[1.0]))
        .unwrap();
    let checkpoint = checkpoint_bytes(&source);

    const ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
    let mut transport = vec![0_u8; checkpoint.len() + ALIGNMENT];
    let offset = (0..ALIGNMENT)
        .find(|offset| !(transport.as_ptr() as usize + offset).is_multiple_of(ALIGNMENT))
        .unwrap();
    transport[offset..offset + checkpoint.len()].copy_from_slice(&checkpoint);
    let unaligned = &transport[offset..offset + checkpoint.len()];
    assert_ne!(unaligned.as_ptr().align_offset(ALIGNMENT), 0);

    let mut restored = TableStore::new();
    restored.create_table("a", test_schema(), "id").unwrap();
    assert!(restored.restore_checkpoint(unaligned).unwrap());
    assert_eq!(restored.table_row_count("a"), 1);
}

#[test]
fn restore_rejects_corrupt_or_partial_inventory_without_mutation() {
    let mut source = TableStore::new();
    source.create_table("a", test_schema(), "id").unwrap();
    source.create_table("b", test_schema(), "id").unwrap();
    source
        .upsert("a", &make_batch(&[1], &["new-a"], &[1.0]))
        .unwrap();
    source
        .upsert("b", &make_batch(&[2], &["new-b"], &[2.0]))
        .unwrap();
    let valid = checkpoint_bytes(&source);

    let mut target = TableStore::new();
    target.create_table("a", test_schema(), "id").unwrap();
    target.create_table("b", test_schema(), "id").unwrap();
    target
        .upsert("a", &make_batch(&[10], &["old-a"], &[10.0]))
        .unwrap();
    target
        .upsert("b", &make_batch(&[20], &["old-b"], &[20.0]))
        .unwrap();
    let before = checkpoint_bytes(&target);

    let partial = rewrite_archive(&valid, |archive| {
        archive.tables.pop();
    });
    assert!(target.restore_checkpoint(&partial).is_err());
    assert_eq!(checkpoint_bytes(&target), before);

    let wrong_version = rewrite_archive(&valid, |archive| {
        archive.version += 1;
    });
    assert!(target.restore_checkpoint(&wrong_version).is_err());
    assert_eq!(checkpoint_bytes(&target), before);

    assert!(target.restore_checkpoint(b"not an rkyv archive").is_err());
    assert_eq!(checkpoint_bytes(&target), before);
}

#[test]
fn restore_prepares_every_table_before_installing_any() {
    let mut source = TableStore::new();
    source.create_table("a", test_schema(), "id").unwrap();
    source.create_table("b", test_schema(), "id").unwrap();
    source
        .upsert("a", &make_batch(&[1], &["new-a"], &[1.0]))
        .unwrap();
    source
        .upsert("b", &make_batch(&[2], &["new-b"], &[2.0]))
        .unwrap();
    let valid = checkpoint_bytes(&source);

    let incompatible_schema = Arc::new(Schema::new(vec![Field::new(
        "wrong",
        DataType::Int32,
        false,
    )]));
    let incompatible_batch = RecordBatch::try_new(
        incompatible_schema,
        vec![Arc::new(Int32Array::from(vec![2]))],
    )
    .unwrap();
    let incompatible_ipc =
        laminar_core::serialization::serialize_batch_stream(&incompatible_batch).unwrap();
    let invalid = rewrite_archive(&valid, |archive| {
        let second = archive.tables.get_mut(1).unwrap();
        second.ipc = incompatible_ipc;
        second.row_count = 1;
    });

    let mut target = TableStore::new();
    target.create_table("a", test_schema(), "id").unwrap();
    target.create_table("b", test_schema(), "id").unwrap();
    target
        .upsert("a", &make_batch(&[10], &["old-a"], &[10.0]))
        .unwrap();
    target
        .upsert("b", &make_batch(&[20], &["old-b"], &[20.0]))
        .unwrap();
    let before = checkpoint_bytes(&target);

    let error = target.restore_checkpoint(&invalid).unwrap_err();
    assert!(error.to_string().contains("schema differs"), "{error}");
    assert_eq!(checkpoint_bytes(&target), before);
}

#[test]
fn restore_rejects_duplicate_primary_keys() {
    let mut source = TableStore::new();
    source.create_table("a", test_schema(), "id").unwrap();
    source
        .upsert("a", &make_batch(&[1], &["valid"], &[1.0]))
        .unwrap();
    let valid = checkpoint_bytes(&source);
    let duplicate_batch = make_batch(&[7, 7], &["first", "second"], &[1.0, 2.0]);
    let duplicate_ipc =
        laminar_core::serialization::serialize_batch_stream(&duplicate_batch).unwrap();
    let invalid = rewrite_archive(&valid, |archive| {
        archive.tables[0].ipc = duplicate_ipc;
        archive.tables[0].row_count = 2;
    });

    let mut target = TableStore::new();
    target.create_table("a", test_schema(), "id").unwrap();
    target
        .upsert("a", &make_batch(&[10], &["old"], &[10.0]))
        .unwrap();
    let before = checkpoint_bytes(&target);
    let error = target.restore_checkpoint(&invalid).unwrap_err();
    assert!(
        error.to_string().contains("duplicate primary keys"),
        "{error}"
    );
    assert_eq!(checkpoint_bytes(&target), before);
}

#[test]
fn restore_rejects_null_in_non_nullable_column_without_mutation() {
    let mut source = TableStore::new();
    source.create_table("a", test_schema(), "id").unwrap();
    source
        .upsert("a", &make_batch(&[1], &["valid"], &[1.0]))
        .unwrap();
    let valid = checkpoint_bytes(&source);
    // SAFETY: field count, data types, and array lengths match. Nullability is violated
    // intentionally to exercise checkpoint corruption handling.
    let null_batch = unsafe {
        RecordBatch::new_unchecked(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Float64Array::from(vec![7.0])),
            ],
            1,
        )
    };
    let null_ipc = laminar_core::serialization::serialize_batch_stream(&null_batch).unwrap();
    let invalid = rewrite_archive(&valid, |archive| {
        archive.tables[0].ipc = null_ipc;
        archive.tables[0].row_count = 1;
    });

    let mut target = TableStore::new();
    target.create_table("a", test_schema(), "id").unwrap();
    target
        .upsert("a", &make_batch(&[10], &["old"], &[10.0]))
        .unwrap();
    let before = checkpoint_bytes(&target);
    let error = target.restore_checkpoint(&invalid).unwrap_err();
    assert!(
        error.to_string().contains("non-nullable") && error.to_string().contains("name"),
        "{error}"
    );
    assert_eq!(checkpoint_bytes(&target), before);
}
