use super::*;
use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};

fn batch(ids: &[i64], width: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec!["x".repeat(width); ids.len()])),
        ],
    )
    .unwrap()
}

fn input(batch: &RecordBatch, mode: &MvStorageMode, weight: i64) -> RecordBatch {
    if matches!(
        mode,
        MvStorageMode::Aggregate | MvStorageMode::Append { .. }
    ) {
        return batch.clone();
    }
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![weight; batch.num_rows()])));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn modes() -> [MvStorageMode; 4] {
    [
        MvStorageMode::Aggregate,
        MvStorageMode::append_default(),
        MvStorageMode::Upsert { key_cols: vec![0] },
        MvStorageMode::Multiset,
    ]
}

#[test]
fn schema_mismatches_are_rejected_before_any_view_changes() {
    let original = batch(&[1], 1);
    let malformed = [
        RecordBatch::try_from_iter(vec![(
            "wrong",
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        )])
        .unwrap(),
        RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        )])
        .unwrap(),
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
            ("value", Arc::new(Int64Array::from(vec![3])) as ArrayRef),
        ])
        .unwrap(),
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![None])) as ArrayRef),
            ("value", Arc::new(StringArray::from(vec!["x"])) as ArrayRef),
        ])
        .unwrap(),
        RecordBatch::new_empty(Arc::new(Schema::empty())),
    ];
    for mode in modes()
        .into_iter()
        .chain([MvStorageMode::Upsert { key_cols: vec![1] }])
    {
        let mut store = store(64, 64 * 1024);
        store
            .create_mv("first", original.schema(), MvStorageMode::Aggregate)
            .unwrap();
        store
            .create_mv("second", original.schema(), mode.clone())
            .unwrap();
        store
            .update_cycle("first", std::slice::from_ref(&original))
            .unwrap();
        store
            .update_cycle("second", &[input(&original, &mode, 1)])
            .unwrap();
        let bytes = store.total_bytes();
        for invalid in &malformed {
            let first = [batch(&[7], 1)];
            let second = [input(invalid, &mode, 1)];
            assert!(matches!(
                store.update_views([("first", first.as_slice()), ("second", second.as_slice())]),
                Err(DbError::MaterializedView(_))
            ));
            assert_eq!(ids(&store, "first"), [1]);
            assert_eq!(ids(&store, "second"), [1]);
            assert_eq!(store.total_bytes(), bytes);
        }
    }
}

#[test]
fn upsert_keys_follow_plain_columns_when_weight_is_first() {
    let mode = MvStorageMode::Upsert { key_cols: vec![0] };
    let data = batch(&[2, 3], 1);
    let weighted = input(&data, &mode, 1);
    let reordered = weighted.project(&[2, 0, 1]).unwrap();
    let mut store = store(4, 64 * 1024);
    store.create_mv("v", data.schema(), mode).unwrap();
    store.update_cycle("v", &[reordered]).unwrap();
    assert_eq!(ids(&store, "v"), [2, 3]);
}

#[test]
fn keyed_staging_is_bounded_across_batches_even_when_final_state_would_fit() {
    for mode in [
        MvStorageMode::Upsert { key_cols: vec![0] },
        MvStorageMode::Multiset,
    ] {
        for (rows, bytes, width, count) in [(2, 65536, 1, 5), (64, 1024, 512, 12)] {
            let original = batch(&[0], 1);
            let mut store = store(rows, bytes);
            store
                .create_mv("first", original.schema(), MvStorageMode::Aggregate)
                .unwrap();
            store
                .create_mv("second", original.schema(), mode.clone())
                .unwrap();
            store
                .update_cycle("first", std::slice::from_ref(&original))
                .unwrap();
            store
                .update_cycle("second", &[input(&original, &mode, 1)])
                .unwrap();
            let old_bytes = store.total_bytes();
            let values: Vec<_> = (1..=count).map(|id| batch(&[id], width)).collect();
            let updates: Vec<_> = values
                .iter()
                .map(|value| input(value, &mode, 1))
                .chain(values.iter().map(|value| input(value, &mode, -1)))
                .collect();
            let first = [batch(&[9], 1)];
            assert!(
                matches!(
                    store.update_views([
                        ("first", first.as_slice()),
                        ("second", updates.as_slice())
                    ]),
                    Err(DbError::MaterializedViewQuotaExceeded { .. })
                ),
                "{mode:?}"
            );
            assert_eq!(ids(&store, "first"), [0]);
            assert_eq!(ids(&store, "second"), [0]);
            assert_eq!(store.total_bytes(), old_bytes);
        }
    }
}

#[test]
fn repeated_dictionary_and_view_values_are_bounded_before_row_encoding() {
    use arrow::array::{DictionaryArray, Int32Array};
    use arrow::datatypes::Int32Type;
    let array = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0; 512]),
        Arc::new(StringArray::from(vec!["x".repeat(4096)])),
    )
    .unwrap();
    let seed = StringViewArray::from(vec!["x".repeat(4096)]);
    let views = StringViewArray::new(
        vec![seed.views()[0]; 512].into(),
        seed.data_buffers().to_vec(),
        None,
    );
    for payload in [Arc::new(array) as ArrayRef, Arc::new(views)] {
        let data = RecordBatch::try_from_iter(vec![("value", payload)]).unwrap();
        for mode in [
            MvStorageMode::Upsert { key_cols: vec![0] },
            MvStorageMode::Multiset,
        ] {
            let mut store = store(1024, 64 * 1024);
            store.create_mv("v", data.schema(), mode.clone()).unwrap();
            assert!(matches!(
                store.update_cycle("v", &[input(&data, &mode, 1)]),
                Err(DbError::MaterializedViewQuotaExceeded { .. })
            ));
            assert_eq!(store.total_bytes(), 0);
            assert_eq!(store.to_record_batch("v").unwrap().unwrap().num_rows(), 0);
        }
    }
}

#[test]
fn ordinary_dictionary_and_view_batches_fit_staging_budget() {
    use arrow::array::{DictionaryArray, Int32Array};
    use arrow::datatypes::Int32Type;
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from_iter_values(0..16_384),
        Arc::new(StringArray::from(vec!["ordinary value"; 16_384])),
    )
    .unwrap();
    let views = StringViewArray::from(vec!["ordinary value"; 16_384]);
    for payload in [Arc::new(dictionary) as ArrayRef, Arc::new(views)] {
        let data = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..16_384)) as ArrayRef,
            ),
            ("value", payload),
        ])
        .unwrap();
        for mode in [
            MvStorageMode::Upsert { key_cols: vec![0] },
            MvStorageMode::Multiset,
        ] {
            let mut store = store(32_768, 64 * 1024 * 1024);
            store.create_mv("v", data.schema(), mode.clone()).unwrap();
            store.update_cycle("v", &[input(&data, &mode, 1)]).unwrap();
            let entry = &store.entries["v"];
            let retained = if let Some(upsert) = &entry.upsert {
                upsert.rows.len()
            } else {
                entry.multiset.as_ref().unwrap().counts.len()
            };
            assert_eq!(retained, 16_384);
            let snapshot = store.to_record_batch("v").unwrap().unwrap();
            assert_eq!(snapshot.schema(), data.schema());
            assert_eq!(snapshot.num_rows(), 16_384);
        }
    }
}

#[test]
fn keyed_staging_releases_replaced_values_and_cancelled_deltas() {
    let plain = batch(&[1], 64);
    let upsert = MvStorageMode::Upsert { key_cols: vec![0] };
    let mut keyed = store(1, 1024);
    keyed
        .create_mv("v", plain.schema(), upsert.clone())
        .unwrap();
    let updates: Vec<_> = (1..=20)
        .map(|width| input(&batch(&[1], width * 4), &upsert, 1))
        .collect();
    keyed.update_cycle("v", &updates).unwrap();
    assert_eq!(ids(&keyed, "v"), [1]);

    let mut counted = store(1, 1024);
    counted
        .create_mv("v", plain.schema(), MvStorageMode::Multiset)
        .unwrap();
    let updates: Vec<_> = (1..=20)
        .flat_map(|id| {
            let data = batch(&[id], 64);
            [1, -1].map(|weight| input(&data, &MvStorageMode::Multiset, weight))
        })
        .collect();
    counted.update_cycle("v", &updates).unwrap();
    assert!(ids(&counted, "v").is_empty());
    assert_eq!(counted.total_bytes(), 0);
}

fn store(rows: usize, bytes: usize) -> MvStore {
    MvStore::from_config(&crate::LaminarConfig {
        materialized_view_max_rows: rows,
        materialized_view_max_bytes: bytes,
        ..Default::default()
    })
}

fn ids(store: &MvStore, name: &str) -> Vec<i64> {
    let snapshot = store.to_record_batch(name).unwrap().unwrap();
    let mut ids = snapshot
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    ids.sort_unstable();
    ids
}

#[test]
fn default_aggregate_quota_rejects_oversized_snapshot() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values(0..1_000_001))],
    )
    .unwrap();
    let mut store = MvStore::new();
    store
        .create_mv("v", batch.schema(), MvStorageMode::Aggregate)
        .unwrap();
    assert!(
        store.update_cycle("v", &[batch]).is_err(),
        "an oversized snapshot must fail admission"
    );
    assert!(ids(&store, "v").is_empty());
}

#[test]
fn all_modes_reject_single_oversized_updates_without_changing_state() {
    for mode in modes() {
        for (large, rows, bytes) in [
            (batch(&[2, 3, 4], 1), 2, 16384),
            (batch(&[2], 8192), 8, 4096),
        ] {
            let small = batch(&[1], 1);
            let mut store = store(rows, bytes);
            store.create_mv("v", small.schema(), mode.clone()).unwrap();
            store.update_cycle("v", &[input(&small, &mode, 1)]).unwrap();
            let old_bytes = store.total_bytes();
            assert!(
                matches!(
                    store.update_cycle("v", &[input(&large, &mode, 1)]),
                    Err(DbError::MaterializedViewQuotaExceeded { .. })
                ),
                "{mode:?}"
            );
            assert_eq!(ids(&store, "v"), [1], "{mode:?}");
            assert_eq!(store.total_bytes(), old_bytes);
        }
    }
}

#[test]
fn aggregate_replacement_counts_every_batch_and_preserves_empty_cycle() {
    let small = batch(&[1], 1);
    let mut store = store(2, 16384);
    store
        .create_mv("v", small.schema(), MvStorageMode::Aggregate)
        .unwrap();
    store.update_cycle("v", &[small]).unwrap();
    assert!(store
        .update_cycle("v", &[batch(&[2, 3], 1), batch(&[4], 1)])
        .is_err());
    assert_eq!(ids(&store, "v"), [1]);
    store
        .update_cycle("v", &[batch(&[2], 1), batch(&[3], 1)])
        .unwrap();
    store.update_cycle("v", &[batch(&[], 1)]).unwrap();
    assert_eq!(ids(&store, "v"), [2, 3]);
}

#[test]
fn append_retains_newest_complete_batches_and_rejects_later_oversized_batch_atomically() {
    let mut store = store(3, 16384);
    store
        .create_mv(
            "v",
            batch(&[], 1).schema(),
            MvStorageMode::Append { max_batches: 2 },
        )
        .unwrap();
    store
        .update_cycle("v", &[batch(&[1], 1), batch(&[2], 1)])
        .unwrap();
    let bytes = store.total_bytes();
    assert!(store
        .update_cycle("v", &[batch(&[3], 1), batch(&[4, 5, 6, 7], 1)])
        .is_err());
    assert_eq!(ids(&store, "v"), [1, 2]);
    assert_eq!(store.total_bytes(), bytes);
    store
        .update_cycle("v", &[batch(&[], 1), batch(&[3], 1), batch(&[4, 5], 1)])
        .unwrap();
    assert_eq!(ids(&store, "v"), [3, 4, 5]);
    store.update_cycle("v", &[batch(&[6, 7], 1)]).unwrap();
    assert_eq!(ids(&store, "v"), [6, 7]);
}

#[test]
fn keyed_deletes_release_memory_and_replacements_use_final_state_quota() {
    for mode in [
        MvStorageMode::Upsert { key_cols: vec![0] },
        MvStorageMode::Multiset,
    ] {
        let original = batch(&[1], 1024);
        let replacement = batch(&[2], 1024);
        let mut probe = MvStore::new();
        probe
            .create_mv("v", original.schema(), mode.clone())
            .unwrap();
        probe
            .update_cycle("v", &[input(&original, &mode, 1)])
            .unwrap();
        let mut store = store(1, probe.total_bytes());
        store
            .create_mv("v", original.schema(), mode.clone())
            .unwrap();
        store
            .update_cycle("v", &[input(&original, &mode, 1)])
            .unwrap();
        assert!(store
            .update_cycle("v", &[input(&replacement, &mode, 1)])
            .is_err());
        store
            .update_cycle(
                "v",
                &[input(&replacement, &mode, 1), input(&original, &mode, -1)],
            )
            .unwrap();
        assert_eq!(ids(&store, "v"), [2]);
        store
            .update_cycle("v", &[input(&replacement, &mode, -1)])
            .unwrap();
        assert_eq!(store.total_bytes(), 0);
        assert!(ids(&store, "v").is_empty());
    }
}

#[test]
fn all_modes_reject_over_limit_restore_and_recovery_images_atomically() {
    for mode in modes() {
        let mut source = MvStore::new();
        source
            .create_mv("v", batch(&[], 1).schema(), mode.clone())
            .unwrap();
        source
            .update_cycle("v", &[input(&batch(&[1, 2, 3], 1), &mode, 1)])
            .unwrap();
        let encoded = source.checkpoint_states().unwrap();
        let bytes = &encoded["mv:v"];
        let mut target = store(2, 16384);
        target
            .create_mv("v", batch(&[], 1).schema(), mode.clone())
            .unwrap();
        target
            .update_cycle("v", &[input(&batch(&[9], 1), &mode, 1)])
            .unwrap();
        let before_bytes = target.total_bytes();
        assert!(matches!(
            target.restore_from_ipc("v", bytes),
            Err(DbError::MaterializedViewQuotaExceeded { .. })
        ));
        assert!(target
            .recovery_image(&HashMap::from([("v".to_owned(), bytes.to_vec())]))
            .is_err());
        assert_eq!(ids(&target, "v"), [9]);
        assert_eq!(target.total_bytes(), before_bytes);
    }
}

#[test]
fn all_modes_reject_over_byte_limit_restore_without_losing_old_rows() {
    for mode in modes() {
        let mut source = MvStore::new();
        source
            .create_mv("v", batch(&[], 1).schema(), mode.clone())
            .unwrap();
        source
            .update_cycle("v", &[input(&batch(&[1], 8192), &mode, 1)])
            .unwrap();
        let encoded = source.checkpoint_states().unwrap();
        let mut target = store(10, 4096);
        target
            .create_mv("v", batch(&[], 1).schema(), mode.clone())
            .unwrap();
        target
            .update_cycle("v", &[input(&batch(&[9], 1), &mode, 1)])
            .unwrap();
        let bytes = target.total_bytes();
        assert!(
            matches!(
                target.restore_from_ipc("v", &encoded["mv:v"]),
                Err(DbError::MaterializedViewQuotaExceeded { .. })
            ),
            "{mode:?}"
        );
        assert_eq!(ids(&target, "v"), [9]);
        assert_eq!(target.total_bytes(), bytes);
    }
}

#[test]
fn append_byte_eviction_and_shorter_keyed_replacements_release_live_charge() {
    let mode = MvStorageMode::Append { max_batches: 100 };
    let original = batch(&[1], 1024);
    let mut probe = MvStore::new();
    probe
        .create_mv("v", original.schema(), mode.clone())
        .unwrap();
    probe
        .update_cycle("v", std::slice::from_ref(&original))
        .unwrap();
    let mut append = store(100, probe.total_bytes());
    append.create_mv("v", original.schema(), mode).unwrap();
    append
        .update_cycle("v", std::slice::from_ref(&original))
        .unwrap();
    append.update_cycle("v", &[batch(&[2], 1024)]).unwrap();
    assert_eq!(ids(&append, "v"), [2]);
    assert_eq!(append.total_bytes(), probe.total_bytes());

    let mode = MvStorageMode::Upsert { key_cols: vec![0] };
    let mut upsert = store(10, 4096);
    upsert
        .create_mv("v", original.schema(), mode.clone())
        .unwrap();
    upsert
        .update_cycle("v", &[input(&original, &mode, 1)])
        .unwrap();
    let old_bytes = upsert.total_bytes();
    upsert
        .update_cycle("v", &[input(&batch(&[1], 1), &mode, 1)])
        .unwrap();
    assert!(upsert.total_bytes() + 1000 < old_bytes);
    upsert.drop_mv("v");
    assert_eq!(upsert.total_bytes(), 0);
}

#[test]
fn sliced_arrow_and_view_backing_storage_count_toward_live_quota() {
    let original = RecordBatch::try_new(
        batch(&[], 1).schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["tiny".to_owned(), "x".repeat(8192)])),
        ],
    )
    .unwrap();
    let view_values = StringViewArray::from(vec!["tiny".to_owned(), "b".repeat(8192)]);
    let views = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8View, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(view_values),
        ],
    )
    .unwrap();
    for data in [original.slice(0, 1), views.slice(0, 1)] {
        for mode in [MvStorageMode::Aggregate, MvStorageMode::append_default()] {
            let mut store = store(4, 4096);
            store.create_mv("v", data.schema(), mode).unwrap();
            assert!(store
                .update_cycle("v", std::slice::from_ref(&data))
                .is_err());
            assert!(ids(&store, "v").is_empty());
        }
    }
}

#[test]
fn nested_and_dictionary_arrow_payloads_are_charged() {
    use arrow::array::{ArrayRef, DictionaryArray, ListArray, StructArray};
    use arrow::datatypes::Int32Type;
    let strings: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(8192)]));
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("value", DataType::Utf8, false)),
        strings,
    )]));
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
        arrow::datatypes::Int64Type,
        _,
        _,
    >([Some(vec![Some(1); 2048])]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            arrow::array::Int32Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["x".repeat(8192)])),
        )
        .unwrap(),
    );
    for payload in [nested, list, dictionary] {
        let plain = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", payload.data_type().clone(), true),
            ])),
            vec![Arc::new(Int64Array::from(vec![1])), payload],
        )
        .unwrap();
        for mode in modes() {
            let mut store = store(2, 4096);
            store.create_mv("v", plain.schema(), mode.clone()).unwrap();
            assert!(
                matches!(
                    store.update_cycle("v", &[input(&plain, &mode, 1)]),
                    Err(DbError::MaterializedViewQuotaExceeded { .. })
                ),
                "{mode:?}"
            );
            assert!(ids(&store, "v").is_empty());
        }
    }
}

#[test]
fn encoded_upsert_keys_and_multiset_rows_are_included_in_byte_quota() {
    let plain = batch(&[1], 3000);
    // A keyed scalar row fits below 4 KiB; its separate encoded string key does not.
    for mode in [
        MvStorageMode::Upsert { key_cols: vec![1] },
        MvStorageMode::Multiset,
    ] {
        let mut store = store(1, 4096);
        store.create_mv("v", plain.schema(), mode.clone()).unwrap();
        if matches!(mode, MvStorageMode::Upsert { .. }) {
            assert!(matches!(
                store.update_cycle("v", &[input(&plain, &mode, 1)]),
                Err(DbError::MaterializedViewQuotaExceeded { .. })
            ));
        } else {
            store.update_cycle("v", &[input(&plain, &mode, 1)]).unwrap();
            assert!(store.total_bytes() >= 3000);
        }
    }
}

proptest::proptest! {
    #[test]
    fn keyed_quota_updates_match_an_independent_row_oracle(operations in proptest::collection::vec((0i64..6, proptest::bool::ANY), 0..60)) {
        for mode in [MvStorageMode::Upsert { key_cols: vec![0] }, MvStorageMode::Multiset] {
            let mut store = store(3, 1024 * 1024);
            store.create_mv("v", batch(&[], 1).schema(), mode.clone()).unwrap();
            let mut expected = std::collections::BTreeMap::<i64, usize>::new();
            for &(key, insert) in &operations {
                let mut candidate = expected.clone();
                let old = candidate.get(&key).copied().unwrap_or(0);
                let invalid_retraction = matches!(mode, MvStorageMode::Multiset) && !insert && old == 0;
                match (&mode, insert) {
                    (MvStorageMode::Multiset, true) => { candidate.insert(key, old + 1); }
                    (MvStorageMode::Multiset, false) if old > 1 => { candidate.insert(key, old - 1); }
                    (_, true) => { candidate.insert(key, 1); }
                    (_, false) => { candidate.remove(&key); }
                }
                let result = store.update_cycle("v", &[input(&batch(&[key], 1), &mode, if insert { 1 } else { -1 })]);
                if invalid_retraction || candidate.len() > 3 {
                    proptest::prop_assert!(result.is_err());
                } else {
                    proptest::prop_assert!(result.is_ok());
                    expected = candidate;
                }
                let oracle: Vec<i64> = expected.iter().flat_map(|(&key, &count)| std::iter::repeat_n(key, count)).collect();
                proptest::prop_assert_eq!(ids(&store, "v"), oracle);
            }
        }
    }
}

#[test]
fn publication_preflight_rejects_multiset_expansion_before_applying_other_view() {
    let plain = batch(&[1], 1);
    let mut store = MvStore::new();
    store
        .create_mv("first", plain.schema(), MvStorageMode::Aggregate)
        .unwrap();
    store
        .create_mv("second", plain.schema(), MvStorageMode::Multiset)
        .unwrap();
    let first = [plain.clone()];
    let second = [input(&plain, &MvStorageMode::Multiset, i64::MAX)];
    assert!(store
        .update_views([("first", first.as_slice()), ("second", second.as_slice())])
        .is_err());
    assert!(ids(&store, "first").is_empty());
    assert!(ids(&store, "second").is_empty());
    assert_eq!(store.total_bytes(), 0);
}
