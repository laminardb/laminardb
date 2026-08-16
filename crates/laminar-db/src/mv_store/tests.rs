use std::sync::Arc;

use super::*;
use arrow::array::{Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn make_batch(ids: &[i32], names: &[&str], values: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn id_names(store: &MvStore, name: &str) -> Vec<(i32, String)> {
    let batch = store.to_record_batch(name).unwrap().unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (ids.value(row), names.value(row).to_string()))
        .collect()
}

/// Plain (weightless) schema for the upsert tests: `(k Int64, total Int64)`.
fn upsert_plain_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]))
}

/// Changelog schema = plain schema + appended `__weight`.
fn upsert_changelog_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]))
}

/// Build a `__weight` changelog batch from `(key, total, weight)` rows.
fn changelog_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
    use arrow::array::Int64Array;
    let ks: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let totals: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let weights: Vec<i64> = rows.iter().map(|r| r.2).collect();
    RecordBatch::try_new(
        upsert_changelog_schema(),
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(totals)),
            Arc::new(Int64Array::from(weights)),
        ],
    )
    .unwrap()
}

fn upsert_snapshot_batch(rows: &[(i64, i64)]) -> RecordBatch {
    let keys: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let totals: Vec<i64> = rows.iter().map(|row| row.1).collect();
    RecordBatch::try_new(
        upsert_plain_schema(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(totals)),
        ],
    )
    .unwrap()
}

fn nullable_upsert_changelog_batch(rows: &[(i64, i64, Option<i64>)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
        Field::new(WEIGHT_COLUMN, DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// `(k, total)` snapshot rows sorted by key, for order-independent assertions.
fn snapshot_rows(store: &MvStore, name: &str) -> Vec<(i64, i64)> {
    use arrow::array::Int64Array;
    let batch = store.to_record_batch(name).unwrap().unwrap();
    let ks = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut out: Vec<(i64, i64)> = (0..batch.num_rows())
        .map(|i| (ks.value(i), totals.value(i)))
        .collect();
    out.sort_unstable();
    out
}

#[test]
fn create_and_drop() {
    let mut store = MvStore::new();
    store
        .create_mv("mv1", test_schema(), MvStorageMode::Aggregate)
        .unwrap();
    assert!(store.has_mv("mv1"));
    assert!(store.drop_mv("mv1"));
    assert!(!store.has_mv("mv1"));
    assert!(!store.drop_mv("mv1"));
}

#[test]
fn aggregate_replaces_on_each_update() {
    let mut store = MvStore::new();
    store
        .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
        .unwrap();

    store.update("agg", &make_batch(&[1], &["a"], &[1.0]));
    assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 1);

    store.update("agg", &make_batch(&[2, 3], &["b", "c"], &[2.0, 3.0]));
    assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 2);
}

#[test]
fn aggregate_keeps_all_batches_of_a_multi_batch_cycle() {
    let mut store = MvStore::new();
    store
        .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
        .unwrap();

    // A non-incremental GROUP BY MV whose output spans several DataFusion batches must
    // retain every chunk within the cycle, not just the last (EX-1).
    store
        .update_cycle(
            "agg",
            &[
                make_batch(&[1, 2], &["a", "b"], &[1.0, 2.0]),
                make_batch(&[3, 4], &["c", "d"], &[3.0, 4.0]),
                make_batch(&[5], &["e"], &[5.0]),
            ],
        )
        .unwrap();
    assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 5);

    // The next cycle replaces the whole result set.
    store
        .update_cycle("agg", &[make_batch(&[9], &["z"], &[9.0])])
        .unwrap();
    assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 1);
}

#[test]
fn append_evicts_oldest() {
    let mut store = MvStore::new();
    store
        .create_mv(
            "app",
            test_schema(),
            MvStorageMode::Append { max_batches: 3 },
        )
        .unwrap();

    for i in 0..4 {
        store.update("app", &make_batch(&[i], &["x"], &[f64::from(i)]));
    }

    let result = store.to_record_batch("app").unwrap().unwrap();
    assert_eq!(result.num_rows(), 3);

    // Batch 0 evicted, should start at 1
    let ids = result
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
}

#[test]
fn empty_mv_returns_empty_batch() {
    let mut store = MvStore::new();
    store
        .create_mv("empty", test_schema(), MvStorageMode::Aggregate)
        .unwrap();
    let result = store.to_record_batch("empty").unwrap().unwrap();
    assert_eq!(result.num_rows(), 0);
    assert_eq!(result.schema(), test_schema());
}

#[test]
fn nonexistent_returns_none() {
    let store = MvStore::new();
    assert!(store.to_record_batch("nope").unwrap().is_none());
}

#[test]
fn checkpoint_round_trip() {
    let mut store = MvStore::new();
    store
        .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
        .unwrap();
    store.update("agg", &make_batch(&[1, 2], &["a", "b"], &[1.0, 2.0]));

    let states = store.checkpoint_states().unwrap();
    assert_eq!(states.len(), 1);
    assert!(states.contains_key("mv:agg"));

    // Simulate recovery into a fresh store
    let mut store2 = MvStore::new();
    store2
        .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
        .unwrap();
    for (key, bytes) in &states {
        let name = key.strip_prefix(CHECKPOINT_KEY_PREFIX).unwrap();
        store2.restore_from_ipc(name, bytes).unwrap();
    }
    assert_eq!(
        store2.to_record_batch("agg").unwrap().unwrap().num_rows(),
        2
    );
}

#[test]
fn checkpoint_capture_is_point_in_time_after_live_mutation() {
    let mut store = MvStore::new();
    store
        .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
        .unwrap();
    store
        .create_mv(
            "append",
            test_schema(),
            MvStorageMode::Append { max_batches: 8 },
        )
        .unwrap();
    store
        .create_mv(
            "upsert",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();
    store
        .create_mv("multiset", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();

    store.update("agg", &make_batch(&[1], &["old-agg"], &[1.0]));
    store.update("append", &make_batch(&[2], &["old-append"], &[2.0]));
    store.update("upsert", &changelog_batch(&[(1, 10, 1)]));
    store.update("multiset", &weight_batch_1col(&[(10, 2)]));
    let capture = store.capture_checkpoint(u64::MAX).unwrap();

    store.update("agg", &make_batch(&[9], &["new-agg"], &[9.0]));
    store.update("append", &make_batch(&[3], &["new-append"], &[3.0]));
    store.update("upsert", &changelog_batch(&[(1, 10, -1), (1, 99, 1)]));
    store.update("multiset", &weight_batch_1col(&[(10, -1), (20, 1)]));

    let states = capture
        .encode(u64::MAX)
        .unwrap()
        .into_parts()
        .0
        .into_iter()
        .map(|(key, bytes)| {
            (
                key.strip_prefix(CHECKPOINT_KEY_PREFIX).unwrap().to_string(),
                bytes.to_vec(),
            )
        })
        .collect::<HashMap<_, _>>();
    let image = store.recovery_image(&states).unwrap();

    assert_eq!(id_names(&image, "agg"), vec![(1, "old-agg".to_string())]);
    assert_eq!(
        id_names(&image, "append"),
        vec![(2, "old-append".to_string())]
    );
    assert_eq!(snapshot_rows(&image, "upsert"), vec![(1, 10)]);
    assert_eq!(multiset_values(&image, "multiset"), vec![10, 10]);

    assert_eq!(id_names(&store, "agg"), vec![(9, "new-agg".to_string())]);
    assert_eq!(
        id_names(&store, "append"),
        vec![(2, "old-append".to_string()), (3, "new-append".to_string())]
    );
    assert_eq!(snapshot_rows(&store, "upsert"), vec![(1, 99)]);
    assert_eq!(multiset_values(&store, "multiset"), vec![10, 20]);
}

#[test]
fn checkpoint_capture_cap_rejection_preserves_live_state() {
    let mut store = MvStore::new();
    store
        .create_mv(
            "upsert",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();
    store.update("upsert", &changelog_batch(&[(1, 10, 1), (2, 20, 1)]));
    let before = snapshot_rows(&store, "upsert");
    let before_bytes = store.total_bytes();
    let estimated = store.checkpoint_capture_estimated_bytes().unwrap();
    assert!(estimated > 0);

    let error = store
        .capture_checkpoint(estimated - 1)
        .err()
        .expect("capture above the remaining checkpoint budget must fail");

    assert!(error.to_string().contains("staged-state cap"));
    assert_eq!(snapshot_rows(&store, "upsert"), before);
    assert_eq!(store.total_bytes(), before_bytes);
}

#[test]
fn checkpoint_ipc_encoding_enforces_dynamic_remaining_budget_without_mutation() {
    let mut store = MvStore::new();
    for name in ["a", "b"] {
        store
            .create_mv(name, test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        store.update(name, &make_batch(&[1, 2], &["one", "two"], &[1.0, 2.0]));
    }
    let before_a = store.to_record_batch("a").unwrap().unwrap();
    let before_b = store.to_record_batch("b").unwrap().unwrap();
    let before_bytes = store.total_bytes();

    let full = store
        .capture_checkpoint(u64::MAX)
        .unwrap()
        .encode(u64::MAX)
        .unwrap();
    let full_bytes = full.states().values().try_fold(0u64, |total, bytes| {
        total.checked_add(u64::try_from(bytes.len()).unwrap())
    });
    let full_bytes = full_bytes.unwrap();

    let error = store
        .capture_checkpoint(u64::MAX)
        .unwrap()
        .encode(full_bytes - 1)
        .unwrap_err();
    assert!(error.to_string().contains("MV 'b'"));
    assert!(error.to_string().contains("configured bound"));

    let tiny_error = store
        .capture_checkpoint(u64::MAX)
        .unwrap()
        .encode(1)
        .unwrap_err();
    assert!(tiny_error.to_string().contains("MV 'a'"));
    assert!(tiny_error.to_string().contains("configured bound"));

    assert_eq!(store.to_record_batch("a").unwrap().unwrap(), before_a);
    assert_eq!(store.to_record_batch("b").unwrap().unwrap(), before_b);
    assert_eq!(store.total_bytes(), before_bytes);
}

#[test]
fn empty_local_checkpoint_has_an_explicit_entry_for_every_storage_mode() {
    let mut store = MvStore::new();
    store
        .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
        .unwrap();
    store
        .create_mv("append", test_schema(), MvStorageMode::append_default())
        .unwrap();
    store
        .create_mv(
            "upsert",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();
    store
        .create_mv("multiset", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();

    let states = store.checkpoint_states().unwrap();
    let mut keys: Vec<&str> = states.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["mv:agg", "mv:append", "mv:multiset", "mv:upsert"]);
    for (key, bytes) in &states {
        let (schema, batches) = ipc_to_schema_and_batches(bytes).unwrap();
        let expected = match key.as_str() {
            "mv:agg" | "mv:append" => test_schema(),
            "mv:upsert" => upsert_plain_schema(),
            "mv:multiset" => multiset_checkpoint_schema(&one_col_schema()),
            other => panic!("unexpected checkpoint key {other}"),
        };
        assert_eq!(schema, expected, "schema for {key}");
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            0,
            "rows for {key}"
        );
    }
}

#[test]
fn update_nonexistent_is_noop() {
    let mut store = MvStore::new();
    store.update("nope", &make_batch(&[1], &["a"], &[1.0]));
    assert!(!store.has_mv("nope"));
}

#[test]
fn create_replaces_existing() {
    let mut store = MvStore::new();
    store
        .create_mv("mv1", test_schema(), MvStorageMode::Aggregate)
        .unwrap();
    store.update("mv1", &make_batch(&[1], &["a"], &[1.0]));
    assert_eq!(store.to_record_batch("mv1").unwrap().unwrap().num_rows(), 1);

    store
        .create_mv("mv1", test_schema(), MvStorageMode::append_default())
        .unwrap();
    assert_eq!(store.to_record_batch("mv1").unwrap().unwrap().num_rows(), 0);
}

#[test]
fn restore_rejects_schema_mismatch() {
    let schema_a = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let schema_b = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Utf8, false),
    ]));

    // Serialize a batch with schema_b
    let batch_b = RecordBatch::try_new(
        schema_b.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    let mut deque = VecDeque::new();
    deque.push_back(batch_b);
    let ipc_bytes = batches_to_ipc(&schema_b, &deque).unwrap();

    // Try to restore into an MV with schema_a
    let mut store = MvStore::new();
    store
        .create_mv("mv1", schema_a, MvStorageMode::Aggregate)
        .unwrap();
    let err = store.restore_from_ipc("mv1", &ipc_bytes);
    assert!(err.is_err(), "should reject mismatched schema");
}

#[test]
fn restore_rejects_schema_only_stream_with_wrong_schema() {
    let expected = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let wrong = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let bytes = batches_to_ipc(&wrong, std::iter::empty::<&RecordBatch>()).unwrap();
    let mut store = MvStore::new();
    store
        .create_mv("mv1", expected, MvStorageMode::Aggregate)
        .unwrap();

    let error = store.restore_from_ipc("mv1", &bytes).unwrap_err();
    assert!(error.to_string().contains("schema mismatch"));
}

#[test]
fn fresh_image_is_empty_and_preserves_the_hot_path_handle() {
    let mut store = MvStore::new();
    store
        .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
        .unwrap();
    store.update("agg", &make_batch(&[1], &["a"], &[1.0]));
    let handle = store.has_any_handle();

    let image = store.fresh_image().unwrap();

    assert!(Arc::ptr_eq(&handle, &image.has_any_handle()));
    assert_eq!(image.to_record_batch("agg").unwrap().unwrap().num_rows(), 0);
    assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 1);
}

#[test]
fn recovery_image_requires_an_exact_inventory() {
    let mut store = MvStore::new();
    store
        .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
        .unwrap();

    let missing = store
        .recovery_image(&HashMap::new())
        .err()
        .expect("missing inventory must fail");
    assert!(missing
        .to_string()
        .contains("missing required state for: agg"));

    let bytes = batches_to_ipc(
        &test_schema(),
        std::iter::once(&make_batch(&[1], &["a"], &[1.0])),
    )
    .unwrap();
    let unknown = store
        .recovery_image(&HashMap::from([("ghost".to_string(), bytes)]))
        .err()
        .expect("unknown inventory must fail");
    assert!(unknown.to_string().contains("no matching registered"));
}

#[test]
fn failed_recovery_image_never_mutates_live_state() {
    let mut store = MvStore::new();
    store
        .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
        .unwrap();
    store
        .create_mv("append", test_schema(), MvStorageMode::append_default())
        .unwrap();
    store.update("agg", &make_batch(&[1], &["live"], &[1.0]));
    store.update("append", &make_batch(&[2], &["live"], &[2.0]));
    let live_bytes = store.total_bytes();

    let valid = batches_to_ipc(
        &test_schema(),
        std::iter::once(&make_batch(&[9], &["checkpoint"], &[9.0])),
    )
    .unwrap();
    let states = HashMap::from([
        ("agg".to_string(), valid),
        ("append".to_string(), b"not arrow ipc".to_vec()),
    ]);

    let error = store
        .recovery_image(&states)
        .err()
        .expect("corrupt inventory must fail");
    assert!(error.to_string().contains("MV restore failed"));
    assert_eq!(store.total_bytes(), live_bytes);
    assert_eq!(id_names(&store, "agg"), [(1, "live".to_string())]);
    assert_eq!(id_names(&store, "append"), [(2, "live".to_string())]);
}

#[test]
fn upsert_applies_inserts_changes_and_deletes() {
    let mut store = MvStore::new();
    store
        .create_mv(
            "u",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();

    // Two new keys.
    store.update("u", &changelog_batch(&[(1, 10, 1), (2, 20, 1)]));
    assert_eq!(snapshot_rows(&store, "u"), vec![(1, 10), (2, 20)]);

    // Change key 1: retract old (−1) then insert new (+1) nets to the new value.
    store.update("u", &changelog_batch(&[(1, 10, -1), (1, 15, 1)]));
    assert_eq!(snapshot_rows(&store, "u"), vec![(1, 15), (2, 20)]);

    // Delete key 2 (pure retract).
    store.update("u", &changelog_batch(&[(2, 20, -1)]));
    assert_eq!(snapshot_rows(&store, "u"), vec![(1, 15)]);
}

#[test]
fn upsert_cycle_is_atomic_when_a_later_batch_has_null_weight() {
    let mut store = MvStore::new();
    store
        .create_mv(
            "u",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();
    store.update("u", &changelog_batch(&[(1, 10, 1)]));
    let before = snapshot_rows(&store, "u");
    let before_bytes = store.total_bytes();

    let error = store
        .update_cycle(
            "u",
            &[
                changelog_batch(&[(2, 20, 1)]),
                nullable_upsert_changelog_batch(&[(3, 30, None)]),
            ],
        )
        .expect_err("null weight must reject the whole cycle");
    assert!(error.to_string().contains("weight is null"));
    assert_eq!(snapshot_rows(&store, "u"), before);
    assert_eq!(store.total_bytes(), before_bytes);
}

#[test]
fn upsert_snapshot_equals_full_recompute() {
    use std::collections::BTreeMap;

    // A changelog stream and the running ground-truth (last +weight value per key).
    let batches = [
        vec![(1i64, 5i64, 1i64), (2, 7, 1), (3, 9, 1)],
        vec![(2, 7, -1), (2, 8, 1), (4, 1, 1)],
        vec![(1, 5, -1)], // delete key 1
        vec![(3, 9, -1), (3, 100, 1)],
    ];
    let mut truth: BTreeMap<i64, i64> = BTreeMap::new();
    let mut store = MvStore::new();
    store
        .create_mv(
            "u",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();
    for rows in &batches {
        for &(k, v, w) in rows {
            if w > 0 {
                truth.insert(k, v);
            } else {
                truth.remove(&k);
            }
        }
        store.update("u", &changelog_batch(rows));
    }
    let expected: Vec<(i64, i64)> = truth.into_iter().collect();
    assert_eq!(snapshot_rows(&store, "u"), expected);
}

#[test]
fn upsert_checkpoint_round_trip() {
    let mut store = MvStore::new();
    store
        .create_mv(
            "u",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();
    store.update("u", &changelog_batch(&[(1, 10, 1), (2, 20, 1), (3, 30, 1)]));
    store.update("u", &changelog_batch(&[(2, 20, -1)]));
    let before = snapshot_rows(&store, "u");

    let states = store.checkpoint_states().unwrap();
    assert!(states.contains_key("mv:u"));

    let mut store2 = MvStore::new();
    store2
        .create_mv(
            "u",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();
    for (key, bytes) in &states {
        let name = key.strip_prefix(CHECKPOINT_KEY_PREFIX).unwrap();
        store2.restore_from_ipc(name, bytes).unwrap();
    }
    assert_eq!(snapshot_rows(&store2, "u"), before);

    // A restored store keeps applying changelog correctly.
    store2.update("u", &changelog_batch(&[(1, 10, -1), (1, 99, 1)]));
    assert_eq!(snapshot_rows(&store2, "u"), vec![(1, 99), (3, 30)]);
}

#[test]
fn failed_upsert_restore_preserves_live_state() {
    let mut store = MvStore::new();
    store
        .create_mv(
            "u",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();
    store.update("u", &changelog_batch(&[(1, 10, 1)]));
    let before = snapshot_rows(&store, "u");
    let before_bytes = store.total_bytes();

    let wrong_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let bad = RecordBatch::try_new(
        wrong_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![9]))],
    )
    .unwrap();
    let bytes = batches_to_ipc(&wrong_schema, std::iter::once(&bad)).unwrap();
    assert!(store.restore_from_ipc("u", &bytes).is_err());
    assert_eq!(snapshot_rows(&store, "u"), before);
    assert_eq!(store.total_bytes(), before_bytes);
}

#[test]
fn upsert_restore_duplicate_key_accounting_tracks_only_replacement() {
    let mut store = MvStore::new();
    store
        .create_mv(
            "u",
            upsert_plain_schema(),
            MvStorageMode::Upsert { key_cols: vec![0] },
        )
        .unwrap();
    let snapshot = upsert_snapshot_batch(&[(1, 10), (1, 20)]);
    let bytes = batches_to_ipc(&upsert_plain_schema(), std::iter::once(&snapshot)).unwrap();

    store.restore_from_ipc("u", &bytes).unwrap();
    assert_eq!(snapshot_rows(&store, "u"), vec![(1, 20)]);
    let expected = ScalarValue::Int64(Some(1))
        .size()
        .saturating_add(ScalarValue::Int64(Some(20)).size());
    assert_eq!(store.total_bytes(), expected);
}

// ── Multiset (Z-set) mode: chained projections/filters over a changelog ──

/// Single-column changelog `(v, __weight)` for the key-dropping multiset case.
fn weight_batch_1col(rows: &[(i64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, true),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let vs: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let ws: Vec<i64> = rows.iter().map(|r| r.1).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vs)),
            Arc::new(arrow::array::Int64Array::from(ws)),
        ],
    )
    .unwrap()
}

fn nullable_weight_batch_1col(rows: &[(i64, Option<i64>)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, true),
        Field::new(WEIGHT_COLUMN, DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn plain_one_col_batch(values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        one_col_schema(),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

fn counted_multiset_checkpoint_batch(rows: &[(i64, i64)]) -> RecordBatch {
    let schema = multiset_checkpoint_schema(&one_col_schema());
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn one_col_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
}

/// `v` snapshot values sorted (with multiplicity).
fn multiset_values(store: &MvStore, name: &str) -> Vec<i64> {
    use arrow::array::Int64Array;
    let batch = store.to_record_batch(name).unwrap().unwrap();
    let vs = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut out: Vec<i64> = (0..batch.num_rows()).map(|i| vs.value(i)).collect();
    out.sort_unstable();
    out
}

#[test]
fn multiset_nets_retractions_for_keyed_rows() {
    let mut store = MvStore::new();
    store
        .create_mv("m", upsert_plain_schema(), MvStorageMode::Multiset)
        .unwrap();
    store.update("m", &changelog_batch(&[(1, 10, 1), (2, 20, 1)]));
    assert_eq!(snapshot_rows(&store, "m"), vec![(1, 10), (2, 20)]);
    // Change k=1: retract old full row, insert new full row.
    store.update("m", &changelog_batch(&[(1, 10, -1), (1, 15, 1)]));
    assert_eq!(snapshot_rows(&store, "m"), vec![(1, 15), (2, 20)]);
}

#[test]
fn multiset_tracks_duplicate_rows() {
    // Key-dropping projection: two upstream keys with the same value v=10 → multiplicity 2.
    let mut store = MvStore::new();
    store
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    store.update("m", &weight_batch_1col(&[(10, 1), (10, 1), (20, 1)]));
    assert_eq!(multiset_values(&store, "m"), vec![10, 10, 20]);

    // One source of the 10 changes 10→15: retract one (10), insert (15). The other 10 survives.
    store.update("m", &weight_batch_1col(&[(10, -1), (15, 1)]));
    assert_eq!(multiset_values(&store, "m"), vec![10, 15, 20]);

    // The remaining 10 retracts → gone.
    store.update("m", &weight_batch_1col(&[(10, -1)]));
    assert_eq!(multiset_values(&store, "m"), vec![15, 20]);
}

#[test]
fn multiset_cycle_is_atomic_when_a_later_batch_is_invalid() {
    let mut store = MvStore::new();
    store
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    store.update("m", &weight_batch_1col(&[(10, 1)]));
    let before = multiset_values(&store, "m");
    let before_bytes = store.total_bytes();

    let error = store
        .update_cycle(
            "m",
            &[weight_batch_1col(&[(20, 1)]), plain_one_col_batch(&[30])],
        )
        .expect_err("missing weight must reject the whole cycle");
    assert!(error.to_string().contains("missing weight"));
    assert_eq!(multiset_values(&store, "m"), before);
    assert_eq!(store.total_bytes(), before_bytes);
}

#[test]
fn multiset_rejects_negative_overflow_and_null_weight_without_mutation() {
    let mut store = MvStore::new();
    store
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    store.update("m", &weight_batch_1col(&[(10, 1)]));
    let before = multiset_values(&store, "m");
    let before_bytes = store.total_bytes();

    let negative = store
        .update_cycle("m", &[weight_batch_1col(&[(10, -2)])])
        .expect_err("negative multiplicity must fail");
    assert!(negative.to_string().contains("negative multiplicity"));
    assert_eq!(multiset_values(&store, "m"), before);
    assert_eq!(store.total_bytes(), before_bytes);

    let null = store
        .update_cycle(
            "m",
            &[nullable_weight_batch_1col(&[(20, Some(1)), (10, None)])],
        )
        .expect_err("null weight must fail");
    assert!(null.to_string().contains("weight is null"));
    assert_eq!(multiset_values(&store, "m"), before);
    assert_eq!(store.total_bytes(), before_bytes);

    let mut overflow_store = MvStore::new();
    overflow_store
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    overflow_store
        .update_cycle("m", &[weight_batch_1col(&[(10, i64::MAX)])])
        .unwrap();
    let before_overflow = overflow_store.total_bytes();
    let overflow = overflow_store
        .update_cycle("m", &[weight_batch_1col(&[(10, 1)])])
        .expect_err("multiplicity overflow must fail");
    assert!(overflow.to_string().contains("multiplicity overflow"));
    let count = overflow_store
        .entries
        .get("m")
        .and_then(|entry| entry.multiset.as_ref())
        .and_then(|state| state.counts.values().next())
        .copied();
    assert_eq!(count, Some(i64::MAX));
    assert_eq!(overflow_store.total_bytes(), before_overflow);
}

#[test]
fn multiset_checkpoint_round_trip_preserves_multiplicity() {
    let mut store = MvStore::new();
    store
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    store.update("m", &weight_batch_1col(&[(10, 1), (10, 1), (20, 1)]));
    let before = multiset_values(&store, "m");
    assert_eq!(before, vec![10, 10, 20]);

    let states = store.checkpoint_states().unwrap();
    let mut store2 = MvStore::new();
    store2
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    for (key, bytes) in &states {
        let name = key.strip_prefix(CHECKPOINT_KEY_PREFIX).unwrap();
        store2.restore_from_ipc(name, bytes).unwrap();
    }
    // Multiplicity (10 appears twice) survives the round-trip.
    assert_eq!(multiset_values(&store2, "m"), before);
    // And a restored store keeps netting: retract one 10.
    store2.update("m", &weight_batch_1col(&[(10, -1)]));
    assert_eq!(multiset_values(&store2, "m"), vec![10, 20]);
}

#[test]
fn multiset_checkpoint_is_counted_and_does_not_materialize_multiplicity() {
    let mut store = MvStore::new();
    store
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    store
        .update_cycle("m", &[weight_batch_1col(&[(10, i64::MAX)])])
        .unwrap();
    let live_bytes = store.total_bytes();

    let read_error = store.to_record_batch("m").unwrap_err();
    assert!(read_error.to_string().contains("safe row limit"));
    assert_eq!(store.total_bytes(), live_bytes);

    let states = store.checkpoint_states().unwrap();
    let bytes = states.get("mv:m").unwrap();
    let (schema, batches) = ipc_to_schema_and_batches(bytes).unwrap();
    assert_eq!(schema, multiset_checkpoint_schema(&one_col_schema()));
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let counts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), i64::MAX);

    let mut restored = MvStore::new();
    restored
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    restored.restore_from_ipc("m", bytes).unwrap();
    let restored_count = restored
        .entries
        .get("m")
        .and_then(|entry| entry.multiset.as_ref())
        .and_then(|state| state.counts.values().next())
        .copied();
    assert_eq!(restored_count, Some(i64::MAX));
    assert!(restored.to_record_batch("m").is_err());
}

#[test]
fn multiset_read_rejects_excessive_expanded_bytes_before_conversion() {
    let value = "x".repeat(300);
    let plain_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
    let changelog_schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Utf8, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let changelog = RecordBatch::try_new(
        changelog_schema,
        vec![
            Arc::new(StringArray::from(vec![value.as_str()])),
            Arc::new(Int64Array::from(vec![i64::try_from(
                MAX_MULTISET_MATERIALIZED_ROWS,
            )
            .unwrap()])),
        ],
    )
    .unwrap();
    let mut store = MvStore::new();
    store
        .create_mv("m", plain_schema, MvStorageMode::Multiset)
        .unwrap();
    store.update_cycle("m", &[changelog]).unwrap();

    let error = store.to_record_batch("m").unwrap_err();
    assert!(error.to_string().contains("safe byte limit"));
}

#[test]
fn multiset_restore_rejects_invalid_counts_atomically() {
    let mut store = MvStore::new();
    store
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    store.update("m", &weight_batch_1col(&[(10, 2)]));
    let before = multiset_values(&store, "m");
    let before_bytes = store.total_bytes();

    let zero = counted_multiset_checkpoint_batch(&[(20, 0)]);
    let zero_bytes = batches_to_ipc(&zero.schema(), std::iter::once(&zero)).unwrap();
    let zero_error = store.restore_from_ipc("m", &zero_bytes).unwrap_err();
    assert!(zero_error.to_string().contains("must be positive"));
    assert_eq!(multiset_values(&store, "m"), before);
    assert_eq!(store.total_bytes(), before_bytes);

    let duplicate = counted_multiset_checkpoint_batch(&[(20, 1), (20, 2)]);
    let duplicate_bytes = batches_to_ipc(&duplicate.schema(), std::iter::once(&duplicate)).unwrap();
    let duplicate_error = store.restore_from_ipc("m", &duplicate_bytes).unwrap_err();
    assert!(duplicate_error.to_string().contains("duplicate value"));
    assert_eq!(multiset_values(&store, "m"), before);
    assert_eq!(store.total_bytes(), before_bytes);
}

#[test]
fn multiset_restore_rejects_expanded_snapshot_atomically() {
    let mut store = MvStore::new();
    store
        .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
        .unwrap();
    store.update("m", &weight_batch_1col(&[(10, 2)]));
    let before = multiset_values(&store, "m");
    let before_bytes = store.total_bytes();

    let expanded = plain_one_col_batch(&[20, 20]);
    let bytes = batches_to_ipc(&expanded.schema(), std::iter::once(&expanded)).unwrap();
    let error = store.restore_from_ipc("m", &bytes).unwrap_err();
    assert!(error.to_string().contains("schema or format mismatch"));
    assert_eq!(multiset_values(&store, "m"), before);
    assert_eq!(store.total_bytes(), before_bytes);
}
