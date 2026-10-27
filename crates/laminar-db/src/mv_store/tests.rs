use std::sync::Arc;

use super::*;
use arrow::array::{Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

fn keyed() -> MvStorageMode {
    MvStorageMode::changelog(ViewKey::Keyed(vec![0]))
}

fn multiset() -> MvStorageMode {
    MvStorageMode::changelog(ViewKey::MultisetByRow)
}

fn nonempty_vnodes(frames: &[(u32, bytes::Bytes)]) -> Vec<u32> {
    frames
        .iter()
        .filter(|(_, bytes)| bytes.as_ref() != [0])
        .map(|(vnode, _)| *vnode)
        .collect()
}

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
        .create_mv("mv1", test_schema(), MvStorageMode::append_rows(1_000))
        .unwrap();
    assert!(store.has_mv("mv1"));
    assert!(store.drop_mv("mv1"));
    assert!(!store.has_mv("mv1"));
    assert!(!store.drop_mv("mv1"));
}

#[test]
fn a_global_view_keeps_one_row_on_vnode_zero() {
    let mut store = MvStore::new();
    store
        .create_mv(
            "g",
            upsert_plain_schema(),
            MvStorageMode::changelog(ViewKey::Global),
        )
        .unwrap();
    store.update("g", &changelog_batch(&[(1, 10, 1)]));
    store.update("g", &changelog_batch(&[(1, 10, -1), (2, 25, 1)]));
    assert_eq!(snapshot_rows(&store, "g"), vec![(2, 25)]);
    assert_eq!(nonempty_vnodes(&store.full_view_frames("g")), vec![0]);

    store.update("g", &changelog_batch(&[(2, 25, -1)]));
    assert!(snapshot_rows(&store, "g").is_empty());
}

#[test]
fn row_retention_keeps_the_latest_rows() {
    let mut store = MvStore::new();
    store
        .create_mv("app", test_schema(), MvStorageMode::append_rows(3))
        .unwrap();
    for i in 0..4 {
        store.update("app", &make_batch(&[i], &["x"], &[f64::from(i)]));
    }
    let ids: Vec<i32> = id_names(&store, "app")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(ids, [1, 2, 3]);
}

#[test]
fn empty_mv_returns_empty_batch() {
    let mut store = MvStore::new();
    store
        .create_mv("empty", test_schema(), MvStorageMode::append_rows(1_000))
        .unwrap();
    store
        .create_mv("keyed", upsert_plain_schema(), keyed())
        .unwrap();
    let result = store.to_record_batch("empty").unwrap().unwrap();
    assert_eq!(result.num_rows(), 0);
    assert_eq!(result.schema(), test_schema());
    let keyed = store.to_record_batch("keyed").unwrap().unwrap();
    assert_eq!(keyed.num_rows(), 0);
    assert_eq!(keyed.schema(), upsert_plain_schema());
}

#[test]
fn nonexistent_returns_none() {
    let store = MvStore::new();
    assert!(store.to_record_batch("nope").unwrap().is_none());
}

#[test]
fn a_capture_seals_staged_rows_until_its_checkpoint_commits() {
    use crate::checkpoint_coordinator::CapturedViewLogSegment;

    let mut store = MvStore::new();
    store
        .create_mv("app", test_schema(), MvStorageMode::append_rows(1_000))
        .unwrap();
    store.update("app", &make_batch(&[1, 2], &["a", "b"], &[1.0, 2.0]));

    let capture = store.capture_view_logs(1, u64::MAX).unwrap();
    assert!(capture.sealed_rows());
    store.update("app", &make_batch(&[3], &["c"], &[3.0]));
    let (logs, _) = capture.encode(u64::MAX).unwrap();
    assert_eq!((logs[0].view.as_str(), logs[0].next_sequence), ("app", 1));
    let [CapturedViewLogSegment::New {
        sequence: 0,
        rows: 2,
        payload,
        ..
    }] = logs[0].segments.as_slice()
    else {
        panic!("unexpected segments {:?}", logs[0].segments);
    };
    let (schema, batches) = ipc_to_schema_and_batches(payload).unwrap();
    assert_eq!(schema, test_schema());
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    // Sealed rows stay readable until the checkpoint commits, and come back on abort.
    assert_eq!(id_names(&store, "app").len(), 3);
    store.abort_view_logs(1);
    assert_eq!(id_names(&store, "app").len(), 3);

    let (logs, _) = store
        .capture_view_logs(2, u64::MAX)
        .unwrap()
        .encode(u64::MAX)
        .unwrap();
    assert!(matches!(
        logs[0].segments.as_slice(),
        [CapturedViewLogSegment::New {
            sequence: 1,
            rows: 3,
            ..
        }]
    ));
    store.commit_view_logs(2);
    assert_eq!(store.total_bytes(), 0);
    let error = store.to_record_batch("app").unwrap_err();
    assert!(error.to_string().contains("read through its log"));

    let (logs, _) = store
        .capture_view_logs(3, u64::MAX)
        .unwrap()
        .encode(u64::MAX)
        .unwrap();
    assert!(matches!(
        logs[0].segments.as_slice(),
        [CapturedViewLogSegment::Retained {
            partition: 0,
            sequence: 1
        }]
    ));
    store.update("app", &make_batch(&[4], &["d"], &[4.0]));
    store.capture_view_logs(4, u64::MAX).unwrap();
    let overlapping = store.capture_view_logs(5, u64::MAX).err().unwrap();
    assert!(overlapping
        .to_string()
        .contains("neither committed nor aborted"));
}

#[test]
fn a_store_adopts_departed_logs_only_into_append_views() {
    use crate::checkpoint_coordinator::CapturedViewLogSegment;
    use laminar_core::checkpoint::{
        AdoptedSegment, AdoptedViewLog, ByteRange, StateChunkId, ViewLogSegment,
    };

    let mut store = MvStore::new();
    store
        .create_mv("app", test_schema(), MvStorageMode::append_rows(1_000))
        .unwrap();
    store
        .create_mv(
            "keyed",
            test_schema(),
            MvStorageMode::changelog(ViewKey::Keyed(vec![0])),
        )
        .unwrap();
    let log = |view: &str| AdoptedViewLog {
        view: view.into(),
        segments: vec![AdoptedSegment {
            segment: ViewLogSegment {
                partition: 0,
                sequence: 3,
                chunk: StateChunkId {
                    participant_id: 9,
                    checkpoint_id: 4,
                },
                range: ByteRange {
                    offset: 0,
                    length: 10,
                },
                sha256: "a".repeat(64),
                rows: 2,
                event_time: None,
            },
            object_length: 10,
            object_sha256: "a".repeat(64),
        }],
    };
    let error = store
        .adopt_view_logs(&[log("app"), log("keyed")])
        .unwrap_err();
    assert!(
        error.to_string().contains("not an append-only view"),
        "{error}"
    );

    store.adopt_view_logs(&[log("app")]).unwrap();
    let (logs, _) = store
        .capture_view_logs(1, u64::MAX)
        .unwrap()
        .encode(u64::MAX)
        .unwrap();
    assert!(
        matches!(
            logs[0].segments.as_slice(),
            [CapturedViewLogSegment::Adopted { sequence: 0, .. }]
        ),
        "{:?}",
        logs[0].segments
    );
}

#[test]
fn checkpoint_capture_is_point_in_time_after_live_mutation() {
    let mut store = MvStore::new();
    store
        .create_mv("upsert", upsert_plain_schema(), keyed())
        .unwrap();
    store
        .create_mv("multiset", one_col_schema(), multiset())
        .unwrap();

    store.update("upsert", &changelog_batch(&[(1, 10, 1)]));
    store.update("multiset", &weight_batch_1col(&[(10, 2)]));
    let vnodes: Vec<u32> = (0..256).collect();
    let upsert = store
        .capture_view_vnodes("upsert", &vnodes, 256, u64::MAX)
        .unwrap();
    let multiset = store
        .capture_view_vnodes("multiset", &vnodes, 256, u64::MAX)
        .unwrap();

    store.update("upsert", &changelog_batch(&[(1, 10, -1), (1, 99, 1)]));
    store.update("multiset", &weight_batch_1col(&[(10, -1), (20, 1)]));

    let mut restored = Vec::new();
    for (view, captures) in [("upsert", upsert), ("multiset", multiset)] {
        let mut target = store.restore_target(view).unwrap();
        for (vnode, bytes) in frames::encode_captures(captures) {
            target.restore_vnode(vnode, 256, &bytes).unwrap();
        }
        restored.push(target);
    }
    let image = store.recovery_image(restored, None, &[]).unwrap();

    assert_eq!(snapshot_rows(&image, "upsert"), vec![(1, 10)]);
    assert_eq!(multiset_values(&image, "multiset"), vec![10, 10]);
    assert_eq!(snapshot_rows(&store, "upsert"), vec![(1, 99)]);
    assert_eq!(multiset_values(&store, "multiset"), vec![10, 20]);
}

#[test]
fn view_capture_budget_rejection_preserves_live_state() {
    let mut store = MvStore::new();
    store
        .create_mv("upsert", upsert_plain_schema(), keyed())
        .unwrap();
    store.update("upsert", &changelog_batch(&[(1, 10, 1), (2, 20, 1)]));
    let before = snapshot_rows(&store, "upsert");
    let before_bytes = store.total_bytes();
    let vnodes: Vec<u32> = (0..256).collect();

    let error = store
        .capture_view_vnodes("upsert", &vnodes, 256, 1)
        .expect_err("capture above the remaining checkpoint budget must fail");

    assert!(error.to_string().contains("remaining capture budget"));
    assert_eq!(snapshot_rows(&store, "upsert"), before);
    assert_eq!(store.total_bytes(), before_bytes);
}

#[test]
fn view_log_encoding_enforces_the_remaining_budget() {
    let mut store = MvStore::new();
    for name in ["a", "b"] {
        store
            .create_mv(name, test_schema(), MvStorageMode::append_rows(1_000))
            .unwrap();
        store.update(name, &make_batch(&[1, 2], &["one", "two"], &[1.0, 2.0]));
    }
    let (_, retained) = store
        .capture_view_logs(1, u64::MAX)
        .unwrap()
        .encode(u64::MAX)
        .unwrap();
    assert!(retained > 0);
    store.abort_view_logs(1);

    let error = store
        .capture_view_logs(2, u64::MAX)
        .unwrap()
        .encode(1)
        .unwrap_err();
    assert!(error.to_string().contains("log serialization"));
    store.abort_view_logs(2);

    let estimate = store.capture_view_logs(3, 1).err().unwrap();
    assert!(estimate
        .to_string()
        .contains("exceeds the staged-state cap"));
    assert_eq!(id_names(&store, "a").len(), 2);
    assert_eq!(id_names(&store, "b").len(), 2);
}

#[test]
fn an_empty_checkpoint_has_an_empty_log_and_an_empty_frame_per_view_vnode() {
    let mut store = MvStore::new();
    store
        .create_mv("append", test_schema(), MvStorageMode::append_rows(1_000))
        .unwrap();
    store
        .create_mv("upsert", upsert_plain_schema(), keyed())
        .unwrap();
    store
        .create_mv("multiset", one_col_schema(), multiset())
        .unwrap();

    let capture = store.capture_view_logs(1, u64::MAX).unwrap();
    assert!(!capture.sealed_rows());
    let (logs, _) = capture.encode(u64::MAX).unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].view, "append");
    assert!(logs[0].segments.is_empty());
    for view in ["upsert", "multiset"] {
        let frames = store.full_view_frames(view);
        assert_eq!(frames.len(), 256, "{view}");
        assert!(nonempty_vnodes(&frames).is_empty(), "{view}");
    }
}

#[test]
fn the_first_view_capture_is_full_and_later_ones_hold_only_changed_vnodes() {
    let mut store = MvStore::new();
    store
        .create_mv("u", upsert_plain_schema(), keyed())
        .unwrap();
    store.update("u", &changelog_batch(&[(1, 10, 1), (2, 20, 1), (3, 30, 1)]));
    assert_eq!(store.encoded_view_frames("u").len(), 256);
    assert!(store.encoded_view_frames("u").is_empty());

    store.update("u", &changelog_batch(&[(2, 20, -1), (2, 21, 1)]));
    let changed = store.encoded_view_frames("u");
    assert_eq!(changed.len(), 1);
    let image = store.image_from_view_frames("u", &changed).unwrap();
    assert_eq!(snapshot_rows(&image, "u"), vec![(2, 21)]);
}

#[test]
fn equal_view_state_encodes_to_equal_frames() {
    let rows = [(1, 10, 1), (2, 20, 1), (3, 30, 1), (4, 40, 1)];
    let mut forward = MvStore::new();
    let mut backward = MvStore::new();
    for store in [&mut forward, &mut backward] {
        store
            .create_mv("u", upsert_plain_schema(), keyed())
            .unwrap();
    }
    forward.update("u", &changelog_batch(&rows));
    let mut reversed = rows;
    reversed.reverse();
    for row in reversed {
        backward.update("u", &changelog_batch(&[row]));
    }
    assert_eq!(
        forward.full_view_frames("u"),
        backward.full_view_frames("u")
    );
}

#[test]
fn keyed_slices_use_the_partition_key_vnode_of_their_key() {
    use laminar_core::state::PartitionKeyCodecV1;

    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("day", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
    ]));
    let changelog_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("day", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let codec = PartitionKeyCodecV1::try_new([DataType::Utf8, DataType::Int64]).unwrap();
    let vnode_count = std::num::NonZeroU32::new(256).unwrap();
    for (symbol, day) in [("a", 1), ("b", 1), ("a", 2), ("zzz", 900)] {
        let mut store = MvStore::new();
        store
            .create_mv(
                "v",
                Arc::clone(&schema),
                MvStorageMode::changelog(ViewKey::Keyed(vec![0, 1])),
            )
            .unwrap();
        let symbols: arrow::array::ArrayRef = Arc::new(StringArray::from(vec![symbol]));
        let days: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![day]));
        let batch = RecordBatch::try_new(
            Arc::clone(&changelog_schema),
            vec![
                Arc::clone(&symbols),
                Arc::clone(&days),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        store.update("v", &batch);
        let encoded = codec.encode_columns(&[symbols, days]).unwrap();
        let expected = PartitionKeyCodecV1::vnode_for_encoded(encoded.row(0).data(), vnode_count);
        assert_eq!(
            nonempty_vnodes(&store.full_view_frames("v")),
            vec![expected],
            "{symbol}/{day}"
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
        .create_mv("mv1", test_schema(), MvStorageMode::append_rows(1_000))
        .unwrap();
    store.update("mv1", &make_batch(&[1], &["a"], &[1.0]));
    assert_eq!(store.to_record_batch("mv1").unwrap().unwrap().num_rows(), 1);

    store
        .create_mv("mv1", test_schema(), MvStorageMode::append_rows(1_000))
        .unwrap();
    assert_eq!(store.to_record_batch("mv1").unwrap().unwrap().num_rows(), 0);
}

#[test]
fn fresh_image_is_empty_and_preserves_the_hot_path_handle() {
    let mut store = MvStore::new();
    store
        .create_mv("app", test_schema(), MvStorageMode::append_rows(1_000))
        .unwrap();
    store.update("app", &make_batch(&[1], &["a"], &[1.0]));
    let handle = store.has_any_handle();

    let image = store.fresh_image().unwrap();

    assert!(Arc::ptr_eq(&handle, &image.has_any_handle()));
    assert_eq!(image.to_record_batch("app").unwrap().unwrap().num_rows(), 0);
    assert_eq!(store.to_record_batch("app").unwrap().unwrap().num_rows(), 1);
}

#[test]
fn recovery_image_requires_an_exact_view_log_inventory() {
    use laminar_core::checkpoint::{CheckpointManifest, ViewLog};

    let mut store = MvStore::new();
    store
        .create_mv("app", test_schema(), MvStorageMode::append_rows(1_000))
        .unwrap();
    store
        .create_mv("keyed", upsert_plain_schema(), keyed())
        .unwrap();
    let log = |view: &str| ViewLog {
        view: view.into(),
        next_sequence: 4,
        frontier: Some(100),
        idle: false,
        segments: Vec::new(),
    };
    let mut manifest = CheckpointManifest::new(7, 7);

    let missing = store
        .recovery_image(Vec::new(), Some(&manifest), &[])
        .err()
        .unwrap();
    assert!(missing
        .to_string()
        .contains("no log for append-only view 'app'"));

    manifest.view_logs = vec![log("app"), log("ghost")];
    let unknown = store
        .recovery_image(Vec::new(), Some(&manifest), &[])
        .err()
        .unwrap();
    assert!(unknown.to_string().contains("log for 'ghost'"));

    manifest.view_logs = vec![log("app")];
    let image = store
        .recovery_image(Vec::new(), Some(&manifest), &[])
        .unwrap();
    assert_eq!(image.to_record_batch("app").unwrap().unwrap().num_rows(), 0);
}

#[test]
fn failed_recovery_image_never_mutates_live_state() {
    let mut store = MvStore::new();
    for name in ["first", "second"] {
        store
            .create_mv(name, test_schema(), MvStorageMode::append_rows(1_000))
            .unwrap();
    }
    store
        .create_mv("u", upsert_plain_schema(), keyed())
        .unwrap();
    store.update("first", &make_batch(&[1], &["live"], &[1.0]));
    store.update("second", &make_batch(&[2], &["live"], &[2.0]));
    store.update("u", &changelog_batch(&[(1, 10, 1)]));
    let live_bytes = store.total_bytes();

    let error = store
        .recovery_image(
            Vec::new(),
            Some(&laminar_core::checkpoint::CheckpointManifest::new(7, 7)),
            &[],
        )
        .err()
        .expect("a checkpoint without the views' logs must fail");
    assert!(error.to_string().contains("no log for append-only view"));
    let corrupt_frame = store.image_from_view_frames("u", &[(0, bytes::Bytes::from_static(b"x"))]);
    assert!(corrupt_frame.is_err());
    assert_eq!(store.total_bytes(), live_bytes);
    assert_eq!(id_names(&store, "first"), [(1, "live".to_string())]);
    assert_eq!(id_names(&store, "second"), [(2, "live".to_string())]);
    assert_eq!(snapshot_rows(&store, "u"), vec![(1, 10)]);
}

#[test]
fn upsert_applies_inserts_changes_and_deletes() {
    let mut store = MvStore::new();
    store
        .create_mv("u", upsert_plain_schema(), keyed())
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
        .create_mv("u", upsert_plain_schema(), keyed())
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
        .create_mv("u", upsert_plain_schema(), keyed())
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
fn upsert_frames_round_trip() {
    let mut store = MvStore::new();
    store
        .create_mv("u", upsert_plain_schema(), keyed())
        .unwrap();
    store.update("u", &changelog_batch(&[(1, 10, 1), (2, 20, 1), (3, 30, 1)]));
    store.update("u", &changelog_batch(&[(2, 20, -1)]));
    let before = snapshot_rows(&store, "u");

    let frames = store.full_view_frames("u");
    let mut restored = store.image_from_view_frames("u", &frames).unwrap();
    assert_eq!(snapshot_rows(&restored, "u"), before);
    assert_eq!(restored.total_bytes(), store.total_bytes());

    // A restored store keeps applying changelog correctly.
    restored.update("u", &changelog_batch(&[(1, 10, -1), (1, 99, 1)]));
    assert_eq!(snapshot_rows(&restored, "u"), vec![(1, 99), (3, 30)]);
}

#[test]
fn failed_upsert_restore_preserves_live_state() {
    let mut store = MvStore::new();
    store
        .create_mv("u", upsert_plain_schema(), keyed())
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
    let error = store
        .image_from_view_frames("u", &[(0, bytes.into())])
        .err()
        .expect("a frame with the wrong schema must fail");
    assert!(error.to_string().contains("schema does not match"));
    assert_eq!(snapshot_rows(&store, "u"), before);
    assert_eq!(store.total_bytes(), before_bytes);
}

#[test]
fn upsert_frames_reject_misplaced_and_repeated_keys() {
    let mut store = MvStore::new();
    store
        .create_mv("u", upsert_plain_schema(), keyed())
        .unwrap();
    store.update("u", &changelog_batch(&[(1, 10, 1)]));
    let frames = store.full_view_frames("u");
    let [owner] = nonempty_vnodes(&frames)[..] else {
        panic!("one key occupies one vnode");
    };

    let encode = |rows: &[(i64, i64)]| -> bytes::Bytes {
        batches_to_ipc(
            &upsert_plain_schema(),
            std::iter::once(&upsert_snapshot_batch(rows)),
        )
        .unwrap()
        .into()
    };
    let misplaced = store
        .image_from_view_frames("u", &[((owner + 1) % 256, encode(&[(1, 10)]))])
        .err()
        .expect("a row on the wrong vnode must fail");
    assert!(misplaced.to_string().contains("placed on vnode"));
    let repeated = store
        .image_from_view_frames("u", &[(owner, encode(&[(1, 10), (1, 20)]))])
        .err()
        .expect("a repeated key must fail");
    assert!(repeated.to_string().contains("repeats a key"));

    let restored = store
        .image_from_view_frames("u", &[(owner, encode(&[(1, 20)]))])
        .unwrap();
    let mut single = store.fresh_image().unwrap();
    single.update("u", &changelog_batch(&[(1, 20, 1)]));
    assert_eq!(restored.total_bytes(), single.total_bytes());
    assert!(
        restored.total_bytes() > 16,
        "owned keys and row metadata must also be charged"
    );
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
        .create_mv("m", upsert_plain_schema(), multiset())
        .unwrap();
    store.update("m", &changelog_batch(&[(1, 10, 1), (2, 20, 1)]));
    assert_eq!(snapshot_rows(&store, "m"), vec![(1, 10), (2, 20)]);
    // Change k=1: retract old full row, insert new full row.
    store.update("m", &changelog_batch(&[(1, 10, -1), (1, 15, 1)]));
    assert_eq!(snapshot_rows(&store, "m"), vec![(1, 15), (2, 20)]);
}

#[test]
fn a_multiset_placed_by_key_keeps_every_row_of_a_key_on_its_vnode() {
    let mut store = MvStore::new();
    store
        .create_mv(
            "m",
            upsert_plain_schema(),
            MvStorageMode::changelog(ViewKey::MultisetByKey(vec![0])),
        )
        .unwrap();
    store.update("m", &changelog_batch(&[(7, 1, 1), (7, 2, 1), (7, 3, 1)]));
    let frames = store.full_view_frames("m");
    assert_eq!(nonempty_vnodes(&frames).len(), 1);
    let image = store.image_from_view_frames("m", &frames).unwrap();
    assert_eq!(snapshot_rows(&image, "m"), vec![(7, 1), (7, 2), (7, 3)]);
}

#[test]
fn multiset_tracks_duplicate_rows() {
    // Key-dropping projection: two upstream keys with the same value v=10 → multiplicity 2.
    let mut store = MvStore::new();
    store.create_mv("m", one_col_schema(), multiset()).unwrap();
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
    store.create_mv("m", one_col_schema(), multiset()).unwrap();
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

fn only_count(store: &MvStore, name: &str) -> Option<i64> {
    store
        .entries
        .get(name)
        .and_then(|entry| entry.multiset.as_ref())
        .and_then(|state| {
            state
                .slices
                .slots()
                .flat_map(|(_, slice)| slice.values())
                .next()
                .copied()
        })
}

#[test]
fn multiset_rejects_negative_overflow_and_null_weight_without_mutation() {
    let mut store = MvStore::new();
    store.create_mv("m", one_col_schema(), multiset()).unwrap();
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
        .create_mv("m", one_col_schema(), multiset())
        .unwrap();
    overflow_store
        .update_cycle("m", &[weight_batch_1col(&[(10, i64::MAX)])])
        .unwrap();
    let before_overflow = overflow_store.total_bytes();
    let overflow = overflow_store
        .update_cycle("m", &[weight_batch_1col(&[(10, 1)])])
        .expect_err("multiplicity overflow must fail");
    assert!(overflow.to_string().contains("multiplicity overflow"));
    assert_eq!(only_count(&overflow_store, "m"), Some(i64::MAX));
    assert_eq!(overflow_store.total_bytes(), before_overflow);
}

#[test]
fn multiset_frames_round_trip_preserves_multiplicity() {
    let mut store = MvStore::new();
    store.create_mv("m", one_col_schema(), multiset()).unwrap();
    store.update("m", &weight_batch_1col(&[(10, 1), (10, 1), (20, 1)]));
    let before = multiset_values(&store, "m");
    assert_eq!(before, vec![10, 10, 20]);

    let frames = store.full_view_frames("m");
    let mut restored = store.image_from_view_frames("m", &frames).unwrap();
    // Multiplicity (10 appears twice) survives the round-trip.
    assert_eq!(multiset_values(&restored, "m"), before);
    // And a restored store keeps netting: retract one 10.
    restored.update("m", &weight_batch_1col(&[(10, -1)]));
    assert_eq!(multiset_values(&restored, "m"), vec![10, 20]);
}

#[test]
fn multiset_frames_are_counted_and_do_not_materialize_multiplicity() {
    let mut store = MvStore::new();
    store.create_mv("m", one_col_schema(), multiset()).unwrap();
    store
        .update_cycle("m", &[weight_batch_1col(&[(10, i64::MAX)])])
        .unwrap();
    let live_bytes = store.total_bytes();

    let read_error = store.to_record_batch("m").unwrap_err();
    assert!(read_error.to_string().contains("safe row limit"));
    assert_eq!(store.total_bytes(), live_bytes);

    let frames = store.full_view_frames("m");
    let [owner] = nonempty_vnodes(&frames)[..] else {
        panic!("one value occupies one vnode");
    };
    let (schema, batches) = ipc_to_schema_and_batches(&frames[owner as usize].1).unwrap();
    assert_eq!(schema, multiset_checkpoint_schema(&one_col_schema()));
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let counts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), i64::MAX);

    let restored = store.image_from_view_frames("m", &frames).unwrap();
    assert_eq!(only_count(&restored, "m"), Some(i64::MAX));
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
    store.create_mv("m", plain_schema, multiset()).unwrap();
    store.update_cycle("m", &[changelog]).unwrap();

    let error = store.to_record_batch("m").unwrap_err();
    assert!(error.to_string().contains("safe byte limit"));
}

#[test]
fn multiset_restore_rejects_invalid_counts() {
    let mut store = MvStore::new();
    store.create_mv("m", one_col_schema(), multiset()).unwrap();
    store.update("m", &weight_batch_1col(&[(20, 1)]));
    let [owner] = nonempty_vnodes(&store.full_view_frames("m"))[..] else {
        panic!("one value occupies one vnode");
    };
    let encode = |rows: &[(i64, i64)]| -> bytes::Bytes {
        let batch = counted_multiset_checkpoint_batch(rows);
        batches_to_ipc(&batch.schema(), std::iter::once(&batch))
            .unwrap()
            .into()
    };

    let zero_error = store
        .image_from_view_frames("m", &[(owner, encode(&[(20, 0)]))])
        .err()
        .expect("a zero count must fail");
    assert!(zero_error.to_string().contains("must be positive"));

    let duplicate_error = store
        .image_from_view_frames("m", &[(owner, encode(&[(20, 1), (20, 2)]))])
        .err()
        .expect("a duplicate value must fail");
    assert!(duplicate_error.to_string().contains("duplicate value"));
    assert_eq!(multiset_values(&store, "m"), vec![20]);
}

#[test]
fn multiset_restore_rejects_expanded_snapshot() {
    let mut store = MvStore::new();
    store.create_mv("m", one_col_schema(), multiset()).unwrap();
    let expanded = plain_one_col_batch(&[20, 20]);
    let bytes = batches_to_ipc(&expanded.schema(), std::iter::once(&expanded)).unwrap();
    let error = store
        .image_from_view_frames("m", &[(0, bytes.into())])
        .err()
        .expect("an expanded snapshot must fail");
    assert!(error
        .to_string()
        .contains("schema or format does not match"));
}
