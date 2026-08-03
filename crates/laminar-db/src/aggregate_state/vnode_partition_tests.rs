use super::*;

const VNODES: u32 = 16;

async fn fresh_state() -> IncrementalAggState {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let seed = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["seed"])),
            Arc::new(arrow::array::Int64Array::from(vec![0])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![seed]]).unwrap();
    ctx.register_table("upstream", Arc::new(table)).unwrap();
    IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT symbol, SUM(total) AS grand_total FROM upstream GROUP BY symbol",
        false,
        KeyGroupCount::try_from(VNODES).unwrap(),
    )
    .await
    .unwrap()
    .unwrap()
}

fn batch(rows: &[(&str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(
                rows.iter().map(|(symbol, _)| *symbol).collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::Int64Array::from(
                rows.iter().map(|(_, total)| *total).collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::Int64Array::from(vec![1; rows.len()])),
        ],
    )
    .unwrap()
}

fn symbol_for_vnode(state: &IncrementalAggState, vnode: u32, start: u32) -> String {
    let vnode_count = NonZeroU32::new(VNODES).unwrap();
    for suffix in start..start + 100_000 {
        let symbol = format!("symbol-{suffix}");
        let input = batch(&[(symbol.as_str(), 0)]);
        let rows = state
            .row_converter
            .convert_columns(&[Arc::clone(input.column(0))])
            .unwrap();
        let key = rows.row(0).owned();
        if IncrementalAggState::vnode_for_group_key(state.num_group_cols, &key, vnode_count)
            == vnode
        {
            return symbol;
        }
    }
    panic!("no symbol found for vnode {vnode}");
}

#[tokio::test]
async fn capture_is_complete_then_only_serializes_dirty_vnodes() {
    let mut state = fresh_state().await;
    let key = symbol_for_vnode(&state, 3, 0);
    state
        .process_batch(&batch(&[(key.as_str(), 1)]), 100)
        .unwrap();
    let owned = (0..VNODES).collect::<Vec<_>>();

    let baseline = state.checkpoint_vnodes(&owned, VNODES).unwrap();
    assert!(baseline.iter().all(Option::is_some));
    assert!(state
        .checkpoint_vnodes(&owned, VNODES)
        .unwrap()
        .iter()
        .all(Option::is_none));

    state
        .process_batch(&batch(&[(key.as_str(), 2)]), 200)
        .unwrap();
    let dirty = state.checkpoint_vnodes(&owned, VNODES).unwrap();
    assert_eq!(dirty.iter().filter(|image| image.is_some()).count(), 1);
    assert!(dirty[3].is_some());

    state.force_full_vnode_capture();
    assert!(state
        .checkpoint_vnodes(&owned, VNODES)
        .unwrap()
        .iter()
        .all(Option::is_some));
}

#[tokio::test]
async fn capture_rejects_wrong_topology_and_unowned_resident_state() {
    let mut state = fresh_state().await;
    let key = symbol_for_vnode(&state, 5, 0);
    state
        .process_batch(&batch(&[(key.as_str(), 1)]), 100)
        .unwrap();

    assert!(state.checkpoint_vnodes(&[5], VNODES * 2).is_err());
    assert!(state.checkpoint_vnodes(&[0, 1], VNODES).is_err());
    let owned = (0..VNODES).collect::<Vec<_>>();
    assert!(state
        .checkpoint_vnodes(&owned, VNODES)
        .unwrap()
        .iter()
        .all(Option::is_some));
}

#[tokio::test]
async fn vnode_restore_replaces_atomically_and_preserves_other_vnodes() {
    let mut donor = fresh_state().await;
    let restored_key = symbol_for_vnode(&donor, 2, 0);
    donor
        .process_batch(&batch(&[(restored_key.as_str(), 7)]), 100)
        .unwrap();
    let image = donor
        .checkpoint_vnodes(&(0..VNODES).collect::<Vec<_>>(), VNODES)
        .unwrap()
        .remove(2)
        .unwrap();

    let mut live = fresh_state().await;
    let stale_key = symbol_for_vnode(&live, 2, 10_000);
    let retained_key = symbol_for_vnode(&live, 4, 20_000);
    live.process_batch(
        &batch(&[(stale_key.as_str(), 1), (retained_key.as_str(), 3)]),
        100,
    )
    .unwrap();
    let before = live.working_set_snapshot_for_test();

    let mut corrupt = image.clone();
    corrupt.fingerprint ^= 1;
    assert!(live.restore_vnode(2, VNODES, corrupt).is_err());
    assert_eq!(live.working_set_snapshot_for_test(), before);

    live.restore_vnode(2, VNODES, image).unwrap();
    let encoded = live
        .working_set_snapshot_for_test()
        .group_timestamps
        .into_keys()
        .collect::<Vec<_>>();
    let restored_row = batch(&[(restored_key.as_str(), 0)]);
    let stale_row = batch(&[(stale_key.as_str(), 0)]);
    let retained_row = batch(&[(retained_key.as_str(), 0)]);
    let encode = |input: &RecordBatch| {
        live.row_converter
            .convert_columns(&[Arc::clone(input.column(0))])
            .unwrap()
            .row(0)
            .as_ref()
            .to_vec()
    };
    assert!(encoded.contains(&encode(&restored_row)));
    assert!(!encoded.contains(&encode(&stale_row)));
    assert!(encoded.contains(&encode(&retained_row)));
}
