use super::*;

const VNODES: u32 = 16;

fn pre_agg_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("total", DataType::Int64, true),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]))
}

async fn fresh_state() -> IncrementalAggState {
    let ctx = laminar_sql::create_session_context();
    // The seed row is for schema inference only — `try_from_sql` plans the
    // query, it does not fold table rows into the accumulators.
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
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![seed]]).unwrap();
    ctx.register_table("upstream", Arc::new(mem)).unwrap();
    IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT symbol, SUM(total) AS grand_total FROM upstream GROUP BY symbol",
        false,
    )
    .await
    .unwrap()
    .unwrap()
}

fn feed(state: &mut IncrementalAggState, rows: &[(&str, i64)]) {
    let batch = pre_agg_batch(rows);
    state.process_batch(&batch, 1000).unwrap();
}

fn pre_agg_batch(rows: &[(&str, i64)]) -> RecordBatch {
    let syms: Vec<&str> = rows.iter().map(|(s, _)| *s).collect();
    let tots: Vec<i64> = rows.iter().map(|(_, t)| *t).collect();
    RecordBatch::try_new(
        pre_agg_schema(),
        vec![
            Arc::new(arrow::array::StringArray::from(syms)),
            Arc::new(arrow::array::Int64Array::from(tots)),
            Arc::new(arrow::array::Int64Array::from(vec![1i64; rows.len()])),
        ],
    )
    .unwrap()
}

fn totals(state: &mut IncrementalAggState) -> std::collections::BTreeMap<String, i64> {
    let mut out = std::collections::BTreeMap::new();
    for b in state.emit().unwrap() {
        let syms = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let tots = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.insert(syms.value(i).to_string(), tots.value(i));
        }
    }
    out
}

#[tokio::test]
async fn per_vnode_checkpoint_merge_round_trips() {
    let mut a = fresh_state().await;
    feed(
        &mut a,
        &[
            ("AAPL", 100),
            ("GOOG", 200),
            ("MSFT", 50),
            ("AMZN", 75),
            ("META", 25),
            ("NVDA", 10),
        ],
    );

    // Partition by vnode, and the full single-blob checkpoint as a baseline.
    let by_vnode = a.checkpoint_groups_by_vnode(VNODES).unwrap();
    let full = a.checkpoint_groups().unwrap();

    // Every group lands in exactly one vnode slice — union == the whole.
    let partitioned: usize = by_vnode.values().map(|cp| cp.last_updated_ms.len()).sum();
    assert_eq!(
        partitioned,
        full.last_updated_ms.len(),
        "per-vnode slices must cover every group exactly once",
    );

    // Reassemble on a fresh node by merging each vnode's slice; the
    // aggregated output must match the original.
    let mut b = fresh_state().await;
    for slice in by_vnode.values() {
        b.merge_groups(slice).unwrap();
    }
    assert_eq!(
        totals(&mut b),
        totals(&mut a),
        "merging the per-vnode slices reproduces the original aggregate",
    );
}

#[tokio::test]
async fn full_replay_replaces_preexisting_rows_and_is_idempotent() {
    // Rehydration is fenced ahead of new owner processing. The committed FULL image is
    // authoritative, including when the same chain is delivered again after a lost ack.
    let mut donor = fresh_state().await;
    feed(&mut donor, &[("AAPL", 100), ("GOOG", 200)]);
    let by_vnode = donor.checkpoint_groups_by_vnode(VNODES).unwrap();

    let mut acquirer = fresh_state().await;
    feed(&mut acquirer, &[("AAPL", 5), ("GOOG", 5)]);
    for slice in by_vnode.values() {
        acquirer.merge_groups(slice).unwrap();
    }

    let first = totals(&mut acquirer);
    assert_eq!(first.get("AAPL"), Some(&100));
    assert_eq!(first.get("GOOG"), Some(&200));

    for slice in by_vnode.values() {
        acquirer.merge_groups(slice).unwrap();
    }
    assert_eq!(totals(&mut acquirer), first);
}

#[tokio::test]
async fn authoritative_replacement_rejects_a_chain_for_another_vnode_atomically() {
    let mut donor = fresh_state().await;
    feed(&mut donor, &[("AAPL", 100), ("GOOG", 200), ("MSFT", 50)]);
    let (actual_vnode, slice) = donor
        .checkpoint_groups_by_vnode(VNODES)
        .unwrap()
        .into_iter()
        .next()
        .expect("the donor must produce a vnode slice");
    let claimed_vnode = (actual_vnode + 1) % VNODES;

    let mut target = fresh_state().await;
    feed(&mut target, &[("LOCAL", 7)]);
    let before = rkyv::to_bytes::<rkyv::rancor::Error>(&target.checkpoint_groups().unwrap())
        .unwrap()
        .to_vec();

    let error = target
        .replace_vnode_chain(claimed_vnode, VNODES, &slice, &[])
        .unwrap_err();
    assert!(
        error.to_string().contains("key for another vnode"),
        "{error}"
    );

    let after = rkyv::to_bytes::<rkyv::rancor::Error>(&target.checkpoint_groups().unwrap())
        .unwrap()
        .to_vec();
    assert_eq!(after, before, "rejected chain changed the live state image");
}

#[tokio::test]
async fn aggregate_key_mapping_matches_shuffle_capture_and_drop() {
    let rows = [
        ("AAPL", 1),
        ("GOOG", 2),
        ("MSFT", 3),
        ("AMZN", 4),
        ("META", 5),
        ("NVDA", 6),
        ("ORCL", 7),
        ("TSLA", 8),
    ];
    let batch = pre_agg_batch(&rows);
    let expected = laminar_core::shuffle::row_vnodes(&batch, &[0], VNODES).unwrap();
    let vnode_count = NonZeroU32::new(VNODES).unwrap();

    let mut state = fresh_state().await;
    let encoded = state
        .row_converter
        .convert_columns(&[Arc::clone(batch.column(0))])
        .unwrap();
    let aggregate_mapping = encoded
        .iter()
        .map(|row| {
            IncrementalAggState::vnode_for_group_key(
                state.num_group_cols,
                &row.owned(),
                vnode_count,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(aggregate_mapping, expected);

    state.process_batch(&batch, 1000).unwrap();
    let expected_counts = expected.into_iter().fold(
        std::collections::BTreeMap::<u32, usize>::new(),
        |mut counts, vnode| {
            *counts.entry(vnode).or_default() += 1;
            counts
        },
    );
    let captures = state.checkpoint_groups_by_vnode(VNODES).unwrap();
    let capture_counts = captures
        .iter()
        .map(|(vnode, checkpoint)| (*vnode, checkpoint.last_updated_ms.len()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(capture_counts, expected_counts);

    let revoked_vnode = *expected_counts.keys().next().unwrap();
    let revoked = [revoked_vnode].into_iter().collect();
    state.drop_vnodes(&revoked, VNODES).unwrap();
    assert_eq!(
        state.groups.len(),
        rows.len() - expected_counts[&revoked_vnode]
    );
    assert!(state.groups.keys().all(|key| {
        IncrementalAggState::vnode_for_group_key(state.num_group_cols, key, vnode_count)
            != revoked_vnode
    }));
}

#[tokio::test]
async fn delta_capture_rejects_vnode_count_change_before_mutation() {
    let mut state = fresh_state().await;
    state.set_delta_enabled(true);
    feed(&mut state, &[("AAPL", 1), ("GOOG", 2), ("MSFT", 3)]);
    state.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
    assert_eq!(state.delta_vnode_count, NonZeroU32::new(VNODES));

    feed(&mut state, &[("AAPL", 10)]);
    let dirty_before = state.dirty_keys_by_vnode.clone();
    let emission_dirty_before = state.last_emitted_dirty_by_vnode.clone();
    let chains_before = state.delta_chain_len.clone();
    let force_rebase_before = state.force_rebase_vnodes.clone();

    let error = state
        .checkpoint_delta_by_vnode(VNODES * 2, 8)
        .err()
        .expect("a delta generation must not reinterpret its vnode space");
    assert!(error.to_string().contains(
        "aggregate delta vnode_count changed within one partition epoch: active=16, requested=32"
    ));
    assert_eq!(state.delta_vnode_count, NonZeroU32::new(VNODES));
    assert_eq!(state.dirty_keys_by_vnode, dirty_before);
    assert_eq!(state.last_emitted_dirty_by_vnode, emission_dirty_before);
    assert_eq!(state.delta_chain_len, chains_before);
    assert_eq!(state.force_rebase_vnodes, force_rebase_before);

    let full_error = state
        .checkpoint_groups_by_vnode(VNODES * 2)
        .err()
        .expect("a full capture cannot silently rotate an active delta generation");
    assert!(full_error.to_string().contains("active=16, requested=32"));
    assert_eq!(state.dirty_keys_by_vnode, dirty_before);
    assert_eq!(state.delta_chain_len, chains_before);

    state.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
}
