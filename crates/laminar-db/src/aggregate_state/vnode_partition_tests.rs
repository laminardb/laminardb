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
    let syms: Vec<&str> = rows.iter().map(|(s, _)| *s).collect();
    let tots: Vec<i64> = rows.iter().map(|(_, t)| *t).collect();
    let n = rows.len();
    let batch = RecordBatch::try_new(
        pre_agg_schema(),
        vec![
            Arc::new(arrow::array::StringArray::from(syms)),
            Arc::new(arrow::array::Int64Array::from(tots)),
            Arc::new(arrow::array::Int64Array::from(vec![1i64; n])),
        ],
    )
    .unwrap();
    state.process_batch(&batch, 1000).unwrap();
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
