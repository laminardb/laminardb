use super::*;

const VNODES: u32 = 16;

#[test]
fn vnode_transition_capacity_arithmetic_accepts_boundaries_and_rejects_overflow() {
    assert_eq!(
        checked_vnode_transition_capacity(usize::MAX - 1, 1).unwrap(),
        usize::MAX
    );
    assert!(checked_vnode_transition_capacity(usize::MAX, 1)
        .unwrap_err()
        .to_string()
        .contains("capacity overflow"));
    assert_eq!(checked_vnode_transition_final_count(2, 1, 1).unwrap(), 2);
    assert!(checked_vnode_transition_final_count(0, 1, 0).is_err());
    assert!(checked_vnode_transition_final_count(usize::MAX, 0, 1).is_err());
}

fn pre_agg_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("total", DataType::Int64, true),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]))
}

async fn state_with_changelog(emit_changelog: bool) -> IncrementalAggState {
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
        emit_changelog,
        KeyGroupCount::try_from(VNODES).unwrap(),
    )
    .await
    .unwrap()
    .unwrap()
}

async fn fresh_state() -> IncrementalAggState {
    state_with_changelog(false).await
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

fn checkpoint_bytes(state: &mut IncrementalAggState) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&state.checkpoint_groups().unwrap())
        .unwrap()
        .to_vec()
}

fn vnode_slot_identities(state: &IncrementalAggState) -> Vec<Option<*const ()>> {
    (0..VNODES)
        .map(|vnode| state.vnode_slot_identity_for_test(vnode))
        .collect()
}

fn symbol_for_vnode(state: &IncrementalAggState, vnode: u32, start: u32) -> String {
    let vnode_count = NonZeroU32::new(VNODES).unwrap();
    for suffix in start..start + 100_000 {
        let symbol = format!("symbol-{suffix}");
        let batch = pre_agg_batch(&[(symbol.as_str(), 0)]);
        let rows = state
            .row_converter
            .convert_columns(&[Arc::clone(batch.column(0))])
            .unwrap();
        let key = rows.iter().next().unwrap().owned();
        if IncrementalAggState::vnode_for_group_key(state.num_group_cols, &key, vnode_count)
            == vnode
        {
            return symbol;
        }
    }
    panic!("no symbol found for vnode {vnode}");
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
async fn whole_state_restore_clears_stale_vnode_delta_metadata_as_the_new_baseline() {
    let mut donor = fresh_state().await;
    let donor_key = symbol_for_vnode(&donor, 3, 0);
    feed(&mut donor, &[(donor_key.as_str(), 91)]);
    let donor_checkpoint = donor.checkpoint_groups().unwrap();
    let donor_totals = totals(&mut donor);

    let mut live = fresh_state().await;
    live.set_delta_enabled(true);
    let first_live_key = symbol_for_vnode(&live, 0, 0);
    let second_live_key = symbol_for_vnode(&live, 1, 0);
    feed(
        &mut live,
        &[
            (first_live_key.as_str(), 10),
            (second_live_key.as_str(), 20),
        ],
    );
    live.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
    assert_eq!(
        live.working_set_snapshot_for_test().delta_chain_len.len(),
        2,
        "fixture must install stale per-vnode chain metadata"
    );

    live.restore_groups(&donor_checkpoint).unwrap();
    let restored = live.working_set_snapshot_for_test();
    assert!(restored.checkpoint_dirty_keys.is_empty());
    assert!(restored.last_emitted_dirty_keys.is_empty());
    assert!(restored.delta_chain_len.is_empty());
    assert!(restored.force_full_rebase_vnodes.is_empty());
    assert_eq!(live.logical_group_count_for_test(), 1);
    assert_eq!(totals(&mut live), donor_totals);

    live.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
    live.force_full_rebase();
    let forced = live.working_set_snapshot_for_test();
    assert!(forced.delta_chain_len.is_empty());
    assert_eq!(
        forced.force_full_rebase_vnodes,
        std::collections::BTreeSet::from([3]),
        "fixture must install a stale forced-rebase marker"
    );

    live.restore_groups(&donor_checkpoint).unwrap();
    let restored = live.working_set_snapshot_for_test();
    assert!(restored.delta_chain_len.is_empty());
    assert!(restored.force_full_rebase_vnodes.is_empty());
    assert_eq!(live.logical_group_count_for_test(), 1);
    assert_eq!(totals(&mut live), donor_totals);
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
async fn prepared_vnode_transition_late_corruption_aborts_the_complete_roster() {
    let mut donor = fresh_state().await;
    let first_key = symbol_for_vnode(&donor, 0, 0);
    let later_key = symbol_for_vnode(&donor, 1, 0);
    feed(
        &mut donor,
        &[(first_key.as_str(), 10), (later_key.as_str(), 20)],
    );
    let mut bases = donor.checkpoint_groups_by_vnode(VNODES).unwrap();
    let first = bases.remove(&0).unwrap();
    let mut corrupt_later = bases.remove(&1).unwrap();
    corrupt_later.fingerprint ^= 1;

    let mut live = fresh_state().await;
    live.set_delta_enabled(true);
    let live_first = symbol_for_vnode(&live, 0, 10_000);
    let live_other = symbol_for_vnode(&live, 2, 10_000);
    feed(
        &mut live,
        &[(live_first.as_str(), 70), (live_other.as_str(), 80)],
    );
    live.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
    feed(&mut live, &[(live_first.as_str(), 7)]);

    let checkpoint_before = checkpoint_bytes(&mut live);
    let working_set_before = live.working_set_snapshot_for_test();
    let slot_identities_before = vnode_slot_identities(&live);
    let restores = [
        AggVnodeRestore {
            vnode: 0,
            base: &first,
            deltas: &[],
        },
        AggVnodeRestore {
            vnode: 1,
            base: &corrupt_later,
            deltas: &[],
        },
    ];

    let error = live
        .prepare_vnode_transition(VNODES, &restores, &Default::default())
        .err()
        .expect("the corrupt later vnode must reject the complete roster");
    assert!(
        error.to_string().contains("fingerprint mismatch"),
        "{error}"
    );
    assert_eq!(checkpoint_bytes(&mut live), checkpoint_before);
    assert_eq!(live.working_set_snapshot_for_test(), working_set_before);
    assert_eq!(vnode_slot_identities(&live), slot_identities_before);
}

#[tokio::test]
async fn vnode_transition_rejects_key_group_count_mismatch_before_mutation() {
    let mut state = fresh_state().await;
    state.set_delta_enabled(true);
    feed(&mut state, &[("AAPL", 1), ("GOOG", 2)]);
    state.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
    feed(&mut state, &[("AAPL", 3)]);

    let checkpoint_before = checkpoint_bytes(&mut state);
    let working_set_before = state.working_set_snapshot_for_test();
    let slot_identities_before = vnode_slot_identities(&state);

    let error = state
        .prepare_vnode_transition(VNODES * 2, &[], &Default::default())
        .err()
        .expect("transition must not reinterpret the aggregate routing topology");
    assert!(error.to_string().contains("state=16, requested=32"));
    assert_eq!(checkpoint_bytes(&mut state), checkpoint_before);
    assert_eq!(state.working_set_snapshot_for_test(), working_set_before);
    assert_eq!(vnode_slot_identities(&state), slot_identities_before);
    assert!(state.delta_tracking_active);
}

#[tokio::test]
async fn prepared_vnode_transition_mixed_revoke_restore_publishes_exact_image() {
    let restore_vnode = 0;
    let revoked_vnode = 1;
    let retained_vnode = 2;

    let mut donor = fresh_state().await;
    let restored_key = symbol_for_vnode(&donor, restore_vnode, 0);
    let restored_new_key = symbol_for_vnode(&donor, restore_vnode, 10_000);
    let post_cut_key = symbol_for_vnode(&donor, restore_vnode, 20_000);
    let revoked_key = symbol_for_vnode(&donor, revoked_vnode, 0);
    let retained_key = symbol_for_vnode(&donor, retained_vnode, 0);
    feed(
        &mut donor,
        &[(restored_key.as_str(), 10), (restored_new_key.as_str(), 20)],
    );
    let restored_base = donor
        .checkpoint_groups_by_vnode(VNODES)
        .unwrap()
        .remove(&restore_vnode)
        .unwrap();

    let mut live = fresh_state().await;
    live.set_delta_enabled(true);
    feed(
        &mut live,
        &[
            (restored_key.as_str(), 10),
            (restored_new_key.as_str(), 20),
            (revoked_key.as_str(), 30),
            (retained_key.as_str(), 40),
        ],
    );
    live.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
    feed(
        &mut live,
        &[(restored_key.as_str(), 90), (post_cut_key.as_str(), 50)],
    );

    let restores = [AggVnodeRestore {
        vnode: restore_vnode,
        base: &restored_base,
        deltas: &[],
    }];
    let revoked = [revoked_vnode]
        .into_iter()
        .collect::<rustc_hash::FxHashSet<_>>();
    let slot_identities_before = vnode_slot_identities(&live);
    assert!(slot_identities_before[restore_vnode as usize].is_some());
    assert!(slot_identities_before[revoked_vnode as usize].is_some());
    assert!(slot_identities_before[retained_vnode as usize].is_some());
    let prepared = live
        .prepare_vnode_transition(VNODES, &restores, &revoked)
        .unwrap();
    let retired = live.publish_prepared_vnode_transition(prepared);
    IncrementalAggState::finish_vnode_transition(retired);

    let slot_identities_after = vnode_slot_identities(&live);
    for vnode in 0..VNODES {
        if vnode == restore_vnode || vnode == revoked_vnode {
            assert_ne!(
                slot_identities_after[vnode as usize], slot_identities_before[vnode as usize],
                "transitioned vnode {vnode} retained its old slot"
            );
        } else {
            assert_eq!(
                slot_identities_after[vnode as usize], slot_identities_before[vnode as usize],
                "retained vnode {vnode} changed slot identity"
            );
        }
    }
    assert!(slot_identities_after[restore_vnode as usize].is_some());
    assert!(slot_identities_after[revoked_vnode as usize].is_none());

    let output = totals(&mut live);
    assert!(!output.contains_key(&post_cut_key));
    assert!(!output.contains_key(&revoked_key));
    assert_eq!(
        output,
        std::collections::BTreeMap::from([
            (restored_key, 10),
            (restored_new_key, 20),
            (retained_key, 40),
        ])
    );
    assert_eq!(live.logical_group_count_for_test(), 3);
    let snapshot = live.working_set_snapshot_for_test();
    assert!(!snapshot.checkpoint_dirty_keys.contains_key(&revoked_vnode));
    assert!(!snapshot.delta_chain_len.contains_key(&restore_vnode));
    assert!(!snapshot.delta_chain_len.contains_key(&revoked_vnode));
}

#[tokio::test]
async fn prepared_vnode_transition_restore_wins_overlap_and_empty_image_stays_sparse() {
    let restored_vnode = 0;
    let emptied_vnode = 1;
    let retained_vnode = 2;

    let mut donor = fresh_state().await;
    let restored_key = symbol_for_vnode(&donor, restored_vnode, 0);
    feed(&mut donor, &[(restored_key.as_str(), 10)]);
    let restored_base = donor
        .checkpoint_groups_by_vnode(VNODES)
        .unwrap()
        .remove(&restored_vnode)
        .unwrap();

    let mut live = fresh_state().await;
    let stale_restored_key = symbol_for_vnode(&live, restored_vnode, 10_000);
    let emptied_key = symbol_for_vnode(&live, emptied_vnode, 0);
    let retained_key = symbol_for_vnode(&live, retained_vnode, 0);
    feed(
        &mut live,
        &[
            (stale_restored_key.as_str(), 90),
            (emptied_key.as_str(), 20),
            (retained_key.as_str(), 30),
        ],
    );
    let empty_base = live.empty_checkpoint();
    let restores = [
        AggVnodeRestore {
            vnode: restored_vnode,
            base: &restored_base,
            deltas: &[],
        },
        AggVnodeRestore {
            vnode: emptied_vnode,
            base: &empty_base,
            deltas: &[],
        },
    ];
    let revoked = [restored_vnode, emptied_vnode]
        .into_iter()
        .collect::<rustc_hash::FxHashSet<_>>();

    let prepared = live
        .prepare_vnode_transition(VNODES, &restores, &revoked)
        .unwrap();
    let retired = live.publish_prepared_vnode_transition(prepared);
    IncrementalAggState::finish_vnode_transition(retired);

    assert_eq!(
        totals(&mut live),
        std::collections::BTreeMap::from([(restored_key, 10), (retained_key, 30)]),
        "an authoritative restore must win over the same vnode's revoke marker",
    );
    assert!(live.vnode_slot_identity_for_test(restored_vnode).is_some());
    assert!(live.vnode_slot_identity_for_test(emptied_vnode).is_none());
    assert_eq!(
        live.active_vnodes_for_test(),
        &[restored_vnode, retained_vnode]
    );
    assert_eq!(live.logical_group_count_for_test(), 2);
}

#[tokio::test]
async fn prepared_vnode_transition_abort_preserves_every_live_slot_and_snapshot() {
    let mut donor = fresh_state().await;
    let restored_key = symbol_for_vnode(&donor, 0, 0);
    feed(&mut donor, &[(restored_key.as_str(), 10)]);
    let restored_base = donor
        .checkpoint_groups_by_vnode(VNODES)
        .unwrap()
        .remove(&0)
        .unwrap();

    let mut live = fresh_state().await;
    live.set_delta_enabled(true);
    let live_key = symbol_for_vnode(&live, 0, 10_000);
    let unaffected_key = symbol_for_vnode(&live, 2, 10_000);
    feed(
        &mut live,
        &[(live_key.as_str(), 70), (unaffected_key.as_str(), 80)],
    );
    live.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
    feed(&mut live, &[(live_key.as_str(), 7)]);

    let totals_before = totals(&mut live);
    let checkpoint_before = checkpoint_bytes(&mut live);
    let working_set_before = live.working_set_snapshot_for_test();
    let slot_identities_before = vnode_slot_identities(&live);
    let restores = [AggVnodeRestore {
        vnode: 0,
        base: &restored_base,
        deltas: &[],
    }];

    let prepared = live
        .prepare_vnode_transition(VNODES, &restores, &Default::default())
        .unwrap();
    drop(prepared);

    assert_eq!(totals(&mut live), totals_before);
    assert_eq!(checkpoint_bytes(&mut live), checkpoint_before);
    assert_eq!(live.working_set_snapshot_for_test(), working_set_before);
    assert_eq!(vnode_slot_identities(&live), slot_identities_before);
}

#[tokio::test]
async fn prepared_vnode_transition_revoke_clears_forced_rebase_marker() {
    let revoked_vnode = 1;
    let mut live = fresh_state().await;
    live.set_delta_enabled(true);
    let revoked_key = symbol_for_vnode(&live, revoked_vnode, 0);
    feed(&mut live, &[(revoked_key.as_str(), 10)]);
    assert!(live
        .checkpoint_delta_by_vnode(VNODES, 8)
        .unwrap()
        .contains_key(&revoked_vnode));
    live.force_full_rebase();
    assert!(live
        .working_set_snapshot_for_test()
        .force_full_rebase_vnodes
        .contains(&revoked_vnode));

    let revoked = [revoked_vnode]
        .into_iter()
        .collect::<rustc_hash::FxHashSet<_>>();
    let prepared = live
        .prepare_vnode_transition(VNODES, &[], &revoked)
        .unwrap();
    let retired = live.publish_prepared_vnode_transition(prepared);
    IncrementalAggState::finish_vnode_transition(retired);

    let snapshot = live.working_set_snapshot_for_test();
    assert!(!snapshot.force_full_rebase_vnodes.contains(&revoked_vnode));
    assert!(!snapshot.delta_chain_len.contains_key(&revoked_vnode));
    assert!(
        !live
            .checkpoint_delta_by_vnode(VNODES, 8)
            .unwrap()
            .contains_key(&revoked_vnode),
        "a revoked vnode must not emit an empty forced re-base"
    );
}

#[tokio::test]
async fn prepared_vnode_transition_delta_only_new_changelog_group_is_dirty() {
    let vnode = 0;
    let mut donor = state_with_changelog(true).await;
    let restored_key = symbol_for_vnode(&donor, vnode, 0);
    feed(&mut donor, &[(restored_key.as_str(), 10)]);
    let changed = donor
        .checkpoint_groups_by_vnode(VNODES)
        .unwrap()
        .remove(&vnode)
        .unwrap();
    let delta = AggVnodeDelta { changed };

    let mut live = state_with_changelog(true).await;
    let empty_base = live.empty_checkpoint();
    let deltas = [delta];
    let restores = [AggVnodeRestore {
        vnode,
        base: &empty_base,
        deltas: &deltas,
    }];
    let prepared = live
        .prepare_vnode_transition(VNODES, &restores, &Default::default())
        .unwrap();
    let retired = live.publish_prepared_vnode_transition(prepared);
    IncrementalAggState::finish_vnode_transition(retired);

    assert_eq!(live.logical_group_count_for_test(), 1);
    let final_key = live
        .group_keys_for_test()
        .into_iter()
        .next()
        .expect("restored delta group");
    assert!(live.contains_group_for_test(&final_key));
    let final_key_bytes: &[u8] = final_key.as_ref();
    assert!(
        live.working_set_snapshot_for_test()
            .emit_dirty_keys
            .contains(final_key_bytes),
        "a group introduced only by a delta must be emitted after restore"
    );
}

#[tokio::test]
async fn prepared_vnode_transition_group_limit_failure_preserves_live_image() {
    let restored_vnode = 0;
    let mut donor = fresh_state().await;
    let first = symbol_for_vnode(&donor, restored_vnode, 0);
    let second = symbol_for_vnode(&donor, restored_vnode, 10_000);
    feed(&mut donor, &[(first.as_str(), 10), (second.as_str(), 20)]);
    let restored_base = donor
        .checkpoint_groups_by_vnode(VNODES)
        .unwrap()
        .remove(&restored_vnode)
        .unwrap();

    let mut live = fresh_state().await;
    let retained = symbol_for_vnode(&live, 2, 0);
    feed(&mut live, &[(retained.as_str(), 30)]);
    live.set_max_groups_for_test(2);
    let checkpoint_before = checkpoint_bytes(&mut live);
    let working_set_before = live.working_set_snapshot_for_test();
    let slot_identities_before = vnode_slot_identities(&live);
    let restores = [AggVnodeRestore {
        vnode: restored_vnode,
        base: &restored_base,
        deltas: &[],
    }];

    let error = live
        .prepare_vnode_transition(VNODES, &restores, &Default::default())
        .err()
        .expect("the final three-group image must exceed the two-group limit");
    assert!(
        error.to_string().contains("group limit exceeded"),
        "{error}"
    );
    assert_eq!(checkpoint_bytes(&mut live), checkpoint_before);
    assert_eq!(live.working_set_snapshot_for_test(), working_set_before);
    assert_eq!(vnode_slot_identities(&live), slot_identities_before);
}

#[tokio::test]
async fn prepared_vnode_transition_accepts_exact_final_group_limit() {
    let restored_vnode = 0;
    let mut donor = fresh_state().await;
    let restored = symbol_for_vnode(&donor, restored_vnode, 0);
    feed(&mut donor, &[(restored.as_str(), 10)]);
    let restored_base = donor
        .checkpoint_groups_by_vnode(VNODES)
        .unwrap()
        .remove(&restored_vnode)
        .unwrap();

    let mut live = fresh_state().await;
    let retained = symbol_for_vnode(&live, 2, 0);
    feed(&mut live, &[(retained.as_str(), 30)]);
    live.set_max_groups_for_test(2);
    let restores = [AggVnodeRestore {
        vnode: restored_vnode,
        base: &restored_base,
        deltas: &[],
    }];

    let prepared = live
        .prepare_vnode_transition(VNODES, &restores, &Default::default())
        .unwrap();
    let retired = live.publish_prepared_vnode_transition(prepared);
    IncrementalAggState::finish_vnode_transition(retired);

    assert_eq!(live.logical_group_count_for_test(), 2);
    assert_eq!(
        totals(&mut live),
        std::collections::BTreeMap::from([(restored, 10), (retained, 30)])
    );
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

    let mut state = fresh_state().await;
    assert_eq!(
        state.key_group_count(),
        KeyGroupCount::try_from(VNODES).unwrap()
    );
    let vnode_count = state.routing_vnode_count();
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
    assert_eq!(
        state.active_vnodes_for_test(),
        expected_counts.keys().copied().collect::<Vec<_>>(),
        "the active roster must contain exactly the resident vnode slots",
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
        state.active_vnodes_for_test(),
        expected_counts
            .keys()
            .copied()
            .filter(|vnode| *vnode != revoked_vnode)
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        state.logical_group_count_for_test(),
        rows.len() - expected_counts[&revoked_vnode]
    );
    assert!(state.group_keys_for_test().iter().all(|key| {
        IncrementalAggState::vnode_for_group_key(state.num_group_cols, key, vnode_count)
            != revoked_vnode
    }));
}

#[tokio::test]
async fn vnode_capture_rejects_key_group_count_mismatch_before_mutation() {
    let mut state = fresh_state().await;
    state.set_delta_enabled(true);
    feed(&mut state, &[("AAPL", 1), ("GOOG", 2), ("MSFT", 3)]);
    let checkpoint_before = checkpoint_bytes(&mut state);
    let working_set_before = state.working_set_snapshot_for_test();
    let slot_identities_before = vnode_slot_identities(&state);
    assert!(working_set_before.checkpoint_dirty_keys.is_empty());
    assert!(working_set_before.last_emitted_dirty_keys.is_empty());

    let full_error = state
        .checkpoint_groups_by_vnode(VNODES * 2)
        .err()
        .expect("a first full capture must not choose a different routing topology");
    assert!(full_error
        .to_string()
        .contains("aggregate key-group count mismatch: state=16, requested=32"));
    let delta_error = state
        .checkpoint_delta_by_vnode(VNODES * 2, 8)
        .err()
        .expect("a first delta capture must not choose a different routing topology");
    assert!(delta_error.to_string().contains("state=16, requested=32"));
    assert!(!state.delta_tracking_active);
    assert_eq!(checkpoint_bytes(&mut state), checkpoint_before);
    assert_eq!(state.working_set_snapshot_for_test(), working_set_before);
    assert_eq!(vnode_slot_identities(&state), slot_identities_before);

    state.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
    assert!(state.delta_tracking_active);

    feed(&mut state, &[("AAPL", 10)]);
    let working_set_before = state.working_set_snapshot_for_test();
    let slot_identities_before = vnode_slot_identities(&state);

    let error = state
        .checkpoint_delta_by_vnode(VNODES * 2, 8)
        .err()
        .expect("a delta generation must not reinterpret its vnode space");
    assert!(error.to_string().contains("state=16, requested=32"));
    assert!(state.delta_tracking_active);
    assert_eq!(state.working_set_snapshot_for_test(), working_set_before);
    assert_eq!(vnode_slot_identities(&state), slot_identities_before);

    let full_error = state
        .checkpoint_groups_by_vnode(VNODES * 2)
        .err()
        .expect("a full capture cannot silently rotate an active delta generation");
    assert!(full_error.to_string().contains("state=16, requested=32"));
    assert_eq!(state.working_set_snapshot_for_test(), working_set_before);
    assert_eq!(vnode_slot_identities(&state), slot_identities_before);

    state.checkpoint_delta_by_vnode(VNODES, 8).unwrap();
}
