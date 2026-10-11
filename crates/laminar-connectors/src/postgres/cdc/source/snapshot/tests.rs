use std::collections::BTreeSet;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::super::drain::{chunk_order_key, wal_order_key};
use super::super::tests::{
    begin, commit, copy_gate, delete, insert, queue, relation_message, row, source_layout,
    streaming_source, update, OID, V,
};
use super::merge::{encode_key, row_action, KeyClass, RowAction};
use super::*;
use crate::config::ConnectorState;
use crate::connector::SourceMutation::{Put, Tombstone};
use crate::connector::{
    source_mutations, source_row_positions, SourceConnector, SourceRowPositions,
};
use crate::postgres::cdc::config::OutputMode;
use crate::postgres::cdc::postgres_io::IndexKeyColumn;
use crate::postgres::cdc::source::{CommittedTransaction, RowImage, RowValues};

fn snapshot(text: &str) -> PgSnapshot {
    PgSnapshot::parse(text).unwrap()
}

fn random_snapshot(rng: &mut StdRng) -> PgSnapshot {
    let xmin = rng.random_range(3_u64..30);
    let xmax = rng.random_range(xmin..40);
    let xip: Vec<String> = (xmin..xmax)
        .filter(|_| rng.random_bool(0.4))
        .map(|xid| xid.to_string())
        .collect();
    snapshot(&format!("{xmin}:{xmax}:{}", xip.join(",")))
}

// ── Visibility ──

#[test]
fn pg_snapshots_round_trip_and_reject_malformed_text() {
    for text in ["900:905:901,903", "900:900:", "3:3:"] {
        assert_eq!(snapshot(text).to_string(), text);
    }
    for text in [
        "905:900:",
        "900:905:906",
        "900:905:899",
        "900:905",
        "x:1:",
        "1:2:a",
    ] {
        assert!(PgSnapshot::parse(text).is_err(), "{text}");
    }
    let parsed = snapshot("900:905:903,901");
    assert_eq!(
        parsed.to_string(),
        "900:905:901,903",
        "xip is kept in order"
    );
    for (xid, visible) in [
        (899, true),
        (900, true),
        (901, false),
        (902, true),
        (904, true),
    ] {
        assert_eq!(parsed.sees(xid), visible, "{xid}");
    }
    assert!(!parsed.sees(905) && !parsed.sees(10_000));
}

/// A 32-bit stream xid widens to the full xid within 2³¹ of the reference `xmax`, across epochs.
#[test]
fn stream_xids_widen_to_the_nearest_full_xid() {
    let mut rng = StdRng::seed_from_u64(0x5eed_0001);
    for _ in 0..10_000 {
        let reference = rng.random_range((1_u64 << 31)..(1_u64 << 40));
        let full = reference - (1 << 31) + 1 + rng.random_range(0..(1_u64 << 32) - 2);
        let low = u32::try_from(full % (1 << 32)).unwrap();
        let widened = snapshot(&format!("{reference}:{reference}:"))
            .widen(low)
            .unwrap();
        assert_eq!(widened, full, "reference {reference}");
    }
    let epoch_one = snapshot(&format!("{0}:{0}:", (1_u64 << 32) + 5));
    assert_eq!(epoch_one.widen(0xffff_fff0).unwrap(), (1 << 32) - 16);
    assert_eq!(epoch_one.widen(10).unwrap(), (1 << 32) + 10);
    assert!(
        snapshot("5:5:").widen(0xffff_fff0).is_err(),
        "no xid precedes the first epoch"
    );
}

/// The floor guard holds exactly when the chunk snapshot sees everything the floor sees.
#[test]
fn the_floor_guard_matches_its_definition() {
    let mut rng = StdRng::seed_from_u64(0x5eed_0002);
    for _ in 0..20_000 {
        let floor = random_snapshot(&mut rng);
        let chunk = random_snapshot(&mut rng);
        let expected = (0..50).all(|xid| !floor.sees(xid) || chunk.sees(xid));
        assert_eq!(
            chunk.covers(&floor),
            expected,
            "floor {floor}, chunk {chunk}"
        );
        assert_eq!(
            chunk.behind(&floor, 64).is_empty(),
            expected,
            "floor {floor}, chunk {chunk}"
        );
    }
}

/// The slot counted a transaction as committed when it decoded its commit record; the procarray
/// keeps it running until the commit returns, which a synchronous standby can delay.
#[test]
fn the_floor_guard_waits_for_transactions_the_slot_counted_but_snapshots_still_run() {
    let floor = snapshot("100:110:105");
    assert!(
        !snapshot("100:111:107").covers(&floor),
        "107 is still running"
    );
    assert!(
        !snapshot("100:108:").covers(&floor),
        "108 and 109 are at or above the chunk's xmax"
    );
    assert_eq!(snapshot("100:108:").behind(&floor, 64), [108, 109]);
    assert!(
        snapshot("100:111:105").covers(&floor),
        "running at the floor, so streamed after it"
    );
    assert!(snapshot("112:112:").covers(&floor));
}

#[test]
fn the_pending_guard_keeps_only_transactions_the_snapshot_misses() {
    let mut pending = BTreeSet::from([5, 101, 104, 120]);
    assert!(!snapshot("100:110:104").settle(&mut pending));
    assert_eq!(pending, BTreeSet::from([104, 120]));
    assert!(snapshot("121:121:").settle(&mut pending));
    assert!(pending.is_empty());
}

// ── Classes and merges ──

#[test]
fn stream_rows_follow_the_class_table_in_both_modes() {
    use RowAction::{Drop, Emit, Merge, MergeAndEmit};
    let hidden = KeyClass::Held { visible: false };
    let seen = KeyClass::Held { visible: true };
    let table = [
        (KeyClass::Copied, [Emit, Emit, Emit, Emit]),
        (seen, [Drop, Drop, Drop, Emit]),
        (hidden, [Merge, Merge, Merge, MergeAndEmit]),
        (KeyClass::Uncopied, [Drop, Drop, Drop, Emit]),
    ];
    let images = [
        RowImage::Weighted(1),
        RowImage::Weighted(-1),
        RowImage::Put,
        RowImage::Tombstone,
    ];
    for (class, actions) in table {
        for (image, action) in images.into_iter().zip(actions) {
            assert_eq!(row_action(class, image), action, "{class:?} {image:?}");
        }
    }
}

fn values(id: &str, status: &str, qty: &str, note: Option<&str>) -> RowValues {
    [Some(id), Some(status), Some(qty), note]
        .into_iter()
        .map(|value| value.map(|text| text.as_bytes().into()))
        .collect()
}

fn key(values: &RowValues) -> Box<[u8]> {
    encode_key(values, &[0]).unwrap()
}

#[test]
fn held_rows_take_upserts_and_tombstones_and_count_their_bytes() {
    let mut held = HeldRows::default();
    let a = values("1", "A", "1", None);
    held.apply(RowImage::Put, key(&a), a.clone(), 10).unwrap();
    let b = values("2", "B", "2", None);
    held.apply(RowImage::Put, key(&b), b.clone(), 20).unwrap();
    let a2 = values("1", "A2", "3", None);
    held.apply(RowImage::Put, key(&a2), a2.clone(), 15).unwrap();
    held.apply(RowImage::Tombstone, key(&b), b.clone(), 20)
        .unwrap();
    held.apply(RowImage::Tombstone, key(&b), b, 20).unwrap();
    assert_eq!(held.bytes(), 15);
    let rows: Vec<(usize, RowValues)> = held
        .rows_from(0)
        .map(|(next, values, _)| (next, values.clone()))
        .collect();
    assert_eq!(rows, [(1, a2)], "replaced in place, deleted rows gone");
    assert_eq!(held.rows_from(1).count(), 0);
    assert_eq!(held.rows_from(9).count(), 0);
}

#[test]
fn changelog_retractions_must_match_the_held_image_exactly() {
    let mut held = HeldRows::default();
    let a = values("1", "A", "1", Some("note"));
    held.apply(RowImage::Weighted(1), key(&a), a.clone(), 10)
        .unwrap();
    let drifted = values("1", "A", "1", None);
    let error = held
        .apply(RowImage::Weighted(-1), key(&drifted), drifted, 10)
        .unwrap_err();
    assert!(matches!(error, ConnectorError::Internal(_)), "{error}");
    assert!(held
        .apply(RowImage::Weighted(1), key(&a), a.clone(), 10)
        .is_err());
    held.apply(RowImage::Weighted(-1), key(&a), a.clone(), 10)
        .unwrap();
    assert!(
        held.apply(RowImage::Weighted(-1), key(&a), a, 10).is_err(),
        "a retraction of a row that is not held"
    );
    assert_eq!(held.bytes(), 0);
}

// ── Draining against a held chunk ──

const HIGH: u64 = 0x900;

/// A copying source with keys up to 10 copied, holding the chunk (10, 20] of `fetched` rows read
/// under `read_at`.
fn copying_source(mode: OutputMode, read_at: &str, fetched: &[RowValues]) -> PostgresCdcSource {
    let mut source = streaming_source(mode);
    let mut copy = copy_gate().holding(snapshot(read_at), Lsn::new(HIGH));
    copy.bound = Some(vec!["10".into()]);
    let layout = source_layout(mode);
    if let Some(held) = copy.held.as_mut() {
        held.upper = Upper::Key(vec!["20".into()]);
        for row in fetched {
            let text = row.iter().flatten().map(|value| value.len()).sum();
            let bytes = layout.planned_row_bytes(text).unwrap();
            held.rows
                .apply(RowImage::Put, key(row), row.clone(), bytes)
                .unwrap();
        }
    }
    source.phase = Phase::Snapshot(Box::new(copy));
    source.enqueue_wal_data(relation_message());
    source
}

/// Where each staged key lies, as the server answers for integer keys.
fn integer_ranges(source: &PostgresCdcSource) -> Vec<(bool, bool)> {
    let Phase::Snapshot(copy) = &source.phase else {
        panic!("not copying");
    };
    let number = |text: &str| text.parse::<i64>().unwrap();
    let bound = copy.bound.as_ref().map(|bound| number(&bound[0]));
    let upper = copy.held.as_ref().map(|held| match &held.upper {
        Upper::Key(key) => Some(number(&key[0])),
        Upper::End => None,
    });
    source
        .committed
        .iter()
        .flat_map(|transaction| transaction.staged.iter().flatten())
        .map(|row| {
            let key = number(std::str::from_utf8(row.values[0].as_deref().unwrap()).unwrap());
            let held = match upper {
                None => false,
                Some(None) => true,
                Some(Some(upper)) => key <= upper,
            };
            (bound.is_some_and(|bound| key <= bound), held)
        })
        .collect()
}

fn drain(source: &mut PostgresCdcSource) -> Result<Option<SourceBatch>, ConnectorError> {
    source.process_pending_messages()?;
    let ranges = integer_ranges(source);
    let selected: Vec<CommittedTransaction> = source.committed.drain(..).collect();
    source.apply_drained(&selected, &ranges)
}

fn ids(batch: &SourceBatch) -> Vec<i64> {
    batch
        .records
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

/// Mark the held chunk as passed by a keepalive.
fn pass_held(source: &mut PostgresCdcSource) {
    if let Phase::Snapshot(copy) = &mut source.phase {
        if let Some(held) = copy.held.as_mut() {
            held.passed = true;
        }
    }
}

/// Emit the held chunk as if a keepalive had passed its `high`.
fn emit_chunk(source: &mut PostgresCdcSource) -> Option<SourceBatch> {
    pass_held(source);
    source.emit_held_chunk(1_024).unwrap()
}

fn fetched(ids: &[&str]) -> Vec<RowValues> {
    ids.iter().map(|id| values(id, "A", "1", None)).collect()
}

/// Key changes into, inside, and out of the held range, inserts of copied keys, and deletes:
/// upsert tombstones are emitted in every class, invisible changes merge into the chunk, and the
/// chunk is the table's state at `high`.
#[tokio::test]
async fn upsert_changes_merge_into_the_held_chunk_and_tombstones_always_flow() {
    let held = fetched(&["11", "12", "14", "15", "16", "17", "18", "19", "20"]);
    let mut source = copying_source(OutputMode::Upsert, "1:1:", &held);
    let old = |id| row(id, "A", "1");
    let new = |id| row(id, "B", "2");
    source.enqueue_wal_data(begin(0x400));
    for change in [
        update(Some(b'O'), &row("5", "A", "1"), &new("15")),
        update(Some(b'O'), &old("12"), &new("13")),
        update(Some(b'O'), &old("18"), &new("25")),
        insert(&row("3", "C", "3")),
        delete(b'O', &old("16")),
    ] {
        source.enqueue_wal_data(change);
    }
    source.enqueue_wal_data(commit(0x400, 0x410));

    let batch = drain(&mut source).unwrap().expect("emitted rows");
    assert_eq!(ids(&batch), [5, 12, 18, 3, 16]);
    let encoded = super::super::tests::encoded(&source, batch);
    let mutations = source_mutations(&encoded).unwrap().unwrap();
    assert_eq!(
        (0..mutations.len())
            .map(|row| mutations.get(row).unwrap())
            .collect::<Vec<_>>(),
        [Tombstone, Tombstone, Tombstone, Put, Tombstone]
    );
    let positions = source_row_positions(&encoded).unwrap().unwrap();
    let rows: Vec<(Vec<u8>, u32)> = (0..positions.len())
        .map(|row| {
            let position = positions.get(row).unwrap();
            (position.order_key.to_vec(), position.sub_offset)
        })
        .collect();
    let key = wal_order_key(Lsn::new(0x410)).to_vec();
    assert_eq!(
        rows,
        [0, 2, 4, 6, 7].map(|offset| (key.clone(), offset)),
        "streamed rows keep their transaction's position and row index"
    );
    let Phase::Snapshot(copy) = &source.phase else {
        panic!("not copying");
    };
    assert_eq!(copy.pending, BTreeSet::from([0x400]));
    assert_eq!(source.polled_lsn, Lsn::new(0x410));

    let chunk = emit_chunk(&mut source).expect("the chunk");
    let mut copied = ids(&chunk);
    copied.sort_unstable();
    assert_eq!(copied, [11, 13, 14, 15, 17, 19, 20]);
    let statuses = chunk.records.column(1).as_string::<i32>();
    let fifteen = ids(&chunk).iter().position(|&id| id == 15).unwrap();
    assert_eq!(
        statuses.value(fifteen),
        "B",
        "the merged update replaced the copied row"
    );
    let Phase::Snapshot(copy) = &source.phase else {
        panic!("not copying");
    };
    assert_eq!(copy.bound, Some(vec!["20".to_string()]));
    assert_eq!(copy.chunks, 1);
}

/// Changelog merges: retractions remove the held image they match, insertions add rows, and
/// only changes of copied keys are emitted. A retraction that differs fails closed.
#[tokio::test]
async fn changelog_changes_merge_exactly_and_a_drifted_retraction_fails_closed() {
    let held = fetched(&["11", "12", "14", "16", "17", "18", "19", "20"]);
    let mut source = copying_source(OutputMode::Changelog, "1:1:", &held);
    source.enqueue_wal_data(begin(0x400));
    for change in [
        update(Some(b'O'), &row("5", "A", "1"), &row("15", "B", "2")),
        update(Some(b'O'), &row("12", "A", "1"), &row("13", "B", "2")),
        update(Some(b'O'), &row("18", "A", "1"), &row("25", "B", "2")),
        insert(&row("3", "C", "3")),
        delete(b'O', &row("16", "A", "1")),
    ] {
        source.enqueue_wal_data(change);
    }
    source.enqueue_wal_data(commit(0x400, 0x410));
    let batch = drain(&mut source).unwrap().expect("emitted rows");
    assert_eq!(ids(&batch), [5, 3]);
    let weights = batch.records.column(4).as_primitive::<Int64Type>();
    assert_eq!(weights.values(), &[-1, 1]);
    let mut copied = ids(&emit_chunk(&mut source).expect("the chunk"));
    copied.sort_unstable();
    assert_eq!(copied, [11, 13, 14, 15, 17, 19, 20]);

    let mut source = copying_source(OutputMode::Changelog, "1:1:", &held);
    source.enqueue_wal_data(begin(0x500));
    source.enqueue_wal_data(delete(b'O', &row("14", "Z", "9")));
    source.enqueue_wal_data(commit(0x500, 0x510));
    let error = drain(&mut source).unwrap_err();
    assert!(matches!(error, ConnectorError::Internal(_)), "{error}");
}

/// An unchanged TOAST value in a merged update is restored from the full old row.
#[tokio::test]
async fn merged_updates_restore_unchanged_toast_from_the_old_row() {
    let held = vec![values("14", "A", "1", Some("toasted"))];
    let mut source = copying_source(OutputMode::Upsert, "1:1:", &held);
    source.enqueue_wal_data(begin(0x400));
    source.enqueue_wal_data(update(
        Some(b'O'),
        &[V::T("14"), V::T("A"), V::T("1"), V::T("toasted")],
        &[V::T("14"), V::T("A"), V::T("7"), V::Unchanged],
    ));
    source.enqueue_wal_data(commit(0x400, 0x410));
    assert!(drain(&mut source).unwrap().is_none());
    let chunk = emit_chunk(&mut source).expect("the chunk");
    let notes = chunk.records.column(3).as_string::<i32>();
    assert_eq!(notes.value(0), "toasted");
    let qty = chunk
        .records
        .column(2)
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(qty.value(0), 7);
}

/// Changes the chunk's snapshot already includes are dropped, except upsert tombstones.
#[tokio::test]
async fn changes_the_chunk_already_sees_are_dropped_but_tombstones_still_flow() {
    let held = fetched(&["11", "12"]);
    let mut source = copying_source(OutputMode::Upsert, "2000:2000:", &held);
    source.enqueue_wal_data(begin(0x400));
    source.enqueue_wal_data(insert(&row("13", "A", "1")));
    source.enqueue_wal_data(delete(b'O', &row("14", "A", "1")));
    source.enqueue_wal_data(commit(0x400, 0x410));
    let batch = drain(&mut source).unwrap().expect("the tombstone");
    assert_eq!(ids(&batch), [14]);
    let mut copied = ids(&emit_chunk(&mut source).expect("the chunk"));
    copied.sort_unstable();
    assert_eq!(
        copied,
        [11, 12],
        "the chunk already reflects the visible transaction"
    );
}

/// A chunk that merges outgrow the Arrow-build budget is dropped and fetched again smaller;
/// its merged transaction stays pending for the next chunk's snapshot.
#[tokio::test]
async fn a_chunk_outgrown_by_merges_is_abandoned() {
    let mut source = copying_source(OutputMode::Upsert, "1:1:", &fetched(&["11"]));
    source.config.max_buffered_bytes = 1024 * 1024;
    if let Phase::Snapshot(copy) = &mut source.phase {
        copy.fetch_rows = 8;
    }
    let note = "n".repeat(20 * 1024);
    source.enqueue_wal_data(begin(0x400));
    for id in 12..=20 {
        let id = id.to_string();
        source.enqueue_wal_data(insert(&[V::T(&id), V::Null, V::Null, V::T(&note)]));
    }
    source.enqueue_wal_data(commit(0x400, 0x410));
    assert!(drain(&mut source).unwrap().is_none());
    let Phase::Snapshot(copy) = &source.phase else {
        panic!("not copying");
    };
    assert!(copy.held.is_none(), "the chunk is abandoned");
    assert_eq!(copy.fetch_rows, 4);
    assert_eq!(copy.pending, BTreeSet::from([0x400]));
    assert_eq!(
        copy.bound,
        Some(vec!["10".to_string()]),
        "nothing was copied"
    );
}

/// While a chunk is held the drain stops before the first transaction ending after `high`.
#[test]
fn the_drain_stops_at_the_held_chunks_high() {
    let mut source = copying_source(OutputMode::Upsert, "1:1:", &[]);
    for (final_lsn, end) in [
        (0x100, 0x110),
        (0x200, 0x900),
        (0x950, 0x960),
        (0x970, 0x980),
    ] {
        source.committed.push_back(CommittedTransaction {
            end_lsn: Lsn::new(end),
            xid: u32::try_from(final_lsn).unwrap(),
            records: None,
            mutations: Box::new([]),
            staged: Some(Box::new([])),
            retained_bytes: 0,
        });
    }
    assert_eq!(source.leading_transactions(100), 2);
    if let Phase::Snapshot(copy) = &mut source.phase {
        copy.held = None;
    }
    assert_eq!(source.leading_transactions(100), 4);
    assert_eq!(source.leading_transactions(0), 0);
}

/// A TRUNCATE committed before `high` fails the source before the chunk could be emitted.
#[tokio::test]
async fn truncate_before_high_fails_the_copy_before_its_chunk_is_emitted() {
    let mut source = copying_source(OutputMode::Upsert, "1:1:", &fetched(&["11"]));
    let _reader = queue(&mut source, Vec::new()).await;
    let mut truncate = vec![b'T'];
    truncate.extend_from_slice(&1_u32.to_be_bytes());
    truncate.push(0);
    truncate.extend_from_slice(&OID.to_be_bytes());
    source.enqueue_wal_data(begin(0x400));
    source.enqueue_wal_data(truncate);
    source.enqueue_wal_data(commit(0x400, 0x410));
    let error = source.poll_batch(100).await.unwrap_err();
    assert!(error.to_string().contains("TRUNCATE"), "{error}");
    assert_eq!(source.state, ConnectorState::Failed);
    let Phase::Snapshot(copy) = &source.phase else {
        panic!("not copying");
    };
    assert!(copy.held.is_some(), "the chunk was never emitted");
}

/// The copy's progress and the held chunk's next piece: `(chunks, bound, emitted)`.
fn copy_progress(source: &PostgresCdcSource) -> (u64, Option<Vec<String>>, Option<usize>) {
    let Phase::Snapshot(copy) = &source.phase else {
        panic!("not copying");
    };
    let emitted = copy.held.as_ref().map(|held| held.emitted);
    (copy.chunks, copy.bound.clone(), emitted)
}

/// A chunk larger than `max_records` is emitted in pieces under one order key with continuing
/// sub-offsets, and only its last piece records it.
#[test]
fn a_chunk_is_emitted_in_pieces_and_recorded_with_its_last() {
    let rows = fetched(&["11", "12", "13", "14", "15"]);
    let mut source = copying_source(OutputMode::Upsert, "1:1:", &rows);
    pass_held(&mut source);
    assert!(source.emit_held_chunk(0).unwrap().is_none());
    let bound = Some(vec!["10".to_string()]);
    let mut pieces = Vec::new();
    for emitted in [2, 4] {
        pieces.push(source.emit_held_chunk(2).unwrap().expect("a piece"));
        assert_eq!(copy_progress(&source), (0, bound.clone(), Some(emitted)));
    }
    pieces.push(source.emit_held_chunk(2).unwrap().expect("the last piece"));
    assert_eq!(copy_progress(&source), (1, Some(vec!["20".into()]), None));
    let ids: Vec<Vec<i64>> = pieces.iter().map(ids).collect();
    assert_eq!(ids, [vec![11, 12], vec![13, 14], vec![15]]);
    let positions: Vec<&SourceRowPositions> = pieces
        .iter()
        .map(|piece| piece.row_positions().unwrap())
        .collect();
    let sub_offsets: Vec<u32> = positions
        .iter()
        .flat_map(|positions| positions.sub_offset().values().iter().copied())
        .collect();
    assert_eq!(sub_offsets, [0, 1, 2, 3, 4]);
    let order_key = chunk_order_key(Lsn::new(HIGH), 0);
    assert!(positions
        .iter()
        .flat_map(|positions| positions.order_key().iter())
        .all(|key| key == Some(order_key.as_slice())));
}

/// A piece that fails to build leaves the copy's progress where it was, so a barrier captured
/// after the failed poll cannot record its rows or the chunk as emitted.
#[test]
fn a_piece_that_fails_to_build_does_not_advance_the_copy() {
    let mut rows = fetched(&["11", "12"]);
    rows.push(values("13", "A", "not a number", None));
    let mut source = copying_source(OutputMode::Upsert, "1:1:", &rows);
    pass_held(&mut source);
    let bound = Some(vec!["10".to_string()]);
    assert!(source.emit_held_chunk(2).unwrap().is_some());
    assert_eq!(copy_progress(&source), (0, bound.clone(), Some(2)));
    assert!(source.emit_held_chunk(2).is_err());
    assert_eq!(
        copy_progress(&source),
        (0, bound, Some(2)),
        "the chunk is fetched again after a restart"
    );
}

/// Chunks emit after every transaction ending at or before their `high` and before any ending
/// after it, so positions strictly increase across any interleaving.
#[test]
fn positions_strictly_increase_across_chunks_and_transactions() {
    let mut rng = StdRng::seed_from_u64(0x5eed_0003);
    for _ in 0..2_000 {
        let mut positions: Vec<(Vec<u8>, u32)> = Vec::new();
        let mut last_end = 0x1000_u64;
        let mut ordinal = 0_u64;
        for _ in 0..rng.random_range(1..40) {
            if rng.random_bool(0.3) {
                let high = last_end + rng.random_range(0..0x10);
                for row in 0..rng.random_range(0..4_u32) {
                    positions.push((chunk_order_key(Lsn::new(high), ordinal).to_vec(), row));
                }
                ordinal += 1;
                last_end = high;
            } else {
                last_end += rng.random_range(1..0x40);
                let mut offset = 0;
                for _ in 0..rng.random_range(1..4) {
                    offset += rng.random_range(0..3_u32);
                    positions.push((wal_order_key(Lsn::new(last_end)).to_vec(), offset));
                    offset += 1;
                }
            }
        }
        assert!(
            positions.windows(2).all(|pair| pair[0] < pair[1]),
            "{positions:?}"
        );
    }
}

// ── Startup checks ──

fn key_column(name: &str) -> IndexKeyColumn {
    IndexKeyColumn {
        name: name.into(),
        type_sql: "text".into(),
        collation: Some("\"pg_catalog\".\"C\"".into()),
        deterministic: true,
        column_collation: true,
        default_opclass: true,
    }
}

#[test]
fn the_copy_needs_a_primary_and_keys_compared_like_their_texts() {
    let table = TableName::parse("public.orders").unwrap();
    let good = [key_column("id")];
    assert_eq!(copy_problem(&table, false, &good), None);
    let standby = copy_problem(&table, true, &good).unwrap();
    assert!(
        standby.contains("primary") && standby.contains("snapshot.mode=never"),
        "{standby}"
    );
    for (column, needle) in [
        (
            IndexKeyColumn {
                deterministic: false,
                ..key_column("id")
            },
            "non-deterministic collation",
        ),
        (
            IndexKeyColumn {
                column_collation: false,
                ..key_column("id")
            },
            "collation other than the column's",
        ),
        (
            IndexKeyColumn {
                default_opclass: false,
                ..key_column("id")
            },
            "non-default operator class",
        ),
    ] {
        let problem = copy_problem(&table, false, &[key_column("tenant"), column]).unwrap();
        assert!(
            problem.contains(needle) && problem.contains("'id'"),
            "{problem}"
        );
    }
    assert!(copy_problem(&table, false, &[]).is_some());
}

#[test]
fn the_copy_plan_quotes_names_and_maps_keys_to_declared_columns() {
    let layout = source_layout(OutputMode::Upsert);
    let table = TableName::parse("public.orders").unwrap();
    let plan = CopyPlan::new(&table, &layout, OID, &[key_column("id")], "f".into()).unwrap();
    assert_eq!(plan.table, "\"public\".\"orders\"");
    assert_eq!(plan.columns, "\"id\", \"status\", \"qty\", \"note\"");
    assert_eq!(plan.keys[0].declared, 0);
    assert_eq!(plan.keys[0].name, "\"id\"");
    assert!(CopyPlan::new(&table, &layout, OID, &[key_column("missing")], "f".into()).is_err());
}
