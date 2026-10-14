use std::cell::Cell;

use super::*;

thread_local! {
    static FAIL_NEXT_WRITE: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn take_injected_write_failure() -> bool {
    FAIL_NEXT_WRITE.with(|flag| flag.replace(false))
}

fn options(segment_bytes: u64) -> JournalOptions {
    JournalOptions {
        sync: SyncPolicy::Fsync,
        segment_bytes,
        max_bytes: 1 << 30,
        max_record_bytes: 64 * 1024,
    }
}

fn open(root: &Path, segment_bytes: u64) -> Journal {
    Journal::open(
        root,
        "FIX.4.4:A->B",
        "contract",
        options(segment_bytes),
        Arc::new(Notify::new()),
    )
    .expect("open")
}

fn application(series: u32, seq: u64) -> Record {
    Record::Application(ApplicationRecord {
        series,
        seq,
        received_at_nanos: i64::try_from(seq).expect("seq") * 1_000,
        poss_dup: false,
        poss_resend: false,
        msg_type: "8".into(),
        content_digest: seq * 31,
        message: format!("message-{seq}").into_bytes(),
    })
}

fn read_all(journal: &Journal, start: RecordIndex) -> Vec<(RecordIndex, Record)> {
    let mut reader = journal.reader(start).expect("reader");
    let end = journal.durable_end();
    let mut records = Vec::new();
    while let Some(record) = reader.next(&end).expect("next") {
        records.push(record);
    }
    records
}

fn seqs(records: &[(RecordIndex, Record)]) -> Vec<u64> {
    records
        .iter()
        .filter_map(|(_, record)| match record {
            Record::Application(application) => Some(application.seq),
            _ => None,
        })
        .collect()
}

fn started(series: u32) -> Record {
    Record::SeriesStarted {
        series,
        store_created_unix_seconds: 1_000 + i64::from(series),
    }
}

#[test]
fn records_survive_reopen_across_segment_rotation() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 256);
    journal.append(&started(1)).expect("series");
    for seq in 2..=20 {
        journal.append(&application(1, seq)).expect("append");
    }
    let before = read_all(&journal, 0);
    assert_eq!(seqs(&before), (2..=20).collect::<Vec<_>>());
    assert!(journal.state.lock().segments.len() > 2, "rotation happened");
    drop(journal);

    let journal = open(dir.path(), 256);
    assert_eq!(read_all(&journal, 0), before);
    assert_eq!(
        journal.progress(),
        SessionSnapshot {
            series: 1,
            store_created_unix_seconds: 1_001,
            high_water: 20
        }
    );
    let middle = before[10].0;
    assert_eq!(read_all(&journal, middle), before[10..].to_vec());
    assert!(read_all(&journal, journal.durable_end().next_index).is_empty());
}

#[test]
fn a_torn_tail_is_truncated_and_appends_continue() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    journal.append(&started(1)).expect("series");
    journal.append(&application(1, 2)).expect("append");
    drop(journal);
    let segment = segment::segment_path(&dir.path().join("segments"), 0);
    let mut file = OpenOptions::new()
        .append(true)
        .open(&segment)
        .expect("open");
    file.write_all(&[9, 0, 0, 0, 1, 2, 3]).expect("torn bytes");
    drop(file);

    let journal = open(dir.path(), 1 << 20);
    journal.append(&application(1, 3)).expect("append");
    assert_eq!(seqs(&read_all(&journal, 0)), vec![2, 3]);
}

#[test]
fn mid_journal_corruption_fails_closed() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    for seq in 1..=4 {
        journal.append(&application(1, seq)).expect("append");
    }
    drop(journal);
    let segment = segment::segment_path(&dir.path().join("segments"), 0);
    let mut bytes = std::fs::read(&segment).expect("read");
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x55;
    std::fs::write(&segment, &bytes).expect("write");
    let error = Journal::open(
        dir.path(),
        "FIX.4.4:A->B",
        "contract",
        options(1 << 20),
        Arc::new(Notify::new()),
    )
    .err()
    .expect("corruption detected");
    assert!(error.to_string().contains("corrupt"), "{error}");
}

#[test]
fn a_failed_write_poisons_the_writer_and_recovery_drops_the_partial_frame() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    journal.append(&application(1, 1)).expect("append");
    FAIL_NEXT_WRITE.with(|flag| flag.set(true));
    let error = journal.append(&application(1, 2)).expect_err("injected");
    assert!(error.is_outcome_unknown());
    assert!(journal.poisoned().is_some());
    assert!(
        journal.append(&application(1, 3)).is_err(),
        "no retry after failure"
    );
    assert_eq!(seqs(&read_all(&journal, 0)), vec![1]);
    drop(journal);

    let journal = open(dir.path(), 1 << 20);
    assert!(journal.poisoned().is_none());
    journal
        .append(&application(1, 2))
        .expect("append after recovery");
    assert_eq!(seqs(&read_all(&journal, 0)), vec![1, 2]);
}

#[test]
fn retention_deletes_only_committed_segments_and_expired_restores_fail() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 200);
    journal.append(&started(1)).expect("series");
    for seq in 1..=30 {
        journal.append(&application(1, seq)).expect("append");
    }
    let records = read_all(&journal, 0);
    let committed = records[20].0;
    let retained_before = journal.retained_bytes();
    journal.release_before(committed).expect("release");
    assert!(journal.retained_bytes() < retained_before);
    let first = journal.first_retained_index();
    assert!(first <= committed && first > 0);
    assert!(journal.reader(first - 1).is_err(), "expired restore point");
    assert!(journal
        .reader(journal.durable_end().next_index + 1)
        .is_err());
    assert_eq!(read_all(&journal, committed), records[20..].to_vec());
    drop(journal);

    let journal = open(dir.path(), 200);
    assert_eq!(journal.progress().series, 1, "series survives retention");
    assert_eq!(journal.progress().high_water, 30);
    assert_eq!(read_all(&journal, committed), records[20..].to_vec());
}

#[test]
fn capacity_is_reported_and_freed_by_retention() {
    let dir = tempfile::tempdir().expect("dir");
    let mut small = options(200);
    small.max_bytes = 600;
    let journal = Journal::open(
        dir.path(),
        "FIX.4.4:A->B",
        "contract",
        small,
        Arc::new(Notify::new()),
    )
    .expect("open");
    let mut last = 0;
    for seq in 1..=40 {
        last = journal.append(&application(1, seq)).expect("append");
    }
    assert!(journal.over_capacity());
    journal.release_before(last).expect("release");
    assert!(!journal.over_capacity());
}

#[test]
fn ownership_and_contract_are_exclusive() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    let second = Journal::open(
        dir.path(),
        "FIX.4.4:A->B",
        "contract",
        options(1 << 20),
        Arc::new(Notify::new()),
    );
    assert!(second.is_err(), "a live journal cannot be opened twice");
    drop(journal);
    let other = Journal::open(
        dir.path(),
        "FIX.4.4:A->B",
        "different-contract",
        options(1 << 20),
        Arc::new(Notify::new()),
    );
    assert!(other.is_err(), "a journal is bound to its capture contract");
}

#[test]
fn redelivery_window_holds_current_series_records_at_or_after_the_engine_target() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    journal.append(&started(1)).expect("series");
    journal.append(&application(1, 5)).expect("append");
    journal.append(&started(2)).expect("series");
    for seq in [2, 3, 4] {
        journal.append(&application(2, seq)).expect("append");
    }
    drop(journal);
    let journal = open(dir.path(), 1 << 20);
    assert_eq!(journal.take_redelivery_window(3), vec![(3, 93), (4, 124)]);
    assert!(
        journal.take_redelivery_window(0).is_empty(),
        "handed out once"
    );
}

/// Receive-to-durable component: per-append latency of a 300-byte application record.
#[test]
#[ignore = "performance profile; run in release with --ignored"]
fn profile_append_latency() {
    for sync in [SyncPolicy::OperatingSystem, SyncPolicy::Fsync] {
        let dir = tempfile::tempdir().expect("dir");
        let mut opts = options(64 << 20);
        opts.sync = sync;
        let journal = Journal::open(
            dir.path(),
            "FIX.4.4:A->B",
            "contract",
            opts,
            Arc::new(Notify::new()),
        )
        .expect("open");
        let message = vec![b'x'; 300];
        let mut latencies = Vec::with_capacity(5000);
        for seq in 1..=5000_u64 {
            let record = Record::Application(ApplicationRecord {
                series: 1,
                seq,
                received_at_nanos: 0,
                poss_dup: false,
                poss_resend: false,
                msg_type: "8".into(),
                content_digest: seq,
                message: message.clone(),
            });
            let begin = std::time::Instant::now();
            journal.append(&record).expect("append");
            latencies.push(begin.elapsed());
        }
        latencies.sort_unstable();
        let at = |quantile: f64| {
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss
            )]
            let index = ((latencies.len() as f64 - 1.0) * quantile).round() as usize;
            latencies[index]
        };
        eprintln!(
            "append sync={sync:?} p50={:?} p95={:?} p99={:?} p99.9={:?} max={:?}",
            at(0.5),
            at(0.95),
            at(0.99),
            at(0.999),
            latencies.last().copied().unwrap_or_default()
        );
    }
}
