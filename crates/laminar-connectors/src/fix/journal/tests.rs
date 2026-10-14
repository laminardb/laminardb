use std::cell::{Cell, RefCell};
use std::time::{Duration, Instant};

use super::*;

type SyncHook = Box<dyn FnOnce()>;

thread_local! {
    static FAIL_NEXT_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_SYNC: Cell<bool> = const { Cell::new(false) };
    /// Runs inside the next sync, after it captured its end and before `fdatasync`.
    static DURING_NEXT_SYNC: RefCell<Option<SyncHook>> = const { RefCell::new(None) };
}

pub(super) fn take_injected_write_failure() -> bool {
    FAIL_NEXT_WRITE.with(|flag| flag.replace(false))
}

pub(super) fn take_injected_sync_failure() -> bool {
    if let Some(hook) = DURING_NEXT_SYNC.with(RefCell::take) {
        hook();
    }
    FAIL_NEXT_SYNC.with(|flag| flag.replace(false))
}

/// Records every floor the journal publishes.
#[derive(Default)]
struct Floors(parking_lot::Mutex<Vec<Option<u64>>>);

impl DurabilityFloor for Floors {
    fn set(&self, floor: Option<u64>) {
        self.0.lock().push(floor);
    }
}

impl Floors {
    fn seen(&self) -> Vec<Option<u64>> {
        self.0.lock().clone()
    }
}

fn with_floors(journal: &Journal) -> Arc<Floors> {
    let floors = Arc::new(Floors::default());
    journal.attach_floor(floors.clone()).expect("attach");
    floors
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
    open_with(root, options(segment_bytes))
}

fn open_with(root: &Path, options: JournalOptions) -> Journal {
    Journal::open(
        root,
        "FIX.4.4:A->B",
        "contract",
        options,
        Arc::new(Notify::new()),
    )
    .expect("open")
}

fn os_options(segment_bytes: u64) -> JournalOptions {
    JournalOptions {
        sync: SyncPolicy::OperatingSystem,
        ..options(segment_bytes)
    }
}

fn application_record(series: u32, seq: u64) -> ApplicationRecord {
    ApplicationRecord {
        series,
        seq,
        received_at_nanos: i64::try_from(seq).expect("seq") * 1_000,
        poss_dup: false,
        poss_resend: false,
        msg_type: "8".into(),
        content_digest: seq * 31,
        message: format!("message-{seq}").into_bytes(),
    }
}

fn application(series: u32, seq: u64) -> Record {
    Record::Application(application_record(series, seq))
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
        store_created_unix_micros: 1_000 + i64::from(series),
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
    journal.sync().expect("sync");
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
            store_created_unix_micros: 1_001,
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
    journal.sync().expect("sync");
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
    journal.sync().expect("sync");
    assert_eq!(seqs(&read_all(&journal, 0)), vec![2, 3]);
}

#[test]
fn mid_journal_corruption_fails_closed() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    for seq in 1..=4 {
        journal.append(&application(1, seq)).expect("append");
    }
    journal.sync().expect("sync");
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
    let journal = open_with(dir.path(), os_options(1 << 20));
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

    let journal = open_with(dir.path(), os_options(1 << 20));
    assert!(journal.poisoned().is_none());
    journal
        .append(&application(1, 2))
        .expect("append after recovery");
    assert_eq!(seqs(&read_all(&journal, 0)), vec![1, 2]);
}

#[test]
fn a_failed_group_write_poisons_the_journal_before_anything_is_published() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    let floors = with_floors(&journal);
    journal.append(&application(1, 1)).expect("append");
    journal.append(&application(1, 2)).expect("append");
    FAIL_NEXT_WRITE.with(|flag| flag.set(true));
    let error = journal.sync().expect_err("injected");
    assert!(error.is_outcome_unknown());
    assert!(journal.append(&application(1, 3)).is_err());
    assert!(journal.sync().is_err(), "a poisoned journal never syncs");
    assert!(seqs(&read_all(&journal, 0)).is_empty());
    assert_eq!(
        floors.seen(),
        vec![Some(1)],
        "both records stay unacknowledged"
    );
    drop(journal);

    let journal = open(dir.path(), 1 << 20);
    assert!(
        seqs(&read_all(&journal, 0)).is_empty(),
        "the partial write was removed"
    );
    journal
        .append(&application(1, 1))
        .expect("append after recovery");
    journal.sync().expect("sync");
    assert_eq!(seqs(&read_all(&journal, 0)), vec![1]);
}

#[test]
fn retention_deletes_only_committed_segments_and_expired_restores_fail() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 200);
    journal.append(&started(1)).expect("series");
    for seq in 1..=30 {
        journal.append(&application(1, seq)).expect("append");
    }
    journal.sync().expect("sync");
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
    journal.sync().expect("sync");
    drop(journal);
    let journal = open(dir.path(), 1 << 20);
    assert_eq!(journal.take_redelivery_window(3), vec![(3, 93), (4, 124)]);
    assert!(
        journal.take_redelivery_window(0).is_empty(),
        "handed out once"
    );
}

#[test]
fn group_committed_records_are_readable_only_once_durable_and_hold_the_floor() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    let floors = with_floors(&journal);
    journal.append(&started(1)).expect("series");
    let synced = journal.durable_end();
    assert_eq!(
        synced.next_index,
        journal.state.lock().writer.next_index,
        "a series start is synced inline"
    );
    journal.append(&application(1, 2)).expect("append");
    journal.append(&application(1, 3)).expect("append");
    assert_eq!(
        journal.durable_end(),
        synced,
        "unsynced records are not readable"
    );
    assert_eq!(
        journal.progress().high_water,
        3,
        "capture sees what it appended"
    );
    assert_eq!(floors.seen(), vec![Some(2)]);
    journal.sync().expect("sync");
    assert_eq!(seqs(&read_all(&journal, 0)), vec![2, 3]);
    assert_eq!(floors.seen(), vec![Some(2), None]);
}

#[test]
fn records_appended_during_a_sync_keep_holding_the_floor() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = Arc::new(open(dir.path(), 1 << 20));
    let floors = with_floors(&journal);
    journal.append(&started(1)).expect("series");
    journal.append(&application(1, 2)).expect("append");
    let during = journal.clone();
    DURING_NEXT_SYNC.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            during
                .append(&application(1, 3))
                .expect("append during sync");
        }));
    });
    journal.sync().expect("sync");
    assert_eq!(seqs(&read_all(&journal, 0)), vec![2]);
    assert_eq!(floors.seen(), vec![Some(2), Some(3)]);
    journal.sync().expect("sync");
    assert_eq!(seqs(&read_all(&journal, 0)), vec![2, 3]);
    assert_eq!(floors.seen(), vec![Some(2), Some(3), None]);
}

#[test]
fn a_sequence_reset_makes_earlier_records_durable_before_it_returns() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    let floors = with_floors(&journal);
    journal.append(&application(1, 2)).expect("append");
    journal
        .append(&Record::SequenceReset {
            series: 1,
            from_seq: 3,
            new_seq: 9,
        })
        .expect("reset");
    assert_eq!(
        journal.durable_end().next_index,
        journal.state.lock().writer.next_index
    );
    assert_eq!(seqs(&read_all(&journal, 0)), vec![2]);
    assert_eq!(floors.seen(), vec![Some(2), None]);
}

#[test]
fn a_failed_sync_poisons_the_journal_and_keeps_holding_the_floor() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 1 << 20);
    let floors = with_floors(&journal);
    journal.append(&application(1, 2)).expect("append");
    FAIL_NEXT_SYNC.with(|flag| flag.set(true));
    let error = journal.sync().expect_err("injected");
    assert!(error.is_outcome_unknown());
    assert!(journal.poisoned().is_some());
    assert!(journal.append(&application(1, 3)).is_err());
    assert!(journal.sync().is_err(), "a failed sync is never retried");
    assert!(seqs(&read_all(&journal, 0)).is_empty());
    assert_eq!(floors.seen(), vec![Some(2)], "the floor is never released");
}

#[test]
fn the_unsynced_backlog_is_bounded_without_a_sync_thread() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = open(dir.path(), 64 << 20);
    let message = vec![b'x'; 60 * 1024];
    let mut seq = 0;
    while journal.durable_end().next_index <= 1 {
        seq += 1;
        assert!(seq < 400, "an inline sync bounded the backlog");
        journal
            .append(&Record::Application(ApplicationRecord {
                message: message.clone(),
                ..application_record(1, seq)
            }))
            .expect("append");
    }
    let unsynced = journal.state.lock().unsynced_bytes;
    assert!(unsynced <= MAX_UNSYNCED_BYTES, "{unsynced}");
}

#[test]
fn the_group_commit_thread_publishes_appends_and_drains_on_drop() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = Arc::new(open(dir.path(), 4096));
    let floors = with_floors(&journal);
    let group_commit = journal.start_group_commit().expect("start").expect("fsync");
    for seq in 1..=200 {
        journal.append(&application(1, seq)).expect("append");
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    while journal.durable_end().next_index != journal.state.lock().writer.next_index {
        assert!(
            Instant::now() < deadline,
            "group commit published the appends"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    for seq in 201..=210 {
        journal.append(&application(1, seq)).expect("append");
    }
    drop(group_commit);
    assert_eq!(seqs(&read_all(&journal, 0)), (1..=210).collect::<Vec<_>>());
    assert_eq!(floors.seen().last(), Some(&None));

    let dir = tempfile::tempdir().expect("dir");
    let journal = Arc::new(open_with(dir.path(), os_options(4096)));
    assert!(journal.start_group_commit().expect("start").is_none());
    journal.append(&application(1, 1)).expect("append");
    assert_eq!(seqs(&read_all(&journal, 0)), vec![1], "durable on return");
}

/// Engine-thread cost of an append and the time until a burst is durable and readable.
#[test]
#[ignore = "performance profile; run in release with --ignored"]
fn profile_append_latency() {
    for sync in [SyncPolicy::OperatingSystem, SyncPolicy::Fsync] {
        let dir = tempfile::tempdir().expect("dir");
        let mut opts = options(64 << 20);
        opts.sync = sync;
        let journal = Arc::new(
            Journal::open(
                dir.path(),
                "FIX.4.4:A->B",
                "contract",
                opts,
                Arc::new(Notify::new()),
            )
            .expect("open"),
        );
        let group_commit = journal.start_group_commit().expect("start");
        let message = vec![b'x'; 300];
        let mut latencies = Vec::with_capacity(5000);
        let burst = Instant::now();
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
            let begin = Instant::now();
            journal.append(&record).expect("append");
            latencies.push(begin.elapsed());
        }
        while journal.durable_end().next_index != journal.state.lock().writer.next_index {
            std::thread::yield_now();
        }
        let durable = burst.elapsed();
        drop(group_commit);
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
            "append sync={sync:?} p50={:?} p95={:?} p99={:?} p99.9={:?} max={:?}; 5000 durable in {durable:?}",
            at(0.5),
            at(0.95),
            at(0.99),
            at(0.999),
            latencies.last().copied().unwrap_or_default()
        );
    }
}

/// Engine-thread append latency at a steady rate while the group-commit thread syncs.
#[test]
#[ignore = "performance profile; run in release with --ignored"]
fn profile_paced_append_latency() {
    let dir = tempfile::tempdir().expect("dir");
    let journal = Arc::new(open(dir.path(), 64 << 20));
    let group_commit = journal.start_group_commit().expect("start");
    let message = vec![b'x'; 300];
    let mut latencies = Vec::with_capacity(2000);
    let interval = Duration::from_micros(200);
    let begin = Instant::now();
    for seq in 1..=2000_u64 {
        let due = begin + interval * u32::try_from(seq).expect("seq");
        while Instant::now() < due {
            std::hint::spin_loop();
        }
        let record = Record::Application(ApplicationRecord {
            message: message.clone(),
            ..application_record(1, seq)
        });
        let start = Instant::now();
        journal.append(&record).expect("append");
        latencies.push(start.elapsed());
    }
    drop(group_commit);
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
        "paced append 5000/s p50={:?} p90={:?} p99={:?} max={:?}",
        at(0.5),
        at(0.9),
        at(0.99),
        latencies.last().copied().unwrap_or_default()
    );
}
