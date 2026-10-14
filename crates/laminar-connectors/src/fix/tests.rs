//! Source-level scenarios against a real local QuickFIX acceptor.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_core::checkpoint::CheckpointAttempt;

use super::testing::{FixTestVenue, TestMessage};
use super::FixSource;
use crate::checkpoint::SourceCheckpoint;
use crate::config::{encode_arrow_schema_ipc, ConnectorConfig};
use crate::connector::{DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart};

const DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/fix/FIX44-synthetic.xml"
);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("exec_id", DataType::Utf8, false),
        Field::new("cum_qty", DataType::Decimal128(18, 4), false),
        Field::new("_fix_series", DataType::UInt32, false),
        Field::new("_fix_seq_num", DataType::UInt64, false),
        Field::new("_fix_poss_dup", DataType::Boolean, false),
        Field::new("_fix_gap_before", DataType::Boolean, false),
    ]))
}

struct Harness {
    dir: tempfile::TempDir,
    name: String,
    venue: Option<FixTestVenue>,
}

impl Harness {
    fn new(name: &str) -> Self {
        let dir = tempfile::tempdir().expect("dir");
        let venue = FixTestVenue::start(
            &dir.path().join("venue"),
            Path::new(DICTIONARY),
            "FIX.4.4",
            &format!("V_{name}"),
            &format!("L_{name}"),
        )
        .expect("venue");
        Self {
            dir,
            name: name.to_string(),
            venue: Some(venue),
        }
    }

    fn venue(&self) -> &FixTestVenue {
        self.venue.as_ref().expect("venue running")
    }

    fn journal(&self) -> PathBuf {
        self.dir.path().join("journal")
    }

    fn config(&self, extra: &[(&str, &str)]) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("fix");
        let port = self
            .venue
            .as_ref()
            .map_or(0, FixTestVenue::port)
            .to_string();
        let journal = self.journal().display().to_string();
        let sender = format!("L_{}", self.name);
        let target = format!("V_{}", self.name);
        for (key, value) in [
            ("host", "127.0.0.1"),
            ("port", port.as_str()),
            ("transport", "tcp"),
            ("sender.comp.id", sender.as_str()),
            ("target.comp.id", target.as_str()),
            ("dictionary.path", DICTIONARY),
            ("messages", "8"),
            ("journal.path", journal.as_str()),
            ("journal.segment.bytes", "1048576"),
            ("max.message.bytes", "16384"),
            ("reconnect.interval.seconds", "1"),
            ("column.order_id", "37"),
            ("column.exec_id", "17"),
            ("column.cum_qty", "14"),
            ("laminar.source.name", "fills"),
        ] {
            config.set(key, value);
        }
        for (key, value) in extra {
            config.set(*key, *value);
        }
        config.set("_arrow_schema", encode_arrow_schema_ipc(&schema()));
        config
    }

    async fn start(&self, position: SourcePosition, extra: &[(&str, &str)]) -> FixSource {
        let mut source = FixSource::new(None);
        let config = self.config(extra);
        let binding = source
            .resolve_schema(&config, Some(schema()))
            .await
            .expect("resolve");
        let mut config = config;
        config.set_schema_binding(binding).expect("binding");
        source
            .start(
                SourceStart::new(config, position, DeliveryGuarantee::AtLeastOnce)
                    .expect("request"),
            )
            .await
            .expect("start");
        assert!(
            self.venue().wait_logged_on(Duration::from_secs(20)),
            "client logon"
        );
        source
    }

    fn send(&self, exec_id: &str) {
        self.venue()
            .send(&execution_report(exec_id, Some(exec_id)))
            .expect("send");
    }
}

fn execution_report(order_id: &str, exec_id: Option<&str>) -> TestMessage {
    let mut fields: Vec<(i32, String)> = vec![(37, order_id.into())];
    if let Some(exec_id) = exec_id {
        fields.push((17, exec_id.into()));
    }
    for (tag, value) in [
        (150, "F"),
        (39, "1"),
        (55, "ABC"),
        (54, "1"),
        (151, "0"),
        (14, "10"),
        (6, "100.25"),
    ] {
        fields.push((tag, value.into()));
    }
    TestMessage {
        msg_type: "8".into(),
        fields,
        groups: Vec::new(),
    }
}

fn resume(checkpoint: SourceCheckpoint) -> SourcePosition {
    SourcePosition::Resume {
        attempt: CheckpointAttempt::canonical(1),
        checkpoint,
    }
}

/// Poll until `rows` rows were emitted; returns the batches and the last batch cursor.
async fn collect(source: &mut FixSource, rows: usize) -> (Vec<RecordBatch>, SourceCheckpoint) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut batches = Vec::new();
    let mut total = 0;
    while total < rows {
        assert!(
            Instant::now() < deadline,
            "timed out after {total} of {rows} rows"
        );
        match source.poll_batch(1024).await.expect("poll") {
            Some(batch) => {
                total += batch.num_rows();
                batches.push(batch.records);
            }
            None => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    (batches, source.checkpoint())
}

async fn poll_error(source: &mut FixSource) -> String {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < deadline, "no terminal error");
        match source.poll_batch(1024).await {
            Err(error) => return error.to_string(),
            Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

fn column<'a>(batches: &'a [RecordBatch], name: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(name)
                .expect("column")
                .as_string::<i32>();
            (0..array.len())
                .map(|row| array.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn seqs(batches: &[RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("_fix_seq_num")
                .expect("seq")
                .as_primitive::<UInt64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

fn store_file(journal: &Path, suffix: &str) -> PathBuf {
    std::fs::read_dir(journal.join("engine"))
        .expect("store")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.to_string_lossy().ends_with(suffix))
        .expect("store file")
}

#[tokio::test(flavor = "multi_thread")]
async fn live_messages_become_rows_and_restart_replays_from_the_journal() {
    let harness = Harness::new("restart");
    let mut source = harness.start(SourcePosition::Initial, &[]).await;
    for exec in ["E1", "E2", "E3"] {
        harness.send(exec);
    }
    let (batches, _) = collect(&mut source, 3).await;
    assert_eq!(column(&batches, "exec_id"), vec!["E1", "E2", "E3"]);
    assert_eq!(seqs(&batches), vec![2, 3, 4], "venue seq 1 is Logon");
    let after_first = {
        let journal_rows = source.checkpoint();
        journal_rows
    };
    source.close().await.expect("close");

    // A checkpoint taken before the last two rows: restart replays them from the journal
    // without asking the venue, then continues live.
    let mut early = after_first.clone();
    let next: u64 = after_first
        .get_offset("journal.next")
        .expect("next")
        .parse()
        .expect("index");
    early.set_offset("journal.next", (next - 2).to_string());
    let logons = harness.venue().logons();
    let mut source = harness.start(resume(early), &[]).await;
    harness.send("E4");
    let (batches, _) = collect(&mut source, 3).await;
    assert_eq!(column(&batches, "exec_id"), vec!["E2", "E3", "E4"]);
    assert_eq!(harness.venue().logons(), logons + 1);
    let dup = batches[0]
        .column_by_name("_fix_poss_dup")
        .expect("dup")
        .as_boolean();
    assert!(!dup.value(0), "replay preserves the original receive flags");
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_regressed_engine_store_is_reconciled_by_retransmission_without_duplicates() {
    let harness = Harness::new("regress");
    let mut source = harness.start(SourcePosition::Initial, &[]).await;
    for exec in ["E1", "E2", "E3"] {
        harness.send(exec);
    }
    let (_, checkpoint) = collect(&mut source, 3).await;
    source.close().await.expect("close");

    // Model a crash after the journal append but before the engine persisted its target:
    // rewind the stored inbound sequence number.
    let seqnums = store_file(&harness.journal(), ".seqnums");
    let text = std::fs::read_to_string(&seqnums).expect("seqnums");
    let (sender, _) = text.split_once(':').expect("format");
    std::fs::write(&seqnums, format!("{} : {:0>20}", sender.trim(), 2)).expect("rewind");

    let mut source = harness.start(resume(checkpoint), &[]).await;
    harness.send("E4");
    let (batches, _) = collect(&mut source, 1).await;
    assert_eq!(
        column(&batches, "exec_id"),
        vec!["E4"],
        "retransmissions of E1-E3 are not re-emitted"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(source.poll_batch(1024).await.expect("poll").is_none());
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_conflicting_retransmission_fails_closed() {
    let harness = Harness::new("conflict");
    let mut source = harness.start(SourcePosition::Initial, &[]).await;
    harness.send("E1");
    let (_, checkpoint) = collect(&mut source, 1).await;
    source.close().await.expect("close");
    let mut harness = harness;
    let venue = harness.venue.take().expect("venue");
    venue.stop().expect("stop venue");
    drop(venue);

    // Alter the venue's stored copy, then make the client ask for it again.
    let body = std::fs::read_dir(harness.dir.path().join("venue"))
        .expect("venue store")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.to_string_lossy().ends_with(".body"))
        .expect("body");
    let stored = std::fs::read(&body).expect("read");
    let tampered: Vec<u8> = String::from_utf8_lossy(&stored)
        .replace("37=E1", "37=X1")
        .into_bytes();
    std::fs::write(&body, tampered).expect("tamper");
    let seqnums = store_file(&harness.journal(), ".seqnums");
    let text = std::fs::read_to_string(&seqnums).expect("seqnums");
    let (sender, _) = text.split_once(':').expect("format");
    std::fs::write(&seqnums, format!("{} : {:0>20}", sender.trim(), 2)).expect("rewind");

    // The tampered stored copy no longer matches its checksum; let the venue resend it anyway.
    let venue = FixTestVenue::start_with_settings(
        &harness.dir.path().join("venue"),
        Path::new(DICTIONARY),
        "FIX.4.4",
        "V_conflict",
        "L_conflict",
        &[("ValidateLengthAndChecksum", "N".to_string())],
    )
    .expect("venue");
    let mut config = harness.config(&[]);
    config.set("port", venue.port().to_string());
    let mut source = FixSource::new(None);
    let binding = source
        .resolve_schema(&config, Some(schema()))
        .await
        .expect("resolve");
    config.set_schema_binding(binding).expect("binding");
    source
        .start(
            SourceStart::new(config, resume(checkpoint), DeliveryGuarantee::AtLeastOnce)
                .expect("request"),
        )
        .await
        .expect("start");
    let error = poll_error(&mut source).await;
    assert!(
        error.contains("differs from the journaled original"),
        "{error}"
    );
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread")]
async fn gap_fills_and_session_rejects_do_not_create_rows_or_incompleteness() {
    let harness = Harness::new("gaps");
    let mut source = harness.start(SourcePosition::Initial, &[]).await;
    harness.send("E1");
    harness.venue().skip_sequence_numbers(3).expect("skip");
    harness.send("E2");
    harness
        .venue()
        .send(&execution_report("BAD", None))
        .expect("invalid");
    harness.send("E3");
    let (batches, _) = collect(&mut source, 3).await;
    assert_eq!(column(&batches, "exec_id"), vec!["E1", "E2", "E3"]);
    assert_eq!(seqs(&batches), vec![2, 6, 8]);
    let gap = batches
        .iter()
        .flat_map(|batch| {
            let flags = batch
                .column_by_name("_fix_gap_before")
                .expect("gap")
                .as_boolean();
            (0..flags.len())
                .map(|row| flags.value(row))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert!(
        gap.iter().all(|flag| !flag),
        "a gap fill is not an unrecoverable gap"
    );
    // The engine validates before sequencing, so the invalid message is rejected on arrival
    // (ahead of the gap) and again when the venue retransmits it.
    assert!(
        !harness.venue().rejects().is_empty(),
        "the invalid message was rejected"
    );
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread")]
async fn sequence_reset_fails_closed_by_default_and_marks_the_gap_when_allowed() {
    let harness = Harness::new("seqreset");
    let mut source = harness.start(SourcePosition::Initial, &[]).await;
    harness.send("E1");
    let (_, checkpoint) = collect(&mut source, 1).await;
    harness.venue().sequence_reset(20).expect("reset");
    harness.send("E2");
    let error = poll_error(&mut source).await;
    assert!(error.contains("SequenceReset-Reset"), "{error}");
    source.close().await.expect("close");

    let mut source = harness
        .start(resume(checkpoint), &[("on.sequence.reset", "continue")])
        .await;
    let (batches, _) = collect(&mut source, 1).await;
    assert_eq!(column(&batches, "exec_id"), vec!["E2"]);
    assert!(batches[0]
        .column_by_name("_fix_gap_before")
        .expect("gap")
        .as_boolean()
        .value(0));
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_genuine_session_reset_starts_a_new_series() {
    // With 'reset.on.logon' every Logon carries ResetSeqNumFlag=Y, the protocol's explicit
    // reset, so each reconnect restarts numbering at 1 on both sides.
    let harness = Harness::new("series");
    let mut source = harness
        .start(SourcePosition::Initial, &[("reset.on.logon", "true")])
        .await;
    harness.send("E1");
    let (first, _) = collect(&mut source, 1).await;
    let logons = harness.venue().logons();
    harness.venue().reset_session().expect("reset");
    let deadline = Instant::now() + Duration::from_secs(20);
    while harness.venue().logons() == logons || !harness.venue().logged_on() {
        assert!(Instant::now() < deadline, "no relogon after reset");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    harness.send("E2");
    let (second, _) = collect(&mut source, 1).await;
    let series = |batches: &[RecordBatch]| {
        batches[0]
            .column_by_name("_fix_series")
            .expect("series")
            .as_primitive::<arrow_array::types::UInt32Type>()
            .value(0)
    };
    assert_eq!(
        (series(&first), series(&second)),
        (1, 2),
        "reset numbering is a new series"
    );
    assert_eq!(
        seqs(&first),
        seqs(&second),
        "the same MsgSeqNum recurs in the new series"
    );
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_journal_pauses_intake_until_a_commit_releases_space() {
    let harness = Harness::new("full");
    let limits = [
        ("journal.max.bytes", "1048576"),
        ("journal.segment.bytes", "1048576"),
    ];
    let mut source = harness.start(SourcePosition::Initial, &limits).await;
    let filler = "F".repeat(4000);
    let mut sent = 0;
    while harness.venue().logged_on() && sent < 600 {
        let mut message = execution_report(&format!("O{sent}"), Some(&format!("E{sent}")));
        message.fields.push((58, filler.clone()));
        harness.venue().send(&message).expect("send");
        sent += 1;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(
        !harness.venue().logged_on(),
        "a full journal pauses the session"
    );
    assert!(harness.journal_bytes() > 1_048_576);

    // Project and commit everything journaled so far; retention frees space and the session
    // logs on again, receiving the refused messages by retransmission.
    let mut checkpoint = source.checkpoint();
    while let Some(batch) = source.poll_batch(1024).await.expect("poll") {
        assert!(batch.num_rows() > 0);
        checkpoint = source.checkpoint();
    }
    source
        .notify_epoch_committed(1, &checkpoint)
        .await
        .expect("commit");
    assert!(
        harness.venue().wait_logged_on(Duration::from_secs(20)),
        "intake resumes after release"
    );
    harness.send("AFTER");
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut seen = Vec::new();
    while !seen.contains(&"AFTER".to_string()) {
        assert!(Instant::now() < deadline, "AFTER never arrived");
        if let Some(batch) = source.poll_batch(1024).await.expect("poll") {
            seen.extend(column(&[batch.records], "exec_id"));
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let unique: std::collections::BTreeSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), seen.len(), "no message is emitted twice");
    source.close().await.expect("close");
}

impl Harness {
    fn journal_bytes(&self) -> u64 {
        std::fs::read_dir(self.journal().join("segments"))
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter_map(|entry| entry.metadata().ok())
                    .map(|metadata| metadata.len())
                    .sum()
            })
            .unwrap_or(0)
    }
}
