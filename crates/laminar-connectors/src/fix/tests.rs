//! Source-level scenarios against a real local QuickFIX acceptor.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_core::checkpoint::CheckpointAttempt;

use super::engine::StoreBackend;
use super::journal::SyncPolicy;
use super::session_store::SessionStore;
use super::testing::{FixTestVenue, TestMessage};
use super::FixSource;
use crate::checkpoint::SourceCheckpoint;
use crate::config::{encode_arrow_schema_ipc, ConnectorConfig};
use crate::connector::{DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart};

const DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/fix/FIX44-synthetic.xml"
);
const TRANSPORT_DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/fix/FIXT11-synthetic.xml"
);
const APPLICATION_DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/fix/FIX50SP2-synthetic.xml"
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
    fixt: bool,
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
            fixt: false,
        }
    }

    /// A FIXT.1.1 venue whose DefaultApplVerID is `venue_version`; the source captures
    /// FIX.5.0SP2.
    fn fixt(name: &str, venue_version: &str) -> Self {
        let dir = tempfile::tempdir().expect("dir");
        let venue = FixTestVenue::start_with_settings(
            &dir.path().join("venue"),
            Path::new(APPLICATION_DICTIONARY),
            "FIXT.1.1",
            &format!("V_{name}"),
            &format!("L_{name}"),
            &[
                ("TransportDataDictionary", TRANSPORT_DICTIONARY.to_string()),
                ("AppDataDictionary", APPLICATION_DICTIONARY.to_string()),
                ("DefaultApplVerID", venue_version.to_string()),
            ],
        )
        .expect("venue");
        Self {
            dir,
            name: name.to_string(),
            venue: Some(venue),
            fixt: true,
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
        if self.fixt {
            for (key, value) in [
                ("begin.string", "FIXT.1.1"),
                ("dictionary.path", APPLICATION_DICTIONARY),
                ("transport.dictionary.path", TRANSPORT_DICTIONARY),
                ("appl.ver.id", "FIX.5.0SP2"),
            ] {
                config.set(key, value);
            }
        }
        for (key, value) in extra {
            config.set(*key, *value);
        }
        config.set("_arrow_schema", encode_arrow_schema_ipc(&schema()));
        config
    }

    async fn start(&self, position: SourcePosition, extra: &[(&str, &str)]) -> FixSource {
        let source = self.start_source(position, extra).await;
        assert!(
            self.venue().wait_logged_on(Duration::from_secs(20)),
            "client logon"
        );
        source
    }

    /// Start the source without waiting for the session to log on.
    async fn start_source(&self, position: SourcePosition, extra: &[(&str, &str)]) -> FixSource {
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
        header_fields: Vec::new(),
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

fn column(batches: &[RecordBatch], name: &str) -> Vec<String> {
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

/// Model a crash after journal appends but before the session store persisted the inbound
/// sequence number: rewind it to the first application message.
fn rewind_inbound(journal: &Path) {
    let store = SessionStore::open(journal, SyncPolicy::Fsync, Box::new(|| Ok(()))).expect("store");
    store.set_next_target(2).expect("rewind");
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
    let after_first = source.checkpoint();
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
async fn fixt_sessions_capture_one_application_version_and_replay_it_after_restart() {
    let harness = Harness::fixt("fixt", "FIX.5.0SP2");
    let mut source = harness.start(SourcePosition::Initial, &[]).await;
    harness.send("E1");
    let mut explicit = execution_report("E2", Some("E2"));
    explicit.header_fields.push((1128, "9".into()));
    harness.venue().send(&explicit).expect("send");
    let (batches, first) = collect(&mut source, 2).await;
    assert_eq!(column(&batches, "exec_id"), vec!["E1", "E2"]);
    source.close().await.expect("close");

    let mut early = first.clone();
    let next: u64 = first
        .get_offset("journal.next")
        .expect("next")
        .parse()
        .expect("index");
    early.set_offset("journal.next", (next - 2).to_string());
    let mut source = harness.start(resume(early), &[]).await;
    harness.send("E3");
    let (batches, _) = collect(&mut source, 3).await;
    assert_eq!(
        column(&batches, "exec_id"),
        vec!["E1", "E2", "E3"],
        "replayed with both pinned dictionaries, then live"
    );
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_counterparty_on_another_application_version_stops_the_source() {
    let harness = Harness::fixt("fixtv", "FIX.5.0SP1");
    let mut source = harness.start_source(SourcePosition::Initial, &[]).await;
    let error = poll_error(&mut source).await;
    assert!(error.contains("DefaultApplVerID '8'"), "{error}");
    source.close().await.expect("close");

    let harness = Harness::fixt("fixtm", "FIX.5.0SP2");
    let mut source = harness.start(SourcePosition::Initial, &[]).await;
    let mut foreign = execution_report("E1", Some("E1"));
    foreign.header_fields.push((1128, "8".into()));
    harness.venue().send(&foreign).expect("send");
    let error = poll_error(&mut source).await;
    assert!(error.contains("ApplVerID '8'"), "{error}");
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

    rewind_inbound(&harness.journal());

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
    rewind_inbound(&harness.journal());

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

    // Project and commit everything journaled so far; retention frees space and the session
    // logs on again, receiving the refused messages by retransmission. Group commit publishes
    // the last records shortly after the pause.
    let mut checkpoint = source.checkpoint();
    let mut idle_since = Instant::now();
    while idle_since.elapsed() < Duration::from_millis(300) {
        match source.poll_batch(1024).await.expect("poll") {
            Some(batch) => {
                assert!(batch.num_rows() > 0);
                checkpoint = source.checkpoint();
                idle_since = Instant::now();
            }
            None => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    assert!(harness.journal_bytes() > 1_048_576);
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
        std::fs::read_dir(self.journal().join("segments")).map_or(0, |entries| {
            entries
                .filter_map(Result::ok)
                .filter_map(|entry| entry.metadata().ok())
                .map(|metadata| metadata.len())
                .sum()
        })
    }
}

mod tls {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::TlsAcceptor;

    use super::*;

    struct Authority {
        cert: rcgen::Certificate,
        key: rcgen::KeyPair,
    }

    fn authority() -> Authority {
        let key = rcgen::KeyPair::generate().expect("ca key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).expect("ca");
        Authority { cert, key }
    }

    /// A TLS terminator for `localhost` in front of the plaintext venue.
    async fn terminator(authority: &Authority, venue_port: u16) -> u16 {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        let key = rcgen::KeyPair::generate().expect("server key");
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .expect("server params")
            .signed_by(&key, &authority.cert, &authority.key)
            .expect("server cert");
        let chain = vec![CertificateDer::from(cert.der().to_vec())];
        let key = PrivateKeyDer::from_pem_slice(key.serialize_pem().as_bytes()).expect("key pem");
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("server config");
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(stream).await else {
                        return;
                    };
                    let Ok(mut venue) = TcpStream::connect(("127.0.0.1", venue_port)).await else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut tls, &mut venue).await;
                });
            }
        });
        port
    }

    async fn start_tls(harness: &Harness, port: u16, ca: &Path, server_name: &str) -> FixSource {
        let ca = ca.display().to_string();
        let port = port.to_string();
        let mut config = harness.config(&[
            ("transport", "tls"),
            ("tls.ca.file", ca.as_str()),
            ("tls.server.name", server_name),
        ]);
        config.set("port", port);
        let mut source = FixSource::new(None);
        let binding = source
            .resolve_schema(&config, Some(schema()))
            .await
            .expect("resolve");
        config.set_schema_binding(binding).expect("binding");
        source
            .start(
                SourceStart::new(
                    config,
                    SourcePosition::Initial,
                    DeliveryGuarantee::AtLeastOnce,
                )
                .expect("request"),
            )
            .await
            .expect("start");
        source
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn verified_tls_carries_the_session_and_untrusted_peers_are_refused() {
        let harness = Harness::new("tls");
        let trusted = authority();
        let port = terminator(&trusted, harness.venue().port()).await;
        let ca = harness.dir.path().join("ca.pem");
        std::fs::write(&ca, trusted.cert.pem()).expect("ca file");

        let untrusted = harness.dir.path().join("other-ca.pem");
        std::fs::write(&untrusted, authority().cert.pem()).expect("other ca");
        for (ca_file, name) in [(&untrusted, "localhost"), (&ca, "fix.example.com")] {
            let mut source = start_tls(&harness, port, ca_file, name).await;
            assert!(
                !harness.venue().wait_logged_on(Duration::from_secs(3)),
                "TLS must fail for CA {} and name {name}",
                ca_file.display()
            );
            assert!(source.poll_batch(16).await.expect("not fatal").is_none());
            source.close().await.expect("close");
        }

        let mut source = start_tls(&harness, port, &ca, "localhost").await;
        assert!(
            harness.venue().wait_logged_on(Duration::from_secs(20)),
            "verified TLS logon"
        );
        harness.send("T1");
        let (batches, _) = collect(&mut source, 1).await;
        assert_eq!(column(&batches, "exec_id"), vec!["T1"]);
        source.close().await.expect("close");
    }
}

/// Latency profile: receive-to-Arrow from a scheduled send time (open loop, so queueing delay is
/// included), per durability policy, plus burst catch-up. Run with
/// `cargo test --release -p laminar-connectors --features fix --lib fix::tests::profile -- --ignored --nocapture`.
mod profile {
    use super::*;

    fn percentile(sorted: &[Duration], quantile: f64) -> Duration {
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let index = ((sorted.len() as f64 - 1.0) * quantile).round() as usize;
        sorted[index]
    }

    async fn run(name: &str, sync: &str, rate: u64, seconds: u64) {
        let harness = Harness::new(name);
        let mut source = harness
            .start(
                SourcePosition::Initial,
                &[
                    ("journal.sync", sync),
                    ("journal.segment.bytes", "67108864"),
                ],
            )
            .await;
        let total = usize::try_from(rate * seconds).expect("count");
        let interval = Duration::from_nanos(1_000_000_000 / rate);
        let start = Instant::now() + Duration::from_millis(100);
        let venue = harness.venue();
        let mut latencies = Vec::with_capacity(total);
        let mut received = 0_usize;
        let mut sent = 0_usize;
        while received < total {
            while sent < total
                && Instant::now() >= start + interval * u32::try_from(sent).expect("index")
            {
                venue
                    .send(&execution_report(
                        &format!("O{sent}"),
                        Some(&format!("{sent}")),
                    ))
                    .expect("send");
                sent += 1;
            }
            if let Some(batch) = source.poll_batch(1024).await.expect("poll") {
                let now = Instant::now();
                for id in column(&[batch.records], "exec_id") {
                    let index: u32 = id.parse().expect("id");
                    latencies.push(now - (start + interval * index));
                    received += 1;
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
        let elapsed = start.elapsed();
        source.close().await.expect("close");
        latencies.sort_unstable();
        eprintln!(
            "{name:>8} sync={sync:<5} rate={rate}/s n={total} p50={:?} p95={:?} p99={:?} p99.9={:?} max={:?} throughput={:.0}/s",
            percentile(&latencies, 0.50),
            percentile(&latencies, 0.95),
            percentile(&latencies, 0.99),
            percentile(&latencies, 0.999),
            latencies.last().copied().unwrap_or_default(),
            total as f64 / elapsed.as_secs_f64(),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "performance profile; run in release with --ignored"]
    async fn profile_receive_to_arrow_latency() {
        for (sync, rate) in [("os", 500), ("os", 2000), ("fsync", 500), ("fsync", 2000)] {
            run(&format!("p{sync}{rate}"), sync, rate, 5).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "performance profile; run in release with --ignored"]
    async fn profile_burst_catch_up() {
        for sync in ["os", "fsync"] {
            let harness = Harness::new(&format!("burst{sync}"));
            let mut source = harness
                .start(
                    SourcePosition::Initial,
                    &[
                        ("journal.sync", sync),
                        ("journal.segment.bytes", "67108864"),
                    ],
                )
                .await;
            let burst = 5000;
            let begin = Instant::now();
            for index in 0..burst {
                harness
                    .venue()
                    .send(&execution_report(
                        &format!("O{index}"),
                        Some(&format!("{index}")),
                    ))
                    .expect("send");
            }
            let queued = begin.elapsed();
            let (batches, _) = collect(&mut source, burst).await;
            let caught_up = begin.elapsed();
            let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            eprintln!(
                "burst sync={sync:<5} messages={burst} venue_send={queued:?} all_rows={caught_up:?} rate={:.0}/s",
                rows as f64 / caught_up.as_secs_f64()
            );
            source.close().await.expect("close");
        }
    }
}
