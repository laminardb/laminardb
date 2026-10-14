//! The Binance drop-copy profile against a local QuickFIX acceptor playing Binance: a signed
//! Logon, a new sequence series at every logon, and the possible gap a reconnect leaves.
//!
//! `LAMINAR_FIX_BINANCE_DICTIONARY` runs the local scenario against Binance's published
//! `spot-fix-oe.xml` instead of the synthetic dictionary; that file is not redistributed here.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt32Type};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use base64::Engine as _;

use super::super::testing::{FixTestVenue, TestGroupEntry, TestMessage};
use super::super::FixSource;
use super::collect;
use crate::config::{encode_arrow_schema_ipc, ConnectorConfig};
use crate::connector::{DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart};

const SYNTHETIC_DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/fix/BINANCE-synthetic.xml"
);
const CLIENT: &str = "LMNR01";
const API_KEY: &str = "test-api-key";

fn dictionary() -> String {
    std::env::var("LAMINAR_FIX_BINANCE_DICTIONARY")
        .unwrap_or_else(|_| SYNTHETIC_DICTIONARY.to_string())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("exec_id", DataType::Int64, true),
        Field::new("order_id", DataType::Int64, true),
        Field::new("symbol", DataType::Utf8, true),
        Field::new("cum_qty", DataType::Decimal128(28, 8), true),
        Field::new(
            "transact_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "fees",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal128(28, 8),
                true,
            ))),
            true,
        ),
        Field::new("_fix_series", DataType::UInt32, false),
        Field::new("_fix_gap_before", DataType::Boolean, false),
    ]))
}

/// A fresh Ed25519 key as a PEM file, and its public key.
fn signing_key(dir: &Path) -> (String, Vec<u8>) {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("generate");
    let public = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())
        .expect("key")
        .public_key()
        .as_ref()
        .to_vec();
    let path = dir.join("binance-ed25519.pem");
    let body = base64::engine::general_purpose::STANDARD.encode(pkcs8.as_ref());
    let label = "PRIVATE KEY";
    std::fs::write(
        &path,
        format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n"),
    )
    .expect("write key");
    (path.display().to_string(), public)
}

fn config(journal: &Path, extra: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("fix");
    let journal = journal.display().to_string();
    let dictionary = dictionary();
    for (key, value) in [
        ("venue", "binance"),
        ("sender.comp.id", CLIENT),
        ("target.comp.id", "SPOT"),
        ("username", API_KEY),
        ("dictionary.path", dictionary.as_str()),
        ("messages", "8,N"),
        ("journal.path", journal.as_str()),
        ("journal.segment.bytes", "1048576"),
        ("max.message.bytes", "16384"),
        ("reconnect.interval.seconds", "1"),
        ("column.exec_id", "17"),
        ("column.order_id", "37"),
        ("column.symbol", "55"),
        ("column.cum_qty", "14"),
        ("column.transact_time", "60"),
        ("column.fees", "136.137"),
        ("laminar.source.name", "binance"),
    ] {
        config.set(key, value);
    }
    for (key, value) in extra {
        config.set(*key, *value);
    }
    config.set("_arrow_schema", encode_arrow_schema_ipc(&schema()));
    config
}

async fn start(config: ConnectorConfig, registry: Option<Arc<prometheus::Registry>>) -> FixSource {
    let mut source = FixSource::new(registry);
    let binding = source
        .resolve_schema(&config, Some(schema()))
        .await
        .expect("resolve");
    let mut config = config;
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

/// An ExecutionReport with the fields and formats of Binance's documented sample.
fn execution_report(exec_id: u64) -> TestMessage {
    let fields = [
        (11, "1718096506197867067".to_string()),
        (14, "0.00100000".to_string()),
        (17, exec_id.to_string()),
        (32, "0.00100000".to_string()),
        (37, "76".to_string()),
        (38, "5.00000000".to_string()),
        (39, "1".to_string()),
        (40, "2".to_string()),
        (44, "10.00000000".to_string()),
        (54, "1".to_string()),
        (55, "LTCBNB".to_string()),
        (59, "4".to_string()),
        (60, "20240611-09:01:46.228000".to_string()),
        (150, "F".to_string()),
        (151, "4.99900000".to_string()),
        (636, "Y".to_string()),
        (1057, "Y".to_string()),
        (25001, "1".to_string()),
        (25017, "0.01000000".to_string()),
        (25018, "20240611-09:01:46.228000".to_string()),
        (25023, "20240611-09:01:46.228000".to_string()),
    ];
    let fee = TestGroupEntry {
        fields: vec![
            (137, "0.00000100".into()),
            (138, "LTC".into()),
            (139, "4".into()),
        ],
        groups: Vec::new(),
    };
    TestMessage {
        msg_type: "8".into(),
        header_fields: Vec::new(),
        fields: fields.into_iter().collect(),
        groups: vec![(136, vec![fee])],
    }
}

fn fields(message: &[u8]) -> BTreeMap<i32, String> {
    message
        .split(|byte| *byte == 1)
        .filter_map(|field| {
            let text = std::str::from_utf8(field).ok()?;
            let (tag, value) = text.split_once('=')?;
            Some((tag.parse().ok()?, value.to_string()))
        })
        .collect()
}

/// The Logon carries Binance's fields and a signature that verifies against the header the
/// venue received.
fn assert_signed_logon(logon: &[u8], public_key: &[u8]) {
    let logon = fields(logon);
    let field = |tag: i32| logon.get(&tag).map(String::as_str);
    assert_eq!(field(141), Some("Y"), "ResetSeqNumFlag");
    assert_eq!(field(553), Some(API_KEY), "Username");
    assert_eq!(field(25035), Some("2"), "MessageHandling");
    assert_eq!(field(9406), Some("Y"), "DropCopyFlag");
    assert_eq!(
        field(383),
        None,
        "Binance's Logon defines no MaxMessageSize"
    );
    assert_eq!(field(554), None, "no password");
    let signature = field(96).expect("RawData");
    assert_eq!(field(95), Some(signature.len().to_string().as_str()));
    let payload = ["A", field(49).expect("49"), field(56).expect("56")]
        .into_iter()
        .chain([field(34).expect("34"), field(52).expect("52")])
        .collect::<Vec<_>>()
        .join("\u{1}");
    let signature = base64::engine::general_purpose::STANDARD
        .decode(signature)
        .expect("base64");
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(payload.as_bytes(), &signature)
        .expect("the Logon signature verifies against the received header");
}

fn int64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(name)
                .expect("column")
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

fn series_and_gaps(batches: &[RecordBatch]) -> Vec<(u32, bool)> {
    batches
        .iter()
        .flat_map(|batch| {
            let series = batch
                .column_by_name("_fix_series")
                .expect("series")
                .as_primitive::<UInt32Type>()
                .clone();
            let gaps = batch
                .column_by_name("_fix_gap_before")
                .expect("gap")
                .as_boolean()
                .clone();
            (0..batch.num_rows())
                .map(|row| (series.value(row), gaps.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_logon_resets_every_session_and_flags_the_reconnect_gap() {
    let dir = tempfile::tempdir().expect("dir");
    let venue = FixTestVenue::start_with_settings(
        &dir.path().join("venue"),
        Path::new(&dictionary()),
        "FIX.4.4",
        "SPOT",
        CLIENT,
        &[("TimestampPrecision", "6".to_string())],
    )
    .expect("venue");
    let port = venue.port().to_string();
    let (key, public_key) = signing_key(dir.path());
    let extra = [
        ("host", "127.0.0.1"),
        ("port", port.as_str()),
        ("transport", "tcp"),
        ("logon.key.file", key.as_str()),
    ];
    let mut source = start(config(&dir.path().join("journal"), &extra), None).await;
    assert!(
        venue.wait_logged_on(Duration::from_secs(20)),
        "signed logon"
    );
    assert_signed_logon(&venue.logon_requests()[0], &public_key);

    venue.send(&execution_report(144)).expect("send");
    venue.send(&execution_report(145)).expect("send");
    let (first, _) = collect(&mut source, 2).await;
    assert_eq!(int64(&first, "exec_id"), vec![144, 145]);
    assert_eq!(int64(&first, "order_id"), vec![76, 76]);
    assert_eq!(series_and_gaps(&first), vec![(1, false), (1, false)]);
    let fees = first[0]
        .column_by_name("fees")
        .expect("fees")
        .as_list::<i32>();
    assert_eq!(fees.value(0).len(), 1, "one fee entry");

    // Binance answers no ResendRequest, so whatever it sent while the session was down is
    // gone: the next series starts with a flagged row.
    venue.reset_session().expect("disconnect");
    let deadline = Instant::now() + Duration::from_secs(20);
    while venue.logons() < 2 || !venue.logged_on() {
        assert!(Instant::now() < deadline, "second logon");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_signed_logon(&venue.logon_requests()[1], &public_key);
    venue.send(&execution_report(146)).expect("send");
    venue.send(&execution_report(147)).expect("send");
    let (second, _) = collect(&mut source, 2).await;
    assert_eq!(int64(&second, "exec_id"), vec![146, 147]);
    assert_eq!(series_and_gaps(&second), vec![(2, true), (2, false)]);
    source.close().await.expect("close");
    venue.stop().expect("stop venue");
}

#[tokio::test(flavor = "multi_thread")]
async fn binance_rejects_a_key_that_is_not_ed25519_before_connecting() {
    let dir = tempfile::tempdir().expect("dir");
    let key = dir.path().join("not-a-key.pem");
    std::fs::write(&key, "not a key").expect("write");
    let key = key.display().to_string();
    let extra = [
        ("host", "127.0.0.1"),
        ("port", "1"),
        ("transport", "tcp"),
        ("logon.key.file", key.as_str()),
    ];
    let mut source = FixSource::new(None);
    let config = config(&dir.path().join("journal"), &extra);
    let binding = source
        .resolve_schema(&config, Some(schema()))
        .await
        .expect("resolve");
    let mut config = config;
    config.set_schema_binding(binding).expect("binding");
    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .expect("request"),
        )
        .await
        .expect_err("unusable key");
    assert!(error.to_string().contains("logon.key.file"), "{error}");
}

fn logged_on(registry: &prometheus::Registry) -> bool {
    let mut text = Vec::new();
    prometheus::Encoder::encode(
        &prometheus::TextEncoder::new(),
        &registry.gather(),
        &mut text,
    )
    .expect("encode");
    String::from_utf8_lossy(&text)
        .lines()
        .any(|line| line.starts_with("fix_source_logged_on{") && line.ends_with(" 1"))
}

/// Live logon to Binance's Spot testnet drop copy over TLS. Needs a testnet Ed25519 API key
/// with FIX permission: `LAMINAR_FIX_BINANCE_TESTNET_API_KEY`,
/// `LAMINAR_FIX_BINANCE_TESTNET_KEY_FILE`, and `LAMINAR_FIX_BINANCE_DICTIONARY` (Binance's
/// `spot-fix-oe.xml`). Rows appear only for orders placed on the account during the run.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Binance Spot testnet credentials and network access"]
async fn binance_testnet_drop_copy_logs_on() {
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("set {name}"));
    let api_key = var("LAMINAR_FIX_BINANCE_TESTNET_API_KEY");
    let key_file = var("LAMINAR_FIX_BINANCE_TESTNET_KEY_FILE");
    var("LAMINAR_FIX_BINANCE_DICTIONARY");
    let dir = tempfile::tempdir().expect("dir");
    let journal: PathBuf = dir.path().join("journal");
    let extra = [
        ("host", "fix-dc.testnet.binance.vision"),
        ("port", "9000"),
        ("username", api_key.as_str()),
        ("logon.key.file", key_file.as_str()),
    ];
    let registry = Arc::new(prometheus::Registry::new());
    let mut source = start(config(&journal, &extra), Some(registry.clone())).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !logged_on(&registry) {
        assert!(Instant::now() < deadline, "testnet logon");
        if let Err(error) = source.poll_batch(1024).await {
            panic!("source failed before logon: {error}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut rows = 0;
    let until = Instant::now() + Duration::from_secs(15);
    while Instant::now() < until {
        if let Some(batch) = source.poll_batch(1024).await.expect("poll") {
            rows += batch.num_rows();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(logged_on(&registry), "session stayed logged on");
    eprintln!("Binance testnet drop copy: logged on, {rows} rows in 15 s");
    source.close().await.expect("close");
}
