//! End-to-end: a local QuickFIX acceptor -> `FROM FIX` source -> keyed aggregate, event-time
//! window, and a files sink, across a checkpointed restart. The counterparty is a real engine
//! acceptor on loopback; this is local conformance evidence, not venue certification.
#![cfg(feature = "fix")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_connectors::fix::testing::{FixTestVenue, TestMessage};
use laminar_db::{ExecuteResult, LaminarDB};

const DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../laminar-connectors/tests/fixtures/fix/FIX44-synthetic.xml"
);
const TRANSPORT_DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../laminar-connectors/tests/fixtures/fix/FIXT11-synthetic.xml"
);
const APPLICATION_DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../laminar-connectors/tests/fixtures/fix/FIX50SP2-synthetic.xml"
);
const CONVERGE: Duration = Duration::from_secs(30);

fn sql_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

/// Test binaries run tests concurrently in one process, and engine sessions are process-wide,
/// so each test uses its own CompIDs.
fn venue(dir: &Path, name: &str) -> FixTestVenue {
    FixTestVenue::start(
        &dir.join("venue"),
        Path::new(DICTIONARY),
        "FIX.4.4",
        &format!("VENUE_{name}"),
        &format!("LAMINAR_{name}"),
    )
    .expect("venue")
}

fn statements(
    name: &str,
    venue: &FixTestVenue,
    journal: &Path,
    out: &Path,
    messages: &str,
) -> Vec<String> {
    vec![
        format!(
            "CREATE SOURCE fills (
                exec_id VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                last_qty DECIMAL(18, 4) NOT NULL,
                transact_time TIMESTAMP NOT NULL,
                _fix_seq_num BIGINT NOT NULL,
                WATERMARK FOR transact_time AS transact_time - INTERVAL '1' SECOND
             ) FROM FIX (
                host = '127.0.0.1',
                port = '{port}',
                transport = 'tcp',
                'sender.comp.id' = 'LAMINAR_{name}',
                'target.comp.id' = 'VENUE_{name}',
                'dictionary.path' = '{dictionary}',
                messages = '{messages}',
                'journal.path' = '{journal}',
                'reconnect.interval.seconds' = '1',
                'column.exec_id' = '17',
                'column.symbol' = '55',
                'column.last_qty' = '32',
                'column.transact_time' = '60'
             )",
            port = venue.port(),
            dictionary = DICTIONARY.replace('\\', "/"),
            journal = sql_path(journal),
        ),
        "CREATE MATERIALIZED VIEW totals AS \
         SELECT symbol, COUNT(*) AS fills FROM fills GROUP BY symbol"
            .to_string(),
        "CREATE MATERIALIZED VIEW per_second AS \
         SELECT symbol, tumble(transact_time, INTERVAL '1' SECOND) AS window_start, \
                COUNT(*) AS fills \
         FROM fills GROUP BY symbol, tumble(transact_time, INTERVAL '1' SECOND)"
            .to_string(),
        format!(
            "CREATE SINK raw_out FROM fills INTO FILES (path = '{}') FORMAT JSON",
            sql_path(out)
        ),
    ]
}

fn trace() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

async fn open(storage: &Path) -> Arc<LaminarDB> {
    trace();
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .build()
        .await
        .expect("open database")
}

/// Open and register `statements`, waiting for the previous generation's locks.
async fn reopen(storage: &Path, statements: &[String]) -> Arc<LaminarDB> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let db = open(storage).await;
        let mut failure = None;
        for statement in statements {
            if let Err(error) = db.execute(statement).await {
                failure = Some(format!("{statement}: {error}"));
                break;
            }
        }
        match failure {
            None => return db,
            Some(error) if error.contains("LDB-0014") && tokio::time::Instant::now() < deadline => {
                drop(db);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Some(error) => panic!("{error}"),
        }
    }
}

fn fill(exec_id: &str, symbol: &str, qty: &str, transact_time: &str) -> TestMessage {
    let mut fields: Vec<(i32, String)> = vec![(37, format!("ORD-{exec_id}")), (17, exec_id.into())];
    for (tag, value) in [
        (150, "F"),
        (39, "1"),
        (55, symbol),
        (54, "1"),
        (32, qty),
        (31, "100.25"),
    ] {
        fields.push((tag, value.into()));
    }
    for (tag, value) in [(151, "0"), (14, qty), (6, "100.25"), (60, transact_time)] {
        fields.push((tag, value.into()));
    }
    TestMessage {
        msg_type: "8".into(),
        header_fields: Vec::new(),
        fields,
        groups: Vec::new(),
    }
}

async fn checkpoint(db: &LaminarDB) {
    match db.checkpoint().await {
        Ok(result) => assert!(result.success, "checkpoint failed: {result:?}"),
        Err(error) => {
            tokio::time::sleep(Duration::from_secs(1)).await;
            panic!(
                "checkpoint: {error}; pipeline {} fault {:?}",
                db.pipeline_state(),
                db.last_fault()
            )
        }
    }
}

async fn totals(db: &LaminarDB) -> BTreeMap<String, i64> {
    let ExecuteResult::Query(mut query) = db.execute("SELECT * FROM totals").await.expect("query")
    else {
        panic!("expected a query result");
    };
    let rows = query.subscribe_raw().expect("subscribe");
    let batches = tokio::time::timeout(CONVERGE, rows.read_all())
        .await
        .expect("snapshot in time")
        .expect("query");
    let mut rows = BTreeMap::new();
    for batch in batches {
        insert_rows(&batch, &mut rows);
    }
    rows
}

fn insert_rows(batch: &RecordBatch, rows: &mut BTreeMap<String, i64>) {
    let symbols = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("symbol");
    let fills = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("fills");
    for row in 0..batch.num_rows() {
        rows.insert(symbols.value(row).to_string(), fills.value(row));
    }
}

async fn wait_totals(db: &LaminarDB, expected: &BTreeMap<String, i64>) {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let observed = totals(db).await;
        if &observed == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "totals {observed:?} never reached {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn sink_exec_ids(out: &Path) -> Vec<String> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(out)
        .expect("sink directory")
        .filter_map(Result::ok)
    {
        let path: PathBuf = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
            continue;
        }
        for line in std::fs::read_to_string(&path).expect("sink file").lines() {
            let value: serde_json::Value = serde_json::from_str(line).expect("json row");
            ids.push(value["exec_id"].as_str().expect("exec_id").to_string());
        }
    }
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn fix_source_feeds_sql_aggregates_windows_and_a_sink_across_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let venue = venue(dir.path(), "FEEDS");
    let storage = dir.path().join("storage");
    let journal = dir.path().join("journal");
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).expect("out");
    let ddl = statements("FEEDS", &venue, &journal, &out, "8");

    let db = reopen(&storage, &ddl).await;
    let mut windows = db
        .open_subscription(
            "per_second",
            None,
            laminar_db::subscription::SubscribeStart::Tail,
        )
        .await
        .expect("window subscription");
    db.start().await.expect("start");
    assert!(venue.wait_logged_on(CONVERGE), "FIX logon");
    let first = [
        ("E1", "AAA", "10", "20261014-12:00:00.100000"),
        ("E2", "BBB", "20", "20261014-12:00:00.200000"),
        ("E3", "AAA", "30", "20261014-12:00:00.900000"),
        ("E4", "AAA", "5", "20261014-12:00:03.000000"),
    ];
    for (exec, symbol, qty, time) in first {
        venue.send(&fill(exec, symbol, qty, time)).expect("send");
    }
    wait_totals(&db, &BTreeMap::from([("AAA".into(), 3), ("BBB".into(), 1)])).await;

    // The view refines each (symbol, window) row as fills arrive; wait for the first window's
    // final counts: E1 and E3 for AAA, E2 for BBB.
    // 2026-10-14T12:00:00Z in epoch microseconds (the view keeps the source's TIMESTAMP unit).
    let first_window = 1_791_979_200_000_000_i64;
    let expected = BTreeMap::from([
        (("AAA".to_string(), first_window), 2),
        (("BBB".to_string(), first_window), 1),
    ]);
    let mut windows_seen: BTreeMap<(String, i64), i64> = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + CONVERGE;
    while expected
        .iter()
        .any(|(key, fills)| windows_seen.get(key) != Some(fills))
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "windows {windows_seen:?} never reached {expected:?}"
        );
        if let Ok(Some(laminar_db::subscription::PortalFrame::Batch { batch, .. })) =
            tokio::time::timeout(Duration::from_millis(500), windows.next_frame()).await
        {
            let symbols = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("symbol");
            let starts = arrow::compute::cast(batch.column(1), &arrow::datatypes::DataType::Int64)
                .expect("start");
            let starts = starts.as_any().downcast_ref::<Int64Array>().expect("start");
            let fills = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("fills");
            for row in 0..batch.num_rows() {
                windows_seen.insert(
                    (symbols.value(row).to_string(), starts.value(row)),
                    fills.value(row),
                );
            }
        }
    }

    checkpoint(&db).await;
    db.shutdown().await.expect("shutdown");
    drop(windows);
    drop(db);

    // Messages sent while the pipeline is down wait in the venue store and arrive after logon.
    venue
        .send(&fill("E5", "BBB", "7", "20261014-12:00:04.000000"))
        .expect("send");
    let db = reopen(&storage, &ddl).await;
    db.start().await.expect("restart");
    venue
        .send(&fill("E6", "AAA", "1", "20261014-12:00:05.000000"))
        .expect("send");
    wait_totals(&db, &BTreeMap::from([("AAA".into(), 4), ("BBB".into(), 2)])).await;
    checkpoint(&db).await;
    db.shutdown().await.expect("shutdown");
    drop(db);

    let ids = sink_exec_ids(&out);
    for expected in ["E1", "E2", "E3", "E4", "E5", "E6"] {
        assert!(
            ids.contains(&expected.to_string()),
            "{expected} missing from sink output {ids:?}"
        );
    }
    venue.stop().expect("stop venue");
}

#[tokio::test(flavor = "multi_thread")]
async fn fixt_source_feeds_a_sql_aggregate() {
    let dir = tempfile::tempdir().expect("dir");
    let venue = FixTestVenue::start_with_settings(
        &dir.path().join("venue"),
        Path::new(APPLICATION_DICTIONARY),
        "FIXT.1.1",
        "VENUE_FIXT",
        "LAMINAR_FIXT",
        &[
            ("TransportDataDictionary", TRANSPORT_DICTIONARY.to_string()),
            ("AppDataDictionary", APPLICATION_DICTIONARY.to_string()),
            ("DefaultApplVerID", "FIX.5.0SP2".to_string()),
        ],
    )
    .expect("venue");
    let ddl = [
        format!(
            "CREATE SOURCE fills (symbol VARCHAR NOT NULL) FROM FIX (
                host = '127.0.0.1',
                port = '{port}',
                transport = 'tcp',
                'begin.string' = 'FIXT.1.1',
                'appl.ver.id' = 'FIX.5.0SP2',
                'sender.comp.id' = 'LAMINAR_FIXT',
                'target.comp.id' = 'VENUE_FIXT',
                'dictionary.path' = '{application}',
                'transport.dictionary.path' = '{transport}',
                messages = '8',
                'journal.path' = '{journal}',
                'reconnect.interval.seconds' = '1',
                'column.symbol' = '55'
             )",
            port = venue.port(),
            application = APPLICATION_DICTIONARY.replace('\\', "/"),
            transport = TRANSPORT_DICTIONARY.replace('\\', "/"),
            journal = sql_path(&dir.path().join("journal")),
        ),
        "CREATE MATERIALIZED VIEW totals AS \
         SELECT symbol, COUNT(*) AS fills FROM fills GROUP BY symbol"
            .to_string(),
    ];
    let db = reopen(&dir.path().join("storage"), &ddl).await;
    db.start().await.expect("start");
    assert!(venue.wait_logged_on(CONVERGE), "FIXT logon");
    for (exec, symbol) in [("F1", "AAA"), ("F2", "BBB"), ("F3", "AAA")] {
        venue
            .send(&fill(exec, symbol, "10", "20261014-12:00:00"))
            .expect("send");
    }
    wait_totals(&db, &BTreeMap::from([("AAA".into(), 2), ("BBB".into(), 1)])).await;
    db.shutdown().await.expect("shutdown");
    venue.stop().expect("stop venue");
}

#[tokio::test(flavor = "multi_thread")]
async fn incompatible_profile_change_is_rejected_at_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let venue = venue(dir.path(), "PROFILE");
    let storage = dir.path().join("storage");
    let journal = dir.path().join("journal");
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).expect("out");
    let db = reopen(
        &storage,
        &statements("PROFILE", &venue, &journal, &out, "8"),
    )
    .await;
    db.start().await.expect("start");
    assert!(
        venue.wait_logged_on(CONVERGE),
        "no logon: state {} fault {:?}",
        db.pipeline_state(),
        db.last_fault()
    );
    checkpoint(&db).await;
    db.shutdown().await.expect("shutdown");
    drop(db);

    let changed = statements("PROFILE", &venue, &journal, &out, "8,U1");
    let db = open(&storage).await;
    let mut outcome = Ok(());
    for statement in &changed {
        if let Err(error) = db.execute(statement).await {
            outcome = Err(error.to_string());
            break;
        }
    }
    if outcome.is_ok() {
        outcome = db.start().await.map_err(|error| error.to_string());
    }
    let error = outcome.expect_err("a changed message selection must not resume the old journal");
    assert!(
        error.contains("LDB-6041") || error.contains("capture contract") || error.contains("DDL"),
        "{error}"
    );
    let _ = db.shutdown().await;
    venue.stop().expect("stop venue");
}

#[tokio::test(flavor = "multi_thread")]
async fn fix_source_requires_checkpointed_at_least_once_delivery() {
    let dir = tempfile::tempdir().expect("dir");
    let venue = venue(dir.path(), "ADMISSION");
    let db = LaminarDB::open().expect("open");
    let journal = dir.path().join("journal");
    let out = dir.path().join("out");
    let ddl = statements("ADMISSION", &venue, &journal, &out, "8");
    db.execute(&ddl[0]).await.expect("create source");
    db.execute(&ddl[1]).await.expect("create view");
    let error = db
        .start()
        .await
        .expect_err("best-effort without checkpoints is not admitted");
    assert!(!venue.logged_on(), "rejected before connecting: {error}");
    venue.stop().expect("stop venue");
}

/// FpML confirmations in SecurityXML (1185): the source appends an `fpml` struct column that
/// SQL reads like any struct, and the committed contract survives a restart.
#[cfg(feature = "fpml")]
#[tokio::test(flavor = "multi_thread")]
async fn fpml_in_security_xml_feeds_sql_across_restart() {
    const CORPUS: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../laminar-connectors/tests/fixtures/fpml/fpml-5-13-8-rec-2/confirmation/"
    );
    let dir = tempfile::tempdir().expect("dir");
    let venue = FixTestVenue::start_with_settings(
        &dir.path().join("venue"),
        Path::new(APPLICATION_DICTIONARY),
        "FIXT.1.1",
        "VENUE_FPML",
        "LAMINAR_FPML",
        &[
            ("TransportDataDictionary", TRANSPORT_DICTIONARY.to_string()),
            ("AppDataDictionary", APPLICATION_DICTIONARY.to_string()),
            ("DefaultApplVerID", "FIX.5.0SP2".to_string()),
        ],
    )
    .expect("venue");
    let ddl = [
        format!(
            "CREATE SOURCE fills (symbol VARCHAR NOT NULL) FROM FIX (
                host = '127.0.0.1',
                port = '{port}',
                transport = 'tcp',
                'begin.string' = 'FIXT.1.1',
                'appl.ver.id' = 'FIX.5.0SP2',
                'sender.comp.id' = 'LAMINAR_FPML',
                'target.comp.id' = 'VENUE_FPML',
                'dictionary.path' = '{application}',
                'transport.dictionary.path' = '{transport}',
                messages = '8',
                'journal.path' = '{journal}',
                'max.message.bytes' = '65536',
                'reconnect.interval.seconds' = '1',
                'column.symbol' = '55',
                'fpml.field' = '1185'
             )",
            port = venue.port(),
            application = APPLICATION_DICTIONARY.replace('\\', "/"),
            transport = TRANSPORT_DICTIONARY.replace('\\', "/"),
            journal = sql_path(&dir.path().join("journal")),
        ),
        "CREATE STREAM roots AS \
         SELECT COALESCE(fpml['document_root'], 'none') AS root FROM fills"
            .to_string(),
        "CREATE MATERIALIZED VIEW totals AS \
         SELECT root, COUNT(*) AS fills FROM roots GROUP BY root"
            .to_string(),
    ];
    let send = |exec_id: &str, document: Option<&str>| {
        let mut message = fill(exec_id, "AAA", "10", "20261014-12:00:00");
        if let Some(path) = document {
            let xml = std::fs::read_to_string(format!("{CORPUS}{path}")).expect("corpus document");
            message.fields.push((1184, xml.len().to_string()));
            message.fields.push((1185, xml));
        }
        venue.send(&message).expect("send");
    };
    let storage = dir.path().join("storage");
    let db = reopen(&storage, &ddl).await;
    db.start().await.expect("start");
    assert!(venue.wait_logged_on(CONVERGE), "FIXT logon");
    send(
        "F1",
        Some("products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml"),
    );
    send("F2", None);
    send(
        "F3",
        Some("business-processes/clearing/msg-ex10-clearingConfirmed-clearing-performed.xml"),
    );
    let mut expected = BTreeMap::from([
        ("dataDocument".to_string(), 1),
        ("none".to_string(), 1),
        ("clearingConfirmed".to_string(), 1),
    ]);
    wait_totals(&db, &expected).await;
    checkpoint(&db).await;
    db.shutdown().await.expect("shutdown");

    let db = reopen(&storage, &ddl).await;
    db.start().await.expect("restart");
    assert!(venue.wait_logged_on(CONVERGE), "FIXT logon after restart");
    send(
        "F4",
        Some("business-processes/allocation/msg-ex20-cds-request-allocation.xml"),
    );
    expected.insert("requestAllocation".to_string(), 1);
    wait_totals(&db, &expected).await;
    db.shutdown().await.expect("shutdown");
    venue.stop().expect("stop venue");
}
