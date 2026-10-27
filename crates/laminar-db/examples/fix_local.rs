//! Local FIX source walkthrough with no external services.
//!
//! Starts a synthetic QuickFIX acceptor on loopback, creates a `FROM FIX` source and a keyed
//! aggregate through SQL, streams a few ExecutionReports, and prints the aggregate. The venue and
//! dictionary are synthetic test fixtures, not a certified venue profile.
//!
//! ```text
//! cargo run -p laminar-db --example fix_local --features fix
//! ```

use std::path::Path;
use std::time::Duration;

use laminar_connectors::connector::DeliveryGuarantee;
use laminar_connectors::fix::testing::{FixTestVenue, TestGroupEntry, TestMessage};
use laminar_db::{ExecuteResult, LaminarDB};

const DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../laminar-connectors/tests/fixtures/fix/FIX44-synthetic.xml"
);

fn fill(exec_id: &str, symbol: &str, qty: &str, venue_seq: &str, parties: &[&str]) -> TestMessage {
    let mut fields: Vec<(i32, String)> = vec![(37, format!("ORD-{exec_id}")), (17, exec_id.into())];
    for (tag, value) in [
        (150, "F"),
        (39, "2"),
        (55, symbol),
        (54, "1"),
        (32, qty),
        (31, "101.25"),
    ] {
        fields.push((tag, value.into()));
    }
    for (tag, value) in [
        (151, "0"),
        (14, qty),
        (6, "101.25"),
        (60, "20261014-12:00:00.000001"),
        (5001, venue_seq),
    ] {
        fields.push((tag, value.into()));
    }
    let parties = parties
        .iter()
        .map(|party| TestGroupEntry {
            fields: vec![(448, (*party).into()), (447, "D".into()), (452, "1".into())],
            groups: Vec::new(),
        })
        .collect();
    TestMessage {
        msg_type: "8".into(),
        header_fields: Vec::new(),
        fields,
        groups: vec![(453, parties)],
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let venue = FixTestVenue::start(
        &dir.path().join("venue"),
        Path::new(DICTIONARY),
        "FIX.4.4",
        "VENUE",
        "LAMINAR",
    )?;
    let db = LaminarDB::builder()
        .storage_dir(dir.path().join("storage"))
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(1_000),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .build()
        .await?;
    let journal = dir
        .path()
        .join("journal")
        .display()
        .to_string()
        .replace('\\', "/");
    db.execute(&format!(
        "CREATE SOURCE fills (
            exec_id       VARCHAR NOT NULL,
            symbol        VARCHAR NOT NULL,
            last_qty      DECIMAL(18, 4) NOT NULL,
            venue_seq     BIGINT,
            party_ids     ARRAY<VARCHAR>,
            transact_time TIMESTAMP NOT NULL,
            _fix_seq_num  BIGINT NOT NULL
         ) FROM FIX (
            host = '127.0.0.1', port = '{port}', transport = 'tcp',
            'sender.comp.id' = 'LAMINAR', 'target.comp.id' = 'VENUE',
            'dictionary.path' = '{dictionary}', messages = '8',
            'journal.path' = '{journal}',
            'column.exec_id' = '17', 'column.symbol' = '55', 'column.last_qty' = '32',
            'column.venue_seq' = '5001', 'column.party_ids' = '453.448',
            'column.transact_time' = '60'
         )",
        port = venue.port(),
        dictionary = DICTIONARY.replace('\\', "/"),
    ))
    .await?;
    db.execute(
        "CREATE MATERIALIZED VIEW by_symbol AS \
         SELECT symbol, COUNT(*) AS fills, SUM(last_qty) AS quantity FROM fills GROUP BY symbol",
    )
    .await?;
    db.start().await?;
    if !venue.wait_logged_on(Duration::from_secs(20)) {
        return Err("the FIX session did not log on".into());
    }
    for (exec_id, symbol, qty, seq) in [
        ("E1", "AAA", "100", "1"),
        ("E2", "BBB", "50", "2"),
        ("E3", "AAA", "25.5", "3"),
    ] {
        venue.send(&fill(exec_id, symbol, qty, seq, &["BROKER-1", "CLIENT-7"]))?;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    if let ExecuteResult::Query(mut query) = db.execute("SELECT * FROM by_symbol").await? {
        let results = query.subscribe_raw()?;
        for batch in tokio::time::timeout(Duration::from_secs(10), results.read_all()).await?? {
            for row in 0..batch.num_rows() {
                let cells = batch
                    .columns()
                    .iter()
                    .map(|column| arrow::util::display::array_value_to_string(column, row))
                    .collect::<Result<Vec<_>, _>>()?;
                println!("{}", cells.join(" | "));
            }
        }
    }
    db.shutdown().await?;
    venue.stop()?;
    Ok(())
}
