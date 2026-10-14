//! Exactly-once delivery from the FIX source into a Delta Lake append sink across process kills.
//! The parent process hosts a local QuickFIX acceptor and streams fills; a child process (this
//! test binary, re-executed) runs an exactly-once pipeline and is killed without warning several
//! times. Every fill the venue sent must appear in the committed table exactly once. This is
//! process-kill evidence only; it does not model power loss.
#![cfg(all(feature = "fix", feature = "delta-lake"))]
#![allow(clippy::disallowed_types)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::AsArray;
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_connectors::fix::testing::{FixTestVenue, TestMessage};
use laminar_db::LaminarDB;

const DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../laminar-connectors/tests/fixtures/fix/FIX44-synthetic.xml"
);
const CHILD: &str = "LAMINAR_FIX_EXACT_CHILD";
const CHILD_DIR: &str = "LAMINAR_FIX_EXACT_DIR";
const CHILD_PORT: &str = "LAMINAR_FIX_EXACT_PORT";
const KILLS: usize = 5;

fn sql_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

/// Child role: run the pipeline until the parent kills this process.
#[tokio::test(flavor = "multi_thread")]
async fn exact_child_pipeline() {
    let (Ok(dir), Ok(port)) = (std::env::var(CHILD_DIR), std::env::var(CHILD_PORT)) else {
        return;
    };
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    let dir = PathBuf::from(dir);
    let db = LaminarDB::builder()
        .storage_dir(dir.join("storage"))
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(200),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
        .build()
        .await
        .expect("open");
    let statements = [
        format!(
            "CREATE SOURCE fills (exec_id VARCHAR NOT NULL) FROM FIX (
                host = '127.0.0.1', port = '{port}', transport = 'tcp',
                'sender.comp.id' = 'LAMINAR_EXACT', 'target.comp.id' = 'VENUE_EXACT',
                'dictionary.path' = '{dictionary}', messages = '8',
                'journal.path' = '{journal}', 'reconnect.interval.seconds' = '1',
                'column.exec_id' = '17')",
            dictionary = DICTIONARY.replace('\\', "/"),
            journal = sql_path(&dir.join("journal")),
        ),
        format!(
            "CREATE SINK out FROM fills INTO \"delta-lake\" ('table.path' = '{}', \
             'write.mode' = 'append', 'auto.create' = 'true')",
            sql_path(&dir.join("table"))
        ),
    ];
    let deadline = Instant::now() + Duration::from_secs(60);
    for statement in &statements {
        // The previous generation may still hold the checkpoint or journal lock briefly.
        while let Err(error) = db.execute(statement).await {
            assert!(Instant::now() < deadline, "{statement}: {error}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    loop {
        match db.start().await {
            Ok(()) => break,
            Err(error) => {
                assert!(Instant::now() < deadline, "start: {error}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn spawn_child(dir: &Path, port: u16) -> Child {
    Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "exact_child_pipeline",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .env(CHILD_DIR, dir)
        .env(CHILD_PORT, port.to_string())
        .spawn()
        .expect("spawn child pipeline")
}

/// Committed rows per `exec_id`; empty until the table exists.
async fn table_counts(table: &Path) -> BTreeMap<String, i64> {
    let mut counts = BTreeMap::new();
    if !table.join("_delta_log").exists() {
        return counts;
    }
    let ctx = datafusion::prelude::SessionContext::new();
    if laminar_connectors::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "t",
        &sql_path(table),
        HashMap::new(),
    )
    .await
    .is_err()
    {
        return counts;
    }
    let Ok(frame) = ctx
        .sql("SELECT exec_id, CAST(COUNT(*) AS BIGINT) FROM t GROUP BY exec_id")
        .await
    else {
        return counts;
    };
    let Ok(batches) = frame.collect().await else {
        return counts;
    };
    for batch in batches {
        let ids = arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Utf8)
            .expect("exec_id");
        let ids = ids.as_string::<i32>();
        let rows = batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            counts.insert(ids.value(row).to_string(), rows.value(row));
        }
    }
    counts
}

fn fill(index: usize) -> TestMessage {
    let mut fields: Vec<(i32, String)> =
        vec![(37, format!("ORD-{index}")), (17, format!("E{index:04}"))];
    for (tag, value) in [
        (150, "F"),
        (39, "1"),
        (55, "AAA"),
        (54, "1"),
        (151, "0"),
        (14, "1"),
        (6, "1"),
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

#[tokio::test(flavor = "multi_thread")]
async fn process_kills_commit_every_fix_message_exactly_once() {
    if std::env::var_os(CHILD).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("dir");
    let table = dir.path().join("table");
    let venue = Arc::new(
        FixTestVenue::start(
            &dir.path().join("venue"),
            Path::new(DICTIONARY),
            "FIX.4.4",
            "VENUE_EXACT",
            "LAMINAR_EXACT",
        )
        .expect("venue"),
    );
    let mut child = spawn_child(dir.path(), venue.port());

    // The venue streams until the kill phase ends, so every kill lands with rows in flight.
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicUsize::new(0));
    let sender = {
        let (venue, stop, sent) = (venue.clone(), stop.clone(), sent.clone());
        std::thread::spawn(move || {
            let mut index = 0;
            while !stop.load(Ordering::SeqCst) {
                venue.send(&fill(index)).expect("send");
                index += 1;
                sent.store(index, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    };

    // Each generation commits new rows before it is killed, so every kill follows a committed
    // epoch while later rows are uncommitted: a broken abort or replay path shows up as
    // duplicates.
    let mut committed_at_kill = Vec::with_capacity(KILLS);
    let mut committed = 0;
    let mut kills = 0;
    while kills < KILLS {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let rows = table_counts(&table).await.len();
            if rows > committed {
                committed = rows;
                break;
            }
            assert!(
                Instant::now() < deadline,
                "generation {kills} committed nothing new"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_millis(100 + 173 * kills as u64 % 400)).await;
        committed_at_kill.push(table_counts(&table).await.len());
        child.kill().expect("kill child");
        let _ = child.wait();
        kills += 1;
        child = spawn_child(dir.path(), venue.port());
    }
    stop.store(true, Ordering::SeqCst);
    sender.join().expect("sender");
    let fills = sent.load(Ordering::SeqCst);

    let deadline = Instant::now() + Duration::from_secs(120);
    let expected: Vec<String> = (0..fills).map(|index| format!("E{index:04}")).collect();
    let counts = loop {
        let counts = table_counts(&table).await;
        if expected.iter().all(|id| counts.contains_key(id)) {
            break counts;
        }
        assert!(
            Instant::now() < deadline,
            "after {kills} kills, {} of {fills} fills were committed; first missing {:?}",
            expected
                .iter()
                .filter(|id| counts.contains_key(*id))
                .count(),
            expected.iter().find(|id| !counts.contains_key(*id))
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    child.kill().expect("kill final child");
    let _ = child.wait();
    venue.stop().expect("stop venue");

    let duplicated: Vec<_> = counts.iter().filter(|(_, rows)| **rows != 1).collect();
    eprintln!(
        "exactly-once process-kill run: {kills} kills, {fills} fills, {} committed, {} \
         duplicated; rows committed before each kill: {committed_at_kill:?}",
        counts.len(),
        duplicated.len()
    );
    assert!(
        duplicated.is_empty(),
        "committed more than once: {duplicated:?}"
    );
    assert_eq!(counts.len(), fills, "no unexpected rows");
}
