//! Process-kill recovery for the FIX source. The parent process hosts a local QuickFIX acceptor
//! and streams fills; a child process (this test binary, re-executed) runs the pipeline into a
//! files sink and is killed without warning several times. Every fill the venue sent must reach
//! the sink at least once. This is process-kill evidence only; it does not model power loss.
#![cfg(all(feature = "fix", feature = "files"))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_connectors::connector::DeliveryGuarantee;
use laminar_connectors::fix::testing::{FixTestVenue, TestMessage};
use laminar_db::LaminarDB;

const DICTIONARY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../laminar-connectors/tests/fixtures/fix/FIX44-synthetic.xml"
);
const CHILD: &str = "LAMINAR_FIX_CRASH_CHILD";
const CHILD_DIR: &str = "LAMINAR_FIX_CRASH_DIR";
const CHILD_PORT: &str = "LAMINAR_FIX_CRASH_PORT";
const FILLS: usize = 300;
const KILLS: usize = 5;

fn sql_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

/// Child role: run the pipeline until the parent kills this process.
#[tokio::test(flavor = "multi_thread")]
async fn crash_child_pipeline() {
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
            interval_ms: Some(150),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .build()
        .await
        .expect("open");
    let statements = [
        format!(
            "CREATE SOURCE fills (exec_id VARCHAR NOT NULL, _fix_seq_num BIGINT NOT NULL) FROM FIX (
                host = '127.0.0.1', port = '{port}', transport = 'tcp',
                'sender.comp.id' = 'LAMINAR_CRASH', 'target.comp.id' = 'VENUE_CRASH',
                'dictionary.path' = '{dictionary}', messages = '8',
                'journal.path' = '{journal}', 'reconnect.interval.seconds' = '1',
                'column.exec_id' = '17')",
            dictionary = DICTIONARY.replace('\\', "/"),
            journal = sql_path(&dir.join("journal")),
        ),
        format!(
            "CREATE SINK out FROM fills INTO FILES (path = '{}') FORMAT JSON",
            sql_path(&dir.join("out"))
        ),
    ];
    let deadline = Instant::now() + Duration::from_secs(60);
    'register: loop {
        for statement in &statements {
            if let Err(error) = db.execute(statement).await {
                // The previous generation may still hold the checkpoint or journal lock briefly.
                assert!(Instant::now() < deadline, "{statement}: {error}");
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue 'register;
            }
        }
        break;
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
            "crash_child_pipeline",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .env(CHILD_DIR, dir)
        .env(CHILD_PORT, port.to_string())
        .spawn()
        .expect("spawn child pipeline")
}

fn sink_counts(out: &Path) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(out) else {
        return counts;
    };
    for path in entries.filter_map(Result::ok).map(|entry| entry.path()) {
        if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines() {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if let Some(id) = value["exec_id"].as_str() {
                *counts.entry(id.to_string()).or_insert(0) += 1;
            }
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
async fn process_kills_lose_no_fix_message_the_venue_sent() {
    if std::env::var_os(CHILD).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("dir");
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).expect("out");
    let venue = Arc::new(
        FixTestVenue::start(
            &dir.path().join("venue"),
            Path::new(DICTIONARY),
            "FIX.4.4",
            "VENUE_CRASH",
            "LAMINAR_CRASH",
        )
        .expect("venue"),
    );
    let mut child = spawn_child(dir.path(), venue.port());

    // The venue persists every fill and retransmits whatever the client did not acknowledge, so
    // fills sent while the client is down are delivered after it logs on again.
    let sent = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let sender = {
        let (venue, sent, done) = (venue.clone(), sent.clone(), done.clone());
        std::thread::spawn(move || {
            for index in 0..FILLS {
                venue.send(&fill(index)).expect("send");
                sent.store(index + 1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
            }
            done.store(true, Ordering::SeqCst);
        })
    };

    let mut kills = 0;
    while kills < KILLS {
        // Kill at varying points: during logon, live streaming, checkpoints, and sink publication.
        let wait = Duration::from_millis(700 + 431 * kills as u64 % 1300);
        std::thread::sleep(wait);
        child.kill().expect("kill child");
        let _ = child.wait();
        kills += 1;
        child = spawn_child(dir.path(), venue.port());
    }
    sender.join().expect("sender");
    assert!(done.load(Ordering::SeqCst));

    let deadline = Instant::now() + Duration::from_secs(120);
    let expected: Vec<String> = (0..FILLS).map(|index| format!("E{index:04}")).collect();
    let counts = loop {
        let counts = sink_counts(&out);
        if expected.iter().all(|id| counts.contains_key(id)) {
            break counts;
        }
        assert!(
            Instant::now() < deadline,
            "after {kills} kills, {} of {FILLS} fills reached the sink; first missing {:?}",
            expected
                .iter()
                .filter(|id| counts.contains_key(*id))
                .count(),
            expected.iter().find(|id| !counts.contains_key(*id))
        );
        std::thread::sleep(Duration::from_millis(250));
    };
    child.kill().expect("kill final child");
    let _ = child.wait();
    venue.stop().expect("stop venue");

    let duplicates = counts.values().filter(|count| **count > 1).count();
    eprintln!(
        "process-kill run: {kills} kills, {FILLS} fills sent, all delivered, {duplicates} delivered more than once"
    );
    assert_eq!(counts.len(), FILLS, "no unexpected rows");
}
