//! Standalone-server FpML soak: official FpML documents arrive continuously in a directory while
//! the server is repeatedly hard-killed; every expected trade row must reach the sink, and no row
//! may appear that the documents do not contain.
//!
//! Run with `cargo test --release -p laminar-server --features "fpml files" --test
//! fpml_files_soak -- --ignored --nocapture`. `LAMINAR_FPML_SOAK_SECONDS` (default 120),
//! `LAMINAR_FPML_SOAK_KILL_EVERY_SECONDS` (default 15), and `LAMINAR_FPML_SOAK_DOCS_PER_SECOND`
//! (default 25) size the run.

#![cfg(all(feature = "fpml", feature = "files"))]
#![allow(clippy::disallowed_types)]

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

const FIXTURES: &str = "../laminar-connectors/tests/fixtures/fpml";
const CORPUS: &str = "fpml-5-13-8-rec-2/confirmation";
const GOLDENS: &str = "goldens/fpml-5-13-confirmation-trade";

type RowKey = (String, String);

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// An official document and the trade-profile row locators its golden records.
struct Template {
    bytes: Vec<u8>,
    locators: Vec<String>,
}

/// Every official document whose trade-profile golden is a row list or a zero-row entry; any
/// rejected document would halt the guaranteed source for a reason unrelated to recovery.
fn templates() -> Vec<Template> {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURES);
    let goldens = fixtures.join(GOLDENS);
    let mut templates = Vec::new();
    for line in std::fs::read_to_string(goldens.join("zero-rows.txt"))
        .unwrap()
        .lines()
    {
        let Some(relative) = line.strip_prefix("official/") else {
            continue;
        };
        templates.push(Template {
            bytes: std::fs::read(fixtures.join(CORPUS).join(relative)).unwrap(),
            locators: Vec::new(),
        });
    }
    let mut pending = vec![goldens.join("official")];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let golden: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            let Some(rows) = golden.as_array() else {
                continue;
            };
            let relative = path
                .strip_prefix(goldens.join("official"))
                .unwrap()
                .with_extension("");
            templates.push(Template {
                bytes: std::fs::read(fixtures.join(CORPUS).join(relative)).unwrap(),
                locators: rows
                    .iter()
                    .map(|row| row["row_locator"].as_str().unwrap().to_owned())
                    .collect(),
            });
        }
    }
    templates.sort_by(|left, right| left.bytes.cmp(&right.bytes));
    templates
}

fn server_config(root: &Path, input: &Path, output: &Path) -> String {
    let checkpoint = path_string(&root.join("checkpoints"));
    let checkpoint_url = if checkpoint.starts_with('/') {
        format!("file://{checkpoint}")
    } else {
        format!("file:///{checkpoint}")
    };
    let sql = format!(
        "CREATE SOURCE trades FROM FILES ('path' = '{input}', 'glob_pattern' = '*.xml', \
         'poll_interval' = '50ms', 'stabilisation_delay' = '0s', \
         'fpml.profile' = 'fpml-5-13-confirmation-trade') FORMAT FPML;
         CREATE STREAM trade_rows AS SELECT document_sha256, row_locator FROM trades;
         CREATE SINK trade_rows_out FROM trade_rows INTO FILES ('path' = '{output}') FORMAT JSON;",
        input = path_string(input),
        output = path_string(output),
    );
    // TOML basic strings accept JSON string escapes; `sql` precedes the first table header.
    format!(
        "sql = {sql}\n[server]\nbind = {bind}\ndelivery = \"at_least_once\"\n\
         [checkpoint]\nurl = {checkpoint_url}\ninterval = \"500ms\"\n",
        sql = serde_json::to_string(&sql).unwrap(),
        bind = serde_json::to_string(&format!("127.0.0.1:{}", free_port())).unwrap(),
        checkpoint_url = serde_json::to_string(&checkpoint_url).unwrap(),
    )
}

fn spawn_server(config: &Path, log: &Path) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    Command::new(env!("CARGO_BIN_EXE_laminardb"))
        .arg("--config")
        .arg(config)
        .args(["--log-level", "warn"])
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("start laminardb")
}

/// Resident memory of `pid` in MiB.
fn resident_mib(pid: u32) -> Option<u64> {
    #[cfg(windows)]
    {
        let output = Command::new("powershell")
            .args([
                "-NoProfile",
                "-Command",
                &format!("(Get-Process -Id {pid}).WorkingSet64"),
            ])
            .output()
            .ok()?;
        String::from_utf8(output.stdout)
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()
            .map(|bytes| bytes >> 20)
    }
    #[cfg(not(windows))]
    {
        std::fs::read_to_string(format!("/proc/{pid}/status"))
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .and_then(|value| {
                value
                    .trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .map(|kib| kib >> 10)
    }
}

/// Write document `index` (a template made unique by a trailing comment) under a name the glob
/// excludes, then publish it by rename so the source never reads a torn file.
fn write_document(
    input: &Path,
    templates: &[Template],
    index: usize,
    expected: &mut BTreeSet<RowKey>,
) {
    let template = &templates[index % templates.len()];
    let mut bytes = template.bytes.clone();
    bytes.extend_from_slice(format!("\n<!-- soak document {index} -->\n").as_bytes());
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let staging = input.join(format!("doc-{index:07}.part"));
    std::fs::write(&staging, &bytes).unwrap();
    std::fs::rename(&staging, input.join(format!("doc-{index:07}.xml"))).unwrap();
    expected.extend(
        template
            .locators
            .iter()
            .map(|locator| (sha256.clone(), locator.clone())),
    );
}

/// Every published sink row; a torn or unparsable published file fails the soak.
fn sink_rows(output: &Path) -> Vec<RowKey> {
    let mut rows = Vec::new();
    for entry in std::fs::read_dir(output).unwrap() {
        let path: PathBuf = entry.unwrap().path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
            continue;
        }
        for line in std::fs::read_to_string(&path).unwrap().lines() {
            let row: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("{} holds a torn row: {error}", path.display()));
            rows.push((
                row["document_sha256"].as_str().unwrap().to_owned(),
                row["row_locator"].as_str().unwrap().to_owned(),
            ));
        }
    }
    rows
}

fn tail(log: &Path) -> String {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(60)..].join("\n")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "long-running soak; run explicitly"]
#[allow(clippy::too_many_lines)]
async fn fpml_files_survive_repeated_hard_kills_under_sustained_arrivals() {
    let run_for = Duration::from_secs(env_number("LAMINAR_FPML_SOAK_SECONDS", 120));
    let kill_every = Duration::from_secs(env_number("LAMINAR_FPML_SOAK_KILL_EVERY_SECONDS", 15));
    let rate = env_number("LAMINAR_FPML_SOAK_DOCS_PER_SECOND", 25).max(1);
    let templates = templates();
    assert!(templates.len() > 500, "{} templates", templates.len());

    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("in");
    let output = root.path().join("out");
    std::fs::create_dir_all(&input).unwrap();
    std::fs::create_dir_all(&output).unwrap();
    let config = root.path().join("server.toml");
    std::fs::write(&config, server_config(root.path(), &input, &output)).unwrap();
    let log = root.path().join("server.log");

    let started = Instant::now();
    let mut server = spawn_server(&config, &log);
    let mut expected = BTreeSet::new();
    let mut written = 0_usize;
    let mut kills = 0_u32;
    let mut peak_mib = 0_u64;
    let mut next_kill = started + kill_every;
    let mut next_sample = started;
    let tick = Duration::from_millis(1000 / rate);
    while Instant::now() < started + run_for {
        write_document(&input, &templates, written, &mut expected);
        written += 1;
        if let Some(status) = server.try_wait().unwrap() {
            panic!("server exited on its own with {status}:\n{}", tail(&log));
        }
        if Instant::now() >= next_sample {
            if let Some(mib) = resident_mib(server.id()) {
                peak_mib = peak_mib.max(mib);
            }
            next_sample = Instant::now() + Duration::from_secs(1);
        }
        if Instant::now() >= next_kill {
            server.kill().unwrap();
            server.wait().unwrap();
            kills += 1;
            server = spawn_server(&config, &log);
            next_kill = Instant::now() + kill_every;
        }
        let due = started + tick * u32::try_from(written).unwrap();
        tokio::time::sleep_until(due.into()).await;
    }

    let converge_started = Instant::now();
    let deadline = converge_started + Duration::from_secs(300);
    let mut rows = Vec::new();
    let mut observed = BTreeSet::new();
    while Instant::now() < deadline {
        rows = sink_rows(&output);
        observed = rows.iter().cloned().collect::<BTreeSet<_>>();
        if expected.is_subset(&observed) {
            break;
        }
        if let Some(status) = server.try_wait().unwrap() {
            panic!("server exited on its own with {status}:\n{}", tail(&log));
        }
        if let Some(mib) = resident_mib(server.id()) {
            peak_mib = peak_mib.max(mib);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let converged = converge_started.elapsed();
    server.kill().unwrap();
    server.wait().unwrap();

    let missing = expected.difference(&observed).count();
    let phantom = observed.difference(&expected).count();
    let mut copies = BTreeMap::<&RowKey, usize>::new();
    for row in &rows {
        *copies.entry(row).or_default() += 1;
    }
    let duplicated = copies.values().filter(|&&count| count > 1).count();
    println!(
        "SOAK ran {:.0}s: {written} documents ({} templates), {} expected rows, {} sink rows, \
         {kills} hard kills, missing={missing} phantom={phantom} duplicated_keys={duplicated} \
         extra_copies={}, converged {:.1}s after arrivals stopped, peak server memory {peak_mib} MiB",
        run_for.as_secs_f64(),
        templates.len(),
        expected.len(),
        rows.len(),
        rows.len() - observed.len(),
        converged.as_secs_f64()
    );
    assert!(kills > 0, "the run must include hard kills");
    assert_eq!(missing, 0, "rows lost:\n{}", tail(&log));
    assert_eq!(phantom, 0, "rows the documents do not contain");
}
