#![cfg(all(feature = "process-remote", feature = "files"))]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use laminar_db::process_function::ProcessFunctionDescriptor;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy)]
enum HostLossCut {
    PendingInvocation,
    PublishedOutput,
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn quoted(value: &str) -> String {
    serde_json::to_string(value).unwrap()
}

fn write_handler(root: &Path, repository: &Path) -> Result<(PathBuf, PathBuf)> {
    let base = std::fs::read_to_string(repository.join("examples/process_python/handler.py"))?;
    let exit = quoted(&path_string(&root.join("exit-worker")));
    let ack = quoted(&path_string(&root.join("worker-exiting")));
    let entered = quoted(&path_string(&root.join("second-entered")));
    let hold = quoted(&path_string(&root.join("hold-second")));
    let release = quoted(&path_string(&root.join("release-second")));
    let handler = format!(
        r#"{base}
import os as _os
import threading as _threading
import time as _time
from pathlib import Path as _Path
_exit = _Path({exit})
_ack = _Path({ack})
_entered = _Path({entered})
_hold = _Path({hold})
_release = _Path({release})
def _exit_now():
    _ack.write_text(str(_os.getpid()))
    _os._exit(47)
def _watch_exit():
    while not _exit.exists():
        _time.sleep(0.01)
    _exit_now()
_threading.Thread(target=_watch_exit, daemon=True).start()
_original_handle = handle
def handle(activations):
    if any(a.input is not None and a.input.column(1)[0].as_py() == 50 for a in activations):
        with _entered.open('a') as marker:
            marker.write(f"{{activations[0].id}}\n")
        if _hold.exists():
            while not _release.exists():
                _time.sleep(0.01)
            _exit_now()
    return _original_handle(activations)
"#
    );
    let handler_path = root.join("host_loss_handler.py");
    std::fs::write(&handler_path, &handler)?;
    let mut descriptor = ProcessFunctionDescriptor::from_manifest_json(&std::fs::read(
        repository.join("examples/process_python/manifest.json"),
    )?)?;
    descriptor.implementation_digest = format!("{:x}", Sha256::digest(handler.as_bytes()));
    let manifest_path = root.join("manifest.json");
    std::fs::write(&manifest_path, descriptor.to_manifest_json()?)?;
    Ok((handler_path, manifest_path))
}

fn write_config(root: &Path, port: u16, python: &str) -> Result<PathBuf> {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let input = root.join("input");
    let output = root.join("output");
    std::fs::create_dir(&input)?;
    std::fs::create_dir(&output)?;
    let (handler, manifest) = write_handler(root, &repository)?;
    let mut python_paths = vec![repository.join("python/laminardb_process")];
    if let Some(dependencies) = std::env::var_os("LAMINAR_PROCESS_PYTHON_DEPS") {
        python_paths.push(PathBuf::from(dependencies).canonicalize()?);
    }
    let python_paths = python_paths
        .iter()
        .map(|path| quoted(&path_string(path)))
        .collect::<Vec<_>>()
        .join(", ");
    let checkpoint_path = path_string(&root.join("checkpoints"));
    let checkpoint_url = if checkpoint_path.starts_with('/') {
        format!("file://{checkpoint_path}")
    } else {
        format!("file:///{checkpoint_path}")
    };
    let source_sql = format!(
        "CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, \
         ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) \
         FROM FILES ('path' = '{}', 'glob_pattern' = '*.json', \
         'stabilisation_delay' = '100ms') FORMAT JSON",
        path_string(&input).replace('\'', "''")
    );
    let config = format!(
        r#"[server]
bind = {bind}
delivery = "best_effort"
[checkpoint]
url = {checkpoint_url}
interval = "1h"
[[process_function]]
source = "events"
output = "activity"
source_sql = {source_sql}
manifest = {manifest}
handler_file = {handler}
function = "handle"
python = {python}
python_paths = [{python_paths}]
max_in_flight = 2
timeout = "15s"
[[sink]]
name = "activity_files"
pipeline = "activity"
connector = "files"
format = "json"
[sink.properties]
path = {output}
"#,
        bind = quoted(&format!("127.0.0.1:{port}")),
        checkpoint_url = quoted(&checkpoint_url),
        source_sql = quoted(&source_sql),
        manifest = quoted(&path_string(&manifest)),
        handler = quoted(&path_string(&handler)),
        python = quoted(python),
        output = quoted(&path_string(&output)),
    );
    let config_path = root.join("server.toml");
    std::fs::write(&config_path, config)?;
    Ok(config_path)
}

fn spawn_server(config: &Path) -> Result<Child> {
    Command::new(env!("CARGO_BIN_EXE_laminardb"))
        .arg("--config")
        .arg(config)
        .args(["--log-level", "error"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .context("start standalone laminardb")
}

fn wait_until(
    host: &mut Child,
    label: &str,
    mut observed: impl FnMut() -> Result<bool>,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = host.try_wait()? {
            anyhow::bail!("standalone server exited before {label}: {status}");
        }
        if observed()? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("standalone server did not reach {label}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn http_request(port: u16, method: &str, path: &str) -> Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes())?;
    let mut response = String::new();
    stream.take(64 * 1024).read_to_string(&mut response)?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .context("HTTP response had no header boundary")?;
    anyhow::ensure!(
        headers
            .lines()
            .next()
            .is_some_and(|line| line.contains(" 200 ")),
        "HTTP {method} {path} failed: {response}"
    );
    Ok(body.to_owned())
}

fn checkpoint(port: u16) -> Result<()> {
    let response: serde_json::Value =
        serde_json::from_str(&http_request(port, "POST", "/api/v1/checkpoint")?)?;
    anyhow::ensure!(response["success"] == true, "checkpoint failed: {response}");
    Ok(())
}

fn publish_file(root: &Path, name: &str, amount: i64, timestamp: i64) -> Result<()> {
    let mut row = serde_json::to_vec(&serde_json::json!({
        "key": "a", "amount": amount, "ts": timestamp
    }))?;
    row.push(b'\n');
    let staged = root.join("staged.json");
    std::fs::write(&staged, row)?;
    std::fs::rename(staged, root.join("input").join(name))?;
    Ok(())
}

fn sink_totals(directory: &Path) -> Result<Vec<i64>> {
    let mut totals = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path
            .extension()
            .is_none_or(|extension| extension != "jsonl")
        {
            continue;
        }
        for line in std::fs::read_to_string(path)?.lines() {
            let row: serde_json::Value = serde_json::from_str(line)?;
            totals.push(row["total"].as_i64().context("sink total was not Int64")?);
        }
    }
    totals.sort_unstable();
    Ok(totals)
}

fn stop_host_and_worker(host: &mut Child, root: &Path) -> Result<()> {
    let kill = host.kill();
    // SIGKILL/TerminateProcess bypasses the server's worker supervisor.
    let signal = std::fs::write(root.join("exit-worker"), []);
    let release = std::fs::write(root.join("release-second"), []);
    let status = host.wait();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !root.join("worker-exiting").exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    kill?;
    signal?;
    release?;
    anyhow::ensure!(!status?.success(), "server host was not terminated");
    anyhow::ensure!(
        root.join("worker-exiting").exists(),
        "Python worker did not acknowledge exit after host loss"
    );
    Ok(())
}

fn run_host_loss_cut(cut: HostLossCut) -> Result<()> {
    let Some(python) = std::env::var_os("LAMINAR_PROCESS_PYTHON") else {
        return Ok(());
    };
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let config = write_config(root, port, &python.to_string_lossy())?;
    let output = root.join("output");
    let mut first = spawn_server(&config)?;
    let initial: Result<()> = (|| {
        wait_until(&mut first, "readiness", || {
            Ok(http_request(port, "GET", "/ready").is_ok())
        })?;
        publish_file(root, "first.json", 60, 100_000)?;
        wait_until(&mut first, "first sink output", || {
            Ok(sink_totals(&output)? == [60])
        })?;
        checkpoint(port)?;
        if matches!(cut, HostLossCut::PendingInvocation) {
            std::fs::write(root.join("hold-second"), [])?;
        }
        publish_file(root, "second.json", 50, 100_050)?;
        match cut {
            HostLossCut::PendingInvocation => wait_until(&mut first, "pending invocation", || {
                Ok(root.join("second-entered").exists())
            }),
            HostLossCut::PublishedOutput => wait_until(&mut first, "second sink output", || {
                Ok(sink_totals(&output)? == [60, 110])
            }),
        }
    })();
    let first_stop = stop_host_and_worker(&mut first, root);
    initial?;
    first_stop?;
    let before_replay = match cut {
        HostLossCut::PendingInvocation => vec![60],
        HostLossCut::PublishedOutput => vec![60, 110],
    };
    anyhow::ensure!(sink_totals(&output)? == before_replay);
    anyhow::ensure!(
        std::fs::read_to_string(root.join("second-entered"))?
            .lines()
            .collect::<Vec<_>>()
            == ["1"]
    );
    for marker in ["exit-worker", "release-second", "worker-exiting"] {
        std::fs::remove_file(root.join(marker))?;
    }
    if matches!(cut, HostLossCut::PendingInvocation) {
        std::fs::remove_file(root.join("hold-second"))?;
    }

    let mut replacement = spawn_server(&config)?;
    let after_replay = match cut {
        HostLossCut::PendingInvocation => vec![60, 110],
        HostLossCut::PublishedOutput => vec![60, 110, 110],
    };
    let recovered: Result<()> = (|| {
        wait_until(&mut replacement, "replacement readiness", || {
            Ok(http_request(port, "GET", "/ready").is_ok())
        })?;
        wait_until(&mut replacement, "replayed sink output", || {
            Ok(sink_totals(&output)? == after_replay)
        })?;
        checkpoint(port)
    })();
    let replacement_stop = stop_host_and_worker(&mut replacement, root);
    recovered?;
    replacement_stop?;
    anyhow::ensure!(sink_totals(&output)? == after_replay);
    anyhow::ensure!(
        std::fs::read_to_string(root.join("second-entered"))?
            .lines()
            .collect::<Vec<_>>()
            == ["1", "1"]
    );
    Ok(())
}

#[test]
fn standalone_server_replays_pending_python_file_after_host_loss() -> Result<()> {
    run_host_loss_cut(HostLossCut::PendingInvocation)
}

#[test]
fn standalone_server_republishes_uncheckpointed_python_file_after_host_loss() -> Result<()> {
    run_host_loss_cut(HostLossCut::PublishedOutput)
}
