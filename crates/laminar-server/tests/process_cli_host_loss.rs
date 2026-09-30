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
    let pid = quoted(&path_string(&root.join("worker-pid")));
    let delay = quoted(&path_string(&root.join("invocation-delay")));
    let memory = quoted(&path_string(&root.join("worker-arrow-samples.csv")));
    let probe = quoted(&path_string(&root.join("sample-worker-memory")));
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
_Path({pid}).write_text(str(_os.getpid()))
_delay = _Path({delay})
def _exit_now():
    _ack.write_text(str(_os.getpid()))
    _os._exit(47)
def _watch_exit():
    samples = None
    if _Path({probe}).exists():
        samples = _Path({memory}).open('w')
        samples.write('elapsed_seconds,backend,allocated_bytes,peak_allocated_bytes\n')
    started = _time.monotonic()
    next_sample = started
    while not _exit.exists():
        now = _time.monotonic()
        if samples is not None and now >= next_sample:
            pool = pa.default_memory_pool()
            samples.write(f"{{now - started:.3f}},{{pool.backend_name}},{{pool.bytes_allocated()}},{{pool.max_memory()}}\n")
            samples.flush()
            next_sample = now + 1
        _time.sleep(0.01)
    if samples is not None:
        samples.close()
    _exit_now()
_threading.Thread(target=_watch_exit, daemon=True).start()
_original_handle = handle
def handle(activations):
    if _delay.exists():
        _time.sleep(float(_delay.read_text()))
    if any(a.input is not None and a.input.column(1)[0].as_py() == 50 for a in activations):
        with _entered.open('a') as marker:
            marker.write(f"{{activations[0].id}}\n")
        if _hold.exists():
            while not _release.exists():
                _time.sleep(0.01)
            if _exit.exists():
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
    let mut stream =
        TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), Duration::from_secs(3))
            .with_context(|| format!("connect HTTP {method} {path} on port {port}"))?;
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

const SATURATION_FIFO_BYTES: usize = 128 * 1024;
const SATURATION_GRAPH_BYTES: usize = 2 * 1024 * 1024;
const SATURATION_KEYS: usize = 64;

fn prepare_saturation(root: &Path, port: u16, python: &str, records: usize) -> Result<PathBuf> {
    let config_path = write_config(root, port, python)?;
    let mut config: toml::Value = toml::from_str(&std::fs::read_to_string(&config_path)?)?;
    let server = config
        .get_mut("server")
        .and_then(toml::Value::as_table_mut)
        .context("saturation config did not contain a server table")?;
    server.insert(
        "source_queue_max_bytes".into(),
        i64::try_from(SATURATION_FIFO_BYTES)?.into(),
    );
    server.insert(
        "pipeline_max_input_buf_bytes".into(),
        i64::try_from(SATURATION_GRAPH_BYTES)?.into(),
    );
    server.insert("pipeline_max_input_buf_batches".into(), 256.into());
    config["process_function"][0]["max_in_flight"] = 1.into();
    std::fs::write(&config_path, toml::to_string(&config)?)?;
    let manifest = root.join("manifest.json");
    let mut descriptor = ProcessFunctionDescriptor::from_manifest_json(&std::fs::read(&manifest)?)?;
    descriptor.limits.max_batch_rows = 1;
    descriptor.limits.max_input_rows = 256;
    descriptor.limits.max_input_bytes = SATURATION_GRAPH_BYTES;
    descriptor.limits.max_keys = SATURATION_KEYS;
    descriptor.limits.max_timers = SATURATION_KEYS;
    descriptor.limits.max_state_bytes = 8 * 1024 * 1024;
    std::fs::write(manifest, descriptor.to_manifest_json()?)?;
    std::fs::write(root.join("invocation-delay"), "0.05")?;
    let suffix = "x".repeat(4 * 1024);
    for index in 0..records {
        let row = serde_json::json!({
            "key": format!("key_{:02}_{suffix}", index % SATURATION_KEYS),
            "amount": 50,
            "ts": 100_000,
        });
        std::fs::write(
            root.join("input").join(format!("{index:05}.json")),
            serde_json::to_vec(&row)?,
        )?;
    }
    Ok(config_path)
}

fn maximum_metric(metrics: &str, name: &str) -> Result<usize> {
    let prefix = format!("laminardb_{name}");
    let mut maximum: Option<usize> = None;
    for line in metrics.lines() {
        let Some(suffix) = line.strip_prefix(&prefix) else {
            continue;
        };
        if !suffix.starts_with(['{', ' ']) {
            continue;
        }
        let value = line
            .rsplit_once(' ')
            .context("metric did not contain a value")?
            .1
            .parse::<usize>()
            .context("metric value was not a nonnegative integer")?;
        maximum = Some(maximum.unwrap_or(0).max(value));
    }
    maximum.with_context(|| format!("missing {name} metric"))
}

fn sample_saturation(host: &mut Child, root: &Path, port: u16, records: usize) -> Result<()> {
    std::fs::write(root.join("host-pid"), host.id().to_string())?;
    wait_until(host, "readiness", || {
        Ok(http_request(port, "GET", "/ready").is_ok())
    })
    .with_context(|| {
        format!(
            "server status during startup: {}",
            http_request(port, "GET", "/api/v1/pipeline/status")
                .unwrap_or_else(|error| format!("{error:#}"))
        )
    })?;
    wait_until(host, "buffer metric publication", || {
        Ok(maximum_metric(&http_request(port, "GET", "/metrics")?, "input_buf_bytes").is_ok())
    })?;
    std::fs::write(root.join("phase"), "load")?;
    let mut samples = std::fs::File::create(root.join("queue-samples.csv"))?;
    writeln!(
        samples,
        "elapsed_seconds,source_reserved_bytes,max_graph_input_bytes,emitted_rows"
    )?;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(360);
    let mut saturated = 0usize;
    let mut peak_graph = 0usize;
    loop {
        if let Some(status) = host.try_wait()? {
            anyhow::bail!("server exited during saturation: {status}");
        }
        let metrics = http_request(port, "GET", "/metrics")?;
        let reserved = maximum_metric(&metrics, "source_queue_reserved_bytes")?;
        let graph = maximum_metric(&metrics, "input_buf_bytes")?;
        let emitted = maximum_metric(&metrics, "events_emitted_total")?;
        anyhow::ensure!(
            reserved <= SATURATION_FIFO_BYTES,
            "FIFO charge exceeded limit: {reserved}"
        );
        anyhow::ensure!(
            graph <= SATURATION_GRAPH_BYTES,
            "graph charge exceeded limit: {graph}"
        );
        anyhow::ensure!(emitted <= records, "unexpected output row count: {emitted}");
        saturated += usize::from(reserved == SATURATION_FIFO_BYTES);
        peak_graph = peak_graph.max(graph);
        writeln!(
            samples,
            "{:.3},{reserved},{graph},{emitted}",
            started.elapsed().as_secs_f64()
        )?;
        if emitted == records {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "saturation workload did not drain"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
    anyhow::ensure!(saturated > 0, "the FIFO never reached its byte limit");
    println!(
        "saturation: {} seconds, {saturated} full-FIFO samples, peak graph {peak_graph} bytes",
        started.elapsed().as_secs_f64()
    );
    let mut expected = (1..=records / SATURATION_KEYS)
        .flat_map(|round| std::iter::repeat_n(50 * round as i64, SATURATION_KEYS))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    wait_until(host, "durable sink output", || {
        Ok(sink_totals(&root.join("output"))? == expected)
    })?;
    Ok(())
}

fn sample_drained_server(
    host: &mut Child,
    root: &Path,
    port: u16,
    records: usize,
    idle_for: Duration,
) -> Result<()> {
    // Capture the completed-file inventory once, then measure without new files or checkpoints.
    std::fs::write(root.join("phase"), "checkpoint")?;
    checkpoint(port)?;
    let published = sink_totals(&root.join("output"))?;
    anyhow::ensure!(published.len() == records);
    let mut samples = std::fs::File::create(root.join("idle-queue-samples.csv"))?;
    writeln!(
        samples,
        "elapsed_seconds,source_reserved_bytes,max_graph_input_bytes,emitted_rows"
    )?;
    std::fs::write(root.join("phase"), "idle")?;
    let started = Instant::now();
    let deadline = started + idle_for;
    loop {
        if let Some(status) = host.try_wait()? {
            anyhow::bail!("server exited after drain: {status}");
        }
        let metrics = http_request(port, "GET", "/metrics")?;
        let reserved = maximum_metric(&metrics, "source_queue_reserved_bytes")?;
        let graph = maximum_metric(&metrics, "input_buf_bytes")?;
        let emitted = maximum_metric(&metrics, "events_emitted_total")?;
        anyhow::ensure!(reserved == 0, "FIFO was not empty after drain: {reserved}");
        anyhow::ensure!(graph == 0, "graph was not empty after drain: {graph}");
        anyhow::ensure!(emitted == records, "output changed after drain: {emitted}");
        writeln!(
            samples,
            "{:.3},{reserved},{graph},{emitted}",
            started.elapsed().as_secs_f64()
        )?;
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    anyhow::ensure!(sink_totals(&root.join("output"))? == published);
    std::fs::write(root.join("phase"), "finished")?;
    println!(
        "post-drain idle: {} seconds",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn run_saturation(records: usize, idle_for: Duration) -> Result<()> {
    let Some(python) = std::env::var_os("LAMINAR_PROCESS_PYTHON") else {
        return Ok(());
    };
    let directory = tempfile::tempdir()?;
    let resource_directory = std::env::var_os("LAMINAR_PROCESS_RESOURCE_DIR").map(PathBuf::from);
    let root = resource_directory.as_deref().unwrap_or(directory.path());
    std::fs::create_dir_all(root)?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let config = prepare_saturation(root, port, &python.to_string_lossy(), records)?;
    std::fs::write(root.join("sample-worker-memory"), [])?;
    let mut host = spawn_server(&config)?;
    let result = sample_saturation(&mut host, root, port, records)
        .and_then(|()| sample_drained_server(&mut host, root, port, records, idle_for));
    let stopped = stop_host_and_worker(&mut host, root);
    match (result, stopped) {
        (Err(primary), Err(cleanup)) => {
            Err(primary.context(format!("saturation cleanup: {cleanup:#}")))
        }
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), stopped) => stopped,
    }
}

#[test]
fn standalone_server_samples_bounded_python_saturation() -> Result<()> {
    run_saturation(128, Duration::from_secs(1))
}

#[test]
#[ignore = "manual server and Python worker load/idle memory sampling"]
fn standalone_server_python_saturation_resource_stress() -> Result<()> {
    anyhow::ensure!(
        std::env::var_os("LAMINAR_PROCESS_PYTHON").is_some(),
        "Python interpreter is required for resource sampling"
    );
    run_saturation(4096, Duration::from_secs(120))
}
