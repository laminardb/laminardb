//! Single-node Kafka ALO workload measurements. A passing run is scoped evidence, not S12 certification.

mod evidence;
mod latency;
mod load;
mod observer;
mod spec;
mod tests;

use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context as _, Result};
use serde_json::json;

use super::{kafka_create_topic, Node, ResolvedExecutable, SOAK_CONSOLE_TOKEN};
use evidence::Evidence;
use load::{Counts, Producer};
use observer::Observer;
use spec::{Fault, Spec};

#[test]
#[ignore = "requires real Kafka, a workload spec and an exclusive evidence directory"]
fn single_node_kafka_workload() -> Result<()> {
    let spec_path =
        std::env::var("LAMINAR_QUALIFICATION_SPEC").context("set LAMINAR_QUALIFICATION_SPEC")?;
    let spec: Spec = serde_json::from_slice(&std::fs::read(spec_path)?)?;
    spec.validate()?;
    let directory = std::env::var("LAMINAR_QUALIFICATION_OUTPUT")
        .context("set LAMINAR_QUALIFICATION_OUTPUT")?;
    let directory = Path::new(&directory);
    std::fs::create_dir(directory).context("evidence directory must not already exist")?;
    std::fs::write(
        directory.join("spec.json"),
        serde_json::to_vec_pretty(&spec)?,
    )?;
    let result = run(&spec, directory);
    if let Err(error) = &result {
        if let Err(report_error) = std::fs::write(
            directory.join("failure.json"),
            json!({
                "status":"failed", "error":format!("{error:#}"), "s12_qualified":false
            })
            .to_string(),
        ) {
            return result.context(format!(
                "writing failure evidence also failed: {report_error}"
            ));
        }
    }
    result
}

fn run(spec: &Spec, directory: &Path) -> Result<()> {
    let source_brokers = std::env::var("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS")?;
    let sink_brokers = std::env::var("LAMINAR_QUALIFICATION_SINK_BROKERS")
        .unwrap_or_else(|_| source_brokers.clone());
    let executable = Arc::new(ResolvedExecutable::from_environment().map_err(anyhow::Error::msg)?);
    executable.describe();
    let run_id = super::soak_run_id();
    let inputs: Vec<_> = (0..spec.pipelines)
        .map(|p| format!("qualification-{run_id}-in-{p}"))
        .collect();
    let outputs: Vec<_> = (0..spec.pipelines)
        .map(|p| format!("qualification-{run_id}-out-{p}"))
        .collect();
    for topic in &inputs {
        kafka_create_topic(&source_brokers, topic, spec.partitions);
    }
    for topic in &outputs {
        kafka_create_topic(&sink_brokers, topic, 1);
    }
    let mut node = make_node(
        spec,
        directory,
        executable,
        &source_brokers,
        &sink_brokers,
        &inputs,
        &outputs,
    )?;
    let verified = node.verify_executable_for_spawn();
    node.spawn(verified);
    let ready_deadline = Instant::now() + Duration::from_secs(60);
    while !node.is_ready() {
        node.assert_running();
        ensure!(
            Instant::now() < ready_deadline,
            "server readiness deadline exceeded"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let counts = Arc::new(Counts::default());
    let start = Instant::now() + Duration::from_secs(1);
    let mut observer = Observer::spawn(
        spec.clone(),
        &sink_brokers,
        outputs,
        start,
        Arc::clone(&counts),
        directory,
    )?;
    let mut producer = Producer::spawn(
        spec.clone(),
        source_brokers,
        inputs,
        start,
        Arc::clone(&counts),
        directory,
    )?;
    let mut evidence = Evidence::new(directory)?;
    let recovery_ms = monitor(
        spec,
        &mut node,
        &counts,
        start,
        &mut producer,
        &mut observer,
        &mut evidence,
    )?;
    let producer_lag = producer.finish()?;
    ensure!(
        counts.acknowledged.load(Ordering::Acquire) == spec.total_rows(),
        "source did not acknowledge every offered row"
    );
    let output = observer.finish()?;
    let summary = evidence.summary(spec);
    let verdict = check_limits(spec, &summary, &output, recovery_ms);
    let report = json!({
        "schema":"laminardb-workload-observation/v2", "s12_qualified":false,
        "status":if verdict.is_err() { "failed" } else if spec.limits.is_some() { "run_limits_passed" } else { "observed" },
        "mode":"single", "delivery":"at_least_once", "composition":"kafka_to_kafka",
        "clock":"one observer-process monotonic Instant; includes producer delay and external consumer polling",
        "latency_resolution":"1us minimum bucket; 1 percent bucket spacing; finite-range overflow is unresolved",
        "spec":spec, "resources":summary, "producer_schedule_lag":producer_lag,
        "output":output, "recovery_ms":recovery_ms,
        "limitations":["projection workload only; no tables, MVs or joins", "sampled process RSS; no total-host memory claim",
            "RSS growth checks each sampled process generation separately, excluding its startup warmup and final drain",
            "backlog is offered minus externally observed rows, not an internal queue-byte gauge",
            "single run only; S12 requires repeated workload/fault matrix and separate mode/composition qualification",
            "slow/failed sinks, corrupt cuts, expired replay and G9 saturation/restart remain separate cases"]
    });
    std::fs::write(
        directory.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    verdict
}

fn check_limits(
    spec: &Spec,
    summary: &evidence::Summary,
    output: &observer::Observation,
    recovery_ms: Option<f64>,
) -> Result<()> {
    ensure!(
        spec.fault != Fault::ProcessKill || recovery_ms.is_some(),
        "requested process kill has no verified post-death recovery"
    );
    summary.check(spec, recovery_ms)?;
    if let Some(limits) = &spec.limits {
        for (pipeline, latency) in output.latency.iter().enumerate() {
            latency
                .check(&limits.visibility_ms)
                .with_context(|| format!("pipeline {pipeline}"))?;
        }
    }
    Ok(())
}

fn monitor(
    spec: &Spec,
    node: &mut Node,
    counts: &Counts,
    start: Instant,
    producer: &mut Producer,
    observer: &mut Observer,
    evidence: &mut Evidence,
) -> Result<Option<f64>> {
    let deadline = start + Duration::from_secs(spec.seconds + spec.drain_seconds);
    let mut next_sample = start;
    let mut killed = false;
    let mut recovery = None;
    let mut recovery_ms = None;
    let mut drain_checkpoint = None;
    while Instant::now() < deadline {
        node.assert_running();
        if producer.is_finished() && counts.acknowledged.load(Ordering::Acquire) < spec.total_rows()
        {
            producer.finish()?;
            anyhow::bail!("producer stopped before acknowledging every offered row");
        }
        if observer.is_finished() {
            observer.finish()?;
            anyhow::bail!("observer stopped during load");
        }
        if Instant::now() < next_sample {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        }
        let elapsed = start.elapsed();
        next_sample = Instant::now() + Duration::from_secs(1);
        evidence.sample(spec, node, counts, elapsed)?;
        if spec.fault == Fault::ProcessKill && !killed && elapsed.as_secs() >= spec.seconds / 2 {
            let permit = node.verify_executable_for_spawn();
            let committed = node
                .durable_checkpoint_status()
                .context("cannot kill without a committed checkpoint")?;
            let killed_at = Instant::now();
            node.kill9();
            // Require an ID scheduled after confirmed process death, not an already-visible prefix.
            let confirmed_dead = start.elapsed();
            let target = spec.offered(confirmed_dead) / spec.pipelines as u64;
            evidence.event(
                confirmed_dead,
                json!({
                    "kind": "process_killed", "generation": node.process_generation,
                    "kill_started_ns": killed_at.duration_since(start).as_nanos(),
                    "target_id": target, "predecessor_checkpoint_id": committed.checkpoint_id
                }),
            )?;
            node.spawn(permit);
            recovery = Some((killed_at, target, committed.checkpoint_id));
            killed = true;
        }
        if let Some((killed_at, target, checkpoint)) = recovery {
            let completed = node.commits();
            let current = node.durable_checkpoint_status();
            if counts.frontiers[..spec.pipelines]
                .iter()
                .all(|f| f.load(Ordering::Acquire) > target)
                && node.is_ready()
                && completed.is_some_and(|count| count > 0.0)
                && current.is_some_and(|c| c.checkpoint_id > checkpoint)
            {
                let recovered_at = Instant::now();
                recovery_ms = Some(recovered_at.duration_since(killed_at).as_secs_f64() * 1_000.0);
                evidence.event(
                    recovered_at.duration_since(start),
                    json!({
                        "kind": "process_recovered", "generation": node.process_generation,
                        "target_id": target, "recovery_ms": recovery_ms,
                        "post_restart_completed_checkpoints": completed,
                        "checkpoint_id": current.map(|c| c.checkpoint_id)
                    }),
                )?;
                recovery = None;
            }
        }
        if producer.is_finished() && counts.observed.load(Ordering::Acquire) == spec.total_rows() {
            let committed = node.durable_checkpoint_status();
            match (drain_checkpoint, committed) {
                (Some(previous), Some(current)) if current.checkpoint_id > previous => {
                    return Ok(recovery_ms)
                }
                (None, Some(current)) => drain_checkpoint = Some(current.checkpoint_id),
                _ => {}
            }
        }
        if elapsed.as_secs().is_multiple_of(30) {
            eprintln!(
                "qualification: elapsed={}s offered={} acknowledged={} observed={}",
                elapsed.as_secs(),
                spec.offered(elapsed),
                counts.acknowledged.load(Ordering::Acquire),
                counts.observed.load(Ordering::Acquire)
            );
        }
    }
    anyhow::bail!(
        "load/drain deadline: acknowledged={} observed={} expected={}",
        counts.acknowledged.load(Ordering::Acquire),
        counts.observed.load(Ordering::Acquire),
        spec.total_rows()
    )
}

fn make_node(
    spec: &Spec,
    directory: &Path,
    executable: Arc<ResolvedExecutable>,
    source_brokers: &str,
    sink_brokers: &str,
    inputs: &[String],
    outputs: &[String],
) -> Result<Node> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let checkpoints = directory.join("checkpoints");
    std::fs::create_dir(&checkpoints)?;
    let checkpoint_path = checkpoints
        .canonicalize()?
        .to_string_lossy()
        .replace('\\', "/");
    let checkpoint_path = checkpoint_path
        .strip_prefix("//?/")
        .unwrap_or(&checkpoint_path);
    let checkpoint_url = format!(
        "file://{}{checkpoint_path}",
        if checkpoint_path.starts_with('/') {
            ""
        } else {
            "/"
        }
    );
    let mut config = format!(
        r#"node_id = "qualification"
[server]
mode = "single"
bind = "127.0.0.1:{port}"
delivery = "at_least_once"
console_token = "{SOAK_CONSOLE_TOKEN}"
datafusion_memory_limit_bytes = 268435456
source_queue_max_bytes = 67108864
pipeline_max_input_buf_bytes = 67108864
[checkpoint]
url = {checkpoint_url:?}
interval = "{}ms"
timeout = "30s"
"#,
        spec.checkpoint_ms
    );
    for (pipeline, (input, output)) in inputs.iter().zip(outputs).enumerate() {
        use std::fmt::Write as _;
        writeln!(
            config,
            r#"
[[source]]
name = "input_{pipeline}"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = {source_brokers:?}
topic = {input:?}
"group.id" = {input:?}
"startup.mode" = "earliest"
"max.poll.records" = "1000"
"reader.channel.capacity" = "8192"
[[source.schema]]
name = "origin"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "payload"
type = "VARCHAR"
nullable = false
[[pipeline]]
name = "projection_{pipeline}"
sql = "SELECT origin, id, key, id * 3 + 7 AS value, UPPER(payload) AS payload FROM input_{pipeline}"
[[sink]]
name = "output_{pipeline}"
pipeline = "projection_{pipeline}"
connector = "kafka"
format = "json"
[sink.properties]
"bootstrap.servers" = {sink_brokers:?}
topic = {output:?}
"linger.ms" = "5"
"batch.size" = "16384"
"#
        )?;
    }
    let path = directory.join("server.toml");
    let mut file = std::fs::File::create(&path)?;
    file.write_all(config.as_bytes())?;
    Ok(Node {
        id: 0,
        executable,
        config_path: path,
        log_path: directory.join("server.log"),
        child: None,
        process_generation: 0,
        http_port: port,
        fault_trigger_path: None,
        checkpoint_gate_path: None,
    })
}
