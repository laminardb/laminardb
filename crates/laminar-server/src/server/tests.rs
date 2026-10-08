use super::*;

#[cfg(all(feature = "process-remote", feature = "files"))]
use std::time::Duration;

#[tokio::test]
async fn graph_input_limit_is_validated_before_server_mode_routing() {
    for mode in [ServerMode::Single, ServerMode::Cluster] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.mode = mode;
        config.server.pipeline_max_input_buf_bytes = Some(0);
        let error = run_server(config, PathBuf::from("unused.toml"))
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("pipeline_max_input_buf_bytes"),
            "{error}"
        );
    }
}

use crate::config::*;

#[tokio::test]
async fn source_queue_limit_is_validated_before_server_mode_routing() {
    for mode in [ServerMode::Single, ServerMode::Cluster] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.mode = mode;
        config.server.source_queue_max_bytes = 0;
        let error = run_server(config, PathBuf::from("unused.toml"))
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("source_queue_max_bytes"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn datafusion_memory_limit_is_validated_before_server_mode_routing() {
    for mode in [ServerMode::Single, ServerMode::Cluster] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.mode = mode;
        config.server.datafusion_memory_limit_bytes = 0;
        let error = run_server(config, PathBuf::from("unused.toml"))
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("datafusion_memory_limit_bytes"),
            "{error}"
        );
    }
}

#[test]
fn checkpoint_config_rejects_relative_file_urls() {
    for url in ["file://./relative", "FILE://./relative"] {
        let result =
            apply_local_checkpoint_config(LaminarDB::builder(), url, &CheckpointSection::default());
        let Err(error) = result else {
            panic!("relative checkpoint URL was admitted: {url}");
        };
        assert!(error.to_string().contains("absolute local path"), "{error}");
    }
}

#[test]
fn checkpoint_state_budget_has_one_default_and_honours_an_override() {
    let mut checkpoint = CheckpointSection::default();
    assert_eq!(
        resolved_checkpoint_node_data_bytes(&checkpoint).unwrap(),
        laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES
    );

    checkpoint.max_node_data_bytes = Some(8 * 1024 * 1024);
    assert_eq!(
        resolved_checkpoint_node_data_bytes(&checkpoint).unwrap(),
        8 * 1024 * 1024
    );
}

#[test]
fn checkpoint_state_budget_rejects_zero_and_unaddressable_limits() {
    let mut checkpoint = CheckpointSection {
        max_node_data_bytes: Some(0),
        ..CheckpointSection::default()
    };
    assert!(resolved_checkpoint_node_data_bytes(&checkpoint).is_err());

    checkpoint.max_node_data_bytes = Some((isize::MAX as u64) + 1);
    let error = resolved_checkpoint_node_data_bytes(&checkpoint).unwrap_err();
    assert!(error
        .to_string()
        .contains("exceeds this process address space"));
}

#[tokio::test]
async fn server_entry_rejects_invalid_budget_before_runtime_mode_routing() {
    for mode in [ServerMode::Single, ServerMode::Cluster] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.mode = mode;
        config.checkpoint.max_node_data_bytes = Some(0);

        let result = run_server(config, PathBuf::from("unused.toml")).await;
        let Err(error) = result else {
            panic!("invalid checkpoint state budget was admitted in {mode:?} mode");
        };
        assert!(
            error.to_string().contains("checkpoint.max_node_data_bytes"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn server_entry_rejects_invalid_temporal_retention_in_both_modes() {
    for mode in [ServerMode::Single, ServerMode::Cluster] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.mode = mode;
        config.server.temporal_join_idle_history_retention =
            Some(std::time::Duration::from_nanos(999_999));

        let result = run_server(config, PathBuf::from("unused.toml")).await;
        let Err(error) = result else {
            panic!("invalid temporal retention was admitted in {mode:?} mode");
        };
        assert!(
            error
                .to_string()
                .contains("temporal_join_idle_history_retention must be at least 1ms"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn server_entry_rejects_anonymous_remote_http_before_other_startup_work() {
    for mode in [ServerMode::Single, ServerMode::Cluster] {
        for bind in ["0.0.0.0:8080", "[::]:8080", "[::ffff:127.0.0.1]:8080"] {
            let mut config: ServerConfig = toml::from_str("").unwrap();
            config.server.mode = mode;
            config.server.bind = bind.into();
            // A later validation failure keeps this regression safe even if the auth
            // guard is removed, without reaching bootstrap or creating a listener.
            config.checkpoint.max_node_data_bytes = Some(0);

            let result = run_server(config, PathBuf::from("unused.toml")).await;
            let Err(error) = result else {
                panic!("remote anonymous HTTP was admitted: {mode:?} {bind}");
            };
            let message = error.to_string();
            assert!(message.contains("HTTP authentication"), "{message}");
            assert!(
                message.contains("non-loopback server.bind requires server.console_token"),
                "{message}"
            );
            assert!(
                !message.contains("checkpoint.max_node_data_bytes"),
                "{message}"
            );
        }
    }
}

#[tokio::test]
async fn server_entry_revalidates_cli_admin_bind_after_file_loading() {
    use clap::Parser as _;

    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("loopback.toml");
    std::fs::write(&config_path, "[server]\nbind = \"127.0.0.1:8080\"\n").unwrap();
    let mut config = load_config(&config_path).expect("anonymous loopback config must load");
    let args = crate::Args::try_parse_from(["laminardb", "--admin-bind", "0.0.0.0:8080"]).unwrap();
    config.server.bind = args.admin_bind.unwrap();
    config.checkpoint.max_node_data_bytes = Some(0);

    let result = run_server(config, config_path).await;
    let Err(error) = result else {
        panic!("CLI bind override bypassed authentication validation");
    };
    let message = error.to_string();
    assert!(message.contains("HTTP authentication"), "{message}");
    assert!(
        message.contains("non-loopback server.bind requires server.console_token"),
        "{message}"
    );
}

#[tokio::test]
async fn server_entry_rejects_programmatic_diagnostic_auth_before_other_startup_work() {
    let mut config: ServerConfig = toml::from_str("").unwrap();
    config.server.diagnostic_read_token = Some(Secret::new("invalid"));
    // This second invalid value makes the test terminate safely even if authentication
    // validation is accidentally moved later; authentication must still win.
    config.checkpoint.max_node_data_bytes = Some(0);

    let result = run_server(config, PathBuf::from("unused.toml")).await;
    let Err(error) = result else {
        panic!("invalid programmatic diagnostic authentication was admitted");
    };
    let message = error.to_string();
    assert!(message.contains("HTTP authentication"), "{message}");
    assert!(message.contains("diagnostic_read_token"), "{message}");
    assert!(
        !message.contains("checkpoint.max_node_data_bytes"),
        "{message}"
    );
}

#[tokio::test]
async fn cancelling_http_start_does_not_detach_the_listener() {
    let server = ServerSection {
        bind: "127.0.0.1:0".into(),
        ..ServerSection::default()
    };
    let config = ServerConfig {
        server,
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        process_functions: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), "test".into()),
        ("pipeline".into(), "test".into()),
    ]));
    let prepared = prepare_http_api(
        LaminarDB::open().unwrap(),
        registry,
        PathBuf::from("unused.toml"),
        config,
        Arc::new(http::ServingGate::starting()),
        #[cfg(feature = "cluster")]
        None,
    )
    .await
    .unwrap();
    let address = prepared.listener.local_addr().unwrap();

    {
        let start = prepared.start();
        tokio::pin!(start);
        assert!(futures::poll!(start.as_mut()).is_pending());
    }

    let rebound = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if let Ok(listener) = tokio::net::TcpListener::bind(address).await {
                return listener;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling HTTP startup must release its listener");
    drop(rebound);
}

#[tokio::test]
async fn aborted_server_task_is_joined_before_cleanup_returns() {
    let mut task = tokio::spawn(std::future::pending::<()>());
    let observer = task.abort_handle();

    assert!(abort_and_join_server_task(&mut task, "test task").await);

    assert!(observer.is_finished());
}

#[tokio::test]
async fn dropping_single_server_handle_fences_and_aborts_owned_tasks() {
    let serving_gate = Arc::new(http::ServingGate::starting());
    assert!(serving_gate.open());
    let api_handle = tokio::spawn(std::future::pending::<()>());
    let api_abort = api_handle.abort_handle();
    let pgwire_handle = tokio::spawn(std::future::pending::<()>());
    let pgwire_abort = pgwire_handle.abort_handle();
    let watcher_handle = tokio::spawn(std::future::pending::<()>());
    let watcher_abort = watcher_handle.abort_handle();
    let db = LaminarDB::open().unwrap();
    let handle = ServerHandle {
        runtime: ServerRuntime::Single(SingleServerRuntime {
            db: Arc::clone(&db),
            #[cfg(feature = "process-remote")]
            process_workers: vec![],
            db_shutdown_complete: false,
            serving_gate: Arc::clone(&serving_gate),
            api_handle,
            pgwire_handle: Some(pgwire_handle),
            watcher_handle: Some(watcher_handle),
        }),
    };

    drop(handle);

    assert_eq!(
        serving_gate.rejection_message(),
        Some("server serving authority is fenced")
    );
    assert!(db.is_closed());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !(api_abort.is_finished()
            && pgwire_abort.is_finished()
            && watcher_abort.is_finished())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped server handle left an owned task running");
    db.shutdown().await.unwrap();
}

#[cfg(feature = "process-remote")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn python_worker_exit_stops_the_server_when_idle_or_in_flight() {
    use sha2::{Digest, Sha256};

    let Some(python) = std::env::var_os("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = repository.join("examples/process_python/server.toml");
    for in_flight in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let exit_marker = directory.path().join("exit-worker");
        let started_marker = directory.path().join("call-started");
        let exit_literal = serde_json::to_string(&exit_marker.to_string_lossy()).unwrap();
        let started_literal = serde_json::to_string(&started_marker.to_string_lossy()).unwrap();
        let invocation = if in_flight {
            format!(
                "def handle(activations):\n    open({started_literal}, 'wb').close()\n    exit_on_signal()\n"
            )
        } else {
            "threading.Thread(target=exit_on_signal, daemon=True).start()\ndef handle(activations):\n    return ()\n"
                .to_string()
        };
        let handler = format!(
            r#"import os
import threading
import time

def exit_on_signal():
    while not os.path.exists({exit_literal}):
        time.sleep(0.01)
    os._exit(47)

{invocation}"#
        );
        let handler_path = directory.path().join("crash_handler.py");
        std::fs::write(&handler_path, &handler).unwrap();
        let mut descriptor =
            laminar_db::process_function::ProcessFunctionDescriptor::from_manifest_json(
                &std::fs::read(repository.join("examples/process_python/manifest.json")).unwrap(),
            )
            .unwrap();
        descriptor.implementation_digest = format!("{:x}", Sha256::digest(handler.as_bytes()));
        let manifest_path = directory.path().join("manifest.json");
        std::fs::write(&manifest_path, descriptor.to_manifest_json().unwrap()).unwrap();

        let mut config = crate::config::load_config(&config_path).unwrap();
        config.server.bind = "127.0.0.1:0".into();
        config.process_functions[0].python = python.clone().into();
        config.process_functions[0].handler_file = handler_path;
        config.process_functions[0].manifest = manifest_path;
        if let Some(dependencies) = std::env::var_os("LAMINAR_PROCESS_PYTHON_DEPS") {
            config.process_functions[0]
                .python_paths
                .push(PathBuf::from(dependencies));
        }
        let checkpoint_path = directory.path().join("checkpoints");
        let checkpoint_path = checkpoint_path.to_string_lossy().replace('\\', "/");
        config.checkpoint.url = if checkpoint_path.starts_with('/') {
            format!("file://{checkpoint_path}")
        } else {
            format!("file:///{checkpoint_path}")
        };

        let handle = run_server(config, config_path.clone()).await.unwrap();
        let runtime = match &handle.runtime {
            ServerRuntime::Single(runtime) => runtime,
            #[cfg(feature = "cluster")]
            ServerRuntime::Cluster(_) => {
                panic!("process function test requires single-node server")
            }
        };
        assert!(runtime.process_workers[0].is_alive());
        let gate = Arc::clone(&runtime.serving_gate);
        let db = Arc::clone(&runtime.db);
        if in_flight {
            db.execute("INSERT INTO events VALUES ('a', 7, 100000)")
                .await
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !started_marker.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("Python handler did not start its in-flight call");
        }
        let shutdown = tokio::spawn(handle.wait_for_shutdown());
        std::fs::write(&exit_marker, []).unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(10), shutdown)
            .await
            .expect("server did not stop after Python worker exit")
            .unwrap()
            .unwrap_err();
        assert!(
            error.to_string().contains("process worker 0 exited"),
            "{error}"
        );
        assert!(db.is_closed());
        assert_eq!(
            gate.rejection_message(),
            Some("server serving authority is fenced")
        );
    }
}

#[cfg(all(feature = "process-remote", feature = "files"))]
fn process_sink_totals(directory: &std::path::Path) -> Vec<i64> {
    let mut totals = Vec::new();
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path
            .extension()
            .is_none_or(|extension| extension != "jsonl")
        {
            continue;
        }
        for line in std::fs::read_to_string(path).unwrap().lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            totals.push(value["total"].as_i64().unwrap());
        }
    }
    totals.sort_unstable();
    totals
}

#[cfg(all(feature = "process-remote", feature = "files"))]
async fn wait_for_process_sink(directory: &std::path::Path, expected: &[i64]) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while process_sink_totals(directory).as_slice() != expected {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "configured process sink did not publish {expected:?}; observed {:?}",
            process_sink_totals(directory)
        )
    });
}

#[cfg(all(feature = "process-remote", feature = "files"))]
fn publish_process_file(
    root: &std::path::Path,
    name: &str,
    amount: i64,
    timestamp: i64,
) -> std::io::Result<()> {
    let mut row = serde_json::to_vec(&serde_json::json!({
        "key": "a", "amount": amount, "ts": timestamp
    }))
    .unwrap();
    row.push(b'\n');
    let staged = root.join("staged.json");
    std::fs::write(&staged, row)?;
    std::fs::rename(staged, root.join("input").join(name))
}

#[cfg(all(feature = "process-remote", feature = "files"))]
fn process_failure_handler(
    root: &std::path::Path,
    repository: &std::path::Path,
) -> (PathBuf, PathBuf) {
    use sha2::{Digest, Sha256};

    let fail_literal = serde_json::to_string(&root.join("fail-second").to_string_lossy()).unwrap();
    let exit_literal = serde_json::to_string(&root.join("exit-worker").to_string_lossy()).unwrap();
    let exit_ack_literal =
        serde_json::to_string(&root.join("worker-exiting").to_string_lossy()).unwrap();
    let entered_literal =
        serde_json::to_string(&root.join("second-entered").to_string_lossy()).unwrap();
    let hold_literal =
        serde_json::to_string(&root.join("hold-invocation").to_string_lossy()).unwrap();
    let release_literal =
        serde_json::to_string(&root.join("release-invocation").to_string_lossy()).unwrap();
    let base =
        std::fs::read_to_string(repository.join("examples/process_python/handler.py")).unwrap();
    let handler = format!(
        r#"{base}
import os as _os
import threading as _threading
import time as _time
from pathlib import Path as _Path
_fail = _Path({fail_literal})
_exit = _Path({exit_literal})
_exit_ack = _Path({exit_ack_literal})
_entered = _Path({entered_literal})
_hold = _Path({hold_literal})
_release = _Path({release_literal})
def _exit_now():
    _exit_ack.write_text(str(_os.getpid()))
    _os._exit(47)
def _watch_exit():
    while not _exit.exists():
        _time.sleep(0.01)
    _exit_now()
_threading.Thread(target=_watch_exit, daemon=True).start()
_original_handle = handle
def handle(activations):
    if any(
        a.input is not None and a.input.column(1)[0].as_py() == 50
        for a in activations
    ):
        with _entered.open('a') as marker:
            marker.write(f"{{activations[0].id}}\n")
        if _hold.exists():
            while not _release.exists():
                _time.sleep(0.01)
            if _exit.exists():
                _exit_now()
        if _fail.exists():
            _exit_now()
    return _original_handle(activations)
"#
    );
    let handler_path = root.join("crash_handler.py");
    std::fs::write(&handler_path, &handler).unwrap();
    let mut descriptor =
        laminar_db::process_function::ProcessFunctionDescriptor::from_manifest_json(
            &std::fs::read(repository.join("examples/process_python/manifest.json")).unwrap(),
        )
        .unwrap();
    descriptor.implementation_digest = format!("{:x}", Sha256::digest(handler.as_bytes()));
    let manifest_path = root.join("manifest.json");
    std::fs::write(&manifest_path, descriptor.to_manifest_json().unwrap()).unwrap();
    (handler_path, manifest_path)
}

#[cfg(all(feature = "process-remote", feature = "files"))]
fn file_process_server_config(
    root: &std::path::Path,
    repository: &std::path::Path,
    python: PathBuf,
) -> ServerConfig {
    let input_dir = root.join("input");
    let output_dir = root.join("output");
    std::fs::create_dir_all(&input_dir).unwrap();
    std::fs::create_dir_all(&output_dir).unwrap();
    let (handler_path, manifest_path) = process_failure_handler(root, repository);
    let mut config =
        crate::config::load_config(&repository.join("examples/process_python/server.toml"))
            .unwrap();
    config.server.bind = "127.0.0.1:0".into();
    config.process_functions[0].python = python;
    config.process_functions[0].handler_file = handler_path;
    config.process_functions[0].manifest = manifest_path;
    if let Some(dependencies) = std::env::var_os("LAMINAR_PROCESS_PYTHON_DEPS") {
        let path = PathBuf::from(dependencies);
        config.process_functions[0]
            .python_paths
            .push(if path.is_absolute() {
                path
            } else {
                repository.join(path)
            });
    }
    let input_path = input_dir.display().to_string().replace('\\', "/");
    config.process_functions[0].source_sql = format!(
        "CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, \
         ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) \
         FROM FILES ('path' = '{input_path}', 'glob_pattern' = '*.json', \
         'stabilisation_delay' = '100ms') FORMAT JSON"
    );
    let mut properties = toml::Table::new();
    properties.insert(
        "path".into(),
        toml::Value::String(output_dir.display().to_string().replace('\\', "/")),
    );
    config.sinks.push(SinkConfig {
        name: "activity_files".into(),
        pipeline: "activity".into(),
        connector: "files".into(),
        format: Some("json".into()),
        properties,
    });
    let checkpoint_path = root
        .join("checkpoints")
        .to_string_lossy()
        .replace('\\', "/");
    config.checkpoint.url = if checkpoint_path.starts_with('/') {
        format!("file://{checkpoint_path}")
    } else {
        format!("file:///{checkpoint_path}")
    };
    config.checkpoint.interval = std::time::Duration::from_secs(3_600);
    config
}

#[cfg(all(feature = "process-remote", feature = "files"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_file_process_recovers_after_in_flight_worker_exit() {
    let Some(python) = std::env::var_os("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = repository.join("examples/process_python/server.toml");
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let output_dir = root.join("output");
    let fail = root.join("fail-second");
    let exit = root.join("exit-worker");
    let entered = root.join("second-entered");
    let config = file_process_server_config(root, &repository, python.into());

    let first = run_server(config.clone(), config_path.clone())
        .await
        .unwrap();
    let db = match &first.runtime {
        ServerRuntime::Single(runtime) => Arc::clone(&runtime.db),
        #[cfg(feature = "cluster")]
        ServerRuntime::Cluster(_) => panic!("process test requires single-node server"),
    };
    publish_process_file(root, "first.json", 60, 100_000).unwrap();
    wait_for_process_sink(&output_dir, &[60]).await;
    assert!(db.checkpoint().await.unwrap().success);
    let stopped = tokio::spawn(first.wait_for_shutdown());
    std::fs::write(&fail, []).unwrap();
    publish_process_file(root, "second.json", 50, 100_050).unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), stopped)
        .await
        .expect("server did not stop after in-flight worker loss")
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("process worker 0 exited"));
    assert!(entered.exists());
    assert!(db.is_closed());
    assert_eq!(process_sink_totals(&output_dir), vec![60]);
    std::fs::remove_file(&fail).unwrap();

    let second = run_server(config, config_path).await.unwrap();
    let restored = match &second.runtime {
        ServerRuntime::Single(runtime) => Arc::clone(&runtime.db),
        #[cfg(feature = "cluster")]
        ServerRuntime::Cluster(_) => panic!("process test requires single-node server"),
    };
    wait_for_process_sink(&output_dir, &[60, 110]).await;
    assert!(restored.checkpoint().await.unwrap().success);
    let stopped = tokio::spawn(second.wait_for_shutdown());
    std::fs::write(&exit, []).unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), stopped)
        .await
        .expect("server did not stop after idle worker exit")
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("process worker 0 exited"));
    assert!(restored.is_closed());
    assert_eq!(process_sink_totals(&output_dir), vec![60, 110]);
}

#[cfg(all(feature = "process-remote", feature = "files"))]
#[derive(Clone, Copy)]
enum ServerHostLossCut {
    PendingInvocation,
    PublishedOutput,
}

#[cfg(all(feature = "process-remote", feature = "files"))]
async fn wait_for_server_host_marker(
    child: &mut tokio::process::Child,
    marker: &std::path::Path,
) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.try_wait()? {
                anyhow::bail!("server host exited before {}: {status}", marker.display());
            }
            if marker.exists() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("server host did not reach {}", marker.display()))?
}

#[cfg(all(feature = "process-remote", feature = "files"))]
async fn run_configured_process_host_child(root: &std::path::Path, cut: ServerHostLossCut) {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let python = PathBuf::from(std::env::var_os("LAMINAR_PROCESS_PYTHON").unwrap());
    let config = file_process_server_config(root, &repository, python);
    let config_path = repository.join("examples/process_python/server.toml");
    let handle = run_server(config, config_path).await.unwrap();
    let db = match &handle.runtime {
        ServerRuntime::Single(runtime) => Arc::clone(&runtime.db),
        #[cfg(feature = "cluster")]
        ServerRuntime::Cluster(_) => panic!("process test requires single-node server"),
    };
    publish_process_file(root, "first.json", 60, 100_000).unwrap();
    wait_for_process_sink(&root.join("output"), &[60]).await;
    assert!(db.checkpoint().await.unwrap().success);
    std::fs::write(root.join("first-checkpointed"), []).unwrap();
    if matches!(cut, ServerHostLossCut::PublishedOutput) {
        wait_for_process_sink(&root.join("output"), &[60, 110]).await;
        std::fs::write(root.join("output-published"), []).unwrap();
    }
    let result = handle.wait_for_shutdown().await;
    panic!("server host stopped before forced termination: {result:?}");
}

#[cfg(all(feature = "process-remote", feature = "files"))]
async fn assert_configured_process_recovery_after_host_loss(
    cut: ServerHostLossCut,
    test_name: &str,
) {
    const CHILD_ENV: &str = "LAMINAR_PROCESS_SERVER_HOST_LOSS_CHILD";
    if let Some(root) = std::env::var_os(CHILD_ENV) {
        run_configured_process_host_child(std::path::Path::new(&root), cut).await;
        return;
    }
    let Some(python) = std::env::var_os("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let output_dir = root.join("output");
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_ENV, root)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
    let mut host = command.spawn().unwrap();
    let cut_marker = match cut {
        ServerHostLossCut::PendingInvocation => "second-entered",
        ServerHostLossCut::PublishedOutput => "output-published",
    };
    let cut_result: anyhow::Result<()> = async {
        wait_for_server_host_marker(&mut host, &root.join("first-checkpointed")).await?;
        if matches!(cut, ServerHostLossCut::PendingInvocation) {
            std::fs::write(root.join("hold-invocation"), [])?;
        }
        publish_process_file(root, "second.json", 50, 100_050)?;
        wait_for_server_host_marker(&mut host, &root.join(cut_marker)).await
    }
    .await;
    let kill_result = host.start_kill();
    // Abrupt host termination bypasses its worker supervisor.
    let exit_signal = std::fs::write(root.join("exit-worker"), []);
    let release_signal = std::fs::write(root.join("release-invocation"), []);
    let host_status = tokio::time::timeout(Duration::from_secs(5), host.wait()).await;
    let worker_exit = tokio::time::timeout(Duration::from_secs(10), async {
        while !root.join("worker-exiting").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    cut_result.unwrap();
    kill_result.unwrap();
    assert!(!host_status.unwrap().unwrap().success());
    exit_signal.unwrap();
    release_signal.unwrap();
    worker_exit.expect("Python worker did not acknowledge exit after host loss");

    let before_replay = match cut {
        ServerHostLossCut::PendingInvocation => vec![60],
        ServerHostLossCut::PublishedOutput => vec![60, 110],
    };
    assert_eq!(process_sink_totals(&output_dir), before_replay);
    assert_eq!(
        std::fs::read_to_string(root.join("second-entered"))
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["1"]
    );
    std::fs::remove_file(root.join("exit-worker")).unwrap();
    std::fs::remove_file(root.join("release-invocation")).unwrap();
    std::fs::remove_file(root.join("worker-exiting")).unwrap();
    if matches!(cut, ServerHostLossCut::PendingInvocation) {
        std::fs::remove_file(root.join("hold-invocation")).unwrap();
    }

    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = repository.join("examples/process_python/server.toml");
    let config = file_process_server_config(root, &repository, python.into());
    let replacement = run_server(config, config_path).await.unwrap();
    let restored_db = match &replacement.runtime {
        ServerRuntime::Single(runtime) => Arc::clone(&runtime.db),
        #[cfg(feature = "cluster")]
        ServerRuntime::Cluster(_) => panic!("process test requires single-node server"),
    };
    let after_replay = match cut {
        ServerHostLossCut::PendingInvocation => vec![60, 110],
        ServerHostLossCut::PublishedOutput => vec![60, 110, 110],
    };
    let replay = tokio::time::timeout(Duration::from_secs(15), async {
        while process_sink_totals(&output_dir) != after_replay {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let checkpoint = if replay.is_ok() {
        Some(restored_db.checkpoint().await)
    } else {
        None
    };
    let stopped = tokio::spawn(replacement.wait_for_shutdown());
    let stop_signal = std::fs::write(root.join("exit-worker"), []);
    let stop_result = tokio::time::timeout(Duration::from_secs(10), stopped).await;
    assert!(
        replay.is_ok(),
        "configured process sink did not publish {after_replay:?}; observed {:?}",
        process_sink_totals(&output_dir)
    );
    assert!(checkpoint.unwrap().unwrap().success);
    stop_signal.unwrap();
    let error = stop_result.unwrap().unwrap().unwrap_err();
    assert!(error.to_string().contains("process worker 0 exited"));
    assert!(restored_db.is_closed());
    assert_eq!(process_sink_totals(&output_dir), after_replay);
    assert_eq!(
        std::fs::read_to_string(root.join("second-entered"))
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["1", "1"]
    );
}

#[cfg(all(feature = "process-remote", feature = "files"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_file_process_replays_pending_after_server_host_loss() {
    assert_configured_process_recovery_after_host_loss(
        ServerHostLossCut::PendingInvocation,
        "server::tests::configured_file_process_replays_pending_after_server_host_loss",
    )
    .await;
}

#[cfg(all(feature = "process-remote", feature = "files"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_file_process_republishes_uncheckpointed_output_after_server_host_loss() {
    assert_configured_process_recovery_after_host_loss(
        ServerHostLossCut::PublishedOutput,
        "server::tests::configured_file_process_republishes_uncheckpointed_output_after_server_host_loss",
    )
    .await;
}

#[cfg(all(feature = "process-remote", feature = "files"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_python_process_limits_worker_slots_and_input_bytes() {
    let Some(python) = std::env::var_os("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = repository.join("examples/process_python/server.toml");
    let mut config = file_process_server_config(root, &repository, python.into());
    config.process_functions[0].max_in_flight = 1;
    config.server.source_queue_max_bytes = 512 * 1024;
    config.server.pipeline_max_input_buf_bytes = Some(512 * 1024);
    let manifest = &config.process_functions[0].manifest;
    let mut descriptor =
        laminar_db::process_function::ProcessFunctionDescriptor::from_manifest_json(
            &std::fs::read(manifest).unwrap(),
        )
        .unwrap();
    descriptor.limits.max_batch_rows = 1;
    descriptor.limits.max_input_rows = 32;
    descriptor.limits.max_input_bytes = 128 * 1024;
    std::fs::write(manifest, descriptor.to_manifest_json().unwrap()).unwrap();

    let hold = root.join("hold-invocation");
    let release = root.join("release-invocation");
    let entered = root.join("second-entered");
    std::fs::write(&hold, []).unwrap();
    let server = run_server(config, config_path).await.unwrap();
    let db = match &server.runtime {
        ServerRuntime::Single(runtime) => Arc::clone(&runtime.db),
        #[cfg(feature = "cluster")]
        ServerRuntime::Cluster(_) => panic!("process test requires single-node server"),
    };
    publish_process_file(root, "first.json", 50, 100_000).unwrap();
    let entered_call = tokio::time::timeout(Duration::from_secs(10), async {
        while !entered.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if entered_call.is_err() {
        let fault = db.last_fault();
        let stopped = tokio::spawn(server.wait_for_shutdown());
        std::fs::write(root.join("exit-worker"), []).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(10), stopped).await;
        panic!("Python worker did not enter the held invocation; fault: {fault:?}");
    }
    for index in 1..32 {
        let row = serde_json::json!({
            "key": format!("key_{index}"), "amount": 50, "ts": 100_000
        });
        let staged = root.join("staged.json");
        std::fs::write(&staged, format!("{row}\n")).unwrap();
        std::fs::rename(
            staged,
            root.join("input").join(format!("more_{index}.json")),
        )
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let held_calls = std::fs::read_to_string(&entered).unwrap().lines().count();
    let held_output = process_sink_totals(&root.join("output"));
    let held_fault = db.last_fault();

    std::fs::write(release, []).unwrap();
    let published = tokio::time::timeout(Duration::from_secs(10), async {
        while process_sink_totals(&root.join("output")) != vec![50; 32] {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if published.is_err() {
        let fault = db.last_fault();
        let totals = process_sink_totals(&root.join("output"));
        let entered_count = std::fs::read_to_string(&entered).unwrap().lines().count();
        let stopped = tokio::spawn(server.wait_for_shutdown());
        std::fs::write(root.join("exit-worker"), []).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(10), stopped).await;
        panic!("queued calls did not publish; fault: {fault:?}, totals: {totals:?}, entered: {entered_count}");
    }
    let drained_calls = std::fs::read_to_string(&entered).unwrap().lines().count();
    let oversized = serde_json::json!({
        "key": "x".repeat(256 * 1024), "amount": 1, "ts": 100_050
    });
    let staged = root.join("staged.json");
    std::fs::write(&staged, format!("{oversized}\n")).unwrap();
    std::fs::rename(staged, root.join("input/oversized.json")).unwrap();
    let fault = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(fault) = db.last_fault() {
                break fault;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    let stopped = tokio::spawn(server.wait_for_shutdown());
    std::fs::write(root.join("exit-worker"), []).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), stopped)
        .await
        .expect("server did not stop after worker exit")
        .unwrap();
    assert_eq!(held_calls, 1);
    assert!(held_output.is_empty());
    assert!(held_fault.is_none(), "{held_fault:?}");
    assert_eq!(drained_calls, 32);
    let fault = fault.expect("oversized input did not fault the process pipeline");
    assert!(fault.contains("process input budget exceeded"), "{fault}");
    assert_eq!(process_sink_totals(&root.join("output")), vec![50; 32]);
    assert_eq!(
        std::fs::read_to_string(&entered).unwrap().lines().count(),
        32
    );
    let error = result.unwrap_err();
    assert!(error.to_string().contains("process worker 0 exited"));
    assert!(db.is_closed());
}

fn make_source(name: &str, connector: &str) -> SourceConfig {
    SourceConfig {
        name: name.to_string(),
        connector: connector.to_string(),
        format: Some("json".to_string()),
        properties: toml::Table::new(),
        schema: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: true,
            },
        ],
        primary_key: vec![],
        watermark: None,
    }
}

#[cfg(feature = "cluster")]
async fn catalog_test_db(
    object_store: Arc<dyn object_store::ObjectStore>,
) -> (
    Arc<LaminarDB>,
    Arc<laminar_core::cluster::control::CatalogManifestStore>,
) {
    use laminar_core::cluster::control::{
        CatalogManifestStore, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner,
        LeaderLeaseStore, LeaseDeadline, LeaseOutcome,
    };
    use laminar_core::cluster::discovery::NodeId;

    let node = NodeId(1);
    let boot = uuid::Uuid::from_u128(101);
    let owner = LeaderLeaseOwner {
        node,
        boot,
        process_term: 1,
    };
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&object_store), 1_000));
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        unreachable!()
    };
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&kv),
        Arc::clone(&kv),
        None,
        members_rx,
        boot,
    ));
    controller.set_active(false);
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(30),
        )))
        .unwrap();
    let (_lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease));
    controller
        .set_leader_lease_watch(
            lease_rx,
            owner,
            Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(30))),
        )
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));
    let manifest_store = Arc::new(CatalogManifestStore::new(authority));
    let participant = laminar_core::checkpoint::CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: boot,
    };
    let verified_namespaces = laminar_core::cluster::control::prove_shared_object_store_namespaces(
        participant,
        &[participant],
        kv,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    let vnode_registry = Arc::new(laminar_core::state::VnodeRegistry::new(1));
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .verified_cluster_namespaces(verified_namespaces)
        .vnode_registry(vnode_registry)
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    (db, manifest_store)
}

#[test]
fn test_source_to_ddl_basic() {
    let mut source = make_source("events", "kafka");
    source.primary_key = vec!["id".to_string()];
    let ddl = source_to_ddl(&source);
    assert!(ddl.starts_with("CREATE SOURCE events"));
    assert!(ddl.contains("id BIGINT NOT NULL"));
    assert!(ddl.contains("name VARCHAR"));
    assert!(ddl.contains("PRIMARY KEY (id)"));
    assert!(ddl.contains("FROM KAFKA FORMAT JSON"));
    assert!(!ddl.contains("format ="));
}

#[test]
fn test_source_to_ddl_omitted_format_uses_connector_default() {
    let mut source = make_source("events", "kafka");
    source.format = None;
    let ddl = source_to_ddl(&source);
    assert!(ddl.ends_with("FROM KAFKA"), "{ddl}");
}

/// Native OTel metadata resolves columns before watermark validation without a codec.
#[cfg(feature = "otel")]
#[tokio::test]
async fn execute_config_ddl_columnless_otel_with_watermark_succeeds() {
    let source: SourceConfig = toml::from_str(
        r#"
name = "otel_events"
connector = "otel"
[properties]
port = 0
signals = "logs"
[watermark]
column = "_laminar_received_at"
max_out_of_orderness = "10s"
"#,
    )
    .unwrap();
    assert!(source.format.is_none());

    let db = laminar_db::LaminarDB::open().unwrap();
    let mut config = ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![source],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        process_functions: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    execute_config_ddl(&db, &config, false)
        .await
        .expect("columnless OTel + WATERMARK FOR should compose");
    config.sources[0].name = "otel_json".to_string();
    config.sources[0].format = Some("json".to_string());
    let error = execute_config_ddl(&db, &config, false)
        .await
        .expect_err("OTLP cannot use an explicit JSON codec");
    assert!(error.to_string().contains("fixed native protocol"));
}

/// Columnless source validation reaches DDL and preserves connector configuration errors.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn execute_config_ddl_columnless_kafka_preserves_connector_config_error() {
    let mut source = make_source("events", "kafka");
    source.schema.clear();
    source.watermark = Some(WatermarkConfig {
        column: "ts".to_string(),
        max_out_of_orderness: std::time::Duration::from_secs(5),
    });

    let db = laminar_db::LaminarDB::open().unwrap();
    let config = ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![source],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        process_functions: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    let err = execute_config_ddl(&db, &config, false).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("source 'events' (kafka) schema resolution")
            && msg.contains("missing required config: bootstrap.servers"),
        "expected the connector configuration error from the DDL layer, got: {msg}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_config_rejects_expanded_connector_secret_before_manifest_write() {
    use object_store::ObjectStore;

    let mut source = make_source("secured", "generator");
    source.properties.insert(
        "password".to_string(),
        toml::Value::String("expanded-password-must-not-persist".to_string()),
    );
    let config = ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![source],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        process_functions: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (db, manifest_store) = catalog_test_db(object_store).await;

    let error = execute_config_ddl(&db, &config, true).await.unwrap_err();
    assert!(error.to_string().contains("cannot persist secret property"));
    assert!(manifest_store.load().await.unwrap().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn empty_cluster_config_still_seals_an_empty_inventory() {
    use object_store::ObjectStore;

    let config = ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        process_functions: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (db, manifest_store) = catalog_test_db(object_store).await;

    execute_config_ddl(&db, &config, true).await.unwrap();
    assert_eq!(
        manifest_store.load().await.unwrap().unwrap().entries,
        Vec::new()
    );
}

#[test]
fn test_source_to_ddl_with_watermark() {
    let mut source = make_source("events", "kafka");
    source.watermark = Some(WatermarkConfig {
        column: "ts".to_string(),
        max_out_of_orderness: std::time::Duration::from_secs(5),
    });
    let ddl = source_to_ddl(&source);
    assert!(ddl.contains("WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"));
}

#[test]
fn connector_identifiers_preserve_provider_punctuation() {
    let hyphenated = source_to_ddl(&make_source("events", "postgres-cdc"));
    assert!(hyphenated.contains("FROM \"postgres-cdc\""));

    let underscored = source_to_ddl(&make_source("events", "vendor_v2"));
    assert!(underscored.contains("FROM VENDOR_V2"));
}

#[test]
fn test_source_to_ddl_with_properties() {
    let mut source = make_source("events", "kafka");
    source.properties.insert(
        "bootstrap.servers".to_string(),
        toml::Value::String("localhost:9092".to_string()),
    );
    source.properties.insert(
        "topic".to_string(),
        toml::Value::String("events".to_string()),
    );
    source.properties.insert(
        "client-id".to_string(),
        toml::Value::String("source-client".to_string()),
    );
    source.properties.insert(
        "vendor\"option".to_string(),
        toml::Value::String("quoted-key".to_string()),
    );
    let ddl = source_to_ddl(&source);
    assert!(ddl.contains("\"bootstrap.servers\" = 'localhost:9092'"));
    assert!(ddl.contains("\"topic\" = 'events'"));
    assert!(ddl.contains("\"client-id\" = 'source-client'"));
    assert!(ddl.contains("\"vendor\"\"option\" = 'quoted-key'"));
    assert!(ddl.ends_with(") FORMAT JSON"));

    let statements = laminar_sql::parser::parse_streaming_sql(&ddl).unwrap();
    let laminar_sql::parser::StreamingStatement::CreateSource(parsed) = &statements[0] else {
        panic!("expected CREATE SOURCE")
    };
    assert_eq!(
        parsed
            .connector_options
            .get("client-id")
            .map(String::as_str),
        Some("source-client")
    );
    assert_eq!(
        parsed
            .connector_options
            .get("vendor\"option")
            .map(String::as_str),
        Some("quoted-key")
    );
}

#[test]
fn test_pipeline_to_ddl() {
    let pipeline = PipelineConfig {
        name: "vwap".to_string(),
        sql: "SELECT symbol, SUM(price) FROM trades GROUP BY symbol".to_string(),
    };
    let ddl = pipeline_to_ddl(&pipeline);
    assert_eq!(
        ddl,
        "CREATE STREAM vwap AS SELECT symbol, SUM(price) FROM trades GROUP BY symbol"
    );
}

#[test]
fn test_sink_to_ddl() {
    let mut props = toml::Table::new();
    props.insert(
        "topic".to_string(),
        toml::Value::String("output".to_string()),
    );
    props.insert(
        "bootstrap.servers".to_string(),
        toml::Value::String("localhost:9092".to_string()),
    );
    props.insert(
        "oauthbearer-token".to_string(),
        toml::Value::String("token".to_string()),
    );
    let sink = SinkConfig {
        name: "output_sink".to_string(),
        pipeline: "vwap".to_string(),
        connector: "kafka".to_string(),
        format: Some("json".to_string()),
        properties: props,
    };
    let ddl = sink_to_ddl(&sink);
    assert!(ddl.starts_with("CREATE SINK output_sink FROM vwap INTO KAFKA"));
    assert!(ddl.contains("\"topic\" = 'output'"));
    assert!(ddl.contains("\"bootstrap.servers\" = 'localhost:9092'"));
    assert!(ddl.contains("\"oauthbearer-token\" = 'token'"));
    assert!(ddl.ends_with(") FORMAT JSON"));
    assert!(!ddl.contains("format ="));
    // Delivery is injected from the pipeline-wide engine contract at connector build time.
    assert!(!ddl.contains("delivery"));

    let statements = laminar_sql::parser::parse_streaming_sql(&ddl).unwrap();
    let laminar_sql::parser::StreamingStatement::CreateSink(parsed) = &statements[0] else {
        panic!("expected CREATE SINK")
    };
    assert_eq!(
        parsed
            .connector_options
            .get("oauthbearer-token")
            .map(String::as_str),
        Some("token")
    );
}

#[test]
fn test_sink_to_ddl_has_no_per_sink_delivery_dimension() {
    let sink = SinkConfig {
        name: "out".to_string(),
        pipeline: "p".to_string(),
        connector: "kafka".to_string(),
        format: None,
        properties: toml::Table::new(),
    };
    let ddl = sink_to_ddl(&sink);
    assert!(!ddl.contains("delivery"));
}

#[test]
fn test_lookup_to_ddl() {
    let lookup = LookupConfig {
        name: "instruments".to_string(),
        connector: "postgres".to_string(),
        strategy: "poll".to_string(),
        cache: LookupCacheConfig::default(),
        properties: {
            let mut t = toml::Table::new();
            t.insert(
                "connection".to_string(),
                toml::Value::String("postgresql://localhost/db".to_string()),
            );
            t
        },
        primary_key: vec!["symbol".to_string()],
        schema: vec![ColumnDef {
            name: "symbol".to_string(),
            data_type: "VARCHAR".to_string(),
            nullable: false,
        }],
    };
    let ddl = lookup_to_ddl(&lookup).unwrap();
    assert!(ddl.starts_with("CREATE LOOKUP TABLE instruments"));
    assert!(ddl.contains("symbol VARCHAR NOT NULL"));
    assert!(ddl.contains("PRIMARY KEY (symbol)"));
    assert!(ddl.contains("'connector' = 'postgres'"));
    assert!(ddl.contains("'strategy' = 'poll'"));
    assert!(ddl.contains("'connection' = 'postgresql://localhost/db'"));
}

#[test]
fn test_lookup_to_ddl_no_primary_key() {
    let lookup = LookupConfig {
        name: "t".to_string(),
        connector: "postgres".to_string(),
        strategy: "poll".to_string(),
        cache: LookupCacheConfig::default(),
        properties: toml::Table::new(),
        primary_key: vec![],
        schema: vec![ColumnDef {
            name: "id".to_string(),
            data_type: "INT".to_string(),
            nullable: false,
        }],
    };
    let ddl = lookup_to_ddl(&lookup).unwrap();
    assert!(!ddl.contains("PRIMARY KEY"));
}

#[test]
fn test_lookup_to_ddl_empty_schema_rejected() {
    let lookup = LookupConfig {
        name: "bad".to_string(),
        connector: "postgres".to_string(),
        strategy: "poll".to_string(),
        cache: LookupCacheConfig::default(),
        properties: toml::Table::new(),
        primary_key: vec![],
        schema: vec![],
    };
    assert!(lookup_to_ddl(&lookup).is_err());
}

#[test]
fn test_toml_value_to_sql() {
    assert_eq!(
        toml_value_to_sql(&toml::Value::String("hello".to_string())),
        "hello"
    );
    assert_eq!(toml_value_to_sql(&toml::Value::Integer(42)), "42");
    assert_eq!(toml_value_to_sql(&toml::Value::Boolean(true)), "true");
    assert_eq!(toml_value_to_sql(&toml::Value::Float(3.25)), "3.25");
}

#[test]
fn test_toml_value_to_sql_escapes_single_quotes() {
    assert_eq!(
        toml_value_to_sql(&toml::Value::String("it's a test".to_string())),
        "it''s a test"
    );
    assert_eq!(
        toml_value_to_sql(&toml::Value::String("a''b".to_string())),
        "a''''b"
    );
}
