//! Offline Python process-function bindings and worker lifecycle for server deployments.

use std::path::{Path, PathBuf};

use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use laminar_db::process_function::remote::{LocalPythonWorker, LocalPythonWorkerConfig};
use laminar_db::LaminarDB;
use laminar_sql::parser::StreamingStatement;

use crate::config::{ProcessFunctionConfig, ServerConfig, ServerMode};
use crate::server::ServerError;

pub(crate) async fn install(
    db: &LaminarDB,
    config: &ServerConfig,
    config_path: &Path,
) -> Result<Vec<LocalPythonWorker>, ServerError> {
    if config.process_functions.is_empty() {
        return Ok(Vec::new());
    }
    let config_dir = config_path
        .canonicalize()
        .map_err(|error| ServerError::Build(format!("resolve process config file: {error}")))?
        .parent()
        .ok_or_else(|| ServerError::Build("process config file has no parent".into()))?
        .to_path_buf();
    let mut workers = Vec::with_capacity(config.process_functions.len());
    for entry in &config.process_functions {
        match install_one(db, entry, &config_dir, config.server.mode).await {
            Ok(worker) => workers.push(worker),
            Err(primary) => {
                return match shutdown(workers).await {
                    Ok(()) => Err(primary),
                    Err(cleanup) => Err(ServerError::Build(format!(
                        "{primary}; process worker cleanup: {cleanup}"
                    ))),
                };
            }
        }
    }
    Ok(workers)
}

async fn install_one(
    db: &LaminarDB,
    entry: &ProcessFunctionConfig,
    config_dir: &Path,
    mode: ServerMode,
) -> Result<LocalPythonWorker, ServerError> {
    let source_sql = validate_source_sql(entry)?;
    if mode == ServerMode::Single {
        db.execute(source_sql)
            .await
            .map_err(|source| ServerError::Ddl {
                section: "process source".into(),
                name: entry.source.clone(),
                source: Box::new(source),
            })?;
    }

    let python = if entry.runtime_root.is_none() && entry.python.components().count() == 1 {
        entry.python.clone()
    } else {
        resolve(config_dir, &entry.python)
    };
    let worker = LocalPythonWorker::start(LocalPythonWorkerConfig {
        python,
        runtime_root: entry
            .runtime_root
            .as_ref()
            .map(|path| resolve(config_dir, path)),
        manifest: resolve(config_dir, &entry.manifest),
        handler_file: resolve(config_dir, &entry.handler_file),
        function: entry.function.clone(),
        python_paths: entry
            .python_paths
            .iter()
            .map(|path| resolve(config_dir, path))
            .collect(),
        max_in_flight: entry.max_in_flight,
        timeout: entry.timeout,
    })
    .await
    .map_err(|error| ServerError::Build(format!("process output '{}': {error}", entry.output)))?;
    let descriptor = worker.client().descriptor().clone();
    let registration = db
        .register_remote_process_function(&entry.output, &entry.source, descriptor, worker.client())
        .await;
    if let Err(primary) = registration {
        let cleanup = worker.shutdown().await;
        return match cleanup {
            Ok(()) => Err(ServerError::Build(format!(
                "process output '{}': {primary}",
                entry.output
            ))),
            Err(cleanup) => Err(ServerError::Build(format!(
                "process output '{}': {primary}; worker cleanup: {cleanup}",
                entry.output
            ))),
        };
    }
    Ok(worker)
}

fn validate_source_sql(entry: &ProcessFunctionConfig) -> Result<&str, ServerError> {
    let sql = entry.source_sql.trim().trim_end_matches(';').trim();
    let statements = laminar_sql::parse_streaming_sql(sql).map_err(|error| {
        ServerError::Build(format!(
            "process output '{}': source_sql must contain one CREATE SOURCE statement: {error}",
            entry.output
        ))
    })?;
    let [StreamingStatement::CreateSource(source)] = statements.as_slice() else {
        return Err(ServerError::Build(format!(
            "process output '{}': source_sql must contain one CREATE SOURCE statement",
            entry.output
        )));
    };
    if source.name.to_string() != entry.source {
        return Err(ServerError::Build(format!(
            "process output '{}': source_sql must create source '{}'",
            entry.output, entry.source
        )));
    }
    Ok(sql)
}

fn resolve(config_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    }
}

pub(crate) async fn wait_for_exit(workers: &[LocalPythonWorker]) -> Option<usize> {
    if workers.is_empty() {
        return std::future::pending().await;
    }
    let mut exits = workers
        .iter()
        .enumerate()
        .map(|(index, worker)| async move {
            worker.wait_for_exit().await;
            index
        })
        .collect::<FuturesUnordered<_>>();
    exits.next().await
}

pub(crate) async fn shutdown(workers: Vec<LocalPythonWorker>) -> Result<(), String> {
    let mut errors = Vec::new();
    for worker in workers {
        if let Err(error) = worker.shutdown().await {
            errors.push(error.to_string());
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_array::{Array, Int64Array};
    use laminar_core::streaming::checkpoint::StreamCheckpointConfig;
    use laminar_db::subscription::{PortalFrame, SubscribeStart, SubscriptionPortal};
    use laminar_db::DeliveryGuarantee;

    use super::*;

    async fn next_total(portal: &mut SubscriptionPortal) -> Option<i64> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match portal.next_frame().await {
                    Some(PortalFrame::Batch { batch, .. }) => {
                        let values = batch.column(1).as_any().downcast_ref::<Int64Array>()?;
                        if !values.is_empty() {
                            return Some(values.value(0));
                        }
                    }
                    Some(PortalFrame::Barrier { .. }) => {}
                    Some(PortalFrame::Error { .. }) | Some(PortalFrame::Lagged(_)) | None => {
                        return None;
                    }
                }
            }
        })
        .await
        .unwrap()
    }

    #[cfg(feature = "files")]
    fn file_totals(directory: &Path) -> Vec<i64> {
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

    #[tokio::test]
    async fn configured_python_function_accepts_sql_input() {
        let Some(python) = std::env::var_os("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config_path = repository.join("examples/process_python/server.toml");
        let mut config = crate::config::load_config(&config_path).unwrap();
        config.process_functions[0].python = python.into();
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
        assert_configured_sql_restart(&config, &config_path).await;
    }

    async fn assert_configured_sql_restart(config: &ServerConfig, config_path: &Path) {
        let storage = tempfile::tempdir().unwrap();
        for (amount, event_time, expected) in [(60, 100_000, 60), (50, 100_050, 110)] {
            let db = LaminarDB::builder()
                .storage_dir(storage.path())
                .checkpoint(StreamCheckpointConfig::default())
                .delivery_guarantee(DeliveryGuarantee::BestEffort)
                .build()
                .await
                .unwrap();
            let workers = install(&db, config, config_path).await.unwrap();
            assert_eq!(db.process_functions().len(), 1);
            db.start().await.unwrap();
            let mut portal = db
                .open_subscription("activity", None, SubscribeStart::Tail)
                .await
                .unwrap();
            db.execute(&format!(
                "INSERT INTO events VALUES ('a', {amount}, {event_time})"
            ))
            .await
            .unwrap();
            assert_eq!(next_total(&mut portal).await, Some(expected));
            db.checkpoint().await.unwrap();
            db.shutdown().await.unwrap();
            shutdown(workers).await.unwrap();
        }
    }

    #[tokio::test]
    async fn configured_environment_bound_python_accepts_sql_and_restores() {
        let (Some(python), Some(runtime)) = (
            std::env::var_os("LAMINAR_PROCESS_PYTHON"),
            std::env::var_os("LAMINAR_PROCESS_PYTHON_RUNTIME_ROOT"),
        ) else {
            return;
        };
        let python = PathBuf::from(python);
        let runtime = PathBuf::from(runtime);
        let package = tempfile::tempdir().unwrap();
        let handlers = package.path().join("handlers");
        std::fs::create_dir(&handlers).unwrap();
        let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let example = repository.join("examples/process_python");
        std::fs::copy(example.join("handler.py"), handlers.join("handler.py")).unwrap();
        let mut roots = vec![
            handlers,
            repository
                .join("python/laminardb_process")
                .canonicalize()
                .unwrap(),
        ];
        if let Some(dependencies) = std::env::var_os("LAMINAR_PROCESS_PYTHON_DEPS") {
            roots.push(PathBuf::from(dependencies).canonicalize().unwrap());
        }
        let mut descriptor =
            laminar_db::process_function::ProcessFunctionDescriptor::from_manifest_json(
                &std::fs::read(example.join("manifest.json")).unwrap(),
            )
            .unwrap();
        descriptor.python_environment = Some(
            laminar_db::process_function::PythonEnvironmentBinding::capture(
                &runtime,
                &python,
                "handler:handle",
                &roots,
            )
            .unwrap(),
        );
        std::fs::write(
            package.path().join("manifest.json"),
            descriptor.to_manifest_json().unwrap(),
        )
        .unwrap();
        let mut document: toml::Value =
            toml::from_str(&std::fs::read_to_string(example.join("server.toml")).unwrap()).unwrap();
        let entry = document
            .get_mut("process_function")
            .unwrap()
            .as_array_mut()
            .unwrap()[0]
            .as_table_mut()
            .unwrap();
        entry.insert(
            "python".into(),
            toml::Value::String(python.to_str().unwrap().into()),
        );
        entry.insert(
            "runtime_root".into(),
            toml::Value::String(runtime.to_str().unwrap().into()),
        );
        entry.insert(
            "handler_file".into(),
            toml::Value::String("handlers/handler.py".into()),
        );
        entry.insert(
            "python_paths".into(),
            toml::Value::Array(
                roots[1..]
                    .iter()
                    .map(|path| toml::Value::String(path.to_str().unwrap().into()))
                    .collect(),
            ),
        );
        let config_path = package.path().join("server.toml");
        std::fs::write(&config_path, toml::to_string(&document).unwrap()).unwrap();
        let config = crate::config::load_config(&config_path).unwrap();
        assert_eq!(
            config.process_functions[0].runtime_root.as_ref(),
            Some(&runtime)
        );
        assert_configured_sql_restart(&config, &config_path).await;
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn configured_python_function_restarts_with_file_source_and_sink() {
        let Some(python) = std::env::var_os("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config_path = repository.join("examples/process_python/server.toml");
        let mut config = crate::config::load_config(&config_path).unwrap();
        config.process_functions[0].python = python.into();
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
        let directory = tempfile::tempdir().unwrap();
        let input_dir = directory.path().join("input");
        let output_dir = directory.path().join("output");
        let checkpoint_dir = directory.path().join("checkpoint");
        std::fs::create_dir(&input_dir).unwrap();
        std::fs::create_dir(&output_dir).unwrap();
        let input_path = input_dir.display().to_string().replace('\\', "/");
        config.process_functions[0].source_sql = format!(
            "CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) \
             FROM FILES ('path' = '{input_path}', 'glob_pattern' = '*.json', \
             'stabilisation_delay' = '100ms') FORMAT JSON"
        );
        let mut sink_properties = toml::Table::new();
        sink_properties.insert(
            "path".into(),
            toml::Value::String(output_dir.display().to_string().replace('\\', "/")),
        );
        config.sinks.push(crate::config::SinkConfig {
            name: "activity_files".into(),
            pipeline: "activity".into(),
            connector: "files".into(),
            format: Some("json".into()),
            properties: sink_properties,
        });
        crate::config::validate_process_functions(&config).unwrap();

        for (name, amount, timestamp, expected, expected_files) in [
            ("first.json", 60, 100_000, 60, &[60][..]),
            ("second.json", 50, 100_050, 110, &[60, 110][..]),
        ] {
            let db = LaminarDB::builder()
                .storage_dir(&checkpoint_dir)
                .checkpoint(StreamCheckpointConfig {
                    interval_ms: None,
                    ..Default::default()
                })
                .delivery_guarantee(DeliveryGuarantee::BestEffort)
                .build()
                .await
                .unwrap();
            let workers = install(&db, &config, &config_path).await.unwrap();
            crate::server::execute_config_ddl(&db, &config, false)
                .await
                .unwrap();
            let mut portal = db
                .open_subscription("activity", None, SubscribeStart::Tail)
                .await
                .unwrap();
            db.start().await.unwrap();
            let staged = directory.path().join("staged.json");
            let mut row = serde_json::to_vec(&serde_json::json!({
                "key": "a", "amount": amount, "ts": timestamp
            }))
            .unwrap();
            row.push(b'\n');
            std::fs::write(&staged, row).unwrap();
            std::fs::rename(staged, input_dir.join(name)).unwrap();
            assert_eq!(next_total(&mut portal).await, Some(expected));
            assert!(db.checkpoint().await.unwrap().success);
            tokio::time::timeout(Duration::from_secs(10), async {
                while file_totals(&output_dir).as_slice() != expected_files {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("configured process sink did not publish the expected files");
            db.shutdown().await.unwrap();
            shutdown(workers).await.unwrap();
        }
    }

    #[tokio::test]
    async fn process_source_sql_must_create_the_bound_source() {
        let mut config: ServerConfig =
            toml::from_str(include_str!("../../../examples/process_python/server.toml")).unwrap();
        config.process_functions[0].source = "other".into();
        let db = LaminarDB::builder()
            .delivery_guarantee(DeliveryGuarantee::BestEffort)
            .build()
            .await
            .unwrap();
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/process_python/server.toml");
        let error = install(&db, &config, &path).await.err().unwrap();
        assert!(error.to_string().contains("must create source 'other'"));
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn process_source_sql_rejects_multiple_statements_before_ddl() {
        let mut config: ServerConfig =
            toml::from_str(include_str!("../../../examples/process_python/server.toml")).unwrap();
        config.process_functions[0]
            .source_sql
            .push_str("; CREATE SOURCE extra (id BIGINT)");
        let db = LaminarDB::open().unwrap();
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/process_python/server.toml");
        let error = install(&db, &config, &path).await.err().unwrap();
        assert!(error.to_string().contains("one CREATE SOURCE statement"));
        assert!(db.sources().is_empty());
        db.shutdown().await.unwrap();
    }
}
