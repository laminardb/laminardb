use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use laminar_db::process_function::remote::{
    LocalPythonWorker, LocalPythonWorkerConfig, RemoteProcessClient,
};
use laminar_db::process_function::ProcessFunctionDescriptor;
use laminar_db::subscription::{PortalFrame, SubscribeStart};
use laminar_db::LaminarDB;

enum DemoMode {
    InMemory,
    Checkpoint(PathBuf),
    Resume(PathBuf),
}

fn demo_mode() -> Result<DemoMode, Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [] => Ok(DemoMode::InMemory),
        [command, path] if command == "checkpoint" => Ok(DemoMode::Checkpoint(PathBuf::from(path))),
        [command, path] if command == "resume" => Ok(DemoMode::Resume(PathBuf::from(path))),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "usage: process_python [checkpoint|resume STORAGE_DIR]",
        )
        .into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mode = demo_mode()?;
    if let Some(endpoint) = std::env::var_os("LAMINAR_PROCESS_ENDPOINT") {
        let endpoint = endpoint.to_str().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "worker endpoint must be UTF-8",
            )
        })?;
        let descriptor = ProcessFunctionDescriptor::from_manifest_json(include_bytes!(
            "../../../examples/process_python/manifest.json"
        ))?;
        let client = Arc::new(
            RemoteProcessClient::connect_loopback(endpoint, descriptor, 2, Duration::from_secs(5))
                .await?,
        );
        return run_database(&client, &mode).await;
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let example = root.join("examples/process_python");
    let python = std::env::var_os("LAMINAR_PROCESS_PYTHON")
        .map_or_else(|| PathBuf::from("python"), PathBuf::from);
    let worker = LocalPythonWorker::start(LocalPythonWorkerConfig {
        python,
        runtime_root: None,
        manifest: example.join("manifest.json"),
        handler_file: example.join("handler.py"),
        function: "handle".into(),
        python_paths: vec![root.join("python/laminardb_process")],
        max_in_flight: 2,
        timeout: Duration::from_secs(5),
    })
    .await?;
    let outcome = run_database(&worker.client(), &mode).await;
    let cleanup = worker.shutdown().await;
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(Box::new(error) as Box<dyn Error>),
        (Err(error), Err(cleanup)) => {
            Err(std::io::Error::other(format!("{error}; worker cleanup: {cleanup}")).into())
        }
    }
}

async fn run_database(
    client: &Arc<RemoteProcessClient>,
    mode: &DemoMode,
) -> Result<(), Box<dyn Error>> {
    let db = match mode {
        DemoMode::InMemory => LaminarDB::open()?,
        DemoMode::Checkpoint(path) | DemoMode::Resume(path) => {
            LaminarDB::builder()
                .storage_dir(path)
                .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
                .build()
                .await?
        }
    };
    let outcome = run_pipeline(&db, client, mode).await;
    let cleanup = db.shutdown().await;
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(Box::new(error) as Box<dyn Error>),
        (Err(error), Err(cleanup)) => {
            Err(std::io::Error::other(format!("{error}; database cleanup: {cleanup}")).into())
        }
    }
}

async fn run_pipeline(
    db: &Arc<LaminarDB>,
    client: &Arc<RemoteProcessClient>,
    mode: &DemoMode,
) -> Result<(), Box<dyn Error>> {
    db.execute(
        "CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, \
         ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await?;
    db.register_remote_process_function(
        "activity",
        "events",
        client.descriptor().clone(),
        Arc::clone(client),
    )
    .await?;
    db.start().await?;
    let mut portal = db
        .open_subscription("activity", None, SubscribeStart::Tail)
        .await?;
    let (rows, expected_totals): (&[(i64, i64)], &[i64]) = match mode {
        DemoMode::InMemory => (&[(60, 100_000), (50, 101_000)], &[60, 110]),
        DemoMode::Checkpoint(_) => (&[(60, 100_000)], &[60]),
        DemoMode::Resume(_) => (&[(50, 100_050)], &[110]),
    };
    let batch = RecordBatch::try_new(
        client.descriptor().input_schema.clone(),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["a"; rows.len()])),
            std::sync::Arc::new(Int64Array::from(
                rows.iter().map(|(amount, _)| *amount).collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|(_, at_us)| *at_us).collect::<Vec<_>>(),
            )),
        ],
    )?;
    db.source_untyped("events")?.push_arrow(batch)?;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut received = 0;
        while received < expected_totals.len() {
            match portal.next_frame().await {
                Some(PortalFrame::Batch { batch, .. }) => {
                    let totals = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| std::io::Error::other("unexpected output schema"))?;
                    for row in 0..totals.len() {
                        if expected_totals.get(received) != Some(&totals.value(row)) {
                            return Err(std::io::Error::other(format!(
                                "unexpected total {} at output {received}; checkpoint may be missing or stale",
                                totals.value(row)
                            )));
                        }
                        println!("key=a total={}", totals.value(row));
                        received += 1;
                    }
                }
                Some(PortalFrame::Barrier { .. }) => {}
                Some(PortalFrame::Error { .. }) | Some(PortalFrame::Lagged(_)) | None => {
                    return Err(std::io::Error::other("process output unavailable"));
                }
            }
        }
        Ok::<(), std::io::Error>(())
    })
    .await??;
    if !matches!(mode, DemoMode::InMemory) {
        db.checkpoint().await?;
    }
    Ok(())
}
