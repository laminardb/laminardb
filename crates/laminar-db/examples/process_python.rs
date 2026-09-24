use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use laminar_db::process_function::remote::{LocalPythonWorker, LocalPythonWorkerConfig};
use laminar_db::subscription::{PortalFrame, SubscribeStart};
use laminar_db::LaminarDB;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let example = root.join("examples/process_python");
    let python = std::env::var_os("LAMINAR_PROCESS_PYTHON")
        .map_or_else(|| PathBuf::from("python"), PathBuf::from);
    let worker = LocalPythonWorker::start(LocalPythonWorkerConfig {
        python,
        manifest: example.join("manifest.json"),
        handler_file: example.join("handler.py"),
        function: "handle".into(),
        python_paths: vec![root.join("python/laminardb_process")],
        max_in_flight: 2,
        timeout: Duration::from_secs(5),
    })
    .await?;
    let outcome = run_database(&worker).await;
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

async fn run_database(worker: &LocalPythonWorker) -> Result<(), Box<dyn Error>> {
    let db = LaminarDB::open()?;
    let outcome = run_pipeline(&db, worker).await;
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
    db: &std::sync::Arc<LaminarDB>,
    worker: &LocalPythonWorker,
) -> Result<(), Box<dyn Error>> {
    db.execute(
        "CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, \
         ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await?;
    db.register_remote_process_function(
        "activity",
        "events",
        worker.client().descriptor().clone(),
        worker.client(),
    )
    .await?;
    db.start().await?;
    let mut portal = db
        .open_subscription("activity", None, SubscribeStart::Tail)
        .await?;
    let input = worker.client().descriptor().input_schema.clone();
    let batch = RecordBatch::try_new(
        input,
        vec![
            std::sync::Arc::new(StringArray::from(vec!["a", "a"])),
            std::sync::Arc::new(Int64Array::from(vec![60, 50])),
            std::sync::Arc::new(TimestampMicrosecondArray::from(vec![100_000, 101_000])),
        ],
    )?;
    db.source_untyped("events")?.push_arrow(batch)?;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut received = 0;
        while received < 2 {
            match portal.next_frame().await {
                Some(PortalFrame::Batch { batch, .. }) => {
                    let totals = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| std::io::Error::other("unexpected output schema"))?;
                    for row in 0..totals.len() {
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
    Ok(())
}
