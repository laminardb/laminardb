use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use futures_util::{future, StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::{ArrowRecordBatchStream, FileScanTaskStream};
use iceberg::table::Table;
use tokio::sync::mpsc;

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::IcebergSourceConfig;

use super::append_lineage::AppendSnapshotPlan;
use super::cursor::IcebergSourceCursorV1;

pub(super) enum ScanOutput {
    Batch {
        batch: RecordBatch,
        completed_cursor: Option<IcebergSourceCursorV1>,
    },
    Cursor(IcebergSourceCursorV1),
}

pub(super) struct ScanTask {
    pub receiver: mpsc::Receiver<Result<ScanOutput, ConnectorError>>,
    pub handle: tokio::task::JoinHandle<()>,
    pub started_at: Instant,
}

enum ScanFiles {
    All,
    Added(Arc<HashSet<String>>),
}

struct ScanPlan {
    snapshot_id: i64,
    files: ScanFiles,
    cursor: IcebergSourceCursorV1,
}

pub(super) fn full_snapshot_task(
    table: Table,
    config: &IcebergSourceConfig,
    snapshot_id: i64,
) -> Result<ScanTask, ConnectorError> {
    let snapshot = table
        .metadata()
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| {
            ConnectorError::ReadError(format!(
                "[LDB-ICEBERG-SNAPSHOT-MISSING] snapshot {snapshot_id} does not exist"
            ))
        })?;
    let cursor = IcebergSourceCursorV1::from_snapshot(config, &table, snapshot);
    spawn_scan(
        table,
        config,
        vec![ScanPlan {
            snapshot_id,
            files: ScanFiles::All,
            cursor,
        }],
    )
}

pub(super) fn append_task(
    table: Table,
    config: &IcebergSourceConfig,
    plans: Vec<AppendSnapshotPlan>,
) -> Result<Option<ScanTask>, ConnectorError> {
    if plans.is_empty() {
        return Ok(None);
    }
    let plans = plans
        .into_iter()
        .map(|plan| ScanPlan {
            snapshot_id: plan.snapshot.snapshot_id(),
            files: ScanFiles::Added(Arc::new(plan.added_file_paths)),
            cursor: IcebergSourceCursorV1::from_snapshot(config, &table, &plan.snapshot),
        })
        .collect();
    spawn_scan(table, config, plans).map(Some)
}

fn spawn_scan(
    table: Table,
    config: &IcebergSourceConfig,
    plans: Vec<ScanPlan>,
) -> Result<ScanTask, ConnectorError> {
    let predicate = config
        .filter
        .as_deref()
        .map(serde_json::from_str::<Predicate>)
        .transpose()
        .map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "invalid Iceberg filter predicate JSON: {error}"
            ))
        })?;
    let projection = config.select_columns.clone();
    let concurrency = config.scan_concurrency;
    let request_timeout = config.storage.request_timeout;
    let (sender, receiver) = mpsc::channel(config.scan_channel_capacity);
    let handle = tokio::spawn(async move {
        for plan in plans {
            if let Err(error) = run_plan(
                &table,
                &projection,
                predicate.clone(),
                concurrency,
                request_timeout,
                plan,
                &sender,
            )
            .await
            {
                let _ = sender.send(Err(error)).await;
                return;
            }
        }
    });
    Ok(ScanTask {
        receiver,
        handle,
        started_at: Instant::now(),
    })
}

async fn run_plan(
    table: &Table,
    projection: &[String],
    predicate: Option<Predicate>,
    concurrency: usize,
    request_timeout: std::time::Duration,
    plan: ScanPlan,
    sender: &mpsc::Sender<Result<ScanOutput, ConnectorError>>,
) -> Result<(), ConnectorError> {
    let mut builder = table
        .scan()
        .snapshot_id(plan.snapshot_id)
        .with_batch_size(Some(8_192))
        .with_concurrency_limit(concurrency);
    builder = if projection.is_empty() {
        builder.select_all()
    } else {
        builder.select(projection.iter().map(String::as_str))
    };
    if let Some(predicate) = predicate {
        builder = builder.with_filter(predicate);
    }
    let scan = builder.build().map_err(|error| {
        ConnectorError::ReadError(format!(
            "[LDB-ICEBERG-SCAN-BUILD] snapshot {}: {error}",
            plan.snapshot_id
        ))
    })?;
    let stream = match plan.files {
        ScanFiles::All => tokio::time::timeout(request_timeout, scan.to_arrow())
            .await
            .map_err(|_| scan_timeout(request_timeout))?
            .map_err(|error| scan_error(&error))?,
        ScanFiles::Added(paths) => {
            let tasks = tokio::time::timeout(request_timeout, scan.plan_files())
                .await
                .map_err(|_| scan_timeout(request_timeout))?
                .map_err(|error| scan_error(&error))?;
            let filtered: FileScanTaskStream = Box::pin(
                tasks.try_filter(move |task| future::ready(paths.contains(task.data_file_path()))),
            );
            table
                .reader_builder()
                .with_batch_size(8_192)
                .build()
                .read(filtered)
                .map_err(|error| scan_error(&error))?
                .stream()
        }
    };
    send_stream(stream, plan.cursor, sender, request_timeout).await
}

async fn send_stream(
    mut stream: ArrowRecordBatchStream,
    cursor: IcebergSourceCursorV1,
    sender: &mpsc::Sender<Result<ScanOutput, ConnectorError>>,
    request_timeout: std::time::Duration,
) -> Result<(), ConnectorError> {
    let mut pending = None;
    loop {
        let result = tokio::time::timeout(request_timeout, stream.next())
            .await
            .map_err(|_| scan_timeout(request_timeout))?;
        let Some(result) = result else {
            break;
        };
        let batch = result.map_err(|error| scan_error(&error))?;
        if batch.num_rows() == 0 {
            continue;
        }
        if let Some(batch) = pending.replace(batch) {
            send(
                sender,
                ScanOutput::Batch {
                    batch,
                    completed_cursor: None,
                },
            )
            .await?;
        }
    }
    if let Some(batch) = pending {
        send(
            sender,
            ScanOutput::Batch {
                batch,
                completed_cursor: Some(cursor),
            },
        )
        .await
    } else {
        send(sender, ScanOutput::Cursor(cursor)).await
    }
}

fn scan_timeout(timeout: std::time::Duration) -> ConnectorError {
    ConnectorError::ReadError(format!(
        "[LDB-ICEBERG-STORAGE-TIMEOUT] Iceberg scan made no progress for {timeout:?}"
    ))
}

async fn send(
    sender: &mpsc::Sender<Result<ScanOutput, ConnectorError>>,
    output: ScanOutput,
) -> Result<(), ConnectorError> {
    sender
        .send(Ok(output))
        .await
        .map_err(|_| ConnectorError::Closed)
}

fn scan_error(error: &iceberg::Error) -> ConnectorError {
    ConnectorError::ReadError(format!("Iceberg scan failed: {error}"))
}
