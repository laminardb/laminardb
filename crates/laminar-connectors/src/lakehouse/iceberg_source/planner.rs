use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
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
use crate::lakehouse::iceberg_scan::{
    connector_scan_error, plan_files, preflight_snapshot, ManifestReadLimits,
};

use super::append_lineage::AppendSnapshotPlan;
use super::cursor::IcebergSourceCursorV1;
use super::read_schema::{ReadProjection, ReadSchemaBinding};

pub(super) enum ScanOutput {
    Batch {
        batch: RecordBatch,
        completed_cursor: Option<IcebergSourceCursorV1>,
    },
    Cursor(IcebergSourceCursorV1),
    ReadMetrics {
        files: u64,
        storage_bytes: u64,
    },
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
    read_schema: &ReadSchemaBinding,
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
    let cursor =
        IcebergSourceCursorV1::from_snapshot(config, &table, snapshot, read_schema.schema_id());
    spawn_scan(
        table,
        config,
        read_schema,
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
    read_schema: &ReadSchemaBinding,
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
            cursor: IcebergSourceCursorV1::from_snapshot(
                config,
                &table,
                &plan.snapshot,
                read_schema.schema_id(),
            ),
        })
        .collect();
    spawn_scan(table, config, read_schema, plans).map(Some)
}

fn spawn_scan(
    table: Table,
    config: &IcebergSourceConfig,
    read_schema: &ReadSchemaBinding,
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
    let concurrency = config.scan_concurrency;
    let request_timeout = config.storage.request_timeout;
    let max_planned_files = config.max_planned_files;
    let metadata_limits = ManifestReadLimits::from_source(config);
    let read_schema = read_schema.clone();
    let (sender, receiver) = mpsc::channel(config.scan_channel_capacity);
    let handle = tokio::spawn(async move {
        for plan in plans {
            if let Err(error) = run_plan(
                &table,
                predicate.clone(),
                concurrency,
                request_timeout,
                max_planned_files,
                metadata_limits,
                plan,
                &read_schema,
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
    predicate: Option<Predicate>,
    concurrency: usize,
    request_timeout: std::time::Duration,
    max_planned_files: usize,
    metadata_limits: ManifestReadLimits,
    plan: ScanPlan,
    read_schema: &ReadSchemaBinding,
    sender: &mpsc::Sender<Result<ScanOutput, ConnectorError>>,
) -> Result<(), ConnectorError> {
    let ScanPlan {
        snapshot_id,
        files,
        cursor,
    } = plan;
    let snapshot = table
        .metadata()
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| {
            ConnectorError::ReadError(format!(
                "[LDB-ICEBERG-SNAPSHOT-MISSING] snapshot {snapshot_id} does not exist"
            ))
        })?;
    let snapshot_schema = snapshot.schema(table.metadata()).map_err(|error| {
        connector_scan_error(
            &format!("resolve Iceberg snapshot {snapshot_id} schema"),
            &error,
        )
    })?;
    let projection = read_schema.projection(&snapshot_schema)?;
    let planning_deadline = tokio::time::Instant::now() + request_timeout;
    preflight_snapshot(table, snapshot, metadata_limits, planning_deadline).await?;
    let mut builder = table
        .scan()
        .snapshot_id(snapshot_id)
        .with_batch_size(Some(8_192))
        .with_concurrency_limit(concurrency);
    builder = builder.select(projection.columns.iter().map(String::as_str));
    if let Some(predicate) = predicate {
        builder = builder.with_filter(predicate);
    }
    let scan = builder.build().map_err(|error| {
        connector_scan_error(
            &format!("[LDB-ICEBERG-SCAN-BUILD] snapshot {snapshot_id} scan build failed"),
            &error,
        )
    })?;
    let tasks = plan_files(&scan, max_planned_files, planning_deadline).await?;
    let tasks = match files {
        ScanFiles::All => tasks,
        ScanFiles::Added(paths) => Box::pin(
            tasks.try_filter(move |task| future::ready(paths.contains(task.data_file_path()))),
        ),
    };
    let read_files = Arc::new(AtomicU64::new(0));
    let read_file_counter = Arc::clone(&read_files);
    let counted_tasks: FileScanTaskStream = Box::pin(tasks.map_ok(move |task| {
        read_file_counter.fetch_add(1, Ordering::Relaxed);
        task
    }));
    let result = table
        .reader_builder()
        .with_batch_size(8_192)
        .with_data_file_concurrency_limit(concurrency)
        .build()
        .read(counted_tasks)
        .map_err(|error| connector_scan_error("Iceberg reader creation failed", &error))?;
    let scan_metrics = result.metrics().clone();
    send_stream(result.stream(), cursor, projection, sender, request_timeout).await?;
    send(
        sender,
        ScanOutput::ReadMetrics {
            files: read_files.load(Ordering::Relaxed),
            storage_bytes: scan_metrics.bytes_read(),
        },
    )
    .await
}

async fn send_stream(
    mut stream: ArrowRecordBatchStream,
    cursor: IcebergSourceCursorV1,
    projection: ReadProjection,
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
        let batch =
            result.map_err(|error| connector_scan_error("Iceberg data read failed", &error))?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch = projection.align(&batch)?;
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
