use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use futures_util::{StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskStream};
use iceberg::spec::{NameMapping, SchemaRef, SnapshotRef, DEFAULT_SCHEMA_NAME_MAPPING};
use iceberg::table::Table;
use tokio::sync::mpsc;

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::IcebergSourceConfig;
use crate::lakehouse::iceberg_scan::{
    bind_filter, connector_scan_error, plan_files, preflight_snapshot, ManifestReadLimits,
};

use super::append_lineage::{AddedDataFile, AppendSnapshotPlan};
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
    Added(Vec<AddedDataFile>),
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
    predicate: Option<Predicate>,
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
        IcebergSourceCursorV1::from_snapshot(config, &table, snapshot, read_schema.schema_id())?;
    Ok(spawn_scan(
        table,
        config,
        read_schema,
        predicate,
        vec![ScanPlan {
            snapshot_id,
            files: ScanFiles::All,
            cursor,
        }],
    ))
}

pub(super) fn append_task(
    table: Table,
    config: &IcebergSourceConfig,
    read_schema: &ReadSchemaBinding,
    predicate: Option<Predicate>,
    plans: Vec<AppendSnapshotPlan>,
) -> Result<Option<ScanTask>, ConnectorError> {
    if plans.is_empty() {
        return Ok(None);
    }
    let plans = plans
        .into_iter()
        .map(|plan| {
            let cursor = IcebergSourceCursorV1::from_snapshot(
                config,
                &table,
                &plan.snapshot,
                read_schema.schema_id(),
            )?;
            Ok(ScanPlan {
                snapshot_id: plan.snapshot.snapshot_id(),
                files: ScanFiles::Added(plan.added_files),
                cursor,
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    Ok(Some(spawn_scan(
        table,
        config,
        read_schema,
        predicate,
        plans,
    )))
}

fn spawn_scan(
    table: Table,
    config: &IcebergSourceConfig,
    read_schema: &ReadSchemaBinding,
    predicate: Option<Predicate>,
    plans: Vec<ScanPlan>,
) -> ScanTask {
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
    ScanTask {
        receiver,
        handle,
        started_at: Instant::now(),
    }
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
    let tasks = match files {
        ScanFiles::All => {
            full_snapshot_tasks(
                table,
                snapshot,
                &projection,
                predicate,
                concurrency,
                max_planned_files,
                metadata_limits,
                request_timeout,
            )
            .await?
        }
        ScanFiles::Added(files) => added_file_tasks(
            table,
            snapshot_schema,
            read_schema.field_ids(),
            predicate.as_ref(),
            files,
        )?,
    };
    read_tasks(
        table,
        tasks,
        cursor,
        projection,
        concurrency,
        request_timeout,
        sender,
    )
    .await
}

async fn full_snapshot_tasks(
    table: &Table,
    snapshot: &SnapshotRef,
    projection: &ReadProjection,
    predicate: Option<Predicate>,
    concurrency: usize,
    max_planned_files: usize,
    metadata_limits: ManifestReadLimits,
    request_timeout: std::time::Duration,
) -> Result<FileScanTaskStream, ConnectorError> {
    let planning_deadline = tokio::time::Instant::now() + request_timeout;
    preflight_snapshot(table, snapshot, metadata_limits, planning_deadline).await?;
    let mut builder = table
        .scan()
        .snapshot_id(snapshot.snapshot_id())
        .with_batch_size(Some(8_192))
        .with_concurrency_limit(concurrency);
    builder = builder.select(projection.columns.iter().map(String::as_str));
    if let Some(predicate) = predicate {
        builder = builder.with_filter(predicate);
    }
    let scan = builder.build().map_err(|error| {
        connector_scan_error(
            &format!(
                "[LDB-ICEBERG-SCAN-BUILD] snapshot {} scan build failed",
                snapshot.snapshot_id()
            ),
            &error,
        )
    })?;
    plan_files(&scan, max_planned_files, planning_deadline).await
}

fn added_file_tasks(
    table: &Table,
    snapshot_schema: SchemaRef,
    field_ids: &[i32],
    predicate: Option<&Predicate>,
    files: Vec<AddedDataFile>,
) -> Result<FileScanTaskStream, ConnectorError> {
    let bound_predicate = bind_filter(predicate, Arc::clone(&snapshot_schema))?;
    let name_mapping = table
        .metadata()
        .properties()
        .get(DEFAULT_SCHEMA_NAME_MAPPING)
        .map(|encoded| serde_json::from_str::<NameMapping>(encoded))
        .transpose()
        .map_err(|_| {
            ConnectorError::TransactionError(
                "[LDB-ICEBERG-NAME-MAPPING-INVALID] table name mapping is invalid".into(),
            )
        })?
        .map(Arc::new);
    for added in &files {
        if table
            .metadata()
            .partition_spec_by_id(added.partition_spec_id)
            .is_none()
        {
            return Err(
                ConnectorError::TransactionError(format!(
                    "[LDB-ICEBERG-PARTITION-SPEC-MISSING] data file references missing partition spec {}",
                    added.partition_spec_id
                )),
            );
        }
    }
    let field_ids = field_ids.to_vec();
    let tasks = files.into_iter().map(move |added| {
        let file = added.data_file;
        let size = file.file_size_in_bytes();
        Ok(FileScanTask::builder()
            .with_file_size_in_bytes(size)
            .with_start(0)
            .with_length(size)
            .with_record_count(Some(file.record_count()))
            .with_data_file_path(file.file_path().to_string())
            .with_data_file_format(file.file_format())
            .with_schema(Arc::clone(&snapshot_schema))
            .with_project_field_ids(field_ids.clone())
            .with_predicate(bound_predicate.clone())
            // INVARIANT: prior deletes cannot apply to later-sequence append files, and lineage
            // admission rejects every delete-file addition after the retained cursor.
            .with_deletes(Vec::new())
            .with_partition(Some(file.partition().clone()))
            // COMPAT: Iceberg 0.10.1 native scan tasks leave this unset; setting it changes the reader schema.
            .with_partition_spec(None)
            .with_name_mapping(name_mapping.clone())
            .with_case_sensitive(true)
            .build())
    });
    Ok(Box::pin(futures_util::stream::iter(tasks)))
}

async fn read_tasks(
    table: &Table,
    tasks: FileScanTaskStream,
    cursor: IcebergSourceCursorV1,
    projection: Arc<ReadProjection>,
    concurrency: usize,
    request_timeout: std::time::Duration,
    sender: &mpsc::Sender<Result<ScanOutput, ConnectorError>>,
) -> Result<(), ConnectorError> {
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
    projection: Arc<ReadProjection>,
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
