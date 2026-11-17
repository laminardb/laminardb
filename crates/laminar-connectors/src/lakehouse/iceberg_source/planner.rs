use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use futures_util::{StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskStream};
use iceberg::spec::{NameMapping, SchemaRef, SnapshotRef, TableProperties};
use iceberg::table::Table;
use tokio::sync::mpsc;

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::IcebergSourceConfig;
use crate::lakehouse::iceberg_scan::{
    bind_filter, connector_scan_error, plan_files, preflight_snapshot, ManifestReadLimits,
};

use super::append_lineage::{AddedDataFile, AppendSnapshotPlan, SplitAppendPlan};
use super::cursor::IcebergSourceCursorV1;
use super::read_schema::{ReadProjection, ReadSchemaBinding};
use super::split::{data_file_channel, ChannelPosition, SnapshotPosition};

/// The position a replay unit makes durable once all of its rows are emitted.
pub(super) enum Completion {
    /// A whole snapshot, for an unsplit source.
    Snapshot(IcebergSourceCursorV1),
    /// One channel's files of a snapshot.
    Channel {
        channel: u32,
        position: ChannelPosition,
    },
}

pub(super) enum ScanOutput {
    Batch {
        batch: RecordBatch,
        completed: Option<Completion>,
    },
    /// A replay unit without rows.
    Completed(Completion),
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
    completion: Completion,
}

enum ScanStep {
    Unit(ScanPlan),
    /// Plan a whole snapshot once and read each listed channel's files as its own replay unit.
    SplitBootstrap {
        snapshot: SnapshotPosition,
        splits: NonZeroU32,
        channels: Vec<u32>,
    },
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
        vec![ScanStep::Unit(ScanPlan {
            snapshot_id,
            files: ScanFiles::All,
            completion: Completion::Snapshot(cursor),
        })],
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
            Ok(ScanStep::Unit(ScanPlan {
                snapshot_id: plan.snapshot.snapshot_id(),
                files: ScanFiles::Added(plan.added_files),
                completion: Completion::Snapshot(cursor),
            }))
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

/// Read the files of `channels` in the whole `snapshot`, one replay unit per channel.
pub(super) fn split_bootstrap_task(
    table: Table,
    config: &IcebergSourceConfig,
    read_schema: &ReadSchemaBinding,
    predicate: Option<Predicate>,
    snapshot: SnapshotPosition,
    splits: NonZeroU32,
    channels: Vec<u32>,
) -> ScanTask {
    spawn_scan(
        table,
        config,
        read_schema,
        predicate,
        vec![ScanStep::SplitBootstrap {
            snapshot,
            splits,
            channels,
        }],
    )
}

/// Read each channel's added files of each snapshot as its own replay unit, in lineage order.
pub(super) fn split_append_task(
    table: Table,
    config: &IcebergSourceConfig,
    read_schema: &ReadSchemaBinding,
    predicate: Option<Predicate>,
    plans: Vec<SplitAppendPlan>,
) -> Option<ScanTask> {
    let steps: Vec<ScanStep> = plans
        .into_iter()
        .flat_map(|plan| {
            let snapshot = SnapshotPosition {
                snapshot_id: plan.snapshot.snapshot_id(),
                sequence_number: plan.snapshot.sequence_number(),
            };
            plan.channels.into_iter().map(move |(channel, files)| {
                ScanStep::Unit(ScanPlan {
                    snapshot_id: snapshot.snapshot_id,
                    files: ScanFiles::Added(files),
                    completion: Completion::Channel {
                        channel,
                        position: ChannelPosition::After(snapshot),
                    },
                })
            })
        })
        .collect();
    if steps.is_empty() {
        return None;
    }
    Some(spawn_scan(table, config, read_schema, predicate, steps))
}

fn spawn_scan(
    table: Table,
    config: &IcebergSourceConfig,
    read_schema: &ReadSchemaBinding,
    predicate: Option<Predicate>,
    steps: Vec<ScanStep>,
) -> ScanTask {
    let concurrency = config.scan_concurrency;
    let request_timeout = config.storage.request_timeout;
    let max_planned_files = config.max_planned_files;
    let metadata_limits = ManifestReadLimits::from_source(config);
    let read_schema = read_schema.clone();
    let (sender, receiver) = mpsc::channel(config.scan_channel_capacity);
    let handle = tokio::spawn(async move {
        let reader = ScanReader {
            table: &table,
            concurrency,
            request_timeout,
            max_planned_files,
            metadata_limits,
            read_schema: &read_schema,
            sender: &sender,
        };
        for step in steps {
            let result = match step {
                ScanStep::Unit(plan) => reader.run_plan(predicate.clone(), plan).await,
                ScanStep::SplitBootstrap {
                    snapshot,
                    splits,
                    channels,
                } => {
                    reader
                        .run_split_bootstrap(predicate.clone(), snapshot, splits, channels)
                        .await
                }
            };
            if let Err(error) = result {
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

/// What every step of one scan task reads with.
struct ScanReader<'a> {
    table: &'a Table,
    concurrency: usize,
    request_timeout: std::time::Duration,
    max_planned_files: usize,
    metadata_limits: ManifestReadLimits,
    read_schema: &'a ReadSchemaBinding,
    sender: &'a mpsc::Sender<Result<ScanOutput, ConnectorError>>,
}

struct ResolvedSnapshot<'a> {
    snapshot: &'a SnapshotRef,
    schema: SchemaRef,
    projection: Arc<ReadProjection>,
}

impl ScanReader<'_> {
    fn resolve_snapshot(&self, snapshot_id: i64) -> Result<ResolvedSnapshot<'_>, ConnectorError> {
        let snapshot = self
            .table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                ConnectorError::ReadError(format!(
                    "[LDB-ICEBERG-SNAPSHOT-MISSING] snapshot {snapshot_id} does not exist"
                ))
            })?;
        let snapshot_schema = snapshot.schema(self.table.metadata()).map_err(|error| {
            connector_scan_error(
                &format!("resolve Iceberg snapshot {snapshot_id} schema"),
                &error,
            )
        })?;
        let projection = self.read_schema.projection(&snapshot_schema)?;
        Ok(ResolvedSnapshot {
            snapshot,
            schema: snapshot_schema,
            projection,
        })
    }

    async fn run_plan(
        &self,
        predicate: Option<Predicate>,
        plan: ScanPlan,
    ) -> Result<(), ConnectorError> {
        let ScanPlan {
            snapshot_id,
            files,
            completion,
        } = plan;
        let resolved = self.resolve_snapshot(snapshot_id)?;
        let tasks = match files {
            ScanFiles::All => {
                full_snapshot_tasks(
                    self.table,
                    resolved.snapshot,
                    &resolved.projection,
                    predicate,
                    self.concurrency,
                    self.max_planned_files,
                    self.metadata_limits,
                    self.request_timeout,
                )
                .await?
            }
            ScanFiles::Added(files) if files.is_empty() => {
                return send(self.sender, ScanOutput::Completed(completion)).await;
            }
            ScanFiles::Added(files) => added_file_tasks(
                self.table,
                resolved.schema,
                self.read_schema.field_ids(),
                predicate.as_ref(),
                files,
            )?,
        };
        self.read(tasks, completion, resolved.projection).await
    }

    /// Plan the whole snapshot once, then read each channel's files as its own replay unit.
    async fn run_split_bootstrap(
        &self,
        predicate: Option<Predicate>,
        snapshot: SnapshotPosition,
        splits: NonZeroU32,
        channels: Vec<u32>,
    ) -> Result<(), ConnectorError> {
        let resolved = self.resolve_snapshot(snapshot.snapshot_id)?;
        let planned = full_snapshot_tasks(
            self.table,
            resolved.snapshot,
            &resolved.projection,
            predicate,
            self.concurrency,
            self.max_planned_files,
            self.metadata_limits,
            self.request_timeout,
        )
        .await?;
        let planned: Vec<FileScanTask> =
            tokio::time::timeout(self.request_timeout, planned.try_collect())
                .await
                .map_err(|_| scan_timeout(self.request_timeout))?
                .map_err(|error| connector_scan_error("Iceberg scan planning failed", &error))?;
        let mut by_channel: BTreeMap<u32, Vec<FileScanTask>> = channels
            .into_iter()
            .map(|channel| (channel, Vec::new()))
            .collect();
        for task in planned {
            if let Some(tasks) =
                by_channel.get_mut(&data_file_channel(task.data_file_path(), splits))
            {
                tasks.push(task);
            }
        }
        for (channel, tasks) in by_channel {
            let completion = Completion::Channel {
                channel,
                position: ChannelPosition::After(snapshot),
            };
            if tasks.is_empty() {
                send(self.sender, ScanOutput::Completed(completion)).await?;
                continue;
            }
            let tasks: FileScanTaskStream =
                Box::pin(futures_util::stream::iter(tasks.into_iter().map(Ok)));
            self.read(tasks, completion, Arc::clone(&resolved.projection))
                .await?;
        }
        Ok(())
    }

    async fn read(
        &self,
        tasks: FileScanTaskStream,
        completion: Completion,
        projection: Arc<ReadProjection>,
    ) -> Result<(), ConnectorError> {
        let read_files = Arc::new(AtomicU64::new(0));
        let read_file_counter = Arc::clone(&read_files);
        let counted_tasks: FileScanTaskStream = Box::pin(tasks.map_ok(move |task| {
            read_file_counter.fetch_add(1, Ordering::Relaxed);
            task
        }));
        let result = self
            .table
            .reader_builder()
            .with_batch_size(8_192)
            .with_data_file_concurrency_limit(self.concurrency)
            .build()
            .read(counted_tasks)
            .map_err(|error| connector_scan_error("Iceberg reader creation failed", &error))?;
        let scan_metrics = result.metrics().clone();
        send_stream(
            result.stream(),
            completion,
            projection,
            self.sender,
            self.request_timeout,
        )
        .await?;
        send(
            self.sender,
            ScanOutput::ReadMetrics {
                files: read_files.load(Ordering::Relaxed),
                storage_bytes: scan_metrics.bytes_read(),
            },
        )
        .await
    }
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
    let planning_deadline =
        crate::lakehouse::iceberg_io::checked_deadline(request_timeout, "storage.request_timeout")?;
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
        .get(TableProperties::PROPERTY_DEFAULT_NAME_MAPPING)
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
    let metadata = table.metadata_ref();
    let tasks = files.into_iter().map(move |added| {
        let file = added.data_file;
        let size = file.file_size_in_bytes();
        FileScanTask::builder()
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
            .with_partition_spec(
                metadata
                    .partition_spec_by_id(added.partition_spec_id)
                    .cloned(),
            )
            .with_name_mapping(name_mapping.clone())
            .with_case_sensitive(true)
            .build()
    });
    Ok(Box::pin(futures_util::stream::iter(tasks)))
}

async fn send_stream(
    mut stream: ArrowRecordBatchStream,
    completion: Completion,
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
                    completed: None,
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
                completed: Some(completion),
            },
        )
        .await
    } else {
        send(sender, ScanOutput::Completed(completion)).await
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
