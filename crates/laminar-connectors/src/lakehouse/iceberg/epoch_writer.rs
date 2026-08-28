//! Bounded partition-aware Iceberg epoch writer.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::{DataFile, PartitionKey};
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, FileNameGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};

use crate::error::ConnectorError;

use super::super::iceberg_config::{IcebergSinkConfig, IcebergWriteDistributionMode};
use super::metrics::IcebergMetrics;

type PartitionWriter =
    DataFileWriter<ParquetWriterBuilder, DefaultLocationGenerator, DeterministicFileNameGenerator>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EpochIdentity {
    pub(super) deployment_id: String,
    pub(super) sink_id: String,
    pub(super) participant_id: u64,
    pub(super) epoch: u64,
}

#[derive(Debug)]
struct ActiveWriter {
    writer: PartitionWriter,
    opened_at: Instant,
    last_used: u64,
    has_written: bool,
}

/// Immutable output returned after every writer is closed.
#[derive(Debug)]
pub(super) struct EpochOutput {
    pub(super) data_files: Vec<DataFile>,
    pub(super) rows: u64,
    pub(super) bytes: u64,
}

#[derive(Debug, Clone)]
struct DeterministicFileNameGenerator {
    prefix: String,
    ordinal: Arc<AtomicU64>,
}

impl DeterministicFileNameGenerator {
    fn new(identity: &EpochIdentity) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"laminardb-iceberg-data-v1\0");
        digest.update(identity.deployment_id.as_bytes());
        digest.update([0]);
        digest.update(identity.sink_id.as_bytes());
        digest.update(identity.participant_id.to_be_bytes());
        digest.update(identity.epoch.to_be_bytes());
        Self {
            prefix: format!("ldb-{:x}", digest.finalize()),
            ordinal: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl FileNameGenerator for DeterministicFileNameGenerator {
    fn generate_file_name(&self) -> String {
        let ordinal = self.ordinal.fetch_add(1, Ordering::Relaxed);
        format!("{}-{ordinal:08}.parquet", self.prefix)
    }
}

/// Streams one checkpoint participant's rows into bounded rolling writers.
pub(super) struct IcebergEpochWriter {
    splitter: Option<RecordBatchPartitionSplitter>,
    parquet_builder: ParquetWriterBuilder,
    file_io: iceberg::io::FileIO,
    location_generator: DefaultLocationGenerator,
    file_name_generator: DeterministicFileNameGenerator,
    target_file_size_bytes: usize,
    max_open_partitions: usize,
    max_files: usize,
    max_buffer_rows: usize,
    max_buffer_bytes: usize,
    max_flush_age: std::time::Duration,
    distribution: IcebergWriteDistributionMode,
    active: HashMap<String, ActiveWriter>,
    clustered_current: Option<String>,
    clustered_closed: HashSet<String>,
    completed: Vec<DataFile>,
    file_count: usize,
    use_clock: u64,
    rows: u64,
    bytes: u64,
    metrics: IcebergMetrics,
}

impl IcebergEpochWriter {
    pub(super) fn new(
        table: &iceberg::table::Table,
        config: &IcebergSinkConfig,
        identity: &EpochIdentity,
        metrics: IcebergMetrics,
    ) -> Result<Self, ConnectorError> {
        config.validate_writer_limits()?;
        let schema = table.current_schema_ref();
        let partition_spec = Arc::clone(table.metadata().default_partition_spec());
        let splitter = if partition_spec.fields().is_empty() {
            None
        } else {
            Some(
                RecordBatchPartitionSplitter::try_new_with_computed_values(
                    Arc::clone(&schema),
                    partition_spec,
                )
                .map_err(|error| iceberg_write_error("create partition evaluator", &error))?,
            )
        };
        let row_group_rows = approximate_row_group_rows(config);
        let properties = WriterProperties::builder()
            .set_compression(super::parquet_compression(&config.compression)?)
            .set_max_row_group_row_count(Some(row_group_rows))
            .set_write_batch_size(row_group_rows.min(8_192))
            .build();
        let parquet_builder = ParquetWriterBuilder::new(properties, schema);
        let location_generator = DefaultLocationGenerator::new(table.metadata())
            .map_err(|error| iceberg_write_error("resolve table data location", &error))?;

        Ok(Self {
            splitter,
            parquet_builder,
            file_io: table.file_io().clone(),
            location_generator,
            file_name_generator: DeterministicFileNameGenerator::new(identity),
            target_file_size_bytes: config.target_file_size_bytes,
            max_open_partitions: config.max_open_partitions,
            max_files: config.max_files_per_checkpoint,
            max_buffer_rows: config.max_buffer_rows,
            max_buffer_bytes: config.max_buffer_bytes,
            max_flush_age: config.max_flush_age,
            distribution: config.distribution_mode,
            active: HashMap::with_capacity(config.max_open_partitions),
            clustered_current: None,
            clustered_closed: HashSet::new(),
            completed: Vec::new(),
            file_count: 0,
            use_clock: 0,
            rows: 0,
            bytes: 0,
            metrics,
        })
    }

    pub(super) async fn write(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch_bytes = batch.get_array_memory_size();
        if batch.num_rows() > self.max_buffer_rows {
            return Err(ConnectorError::WriteError(format!(
                "Iceberg batch has {} rows; max.buffer.rows is {}",
                batch.num_rows(),
                self.max_buffer_rows
            )));
        }
        if batch_bytes > self.max_buffer_bytes {
            return Err(ConnectorError::WriteError(format!(
                "Iceberg batch uses {batch_bytes} Arrow bytes; max.buffer.bytes is {}",
                self.max_buffer_bytes
            )));
        }

        self.metrics.set_buffer(batch.num_rows(), batch_bytes);
        let result = self.write_bounded(batch).await;
        self.metrics.set_buffer(0, 0);
        result
    }

    async fn write_bounded(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        let rows = u64::try_from(batch.num_rows()).map_err(|_| {
            ConnectorError::WriteError("Iceberg batch row count exceeds u64".into())
        })?;
        let bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            ConnectorError::WriteError("Iceberg batch byte count exceeds u64".into())
        })?;
        let mut partitions = match &self.splitter {
            Some(splitter) => splitter
                .split(&batch)
                .map_err(|error| iceberg_write_error("evaluate partition spec", &error))?
                .into_iter()
                .map(|(partition, batch)| (Some(partition), batch))
                .collect::<Vec<_>>(),
            None => vec![(None, batch)],
        };
        partitions.sort_by_key(|(partition, _)| partition_path(partition.as_ref()));

        for (partition, partition_batch) in partitions {
            self.write_partition(partition, partition_batch).await?;
        }
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| ConnectorError::WriteError("Iceberg epoch row count overflow".into()))?;
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(|| {
            ConnectorError::WriteError("Iceberg epoch byte count overflow".into())
        })?;
        Ok(())
    }

    async fn write_partition(
        &mut self,
        partition: Option<PartitionKey>,
        batch: RecordBatch,
    ) -> Result<(), ConnectorError> {
        let key = partition_path(partition.as_ref());
        self.prepare_partition(&key).await?;
        self.rotate_expired(&key, partition.as_ref()).await?;
        if !self.active.contains_key(&key) {
            self.open_partition(key.clone(), partition).await?;
        }

        self.use_clock = self.use_clock.saturating_add(1);
        let active = self.active.get_mut(&key).ok_or_else(|| {
            ConnectorError::Internal("Iceberg partition writer was not opened".into())
        })?;
        let starts_file = !active.has_written
            || active.writer.current_written_size() > self.target_file_size_bytes;
        if starts_file {
            let next = self.file_count.checked_add(1).ok_or_else(|| {
                ConnectorError::WriteError("Iceberg epoch file count overflow".into())
            })?;
            if next > self.max_files {
                return Err(ConnectorError::WriteError(format!(
                    "Iceberg checkpoint would exceed max.files.per.checkpoint ({})",
                    self.max_files
                )));
            }
            self.file_count = next;
        }
        active.last_used = self.use_clock;
        active
            .writer
            .write(batch)
            .await
            .map_err(|error| iceberg_write_error("write rolling Parquet data", &error))?;
        active.has_written = true;
        Ok(())
    }

    async fn prepare_partition(&mut self, key: &str) -> Result<(), ConnectorError> {
        match self.distribution {
            IcebergWriteDistributionMode::Clustered => {
                if self.clustered_current.as_deref() == Some(key) {
                    return Ok(());
                }
                if self.clustered_closed.contains(key) {
                    return Err(ConnectorError::WriteError(format!(
                        "clustered Iceberg input returned to closed partition '{key}'"
                    )));
                }
                if let Some(previous) = self.clustered_current.take() {
                    self.close_partition(&previous).await?;
                    self.clustered_closed.insert(previous);
                }
                self.clustered_current = Some(key.to_string());
            }
            IcebergWriteDistributionMode::Fanout => {
                if !self.active.contains_key(key) && self.active.len() >= self.max_open_partitions {
                    let evicted = self.least_recently_used_partition().ok_or_else(|| {
                        ConnectorError::Internal(
                            "Iceberg fanout writer limit reached without an active writer".into(),
                        )
                    })?;
                    self.close_partition(&evicted).await?;
                }
            }
        }
        Ok(())
    }

    async fn rotate_expired(
        &mut self,
        key: &str,
        partition: Option<&PartitionKey>,
    ) -> Result<(), ConnectorError> {
        let expired = self.active.get(key).is_some_and(|active| {
            active.has_written && active.opened_at.elapsed() >= self.max_flush_age
        });
        if expired {
            self.close_partition(key).await?;
            self.open_partition(key.to_string(), partition.cloned())
                .await?;
        }
        Ok(())
    }

    async fn open_partition(
        &mut self,
        key: String,
        partition: Option<PartitionKey>,
    ) -> Result<(), ConnectorError> {
        let rolling = RollingFileWriterBuilder::new(
            self.parquet_builder.clone(),
            self.target_file_size_bytes,
            self.file_io.clone(),
            self.location_generator.clone(),
            self.file_name_generator.clone(),
        );
        let writer = DataFileWriterBuilder::new(rolling)
            .build(partition)
            .await
            .map_err(|error| iceberg_write_error("open partition writer", &error))?;
        self.active.insert(
            key,
            ActiveWriter {
                writer,
                opened_at: Instant::now(),
                last_used: self.use_clock,
                has_written: false,
            },
        );
        self.metrics.set_active_writers(self.active.len());
        Ok(())
    }

    fn least_recently_used_partition(&self) -> Option<String> {
        self.active
            .iter()
            .min_by(|(left_key, left), (right_key, right)| {
                left.last_used
                    .cmp(&right.last_used)
                    .then_with(|| left_key.cmp(right_key))
            })
            .map(|(key, _)| key.clone())
    }

    async fn close_partition(&mut self, key: &str) -> Result<(), ConnectorError> {
        let Some(mut active) = self.active.remove(key) else {
            return Ok(());
        };
        #[cfg(test)]
        super::fault_injection::fail_if(
            super::fault_injection::IcebergFaultPoint::BeforeFileClose,
        )?;
        let files = active
            .writer
            .close()
            .await
            .map_err(|error| iceberg_write_error("close partition writer", &error))?;
        self.metrics
            .observe_files(&files, self.target_file_size_bytes);
        self.completed.extend(files);
        self.metrics.set_active_writers(self.active.len());
        Ok(())
    }

    pub(super) async fn close(mut self) -> Result<EpochOutput, ConnectorError> {
        let mut keys = self.active.keys().cloned().collect::<Vec<_>>();
        keys.sort_unstable();
        for key in keys {
            self.close_partition(&key).await?;
        }
        if self.completed.len() != self.file_count || self.completed.len() > self.max_files {
            return Err(ConnectorError::Internal(format!(
                "Iceberg file accounting mismatch: predicted {}, completed {}",
                self.file_count,
                self.completed.len()
            )));
        }
        Ok(EpochOutput {
            data_files: self.completed,
            rows: self.rows,
            bytes: self.bytes,
        })
    }
}

fn partition_path(partition: Option<&PartitionKey>) -> String {
    partition.map(PartitionKey::to_path).unwrap_or_default()
}

fn approximate_row_group_rows(config: &IcebergSinkConfig) -> usize {
    let per_writer_budget = config
        .max_buffer_bytes
        .checked_div(config.max_open_partitions)
        .unwrap_or(1)
        .max(1);
    let row_group_bytes = config
        .parquet_row_group_size_bytes
        .min(config.target_file_size_bytes)
        .min(per_writer_budget);
    config
        .max_buffer_rows
        .saturating_mul(row_group_bytes)
        .checked_div(config.max_buffer_bytes)
        .unwrap_or(config.max_buffer_rows)
        .clamp(1, config.max_buffer_rows)
}

fn iceberg_write_error(context: &str, error: &iceberg::Error) -> ConnectorError {
    ConnectorError::WriteError(format!("Iceberg {context}: {error}"))
}

#[cfg(test)]
mod tests {
    use crate::lakehouse::iceberg::test_support::{batch, create_test_table};

    use super::*;

    fn identity(epoch: u64) -> EpochIdentity {
        EpochIdentity {
            deployment_id: "018f0000-0000-7000-8000-000000000001".into(),
            sink_id: "orders".into(),
            participant_id: 7,
            epoch,
        }
    }

    #[test]
    fn deterministic_names_are_replay_stable_and_epoch_scoped() {
        let first = DeterministicFileNameGenerator::new(&identity(11));
        let replay = DeterministicFileNameGenerator::new(&identity(11));
        assert_eq!(first.generate_file_name(), replay.generate_file_name());
        assert_eq!(first.generate_file_name(), replay.generate_file_name());
        assert_ne!(
            DeterministicFileNameGenerator::new(&identity(11)).generate_file_name(),
            DeterministicFileNameGenerator::new(&identity(12)).generate_file_name()
        );
    }

    #[tokio::test]
    async fn fanout_bounds_open_partition_writers() {
        let fixture = create_test_table(true).await;
        let mut config = fixture.config;
        config.max_open_partitions = 2;
        config.max_files_per_checkpoint = 16;
        let mut writer = IcebergEpochWriter::new(
            &fixture.table,
            &config,
            &identity(11),
            IcebergMetrics::new(None),
        )
        .unwrap();
        writer
            .write(batch(
                &fixture.table,
                &[
                    (1, Some("e")),
                    (2, Some("d")),
                    (3, Some("c")),
                    (4, Some("b")),
                    (5, Some("a")),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(writer.active.len(), 2);
        assert_eq!(writer.completed.len(), 3);

        let output = writer.close().await.unwrap();
        assert_eq!(output.data_files.len(), 5);
        assert_eq!(output.rows, 5);
        let paths = output
            .data_files
            .iter()
            .map(iceberg::spec::DataFile::file_path)
            .collect::<Vec<_>>();
        assert!(paths.iter().any(|path| path.contains("category=a")));
        assert!(paths.iter().any(|path| path.contains("category=e")));
    }

    #[tokio::test]
    async fn age_rotation_and_file_limit_fail_before_extra_rows_are_written() {
        let fixture = create_test_table(false).await;
        let mut config = fixture.config;
        config.max_flush_age = std::time::Duration::ZERO;
        config.max_files_per_checkpoint = 1;
        let mut writer = IcebergEpochWriter::new(
            &fixture.table,
            &config,
            &identity(12),
            IcebergMetrics::new(None),
        )
        .unwrap();
        writer
            .write(batch(&fixture.table, &[(1, Some("a"))]))
            .await
            .unwrap();
        let error = writer
            .write(batch(&fixture.table, &[(2, Some("a"))]))
            .await
            .expect_err("second file must exceed the checkpoint bound");
        assert!(error.to_string().contains("max.files.per.checkpoint"));
        assert_eq!(writer.rows, 1);
    }

    #[tokio::test]
    async fn deterministic_paths_survive_participant_replay() {
        let fixture = create_test_table(false).await;
        let input = batch(&fixture.table, &[(1, Some("a")), (2, Some("b")), (3, None)]);
        let write = || {
            IcebergEpochWriter::new(
                &fixture.table,
                &fixture.config,
                &identity(19),
                IcebergMetrics::new(None),
            )
        };
        let mut first = write().unwrap();
        first.write(input.clone()).await.unwrap();
        let first = first.close().await.unwrap();
        let mut replay = write().unwrap();
        replay.write(input).await.unwrap();
        let replay = replay.close().await.unwrap();
        assert_eq!(
            first
                .data_files
                .iter()
                .map(DataFile::file_path)
                .collect::<Vec<_>>(),
            replay
                .data_files
                .iter()
                .map(DataFile::file_path)
                .collect::<Vec<_>>()
        );
        assert_eq!(first.rows, replay.rows);
        assert_eq!(first.data_files, replay.data_files);
    }

    #[tokio::test]
    async fn batch_row_and_byte_limits_reject_before_opening_a_writer() {
        let fixture = create_test_table(false).await;
        let two_rows = batch(&fixture.table, &[(1, Some("a")), (2, Some("b"))]);
        let mut row_config = fixture.config.clone();
        row_config.max_buffer_rows = 1;
        let mut row_writer = IcebergEpochWriter::new(
            &fixture.table,
            &row_config,
            &identity(20),
            IcebergMetrics::new(None),
        )
        .unwrap();
        let row_error = row_writer
            .write(two_rows)
            .await
            .expect_err("oversized row batch must fail");
        assert!(row_error.to_string().contains("max.buffer.rows"));
        assert!(row_writer.active.is_empty());
        assert_eq!(row_writer.rows, 0);

        let one_row = batch(&fixture.table, &[(1, Some("payload"))]);
        let mut byte_config = fixture.config;
        byte_config.max_buffer_bytes = one_row.get_array_memory_size() - 1;
        let mut byte_writer = IcebergEpochWriter::new(
            &fixture.table,
            &byte_config,
            &identity(21),
            IcebergMetrics::new(None),
        )
        .unwrap();
        let byte_error = byte_writer
            .write(one_row)
            .await
            .expect_err("oversized byte batch must fail");
        assert!(byte_error.to_string().contains("max.buffer.bytes"));
        assert!(byte_writer.active.is_empty());
        assert_eq!(byte_writer.bytes, 0);
    }

    #[tokio::test]
    async fn high_partition_cardinality_stays_within_writer_and_file_bounds() {
        let fixture = create_test_table(true).await;
        let mut config = fixture.config;
        config.max_open_partitions = 4;
        config.max_files_per_checkpoint = 128;
        let mut writer = IcebergEpochWriter::new(
            &fixture.table,
            &config,
            &identity(22),
            IcebergMetrics::new(None),
        )
        .unwrap();

        for id in 0_i64..128 {
            let category = format!("partition-{id:03}");
            writer
                .write(batch(&fixture.table, &[(id, Some(&category))]))
                .await
                .unwrap();
            assert!(writer.active.len() <= config.max_open_partitions);
            assert!(writer.completed.len() <= config.max_files_per_checkpoint);
        }
        let extra = writer
            .write(batch(&fixture.table, &[(128, Some("partition-128"))]))
            .await
            .expect_err("the next partition must exceed the file bound");
        assert!(extra.to_string().contains("max.files.per.checkpoint"));
        assert!(writer.active.len() <= config.max_open_partitions);
        assert!(writer.completed.len() <= config.max_files_per_checkpoint);

        let output = writer.close().await.unwrap();
        assert_eq!(output.data_files.len(), 128);
        assert_eq!(output.rows, 128);
    }
}
