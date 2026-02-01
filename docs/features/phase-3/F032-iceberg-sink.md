# F032: Apache Iceberg Sink Connector

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P1 (Second lakehouse format after Delta Lake)
**Estimated Complexity:** Large (1-2 weeks)
**Prerequisites:** F023 (Exactly-Once Sinks), F034 (Connector SDK), F063 (Changelog/Retraction)

---

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F032 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F023 (Exactly-Once Sinks), F034 (Connector SDK), F063 (Changelog/Retraction) |
| **Blocks** | F061 (Historical Backfill) |
| **Owner** | TBD |
| **Crate** | `laminar-connectors` (feature: `iceberg-sink`) |
| **Created** | 2026-02-01 |

---

## Executive Summary

This specification defines the Apache Iceberg Sink Connector for LaminarDB. The connector writes streaming Arrow `RecordBatch` data into Apache Iceberg tables with snapshot isolation, hidden partitioning, and exactly-once delivery semantics. It integrates with Iceberg catalogs (REST, AWS Glue, Hive Metastore) for metadata management, uses the `iceberg-rust` crate for native Rust Iceberg operations, and implements the `SinkConnector` trait from F034 with epoch-based commits.

Key differentiators from the Delta Lake Sink (F031):
- **Catalog-native**: Iceberg uses a centralized catalog (REST, Glue, Hive) for metadata rather than a file-based transaction log.
- **Hidden partitioning**: Iceberg partition transforms (days, hours, bucket, truncate) are invisible to queries — no user-facing partition columns needed.
- **Schema evolution**: Iceberg supports full schema evolution (add, drop, rename, reorder columns, type promotion) tracked by schema IDs.
- **Multi-engine reads**: Iceberg tables are readable by Spark, Trino, Presto, Flink, Dremio, Snowflake, and BigQuery without format-specific connectors.

The Iceberg Sink runs entirely in Ring 1 (background I/O), with zero impact on Ring 0 hot path latency.

---

## 1. Problem Statement

### 1.1 Current Limitation

LaminarDB's streaming engine processes events at sub-microsecond latency through its three-ring architecture, but there is no way to persist streaming results to Apache Iceberg tables. The F031 Delta Lake Sink addresses lakehouse writes for Delta Lake, but many production environments standardize on Apache Iceberg as their table format — especially multi-engine analytics platforms using Trino, Spark, and Snowflake.

Without an Iceberg Sink, users cannot:

- Write streaming results to Iceberg tables for multi-engine analytics
- Leverage Iceberg's catalog ecosystem (Nessie, Polaris, AWS Glue, Tabular)
- Use hidden partitioning for automatic partition evolution
- Build pipelines readable by Snowflake, BigQuery, and other cloud warehouses

### 1.2 Required Capability

An Apache Iceberg Sink Connector that:

1. Writes Arrow RecordBatch data to Iceberg tables via the `iceberg-rust` crate
2. Integrates with Iceberg catalogs (REST, AWS Glue, Hive Metastore) for metadata
3. Supports hidden partitioning with Iceberg transforms (identity, bucket, truncate, year/month/day/hour)
4. Provides exactly-once delivery via epoch-to-snapshot mapping
5. Handles schema evolution (additive columns, type promotion)
6. Supports CDC/upsert mode via Iceberg's equality delete files or row-level merge
7. Runs background compaction (rewrite data files, expire snapshots) without blocking ingestion
8. Works with S3, Azure ADLS, GCS, and local filesystem via `object_store`

### 1.3 Use Case Diagram

```
+------------------------------------------------------------------+
|                    Iceberg Sink Architecture                       |
|                                                                    |
|  LaminarDB Pipeline          Iceberg Sink            Storage       |
|  +------------------+     +---------------------+                  |
|  |  Source (Kafka)   |     |                     |     +---------+ |
|  |       |           |     | RecordBatchWriter   |---->| Parquet | |
|  |  [Operators]      |     |  (Arrow zero-copy)  |     | Files   | |
|  |       |           |     |                     |     +---------+ |
|  |  Subscription     |---->| EpochTracker        |                  |
|  |  (Ring 0 -> 1)    |     |  (epoch -> snapshot)|     +---------+ |
|  +------------------+     |                     |---->| Catalog  | |
|                            | PartitionWriter    |     | (REST/   | |
|  Ring 0 (Hot Path)         |  (hidden partitions)|     | Glue/    | |
|  <500ns, no I/O            |                     |     | Hive)    | |
|                            | MaintenanceManager |     +---------+ |
|  Ring 1 (Background)       |  (compaction,       |                  |
|  Async I/O, Iceberg writes |   snapshot expire)  |     +---------+ |
|                            +---------------------+     | Cloud    | |
|  Ring 2 (Control Plane)                                | Storage  | |
|  Schema management, config                             | S3/ADLS  | |
|                                                         | GCS/Local| |
+------------------------------------------------------------------+
```

---

## 2. Design Principles

### 2.1 Core Principles (Aligned with LaminarDB Philosophy)

| Principle | Application |
|-----------|-------------|
| **Three-ring separation** | All Iceberg I/O runs in Ring 1; catalog queries and schema management in Ring 2; Ring 0 untouched |
| **Zero-allocation hot path** | Sink receives data via SPSC channel subscription; no allocations in Ring 0 |
| **Arrow-native** | RecordBatch passes directly to Iceberg's Parquet writer; zero serialization overhead |
| **Exactly-once via epochs** | Epoch-to-Iceberg-snapshot mapping with summary properties for dedup |
| **SQL-first configuration** | `CREATE SINK TABLE ... WITH (connector = 'iceberg', ...)` DDL |
| **Catalog-centric** | Always goes through an Iceberg catalog — no bare file writes |

### 2.2 Industry-Informed Patterns

| Pattern | Source | LaminarDB Adaptation |
|---------|--------|---------------------|
| **Iceberg v2 spec** | Apache Iceberg project | Row-level deletes, equality deletes for CDC |
| **REST Catalog protocol** | Iceberg REST OpenAPI spec | Standard catalog access via HTTP |
| **Hidden partitioning** | Iceberg design philosophy | Partition transforms in table metadata, invisible to queries |
| **Snapshot isolation** | Iceberg spec | Each commit produces an immutable snapshot |
| **Exactly-once writes** | Flink Iceberg Connector | Epoch tracking via snapshot summary properties |
| **iceberg-rust** | Apache iceberg-rust 0.4+ | Native Rust Iceberg client with DataFusion integration |

---

## 3. Architecture

### 3.1 Core Data Structures

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::connector::{
    ConnectorConfig, ConnectorError, SinkConnector, SinkConnectorCapabilities, WriteResult,
};
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

/// Configuration for the Iceberg sink connector.
#[derive(Debug, Clone)]
pub struct IcebergSinkConfig {
    /// Iceberg catalog type.
    pub catalog_type: IcebergCatalogType,
    /// Iceberg catalog URI.
    pub catalog_uri: String,
    /// Warehouse location (e.g., s3://bucket/warehouse).
    pub warehouse: String,
    /// Namespace for the table (e.g., ["analytics", "prod"]).
    pub namespace: Vec<String>,
    /// Table name within the namespace.
    pub table_name: String,
    /// Partition specification using Iceberg transforms.
    /// Empty = unpartitioned table.
    pub partition_spec: Vec<IcebergPartitionField>,
    /// Target Parquet data file size in bytes (default: 128 MB).
    pub target_file_size: usize,
    /// Maximum number of records to buffer before flushing to Parquet.
    pub max_buffer_records: usize,
    /// Maximum time to buffer records before flushing.
    pub max_buffer_duration: Duration,
    /// Whether to enable schema evolution (auto-merge new columns).
    pub schema_evolution: bool,
    /// Write mode: Append or Upsert (equality delete + insert).
    pub write_mode: IcebergWriteMode,
    /// Key columns for upsert/merge operations (required for Upsert mode).
    /// These define the equality delete fields.
    pub equality_delete_columns: Vec<String>,
    /// Delivery guarantee: AtLeastOnce or ExactlyOnce.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Writer ID for exactly-once deduplication.
    pub writer_id: String,
    /// Catalog-specific properties (e.g., Glue region, auth tokens).
    pub catalog_properties: HashMap<String, String>,
    /// Storage-specific properties (S3 credentials, etc.).
    pub storage_properties: HashMap<String, String>,
    /// Maintenance configuration (compaction, snapshot expiry).
    pub maintenance: MaintenanceConfig,
    /// Sort order columns for data files (optional).
    pub sort_order: Vec<SortField>,
    /// File format for data files.
    pub file_format: IcebergFileFormat,
}

impl Default for IcebergSinkConfig {
    fn default() -> Self {
        Self {
            catalog_type: IcebergCatalogType::Rest,
            catalog_uri: String::new(),
            warehouse: String::new(),
            namespace: Vec::new(),
            table_name: String::new(),
            partition_spec: Vec::new(),
            target_file_size: 128 * 1024 * 1024, // 128 MB
            max_buffer_records: 100_000,
            max_buffer_duration: Duration::from_secs(60),
            schema_evolution: false,
            write_mode: IcebergWriteMode::Append,
            equality_delete_columns: Vec::new(),
            delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
            writer_id: uuid::Uuid::new_v4().to_string(),
            catalog_properties: HashMap::new(),
            storage_properties: HashMap::new(),
            maintenance: MaintenanceConfig::default(),
            sort_order: Vec::new(),
            file_format: IcebergFileFormat::Parquet,
        }
    }
}

/// Iceberg catalog type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergCatalogType {
    /// REST catalog (Iceberg REST OpenAPI spec).
    /// Compatible with Polaris, Tabular, Nessie REST, Unity Catalog.
    Rest,
    /// AWS Glue Data Catalog.
    Glue,
    /// Hive Metastore (Thrift).
    Hive,
    /// In-memory catalog (testing only).
    Memory,
}

/// Iceberg write mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergWriteMode {
    /// Append-only: all records are inserts. Most efficient mode.
    Append,
    /// Upsert: equality delete + insert for CDC/changelog streams.
    /// Requires `equality_delete_columns` to be set.
    /// Uses Iceberg v2 equality delete files for row-level operations.
    Upsert,
}

/// Delivery guarantee level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// May produce duplicates on recovery. Simpler, slightly higher throughput.
    AtLeastOnce,
    /// No duplicates. Uses epoch-to-snapshot mapping via summary properties.
    ExactlyOnce,
}

/// Iceberg partition field with transform.
#[derive(Debug, Clone)]
pub struct IcebergPartitionField {
    /// Source column name.
    pub source_column: String,
    /// Partition transform to apply.
    pub transform: IcebergTransform,
    /// Optional partition field name (defaults to transform_source).
    pub name: Option<String>,
}

/// Iceberg partition transforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergTransform {
    /// Identity transform: partition by raw column value.
    Identity,
    /// Bucket hash transform: distribute into N buckets.
    Bucket(u32),
    /// Truncate transform: truncate to width W.
    Truncate(u32),
    /// Year transform: extract year from timestamp/date.
    Year,
    /// Month transform: extract year-month from timestamp/date.
    Month,
    /// Day transform: extract date from timestamp.
    Day,
    /// Hour transform: extract date-hour from timestamp.
    Hour,
}

/// Sort field for data file ordering.
#[derive(Debug, Clone)]
pub struct SortField {
    /// Column name.
    pub column: String,
    /// Sort direction.
    pub direction: SortDirection,
    /// Null ordering.
    pub null_order: NullOrder,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Null ordering in sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrder {
    NullsFirst,
    NullsLast,
}

/// Iceberg data file format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergFileFormat {
    /// Apache Parquet (recommended, default).
    Parquet,
    /// Apache ORC.
    Orc,
    /// Apache Avro.
    Avro,
}

/// Configuration for background table maintenance.
#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// Whether to run automatic compaction (rewrite data files).
    pub compaction_enabled: bool,
    /// Minimum number of small files before triggering compaction.
    pub compaction_min_files: usize,
    /// Target file size after compaction.
    pub compaction_target_file_size: usize,
    /// Whether to expire old snapshots automatically.
    pub expire_snapshots_enabled: bool,
    /// Retention period for snapshots before expiry.
    pub snapshot_retention: Duration,
    /// Maximum number of snapshots to retain.
    pub max_snapshots: usize,
    /// How often to check if maintenance is needed.
    pub check_interval: Duration,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            compaction_enabled: true,
            compaction_min_files: 10,
            compaction_target_file_size: 128 * 1024 * 1024, // 128 MB
            expire_snapshots_enabled: true,
            snapshot_retention: Duration::from_secs(7 * 24 * 3600), // 7 days
            max_snapshots: 100,
            check_interval: Duration::from_secs(300), // 5 minutes
        }
    }
}
```

### 3.2 Iceberg Sink Implementation

```rust
/// Apache Iceberg Sink Connector.
///
/// Writes Arrow RecordBatch data to an Apache Iceberg table with snapshot
/// isolation and exactly-once semantics. Implements the `SinkConnector` trait
/// from F034.
///
/// # Lifecycle
///
/// ```text
/// new() -> open() -> [begin_epoch() -> write_batch()* -> commit_epoch()] -> close()
///                          |                                    |
///                          +--- rollback_epoch() (on failure) --+
/// ```
///
/// # Exactly-Once Semantics
///
/// Each LaminarDB epoch maps to exactly one Iceberg snapshot. On recovery,
/// the sink reads snapshot summary properties to find the last committed epoch.
/// If an epoch was already committed, it is skipped (idempotent commit).
///
/// # Catalog Integration
///
/// All table operations go through the Iceberg catalog:
/// - Table creation with schema and partition spec
/// - Schema evolution (add columns, type promotion)
/// - Snapshot commits with optimistic concurrency
/// - Snapshot expiry and table maintenance
///
/// # Ring Architecture
///
/// All Iceberg I/O runs in Ring 1 (background async I/O).
/// Ring 0 pushes RecordBatch data via SPSC subscription channel.
/// Ring 2 manages configuration and schema discovery.
pub struct IcebergSink {
    /// Connector configuration.
    config: IcebergSinkConfig,
    /// Iceberg catalog handle.
    catalog: Option<Arc<dyn Catalog>>,
    /// Iceberg table handle.
    table: Option<Table>,
    /// Arrow schema of the table.
    schema: Option<SchemaRef>,
    /// Current connector state.
    state: ConnectorState,
    /// Current epoch being written.
    current_epoch: u64,
    /// Last successfully committed epoch.
    last_committed_epoch: u64,
    /// RecordBatch buffer for the current epoch.
    batch_buffer: Vec<RecordBatch>,
    /// Total rows buffered in current epoch.
    buffered_rows: usize,
    /// Total bytes buffered (estimated) in current epoch.
    buffered_bytes: u64,
    /// Data files written in the current epoch (pending commit).
    pending_data_files: Vec<DataFile>,
    /// Equality delete files written in the current epoch (for upsert mode).
    pending_delete_files: Vec<DataFile>,
    /// Background maintenance manager.
    maintenance_manager: Option<MaintenanceManager>,
    /// Metrics for this connector.
    metrics: IcebergSinkMetrics,
    /// Time when the current buffer started accumulating.
    buffer_start_time: Option<Instant>,
}

impl IcebergSink {
    /// Creates a new Iceberg sink with the given configuration.
    pub fn new(config: IcebergSinkConfig) -> Self {
        Self {
            config,
            catalog: None,
            table: None,
            schema: None,
            state: ConnectorState::Created,
            current_epoch: 0,
            last_committed_epoch: 0,
            batch_buffer: Vec::new(),
            buffered_rows: 0,
            buffered_bytes: 0,
            pending_data_files: Vec::new(),
            pending_delete_files: Vec::new(),
            maintenance_manager: None,
            metrics: IcebergSinkMetrics::default(),
            buffer_start_time: None,
        }
    }

    /// Creates an Iceberg sink from SQL connector config.
    pub fn from_connector_config(
        connector_config: &ConnectorConfig,
    ) -> Result<Self, ConnectorError> {
        let catalog_type = match connector_config
            .options
            .get("catalog.type")
            .map(|s| s.as_str())
        {
            Some("rest") | None => IcebergCatalogType::Rest,
            Some("glue") => IcebergCatalogType::Glue,
            Some("hive") => IcebergCatalogType::Hive,
            Some("memory") => IcebergCatalogType::Memory,
            Some(other) => {
                return Err(ConnectorError::Config(format!(
                    "Unknown catalog type: '{other}'. Expected: rest, glue, hive, memory"
                )));
            }
        };

        let catalog_uri = connector_config
            .require("catalog.uri")?
            .to_string();

        let warehouse = connector_config
            .require("warehouse")?
            .to_string();

        let namespace: Vec<String> = connector_config
            .require("namespace")?
            .split('.')
            .map(|s| s.trim().to_string())
            .collect();

        let table_name = connector_config
            .require("table.name")?
            .to_string();

        let partition_spec = Self::parse_partition_spec(
            connector_config.options.get("partition.spec"),
        )?;

        let write_mode = match connector_config
            .options
            .get("write.mode")
            .map(|s| s.as_str())
        {
            Some("append") | None => IcebergWriteMode::Append,
            Some("upsert") => IcebergWriteMode::Upsert,
            Some(other) => {
                return Err(ConnectorError::Config(format!(
                    "Unknown write mode: '{other}'. Expected: append, upsert"
                )));
            }
        };

        let equality_delete_columns: Vec<String> = connector_config
            .options
            .get("equality.delete.columns")
            .map(|s| s.split(',').map(|c| c.trim().to_string()).collect())
            .unwrap_or_default();

        if write_mode == IcebergWriteMode::Upsert && equality_delete_columns.is_empty() {
            return Err(ConnectorError::Config(
                "Upsert mode requires 'equality.delete.columns' to be specified".into(),
            ));
        }

        let delivery_guarantee = match connector_config
            .options
            .get("delivery.guarantee")
            .map(|s| s.as_str())
        {
            Some("exactly-once") | None => DeliveryGuarantee::ExactlyOnce,
            Some("at-least-once") => DeliveryGuarantee::AtLeastOnce,
            Some(other) => {
                return Err(ConnectorError::Config(format!(
                    "Unknown delivery guarantee: '{other}'"
                )));
            }
        };

        let target_file_size: usize = connector_config
            .get("target.file.size")
            .unwrap_or(128 * 1024 * 1024);

        // Collect catalog-prefixed options.
        let catalog_properties: HashMap<String, String> = connector_config
            .options
            .iter()
            .filter(|(k, _)| k.starts_with("catalog.prop."))
            .map(|(k, v)| {
                (k.strip_prefix("catalog.prop.").unwrap().to_string(), v.clone())
            })
            .collect();

        // Collect storage-prefixed options.
        let storage_properties: HashMap<String, String> = connector_config
            .options
            .iter()
            .filter(|(k, _)| k.starts_with("storage."))
            .map(|(k, v)| {
                (k.strip_prefix("storage.").unwrap().to_string(), v.clone())
            })
            .collect();

        let config = IcebergSinkConfig {
            catalog_type,
            catalog_uri,
            warehouse,
            namespace,
            table_name,
            partition_spec,
            target_file_size,
            write_mode,
            equality_delete_columns,
            delivery_guarantee,
            catalog_properties,
            storage_properties,
            ..IcebergSinkConfig::default()
        };

        Ok(Self::new(config))
    }

    /// Parses a partition spec string into partition fields.
    ///
    /// Format: `"DAYS(event_time), BUCKET(16, user_id), IDENTITY(region)"`
    fn parse_partition_spec(
        spec: Option<&String>,
    ) -> Result<Vec<IcebergPartitionField>, ConnectorError> {
        let spec_str = match spec {
            Some(s) if !s.is_empty() => s,
            _ => return Ok(Vec::new()),
        };

        let mut fields = Vec::new();
        for part in spec_str.split(',') {
            let part = part.trim();
            let field = Self::parse_single_partition_field(part)?;
            fields.push(field);
        }
        Ok(fields)
    }

    /// Parses a single partition field like "DAYS(event_time)" or "BUCKET(16, user_id)".
    fn parse_single_partition_field(
        spec: &str,
    ) -> Result<IcebergPartitionField, ConnectorError> {
        let open = spec.find('(').ok_or_else(|| {
            ConnectorError::Config(format!(
                "Invalid partition field '{spec}': expected TRANSFORM(column)"
            ))
        })?;
        let close = spec.rfind(')').ok_or_else(|| {
            ConnectorError::Config(format!(
                "Invalid partition field '{spec}': missing closing parenthesis"
            ))
        })?;

        let transform_name = spec[..open].trim().to_uppercase();
        let args: Vec<&str> = spec[open + 1..close].split(',').map(|s| s.trim()).collect();

        let (transform, source_column) = match transform_name.as_str() {
            "IDENTITY" => {
                if args.len() != 1 {
                    return Err(ConnectorError::Config(
                        "IDENTITY transform requires exactly 1 argument: IDENTITY(column)".into(),
                    ));
                }
                (IcebergTransform::Identity, args[0].to_string())
            }
            "BUCKET" => {
                if args.len() != 2 {
                    return Err(ConnectorError::Config(
                        "BUCKET transform requires 2 arguments: BUCKET(n, column)".into(),
                    ));
                }
                let n: u32 = args[0].parse().map_err(|_| {
                    ConnectorError::Config(format!("Invalid bucket count: '{}'", args[0]))
                })?;
                (IcebergTransform::Bucket(n), args[1].to_string())
            }
            "TRUNCATE" => {
                if args.len() != 2 {
                    return Err(ConnectorError::Config(
                        "TRUNCATE transform requires 2 arguments: TRUNCATE(width, column)".into(),
                    ));
                }
                let w: u32 = args[0].parse().map_err(|_| {
                    ConnectorError::Config(format!("Invalid truncate width: '{}'", args[0]))
                })?;
                (IcebergTransform::Truncate(w), args[1].to_string())
            }
            "YEAR" | "YEARS" => {
                if args.len() != 1 {
                    return Err(ConnectorError::Config(
                        "YEAR transform requires exactly 1 argument: YEAR(column)".into(),
                    ));
                }
                (IcebergTransform::Year, args[0].to_string())
            }
            "MONTH" | "MONTHS" => {
                if args.len() != 1 {
                    return Err(ConnectorError::Config(
                        "MONTH transform requires exactly 1 argument: MONTH(column)".into(),
                    ));
                }
                (IcebergTransform::Month, args[0].to_string())
            }
            "DAY" | "DAYS" => {
                if args.len() != 1 {
                    return Err(ConnectorError::Config(
                        "DAY transform requires exactly 1 argument: DAY(column)".into(),
                    ));
                }
                (IcebergTransform::Day, args[0].to_string())
            }
            "HOUR" | "HOURS" => {
                if args.len() != 1 {
                    return Err(ConnectorError::Config(
                        "HOUR transform requires exactly 1 argument: HOUR(column)".into(),
                    ));
                }
                (IcebergTransform::Hour, args[0].to_string())
            }
            other => {
                return Err(ConnectorError::Config(format!(
                    "Unknown partition transform: '{other}'. \
                     Expected: IDENTITY, BUCKET, TRUNCATE, YEAR, MONTH, DAY, HOUR"
                )));
            }
        };

        Ok(IcebergPartitionField {
            source_column,
            transform,
            name: None,
        })
    }

    /// Flushes the current buffer to Parquet data files.
    ///
    /// Converts buffered RecordBatch data to Parquet format using the
    /// `iceberg-rust` writer and stages the data files for snapshot commit.
    async fn flush_buffer(&mut self) -> Result<(), ConnectorError> {
        if self.batch_buffer.is_empty() {
            return Ok(());
        }

        let table = self.table.as_ref()
            .ok_or_else(|| ConnectorError::InvalidState(
                "Table not opened".into(),
            ))?;

        let batches = std::mem::take(&mut self.batch_buffer);
        let rows_flushed = self.buffered_rows;
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.buffer_start_time = None;

        let flush_start = Instant::now();

        // Use iceberg-rust DataFileWriter for Parquet file creation.
        // The writer handles partitioning transforms automatically.
        let file_io = table.file_io();
        let location = table.metadata().location();

        // Write batches to Parquet data files through iceberg-rust writer.
        // Each batch may produce multiple data files if partitioned.
        let data_files = write_data_files(
            file_io,
            location,
            table.metadata().current_schema(),
            table.metadata().default_partition_spec(),
            &batches,
            self.config.target_file_size,
            &self.config.sort_order,
        )
        .await
        .map_err(|e| ConnectorError::Io(
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
        ))?;

        // Stage files for snapshot commit.
        self.pending_data_files.extend(data_files);

        let flush_duration = flush_start.elapsed();
        self.metrics.flush_count += 1;
        self.metrics.rows_flushed += rows_flushed as u64;
        self.metrics.bytes_written += self.pending_data_files.iter()
            .map(|f| f.file_size_in_bytes as u64)
            .sum::<u64>();
        self.metrics.last_flush_duration = flush_duration;

        tracing::debug!(
            rows = rows_flushed,
            files = self.pending_data_files.len(),
            duration_ms = flush_duration.as_millis(),
            "Iceberg: flushed buffer to Parquet"
        );

        Ok(())
    }

    /// Commits pending data files as a new Iceberg snapshot.
    ///
    /// This creates a new snapshot with the pending data files and
    /// includes epoch metadata in the snapshot summary for exactly-once.
    async fn commit_iceberg_snapshot(
        &mut self,
        epoch: u64,
    ) -> Result<i64, ConnectorError> {
        let table = self.table.as_mut()
            .ok_or_else(|| ConnectorError::InvalidState(
                "Table not opened".into(),
            ))?;

        if self.pending_data_files.is_empty() && self.pending_delete_files.is_empty() {
            tracing::debug!(epoch, "Iceberg: no files to commit for epoch");
            return Ok(table.metadata().current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0));
        }

        let commit_start = Instant::now();

        // Build the append or overwrite operation.
        let snapshot_id = match self.config.write_mode {
            IcebergWriteMode::Append => {
                // Append data files to the table.
                let mut append = table.new_append();
                for data_file in std::mem::take(&mut self.pending_data_files) {
                    append = append.add_data_file(data_file);
                }

                // Add epoch metadata for exactly-once tracking.
                if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
                    append = append.set_snapshot_property(
                        "laminardb.writer-id",
                        &self.config.writer_id,
                    );
                    append = append.set_snapshot_property(
                        "laminardb.epoch",
                        &epoch.to_string(),
                    );
                }

                let snapshot = append.commit().await.map_err(|e| {
                    ConnectorError::Transaction(format!(
                        "Iceberg append failed for epoch {epoch}: {e}"
                    ))
                })?;

                snapshot.snapshot_id()
            }
            IcebergWriteMode::Upsert => {
                // For upsert: write equality delete files, then append data files.
                // Uses Iceberg v2 row-level operations.
                let mut overwrite = table.new_overwrite();

                // Add delete files (equality deletes for removed/updated keys).
                for delete_file in std::mem::take(&mut self.pending_delete_files) {
                    overwrite = overwrite.add_delete_file(delete_file);
                }

                // Add data files (inserts and updated rows).
                for data_file in std::mem::take(&mut self.pending_data_files) {
                    overwrite = overwrite.add_data_file(data_file);
                }

                if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
                    overwrite = overwrite.set_snapshot_property(
                        "laminardb.writer-id",
                        &self.config.writer_id,
                    );
                    overwrite = overwrite.set_snapshot_property(
                        "laminardb.epoch",
                        &epoch.to_string(),
                    );
                }

                let snapshot = overwrite.commit().await.map_err(|e| {
                    ConnectorError::Transaction(format!(
                        "Iceberg overwrite failed for epoch {epoch}: {e}"
                    ))
                })?;

                snapshot.snapshot_id()
            }
        };

        let commit_duration = commit_start.elapsed();
        self.metrics.commits += 1;
        self.metrics.last_commit_duration = commit_duration;
        self.metrics.last_snapshot_id = snapshot_id;

        tracing::info!(
            epoch,
            snapshot_id,
            duration_ms = commit_duration.as_millis(),
            "Iceberg: committed snapshot"
        );

        Ok(snapshot_id)
    }

    /// Resolves the last committed epoch from Iceberg snapshot history.
    ///
    /// Scans snapshot summary properties for the latest epoch written
    /// by this writer ID.
    async fn resolve_last_committed_epoch(
        table: &Table,
        writer_id: &str,
    ) -> u64 {
        // Walk snapshots from newest to oldest.
        let snapshots = table.metadata().snapshots();
        for snapshot in snapshots.iter().rev() {
            if let Some(summary) = snapshot.summary() {
                let matches_writer = summary
                    .get("laminardb.writer-id")
                    .map(|id| id == writer_id)
                    .unwrap_or(false);

                if matches_writer {
                    if let Some(epoch_str) = summary.get("laminardb.epoch") {
                        if let Ok(epoch) = epoch_str.parse::<u64>() {
                            return epoch;
                        }
                    }
                }
            }
        }
        0
    }

    /// Checks if a buffer flush is needed based on size or time thresholds.
    fn should_flush(&self) -> bool {
        if self.buffered_rows >= self.config.max_buffer_records {
            return true;
        }
        if self.buffered_bytes >= self.config.target_file_size as u64 {
            return true;
        }
        if let Some(start) = self.buffer_start_time {
            if start.elapsed() >= self.config.max_buffer_duration {
                return true;
            }
        }
        false
    }

    /// Estimates the byte size of a RecordBatch.
    fn estimate_batch_size(batch: &RecordBatch) -> u64 {
        batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size() as u64)
            .sum()
    }
}
```

### 3.3 SinkConnector Trait Implementation

```rust
#[async_trait]
impl SinkConnector for IcebergSink {
    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        // Build the catalog based on catalog type.
        let catalog = build_catalog(
            self.config.catalog_type,
            &self.config.catalog_uri,
            &self.config.warehouse,
            &self.config.catalog_properties,
        )
        .await
        .map_err(|e| ConnectorError::Connection(format!(
            "Failed to connect to Iceberg catalog at '{}': {e}",
            self.config.catalog_uri,
        )))?;

        let table_ident = TableIdent::new(
            NamespaceIdent::from_vec(self.config.namespace.clone())
                .map_err(|e| ConnectorError::Config(e.to_string()))?,
            self.config.table_name.clone(),
        );

        // Load or create the table.
        let table = match catalog.load_table(&table_ident).await {
            Ok(t) => t,
            Err(_) => {
                // Table does not exist; will be created on first write
                // when we know the schema.
                tracing::info!(
                    namespace = ?self.config.namespace,
                    table = %self.config.table_name,
                    "Iceberg: table not found, will create on first write"
                );
                // Store catalog for later table creation.
                self.catalog = Some(Arc::new(catalog));
                self.state = ConnectorState::Running;
                return Ok(());
            }
        };

        // Resolve last committed epoch for exactly-once recovery.
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            self.last_committed_epoch =
                Self::resolve_last_committed_epoch(&table, &self.config.writer_id).await;
            tracing::info!(
                last_epoch = self.last_committed_epoch,
                writer_id = %self.config.writer_id,
                "Iceberg: resolved last committed epoch"
            );
        }

        // Extract Arrow schema from Iceberg schema.
        self.schema = Some(Arc::new(
            iceberg_schema_to_arrow(table.metadata().current_schema())
                .map_err(|e| ConnectorError::Config(format!(
                    "Failed to convert Iceberg schema to Arrow: {e}"
                )))?,
        ));

        // Start background maintenance if enabled.
        if self.config.maintenance.compaction_enabled
            || self.config.maintenance.expire_snapshots_enabled
        {
            self.maintenance_manager = Some(MaintenanceManager::new(
                table_ident.clone(),
                self.config.maintenance.clone(),
            ));
        }

        self.catalog = Some(Arc::new(catalog));
        self.table = Some(table);
        self.state = ConnectorState::Running;

        tracing::info!(
            namespace = ?self.config.namespace,
            table = %self.config.table_name,
            "Iceberg sink: opened successfully"
        );

        Ok(())
    }

    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState(
                format!("Sink not in Running state: {:?}", self.state),
            ));
        }

        let num_rows = batch.num_rows();
        let estimated_bytes = Self::estimate_batch_size(batch);

        // Create table on first write if it doesn't exist yet.
        if self.table.is_none() {
            self.create_table(batch.schema()).await?;
        }

        // Handle schema evolution if enabled.
        if self.config.schema_evolution {
            if let Some(ref existing_schema) = self.schema {
                if existing_schema.as_ref() != batch.schema().as_ref() {
                    self.evolve_schema(batch.schema()).await?;
                }
            }
        }

        // Buffer the batch.
        if self.buffer_start_time.is_none() {
            self.buffer_start_time = Some(Instant::now());
        }
        self.batch_buffer.push(batch.clone());
        self.buffered_rows += num_rows;
        self.buffered_bytes += estimated_bytes;

        // Flush if buffer threshold reached.
        if self.should_flush() {
            self.flush_buffer().await?;
        }

        Ok(WriteResult {
            records_written: num_rows,
            bytes_written: estimated_bytes,
        })
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone().unwrap_or_else(|| {
            Arc::new(arrow::datatypes::Schema::empty())
        })
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // For exactly-once, skip epochs already committed.
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            tracing::warn!(
                epoch,
                last_committed = self.last_committed_epoch,
                "Iceberg: skipping already-committed epoch"
            );
            return Ok(());
        }

        self.current_epoch = epoch;
        self.batch_buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_data_files.clear();
        self.pending_delete_files.clear();
        self.buffer_start_time = None;

        tracing::debug!(epoch, "Iceberg: began epoch");
        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Skip if already committed (exactly-once idempotency).
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            return Ok(());
        }

        // Flush any remaining buffered data.
        self.flush_buffer().await?;

        // Commit all pending files as a single Iceberg snapshot.
        let snapshot_id = self.commit_iceberg_snapshot(epoch).await?;

        self.last_committed_epoch = epoch;

        // Trigger background maintenance check.
        if let Some(ref mut manager) = self.maintenance_manager {
            manager.maybe_maintain().await;
        }

        tracing::info!(
            epoch,
            snapshot_id,
            "Iceberg: committed epoch"
        );

        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Discard buffered data and pending files.
        // Iceberg: uncommitted data files will be cleaned up by
        // orphan file cleanup during maintenance.
        self.batch_buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_data_files.clear();
        self.pending_delete_files.clear();
        self.buffer_start_time = None;

        tracing::warn!(epoch, "Iceberg: rolled back epoch");
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        if self.table.is_some() || self.catalog.is_some() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.metrics.rows_flushed,
            bytes_total: self.metrics.bytes_written,
            errors_total: self.metrics.errors,
            ..ConnectorMetrics::default()
        }
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        SinkConnectorCapabilities::default()
            .with_exactly_once()
            .with_partitioned()
            .with_changelog()
            .with_schema_evolution()
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flush_buffer().await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Flush any remaining data.
        if !self.batch_buffer.is_empty() {
            self.flush_buffer().await?;
            if !self.pending_data_files.is_empty() {
                self.commit_iceberg_snapshot(self.current_epoch).await?;
            }
        }

        // Stop background maintenance.
        if let Some(ref mut manager) = self.maintenance_manager {
            manager.stop().await;
        }

        self.state = ConnectorState::Stopped;
        tracing::info!("Iceberg sink: closed");
        Ok(())
    }
}
```

### 3.4 Table Creation and Schema Evolution

```rust
impl IcebergSink {
    /// Creates the Iceberg table when writing the first batch.
    ///
    /// Uses the Arrow schema from the first batch and the configured
    /// partition spec to create the table in the catalog.
    async fn create_table(
        &mut self,
        arrow_schema: SchemaRef,
    ) -> Result<(), ConnectorError> {
        let catalog = self.catalog.as_ref()
            .ok_or_else(|| ConnectorError::InvalidState(
                "Catalog not connected".into(),
            ))?;

        let iceberg_schema = arrow_to_iceberg_schema(arrow_schema.as_ref())
            .map_err(|e| ConnectorError::Config(format!(
                "Failed to convert Arrow schema to Iceberg: {e}"
            )))?;

        let partition_spec = build_partition_spec(
            &iceberg_schema,
            &self.config.partition_spec,
        )
        .map_err(|e| ConnectorError::Config(format!(
            "Failed to build partition spec: {e}"
        )))?;

        let namespace = NamespaceIdent::from_vec(self.config.namespace.clone())
            .map_err(|e| ConnectorError::Config(e.to_string()))?;

        // Ensure namespace exists.
        if !catalog.namespace_exists(&namespace).await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?
        {
            catalog.create_namespace(&namespace, HashMap::new()).await
                .map_err(|e| ConnectorError::Connection(format!(
                    "Failed to create namespace {:?}: {e}", self.config.namespace,
                )))?;
        }

        let table_creation = TableCreation::builder()
            .name(self.config.table_name.clone())
            .schema(iceberg_schema)
            .partition_spec(partition_spec)
            .location(format!(
                "{}/{}/{}",
                self.config.warehouse,
                self.config.namespace.join("/"),
                self.config.table_name,
            ))
            .build();

        let table = catalog
            .create_table(&namespace, table_creation)
            .await
            .map_err(|e| ConnectorError::Connection(format!(
                "Failed to create Iceberg table '{}': {e}", self.config.table_name,
            )))?;

        self.schema = Some(arrow_schema);
        self.table = Some(table);

        tracing::info!(
            namespace = ?self.config.namespace,
            table = %self.config.table_name,
            "Iceberg: created table"
        );

        Ok(())
    }

    /// Evolves the Iceberg table schema to accommodate new columns.
    ///
    /// Supports additive changes (new nullable columns) and type promotion
    /// (e.g., int -> long, float -> double).
    async fn evolve_schema(
        &mut self,
        new_arrow_schema: SchemaRef,
    ) -> Result<(), ConnectorError> {
        let table = self.table.as_mut()
            .ok_or_else(|| ConnectorError::InvalidState(
                "Table not opened".into(),
            ))?;

        let existing_schema = table.metadata().current_schema();

        // Find new columns that don't exist in the current schema.
        let new_columns = find_new_columns(existing_schema, new_arrow_schema.as_ref());
        if new_columns.is_empty() {
            return Ok(());
        }

        // Build schema update transaction.
        let mut update = table.update_schema();
        for (name, data_type, nullable) in &new_columns {
            let iceberg_type = arrow_type_to_iceberg(data_type)
                .map_err(|e| ConnectorError::Config(format!(
                    "Cannot convert Arrow type {:?} to Iceberg: {e}", data_type,
                )))?;
            update = update.add_column(name, iceberg_type, *nullable);
        }

        update.commit().await.map_err(|e| {
            ConnectorError::Transaction(format!("Schema evolution failed: {e}"))
        })?;

        // Refresh table metadata.
        table.refresh().await.map_err(|e| {
            ConnectorError::Io(std::io::Error::new(
                std::io::ErrorKind::Other, e.to_string(),
            ))
        })?;

        self.schema = Some(new_arrow_schema);

        tracing::info!(
            new_columns = new_columns.len(),
            "Iceberg: schema evolved"
        );

        Ok(())
    }
}
```

### 3.5 CDC / Upsert Mode (F063 Integration)

```rust
/// Handles CDC/upsert operations by converting F063 Z-set changelog
/// records into Iceberg equality delete files + data file inserts.
///
/// When `write_mode = Upsert`, the sink interprets changelog records:
/// - Inserts (weight +1): written as data files
/// - Deletes (weight -1): written as equality delete files
/// - Updates: delete old row (equality delete) + insert new row (data file)
///
/// Iceberg v2 equality delete files identify rows to delete by matching
/// on `equality_delete_columns`. The Iceberg reader merges data files
/// with delete files at read time.
impl IcebergSink {
    /// Processes a batch of changelog records for upsert mode.
    ///
    /// Splits the batch into:
    /// - Insert records -> buffered for data files
    /// - Delete records -> buffered for equality delete files
    async fn process_changelog_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), ConnectorError> {
        // Find the _op column (F063 changelog operation).
        let op_col_idx = batch.schema().index_of("_op").map_err(|_| {
            ConnectorError::Config(
                "Upsert mode requires '_op' column in input (F063 changelog format)".into(),
            )
        })?;

        let op_array = batch
            .column(op_col_idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| ConnectorError::Config(
                "'_op' column must be String type".into(),
            ))?;

        // Split into inserts and deletes.
        let mut insert_indices = Vec::new();
        let mut delete_indices = Vec::new();

        for i in 0..batch.num_rows() {
            match op_array.value(i) {
                "I" | "+I" | "c" | "r" | "+U" => insert_indices.push(i as u32),
                "D" | "-D" | "-U" => delete_indices.push(i as u32),
                other => {
                    tracing::warn!(
                        op = other,
                        row = i,
                        "Iceberg upsert: unknown CDC operation, treating as insert"
                    );
                    insert_indices.push(i as u32);
                }
            }
        }

        // Project out the _op column for data files (Iceberg doesn't need it).
        let data_columns: Vec<usize> = (0..batch.num_columns())
            .filter(|&i| i != op_col_idx)
            .collect();

        // Buffer insert records as data files.
        if !insert_indices.is_empty() {
            let insert_batch = take_rows(batch, &insert_indices, &data_columns)?;
            self.batch_buffer.push(insert_batch);
        }

        // Write delete records as equality delete files.
        if !delete_indices.is_empty() {
            let delete_batch = take_rows(batch, &delete_indices, &data_columns)?;
            let delete_file = write_equality_delete_file(
                self.table.as_ref().unwrap(),
                &delete_batch,
                &self.config.equality_delete_columns,
            )
            .await
            .map_err(|e| ConnectorError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            ))?;
            self.pending_delete_files.push(delete_file);
        }

        Ok(())
    }
}
```

### 3.6 Background Maintenance Manager

```rust
/// Manages background maintenance of Iceberg tables.
///
/// Performs:
/// - Data file compaction (rewrite small files into larger ones)
/// - Snapshot expiry (remove old snapshots beyond retention)
/// - Orphan file cleanup (remove unreferenced data files)
pub struct MaintenanceManager {
    /// Table identifier for catalog operations.
    table_ident: TableIdent,
    /// Maintenance configuration.
    config: MaintenanceConfig,
    /// Handle to the background maintenance task.
    task_handle: Option<tokio::task::JoinHandle<()>>,
    /// Shutdown signal.
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl MaintenanceManager {
    /// Creates a new maintenance manager.
    pub fn new(table_ident: TableIdent, config: MaintenanceConfig) -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            table_ident,
            config,
            task_handle: None,
            shutdown: shutdown_tx,
        }
    }

    /// Checks if maintenance is needed and triggers it asynchronously.
    pub async fn maybe_maintain(&mut self) {
        if self.task_handle.is_some() {
            return; // Already running.
        }

        let table_ident = self.table_ident.clone();
        let config = self.config.clone();

        self.task_handle = Some(tokio::spawn(async move {
            // Compact small files if threshold reached.
            if config.compaction_enabled {
                if let Err(e) = compact_data_files(
                    &table_ident,
                    config.compaction_min_files,
                    config.compaction_target_file_size,
                ).await {
                    tracing::error!(error = %e, "Iceberg maintenance: compaction failed");
                }
            }

            // Expire old snapshots.
            if config.expire_snapshots_enabled {
                if let Err(e) = expire_snapshots(
                    &table_ident,
                    config.snapshot_retention,
                    config.max_snapshots,
                ).await {
                    tracing::error!(error = %e, "Iceberg maintenance: snapshot expiry failed");
                }
            }
        }));
    }

    /// Stops the maintenance manager.
    pub async fn stop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}
```

### 3.7 Metrics

```rust
/// Metrics for the Iceberg sink connector.
#[derive(Debug, Default)]
pub struct IcebergSinkMetrics {
    /// Total rows flushed to Parquet files.
    pub rows_flushed: u64,
    /// Total bytes written to storage.
    pub bytes_written: u64,
    /// Total number of Parquet flush operations.
    pub flush_count: u64,
    /// Total number of Iceberg snapshot commits.
    pub commits: u64,
    /// Total errors encountered.
    pub errors: u64,
    /// Duration of the last flush operation.
    pub last_flush_duration: Duration,
    /// Duration of the last commit operation.
    pub last_commit_duration: Duration,
    /// Last Iceberg snapshot ID.
    pub last_snapshot_id: i64,
    /// Total data files written.
    pub data_files_written: u64,
    /// Total equality delete files written (upsert mode).
    pub delete_files_written: u64,
    /// Start time for throughput calculation.
    pub start_time: Option<Instant>,
}

impl IcebergSinkMetrics {
    /// Returns current records-per-second throughput.
    pub fn records_per_sec(&self) -> f64 {
        match self.start_time {
            Some(start) => {
                let elapsed = start.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    self.rows_flushed as f64 / elapsed
                } else {
                    0.0
                }
            }
            None => 0.0,
        }
    }
}
```

### 3.8 Three-Ring Integration

| Ring | Iceberg Sink Responsibilities |
|------|------------------------------|
| **Ring 0 (Hot Path)** | No Iceberg code runs here. Data arrives via SPSC subscription channel. |
| **Ring 1 (Background)** | `IcebergSink.write_batch()` buffers RecordBatch. `flush_buffer()` writes Parquet files. `commit_iceberg_snapshot()` commits to catalog. `MaintenanceManager` runs compaction and snapshot expiry. |
| **Ring 2 (Control Plane)** | Schema discovery from catalog. Schema evolution. Table creation. Configuration parsing from SQL DDL. Health checks. Metrics export. |

---

## 4. Latency Considerations

### 4.1 Hot Path Budget

The Iceberg Sink runs entirely in Ring 1. Ring 0 is unaffected:

| Component | Ring | Latency | Notes |
|-----------|------|---------|-------|
| SPSC subscription poll (Ring 0 -> Ring 1) | Ring 0 | ~5ns | Lock-free, existing implementation |
| RecordBatch buffer append | Ring 1 | ~100ns | Vec push, no copy |
| Parquet file write (flush) | Ring 1 | 10-500ms | Depends on batch size, storage backend |
| Iceberg snapshot commit (catalog) | Ring 1 | 10-200ms | REST catalog round-trip |
| S3 PUT (Parquet file upload) | Ring 1 | 50-200ms | Network I/O to cloud storage |
| Data file compaction | Ring 1 | 1-60s | Background, does not block writes |
| Snapshot expiry | Ring 1 | 100ms-5s | Catalog metadata operation |
| **Ring 0 impact** | **Ring 0** | **~5ns** | **SPSC channel read only** |

---

## 5. SQL Integration

### 5.1 CREATE SINK Syntax

```sql
-- Append-only Iceberg sink with REST catalog
CREATE SINK TABLE trade_archive
FROM processed_trades
WITH (
    connector = 'iceberg',
    'catalog.type' = 'rest',
    'catalog.uri' = 'http://iceberg-catalog:8181',
    'warehouse' = 's3://data-lake/warehouse',
    'namespace' = 'analytics.prod',
    'table.name' = 'trades',
    'partition.spec' = 'DAYS(event_time)',
    'delivery.guarantee' = 'exactly-once'
);

-- Iceberg sink with AWS Glue catalog
CREATE SINK TABLE orders_iceberg
FROM enriched_orders
WITH (
    connector = 'iceberg',
    'catalog.type' = 'glue',
    'catalog.uri' = 'arn:aws:glue:us-east-1:123456789:catalog',
    'warehouse' = 's3://lakehouse/warehouse',
    'namespace' = 'analytics',
    'table.name' = 'orders',
    'partition.spec' = 'DAYS(order_time), BUCKET(16, customer_id)',
    'delivery.guarantee' = 'exactly-once',
    'storage.aws_region' = 'us-east-1'
);

-- CDC/Upsert sink from changelog stream (F063 integration)
CREATE SINK TABLE customer_snapshot
FROM customer_changes
WITH (
    connector = 'iceberg',
    'catalog.type' = 'rest',
    'catalog.uri' = 'http://polaris:8181',
    'warehouse' = 's3://data-lake/warehouse',
    'namespace' = 'analytics',
    'table.name' = 'customers',
    'write.mode' = 'upsert',
    'equality.delete.columns' = 'customer_id',
    'delivery.guarantee' = 'exactly-once'
);

-- Iceberg sink with hidden multi-level partitioning
CREATE SINK TABLE sensor_data
FROM sensor_events
WITH (
    connector = 'iceberg',
    'catalog.type' = 'rest',
    'catalog.uri' = 'http://nessie:19120/api/v1',
    'warehouse' = 's3://iot-lake/warehouse',
    'namespace' = 'iot.sensors',
    'table.name' = 'readings',
    'partition.spec' = 'DAYS(event_time), IDENTITY(region), BUCKET(32, device_id)',
    'target.file.size' = '268435456',
    'schema.evolution' = 'true'
);

-- Local filesystem Iceberg sink (testing/development)
CREATE SINK TABLE dev_output
FROM test_stream
WITH (
    connector = 'iceberg',
    'catalog.type' = 'memory',
    'warehouse' = '/tmp/iceberg-warehouse',
    'namespace' = 'dev',
    'table.name' = 'output',
    'delivery.guarantee' = 'at-least-once'
);
```

### 5.2 DDL Resolution Flow

```
SQL Statement                    ConnectorRegistry              Iceberg Sink
     |                                |                              |
     | CREATE SINK TABLE ... WITH     |                              |
     | (connector = 'iceberg', ..)    |                              |
     |------------------------------->|                              |
     |                                | lookup("iceberg")            |
     |                                |----------------------------->|
     |                                |                              |
     |                                | IcebergSink::from_config()   |
     |                                |<-----------------------------|
     |                                |                              |
     | SinkRunner started in Ring 1   |                              |
     |<-------------------------------|                              |
```

---

## 6. Rust API Examples

### 6.1 Programmatic Usage

```rust
use laminar_connectors::lakehouse::iceberg::{
    IcebergSink, IcebergSinkConfig, IcebergCatalogType,
    IcebergWriteMode, IcebergPartitionField, IcebergTransform,
    DeliveryGuarantee,
};
use laminar_connectors::connector::SinkConnector;

// Create Iceberg sink with programmatic config.
let config = IcebergSinkConfig {
    catalog_type: IcebergCatalogType::Rest,
    catalog_uri: "http://iceberg-catalog:8181".to_string(),
    warehouse: "s3://data-lake/warehouse".to_string(),
    namespace: vec!["analytics".to_string(), "prod".to_string()],
    table_name: "trades".to_string(),
    partition_spec: vec![
        IcebergPartitionField {
            source_column: "event_time".to_string(),
            transform: IcebergTransform::Day,
            name: None,
        },
    ],
    delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
    ..Default::default()
};

let mut sink = IcebergSink::new(config);

// Open the sink (connects to catalog, resolves last epoch).
sink.open(&connector_config).await?;

// Write data in epoch-based transactions.
sink.begin_epoch(1).await?;
sink.write_batch(&record_batch_1).await?;
sink.write_batch(&record_batch_2).await?;
sink.commit_epoch(1).await?;

// On recovery, epoch 1 is skipped (idempotent).
sink.begin_epoch(1).await?;  // No-op: already committed

// Close the sink.
sink.close().await?;
```

### 6.2 CDC/Upsert Example

```rust
use laminar_core::operator::changelog::{ChangelogRecord, CdcOperation};

// Changelog stream produces Z-set records (F063).
let changelog_batch = create_changelog_batch(vec![
    ChangelogRecord::insert(customer_row_1, timestamp),
    ChangelogRecord::delete(customer_row_2, timestamp),
    ChangelogRecord::update_after(customer_row_3, timestamp),
]);

// Iceberg sink in upsert mode uses equality delete files.
let config = IcebergSinkConfig {
    write_mode: IcebergWriteMode::Upsert,
    equality_delete_columns: vec!["customer_id".to_string()],
    ..Default::default()
};

let mut sink = IcebergSink::new(config);
sink.open(&connector_config).await?;

sink.begin_epoch(1).await?;
sink.write_batch(&changelog_batch).await?;
sink.commit_epoch(1).await?;
// Iceberg snapshot contains:
// - Data files with insert/update rows
// - Equality delete files matching customer_id for deleted rows
```

---

## 7. Configuration Reference

### 7.1 Required Options

| Option | Type | Description |
|--------|------|-------------|
| `connector` | String | Must be `'iceberg'` |
| `catalog.uri` | String | Catalog URI (REST endpoint, Glue ARN, Hive thrift URI) |
| `warehouse` | String | Warehouse location (s3://bucket/path or local path) |
| `namespace` | String | Dot-separated namespace (e.g., `analytics.prod`) |
| `table.name` | String | Table name within the namespace |

### 7.2 Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `catalog.type` | String | `rest` | Catalog type: `rest`, `glue`, `hive`, `memory` |
| `partition.spec` | String | (none) | Partition spec: `DAYS(col)`, `BUCKET(n, col)`, etc. |
| `target.file.size` | Integer | `134217728` (128 MB) | Target Parquet file size in bytes |
| `max.buffer.records` | Integer | `100000` | Maximum records to buffer before flushing |
| `max.buffer.duration` | String | `60s` | Maximum time to buffer before flushing |
| `write.mode` | String | `append` | Write mode: `append`, `upsert` |
| `equality.delete.columns` | String (CSV) | (none) | Key columns for upsert (required for upsert mode) |
| `delivery.guarantee` | String | `exactly-once` | Delivery guarantee: `exactly-once`, `at-least-once` |
| `schema.evolution` | Boolean | `false` | Enable automatic schema evolution |
| `file.format` | String | `parquet` | Data file format: `parquet`, `orc`, `avro` |
| `sort.order` | String | (none) | Sort order for data files (e.g., `event_time ASC`) |
| `maintenance.compaction` | Boolean | `true` | Enable background data file compaction |
| `maintenance.expire-snapshots` | Boolean | `true` | Enable automatic snapshot expiry |
| `maintenance.snapshot-retention` | String | `168h` (7 days) | Snapshot retention period |
| `writer.id` | String | (auto UUID) | Writer ID for exactly-once deduplication |

### 7.3 Catalog-Specific Properties

All catalog properties are prefixed with `catalog.prop.` in the SQL WITH clause.

**REST Catalog:**

| Option | Description |
|--------|-------------|
| `catalog.prop.token` | Bearer token for authentication |
| `catalog.prop.credential` | Client credential for OAuth2 |
| `catalog.prop.scope` | OAuth2 scope |

**AWS Glue:**

| Option | Description |
|--------|-------------|
| `catalog.prop.aws_region` | AWS region for Glue catalog |
| `catalog.prop.aws_access_key_id` | AWS access key ID |
| `catalog.prop.aws_secret_access_key` | AWS secret access key |

**Hive Metastore:**

| Option | Description |
|--------|-------------|
| `catalog.prop.thrift_uri` | Hive Metastore thrift URI |

### 7.4 Storage Backend Properties

All storage options are prefixed with `storage.` in the SQL WITH clause. Same as F031 Delta Lake Sink.

---

## 8. Exactly-Once Semantics Deep Dive

### 8.1 Epoch-to-Snapshot Mapping

```
LaminarDB Epochs              Iceberg Snapshots
+---+---+---+---+---+         +---+---+---+---+---+
| 1 | 2 | 3 | 4 | 5 |  --->  |s01|s02|s03|s04|s05|
+---+---+---+---+---+         +---+---+---+---+---+
                                         |
                               Snapshot Summary Properties:
                               {
                                 "laminardb.writer-id": "abc123",
                                 "laminardb.epoch": "3",
                                 ...
                               }
```

### 8.2 Recovery Protocol

```
1. Sink opens via catalog -> loads table
2. Iterate snapshots from newest to oldest
3. Find snapshot with matching writer-id in summary
4. last_committed_epoch = laminardb.epoch value (e.g., 3)
5. On begin_epoch(3): skip (already committed)
6. On begin_epoch(4): proceed normally
7. On commit_epoch(4): snapshot with epoch=4 in summary
```

### 8.3 Optimistic Concurrency

Iceberg uses optimistic concurrency for snapshot commits. If another writer
has committed a conflicting snapshot between our read and write:

1. The commit is retried with a refreshed table state
2. For append-only writes: retries always succeed (compatible change)
3. For upsert writes: retries succeed if equality delete columns don't conflict
4. After N retries: report CommitConflict error

---

## 9. Comparison with F031 (Delta Lake Sink)

| Aspect | F031 Delta Lake Sink | F032 Iceberg Sink |
|--------|---------------------|-------------------|
| **Metadata storage** | File-based `_delta_log/` | Catalog-based (REST/Glue/Hive) |
| **Partitioning** | Explicit partition columns | Hidden partition transforms |
| **Schema evolution** | Additive columns, type widening | Full (add, drop, rename, reorder, promote) |
| **CDC/Upsert** | MERGE operations | Equality delete files (v2) |
| **Exactly-once** | `txn` action in commit log | Snapshot summary properties |
| **Compaction** | OPTIMIZE + Z-ORDER | Rewrite data files |
| **Multi-engine** | Spark, Trino via Delta Sharing | Spark, Trino, Snowflake, BigQuery |
| **UniForm interop** | Delta writes -> Iceberg reads | Native Iceberg |
| **Rust crate** | `deltalake` (delta-rs) | `iceberg` (iceberg-rust) |

---

## 10. Implementation Roadmap

### Phase 1: Core Sink and Append Mode (3-4 days)

- [ ] Define `IcebergSinkConfig`, `IcebergCatalogType`, `IcebergWriteMode`
- [ ] Define `IcebergPartitionField`, `IcebergTransform` enums
- [ ] Implement `IcebergSink` struct with RecordBatch buffering
- [ ] Implement `SinkConnector` trait (open, write_batch, close)
- [ ] Implement `flush_buffer()` with Arrow-to-Parquet via iceberg-rust writer
- [ ] Implement catalog connection (REST catalog initially)
- [ ] Implement table creation from first batch schema
- [ ] Unit tests for config parsing, partition spec parsing
- [ ] Integration test: write batches and verify Parquet files exist

### Phase 2: Exactly-Once and Epoch Management (2-3 days)

- [ ] Implement `begin_epoch()` / `commit_epoch()` / `rollback_epoch()`
- [ ] Implement epoch-to-snapshot mapping via summary properties
- [ ] Implement `resolve_last_committed_epoch()` for recovery
- [ ] Implement idempotent commit (skip already-committed epochs)
- [ ] Integration test: crash-recovery with no duplicates
- [ ] Integration test: rollback discards pending files

### Phase 3: Hidden Partitioning (2-3 days)

- [ ] Implement partition spec builder from `IcebergPartitionField`
- [ ] Implement all transforms: identity, bucket, truncate, year/month/day/hour
- [ ] Partition spec parsing from SQL DDL string
- [ ] Test: partitioned writes produce correct directory structure
- [ ] Test: each transform type produces correct partitioning

### Phase 4: Schema Evolution (1-2 days)

- [ ] Implement `evolve_schema()` for additive column changes
- [ ] Implement type promotion (int->long, float->double)
- [ ] Arrow-to-Iceberg schema conversion
- [ ] Test: schema evolution adds nullable columns
- [ ] Test: type promotion succeeds

### Phase 5: CDC/Upsert Mode (2-3 days)

- [ ] Implement `process_changelog_batch()` for F063 integration
- [ ] Implement equality delete file writing
- [ ] Map `CdcOperation` to insert/delete file separation
- [ ] Test: changelog stream with mixed insert/update/delete
- [ ] Test: upsert mode requires equality delete columns
- [ ] Test: equality delete files are correctly formatted

### Phase 6: Additional Catalogs (1-2 days)

- [ ] AWS Glue catalog integration
- [ ] Hive Metastore catalog integration
- [ ] In-memory catalog for testing
- [ ] Test: each catalog type connects and operates correctly

### Phase 7: Maintenance and SQL Integration (2-3 days)

- [ ] Implement `MaintenanceManager` with background compaction
- [ ] Implement snapshot expiry
- [ ] Implement orphan file cleanup
- [ ] Register `iceberg` connector in `ConnectorRegistry`
- [ ] Wire `CREATE SINK ... WITH (connector = 'iceberg')` to factory
- [ ] SQL integration tests with full DDL flow
- [ ] API documentation with examples

---

## 11. Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Ring 0 latency impact | < 5ns per event | Benchmark with/without Iceberg sink |
| Write throughput | > 200K records/sec | Benchmark with 1KB records to local storage |
| S3 write throughput | > 50K records/sec | Benchmark with 1KB records to S3 |
| Parquet file size | Within 10% of target | Verify file sizes on disk |
| Commit latency (local) | < 100ms | Benchmark commit_epoch() |
| Commit latency (REST catalog) | < 500ms | Benchmark commit_epoch() with REST catalog |
| Recovery time | < 5s | Benchmark open() with epoch resolution |
| Exactly-once verification | 0 duplicates after crash | Recovery integration test |
| Schema evolution | Additive columns work | Integration test |
| CDC/Upsert correctness | Insert/Update/Delete handled | Integration test with F063 changelog |
| Partition transforms | All 7 transforms correct | Unit tests per transform |
| Test coverage | > 80% | cargo tarpaulin |
| Test count | 30+ tests | cargo test |

---

## 12. Module Structure

```
crates/laminar-connectors/src/
├── lakehouse/
│   ├── mod.rs                    # Module declarations, shared types
│   ├── delta.rs                  # DeltaLakeSink (F031)
│   ├── iceberg.rs                # IcebergSink (F032)
│   ├── iceberg_catalog.rs        # Catalog builders (REST, Glue, Hive)
│   ├── iceberg_partition.rs      # Partition spec parsing and transforms
│   ├── iceberg_schema.rs         # Arrow <-> Iceberg schema conversion
│   ├── iceberg_maintenance.rs    # MaintenanceManager (compaction, expiry)
│   ├── iceberg_metrics.rs        # IcebergSinkMetrics
│   └── iceberg_cdc.rs            # CDC/upsert with equality deletes
```

---

## 13. Dependencies

### Rust Crates

| Crate | Version | Purpose |
|-------|---------|---------|
| `iceberg` | 0.4+ | Apache Iceberg core library (schema, partition, snapshot) |
| `iceberg-datafusion` | 0.4+ | DataFusion integration for reads (future source) |
| `iceberg-catalog-rest` | 0.4+ | REST catalog client |
| `iceberg-catalog-glue` | 0.4+ | AWS Glue catalog client |
| `iceberg-catalog-hive` | 0.4+ | Hive Metastore catalog client |
| `arrow` | 54+ | RecordBatch, Schema, Parquet integration |
| `parquet` | 54+ | Parquet file reading/writing |
| `object_store` | 0.12+ | Multi-cloud storage abstraction (S3, ADLS, GCS) |
| `tokio` | 1.x | Async runtime for Ring 1 I/O |
| `uuid` | 1.x | Writer ID generation |
| `tracing` | 0.1 | Structured logging |

### Feature Flags

```toml
[features]
iceberg-sink = ["dep:iceberg", "dep:iceberg-catalog-rest"]
iceberg-glue = ["iceberg-sink", "dep:iceberg-catalog-glue"]
iceberg-hive = ["iceberg-sink", "dep:iceberg-catalog-hive"]
iceberg-datafusion = ["iceberg-sink", "dep:iceberg-datafusion"]
```

---

## 14. References

1. **Apache Iceberg Specification** - Table format, schema evolution, partitioning
   - https://iceberg.apache.org/spec/

2. **iceberg-rust** - Native Rust Iceberg implementation
   - https://github.com/apache/iceberg-rust

3. **Iceberg REST Catalog OpenAPI Spec** - Standard catalog protocol
   - https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml

4. **Flink Iceberg Connector** - Exactly-once streaming writes pattern
   - Epoch tracking via snapshot summary properties

5. **F023: Exactly-Once Sinks** - LaminarDB transactional sink framework
   - Provides `SinkConnector` epoch-based commit protocol

6. **F034: Connector SDK** - LaminarDB connector trait hierarchy
   - Defines `SinkConnector`, `ConnectorRegistry`

7. **F063: Changelog/Retraction** - Z-set CDC format for upsert support
   - `CdcOperation` types for equality delete file generation

8. **Iceberg Equality Delete Files** - Row-level delete specification (v2)
   - https://iceberg.apache.org/spec/#equality-delete-files

9. **Data Lake Research** - `docs/research/laminardb-datalake-research.md`
   - Format comparison, Rust crate versions, integration strategy

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Apache Iceberg** | Open table format for huge analytic datasets with schema evolution, hidden partitioning, and time travel |
| **Catalog** | Central metadata service for Iceberg tables (REST, AWS Glue, Hive Metastore) |
| **Snapshot** | An immutable, consistent view of a table at a point in time |
| **Manifest List** | A file that lists all manifest files for a snapshot |
| **Manifest File** | A file that lists data files with column-level stats for partition pruning |
| **Data File** | A Parquet/ORC/Avro file containing table data |
| **Equality Delete File** | An Iceberg v2 file that identifies rows to delete by matching equality columns |
| **Partition Transform** | A function (identity, bucket, truncate, year/month/day/hour) applied to a column for partitioning |
| **Hidden Partitioning** | Partitioning that is invisible to queries — readers never need to know partition structure |
| **Schema Evolution** | Adding, dropping, renaming, or reordering columns without rewriting data |
| **Summary Properties** | Key-value metadata stored in each snapshot, used here for epoch tracking |
| **Optimistic Concurrency** | Iceberg's conflict resolution: retry commits on conflict with refreshed state |
| **RecordBatch** | Arrow columnar data structure; the native data format in LaminarDB's pipeline |
| **SinkConnector** | F034 trait for writing data to external systems with epoch-based transactions |
| **Z-Set** | F063 mathematical model where records carry integer weights (+1 insert, -1 delete) |

---

## Appendix B: Data Flow Diagram

```
                     LaminarDB Pipeline
                     ==================

  +---------+     +-----------+     +----------+     +-----------+
  | Source  |---->| Operators |---->| Ring 0   |---->|   SPSC    |
  | (Kafka) |     | (window,  |     | Emit     |     |  Channel  |
  |         |     |  join,    |     |          |     |           |
  +---------+     |  agg)     |     +----------+     +-----+-----+
                  +-----------+                            |
                                                           | Subscription
                                                           | (Ring 0 -> Ring 1)
                                                           v
                     +------------------------------------------+
                     |            IcebergSink (Ring 1)          |
                     |                                          |
                     |  +------------------+                    |
                     |  | RecordBatch      |                    |
                     |  | Buffer           |                    |
                     |  +--------+---------+                    |
                     |           |                              |
                     |           | flush (target size reached)  |
                     |           v                              |
                     |  +------------------+                    |
                     |  | Arrow -> Parquet |  (zero-copy)       |
                     |  | Writer (iceberg) |                    |
                     |  +--------+---------+                    |
                     |           |                              |
                     |     +-----+------+                       |
                     |     |            |                       |
                     |     v            v                       |
                     |  +------+  +--------+                    |
                     |  | Data |  | Delete |  (upsert mode)    |
                     |  | Files|  | Files  |                    |
                     |  +--+---+  +---+----+                    |
                     |     |          |                         |
                     |     +----+-----+                         |
                     |          |  commit_epoch()               |
                     |          v                               |
                     |  +------------------+                    |
                     |  | Iceberg Catalog  |                    |
                     |  | (REST/Glue/Hive) |                    |
                     |  | Snapshot Commit  |                    |
                     |  +--------+---------+                    |
                     |           |                              |
                     |           | Background (async)           |
                     |           v                              |
                     |  +------------------+                    |
                     |  | Maintenance      |                    |
                     |  | Manager          |                    |
                     |  | (compaction,     |                    |
                     |  |  snapshot expiry)|                    |
                     |  +------------------+                    |
                     +------------------------------------------+
                                 |
                                 v
                     +---------------------+
                     |   Storage Backend   |
                     |  Local / S3 / ADLS  |
                     |  / GCS              |
                     +---------------------+
                         |           |
                    +----+----+ +----+----+
                    | Parquet | | Metadata|
                    | Data    | | (JSON)  |
                    | Files   | |         |
                    +---------+ +---------+
```
