//! Delta Lake sink connector implementation.
//!
//! [`DeltaLakeSink`] implements [`SinkConnector`], writing Arrow `RecordBatch`
//! data to Delta Lake tables with ACID transactions and at-least-once delivery
//! (exactly-once is selected once at the pipeline/runtime boundary).
//!
//! # Write Strategies
//!
//! - **Append mode**: Arrow-to-Parquet zero-copy writes for immutable streams
//! - **Overwrite mode**: Replace partition contents for recomputation
//! - **Upsert mode**: CDC MERGE via Z-set changelog integration
//!
//! Exactly-once append writes prepare immutable Parquet descriptors during the
//! checkpoint and publish them with one namespaced, designated Delta commit.
//! At-least-once append, overwrite, and upsert writes commit through `flush()`.
//!
//! # Ring Architecture
//!
//! - **Ring 0**: No sink code. Data arrives via SPSC channel (~5ns push).
//! - **Ring 1**: Batch buffering, Parquet writes, Delta log commits.
//! - **Ring 2**: Schema management, configuration, health checks.

mod lifecycle;
mod operations;
mod publication;

#[cfg(feature = "delta-lake")]
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, RecordBatch};
#[cfg(feature = "delta-lake")]
use arrow_schema::DataType;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tracing::{debug, info, warn};

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

#[cfg(feature = "delta-lake")]
use deltalake::protocol::SaveMode;

use crate::config::{ConnectorConfig, ConnectorState};
#[cfg(feature = "delta-lake")]
use crate::connector::{ConnectorTaskGuard, MAX_COORDINATED_COMMIT_PAYLOAD_BYTES};
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use crate::storage::StorageProvider;

use super::delta_config::{DeltaLakeSinkConfig, DeltaWriteMode};
use super::delta_metrics::DeltaLakeSinkMetrics;
use crate::connector::DeliveryGuarantee;

#[cfg(feature = "delta-lake")]
use publication::{
    classify_delta_attempt_error, count_collapsed_ops, publish_coordinated_delta_batch,
    retry_coordinated_metadata_until, run_tracked_delta_task, DeltaWriteTaskSuccess,
    UnresolvedDeltaPublication,
};

/// Delta Lake sink connector.
///
/// Writes Arrow `RecordBatch` to Delta Lake tables with ACID transactions,
/// at-least-once delivery (exactly-once opt-in), partitioning, and
/// externally managed table maintenance.
///
/// # Exactly-Once Semantics
///
/// `pre_commit()` materializes immutable Parquet files and returns their Delta
/// `Add` actions. The runtime durably seals the checkpoint before one
/// designated committer calls `commit_aggregated()` with every participant's
/// descriptor. `rollback_epoch()` discards in-memory state; unreferenced files
/// are reclaimed later by retention-safe vacuum.
pub struct DeltaLakeSink {
    /// Sole admission authority for Delta tasks that may outlive a cancelled caller.
    #[cfg(feature = "delta-lake")]
    task_owner: ConnectorTaskOwner,
    /// Stable terminal observer retained by the runtime after this sink is retired.
    task_tracker: ConnectorTaskTracker,
    /// Sink configuration.
    config: DeltaLakeSinkConfig,
    /// Arrow schema for input batches (set on first write or from existing table).
    schema: Option<SchemaRef>,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Current epoch being written.
    current_epoch: u64,
    /// `RecordBatch` buffer for the current epoch.
    buffer: Vec<RecordBatch>,
    /// Total rows buffered in current epoch.
    buffered_rows: usize,
    /// Total bytes buffered (estimated) in current epoch.
    buffered_bytes: u64,
    /// Current Delta Lake table version.
    delta_version: u64,
    /// Time when the current buffer started accumulating.
    buffer_start_time: Option<Instant>,
    /// Sink metrics.
    metrics: DeltaLakeSinkMetrics,
    /// Delta Lake table handle (present when `delta-lake` feature is enabled).
    #[cfg(feature = "delta-lake")]
    table: Option<DeltaTable>,
    /// Staged batches ready for either coordinated descriptor creation or an
    /// at-least-once flush. This separation lets `rollback_epoch()` discard
    /// prepared in-memory data without publishing it to the Delta log.
    staged_batches: Vec<RecordBatch>,
    /// Rows staged for commit (mirrors `staged_batches`).
    staged_rows: usize,
    /// Estimated bytes staged for commit.
    staged_bytes: u64,
    /// Uncommitted `Add` actions for immutable Parquet files materialized in
    /// the current coordinated epoch.
    #[cfg(feature = "delta-lake")]
    coordinated_adds: Vec<deltalake::kernel::Add>,
    /// Canonical table incarnation and write-metadata fingerprint captured
    /// from the exact immutable snapshot used to create `coordinated_adds`.
    #[cfg(feature = "delta-lake")]
    coordinated_binding: Option<super::commit_descriptor::DeltaTableBinding>,
    /// Exact encoded descriptor size for `coordinated_adds`, or zero when the
    /// vector is empty.
    #[cfg(feature = "delta-lake")]
    coordinated_descriptor_bytes: usize,
    /// Exact publication that must be retried or observed at its target before
    /// this instance stages a later cut. Absence cannot resolve a timed-out
    /// remote catalog mutation because the server may still complete it.
    #[cfg(feature = "delta-lake")]
    coordinated_unresolved_publication: Arc<parking_lot::Mutex<Option<UnresolvedDeltaPublication>>>,
    /// Resolved table path after catalog lookup (may differ from `config.table_path`
    /// when using Unity/Glue catalogs). Used by `reopen_table()` so retries
    /// target the same resolved path that `open()` connected to.
    #[cfg(feature = "delta-lake")]
    resolved_table_path: String,
    /// Resolved storage options after catalog lookup.
    #[cfg(feature = "delta-lake")]
    resolved_storage_options: std::collections::HashMap<String, String>,
    /// When true, Delta table init is deferred until the first `write_batch()`
    /// provides a schema. This happens when Unity Catalog auto-create is
    /// configured but the pipeline schema is not yet available at `open()` time.
    #[cfg(feature = "delta-lake")]
    needs_deferred_delta_init: bool,
    /// Pre-built Parquet writer properties for hot-path writes. Built once
    /// in `init_delta_table()` from `config.parquet`; cloning this is far
    /// cheaper than rebuilding (string parsing, bloom-filter column setup)
    /// from scratch on every commit.
    #[cfg(feature = "delta-lake")]
    cached_writer_properties: Option<deltalake::parquet::file::properties::WriterProperties>,
    /// Shared `DataFusion` session for upsert/merge operations. Creating a
    /// fresh `SessionContext` per merge allocated a runtime env, memory
    /// pool, and object-store registry each commit; reusing one flattens
    /// allocator churn under steady-state upsert load.
    #[cfg(feature = "delta-lake")]
    merge_session: Option<datafusion::prelude::SessionContext>,
    /// A dispatched Delta operation lost its result, so this generation cannot
    /// safely admit another write even after its detached task terminates.
    #[cfg(feature = "delta-lake")]
    unresolved_delta_write: bool,
    /// Test hook that makes coordinated descriptor preparation remain pending.
    /// This is compiled out of production builds.
    #[cfg(all(test, feature = "delta-lake"))]
    stall_descriptor_write: bool,
}

/// Pre-creates a Unity Catalog external Delta table via the REST API if
/// `catalog.storage.location` is configured and a schema is available.
/// Idempotent: treats "already exists" (HTTP 409 / `ALREADY_EXISTS`) as success.
#[cfg(all(feature = "delta-lake", feature = "delta-lake-unity"))]
async fn ensure_uc_table_exists(
    config: &DeltaLakeSinkConfig,
    schema: Option<&SchemaRef>,
) -> Result<(), ConnectorError> {
    let super::delta_config::DeltaCatalogType::Unity {
        ref workspace_url,
        ref access_token,
    } = config.catalog_type
    else {
        return Ok(());
    };

    let Some(ref storage_location) = config.catalog_storage_location else {
        return Ok(());
    };

    let Some(arrow_schema) = schema else {
        warn!(
            "catalog.storage.location is set but no schema available — \
             skipping Unity Catalog auto-create"
        );
        return Ok(());
    };

    let catalog = config.catalog_name.as_deref().unwrap_or_default();
    let schema_name = config.catalog_schema.as_deref().unwrap_or_default();
    let table_name = config
        .table_path
        .strip_prefix("uc://")
        .and_then(|s| s.rsplit('.').next())
        .unwrap_or(&config.table_path);

    let columns = super::unity_catalog::arrow_to_uc_columns(arrow_schema);
    super::unity_catalog::create_uc_table(
        workspace_url,
        access_token,
        catalog,
        schema_name,
        table_name,
        storage_location,
        &columns,
    )
    .await
}

impl std::fmt::Debug for DeltaLakeSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaLakeSink")
            .field("state", &self.state)
            .field("table_path", &self.config.table_path)
            .field("mode", &self.config.write_mode)
            .field("guarantee", &self.config.delivery_guarantee)
            .field("current_epoch", &self.current_epoch)
            .field("buffered_rows", &self.buffered_rows)
            .field("delta_version", &self.delta_version)
            .finish_non_exhaustive()
    }
}

// ── Helper functions ────────────────────────────────────────────────

/// Filters a `RecordBatch` using a boolean mask and projects to the given column indices.
///
/// Takes `mask` by value to hand it straight to `BooleanArray::from` without
/// an intermediate `Vec<bool>` copy. Projects before filtering so the SIMD
/// kernel only walks user columns, not the dropped metadata columns.
fn filter_and_project(
    batch: &RecordBatch,
    mask: Vec<bool>,
    col_indices: &[usize],
) -> Result<RecordBatch, ConnectorError> {
    use arrow_array::BooleanArray;
    use arrow_select::filter::filter_record_batch;

    let bool_array = BooleanArray::from(mask);

    let projected = batch
        .project(col_indices)
        .map_err(|e| ConnectorError::Internal(format!("batch projection failed: {e}")))?;

    filter_record_batch(&projected, &bool_array)
        .map_err(|e| ConnectorError::Internal(format!("arrow filter failed: {e}")))
}

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_precision_loss)]
#[allow(clippy::float_cmp)]
mod tests;
