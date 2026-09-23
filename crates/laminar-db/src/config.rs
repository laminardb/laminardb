//! Configuration for `LaminarDB`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::path::PathBuf;

use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::streaming::{BackpressureStrategy, StreamCheckpointConfig};

use crate::error::DbError;

/// Default pipeline-wide lower-bound charge allowed for managed operator working state.
///
/// This execution budget is independent of checkpoint storage.
pub const DEFAULT_MAX_MANAGED_STATE_BYTES: usize = 256 * 1024 * 1024;

/// Default per-DB limit for participating `DataFusion` reservations (256 MiB).
/// This does not limit direct Arrow allocations or process RSS.
pub const DEFAULT_DATAFUSION_MEMORY_LIMIT_BYTES: usize = 256 * 1024 * 1024;

/// Default shared connector-to-coordinator Arrow-byte budget (64 MiB).
pub const DEFAULT_SOURCE_QUEUE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Default maximum number of live rows in each local reference table.
pub const DEFAULT_REFERENCE_TABLE_MAX_ROWS: usize = 1_000_000;

/// Default retained-memory charge limit for each local reference table (256 MiB).
pub const DEFAULT_REFERENCE_TABLE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Default live row limit per local materialized view (distinct rows for multisets).
pub const DEFAULT_MATERIALIZED_VIEW_MAX_ROWS: usize = 1_000_000;

/// Default retained-memory charge limit per local materialized view (256 MiB).
pub const DEFAULT_MATERIALIZED_VIEW_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Validate the live row and retained-memory limits for each local materialized view.
///
/// # Errors
/// Returns an error if either limit is zero.
pub fn validate_materialized_view_limits(rows: usize, bytes: usize) -> Result<(), &'static str> {
    if rows == 0 || bytes == 0 {
        return Err(
            "materialized_view_max_rows and materialized_view_max_bytes must be greater than zero",
        );
    }
    Ok(())
}

/// Validate per-table live row and retained-memory limits.
///
/// # Errors
/// Returns an error if either limit is zero.
pub fn validate_reference_table_limits(rows: usize, bytes: usize) -> Result<(), &'static str> {
    if rows == 0 || bytes == 0 {
        return Err(
            "reference_table_max_rows and reference_table_max_bytes must be greater than zero",
        );
    }
    Ok(())
}

/// Largest source queue budget supported by byte-sized semaphore reservations.
pub const MAX_SOURCE_QUEUE_BYTES: usize = if tokio::sync::Semaphore::MAX_PERMITS < u32::MAX as usize
{
    tokio::sync::Semaphore::MAX_PERMITS
} else {
    u32::MAX as usize
};

/// Validates the shared source queue byte budget before connector I/O.
///
/// # Errors
/// Returns an error for zero or values above [`MAX_SOURCE_QUEUE_BYTES`].
pub fn validate_source_queue_max_bytes(bytes: usize) -> Result<(), &'static str> {
    if bytes == 0 || bytes > MAX_SOURCE_QUEUE_BYTES {
        return Err("source_queue_max_bytes must be nonzero and at most MAX_SOURCE_QUEUE_BYTES");
    }
    Ok(())
}

pub(crate) fn event_time_max_future_skew_ms(
    skew: std::time::Duration,
) -> Result<i64, &'static str> {
    let skew_ms = i64::try_from(skew.as_millis())
        .map_err(|_| "event_time_max_future_skew exceeds the supported millisecond range")?;
    if !skew.is_zero() && skew_ms == 0 {
        return Err("event_time_max_future_skew must be zero or at least 1ms");
    }
    Ok(skew_ms)
}

pub(crate) fn source_idle_timeout_ms(
    timeout: Option<std::time::Duration>,
) -> Result<Option<u64>, &'static str> {
    let Some(timeout) = timeout else {
        return Ok(None);
    };
    let timeout_ms = u64::try_from(timeout.as_millis())
        .map_err(|_| "source_idle_timeout exceeds the supported millisecond range")?;
    if timeout_ms == 0 {
        return Err("source_idle_timeout must be at least 1ms");
    }
    Ok(Some(timeout_ms))
}

pub(crate) fn temporal_join_idle_history_retention_ms(
    retention: Option<std::time::Duration>,
) -> Result<i64, &'static str> {
    let retention = retention
        .ok_or("temporal_join_idle_history_retention must be configured for temporal joins")?;
    let retention_ms = i64::try_from(retention.as_millis()).map_err(|_| {
        "temporal_join_idle_history_retention exceeds the supported millisecond range"
    })?;
    if retention_ms == 0 {
        return Err("temporal_join_idle_history_retention must be at least 1ms");
    }
    Ok(retention_ms)
}

/// What to do when an operator's input buffer exceeds its cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressurePolicy {
    /// Defer producers before execution when a downstream port cannot accept input.
    /// An executed result that exceeds a port's prospective limit halts the pipeline;
    /// it cannot safely be retried against already mutated operator state.
    #[default]
    Backpressure,
    /// Drop oldest batches; counted in `shed_records_total`.
    /// Available only with [`DeliveryGuarantee::BestEffort`].
    ShedOldest,
    /// Error out the cycle.
    Fail,
}

/// String wrapper whose `Debug` redacts the value, for credentials in [`LaminarConfig`].
#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the underlying secret. Call only at the point of use.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("\"[REDACTED]\"")
    }
}

/// Auto-restart policy for the fault supervisor (see `LaminarDB::enable_supervision`).
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    /// Max restarts within `window` before the pipeline is left hard-faulted.
    pub max_restarts: usize,
    /// Sliding window over which `max_restarts` is counted.
    pub window: std::time::Duration,
    /// Backoff before the first restart in a window.
    pub initial_backoff: std::time::Duration,
    /// Cap on the exponential backoff.
    pub max_backoff: std::time::Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            window: std::time::Duration::from_secs(60),
            initial_backoff: std::time::Duration::from_millis(500),
            max_backoff: std::time::Duration::from_secs(30),
        }
    }
}

/// Configuration for a `LaminarDB` instance.
#[derive(Debug, Clone)]
pub struct LaminarConfig {
    /// Live row limit per local MV; multiset mode counts distinct rows (default 1,000,000).
    /// Applies to embedded and single-node execution; must be nonzero.
    /// Keyed staging and each input batch allow at most twice this many rows.
    pub materialized_view_max_rows: usize,
    /// Per-MV retained-memory charge limit (default 256 MiB); must be nonzero.
    /// Counts Arrow backing capacity, owned scalar/key storage and fixed entry metadata.
    /// Shared buffers are conservatively charged per stored batch/scalar. Map spare capacity,
    /// schema/converter/allocator overhead, snapshots and checkpoint scratch are separate.
    /// Keyed staging and input conversion have separate limits of twice this charge plus
    /// bounded staging metadata. Oversized net-neutral cycles can fail before live admission.
    /// Append mode evicts oldest batches; an oversized single batch fails.
    pub materialized_view_max_bytes: usize,
    /// Maximum live rows in each reference table (default 1,000,000).
    /// Applies to embedded and single-node tables; must be nonzero.
    pub reference_table_max_rows: usize,
    /// Per-table encoded-key, row metadata and retained Arrow capacity charge (default 256 MiB).
    /// Shared buffers count once per table, using Arrow-reported allocation capacity.
    /// Hash-map spare capacity, schema/allocator/opaque-owner overhead, caller-held data, prepared
    /// replacements, query snapshots and checkpoint scratch are separate from this live limit.
    pub reference_table_max_bytes: usize,
    /// Shared limit for participating `DataFusion` reservations across this DB's contexts.
    /// Must be greater than zero; defaults to [`DEFAULT_DATAFUSION_MEMORY_LIMIT_BYTES`].
    /// DB-owned contexts cannot spill to disk. Direct Arrow allocations, managed state,
    /// queues, connector-owned contexts and process RSS are outside this budget.
    pub datafusion_memory_limit_bytes: usize,
    /// Streaming channel buffer size.
    pub default_buffer_size: usize,
    /// Per-source Arrow-byte limit for in-process push rings and queued broadcast data.
    /// Each source's snapshot history has a separate cap of the same size (default 64 MiB).
    /// Applies to typed handles after Arrow conversion. Caller-held batches, conversion
    /// scratch, query snapshots and downstream buffers are outside these two limits.
    pub push_source_max_bytes: usize,
    /// Backpressure strategy.
    pub default_backpressure: BackpressureStrategy,
    /// Checkpoint directory. `None` = in-memory only.
    pub storage_dir: Option<PathBuf>,
    /// Checkpoint config. `None` = disabled.
    pub checkpoint: Option<StreamCheckpointConfig>,
    /// Emit dirty-only changelogs for keyed non-windowed aggregate materialized views instead of
    /// re-materializing every group each cycle. This is query execution policy, not checkpointing.
    pub incremental_emit: bool,
    /// Cloud checkpoint URL, e.g. `s3://bucket/prefix`.
    pub object_store_url: Option<String>,
    /// Credential/config overrides for the object store.
    pub object_store_options: HashMap<String, String>,
    /// Bearer token presented when forwarding requests to the cluster leader's
    /// HTTP API (set when the server gates `/api/v1` with `console_token`).
    pub http_auth_token: Option<SecretString>,
    /// Delivery guarantee.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Source-to-coordinator channel capacity. `None` = 64.
    pub pipeline_channel_capacity: Option<usize>,
    /// Shared connector FIFO Arrow-byte budget, including parked messages (default 64 MiB).
    /// Also caps each source's one batch waiting for capacity or a cursor. Does not cover
    /// connector decode scratch, embedded push rings, staged cycles, graph buffers or RSS.
    pub source_queue_max_bytes: usize,
    /// Micro-batch coalescing window. `None` = 5ms connectors / 0 embedded.
    pub pipeline_batch_window: Option<std::time::Duration>,
    /// Drain budget per cycle (ns). `None` = 1ms.
    pub pipeline_drain_budget_ns: Option<u64>,
    /// Per-query budget (ns). `None` = 8ms.
    pub pipeline_query_budget_ns: Option<u64>,
    /// Per-port operator input-buffer cap (batches). `None` = 256.
    pub pipeline_max_input_buf_batches: Option<usize>,
    /// Per-port retained Arrow-byte cap, including source priming. `None` = disabled.
    /// Slices charge their backing storage; each fan-out port charges independently.
    pub pipeline_max_input_buf_bytes: Option<usize>,
    /// Pipeline-wide managed working-state budget in charged bytes. `None` resolves to
    /// [`DEFAULT_MAX_MANAGED_STATE_BYTES`] when the database is constructed.
    pub pipeline_max_managed_state_bytes: Option<usize>,
    /// Retention contract for right-side history while a temporal join input is idle.
    /// Required only when the pipeline contains a temporal join.
    pub temporal_join_idle_history_retention: Option<std::time::Duration>,
    /// Mark inactive watermarked sources and input channels idle after this duration.
    /// `None` disables automatic idle detection.
    pub source_idle_timeout: Option<std::time::Duration>,
    /// Event timestamps farther ahead of wall clock do not advance source watermarks.
    /// Zero disables the guard.
    pub event_time_max_future_skew: std::time::Duration,
    /// Backpressure policy. [`BackpressurePolicy::ShedOldest`] is `BestEffort` only.
    pub pipeline_backpressure_policy: BackpressurePolicy,
    /// Auto-restart policy applied when supervision is enabled.
    pub restart_policy: RestartPolicy,
    /// Isolate queries that share a source into independent failure domains.
    /// Default off; when off, shared-source queries fault and recover together.
    pub shared_source_isolation: bool,
}

impl LaminarConfig {
    pub(crate) fn validate_and_normalize(&mut self) -> Result<(), DbError> {
        validate_materialized_view_limits(
            self.materialized_view_max_rows,
            self.materialized_view_max_bytes,
        )
        .map_err(|error| DbError::Config(error.into()))?;
        validate_reference_table_limits(
            self.reference_table_max_rows,
            self.reference_table_max_bytes,
        )
        .map_err(|error| DbError::Config(error.into()))?;
        if self.datafusion_memory_limit_bytes == 0 {
            return Err(DbError::Config(
                "datafusion_memory_limit_bytes must be greater than zero".into(),
            ));
        }
        validate_source_queue_max_bytes(self.source_queue_max_bytes)
            .map_err(|error| DbError::Config(error.into()))?;
        laminar_core::streaming::validate_source_max_queued_bytes(self.push_source_max_bytes)
            .map_err(|error| DbError::Config(format!("push_source_max_bytes: {error}")))?;
        self.source_idle_timeout = source_idle_timeout_ms(self.source_idle_timeout)
            .map_err(|error| DbError::Config(error.to_string()))?
            .map(std::time::Duration::from_millis);
        let future_skew_ms = event_time_max_future_skew_ms(self.event_time_max_future_skew)
            .map_err(|error| DbError::Config(error.to_string()))?;
        self.event_time_max_future_skew =
            std::time::Duration::from_millis(future_skew_ms.unsigned_abs());
        let max_managed_state_bytes = self
            .pipeline_max_managed_state_bytes
            .unwrap_or(DEFAULT_MAX_MANAGED_STATE_BYTES);
        if max_managed_state_bytes == 0 {
            return Err(DbError::Config(
                "pipeline_max_managed_state_bytes must be greater than zero".into(),
            ));
        }
        self.pipeline_max_managed_state_bytes = Some(max_managed_state_bytes);

        if let Some(checkpoint) = self.checkpoint.as_mut() {
            let max_node_data_bytes = checkpoint.max_node_data_bytes.unwrap_or(
                laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES,
            );
            laminar_core::checkpoint::checkpoint_store::validate_max_checkpoint_node_data_bytes(
                max_node_data_bytes,
            )
            .map_err(|error| DbError::Config(format!("checkpoint.max_node_data_bytes: {error}")))?;
            checkpoint.max_node_data_bytes = Some(max_node_data_bytes);
        }

        self.validate_backpressure_policy()
    }

    pub(crate) fn validate_backpressure_policy(&self) -> Result<(), DbError> {
        if self.pipeline_max_input_buf_bytes == Some(0) {
            return Err(DbError::Config(
                "pipeline_max_input_buf_bytes must be greater than zero when configured".into(),
            ));
        }
        let policy = self.pipeline_backpressure_policy;
        if policy == BackpressurePolicy::Backpressure {
            return Ok(());
        }

        let has_count_cap = self.pipeline_max_input_buf_batches.is_none_or(|c| c > 0);
        let has_byte_cap = self.pipeline_max_input_buf_bytes.is_some_and(|b| b > 0);
        if !has_count_cap && !has_byte_cap {
            return Err(DbError::Config(format!(
                "backpressure_policy={policy:?} requires at least one of \
                 pipeline_max_input_buf_batches (>0) or pipeline_max_input_buf_bytes"
            )));
        }

        if policy == BackpressurePolicy::ShedOldest
            && self.delivery_guarantee != DeliveryGuarantee::BestEffort
        {
            return Err(DbError::Config(
                "ShedOldest drops data and supports BestEffort only; at-least-once and \
                 exactly-once delivery require Backpressure or Fail."
                    .into(),
            ));
        }
        Ok(())
    }
}

impl Default for LaminarConfig {
    fn default() -> Self {
        Self {
            reference_table_max_rows: DEFAULT_REFERENCE_TABLE_MAX_ROWS,
            reference_table_max_bytes: DEFAULT_REFERENCE_TABLE_MAX_BYTES,
            materialized_view_max_rows: DEFAULT_MATERIALIZED_VIEW_MAX_ROWS,
            materialized_view_max_bytes: DEFAULT_MATERIALIZED_VIEW_MAX_BYTES,
            datafusion_memory_limit_bytes: DEFAULT_DATAFUSION_MEMORY_LIMIT_BYTES,
            default_buffer_size: 65536,
            push_source_max_bytes: laminar_core::streaming::DEFAULT_SOURCE_MAX_QUEUED_BYTES,
            default_backpressure: BackpressureStrategy::Block,
            storage_dir: None,
            checkpoint: None,
            incremental_emit: true,
            object_store_url: None,
            object_store_options: HashMap::new(),
            http_auth_token: None,
            delivery_guarantee: DeliveryGuarantee::default(),
            pipeline_channel_capacity: None,
            source_queue_max_bytes: DEFAULT_SOURCE_QUEUE_MAX_BYTES,
            pipeline_batch_window: None,
            pipeline_drain_budget_ns: None,
            pipeline_query_budget_ns: None,
            pipeline_max_input_buf_batches: None,
            pipeline_max_input_buf_bytes: None,
            pipeline_max_managed_state_bytes: None,
            temporal_join_idle_history_retention: None,
            source_idle_timeout: None,
            event_time_max_future_skew: std::time::Duration::from_millis(
                laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS.unsigned_abs(),
            ),
            pipeline_backpressure_policy: BackpressurePolicy::default(),
            restart_policy: RestartPolicy::default(),
            shared_source_isolation: false,
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn materialized_view_limits_apply_through_config_and_builder() {
        use super::*;
        assert_eq!(
            LaminarConfig::default().materialized_view_max_rows,
            1_000_000
        );
        assert_eq!(
            LaminarConfig::default().materialized_view_max_bytes,
            256 * 1024 * 1024
        );
        for (rows, bytes) in [(0, 1024), (1, 0)] {
            assert!(matches!(
                crate::LaminarDB::open_with_config(LaminarConfig {
                    materialized_view_max_rows: rows,
                    materialized_view_max_bytes: bytes,
                    ..Default::default()
                }),
                Err(DbError::Config(_))
            ));
            assert!(matches!(
                crate::LaminarDB::builder()
                    .materialized_view_max_rows(rows)
                    .materialized_view_max_bytes(bytes)
                    .build()
                    .await,
                Err(DbError::Config(_))
            ));
        }
        let db = crate::LaminarDB::builder()
            .materialized_view_max_rows(1)
            .materialized_view_max_bytes(8192)
            .build()
            .await
            .unwrap();
        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![
                1, 2,
            ]))],
        )
        .unwrap();
        let mut store = db.mv_store.write();
        store
            .create_mv(
                "v",
                batch.schema(),
                crate::mv_store::MvStorageMode::Aggregate,
            )
            .unwrap();
        assert!(matches!(
            store.update_cycle("v", &[batch]),
            Err(DbError::MaterializedViewQuotaExceeded { max_rows: 1, .. })
        ));
    }

    use super::*;

    #[tokio::test]
    async fn reference_table_limits_apply_through_config_and_builder() {
        assert_eq!(LaminarConfig::default().reference_table_max_rows, 1_000_000);
        assert_eq!(
            LaminarConfig::default().reference_table_max_bytes,
            256 * 1024 * 1024
        );
        for (rows, bytes) in [(0, 1024), (1, 0)] {
            let error = crate::LaminarDB::open_with_config(LaminarConfig {
                reference_table_max_rows: rows,
                reference_table_max_bytes: bytes,
                ..Default::default()
            })
            .err()
            .unwrap();
            assert!(matches!(error, DbError::Config(_)));
            let error = crate::LaminarDB::builder()
                .reference_table_max_rows(rows)
                .reference_table_max_bytes(bytes)
                .build()
                .await
                .err()
                .unwrap();
            assert!(matches!(error, DbError::Config(_)));
        }
        let db = crate::LaminarDB::builder()
            .reference_table_max_rows(1)
            .reference_table_max_bytes(8192)
            .build()
            .await
            .unwrap();
        db.execute("CREATE TABLE dimensions (id BIGINT PRIMARY KEY, value VARCHAR)")
            .await
            .unwrap();
        db.execute("INSERT INTO dimensions VALUES (1, 'first')")
            .await
            .unwrap();
        assert!(matches!(
            db.execute("INSERT INTO dimensions VALUES (1, 'changed'), (2, 'new')")
                .await,
            Err(DbError::ReferenceTableQuotaExceeded {
                rows: 2,
                max_rows: 1,
                ..
            })
        ));
        let result = db.execute("SELECT value FROM dimensions").await.unwrap();
        let crate::ExecuteResult::Query(mut query) = result else {
            panic!("expected query");
        };
        let mut subscription = query.subscribe_raw().unwrap();
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(5), subscription.recv_async())
                .await
                .unwrap()
                .unwrap();
        let value = output
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(value.value(0), "first");
    }

    #[tokio::test]
    async fn push_source_limits_are_validated_at_db_entry() {
        assert_eq!(
            LaminarConfig::default().push_source_max_bytes,
            64 * 1024 * 1024
        );
        for bytes in [0, laminar_core::streaming::MAX_SOURCE_QUEUED_BYTES + 1] {
            let error = crate::LaminarDB::open_with_config(LaminarConfig {
                push_source_max_bytes: bytes,
                ..Default::default()
            })
            .err()
            .unwrap();
            assert!(
                matches!(error, DbError::Config(ref message) if message.contains("push_source_max_bytes"))
            );
            let error = crate::LaminarDB::builder()
                .push_source_max_bytes(bytes)
                .build()
                .await
                .err()
                .unwrap();
            assert!(
                matches!(error, DbError::Config(ref message) if message.contains("push_source_max_bytes"))
            );
        }
    }

    #[tokio::test]
    async fn source_queue_limits_are_validated_at_db_entry() {
        assert_eq!(
            LaminarConfig::default().source_queue_max_bytes,
            64 * 1024 * 1024
        );
        for bytes in [0, MAX_SOURCE_QUEUE_BYTES + 1] {
            let error = crate::LaminarDB::open_with_config(LaminarConfig {
                source_queue_max_bytes: bytes,
                ..Default::default()
            })
            .err()
            .unwrap();
            assert!(
                matches!(error, DbError::Config(ref message) if message.contains("source_queue_max_bytes"))
            );
            let error = crate::LaminarDB::builder()
                .source_queue_max_bytes(bytes)
                .build()
                .await
                .err()
                .unwrap();
            assert!(
                matches!(error, DbError::Config(ref message) if message.contains("source_queue_max_bytes"))
            );
        }
        for bytes in [1, MAX_SOURCE_QUEUE_BYTES] {
            assert!(validate_source_queue_max_bytes(bytes).is_ok());
        }
    }
}
