//! `PostgreSQL` sink connector implementation.
//!
//! [`PostgresSink`] implements [`SinkConnector`], writing Arrow `RecordBatch`
//! data to `PostgreSQL` tables via two strategies:
//!
//! - **Append mode**: COPY BINARY for maximum throughput (>500K rows/sec)
//! - **Upsert mode**: `INSERT ... ON CONFLICT DO UPDATE` with UNNEST arrays
//!
//! The connector provides durable at-least-once delivery. Typed runtime
//! admission excludes it from exactly-once pipelines because it has no
//! coordinated external checkpoint committer.
//!
//! # Ring Architecture
//!
//! - **Ring 0**: No sink code. Data arrives via SPSC channel (~5ns push).
//! - **Ring 1**: Batch buffering, COPY/INSERT writes, transaction management.
//! - **Ring 2**: Connection pool and table creation.

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tracing::{debug, info};

#[cfg(feature = "postgres-sink")]
use crate::changelog::collapse_changelog;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;

use super::sink_config::{PostgresSinkConfig, WriteMode};
use super::sink_metrics::PostgresSinkMetrics;

mod input;
mod statements;

use input::{build_user_schema, validate_sink_schema};
#[cfg(any(feature = "postgres-sink", test))]
use input::{collapse_upsert_batch, requires_preflush, retained_batch_bytes};
#[cfg(feature = "postgres-sink")]
use input::{
    execute_unnest, retained_batch_bytes_u64, strip_metadata_columns, validate_changelog_input,
    validate_input_batch,
};

#[cfg(feature = "postgres-sink")]
fn postgres_dispatched_write_error(
    operation: &str,
    error: &tokio_postgres::Error,
) -> ConnectorError {
    classify_postgres_write_failure(operation, error, error.as_db_error().is_some())
}

#[cfg(any(feature = "postgres-sink", test))]
fn classify_postgres_write_failure(
    operation: &str,
    error: &dyn std::fmt::Display,
    server_rejected: bool,
) -> ConnectorError {
    if server_rejected {
        ConnectorError::WriteError(format!("{operation}: {error}"))
    } else {
        ConnectorError::outcome_unknown(
            format!(
                "PostgreSQL {operation} failed without a server response and may have committed: {error}"
            ),
            true,
        )
    }
}

#[cfg(any(feature = "postgres-sink", test))]
fn resolve_uncommitted_transaction_error(error: ConnectorError) -> ConnectorError {
    match error {
        ConnectorError::OutcomeUnknown { message, retryable } if retryable => {
            ConnectorError::ConnectionFailed(format!(
                "PostgreSQL transaction mutation failed before COMMIT; no commit was dispatched: {message}"
            ))
        }
        ConnectorError::OutcomeUnknown { message, .. } => ConnectorError::TransactionError(
            format!(
                "PostgreSQL transaction mutation failed before COMMIT; no commit was dispatched: {message}"
            ),
        ),
        error => error,
    }
}
#[cfg(feature = "postgres-sink")]
use super::types::{arrow_column_to_pg_array, postgres_copy_batch};
#[cfg(feature = "postgres-sink")]
use bytes::BytesMut;
#[cfg(feature = "postgres-sink")]
use deadpool_postgres::Pool;

// A flush temporarily owns the buffered Arrow arrays plus a concatenated batch and either COPY
// wire bytes or owned UNNEST parameters. Keeping the retained input at 16 MiB leaves conservative
// headroom for those transient copies without adding another deployment tuning dimension.
#[cfg(feature = "postgres-sink")]
const MAX_BUFFERED_RETAINED_BYTES: usize = 16 * 1024 * 1024;
#[cfg(feature = "postgres-sink")]
const COPY_ENCODE_INITIAL_CAPACITY: usize = 64 * 1024;
#[cfg(feature = "postgres-sink")]
const SINK_POOL_SIZE: usize = 1;

#[cfg(feature = "postgres-sink")]
fn build_pool(config: &PostgresSinkConfig) -> Result<Pool, ConnectorError> {
    config.validate()?;

    let mut pool_config = deadpool_postgres::Config::new();
    pool_config.host = Some(config.hostname.clone());
    pool_config.port = Some(config.port);
    pool_config.dbname = Some(config.database.clone());
    pool_config.user = Some(config.username.clone());
    pool_config.password = Some(config.password.clone());
    pool_config.options = Some(config.statement_timeout_startup_option());
    pool_config.connect_timeout = Some(config.connect_timeout);
    pool_config.ssl_mode = Some(match config.ssl_mode {
        crate::postgres::SslMode::Disable => deadpool_postgres::SslMode::Disable,
        crate::postgres::SslMode::VerifyFull => deadpool_postgres::SslMode::Require,
    });
    let mut deadpool_config = deadpool_postgres::PoolConfig::new(SINK_POOL_SIZE);
    deadpool_config.timeouts.wait = Some(config.connect_timeout);
    deadpool_config.timeouts.create = Some(config.connect_timeout);
    pool_config.pool = Some(deadpool_config);

    let runtime = Some(deadpool_postgres::Runtime::Tokio1);
    match config.ssl_mode {
        crate::postgres::SslMode::Disable => pool_config
            .create_pool(runtime, tokio_postgres::NoTls)
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!("pool creation failed: {error}"))
            }),
        crate::postgres::SslMode::VerifyFull => {
            let tls = crate::postgres::make_rustls_connector(config.ssl_ca_cert_path.as_deref())?;
            pool_config.create_pool(runtime, tls).map_err(|error| {
                ConnectorError::ConnectionFailed(format!("TLS pool creation failed: {error}"))
            })
        }
    }
}

/// `PostgreSQL` sink connector.
///
/// Writes Arrow `RecordBatch` to `PostgreSQL` tables using COPY BINARY
/// (append) or UNNEST-based upsert with durable at-least-once semantics.
pub struct PostgresSink {
    /// Sink configuration.
    config: PostgresSinkConfig,
    /// Arrow schema for input batches.
    schema: SchemaRef,
    /// User-visible schema (metadata columns stripped).
    user_schema: SchemaRef,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Buffered records awaiting flush.
    buffer: Vec<RecordBatch>,
    /// Total rows in buffer.
    buffered_rows: usize,
    /// Arrow-reported retained bytes held by `buffer`.
    buffered_retained_bytes: usize,
    /// Sink metrics.
    metrics: PostgresSinkMetrics,
    /// Cached upsert SQL statement (for upsert mode).
    upsert_sql: Option<String>,
    /// Cached COPY SQL statement (for append mode).
    copy_sql: Option<String>,
    /// Cached CREATE TABLE SQL (for auto-create).
    create_table_sql: Option<String>,
    /// Cached DELETE SQL (for changelog mode).
    delete_sql: Option<String>,
    /// Connection pool (`None` until `open()` is called).
    #[cfg(feature = "postgres-sink")]
    pool: Option<Pool>,
}

impl PostgresSink {
    /// Creates a new `PostgreSQL` sink connector.
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        config: PostgresSinkConfig,
        registry: Option<&prometheus::Registry>,
    ) -> Self {
        let user_schema = build_user_schema(&schema);
        Self {
            config,
            schema,
            user_schema,
            state: ConnectorState::Created,
            buffer: Vec::with_capacity(4),
            buffered_rows: 0,
            buffered_retained_bytes: 0,
            metrics: PostgresSinkMetrics::new(registry),
            upsert_sql: None,
            copy_sql: None,
            create_table_sql: None,
            delete_sql: None,
            #[cfg(feature = "postgres-sink")]
            pool: None,
        }
    }

    /// Builds the engine-managed sink from its complete runtime configuration.
    /// Engine-managed sinks must carry the upstream Arrow schema; using a
    /// placeholder here would generate the wrong table and write statements.
    pub(crate) fn from_connector_config(
        config: &ConnectorConfig,
        registry: Option<&prometheus::Registry>,
    ) -> Result<Self, ConnectorError> {
        let (sink_config, schema) = Self::decode_connector_config(config)?;
        Ok(Self::new(schema, sink_config, registry))
    }

    fn decode_connector_config(
        config: &ConnectorConfig,
    ) -> Result<(PostgresSinkConfig, SchemaRef), ConnectorError> {
        let sink_config = PostgresSinkConfig::from_config(config)?;
        if config.get("_arrow_schema").is_none() {
            return Err(ConnectorError::ConfigurationError(
                "PostgreSQL sink requires the engine-injected '_arrow_schema'".into(),
            ));
        }
        let schema = config.arrow_schema().ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "invalid PostgreSQL sink '_arrow_schema' encoding".into(),
            )
        })?;
        validate_sink_schema(&schema, &sink_config)?;
        Ok((sink_config, schema))
    }

    fn apply_connector_config(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let (sink_config, schema) = Self::decode_connector_config(config)?;
        let user_schema = build_user_schema(&schema);
        self.config = sink_config;
        self.schema = schema;
        self.user_schema = user_schema;
        Ok(())
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Returns the number of buffered rows pending flush.
    #[must_use]
    pub fn buffered_rows(&self) -> usize {
        self.buffered_rows
    }

    #[cfg(feature = "postgres-sink")]
    fn retain_batch(&mut self, batch: &RecordBatch, retained_bytes: usize) {
        self.buffer.push(batch.clone());
        self.buffered_rows = self.buffered_rows.saturating_add(batch.num_rows());
        self.buffered_retained_bytes = self.buffered_retained_bytes.saturating_add(retained_bytes);
    }

    /// Moves pending batches out before any await so timeout cancellation cannot leave stale
    /// records or accounting in the connector.
    fn take_buffer(&mut self) -> Vec<RecordBatch> {
        self.buffered_rows = 0;
        self.buffered_retained_bytes = 0;
        std::mem::take(&mut self.buffer)
    }

    /// Returns a reference to the sink metrics.
    #[must_use]
    pub fn sink_metrics(&self) -> &PostgresSinkMetrics {
        &self.metrics
    }

    // ── Internal helpers ────────────────────────────────────────────

    /// Prepares cached SQL statements based on schema and config.
    fn prepare_statements(&mut self) -> Result<(), ConnectorError> {
        self.copy_sql = (self.config.write_mode == WriteMode::Append)
            .then(|| Self::build_copy_sql(&self.schema, &self.config))
            .transpose()?;
        self.upsert_sql = (self.config.write_mode == WriteMode::Upsert)
            .then(|| Self::build_upsert_sql(&self.schema, &self.config))
            .transpose()?;
        self.create_table_sql = self
            .config
            .auto_create_table
            .then(|| Self::build_create_table_sql(&self.schema, &self.config))
            .transpose()?;
        self.delete_sql = self
            .config
            .changelog_mode
            .then(|| Self::build_delete_sql(&self.schema, &self.config))
            .transpose()?;
        Ok(())
    }

    /// Returns a reference to the connection pool, or an error if not initialized.
    #[cfg(feature = "postgres-sink")]
    fn pool(&self) -> Result<&Pool, ConnectorError> {
        self.pool.as_ref().ok_or(ConnectorError::InvalidState {
            expected: "pool initialized (call open() first)".into(),
            actual: "pool not initialized".into(),
        })
    }

    /// Concatenates all buffered batches and strips metadata columns.
    #[cfg(feature = "postgres-sink")]
    fn concat_buffer(&self, buffer: &[RecordBatch]) -> Result<RecordBatch, ConnectorError> {
        if buffer.is_empty() {
            return Ok(RecordBatch::new_empty(self.user_schema.clone()));
        }

        let stripped: Result<Vec<RecordBatch>, ConnectorError> =
            buffer.iter().map(strip_metadata_columns).collect();
        let stripped = stripped?;

        arrow_select::concat::concat_batches(&self.user_schema, &stripped)
            .map_err(|e| ConnectorError::Internal(format!("batch concat failed: {e}")))
    }

    /// Flushes buffered data to `PostgreSQL` using the COPY BINARY protocol.
    #[cfg(feature = "postgres-sink")]
    async fn flush_append(
        &mut self,
        client: &tokio_postgres::Client,
        buffer: &[RecordBatch],
    ) -> Result<WriteResult, ConnectorError> {
        let user_batch = self.concat_buffer(buffer)?;
        if user_batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }
        let copy_batch = postgres_copy_batch(&user_batch)?;
        let mut encoder = pgpq::ArrowToPostgresBinaryEncoder::try_new(copy_batch.schema().as_ref())
            .map_err(|e| ConnectorError::Internal(format!("pgpq encoder init: {e}")))?;

        // `freeze()` transfers ownership to the COPY sink, so keeping this local also guarantees
        // cancellation releases partially encoded data.
        let mut encode_buf = BytesMut::with_capacity(COPY_ENCODE_INITIAL_CAPACITY);
        encoder.write_header(&mut encode_buf);
        encoder
            .write_batch(&copy_batch, &mut encode_buf)
            .map_err(|e| ConnectorError::Internal(format!("pgpq encode: {e}")))?;
        encoder
            .write_footer(&mut encode_buf)
            .map_err(|e| ConnectorError::Internal(format!("pgpq footer: {e}")))?;

        let encoded_bytes = encode_buf.len();
        let bytes_to_send = encode_buf.freeze();

        let copy_sql = self
            .copy_sql
            .as_deref()
            .ok_or_else(|| ConnectorError::Internal("COPY SQL not prepared".into()))?;

        let sink = client
            .copy_in(copy_sql)
            .await
            .map_err(|error| postgres_dispatched_write_error("COPY start", &error))?;

        {
            use futures_util::SinkExt;
            futures_util::pin_mut!(sink);
            sink.send(bytes_to_send)
                .await
                .map_err(|error| postgres_dispatched_write_error("COPY send", &error))?;
            sink.close()
                .await
                .map_err(|error| postgres_dispatched_write_error("COPY finish", &error))?;
        }

        let rows = user_batch.num_rows();
        self.metrics.record_write(rows as u64, encoded_bytes as u64);
        self.metrics.record_flush();
        self.metrics.record_copy();

        Ok(WriteResult::new(rows, encoded_bytes as u64))
    }

    /// Flushes buffered data to `PostgreSQL` using UNNEST-based upsert.
    #[cfg(feature = "postgres-sink")]
    #[allow(clippy::cast_possible_truncation)]
    async fn flush_upsert(
        &mut self,
        client: &mut tokio_postgres::Client,
        buffer: &[RecordBatch],
    ) -> Result<WriteResult, ConnectorError> {
        if self.config.changelog_mode {
            return self.flush_changelog(client, buffer).await;
        }

        let user_batch = self.concat_buffer(buffer)?;
        if user_batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }
        let user_batch = collapse_upsert_batch(&user_batch, &self.config.primary_key_columns)?;

        let upsert_sql = self
            .upsert_sql
            .as_deref()
            .ok_or_else(|| ConnectorError::Internal("upsert SQL not prepared".into()))?;

        let rows = execute_unnest(client, upsert_sql, &user_batch).await?;

        let byte_estimate = retained_batch_bytes_u64(&user_batch);
        self.metrics.record_write(rows, byte_estimate);
        self.metrics.record_flush();
        self.metrics.record_upsert();

        Ok(WriteResult::new(rows as usize, byte_estimate))
    }

    /// Flushes changelog batches per primary key: one collapsed terminal op each, then upserts
    /// before deletes.
    #[cfg(feature = "postgres-sink")]
    #[allow(clippy::cast_possible_truncation)]
    async fn flush_changelog(
        &mut self,
        client: &mut tokio_postgres::Client,
        buffer: &[RecordBatch],
    ) -> Result<WriteResult, ConnectorError> {
        if buffer.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }

        // Validate the entire flush window before issuing either the UPSERT or DELETE. A malformed
        // later batch must not be discovered after an earlier key has already mutated PostgreSQL.
        for batch in buffer {
            validate_changelog_input(batch)?;
        }

        // Collapse the whole buffered flush window per primary key into a cardinality-safe
        // `{U,D}` batch that `split_changelog_batch` understands. A Z-set changelog (`__weight`,
        // no `_op`) MUST be collapsed or its many retract+insert events per key fail the split /
        // violate ON CONFLICT cardinality. A CDC changelog (`_op`) MUST be collapsed too:
        // otherwise a delete-then-reinsert of a key in one flush window splits into both bins
        // and, applied upserts-then-deletes, wrongly ends deleted (CN-3). `collapse_changelog`
        // keeps the last arrival per key and normalizes `_op`, so each key contributes exactly
        // one terminal U or D.
        let split_input: Vec<RecordBatch> = {
            let schema = buffer[0].schema();
            let combined = arrow_select::concat::concat_batches(&schema, buffer)
                .map_err(|e| ConnectorError::Internal(format!("concat changelog: {e}")))?;
            vec![collapse_changelog(
                &combined,
                &self.config.primary_key_columns,
            )?]
        };

        // Split each batch into inserts/deletes.
        let mut all_inserts = Vec::new();
        let mut all_deletes = Vec::new();
        for batch in &split_input {
            let (ins, del) = Self::split_changelog_batch(batch)?;
            if ins.num_rows() > 0 {
                all_inserts.push(ins);
            }
            if del.num_rows() > 0 {
                all_deletes.push(del);
            }
        }

        let transaction = client.transaction().await.map_err(|error| {
            ConnectorError::ConnectionFailed(format!(
                "begin PostgreSQL changelog transaction: {error}"
            ))
        })?;
        let mutation = async {
            let mut upserted = 0_u64;
            let mut deleted = 0_usize;
            let mut bytes = 0_u64;

            if !all_inserts.is_empty() {
                let insert_batch =
                    arrow_select::concat::concat_batches(&self.user_schema, &all_inserts)
                        .map_err(|e| ConnectorError::Internal(format!("concat inserts: {e}")))?;
                let upsert_sql = self
                    .upsert_sql
                    .as_deref()
                    .ok_or_else(|| ConnectorError::Internal("upsert SQL not prepared".into()))?;
                upserted = execute_unnest(&transaction, upsert_sql, &insert_batch).await?;
                bytes = retained_batch_bytes_u64(&insert_batch);
            }

            if !all_deletes.is_empty() {
                let delete_batch =
                    arrow_select::concat::concat_batches(&self.user_schema, &all_deletes)
                        .map_err(|e| ConnectorError::Internal(format!("concat deletes: {e}")))?;
                bytes = bytes.saturating_add(retained_batch_bytes_u64(&delete_batch));
                deleted = self.execute_deletes(&transaction, &delete_batch).await?;
            }

            Ok::<_, ConnectorError>((upserted, deleted, bytes))
        }
        .await;

        let (upserted, deleted, total_bytes) = match mutation {
            Ok(result) => {
                transaction.commit().await.map_err(|error| {
                    postgres_dispatched_write_error("transaction COMMIT", &error)
                })?;
                result
            }
            Err(error) => {
                if let Err(rollback_error) = transaction.rollback().await {
                    tracing::warn!(
                        %rollback_error,
                        "PostgreSQL changelog rollback failed after a mutation error; no COMMIT was dispatched"
                    );
                }
                return Err(resolve_uncommitted_transaction_error(error));
            }
        };

        let total_rows = upserted.saturating_add(deleted as u64);
        if total_rows != 0 {
            self.metrics.record_write(total_rows, total_bytes);
        }
        if upserted != 0 {
            self.metrics.record_upsert();
        }
        if deleted != 0 {
            self.metrics.record_deletes(deleted as u64);
        }
        self.metrics.record_flush();
        Ok(WriteResult::new(total_rows as usize, total_bytes))
    }

    /// Executes batched DELETE for changelog delete records.
    #[cfg(feature = "postgres-sink")]
    #[allow(clippy::cast_possible_truncation)]
    async fn execute_deletes<C>(
        &self,
        client: &C,
        delete_batch: &RecordBatch,
    ) -> Result<usize, ConnectorError>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        if delete_batch.num_rows() == 0 {
            return Ok(0);
        }

        let delete_sql = self
            .delete_sql
            .as_deref()
            .ok_or_else(|| ConnectorError::Internal("DELETE SQL not prepared".into()))?;

        let pk_params: Vec<Box<dyn postgres_types::ToSql + Sync + Send>> = self
            .config
            .primary_key_columns
            .iter()
            .map(|col| {
                let idx = delete_batch.schema().index_of(col).map_err(|_| {
                    ConnectorError::ConfigurationError(format!(
                        "primary key column '{col}' not in delete batch"
                    ))
                })?;
                arrow_column_to_pg_array(delete_batch.column(idx))
            })
            .collect::<Result<_, _>>()?;

        let pk_refs: Vec<&(dyn postgres_types::ToSql + Sync)> = pk_params
            .iter()
            .map(|p| p.as_ref() as &(dyn postgres_types::ToSql + Sync))
            .collect();

        let rows = client
            .execute(delete_sql, &pk_refs)
            .await
            .map_err(|error| postgres_dispatched_write_error("DELETE", &error))?;

        Ok(rows as usize)
    }

    /// Dispatches flush to the appropriate write mode.
    #[cfg(feature = "postgres-sink")]
    async fn flush_to_client(
        &mut self,
        client: &mut tokio_postgres::Client,
        buffer: &[RecordBatch],
    ) -> Result<WriteResult, ConnectorError> {
        match self.config.write_mode {
            WriteMode::Append => self.flush_append(client, buffer).await,
            WriteMode::Upsert => self.flush_upsert(client, buffer).await,
        }
    }

    /// Flushes the current buffer, dropping the drained batches on every success, error, or
    /// cancellation path. The empty vector allocation is recovered after non-cancelled I/O.
    #[cfg(feature = "postgres-sink")]
    async fn flush_buffer(&mut self) -> Result<WriteResult, ConnectorError> {
        let mut pending = self.take_buffer();
        if pending.is_empty() {
            self.buffer = pending;
            return Ok(WriteResult::new(0, 0));
        }

        let result = async {
            let mut client = self
                .pool()?
                .get()
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(format!("pool checkout: {e}")))?;
            self.flush_to_client(&mut client, &pending).await
        }
        .await;

        pending.clear();
        self.buffer = pending;
        result
    }

    #[cfg(feature = "postgres-sink")]
    async fn write_batch_with_retained_limit(
        &mut self,
        batch: &RecordBatch,
        retained_limit: usize,
    ) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        validate_input_batch(batch, &self.schema, &self.config)?;

        if batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        let batch_retained_bytes = retained_batch_bytes(batch);
        let preflush = requires_preflush(
            self.buffered_retained_bytes,
            batch_retained_bytes,
            retained_limit,
        )?;
        let result = if preflush {
            self.flush_buffer().await?
        } else {
            WriteResult::new(0, 0)
        };

        self.retain_batch(batch, batch_retained_bytes);
        Ok(result)
    }
}

// ── SinkConnector implementation ────────────────────────────────────

#[cfg(feature = "postgres-sink")]
#[async_trait]
impl SinkConnector for PostgresSink {
    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let cfg = if config.properties().is_empty() {
            self.config.clone()
        } else {
            Self::decode_connector_config(config)?.0
        };
        cfg.validate()?;
        if config.properties().is_empty() {
            validate_sink_schema(&self.schema, &cfg)?;
        }
        let input_mode = if cfg.changelog_mode {
            SinkInputMode::FullChangelog
        } else if cfg.write_mode == WriteMode::Upsert {
            SinkInputMode::KeyedUpsert
        } else {
            SinkInputMode::AppendOnly
        };
        // Append-only writes commute across independent runtime writers. Mutable writes do not:
        // without key-affine placement and a fenced handoff, an older upsert/delete from one node
        // can land after a newer value from another node. Keep those modes singleton until the
        // runtime can prove that stronger topology protocol.
        let topology = if cfg.write_mode == WriteMode::Append {
            SinkTopology::MultiWriter
        } else {
            SinkTopology::Singleton
        };
        Ok(SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            topology,
            input_mode,
        ))
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if config.properties().is_empty() {
            // Direct programmatic construction supplies the schema to `new`.
            self.config.validate()?;
            validate_sink_schema(&self.schema, &self.config)?;
        } else {
            self.apply_connector_config(config)?;
        }

        // Complete all deterministic admission before touching the network.
        self.prepare_statements()?;
        let pool = build_pool(&self.config)?;
        self.state = ConnectorState::Initializing;

        info!(
            table = %self.config.qualified_table_name(),
            mode = %self.config.write_mode,
            "opening PostgreSQL sink connector"
        );

        // Validate connectivity.
        let client = pool.get().await.map_err(|e| {
            ConnectorError::ConnectionFailed(format!("initial connection failed: {e}"))
        })?;

        // Auto-create target table.
        if self.config.auto_create_table {
            if let Some(ddl) = &self.create_table_sql {
                client.batch_execute(ddl.as_str()).await.map_err(|e| {
                    ConnectorError::Internal(format!("auto-create table failed: {e}"))
                })?;
                debug!(table = %self.config.qualified_table_name(), "target table ensured");
            }
        }

        self.pool = Some(pool);
        self.state = ConnectorState::Running;

        info!(table = %self.config.qualified_table_name(), "PostgreSQL sink connector opened");

        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        self.write_batch_with_retained_limit(batch, MAX_BUFFERED_RETAINED_BYTES)
            .await
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn suggested_write_timeout(&self) -> Duration {
        let statement_budget = if self.config.changelog_mode {
            self.config.statement_timeout.saturating_mul(2)
        } else {
            self.config.statement_timeout
        };
        statement_budget + Duration::from_secs(5)
    }

    fn flush_interval(&self) -> Duration {
        self.config.flush_interval
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flush_buffer().await.map(|_| ())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing PostgreSQL sink connector");

        let flush_result = self.flush_buffer().await.map(|_| ());
        self.pool = None;
        self.state = ConnectorState::Closed;

        info!(
            table = %self.config.qualified_table_name(),
            records = self.metrics.records_written.get(),
            "PostgreSQL sink connector closed"
        );

        flush_result
    }
}

#[cfg(not(feature = "postgres-sink"))]
#[async_trait]
impl SinkConnector for PostgresSink {
    fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "PostgreSQL sink requires the 'postgres-sink' feature".into(),
        ))
    }

    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "PostgreSQL sink requires the 'postgres-sink' feature".into(),
        ))
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "PostgreSQL sink requires the 'postgres-sink' feature".into(),
        ))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn suggested_write_timeout(&self) -> Duration {
        self.config.statement_timeout + Duration::from_secs(5)
    }

    fn flush_interval(&self) -> Duration {
        self.config.flush_interval
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        drop(self.take_buffer());
        self.state = ConnectorState::Closed;
        Ok(())
    }
}

impl std::fmt::Debug for PostgresSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSink")
            .field("state", &self.state)
            .field("table", &self.config.qualified_table_name())
            .field("mode", &self.config.write_mode)
            .field("buffered_rows", &self.buffered_rows)
            .field("buffered_retained_bytes", &self.buffered_retained_bytes)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
