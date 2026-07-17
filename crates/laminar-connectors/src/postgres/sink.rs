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

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use tracing::{debug, info};

use crate::changelog::collapse_changelog;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;

use super::sink_config::{
    quote_sql_identifier, validate_sql_identifier, PostgresSinkConfig, WriteMode,
};
use super::sink_metrics::PostgresSinkMetrics;

#[cfg(feature = "postgres-sink")]
fn postgres_dispatched_write_error(
    operation: &str,
    error: tokio_postgres::Error,
) -> ConnectorError {
    classify_postgres_write_failure(operation, &error, error.as_db_error().is_some())
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
use super::types::{
    arrow_to_pg_ddl_type, arrow_type_to_pg_array_cast, arrow_type_to_pg_sql, postgres_type,
};

#[cfg(feature = "postgres-sink")]
use super::types::{arrow_column_to_pg_array, postgres_copy_batch, validate_postgres_array_values};
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

const CHANGELOG_METADATA_COLUMNS: &[&str] = &["_op", "_ts_ms", "__weight"];

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

    // ── SQL Generation ──────────────────────────────────────────────

    /// Builds the COPY BINARY SQL statement.
    ///
    /// ```sql
    /// COPY "public"."events" ("id", "value", "ts") FROM STDIN BINARY
    /// ```
    pub fn build_copy_sql(
        schema: &SchemaRef,
        config: &PostgresSinkConfig,
    ) -> Result<String, ConnectorError> {
        validate_sink_schema(schema, config)?;
        let columns = quoted_user_columns(schema);
        Ok(format!(
            "COPY {} ({}) FROM STDIN BINARY",
            config.qualified_table_name(),
            columns.join(", "),
        ))
    }

    /// Builds the UNNEST-based upsert SQL statement.
    ///
    /// ```sql
    /// INSERT INTO "public"."target" ("id", "value", "updated_at")
    /// SELECT * FROM UNNEST($1::int8[], $2::text[], $3::timestamptz[])
    /// ON CONFLICT (id) DO UPDATE SET
    ///     value = EXCLUDED.value,
    ///     updated_at = EXCLUDED.updated_at
    /// ```
    pub fn build_upsert_sql(
        schema: &SchemaRef,
        config: &PostgresSinkConfig,
    ) -> Result<String, ConnectorError> {
        validate_sink_schema(schema, config)?;
        let fields = user_fields(schema);

        let columns: Vec<String> = fields
            .iter()
            .map(|field| quote_sql_identifier(field.name()))
            .collect();

        let unnest_params: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| arrow_type_to_pg_array_cast(field.data_type(), i + 1))
            .collect::<Result<_, _>>()?;

        let non_key_columns: Vec<&Arc<Field>> = fields
            .iter()
            .copied()
            .filter(|field| {
                !config
                    .primary_key_columns
                    .iter()
                    .any(|primary_key| primary_key == field.name())
            })
            .collect();

        let update_clause: Vec<String> = non_key_columns
            .iter()
            .map(|field| {
                let column = quote_sql_identifier(field.name());
                format!("{column} = EXCLUDED.{column}")
            })
            .collect();

        let pk_list = config
            .primary_key_columns
            .iter()
            .map(|column| quote_sql_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        if update_clause.is_empty() {
            // Key-only table: use DO NOTHING
            Ok(format!(
                "INSERT INTO {} ({}) \
                 SELECT * FROM UNNEST({}) \
                 ON CONFLICT ({}) DO NOTHING",
                config.qualified_table_name(),
                columns.join(", "),
                unnest_params.join(", "),
                pk_list,
            ))
        } else {
            Ok(format!(
                "INSERT INTO {} ({}) \
                 SELECT * FROM UNNEST({}) \
                 ON CONFLICT ({}) DO UPDATE SET {}",
                config.qualified_table_name(),
                columns.join(", "),
                unnest_params.join(", "),
                pk_list,
                update_clause.join(", "),
            ))
        }
    }

    /// Builds the DELETE SQL for changelog deletes. One array parameter is bound per primary-key
    /// column (`$1`, `$2`, …), each holding that column's values for the batch's deleted keys.
    ///
    /// ```sql
    /// -- single PK
    /// DELETE FROM "public"."events" WHERE "id" = ANY($1::int8[])
    /// -- composite PK: match tuple-wise via UNNEST, not the cross-product
    /// DELETE FROM "public"."events" AS "target"
    ///   USING UNNEST($1::int8[], $2::text[]) AS "keys"("id", "name")
    /// ```
    pub fn build_delete_sql(
        schema: &SchemaRef,
        config: &PostgresSinkConfig,
    ) -> Result<String, ConnectorError> {
        validate_sink_schema(schema, config)?;
        let pg_type = |column: &str| -> Result<&'static str, ConnectorError> {
            let field = schema.field_with_name(column).map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "primary key column '{column}' is not present in PostgreSQL sink schema"
                ))
            })?;
            arrow_type_to_pg_sql(field.data_type())
        };
        let pk = &config.primary_key_columns;

        // A single PK column can use a plain ANY(); a composite PK must match keys tuple-wise, or
        // `col1 = ANY($1) AND col2 = ANY($2)` deletes the cross-product — e.g. deleting (1,'a') and
        // (2,'b') would also delete (1,'b') and (2,'a') (CN-2). UNNEST zips the arrays positionally.
        if pk.len() <= 1 {
            let column = pk.first().ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "PostgreSQL changelog delete requires a primary key".into(),
                )
            })?;
            let quoted_column = quote_sql_identifier(column);
            Ok(format!(
                "DELETE FROM {} WHERE {quoted_column} = ANY($1::{}[])",
                config.qualified_table_name(),
                pg_type(column)?,
            ))
        } else {
            let unnest_args: Vec<String> = pk
                .iter()
                .enumerate()
                .map(|(i, column)| Ok(format!("${}::{}[]", i + 1, pg_type(column)?)))
                .collect::<Result<_, ConnectorError>>()?;
            let quoted_keys: Vec<String> = pk
                .iter()
                .map(|column| quote_sql_identifier(column))
                .collect();
            let target_alias = quote_sql_identifier("target");
            let key_alias = quote_sql_identifier("keys");
            let match_conditions: Vec<String> = quoted_keys
                .iter()
                .map(|column| format!("{target_alias}.{column} = {key_alias}.{column}"))
                .collect();
            Ok(format!(
                "DELETE FROM {} AS {target_alias} USING UNNEST({}) AS {key_alias}({}) WHERE {}",
                config.qualified_table_name(),
                unnest_args.join(", "),
                quoted_keys.join(", "),
                match_conditions.join(" AND "),
            ))
        }
    }

    /// Builds CREATE TABLE DDL from the Arrow schema.
    ///
    /// ```sql
    /// CREATE TABLE IF NOT EXISTS "public"."events" (
    ///     "id" BIGINT NOT NULL,
    ///     "value" TEXT,
    ///     "ts" TIMESTAMPTZ,
    ///     PRIMARY KEY ("id")
    /// )
    /// ```
    pub fn build_create_table_sql(
        schema: &SchemaRef,
        config: &PostgresSinkConfig,
    ) -> Result<String, ConnectorError> {
        validate_sink_schema(schema, config)?;
        let fields = user_fields(schema);

        let column_defs: Vec<String> = fields
            .iter()
            .map(|field| {
                let pg_type = arrow_to_pg_ddl_type(field.data_type())?;
                let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
                Ok(format!(
                    "    {} {}{}",
                    quote_sql_identifier(field.name()),
                    pg_type,
                    nullable
                ))
            })
            .collect::<Result<_, ConnectorError>>()?;

        let mut ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n",
            config.qualified_table_name(),
            column_defs.join(",\n"),
        );

        if !config.primary_key_columns.is_empty() {
            use std::fmt::Write;
            let primary_keys = config
                .primary_key_columns
                .iter()
                .map(|column| quote_sql_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = write!(ddl, ",\n    PRIMARY KEY ({})\n", primary_keys);
        }

        ddl.push(')');
        Ok(ddl)
    }

    // ── Changelog/Retraction ────────────────────────────────────────

    /// Splits a changelog `RecordBatch` into insert and delete batches.
    ///
    /// Uses the `_op` metadata column:
    /// - `"I"` (insert), `"U"` (update-after), `"r"` (snapshot read) → insert batch
    /// - `"D"` (delete) → delete batch
    ///
    /// The returned batches exclude the exact changelog metadata field set.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the `_op` column is
    /// missing or not a string type.
    pub fn split_changelog_batch(
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, RecordBatch), ConnectorError> {
        let op_idx = batch.schema().index_of("_op").map_err(|_| {
            ConnectorError::ConfigurationError(
                "changelog mode requires '_op' column in input schema".into(),
            )
        })?;

        let op_array = batch
            .column(op_idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("'_op' column must be String (Utf8) type".into())
            })?;

        validate_operation_values(op_array)?;

        let mut insert_indices = Vec::new();
        let mut delete_indices = Vec::new();

        for i in 0..op_array.len() {
            match op_array.value(i) {
                "I" | "U" | "U+" | "R" | "r" => {
                    insert_indices.push(u32::try_from(i).map_err(|_| {
                        ConnectorError::Internal(
                            "PostgreSQL changelog batch exceeds UInt32 row indexing".into(),
                        )
                    })?);
                }
                "D" | "U-" => {
                    delete_indices.push(u32::try_from(i).map_err(|_| {
                        ConnectorError::Internal(
                            "PostgreSQL changelog batch exceeds UInt32 row indexing".into(),
                        )
                    })?);
                }
                _ => unreachable!("operation values validated above"),
            }
        }

        let insert_batch = filter_batch_by_indices(batch, &insert_indices)?;
        let delete_batch = filter_batch_by_indices(batch, &delete_indices)?;

        Ok((insert_batch, delete_batch))
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
            .map_err(|error| postgres_dispatched_write_error("COPY start", error))?;

        {
            use futures_util::SinkExt;
            futures_util::pin_mut!(sink);
            sink.send(bytes_to_send)
                .await
                .map_err(|error| postgres_dispatched_write_error("COPY send", error))?;
            sink.close()
                .await
                .map_err(|error| postgres_dispatched_write_error("COPY finish", error))?;
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
                    postgres_dispatched_write_error("transaction COMMIT", error)
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
            .map_err(|error| postgres_dispatched_write_error("DELETE", error))?;

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
        if !config.properties().is_empty() {
            self.apply_connector_config(config)?;
        } else {
            // Direct programmatic construction supplies the schema to `new`.
            self.config.validate()?;
            validate_sink_schema(&self.schema, &self.config)?;
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

// ── Helper functions ────────────────────────────────────────────────

fn is_changelog_metadata(name: &str) -> bool {
    CHANGELOG_METADATA_COLUMNS.contains(&name)
}

fn quoted_user_columns(schema: &SchemaRef) -> Vec<String> {
    user_fields(schema)
        .iter()
        .map(|field| quote_sql_identifier(field.name()))
        .collect()
}

/// Returns writable fields, excluding only the defined changelog metadata columns.
fn user_fields(schema: &SchemaRef) -> Vec<&Arc<Field>> {
    schema
        .fields()
        .iter()
        .filter(|field| !is_changelog_metadata(field.name()))
        .collect()
}

/// Builds a schema containing only user-visible columns.
fn build_user_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|field| !is_changelog_metadata(field.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

fn validate_sink_schema(
    schema: &SchemaRef,
    config: &PostgresSinkConfig,
) -> Result<(), ConnectorError> {
    config.validate()?;
    let fields = user_fields(schema);
    if fields.is_empty() {
        return Err(ConnectorError::SchemaMismatch(
            "PostgreSQL sink schema has no writable columns".into(),
        ));
    }

    let mut names = std::collections::HashSet::new();
    for field in schema.fields() {
        validate_sql_identifier(field.name(), "PostgreSQL sink column name")?;
        if !names.insert(field.name()) {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL sink schema contains duplicate column '{}'",
                field.name()
            )));
        }
        if !is_changelog_metadata(field.name()) {
            postgres_type(field.data_type()).map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "PostgreSQL sink column '{}': {error}",
                    field.name()
                ))
            })?;
        }
    }

    let op = schema.field_with_name("_op").ok();
    let weight = schema.field_with_name("__weight").ok();
    let timestamp = schema.field_with_name("_ts_ms").ok();
    if config.changelog_mode {
        if op.is_none() && weight.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "PostgreSQL changelog mode requires an Utf8 '_op' or Int64 '__weight' column"
                    .into(),
            ));
        }
    } else if op.is_some() || weight.is_some() || timestamp.is_some() {
        return Err(ConnectorError::ConfigurationError(
            "PostgreSQL sink '_op', '_ts_ms', and '__weight' columns require changelog.mode=true"
                .into(),
        ));
    }
    if let Some(field) = op {
        if field.data_type() != &DataType::Utf8 || field.is_nullable() {
            return Err(ConnectorError::ConfigurationError(
                "PostgreSQL changelog '_op' must be non-null Utf8".into(),
            ));
        }
    }
    if let Some(field) = weight {
        if field.data_type() != &DataType::Int64 || field.is_nullable() {
            return Err(ConnectorError::ConfigurationError(
                "PostgreSQL changelog '__weight' must be non-null Int64".into(),
            ));
        }
    }

    for primary_key in &config.primary_key_columns {
        let field = schema.field_with_name(primary_key).map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "primary key column '{primary_key}' is not present in PostgreSQL sink schema"
            ))
        })?;
        if is_changelog_metadata(primary_key) {
            return Err(ConnectorError::ConfigurationError(format!(
                "primary key column '{primary_key}' is reserved changelog metadata"
            )));
        }
        if field.is_nullable() {
            return Err(ConnectorError::ConfigurationError(format!(
                "primary key column '{primary_key}' must be non-nullable"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "postgres-sink")]
fn validate_input_batch(
    batch: &RecordBatch,
    expected_schema: &SchemaRef,
    config: &PostgresSinkConfig,
) -> Result<(), ConnectorError> {
    if batch.schema().as_ref() != expected_schema.as_ref() {
        return Err(ConnectorError::SchemaMismatch(
            "PostgreSQL sink batch schema does not match its admitted Arrow schema".into(),
        ));
    }
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if !is_changelog_metadata(field.name()) {
            validate_postgres_array_values(column.as_ref())?;
        }
    }
    for primary_key in &config.primary_key_columns {
        let index = batch.schema().index_of(primary_key).map_err(|_| {
            ConnectorError::SchemaMismatch(format!(
                "primary key column '{primary_key}' is absent from PostgreSQL sink batch"
            ))
        })?;
        if batch.column(index).null_count() != 0 {
            return Err(ConnectorError::SchemaMismatch(format!(
                "PostgreSQL primary key column '{primary_key}' contains NULL"
            )));
        }
    }
    if config.changelog_mode {
        validate_changelog_input(batch)?;
    }
    Ok(())
}

fn validate_operation_values(operations: &arrow_array::StringArray) -> Result<(), ConnectorError> {
    for row in 0..operations.len() {
        if operations.is_null(row) {
            return Err(ConnectorError::SchemaMismatch(format!(
                "PostgreSQL changelog '_op' is NULL at row {row}"
            )));
        }
        let operation = operations.value(row);
        if !matches!(operation, "I" | "U" | "U+" | "R" | "r" | "D" | "U-") {
            return Err(ConnectorError::SchemaMismatch(format!(
                "PostgreSQL changelog operation '{operation}' at row {row} is not supported"
            )));
        }
    }
    Ok(())
}

fn validate_changelog_input(batch: &RecordBatch) -> Result<(), ConnectorError> {
    let schema = batch.schema();
    let mut encoding_found = false;
    if let Ok(index) = schema.index_of("_op") {
        encoding_found = true;
        let operations = batch
            .column(index)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("PostgreSQL changelog '_op' must be Utf8".into())
            })?;
        validate_operation_values(operations)?;
    }
    if let Ok(index) = schema.index_of("__weight") {
        encoding_found = true;
        let weights = batch
            .column(index)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "PostgreSQL changelog '__weight' must be Int64".into(),
                )
            })?;
        if weights.null_count() != 0 {
            return Err(ConnectorError::SchemaMismatch(
                "PostgreSQL changelog '__weight' contains NULL".into(),
            ));
        }
    }
    if !encoding_found {
        return Err(ConnectorError::ConfigurationError(
            "PostgreSQL changelog input requires '_op' or '__weight'".into(),
        ));
    }
    Ok(())
}

/// Filters a `RecordBatch` to include only rows at the given indices.
///
/// Also strips the exact changelog metadata field set from the output.
fn filter_batch_by_indices(
    batch: &RecordBatch,
    indices: &[u32],
) -> Result<RecordBatch, ConnectorError> {
    if indices.is_empty() {
        let user_schema = Arc::new(Schema::new(
            batch
                .schema()
                .fields()
                .iter()
                .filter(|field| !is_changelog_metadata(field.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));
        return Ok(RecordBatch::new_empty(user_schema));
    }

    let indices_array = arrow_array::UInt32Array::from(indices.to_vec());

    let user_schema = Arc::new(Schema::new(
        batch
            .schema()
            .fields()
            .iter()
            .filter(|field| !is_changelog_metadata(field.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ));

    let filtered_columns: Vec<Arc<dyn arrow_array::Array>> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !is_changelog_metadata(field.name()))
        .map(|(i, _)| {
            arrow_select::take::take(batch.column(i), &indices_array, None)
                .map_err(|e| ConnectorError::Internal(format!("arrow take failed: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(user_schema, filtered_columns)
        .map_err(|e| ConnectorError::Internal(format!("batch construction failed: {e}")))
}

/// Strips only the defined changelog metadata columns from a `RecordBatch`.
fn strip_metadata_columns(batch: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
    let schema = batch.schema();
    let user_indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !is_changelog_metadata(field.name()))
        .map(|(i, _)| i)
        .collect();

    if user_indices.len() == schema.fields().len() {
        return Ok(batch.clone());
    }

    let user_schema = Arc::new(Schema::new(
        user_indices
            .iter()
            .map(|&i| schema.field(i).clone())
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<Arc<dyn Array>> = user_indices
        .iter()
        .map(|&i| batch.column(i).clone())
        .collect();

    RecordBatch::try_new(user_schema, columns)
        .map_err(|e| ConnectorError::Internal(format!("strip metadata: {e}")))
}

/// Collapse one ordinary upsert flush to the last-arriving row per configured
/// key, then remove the normalized changelog marker before binding UNNEST
/// parameters. PostgreSQL rejects duplicate conflict keys in one statement.
fn collapse_upsert_batch(
    batch: &RecordBatch,
    primary_key_columns: &[String],
) -> Result<RecordBatch, ConnectorError> {
    let collapsed = collapse_changelog(batch, primary_key_columns)?;
    strip_metadata_columns(&collapsed)
}

/// Executes an UNNEST-based INSERT/UPSERT using Arrow column arrays as parameters.
#[cfg(feature = "postgres-sink")]
async fn execute_unnest<C>(
    client: &C,
    sql: &str,
    batch: &RecordBatch,
) -> Result<u64, ConnectorError>
where
    C: tokio_postgres::GenericClient + Sync,
{
    let params: Vec<Box<dyn postgres_types::ToSql + Sync + Send>> = (0..batch.num_columns())
        .map(|i| arrow_column_to_pg_array(batch.column(i)))
        .collect::<Result<_, _>>()?;

    let param_refs: Vec<&(dyn postgres_types::ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn postgres_types::ToSql + Sync))
        .collect();

    client
        .execute(sql, &param_refs)
        .await
        .map_err(|error| postgres_dispatched_write_error("UNNEST execute", error))
}

#[cfg(any(feature = "postgres-sink", test))]
fn retained_batch_bytes(batch: &RecordBatch) -> usize {
    let batch_overhead = std::mem::size_of::<RecordBatch>().saturating_add(
        batch
            .num_columns()
            .saturating_mul(std::mem::size_of::<Arc<dyn Array>>()),
    );
    batch
        .columns()
        .iter()
        .fold(batch_overhead, |total, column| {
            total.saturating_add(column.get_array_memory_size())
        })
}

#[cfg(feature = "postgres-sink")]
fn retained_batch_bytes_u64(batch: &RecordBatch) -> u64 {
    u64::try_from(retained_batch_bytes(batch)).unwrap_or(u64::MAX)
}

/// Validates a single batch before mutation, then reports whether already-buffered input must be
/// flushed before admission. Equality is allowed; addition fails closed on overflow.
#[cfg(any(feature = "postgres-sink", test))]
fn requires_preflush(
    buffered_bytes: usize,
    incoming_bytes: usize,
    retained_limit: usize,
) -> Result<bool, ConnectorError> {
    if incoming_bytes > retained_limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL sink input batch retains {incoming_bytes} bytes, exceeding the fixed \
             {retained_limit}-byte per-sink buffer limit; split the batch upstream"
        )));
    }

    Ok(buffered_bytes
        .checked_add(incoming_bytes)
        .is_none_or(|total| total > retained_limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn dispatched_write_without_server_response_has_unknown_outcome() {
        let unknown = classify_postgres_write_failure("UNNEST execute", &"connection lost", false);
        assert!(unknown.is_outcome_unknown());
        assert!(unknown.to_string().contains("may have committed"));

        let rejected = classify_postgres_write_failure("UNNEST execute", &"unique violation", true);
        assert!(matches!(rejected, ConnectorError::WriteError(_)));
        assert!(!rejected.is_outcome_unknown());
    }

    #[test]
    fn uncommitted_transaction_resolves_an_ambiguous_statement_outcome() {
        let retryable = resolve_uncommitted_transaction_error(ConnectorError::outcome_unknown(
            "connection lost after UNNEST",
            true,
        ));
        assert!(matches!(retryable, ConnectorError::ConnectionFailed(_)));
        assert!(retryable.is_transient());
        assert!(!retryable.is_outcome_unknown());

        let terminal = resolve_uncommitted_transaction_error(ConnectorError::outcome_unknown(
            "protocol state invalid",
            false,
        ));
        assert!(matches!(terminal, ConnectorError::TransactionError(_)));
        assert!(!terminal.is_transient());
        assert!(!terminal.is_outcome_unknown());
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn composite_key_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn test_config() -> PostgresSinkConfig {
        PostgresSinkConfig::new("localhost", "mydb", "events")
    }

    fn upsert_config() -> PostgresSinkConfig {
        let mut cfg = test_config();
        cfg.write_mode = WriteMode::Upsert;
        cfg.primary_key_columns = vec!["id".to_string()];
        cfg
    }

    fn test_batch(n: usize) -> RecordBatch {
        let ids: Vec<i64> = (0..n as i64).collect();
        let names: Vec<&str> = (0..n).map(|_| "test").collect();
        let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(arrow_array::Float64Array::from(values)),
            ],
        )
        .expect("test batch creation")
    }

    fn variable_width_batch(value: &str) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![value])),
                Arc::new(arrow_array::Float64Array::from(vec![1.0])),
            ],
        )
        .expect("variable-width test batch")
    }

    // ── Constructor tests ──

    #[test]
    fn test_new_defaults() {
        let sink = PostgresSink::new(test_schema(), test_config(), None);
        assert_eq!(sink.state(), ConnectorState::Created);
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.buffered_retained_bytes, 0);
        assert!(sink.upsert_sql.is_none());
        assert!(sink.copy_sql.is_none());
    }

    #[test]
    fn constructor_uses_small_fixed_buffer_preallocation() {
        let sink = PostgresSink::new(test_schema(), test_config(), None);
        assert_eq!(sink.buffer.capacity(), 4);
    }

    #[test]
    fn test_user_schema_strips_metadata() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, false),
            Field::new("_private_value", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));
        let sink = PostgresSink::new(schema, test_config(), None);
        assert_eq!(sink.user_schema.fields().len(), 3);
        assert_eq!(sink.user_schema.field(0).name(), "id");
        assert_eq!(sink.user_schema.field(1).name(), "_private_value");
        assert_eq!(sink.user_schema.field(2).name(), "value");
    }

    #[test]
    fn test_schema_returned() {
        let schema = test_schema();
        let sink = PostgresSink::new(schema.clone(), test_config(), None);
        assert_eq!(sink.schema(), schema);
    }

    #[test]
    fn engine_schema_replaces_placeholder_and_drives_write_sql() {
        let engine_schema = Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("sequence", DataType::Int64, false),
            Field::new("enabled", DataType::Boolean, true),
            Field::new("_op", DataType::Utf8, false),
        ]));
        let placeholder = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let mut config = ConnectorConfig::new("postgres-sink");
        config.set("hostname", "localhost");
        config.set("database", "analytics");
        config.set("username", "writer");
        config.set("table.name", "events");
        config.set("auto.create.table", "true");
        config.set("write.mode", "upsert");
        config.set("primary.key", "tenant");
        config.set("changelog.mode", "true");
        config.set(
            "_arrow_schema",
            crate::config::encode_arrow_schema_ipc(engine_schema.as_ref()),
        );

        let mut sink = PostgresSink::new(placeholder, test_config(), None);
        sink.apply_connector_config(&config).unwrap();
        sink.prepare_statements().unwrap();

        assert_eq!(sink.schema, engine_schema);
        assert_eq!(
            sink.user_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["tenant", "sequence", "enabled"]
        );
        let upsert = sink.upsert_sql.as_deref().unwrap();
        assert!(upsert.contains("\"tenant\"") && upsert.contains("\"sequence\""));
        let ddl = sink.create_table_sql.as_deref().unwrap();
        assert!(ddl.contains("\"tenant\" TEXT NOT NULL"), "{ddl}");
        assert!(ddl.contains("\"sequence\" BIGINT NOT NULL"), "{ddl}");
        assert!(ddl.contains("\"enabled\" BOOLEAN"), "{ddl}");
        assert!(!ddl.contains("_op"), "{ddl}");
    }

    // ── SQL generation tests ──

    #[test]
    fn test_build_copy_sql() {
        let schema = test_schema();
        let config = test_config();
        let sql = PostgresSink::build_copy_sql(&schema, &config).unwrap();
        assert_eq!(
            sql,
            "COPY \"public\".\"events\" (\"id\", \"name\", \"value\") FROM STDIN BINARY"
        );
    }

    #[test]
    fn test_build_copy_sql_custom_schema() {
        let schema = test_schema();
        let mut config = test_config();
        config.schema_name = "analytics".to_string();
        let sql = PostgresSink::build_copy_sql(&schema, &config).unwrap();
        assert!(sql.starts_with("COPY \"analytics\".\"events\""));
    }

    #[test]
    fn sql_generation_quotes_reserved_mixed_case_and_embedded_quote_identifiers() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("select", DataType::Int64, false),
            Field::new("MixedCase", DataType::Utf8, true),
            Field::new("a\"b", DataType::Boolean, true),
        ]));
        let mut config = test_config();
        config.schema_name = "Tenant.Schema".into();
        config.table_name = "Order".into();

        let sql = PostgresSink::build_copy_sql(&schema, &config).unwrap();
        assert_eq!(
            sql,
            "COPY \"Tenant.Schema\".\"Order\" (\"select\", \"MixedCase\", \"a\"\"b\") FROM STDIN BINARY"
        );
    }

    #[test]
    fn schema_admission_rejects_unsupported_types_and_nullable_keys() {
        let unsupported = Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]));
        assert!(PostgresSink::build_copy_sql(&unsupported, &test_config()).is_err());

        let nullable_key = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        assert!(PostgresSink::build_upsert_sql(&nullable_key, &upsert_config()).is_err());

        let nul_name = Arc::new(Schema::new(vec![Field::new(
            "bad\0column",
            DataType::Int64,
            false,
        )]));
        assert!(PostgresSink::build_copy_sql(&nul_name, &test_config()).is_err());
    }

    #[test]
    fn test_build_copy_sql_excludes_metadata_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, false),
            Field::new("_ts_ms", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let mut config = upsert_config();
        config.changelog_mode = true;
        let sql = PostgresSink::build_copy_sql(&schema, &config).unwrap();
        assert_eq!(
            sql,
            "COPY \"public\".\"events\" (\"id\", \"value\") FROM STDIN BINARY"
        );
    }

    #[test]
    fn timestamp_metadata_is_never_silently_dropped_outside_changelog_mode() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_ts_ms", DataType::Int64, false),
        ]));
        let error = PostgresSink::build_copy_sql(&schema, &test_config()).unwrap_err();
        assert!(error.to_string().contains("changelog.mode=true"));

        let user_underscore = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_private", DataType::Utf8, true),
        ]));
        let sql = PostgresSink::build_copy_sql(&user_underscore, &test_config()).unwrap();
        assert!(sql.contains("\"_private\""), "{sql}");
    }

    #[test]
    fn test_build_upsert_sql() {
        let schema = test_schema();
        let config = upsert_config();
        let sql = PostgresSink::build_upsert_sql(&schema, &config).unwrap();

        assert!(sql.starts_with("INSERT INTO \"public\".\"events\""));
        assert!(sql.contains("SELECT * FROM UNNEST"));
        assert!(sql.contains("$1::int8[]"));
        assert!(sql.contains("$2::text[]"));
        assert!(sql.contains("$3::float8[]"));
        assert!(sql.contains("ON CONFLICT (\"id\")"));
        assert!(sql.contains("DO UPDATE SET"));
        assert!(sql.contains("\"name\" = EXCLUDED.\"name\""));
        assert!(sql.contains("\"value\" = EXCLUDED.\"value\""));
        assert!(!sql.contains("\"id\" = EXCLUDED.\"id\""));
    }

    #[test]
    fn test_build_upsert_sql_composite_key() {
        let schema = composite_key_schema();
        let mut config = upsert_config();
        config.primary_key_columns = vec!["id".to_string(), "name".to_string()];
        let sql = PostgresSink::build_upsert_sql(&schema, &config).unwrap();

        assert!(sql.contains("ON CONFLICT (\"id\", \"name\")"));
        assert!(sql.contains("\"value\" = EXCLUDED.\"value\""));
        assert!(!sql.contains("\"id\" = EXCLUDED.\"id\""));
        assert!(!sql.contains("\"name\" = EXCLUDED.\"name\""));
    }

    #[test]
    fn test_build_upsert_sql_key_only_table() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut config = test_config();
        config.write_mode = WriteMode::Upsert;
        config.primary_key_columns = vec!["id".to_string()];

        let sql = PostgresSink::build_upsert_sql(&schema, &config).unwrap();
        assert!(sql.contains("DO NOTHING"), "sql: {sql}");
    }

    #[test]
    fn test_build_delete_sql() {
        let schema = test_schema();
        let config = upsert_config();
        let sql = PostgresSink::build_delete_sql(&schema, &config).unwrap();

        assert_eq!(
            sql,
            "DELETE FROM \"public\".\"events\" WHERE \"id\" = ANY($1::int8[])"
        );
    }

    #[test]
    fn test_build_delete_sql_composite_key() {
        let schema = composite_key_schema();
        let mut config = upsert_config();
        config.primary_key_columns = vec!["id".to_string(), "name".to_string()];
        let sql = PostgresSink::build_delete_sql(&schema, &config).unwrap();

        // Composite PK must match tuple-wise (UNNEST zips $1/$2 positionally), NOT the
        // cross-product `id = ANY($1) AND name = ANY($2)` which over-deletes (CN-2).
        assert_eq!(
            sql,
            "DELETE FROM \"public\".\"events\" AS \"target\" USING UNNEST($1::int8[], \
             $2::text[]) AS \"keys\"(\"id\", \"name\") WHERE \"target\".\"id\" = \
             \"keys\".\"id\" AND \"target\".\"name\" = \"keys\".\"name\""
        );
        assert!(!sql.contains("ANY($1::int8[]) AND"));
    }

    #[test]
    fn test_build_create_table_sql() {
        let schema = test_schema();
        let config = upsert_config();
        let sql = PostgresSink::build_create_table_sql(&schema, &config).unwrap();

        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"public\".\"events\""));
        assert!(sql.contains("\"id\" BIGINT NOT NULL"));
        assert!(sql.contains("\"name\" TEXT"));
        assert!(sql.contains("\"value\" DOUBLE PRECISION"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn test_build_create_table_sql_no_pk() {
        let schema = test_schema();
        let config = test_config();
        let sql = PostgresSink::build_create_table_sql(&schema, &config).unwrap();

        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS"));
        assert!(!sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn ordinary_upsert_collapse_keeps_last_row_per_primary_key() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 1])),
                Arc::new(StringArray::from(vec!["old", "other", "new"])),
            ],
        )
        .unwrap();

        let collapsed = collapse_upsert_batch(&batch, &["id".to_string()]).unwrap();

        assert_eq!(collapsed.num_rows(), 2);
        assert!(collapsed.schema().index_of("_op").is_err());
        let ids = collapsed
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = collapsed
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let id_one = (0..collapsed.num_rows())
            .find(|&row| ids.value(row) == 1)
            .unwrap();
        assert_eq!(values.value(id_one), "new");
    }

    // ── Changelog splitting tests ──

    fn changelog_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_op", DataType::Utf8, false),
            Field::new("_ts_ms", DataType::Int64, false),
        ]))
    }

    fn changelog_batch() -> RecordBatch {
        RecordBatch::try_new(
            changelog_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(StringArray::from(vec!["I", "U", "D", "I", "D"])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            ],
        )
        .expect("changelog batch creation")
    }

    #[test]
    fn test_split_changelog_batch() {
        let batch = changelog_batch();
        let (inserts, deletes) = PostgresSink::split_changelog_batch(&batch).expect("split");

        assert_eq!(inserts.num_rows(), 3);
        assert_eq!(deletes.num_rows(), 2);
        assert_eq!(inserts.num_columns(), 2);
        assert_eq!(deletes.num_columns(), 2);

        let insert_ids = inserts
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 array");
        assert_eq!(insert_ids.value(0), 1);
        assert_eq!(insert_ids.value(1), 2);
        assert_eq!(insert_ids.value(2), 4);

        let delete_ids = deletes
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 array");
        assert_eq!(delete_ids.value(0), 3);
        assert_eq!(delete_ids.value(1), 5);
    }

    #[test]
    fn test_split_changelog_all_inserts() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["I", "I"])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .expect("batch");

        let (inserts, deletes) = PostgresSink::split_changelog_batch(&batch).expect("split");
        assert_eq!(inserts.num_rows(), 2);
        assert_eq!(deletes.num_rows(), 0);
    }

    #[test]
    fn test_split_changelog_all_deletes() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["D", "D"])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .expect("batch");

        let (inserts, deletes) = PostgresSink::split_changelog_batch(&batch).expect("split");
        assert_eq!(inserts.num_rows(), 0);
        assert_eq!(deletes.num_rows(), 2);
    }

    #[test]
    fn test_split_changelog_missing_op_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).expect("batch");

        let result = PostgresSink::split_changelog_batch(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_changelog_snapshot_read() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["r"])),
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .expect("batch");

        let (inserts, deletes) = PostgresSink::split_changelog_batch(&batch).expect("split");
        assert_eq!(inserts.num_rows(), 1);
        assert_eq!(deletes.num_rows(), 0);
    }

    #[test]
    fn changelog_null_and_unknown_operations_fail_closed() {
        for operations in [
            StringArray::from(vec![Some("I"), None]),
            StringArray::from(vec![Some("I"), Some("future")]),
        ] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("_op", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(operations)],
            )
            .unwrap();
            assert!(PostgresSink::split_changelog_batch(&batch).is_err());
        }
    }

    #[cfg(feature = "postgres-sink")]
    #[tokio::test]
    async fn invalid_changelog_operation_is_rejected_before_buffer_mutation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["future"])),
            ],
        )
        .unwrap();
        let mut config = upsert_config();
        config.changelog_mode = true;
        let mut sink = PostgresSink::new(schema, config, None);
        sink.state = ConnectorState::Running;

        let error = sink.write_batch(&batch).await.unwrap_err();
        assert!(!error.is_transient());
        assert!(sink.buffer.is_empty());
        assert_eq!(sink.buffered_rows, 0);
        assert_eq!(sink.buffered_retained_bytes, 0);
    }

    // ── Buffering tests ──

    #[tokio::test]
    async fn test_write_batch_buffering() {
        let mut sink = PostgresSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await.expect("write");

        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 10);
        assert_eq!(sink.buffered_retained_bytes, retained_batch_bytes(&batch));
    }

    #[tokio::test]
    async fn test_write_batch_empty() {
        let mut sink = PostgresSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;

        let batch = test_batch(0);
        let result = sink.write_batch(&batch).await.expect("write");
        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.buffered_retained_bytes, 0);
    }

    #[tokio::test]
    async fn test_write_batch_not_running() {
        let mut sink = PostgresSink::new(test_schema(), test_config(), None);

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await;
        assert!(result.is_err());
    }

    #[test]
    fn retained_limit_uses_variable_width_memory_and_allows_exact_boundary() {
        let narrow = variable_width_batch("x");
        let wide_value = "x".repeat(4096);
        let wide = variable_width_batch(&wide_value);
        let narrow_bytes = retained_batch_bytes(&narrow);
        let wide_bytes = retained_batch_bytes(&wide);
        assert!(wide_bytes > narrow_bytes);

        let exact_limit = narrow_bytes + wide_bytes;
        assert!(!requires_preflush(narrow_bytes, wide_bytes, exact_limit).unwrap());
        assert!(requires_preflush(narrow_bytes, wide_bytes, exact_limit - 1).unwrap());
        assert!(requires_preflush(usize::MAX, 1, usize::MAX).unwrap());
    }

    #[cfg(feature = "postgres-sink")]
    #[tokio::test]
    async fn oversized_batch_rejection_does_not_mutate_existing_buffer() {
        let mut sink = PostgresSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;

        let existing = variable_width_batch("retained");
        sink.write_batch_with_retained_limit(&existing, usize::MAX)
            .await
            .unwrap();
        let rows_before = sink.buffered_rows;
        let bytes_before = sink.buffered_retained_bytes;
        let batches_before = sink.buffer.len();

        let incoming_value = "x".repeat(4096);
        let incoming = variable_width_batch(&incoming_value);
        let incoming_bytes = retained_batch_bytes(&incoming);
        let error = sink
            .write_batch_with_retained_limit(&incoming, incoming_bytes - 1)
            .await
            .expect_err("single oversized batch must fail before admission");

        assert!(error.to_string().contains("split the batch upstream"));
        assert!(!error.is_transient());
        assert_eq!(sink.buffered_rows, rows_before);
        assert_eq!(sink.buffered_retained_bytes, bytes_before);
        assert_eq!(sink.buffer.len(), batches_before);
        assert_eq!(sink.buffer[0].num_rows(), existing.num_rows());
    }

    #[cfg(feature = "postgres-sink")]
    #[tokio::test]
    async fn explicit_flush_error_clears_buffer_accounting() {
        let mut sink = PostgresSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;

        sink.write_batch_with_retained_limit(&variable_width_batch("pending"), usize::MAX)
            .await
            .unwrap();
        let error = sink
            .flush()
            .await
            .expect_err("missing pool must fail flush");

        assert!(matches!(error, ConnectorError::InvalidState { .. }));
        assert!(sink.buffer.is_empty());
        assert_eq!(sink.buffered_rows, 0);
        assert_eq!(sink.buffered_retained_bytes, 0);
    }

    #[cfg(feature = "postgres-sink")]
    #[tokio::test]
    async fn crossing_batch_flushes_existing_before_admission() {
        let mut sink = PostgresSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;

        let existing = variable_width_batch("existing");
        sink.write_batch_with_retained_limit(&existing, usize::MAX)
            .await
            .unwrap();
        let incoming = variable_width_batch("incoming");
        let crossing_limit =
            retained_batch_bytes(&existing).saturating_add(retained_batch_bytes(&incoming)) - 1;

        let error = sink
            .write_batch_with_retained_limit(&incoming, crossing_limit)
            .await
            .expect_err("crossing admission must flush the existing buffer first");

        assert!(matches!(error, ConnectorError::InvalidState { .. }));
        assert!(sink.buffer.is_empty());
        assert_eq!(sink.buffered_rows, 0);
        assert_eq!(sink.buffered_retained_bytes, 0);
    }

    #[cfg(feature = "postgres-sink")]
    #[tokio::test]
    async fn close_reports_flush_failure_but_releases_state() {
        let mut sink = PostgresSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;
        sink.write_batch_with_retained_limit(&variable_width_batch("pending"), usize::MAX)
            .await
            .unwrap();

        let error = sink
            .close()
            .await
            .expect_err("missing pool must fail flush");

        assert!(matches!(error, ConnectorError::InvalidState { .. }));
        assert!(sink.buffer.is_empty());
        assert_eq!(sink.buffered_rows, 0);
        assert_eq!(sink.buffered_retained_bytes, 0);
        assert_eq!(sink.state, ConnectorState::Closed);
    }

    // ── Contract tests ──

    #[cfg(feature = "postgres-sink")]
    #[test]
    fn contract_append_is_multi_writer_durable_at_least_once() {
        let sink = PostgresSink::new(test_schema(), test_config(), None);
        let contract = sink.contract(&ConnectorConfig::new("postgres")).unwrap();
        assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
        assert_eq!(contract.topology, SinkTopology::MultiWriter);
        assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
        assert_eq!(
            sink.suggested_write_timeout(),
            sink.config.statement_timeout + Duration::from_secs(5)
        );
        assert_eq!(sink.flush_interval(), Duration::from_millis(250));
    }

    #[cfg(feature = "postgres-sink")]
    #[test]
    fn contract_upsert_requires_keyed_input() {
        let sink = PostgresSink::new(test_schema(), upsert_config(), None);
        let contract = sink.contract(&ConnectorConfig::new("postgres")).unwrap();
        assert_eq!(contract.input_mode, SinkInputMode::KeyedUpsert);
        assert_eq!(contract.topology, SinkTopology::Singleton);
    }

    #[cfg(feature = "postgres-sink")]
    #[test]
    fn contract_changelog_accepts_full_changelog() {
        let mut config = upsert_config();
        config.changelog_mode = true;
        let sink = PostgresSink::new(changelog_schema(), config, None);
        let contract = sink.contract(&ConnectorConfig::new("postgres")).unwrap();
        assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
        assert_eq!(contract.topology, SinkTopology::Singleton);
        assert!(contract.accepts_full_changelog());
        assert_eq!(
            sink.suggested_write_timeout(),
            sink.config.statement_timeout.saturating_mul(2) + Duration::from_secs(5)
        );
    }

    #[cfg(feature = "postgres-sink")]
    #[tokio::test]
    async fn open_rejects_invalid_ca_before_network_io() {
        let directory = tempfile::tempdir().unwrap();
        let ca_path = directory.path().join("missing.pem");
        let mut config = test_config();
        config.ssl_ca_cert_path = Some(ca_path.clone());
        let mut sink = PostgresSink::new(test_schema(), config, None);

        let error = sink
            .open(&ConnectorConfig::new("postgres-sink"))
            .await
            .expect_err("invalid custom CA must fail before pool I/O");

        let message = error.to_string();
        assert!(
            message.contains(&ca_path.display().to_string()),
            "{message}"
        );
        assert_eq!(sink.state(), ConnectorState::Created);
    }

    #[cfg(not(feature = "postgres-sink"))]
    #[test]
    fn missing_feature_fails_contract_before_io() {
        let sink = PostgresSink::new(test_schema(), test_config(), None);
        let error = sink
            .contract(&ConnectorConfig::new("postgres"))
            .expect_err("disabled PostgreSQL sink must fail admission");
        assert!(error.to_string().contains("postgres-sink"));
    }

    // ── Debug output test ──

    #[test]
    fn test_debug_output() {
        let sink = PostgresSink::new(test_schema(), test_config(), None);
        let debug = format!("{sink:?}");
        assert!(debug.contains("PostgresSink"));
        assert!(debug.contains("public") && debug.contains("events"));
    }

    // ── Helper function tests ──

    #[test]
    fn test_build_user_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            Field::new("_ts_ms", DataType::Int64, false),
        ]));
        let user = build_user_schema(&schema);
        assert_eq!(user.fields().len(), 2);
        assert_eq!(user.field(0).name(), "id");
        assert_eq!(user.field(1).name(), "value");
    }

    #[test]
    fn test_build_user_schema_no_metadata() {
        let schema = test_schema();
        let user = build_user_schema(&schema);
        assert_eq!(user.fields().len(), 3);
    }
}
