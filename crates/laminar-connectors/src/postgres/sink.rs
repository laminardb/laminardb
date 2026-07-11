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
use std::time::{Duration, Instant};

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

use super::sink_config::{PostgresSinkConfig, WriteMode};
use super::sink_metrics::PostgresSinkMetrics;
use super::types::{arrow_to_pg_ddl_type, arrow_type_to_pg_array_cast, arrow_type_to_pg_sql};

#[cfg(feature = "postgres-sink")]
use super::types::arrow_column_to_pg_array;
#[cfg(feature = "postgres-sink")]
use bytes::BytesMut;
#[cfg(feature = "postgres-sink")]
use deadpool_postgres::Pool;

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
    /// Last flush time.
    last_flush: Instant,
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
    /// Pre-allocated encode buffer for pgpq COPY BINARY output.
    #[cfg(feature = "postgres-sink")]
    encode_buf: BytesMut,
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
        // Pre-allocate the write buffer to avoid reallocation on the first batches.
        let buf_capacity = (config.batch_size / 1024).max(4);
        Self {
            config,
            schema,
            user_schema,
            state: ConnectorState::Created,
            buffer: Vec::with_capacity(buf_capacity),
            buffered_rows: 0,
            last_flush: Instant::now(),
            metrics: PostgresSinkMetrics::new(registry),
            upsert_sql: None,
            copy_sql: None,
            create_table_sql: None,
            delete_sql: None,
            #[cfg(feature = "postgres-sink")]
            pool: None,
            #[cfg(feature = "postgres-sink")]
            encode_buf: BytesMut::with_capacity(64 * 1024),
        }
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

    /// Returns a reference to the sink metrics.
    #[must_use]
    pub fn sink_metrics(&self) -> &PostgresSinkMetrics {
        &self.metrics
    }

    // ── SQL Generation ──────────────────────────────────────────────

    /// Builds the COPY BINARY SQL statement.
    ///
    /// ```sql
    /// COPY public.events (id, value, ts) FROM STDIN BINARY
    /// ```
    #[must_use]
    pub fn build_copy_sql(schema: &SchemaRef, config: &PostgresSinkConfig) -> String {
        let columns = user_columns(schema);
        let col_list = columns.join(", ");
        format!(
            "COPY {} ({}) FROM STDIN BINARY",
            config.qualified_table_name(),
            col_list,
        )
    }

    /// Builds the UNNEST-based upsert SQL statement.
    ///
    /// ```sql
    /// INSERT INTO public.target (id, value, updated_at)
    /// SELECT * FROM UNNEST($1::int8[], $2::text[], $3::timestamptz[])
    /// ON CONFLICT (id) DO UPDATE SET
    ///     value = EXCLUDED.value,
    ///     updated_at = EXCLUDED.updated_at
    /// ```
    #[must_use]
    pub fn build_upsert_sql(schema: &SchemaRef, config: &PostgresSinkConfig) -> String {
        let fields = user_fields(schema);

        let columns: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();

        let unnest_params: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, f)| arrow_type_to_pg_array_cast(f.data_type(), i + 1))
            .collect();

        let non_key_columns: Vec<&str> = columns
            .iter()
            .copied()
            .filter(|c| {
                !config
                    .primary_key_columns
                    .iter()
                    .any(|pk| pk.as_str() == *c)
            })
            .collect();

        let update_clause: Vec<String> = non_key_columns
            .iter()
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .collect();

        let pk_list = config.primary_key_columns.join(", ");

        if update_clause.is_empty() {
            // Key-only table: use DO NOTHING
            format!(
                "INSERT INTO {} ({}) \
                 SELECT * FROM UNNEST({}) \
                 ON CONFLICT ({}) DO NOTHING",
                config.qualified_table_name(),
                columns.join(", "),
                unnest_params.join(", "),
                pk_list,
            )
        } else {
            format!(
                "INSERT INTO {} ({}) \
                 SELECT * FROM UNNEST({}) \
                 ON CONFLICT ({}) DO UPDATE SET {}",
                config.qualified_table_name(),
                columns.join(", "),
                unnest_params.join(", "),
                pk_list,
                update_clause.join(", "),
            )
        }
    }

    /// Builds the DELETE SQL for changelog deletes. One array parameter is bound per primary-key
    /// column (`$1`, `$2`, …), each holding that column's values for the batch's deleted keys.
    ///
    /// ```sql
    /// -- single PK
    /// DELETE FROM public.events WHERE id = ANY($1::int8[])
    /// -- composite PK: match tuple-wise via UNNEST, not the cross-product
    /// DELETE FROM public.events AS t USING UNNEST($1::int8[], $2::text[]) AS k(id, name)
    ///   WHERE t.id = k.id AND t.name = k.name
    /// ```
    #[must_use]
    pub fn build_delete_sql(schema: &SchemaRef, config: &PostgresSinkConfig) -> String {
        let pg_type = |col: &str| {
            let dt = schema
                .field_with_name(col)
                .map_or(DataType::Utf8, |f| f.data_type().clone());
            arrow_type_to_pg_sql(&dt)
        };
        let pk = &config.primary_key_columns;

        // A single PK column can use a plain ANY(); a composite PK must match keys tuple-wise, or
        // `col1 = ANY($1) AND col2 = ANY($2)` deletes the cross-product — e.g. deleting (1,'a') and
        // (2,'b') would also delete (1,'b') and (2,'a') (CN-2). UNNEST zips the arrays positionally.
        if pk.len() <= 1 {
            let col = pk.first().map_or("", String::as_str);
            format!(
                "DELETE FROM {} WHERE {col} = ANY($1::{}[])",
                config.qualified_table_name(),
                pg_type(col),
            )
        } else {
            let unnest_args: Vec<String> = pk
                .iter()
                .enumerate()
                .map(|(i, col)| format!("${}::{}[]", i + 1, pg_type(col)))
                .collect();
            let match_conds: Vec<String> =
                pk.iter().map(|col| format!("t.{col} = k.{col}")).collect();
            format!(
                "DELETE FROM {} AS t USING UNNEST({}) AS k({}) WHERE {}",
                config.qualified_table_name(),
                unnest_args.join(", "),
                pk.join(", "),
                match_conds.join(" AND "),
            )
        }
    }

    /// Builds CREATE TABLE DDL from the Arrow schema.
    ///
    /// ```sql
    /// CREATE TABLE IF NOT EXISTS public.events (
    ///     id BIGINT NOT NULL,
    ///     value TEXT,
    ///     ts TIMESTAMPTZ,
    ///     PRIMARY KEY (id)
    /// )
    /// ```
    #[must_use]
    pub fn build_create_table_sql(schema: &SchemaRef, config: &PostgresSinkConfig) -> String {
        let fields = user_fields(schema);

        let column_defs: Vec<String> = fields
            .iter()
            .map(|f| {
                let pg_type = arrow_to_pg_ddl_type(f.data_type());
                let nullable = if f.is_nullable() { "" } else { " NOT NULL" };
                format!("    {} {}{}", f.name(), pg_type, nullable)
            })
            .collect();

        let mut ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n",
            config.qualified_table_name(),
            column_defs.join(",\n"),
        );

        if !config.primary_key_columns.is_empty() {
            use std::fmt::Write;
            let _ = write!(
                ddl,
                ",\n    PRIMARY KEY ({})\n",
                config.primary_key_columns.join(", ")
            );
        }

        ddl.push(')');
        ddl
    }

    // ── Changelog/Retraction ────────────────────────────────────────

    /// Splits a changelog `RecordBatch` into insert and delete batches.
    ///
    /// Uses the `_op` metadata column:
    /// - `"I"` (insert), `"U"` (update-after), `"r"` (snapshot read) → insert batch
    /// - `"D"` (delete) → delete batch
    ///
    /// The returned batches exclude metadata columns (those starting with `_`).
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

        let mut insert_indices = Vec::new();
        let mut delete_indices = Vec::new();

        for i in 0..op_array.len() {
            if op_array.is_null(i) {
                continue;
            }
            match op_array.value(i) {
                "I" | "U" | "r" => {
                    insert_indices.push(u32::try_from(i).unwrap_or(u32::MAX));
                }
                "D" => {
                    delete_indices.push(u32::try_from(i).unwrap_or(u32::MAX));
                }
                _ => {} // Skip unknown ops
            }
        }

        let insert_batch = filter_batch_by_indices(batch, &insert_indices)?;
        let delete_batch = filter_batch_by_indices(batch, &delete_indices)?;

        Ok((insert_batch, delete_batch))
    }

    // ── Internal helpers ────────────────────────────────────────────

    /// Prepares cached SQL statements based on schema and config.
    fn prepare_statements(&mut self) {
        self.copy_sql = Some(Self::build_copy_sql(&self.schema, &self.config));

        if self.config.write_mode == WriteMode::Upsert {
            self.upsert_sql = Some(Self::build_upsert_sql(&self.schema, &self.config));
        }

        if self.config.auto_create_table {
            self.create_table_sql = Some(Self::build_create_table_sql(&self.schema, &self.config));
        }

        if self.config.changelog_mode {
            self.delete_sql = Some(Self::build_delete_sql(&self.schema, &self.config));
        }
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
    fn concat_buffer(&self) -> Result<RecordBatch, ConnectorError> {
        if self.buffer.is_empty() {
            return Ok(RecordBatch::new_empty(self.user_schema.clone()));
        }

        let stripped: Result<Vec<RecordBatch>, ConnectorError> =
            self.buffer.iter().map(strip_metadata_columns).collect();
        let stripped = stripped?;

        arrow_select::concat::concat_batches(&self.user_schema, &stripped)
            .map_err(|e| ConnectorError::Internal(format!("batch concat failed: {e}")))
    }

    /// Flushes buffered data to `PostgreSQL` using the COPY BINARY protocol.
    #[cfg(feature = "postgres-sink")]
    async fn flush_append(
        &mut self,
        client: &tokio_postgres::Client,
    ) -> Result<WriteResult, ConnectorError> {
        let user_batch = self.concat_buffer()?;
        if user_batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        let mut encoder = pgpq::ArrowToPostgresBinaryEncoder::try_new(self.user_schema.as_ref())
            .map_err(|e| ConnectorError::Internal(format!("pgpq encoder init: {e}")))?;

        self.encode_buf.clear();
        encoder.write_header(&mut self.encode_buf);
        encoder
            .write_batch(&user_batch, &mut self.encode_buf)
            .map_err(|e| ConnectorError::Internal(format!("pgpq encode: {e}")))?;
        encoder
            .write_footer(&mut self.encode_buf)
            .map_err(|e| ConnectorError::Internal(format!("pgpq footer: {e}")))?;

        let encoded_bytes = self.encode_buf.len();
        let bytes_to_send = self.encode_buf.split().freeze();

        let copy_sql = self
            .copy_sql
            .as_deref()
            .ok_or_else(|| ConnectorError::Internal("COPY SQL not prepared".into()))?;

        let sink = client
            .copy_in(copy_sql)
            .await
            .map_err(|e| ConnectorError::WriteError(format!("COPY start: {e}")))?;

        {
            use futures_util::SinkExt;
            futures_util::pin_mut!(sink);
            sink.send(bytes_to_send)
                .await
                .map_err(|e| ConnectorError::WriteError(format!("COPY send: {e}")))?;
            sink.close()
                .await
                .map_err(|e| ConnectorError::WriteError(format!("COPY finish: {e}")))?;
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
        client: &tokio_postgres::Client,
    ) -> Result<WriteResult, ConnectorError> {
        if self.config.changelog_mode {
            return self.flush_changelog(client).await;
        }

        let user_batch = self.concat_buffer()?;
        if user_batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        let upsert_sql = self
            .upsert_sql
            .as_deref()
            .ok_or_else(|| ConnectorError::Internal("upsert SQL not prepared".into()))?;

        let rows = execute_unnest(client, upsert_sql, &user_batch).await?;

        let byte_estimate = estimate_batch_bytes(&user_batch);
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
        client: &tokio_postgres::Client,
    ) -> Result<WriteResult, ConnectorError> {
        let mut total_rows: u64 = 0;
        let mut total_bytes: u64 = 0;

        if self.buffer.is_empty() {
            return Ok(WriteResult::new(0, 0));
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
            let schema = self.buffer[0].schema();
            let combined = arrow_select::concat::concat_batches(&schema, &self.buffer)
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

        // Inserts/updates before deletes: a key inserted then deleted in the same flush window
        // ends deleted; deleted then re-inserted ends at the upserted state.
        if !all_inserts.is_empty() {
            let insert_batch =
                arrow_select::concat::concat_batches(&self.user_schema, &all_inserts)
                    .map_err(|e| ConnectorError::Internal(format!("concat inserts: {e}")))?;

            let upsert_sql = self
                .upsert_sql
                .as_deref()
                .ok_or_else(|| ConnectorError::Internal("upsert SQL not prepared".into()))?;

            let rows = execute_unnest(client, upsert_sql, &insert_batch).await?;
            let bytes = estimate_batch_bytes(&insert_batch);
            self.metrics.record_write(rows, bytes);
            self.metrics.record_upsert();
            total_rows += rows;
            total_bytes += bytes;
        }

        if !all_deletes.is_empty() {
            let delete_batch =
                arrow_select::concat::concat_batches(&self.user_schema, &all_deletes)
                    .map_err(|e| ConnectorError::Internal(format!("concat deletes: {e}")))?;
            let deleted = self.execute_deletes(client, &delete_batch).await?;
            self.metrics.record_deletes(deleted as u64);
            total_rows += deleted as u64;
        }

        self.metrics.record_flush();
        Ok(WriteResult::new(total_rows as usize, total_bytes))
    }

    /// Executes batched DELETE for changelog delete records.
    #[cfg(feature = "postgres-sink")]
    #[allow(clippy::cast_possible_truncation)]
    async fn execute_deletes(
        &self,
        client: &tokio_postgres::Client,
        delete_batch: &RecordBatch,
    ) -> Result<usize, ConnectorError> {
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
            .map_err(|e| ConnectorError::WriteError(format!("DELETE: {e}")))?;

        Ok(rows as usize)
    }

    /// Dispatches flush to the appropriate write mode.
    #[cfg(feature = "postgres-sink")]
    async fn flush_to_client(
        &mut self,
        client: &tokio_postgres::Client,
    ) -> Result<WriteResult, ConnectorError> {
        client
            .execute(
                &format!(
                    "SET statement_timeout = '{}'",
                    self.config.statement_timeout.as_millis()
                ),
                &[],
            )
            .await
            .ok(); // non-fatal if unsupported
        match self.config.write_mode {
            WriteMode::Append => self.flush_append(client).await,
            WriteMode::Upsert => self.flush_upsert(client).await,
        }
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
            PostgresSinkConfig::from_config(config)?
        };
        cfg.validate()?;
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
        self.state = ConnectorState::Initializing;

        if !config.properties().is_empty() {
            self.config = PostgresSinkConfig::from_config(config)?;
        }
        // Validate unconditionally — `from_config` above is skipped when `properties` is empty
        // (pre-set config), so this is the only place all write invariants are guaranteed to run
        // on the config actually in effect.
        self.config.validate()?;

        info!(
            table = %self.config.qualified_table_name(),
            mode = %self.config.write_mode,
            "opening PostgreSQL sink connector"
        );

        self.prepare_statements();

        // Build connection pool.
        let mut pool_cfg = deadpool_postgres::Config::new();
        pool_cfg.host = Some(self.config.hostname.clone());
        pool_cfg.port = Some(self.config.port);
        pool_cfg.dbname = Some(self.config.database.clone());
        pool_cfg.user = Some(self.config.username.clone());
        pool_cfg.password = Some(self.config.password.clone());
        let mut deadpool_cfg = deadpool_postgres::PoolConfig::new(self.config.pool_size);
        deadpool_cfg.timeouts.wait = Some(self.config.connect_timeout);
        deadpool_cfg.timeouts.create = Some(self.config.connect_timeout);
        pool_cfg.pool = Some(deadpool_cfg);

        let pool = pool_cfg
            .create_pool(
                Some(deadpool_postgres::Runtime::Tokio1),
                tokio_postgres::NoTls,
            )
            .map_err(|e| ConnectorError::ConnectionFailed(format!("pool creation failed: {e}")))?;

        // Validate connectivity.
        let client = pool.get().await.map_err(|e| {
            ConnectorError::ConnectionFailed(format!("initial connection failed: {e}"))
        })?;

        client
            .execute(
                &format!(
                    "SET statement_timeout = '{}'",
                    self.config.statement_timeout.as_millis()
                ),
                &[],
            )
            .await
            .ok(); // non-fatal if unsupported

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

        info!(
            table = %self.config.qualified_table_name(),
            pool_size = self.config.pool_size,
            "PostgreSQL sink connector opened"
        );

        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        if batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        self.buffer.push(batch.clone());
        self.buffered_rows += batch.num_rows();

        // Auto-flush when thresholds are reached.
        let should_flush = self.buffered_rows >= self.config.batch_size
            || self.last_flush.elapsed() >= self.config.flush_interval;

        if should_flush {
            let client = self
                .pool()?
                .get()
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(format!("pool checkout: {e}")))?;
            let result = self.flush_to_client(&client).await?;
            self.buffer.clear();
            self.buffered_rows = 0;
            self.last_flush = Instant::now();
            return Ok(result);
        }

        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn suggested_write_timeout(&self) -> Duration {
        // statement_timeout + small margin for pool checkout / setup.
        self.config.statement_timeout + Duration::from_secs(5)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let client = self
            .pool()?
            .get()
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("pool checkout: {e}")))?;
        self.flush_to_client(&client).await?;
        self.buffer.clear();
        self.buffered_rows = 0;
        self.last_flush = Instant::now();
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing PostgreSQL sink connector");

        if !self.buffer.is_empty() {
            if let Some(pool) = &self.pool {
                if let Ok(client) = pool.get().await {
                    let _ = self.flush_to_client(&client).await;
                }
            }
        }

        self.buffer.clear();
        self.buffered_rows = 0;
        self.pool = None;
        self.state = ConnectorState::Closed;

        info!(
            table = %self.config.qualified_table_name(),
            records = self.metrics.records_written.get(),
            "PostgreSQL sink connector closed"
        );

        Ok(())
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

    async fn close(&mut self) -> Result<(), ConnectorError> {
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
            .finish_non_exhaustive()
    }
}

// ── Helper functions ────────────────────────────────────────────────

/// Returns user-visible column names (excluding metadata columns starting with `_`).
fn user_columns(schema: &SchemaRef) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|f| !f.name().starts_with('_'))
        .map(|f| f.name().clone())
        .collect()
}

/// Returns user-visible fields (excluding metadata columns starting with `_`).
fn user_fields(schema: &SchemaRef) -> Vec<&Arc<Field>> {
    schema
        .fields()
        .iter()
        .filter(|f| !f.name().starts_with('_'))
        .collect()
}

/// Builds a schema containing only user-visible columns.
fn build_user_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|f| !f.name().starts_with('_'))
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

/// Filters a `RecordBatch` to include only rows at the given indices.
///
/// Also strips metadata columns (those starting with `_`) from the output.
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
                .filter(|f| !f.name().starts_with('_'))
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
            .filter(|f| !f.name().starts_with('_'))
            .cloned()
            .collect::<Vec<_>>(),
    ));

    let filtered_columns: Vec<Arc<dyn arrow_array::Array>> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !f.name().starts_with('_'))
        .map(|(i, _)| {
            arrow_select::take::take(batch.column(i), &indices_array, None)
                .map_err(|e| ConnectorError::Internal(format!("arrow take failed: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(user_schema, filtered_columns)
        .map_err(|e| ConnectorError::Internal(format!("batch construction failed: {e}")))
}

/// Strips metadata columns (starting with `_`) from a `RecordBatch`.
#[cfg(feature = "postgres-sink")]
fn strip_metadata_columns(batch: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
    let schema = batch.schema();
    let user_indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !f.name().starts_with('_'))
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

/// Executes an UNNEST-based INSERT/UPSERT using Arrow column arrays as parameters.
#[cfg(feature = "postgres-sink")]
async fn execute_unnest(
    client: &tokio_postgres::Client,
    sql: &str,
    batch: &RecordBatch,
) -> Result<u64, ConnectorError> {
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
        .map_err(|e| ConnectorError::WriteError(format!("UNNEST execute: {e}")))
}

/// Rough byte-size estimate for metrics (sum of column buffer sizes).
#[cfg(feature = "postgres-sink")]
fn estimate_batch_bytes(batch: &RecordBatch) -> u64 {
    (0..batch.num_columns())
        .map(|i| batch.column(i).get_buffer_memory_size() as u64)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
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

    // ── Constructor tests ──

    #[test]
    fn test_new_defaults() {
        let sink = PostgresSink::new(test_schema(), test_config(), None);
        assert_eq!(sink.state(), ConnectorState::Created);
        assert_eq!(sink.buffered_rows(), 0);
        assert!(sink.upsert_sql.is_none());
        assert!(sink.copy_sql.is_none());
    }

    #[test]
    fn test_user_schema_strips_metadata() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let sink = PostgresSink::new(schema, test_config(), None);
        assert_eq!(sink.user_schema.fields().len(), 2);
        assert_eq!(sink.user_schema.field(0).name(), "id");
        assert_eq!(sink.user_schema.field(1).name(), "value");
    }

    #[test]
    fn test_schema_returned() {
        let schema = test_schema();
        let sink = PostgresSink::new(schema.clone(), test_config(), None);
        assert_eq!(sink.schema(), schema);
    }

    // ── SQL generation tests ──

    #[test]
    fn test_build_copy_sql() {
        let schema = test_schema();
        let config = test_config();
        let sql = PostgresSink::build_copy_sql(&schema, &config);
        assert_eq!(
            sql,
            "COPY public.events (id, name, value) FROM STDIN BINARY"
        );
    }

    #[test]
    fn test_build_copy_sql_custom_schema() {
        let schema = test_schema();
        let mut config = test_config();
        config.schema_name = "analytics".to_string();
        let sql = PostgresSink::build_copy_sql(&schema, &config);
        assert!(sql.starts_with("COPY analytics.events"));
    }

    #[test]
    fn test_build_copy_sql_excludes_metadata_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, false),
            Field::new("_ts_ms", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let config = test_config();
        let sql = PostgresSink::build_copy_sql(&schema, &config);
        assert_eq!(sql, "COPY public.events (id, value) FROM STDIN BINARY");
    }

    #[test]
    fn test_build_upsert_sql() {
        let schema = test_schema();
        let config = upsert_config();
        let sql = PostgresSink::build_upsert_sql(&schema, &config);

        assert!(sql.starts_with("INSERT INTO public.events"));
        assert!(sql.contains("SELECT * FROM UNNEST"));
        assert!(sql.contains("$1::int8[]"));
        assert!(sql.contains("$2::text[]"));
        assert!(sql.contains("$3::float8[]"));
        assert!(sql.contains("ON CONFLICT (id)"));
        assert!(sql.contains("DO UPDATE SET"));
        assert!(sql.contains("name = EXCLUDED.name"));
        assert!(sql.contains("value = EXCLUDED.value"));
        assert!(!sql.contains("id = EXCLUDED.id"));
    }

    #[test]
    fn test_build_upsert_sql_composite_key() {
        let schema = test_schema();
        let mut config = upsert_config();
        config.primary_key_columns = vec!["id".to_string(), "name".to_string()];
        let sql = PostgresSink::build_upsert_sql(&schema, &config);

        assert!(sql.contains("ON CONFLICT (id, name)"));
        assert!(sql.contains("value = EXCLUDED.value"));
        assert!(!sql.contains("id = EXCLUDED.id"));
        assert!(!sql.contains("name = EXCLUDED.name"));
    }

    #[test]
    fn test_build_upsert_sql_key_only_table() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut config = test_config();
        config.write_mode = WriteMode::Upsert;
        config.primary_key_columns = vec!["id".to_string()];

        let sql = PostgresSink::build_upsert_sql(&schema, &config);
        assert!(sql.contains("DO NOTHING"), "sql: {sql}");
    }

    #[test]
    fn test_build_delete_sql() {
        let schema = test_schema();
        let config = upsert_config();
        let sql = PostgresSink::build_delete_sql(&schema, &config);

        assert_eq!(sql, "DELETE FROM public.events WHERE id = ANY($1::int8[])");
    }

    #[test]
    fn test_build_delete_sql_composite_key() {
        let schema = test_schema();
        let mut config = upsert_config();
        config.primary_key_columns = vec!["id".to_string(), "name".to_string()];
        let sql = PostgresSink::build_delete_sql(&schema, &config);

        // Composite PK must match tuple-wise (UNNEST zips $1/$2 positionally), NOT the
        // cross-product `id = ANY($1) AND name = ANY($2)` which over-deletes (CN-2).
        assert_eq!(
            sql,
            "DELETE FROM public.events AS t USING UNNEST($1::int8[], $2::text[]) AS k(id, name) \
             WHERE t.id = k.id AND t.name = k.name"
        );
        assert!(!sql.contains("id = ANY($1::int8[]) AND name = ANY"));
    }

    #[test]
    fn test_build_create_table_sql() {
        let schema = test_schema();
        let config = upsert_config();
        let sql = PostgresSink::build_create_table_sql(&schema, &config);

        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS public.events"));
        assert!(sql.contains("id BIGINT NOT NULL"));
        assert!(sql.contains("name TEXT"));
        assert!(sql.contains("value DOUBLE PRECISION"));
        assert!(sql.contains("PRIMARY KEY (id)"));
    }

    #[test]
    fn test_build_create_table_sql_no_pk() {
        let schema = test_schema();
        let config = test_config();
        let sql = PostgresSink::build_create_table_sql(&schema, &config);

        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS"));
        assert!(!sql.contains("PRIMARY KEY"));
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

    // ── Buffering tests ──

    #[tokio::test]
    async fn test_write_batch_buffering() {
        let mut config = test_config();
        config.batch_size = 100;
        let mut sink = PostgresSink::new(test_schema(), config, None);
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await.expect("write");

        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 10);
    }

    #[tokio::test]
    async fn test_write_batch_empty() {
        let mut sink = PostgresSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;

        let batch = test_batch(0);
        let result = sink.write_batch(&batch).await.expect("write");
        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 0);
    }

    #[tokio::test]
    async fn test_write_batch_not_running() {
        let mut sink = PostgresSink::new(test_schema(), test_config(), None);

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await;
        assert!(result.is_err());
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
        let sink = PostgresSink::new(test_schema(), config, None);
        let contract = sink.contract(&ConnectorConfig::new("postgres")).unwrap();
        assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
        assert_eq!(contract.topology, SinkTopology::Singleton);
        assert!(contract.accepts_full_changelog());
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
        assert!(debug.contains("public.events"));
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
