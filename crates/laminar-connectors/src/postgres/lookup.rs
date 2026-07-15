//! `PostgreSQL` on-demand lookup source for cache-miss fallback.
//!
//! A `deadpool`-pooled client issues one parameterized `WHERE pk = ANY($1)`
//! per fetch, so all missed keys of a probe fold into one index-served round
//! trip. [`KeyAligner`](laminar_core::lookup::KeyAligner) handles key decode and result realignment.
//!
//! TLS is server-auth via `rustls`: verified chain and hostname is the default,
//! using `ssl.ca.cert.path` when set and Mozilla roots otherwise. Plaintext is
//! available only through explicit `ssl.mode=disable`. Weaker libpq modes are
//! rejected rather than presented as aliases. v1 limits: single-column key,
//! server-auth only (no mTLS client certs).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, RecordBatch};
use arrow_row::SortField;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deadpool_postgres::Pool;
use tokio_postgres::types::{ToSql, Type};

use laminar_core::lookup::predicate::Predicate;
use laminar_core::lookup::source::{
    projection_names, ColumnId, LookupError, LookupSource, LookupSourceCapabilities,
};
use laminar_core::lookup::KeyAligner;

use super::await_owned_driver;

const MAX_LOOKUP_KEYS: usize = 4_096;
const MAX_LOOKUP_KEY_BYTES: usize = 4 * 1024 * 1024;
const MAX_LOOKUP_RESULT_BYTES: usize = 64 * 1024 * 1024;
const MAX_POOL_SIZE: usize = 64;
const POOL_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const UNIQUE_LOOKUP_KEY_QUERY: &str = r#"
WITH target AS (
    SELECT pg_catalog.to_regclass($1)::oid AS table_oid
)
SELECT
    target.table_oid,
    EXISTS (
        SELECT 1
        FROM pg_catalog.pg_index AS idx
        JOIN pg_catalog.pg_attribute AS attr
          ON attr.attrelid = idx.indrelid
         AND attr.attnum = idx.indkey[0]
        WHERE idx.indrelid = target.table_oid
          AND idx.indisunique
          AND idx.indisvalid
          AND idx.indisready
          AND idx.indislive
          AND idx.indnkeyatts = 1
          AND idx.indpred IS NULL
          AND idx.indexprs IS NULL
          AND attr.attname = $2
          AND attr.attnum > 0
          AND NOT attr.attisdropped
    ) AS has_unique_key
FROM target
"#;

async fn await_lookup_driver<T>(
    operation: &'static str,
    future: impl std::future::Future<Output = Result<T, LookupError>> + Send + 'static,
) -> Result<T, LookupError>
where
    T: Send + 'static,
{
    await_owned_driver(future, move |error| {
        LookupError::Internal(format!("postgres {operation} task failed: {error}"))
    })
    .await
}

fn validate_unique_lookup_key(
    table: &str,
    key: &str,
    table_oid: Option<u32>,
    has_unique_key: bool,
) -> Result<u32, LookupError> {
    let table_oid = table_oid.ok_or_else(|| {
        LookupError::Internal(format!(
            "postgres lookup table {table} could not be resolved with to_regclass"
        ))
    })?;
    if !has_unique_key {
        return Err(LookupError::Internal(format!(
            "postgres lookup requires a valid, ready, non-partial unique index whose sole key column is '{key}' on {table}"
        )));
    }
    Ok(table_oid)
}

/// Configuration for [`PostgresLookupSource`].
#[derive(Debug, Clone)]
pub struct PostgresLookupSourceConfig {
    /// libpq-style connection settings (host/port/database/user/password or a
    /// pre-formed `connection` string).
    pub properties: HashMap<String, String>,
    /// Table name (optionally schema-qualified).
    pub table: String,
    /// Primary key column names (v1: exactly one).
    pub primary_key_columns: Vec<String>,
    /// Connection pool size.
    pub pool_size: usize,
}

/// `PostgreSQL` lookup source for on-demand/partial cache mode.
pub struct PostgresLookupSource {
    pool: Pool,
    select_sql: String,
    select_expressions: Vec<String>,
    /// Quoted table name, kept to build a projected `SELECT`.
    table: String,
    pk_column: String,
    quoted_pk_column: String,
    schema: SchemaRef,
    aligner: KeyAligner,
}

fn quote_identifier(name: &str) -> Result<String, LookupError> {
    if name.is_empty() || name.contains('\0') {
        return Err(LookupError::Internal(
            "postgres identifiers must be non-empty and cannot contain NUL".into(),
        ));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

fn quote_qualified_identifier(name: &str) -> Result<String, LookupError> {
    name.split('.')
        .map(quote_identifier)
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("."))
}

impl PostgresLookupSource {
    /// Opens a pooled connection and derives the table's Arrow schema.
    ///
    /// # Errors
    ///
    /// Returns `LookupError` if the pool/connection fails, the key is not a
    /// single column, or the table schema cannot be read.
    pub async fn open(config: PostgresLookupSourceConfig) -> Result<Self, LookupError> {
        if config.primary_key_columns.len() != 1 {
            return Err(LookupError::Internal(format!(
                "postgres lookup requires exactly one primary key column, got {}",
                config.primary_key_columns.len()
            )));
        }
        if config.pool_size == 0 || config.pool_size > MAX_POOL_SIZE {
            return Err(LookupError::Connection(format!(
                "postgres lookup pool_size must be between 1 and {MAX_POOL_SIZE}, got {}",
                config.pool_size
            )));
        }
        let pk_column = config.primary_key_columns[0].clone();
        let table = quote_qualified_identifier(&config.table)?;
        let quoted_pk_column = quote_identifier(&pk_column)?;

        let pool = build_pool(&config.properties, config.pool_size)?;

        // Keep the checked-out client in the owned task until prepare is terminal. If the
        // startup waiter is cancelled, the task continues and cannot return an in-flight client.
        let probe_pool = pool.clone();
        let schema_probe = format!("SELECT * FROM {table} LIMIT 0");
        let probe_table = table.clone();
        let probe_key = pk_column.clone();
        let stmt = await_lookup_driver("schema probe", async move {
            let client = probe_pool
                .get()
                .await
                .map_err(|e| LookupError::Connection(format!("postgres pool: {e}")))?;
            let identity = match tokio::time::timeout(
                QUERY_TIMEOUT,
                client.query_one(
                    UNIQUE_LOOKUP_KEY_QUERY,
                    &[&probe_table.as_str(), &probe_key.as_str()],
                ),
            )
            .await
            {
                Ok(result) => result.map_err(|e| {
                    LookupError::Connection(format!("inspect postgres lookup key index: {e}"))
                })?,
                Err(_) => {
                    discard_pool_client(client);
                    return Err(LookupError::Timeout(QUERY_TIMEOUT));
                }
            };
            let table_oid = identity
                .try_get::<_, Option<u32>>("table_oid")
                .map_err(|e| LookupError::Connection(format!("decode lookup table OID: {e}")))?;
            let has_unique_key = identity
                .try_get::<_, bool>("has_unique_key")
                .map_err(|e| LookupError::Connection(format!("decode lookup index check: {e}")))?;
            validate_unique_lookup_key(&probe_table, &probe_key, table_oid, has_unique_key)?;
            match tokio::time::timeout(QUERY_TIMEOUT, client.prepare(&schema_probe)).await {
                Ok(result) => result
                    .map_err(|e| LookupError::Connection(format!("prepare schema probe: {e}"))),
                Err(_) => {
                    discard_pool_client(client);
                    Err(LookupError::Timeout(QUERY_TIMEOUT))
                }
            }
        })
        .await?;
        let fields: Vec<Field> = stmt
            .columns()
            .iter()
            .map(|c| Field::new(c.name(), pg_type_to_arrow(c.type_()), true))
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        let pk_idx = schema.index_of(&pk_column).map_err(|_| {
            LookupError::Internal(format!("pk column not found in table: {pk_column}"))
        })?;
        let pk_pg_type = stmt.columns()[pk_idx].type_();
        if !supports_any_parameter(pk_pg_type) {
            return Err(LookupError::Internal(format!(
                "postgres lookup primary key column '{pk_column}' has unsupported type {pk_pg_type}"
            )));
        }
        let select_expressions = stmt
            .columns()
            .iter()
            .map(select_expression)
            .collect::<Result<Vec<_>, _>>()?;
        let select_sql = format!(
            "SELECT {} FROM {table} WHERE {quoted_pk_column} = ANY($1) LIMIT {}",
            select_expressions.join(", "),
            MAX_LOOKUP_KEYS + 1
        );
        let pk_sort_fields = vec![SortField::new(schema.field(pk_idx).data_type().clone())];
        let aligner = KeyAligner::new(pk_sort_fields, config.primary_key_columns)?;

        Ok(Self {
            pool,
            select_sql,
            select_expressions,
            table,
            pk_column,
            quoted_pk_column,
            schema,
            aligner,
        })
    }

    /// Build the `ANY($1)` array parameter from the decoded PK column. NULL
    /// keys are dropped (a NULL never `= ANY`, so they resolve to a miss).
    fn build_any_param(pk_array: &dyn Array) -> Result<Box<dyn ToSql + Sync + Send>, LookupError> {
        use arrow_array::{
            BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
            LargeStringArray, StringArray, StringViewArray,
        };

        fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, LookupError> {
            array
                .as_any()
                .downcast_ref::<T>()
                .ok_or_else(|| LookupError::Internal("pk column downcast failed".into()))
        }
        fn non_null<A: Array, T>(a: &A, get: impl Fn(usize) -> T) -> Vec<T> {
            (0..a.len()).filter(|&i| !a.is_null(i)).map(get).collect()
        }

        let param: Box<dyn ToSql + Sync + Send> = match pk_array.data_type() {
            DataType::Int16 => {
                let a = downcast::<Int16Array>(pk_array)?;
                Box::new(non_null(a, |i| a.value(i)))
            }
            DataType::Int32 => {
                let a = downcast::<Int32Array>(pk_array)?;
                Box::new(non_null(a, |i| a.value(i)))
            }
            DataType::Int64 => {
                let a = downcast::<Int64Array>(pk_array)?;
                Box::new(non_null(a, |i| a.value(i)))
            }
            DataType::Float32 => {
                let a = downcast::<Float32Array>(pk_array)?;
                Box::new(non_null(a, |i| a.value(i)))
            }
            DataType::Float64 => {
                let a = downcast::<Float64Array>(pk_array)?;
                Box::new(non_null(a, |i| a.value(i)))
            }
            DataType::Boolean => {
                let a = downcast::<BooleanArray>(pk_array)?;
                Box::new(non_null(a, |i| a.value(i)))
            }
            DataType::Utf8 => {
                let a = downcast::<StringArray>(pk_array)?;
                Box::new(non_null(a, |i| a.value(i).to_string()))
            }
            DataType::LargeUtf8 => {
                let a = downcast::<LargeStringArray>(pk_array)?;
                Box::new(non_null(a, |i| a.value(i).to_string()))
            }
            DataType::Utf8View => {
                let a = downcast::<StringViewArray>(pk_array)?;
                Box::new(non_null(a, |i| a.value(i).to_string()))
            }
            dt => {
                return Err(LookupError::Internal(format!(
                    "unsupported PK data type for postgres lookup: {dt}"
                )));
            }
        };
        Ok(param)
    }
}

fn validate_lookup_keys(keys: &[&[u8]]) -> Result<(), LookupError> {
    if keys.len() > MAX_LOOKUP_KEYS {
        return Err(LookupError::Query(format!(
            "postgres lookup received {} keys, exceeding the fixed {MAX_LOOKUP_KEYS}-key batch limit",
            keys.len()
        )));
    }
    let bytes = keys.iter().try_fold(0_usize, |total, key| {
        total
            .checked_add(key.len())
            .ok_or_else(|| LookupError::Query("postgres lookup key byte count overflow".into()))
    })?;
    if bytes > MAX_LOOKUP_KEY_BYTES {
        return Err(LookupError::Query(format!(
            "postgres lookup received {bytes} key bytes, exceeding the fixed {MAX_LOOKUP_KEY_BYTES}-byte batch limit"
        )));
    }
    Ok(())
}

fn enforce_lookup_result_bytes(batch: &RecordBatch) -> Result<(), LookupError> {
    let bytes = batch.columns().iter().try_fold(0_usize, |total, column| {
        total
            .checked_add(column.get_array_memory_size())
            .ok_or_else(|| LookupError::Query("postgres lookup result byte count overflow".into()))
    })?;
    if bytes > MAX_LOOKUP_RESULT_BYTES {
        return Err(LookupError::Query(format!(
            "postgres lookup result retains {bytes} bytes, exceeding the fixed {MAX_LOOKUP_RESULT_BYTES}-byte limit"
        )));
    }
    Ok(())
}

impl LookupSource for PostgresLookupSource {
    async fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        validate_lookup_keys(keys)?;
        let pk_arrays = self.aligner.decode_keys(keys)?;
        let pk_array = pk_arrays
            .first()
            .ok_or_else(|| LookupError::Internal("postgres lookup decoded no key column".into()))?
            .as_ref();
        if pk_array.len() != keys.len() {
            return Err(LookupError::Internal(format!(
                "postgres lookup decoded {} keys from {} inputs",
                pk_array.len(),
                keys.len()
            )));
        }
        let unique_key_count = keys
            .iter()
            .enumerate()
            .filter(|(index, _)| !pk_array.is_null(*index))
            .map(|(_, key)| *key)
            .collect::<rustc_hash::FxHashSet<_>>()
            .len();

        let param = Self::build_any_param(pk_array)?;

        // Projection pushdown selects only requested columns plus the key used
        // for result alignment. Unsupported native result types use the text
        // casts derived during the schema probe.
        let (sql, out_schema, project_needed) = if projection.is_empty() {
            (self.select_sql.clone(), Arc::clone(&self.schema), false)
        } else {
            let mut proj_names = projection_names(&self.schema, projection)?;
            let mut idx: Vec<usize> = projection.iter().map(|&c| c as usize).collect();
            let mut project_needed = false;

            if !proj_names.contains(&self.pk_column) {
                proj_names.push(self.pk_column.clone());
                let pk_idx = self
                    .schema
                    .index_of(&self.pk_column)
                    .map_err(|e| LookupError::Internal(format!("pk column index: {e}")))?;
                idx.push(pk_idx);
                project_needed = true;
            }

            let cols = idx
                .iter()
                .map(|&index| {
                    self.select_expressions
                        .get(index)
                        .map(String::as_str)
                        .ok_or_else(|| {
                            LookupError::Internal(format!(
                                "postgres projection column index {index} is out of bounds"
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            let sql = format!(
                "SELECT {cols} FROM {} WHERE {} = ANY($1) LIMIT {}",
                self.table,
                self.quoted_pk_column,
                MAX_LOOKUP_KEYS + 1
            );
            let proj_schema = Arc::new(
                self.schema
                    .project(&idx)
                    .map_err(|e| LookupError::Internal(format!("project postgres schema: {e}")))?,
            );
            (sql, proj_schema, project_needed)
        };

        let query_pool = self.pool.clone();
        let pg_rows = await_lookup_driver("lookup query", async move {
            let client = query_pool
                .get()
                .await
                .map_err(|e| LookupError::Connection(format!("postgres pool: {e}")))?;
            match tokio::time::timeout(QUERY_TIMEOUT, client.query(&sql, &[&*param])).await {
                Ok(result) => {
                    result.map_err(|e| LookupError::Query(format!("postgres lookup query: {e}")))
                }
                Err(_) => {
                    discard_pool_client(client);
                    Err(LookupError::Timeout(QUERY_TIMEOUT))
                }
            }
        })
        .await?;
        if pg_rows.len() > unique_key_count {
            return Err(LookupError::Query(format!(
                "postgres lookup returned {} rows for {unique_key_count} distinct keys; the configured key column is not unique",
                pg_rows.len()
            )));
        }

        let batches = if pg_rows.is_empty() {
            Vec::new()
        } else {
            let batch = rows_to_batch(&out_schema, &pg_rows)?;
            enforce_lookup_result_bytes(&batch)?;
            vec![batch]
        };
        let aligned = self.aligner.align(keys, &batches)?;

        if project_needed {
            let orig_names = projection_names(&self.schema, projection)?;
            let mut projected_aligned = Vec::with_capacity(aligned.len());
            for maybe_batch in aligned {
                if let Some(batch) = maybe_batch {
                    let indices: Vec<usize> = orig_names
                        .iter()
                        .map(|name| {
                            batch.schema().index_of(name).map_err(|e| {
                                LookupError::Internal(format!(
                                    "column not found in aligned schema: {e}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<usize>, LookupError>>()?;
                    let projected = batch.project(&indices).map_err(|e| {
                        LookupError::Internal(format!("project aligned batch: {e}"))
                    })?;
                    projected_aligned.push(Some(projected));
                } else {
                    projected_aligned.push(None);
                }
            }
            Ok(projected_aligned)
        } else {
            Ok(aligned)
        }
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        LookupSourceCapabilities {
            supports_batch_lookup: true,
            supports_projection_pushdown: true,
            max_batch_size: MAX_LOOKUP_KEYS,
            ..LookupSourceCapabilities::none()
        }
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn source_name(&self) -> &str {
        "postgres"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        let health_pool = self.pool.clone();
        await_lookup_driver("health check", async move {
            let client = health_pool
                .get()
                .await
                .map_err(|e| LookupError::Connection(format!("health check pool: {e}")))?;
            match tokio::time::timeout(QUERY_TIMEOUT, client.query_one("SELECT 1", &[])).await {
                Ok(result) => result
                    .map(|_| ())
                    .map_err(|e| LookupError::Connection(format!("health check: {e}"))),
                Err(_) => {
                    discard_pool_client(client);
                    Err(LookupError::Timeout(QUERY_TIMEOUT))
                }
            }
        })
        .await
    }
}

fn discard_pool_client(client: deadpool_postgres::Client) {
    drop(deadpool_postgres::Client::take(client));
}

/// Build a `deadpool` pool from libpq-style properties (individual keys or a
/// pre-formed `connection`/`connection_string` parsed via `tokio_postgres`).
fn build_pool(props: &HashMap<String, String>, pool_size: usize) -> Result<Pool, LookupError> {
    if pool_size == 0 || pool_size > MAX_POOL_SIZE {
        return Err(LookupError::Connection(format!(
            "postgres lookup pool_size must be between 1 and {MAX_POOL_SIZE}, got {pool_size}"
        )));
    }
    let mut cfg = deadpool_postgres::Config::new();
    for (left, right) in [
        ("connection", "connection_string"),
        ("database", "dbname"),
        ("user", "username"),
    ] {
        if props.contains_key(left) && props.contains_key(right) {
            return Err(LookupError::Connection(format!(
                "postgres lookup cannot configure both '{left}' and '{right}'"
            )));
        }
    }
    for key in ["host", "database", "dbname", "user", "username"] {
        if props.get(key).is_some_and(|value| value.trim().is_empty()) {
            return Err(LookupError::Connection(format!(
                "postgres lookup '{key}' must not be empty"
            )));
        }
    }

    if let Some(conn) = props
        .get("connection")
        .or_else(|| props.get("connection_string"))
    {
        if conn.trim().is_empty() {
            return Err(LookupError::Connection(
                "postgres lookup connection string must not be empty".into(),
            ));
        }
        if let Some(conflict) = [
            "host", "port", "database", "dbname", "user", "username", "password", "options",
        ]
        .into_iter()
        .find(|key| props.contains_key(*key))
        {
            return Err(LookupError::Connection(format!(
                "postgres lookup cannot combine a connection string with '{conflict}'"
            )));
        }
        let parsed: tokio_postgres::Config = conn
            .parse()
            .map_err(|e| LookupError::Connection(format!("parse connection string: {e}")))?;
        if parsed.get_ports().contains(&0) {
            return Err(LookupError::Connection(
                "postgres lookup port must be greater than zero".into(),
            ));
        }
        if parsed.get_user().is_none_or(str::is_empty) {
            return Err(LookupError::Connection(
                "postgres lookup connection string must specify a user".into(),
            ));
        }
        if parsed.get_dbname().is_none_or(str::is_empty) {
            return Err(LookupError::Connection(
                "postgres lookup connection string must specify a database".into(),
            ));
        }
        cfg.url = Some(conn.clone());
    } else {
        cfg.host = props.get("host").cloned();
        cfg.port = props
            .get("port")
            .map(|port| {
                let parsed = port.parse::<u16>().map_err(|error| {
                    LookupError::Connection(format!(
                        "invalid postgres lookup port '{port}': {error}"
                    ))
                })?;
                if parsed == 0 {
                    return Err(LookupError::Connection(
                        "postgres lookup port must be greater than zero".into(),
                    ));
                }
                Ok(parsed)
            })
            .transpose()?;
        cfg.dbname = props
            .get("database")
            .or_else(|| props.get("dbname"))
            .cloned();
        cfg.user = props.get("user").or_else(|| props.get("username")).cloned();
        if cfg.user.is_none() {
            return Err(LookupError::Connection(
                "postgres lookup requires 'user' or 'username'".into(),
            ));
        }
        if cfg.dbname.is_none() {
            return Err(LookupError::Connection(
                "postgres lookup requires 'database' or 'dbname'".into(),
            ));
        }
        cfg.password = props.get("password").cloned();
        if let Some(options) = props.get("options") {
            if options.contains('\0') {
                return Err(LookupError::Connection(
                    "postgres lookup options cannot contain NUL".into(),
                ));
            }
            cfg.options = Some(options.clone());
        }
    }

    cfg.connect_timeout = Some(CONNECT_TIMEOUT);
    let mut pool_config = deadpool_postgres::PoolConfig::new(pool_size);
    pool_config.timeouts.wait = Some(POOL_WAIT_TIMEOUT);
    pool_config.timeouts.create = Some(CONNECT_TIMEOUT);
    pool_config.timeouts.recycle = Some(POOL_WAIT_TIMEOUT);
    cfg.pool = Some(pool_config);
    let runtime = Some(deadpool_postgres::Runtime::Tokio1);
    let ssl_mode = ssl_mode(props)?;
    cfg.ssl_mode = Some(driver_ssl_mode(ssl_mode));

    match ssl_mode {
        crate::postgres::SslMode::VerifyFull => {
            let connector = build_rustls_connector(props)?;
            cfg.create_pool(runtime, connector)
                .map_err(|e| LookupError::Connection(format!("create pool: {e}")))
        }
        crate::postgres::SslMode::Disable => cfg
            .create_pool(runtime, tokio_postgres::NoTls)
            .map_err(|e| LookupError::Connection(format!("create pool: {e}"))),
    }
}

fn driver_ssl_mode(mode: crate::postgres::SslMode) -> deadpool_postgres::SslMode {
    match mode {
        crate::postgres::SslMode::Disable => deadpool_postgres::SslMode::Disable,
        crate::postgres::SslMode::VerifyFull => deadpool_postgres::SslMode::Require,
    }
}

/// Whether the configuration requests verified TLS. It is secure by default;
/// plaintext requires an explicit opt-out.
fn ssl_mode(props: &HashMap<String, String>) -> Result<crate::postgres::SslMode, LookupError> {
    if props.contains_key("sslmode") {
        return Err(LookupError::Connection(
            "postgres lookup uses ssl.mode (disable or verify-full), not sslmode".into(),
        ));
    }
    if props
        .get("ssl.ca.cert.path")
        .is_some_and(|path| path.trim().is_empty())
    {
        return Err(LookupError::Connection(
            "postgres lookup ssl.ca.cert.path must not be empty".into(),
        ));
    }
    let mode = match props.get("ssl.mode") {
        Some(value) => value.parse::<crate::postgres::SslMode>().map_err(|_| {
            LookupError::Connection(format!(
                "unsupported ssl.mode '{value}' (use disable or verify-full)"
            ))
        })?,
        None => crate::postgres::SslMode::default(),
    };
    if mode == crate::postgres::SslMode::Disable && props.contains_key("ssl.ca.cert.path") {
        return Err(LookupError::Connection(
            "postgres lookup ssl.ca.cert.path requires ssl.mode=verify-full".into(),
        ));
    }
    Ok(mode)
}

/// Build a server-auth rustls TLS connector. Roots come from `ssl.ca.cert.path`
/// (CA PEM) if set, otherwise the Mozilla webpki roots; the server certificate
/// is always verified (no insecure skip-verify).
fn build_rustls_connector(
    props: &HashMap<String, String>,
) -> Result<tokio_postgres_rustls::MakeRustlsConnect, LookupError> {
    let ca_path = props.get("ssl.ca.cert.path").map(std::path::Path::new);
    crate::postgres::make_rustls_connector(ca_path)
        .map_err(|error| LookupError::Connection(error.to_string()))
}

/// Convert `tokio_postgres` rows into one Arrow `RecordBatch` via the
/// pre-derived schema.
fn rows_to_batch(
    schema: &SchemaRef,
    rows: &[tokio_postgres::Row],
) -> Result<RecordBatch, LookupError> {
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, StringArray, TimestampMicrosecondArray,
    };

    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name().as_str();
        let array: Arc<dyn Array> = match field.data_type() {
            DataType::Boolean => Arc::new(
                collect_col::<bool>(rows, name)?
                    .into_iter()
                    .collect::<BooleanArray>(),
            ),
            DataType::Int16 => Arc::new(Int16Array::from(collect_col::<i16>(rows, name)?)),
            DataType::Int32 => Arc::new(Int32Array::from(collect_col::<i32>(rows, name)?)),
            DataType::Int64 => Arc::new(Int64Array::from(collect_col::<i64>(rows, name)?)),
            DataType::Float32 => Arc::new(Float32Array::from(collect_col::<f32>(rows, name)?)),
            DataType::Float64 => Arc::new(Float64Array::from(collect_col::<f64>(rows, name)?)),
            DataType::Utf8 => {
                let values = collect_col::<String>(rows, name)?;
                Arc::new(StringArray::from(
                    values.iter().map(Option::as_deref).collect::<Vec<_>>(),
                ))
            }
            DataType::Binary => {
                let values = collect_col::<Vec<u8>>(rows, name)?;
                Arc::new(BinaryArray::from(
                    values.iter().map(Option::as_deref).collect::<Vec<_>>(),
                ))
            }
            DataType::Date32 => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
                let values = collect_col::<chrono::NaiveDate>(rows, name)?
                    .into_iter()
                    .map(|value| {
                        value
                            .map(|date| {
                                i32::try_from(date.signed_duration_since(epoch).num_days()).map_err(
                                    |_| {
                                        LookupError::Internal(format!(
                                            "postgres column '{name}' contains a date outside the Arrow Date32 range"
                                        ))
                                    },
                                )
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(Date32Array::from(values))
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let values = collect_col::<chrono::NaiveDateTime>(rows, name)?
                    .into_iter()
                    .map(|value| value.map(|timestamp| timestamp.and_utc().timestamp_micros()))
                    .collect::<Vec<_>>();
                Arc::new(TimestampMicrosecondArray::from(values))
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone))
                if timezone.as_ref() == "UTC" =>
            {
                let values = collect_col::<chrono::DateTime<chrono::Utc>>(rows, name)?
                    .into_iter()
                    .map(|value| value.map(|timestamp| timestamp.timestamp_micros()))
                    .collect::<Vec<_>>();
                Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"))
            }
            unsupported => {
                return Err(LookupError::Internal(format!(
                    "unsupported PostgreSQL lookup result type {unsupported}"
                )));
            }
        };
        columns.push(array);
    }
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| LookupError::Internal(format!("arrow batch construction: {e}")))
}

/// Collect a typed nullable column from all rows.
fn collect_col<'a, T>(
    rows: &'a [tokio_postgres::Row],
    name: &str,
) -> Result<Vec<Option<T>>, LookupError>
where
    T: tokio_postgres::types::FromSql<'a>,
{
    rows.iter()
        .map(|r| {
            r.try_get::<_, Option<T>>(name)
                .map_err(|e| LookupError::Internal(format!("column '{name}': {e}")))
        })
        .collect()
}

/// Map a `tokio_postgres` type to an Arrow `DataType`. Types without a native
/// mapping are explicitly projected as PostgreSQL text.
fn pg_type_to_arrow(pg_type: &Type) -> DataType {
    native_pg_type_to_arrow(pg_type).unwrap_or(DataType::Utf8)
}

fn native_pg_type_to_arrow(pg_type: &Type) -> Option<DataType> {
    match *pg_type {
        Type::BOOL => Some(DataType::Boolean),
        Type::INT2 => Some(DataType::Int16),
        Type::INT4 => Some(DataType::Int32),
        Type::INT8 => Some(DataType::Int64),
        Type::FLOAT4 => Some(DataType::Float32),
        Type::FLOAT8 => Some(DataType::Float64),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => Some(DataType::Utf8),
        Type::BYTEA => Some(DataType::Binary),
        Type::DATE => Some(DataType::Date32),
        Type::TIMESTAMP => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        Type::TIMESTAMPTZ => Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        _ => None,
    }
}

fn supports_any_parameter(pg_type: &Type) -> bool {
    matches!(
        *pg_type,
        Type::BOOL
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::TEXT
            | Type::VARCHAR
            | Type::BPCHAR
            | Type::NAME
    )
}

fn select_expression(column: &tokio_postgres::Column) -> Result<String, LookupError> {
    select_expression_for(column.name(), column.type_())
}

fn select_expression_for(name: &str, pg_type: &Type) -> Result<String, LookupError> {
    let identifier = quote_identifier(name)?;
    if native_pg_type_to_arrow(pg_type).is_some() {
        Ok(identifier)
    } else {
        Ok(format!("CAST({identifier} AS TEXT) AS {identifier}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};

    #[test]
    fn pg_type_map_native_and_explicit_text_projection() {
        assert_eq!(pg_type_to_arrow(&Type::INT8), DataType::Int64);
        assert_eq!(pg_type_to_arrow(&Type::FLOAT8), DataType::Float64);
        assert_eq!(pg_type_to_arrow(&Type::BOOL), DataType::Boolean);
        assert_eq!(
            pg_type_to_arrow(&Type::TIMESTAMP),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(pg_type_to_arrow(&Type::NUMERIC), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(&Type::UUID), DataType::Utf8);
        assert_eq!(
            select_expression_for("amount", &Type::NUMERIC).unwrap(),
            "CAST(\"amount\" AS TEXT) AS \"amount\""
        );
        assert_eq!(
            select_expression_for("created_at", &Type::TIMESTAMP).unwrap(),
            "\"created_at\""
        );
        assert!(!supports_any_parameter(&Type::UUID));
        assert!(supports_any_parameter(&Type::INT8));
    }

    #[test]
    fn unique_key_catalog_probe_is_fail_closed_and_allows_include_columns() {
        for required in [
            "pg_catalog.to_regclass($1)",
            "idx.indisunique",
            "idx.indisvalid",
            "idx.indisready",
            "idx.indislive",
            "idx.indnkeyatts = 1",
            "idx.indpred IS NULL",
            "idx.indexprs IS NULL",
            "attr.attnum = idx.indkey[0]",
            "attr.attname = $2",
        ] {
            assert!(
                UNIQUE_LOOKUP_KEY_QUERY.contains(required),
                "missing {required}"
            );
        }
        assert!(
            !UNIQUE_LOOKUP_KEY_QUERY.contains("indnatts = 1"),
            "included columns must not invalidate a single-key unique index"
        );

        assert!(validate_unique_lookup_key("events", "id", None, false).is_err());
        assert!(validate_unique_lookup_key("events", "id", Some(42), false).is_err());
        assert_eq!(
            validate_unique_lookup_key("events", "id", Some(42), true).unwrap(),
            42
        );
    }

    #[test]
    fn any_param_built_for_supported_types_skipping_nulls() {
        assert!(
            PostgresLookupSource::build_any_param(&Int64Array::from(vec![
                Some(1i64),
                None,
                Some(3)
            ]))
            .is_ok()
        );
        assert!(PostgresLookupSource::build_any_param(&StringArray::from(vec!["a", "b"])).is_ok());
    }

    #[test]
    fn any_param_rejects_unsupported_type() {
        assert!(
            PostgresLookupSource::build_any_param(&arrow_array::Date32Array::from(vec![1]))
                .is_err()
        );
    }

    fn props(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect()
    }

    #[test]
    fn tls_mode_parsing() {
        assert_eq!(
            ssl_mode(&HashMap::new()).unwrap(),
            crate::postgres::SslMode::VerifyFull
        );
        assert_eq!(
            ssl_mode(&props(&[("ssl.mode", "disable")])).unwrap(),
            crate::postgres::SslMode::Disable
        );
        assert_eq!(
            ssl_mode(&props(&[("ssl.mode", "verify-full")])).unwrap(),
            crate::postgres::SslMode::VerifyFull
        );
        assert_eq!(
            driver_ssl_mode(crate::postgres::SslMode::VerifyFull),
            deadpool_postgres::SslMode::Require
        );
        assert_eq!(
            driver_ssl_mode(crate::postgres::SslMode::Disable),
            deadpool_postgres::SslMode::Disable
        );
        for rejected in ["prefer", "require", "verify-ca", "bogus"] {
            assert!(ssl_mode(&props(&[("ssl.mode", rejected)])).is_err());
        }
        assert!(ssl_mode(&props(&[("sslmode", "disable")])).is_err());
        assert!(ssl_mode(&props(&[
            ("ssl.mode", "disable"),
            ("ssl.ca.cert.path", "/certs/ca.pem"),
        ]))
        .is_err());
    }

    #[test]
    fn lookup_key_admission_is_bounded() {
        assert!(validate_lookup_keys(&[&b"a"[..], &b"bc"[..]]).is_ok());

        let too_many = vec![&b""[..]; MAX_LOOKUP_KEYS + 1];
        assert!(validate_lookup_keys(&too_many).is_err());

        let oversized = vec![0_u8; MAX_LOOKUP_KEY_BYTES + 1];
        assert!(validate_lookup_keys(&[oversized.as_slice()]).is_err());
    }

    #[test]
    fn pool_configuration_rejects_invalid_values() {
        let base = [
            ("host", "localhost"),
            ("database", "db"),
            ("user", "user"),
            ("ssl.mode", "disable"),
        ];
        assert!(build_pool(&props(&base), 0).is_err());
        assert!(build_pool(&props(&base), MAX_POOL_SIZE + 1).is_err());

        let mut invalid_port = props(&base);
        invalid_port.insert("port".into(), "not-a-port".into());
        assert!(build_pool(&invalid_port, 1).is_err());
        invalid_port.insert("port".into(), "0".into());
        assert!(build_pool(&invalid_port, 1).is_err());

        let mut invalid_options = props(&base);
        invalid_options.insert("options".into(), "bad\0option".into());
        assert!(build_pool(&invalid_options, 1).is_err());

        let mut empty_user = props(&base);
        empty_user.insert("user".into(), " ".into());
        assert!(build_pool(&empty_user, 1).is_err());

        let conflict = props(&[
            ("connection", "host=localhost dbname=db user=user"),
            ("host", "other"),
            ("ssl.mode", "disable"),
        ]);
        assert!(build_pool(&conflict, 1).is_err());

        let zero_port = props(&[
            ("connection", "host=localhost port=0 dbname=db user=user"),
            ("ssl.mode", "disable"),
        ]);
        assert!(build_pool(&zero_port, 1).is_err());
        assert!(build_pool(&props(&[("connection", ""), ("ssl.mode", "disable")]), 1).is_err());
    }

    #[test]
    fn identifier_validation_rejects_unsafe_shapes() {
        assert_eq!(
            quote_qualified_identifier("public.events").unwrap(),
            "\"public\".\"events\""
        );
        assert!(quote_qualified_identifier("public.").is_err());
        assert!(quote_identifier("bad\0name").is_err());
    }

    #[test]
    fn tls_connector_builds_with_roots_and_rejects_bad_ca() {
        // Default webpki roots: builds without a CA file.
        assert!(build_rustls_connector(&HashMap::new()).is_ok());
        // An explicit but missing CA path is a clear error, not a panic.
        assert!(
            build_rustls_connector(&props(&[("ssl.ca.cert.path", "/no/such/ca.pem")])).is_err()
        );
    }
}
