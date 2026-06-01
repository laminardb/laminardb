//! `PostgreSQL` on-demand lookup source for cache-miss fallback.
//!
//! A `deadpool`-pooled client issues one parameterized `WHERE pk = ANY($1)`
//! per fetch, so all missed keys of a probe fold into one index-served round
//! trip. [`KeyAligner`] handles key decode and result realignment.
//!
//! TLS is server-auth via `rustls`: `sslmode = disable` (default) leaves the
//! connection plaintext; `require` / `verify-ca` / `verify-full` all enable
//! TLS with full server-certificate verification (chain + hostname) against
//! `sslrootcert` (CA PEM) or, absent that, the Mozilla webpki roots. There is
//! deliberately no insecure skip-verify, and the weaker libpq variants are not
//! emulated (the three modes are aliases for "verified TLS"). v1 limits:
//! single-column key, server-auth only (no mTLS client certs).

#[cfg(feature = "postgres-cdc")]
use std::collections::HashMap;
#[cfg(feature = "postgres-cdc")]
use std::sync::Arc;

#[cfg(feature = "postgres-cdc")]
use arrow_array::{Array, RecordBatch};
#[cfg(feature = "postgres-cdc")]
use arrow_row::SortField;
#[cfg(feature = "postgres-cdc")]
use arrow_schema::{DataType, Field, Schema, SchemaRef};
#[cfg(feature = "postgres-cdc")]
use deadpool_postgres::Pool;
#[cfg(feature = "postgres-cdc")]
use tokio_postgres::types::{ToSql, Type};

#[cfg(feature = "postgres-cdc")]
use laminar_core::lookup::predicate::Predicate;
#[cfg(feature = "postgres-cdc")]
use laminar_core::lookup::source::{
    projection_names, ColumnId, LookupError, LookupSource, LookupSourceCapabilities,
};
#[cfg(feature = "postgres-cdc")]
use laminar_core::lookup::KeyAligner;

/// Configuration for [`PostgresLookupSource`].
#[cfg(feature = "postgres-cdc")]
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
#[cfg(feature = "postgres-cdc")]
pub struct PostgresLookupSource {
    pool: Pool,
    select_sql: String,
    /// Quoted table name and key column, kept to build a projected `SELECT`
    /// when a query pushes down a column projection.
    table: String,
    pk_column: String,
    schema: SchemaRef,
    aligner: KeyAligner,
}

#[cfg(feature = "postgres-cdc")]
fn quote_identifier(name: &str) -> String {
    if name.contains('.') {
        name.split('.')
            .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(".")
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[cfg(feature = "postgres-cdc")]
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
        let pk_column = config.primary_key_columns[0].clone();

        let pool = build_pool(&config.properties, config.pool_size)?;
        let select_sql = format!(
            "SELECT * FROM {} WHERE {} = ANY($1)",
            quote_identifier(&config.table),
            quote_identifier(&pk_column)
        );

        // Read column metadata via a prepared zero-row statement.
        let client = pool
            .get()
            .await
            .map_err(|e| LookupError::Connection(format!("postgres pool: {e}")))?;
        let stmt = client
            .prepare(&format!("SELECT * FROM {} LIMIT 0", quote_identifier(&config.table)))
            .await
            .map_err(|e| LookupError::Connection(format!("prepare schema probe: {e}")))?;
        let fields: Vec<Field> = stmt
            .columns()
            .iter()
            .map(|c| Field::new(c.name(), pg_type_to_arrow(c.type_()), true))
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        let pk_idx = schema.index_of(&pk_column).map_err(|_| {
            LookupError::Internal(format!("pk column not found in table: {pk_column}"))
        })?;
        let pk_sort_fields = vec![SortField::new(schema.field(pk_idx).data_type().clone())];
        let aligner = KeyAligner::new(pk_sort_fields, config.primary_key_columns)?;

        Ok(Self {
            pool,
            select_sql,
            table: config.table,
            pk_column,
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

#[cfg(feature = "postgres-cdc")]
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

        let pk_arrays = self.aligner.decode_keys(keys)?;
        let param = Self::build_any_param(pk_arrays[0].as_ref())?;

        // Projection pushdown: SELECT only the requested columns (always incl.
        // the key, so the row maps back); else the prebuilt `SELECT *`. The
        // result schema follows so `rows_to_batch` reads the right columns.
        let (sql, out_schema, project_needed) = if projection.is_empty() {
            (self.select_sql.clone(), Arc::clone(&self.schema), false)
        } else {
            let mut proj_names = projection_names(&self.schema, projection)?;
            let mut idx: Vec<usize> = projection.iter().map(|&c| c as usize).collect();
            let mut project_needed = false;

            if !proj_names.contains(&self.pk_column) {
                proj_names.push(self.pk_column.clone());
                let pk_idx = self.schema.index_of(&self.pk_column)
                    .map_err(|e| LookupError::Internal(format!("pk column index: {e}")))?;
                idx.push(pk_idx);
                project_needed = true;
            }

            let cols = proj_names
                .iter()
                .map(|n| quote_identifier(n))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT {cols} FROM {} WHERE {} = ANY($1)",
                quote_identifier(&self.table),
                quote_identifier(&self.pk_column)
            );
            let proj_schema = Arc::new(
                self.schema
                    .project(&idx)
                    .map_err(|e| LookupError::Internal(format!("project postgres schema: {e}")))?,
            );
            (sql, proj_schema, project_needed)
        };

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| LookupError::Connection(format!("postgres pool: {e}")))?;
        let pg_rows = client
            .query(&sql, &[&*param])
            .await
            .map_err(|e| LookupError::Query(format!("postgres lookup query: {e}")))?;

        let batches = if pg_rows.is_empty() {
            Vec::new()
        } else {
            vec![rows_to_batch(&out_schema, &pg_rows)?]
        };
        let aligned = self.aligner.align(keys, &batches)?;

        if project_needed {
            let orig_names = projection_names(&self.schema, projection)?;
            let mut projected_aligned = Vec::with_capacity(aligned.len());
            for maybe_batch in aligned {
                if let Some(batch) = maybe_batch {
                    let indices: Vec<usize> = orig_names
                        .iter()
                        .map(|name| batch.schema().index_of(name).map_err(|e| LookupError::Internal(format!("column not found in aligned schema: {e}"))))
                        .collect::<Result<Vec<usize>, LookupError>>()?;
                    let projected = batch.project(&indices)
                        .map_err(|e| LookupError::Internal(format!("project aligned batch: {e}")))?;
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| LookupError::Connection(format!("health check pool: {e}")))?;
        client
            .query_one("SELECT 1", &[])
            .await
            .map(|_| ())
            .map_err(|e| LookupError::Connection(format!("health check: {e}")))
    }
}

#[cfg(feature = "postgres-cdc")]
fn parse_conn_string_params(conn: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if conn.starts_with("postgresql://") || conn.starts_with("postgres://") {
        if let Some(pos) = conn.find('?') {
            let query = &conn[pos + 1..];
            for pair in query.split('&') {
                let mut parts = pair.splitn(2, '=');
                if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                    params.insert(k.to_string(), v.replace("%2F", "/").replace("%2f", "/"));
                }
            }
        }
    } else {
        let mut chars = conn.chars().peekable();
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() {
                chars.next();
                continue;
            }
            let mut key = String::new();
            while let Some(&c) = chars.peek() {
                if c == '=' {
                    chars.next();
                    break;
                }
                if c.is_whitespace() {
                    break;
                }
                key.push(c);
                chars.next();
            }
            if key.is_empty() {
                break;
            }
            let mut val = String::new();
            if chars.peek() == Some(&'\'') {
                chars.next();
                while let Some(c) = chars.next() {
                    if c == '\'' {
                        break;
                    }
                    val.push(c);
                }
            } else {
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() {
                        break;
                    }
                    val.push(c);
                    chars.next();
                }
            }
            params.insert(key, val);
        }
    }
    params
}

/// Build a `deadpool` pool from libpq-style properties (individual keys or a
/// pre-formed `connection`/`connection_string` parsed via `tokio_postgres`).
#[cfg(feature = "postgres-cdc")]
fn build_pool(props: &HashMap<String, String>, pool_size: usize) -> Result<Pool, LookupError> {
    let mut cfg = deadpool_postgres::Config::new();
    let mut merged_props = props.clone();

    if let Some(conn) = props
        .get("connection")
        .or_else(|| props.get("connection_string"))
    {
        let conn_params = parse_conn_string_params(conn);
        for (k, v) in conn_params {
            merged_props.insert(k, v);
        }

        let pg: tokio_postgres::Config = conn
            .parse()
            .map_err(|e| LookupError::Connection(format!("parse connection string: {e}")))?;
        cfg.host = pg.get_hosts().iter().find_map(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
            #[allow(unreachable_patterns)]
            _ => None,
        });
        cfg.port = pg.get_ports().first().copied();
        cfg.dbname = pg.get_dbname().map(str::to_string);
        cfg.user = pg.get_user().map(str::to_string);
        cfg.password = pg
            .get_password()
            .map(|p| String::from_utf8_lossy(p).into_owned());
    } else {
        cfg.host = props.get("host").cloned();
        cfg.port = props.get("port").and_then(|p| p.parse().ok());
        cfg.dbname = props
            .get("database")
            .or_else(|| props.get("dbname"))
            .cloned();
        cfg.user = props.get("user").cloned();
        cfg.password = props.get("password").cloned();
    }

    cfg.pool = Some(deadpool_postgres::PoolConfig::new(pool_size.max(1)));
    let runtime = Some(deadpool_postgres::Runtime::Tokio1);

    // `create_pool` is generic over the TLS connector but erases it into the
    // same `Pool` type, so the two arms unify.
    if tls_enabled(&merged_props)? {
        let connector = build_rustls_connector(&merged_props)?;
        cfg.create_pool(runtime, connector)
            .map_err(|e| LookupError::Connection(format!("create pool: {e}")))
    } else {
        cfg.create_pool(runtime, tokio_postgres::NoTls)
            .map_err(|e| LookupError::Connection(format!("create pool: {e}")))
    }
}

/// Whether the configured `sslmode`/`ssl.mode` requests TLS. Absent or
/// `disable` → no TLS (backward compatible); `require`/`verify-ca`/`verify-full`
/// → verified TLS. `prefer` (opportunistic fallback) is rejected because a
/// pooled static connector cannot implement its plaintext fallback.
#[cfg(feature = "postgres-cdc")]
fn tls_enabled(props: &HashMap<String, String>) -> Result<bool, LookupError> {
    let Some(mode) = props.get("sslmode").or_else(|| props.get("ssl.mode")) else {
        return Ok(false);
    };
    match mode.to_ascii_lowercase().as_str() {
        "disable" => Ok(false),
        "require" | "verify-ca" | "verify-full" => Ok(true),
        other => Err(LookupError::Connection(format!(
            "unsupported sslmode '{other}' (use disable/require/verify-ca/verify-full)"
        ))),
    }
}

/// Build a server-auth rustls TLS connector. Roots come from `sslrootcert`
/// (CA PEM) if set, otherwise the Mozilla webpki roots; the server certificate
/// is always verified (no insecure skip-verify).
#[cfg(feature = "postgres-cdc")]
fn build_rustls_connector(
    props: &HashMap<String, String>,
) -> Result<tokio_postgres_rustls::MakeRustlsConnect, LookupError> {
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    // Idempotent process-wide install; matches the rest of the workspace.
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut roots = RootCertStore::empty();
    if let Some(ca_path) = props.get("sslrootcert").or_else(|| props.get("ssl.ca")) {
        let pem = std::fs::read(ca_path)
            .map_err(|e| LookupError::Connection(format!("read sslrootcert '{ca_path}': {e}")))?;
        let certs = rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| LookupError::Connection(format!("parse sslrootcert: {e}")))?;
        if certs.is_empty() {
            return Err(LookupError::Connection(
                "sslrootcert contained no certificates".into(),
            ));
        }
        for cert in certs {
            roots
                .add(cert)
                .map_err(|e| LookupError::Connection(format!("add CA cert: {e}")))?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let client_cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(client_cfg))
}

/// Convert `tokio_postgres` rows into one Arrow `RecordBatch` via the
/// pre-derived schema.
#[cfg(feature = "postgres-cdc")]
fn rows_to_batch(
    schema: &SchemaRef,
    rows: &[tokio_postgres::Row],
) -> Result<RecordBatch, LookupError> {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
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
            // Everything else (Decimal/Date/Timestamp/UUID/JSON) renders as text.
            _ => {
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, Option<String>>(name).unwrap_or(None))
                    .collect();
                Arc::new(StringArray::from(vals))
            }
        };
        columns.push(array);
    }
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| LookupError::Internal(format!("arrow batch construction: {e}")))
}

/// Collect a typed nullable column from all rows.
#[cfg(feature = "postgres-cdc")]
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

/// Map a `tokio_postgres` type to an Arrow `DataType`. Native columnar types
/// map directly; richer types fall back to text so they survive the round trip.
#[cfg(feature = "postgres-cdc")]
fn pg_type_to_arrow(pg_type: &Type) -> DataType {
    match *pg_type {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        _ => DataType::Utf8,
    }
}

#[cfg(all(test, feature = "postgres-cdc"))]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};

    #[test]
    fn pg_type_map_native_and_text_fallback() {
        assert_eq!(pg_type_to_arrow(&Type::INT8), DataType::Int64);
        assert_eq!(pg_type_to_arrow(&Type::FLOAT8), DataType::Float64);
        assert_eq!(pg_type_to_arrow(&Type::BOOL), DataType::Boolean);
        // Rich types render as text.
        assert_eq!(pg_type_to_arrow(&Type::TIMESTAMP), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(&Type::NUMERIC), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(&Type::UUID), DataType::Utf8);
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
        assert!(!tls_enabled(&HashMap::new()).unwrap()); // absent → no TLS
        assert!(!tls_enabled(&props(&[("sslmode", "disable")])).unwrap());
        assert!(tls_enabled(&props(&[("sslmode", "require")])).unwrap());
        assert!(tls_enabled(&props(&[("ssl.mode", "verify-full")])).unwrap());
        assert!(tls_enabled(&props(&[("sslmode", "bogus")])).is_err());
    }

    #[test]
    fn tls_connector_builds_with_roots_and_rejects_bad_ca() {
        // Default webpki roots: builds without a CA file.
        assert!(build_rustls_connector(&HashMap::new()).is_ok());
        // An explicit but missing CA path is a clear error, not a panic.
        assert!(build_rustls_connector(&props(&[("sslrootcert", "/no/such/ca.pem")])).is_err());
    }
}
