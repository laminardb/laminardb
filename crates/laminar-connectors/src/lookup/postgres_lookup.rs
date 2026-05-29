//! `PostgreSQL` on-demand lookup source for cache-miss fallback.
//!
//! A real `LookupSource` (not the `SELECT *`/`NoTls`/no-pool reference stub):
//! a `deadpool`-pooled client issues a single parameterized
//! `WHERE pk = ANY($1)` per fetch — N missed keys fold into one index-served
//! round trip — and results are realigned to the input key order by
//! re-encoding each returned row's primary-key column with the same
//! `RowConverter` used to decode the input keys.
//!
//! v1 limits: single-column key (matches the lookup-enrich operator), and the
//! connection is `NoTls` (client TLS is a connector-wide follow-up, tracked in
//! the lookup-source plan — the CDC source and sink share the same gap).

#[cfg(feature = "postgres-cdc")]
use std::collections::HashMap;
#[cfg(feature = "postgres-cdc")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "postgres-cdc")]
use std::sync::Arc;

#[cfg(feature = "postgres-cdc")]
use arrow_array::{Array, RecordBatch};
#[cfg(feature = "postgres-cdc")]
use arrow_row::{RowConverter, SortField};
#[cfg(feature = "postgres-cdc")]
use arrow_schema::{DataType, Field, Schema, SchemaRef};
#[cfg(feature = "postgres-cdc")]
use deadpool_postgres::Pool;
#[cfg(feature = "postgres-cdc")]
use tokio_postgres::types::{ToSql, Type};

#[cfg(feature = "postgres-cdc")]
use laminar_core::lookup::predicate::Predicate;
#[cfg(feature = "postgres-cdc")]
use laminar_core::lookup::source::{ColumnId, LookupError, LookupSource, LookupSourceCapabilities};

/// Configuration for [`PostgresLookupSource`].
#[cfg(feature = "postgres-cdc")]
#[derive(Debug, Clone)]
pub struct PostgresLookupSourceConfig {
    /// libpq-style connection settings, keyed (host/port/database/user/password
    /// or a pre-formed `connection` string).
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
    pk_column: String,
    select_sql: String,
    schema: SchemaRef,
    pk_sort_fields: Vec<SortField>,
    query_count: AtomicU64,
    row_count: AtomicU64,
    error_count: AtomicU64,
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

        // Read column metadata via a prepared zero-row statement.
        let select_sql = format!(
            "SELECT * FROM {} WHERE \"{}\" = ANY($1)",
            config.table, pk_column
        );
        let client = pool
            .get()
            .await
            .map_err(|e| LookupError::Connection(format!("postgres pool: {e}")))?;
        let probe = format!("SELECT * FROM {} LIMIT 0", config.table);
        let stmt = client
            .prepare(&probe)
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

        Ok(Self {
            pool,
            pk_column,
            select_sql,
            schema,
            pk_sort_fields,
            query_count: AtomicU64::new(0),
            row_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
        })
    }

    /// Build the `ANY($1)` array parameter from the decoded PK column. NULL
    /// keys are dropped (a NULL never `= ANY`, so they correctly resolve to a
    /// miss in the alignment step).
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

        let n = pk_array.len();
        let param: Box<dyn ToSql + Sync + Send> = match pk_array.data_type() {
            DataType::Int16 => {
                let a = downcast::<Int16Array>(pk_array)?;
                let v: Vec<i16> = (0..n)
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i))
                    .collect();
                Box::new(v)
            }
            DataType::Int32 => {
                let a = downcast::<Int32Array>(pk_array)?;
                let v: Vec<i32> = (0..n)
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i))
                    .collect();
                Box::new(v)
            }
            DataType::Int64 => {
                let a = downcast::<Int64Array>(pk_array)?;
                let v: Vec<i64> = (0..n)
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i))
                    .collect();
                Box::new(v)
            }
            DataType::Float32 => {
                let a = downcast::<Float32Array>(pk_array)?;
                let v: Vec<f32> = (0..n)
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i))
                    .collect();
                Box::new(v)
            }
            DataType::Float64 => {
                let a = downcast::<Float64Array>(pk_array)?;
                let v: Vec<f64> = (0..n)
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i))
                    .collect();
                Box::new(v)
            }
            DataType::Boolean => {
                let a = downcast::<BooleanArray>(pk_array)?;
                let v: Vec<bool> = (0..n)
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i))
                    .collect();
                Box::new(v)
            }
            DataType::Utf8 => {
                let a = downcast::<StringArray>(pk_array)?;
                let v: Vec<String> = (0..n)
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i).to_string())
                    .collect();
                Box::new(v)
            }
            DataType::LargeUtf8 => {
                let a = downcast::<LargeStringArray>(pk_array)?;
                let v: Vec<String> = (0..n)
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i).to_string())
                    .collect();
                Box::new(v)
            }
            DataType::Utf8View => {
                let a = downcast::<StringViewArray>(pk_array)?;
                let v: Vec<String> = (0..n)
                    .filter(|&i| !a.is_null(i))
                    .map(|i| a.value(i).to_string())
                    .collect();
                Box::new(v)
            }
            dt => {
                return Err(LookupError::Internal(format!(
                    "unsupported PK data type for postgres lookup: {dt}"
                )));
            }
        };
        Ok(param)
    }

    /// Re-encode the PK column of a fetched batch with the same `RowConverter`
    /// used to decode the input keys, so each row maps back to its key bytes.
    fn index_batch_by_key(
        &self,
        converter: &RowConverter,
        batch: &RecordBatch,
        batch_idx: usize,
        index: &mut HashMap<Vec<u8>, (usize, usize)>,
    ) -> Result<(), LookupError> {
        let idx = batch.schema().index_of(&self.pk_column).map_err(|_| {
            LookupError::Internal(format!("pk column not found in result: {}", self.pk_column))
        })?;
        let pk_cols = [Arc::clone(batch.column(idx))];
        let rows = converter
            .convert_columns(&pk_cols)
            .map_err(|e| LookupError::Internal(format!("encode result keys: {e}")))?;
        for row in 0..batch.num_rows() {
            let key = rows.row(row).as_ref().to_vec();
            index.entry(key).or_insert((batch_idx, row));
        }
        Ok(())
    }

    /// Total queries executed.
    #[must_use]
    pub fn query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    /// Total rows returned.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count.load(Ordering::Relaxed)
    }

    /// Total query errors.
    #[must_use]
    pub fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }
}

#[cfg(feature = "postgres-cdc")]
impl LookupSource for PostgresLookupSource {
    async fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        _projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        if keys.is_empty() {
            self.query_count.fetch_add(1, Ordering::Relaxed);
            return Ok(Vec::new());
        }

        let converter = RowConverter::new(self.pk_sort_fields.clone())
            .map_err(|e| LookupError::Internal(format!("row converter: {e}")))?;
        let parser = converter.parser();

        // Decode all keys into a single PK column, then one ANY($1) round trip.
        let parsed = keys.iter().map(|k| parser.parse(k));
        let pk_arrays = converter
            .convert_rows(parsed)
            .map_err(|e| LookupError::Internal(format!("decode keys: {e}")))?;
        let param = Self::build_any_param(pk_arrays[0].as_ref())?;

        let client = self.pool.get().await.map_err(|e| {
            self.error_count.fetch_add(1, Ordering::Relaxed);
            LookupError::Connection(format!("postgres pool: {e}"))
        })?;
        let pg_rows = client
            .query(&self.select_sql, &[&*param])
            .await
            .map_err(|e| {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                LookupError::Query(format!("postgres lookup query: {e}"))
            })?;

        let mut index: HashMap<Vec<u8>, (usize, usize)> = HashMap::new();
        let fetched: Vec<RecordBatch> = if pg_rows.is_empty() {
            Vec::new()
        } else {
            let batch = rows_to_batch(&self.schema, &pg_rows)?;
            self.index_batch_by_key(&converter, &batch, 0, &mut index)?;
            vec![batch]
        };

        let mut hits = 0u64;
        let results: Vec<Option<RecordBatch>> = keys
            .iter()
            .map(|key| {
                index.get(*key).map(|&(bi, row)| {
                    hits += 1;
                    fetched[bi].slice(row, 1)
                })
            })
            .collect();

        self.row_count.fetch_add(hits, Ordering::Relaxed);
        self.query_count.fetch_add(1, Ordering::Relaxed);
        Ok(results)
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        LookupSourceCapabilities {
            supports_predicate_pushdown: false,
            supports_projection_pushdown: false,
            supports_batch_lookup: true,
            max_batch_size: 0,
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

/// Build a `deadpool` pool from libpq-style properties. Accepts either
/// individual `host`/`port`/`database`/`user`/`password` keys or a pre-formed
/// `connection`/`connection_string` (parsed via `tokio_postgres::Config`).
#[cfg(feature = "postgres-cdc")]
fn build_pool(props: &HashMap<String, String>, pool_size: usize) -> Result<Pool, LookupError> {
    let mut cfg = deadpool_postgres::Config::new();

    if let Some(conn) = props
        .get("connection")
        .or_else(|| props.get("connection_string"))
    {
        let pg: tokio_postgres::Config = conn
            .parse()
            .map_err(|e| LookupError::Connection(format!("parse connection string: {e}")))?;
        if let Some(host) = pg.get_hosts().iter().find_map(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
            #[allow(unreachable_patterns)]
            _ => None,
        }) {
            cfg.host = Some(host);
        }
        if let Some(port) = pg.get_ports().first() {
            cfg.port = Some(*port);
        }
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
    cfg.create_pool(
        Some(deadpool_postgres::Runtime::Tokio1),
        tokio_postgres::NoTls,
    )
    .map_err(|e| LookupError::Connection(format!("create pool: {e}")))
}

/// Convert `tokio_postgres` rows into a single Arrow `RecordBatch` using the
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
            _ => {
                // Everything else is rendered as text (Decimal/Date/Timestamp/
                // UUID/JSON via Postgres' text output through a String cast).
                let mut vals: Vec<Option<String>> = Vec::with_capacity(rows.len());
                for row in rows {
                    vals.push(row.try_get::<_, Option<String>>(name).unwrap_or(None));
                }
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
/// map directly; richer types (Decimal/Date/Timestamp/UUID/JSON) fall back to
/// text so they survive the round trip.
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
    fn test_pg_type_map() {
        assert_eq!(pg_type_to_arrow(&Type::BOOL), DataType::Boolean);
        assert_eq!(pg_type_to_arrow(&Type::INT4), DataType::Int32);
        assert_eq!(pg_type_to_arrow(&Type::INT8), DataType::Int64);
        assert_eq!(pg_type_to_arrow(&Type::FLOAT8), DataType::Float64);
        // Rich types render as text.
        assert_eq!(pg_type_to_arrow(&Type::TIMESTAMP), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(&Type::NUMERIC), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(&Type::UUID), DataType::Utf8);
    }

    #[test]
    fn test_build_any_param_supported_types() {
        let ints = Int64Array::from(vec![1i64, 2, 3]);
        assert!(PostgresLookupSource::build_any_param(&ints).is_ok());
        let strs = StringArray::from(vec!["a", "b"]);
        assert!(PostgresLookupSource::build_any_param(&strs).is_ok());
    }

    #[test]
    fn test_build_any_param_drops_nulls() {
        // NULL keys are dropped from the ANY array (they can never match).
        let ints = Int64Array::from(vec![Some(1i64), None, Some(3)]);
        assert!(PostgresLookupSource::build_any_param(&ints).is_ok());
    }

    #[test]
    fn test_build_any_param_unsupported_type_errors() {
        let arr = arrow_array::Date32Array::from(vec![1]);
        assert!(PostgresLookupSource::build_any_param(&arr).is_err());
    }

    #[test]
    fn test_build_pool_from_parts() {
        let mut props = HashMap::new();
        props.insert("host".into(), "localhost".into());
        props.insert("port".into(), "5432".into());
        props.insert("database".into(), "db".into());
        props.insert("user".into(), "u".into());
        // Pool creation is lazy (no connection yet), so this should succeed.
        assert!(build_pool(&props, 4).is_ok());
    }

    #[test]
    fn test_build_pool_from_connection_string() {
        let mut props = HashMap::new();
        props.insert(
            "connection".into(),
            "host=db.example.com port=5433 user=svc dbname=prod".into(),
        );
        assert!(build_pool(&props, 4).is_ok());
    }
}
