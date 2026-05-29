//! `PostgreSQL` on-demand lookup source for cache-miss fallback.
//!
//! A `deadpool`-pooled client issues one parameterized `WHERE pk = ANY($1)`
//! per fetch, so all missed keys of a probe fold into one index-served round
//! trip. [`KeyAligner`] handles key decode and result realignment.
//!
//! v1 limits: single-column key (matches the lookup-enrich operator), and the
//! connection is `NoTls` (client TLS is a connector-wide follow-up the CDC
//! source and sink share).

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
use laminar_core::lookup::source::{ColumnId, LookupError, LookupSource, LookupSourceCapabilities};
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
    schema: SchemaRef,
    aligner: KeyAligner,
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
            "SELECT * FROM {} WHERE \"{}\" = ANY($1)",
            config.table, pk_column
        );

        // Read column metadata via a prepared zero-row statement.
        let client = pool
            .get()
            .await
            .map_err(|e| LookupError::Connection(format!("postgres pool: {e}")))?;
        let stmt = client
            .prepare(&format!("SELECT * FROM {} LIMIT 0", config.table))
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
        _projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let pk_arrays = self.aligner.decode_keys(keys)?;
        let param = Self::build_any_param(pk_arrays[0].as_ref())?;

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| LookupError::Connection(format!("postgres pool: {e}")))?;
        let pg_rows = client
            .query(&self.select_sql, &[&*param])
            .await
            .map_err(|e| LookupError::Query(format!("postgres lookup query: {e}")))?;

        let batches = if pg_rows.is_empty() {
            Vec::new()
        } else {
            vec![rows_to_batch(&self.schema, &pg_rows)?]
        };
        self.aligner.align(keys, &batches)
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        LookupSourceCapabilities {
            supports_batch_lookup: true,
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

/// Build a `deadpool` pool from libpq-style properties (individual keys or a
/// pre-formed `connection`/`connection_string` parsed via `tokio_postgres`).
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
    cfg.create_pool(
        Some(deadpool_postgres::Runtime::Tokio1),
        tokio_postgres::NoTls,
    )
    .map_err(|e| LookupError::Connection(format!("create pool: {e}")))
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
}
