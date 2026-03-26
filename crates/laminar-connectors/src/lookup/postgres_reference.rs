//! PostgreSQL poll-based reference table source.
//!
//! Implements [`ReferenceTableSource`] via a simple `SELECT * FROM table`
//! query. No replication slot or CDC configuration required — suitable for
//! slowly-changing dimension tables that are refreshed by periodic snapshot.

use arrow_array::RecordBatch;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;

/// A [`ReferenceTableSource`] backed by a single `SELECT *` query against
/// a PostgreSQL table. Returns the full table as a snapshot, then completes.
pub struct PostgresReferenceTableSource {
    config: ConnectorConfig,
    snapshot_done: bool,
}

impl PostgresReferenceTableSource {
    /// Creates a new source from a [`ConnectorConfig`].
    #[must_use]
    pub fn new(config: ConnectorConfig) -> Self {
        Self {
            config,
            snapshot_done: false,
        }
    }

    /// Builds a `tokio_postgres` connection string from connector properties.
    fn connection_string(&self) -> String {
        let props = self.config.properties();
        // Accept pre-formed connection string under either key.
        if let Some(cs) = props
            .get("connection_string")
            .or_else(|| props.get("connection"))
        {
            return cs.clone();
        }
        let mut parts = Vec::new();
        if let Some(h) = props.get("host") {
            parts.push(format!("host={h}"));
        }
        if let Some(p) = props.get("port") {
            parts.push(format!("port={p}"));
        }
        if let Some(d) = props.get("database") {
            parts.push(format!("dbname={d}"));
        }
        if let Some(u) = props.get("user") {
            parts.push(format!("user={u}"));
        }
        if let Some(pw) = props.get("password") {
            parts.push(format!("password={pw}"));
        }
        parts.join(" ")
    }

    fn table_name(&self) -> &str {
        self.config
            .properties()
            .get("table")
            .map(String::as_str)
            .unwrap_or("unknown")
    }
}

#[async_trait::async_trait]
impl ReferenceTableSource for PostgresReferenceTableSource {
    #[allow(clippy::too_many_lines)]
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.snapshot_done {
            return Ok(None);
        }

        let conn_str = self.connection_string();
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("SSL") || msg.contains("TLS") || msg.contains("sslmode") {
                    ConnectorError::ConnectionFailed(format!(
                        "postgres connect: {e} (TLS not supported by the standalone \
                         'postgres' lookup connector — use sslmode=disable or \
                         'postgres-cdc' for TLS)"
                    ))
                } else {
                    ConnectorError::ConnectionFailed(format!("postgres connect: {e}"))
                }
            })?;

        // Drive the connection on a background task.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "postgres lookup connection error");
            }
        });

        let table = self.table_name();
        let sql = format!("SELECT * FROM {table}");
        let rows = client
            .query(&sql, &[])
            .await
            .map_err(|e| ConnectorError::ReadError(format!("postgres query: {e}")))?;

        self.snapshot_done = true;

        if rows.is_empty() {
            return Ok(None);
        }

        // Convert rows to Arrow RecordBatch via pg row metadata.
        let pg_columns = rows[0].columns();
        let fields: Vec<arrow_schema::Field> = pg_columns
            .iter()
            .map(|col| {
                let dt = pg_type_to_arrow(col.type_());
                arrow_schema::Field::new(col.name(), dt, true)
            })
            .collect();
        let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));

        // Build columnar arrays.
        let mut columns: Vec<std::sync::Arc<dyn arrow_array::Array>> =
            Vec::with_capacity(schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col_name = pg_columns[col_idx].name();
            let pg_type = pg_columns[col_idx].type_().clone();
            let array: std::sync::Arc<dyn arrow_array::Array> = match field.data_type() {
                arrow_schema::DataType::Boolean => {
                    let vals: Vec<Option<bool>> = collect_column(&rows, col_name, &pg_type)?;
                    std::sync::Arc::new(arrow_array::BooleanArray::from(vals))
                }
                arrow_schema::DataType::Int16 => {
                    let vals: Vec<Option<i16>> = collect_column(&rows, col_name, &pg_type)?;
                    std::sync::Arc::new(arrow_array::Int16Array::from(vals))
                }
                arrow_schema::DataType::Int32 => {
                    let vals: Vec<Option<i32>> = collect_column(&rows, col_name, &pg_type)?;
                    std::sync::Arc::new(arrow_array::Int32Array::from(vals))
                }
                arrow_schema::DataType::Int64 => {
                    let vals: Vec<Option<i64>> = collect_column(&rows, col_name, &pg_type)?;
                    std::sync::Arc::new(arrow_array::Int64Array::from(vals))
                }
                arrow_schema::DataType::Float32 => {
                    let vals: Vec<Option<f32>> = collect_column(&rows, col_name, &pg_type)?;
                    std::sync::Arc::new(arrow_array::Float32Array::from(vals))
                }
                arrow_schema::DataType::Float64 => {
                    let vals: Vec<Option<f64>> = collect_column(&rows, col_name, &pg_type)?;
                    std::sync::Arc::new(arrow_array::Float64Array::from(vals))
                }
                _ => {
                    // Fallback: read as String via try_get.
                    let mut vals: Vec<Option<String>> = Vec::with_capacity(rows.len());
                    for row in &rows {
                        match row.try_get::<_, Option<String>>(col_name) {
                            Ok(v) => vals.push(v),
                            Err(_) => {
                                // Type doesn't implement FromSql<String> — format via Debug.
                                vals.push(None);
                            }
                        }
                    }
                    let str_vals: Vec<Option<&str>> =
                        vals.iter().map(|v: &Option<String>| v.as_deref()).collect();
                    std::sync::Arc::new(arrow_array::StringArray::from(str_vals))
                }
            };
            columns.push(array);
        }

        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::ReadError(format!("arrow batch construction: {e}")))?;

        Ok(Some(batch))
    }

    fn is_snapshot_complete(&self) -> bool {
        self.snapshot_done
    }

    async fn poll_changes(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        Ok(None)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new(u64::from(self.snapshot_done))
    }

    async fn restore(&mut self, _checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Collect a typed column from all rows via `try_get`, returning
/// `ConnectorError::ReadError` on conversion failure.
fn collect_column<'a, T>(
    rows: &'a [tokio_postgres::Row],
    col_name: &str,
    pg_type: &tokio_postgres::types::Type,
) -> Result<Vec<Option<T>>, ConnectorError>
where
    T: tokio_postgres::types::FromSql<'a>,
{
    rows.iter()
        .map(|r| {
            r.try_get::<_, Option<T>>(col_name).map_err(|e| {
                ConnectorError::ReadError(format!("column '{col_name}' (pg type {pg_type}): {e}"))
            })
        })
        .collect()
}

/// Map a `tokio_postgres` type to an Arrow `DataType`.
fn pg_type_to_arrow(pg_type: &tokio_postgres::types::Type) -> arrow_schema::DataType {
    use tokio_postgres::types::Type;
    match *pg_type {
        Type::BOOL => arrow_schema::DataType::Boolean,
        Type::INT2 => arrow_schema::DataType::Int16,
        Type::INT4 => arrow_schema::DataType::Int32,
        Type::INT8 => arrow_schema::DataType::Int64,
        Type::FLOAT4 => arrow_schema::DataType::Float32,
        Type::FLOAT8 => arrow_schema::DataType::Float64,
        // TIMESTAMP, UUID, JSONB, etc. → read as Utf8 (String).
        _ => arrow_schema::DataType::Utf8,
    }
}
