//! `PostgreSQL` startup snapshot source for reference tables.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;

use super::await_owned_driver;

const SNAPSHOT_ROWS_PER_BATCH: usize = 4_096;
const MAX_SNAPSHOT_BATCH_BYTES: usize = 64 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const SNAPSHOT_CURSOR: &str = "laminar_reference_snapshot";

fn timeout_error(timeout: Duration) -> ConnectorError {
    ConnectorError::Timeout(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX))
}

async fn await_reference_driver<T>(
    operation: &'static str,
    future: impl std::future::Future<Output = Result<T, ConnectorError>> + Send + 'static,
) -> Result<T, ConnectorError>
where
    T: Send + 'static,
{
    await_owned_driver(future, move |error| {
        ConnectorError::Internal(format!(
            "postgres reference {operation} task failed: {error}"
        ))
    })
    .await
}

struct PostgresSession {
    client: tokio_postgres::Client,
    connection: tokio::task::JoinHandle<()>,
}

impl Drop for PostgresSession {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

fn spawn_connection<F>(connection: F) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "postgres reference connection failed");
        }
    })
}

async fn owned_batch_execute(
    session: PostgresSession,
    sql: String,
    operation: &'static str,
) -> Result<PostgresSession, ConnectorError> {
    await_reference_driver(operation, async move {
        match tokio::time::timeout(QUERY_TIMEOUT, session.client.batch_execute(&sql)).await {
            Ok(Ok(())) => Ok(session),
            Ok(Err(error)) => Err(ConnectorError::ReadError(format!(
                "postgres {operation}: {error}"
            ))),
            Err(_) => Err(timeout_error(QUERY_TIMEOUT)),
        }
    })
    .await
}

async fn owned_prepare(
    session: PostgresSession,
    sql: String,
    operation: &'static str,
) -> Result<(PostgresSession, tokio_postgres::Statement), ConnectorError> {
    await_reference_driver(operation, async move {
        match tokio::time::timeout(QUERY_TIMEOUT, session.client.prepare(&sql)).await {
            Ok(Ok(statement)) => Ok((session, statement)),
            Ok(Err(error)) => Err(ConnectorError::ReadError(format!(
                "postgres {operation}: {error}"
            ))),
            Err(_) => Err(timeout_error(QUERY_TIMEOUT)),
        }
    })
    .await
}

async fn owned_query(
    session: PostgresSession,
    sql: String,
    operation: &'static str,
) -> Result<(PostgresSession, Vec<tokio_postgres::Row>), ConnectorError> {
    await_reference_driver(operation, async move {
        match tokio::time::timeout(QUERY_TIMEOUT, session.client.query(&sql, &[])).await {
            Ok(Ok(rows)) => Ok((session, rows)),
            Ok(Err(error)) => Err(ConnectorError::ReadError(format!(
                "postgres {operation}: {error}"
            ))),
            Err(_) => Err(timeout_error(QUERY_TIMEOUT)),
        }
    })
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Ready,
    Reading,
    Done,
    Closed,
}

/// A finite `PostgreSQL` `SELECT` snapshot projected into the declared table schema.
pub struct PostgresReferenceTableSource {
    config: ConnectorConfig,
    declared_schema: SchemaRef,
    state: State,
    session: Option<PostgresSession>,
}

impl PostgresReferenceTableSource {
    /// Creates a source for the LaminarDB-declared table schema.
    #[must_use]
    pub fn new(config: ConnectorConfig, declared_schema: SchemaRef) -> Self {
        Self {
            config,
            declared_schema,
            state: State::Ready,
            session: None,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn postgres_config(&self) -> Result<tokio_postgres::Config, ConnectorError> {
        let properties = self.config.properties();
        for (left, right) in [
            ("connection", "connection_string"),
            ("database", "dbname"),
            ("user", "username"),
        ] {
            if properties.contains_key(left) && properties.contains_key(right) {
                return Err(ConnectorError::ConfigurationError(format!(
                    "postgres reference cannot configure both '{left}' and '{right}'"
                )));
            }
        }
        for key in ["host", "database", "dbname", "user", "username"] {
            if properties
                .get(key)
                .is_some_and(|value| value.trim().is_empty())
            {
                return Err(ConnectorError::ConfigurationError(format!(
                    "postgres reference '{key}' must not be empty"
                )));
            }
        }
        let connection_string = properties
            .get("connection_string")
            .or_else(|| properties.get("connection"));
        let mut config = if let Some(connection_string) = connection_string {
            if connection_string.trim().is_empty() {
                return Err(ConnectorError::ConfigurationError(
                    "postgres reference connection string must not be empty".into(),
                ));
            }
            if let Some(conflict) = [
                "host", "port", "database", "dbname", "user", "username", "password", "options",
            ]
            .into_iter()
            .find(|key| properties.contains_key(*key))
            {
                return Err(ConnectorError::ConfigurationError(format!(
                    "postgres reference cannot combine a connection string with '{conflict}'"
                )));
            }
            connection_string.parse().map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "invalid postgres reference connection: {error}"
                ))
            })?
        } else {
            let mut config = tokio_postgres::Config::new();
            if let Some(host) = properties.get("host") {
                config.host(host);
            }
            if let Some(port) = properties.get("port") {
                let port = port.parse::<u16>().map_err(|error| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid postgres reference port '{port}': {error}"
                    ))
                })?;
                if port == 0 {
                    return Err(ConnectorError::ConfigurationError(
                        "postgres reference port must be greater than zero".into(),
                    ));
                }
                config.port(port);
            }
            if let Some(database) = properties
                .get("database")
                .or_else(|| properties.get("dbname"))
            {
                config.dbname(database);
            }
            if let Some(user) = properties
                .get("user")
                .or_else(|| properties.get("username"))
            {
                config.user(user);
            }
            if let Some(password) = properties.get("password") {
                config.password(password);
            }
            if let Some(options) = properties.get("options") {
                if options.contains('\0') {
                    return Err(ConnectorError::ConfigurationError(
                        "postgres reference options cannot contain NUL".into(),
                    ));
                }
                config.options(options);
            }
            config
        };
        if config.get_ports().contains(&0) {
            return Err(ConnectorError::ConfigurationError(
                "postgres reference port must be greater than zero".into(),
            ));
        }

        if properties.contains_key("sslmode") {
            return Err(ConnectorError::ConfigurationError(
                "postgres reference uses ssl.mode (disable or verify-full), not sslmode".into(),
            ));
        }
        let ssl_mode = self.ssl_mode()?;
        let ca_path = properties.get("ssl.ca.cert.path");
        if ca_path.is_some_and(|path| path.trim().is_empty()) {
            return Err(ConnectorError::ConfigurationError(
                "postgres reference ssl.ca.cert.path must not be empty".into(),
            ));
        }
        if ssl_mode == crate::postgres::SslMode::Disable && ca_path.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "ssl.ca.cert.path requires ssl.mode=verify-full".into(),
            ));
        }
        config.ssl_mode(match ssl_mode {
            crate::postgres::SslMode::Disable => tokio_postgres::config::SslMode::Disable,
            crate::postgres::SslMode::VerifyFull => tokio_postgres::config::SslMode::Require,
        });
        config.connect_timeout(CONNECT_TIMEOUT);
        Ok(config)
    }

    fn snapshot_query(&self) -> Result<String, ConnectorError> {
        let table = self.config.get("table").ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "postgres reference source requires a 'table' property".into(),
            )
        })?;
        let table = quote_qualified_identifier(table)?;
        let columns = self
            .declared_schema
            .fields()
            .iter()
            .map(|field| quote_identifier(field.name()))
            .collect::<Result<Vec<_>, _>>()?;
        if columns.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "postgres reference source requires at least one declared column".into(),
            ));
        }
        Ok(format!("SELECT {} FROM {table}", columns.join(", ")))
    }

    fn projected_snapshot_query(
        &self,
        columns: &[tokio_postgres::Column],
    ) -> Result<String, ConnectorError> {
        if columns.len() != self.declared_schema.fields().len() {
            return Err(ConnectorError::ReadError(format!(
                "postgres snapshot has {} columns but {} were declared",
                columns.len(),
                self.declared_schema.fields().len()
            )));
        }
        let expressions = columns
            .iter()
            .zip(self.declared_schema.fields())
            .map(|(column, declared)| {
                if column.name() != declared.name() {
                    return Err(ConnectorError::ReadError(format!(
                        "postgres column '{}' does not match declared column '{}'",
                        column.name(),
                        declared.name()
                    )));
                }
                reference_select_expression(column.name(), column.type_(), declared.data_type())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let table = self.config.get("table").ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "postgres reference source requires a 'table' property".into(),
            )
        })?;
        Ok(format!(
            "SELECT {} FROM {}",
            expressions.join(", "),
            quote_qualified_identifier(table)?
        ))
    }

    fn ssl_mode(&self) -> Result<crate::postgres::SslMode, ConnectorError> {
        self.config
            .get("ssl.mode")
            .map(str::parse)
            .transpose()
            .map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "invalid postgres reference ssl.mode: {error}"
                ))
            })
            .map(std::option::Option::unwrap_or_default)
    }

    async fn start_snapshot(&mut self) -> Result<(), ConnectorError> {
        let pg_config = self.postgres_config()?;
        let ssl_mode = self.ssl_mode()?;
        let probe_query = self.snapshot_query()?;
        let ca_path = self
            .config
            .get("ssl.ca.cert.path")
            .map(std::path::Path::new);
        let session = match ssl_mode {
            crate::postgres::SslMode::Disable => {
                await_reference_driver("connect", async move {
                    let (client, connection) = tokio::time::timeout(
                        CONNECT_TIMEOUT,
                        pg_config.connect(tokio_postgres::NoTls),
                    )
                    .await
                    .map_err(|_| timeout_error(CONNECT_TIMEOUT))?
                    .map_err(|error| {
                        ConnectorError::ConnectionFailed(format!(
                            "postgres reference connect: {error}"
                        ))
                    })?;
                    Ok(PostgresSession {
                        client,
                        connection: spawn_connection(connection),
                    })
                })
                .await?
            }
            crate::postgres::SslMode::VerifyFull => {
                let tls = crate::postgres::make_rustls_connector(ca_path)?;
                await_reference_driver("TLS connect", async move {
                    let (client, connection) =
                        tokio::time::timeout(CONNECT_TIMEOUT, pg_config.connect(tls))
                            .await
                            .map_err(|_| timeout_error(CONNECT_TIMEOUT))?
                            .map_err(|error| {
                                ConnectorError::ConnectionFailed(format!(
                                    "postgres reference verified TLS connect: {error}"
                                ))
                            })?;
                    Ok(PostgresSession {
                        client,
                        connection: spawn_connection(connection),
                    })
                })
                .await?
            }
        };

        let session = owned_batch_execute(
            session,
            "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY".into(),
            "begin snapshot",
        )
        .await?;
        let (session, probe) =
            owned_prepare(session, probe_query, "prepare snapshot probe").await?;
        let query = self.projected_snapshot_query(probe.columns())?;
        let (session, statement) =
            owned_prepare(session, query.clone(), "prepare snapshot projection").await?;
        validate_postgres_schema(statement.columns(), self.declared_schema.as_ref())?;
        let declare = format!("DECLARE {SNAPSHOT_CURSOR} NO SCROLL CURSOR FOR {query}");
        let session = owned_batch_execute(session, declare, "declare snapshot cursor").await?;

        self.session = Some(session);
        self.state = State::Reading;
        Ok(())
    }

    async fn read_snapshot_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        let session = self.session.take().ok_or_else(|| {
            ConnectorError::Internal("postgres reference session is not initialized".into())
        })?;
        let fetch = format!("FETCH FORWARD {SNAPSHOT_ROWS_PER_BATCH} FROM {SNAPSHOT_CURSOR}");
        let (session, rows) = owned_query(session, fetch, "fetch snapshot cursor").await?;
        self.session = Some(session);
        let exhausted = rows.len() < SNAPSHOT_ROWS_PER_BATCH;

        if rows.is_empty() {
            debug_assert!(exhausted);
            self.finish_snapshot().await?;
            return Ok(None);
        }

        let columns = self
            .declared_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| build_column(&rows, index, field.data_type()))
            .collect::<Result<Vec<_>, _>>()?;
        validate_reference_columns(self.declared_schema.as_ref(), &columns)?;
        enforce_snapshot_batch_bytes(&columns)?;
        let batch =
            RecordBatch::try_new(self.declared_schema.clone(), columns).map_err(|error| {
                ConnectorError::ReadError(format!(
                    "postgres snapshot does not satisfy the declared schema: {error}"
                ))
            })?;
        if exhausted {
            self.finish_snapshot().await?;
        }
        Ok(Some(batch))
    }

    async fn finish_snapshot(&mut self) -> Result<(), ConnectorError> {
        let session = self.session.take().ok_or_else(|| {
            ConnectorError::Internal("postgres reference session is not initialized".into())
        })?;
        let _session = owned_batch_execute(session, "COMMIT".into(), "finish snapshot").await?;
        self.state = State::Done;
        Ok(())
    }

    fn fail_closed(&mut self) {
        self.session = None;
        self.state = State::Closed;
    }
}

#[async_trait::async_trait]
impl ReferenceTableSource for PostgresReferenceTableSource {
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        match self.state {
            State::Done => return Ok(None),
            State::Closed => {
                return Err(ConnectorError::InvalidState {
                    expected: "open reference snapshot source".into(),
                    actual: "closed".into(),
                });
            }
            State::Ready | State::Reading => {}
        }

        let result = async {
            if self.state == State::Ready {
                self.start_snapshot().await?;
            }
            self.read_snapshot_batch().await
        }
        .await;
        if result.is_err() {
            self.fail_closed();
        }
        result
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let session = if self.state == State::Reading {
            self.session.take()
        } else {
            None
        };
        // Publish the terminal local state before awaiting. Cancellation leaves the driver task
        // owning the connection and this source closed instead of reusable with an in-flight query.
        self.fail_closed();
        if let Some(session) = session {
            let _session =
                owned_batch_execute(session, "ROLLBACK".into(), "close snapshot transaction")
                    .await?;
        }
        Ok(())
    }
}

fn quote_qualified_identifier(identifier: &str) -> Result<String, ConnectorError> {
    identifier
        .split('.')
        .map(quote_identifier)
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("."))
}

fn quote_identifier(identifier: &str) -> Result<String, ConnectorError> {
    if identifier.is_empty() || identifier.contains('\0') {
        return Err(ConnectorError::ConfigurationError(
            "postgres identifiers must be non-empty and cannot contain NUL".into(),
        ));
    }
    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

fn validate_reference_columns(
    schema: &Schema,
    columns: &[Arc<dyn Array>],
) -> Result<(), ConnectorError> {
    for (field, column) in schema.fields().iter().zip(columns) {
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(ConnectorError::ReadError(format!(
                "postgres reference column '{}' contains {} null values but is declared NOT NULL",
                field.name(),
                column.null_count()
            )));
        }
    }
    Ok(())
}

fn enforce_snapshot_batch_bytes(columns: &[Arc<dyn Array>]) -> Result<(), ConnectorError> {
    enforce_snapshot_batch_bytes_with_limit(columns, MAX_SNAPSHOT_BATCH_BYTES)
}

fn enforce_snapshot_batch_bytes_with_limit(
    columns: &[Arc<dyn Array>],
    limit: usize,
) -> Result<(), ConnectorError> {
    let bytes = columns.iter().try_fold(0_usize, |total, column| {
        total
            .checked_add(column.get_array_memory_size())
            .ok_or_else(|| {
                ConnectorError::ReadError("postgres snapshot byte count overflow".into())
            })
    })?;
    if bytes > limit {
        return Err(ConnectorError::ReadError(format!(
            "postgres snapshot batch retains {bytes} bytes, exceeding the fixed {limit}-byte limit"
        )));
    }
    Ok(())
}

fn reference_select_expression(
    name: &str,
    pg_type: &tokio_postgres::types::Type,
    declared_type: &DataType,
) -> Result<String, ConnectorError> {
    let identifier = quote_identifier(name)?;
    match postgres_type_to_arrow(pg_type) {
        Some(source_type) if &source_type == declared_type => Ok(identifier),
        None if declared_type == &DataType::Utf8 => {
            Ok(format!("CAST({identifier} AS TEXT) AS {identifier}"))
        }
        Some(source_type) => Err(ConnectorError::ReadError(format!(
            "postgres column '{name}' has type {source_type}, expected {declared_type}"
        ))),
        None => Err(ConnectorError::ReadError(format!(
            "postgres column '{name}' has unsupported type {pg_type}"
        ))),
    }
}

fn validate_postgres_schema(
    columns: &[tokio_postgres::Column],
    declared_schema: &Schema,
) -> Result<(), ConnectorError> {
    if columns.len() != declared_schema.fields().len() {
        return Err(ConnectorError::ReadError(format!(
            "postgres snapshot has {} columns but {} were declared",
            columns.len(),
            declared_schema.fields().len()
        )));
    }

    for (column, declared) in columns.iter().zip(declared_schema.fields()) {
        let source_type = postgres_type_to_arrow(column.type_()).ok_or_else(|| {
            ConnectorError::ReadError(format!(
                "postgres column '{}' has unsupported type {}",
                column.name(),
                column.type_()
            ))
        })?;
        if column.name() != declared.name() || &source_type != declared.data_type() {
            return Err(ConnectorError::ReadError(format!(
                "postgres column '{}' has type {source_type}, expected declared column '{}' with type {}",
                column.name(),
                declared.name(),
                declared.data_type()
            )));
        }
    }
    Ok(())
}

fn build_column(
    rows: &[tokio_postgres::Row],
    index: usize,
    data_type: &DataType,
) -> Result<Arc<dyn Array>, ConnectorError> {
    macro_rules! primitive {
        ($native:ty, $array:ty) => {{
            let values: Vec<Option<$native>> = collect_column(rows, index)?;
            Arc::new(<$array>::from(values)) as Arc<dyn Array>
        }};
    }

    let column = match data_type {
        DataType::Boolean => primitive!(bool, arrow_array::BooleanArray),
        DataType::Int16 => primitive!(i16, arrow_array::Int16Array),
        DataType::Int32 => primitive!(i32, arrow_array::Int32Array),
        DataType::Int64 => primitive!(i64, arrow_array::Int64Array),
        DataType::Float32 => primitive!(f32, arrow_array::Float32Array),
        DataType::Float64 => primitive!(f64, arrow_array::Float64Array),
        DataType::Utf8 => {
            let values: Vec<Option<String>> = collect_column(rows, index)?;
            let values = values.iter().map(Option::as_deref).collect::<Vec<_>>();
            Arc::new(arrow_array::StringArray::from(values))
        }
        DataType::Binary => {
            let values: Vec<Option<Vec<u8>>> = collect_column(rows, index)?;
            let values = values
                .iter()
                .map(|value| value.as_deref())
                .collect::<Vec<_>>();
            Arc::new(arrow_array::BinaryArray::from(values))
        }
        DataType::Date32 => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
            let values: Vec<Option<chrono::NaiveDate>> = collect_column(rows, index)?;
            let values = values
                .into_iter()
                .map(|value| {
                    value
                        .map(|date| {
                            i32::try_from(date.signed_duration_since(epoch).num_days()).map_err(
                                |_| {
                                    ConnectorError::ReadError(
                                        "postgres date is outside the Arrow Date32 range".into(),
                                    )
                                },
                            )
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            Arc::new(arrow_array::Date32Array::from(values))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let values: Vec<Option<chrono::NaiveDateTime>> = collect_column(rows, index)?;
            let values = values
                .into_iter()
                .map(|value| value.map(|timestamp| timestamp.and_utc().timestamp_micros()))
                .collect::<Vec<_>>();
            Arc::new(arrow_array::TimestampMicrosecondArray::from(values))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone))
            if timezone.as_ref() == "UTC" =>
        {
            let values: Vec<Option<chrono::DateTime<chrono::Utc>>> = collect_column(rows, index)?;
            let values = values
                .into_iter()
                .map(|value| value.map(|timestamp| timestamp.timestamp_micros()))
                .collect::<Vec<_>>();
            Arc::new(arrow_array::TimestampMicrosecondArray::from(values).with_timezone("UTC"))
        }
        unsupported => {
            return Err(ConnectorError::ReadError(format!(
                "declared PostgreSQL reference type {unsupported} is not supported"
            )));
        }
    };
    Ok(column)
}

fn collect_column<'a, T>(
    rows: &'a [tokio_postgres::Row],
    index: usize,
) -> Result<Vec<Option<T>>, ConnectorError>
where
    T: tokio_postgres::types::FromSql<'a>,
{
    rows.iter()
        .map(|row| {
            row.try_get::<_, Option<T>>(index).map_err(|error| {
                ConnectorError::ReadError(format!(
                    "postgres column {index} could not be decoded: {error}"
                ))
            })
        })
        .collect()
}

fn postgres_type_to_arrow(pg_type: &tokio_postgres::types::Type) -> Option<DataType> {
    use tokio_postgres::types::Type;

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

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use super::*;

    fn declared_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn query_uses_declared_field_order_and_quotes_identifiers() {
        let mut config = ConnectorConfig::new("postgres");
        config.set("table", "public.order");
        let source = PostgresReferenceTableSource::new(config, declared_schema());

        assert_eq!(
            source.snapshot_query().unwrap(),
            "SELECT \"id\", \"name\" FROM \"public\".\"order\""
        );
        assert_eq!(source.declared_schema.field(0).name(), "id");
        assert!(!source.declared_schema.field(0).is_nullable());
    }

    #[test]
    fn tls_is_verified_by_default_and_plaintext_is_explicit() {
        let source =
            PostgresReferenceTableSource::new(ConnectorConfig::new("postgres"), declared_schema());
        assert_eq!(
            source.ssl_mode().unwrap(),
            crate::postgres::SslMode::VerifyFull
        );
        assert_eq!(
            source.postgres_config().unwrap().get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        );

        let mut config = ConnectorConfig::new("postgres");
        config.set(
            "connection",
            "postgresql://user@localhost/db?sslmode=disable",
        );
        let source = PostgresReferenceTableSource::new(config, declared_schema());
        assert_eq!(
            source.postgres_config().unwrap().get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        );

        let mut config = ConnectorConfig::new("postgres");
        config.set("ssl.mode", "disable");
        let source = PostgresReferenceTableSource::new(config, declared_schema());
        assert_eq!(
            source.ssl_mode().unwrap(),
            crate::postgres::SslMode::Disable
        );
        assert_eq!(
            source.postgres_config().unwrap().get_ssl_mode(),
            tokio_postgres::config::SslMode::Disable
        );

        let mut config = ConnectorConfig::new("postgres");
        config.set("ssl.mode", "require");
        let source = PostgresReferenceTableSource::new(config, declared_schema());
        assert!(source.ssl_mode().is_err());
    }

    #[test]
    fn connection_options_fail_closed() {
        for (key, value) in [
            ("port", "not-a-port"),
            ("port", "0"),
            ("options", "bad\0option"),
            ("host", " "),
            ("connection", ""),
        ] {
            let mut config = ConnectorConfig::new("postgres");
            config.set(key, value);
            let source = PostgresReferenceTableSource::new(config, declared_schema());
            assert!(source.postgres_config().is_err());
        }

        let mut config = ConnectorConfig::new("postgres");
        config.set("connection", "host=localhost dbname=db user=user");
        config.set("host", "other");
        let source = PostgresReferenceTableSource::new(config, declared_schema());
        assert!(source.postgres_config().is_err());

        let mut config = ConnectorConfig::new("postgres");
        config.set("connection", "host=localhost port=0 dbname=db user=user");
        let source = PostgresReferenceTableSource::new(config, declared_schema());
        assert!(source.postgres_config().is_err());

        let mut config = ConnectorConfig::new("postgres");
        config.set("ssl.mode", "disable");
        config.set("ssl.ca.cert.path", "/certs/ca.pem");
        let source = PostgresReferenceTableSource::new(config, declared_schema());
        assert!(source.postgres_config().is_err());
    }

    #[test]
    fn snapshot_batch_memory_is_bounded() {
        let column: Arc<dyn Array> = Arc::new(arrow_array::StringArray::from(vec!["payload"]));
        let bytes = column.get_array_memory_size();
        enforce_snapshot_batch_bytes_with_limit(&[Arc::clone(&column)], bytes).unwrap();
        assert!(enforce_snapshot_batch_bytes_with_limit(&[column], bytes - 1).is_err());
    }

    #[test]
    fn supported_postgres_types_have_explicit_arrow_mappings() {
        use tokio_postgres::types::Type;

        assert_eq!(postgres_type_to_arrow(&Type::BYTEA), Some(DataType::Binary));
        assert_eq!(postgres_type_to_arrow(&Type::DATE), Some(DataType::Date32));
        assert_eq!(postgres_type_to_arrow(&Type::NUMERIC), None);
        assert_eq!(
            reference_select_expression("amount", &Type::NUMERIC, &DataType::Utf8).unwrap(),
            "CAST(\"amount\" AS TEXT) AS \"amount\""
        );
        assert!(reference_select_expression("amount", &Type::NUMERIC, &DataType::Float64).is_err());
        assert_eq!(
            reference_select_expression("id", &Type::INT8, &DataType::Int64).unwrap(),
            "\"id\""
        );
    }

    #[tokio::test]
    async fn close_is_idempotent_and_prevents_reads() {
        let mut source =
            PostgresReferenceTableSource::new(ConnectorConfig::new("postgres"), declared_schema());
        source.close().await.unwrap();
        source.close().await.unwrap();
        assert!(source.poll_snapshot().await.is_err());
    }
}
