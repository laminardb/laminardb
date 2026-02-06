//! MySQL CDC binlog I/O integration module.
//!
//! This module provides the actual I/O operations for MySQL binlog replication
//! via the `mysql_async` crate. All functions are feature-gated behind `mysql-cdc`.
//!
//! # Architecture
//!
//! The I/O module is separate from the business logic in [`source.rs`](super::source)
//! to allow:
//! - Testing business logic without a running MySQL server
//! - Clean separation of concerns (connection management vs. event decoding)
//! - Easy mocking for unit tests
//!
//! # Usage
//!
//! ```ignore
//! use laminar_connectors::cdc::mysql::mysql_io;
//!
//! let conn = mysql_io::connect(&config).await?;
//! let stream = mysql_io::start_binlog_stream(conn, &config, None, None).await?;
//! let events = mysql_io::read_events(&mut stream, 100, Duration::from_secs(1)).await?;
//! for event in events {
//!     if let Some(msg) = mysql_io::decode_binlog_event(&event, &stream)? {
//!         // Process BinlogMessage...
//!     }
//! }
//! ```

#[cfg(feature = "mysql-cdc")]
use std::time::Duration;

#[cfg(feature = "mysql-cdc")]
use tokio_stream::StreamExt as _;

#[cfg(feature = "mysql-cdc")]
use mysql_async::binlog::events::{Event as MysqlEvent, EventData, RowsEventData};
#[cfg(feature = "mysql-cdc")]
use mysql_async::binlog::row::BinlogRow;
#[cfg(feature = "mysql-cdc")]
use mysql_async::binlog::value::BinlogValue;
#[cfg(feature = "mysql-cdc")]
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, GnoInterval, OptsBuilder, Sid};

#[cfg(feature = "mysql-cdc")]
use tracing::{debug, info};

#[cfg(feature = "mysql-cdc")]
use crate::error::ConnectorError;

#[cfg(feature = "mysql-cdc")]
use super::config::{MySqlCdcConfig, SslMode};
#[cfg(feature = "mysql-cdc")]
use super::decoder::{
    BeginMessage, BinlogMessage, BinlogPosition, ColumnValue, CommitMessage, DeleteMessage,
    InsertMessage, QueryMessage, RotateMessage, RowData, TableMapMessage, UpdateMessage,
    UpdateRowData,
};
#[cfg(feature = "mysql-cdc")]
use super::gtid::{Gtid, GtidSet};
#[cfg(feature = "mysql-cdc")]
use super::types::MySqlColumn;

/// Connects to a MySQL server using the provided configuration.
///
/// Builds connection options from the config and establishes a TCP connection.
///
/// # Errors
///
/// Returns `ConnectorError::ConnectionFailed` if the connection cannot be established.
#[cfg(feature = "mysql-cdc")]
pub async fn connect(config: &MySqlCdcConfig) -> Result<Conn, ConnectorError> {
    info!(
        host = %config.host,
        port = config.port,
        user = %config.username,
        ssl_mode = %config.ssl_mode,
        "connecting to MySQL server"
    );

    let mut opts = OptsBuilder::default()
        .ip_or_hostname(&config.host)
        .tcp_port(config.port)
        .user(Some(&config.username))
        .prefer_socket(Some(false));

    if let Some(ref password) = config.password {
        opts = opts.pass(Some(password));
    }

    if let Some(ref database) = config.database {
        opts = opts.db_name(Some(database));
    }

    // Configure SSL based on ssl_mode.
    opts = match config.ssl_mode {
        SslMode::Disabled => opts,
        SslMode::Preferred => opts.ssl_opts(Some(
            mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true),
        )),
        SslMode::Required | SslMode::VerifyCa | SslMode::VerifyIdentity => {
            opts.ssl_opts(Some(mysql_async::SslOpts::default()))
        }
    };

    let conn = Conn::new(opts).await.map_err(|e| {
        ConnectorError::ConnectionFailed(format!(
            "failed to connect to MySQL {}:{}: {}",
            config.host, config.port, e
        ))
    })?;

    info!(
        host = %config.host,
        port = config.port,
        "connected to MySQL server"
    );

    Ok(conn)
}

/// Starts a binlog replication stream from the MySQL server.
///
/// The connection is consumed by the binlog stream (MySQL protocol requirement).
///
/// # Arguments
///
/// * `conn` - MySQL connection (consumed)
/// * `config` - CDC configuration
/// * `gtid_set` - Optional GTID set for GTID-based replication
/// * `position` - Optional binlog position for file/position-based replication
///
/// # Errors
///
/// Returns `ConnectorError::ConnectionFailed` if the binlog stream cannot be started.
#[cfg(feature = "mysql-cdc")]
pub async fn start_binlog_stream(
    conn: Conn,
    config: &MySqlCdcConfig,
    gtid_set: Option<&GtidSet>,
    position: Option<&BinlogPosition>,
) -> Result<BinlogStream, ConnectorError> {
    let mut request = BinlogStreamRequest::new(config.server_id);

    if config.use_gtid {
        request = request.with_gtid();

        // Add GTID set if available (resume from position).
        if let Some(gtid_set) = gtid_set {
            let sids = gtid_set_to_sids(gtid_set);
            if sids.is_empty() {
                info!("starting GTID-based binlog replication from beginning");
            } else {
                request = request.with_gtid_set(sids);
                info!("starting GTID-based binlog replication from {}", gtid_set);
            }
        } else {
            info!("starting GTID-based binlog replication from beginning");
        }
    } else {
        // File/position-based replication.
        if let Some(pos) = position {
            request = request
                .with_filename(pos.filename.as_bytes())
                .with_pos(pos.position);
            info!(
                "starting binlog replication from {}:{}",
                pos.filename, pos.position
            );
        } else if let Some(ref filename) = config.binlog_filename {
            let pos = config.binlog_position.unwrap_or(4);
            request = request.with_filename(filename.as_bytes()).with_pos(pos);
            info!("starting binlog replication from {}:{}", filename, pos);
        } else {
            info!("starting binlog replication from current position");
        }
    }

    let stream = conn.get_binlog_stream(request).await.map_err(|e| {
        ConnectorError::ConnectionFailed(format!("failed to start binlog stream: {e}"))
    })?;

    Ok(stream)
}

/// Reads binlog events from the stream with a timeout.
///
/// Returns up to `max_events` events, or fewer if the timeout expires.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if an I/O error occurs.
#[cfg(feature = "mysql-cdc")]
pub async fn read_events(
    stream: &mut BinlogStream,
    max_events: usize,
    timeout: Duration,
) -> Result<Vec<MysqlEvent>, ConnectorError> {
    let mut events = Vec::with_capacity(max_events.min(64));
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if events.len() >= max_events {
            break;
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(event))) => {
                events.push(event);
            }
            Ok(Some(Err(e))) => {
                // I/O error reading from binlog stream.
                return Err(ConnectorError::ReadError(format!(
                    "binlog stream error: {e}"
                )));
            }
            Ok(None) => {
                // Stream ended (server disconnected or NON_BLOCK mode).
                debug!("binlog stream ended");
                break;
            }
            Err(_) => {
                // Timeout - return whatever we have.
                break;
            }
        }
    }

    Ok(events)
}

/// Decodes a mysql_async binlog event into our internal `BinlogMessage` type.
///
/// Returns `None` for unsupported or irrelevant event types (e.g., FORMAT_DESCRIPTION).
///
/// # Arguments
///
/// * `event` - The raw binlog event from mysql_async
/// * `stream` - The binlog stream (provides TABLE_MAP cache via `get_tme()`)
///
/// # Errors
///
/// Returns `ConnectorError::Internal` if event data cannot be parsed.
#[cfg(feature = "mysql-cdc")]
pub fn decode_binlog_event(
    event: &MysqlEvent,
    stream: &BinlogStream,
) -> Result<Option<BinlogMessage>, ConnectorError> {
    let header = event.header();
    let timestamp_ms = i64::from(header.timestamp()) * 1000;
    let log_pos = u64::from(header.log_pos());

    let event_data = event
        .read_data()
        .map_err(|e| ConnectorError::Internal(format!("failed to parse binlog event: {e}")))?;

    let Some(event_data) = event_data else {
        return Ok(None);
    };

    match event_data {
        EventData::TableMapEvent(tme) => {
            let columns = extract_columns_from_tme(&tme);
            Ok(Some(BinlogMessage::TableMap(TableMapMessage {
                table_id: tme.table_id(),
                database: tme.database_name().into_owned(),
                table: tme.table_name().into_owned(),
                columns,
            })))
        }

        EventData::RowsEvent(rows_event) => {
            let table_id = rows_event.table_id();

            // Get the TABLE_MAP event from the stream's internal cache.
            let tme = stream.get_tme(table_id).ok_or_else(|| {
                ConnectorError::Internal(format!("missing TABLE_MAP for table_id {table_id}"))
            })?;

            let database = tme.database_name().into_owned();
            let table = tme.table_name().into_owned();

            match &rows_event {
                RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                    let rows = decode_rows_after(&rows_event, tme)?;
                    Ok(Some(BinlogMessage::Insert(InsertMessage {
                        table_id,
                        database,
                        table,
                        rows,
                        binlog_position: log_pos,
                        timestamp_ms,
                    })))
                }

                RowsEventData::UpdateRowsEvent(_)
                | RowsEventData::UpdateRowsEventV1(_)
                | RowsEventData::PartialUpdateRowsEvent(_) => {
                    let rows = decode_rows_update(&rows_event, tme)?;
                    Ok(Some(BinlogMessage::Update(UpdateMessage {
                        table_id,
                        database,
                        table,
                        rows,
                        binlog_position: log_pos,
                        timestamp_ms,
                    })))
                }

                RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                    let rows = decode_rows_before(&rows_event, tme)?;
                    Ok(Some(BinlogMessage::Delete(DeleteMessage {
                        table_id,
                        database,
                        table,
                        rows,
                        binlog_position: log_pos,
                        timestamp_ms,
                    })))
                }
            }
        }

        EventData::QueryEvent(qe) => Ok(Some(BinlogMessage::Query(QueryMessage {
            database: qe.schema().into_owned(),
            query: qe.query().into_owned(),
            binlog_position: log_pos,
            timestamp_ms,
        }))),

        EventData::GtidEvent(ge) => {
            let gtid = sid_bytes_to_gtid(ge.sid(), ge.gno());
            Ok(Some(BinlogMessage::Begin(BeginMessage {
                gtid: Some(gtid),
                binlog_filename: String::new(), // Filled by caller from stream state.
                binlog_position: log_pos,
                timestamp_ms,
            })))
        }

        EventData::AnonymousGtidEvent(_) => Ok(Some(BinlogMessage::Begin(BeginMessage {
            gtid: None,
            binlog_filename: String::new(),
            binlog_position: log_pos,
            timestamp_ms,
        }))),

        EventData::RotateEvent(re) => Ok(Some(BinlogMessage::Rotate(RotateMessage {
            next_binlog: re.name().into_owned(),
            position: re.position(),
        }))),

        EventData::XidEvent(xe) => Ok(Some(BinlogMessage::Commit(CommitMessage {
            xid: xe.xid,
            binlog_position: log_pos,
            timestamp_ms,
        }))),

        EventData::HeartbeatEvent => Ok(Some(BinlogMessage::Heartbeat)),

        // All other event types are ignored (FORMAT_DESCRIPTION, PREVIOUS_GTIDS, etc.).
        _ => Ok(None),
    }
}

/// Converts a `mysql_async` `BinlogValue` to our `ColumnValue` enum.
#[cfg(feature = "mysql-cdc")]
fn binlog_value_to_column_value(val: &BinlogValue<'_>) -> ColumnValue {
    match val {
        BinlogValue::Value(v) => mysql_value_to_column_value(v),
        BinlogValue::Jsonb(jsonb_val) => ColumnValue::Json(format!("{jsonb_val:?}")),
        BinlogValue::JsonDiff(_diffs) => {
            // Partial JSON updates (MySQL 8.0.20+) — represent as JSON string.
            ColumnValue::Json("{}".to_string())
        }
    }
}

/// Converts a `mysql_common::value::Value` to our `ColumnValue` enum.
#[cfg(feature = "mysql-cdc")]
fn mysql_value_to_column_value(val: &mysql_async::Value) -> ColumnValue {
    match val {
        mysql_async::Value::NULL => ColumnValue::Null,
        mysql_async::Value::Int(v) => ColumnValue::SignedInt(*v),
        mysql_async::Value::UInt(v) => ColumnValue::UnsignedInt(*v),
        mysql_async::Value::Float(v) => ColumnValue::Float(*v),
        mysql_async::Value::Double(v) => ColumnValue::Double(*v),
        mysql_async::Value::Bytes(b) => {
            // Try to interpret as UTF-8 string first.
            match String::from_utf8(b.clone()) {
                Ok(s) => ColumnValue::String(s),
                Err(_) => ColumnValue::Bytes(b.clone()),
            }
        }
        mysql_async::Value::Date(year, month, day, hour, min, sec, micro) => {
            if *hour == 0 && *min == 0 && *sec == 0 && *micro == 0 {
                ColumnValue::Date(i32::from(*year), u32::from(*month), u32::from(*day))
            } else {
                ColumnValue::DateTime(
                    i32::from(*year),
                    u32::from(*month),
                    u32::from(*day),
                    u32::from(*hour),
                    u32::from(*min),
                    u32::from(*sec),
                    *micro,
                )
            }
        }
        mysql_async::Value::Time(is_negative, days, hours, minutes, seconds, micros) => {
            // Convert days+hours to total hours.
            // days is u32 and hours is u8, product fits in i32.
            #[allow(clippy::cast_possible_wrap)]
            let total_hours = (*days as i32) * 24 + i32::from(*hours);
            let h = if *is_negative {
                -total_hours
            } else {
                total_hours
            };
            ColumnValue::Time(h, u32::from(*minutes), u32::from(*seconds), *micros)
        }
    }
}

/// Extracts column definitions from a `TableMapEvent`.
#[cfg(feature = "mysql-cdc")]
fn extract_columns_from_tme(
    tme: &mysql_async::binlog::events::TableMapEvent<'_>,
) -> Vec<MySqlColumn> {
    #[allow(clippy::cast_possible_truncation)]
    let count = tme.columns_count() as usize;
    let null_bitmap = tme.null_bitmask();

    // Try to extract column names from optional metadata.
    let col_names = extract_column_names(tme);

    (0..count)
        .map(|i| {
            let type_id = tme
                .get_raw_column_type(i)
                .ok()
                .flatten()
                .map_or(0xFF, |ct| ct as u8);

            let nullable = null_bitmap.get(i).as_deref().copied().unwrap_or(true);

            let name = col_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col_{i}"));

            MySqlColumn::new(name, type_id, 0, nullable, false)
        })
        .collect()
}

/// Extracts column names from TABLE_MAP optional metadata.
#[cfg(feature = "mysql-cdc")]
fn extract_column_names(tme: &mysql_async::binlog::events::TableMapEvent<'_>) -> Vec<String> {
    let mut names = Vec::new();
    for meta in tme.iter_optional_meta().flatten() {
        use mysql_async::binlog::events::OptionalMetadataField;
        if let OptionalMetadataField::ColumnName(col_names) = meta {
            for name in col_names.iter_names().flatten() {
                names.push(name.name().into_owned());
            }
        }
    }
    names
}

/// Decodes rows from a WriteRows event (after images only).
#[cfg(feature = "mysql-cdc")]
fn decode_rows_after(
    rows_event: &RowsEventData<'_>,
    tme: &mysql_async::binlog::events::TableMapEvent<'_>,
) -> Result<Vec<RowData>, ConnectorError> {
    let mut rows = Vec::new();
    for row_result in rows_event.rows(tme) {
        let (_before, after) = row_result
            .map_err(|e| ConnectorError::Internal(format!("failed to decode row: {e}")))?;
        if let Some(row) = after {
            rows.push(binlog_row_to_row_data(&row));
        }
    }
    Ok(rows)
}

/// Decodes rows from a DeleteRows event (before images only).
#[cfg(feature = "mysql-cdc")]
fn decode_rows_before(
    rows_event: &RowsEventData<'_>,
    tme: &mysql_async::binlog::events::TableMapEvent<'_>,
) -> Result<Vec<RowData>, ConnectorError> {
    let mut rows = Vec::new();
    for row_result in rows_event.rows(tme) {
        let (before, _after) = row_result
            .map_err(|e| ConnectorError::Internal(format!("failed to decode row: {e}")))?;
        if let Some(row) = before {
            rows.push(binlog_row_to_row_data(&row));
        }
    }
    Ok(rows)
}

/// Decodes rows from an UpdateRows event (before + after image pairs).
#[cfg(feature = "mysql-cdc")]
fn decode_rows_update(
    rows_event: &RowsEventData<'_>,
    tme: &mysql_async::binlog::events::TableMapEvent<'_>,
) -> Result<Vec<UpdateRowData>, ConnectorError> {
    let mut rows = Vec::new();
    for row_result in rows_event.rows(tme) {
        let (before, after) = row_result
            .map_err(|e| ConnectorError::Internal(format!("failed to decode row: {e}")))?;

        let before_data = before.map_or_else(
            || RowData {
                columns: Vec::new(),
            },
            |r| binlog_row_to_row_data(&r),
        );

        let after_data = after.map_or_else(
            || RowData {
                columns: Vec::new(),
            },
            |r| binlog_row_to_row_data(&r),
        );

        rows.push(UpdateRowData {
            before: before_data,
            after: after_data,
        });
    }
    Ok(rows)
}

/// Converts a `BinlogRow` to our `RowData` type.
#[cfg(feature = "mysql-cdc")]
fn binlog_row_to_row_data(row: &BinlogRow) -> RowData {
    let columns = (0..row.len())
        .map(|i| {
            row.as_ref(i)
                .map_or(ColumnValue::Null, binlog_value_to_column_value)
        })
        .collect();
    RowData { columns }
}

/// Converts a 16-byte SID and GNO to our `Gtid` type.
#[cfg(feature = "mysql-cdc")]
fn sid_bytes_to_gtid(sid: [u8; 16], gno: u64) -> Gtid {
    let uuid = uuid::Uuid::from_bytes(sid);
    Gtid::new(uuid, gno)
}

/// Converts our `GtidSet` to mysql_async `Sid` items for `BinlogStreamRequest`.
#[cfg(feature = "mysql-cdc")]
fn gtid_set_to_sids(gtid_set: &GtidSet) -> Vec<Sid<'static>> {
    let mut sids = Vec::new();

    for (source_id, ranges) in gtid_set.iter_sets() {
        let uuid_bytes = source_id.into_bytes();
        let mut sid = Sid::new(uuid_bytes);

        for range in ranges {
            // GnoInterval is [start, end) in mysql_async but our GtidRange is [start, end] inclusive.
            let interval = GnoInterval::new(range.start, range.end + 1);
            sid = sid.with_interval(interval);
        }

        sids.push(sid);
    }

    sids
}

/// Builds SSL options from our config.
///
/// This is a helper for testing; the main `connect()` function handles SSL inline.
#[cfg(feature = "mysql-cdc")]
#[must_use]
pub fn build_ssl_opts(ssl_mode: &SslMode) -> Option<mysql_async::SslOpts> {
    match ssl_mode {
        SslMode::Disabled => None,
        SslMode::Preferred => {
            Some(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true))
        }
        SslMode::Required | SslMode::VerifyCa | SslMode::VerifyIdentity => {
            Some(mysql_async::SslOpts::default())
        }
    }
}

/// Builds an `OptsBuilder` from our config (for testing).
#[cfg(feature = "mysql-cdc")]
#[must_use]
pub fn build_opts(config: &MySqlCdcConfig) -> OptsBuilder {
    let mut opts = OptsBuilder::default()
        .ip_or_hostname(&config.host)
        .tcp_port(config.port)
        .user(Some(&config.username))
        .prefer_socket(Some(false));

    if let Some(ref password) = config.password {
        opts = opts.pass(Some(password));
    }

    if let Some(ref database) = config.database {
        opts = opts.db_name(Some(database));
    }

    if let Some(ssl_opts) = build_ssl_opts(&config.ssl_mode) {
        opts = opts.ssl_opts(Some(ssl_opts));
    }

    opts
}

/// Builds a `BinlogStreamRequest` from our config (for testing).
#[cfg(feature = "mysql-cdc")]
#[must_use]
pub fn build_binlog_request<'a>(
    config: &'a MySqlCdcConfig,
    gtid_set: Option<&GtidSet>,
    position: Option<&'a BinlogPosition>,
) -> BinlogStreamRequest<'a> {
    let mut request = BinlogStreamRequest::new(config.server_id);

    if config.use_gtid {
        request = request.with_gtid();

        if let Some(gtid_set) = gtid_set {
            let sids = gtid_set_to_sids(gtid_set);
            if !sids.is_empty() {
                request = request.with_gtid_set(sids);
            }
        }
    } else if let Some(pos) = position {
        request = request
            .with_filename(pos.filename.as_bytes())
            .with_pos(pos.position);
    } else if let Some(ref filename) = config.binlog_filename {
        let pos = config.binlog_position.unwrap_or(4);
        request = request.with_filename(filename.as_bytes()).with_pos(pos);
    }

    request
}

// ============================================================================
// Tests (require mysql-cdc feature)
// ============================================================================

#[cfg(all(test, feature = "mysql-cdc"))]
mod tests {
    use super::*;
    use mysql_async::Opts;

    fn test_config() -> MySqlCdcConfig {
        MySqlCdcConfig {
            host: "localhost".to_string(),
            port: 3306,
            database: Some("testdb".to_string()),
            username: "root".to_string(),
            password: Some("secret".to_string()),
            server_id: 12345,
            ..Default::default()
        }
    }

    #[test]
    fn test_connect_builds_opts() {
        let config = test_config();
        let opts_builder = build_opts(&config);

        // Convert to Opts to verify values.
        let opts: Opts = opts_builder.into();

        assert_eq!(opts.ip_or_hostname(), "localhost");
        assert_eq!(opts.tcp_port(), 3306);
        assert_eq!(opts.user(), Some("root"));
        assert_eq!(opts.pass(), Some("secret"));
        assert_eq!(opts.db_name(), Some("testdb"));
    }

    #[test]
    fn test_connect_builds_opts_no_password() {
        let config = MySqlCdcConfig {
            host: "dbhost".to_string(),
            port: 3307,
            database: None,
            username: "repl".to_string(),
            password: None,
            server_id: 999,
            ..Default::default()
        };
        let opts_builder = build_opts(&config);
        let opts: Opts = opts_builder.into();

        assert_eq!(opts.ip_or_hostname(), "dbhost");
        assert_eq!(opts.tcp_port(), 3307);
        assert_eq!(opts.user(), Some("repl"));
        assert_eq!(opts.pass(), None);
        assert_eq!(opts.db_name(), None);
    }

    #[test]
    fn test_ssl_opts_disabled() {
        let ssl = build_ssl_opts(&SslMode::Disabled);
        assert!(ssl.is_none());
    }

    #[test]
    fn test_ssl_opts_preferred() {
        let ssl = build_ssl_opts(&SslMode::Preferred);
        assert!(ssl.is_some());
        let ssl = ssl.unwrap();
        assert!(ssl.accept_invalid_certs());
    }

    #[test]
    fn test_ssl_opts_required() {
        let ssl = build_ssl_opts(&SslMode::Required);
        assert!(ssl.is_some());
        let ssl = ssl.unwrap();
        assert!(!ssl.accept_invalid_certs());
    }

    #[test]
    fn test_ssl_opts_verify_ca() {
        let ssl = build_ssl_opts(&SslMode::VerifyCa);
        assert!(ssl.is_some());
    }

    #[test]
    fn test_ssl_opts_verify_identity() {
        let ssl = build_ssl_opts(&SslMode::VerifyIdentity);
        assert!(ssl.is_some());
    }

    #[test]
    fn test_binlog_request_gtid_mode() {
        let config = MySqlCdcConfig {
            server_id: 999,
            use_gtid: true,
            ..Default::default()
        };

        // Without GTID set — should still work.
        let _request = build_binlog_request(&config, None, None);

        // With GTID set.
        let gtid_set: GtidSet = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5".parse().unwrap();
        let _request = build_binlog_request(&config, Some(&gtid_set), None);
    }

    #[test]
    fn test_binlog_request_file_position() {
        let config = MySqlCdcConfig {
            server_id: 999,
            use_gtid: false,
            ..Default::default()
        };

        let pos = BinlogPosition::new("mysql-bin.000003".to_string(), 12345);
        let _request = build_binlog_request(&config, None, Some(&pos));
    }

    #[test]
    fn test_binlog_request_config_filename() {
        let config = MySqlCdcConfig {
            server_id: 999,
            use_gtid: false,
            binlog_filename: Some("mysql-bin.000001".to_string()),
            binlog_position: Some(154),
            ..Default::default()
        };

        let _request = build_binlog_request(&config, None, None);
    }

    #[test]
    fn test_mysql_value_to_column_value_null() {
        let val = mysql_value_to_column_value(&mysql_async::Value::NULL);
        assert_eq!(val, ColumnValue::Null);
    }

    #[test]
    fn test_mysql_value_to_column_value_int() {
        let val = mysql_value_to_column_value(&mysql_async::Value::Int(-42));
        assert_eq!(val, ColumnValue::SignedInt(-42));

        let val = mysql_value_to_column_value(&mysql_async::Value::UInt(100));
        assert_eq!(val, ColumnValue::UnsignedInt(100));
    }

    #[test]
    fn test_mysql_value_to_column_value_float() {
        let val = mysql_value_to_column_value(&mysql_async::Value::Float(3.14));
        assert_eq!(val, ColumnValue::Float(3.14));

        let val = mysql_value_to_column_value(&mysql_async::Value::Double(2.718));
        assert_eq!(val, ColumnValue::Double(2.718));
    }

    #[test]
    fn test_mysql_value_to_column_value_string() {
        let val = mysql_value_to_column_value(&mysql_async::Value::Bytes(b"hello world".to_vec()));
        assert_eq!(val, ColumnValue::String("hello world".to_string()));
    }

    #[test]
    fn test_mysql_value_to_column_value_bytes() {
        // Non-UTF8 bytes should map to Bytes.
        let val = mysql_value_to_column_value(&mysql_async::Value::Bytes(vec![0xFF, 0xFE, 0xFD]));
        assert_eq!(val, ColumnValue::Bytes(vec![0xFF, 0xFE, 0xFD]));
    }

    #[test]
    fn test_mysql_value_to_column_value_date() {
        // Date only (no time component).
        let val = mysql_value_to_column_value(&mysql_async::Value::Date(2024, 6, 15, 0, 0, 0, 0));
        assert_eq!(val, ColumnValue::Date(2024, 6, 15));

        // DateTime (has time component).
        let val =
            mysql_value_to_column_value(&mysql_async::Value::Date(2024, 6, 15, 14, 30, 45, 0));
        assert_eq!(val, ColumnValue::DateTime(2024, 6, 15, 14, 30, 45, 0));
    }

    #[test]
    fn test_mysql_value_to_column_value_time() {
        let val = mysql_value_to_column_value(&mysql_async::Value::Time(false, 0, 14, 30, 45, 0));
        assert_eq!(val, ColumnValue::Time(14, 30, 45, 0));

        // Negative time.
        let val = mysql_value_to_column_value(&mysql_async::Value::Time(true, 0, 2, 30, 0, 0));
        assert_eq!(val, ColumnValue::Time(-2, 30, 0, 0));

        // Time with days.
        let val = mysql_value_to_column_value(&mysql_async::Value::Time(false, 1, 6, 0, 0, 0));
        assert_eq!(val, ColumnValue::Time(30, 0, 0, 0));
    }

    #[test]
    fn test_sid_bytes_to_gtid() {
        let uuid = uuid::Uuid::parse_str("3E11FA47-71CA-11E1-9E33-C80AA9429562").unwrap();
        let gtid = sid_bytes_to_gtid(uuid.into_bytes(), 42);
        assert_eq!(gtid.source_id(), uuid);
        assert_eq!(gtid.transaction_id(), 42);
    }

    #[test]
    fn test_gtid_set_to_sids_empty() {
        let gtid_set = GtidSet::new();
        let sids = gtid_set_to_sids(&gtid_set);
        assert!(sids.is_empty());
    }

    #[test]
    fn test_gtid_set_to_sids_single() {
        let gtid_set: GtidSet = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10".parse().unwrap();
        let sids = gtid_set_to_sids(&gtid_set);
        assert_eq!(sids.len(), 1);
    }

    #[test]
    fn test_gtid_set_to_sids_multiple() {
        let gtid_set: GtidSet =
            "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10,4E11FA47-71CA-11E1-9E33-C80AA9429563:1-5"
                .parse()
                .unwrap();
        let sids = gtid_set_to_sids(&gtid_set);
        assert_eq!(sids.len(), 2);
    }
}
