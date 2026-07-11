//! MySQL CDC source connector implementation.
//!
//! Implements the [`SourceConnector`] trait for MySQL binlog replication.
//! This module provides the main entry point for MySQL CDC: [`MySqlCdcSource`].

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourcePosition, SourceStart,
    SourceTopology,
};
use crate::error::ConnectorError;

use super::changelog::ChangeEvent;
use super::config::MySqlCdcConfig;
use super::decoder::{BinlogMessage, BinlogPosition};
use super::gtid::GtidSet;
use super::metrics::MySqlCdcMetrics;
use super::schema::{cdc_envelope_schema, TableCache, TableInfo};

/// Single-consumer async receiver for the binlog reader → `poll_batch` queue.
#[cfg(feature = "mysql-cdc")]
type BinlogMessageRx = crossfire::AsyncRx<crossfire::mpsc::Array<BinlogReaderMessage>>;

#[cfg(feature = "mysql-cdc")]
enum BinlogReaderMessage {
    Event(BinlogMessage),
    Terminal(String),
}

/// MySQL binlog CDC source connector. Reads change events from the MySQL
/// binary log via replication protocol; supports GTID-based and
/// file/position-based replication.
pub struct MySqlCdcSource {
    /// Configuration for the MySQL CDC connection.
    config: MySqlCdcConfig,

    /// Whether the source is currently connected.
    connected: bool,

    /// Cache of table schemas from TABLE_MAP events.
    table_cache: TableCache,

    /// Current binlog position (file/position).
    position: Option<BinlogPosition>,

    /// Current GTID set (for GTID-based replication).
    gtid_set: Option<GtidSet>,

    /// Current binlog filename (updated by ROTATE events).
    current_binlog_file: String,

    /// Current GTID string (updated by GTID events within a transaction).
    current_gtid: Option<String>,

    /// Buffered change events waiting to be emitted.
    event_buffer: Vec<ChangeEvent>,

    /// Metrics for this source.
    metrics: MySqlCdcMetrics,

    /// Arrow schema for CDC envelope.
    schema: Option<SchemaRef>,

    /// Last time we received data (for health checks).
    last_activity: Option<Instant>,

    /// Notification handle signalled when binlog data arrives from the reader task.
    data_ready: Arc<Notify>,

    /// Channel receiver for decoded binlog messages from the background reader task.
    #[cfg(feature = "mysql-cdc")]
    msg_rx: Option<BinlogMessageRx>,

    /// Terminal reader failure, reported only after already-decoded rows have been emitted.
    #[cfg(feature = "mysql-cdc")]
    reader_error: Option<String>,

    /// Background binlog reader task handle.
    #[cfg(feature = "mysql-cdc")]
    reader_handle: Option<tokio::task::JoinHandle<()>>,

    /// Shutdown signal for the background reader task.
    #[cfg(feature = "mysql-cdc")]
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,
}

// Manual Debug impl because BinlogStream doesn't implement Debug.
impl std::fmt::Debug for MySqlCdcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlCdcSource")
            .field("config", &self.config)
            .field("connected", &self.connected)
            .field("table_cache", &self.table_cache)
            .field("position", &self.position)
            .field("gtid_set", &self.gtid_set)
            .field("current_binlog_file", &self.current_binlog_file)
            .field("current_gtid", &self.current_gtid)
            .field("event_buffer_len", &self.event_buffer.len())
            .field("metrics", &self.metrics)
            .field("schema", &self.schema)
            .field("last_activity", &self.last_activity)
            .finish_non_exhaustive()
    }
}

impl MySqlCdcSource {
    /// Creates a new MySQL CDC source with the given configuration.
    #[must_use]
    pub fn new(config: MySqlCdcConfig, registry: Option<&prometheus::Registry>) -> Self {
        Self {
            config,
            connected: false,
            table_cache: TableCache::new(),
            position: None,
            gtid_set: None,
            current_binlog_file: String::new(),
            current_gtid: None,
            event_buffer: Vec::new(),
            metrics: MySqlCdcMetrics::new(registry),
            schema: None,
            last_activity: None,
            data_ready: Arc::new(Notify::new()),
            #[cfg(feature = "mysql-cdc")]
            msg_rx: None,
            #[cfg(feature = "mysql-cdc")]
            reader_error: None,
            #[cfg(feature = "mysql-cdc")]
            reader_handle: None,
            #[cfg(feature = "mysql-cdc")]
            reader_shutdown: None,
        }
    }

    /// Creates a MySQL CDC source from a generic connector config.
    ///
    /// # Errors
    ///
    /// Returns error if required configuration keys are missing.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mysql_config = MySqlCdcConfig::from_config(config)?;
        Ok(Self::new(mysql_config, None))
    }

    /// Returns the number of cached table schemas.
    #[must_use]
    pub fn cached_table_count(&self) -> usize {
        self.table_cache.len()
    }

    /// Returns the current binlog position.
    #[must_use]
    pub fn position(&self) -> Option<&BinlogPosition> {
        self.position.as_ref()
    }

    /// Returns the current GTID set.
    #[must_use]
    pub fn gtid_set(&self) -> Option<&GtidSet> {
        self.gtid_set.as_ref()
    }

    /// Returns a reference to the table cache.
    #[must_use]
    pub fn table_cache(&self) -> &TableCache {
        &self.table_cache
    }

    /// Returns a reference to the metrics.
    #[must_use]
    pub fn cdc_metrics(&self) -> &MySqlCdcMetrics {
        &self.metrics
    }

    /// Checks if a table should be included based on filters.
    #[must_use]
    pub fn should_include_table(&self, database: &str, table: &str) -> bool {
        self.config.should_include_table(database, table)
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &MySqlCdcConfig {
        &self.config
    }

    /// Returns whether the source is connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Creates a checkpoint representing the current position.
    #[must_use]
    pub fn create_checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();

        if self.config.use_gtid {
            if let Some(ref gtid_set) = self.gtid_set {
                checkpoint.set_offset("gtid", gtid_set.to_string());
            }
        } else if let Some(ref pos) = self.position {
            checkpoint.set_offset("binlog_file", &pos.filename);
            checkpoint.set_offset("binlog_position", pos.position.to_string());
        }

        checkpoint.set_metadata("server_id", self.config.server_id.to_string());

        checkpoint
    }

    /// Builds the CDC envelope schema based on a table schema.
    #[allow(clippy::unused_self)] // Will use self for config options
    fn build_envelope_schema(&self, table_schema: &Schema) -> SchemaRef {
        Arc::new(cdc_envelope_schema(table_schema))
    }

    /// Flushes buffered events to a RecordBatch.
    ///
    /// # Errors
    ///
    /// Returns error if batch conversion fails.
    pub fn flush_events(
        &mut self,
        table_info: &TableInfo,
    ) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.event_buffer.is_empty() {
            return Ok(None);
        }

        let expected_table = table_info.full_name();
        if self
            .event_buffer
            .iter()
            .any(|event| event.table != expected_table)
        {
            return Err(ConnectorError::Internal(format!(
                "MySQL CDC buffered events span multiple table schemas; expected only \
                 '{expected_table}'"
            )));
        }
        // Convert before removal. A schema/conversion failure must preserve the events so the
        // source faults and recovery can replay instead of silently advancing past them.
        let batch = super::changelog::events_to_record_batch(&self.event_buffer, table_info)
            .map_err(|e| ConnectorError::Internal(e.to_string()))?;
        self.event_buffer.clear();
        Ok(Some(batch))
    }

    fn buffered_table_info(&self) -> Result<Option<TableInfo>, ConnectorError> {
        let Some(event) = self.event_buffer.first() else {
            return Ok(None);
        };
        let (database, table) = event.table.split_once('.').ok_or_else(|| {
            ConnectorError::Internal(format!(
                "invalid MySQL CDC event table identity '{}'",
                event.table
            ))
        })?;
        self.table_cache
            .get_by_name(database, table)
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                ConnectorError::Internal(format!(
                    "missing TABLE_MAP schema for buffered MySQL table '{}'",
                    event.table
                ))
            })
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl SourceConnector for MySqlCdcSource {
    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        #[cfg(not(feature = "mysql-cdc"))]
        {
            let _ = config;
            Err(ConnectorError::ConfigurationError(
                "MySQL CDC source requires the `mysql-cdc` feature flag".into(),
            ))
        }

        #[cfg(feature = "mysql-cdc")]
        {
            if config.properties().is_empty() {
                self.config.validate()?;
            } else {
                MySqlCdcConfig::from_config(config)?.validate()?;
            }
            Ok(SourceContract {
                consistency: SourceConsistency::Ephemeral,
                topology: SourceTopology::Singleton,
            })
        }
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let SourceStart {
            config, position, ..
        } = request;
        if let SourcePosition::Resume { attempt, .. } = position {
            return Err(ConnectorError::ConfigurationError(format!(
                "MySQL CDC is an ephemeral source and cannot resume checkpoint attempt {attempt:?}"
            )));
        }
        let config = &config;

        // Parse and update config if provided
        if !config.properties().is_empty() {
            self.config = MySqlCdcConfig::from_config(config)?;
        }

        // Validate configuration
        self.config.validate()?;

        // Initialize GTID set from config
        self.gtid_set.clone_from(&self.config.gtid_set);

        // Initialize binlog position from config
        if let Some(ref filename) = self.config.binlog_filename {
            self.current_binlog_file.clone_from(filename);
            if let Some(pos) = self.config.binlog_position {
                self.position = Some(BinlogPosition::new(filename.clone(), pos));
            }
        }

        // Without mysql-cdc feature, start() must fail loudly to prevent
        // silent data loss (poll_batch would return Ok(None) forever).
        #[cfg(not(feature = "mysql-cdc"))]
        {
            return Err(ConnectorError::ConfigurationError(
                "MySQL CDC source requires the `mysql-cdc` feature flag. \
                 Rebuild with `--features mysql-cdc` to enable."
                    .to_string(),
            ));
        }

        // When mysql-cdc feature is enabled, establish a real connection
        // and spawn a background reader task for event-driven wake-up.
        #[cfg(feature = "mysql-cdc")]
        {
            self.reader_error = None;
            let conn = super::mysql_io::connect(&self.config).await?;
            let stream = super::mysql_io::start_binlog_stream(
                conn,
                &self.config,
                self.gtid_set.as_ref(),
                self.position.as_ref(),
            )
            .await?;

            let (msg_tx, msg_rx) = crossfire::mpsc::bounded_async::<BinlogReaderMessage>(4096);
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
            let data_ready = Arc::clone(&self.data_ready);

            let reader_handle = tokio::spawn(async move {
                use tokio_stream::StreamExt as _;
                let mut stream = stream;
                loop {
                    let event = tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => break,
                        event = stream.next() => event,
                    };
                    match event {
                        Some(Ok(raw_event)) => {
                            match super::mysql_io::decode_binlog_event(&raw_event, &stream) {
                                Ok(Some(msg)) => {
                                    if msg_tx.send(BinlogReaderMessage::Event(msg)).await.is_err() {
                                        break;
                                    }
                                    data_ready.notify_one();
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    tracing::warn!(error = %e, "binlog decode error");
                                    let _ = msg_tx
                                        .send(BinlogReaderMessage::Terminal(format!(
                                            "binlog decode failed: {e}"
                                        )))
                                        .await;
                                    data_ready.notify_one();
                                    break;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "binlog stream error");
                            let _ = msg_tx
                                .send(BinlogReaderMessage::Terminal(format!(
                                    "binlog stream failed: {e}"
                                )))
                                .await;
                            data_ready.notify_one();
                            break;
                        }
                        None => {
                            let _ = msg_tx
                                .send(BinlogReaderMessage::Terminal(
                                    "binlog stream ended unexpectedly".into(),
                                ))
                                .await;
                            data_ready.notify_one();
                            break;
                        }
                    }
                }
                if let Err(e) = stream.close().await {
                    tracing::warn!(error = %e, "error closing binlog stream");
                }
            });

            self.msg_rx = Some(msg_rx);
            self.reader_handle = Some(reader_handle);
            self.reader_shutdown = Some(shutdown_tx);
        }

        self.connected = true;
        self.last_activity = Some(Instant::now());

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if !self.connected {
            return Err(ConnectorError::ConfigurationError(
                "Source not connected".to_string(),
            ));
        }

        // Drain decoded binlog messages from background reader task.
        //
        // Backpressure: when the event buffer exceeds the high watermark,
        // stop draining the reader channel. The bounded mpsc channel (4096)
        // propagates backpressure to the binlog reader task, which in turn
        // applies TCP backpressure to the replication connection.
        #[cfg(feature = "mysql-cdc")]
        {
            // A prior poll may have drained messages but been unable to return its batch (for
            // example because the configured record budget was reached). Publish that exact
            // single-table buffer before advancing the reader again.
            if let Some(table_info) = self.buffered_table_info()? {
                if let Some(batch) = self.flush_events(&table_info)? {
                    self.schema = Some(self.build_envelope_schema(&table_info.arrow_schema));
                    return Ok(Some(SourceBatch::new(batch)));
                }
            }
            if let Some(error) = self.reader_error.take() {
                self.connected = false;
                return Err(ConnectorError::ReadError(error));
            }

            let high_watermark = self.config.backpressure_high_watermark();

            if self.event_buffer.len() >= high_watermark {
                tracing::debug!(
                    buffered = self.event_buffer.len(),
                    high_watermark,
                    "CDC backpressure active — pausing binlog reader drain"
                );
            } else if let Some(rx) = self.msg_rx.as_mut() {
                let mut last_table_info: Option<TableInfo> = None;

                while self.event_buffer.len() < max_records
                    && self.event_buffer.len() < high_watermark
                {
                    match rx.try_recv() {
                        Ok(BinlogReaderMessage::Event(msg)) => {
                            self.metrics.inc_events_received();
                            match msg {
                                BinlogMessage::TableMap(tme) => {
                                    self.metrics.inc_table_maps();
                                    self.table_cache.update(&tme);
                                }
                                BinlogMessage::Insert(insert_msg) => {
                                    if !self.config.should_include_table(
                                        &insert_msg.database,
                                        &insert_msg.table,
                                    ) {
                                        continue;
                                    }
                                    let table_info = self
                                        .table_cache
                                        .get(insert_msg.table_id)
                                        .cloned()
                                        .ok_or_else(|| {
                                            ConnectorError::Internal(format!(
                                                "missing TABLE_MAP schema for {}.{}",
                                                insert_msg.database, insert_msg.table
                                            ))
                                        })?;
                                    let row_count = insert_msg.rows.len() as u64;
                                    let events = super::changelog::insert_to_events(
                                        &insert_msg,
                                        &self.current_binlog_file,
                                        self.current_gtid.as_deref(),
                                    );
                                    self.event_buffer.extend(events);
                                    self.metrics.inc_inserts(row_count);
                                    last_table_info = Some(table_info);
                                }
                                BinlogMessage::Update(update_msg) => {
                                    if !self.config.should_include_table(
                                        &update_msg.database,
                                        &update_msg.table,
                                    ) {
                                        continue;
                                    }
                                    let table_info = self
                                        .table_cache
                                        .get(update_msg.table_id)
                                        .cloned()
                                        .ok_or_else(|| {
                                            ConnectorError::Internal(format!(
                                                "missing TABLE_MAP schema for {}.{}",
                                                update_msg.database, update_msg.table
                                            ))
                                        })?;
                                    let row_count = update_msg.rows.len() as u64;
                                    let events = super::changelog::update_to_events(
                                        &update_msg,
                                        &self.current_binlog_file,
                                        self.current_gtid.as_deref(),
                                    );
                                    self.event_buffer.extend(events);
                                    self.metrics.inc_updates(row_count);
                                    last_table_info = Some(table_info);
                                }
                                BinlogMessage::Delete(delete_msg) => {
                                    if !self.config.should_include_table(
                                        &delete_msg.database,
                                        &delete_msg.table,
                                    ) {
                                        continue;
                                    }
                                    let table_info = self
                                        .table_cache
                                        .get(delete_msg.table_id)
                                        .cloned()
                                        .ok_or_else(|| {
                                            ConnectorError::Internal(format!(
                                                "missing TABLE_MAP schema for {}.{}",
                                                delete_msg.database, delete_msg.table
                                            ))
                                        })?;
                                    let row_count = delete_msg.rows.len() as u64;
                                    let events = super::changelog::delete_to_events(
                                        &delete_msg,
                                        &self.current_binlog_file,
                                        self.current_gtid.as_deref(),
                                    );
                                    self.event_buffer.extend(events);
                                    self.metrics.inc_deletes(row_count);
                                    last_table_info = Some(table_info);
                                }
                                BinlogMessage::Begin(begin_msg) => {
                                    if let Some(ref gtid) = begin_msg.gtid {
                                        self.current_gtid = Some(gtid.to_string());
                                        if let Some(ref mut gtid_set) = self.gtid_set {
                                            gtid_set.add(gtid);
                                        }
                                    } else {
                                        self.current_gtid = None;
                                    }
                                }
                                BinlogMessage::Commit(commit_msg) => {
                                    self.metrics.inc_transactions();
                                    self.metrics.set_binlog_position(commit_msg.binlog_position);
                                    if let Some(ref mut pos) = self.position {
                                        pos.position = commit_msg.binlog_position;
                                    }
                                }
                                BinlogMessage::Rotate(rotate_msg) => {
                                    self.current_binlog_file.clone_from(&rotate_msg.next_binlog);
                                    if let Some(ref mut pos) = self.position {
                                        pos.filename = rotate_msg.next_binlog;
                                        pos.position = rotate_msg.position;
                                    } else {
                                        self.position = Some(BinlogPosition::new(
                                            self.current_binlog_file.clone(),
                                            rotate_msg.position,
                                        ));
                                    }
                                }
                                BinlogMessage::Query(query_msg) => {
                                    self.metrics.inc_ddl_events();
                                    let _ = query_msg;
                                }
                                BinlogMessage::Heartbeat => {
                                    self.metrics.inc_heartbeats();
                                }
                            }
                        }
                        Ok(BinlogReaderMessage::Terminal(error)) => {
                            self.reader_error = Some(error);
                            break;
                        }
                        Err(crossfire::TryRecvError::Empty) => break,
                        Err(crossfire::TryRecvError::Disconnected) => {
                            self.reader_error =
                                Some("binlog reader task stopped without a terminal status".into());
                            break;
                        }
                    }
                }

                self.last_activity = Some(Instant::now());

                if let Some(table_info) = last_table_info {
                    if let Some(batch) = self.flush_events(&table_info)? {
                        let schema = self.build_envelope_schema(&table_info.arrow_schema);
                        self.schema = Some(schema);
                        return Ok(Some(SourceBatch::new(batch)));
                    }
                }

                if let Some(error) = self.reader_error.take() {
                    self.connected = false;
                    return Err(ConnectorError::ReadError(error));
                }

                return Ok(None);
            }

            self.last_activity = Some(Instant::now());
            return Ok(None);
        }

        // Without mysql-cdc feature: stub returns None.
        #[cfg(not(feature = "mysql-cdc"))]
        {
            let _ = max_records;
            self.last_activity = Some(Instant::now());
            Ok(None)
        }
    }

    fn schema(&self) -> SchemaRef {
        // Return cached schema or a default CDC envelope schema
        self.schema.clone().unwrap_or_else(|| {
            // Default CDC envelope with no table-specific columns
            Arc::new(cdc_envelope_schema(&Schema::empty()))
        })
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.create_checkpoint()
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Signal reader task to shut down (it closes the binlog stream internally).
        #[cfg(feature = "mysql-cdc")]
        {
            if let Some(tx) = self.reader_shutdown.take() {
                let _ = tx.send(true);
            }
            if let Some(handle) = self.reader_handle.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
            }
            self.msg_rx = None;
        }

        self.connected = false;
        self.table_cache.clear();
        self.event_buffer.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MySqlCdcConfig {
        MySqlCdcConfig {
            host: "localhost".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: Some("test".to_string()),
            server_id: 12345,
            table_include: vec!["testdb.users".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn test_new_source() {
        let config = test_config();
        let source = MySqlCdcSource::new(config, None);

        assert!(!source.is_connected());
        assert_eq!(source.cached_table_count(), 0);
        assert!(source.position().is_none());
        assert!(source.gtid_set().is_none());
    }

    #[test]
    fn test_from_config() {
        let mut config = ConnectorConfig::new("mysql-cdc");
        config.set("host", "mysql.example.com");
        config.set("port", "3307");
        config.set("username", "repl");
        config.set("password", "secret");
        config.set("server.id", "999");
        config.set("table.include", "app.users");

        let source = MySqlCdcSource::from_config(&config).unwrap();
        assert_eq!(source.config().host, "mysql.example.com");
        assert_eq!(source.config().port, 3307);
        assert_eq!(source.config().server_id, 999);
    }

    #[test]
    fn test_from_config_missing_required() {
        let config = ConnectorConfig::new("mysql-cdc");

        let result = MySqlCdcSource::from_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_checkpoint_gtid() {
        let mut source = MySqlCdcSource::new(test_config(), None);
        source.config.use_gtid = true;
        source.gtid_set = Some("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5".parse().unwrap());

        let checkpoint = source.create_checkpoint();
        assert!(checkpoint.get_offset("gtid").is_some());
        // UUID is stored and displayed as lowercase
        assert!(checkpoint.get_offset("gtid").unwrap().contains("3e11fa47"));
    }

    #[test]
    fn test_create_checkpoint_file() {
        let mut source = MySqlCdcSource::new(test_config(), None);
        source.config.use_gtid = false;
        source.position = Some(BinlogPosition::new("mysql-bin.000003".to_string(), 9999));

        let checkpoint = source.create_checkpoint();
        assert_eq!(
            checkpoint.get_offset("binlog_file"),
            Some("mysql-bin.000003")
        );
        assert_eq!(checkpoint.get_offset("binlog_position"), Some("9999"));
    }

    #[test]
    fn test_schema() {
        let source = MySqlCdcSource::new(test_config(), None);
        let schema = source.schema();

        // Should have CDC envelope fields
        let field_names: Vec<_> = schema.fields().iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&&"_table".to_string()));
        assert!(field_names.contains(&&"_op".to_string()));
        assert!(field_names.contains(&&"_ts_ms".to_string()));
    }

    #[test]
    fn test_table_filtering() {
        let mut config = test_config();
        config.table_include = vec!["testdb.users".to_string()];

        let source = MySqlCdcSource::new(config, None);

        assert!(source.should_include_table("testdb", "users"));
        assert!(!source.should_include_table("testdb", "orders"));
        assert!(!source.should_include_table("testdb", "other"));
        assert!(!source.should_include_table("other", "users"));
    }

    #[test]
    fn mixed_table_flush_fails_without_discarding_events() {
        use super::super::decoder::RowData;

        let mut source = MySqlCdcSource::new(test_config(), None);
        source.event_buffer = vec![
            ChangeEvent::insert(
                "testdb.users".into(),
                1,
                "mysql-bin.000001".into(),
                10,
                None,
                RowData { columns: vec![] },
            ),
            ChangeEvent::insert(
                "testdb.orders".into(),
                1,
                "mysql-bin.000001".into(),
                11,
                None,
                RowData { columns: vec![] },
            ),
        ];
        let table_info = TableInfo {
            table_id: 1,
            database: "testdb".into(),
            table: "users".into(),
            columns: vec![],
            arrow_schema: Schema::empty(),
        };

        let error = source.flush_events(&table_info).unwrap_err();

        assert!(error.to_string().contains("multiple table schemas"));
        assert_eq!(source.event_buffer.len(), 2);
    }

    // Without mysql-cdc feature, start() must return an error to prevent silent data loss.
    #[cfg(not(feature = "mysql-cdc"))]
    #[tokio::test]
    async fn test_start_fails_without_feature() {
        let mut source = MySqlCdcSource::new(test_config(), None);

        let result = source
            .start(SourceStart {
                config: ConnectorConfig::default(),
                position: SourcePosition::Initial,
                delivery: crate::connector::DeliveryGuarantee::BestEffort,
            })
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("mysql-cdc"),
            "error should mention feature flag: {err}"
        );
    }

    #[tokio::test]
    async fn test_poll_not_connected() {
        let mut source = MySqlCdcSource::new(test_config(), None);

        let result = source.poll_batch(100).await;
        assert!(result.is_err());
    }

    #[cfg(feature = "mysql-cdc")]
    #[tokio::test]
    async fn terminal_reader_status_is_not_silently_treated_as_an_empty_poll() {
        let mut source = MySqlCdcSource::new(test_config(), None);
        let (tx, rx) = crossfire::mpsc::bounded_async::<BinlogReaderMessage>(1);
        tx.send(BinlogReaderMessage::Terminal(
            "injected reader failure".into(),
        ))
        .await
        .unwrap();
        source.msg_rx = Some(rx);
        source.connected = true;

        let error = source.poll_batch(100).await.unwrap_err();

        assert!(error.to_string().contains("injected reader failure"));
        assert!(!source.connected);
    }

    #[cfg(feature = "mysql-cdc")]
    #[tokio::test]
    async fn unreported_reader_task_loss_is_terminal() {
        let mut source = MySqlCdcSource::new(test_config(), None);
        let (tx, rx) = crossfire::mpsc::bounded_async::<BinlogReaderMessage>(1);
        drop(tx);
        source.msg_rx = Some(rx);
        source.connected = true;

        let error = source.poll_batch(100).await.unwrap_err();

        assert!(error.to_string().contains("without a terminal status"));
        assert!(!source.connected);
    }

    // Without mysql-cdc feature, start() returns an error, so poll is unreachable.
    // With mysql-cdc, start() needs a real MySQL server. Covered by integration tests.

    #[tokio::test]
    async fn resume_is_rejected_before_mutating_ephemeral_source_position() {
        let mut source = MySqlCdcSource::new(test_config(), None);

        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("binlog_file", "mysql-bin.000005");
        checkpoint.set_offset("binlog_position", "54321");

        let error = source
            .start(SourceStart {
                config: ConnectorConfig::default(),
                position: SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint,
                },
                delivery: crate::connector::DeliveryGuarantee::BestEffort,
            })
            .await
            .expect_err("ephemeral MySQL CDC must reject recovery");

        assert!(error.to_string().contains("ephemeral"));
        assert!(source.position().is_none());
    }
}
