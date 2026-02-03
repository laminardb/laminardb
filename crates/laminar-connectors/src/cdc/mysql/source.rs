//! MySQL CDC source connector implementation.
//!
//! Implements the [`SourceConnector`] trait for MySQL binlog replication.
//! This module provides the main entry point for MySQL CDC: [`MySqlCdcSource`].

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::changelog::ChangeEvent;
use super::config::MySqlCdcConfig;
use super::decoder::BinlogPosition;
use super::gtid::GtidSet;
use super::metrics::MySqlCdcMetrics;
use super::schema::{cdc_envelope_schema, TableCache, TableInfo};

/// MySQL binlog CDC source connector.
///
/// Reads change events from MySQL binary log using replication protocol.
/// Supports both GTID-based and file/position-based replication.
///
/// # Example
///
/// ```ignore
/// use laminar_connectors::cdc::mysql::{MySqlCdcSource, MySqlCdcConfig};
///
/// let config = MySqlCdcConfig {
///     host: "localhost".to_string(),
///     port: 3306,
///     username: "replicator".to_string(),
///     password: "secret".to_string(),
///     server_id: 12345,
///     ..Default::default()
/// };
///
/// let mut source = MySqlCdcSource::new(config);
/// source.open(&ConnectorConfig::default()).await?;
///
/// while let Some(batch) = source.poll_batch(1000).await? {
///     println!("Received {} rows", batch.num_rows());
/// }
/// ```
#[derive(Debug)]
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

    /// Buffered change events waiting to be emitted.
    event_buffer: Vec<ChangeEvent>,

    /// Metrics for this source.
    metrics: MySqlCdcMetrics,

    /// Arrow schema for CDC envelope.
    schema: Option<SchemaRef>,

    /// Last time we received data (for health checks).
    last_activity: Option<Instant>,
}

impl MySqlCdcSource {
    /// Creates a new MySQL CDC source with the given configuration.
    #[must_use]
    pub fn new(config: MySqlCdcConfig) -> Self {
        Self {
            config,
            connected: false,
            table_cache: TableCache::new(),
            position: None,
            gtid_set: None,
            event_buffer: Vec::new(),
            metrics: MySqlCdcMetrics::new(),
            schema: None,
            last_activity: None,
        }
    }

    /// Creates a MySQL CDC source from a generic connector config.
    ///
    /// # Errors
    ///
    /// Returns error if required configuration keys are missing.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mysql_config = MySqlCdcConfig::from_config(config)?;
        Ok(Self::new(mysql_config))
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

    /// Restores the position from a checkpoint.
    ///
    /// Parses the checkpoint offset to extract GTID set or file/position.
    pub fn restore_position(&mut self, checkpoint: &SourceCheckpoint) {
        // Try GTID from offset key first
        if let Some(gtid_str) = checkpoint.get_offset("gtid") {
            if let Ok(gtid_set) = gtid_str.parse::<GtidSet>() {
                self.gtid_set = Some(gtid_set);
                return;
            }
        }

        // Try binlog file/position
        if let (Some(filename), Some(pos_str)) = (
            checkpoint.get_offset("binlog_file"),
            checkpoint.get_offset("binlog_position"),
        ) {
            if let Ok(pos) = pos_str.parse::<u64>() {
                self.position = Some(BinlogPosition::new(filename.to_string(), pos));
            }
        }
    }

    /// Creates a checkpoint representing the current position.
    #[must_use]
    pub fn create_checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new(0);

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
    fn build_envelope_schema(&self, table_schema: &Schema) -> SchemaRef {
        Arc::new(cdc_envelope_schema(table_schema))
    }

    /// Flushes buffered events to a RecordBatch.
    ///
    /// # Errors
    ///
    /// Returns error if batch conversion fails.
    pub fn flush_events(&mut self, table_info: &TableInfo) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.event_buffer.is_empty() {
            return Ok(None);
        }

        let events: Vec<_> = self.event_buffer.drain(..).collect();
        let batch = super::changelog::events_to_record_batch(&events, table_info)
            .map_err(|e| ConnectorError::Internal(e.to_string()))?;
        Ok(Some(batch))
    }
}

#[async_trait]
impl SourceConnector for MySqlCdcSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        // Parse and update config if provided
        if !config.properties().is_empty() {
            self.config = MySqlCdcConfig::from_config(config)?;
        }

        // Validate configuration
        self.config.validate()?;

        // Initialize GTID set from config
        self.gtid_set = self.config.gtid_set.clone();

        // Initialize binlog position from config
        if let Some(ref filename) = self.config.binlog_filename {
            if let Some(pos) = self.config.binlog_position {
                self.position = Some(BinlogPosition::new(filename.clone(), pos));
            }
        }

        // NOTE: Actual connection to MySQL server would happen here.
        // This requires mysql_async crate which has OpenSSL dependency.
        // For now, we set connected to true to allow business logic testing.
        // Actual I/O will be implemented in F028A.
        self.connected = true;
        self.last_activity = Some(Instant::now());

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if !self.connected {
            return Err(ConnectorError::ConfigurationError(
                "Source not connected".to_string(),
            ));
        }

        // NOTE: Actual binlog reading would happen here.
        // This requires mysql_async crate for the connection.
        // For now, return None (no data available).
        // Actual I/O will be implemented in F028A.

        // Update last activity timestamp
        self.last_activity = Some(Instant::now());

        Ok(None)
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

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        self.restore_position(checkpoint);
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        if !self.connected {
            return HealthStatus::Unhealthy("Not connected".to_string());
        }

        // Check for recent activity
        if let Some(last) = self.last_activity {
            let idle_duration = self.config.heartbeat_interval * 3;
            if last.elapsed() > idle_duration {
                return HealthStatus::Degraded(format!(
                    "No activity for {}s",
                    last.elapsed().as_secs()
                ));
            }
        }

        // Check error count
        let errors = self.metrics.errors.load(std::sync::atomic::Ordering::Relaxed);
        if errors > 100 {
            return HealthStatus::Degraded(format!("{errors} errors encountered"));
        }

        HealthStatus::Healthy
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // NOTE: Actual disconnection would happen here.
        self.connected = false;
        self.table_cache.clear();
        self.event_buffer.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_config() -> MySqlCdcConfig {
        MySqlCdcConfig {
            host: "localhost".to_string(),
            port: 3306,
            database: Some("testdb".to_string()),
            username: "root".to_string(),
            password: Some("test".to_string()),
            server_id: 12345,
            ..Default::default()
        }
    }

    #[test]
    fn test_new_source() {
        let config = test_config();
        let source = MySqlCdcSource::new(config);

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
    fn test_restore_position_gtid() {
        let mut source = MySqlCdcSource::new(test_config());

        let mut checkpoint = SourceCheckpoint::new(1);
        checkpoint.set_offset("gtid", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5");

        source.restore_position(&checkpoint);
        assert!(source.gtid_set().is_some());
    }

    #[test]
    fn test_restore_position_file() {
        let mut source = MySqlCdcSource::new(test_config());

        let mut checkpoint = SourceCheckpoint::new(1);
        checkpoint.set_offset("binlog_file", "mysql-bin.000003");
        checkpoint.set_offset("binlog_position", "12345");

        source.restore_position(&checkpoint);
        let pos = source.position().unwrap();
        assert_eq!(pos.filename, "mysql-bin.000003");
        assert_eq!(pos.position, 12345);
    }

    #[test]
    fn test_create_checkpoint_gtid() {
        let mut source = MySqlCdcSource::new(test_config());
        source.config.use_gtid = true;
        source.gtid_set = Some("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5".parse().unwrap());

        let checkpoint = source.create_checkpoint();
        assert!(checkpoint.get_offset("gtid").is_some());
        // UUID is stored and displayed as lowercase
        assert!(checkpoint.get_offset("gtid").unwrap().contains("3e11fa47"));
    }

    #[test]
    fn test_create_checkpoint_file() {
        let mut source = MySqlCdcSource::new(test_config());
        source.config.use_gtid = false;
        source.position = Some(BinlogPosition::new("mysql-bin.000003".to_string(), 9999));

        let checkpoint = source.create_checkpoint();
        assert_eq!(checkpoint.get_offset("binlog_file"), Some("mysql-bin.000003"));
        assert_eq!(checkpoint.get_offset("binlog_position"), Some("9999"));
    }

    #[test]
    fn test_schema() {
        let source = MySqlCdcSource::new(test_config());
        let schema = source.schema();

        // Should have CDC envelope fields
        let field_names: Vec<_> = schema.fields().iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&&"_table".to_string()));
        assert!(field_names.contains(&&"_op".to_string()));
        assert!(field_names.contains(&&"_ts_ms".to_string()));
    }

    #[test]
    fn test_health_check_not_connected() {
        let source = MySqlCdcSource::new(test_config());

        match source.health_check() {
            HealthStatus::Unhealthy(message) => {
                assert!(message.contains("Not connected"));
            }
            _ => panic!("Expected unhealthy status"),
        }
    }

    #[test]
    fn test_health_check_healthy() {
        let mut source = MySqlCdcSource::new(test_config());
        source.connected = true;
        source.last_activity = Some(Instant::now());

        assert!(matches!(source.health_check(), HealthStatus::Healthy));
    }

    #[test]
    fn test_health_check_degraded_no_activity() {
        let mut source = MySqlCdcSource::new(test_config());
        source.connected = true;
        source.config.heartbeat_interval = Duration::from_millis(1);
        source.last_activity = Some(Instant::now() - Duration::from_secs(10));

        match source.health_check() {
            HealthStatus::Degraded(message) => {
                assert!(message.contains("No activity"));
            }
            _ => panic!("Expected degraded status"),
        }
    }

    #[test]
    fn test_health_check_degraded_errors() {
        let mut source = MySqlCdcSource::new(test_config());
        source.connected = true;
        source.last_activity = Some(Instant::now());

        // Simulate many errors
        for _ in 0..150 {
            source.metrics.inc_errors();
        }

        match source.health_check() {
            HealthStatus::Degraded(message) => {
                assert!(message.contains("errors"));
            }
            _ => panic!("Expected degraded status"),
        }
    }

    #[test]
    fn test_metrics() {
        let mut source = MySqlCdcSource::new(test_config());
        source.metrics.inc_inserts(100);
        source.metrics.inc_updates(50);
        source.metrics.inc_deletes(25);
        source.metrics.add_bytes_received(10000);
        source.metrics.inc_errors();

        let metrics = source.metrics();
        assert_eq!(metrics.records_total, 175);
        assert_eq!(metrics.bytes_total, 10000);
        assert_eq!(metrics.errors_total, 1);
    }

    #[test]
    fn test_table_filtering() {
        let mut config = test_config();
        config.table_include = vec!["users".to_string(), "orders".to_string()];

        let source = MySqlCdcSource::new(config);

        assert!(source.should_include_table("testdb", "users"));
        assert!(source.should_include_table("testdb", "orders"));
        assert!(!source.should_include_table("testdb", "other"));
    }

    #[tokio::test]
    async fn test_open_close() {
        let mut source = MySqlCdcSource::new(test_config());

        source.open(&ConnectorConfig::default()).await.unwrap();
        assert!(source.is_connected());

        source.close().await.unwrap();
        assert!(!source.is_connected());
        assert_eq!(source.cached_table_count(), 0);
    }

    #[tokio::test]
    async fn test_poll_not_connected() {
        let mut source = MySqlCdcSource::new(test_config());

        let result = source.poll_batch(100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_poll_connected() {
        let mut source = MySqlCdcSource::new(test_config());
        source.open(&ConnectorConfig::default()).await.unwrap();

        // Should return None (no actual data without real connection)
        let result = source.poll_batch(100).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_restore_async() {
        let mut source = MySqlCdcSource::new(test_config());

        let mut checkpoint = SourceCheckpoint::new(1);
        checkpoint.set_offset("binlog_file", "mysql-bin.000005");
        checkpoint.set_offset("binlog_position", "54321");

        source.restore(&checkpoint).await.unwrap();

        let pos = source.position().unwrap();
        assert_eq!(pos.filename, "mysql-bin.000005");
        assert_eq!(pos.position, 54321);
    }
}
