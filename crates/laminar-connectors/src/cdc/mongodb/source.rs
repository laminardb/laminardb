//! MongoDB CDC source connector implementation.
//!
//! Implements the [`SourceConnector`] trait for MongoDB change streams.
//! This module provides the main entry point: [`MongoCdcSource`].

use std::sync::Arc;
use std::time::Instant;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::changelog::{cdc_envelope_schema, events_to_record_batch, CdcOperation, ChangeEvent};
use super::config::MongoCdcConfig;
use super::metrics::MongoCdcMetrics;

/// MongoDB change stream CDC source connector.
///
/// Reads change events from MongoDB change streams. Supports database-level
/// and collection-level change streams with optional pipeline filtering.
///
/// # Example
///
/// ```ignore
/// use laminar_connectors::cdc::mongodb::{MongoCdcSource, MongoCdcConfig};
///
/// let config = MongoCdcConfig::new("mongodb://localhost:27017", "mydb");
/// let mut source = MongoCdcSource::new(config);
/// source.open(&ConnectorConfig::default()).await?;
///
/// while let Some(batch) = source.poll_batch(1000).await? {
///     println!("Received {} rows", batch.num_rows());
/// }
/// ```
pub struct MongoCdcSource {
    /// Configuration for the MongoDB CDC connection.
    config: MongoCdcConfig,

    /// Whether the source is currently connected.
    connected: bool,

    /// Current resume token (for checkpointing).
    resume_token: Option<String>,

    /// Buffered change events waiting to be emitted.
    event_buffer: Vec<ChangeEvent>,

    /// Metrics for this source.
    metrics: MongoCdcMetrics,

    /// Arrow schema for CDC envelope.
    schema: SchemaRef,

    /// Last time we received data (for health checks).
    last_activity: Option<Instant>,

    /// Notification handle signalled when change stream data arrives.
    data_ready: Arc<Notify>,

    /// Channel receiver for decoded change events from the background reader task.
    #[cfg(feature = "mongodb-cdc")]
    msg_rx: Option<tokio::sync::mpsc::Receiver<ChangeStreamMessage>>,

    /// Background change stream reader task handle.
    #[cfg(feature = "mongodb-cdc")]
    reader_handle: Option<tokio::task::JoinHandle<()>>,

    /// Shutdown signal for the background reader task.
    #[cfg(feature = "mongodb-cdc")]
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,
}

/// Messages received from the background change stream reader task.
#[cfg(feature = "mongodb-cdc")]
#[derive(Debug)]
pub enum ChangeStreamMessage {
    /// A change event from the stream, with its serialized byte size.
    Event(ChangeEvent, u64),
    /// A stream error (non-fatal; reader may continue or break).
    Error(String),
}

impl std::fmt::Debug for MongoCdcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoCdcSource")
            .field("config", &self.config)
            .field("connected", &self.connected)
            .field("resume_token", &self.resume_token)
            .field("event_buffer_len", &self.event_buffer.len())
            .field("metrics", &self.metrics)
            .field("last_activity", &self.last_activity)
            .finish_non_exhaustive()
    }
}

impl MongoCdcSource {
    /// Creates a new MongoDB CDC source with the given configuration.
    #[must_use]
    pub fn new(config: MongoCdcConfig) -> Self {
        Self {
            config,
            connected: false,
            resume_token: None,
            event_buffer: Vec::new(),
            metrics: MongoCdcMetrics::new(),
            schema: Arc::new(cdc_envelope_schema()),
            last_activity: None,
            data_ready: Arc::new(Notify::new()),
            #[cfg(feature = "mongodb-cdc")]
            msg_rx: None,
            #[cfg(feature = "mongodb-cdc")]
            reader_handle: None,
            #[cfg(feature = "mongodb-cdc")]
            reader_shutdown: None,
        }
    }

    /// Creates a MongoDB CDC source from a generic connector config.
    ///
    /// # Errors
    ///
    /// Returns error if required configuration keys are missing.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mongo_config = MongoCdcConfig::from_config(config)?;
        Ok(Self::new(mongo_config))
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &MongoCdcConfig {
        &self.config
    }

    /// Returns whether the source is connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Returns the current resume token.
    #[must_use]
    pub fn resume_token(&self) -> Option<&str> {
        self.resume_token.as_deref()
    }

    /// Returns a reference to the metrics.
    #[must_use]
    pub fn cdc_metrics(&self) -> &MongoCdcMetrics {
        &self.metrics
    }

    /// Checks if a collection should be included based on filters.
    #[must_use]
    pub fn should_include_collection(&self, collection: &str) -> bool {
        self.config.should_include_collection(collection)
    }

    /// Restores the resume token from a checkpoint.
    pub fn restore_position(&mut self, checkpoint: &SourceCheckpoint) {
        if let Some(token) = checkpoint.get_offset("resume_token") {
            self.resume_token = Some(token.to_string());
        }
    }

    /// Creates a checkpoint representing the current position.
    #[must_use]
    pub fn create_checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new(0);

        if let Some(ref token) = self.resume_token {
            checkpoint.set_offset("resume_token", token.clone());
        }

        checkpoint.set_metadata("database", &self.config.database);
        if let Some(ref collection) = self.config.collection {
            checkpoint.set_metadata("collection", collection.as_str());
        }

        checkpoint
    }

    /// Flushes buffered events to a RecordBatch.
    ///
    /// # Errors
    ///
    /// Returns error if batch conversion fails.
    pub fn flush_events(&mut self) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.event_buffer.is_empty() {
            return Ok(None);
        }

        let events: Vec<_> = self.event_buffer.drain(..).collect();

        // Track the latest resume token
        if let Some(last_event) = events.last() {
            self.resume_token = Some(last_event.resume_token.clone());
            self.metrics.inc_resume_token_updates();
        }

        let batch =
            events_to_record_batch(&events).map_err(|e| ConnectorError::Internal(e.to_string()))?;
        Ok(Some(SourceBatch::new(batch)))
    }
}

#[async_trait]
impl SourceConnector for MongoCdcSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        // Parse and update config if provided
        if !config.properties().is_empty() {
            self.config = MongoCdcConfig::from_config(config)?;
        }

        // Validate configuration
        self.config.validate()?;

        // Use restored checkpoint token if present, else fall back to config token
        if self.resume_token.is_none() {
            self.resume_token.clone_from(&self.config.resume_token);
        }

        // Without mongodb-cdc feature, open() must fail loudly to prevent
        // silent data loss (poll_batch would return Ok(None) forever).
        #[cfg(not(feature = "mongodb-cdc"))]
        {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB CDC source requires the `mongodb-cdc` feature flag. \
                 Rebuild with `--features mongodb-cdc` to enable."
                    .to_string(),
            ));
        }

        // When mongodb-cdc feature is enabled, establish a real connection
        // and spawn a background reader task.
        #[cfg(feature = "mongodb-cdc")]
        {
            let client = super::mongo_io::connect(&self.config).await?;
            let stream = super::mongo_io::open_change_stream(
                &client,
                &self.config,
                self.resume_token.as_deref(),
            )
            .await?;

            let (msg_tx, msg_rx) =
                tokio::sync::mpsc::channel(self.config.max_buffered_events);
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
            let data_ready = Arc::clone(&self.data_ready);
            let collection_filter = self.config.collection_include.clone();
            let collection_exclude = self.config.collection_exclude.clone();

            let reader_handle = tokio::spawn(async move {
                use futures_util::StreamExt as _;
                let mut stream = stream;
                loop {
                    let event = tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => break,
                        event = stream.next() => event,
                    };
                    match event {
                        Some(Ok(change_event)) => {
                            // Estimate byte size from the BSON document
                            let byte_size = serde_json::to_vec(&change_event)
                                .map(|v| v.len() as u64)
                                .unwrap_or(0);
                            let parsed = super::mongo_io::decode_change_event(
                                &change_event,
                                &collection_filter,
                                &collection_exclude,
                            );
                            if let Some(evt) = parsed {
                                if msg_tx
                                    .send(ChangeStreamMessage::Event(evt, byte_size))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                                data_ready.notify_one();
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "change stream error");
                            let _ = msg_tx
                                .send(ChangeStreamMessage::Error(e.to_string()))
                                .await;
                            break;
                        }
                        None => break,
                    }
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

        // Drain decoded change events from background reader task.
        #[cfg(feature = "mongodb-cdc")]
        {
            if let Some(rx) = self.msg_rx.as_mut() {
                let mut reader_closed = false;
                while self.event_buffer.len() < max_records {
                    match rx.try_recv() {
                        Ok(ChangeStreamMessage::Event(event, byte_size)) => {
                            self.metrics.inc_events_received();
                            self.metrics.add_bytes_received(byte_size);
                            match event.operation {
                                CdcOperation::Insert => self.metrics.inc_inserts(1),
                                CdcOperation::Update => self.metrics.inc_updates(1),
                                CdcOperation::Replace => self.metrics.inc_replaces(1),
                                CdcOperation::Delete => self.metrics.inc_deletes(1),
                            }
                            self.event_buffer.push(event);
                        }
                        Ok(ChangeStreamMessage::Error(_)) => {
                            self.metrics.inc_errors();
                            reader_closed = true;
                            break;
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                            reader_closed = true;
                            break;
                        }
                    }
                }
                if reader_closed && self.event_buffer.is_empty() {
                    self.connected = false;
                    return Err(ConnectorError::ReadError(
                        "MongoDB change stream reader terminated unexpectedly".to_string(),
                    ));
                }
            }

            self.last_activity = Some(Instant::now());
            return self.flush_events();
        }

        // Without mongodb-cdc feature: stub returns None.
        #[cfg(not(feature = "mongodb-cdc"))]
        {
            let _ = max_records;
            self.last_activity = Some(Instant::now());
            Ok(None)
        }
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
            if last.elapsed() > self.config.poll_timeout * 30 {
                return HealthStatus::Degraded(format!(
                    "No activity for {}s",
                    last.elapsed().as_secs()
                ));
            }
        }

        // Check error count
        let errors = self
            .metrics
            .errors
            .load(std::sync::atomic::Ordering::Relaxed);
        if errors > 100 {
            return HealthStatus::Degraded(format!("{errors} errors encountered"));
        }

        HealthStatus::Healthy
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Signal reader task to shut down.
        #[cfg(feature = "mongodb-cdc")]
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
        self.event_buffer.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_config() -> MongoCdcConfig {
        MongoCdcConfig::new("mongodb://localhost:27017", "testdb")
    }

    #[test]
    fn test_new_source() {
        let config = test_config();
        let source = MongoCdcSource::new(config);

        assert!(!source.is_connected());
        assert!(source.resume_token().is_none());
    }

    #[test]
    fn test_from_config() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://mongo.example.com:27017");
        config.set("database", "mydb");
        config.set("collection", "events");

        let source = MongoCdcSource::from_config(&config).unwrap();
        assert_eq!(
            source.config().connection_uri,
            "mongodb://mongo.example.com:27017"
        );
        assert_eq!(source.config().database, "mydb");
        assert_eq!(source.config().collection.as_deref(), Some("events"));
    }

    #[test]
    fn test_from_config_missing_required() {
        let config = ConnectorConfig::new("mongodb-cdc");
        let result = MongoCdcSource::from_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_restore_position() {
        let mut source = MongoCdcSource::new(test_config());

        let mut checkpoint = SourceCheckpoint::new(1);
        checkpoint.set_offset("resume_token", r#"{"_data": "token123"}"#);

        source.restore_position(&checkpoint);
        assert_eq!(source.resume_token(), Some(r#"{"_data": "token123"}"#));
    }

    #[test]
    fn test_create_checkpoint() {
        let mut source = MongoCdcSource::new(test_config());
        source.resume_token = Some(r#"{"_data": "token456"}"#.to_string());

        let checkpoint = source.create_checkpoint();
        assert_eq!(
            checkpoint.get_offset("resume_token"),
            Some(r#"{"_data": "token456"}"#)
        );
        assert_eq!(checkpoint.get_metadata("database"), Some("testdb"));
    }

    #[test]
    fn test_create_checkpoint_with_collection() {
        let config =
            MongoCdcConfig::with_collection("mongodb://localhost:27017", "testdb", "users");
        let source = MongoCdcSource::new(config);

        let checkpoint = source.create_checkpoint();
        assert_eq!(checkpoint.get_metadata("collection"), Some("users"));
    }

    #[test]
    fn test_create_checkpoint_empty() {
        let source = MongoCdcSource::new(test_config());
        let checkpoint = source.create_checkpoint();
        assert!(checkpoint.get_offset("resume_token").is_none());
    }

    #[test]
    fn test_schema() {
        let source = MongoCdcSource::new(test_config());
        let schema = source.schema();

        let field_names: Vec<_> = schema.fields().iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&&"_collection".to_string()));
        assert!(field_names.contains(&&"_op".to_string()));
        assert!(field_names.contains(&&"_ts_ms".to_string()));
        assert!(field_names.contains(&&"_document_key".to_string()));
        assert!(field_names.contains(&&"_resume_token".to_string()));
    }

    #[test]
    fn test_health_check_not_connected() {
        let source = MongoCdcSource::new(test_config());

        match source.health_check() {
            HealthStatus::Unhealthy(message) => {
                assert!(message.contains("Not connected"));
            }
            _ => panic!("Expected unhealthy status"),
        }
    }

    #[test]
    fn test_health_check_healthy() {
        let mut source = MongoCdcSource::new(test_config());
        source.connected = true;
        source.last_activity = Some(Instant::now());

        assert!(matches!(source.health_check(), HealthStatus::Healthy));
    }

    #[test]
    fn test_health_check_degraded_no_activity() {
        let mut source = MongoCdcSource::new(test_config());
        source.connected = true;
        source.config.poll_timeout = Duration::from_millis(1);
        source.last_activity = Instant::now().checked_sub(Duration::from_secs(10));

        match source.health_check() {
            HealthStatus::Degraded(message) => {
                assert!(message.contains("No activity"));
            }
            _ => panic!("Expected degraded status"),
        }
    }

    #[test]
    fn test_health_check_degraded_errors() {
        let mut source = MongoCdcSource::new(test_config());
        source.connected = true;
        source.last_activity = Some(Instant::now());

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
        let mut source = MongoCdcSource::new(test_config());
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
    fn test_collection_filtering() {
        let mut config = test_config();
        config.collection_include = vec!["users".to_string(), "orders".to_string()];

        let source = MongoCdcSource::new(config);

        assert!(source.should_include_collection("users"));
        assert!(source.should_include_collection("orders"));
        assert!(!source.should_include_collection("other"));
    }

    // Without mongodb-cdc feature, open() must return an error to prevent silent data loss.
    #[cfg(not(feature = "mongodb-cdc"))]
    #[tokio::test]
    async fn test_open_fails_without_feature() {
        let mut source = MongoCdcSource::new(test_config());

        let result = source.open(&ConnectorConfig::default()).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("mongodb-cdc"),
            "error should mention feature flag: {err}"
        );
    }

    #[tokio::test]
    async fn test_poll_not_connected() {
        let mut source = MongoCdcSource::new(test_config());

        let result = source.poll_batch(100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_restore_async() {
        let mut source = MongoCdcSource::new(test_config());

        let mut checkpoint = SourceCheckpoint::new(1);
        checkpoint.set_offset("resume_token", "test_token_123");

        source.restore(&checkpoint).await.unwrap();
        assert_eq!(source.resume_token(), Some("test_token_123"));
    }

    #[test]
    fn test_flush_events_empty() {
        let mut source = MongoCdcSource::new(test_config());
        let result = source.flush_events().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_flush_events_with_data() {
        let mut source = MongoCdcSource::new(test_config());

        source.event_buffer.push(ChangeEvent {
            collection: "testdb.users".to_string(),
            operation: CdcOperation::Insert,
            timestamp_ms: 1_704_067_200_000,
            document_key: r#"{"_id": "abc"}"#.to_string(),
            full_document: Some(r#"{"_id": "abc", "name": "Alice"}"#.to_string()),
            full_document_before_change: None,
            update_description: None,
            resume_token: r#"{"_data": "tok1"}"#.to_string(),
        });

        let result = source.flush_events().unwrap();
        assert!(result.is_some());

        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Resume token should be updated
        assert_eq!(source.resume_token(), Some(r#"{"_data": "tok1"}"#));
        assert!(source.event_buffer.is_empty());
    }
}
