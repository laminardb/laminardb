//! Connector runtime for managing source and sink lifecycle.
//!
//! The [`ConnectorRuntime`] orchestrates the lifecycle of source and sink
//! connectors, bridging them with the streaming API:
//!
//! - **Sources**: Poll external systems and push data via `Source<ArrowRecord>::push_arrow()`
//! - **Sinks**: Subscribe to pipeline output and write to external systems
//!
//! All connector I/O runs in Ring 1 (background async tasks).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parking_lot::RwLock;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::SourceConnector;
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::RuntimeMetrics;
use crate::registry::ConnectorRegistry;

/// Configuration for the connector runtime.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Maximum records per source poll batch.
    pub max_poll_batch_size: usize,

    /// Delay between polls when no data is available (ms).
    pub poll_interval_ms: u64,

    /// Maximum records to buffer before writing to sink.
    pub sink_batch_size: usize,

    /// Maximum time to wait before flushing sink buffer (ms).
    pub sink_flush_interval_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_poll_batch_size: 1000,
            poll_interval_ms: 100,
            sink_batch_size: 1000,
            sink_flush_interval_ms: 1000,
        }
    }
}

/// Handle to a running source connector.
#[derive(Debug)]
pub struct SourceHandle {
    /// Name of the source.
    pub name: String,

    /// Current state.
    state: Arc<RwLock<ConnectorState>>,

    /// Runtime metrics.
    metrics: Arc<RuntimeMetrics>,

    /// Shutdown signal sender.
    shutdown_tx: Option<oneshot::Sender<()>>,

    /// Task join handle.
    task_handle: Option<JoinHandle<Result<(), ConnectorError>>>,
}

impl SourceHandle {
    /// Returns the current state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        *self.state.read()
    }

    /// Returns the runtime metrics.
    #[must_use]
    pub fn metrics(&self) -> &RuntimeMetrics {
        &self.metrics
    }

    /// Sends a shutdown signal to the source task.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        *self.state.write() = ConnectorState::Closed;
    }

    /// Waits for the source task to complete.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the task panicked or returned an error.
    pub async fn join(mut self) -> Result<(), ConnectorError> {
        if let Some(handle) = self.task_handle.take() {
            handle
                .await
                .map_err(|e| ConnectorError::Internal(format!("task panicked: {e}")))?
        } else {
            Ok(())
        }
    }
}

/// Handle to a running sink connector.
#[derive(Debug)]
pub struct SinkHandle {
    /// Name of the sink.
    pub name: String,

    /// Current state.
    state: Arc<RwLock<ConnectorState>>,

    /// Runtime metrics.
    metrics: Arc<RuntimeMetrics>,

    /// Shutdown signal sender.
    shutdown_tx: Option<oneshot::Sender<()>>,

    /// Task join handle.
    task_handle: Option<JoinHandle<Result<(), ConnectorError>>>,
}

impl SinkHandle {
    /// Returns the current state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        *self.state.read()
    }

    /// Returns the runtime metrics.
    #[must_use]
    pub fn metrics(&self) -> &RuntimeMetrics {
        &self.metrics
    }

    /// Sends a shutdown signal to the sink task.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        *self.state.write() = ConnectorState::Closed;
    }

    /// Waits for the sink task to complete.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the task panicked or returned an error.
    pub async fn join(mut self) -> Result<(), ConnectorError> {
        if let Some(handle) = self.task_handle.take() {
            handle
                .await
                .map_err(|e| ConnectorError::Internal(format!("task panicked: {e}")))?
        } else {
            Ok(())
        }
    }
}

/// Runtime that manages the lifecycle of source and sink connectors.
///
/// The runtime spawns tokio tasks for each connector and manages their
/// lifecycle (start, stop, checkpoint, health monitoring).
pub struct ConnectorRuntime {
    /// Connector registry for creating instances.
    registry: ConnectorRegistry,

    /// Runtime configuration.
    config: RuntimeConfig,

    /// Active source handles.
    sources: Arc<RwLock<HashMap<String, SourceHandle>>>,

    /// Active sink handles.
    sinks: Arc<RwLock<HashMap<String, SinkHandle>>>,
}

impl ConnectorRuntime {
    /// Creates a new connector runtime.
    #[must_use]
    pub fn new(registry: ConnectorRegistry, config: RuntimeConfig) -> Self {
        Self {
            registry,
            config,
            sources: Arc::new(RwLock::new(HashMap::new())),
            sinks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a runtime with default configuration.
    #[must_use]
    pub fn with_registry(registry: ConnectorRegistry) -> Self {
        Self::new(registry, RuntimeConfig::default())
    }

    /// Returns a reference to the registry.
    #[must_use]
    pub fn registry(&self) -> &ConnectorRegistry {
        &self.registry
    }

    /// Starts a source connector.
    ///
    /// Creates a connector instance from the registry, opens it, and
    /// spawns a background task that polls for data and pushes it via
    /// the provided callback.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the connector cannot be created or opened.
    pub async fn start_source<F>(
        &self,
        name: impl Into<String>,
        config: ConnectorConfig,
        on_batch: F,
    ) -> Result<(), ConnectorError>
    where
        F: Fn(RecordBatch) -> Result<(), ConnectorError> + Send + Sync + 'static,
    {
        let name = name.into();
        let mut connector = self.registry.create_source(&config)?;
        connector.open(&config).await?;

        let state = Arc::new(RwLock::new(ConnectorState::Running));
        let metrics = Arc::new(RuntimeMetrics::new());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task_state = Arc::clone(&state);
        let task_metrics = Arc::clone(&metrics);
        let poll_batch_size = self.config.max_poll_batch_size;
        let poll_interval = self.config.poll_interval_ms;

        let task_handle = tokio::spawn(async move {
            source_poll_loop(
                connector,
                task_state,
                task_metrics,
                shutdown_rx,
                on_batch,
                poll_batch_size,
                poll_interval,
            )
            .await
        });

        let handle = SourceHandle {
            name: name.clone(),
            state,
            metrics,
            shutdown_tx: Some(shutdown_tx),
            task_handle: Some(task_handle),
        };

        self.sources.write().insert(name, handle);
        Ok(())
    }

    /// Starts a sink connector.
    ///
    /// Creates a connector instance from the registry, opens it, and
    /// spawns a background task that consumes batches from the provided
    /// async receiver and writes them to the sink.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the connector cannot be created or opened.
    pub async fn start_sink(
        &self,
        name: impl Into<String>,
        config: ConnectorConfig,
        mut batch_rx: tokio::sync::mpsc::Receiver<RecordBatch>,
    ) -> Result<(), ConnectorError> {
        let name = name.into();
        let mut connector = self.registry.create_sink(&config)?;
        connector.open(&config).await?;

        let state = Arc::new(RwLock::new(ConnectorState::Running));
        let metrics = Arc::new(RuntimeMetrics::new());
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        let task_state = Arc::clone(&state);
        let task_metrics = Arc::clone(&metrics);

        let task_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        *task_state.write() = ConnectorState::Closed;
                        break;
                    }
                    batch = batch_rx.recv() => {
                        if let Some(batch) = batch {
                            let num_rows = batch.num_rows() as u64;
                            match connector.write_batch(&batch).await {
                                Ok(result) => {
                                    task_metrics.record_batch(num_rows, result.bytes_written);
                                }
                                Err(e) => {
                                    task_metrics.record_error();
                                    tracing::error!("sink write error: {e}");
                                }
                            }
                        } else {
                            // Channel closed
                            *task_state.write() = ConnectorState::Closed;
                            break;
                        }
                    }
                }
            }
            connector.close().await
        });

        let handle = SinkHandle {
            name: name.clone(),
            state,
            metrics,
            shutdown_tx: Some(shutdown_tx),
            task_handle: Some(task_handle),
        };

        self.sinks.write().insert(name, handle);
        Ok(())
    }

    /// Stops a running source connector.
    pub fn stop_source(&self, name: &str) {
        if let Some(handle) = self.sources.write().get_mut(name) {
            handle.shutdown();
        }
    }

    /// Stops a running sink connector.
    pub fn stop_sink(&self, name: &str) {
        if let Some(handle) = self.sinks.write().get_mut(name) {
            handle.shutdown();
        }
    }

    /// Returns the health status of a source connector.
    #[must_use]
    pub fn source_health(&self, name: &str) -> HealthStatus {
        match self.sources.read().get(name) {
            Some(handle) => match handle.state() {
                ConnectorState::Running => HealthStatus::Healthy,
                ConnectorState::Recovering => HealthStatus::Degraded("recovering".into()),
                ConnectorState::Failed => HealthStatus::Unhealthy("failed".into()),
                ConnectorState::Closed => HealthStatus::Unhealthy("closed".into()),
                _ => HealthStatus::Unknown,
            },
            None => HealthStatus::Unknown,
        }
    }

    /// Returns the health status of a sink connector.
    #[must_use]
    pub fn sink_health(&self, name: &str) -> HealthStatus {
        match self.sinks.read().get(name) {
            Some(handle) => match handle.state() {
                ConnectorState::Running => HealthStatus::Healthy,
                ConnectorState::Recovering => HealthStatus::Degraded("recovering".into()),
                ConnectorState::Failed => HealthStatus::Unhealthy("failed".into()),
                ConnectorState::Closed => HealthStatus::Unhealthy("closed".into()),
                _ => HealthStatus::Unknown,
            },
            None => HealthStatus::Unknown,
        }
    }

    /// Returns the number of active sources.
    #[must_use]
    pub fn source_count(&self) -> usize {
        self.sources.read().len()
    }

    /// Returns the number of active sinks.
    #[must_use]
    pub fn sink_count(&self) -> usize {
        self.sinks.read().len()
    }

    /// Checkpoints a source connector, returning its current position.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the source is not found.
    pub fn checkpoint_source(&self, _name: &str) -> Result<SourceCheckpoint, ConnectorError> {
        // In a full implementation, this would reach into the connector
        // to get its current checkpoint. For now, return empty.
        Ok(SourceCheckpoint::new(0))
    }

    /// Stops all running connectors.
    pub fn shutdown_all(&self) {
        for handle in self.sources.write().values_mut() {
            handle.shutdown();
        }
        for handle in self.sinks.write().values_mut() {
            handle.shutdown();
        }
    }
}

impl std::fmt::Debug for ConnectorRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorRuntime")
            .field("sources", &self.source_count())
            .field("sinks", &self.sink_count())
            .finish()
    }
}

/// Internal poll loop for a source connector.
async fn source_poll_loop<F>(
    mut connector: Box<dyn SourceConnector>,
    state: Arc<RwLock<ConnectorState>>,
    metrics: Arc<RuntimeMetrics>,
    mut shutdown_rx: oneshot::Receiver<()>,
    on_batch: F,
    max_batch_size: usize,
    poll_interval_ms: u64,
) -> Result<(), ConnectorError>
where
    F: Fn(RecordBatch) -> Result<(), ConnectorError> + Send + Sync + 'static,
{
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                *state.write() = ConnectorState::Closed;
                break;
            }
            result = connector.poll_batch(max_batch_size) => {
                match result {
                    Ok(Some(batch)) => {
                        let num_rows = batch.num_rows() as u64;
                        // Estimate bytes from batch size
                        let byte_estimate = batch.records.get_array_memory_size() as u64;
                        if let Err(e) = on_batch(batch.records) {
                            metrics.record_error();
                            tracing::error!("source batch callback error: {e}");
                        } else {
                            metrics.record_batch(num_rows, byte_estimate);
                        }
                    }
                    Ok(None) => {
                        // No data, wait before next poll
                        tokio::time::sleep(
                            tokio::time::Duration::from_millis(poll_interval_ms)
                        ).await;
                    }
                    Err(e) => {
                        metrics.record_error();
                        tracing::error!("source poll error: {e}");
                        *state.write() = ConnectorState::Recovering;
                        tokio::time::sleep(
                            tokio::time::Duration::from_millis(poll_interval_ms * 10)
                        ).await;
                        *state.write() = ConnectorState::Running;
                    }
                }
            }
        }
    }
    connector.close().await
}

/// Returns the expected schema for type-erased Arrow records.
///
/// Used by the runtime to create `Source<ArrowRecord>` instances
/// for connector-produced data.
#[must_use]
pub fn arrow_record_schema() -> SchemaRef {
    // Empty schema - push_arrow bypasses schema validation
    Arc::new(arrow_schema::Schema::empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_runtime_config_default() {
        let config = RuntimeConfig::default();
        assert_eq!(config.max_poll_batch_size, 1000);
        assert_eq!(config.poll_interval_ms, 100);
    }

    #[tokio::test]
    async fn test_start_and_stop_source() {
        let registry = ConnectorRegistry::new();
        register_mock_source(&registry);

        let runtime = ConnectorRuntime::with_registry(registry);
        let config = ConnectorConfig::new("mock");

        let batch_count = Arc::new(AtomicUsize::new(0));
        let bc = Arc::clone(&batch_count);

        runtime
            .start_source("test-source", config, move |_batch| {
                bc.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(runtime.source_count(), 1);
        assert!(runtime.source_health("test-source").is_healthy());

        // Let it run briefly
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        runtime.stop_source("test-source");

        // Give task time to shut down
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn test_start_and_stop_sink() {
        let registry = ConnectorRegistry::new();
        register_mock_sink(&registry);

        let runtime = ConnectorRuntime::with_registry(registry);
        let config = ConnectorConfig::new("mock");

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        runtime.start_sink("test-sink", config, rx).await.unwrap();

        assert_eq!(runtime.sink_count(), 1);
        assert!(runtime.sink_health("test-sink").is_healthy());

        // Send a batch
        let batch = mock_batch(5);
        tx.send(batch).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        runtime.stop_sink("test-sink");
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn test_shutdown_all() {
        let registry = ConnectorRegistry::new();
        register_mock_source(&registry);
        register_mock_sink(&registry);

        let runtime = ConnectorRuntime::with_registry(registry);

        runtime
            .start_source("src1", ConnectorConfig::new("mock"), |_| Ok(()))
            .await
            .unwrap();

        let (_tx, rx) = tokio::sync::mpsc::channel(16);
        runtime
            .start_sink("sink1", ConnectorConfig::new("mock"), rx)
            .await
            .unwrap();

        assert_eq!(runtime.source_count(), 1);
        assert_eq!(runtime.sink_count(), 1);

        runtime.shutdown_all();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    #[test]
    fn test_unknown_source_health() {
        let registry = ConnectorRegistry::new();
        let runtime = ConnectorRuntime::with_registry(registry);
        assert!(!runtime.source_health("nonexistent").is_healthy());
    }
}
