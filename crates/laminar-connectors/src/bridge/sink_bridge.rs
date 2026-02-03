//! DAG sink bridge — connects a DAG sink node to a [`SinkConnector`].
//!
//! [`DagSinkBridge`] drains output events from the [`DagExecutor`] at
//! a designated sink node and writes them to an external connector,
//! with optional exactly-once epoch lifecycle management.

use std::sync::atomic::Ordering;

use arrow_schema::SchemaRef;

use laminar_core::dag::{DagExecutor, NodeId};

use crate::config::ConnectorConfig;
use crate::connector::{SinkConnector, SinkConnectorCapabilities};
use crate::error::ConnectorError;

use super::metrics::SinkBridgeMetrics;

/// Result of flushing sink outputs.
#[derive(Debug, Clone)]
pub struct FlushResult {
    /// Number of events (rows) written in this flush.
    pub events_written: usize,
    /// Approximate bytes written in this flush.
    pub bytes_written: u64,
    /// Number of record batches written.
    pub batches_written: usize,
}

/// State of a sink bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkBridgeState {
    /// Bridge has been created but not yet opened.
    Created,
    /// Bridge is open and ready to write.
    Open,
    /// An epoch is currently active (exactly-once mode).
    EpochActive(u64),
    /// Bridge has been closed.
    Closed,
}

/// Bridges a DAG sink node to a [`SinkConnector`].
///
/// Drains output events produced by the DAG executor and writes them
/// to the external sink. Supports exactly-once semantics via epoch
/// begin/commit/rollback when the sink advertises the capability.
pub struct DagSinkBridge {
    connector: Box<dyn SinkConnector>,
    node_id: NodeId,
    node_name: String,
    output_schema: SchemaRef,
    current_epoch: u64,
    exactly_once: bool,
    metrics: SinkBridgeMetrics,
    state: SinkBridgeState,
}

impl DagSinkBridge {
    /// Creates a new sink bridge.
    ///
    /// # Arguments
    ///
    /// * `connector` — The sink connector to write batches to.
    /// * `node_id` — The DAG sink node to drain outputs from.
    /// * `node_name` — Human-readable name for this sink.
    /// * `output_schema` — Expected schema of output batches.
    #[must_use]
    pub fn new(
        connector: Box<dyn SinkConnector>,
        node_id: NodeId,
        node_name: impl Into<String>,
        output_schema: SchemaRef,
    ) -> Self {
        let exactly_once = connector.capabilities().exactly_once;
        Self {
            connector,
            node_id,
            node_name: node_name.into(),
            output_schema,
            current_epoch: 0,
            exactly_once,
            metrics: SinkBridgeMetrics::new(),
            state: SinkBridgeState::Created,
        }
    }

    /// Opens the underlying connector.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the connector fails to open.
    pub async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.connector.open(config).await?;
        self.state = SinkBridgeState::Open;
        Ok(())
    }

    /// Drains all pending outputs from the executor's sink node and
    /// writes them to the connector.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if any write fails.
    pub async fn flush_outputs(
        &mut self,
        executor: &mut DagExecutor,
    ) -> Result<FlushResult, ConnectorError> {
        let events = executor.take_sink_outputs(self.node_id);

        if events.is_empty() {
            return Ok(FlushResult {
                events_written: 0,
                bytes_written: 0,
                batches_written: 0,
            });
        }

        let mut total_rows = 0usize;
        let mut total_bytes = 0u64;
        let batch_count = events.len();

        for event in &events {
            let result = self
                .connector
                .write_batch(&event.data)
                .await
                .inspect_err(|_| {
                    self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                })?;
            total_rows += result.records_written;
            total_bytes += result.bytes_written;
        }

        self.metrics
            .batches_written
            .fetch_add(batch_count as u64, Ordering::Relaxed);
        self.metrics
            .events_output
            .fetch_add(total_rows as u64, Ordering::Relaxed);
        self.metrics
            .bytes_output
            .fetch_add(total_bytes, Ordering::Relaxed);

        Ok(FlushResult {
            events_written: total_rows,
            bytes_written: total_bytes,
            batches_written: batch_count,
        })
    }

    /// Begins an epoch for exactly-once sinks.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the connector rejects the epoch.
    pub async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if self.exactly_once {
            self.connector.begin_epoch(epoch).await?;
        }
        self.current_epoch = epoch;
        self.state = SinkBridgeState::EpochActive(epoch);
        Ok(())
    }

    /// Commits the current epoch for exactly-once sinks.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the commit fails.
    pub async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if self.exactly_once {
            self.connector.commit_epoch(epoch).await?;
        }
        self.metrics
            .epochs_committed
            .fetch_add(1, Ordering::Relaxed);
        self.state = SinkBridgeState::Open;
        Ok(())
    }

    /// Rolls back the current epoch for exactly-once sinks.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the rollback fails.
    pub async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if self.exactly_once {
            self.connector.rollback_epoch(epoch).await?;
        }
        self.metrics
            .epochs_rolled_back
            .fetch_add(1, Ordering::Relaxed);
        self.state = SinkBridgeState::Open;
        Ok(())
    }

    /// Closes the underlying connector.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if close fails.
    pub async fn close(&mut self) -> Result<(), ConnectorError> {
        self.connector.close().await?;
        self.state = SinkBridgeState::Closed;
        Ok(())
    }

    /// Returns the DAG node ID this sink is bound to.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the human-readable name of this sink bridge.
    #[must_use]
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Returns the bridge metrics.
    #[must_use]
    pub fn metrics(&self) -> &SinkBridgeMetrics {
        &self.metrics
    }

    /// Returns the sink connector's capabilities.
    #[must_use]
    pub fn capabilities(&self) -> SinkConnectorCapabilities {
        self.connector.capabilities()
    }

    /// Returns the current bridge state.
    #[must_use]
    pub fn state(&self) -> SinkBridgeState {
        self.state
    }

    /// Returns the expected output schema.
    #[must_use]
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Returns the current epoch number.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Returns whether this sink uses exactly-once semantics.
    #[must_use]
    pub fn is_exactly_once(&self) -> bool {
        self.exactly_once
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use laminar_core::dag::builder::DagBuilder;
    use laminar_core::operator::Event;

    use crate::testing::MockSinkConnector;

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn test_batch(n: usize) -> RecordBatch {
        #[allow(clippy::cast_possible_wrap)]
        let ids: Vec<i64> = (0..n as i64).collect();
        let values: Vec<String> = (0..n).map(|i| format!("v{i}")).collect();
        let value_refs: Vec<&str> = values.iter().map(String::as_str).collect();
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(value_refs)),
            ],
        )
        .unwrap()
    }

    fn build_dag_and_executor(schema: SchemaRef) -> (DagExecutor, NodeId, NodeId) {
        let dag = DagBuilder::new()
            .source("src", schema.clone())
            .sink_for("src", "snk", schema)
            .build()
            .unwrap();
        let src_id = dag.node_id_by_name("src").unwrap();
        let snk_id = dag.node_id_by_name("snk").unwrap();
        let executor = DagExecutor::from_dag(&dag);
        (executor, src_id, snk_id)
    }

    #[tokio::test]
    async fn test_sink_bridge_flush_outputs() {
        let schema = test_schema();
        let (mut executor, src_id, snk_id) = build_dag_and_executor(schema.clone());

        // Inject an event at the source so it flows to the sink
        let event = Event::new(1000, test_batch(10));
        executor.process_event(src_id, event).unwrap();

        let connector = MockSinkConnector::new();
        let mut bridge = DagSinkBridge::new(Box::new(connector), snk_id, "test-sink", schema);

        bridge.open(&ConnectorConfig::new("mock")).await.unwrap();
        assert_eq!(bridge.state(), SinkBridgeState::Open);

        let result = bridge.flush_outputs(&mut executor).await.unwrap();
        assert_eq!(result.events_written, 10);
        assert!(result.bytes_written > 0);
        assert_eq!(result.batches_written, 1);

        assert_eq!(bridge.metrics().batches_written.load(Ordering::Relaxed), 1);
        assert_eq!(bridge.metrics().events_output.load(Ordering::Relaxed), 10);

        bridge.close().await.unwrap();
        assert_eq!(bridge.state(), SinkBridgeState::Closed);
    }

    #[tokio::test]
    async fn test_sink_bridge_empty_flush() {
        let schema = test_schema();
        let (mut executor, _src_id, snk_id) = build_dag_and_executor(schema.clone());

        let connector = MockSinkConnector::new();
        let mut bridge = DagSinkBridge::new(Box::new(connector), snk_id, "empty-sink", schema);

        bridge.open(&ConnectorConfig::new("mock")).await.unwrap();

        // No events in the executor → flush returns zero
        let result = bridge.flush_outputs(&mut executor).await.unwrap();
        assert_eq!(result.events_written, 0);
        assert_eq!(result.bytes_written, 0);
        assert_eq!(result.batches_written, 0);
    }

    #[tokio::test]
    async fn test_sink_bridge_epoch_lifecycle() {
        let schema = test_schema();
        let (mut executor, src_id, snk_id) = build_dag_and_executor(schema.clone());

        let connector = MockSinkConnector::new();
        let mut bridge = DagSinkBridge::new(Box::new(connector), snk_id, "epoch-sink", schema);

        bridge.open(&ConnectorConfig::new("mock")).await.unwrap();
        assert!(bridge.is_exactly_once());

        // Begin epoch
        bridge.begin_epoch(1).await.unwrap();
        assert_eq!(bridge.state(), SinkBridgeState::EpochActive(1));
        assert_eq!(bridge.current_epoch(), 1);

        // Write some data
        let event = Event::new(2000, test_batch(5));
        executor.process_event(src_id, event).unwrap();
        bridge.flush_outputs(&mut executor).await.unwrap();

        // Commit epoch
        bridge.commit_epoch(1).await.unwrap();
        assert_eq!(bridge.state(), SinkBridgeState::Open);
        assert_eq!(bridge.metrics().epochs_committed.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_sink_bridge_rollback() {
        let schema = test_schema();
        let (_executor, _src_id, snk_id) = build_dag_and_executor(schema.clone());

        let connector = MockSinkConnector::new();
        let mut bridge = DagSinkBridge::new(Box::new(connector), snk_id, "rollback-sink", schema);

        bridge.open(&ConnectorConfig::new("mock")).await.unwrap();

        bridge.begin_epoch(1).await.unwrap();
        bridge.rollback_epoch(1).await.unwrap();

        assert_eq!(bridge.state(), SinkBridgeState::Open);
        assert_eq!(
            bridge.metrics().epochs_rolled_back.load(Ordering::Relaxed),
            1
        );
        assert_eq!(bridge.metrics().epochs_committed.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_sink_bridge_capabilities() {
        let schema = test_schema();
        let (_executor, _src_id, snk_id) = build_dag_and_executor(schema.clone());

        let connector = MockSinkConnector::new();
        let bridge = DagSinkBridge::new(Box::new(connector), snk_id, "cap-sink", schema);

        let caps = bridge.capabilities();
        assert!(caps.exactly_once);
        assert!(caps.idempotent);
    }
}
