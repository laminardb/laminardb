//! DAG source bridge — connects a [`SourceConnector`] to a DAG source node.
//!
//! [`DagSourceBridge`] polls batches from an external connector, wraps them
//! as [`Event`]s with extracted timestamps, and feeds them into the
//! [`DagExecutor`] at the designated source node.

use std::sync::atomic::Ordering;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::Array;
use arrow_schema::SchemaRef;

use laminar_core::dag::{DagExecutor, NodeId};
use laminar_core::operator::Event;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::SourceConnector;
use crate::error::ConnectorError;

use super::metrics::SourceBridgeMetrics;

/// Result of a single poll-and-process cycle.
#[derive(Debug, Clone)]
pub struct PollResult {
    /// Number of events (rows) processed in this cycle.
    pub events_processed: usize,
    /// Approximate bytes ingested in this cycle.
    pub bytes_processed: u64,
    /// Partition offsets observed (`partition_id`, offset) pairs.
    pub partition_offsets: Vec<(String, String)>,
}

/// State of a source bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceBridgeState {
    /// Bridge has been created but not yet opened.
    Created,
    /// Bridge is open and ready to poll.
    Open,
    /// Bridge has been closed.
    Closed,
}

/// Bridges a [`SourceConnector`] to a DAG source node.
///
/// Polls the connector, extracts timestamps from the batch data, wraps
/// rows as [`Event`]s, and injects them into the executor at the
/// configured source node.
pub struct DagSourceBridge {
    connector: Box<dyn SourceConnector>,
    node_id: NodeId,
    node_name: String,
    schema: SchemaRef,
    timestamp_column: Option<String>,
    last_checkpoint: SourceCheckpoint,
    metrics: SourceBridgeMetrics,
    state: SourceBridgeState,
}

impl DagSourceBridge {
    /// Creates a new source bridge.
    ///
    /// # Arguments
    ///
    /// * `connector` — The source connector to poll batches from.
    /// * `node_id` — The DAG source node to inject events into.
    /// * `node_name` — Human-readable name for this source.
    /// * `schema` — Expected output schema of the connector.
    #[must_use]
    pub fn new(
        connector: Box<dyn SourceConnector>,
        node_id: NodeId,
        node_name: impl Into<String>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            connector,
            node_id,
            node_name: node_name.into(),
            schema,
            timestamp_column: None,
            last_checkpoint: SourceCheckpoint::default(),
            metrics: SourceBridgeMetrics::new(),
            state: SourceBridgeState::Created,
        }
    }

    /// Sets the column name to extract event timestamps from.
    ///
    /// If not set, processing time (0) is used as the event timestamp.
    /// The column must be of type `Int64`.
    #[must_use]
    pub fn with_timestamp_column(mut self, col: &str) -> Self {
        self.timestamp_column = Some(col.to_string());
        self
    }

    /// Opens the underlying connector.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the connector fails to open.
    pub async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.connector.open(config).await?;
        self.state = SourceBridgeState::Open;
        Ok(())
    }

    /// Polls the connector for a batch and processes it through the executor.
    ///
    /// Returns `Ok(PollResult)` with zero `events_processed` if no data
    /// was available.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if polling fails or if the DAG executor
    /// rejects the event.
    pub async fn poll_and_process(
        &mut self,
        executor: &mut DagExecutor,
        max_records: usize,
    ) -> Result<PollResult, ConnectorError> {
        let batch_opt = self.connector.poll_batch(max_records).await?;

        let Some(batch) = batch_opt else {
            self.metrics
                .poll_empty_count
                .fetch_add(1, Ordering::Relaxed);
            return Ok(PollResult {
                events_processed: 0,
                bytes_processed: 0,
                partition_offsets: Vec::new(),
            });
        };

        let num_rows = batch.num_rows();
        let bytes = batch.records.get_array_memory_size() as u64;

        // Extract timestamp from the designated column, or use 0
        let timestamp = self.extract_timestamp(&batch.records);

        // Collect partition offsets if present
        let partition_offsets = batch
            .partition
            .as_ref()
            .map(|p| vec![(p.id.clone(), p.offset.clone())])
            .unwrap_or_default();

        let event = Event::new(timestamp, batch.records);

        executor
            .process_event(self.node_id, event)
            .map_err(|e| ConnectorError::Internal(format!("DAG executor error: {e}")))?;

        self.metrics.batches_polled.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .events_ingested
            .fetch_add(num_rows as u64, Ordering::Relaxed);
        self.metrics
            .bytes_ingested
            .fetch_add(bytes, Ordering::Relaxed);

        // Save checkpoint from connector
        self.last_checkpoint = self.connector.checkpoint();

        Ok(PollResult {
            events_processed: num_rows,
            bytes_processed: bytes,
            partition_offsets,
        })
    }

    /// Returns the current checkpoint from the underlying connector.
    #[must_use]
    pub fn checkpoint(&self) -> SourceCheckpoint {
        self.connector.checkpoint()
    }

    /// Restores the underlying connector from a checkpoint.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if restore fails.
    pub async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        self.connector.restore(checkpoint).await?;
        self.last_checkpoint = checkpoint.clone();
        Ok(())
    }

    /// Closes the underlying connector.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if close fails.
    pub async fn close(&mut self) -> Result<(), ConnectorError> {
        self.connector.close().await?;
        self.state = SourceBridgeState::Closed;
        Ok(())
    }

    /// Returns the DAG node ID this source is bound to.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the human-readable name of this source bridge.
    #[must_use]
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Returns the bridge metrics.
    #[must_use]
    pub fn metrics(&self) -> &SourceBridgeMetrics {
        &self.metrics
    }

    /// Returns the current bridge state.
    #[must_use]
    pub fn state(&self) -> SourceBridgeState {
        self.state
    }

    /// Returns the expected output schema.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Extracts a timestamp from the first row of the batch using the
    /// configured timestamp column. Falls back to 0 if the column is
    /// missing, empty, or not configured.
    fn extract_timestamp(&self, batch: &arrow_array::RecordBatch) -> i64 {
        let Some(col_name) = &self.timestamp_column else {
            return 0;
        };

        let Some(col_idx) = batch.schema().index_of(col_name).ok() else {
            return 0;
        };

        if batch.num_rows() == 0 {
            return 0;
        }

        let column = batch.column(col_idx);
        // Try Int64 first (most common for event timestamps)
        if let Some(arr) = column.as_primitive_opt::<Int64Type>() {
            if !arr.is_null(0) {
                return arr.value(0);
            }
        }

        0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use laminar_core::dag::builder::DagBuilder;

    use crate::testing::MockSourceConnector;

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn ts_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
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
    async fn test_source_bridge_poll_and_process() {
        let schema = test_schema();
        let (mut executor, src_id, snk_id) = build_dag_and_executor(schema.clone());

        let connector = MockSourceConnector::with_batches(3, 5);
        let mut bridge = DagSourceBridge::new(Box::new(connector), src_id, "test-source", schema);

        bridge.open(&ConnectorConfig::new("mock")).await.unwrap();
        assert_eq!(bridge.state(), SourceBridgeState::Open);

        let result = bridge.poll_and_process(&mut executor, 100).await.unwrap();
        assert_eq!(result.events_processed, 5);
        assert!(result.bytes_processed > 0);

        // Events should arrive at the sink node
        let outputs = executor.take_sink_outputs(snk_id);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].data.num_rows(), 5);

        assert_eq!(bridge.metrics().batches_polled.load(Ordering::Relaxed), 1);
        assert_eq!(bridge.metrics().events_ingested.load(Ordering::Relaxed), 5);

        bridge.close().await.unwrap();
        assert_eq!(bridge.state(), SourceBridgeState::Closed);
    }

    #[tokio::test]
    async fn test_source_bridge_empty_poll() {
        let schema = test_schema();
        let (mut executor, src_id, _snk_id) = build_dag_and_executor(schema.clone());

        // Source with 0 batches → immediately returns None
        let connector = MockSourceConnector::with_batches(0, 5);
        let mut bridge = DagSourceBridge::new(Box::new(connector), src_id, "empty-source", schema);

        bridge.open(&ConnectorConfig::new("mock")).await.unwrap();

        let result = bridge.poll_and_process(&mut executor, 100).await.unwrap();
        assert_eq!(result.events_processed, 0);
        assert_eq!(result.bytes_processed, 0);
        assert!(result.partition_offsets.is_empty());

        assert_eq!(bridge.metrics().poll_empty_count.load(Ordering::Relaxed), 1);
        assert_eq!(bridge.metrics().batches_polled.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_source_bridge_checkpoint_restore() {
        let schema = test_schema();
        let (mut executor, src_id, _snk_id) = build_dag_and_executor(schema.clone());

        let connector = MockSourceConnector::with_batches(5, 3);
        let mut bridge = DagSourceBridge::new(Box::new(connector), src_id, "cp-source", schema);

        bridge.open(&ConnectorConfig::new("mock")).await.unwrap();

        // Poll a couple of batches
        bridge.poll_and_process(&mut executor, 100).await.unwrap();
        bridge.poll_and_process(&mut executor, 100).await.unwrap();

        // Checkpoint
        let cp = bridge.checkpoint();
        assert_eq!(cp.get_offset("records"), Some("6"));

        // Restore (mock just accepts it)
        let restore_cp = SourceCheckpoint::new(1);
        bridge.restore(&restore_cp).await.unwrap();

        bridge.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_source_bridge_timestamp_extraction() {
        let schema = ts_schema();
        let (mut executor, src_id, snk_id) = build_dag_and_executor(schema.clone());

        // Build a source that uses a custom batch with timestamp column
        let connector = TimestampMockSource::new(schema.clone(), 42_000);
        let mut bridge = DagSourceBridge::new(Box::new(connector), src_id, "ts-source", schema)
            .with_timestamp_column("ts");

        bridge.open(&ConnectorConfig::new("mock")).await.unwrap();

        let result = bridge.poll_and_process(&mut executor, 100).await.unwrap();
        assert_eq!(result.events_processed, 1);

        let outputs = executor.take_sink_outputs(snk_id);
        assert_eq!(outputs.len(), 1);
        // The event timestamp should have been extracted from the "ts" column
        assert_eq!(outputs[0].timestamp, 42_000);
    }

    // A helper mock that produces a single batch with a specific timestamp value.
    struct TimestampMockSource {
        schema: SchemaRef,
        ts_value: i64,
        produced: bool,
    }

    impl TimestampMockSource {
        fn new(schema: SchemaRef, ts_value: i64) -> Self {
            Self {
                schema,
                ts_value,
                produced: false,
            }
        }
    }

    #[async_trait::async_trait]
    impl SourceConnector for TimestampMockSource {
        async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<crate::connector::SourceBatch>, ConnectorError> {
            if self.produced {
                return Ok(None);
            }
            self.produced = true;

            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![self.ts_value])),
                    Arc::new(StringArray::from(vec!["hello"])),
                ],
            )
            .unwrap();
            Ok(Some(crate::connector::SourceBatch::new(batch)))
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new(0)
        }

        async fn restore(&mut self, _cp: &SourceCheckpoint) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }
}
