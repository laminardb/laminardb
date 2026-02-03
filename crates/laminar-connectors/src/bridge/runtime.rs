//! Connector bridge runtime — orchestrates sources → DAG → sinks.
//!
//! [`ConnectorBridgeRuntime`] is the top-level coordinator that:
//!
//! 1. Attaches source and sink connectors to a [`StreamingDag`].
//! 2. Runs process cycles: poll sources → execute DAG → flush sinks.
//! 3. Manages checkpoint/recovery with [`DagCheckpointCoordinator`]
//!    and [`DagRecoveryManager`].

use std::sync::atomic::Ordering;
use std::time::Instant;

use laminar_core::dag::recovery::{DagRecoveryManager, RecoveredDagState};
use laminar_core::dag::watermark::DagWatermarkTracker;
use laminar_core::dag::{
    DagCheckpointConfig, DagCheckpointCoordinator, DagExecutor, DagNodeType, NodeId, StreamingDag,
};
use laminar_core::operator::Operator;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{SinkConnector, SourceConnector};
use crate::error::ConnectorError;

use super::config::BridgeRuntimeConfig;
use super::metrics::BridgeRuntimeMetrics;
use super::sink_bridge::DagSinkBridge;
use super::source_bridge::DagSourceBridge;

/// Result of a single process cycle.
#[derive(Debug, Clone)]
pub struct CycleResult {
    /// Total events ingested from all sources in this cycle.
    pub events_ingested: u64,
    /// Total events output to all sinks in this cycle.
    pub events_output: u64,
    /// Epoch of a completed checkpoint, if one finished this cycle.
    pub checkpoint_completed: Option<u64>,
}

/// State of the bridge runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeState {
    /// Runtime has been created but not yet opened.
    Created,
    /// Runtime is running (sources polling, sinks flushing).
    Running,
    /// A checkpoint is in progress for the given epoch.
    CheckpointInProgress {
        /// The epoch being checkpointed.
        epoch: u64,
    },
    /// Runtime has been stopped.
    Stopped,
}

/// Orchestrates source connectors → DAG executor → sink connectors
/// with checkpointing and recovery.
pub struct ConnectorBridgeRuntime {
    executor: DagExecutor,
    dag: StreamingDag,
    sources: Vec<DagSourceBridge>,
    sinks: Vec<DagSinkBridge>,
    coordinator: DagCheckpointCoordinator,
    recovery_manager: DagRecoveryManager,
    watermark_tracker: DagWatermarkTracker,
    config: BridgeRuntimeConfig,
    state: RuntimeState,
    metrics: BridgeRuntimeMetrics,
}

impl ConnectorBridgeRuntime {
    /// Creates a new bridge runtime from a finalized [`StreamingDag`].
    ///
    /// The DAG must already have source and sink nodes defined.
    /// Connectors are attached later via [`attach_source`](Self::attach_source)
    /// and [`attach_sink`](Self::attach_sink).
    #[must_use]
    pub fn new(dag: StreamingDag, config: BridgeRuntimeConfig) -> Self {
        let executor = DagExecutor::from_dag(&dag);
        let watermark_tracker = DagWatermarkTracker::from_dag(&dag);

        let all_nodes: Vec<NodeId> = dag.execution_order().to_vec();
        let source_nodes: Vec<NodeId> = dag.sources().to_vec();

        let coordinator =
            DagCheckpointCoordinator::new(source_nodes, all_nodes, DagCheckpointConfig::default());
        let recovery_manager = DagRecoveryManager::new();

        Self {
            executor,
            dag,
            sources: Vec::new(),
            sinks: Vec::new(),
            coordinator,
            recovery_manager,
            watermark_tracker,
            config,
            state: RuntimeState::Created,
            metrics: BridgeRuntimeMetrics::new(),
        }
    }

    /// Attaches a source connector to a DAG source node by name.
    ///
    /// The node must exist in the DAG and be of type `Source`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the node is not
    /// found or is not a source node.
    pub fn attach_source(
        &mut self,
        name: &str,
        connector: Box<dyn SourceConnector>,
        _config: &ConnectorConfig,
    ) -> Result<NodeId, ConnectorError> {
        let node_id = self.dag.node_id_by_name(name).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("source node '{name}' not found in DAG"))
        })?;

        let node_type = self.dag.node(node_id).map(|n| n.node_type).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("node '{name}' not found in DAG"))
        })?;

        if node_type != DagNodeType::Source {
            return Err(ConnectorError::ConfigurationError(format!(
                "node '{name}' is {node_type:?}, not Source"
            )));
        }

        let schema = connector.schema();
        let bridge = DagSourceBridge::new(connector, node_id, name, schema);
        self.sources.push(bridge);
        Ok(node_id)
    }

    /// Attaches a sink connector to a DAG sink node by name.
    ///
    /// The node must exist in the DAG and be of type `Sink`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the node is not
    /// found or is not a sink node.
    pub fn attach_sink(
        &mut self,
        name: &str,
        connector: Box<dyn SinkConnector>,
        _config: &ConnectorConfig,
    ) -> Result<NodeId, ConnectorError> {
        let node_id = self.dag.node_id_by_name(name).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("sink node '{name}' not found in DAG"))
        })?;

        let node_type = self.dag.node(node_id).map(|n| n.node_type).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("node '{name}' not found in DAG"))
        })?;

        if node_type != DagNodeType::Sink {
            return Err(ConnectorError::ConfigurationError(format!(
                "node '{name}' is {node_type:?}, not Sink"
            )));
        }

        let schema = connector.schema();
        let bridge = DagSinkBridge::new(connector, node_id, name, schema);
        self.sinks.push(bridge);
        Ok(node_id)
    }

    /// Registers an operator for a DAG node.
    ///
    /// This delegates directly to the underlying [`DagExecutor`].
    pub fn register_operator(&mut self, node: NodeId, operator: Box<dyn Operator>) {
        self.executor.register_operator(node, operator);
    }

    /// Opens all attached source and sink connectors.
    ///
    /// # Errors
    ///
    /// Returns the first `ConnectorError` encountered during open.
    pub async fn open(&mut self) -> Result<(), ConnectorError> {
        let config = ConnectorConfig::default();
        for source in &mut self.sources {
            source.open(&config).await?;
        }
        for sink in &mut self.sinks {
            sink.open(&config).await?;
        }
        self.state = RuntimeState::Running;
        Ok(())
    }

    /// Runs a single process cycle: poll all sources → execute DAG →
    /// flush all sinks.
    ///
    /// # Errors
    ///
    /// Returns the first `ConnectorError` from source polling or sink
    /// flushing.
    pub async fn process_cycle(
        &mut self,
        max_records: usize,
    ) -> Result<CycleResult, ConnectorError> {
        let mut total_in = 0u64;
        let mut total_out = 0u64;

        // 1. Poll all sources
        for source in &mut self.sources {
            let result = source
                .poll_and_process(&mut self.executor, max_records)
                .await?;
            total_in += result.events_processed as u64;
        }

        // 2. Flush all sinks
        for sink in &mut self.sinks {
            let result = sink.flush_outputs(&mut self.executor).await?;
            total_out += result.events_written as u64;
        }

        // 3. Update runtime metrics
        self.metrics
            .cycles_completed
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .total_events_in
            .fetch_add(total_in, Ordering::Relaxed);
        self.metrics
            .total_events_out
            .fetch_add(total_out, Ordering::Relaxed);

        Ok(CycleResult {
            events_ingested: total_in,
            events_output: total_out,
            checkpoint_completed: None,
        })
    }

    /// Triggers a checkpoint: captures operator states, source offsets,
    /// and commits sink epochs.
    ///
    /// Returns the completed epoch number.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if any part of the checkpoint fails.
    pub async fn trigger_checkpoint(&mut self) -> Result<u64, ConnectorError> {
        let start = Instant::now();

        // 1. Trigger barrier from coordinator
        let barrier = self.coordinator.trigger_checkpoint().map_err(|e| {
            ConnectorError::CheckpointError(format!("failed to trigger checkpoint: {e}"))
        })?;

        let epoch = barrier.epoch;
        self.state = RuntimeState::CheckpointInProgress { epoch };

        // 2. Capture operator states
        let operator_states = self.executor.process_checkpoint_barrier(&barrier);

        // 3. Feed operator states to coordinator.
        //    process_checkpoint_barrier only returns states for nodes with
        //    registered operators. Source/sink nodes have no operator, so
        //    we must report empty states for them as well.
        for (node_id, state) in &operator_states {
            self.coordinator
                .on_node_snapshot_complete(*node_id, state.clone());
        }

        // Report empty states for source nodes (connector state is
        // captured separately via SourceCheckpoint).
        for source in &self.sources {
            if !operator_states.contains_key(&source.node_id()) {
                self.coordinator.on_node_snapshot_complete(
                    source.node_id(),
                    laminar_core::operator::OperatorState {
                        operator_id: source.node_name().to_string(),
                        data: Vec::new(),
                    },
                );
            }
        }

        // Report empty states for sink nodes.
        for sink in &self.sinks {
            if !operator_states.contains_key(&sink.node_id()) {
                self.coordinator.on_node_snapshot_complete(
                    sink.node_id(),
                    laminar_core::operator::OperatorState {
                        operator_id: sink.node_name().to_string(),
                        data: Vec::new(),
                    },
                );
            }
        }

        // 4. Finalize the checkpoint snapshot
        let snapshot = self.coordinator.finalize_checkpoint().map_err(|e| {
            ConnectorError::CheckpointError(format!("failed to finalize checkpoint: {e}"))
        })?;

        // 5. Store snapshot in recovery manager
        self.recovery_manager.add_snapshot(snapshot);

        // 6. Commit sink epochs (exactly-once)
        if self.config.enable_exactly_once {
            for sink in &mut self.sinks {
                if sink.is_exactly_once() {
                    sink.commit_epoch(epoch).await?;
                }
            }
        }

        // 7. Update metrics
        #[allow(clippy::cast_possible_truncation)] // u64 μs won't overflow for ~584K years
        let elapsed_us = start.elapsed().as_micros() as u64;
        self.metrics
            .checkpoints_completed
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .last_checkpoint_duration_us
            .store(elapsed_us, Ordering::Relaxed);

        self.state = RuntimeState::Running;
        Ok(epoch)
    }

    /// Recovers from the latest checkpoint.
    ///
    /// Restores operator states in the executor and source offsets in
    /// the connectors.
    ///
    /// Returns `Some(epoch)` if recovery succeeded, `None` if no
    /// snapshots are available.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if restoring any source connector fails.
    pub async fn recover(&mut self) -> Result<Option<u64>, ConnectorError> {
        if !self.recovery_manager.has_snapshots() {
            return Ok(None);
        }

        let recovered: RecoveredDagState = self
            .recovery_manager
            .recover_latest()
            .map_err(|e| ConnectorError::CheckpointError(format!("recovery failed: {e}")))?;

        let epoch = recovered.snapshot.epoch;

        // 1. Restore executor operator states
        self.executor
            .restore(&recovered.operator_states)
            .map_err(|e| {
                ConnectorError::CheckpointError(format!("executor restore failed: {e}"))
            })?;

        // 2. Restore source connectors from saved checkpoint offsets
        // Build a SourceCheckpoint from the recovered source_offsets
        let mut source_cp = SourceCheckpoint::new(epoch);
        for (key, value) in &recovered.source_offsets {
            source_cp.set_offset(key.clone(), value.to_string());
        }
        for source in &mut self.sources {
            source.restore(&source_cp).await?;
        }

        // 3. Rollback sink epochs if exactly-once
        if self.config.enable_exactly_once {
            for sink in &mut self.sinks {
                if sink.is_exactly_once() {
                    sink.rollback_epoch(epoch).await?;
                }
            }
        }

        // 4. Restore watermarks
        if let Some(wm) = recovered.watermark {
            for source in &self.sources {
                self.watermark_tracker
                    .update_watermark(source.node_id(), wm);
            }
        }

        self.metrics.recoveries.fetch_add(1, Ordering::Relaxed);
        Ok(Some(epoch))
    }

    /// Closes all source and sink connectors and stops the runtime.
    ///
    /// # Errors
    ///
    /// Returns the first `ConnectorError` encountered during close.
    pub async fn close(&mut self) -> Result<(), ConnectorError> {
        for source in &mut self.sources {
            source.close().await?;
        }
        for sink in &mut self.sinks {
            sink.close().await?;
        }
        self.state = RuntimeState::Stopped;
        Ok(())
    }

    /// Returns the current runtime state.
    #[must_use]
    pub fn state(&self) -> &RuntimeState {
        &self.state
    }

    /// Returns the runtime metrics.
    #[must_use]
    pub fn metrics(&self) -> &BridgeRuntimeMetrics {
        &self.metrics
    }

    /// Returns the number of attached sources.
    #[must_use]
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    /// Returns the number of attached sinks.
    #[must_use]
    pub fn sink_count(&self) -> usize {
        self.sinks.len()
    }

    /// Returns a reference to the underlying DAG executor.
    #[must_use]
    pub fn executor(&self) -> &DagExecutor {
        &self.executor
    }

    /// Returns a reference to the watermark tracker.
    #[must_use]
    pub fn watermark_tracker(&self) -> &DagWatermarkTracker {
        &self.watermark_tracker
    }

    /// Returns a reference to the recovery manager.
    #[must_use]
    pub fn recovery_manager(&self) -> &DagRecoveryManager {
        &self.recovery_manager
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use laminar_core::dag::builder::DagBuilder;

    use crate::config::ConnectorConfig;
    use crate::testing::{MockSinkConnector, MockSourceConnector};

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn build_simple_dag(schema: SchemaRef) -> StreamingDag {
        DagBuilder::new()
            .source("src", schema.clone())
            .sink_for("src", "snk", schema)
            .build()
            .unwrap()
    }

    fn build_multi_dag(schema: SchemaRef) -> StreamingDag {
        DagBuilder::new()
            .source("src1", schema.clone())
            .source("src2", schema.clone())
            .sink("snk1", schema.clone())
            .sink("snk2", schema.clone())
            .connect("src1", "snk1")
            .connect("src2", "snk2")
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_runtime_end_to_end() {
        let schema = test_schema();
        let dag = build_simple_dag(schema.clone());

        let config = BridgeRuntimeConfig::default();
        let mut runtime = ConnectorBridgeRuntime::new(dag, config);

        let conn_config = ConnectorConfig::new("mock");
        let source = MockSourceConnector::with_batches(3, 5);
        let sink = MockSinkConnector::new();

        runtime
            .attach_source("src", Box::new(source), &conn_config)
            .unwrap();
        runtime
            .attach_sink("snk", Box::new(sink), &conn_config)
            .unwrap();

        assert_eq!(runtime.source_count(), 1);
        assert_eq!(runtime.sink_count(), 1);

        runtime.open().await.unwrap();
        assert_eq!(*runtime.state(), RuntimeState::Running);

        // Run 3 cycles — source has 3 batches of 5 rows
        let mut total_in = 0u64;
        let mut total_out = 0u64;
        for _ in 0..3 {
            let result = runtime.process_cycle(100).await.unwrap();
            total_in += result.events_ingested;
            total_out += result.events_output;
        }

        assert_eq!(total_in, 15);
        assert_eq!(total_out, 15);

        assert_eq!(
            runtime.metrics().cycles_completed.load(Ordering::Relaxed),
            3
        );
        assert_eq!(
            runtime.metrics().total_events_in.load(Ordering::Relaxed),
            15
        );
        assert_eq!(
            runtime.metrics().total_events_out.load(Ordering::Relaxed),
            15
        );

        runtime.close().await.unwrap();
        assert_eq!(*runtime.state(), RuntimeState::Stopped);
    }

    #[tokio::test]
    async fn test_runtime_checkpoint_saves_offsets() {
        let schema = test_schema();
        let dag = build_simple_dag(schema.clone());

        let config = BridgeRuntimeConfig {
            enable_exactly_once: true,
            ..Default::default()
        };
        let mut runtime = ConnectorBridgeRuntime::new(dag, config);

        let conn_config = ConnectorConfig::new("mock");
        let source = MockSourceConnector::with_batches(5, 3);
        let sink = MockSinkConnector::new();

        runtime
            .attach_source("src", Box::new(source), &conn_config)
            .unwrap();
        runtime
            .attach_sink("snk", Box::new(sink), &conn_config)
            .unwrap();

        runtime.open().await.unwrap();

        // Process a couple of cycles
        runtime.process_cycle(100).await.unwrap();
        runtime.process_cycle(100).await.unwrap();

        // Trigger checkpoint
        let epoch = runtime.trigger_checkpoint().await.unwrap();
        assert!(epoch > 0);
        assert_eq!(*runtime.state(), RuntimeState::Running);
        assert_eq!(
            runtime
                .metrics()
                .checkpoints_completed
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            runtime
                .metrics()
                .last_checkpoint_duration_us
                .load(Ordering::Relaxed)
                < 1_000_000 // should be < 1 second
        );

        // Recovery manager should have a snapshot
        assert!(runtime.recovery_manager().has_snapshots());
    }

    #[tokio::test]
    async fn test_runtime_recovery() {
        let schema = test_schema();
        let dag = build_simple_dag(schema.clone());

        let config = BridgeRuntimeConfig {
            enable_exactly_once: true,
            ..Default::default()
        };
        let mut runtime = ConnectorBridgeRuntime::new(dag, config);

        let conn_config = ConnectorConfig::new("mock");
        let source = MockSourceConnector::with_batches(10, 3);
        let sink = MockSinkConnector::new();

        runtime
            .attach_source("src", Box::new(source), &conn_config)
            .unwrap();
        runtime
            .attach_sink("snk", Box::new(sink), &conn_config)
            .unwrap();

        runtime.open().await.unwrap();

        // Process and checkpoint
        runtime.process_cycle(100).await.unwrap();
        let epoch = runtime.trigger_checkpoint().await.unwrap();

        // Recover
        let recovered_epoch = runtime.recover().await.unwrap();
        assert_eq!(recovered_epoch, Some(epoch));
        assert_eq!(runtime.metrics().recoveries.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_runtime_recovery_no_snapshots() {
        let schema = test_schema();
        let dag = build_simple_dag(schema.clone());

        let config = BridgeRuntimeConfig::default();
        let mut runtime = ConnectorBridgeRuntime::new(dag, config);

        // No checkpoints taken → recovery returns None
        let result = runtime.recover().await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_runtime_multiple_sources_sinks() {
        let schema = test_schema();
        let dag = build_multi_dag(schema.clone());

        let config = BridgeRuntimeConfig::default();
        let mut runtime = ConnectorBridgeRuntime::new(dag, config);

        let conn_config = ConnectorConfig::new("mock");
        let source1 = MockSourceConnector::with_batches(2, 4);
        let source2 = MockSourceConnector::with_batches(2, 6);
        let sink1 = MockSinkConnector::new();
        let sink2 = MockSinkConnector::new();

        runtime
            .attach_source("src1", Box::new(source1), &conn_config)
            .unwrap();
        runtime
            .attach_source("src2", Box::new(source2), &conn_config)
            .unwrap();
        runtime
            .attach_sink("snk1", Box::new(sink1), &conn_config)
            .unwrap();
        runtime
            .attach_sink("snk2", Box::new(sink2), &conn_config)
            .unwrap();

        assert_eq!(runtime.source_count(), 2);
        assert_eq!(runtime.sink_count(), 2);

        runtime.open().await.unwrap();

        // Run 2 cycles
        let r1 = runtime.process_cycle(100).await.unwrap();
        // source1 produces 4, source2 produces 6 → total 10 in
        assert_eq!(r1.events_ingested, 10);
        assert_eq!(r1.events_output, 10);

        let r2 = runtime.process_cycle(100).await.unwrap();
        assert_eq!(r2.events_ingested, 10);
        assert_eq!(r2.events_output, 10);

        // 3rd cycle: sources are exhausted
        let r3 = runtime.process_cycle(100).await.unwrap();
        assert_eq!(r3.events_ingested, 0);
        assert_eq!(r3.events_output, 0);

        runtime.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_runtime_state_transitions() {
        let schema = test_schema();
        let dag = build_simple_dag(schema.clone());

        let config = BridgeRuntimeConfig::default();
        let mut runtime = ConnectorBridgeRuntime::new(dag, config);
        assert_eq!(*runtime.state(), RuntimeState::Created);

        let conn_config = ConnectorConfig::new("mock");
        runtime
            .attach_source(
                "src",
                Box::new(MockSourceConnector::with_batches(5, 3)),
                &conn_config,
            )
            .unwrap();
        runtime
            .attach_sink("snk", Box::new(MockSinkConnector::new()), &conn_config)
            .unwrap();

        runtime.open().await.unwrap();
        assert_eq!(*runtime.state(), RuntimeState::Running);

        runtime.process_cycle(100).await.unwrap();
        assert_eq!(*runtime.state(), RuntimeState::Running);

        // Checkpoint transitions to CheckpointInProgress then back to Running
        runtime.trigger_checkpoint().await.unwrap();
        assert_eq!(*runtime.state(), RuntimeState::Running);

        runtime.close().await.unwrap();
        assert_eq!(*runtime.state(), RuntimeState::Stopped);
    }

    #[tokio::test]
    async fn test_attach_wrong_node_type() {
        let schema = test_schema();
        let dag = build_simple_dag(schema.clone());

        let config = BridgeRuntimeConfig::default();
        let mut runtime = ConnectorBridgeRuntime::new(dag, config);
        let conn_config = ConnectorConfig::new("mock");

        // Trying to attach a source to a sink node
        let result =
            runtime.attach_source("snk", Box::new(MockSourceConnector::new()), &conn_config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not Source"));

        // Trying to attach a sink to a source node
        let result = runtime.attach_sink("src", Box::new(MockSinkConnector::new()), &conn_config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not Sink"));
    }

    #[tokio::test]
    async fn test_attach_nonexistent_node() {
        let schema = test_schema();
        let dag = build_simple_dag(schema.clone());

        let config = BridgeRuntimeConfig::default();
        let mut runtime = ConnectorBridgeRuntime::new(dag, config);
        let conn_config = ConnectorConfig::new("mock");

        let result = runtime.attach_source(
            "nonexistent",
            Box::new(MockSourceConnector::new()),
            &conn_config,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }
}
