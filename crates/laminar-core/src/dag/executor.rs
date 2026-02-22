//! Ring 0 DAG executor for event processing.
//!
//! [`DagExecutor`] processes events through a finalized [`StreamingDag`] in
//! topological order. It uses the pre-computed [`RoutingTable`] for O(1)
//! dispatch and integrates with the [`Operator`] trait for operator invocation.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────┐
//! │                      RING 0: HOT PATH                            │
//! │                                                                  │
//! │  process_event(source, event)                                    │
//! │       │                                                          │
//! │       ▼                                                          │
//! │  ┌──────────┐   topological   ┌───────────┐   route_output()    │
//! │  │  enqueue  │──────order────▶│  operator  │──────────────────┐  │
//! │  │  (input   │                │  .process()│                  │  │
//! │  │   queue)  │                └───────────┘                  │  │
//! │  └──────────┘                                                │  │
//! │       ▲                                                      │  │
//! │       │                  ┌─────────────────┐                 │  │
//! │       └──────────────────│  RoutingTable   │◀────────────────┘  │
//! │         enqueue targets  │  O(1) lookup    │                    │
//! │                          └─────────────────┘                    │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Latency Budget
//!
//! | Component | Budget |
//! |-----------|--------|
//! | Routing table lookup | < 50ns |
//! | Operator dispatch | < 200ns |
//! | Multicast to N consumers | < 100ns |
//! | State access | < 200ns |
//! | **Total** | **< 500ns** |

use std::collections::VecDeque;

use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use crate::alloc::HotPathGuard;
use crate::operator::{
    Event, Operator, OperatorContext, OperatorState, Output, OutputVec, SideOutputData, Timer,
};
use crate::state::InMemoryStore;
use crate::time::{BoundedOutOfOrdernessGenerator, TimerService};

use super::checkpoint::CheckpointBarrier;
use super::error::DagError;
use super::routing::RoutingTable;
use super::topology::{DagNodeType, NodeId, StreamingDag};
use super::watermark::{DagWatermarkCheckpoint, DagWatermarkTracker};

/// Per-node runtime state (timer service, state store, watermark generator).
///
/// Created during executor construction (Ring 2) and used during event
/// processing (Ring 0). Temporarily moved out of the executor during
/// operator dispatch to satisfy Rust's borrow checker.
struct NodeRuntime {
    /// Timer service for this node.
    timer_service: TimerService,
    /// State store for this node.
    state_store: Box<dyn crate::state::StateStore>,
    /// Watermark generator for this node.
    watermark_generator: Box<dyn crate::time::WatermarkGenerator>,
}

impl Default for NodeRuntime {
    fn default() -> Self {
        Self {
            timer_service: TimerService::new(),
            state_store: Box::new(InMemoryStore::new()),
            watermark_generator: Box::new(BoundedOutOfOrdernessGenerator::new(0)),
        }
    }
}

/// Metrics tracked by the DAG executor.
///
/// Counters are updated during event processing and can be read
/// at any time for observability.
#[derive(Debug, Clone, Default)]
pub struct DagExecutorMetrics {
    /// Total events processed through operator dispatch.
    pub events_processed: u64,
    /// Total events routed to downstream nodes.
    pub events_routed: u64,
    /// Total multicast dispatches (fan-out to > 1 target).
    pub multicast_publishes: u64,
    /// Total backpressure stalls encountered.
    pub backpressure_stalls: u64,
    /// Total nodes skipped (empty input queue).
    pub nodes_skipped: u64,
    /// Total watermark advances processed via `process_watermark()`.
    pub watermarks_processed: u64,
}

/// Per-operator metrics tracked by the DAG executor.
///
/// Only populated when the `dag-metrics` feature is enabled.
/// Tracks event counts and cumulative processing time per operator node.
#[cfg(feature = "dag-metrics")]
#[derive(Debug, Clone, Default)]
pub struct OperatorNodeMetrics {
    /// Total events received by this operator.
    pub events_in: u64,
    /// Total events emitted by this operator.
    pub events_out: u64,
    /// Total processing time in nanoseconds.
    pub total_time_ns: u64,
    /// Number of `process()` invocations.
    pub invocations: u64,
}

/// Ring 0 DAG executor for event processing.
///
/// Processes events through a finalized [`StreamingDag`] in topological order
/// using the pre-computed [`RoutingTable`] for O(1) dispatch.
///
/// # Construction
///
/// ```rust,ignore
/// let dag = DagBuilder::new()
///     .source("src", schema.clone())
///     .operator("transform", schema.clone())
///     .connect("src", "transform")
///     .sink_for("transform", "out", schema.clone())
///     .build()?;
///
/// let mut executor = DagExecutor::from_dag(&dag);
/// executor.register_operator(transform_id, Box::new(my_operator));
/// executor.process_event(src_id, event)?;
/// let outputs = executor.take_sink_outputs(out_id);
/// ```
pub struct DagExecutor {
    /// Registered operators, indexed by `NodeId.0`. `None` = passthrough.
    operators: Vec<Option<Box<dyn Operator>>>,
    /// Per-node runtime state (timer, state store, watermark generator).
    runtimes: Vec<Option<NodeRuntime>>,
    /// Pre-allocated input queues per node, indexed by `NodeId.0`.
    input_queues: Vec<VecDeque<Event>>,
    /// Collected sink outputs, indexed by `NodeId.0`.
    sink_outputs: Vec<Vec<Event>>,
    /// Pre-computed routing table for O(1) dispatch.
    routing: RoutingTable,
    /// Topological execution order (from the finalized DAG).
    execution_order: Vec<NodeId>,
    /// Source node IDs.
    source_nodes: Vec<NodeId>,
    /// Sink node IDs.
    sink_nodes: Vec<NodeId>,
    /// Node types, indexed by `NodeId.0`.
    node_types: Vec<DagNodeType>,
    /// Total number of node slots allocated.
    slot_count: usize,
    /// Number of incoming edges per node, indexed by `NodeId.0`.
    input_counts: Vec<usize>,
    /// Temporary buffer for draining input queues (avoids allocation).
    temp_events: Vec<Event>,
    /// Executor metrics.
    metrics: DagExecutorMetrics,
    /// DAG-native watermark tracker for topology-aware propagation.
    watermark_tracker: DagWatermarkTracker,
    /// Per-operator node metrics (feature-gated).
    #[cfg(feature = "dag-metrics")]
    node_metrics: Vec<OperatorNodeMetrics>,
}

impl DagExecutor {
    /// Creates a new executor from a finalized [`StreamingDag`].
    ///
    /// Allocates all per-node state (input queues, runtimes, sink buffers)
    /// up front in Ring 2. The hot path (`process_event`) is allocation-free.
    ///
    /// # Arguments
    ///
    /// * `dag` - A finalized `StreamingDag` topology
    #[must_use]
    pub fn from_dag(dag: &StreamingDag) -> Self {
        let slot_count = dag.nodes().keys().map(|n| n.0).max().map_or(0, |n| n + 1) as usize;

        let routing = RoutingTable::from_dag(dag);

        let mut operators = Vec::with_capacity(slot_count);
        let mut runtimes = Vec::with_capacity(slot_count);
        let mut input_queues = Vec::with_capacity(slot_count);
        let mut sink_outputs = Vec::with_capacity(slot_count);
        let mut node_types = Vec::with_capacity(slot_count);
        let mut input_counts = vec![0usize; slot_count];

        for _ in 0..slot_count {
            operators.push(None);
            runtimes.push(Some(NodeRuntime::default()));
            input_queues.push(VecDeque::with_capacity(16));
            sink_outputs.push(Vec::new());
            node_types.push(DagNodeType::StatelessOperator);
        }

        // Populate node types and input counts from the DAG.
        for node in dag.nodes().values() {
            let idx = node.id.0 as usize;
            if idx < slot_count {
                node_types[idx] = node.node_type;
                input_counts[idx] = dag.incoming_edge_count(node.id);
            }
        }

        let watermark_tracker = DagWatermarkTracker::from_dag(dag);

        Self {
            operators,
            runtimes,
            input_queues,
            sink_outputs,
            routing,
            execution_order: dag.execution_order().to_vec(),
            source_nodes: dag.sources().to_vec(),
            sink_nodes: dag.sinks().to_vec(),
            node_types,
            slot_count,
            input_counts,
            temp_events: Vec::with_capacity(64),
            metrics: DagExecutorMetrics::default(),
            watermark_tracker,
            #[cfg(feature = "dag-metrics")]
            node_metrics: (0..slot_count)
                .map(|_| OperatorNodeMetrics::default())
                .collect(),
        }
    }

    /// Registers an operator for a node.
    ///
    /// Nodes without registered operators act as passthrough: events are
    /// forwarded to downstream nodes unchanged. This is the default for
    /// source and sink nodes.
    ///
    /// # Arguments
    ///
    /// * `node` - The node ID to register the operator for
    /// * `operator` - The operator implementation
    pub fn register_operator(&mut self, node: NodeId, operator: Box<dyn Operator>) {
        let idx = node.0 as usize;
        if idx < self.slot_count {
            self.operators[idx] = Some(operator);
        }
    }

    /// Processes an event from a source node through the entire DAG.
    ///
    /// The event is enqueued at the source node, then all nodes are processed
    /// in topological order. Events produced by operators are routed to
    /// downstream nodes via the [`RoutingTable`]. Sink outputs are collected
    /// and can be retrieved via [`take_sink_outputs()`](Self::take_sink_outputs).
    ///
    /// # Arguments
    ///
    /// * `source_node` - The source node to inject the event
    /// * `event` - The event to process
    ///
    /// # Errors
    ///
    /// Returns [`DagError::NodeNotFound`] if the source node is out of bounds.
    pub fn process_event(&mut self, source_node: NodeId, event: Event) -> Result<(), DagError> {
        let idx = source_node.0 as usize;
        if idx >= self.slot_count {
            return Err(DagError::NodeNotFound(format!("{source_node}")));
        }

        self.input_queues[idx].push_back(event);
        self.process_dag();
        Ok(())
    }

    /// Advances watermark generators for all nodes in topological order.
    ///
    /// Called when an external source provides a watermark (e.g., via
    /// `Source::watermark()`). Propagates the watermark through the DAG
    /// so downstream operators can use it for late-data filtering and
    /// window triggering.
    ///
    /// # Arguments
    ///
    /// * `source_node` - The source node that originated the watermark
    /// * `watermark` - The watermark timestamp in milliseconds
    ///
    /// # Errors
    ///
    /// Returns [`DagError::NodeNotFound`] if `source_node` is out of bounds.
    pub fn process_watermark(
        &mut self,
        source_node: NodeId,
        watermark: i64,
    ) -> Result<(), DagError> {
        let idx = source_node.0 as usize;
        if idx >= self.slot_count {
            return Err(DagError::NodeNotFound(format!("{source_node}")));
        }

        // Use topology-aware propagation: only advance downstream nodes
        let updated: SmallVec<[(NodeId, i64); 8]> = self
            .watermark_tracker
            .update_watermark(source_node, watermark)
            .iter()
            .copied()
            .collect();

        for (node_id, wm) in &updated {
            let nidx = node_id.0 as usize;
            if nidx < self.slot_count {
                if let Some(ref mut rt) = self.runtimes[nidx] {
                    rt.watermark_generator.advance_watermark(*wm);
                }
            }
        }

        // Fire timers for any nodes whose watermark advanced
        self.fire_timers(watermark);

        #[cfg(feature = "dag-metrics")]
        {
            self.metrics.watermarks_processed += 1;
        }
        Ok(())
    }

    /// Fires all expired timers for each node in topological order.
    ///
    /// Polls each node's [`TimerService`] for timers with
    /// `timestamp <= current_time`, calls [`Operator::on_timer()`] for each
    /// fired timer, and routes the resulting outputs downstream.
    ///
    /// This is used to trigger window closures after watermark advancement.
    ///
    /// # Arguments
    ///
    /// * `current_time` - The current event-time (typically the latest watermark)
    pub fn fire_timers(&mut self, current_time: i64) {
        for i in 0..self.execution_order.len() {
            let node_id = self.execution_order[i];
            let idx = node_id.0 as usize;
            if idx >= self.slot_count {
                continue;
            }

            let mut operator = self.operators[idx].take();
            let mut runtime = self.runtimes[idx].take();

            if let (Some(op), Some(rt)) = (&mut operator, &mut runtime) {
                let fired = rt.timer_service.poll_timers(current_time);
                for reg in fired {
                    let timer = Timer {
                        key: reg.key.unwrap_or_default(),
                        timestamp: reg.timestamp,
                    };
                    let mut ctx = OperatorContext {
                        event_time: reg.timestamp,
                        processing_time: current_time,
                        timers: &mut rt.timer_service,
                        state: rt.state_store.as_mut(),
                        watermark_generator: rt.watermark_generator.as_mut(),
                        operator_index: idx,
                    };
                    let outputs = op.on_timer(timer, &mut ctx);
                    self.route_all_outputs(node_id, outputs);
                }
            }

            self.operators[idx] = operator;
            self.runtimes[idx] = runtime;
        }

        // Process any events generated by timer firings through the rest of the DAG.
        self.process_dag();
    }

    /// Takes collected sink outputs for a given sink node.
    ///
    /// Returns all events that reached this sink during prior
    /// `process_event` calls, draining the internal buffer.
    #[must_use]
    pub fn take_sink_outputs(&mut self, sink_node: NodeId) -> Vec<Event> {
        let idx = sink_node.0 as usize;
        if idx < self.slot_count {
            std::mem::take(&mut self.sink_outputs[idx])
        } else {
            Vec::new()
        }
    }

    /// Takes all sink outputs across all sink nodes.
    #[must_use]
    pub fn take_all_sink_outputs(&mut self) -> FxHashMap<NodeId, Vec<Event>> {
        let mut outputs = FxHashMap::default();
        let sink_ids: SmallVec<[NodeId; 8]> = self.sink_nodes.iter().copied().collect();
        for sink_id in sink_ids {
            let events = self.take_sink_outputs(sink_id);
            if !events.is_empty() {
                outputs.insert(sink_id, events);
            }
        }
        outputs
    }

    /// Returns a reference to the executor metrics.
    #[must_use]
    pub fn metrics(&self) -> &DagExecutorMetrics {
        &self.metrics
    }

    /// Returns per-operator node metrics (only available with `dag-metrics` feature).
    #[cfg(feature = "dag-metrics")]
    #[must_use]
    pub fn node_metrics(&self) -> &[OperatorNodeMetrics] {
        &self.node_metrics
    }

    /// Resets all executor metrics to zero.
    pub fn reset_metrics(&mut self) {
        self.metrics = DagExecutorMetrics::default();
        #[cfg(feature = "dag-metrics")]
        {
            for m in &mut self.node_metrics {
                *m = OperatorNodeMetrics::default();
            }
        }
    }

    /// Returns the source node IDs.
    #[must_use]
    pub fn source_nodes(&self) -> &[NodeId] {
        &self.source_nodes
    }

    /// Returns the sink node IDs.
    #[must_use]
    pub fn sink_nodes(&self) -> &[NodeId] {
        &self.sink_nodes
    }

    /// Returns the node type for a given node ID.
    #[must_use]
    pub fn node_type(&self, node: NodeId) -> Option<DagNodeType> {
        let idx = node.0 as usize;
        if idx < self.slot_count {
            Some(self.node_types[idx])
        } else {
            None
        }
    }

    /// Checkpoints all registered operators.
    ///
    /// Returns a map of `NodeId` to `OperatorState` for all nodes
    /// that have registered operators.
    #[must_use]
    pub fn checkpoint(&self) -> FxHashMap<NodeId, OperatorState> {
        let mut states = FxHashMap::default();
        for (idx, op) in self.operators.iter().enumerate() {
            if let Some(operator) = op {
                #[allow(clippy::cast_possible_truncation)]
                // DAG node count bounded by topology (< u32::MAX)
                let node_id = NodeId(idx as u32);
                states.insert(node_id, operator.checkpoint());
            }
        }
        states
    }

    /// Restores operator state from a checkpoint snapshot.
    ///
    /// Iterates the provided states and calls `operator.restore()` on each
    /// registered operator.
    ///
    /// # Errors
    ///
    /// Returns [`DagError::RestoreFailed`] if any operator fails to restore.
    pub fn restore(&mut self, states: &FxHashMap<NodeId, OperatorState>) -> Result<(), DagError> {
        for (node_id, state) in states {
            let idx = node_id.0 as usize;
            if idx < self.slot_count {
                if let Some(ref mut operator) = self.operators[idx] {
                    operator
                        .restore(state.clone())
                        .map_err(|e| DagError::RestoreFailed {
                            node_id: format!("{node_id}"),
                            reason: e.to_string(),
                        })?;
                }
            }
        }
        Ok(())
    }

    /// Checkpoints the watermark tracker state.
    #[must_use]
    pub fn checkpoint_watermarks(&self) -> DagWatermarkCheckpoint {
        self.watermark_tracker.checkpoint()
    }

    /// Restores watermark tracker state from a checkpoint.
    pub fn restore_watermarks(&mut self, checkpoint: &DagWatermarkCheckpoint) {
        self.watermark_tracker.restore(checkpoint);
    }

    /// Injects events into a node's input queue.
    ///
    /// Used during recovery to repopulate queues with buffered events.
    pub fn inject_events(&mut self, node_id: NodeId, events: Vec<Event>) {
        let idx = node_id.0 as usize;
        if idx < self.slot_count {
            self.input_queues[idx].extend(events);
        }
    }

    /// Returns the number of incoming edges for a node.
    #[must_use]
    pub fn input_count(&self, node_id: NodeId) -> usize {
        let idx = node_id.0 as usize;
        if idx < self.slot_count {
            self.input_counts[idx]
        } else {
            0
        }
    }

    /// Snapshots all registered operators in topological order.
    ///
    /// Takes the barrier for consistency (future use with epoch tracking).
    /// In the synchronous single-threaded executor, topological ordering
    /// guarantees upstream-first snapshots.
    #[must_use]
    pub fn process_checkpoint_barrier(
        &mut self,
        _barrier: &CheckpointBarrier,
    ) -> FxHashMap<NodeId, OperatorState> {
        let mut states = FxHashMap::default();
        for &node_id in &self.execution_order {
            let idx = node_id.0 as usize;
            if idx < self.slot_count {
                if let Some(ref operator) = self.operators[idx] {
                    states.insert(node_id, operator.checkpoint());
                }
            }
        }
        states
    }

    /// Processes all nodes in topological order.
    ///
    /// Drains input queues, dispatches to operators, and routes outputs
    /// to downstream nodes. Uses [`HotPathGuard`] for zero-allocation
    /// enforcement in debug builds.
    fn process_dag(&mut self) {
        let _guard = HotPathGuard::enter("dag_executor");

        let order_len = self.execution_order.len();
        for i in 0..order_len {
            let node_id = self.execution_order[i];
            self.process_node(node_id);
        }
    }

    /// Processes a single node: drains its input queue, dispatches each event
    /// to the operator, and routes outputs downstream.
    fn process_node(&mut self, node_id: NodeId) {
        let idx = node_id.0 as usize;

        if self.input_queues[idx].is_empty() {
            #[cfg(feature = "dag-metrics")]
            {
                self.metrics.nodes_skipped += 1;
            }
            return;
        }

        // Swap temp buffer out of self so the borrow checker allows
        // mutable access to other fields during the loop.
        let mut events = std::mem::take(&mut self.temp_events);
        events.clear();
        events.extend(self.input_queues[idx].drain(..));

        // Take operator and runtime out temporarily.
        // This lets us mutably access the rest of `self` for routing.
        let mut operator = self.operators[idx].take();
        let mut runtime = self.runtimes[idx].take();

        for event in events.drain(..) {
            #[cfg(feature = "dag-metrics")]
            {
                self.metrics.events_processed += 1;
                self.node_metrics[idx].events_in += 1;
            }

            #[cfg(feature = "dag-metrics")]
            let start = std::time::Instant::now();

            let outputs = if let Some(op) = &mut operator {
                if let Some(rt) = &mut runtime {
                    let mut ctx = OperatorContext {
                        event_time: event.timestamp,
                        processing_time: 0,
                        timers: &mut rt.timer_service,
                        state: rt.state_store.as_mut(),
                        watermark_generator: rt.watermark_generator.as_mut(),
                        operator_index: idx,
                    };
                    op.process(&event, &mut ctx)
                } else {
                    passthrough_output(event)
                }
            } else {
                passthrough_output(event)
            };

            #[cfg(feature = "dag-metrics")]
            {
                #[allow(clippy::cast_possible_truncation)]
                let elapsed = start.elapsed().as_nanos() as u64;
                self.node_metrics[idx].total_time_ns += elapsed;
                self.node_metrics[idx].invocations += 1;
            }

            // Route all output variants to downstream nodes.
            self.route_all_outputs(node_id, outputs);
        }

        // Put operator and runtime back.
        self.operators[idx] = operator;
        self.runtimes[idx] = runtime;
        self.temp_events = events;
    }

    /// Routes all output variants from a source node.
    ///
    /// Handles each [`Output`] variant:
    /// - `Event` → route to downstream nodes via [`route_output()`]
    /// - `Watermark` → feed into watermark tracker, advance downstream runtimes
    /// - `LateEvent` → collect in sink outputs for the source node
    /// - `SideOutput` → collect in sink outputs for the source node
    /// - `Changelog` → collect changelog event in sink outputs
    /// - `CheckpointComplete` → no-op (consumed by coordinator)
    fn route_all_outputs(&mut self, source: NodeId, outputs: OutputVec) {
        use crate::operator::window::ChangelogRecord;

        for output in outputs {
            match output {
                Output::Event(out_event) => {
                    #[cfg(feature = "dag-metrics")]
                    {
                        let sidx = source.0 as usize;
                        if sidx < self.slot_count {
                            self.node_metrics[sidx].events_out += 1;
                        }
                    }
                    self.route_output(source, out_event);
                }
                Output::Watermark(wm) => {
                    let updated: SmallVec<[(NodeId, i64); 8]> = self
                        .watermark_tracker
                        .update_watermark(source, wm)
                        .iter()
                        .copied()
                        .collect();
                    for (node_id, new_wm) in &updated {
                        let nidx = node_id.0 as usize;
                        if nidx < self.slot_count {
                            if let Some(ref mut rt) = self.runtimes[nidx] {
                                rt.watermark_generator.advance_watermark(*new_wm);
                            }
                        }
                    }
                }
                Output::LateEvent(late_event) => {
                    self.sink_outputs[source.0 as usize].push(late_event);
                }
                Output::SideOutput(data) => {
                    let SideOutputData { event, .. } = *data;
                    self.sink_outputs[source.0 as usize].push(event);
                }
                Output::Changelog(ChangelogRecord { event, .. }) => {
                    self.sink_outputs[source.0 as usize].push(event);
                }
                Output::CheckpointComplete(_) => {
                    // No-op: consumed by checkpoint coordinator
                }
            }
        }
    }

    /// Routes an output event from a source node to its downstream targets.
    ///
    /// - **Terminal (sink)**: event is collected in `sink_outputs`.
    /// - **Single target**: event is enqueued directly (no clone).
    /// - **Multicast**: event is cloned to N-1 targets, moved to the last.
    fn route_output(&mut self, source: NodeId, event: Event) {
        let entry = self.routing.node_targets(source);

        if entry.is_terminal() {
            // Sink node: collect output.
            self.sink_outputs[source.0 as usize].push(event);
            return;
        }

        #[cfg(feature = "dag-metrics")]
        {
            self.metrics.events_routed += 1;
        }

        if entry.is_multicast {
            #[cfg(feature = "dag-metrics")]
            {
                self.metrics.multicast_publishes += 1;
            }
            let targets = entry.target_ids();

            // Clone to all targets except the last, which gets the moved value.
            for &target_id in &targets[..targets.len() - 1] {
                self.input_queues[target_id as usize].push_back(event.clone());
            }
            self.input_queues[targets[targets.len() - 1] as usize].push_back(event);
        } else {
            // Single target: enqueue directly (zero-copy move).
            self.input_queues[entry.targets[0] as usize].push_back(event);
        }
    }
}

impl std::fmt::Debug for DagExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DagExecutor")
            .field("slot_count", &self.slot_count)
            .field("source_nodes", &self.source_nodes)
            .field("sink_nodes", &self.sink_nodes)
            .field("execution_order", &self.execution_order)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

/// Creates a passthrough output (forwards the event unchanged).
#[inline]
fn passthrough_output(event: Event) -> OutputVec {
    let mut v = OutputVec::new();
    v.push(Output::Event(event));
    v
}
