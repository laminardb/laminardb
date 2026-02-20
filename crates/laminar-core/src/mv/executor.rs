//! Executor for cascading materialized view pipelines.
//!
//! Processes events through the MV DAG in topological order, ensuring
//! that dependencies are updated before their dependents.

use super::error::MvError;
use super::registry::MvRegistry;
use super::watermark::CascadingWatermarkTracker;
use crate::operator::{Event, Operator, OperatorContext, OperatorState, Output, OutputVec};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::sync::Arc;

/// Executor for cascading materialized view pipelines.
///
/// Processes events through the MV DAG in topological order, ensuring
/// correct watermark propagation and event routing between views.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::mv::{MvRegistry, MvPipelineExecutor, MaterializedView};
/// use std::sync::Arc;
///
/// // Setup registry with cascading views
/// let registry = Arc::new(setup_registry());
///
/// // Create executor with operators for each view
/// let mut executor = MvPipelineExecutor::new(registry);
/// executor.register_operator("ohlc_1s", Box::new(ohlc_1s_operator))?;
/// executor.register_operator("ohlc_1m", Box::new(ohlc_1m_operator))?;
///
/// // Process events from base source
/// let outputs = executor.process_source_event("trades", event, &mut ctx)?;
/// ```
pub struct MvPipelineExecutor {
    /// MV registry.
    registry: Arc<MvRegistry>,
    /// Operators per MV.
    operators: FxHashMap<String, Box<dyn Operator>>,
    /// Watermark tracker.
    watermarks: CascadingWatermarkTracker,
    /// Output queues per source/MV (for downstream consumption).
    output_queues: FxHashMap<String, VecDeque<Event>>,
    /// Metrics for the pipeline.
    metrics: PipelineMetrics,
}

/// Metrics for the MV pipeline.
#[derive(Debug, Clone, Default)]
pub struct PipelineMetrics {
    /// Total events processed.
    pub events_processed: u64,
    /// Events processed per MV.
    pub events_per_mv: FxHashMap<String, u64>,
    /// Watermarks advanced.
    pub watermarks_advanced: u64,
    /// Processing errors.
    pub errors: u64,
}

impl MvPipelineExecutor {
    /// Creates a new executor with the given registry.
    #[must_use]
    pub fn new(registry: Arc<MvRegistry>) -> Self {
        let watermarks = CascadingWatermarkTracker::new(Arc::clone(&registry));
        Self {
            registry,
            operators: FxHashMap::default(),
            watermarks,
            output_queues: FxHashMap::default(),
            metrics: PipelineMetrics::default(),
        }
    }

    /// Registers an operator for a materialized view.
    ///
    /// # Errors
    ///
    /// Returns error if the view doesn't exist in the registry.
    pub fn register_operator(
        &mut self,
        mv_name: &str,
        operator: Box<dyn Operator>,
    ) -> Result<(), MvError> {
        if !self.registry.views().any(|v| v.name == mv_name) {
            return Err(MvError::ViewNotFound(mv_name.to_string()));
        }
        self.operators.insert(mv_name.to_string(), operator);
        Ok(())
    }

    /// Checks if all MVs have registered operators.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.registry
            .views()
            .all(|v| self.operators.contains_key(&v.name))
    }

    /// Returns MVs that are missing operators.
    pub fn missing_operators(&self) -> impl Iterator<Item = &str> {
        self.registry
            .views()
            .filter(|v| !self.operators.contains_key(&v.name))
            .map(|v| v.name.as_str())
    }

    /// Processes an event from a base source.
    ///
    /// The event is queued for processing by dependent MVs.
    /// MVs are processed in topological order to ensure dependencies
    /// are updated before their dependents.
    ///
    /// Returns all outputs produced by the pipeline.
    ///
    /// # Errors
    ///
    /// Returns error if processing fails for any MV.
    pub fn process_source_event(
        &mut self,
        source: &str,
        event: Event,
        ctx: &mut OperatorContext,
    ) -> Result<Vec<Output>, MvError> {
        // Queue the event for processing by dependents
        self.output_queues
            .entry(source.to_string())
            .or_default()
            .push_back(event);

        self.metrics.events_processed += 1;

        // Process MVs in topological order
        let mut all_outputs = Vec::new();
        for mv_name in self.registry.topo_order().to_vec() {
            let outputs = self.process_mv_inputs(&mv_name, ctx)?;
            all_outputs.extend(outputs);
        }

        Ok(all_outputs)
    }

    /// Processes a batch of events from a base source.
    ///
    /// More efficient than calling `process_source_event` repeatedly.
    ///
    /// # Errors
    ///
    /// Returns `MvError` if processing fails for any MV.
    pub fn process_source_events(
        &mut self,
        source: &str,
        events: impl IntoIterator<Item = Event>,
        ctx: &mut OperatorContext,
    ) -> Result<Vec<Output>, MvError> {
        // Queue all events
        let queue = self.output_queues.entry(source.to_string()).or_default();
        let mut count = 0u64;
        for event in events {
            queue.push_back(event);
            count += 1;
        }
        self.metrics.events_processed += count;

        // Process MVs in topological order
        let mut all_outputs = Vec::new();
        for mv_name in self.registry.topo_order().to_vec() {
            let outputs = self.process_mv_inputs(&mv_name, ctx)?;
            all_outputs.extend(outputs);
        }

        Ok(all_outputs)
    }

    /// Advances the watermark for a source and propagates through the pipeline.
    ///
    /// Returns all watermark updates that occurred.
    pub fn advance_watermark(&mut self, source: &str, watermark: i64) -> Vec<(String, i64)> {
        self.metrics.watermarks_advanced += 1;
        self.watermarks.update_watermark(source, watermark)
    }

    /// Triggers timer callbacks for all MVs.
    ///
    /// Returns all outputs produced by timer handlers.
    ///
    /// # Errors
    ///
    /// Returns `MvError` if any operator fails during processing.
    pub fn on_timer(
        &mut self,
        timer: &crate::operator::Timer,
        ctx: &mut OperatorContext,
    ) -> Result<Vec<Output>, MvError> {
        let mut all_outputs = Vec::new();

        // Process timers for each MV in topological order
        for mv_name in self.registry.topo_order().to_vec() {
            if let Some(operator) = self.operators.get_mut(&mv_name) {
                let outputs = operator.on_timer(timer.clone(), ctx);
                for output in outputs {
                    match output {
                        Output::Event(event) => {
                            // Queue for downstream processing
                            self.output_queues
                                .entry(mv_name.clone())
                                .or_default()
                                .push_back(event);
                        }
                        Output::Changelog(ref changelog) => {
                            // Propagate event from changelog to downstream MVs
                            let event = changelog.event.clone();
                            self.output_queues
                                .entry(mv_name.clone())
                                .or_default()
                                .push_back(event);
                            all_outputs.push(output);
                        }
                        other => all_outputs.push(other),
                    }
                }
            }
        }

        // Process any events generated by timers
        for mv_name in self.registry.topo_order().to_vec() {
            let outputs = self.process_mv_inputs(&mv_name, ctx)?;
            all_outputs.extend(outputs);
        }

        Ok(all_outputs)
    }

    /// Gets the current watermark for a source or MV.
    #[must_use]
    pub fn get_watermark(&self, name: &str) -> Option<i64> {
        self.watermarks.get_watermark(name)
    }

    /// Gets all pending events for a source/MV.
    #[must_use]
    pub fn pending_events(&self, name: &str) -> Option<&VecDeque<Event>> {
        self.output_queues.get(name)
    }

    /// Gets pipeline metrics.
    #[must_use]
    pub fn metrics(&self) -> &PipelineMetrics {
        &self.metrics
    }

    /// Resets pipeline metrics.
    pub fn reset_metrics(&mut self) {
        self.metrics = PipelineMetrics::default();
    }

    fn process_mv_inputs(
        &mut self,
        mv_name: &str,
        ctx: &mut OperatorContext,
    ) -> Result<Vec<Output>, MvError> {
        let view = self
            .registry
            .get(mv_name)
            .ok_or_else(|| MvError::ViewNotFound(mv_name.to_string()))?;

        // Collect inputs from all sources
        let mut inputs = Vec::new();
        for source in &view.sources {
            if let Some(queue) = self.output_queues.get_mut(source) {
                inputs.extend(queue.drain(..));
            }
        }

        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        // Get the operator
        let operator = self
            .operators
            .get_mut(mv_name)
            .ok_or_else(|| MvError::OperatorNotFound(mv_name.to_string()))?;

        // Process all inputs
        let mut outputs = Vec::new();
        for input in inputs {
            let op_outputs = operator.process(&input, ctx);
            *self
                .metrics
                .events_per_mv
                .entry(mv_name.to_string())
                .or_default() += 1;

            for output in op_outputs {
                match output {
                    Output::Event(event) => {
                        // Queue for downstream MVs
                        self.output_queues
                            .entry(mv_name.to_string())
                            .or_default()
                            .push_back(event);
                    }
                    Output::Changelog(ref changelog) => {
                        // Propagate the event from changelog to downstream MVs
                        let event = changelog.event.clone();
                        self.output_queues
                            .entry(mv_name.to_string())
                            .or_default()
                            .push_back(event);
                        // Also pass through to final output for sinks
                        outputs.push(output);
                    }
                    other => outputs.push(other),
                }
            }
        }

        Ok(outputs)
    }
}

/// Checkpoint data for the MV pipeline.
#[derive(Debug, Clone)]
pub struct MvPipelineCheckpoint {
    /// Operator states per MV.
    pub operator_states: Vec<(String, OperatorState)>,
    /// Watermarks at checkpoint time.
    pub watermarks: Vec<(String, i64)>,
    /// Pending events per source/MV.
    pub pending_events: Vec<(String, Vec<Event>)>,
}

impl MvPipelineExecutor {
    /// Creates a checkpoint of the pipeline state.
    #[must_use]
    pub fn checkpoint(&self) -> MvPipelineCheckpoint {
        let operator_states = self
            .operators
            .iter()
            .map(|(name, op)| (name.clone(), op.checkpoint()))
            .collect();

        let watermarks = self.watermarks.checkpoint().watermarks;

        let pending_events = self
            .output_queues
            .iter()
            .map(|(name, queue)| (name.clone(), queue.iter().cloned().collect()))
            .collect();

        MvPipelineCheckpoint {
            operator_states,
            watermarks,
            pending_events,
        }
    }

    /// Restores pipeline state from a checkpoint.
    ///
    /// # Errors
    ///
    /// Returns error if any operator fails to restore.
    pub fn restore(&mut self, checkpoint: MvPipelineCheckpoint) -> Result<(), MvError> {
        // Restore operator states
        for (name, state) in checkpoint.operator_states {
            if let Some(operator) = self.operators.get_mut(&name) {
                operator.restore(state)?;
            }
        }

        // Restore watermarks
        self.watermarks
            .restore(super::watermark::WatermarkTrackerCheckpoint {
                watermarks: checkpoint.watermarks,
            });

        // Restore pending events
        self.output_queues.clear();
        for (name, events) in checkpoint.pending_events {
            self.output_queues
                .insert(name, events.into_iter().collect());
        }

        Ok(())
    }
}

/// A simple pass-through operator for testing.
#[derive(Debug, Clone)]
pub struct PassThroughOperator {
    /// Operator ID for checkpointing.
    pub id: String,
}

impl PassThroughOperator {
    /// Creates a new pass-through operator.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

impl Operator for PassThroughOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let mut outputs = OutputVec::new();
        outputs.push(Output::Event(event.clone()));
        outputs
    }

    fn on_timer(
        &mut self,
        _timer: crate::operator::Timer,
        _ctx: &mut OperatorContext,
    ) -> OutputVec {
        OutputVec::new()
    }

    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: self.id.clone(),
            data: Vec::new(),
        }
    }

    fn restore(&mut self, _state: OperatorState) -> Result<(), crate::operator::OperatorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::registry::MaterializedView;
    use crate::operator::window::{CdcOperation, ChangelogRecord};
    use crate::state::InMemoryStore;
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService};
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    fn setup_registry() -> Arc<MvRegistry> {
        let mut registry = MvRegistry::new();
        registry.register_base_table("trades");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let mv = |n: &str, s: Vec<&str>| {
            MaterializedView::new(
                n,
                "",
                s.into_iter().map(String::from).collect(),
                schema.clone(),
            )
        };

        registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
        registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();

        Arc::new(registry)
    }

    fn create_test_event(value: i64, timestamp: i64) -> Event {
        let array = Arc::new(Int64Array::from(vec![value]));
        let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
        Event::new(timestamp, batch)
    }

    fn create_context() -> (InMemoryStore, TimerService, BoundedOutOfOrdernessGenerator) {
        (
            InMemoryStore::new(),
            TimerService::new(),
            BoundedOutOfOrdernessGenerator::new(1000),
        )
    }

    #[test]
    fn test_executor_creation() {
        let registry = setup_registry();
        let executor = MvPipelineExecutor::new(registry);

        assert!(!executor.is_ready());
        let missing: Vec<_> = executor.missing_operators().collect();
        assert!(missing.contains(&"ohlc_1s"));
        assert!(missing.contains(&"ohlc_1m"));
    }

    #[test]
    fn test_register_operators() {
        let registry = setup_registry();
        let mut executor = MvPipelineExecutor::new(registry);

        executor
            .register_operator("ohlc_1s", Box::new(PassThroughOperator::new("ohlc_1s")))
            .unwrap();
        executor
            .register_operator("ohlc_1m", Box::new(PassThroughOperator::new("ohlc_1m")))
            .unwrap();

        assert!(executor.is_ready());
    }

    #[test]
    fn test_register_nonexistent_mv() {
        let registry = setup_registry();
        let mut executor = MvPipelineExecutor::new(registry);

        let result =
            executor.register_operator("nonexistent", Box::new(PassThroughOperator::new("x")));
        assert!(matches!(result, Err(MvError::ViewNotFound(_))));
    }

    #[test]
    fn test_process_event_propagation() {
        let registry = setup_registry();
        let mut executor = MvPipelineExecutor::new(registry);

        executor
            .register_operator("ohlc_1s", Box::new(PassThroughOperator::new("ohlc_1s")))
            .unwrap();
        executor
            .register_operator("ohlc_1m", Box::new(PassThroughOperator::new("ohlc_1m")))
            .unwrap();

        let (mut state, mut timers, mut wm_gen) = create_context();
        let mut ctx = OperatorContext {
            event_time: 1000,
            processing_time: 1000,
            timers: &mut timers,
            state: &mut state,
            watermark_generator: &mut wm_gen,
            operator_index: 0,
        };

        let event = create_test_event(100, 1000);
        let _outputs = executor
            .process_source_event("trades", event, &mut ctx)
            .unwrap();

        // Check metrics
        assert_eq!(executor.metrics().events_processed, 1);
        assert_eq!(executor.metrics().events_per_mv.get("ohlc_1s"), Some(&1));
        assert_eq!(executor.metrics().events_per_mv.get("ohlc_1m"), Some(&1));

        // Events should have propagated through to ohlc_1m's output queue
        let pending = executor.pending_events("ohlc_1m");
        assert!(pending.is_some());
        assert_eq!(pending.unwrap().len(), 1);
    }

    #[test]
    fn test_batch_processing() {
        let registry = setup_registry();
        let mut executor = MvPipelineExecutor::new(registry);

        executor
            .register_operator("ohlc_1s", Box::new(PassThroughOperator::new("ohlc_1s")))
            .unwrap();
        executor
            .register_operator("ohlc_1m", Box::new(PassThroughOperator::new("ohlc_1m")))
            .unwrap();

        let (mut state, mut timers, mut wm_gen) = create_context();
        let mut ctx = OperatorContext {
            event_time: 1000,
            processing_time: 1000,
            timers: &mut timers,
            state: &mut state,
            watermark_generator: &mut wm_gen,
            operator_index: 0,
        };

        let events = vec![
            create_test_event(100, 1000),
            create_test_event(200, 2000),
            create_test_event(300, 3000),
        ];

        executor
            .process_source_events("trades", events, &mut ctx)
            .unwrap();

        assert_eq!(executor.metrics().events_processed, 3);
        assert_eq!(executor.metrics().events_per_mv.get("ohlc_1s"), Some(&3));
        assert_eq!(executor.metrics().events_per_mv.get("ohlc_1m"), Some(&3));
    }

    #[test]
    fn test_watermark_propagation() {
        let registry = setup_registry();
        let mut executor = MvPipelineExecutor::new(registry);

        executor
            .register_operator("ohlc_1s", Box::new(PassThroughOperator::new("ohlc_1s")))
            .unwrap();
        executor
            .register_operator("ohlc_1m", Box::new(PassThroughOperator::new("ohlc_1m")))
            .unwrap();

        let updated = executor.advance_watermark("trades", 60_000);

        assert!(!updated.is_empty());
        assert_eq!(executor.get_watermark("trades"), Some(60_000));
        assert_eq!(executor.get_watermark("ohlc_1s"), Some(60_000));
        assert_eq!(executor.get_watermark("ohlc_1m"), Some(60_000));
    }

    #[test]
    fn test_checkpoint_restore() {
        let registry = setup_registry();
        let mut executor = MvPipelineExecutor::new(Arc::clone(&registry));

        executor
            .register_operator("ohlc_1s", Box::new(PassThroughOperator::new("ohlc_1s")))
            .unwrap();
        executor
            .register_operator("ohlc_1m", Box::new(PassThroughOperator::new("ohlc_1m")))
            .unwrap();

        // Process some events and advance watermark
        let (mut state, mut timers, mut wm_gen) = create_context();
        let mut ctx = OperatorContext {
            event_time: 1000,
            processing_time: 1000,
            timers: &mut timers,
            state: &mut state,
            watermark_generator: &mut wm_gen,
            operator_index: 0,
        };

        executor
            .process_source_event("trades", create_test_event(100, 1000), &mut ctx)
            .unwrap();
        executor.advance_watermark("trades", 5000);

        // Take checkpoint
        let checkpoint = executor.checkpoint();
        assert!(!checkpoint.operator_states.is_empty());
        assert!(!checkpoint.watermarks.is_empty());

        // Create new executor and restore
        let mut executor2 = MvPipelineExecutor::new(registry);
        executor2
            .register_operator("ohlc_1s", Box::new(PassThroughOperator::new("ohlc_1s")))
            .unwrap();
        executor2
            .register_operator("ohlc_1m", Box::new(PassThroughOperator::new("ohlc_1m")))
            .unwrap();

        executor2.restore(checkpoint).unwrap();

        // Watermarks should be restored
        assert_eq!(executor2.get_watermark("trades"), Some(5000));
    }

    /// Operator that emits Changelog records (for testing changelog propagation).
    #[derive(Debug, Clone)]
    struct ChangelogEmittingOperator {
        id: String,
    }

    impl Operator for ChangelogEmittingOperator {
        fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
            let mut outputs = OutputVec::new();
            outputs.push(Output::Changelog(ChangelogRecord {
                operation: CdcOperation::Insert,
                weight: 1,
                emit_timestamp: event.timestamp,
                event: event.clone(),
            }));
            outputs
        }

        fn on_timer(
            &mut self,
            _timer: crate::operator::Timer,
            _ctx: &mut OperatorContext,
        ) -> OutputVec {
            OutputVec::new()
        }

        fn checkpoint(&self) -> OperatorState {
            OperatorState {
                operator_id: self.id.clone(),
                data: Vec::new(),
            }
        }

        fn restore(&mut self, _state: OperatorState) -> Result<(), crate::operator::OperatorError> {
            Ok(())
        }
    }

    #[test]
    fn test_changelog_propagation_three_level_mv_chain() {
        // Setup: source → MV1 (emits Changelog) → MV2 (pass-through)
        let mut registry = MvRegistry::new();
        registry.register_base_table("trades");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let mv = |n: &str, s: Vec<&str>| {
            MaterializedView::new(
                n,
                "",
                s.into_iter().map(String::from).collect(),
                schema.clone(),
            )
        };

        registry.register(mv("mv1", vec!["trades"])).unwrap();
        registry.register(mv("mv2", vec!["mv1"])).unwrap();
        let registry = Arc::new(registry);

        let mut executor = MvPipelineExecutor::new(registry);

        // MV1 emits Changelog, MV2 is a pass-through
        executor
            .register_operator(
                "mv1",
                Box::new(ChangelogEmittingOperator { id: "mv1".into() }),
            )
            .unwrap();
        executor
            .register_operator("mv2", Box::new(PassThroughOperator::new("mv2")))
            .unwrap();

        let (mut state, mut timers, mut wm_gen) = create_context();
        let mut ctx = OperatorContext {
            event_time: 1000,
            processing_time: 1000,
            timers: &mut timers,
            state: &mut state,
            watermark_generator: &mut wm_gen,
            operator_index: 0,
        };

        let event = create_test_event(42, 1000);
        let outputs = executor
            .process_source_event("trades", event, &mut ctx)
            .unwrap();

        // MV2 should have received the event from MV1's changelog
        assert_eq!(
            executor.metrics().events_per_mv.get("mv2"),
            Some(&1),
            "MV2 should have processed 1 event from MV1's changelog"
        );

        // Final outputs should contain the Changelog from MV1
        // (MV2's output is an Event queued to its output_queue, plus MV1's Changelog)
        let changelog_count = outputs
            .iter()
            .filter(|o| matches!(o, Output::Changelog(_)))
            .count();
        assert_eq!(
            changelog_count, 1,
            "should have 1 changelog in final output from MV1"
        );

        // MV2's output should be queued (pass-through emits Event)
        let pending_mv2 = executor.pending_events("mv2");
        assert!(pending_mv2.is_some());
        assert_eq!(pending_mv2.unwrap().len(), 1);
    }

    #[test]
    fn test_metrics_reset() {
        let registry = setup_registry();
        let mut executor = MvPipelineExecutor::new(registry);

        executor
            .register_operator("ohlc_1s", Box::new(PassThroughOperator::new("ohlc_1s")))
            .unwrap();
        executor
            .register_operator("ohlc_1m", Box::new(PassThroughOperator::new("ohlc_1m")))
            .unwrap();

        let (mut state, mut timers, mut wm_gen) = create_context();
        let mut ctx = OperatorContext {
            event_time: 1000,
            processing_time: 1000,
            timers: &mut timers,
            state: &mut state,
            watermark_generator: &mut wm_gen,
            operator_index: 0,
        };

        executor
            .process_source_event("trades", create_test_event(100, 1000), &mut ctx)
            .unwrap();
        assert_eq!(executor.metrics().events_processed, 1);

        executor.reset_metrics();
        assert_eq!(executor.metrics().events_processed, 0);
    }
}
