//! # Reactor Module
//!
//! The core event loop for `LaminarDB`, implementing a single-threaded reactor pattern
//! optimized for streaming workloads.
//!
//! ## Design Goals
//!
//! - **Zero allocations** during event processing
//! - **CPU-pinned** execution for cache locality
//! - **Lock-free** communication with other threads
//! - **500K+ events/sec** per core throughput
//!
//! ## Architecture
//!
//! The reactor runs a tight event loop that:
//! 1. Polls input sources for events
//! 2. Routes events to operators
//! 3. Manages operator state
//! 4. Emits results to sinks
//!
//! Communication with Ring 1 (background tasks) happens via SPSC queues.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::alloc::HotPathGuard;
use crate::budget::TaskBudget;
use crate::operator::{Event, Operator, OperatorContext, OperatorState, Output};
use crate::state::{InMemoryStore, StateStore};
use crate::time::{BoundedOutOfOrdernessGenerator, TimerService, WatermarkGenerator};

/// Trait for output sinks that consume reactor outputs.
pub trait Sink: Send {
    /// Write outputs to the sink.
    ///
    /// # Errors
    ///
    /// Returns an error if the sink cannot accept the outputs.
    fn write(&mut self, outputs: Vec<Output>) -> Result<(), SinkError>;

    /// Flush any buffered data.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush operation fails.
    fn flush(&mut self) -> Result<(), SinkError>;
}

/// Errors that can occur in sinks.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    /// Failed to write to sink
    #[error("Write failed: {0}")]
    WriteFailed(String),

    /// Failed to flush sink
    #[error("Flush failed: {0}")]
    FlushFailed(String),

    /// Sink is closed
    #[error("Sink is closed")]
    Closed,
}

/// Configuration for the reactor
#[derive(Debug, Clone)]
pub struct ReactorConfig {
    /// Maximum events to process per poll
    pub batch_size: usize,
    /// CPU core to pin the reactor thread to (None = no pinning)
    pub cpu_affinity: Option<usize>,
    /// Maximum time to spend in one iteration
    pub max_iteration_time: Duration,
    /// Size of the event buffer
    pub event_buffer_size: usize,
    /// Maximum out-of-orderness for watermark generation (milliseconds)
    pub max_out_of_orderness: i64,
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            cpu_affinity: None,
            max_iteration_time: Duration::from_millis(10),
            event_buffer_size: 65536,
            max_out_of_orderness: 1000, // 1 second
        }
    }
}

/// The main reactor for event processing
pub struct Reactor {
    config: ReactorConfig,
    operators: Vec<Box<dyn Operator>>,
    timer_service: TimerService,
    event_queue: VecDeque<Event>,
    output_buffer: Vec<Output>,
    state_store: Box<dyn StateStore>,
    watermark_generator: Box<dyn WatermarkGenerator>,
    current_event_time: i64,
    start_time: Instant,
    events_processed: u64,
    /// Pre-allocated buffers for operator chain processing
    /// We use two buffers and swap between them to avoid allocations
    operator_buffer_1: Vec<Output>,
    operator_buffer_2: Vec<Output>,
    /// Optional sink for outputs
    sink: Option<Box<dyn Sink>>,
    /// Shutdown flag for graceful termination
    shutdown: Arc<AtomicBool>,
}

impl Reactor {
    /// Creates a new reactor with the given configuration
    ///
    /// # Errors
    ///
    /// Currently does not return any errors, but may in the future if initialization fails
    pub fn new(config: ReactorConfig) -> Result<Self, ReactorError> {
        let event_queue = VecDeque::with_capacity(config.event_buffer_size);
        let watermark_generator = Box::new(BoundedOutOfOrdernessGenerator::new(
            config.max_out_of_orderness,
        ));

        Ok(Self {
            config,
            operators: Vec::new(),
            timer_service: TimerService::new(),
            event_queue,
            output_buffer: Vec::with_capacity(1024),
            state_store: Box::new(InMemoryStore::new()),
            watermark_generator,
            current_event_time: 0,
            start_time: Instant::now(),
            events_processed: 0,
            operator_buffer_1: Vec::with_capacity(256),
            operator_buffer_2: Vec::with_capacity(256),
            sink: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Register an operator in the processing chain
    pub fn add_operator(&mut self, operator: Box<dyn Operator>) {
        self.operators.push(operator);
    }

    /// Set the output sink for the reactor
    pub fn set_sink(&mut self, sink: Box<dyn Sink>) {
        self.sink = Some(sink);
    }

    /// Get a handle to the shutdown flag
    #[must_use]
    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Submit an event for processing
    ///
    /// # Errors
    ///
    /// Returns `ReactorError::QueueFull` if the event queue is at capacity
    pub fn submit(&mut self, event: Event) -> Result<(), ReactorError> {
        if self.event_queue.len() >= self.config.event_buffer_size {
            return Err(ReactorError::QueueFull {
                capacity: self.config.event_buffer_size,
            });
        }

        self.event_queue.push_back(event);
        Ok(())
    }

    /// Submit multiple events for processing
    ///
    /// # Errors
    ///
    /// Returns `ReactorError::QueueFull` if there's insufficient capacity for all events
    pub fn submit_batch(&mut self, events: Vec<Event>) -> Result<(), ReactorError> {
        let available = self.config.event_buffer_size - self.event_queue.len();
        if events.len() > available {
            return Err(ReactorError::QueueFull {
                capacity: self.config.event_buffer_size,
            });
        }

        self.event_queue.extend(events);
        Ok(())
    }

    /// Run one iteration of the event loop
    /// Returns outputs ready for downstream
    pub fn poll(&mut self) -> Vec<Output> {
        // Hot path guard - will panic on allocation when allocation-tracking is enabled
        let _guard = HotPathGuard::enter("Reactor::poll");

        // Task budget tracking - records metrics on drop
        let _iteration_budget = TaskBudget::ring0_iteration();

        let poll_start = Instant::now();
        let processing_time = self.get_processing_time();

        // 1. Fire expired timers
        let fired_timers = self.timer_service.poll_timers(self.current_event_time);
        for mut timer in fired_timers {
            if let Some(idx) = timer.operator_index {
                // Route to specific operator
                if let Some(operator) = self.operators.get_mut(idx) {
                    let timer_key = timer.key.take().unwrap_or_default();
                    let timer_for_operator = crate::operator::Timer {
                        key: timer_key,
                        timestamp: timer.timestamp,
                    };

                    let mut ctx = OperatorContext {
                        event_time: self.current_event_time,
                        processing_time,
                        timers: &mut self.timer_service,
                        state: self.state_store.as_mut(),
                        watermark_generator: self.watermark_generator.as_mut(),
                        operator_index: idx,
                    };

                    let outputs = operator.on_timer(timer_for_operator, &mut ctx);
                    self.output_buffer.extend(outputs);
                }
            } else {
                // Legacy: Broadcast to all operators (warning: creates key contention)
                for (idx, operator) in self.operators.iter_mut().enumerate() {
                    // Move key out of timer (only first operator gets it!)
                    let timer_key = timer.key.take().unwrap_or_default();
                    let timer_for_operator = crate::operator::Timer {
                        key: timer_key,
                        timestamp: timer.timestamp,
                    };

                    let mut ctx = OperatorContext {
                        event_time: self.current_event_time,
                        processing_time,
                        timers: &mut self.timer_service,
                        state: self.state_store.as_mut(),
                        watermark_generator: self.watermark_generator.as_mut(),
                        operator_index: idx,
                    };

                    let outputs = operator.on_timer(timer_for_operator, &mut ctx);
                    self.output_buffer.extend(outputs);
                }
            }
        }

        // 2. Process events
        let mut events_in_batch = 0;
        while let Some(event) = self.event_queue.pop_front() {
            // Update current event time
            if event.timestamp > self.current_event_time {
                self.current_event_time = event.timestamp;
            }

            // Generate watermark if needed
            if let Some(watermark) = self.watermark_generator.on_event(event.timestamp) {
                self.output_buffer
                    .push(Output::Watermark(watermark.timestamp()));
            }

            // Process through operator chain using pre-allocated buffers
            // Start with the event in buffer 1
            self.operator_buffer_1.clear();
            self.operator_buffer_1.push(Output::Event(event));

            let mut current_buffer_is_1 = true;

            for (idx, operator) in self.operators.iter_mut().enumerate() {
                // Determine which buffer to read from and which to write to
                let (current_buffer, next_buffer) = if current_buffer_is_1 {
                    (&mut self.operator_buffer_1, &mut self.operator_buffer_2)
                } else {
                    (&mut self.operator_buffer_2, &mut self.operator_buffer_1)
                };

                next_buffer.clear();

                for output in current_buffer.drain(..) {
                    if let Output::Event(event) = output {
                        let mut ctx = OperatorContext {
                            event_time: self.current_event_time,
                            processing_time,
                            timers: &mut self.timer_service,
                            state: self.state_store.as_mut(),
                            watermark_generator: self.watermark_generator.as_mut(),
                            operator_index: idx,
                        };

                        let operator_outputs = operator.process(&event, &mut ctx);
                        next_buffer.extend(operator_outputs);
                    } else {
                        // Pass through watermarks and late events
                        next_buffer.push(output);
                    }
                }

                // Swap buffers for next iteration
                current_buffer_is_1 = !current_buffer_is_1;
            }

            // Extend output buffer with final results
            let final_buffer = if current_buffer_is_1 {
                &mut self.operator_buffer_1
            } else {
                &mut self.operator_buffer_2
            };
            self.output_buffer.append(final_buffer);
            self.events_processed += 1;
            events_in_batch += 1;

            // Check batch size limit
            if events_in_batch >= self.config.batch_size {
                break;
            }

            // Check time limit
            if poll_start.elapsed() >= self.config.max_iteration_time {
                break;
            }
        }

        // 3. Return outputs
        std::mem::take(&mut self.output_buffer)
    }

    /// Advances the watermark to the given timestamp.
    ///
    /// Called when an external watermark message arrives (e.g., from TPC coordination).
    /// Updates the reactor's event time tracking and watermark generator state.
    /// Any resulting watermark output will be included in the next `poll()` result.
    pub fn advance_watermark(&mut self, timestamp: i64) {
        // Update current event time if this watermark is newer
        if timestamp > self.current_event_time {
            self.current_event_time = timestamp;
        }

        // Feed the timestamp to the watermark generator so it can advance
        if let Some(watermark) = self.watermark_generator.on_event(timestamp) {
            self.output_buffer
                .push(Output::Watermark(watermark.timestamp()));
        }
    }

    /// Triggers a checkpoint by snapshotting all operator states.
    ///
    /// Called when a `CheckpointRequest` arrives from the control plane.
    /// Collects the serialized state from each operator and returns it
    /// for persistence by Ring 1.
    pub fn trigger_checkpoint(&mut self) -> Vec<OperatorState> {
        self.operators.iter().map(|op| op.checkpoint()).collect()
    }

    /// Get current processing time in microseconds since reactor start
    #[allow(clippy::cast_possible_truncation)] // Saturating conversion handles overflow on next line
    fn get_processing_time(&self) -> i64 {
        // Saturating conversion - after ~292 years this will saturate at i64::MAX
        let micros = self.start_time.elapsed().as_micros();
        if micros > i64::MAX as u128 {
            i64::MAX
        } else {
            micros as i64
        }
    }

    /// Get the number of events processed
    #[must_use]
    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    /// Get the number of events in the queue
    #[must_use]
    pub fn queue_size(&self) -> usize {
        self.event_queue.len()
    }

    /// Set CPU affinity if configured
    ///
    /// # Errors
    ///
    /// Returns `ReactorError` if CPU affinity cannot be set (platform-specific)
    #[allow(unused_variables)]
    pub fn set_cpu_affinity(&self) -> Result<(), ReactorError> {
        if let Some(cpu_id) = self.config.cpu_affinity {
            #[cfg(target_os = "linux")]
            {
                use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
                use std::mem;

                // SAFETY: We're calling libc functions with valid parameters.
                // The cpu_set_t is properly initialized with CPU_ZERO.
                // The process ID 0 refers to the current thread.
                #[allow(unsafe_code)]
                unsafe {
                    let mut set: cpu_set_t = mem::zeroed();
                    CPU_ZERO(&mut set);
                    CPU_SET(cpu_id, &mut set);

                    let result = sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &set);
                    if result != 0 {
                        return Err(ReactorError::InitializationFailed(format!(
                            "Failed to set CPU affinity to core {}: {}",
                            cpu_id,
                            std::io::Error::last_os_error()
                        )));
                    }
                }
            }

            #[cfg(target_os = "windows")]
            {
                use winapi::shared::basetsd::DWORD_PTR;
                use winapi::um::processthreadsapi::GetCurrentThread;
                use winapi::um::winbase::SetThreadAffinityMask;

                // SAFETY: We're calling Windows API functions with valid parameters.
                // GetCurrentThread returns a pseudo-handle that doesn't need to be closed.
                // The mask is a valid CPU mask for the specified core.
                #[allow(unsafe_code)]
                unsafe {
                    let mask: DWORD_PTR = 1 << cpu_id;
                    let result = SetThreadAffinityMask(GetCurrentThread(), mask);
                    if result == 0 {
                        return Err(ReactorError::InitializationFailed(format!(
                            "Failed to set CPU affinity to core {}: {}",
                            cpu_id,
                            std::io::Error::last_os_error()
                        )));
                    }
                }
            }

            #[cfg(not(any(target_os = "linux", target_os = "windows")))]
            {
                eprintln!("Warning: CPU affinity is not implemented for this platform");
            }
        }
        Ok(())
    }

    /// Runs the event loop continuously until shutdown
    ///
    /// # Errors
    ///
    /// Returns `ReactorError` if CPU affinity cannot be set or if shutdown fails
    pub fn run(&mut self) -> Result<(), ReactorError> {
        self.set_cpu_affinity()?;

        while !self.shutdown.load(Ordering::Relaxed) {
            // Process events
            let outputs = self.poll();

            // Send outputs to sink if configured
            if !outputs.is_empty() {
                if let Some(sink) = &mut self.sink {
                    if let Err(e) = sink.write(outputs) {
                        eprintln!("Failed to write to sink: {e}");
                        // Continue processing even if sink fails
                    }
                }
            }

            // If no events to process, yield to avoid busy-waiting
            if self.event_queue.is_empty() {
                std::thread::yield_now();
            }

            // Periodically check for shutdown signal
            if self.events_processed.is_multiple_of(1000) && self.shutdown.load(Ordering::Relaxed) {
                break;
            }
        }

        // Flush sink before shutdown
        if let Some(sink) = &mut self.sink {
            if let Err(e) = sink.flush() {
                eprintln!("Failed to flush sink during shutdown: {e}");
            }
        }

        Ok(())
    }

    /// Stops the reactor gracefully
    ///
    /// # Errors
    ///
    /// Currently does not return any errors, but may in the future if shutdown fails
    pub fn shutdown(&mut self) -> Result<(), ReactorError> {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Relaxed);

        // Process remaining events
        while !self.event_queue.is_empty() {
            let outputs = self.poll();

            // Send final outputs to sink
            if !outputs.is_empty() {
                if let Some(sink) = &mut self.sink {
                    if let Err(e) = sink.write(outputs) {
                        eprintln!("Failed to write final outputs during shutdown: {e}");
                    }
                }
            }
        }

        // Final flush
        if let Some(sink) = &mut self.sink {
            if let Err(e) = sink.flush() {
                eprintln!("Failed to flush sink during shutdown: {e}");
            }
        }

        Ok(())
    }
}

/// Errors that can occur in the reactor
#[derive(Debug, thiserror::Error)]
pub enum ReactorError {
    /// Failed to initialize the reactor
    #[error("Initialization failed: {0}")]
    InitializationFailed(String),

    /// Event processing error
    #[error("Event processing failed: {0}")]
    EventProcessingFailed(String),

    /// Shutdown error
    #[error("Shutdown failed: {0}")]
    ShutdownFailed(String),

    /// Event queue is full
    #[error("Event queue full (capacity: {capacity})")]
    QueueFull {
        /// The configured capacity of the event queue
        capacity: usize,
    },
}

/// A simple sink that writes outputs to stdout (for testing).
pub struct StdoutSink;

impl Sink for StdoutSink {
    fn write(&mut self, outputs: Vec<Output>) -> Result<(), SinkError> {
        for output in outputs {
            match output {
                Output::Event(event) => {
                    println!(
                        "Event: timestamp={}, data={:?}",
                        event.timestamp, event.data
                    );
                }
                Output::Watermark(timestamp) => {
                    println!("Watermark: {timestamp}");
                }
                Output::LateEvent(event) => {
                    println!(
                        "Late Event (dropped): timestamp={}, data={:?}",
                        event.timestamp, event.data
                    );
                }
                Output::SideOutput { name, event } => {
                    println!(
                        "Side Output [{}]: timestamp={}, data={:?}",
                        name, event.timestamp, event.data
                    );
                }
                Output::Changelog(record) => {
                    println!(
                        "Changelog: op={:?}, weight={}, emit_ts={}, event_ts={}, data={:?}",
                        record.operation,
                        record.weight,
                        record.emit_timestamp,
                        record.event.timestamp,
                        record.event.data
                    );
                }
                Output::CheckpointComplete {
                    checkpoint_id,
                    operator_states,
                } => {
                    println!(
                        "Checkpoint: id={checkpoint_id}, operators={}",
                        operator_states.len()
                    );
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), SinkError> {
        Ok(())
    }
}

/// A buffering sink that collects outputs (for testing).
#[derive(Default)]
pub struct BufferingSink {
    buffer: Vec<Output>,
}

impl BufferingSink {
    /// Create a new buffering sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the buffered outputs.
    #[must_use]
    pub fn take_buffer(&mut self) -> Vec<Output> {
        std::mem::take(&mut self.buffer)
    }
}

impl Sink for BufferingSink {
    fn write(&mut self, mut outputs: Vec<Output>) -> Result<(), SinkError> {
        self.buffer.append(&mut outputs);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), SinkError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::OutputVec;
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;

    // Mock operator for testing
    struct PassthroughOperator;

    impl Operator for PassthroughOperator {
        fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
            let mut output = OutputVec::new();
            output.push(Output::Event(event.clone()));
            output
        }

        fn on_timer(
            &mut self,
            _timer: crate::operator::Timer,
            _ctx: &mut OperatorContext,
        ) -> OutputVec {
            OutputVec::new()
        }

        fn checkpoint(&self) -> crate::operator::OperatorState {
            crate::operator::OperatorState {
                operator_id: "passthrough".to_string(),
                data: vec![],
            }
        }

        fn restore(
            &mut self,
            _state: crate::operator::OperatorState,
        ) -> Result<(), crate::operator::OperatorError> {
            Ok(())
        }
    }

    #[test]
    fn test_default_config() {
        let config = ReactorConfig::default();
        assert_eq!(config.batch_size, 1024);
        assert_eq!(config.event_buffer_size, 65536);
    }

    #[test]
    fn test_reactor_creation() {
        let config = ReactorConfig::default();
        let reactor = Reactor::new(config);
        assert!(reactor.is_ok());
    }

    #[test]
    fn test_reactor_add_operator() {
        let config = ReactorConfig::default();
        let mut reactor = Reactor::new(config).unwrap();

        let operator = Box::new(PassthroughOperator);
        reactor.add_operator(operator);

        assert_eq!(reactor.operators.len(), 1);
    }

    #[test]
    fn test_reactor_submit_event() {
        let config = ReactorConfig::default();
        let mut reactor = Reactor::new(config).unwrap();

        let array = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();
        let event = Event::new(12345, batch);

        assert!(reactor.submit(event).is_ok());
        assert_eq!(reactor.queue_size(), 1);
    }

    #[test]
    fn test_reactor_poll_processes_events() {
        let config = ReactorConfig::default();
        let mut reactor = Reactor::new(config).unwrap();

        // Add a passthrough operator
        reactor.add_operator(Box::new(PassthroughOperator));

        // Submit an event
        let array = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();
        let event = Event::new(12345, batch);

        reactor.submit(event.clone()).unwrap();

        // Poll should process the event
        let outputs = reactor.poll();
        assert!(!outputs.is_empty());
        assert_eq!(reactor.events_processed(), 1);
        assert_eq!(reactor.queue_size(), 0);
    }

    #[test]
    fn test_reactor_queue_full() {
        let config = ReactorConfig {
            event_buffer_size: 2, // Very small buffer
            ..ReactorConfig::default()
        };
        let mut reactor = Reactor::new(config).unwrap();

        let array = Arc::new(Int64Array::from(vec![1]));
        let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();

        // Fill the queue
        for i in 0..2 {
            let event = Event::new(i64::from(i), batch.clone());
            assert!(reactor.submit(event).is_ok());
        }

        // Next submit should fail
        let event = Event::new(100, batch);
        assert!(matches!(
            reactor.submit(event),
            Err(ReactorError::QueueFull { .. })
        ));
    }

    #[test]
    fn test_reactor_batch_processing() {
        let config = ReactorConfig {
            batch_size: 2, // Small batch size
            ..ReactorConfig::default()
        };
        let mut reactor = Reactor::new(config).unwrap();

        reactor.add_operator(Box::new(PassthroughOperator));

        let array = Arc::new(Int64Array::from(vec![1]));
        let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();

        // Submit 5 events
        for i in 0..5 {
            let event = Event::new(i64::from(i), batch.clone());
            reactor.submit(event).unwrap();
        }

        // First poll should process only batch_size events
        reactor.poll();
        assert_eq!(reactor.events_processed(), 2);
        assert_eq!(reactor.queue_size(), 3);

        // Second poll should process 2 more
        reactor.poll();
        assert_eq!(reactor.events_processed(), 4);
        assert_eq!(reactor.queue_size(), 1);

        // Third poll should process the last one
        reactor.poll();
        assert_eq!(reactor.events_processed(), 5);
        assert_eq!(reactor.queue_size(), 0);
    }

    #[test]
    fn test_reactor_with_sink() {
        let config = ReactorConfig::default();
        let mut reactor = Reactor::new(config).unwrap();

        // Add a buffering sink
        let sink = Box::new(BufferingSink::new());
        reactor.set_sink(sink);

        // Add passthrough operator
        reactor.add_operator(Box::new(PassthroughOperator));

        let array = Arc::new(Int64Array::from(vec![42]));
        let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
        let event = Event::new(1000, batch);

        // Submit an event
        reactor.submit(event).unwrap();

        // Process
        let outputs = reactor.poll();
        // Should get the event output (and possibly a watermark)
        assert!(!outputs.is_empty());

        // Verify we got the event
        assert!(outputs.iter().any(|o| matches!(o, Output::Event(_))));
    }

    #[test]
    fn test_reactor_shutdown() {
        let config = ReactorConfig::default();
        let mut reactor = Reactor::new(config).unwrap();

        // Get shutdown handle
        let shutdown_handle = reactor.shutdown_handle();
        assert!(!shutdown_handle.load(Ordering::Relaxed));

        let array = Arc::new(Int64Array::from(vec![1]));
        let batch = RecordBatch::try_from_iter(vec![("col", array as _)]).unwrap();

        // Submit some events
        for i in 0..5 {
            reactor.submit(Event::new(i * 1000, batch.clone())).unwrap();
        }

        // Shutdown should process remaining events
        reactor.shutdown().unwrap();
        assert!(shutdown_handle.load(Ordering::Relaxed));
        assert_eq!(reactor.queue_size(), 0);
    }
}
