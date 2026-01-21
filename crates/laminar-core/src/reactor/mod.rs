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
use std::time::{Duration, Instant};

use crate::operator::{Event, Operator, OperatorContext, Output};
use crate::state::{InMemoryStore, StateStore};
use crate::time::{BoundedOutOfOrdernessGenerator, TimerService, WatermarkGenerator};

/// Configuration for the reactor
#[derive(Debug, Clone)]
pub struct Config {
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

impl Default for Config {
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
    config: Config,
    operators: Vec<Box<dyn Operator>>,
    timer_service: TimerService,
    event_queue: VecDeque<Event>,
    output_buffer: Vec<Output>,
    state_store: Box<dyn StateStore>,
    watermark_generator: Box<dyn WatermarkGenerator>,
    current_event_time: i64,
    start_time: Instant,
    events_processed: u64,
}

impl Reactor {
    /// Creates a new reactor with the given configuration
    ///
    /// # Errors
    ///
    /// Currently does not return any errors, but may in the future if initialization fails
    pub fn new(config: Config) -> Result<Self, ReactorError> {
        let event_queue = VecDeque::with_capacity(config.event_buffer_size);
        let watermark_generator = Box::new(
            BoundedOutOfOrdernessGenerator::new(config.max_out_of_orderness)
        );

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
        })
    }

    /// Register an operator in the processing chain
    pub fn add_operator(&mut self, operator: Box<dyn Operator>) {
        self.operators.push(operator);
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
        let poll_start = Instant::now();
        let processing_time = self.get_processing_time();

        // 1. Fire expired timers
        let fired_timers = self.timer_service.poll_timers(self.current_event_time);
        for timer in fired_timers {
            // Find the operator that registered this timer
            // For now, we'll process timers for all operators
            for (idx, operator) in self.operators.iter_mut().enumerate() {
                let timer_clone = crate::operator::Timer {
                    key: timer.key.clone().unwrap_or_default(),
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

                let outputs = operator.on_timer(timer_clone, &mut ctx);
                self.output_buffer.extend(outputs);
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
                self.output_buffer.push(Output::Watermark(watermark.timestamp()));
            }

            // Process through operator chain
            let mut current_outputs = vec![Output::Event(event.clone())];

            for (idx, operator) in self.operators.iter_mut().enumerate() {
                let mut next_outputs = Vec::new();

                for output in current_outputs {
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
                        next_outputs.extend(operator_outputs);
                    } else {
                        // Pass through watermarks and late events
                        next_outputs.push(output);
                    }
                }

                current_outputs = next_outputs;
            }

            self.output_buffer.extend(current_outputs);
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

    /// Get current processing time in microseconds since reactor start
    #[allow(clippy::cast_possible_truncation)]
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
    pub fn set_cpu_affinity(&self) -> Result<(), ReactorError> {
        if let Some(cpu_id) = self.config.cpu_affinity {
            // Note: CPU affinity setting is platform-specific
            // This would need platform-specific implementation
            // For now, we'll just return Ok
            eprintln!("CPU affinity setting to core {cpu_id} not implemented yet");
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

        loop {
            // Process events
            let outputs = self.poll();

            // In a real implementation, we would send outputs to sinks
            if !outputs.is_empty() {
                // For now, just count them
                let _ = outputs.len();
            }

            // If no events to process, yield to avoid busy-waiting
            if self.event_queue.is_empty() {
                std::thread::yield_now();
            }

            // Check for shutdown signal (placeholder)
            // In a real implementation, we would check a shutdown flag
            if false {
                break;
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
        // Process remaining events
        while !self.event_queue.is_empty() {
            let _ = self.poll();
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;

    // Mock operator for testing
    struct PassthroughOperator;

    impl Operator for PassthroughOperator {
        fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> Vec<Output> {
            vec![Output::Event(event.clone())]
        }

        fn on_timer(&mut self, _timer: crate::operator::Timer, _ctx: &mut OperatorContext) -> Vec<Output> {
            vec![]
        }

        fn checkpoint(&self) -> crate::operator::OperatorState {
            crate::operator::OperatorState {
                operator_id: "passthrough".to_string(),
                data: vec![],
            }
        }

        fn restore(&mut self, _state: crate::operator::OperatorState) -> Result<(), crate::operator::OperatorError> {
            Ok(())
        }
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.batch_size, 1024);
        assert_eq!(config.event_buffer_size, 65536);
    }

    #[test]
    fn test_reactor_creation() {
        let config = Config::default();
        let reactor = Reactor::new(config);
        assert!(reactor.is_ok());
    }

    #[test]
    fn test_reactor_add_operator() {
        let config = Config::default();
        let mut reactor = Reactor::new(config).unwrap();

        let operator = Box::new(PassthroughOperator);
        reactor.add_operator(operator);

        assert_eq!(reactor.operators.len(), 1);
    }

    #[test]
    fn test_reactor_submit_event() {
        let config = Config::default();
        let mut reactor = Reactor::new(config).unwrap();

        let array = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();
        let event = Event {
            timestamp: 12345,
            data: batch,
        };

        assert!(reactor.submit(event).is_ok());
        assert_eq!(reactor.queue_size(), 1);
    }

    #[test]
    fn test_reactor_poll_processes_events() {
        let config = Config::default();
        let mut reactor = Reactor::new(config).unwrap();

        // Add a passthrough operator
        reactor.add_operator(Box::new(PassthroughOperator));

        // Submit an event
        let array = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();
        let event = Event {
            timestamp: 12345,
            data: batch,
        };

        reactor.submit(event.clone()).unwrap();

        // Poll should process the event
        let outputs = reactor.poll();
        assert!(!outputs.is_empty());
        assert_eq!(reactor.events_processed(), 1);
        assert_eq!(reactor.queue_size(), 0);
    }

    #[test]
    fn test_reactor_queue_full() {
        let mut config = Config::default();
        config.event_buffer_size = 2; // Very small buffer
        let mut reactor = Reactor::new(config).unwrap();

        let array = Arc::new(Int64Array::from(vec![1]));
        let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();

        // Fill the queue
        for i in 0..2 {
            let event = Event {
                timestamp: i as i64,
                data: batch.clone(),
            };
            assert!(reactor.submit(event).is_ok());
        }

        // Next submit should fail
        let event = Event {
            timestamp: 100,
            data: batch,
        };
        assert!(matches!(reactor.submit(event), Err(ReactorError::QueueFull { .. })));
    }

    #[test]
    fn test_reactor_batch_processing() {
        let mut config = Config::default();
        config.batch_size = 2; // Small batch size
        let mut reactor = Reactor::new(config).unwrap();

        reactor.add_operator(Box::new(PassthroughOperator));

        let array = Arc::new(Int64Array::from(vec![1]));
        let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();

        // Submit 5 events
        for i in 0..5 {
            let event = Event {
                timestamp: i as i64,
                data: batch.clone(),
            };
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
}