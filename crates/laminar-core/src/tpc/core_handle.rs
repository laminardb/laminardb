//! # Core Handle
//!
//! Manages a single core's reactor thread with SPSC queue communication.
//!
//! ## Architecture
//!
//! Each `CoreHandle` spawns a dedicated thread that:
//! 1. Sets CPU affinity to pin to a specific core
//! 2. Creates a `Reactor` with its own state partition
//! 3. Optionally initializes an `io_uring` ring for async I/O (Linux only)
//! 4. Drains the inbox SPSC queue for incoming events
//! 5. Processes events through the reactor
//! 6. Pushes outputs to the outbox SPSC queue
//!
//! ## Communication
//!
//! - **Inbox**: Main thread → Core thread (events, watermarks, commands)
//! - **Outbox**: Core thread → Main thread (outputs)
//!
//! Both use lock-free SPSC queues for minimal latency.
//!
//! ## `io_uring` Integration
//!
//! On Linux with the `io-uring` feature enabled, each core thread can have its own
//! `io_uring` ring for high-performance async I/O. This enables:
//! - SQPOLL mode for syscall-free submission
//! - Registered buffers for zero-copy I/O
//! - Per-core I/O isolation for thread-per-core architecture

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
use crate::io_uring::{CoreRingManager, IoUringConfig};

use crate::alloc::HotPathGuard;
use crate::budget::TaskBudget;
use crate::numa::{NumaAllocator, NumaTopology};
use crate::operator::{Event, Operator, Output};
use crate::reactor::{Reactor, ReactorConfig};

use super::backpressure::{
    BackpressureConfig, CreditAcquireResult, CreditGate, CreditMetrics, OverflowStrategy,
};
use super::spsc::SpscQueue;
use super::TpcError;

/// Messages sent to a core thread.
#[derive(Debug)]
pub enum CoreMessage {
    /// Process an event
    Event(Event),
    /// Watermark advancement
    Watermark(i64),
    /// Request a checkpoint
    CheckpointRequest(u64),
    /// Graceful shutdown
    Shutdown,
}

/// Configuration for a core handle.
#[derive(Debug, Clone)]
pub struct CoreConfig {
    /// Core ID (0-indexed)
    pub core_id: usize,
    /// CPU ID to pin to (None = no pinning)
    pub cpu_affinity: Option<usize>,
    /// Inbox queue capacity
    pub inbox_capacity: usize,
    /// Outbox queue capacity
    pub outbox_capacity: usize,
    /// Reactor configuration
    pub reactor_config: ReactorConfig,
    /// Backpressure configuration
    pub backpressure: BackpressureConfig,
    /// Enable NUMA-aware memory allocation
    pub numa_aware: bool,
    /// `io_uring` configuration (Linux only, requires `io-uring` feature)
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    pub io_uring_config: Option<IoUringConfig>,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            core_id: 0,
            cpu_affinity: None,
            inbox_capacity: 65536,
            outbox_capacity: 65536,
            reactor_config: ReactorConfig::default(),
            backpressure: BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        }
    }
}

/// Handle to a core's reactor thread.
///
/// Provides lock-free communication with the core via SPSC queues
/// and credit-based flow control for backpressure.
pub struct CoreHandle {
    /// Core ID
    core_id: usize,
    /// NUMA node for this core
    numa_node: usize,
    /// Inbox queue (main thread writes, core reads)
    inbox: Arc<SpscQueue<CoreMessage>>,
    /// Outbox queue (core writes, main thread reads)
    outbox: Arc<SpscQueue<Output>>,
    /// Credit gate for backpressure (sender acquires, receiver releases)
    credit_gate: Arc<CreditGate>,
    /// Thread handle (None if thread hasn't started or has been joined)
    thread: Option<JoinHandle<Result<(), TpcError>>>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Events processed counter
    events_processed: Arc<AtomicU64>,
    /// Outputs dropped due to full outbox
    outputs_dropped: Arc<AtomicU64>,
    /// Running state
    is_running: Arc<AtomicBool>,
}

impl CoreHandle {
    /// Spawns a new core thread with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the thread cannot be spawned.
    pub fn spawn(config: CoreConfig) -> Result<Self, TpcError> {
        Self::spawn_with_operators(config, Vec::new())
    }

    /// Spawns a new core thread with operators.
    ///
    /// # Errors
    ///
    /// Returns an error if the thread cannot be spawned.
    #[allow(clippy::needless_pass_by_value)]
    pub fn spawn_with_operators(
        config: CoreConfig,
        operators: Vec<Box<dyn Operator>>,
    ) -> Result<Self, TpcError> {
        let core_id = config.core_id;
        let cpu_affinity = config.cpu_affinity;
        let reactor_config = config.reactor_config.clone();

        // Detect NUMA topology for NUMA-aware allocation
        let topology = NumaTopology::detect();
        let numa_node =
            cpu_affinity.map_or_else(|| topology.current_node(), |cpu| topology.node_for_cpu(cpu));

        let inbox = Arc::new(SpscQueue::new(config.inbox_capacity));
        let outbox = Arc::new(SpscQueue::new(config.outbox_capacity));
        let credit_gate = Arc::new(CreditGate::new(config.backpressure.clone()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let events_processed = Arc::new(AtomicU64::new(0));
        let outputs_dropped = Arc::new(AtomicU64::new(0));
        let is_running = Arc::new(AtomicBool::new(false));

        let thread_context = CoreThreadContext {
            core_id,
            cpu_affinity,
            reactor_config,
            numa_aware: config.numa_aware,
            numa_node,
            inbox: Arc::clone(&inbox),
            outbox: Arc::clone(&outbox),
            credit_gate: Arc::clone(&credit_gate),
            shutdown: Arc::clone(&shutdown),
            events_processed: Arc::clone(&events_processed),
            outputs_dropped: Arc::clone(&outputs_dropped),
            is_running: Arc::clone(&is_running),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: config.io_uring_config,
        };

        let thread = thread::Builder::new()
            .name(format!("laminar-core-{core_id}"))
            .spawn(move || core_thread_main(&thread_context, operators))
            .map_err(|e| TpcError::SpawnFailed {
                core_id,
                message: e.to_string(),
            })?;

        // Wait for thread to signal it's running
        while !is_running.load(Ordering::Acquire) {
            thread::yield_now();
        }

        Ok(Self {
            core_id,
            numa_node,
            inbox,
            outbox,
            credit_gate,
            thread: Some(thread),
            shutdown,
            events_processed,
            outputs_dropped,
            is_running,
        })
    }

    /// Returns the core ID.
    #[must_use]
    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Returns the NUMA node for this core.
    #[must_use]
    pub fn numa_node(&self) -> usize {
        self.numa_node
    }

    /// Returns true if the core thread is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    /// Returns the number of events processed by this core.
    #[must_use]
    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    /// Returns the number of outputs dropped due to a full outbox.
    #[must_use]
    pub fn outputs_dropped(&self) -> u64 {
        self.outputs_dropped.load(Ordering::Relaxed)
    }

    /// Sends a message to the core with credit-based flow control.
    ///
    /// This method respects the backpressure configuration:
    /// - `Block`: Spins until credits available, then sends
    /// - `Drop`: Returns Ok but drops the message if no credits
    /// - `Error`: Returns error if no credits available
    ///
    /// # Errors
    ///
    /// Returns an error if the inbox queue is full or credits exhausted (with Error strategy).
    pub fn send(&self, message: CoreMessage) -> Result<(), TpcError> {
        // Try to acquire a credit
        match self.credit_gate.try_acquire() {
            CreditAcquireResult::Acquired => {
                // Have credit, try to push
                self.inbox.push(message).map_err(|_| TpcError::QueueFull {
                    core_id: self.core_id,
                })
            }
            CreditAcquireResult::WouldBlock => {
                // Check overflow strategy
                if self.credit_gate.config().overflow_strategy == OverflowStrategy::Block {
                    // Spin until we get a credit
                    self.credit_gate.acquire_blocking(1);
                    self.inbox.push(message).map_err(|_| TpcError::QueueFull {
                        core_id: self.core_id,
                    })
                } else {
                    // Error strategy - return error
                    Err(TpcError::Backpressure {
                        core_id: self.core_id,
                    })
                }
            }
            CreditAcquireResult::Dropped => {
                // Drop strategy - silently drop, already recorded in metrics
                Ok(())
            }
        }
    }

    /// Tries to send a message without blocking.
    ///
    /// Returns `Err` if no credits available or queue is full.
    /// Does not block regardless of overflow strategy.
    ///
    /// # Errors
    ///
    /// Returns an error if credits exhausted or queue full.
    pub fn try_send(&self, message: CoreMessage) -> Result<(), TpcError> {
        match self.credit_gate.try_acquire() {
            CreditAcquireResult::Acquired => {
                self.inbox.push(message).map_err(|_| TpcError::QueueFull {
                    core_id: self.core_id,
                })
            }
            CreditAcquireResult::WouldBlock | CreditAcquireResult::Dropped => {
                Err(TpcError::Backpressure {
                    core_id: self.core_id,
                })
            }
        }
    }

    /// Sends an event to the core with credit-based flow control.
    ///
    /// # Errors
    ///
    /// Returns an error if the inbox queue is full or backpressure applies.
    pub fn send_event(&self, event: Event) -> Result<(), TpcError> {
        self.send(CoreMessage::Event(event))
    }

    /// Tries to send an event without blocking.
    ///
    /// # Errors
    ///
    /// Returns an error if credits exhausted or queue full.
    pub fn try_send_event(&self, event: Event) -> Result<(), TpcError> {
        self.try_send(CoreMessage::Event(event))
    }

    /// Polls the outbox for outputs.
    ///
    /// Returns up to `max_count` outputs.
    ///
    /// # Note
    ///
    /// This method allocates memory. For zero-allocation polling, use
    /// [`poll_outputs_into`](Self::poll_outputs_into) or [`poll_each`](Self::poll_each) instead.
    #[must_use]
    pub fn poll_outputs(&self, max_count: usize) -> Vec<Output> {
        self.outbox.pop_batch(max_count)
    }

    /// Polls the outbox for outputs into a pre-allocated buffer (zero-allocation).
    ///
    /// Outputs are appended to `buffer`. Returns the number of outputs added.
    /// The buffer should have sufficient capacity to avoid reallocation.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut buffer = Vec::with_capacity(1024);
    ///
    /// // First poll - fills buffer
    /// let count1 = core_handle.poll_outputs_into(&mut buffer, 100);
    ///
    /// // Process outputs...
    /// for output in &buffer[..count1] {
    ///     process(output);
    /// }
    ///
    /// // Clear and reuse buffer for next poll (no allocation)
    /// buffer.clear();
    /// let count2 = core_handle.poll_outputs_into(&mut buffer, 100);
    /// ```
    #[inline]
    pub fn poll_outputs_into(&self, buffer: &mut Vec<Output>, max_count: usize) -> usize {
        let start_len = buffer.len();

        self.outbox.pop_each(max_count, |output| {
            buffer.push(output);
            true
        });

        buffer.len() - start_len
    }

    /// Polls the outbox with a callback for each output (zero-allocation).
    ///
    /// Processing stops when:
    /// - `max_count` outputs have been processed
    /// - The outbox becomes empty
    /// - The callback returns `false`
    ///
    /// Returns the number of outputs processed.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Process outputs without any allocation
    /// let count = core_handle.poll_each(100, |output| {
    ///     match output {
    ///         Output::Event(event) => handle_event(event),
    ///         _ => {}
    ///     }
    ///     true // Continue processing
    /// });
    /// ```
    #[inline]
    pub fn poll_each<F>(&self, max_count: usize, f: F) -> usize
    where
        F: FnMut(Output) -> bool,
    {
        self.outbox.pop_each(max_count, f)
    }

    /// Polls a single output from the outbox.
    #[must_use]
    pub fn poll_output(&self) -> Option<Output> {
        self.outbox.pop()
    }

    /// Returns the number of pending messages in the inbox.
    #[must_use]
    pub fn inbox_len(&self) -> usize {
        self.inbox.len()
    }

    /// Returns the number of pending outputs in the outbox.
    #[must_use]
    pub fn outbox_len(&self) -> usize {
        self.outbox.len()
    }

    /// Returns true if backpressure is currently active.
    #[must_use]
    pub fn is_backpressured(&self) -> bool {
        self.credit_gate.is_backpressured()
    }

    /// Returns the number of available credits.
    #[must_use]
    pub fn available_credits(&self) -> usize {
        self.credit_gate.available()
    }

    /// Returns the maximum credits configured.
    #[must_use]
    pub fn max_credits(&self) -> usize {
        self.credit_gate.max_credits()
    }

    /// Returns the credit metrics.
    #[must_use]
    pub fn credit_metrics(&self) -> &CreditMetrics {
        self.credit_gate.metrics()
    }

    /// Signals the core to shut down gracefully.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        // Also send a shutdown message to wake up the thread
        let _ = self.inbox.push(CoreMessage::Shutdown);
    }

    /// Waits for the core thread to finish.
    ///
    /// # Errors
    ///
    /// Returns an error if the thread panicked or returned an error.
    pub fn join(mut self) -> Result<(), TpcError> {
        if let Some(handle) = self.thread.take() {
            handle.join().map_err(|_| TpcError::SpawnFailed {
                core_id: self.core_id,
                message: "Thread panicked".to_string(),
            })?
        } else {
            Ok(())
        }
    }

    /// Sends a shutdown signal and waits for the thread to finish.
    ///
    /// # Errors
    ///
    /// Returns an error if the thread cannot be joined cleanly.
    pub fn shutdown_and_join(self) -> Result<(), TpcError> {
        self.shutdown();
        self.join()
    }
}

impl Drop for CoreHandle {
    fn drop(&mut self) {
        // Signal shutdown if not already done
        self.shutdown.store(true, Ordering::Release);
        // Try to send shutdown message (may fail if queue is full, that's OK)
        let _ = self.inbox.push(CoreMessage::Shutdown);

        // Join the thread if we haven't already
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl std::fmt::Debug for CoreHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreHandle")
            .field("core_id", &self.core_id)
            .field("numa_node", &self.numa_node)
            .field("is_running", &self.is_running())
            .field("events_processed", &self.events_processed())
            .field("outputs_dropped", &self.outputs_dropped())
            .field("inbox_len", &self.inbox_len())
            .field("outbox_len", &self.outbox_len())
            .field("available_credits", &self.available_credits())
            .field("is_backpressured", &self.is_backpressured())
            .finish_non_exhaustive()
    }
}

/// Context passed to the core thread.
struct CoreThreadContext {
    core_id: usize,
    cpu_affinity: Option<usize>,
    reactor_config: ReactorConfig,
    numa_aware: bool,
    numa_node: usize,
    inbox: Arc<SpscQueue<CoreMessage>>,
    outbox: Arc<SpscQueue<Output>>,
    credit_gate: Arc<CreditGate>,
    shutdown: Arc<AtomicBool>,
    events_processed: Arc<AtomicU64>,
    outputs_dropped: Arc<AtomicU64>,
    is_running: Arc<AtomicBool>,
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    io_uring_config: Option<IoUringConfig>,
}

/// Initializes the core thread: sets CPU affinity, NUMA allocator, `io_uring`, and creates the reactor.
fn init_core_thread(
    ctx: &CoreThreadContext,
    operators: Vec<Box<dyn Operator>>,
) -> Result<Reactor, TpcError> {
    // Set CPU affinity if requested
    if let Some(cpu_id) = ctx.cpu_affinity {
        set_cpu_affinity(ctx.core_id, cpu_id)?;
    }

    // Log NUMA information if NUMA-aware mode is enabled
    if ctx.numa_aware {
        tracing::info!(
            "Core {} starting on NUMA node {}",
            ctx.core_id,
            ctx.numa_node
        );
    }

    // Create NUMA allocator for this core (optional - for future state store integration)
    if ctx.numa_aware {
        let topology = NumaTopology::detect();
        let _numa_allocator = NumaAllocator::new(&topology);
    }

    // Initialize io_uring ring manager if configured (Linux only)
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    let _ring_manager = if let Some(ref io_uring_config) = ctx.io_uring_config {
        match CoreRingManager::new(ctx.core_id, io_uring_config) {
            Ok(manager) => Some(manager),
            Err(e) => {
                tracing::error!(
                    "Core {}: Failed to initialize io_uring ring: {e}. Falling back to standard I/O.",
                    ctx.core_id
                );
                None
            }
        }
    } else {
        None
    };

    // Create the reactor with configured settings
    let mut reactor_config = ctx.reactor_config.clone();
    reactor_config.cpu_affinity = ctx.cpu_affinity;

    let mut reactor = Reactor::new(reactor_config).map_err(|e| TpcError::ReactorError {
        core_id: ctx.core_id,
        source: e,
    })?;

    // Add operators
    for op in operators {
        reactor.add_operator(op);
    }

    Ok(reactor)
}

/// Main function for the core thread.
fn core_thread_main(
    ctx: &CoreThreadContext,
    operators: Vec<Box<dyn Operator>>,
) -> Result<(), TpcError> {
    let mut reactor = init_core_thread(ctx, operators)?;

    // Signal that we're running
    ctx.is_running.store(true, Ordering::Release);

    // Main loop
    loop {
        // Check for shutdown
        if ctx.shutdown.load(Ordering::Acquire) {
            break;
        }

        // Hot path guard for inbox processing
        let _guard = HotPathGuard::enter("CoreThread::process_inbox");

        // Task budget tracking for batch processing
        let batch_budget = TaskBudget::ring0_batch();

        // Drain inbox and track messages processed for credit release
        let mut had_work = false;
        let mut messages_processed = 0usize;

        while let Some(message) = ctx.inbox.pop() {
            match message {
                CoreMessage::Event(event) => {
                    if let Err(e) = reactor.submit(event) {
                        tracing::error!("Core {}: Failed to submit event: {e}", ctx.core_id);
                    }
                    messages_processed += 1;
                    had_work = true;
                }
                CoreMessage::Watermark(timestamp) => {
                    // Advance the reactor's watermark so downstream operators
                    // see the updated event-time progress on the next poll().
                    reactor.advance_watermark(timestamp);
                    messages_processed += 1;
                    had_work = true;
                }
                CoreMessage::CheckpointRequest(checkpoint_id) => {
                    // Snapshot all operator states and push to outbox
                    // for Ring 1 to persist via WAL + RocksDB.
                    let operator_states = reactor.trigger_checkpoint();
                    let checkpoint_output = Output::CheckpointComplete {
                        checkpoint_id,
                        operator_states,
                    };
                    if ctx.outbox.push(checkpoint_output).is_err() {
                        ctx.outputs_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                    messages_processed += 1;
                    had_work = true;
                }
                CoreMessage::Shutdown => {
                    // Release credits for any messages we've processed so far
                    if messages_processed > 0 {
                        ctx.credit_gate.release(messages_processed);
                    }
                    break;
                }
            }

            // Check if batch budget is almost exceeded and break to process reactor
            // This ensures Ring 0 latency guarantees by limiting batch processing time
            if batch_budget.almost_exceeded() {
                break;
            }
        }

        // Release credits for processed messages
        // This signals to senders that we have capacity for more
        if messages_processed > 0 {
            ctx.credit_gate.release(messages_processed);
        }

        // Process events in reactor
        let outputs = reactor.poll();
        ctx.events_processed
            .fetch_add(outputs.len() as u64, Ordering::Relaxed);

        // Push outputs to outbox
        for output in outputs {
            if ctx.outbox.push(output).is_err() {
                ctx.outputs_dropped.fetch_add(1, Ordering::Relaxed);
            }
            had_work = true;
        }

        // If no work was done, yield to avoid busy-waiting
        if !had_work {
            thread::yield_now();
        }
    }

    // Drain any remaining events before shutdown
    let outputs = reactor.poll();
    for output in outputs {
        let _ = ctx.outbox.push(output);
    }

    ctx.is_running.store(false, Ordering::Release);
    Ok(())
}

/// Sets CPU affinity for the current thread.
fn set_cpu_affinity(core_id: usize, cpu_id: usize) -> Result<(), TpcError> {
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

            let result = sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &raw const set);
            if result != 0 {
                return Err(TpcError::AffinityFailed {
                    core_id,
                    message: format!(
                        "sched_setaffinity failed: {}",
                        std::io::Error::last_os_error()
                    ),
                });
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
                return Err(TpcError::AffinityFailed {
                    core_id,
                    message: format!(
                        "SetThreadAffinityMask failed: {}",
                        std::io::Error::last_os_error()
                    ),
                });
            }
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    {
        let _ = (core_id, cpu_id);
        // No-op on other platforms
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::{OperatorState, OutputVec, Timer};
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;
    use std::time::Duration;

    // Simple passthrough operator for testing
    struct PassthroughOperator;

    impl Operator for PassthroughOperator {
        fn process(
            &mut self,
            event: &Event,
            _ctx: &mut crate::operator::OperatorContext,
        ) -> OutputVec {
            let mut output = OutputVec::new();
            output.push(Output::Event(event.clone()));
            output
        }

        fn on_timer(
            &mut self,
            _timer: Timer,
            _ctx: &mut crate::operator::OperatorContext,
        ) -> OutputVec {
            OutputVec::new()
        }

        fn checkpoint(&self) -> OperatorState {
            OperatorState {
                operator_id: "passthrough".to_string(),
                data: vec![],
            }
        }

        fn restore(&mut self, _state: OperatorState) -> Result<(), crate::operator::OperatorError> {
            Ok(())
        }
    }

    fn make_event(value: i64) -> Event {
        let array = Arc::new(Int64Array::from(vec![value]));
        let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
        Event::new(value, batch)
    }

    #[test]
    fn test_core_handle_spawn() {
        let config = CoreConfig {
            core_id: 0,
            cpu_affinity: None, // Don't pin in tests
            inbox_capacity: 1024,
            outbox_capacity: 1024,
            reactor_config: ReactorConfig::default(),
            backpressure: super::BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        };

        let handle = CoreHandle::spawn(config).unwrap();
        assert!(handle.is_running());
        assert_eq!(handle.core_id(), 0);

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_core_handle_with_operator() {
        let config = CoreConfig {
            core_id: 0,
            cpu_affinity: None,
            inbox_capacity: 1024,
            outbox_capacity: 1024,
            reactor_config: ReactorConfig::default(),
            backpressure: super::BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        };

        let handle =
            CoreHandle::spawn_with_operators(config, vec![Box::new(PassthroughOperator)]).unwrap();

        // Send an event
        let event = make_event(42);
        handle.send_event(event).unwrap();

        // Wait a bit for processing
        thread::sleep(Duration::from_millis(50));

        // Poll for outputs
        let outputs = handle.poll_outputs(10);
        assert!(!outputs.is_empty());

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_core_handle_multiple_events() {
        let config = CoreConfig {
            core_id: 1,
            cpu_affinity: None,
            inbox_capacity: 1024,
            outbox_capacity: 1024,
            reactor_config: ReactorConfig::default(),
            backpressure: super::BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        };

        let handle =
            CoreHandle::spawn_with_operators(config, vec![Box::new(PassthroughOperator)]).unwrap();

        // Send multiple events
        for i in 0..100 {
            handle.send_event(make_event(i)).unwrap();
        }

        // Wait for processing
        thread::sleep(Duration::from_millis(100));

        // Poll all outputs
        let mut total_outputs = 0;
        loop {
            let outputs = handle.poll_outputs(1000);
            if outputs.is_empty() {
                break;
            }
            total_outputs += outputs.len();
        }

        // Should have at least some outputs (may have watermarks too)
        assert!(total_outputs >= 100);

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_core_handle_shutdown() {
        let config = CoreConfig::default();
        let handle = CoreHandle::spawn(config).unwrap();

        assert!(handle.is_running());

        handle.shutdown();

        // Wait for shutdown
        thread::sleep(Duration::from_millis(100));

        // Thread should have stopped
        assert!(!handle.is_running());
    }

    #[test]
    fn test_core_handle_debug() {
        let config = CoreConfig {
            core_id: 42,
            ..Default::default()
        };
        let handle = CoreHandle::spawn(config).unwrap();

        let debug_str = format!("{handle:?}");
        assert!(debug_str.contains("CoreHandle"));
        assert!(debug_str.contains("42"));

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_core_config_default() {
        let config = CoreConfig::default();
        assert_eq!(config.core_id, 0);
        assert!(config.cpu_affinity.is_none());
        assert_eq!(config.inbox_capacity, 65536);
        assert_eq!(config.outbox_capacity, 65536);
        assert!(!config.numa_aware);
    }

    #[test]
    fn test_core_handle_numa_node() {
        let config = CoreConfig {
            core_id: 0,
            cpu_affinity: None,
            numa_aware: true,
            ..Default::default()
        };

        let handle = CoreHandle::spawn(config).unwrap();
        // On any system, numa_node should be a valid value (0 on non-NUMA systems)
        assert!(handle.numa_node() < 64);

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_poll_outputs_into() {
        let config = CoreConfig {
            core_id: 0,
            cpu_affinity: None,
            inbox_capacity: 1024,
            outbox_capacity: 1024,
            reactor_config: ReactorConfig::default(),
            backpressure: super::BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        };

        let handle =
            CoreHandle::spawn_with_operators(config, vec![Box::new(PassthroughOperator)]).unwrap();

        // Send events
        for i in 0..10 {
            handle.send_event(make_event(i)).unwrap();
        }

        // Wait for processing
        thread::sleep(Duration::from_millis(100));

        // Poll into pre-allocated buffer
        let mut buffer = Vec::with_capacity(100);
        let count = handle.poll_outputs_into(&mut buffer, 100);

        assert!(count > 0);
        assert_eq!(buffer.len(), count);

        // Reuse buffer - no new allocation
        let cap_before = buffer.capacity();
        buffer.clear();
        let _ = handle.poll_outputs_into(&mut buffer, 100);
        assert_eq!(buffer.capacity(), cap_before); // Capacity unchanged

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_poll_each() {
        let config = CoreConfig {
            core_id: 0,
            cpu_affinity: None,
            inbox_capacity: 1024,
            outbox_capacity: 1024,
            reactor_config: ReactorConfig::default(),
            backpressure: super::BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        };

        let handle =
            CoreHandle::spawn_with_operators(config, vec![Box::new(PassthroughOperator)]).unwrap();

        // Send events
        for i in 0..10 {
            handle.send_event(make_event(i)).unwrap();
        }

        // Wait for processing
        thread::sleep(Duration::from_millis(100));

        // Poll with callback
        let mut event_count = 0;
        let count = handle.poll_each(100, |output| {
            if matches!(output, Output::Event(_)) {
                event_count += 1;
            }
            true
        });

        assert!(count > 0);
        assert!(event_count > 0);

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_poll_each_early_stop() {
        let config = CoreConfig {
            core_id: 0,
            cpu_affinity: None,
            inbox_capacity: 1024,
            outbox_capacity: 1024,
            reactor_config: ReactorConfig::default(),
            backpressure: super::BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        };

        let handle =
            CoreHandle::spawn_with_operators(config, vec![Box::new(PassthroughOperator)]).unwrap();

        // Send events
        for i in 0..20 {
            handle.send_event(make_event(i)).unwrap();
        }

        // Wait for processing
        thread::sleep(Duration::from_millis(100));

        // Poll with early stop after 5 items
        let mut processed = 0;
        let count = handle.poll_each(100, |_| {
            processed += 1;
            processed < 5 // Stop after 5
        });

        assert_eq!(count, 5);
        assert_eq!(processed, 5);

        // There should be more items remaining
        let remaining = handle.outbox_len();
        assert!(remaining > 0);

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_watermark_propagation() {
        let config = CoreConfig {
            core_id: 0,
            cpu_affinity: None,
            inbox_capacity: 1024,
            outbox_capacity: 1024,
            reactor_config: ReactorConfig::default(),
            backpressure: super::BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        };

        let handle =
            CoreHandle::spawn_with_operators(config, vec![Box::new(PassthroughOperator)]).unwrap();

        // Send a watermark advancement
        handle.send(CoreMessage::Watermark(5000)).unwrap();

        // Wait for processing
        thread::sleep(Duration::from_millis(50));

        // Poll outputs - should contain a watermark
        let outputs = handle.poll_outputs(100);
        let has_watermark = outputs.iter().any(|o| matches!(o, Output::Watermark(_)));
        assert!(
            has_watermark,
            "Expected watermark output after Watermark message"
        );

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_checkpoint_triggering() {
        let config = CoreConfig {
            core_id: 0,
            cpu_affinity: None,
            inbox_capacity: 1024,
            outbox_capacity: 1024,
            reactor_config: ReactorConfig::default(),
            backpressure: super::BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        };

        let handle =
            CoreHandle::spawn_with_operators(config, vec![Box::new(PassthroughOperator)]).unwrap();

        // Send a checkpoint request
        handle.send(CoreMessage::CheckpointRequest(42)).unwrap();

        // Wait for processing
        thread::sleep(Duration::from_millis(50));

        // Poll outputs - should contain a CheckpointComplete
        let outputs = handle.poll_outputs(100);
        let checkpoint = outputs
            .iter()
            .find(|o| matches!(o, Output::CheckpointComplete { .. }));
        assert!(checkpoint.is_some(), "Expected CheckpointComplete output");

        if let Some(Output::CheckpointComplete {
            checkpoint_id,
            operator_states,
        }) = checkpoint
        {
            assert_eq!(*checkpoint_id, 42);
            // One operator (PassthroughOperator)
            assert_eq!(operator_states.len(), 1);
            assert_eq!(operator_states[0].operator_id, "passthrough");
        }

        handle.shutdown_and_join().unwrap();
    }

    #[test]
    fn test_outputs_dropped_counter() {
        let config = CoreConfig {
            core_id: 0,
            cpu_affinity: None,
            inbox_capacity: 1024,
            outbox_capacity: 4, // Very small outbox to force drops
            reactor_config: ReactorConfig::default(),
            backpressure: super::BackpressureConfig::default(),
            numa_aware: false,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        };

        let handle =
            CoreHandle::spawn_with_operators(config, vec![Box::new(PassthroughOperator)]).unwrap();

        // Send many events without polling - outbox should fill up
        for i in 0..100 {
            let _ = handle.send_event(make_event(i));
        }

        // Wait for processing to fill and overflow the outbox
        thread::sleep(Duration::from_millis(200));

        // Some outputs should have been dropped
        let dropped = handle.outputs_dropped();
        assert!(
            dropped > 0,
            "Expected some outputs to be dropped with outbox_capacity=4"
        );

        handle.shutdown_and_join().unwrap();
    }
}
