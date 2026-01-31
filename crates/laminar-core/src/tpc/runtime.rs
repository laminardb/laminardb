//! # Thread-Per-Core Runtime
//!
//! Orchestrates multiple cores for parallel event processing with state locality.
//!
//! ## Architecture
//!
//! The runtime manages N reactors (one per core), routing events by key hash
//! to ensure state locality. Each core processes events independently with
//! no cross-core synchronization on the hot path.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use laminar_core::tpc::{TpcConfig, ThreadPerCoreRuntime, KeySpec};
//!
//! // Create runtime with 4 cores
//! let config = TpcConfig::builder()
//!     .num_cores(4)
//!     .key_spec(KeySpec::Columns(vec!["user_id".to_string()]))
//!     .build()?;
//!
//! let mut runtime = ThreadPerCoreRuntime::new(config)?;
//!
//! // Submit events
//! runtime.submit(event)?;
//!
//! // Process and collect outputs
//! let outputs = runtime.poll();
//! ```

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::operator::{Event, Operator, Output};
use crate::reactor::ReactorConfig;

// OutputBuffer - Pre-allocated buffer for zero-allocation polling

/// A pre-allocated buffer for collecting outputs without allocation.
///
/// This buffer can be reused across multiple poll cycles, avoiding
/// memory allocation on the hot path.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::tpc::OutputBuffer;
///
/// // Create buffer once at startup
/// let mut buffer = OutputBuffer::with_capacity(1024);
///
/// // Poll loop - no allocation after warmup
/// loop {
///     let count = runtime.poll_into(&mut buffer, 256);
///     for output in buffer.iter() {
///         process(output);
///     }
///     buffer.clear();
/// }
/// ```
#[derive(Debug)]
pub struct OutputBuffer {
    /// Internal storage (pre-allocated)
    items: Vec<Output>,
}

impl OutputBuffer {
    /// Creates a new output buffer with the given capacity.
    ///
    /// The buffer will not allocate until `capacity` items are added.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            items: Vec::with_capacity(capacity),
        }
    }

    /// Clears the buffer for reuse (no deallocation).
    ///
    /// The capacity remains unchanged, allowing zero-allocation reuse.
    #[inline]
    pub fn clear(&mut self) {
        self.items.clear();
    }

    /// Returns the number of items in the buffer.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true if the buffer is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the current capacity of the buffer.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.items.capacity()
    }

    /// Returns the remaining capacity before reallocation.
    #[inline]
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.items.capacity() - self.items.len()
    }

    /// Returns a slice of the collected outputs.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[Output] {
        &self.items
    }

    /// Returns an iterator over the outputs.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Output> {
        self.items.iter()
    }

    /// Consumes the buffer and returns the inner Vec.
    #[must_use]
    pub fn into_vec(self) -> Vec<Output> {
        self.items
    }

    /// Extends the buffer with outputs from an iterator.
    ///
    /// Note: This may allocate if the iterator produces more items than
    /// the remaining capacity.
    #[inline]
    pub fn extend<I: IntoIterator<Item = Output>>(&mut self, iter: I) {
        self.items.extend(iter);
    }

    /// Pushes a single output to the buffer.
    ///
    /// Note: This may allocate if the buffer is at capacity.
    #[inline]
    pub fn push(&mut self, output: Output) {
        self.items.push(output);
    }

    /// Returns a mutable reference to the internal Vec.
    ///
    /// This is useful for passing to functions that expect `&mut Vec<Output>`.
    #[inline]
    pub fn as_vec_mut(&mut self) -> &mut Vec<Output> {
        &mut self.items
    }
}

impl Default for OutputBuffer {
    fn default() -> Self {
        Self::with_capacity(1024)
    }
}

impl Deref for OutputBuffer {
    type Target = [Output];

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl<'a> IntoIterator for &'a OutputBuffer {
    type Item = &'a Output;
    type IntoIter = std::slice::Iter<'a, Output>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

impl IntoIterator for OutputBuffer {
    type Item = Output;
    type IntoIter = std::vec::IntoIter<Output>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

use super::core_handle::{CoreConfig, CoreHandle};
use super::router::{KeyRouter, KeySpec};
use super::TpcError;

/// Configuration for the thread-per-core runtime.
#[derive(Debug, Clone)]
pub struct TpcConfig {
    /// Number of cores to use
    pub num_cores: usize,
    /// Key specification for routing
    pub key_spec: KeySpec,
    /// Whether to pin cores to CPUs
    pub cpu_pinning: bool,
    /// Starting CPU ID for pinning (cores use `cpu_start`, `cpu_start+1`, ...)
    pub cpu_start: usize,
    /// Inbox queue capacity per core
    pub inbox_capacity: usize,
    /// Outbox queue capacity per core
    pub outbox_capacity: usize,
    /// Reactor configuration (applied to all cores)
    pub reactor_config: ReactorConfig,
    /// Enable NUMA-aware memory allocation
    pub numa_aware: bool,
}

impl Default for TpcConfig {
    fn default() -> Self {
        Self {
            num_cores: num_cpus::get(),
            key_spec: KeySpec::RoundRobin,
            cpu_pinning: false,
            cpu_start: 0,
            inbox_capacity: 65536,
            outbox_capacity: 65536,
            reactor_config: ReactorConfig::default(),
            numa_aware: false,
        }
    }
}

impl TpcConfig {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> TpcConfigBuilder {
        TpcConfigBuilder::default()
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<(), TpcError> {
        if self.num_cores == 0 {
            return Err(TpcError::InvalidConfig("num_cores must be > 0".to_string()));
        }
        if self.inbox_capacity == 0 {
            return Err(TpcError::InvalidConfig("inbox_capacity must be > 0".to_string()));
        }
        if self.outbox_capacity == 0 {
            return Err(TpcError::InvalidConfig("outbox_capacity must be > 0".to_string()));
        }
        Ok(())
    }
}

/// Builder for `TpcConfig`.
#[derive(Debug, Default)]
pub struct TpcConfigBuilder {
    num_cores: Option<usize>,
    key_spec: Option<KeySpec>,
    cpu_pinning: Option<bool>,
    cpu_start: Option<usize>,
    inbox_capacity: Option<usize>,
    outbox_capacity: Option<usize>,
    reactor_config: Option<ReactorConfig>,
    numa_aware: Option<bool>,
}

impl TpcConfigBuilder {
    /// Sets the number of cores.
    #[must_use]
    pub fn num_cores(mut self, n: usize) -> Self {
        self.num_cores = Some(n);
        self
    }

    /// Sets the key specification for routing.
    #[must_use]
    pub fn key_spec(mut self, spec: KeySpec) -> Self {
        self.key_spec = Some(spec);
        self
    }

    /// Sets key columns for routing (convenience method).
    #[must_use]
    pub fn key_columns(self, columns: Vec<String>) -> Self {
        self.key_spec(KeySpec::Columns(columns))
    }

    /// Enables or disables CPU pinning.
    #[must_use]
    pub fn cpu_pinning(mut self, enabled: bool) -> Self {
        self.cpu_pinning = Some(enabled);
        self
    }

    /// Sets the starting CPU ID for pinning.
    #[must_use]
    pub fn cpu_start(mut self, cpu: usize) -> Self {
        self.cpu_start = Some(cpu);
        self
    }

    /// Sets the inbox capacity per core.
    #[must_use]
    pub fn inbox_capacity(mut self, capacity: usize) -> Self {
        self.inbox_capacity = Some(capacity);
        self
    }

    /// Sets the outbox capacity per core.
    #[must_use]
    pub fn outbox_capacity(mut self, capacity: usize) -> Self {
        self.outbox_capacity = Some(capacity);
        self
    }

    /// Sets the reactor configuration.
    #[must_use]
    pub fn reactor_config(mut self, config: ReactorConfig) -> Self {
        self.reactor_config = Some(config);
        self
    }

    /// Enables or disables NUMA-aware memory allocation.
    ///
    /// When enabled, per-core state stores and buffers are allocated
    /// on the NUMA node local to that core, improving memory access latency.
    #[must_use]
    pub fn numa_aware(mut self, enabled: bool) -> Self {
        self.numa_aware = Some(enabled);
        self
    }

    /// Builds the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn build(self) -> Result<TpcConfig, TpcError> {
        let config = TpcConfig {
            num_cores: self.num_cores.unwrap_or_else(num_cpus::get),
            key_spec: self.key_spec.unwrap_or_default(),
            cpu_pinning: self.cpu_pinning.unwrap_or(false),
            cpu_start: self.cpu_start.unwrap_or(0),
            inbox_capacity: self.inbox_capacity.unwrap_or(65536),
            outbox_capacity: self.outbox_capacity.unwrap_or(65536),
            reactor_config: self.reactor_config.unwrap_or_default(),
            numa_aware: self.numa_aware.unwrap_or(false),
        };
        config.validate()?;
        Ok(config)
    }
}

/// Factory for creating operators for each core.
///
/// This trait allows the runtime to create separate operator instances
/// for each core, ensuring no shared state between cores.
pub trait OperatorFactory: Send {
    /// Creates operators for a specific core.
    fn create(&self, core_id: usize) -> Vec<Box<dyn Operator>>;
}

impl<F> OperatorFactory for F
where
    F: Fn(usize) -> Vec<Box<dyn Operator>> + Send,
{
    fn create(&self, core_id: usize) -> Vec<Box<dyn Operator>> {
        self(core_id)
    }
}

/// Thread-per-core runtime for parallel event processing.
///
/// Manages multiple reactor threads, routing events by key hash
/// to ensure state locality.
pub struct ThreadPerCoreRuntime {
    /// Core handles
    cores: Vec<CoreHandle>,
    /// Key router
    router: KeyRouter,
    /// Configuration
    config: TpcConfig,
    /// Running state
    is_running: Arc<AtomicBool>,
}

impl ThreadPerCoreRuntime {
    /// Creates a new runtime with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if any core thread cannot be spawned.
    pub fn new(config: TpcConfig) -> Result<Self, TpcError> {
        config.validate()?;
        Self::new_with_factory(config, &|_| Vec::new())
    }

    /// Creates a new runtime with operators from a factory.
    ///
    /// The factory is called once per core to create that core's operators.
    ///
    /// # Errors
    ///
    /// Returns an error if any core thread cannot be spawned.
    pub fn new_with_factory<F>(config: TpcConfig, factory: &F) -> Result<Self, TpcError>
    where
        F: OperatorFactory,
    {
        config.validate()?;

        let router = KeyRouter::new(config.num_cores, config.key_spec.clone());
        let is_running = Arc::new(AtomicBool::new(true));

        let mut cores = Vec::with_capacity(config.num_cores);

        for core_id in 0..config.num_cores {
            let cpu_affinity = if config.cpu_pinning {
                Some(config.cpu_start + core_id)
            } else {
                None
            };

            let core_config = CoreConfig {
                core_id,
                cpu_affinity,
                inbox_capacity: config.inbox_capacity,
                outbox_capacity: config.outbox_capacity,
                reactor_config: config.reactor_config.clone(),
                backpressure: super::backpressure::BackpressureConfig::default(),
                numa_aware: config.numa_aware,
                #[cfg(all(target_os = "linux", feature = "io-uring"))]
                io_uring_config: None,
            };

            let operators = factory.create(core_id);
            let handle = CoreHandle::spawn_with_operators(core_config, operators)?;
            cores.push(handle);
        }

        Ok(Self {
            cores,
            router,
            config,
            is_running,
        })
    }

    /// Returns the number of cores.
    #[must_use]
    pub fn num_cores(&self) -> usize {
        self.cores.len()
    }

    /// Returns true if the runtime is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    /// Submits an event for processing.
    ///
    /// The event is routed to a core based on its key.
    ///
    /// # Errors
    ///
    /// Returns an error if the target core's queue is full.
    pub fn submit(&self, event: Event) -> Result<(), TpcError> {
        if !self.is_running() {
            return Err(TpcError::NotRunning);
        }

        let core_id = self.router.route(&event)?;
        self.cores[core_id].send_event(event)
    }

    /// Submits an event to a specific core.
    ///
    /// Use this when you've already computed the routing.
    ///
    /// # Errors
    ///
    /// Returns an error if the core's queue is full or the `core_id` is invalid.
    pub fn submit_to_core(&self, core_id: usize, event: Event) -> Result<(), TpcError> {
        if !self.is_running() {
            return Err(TpcError::NotRunning);
        }
        if core_id >= self.cores.len() {
            return Err(TpcError::InvalidConfig(format!(
                "core_id {} out of range (0..{})", core_id, self.cores.len()
            )));
        }
        self.cores[core_id].send_event(event)
    }

    /// Submits a batch of events for processing.
    ///
    /// Events are routed to cores based on their keys.
    ///
    /// # Errors
    ///
    /// Returns the number of successfully submitted events and an error
    /// if any event couldn't be submitted.
    pub fn submit_batch(&self, events: Vec<Event>) -> (usize, Option<TpcError>) {
        if !self.is_running() {
            return (0, Some(TpcError::NotRunning));
        }

        let mut submitted = 0;
        for event in events {
            match self.submit(event) {
                Ok(()) => submitted += 1,
                Err(e) => return (submitted, Some(e)),
            }
        }
        (submitted, None)
    }

    /// Polls all cores for outputs.
    ///
    /// Returns all available outputs from all cores.
    ///
    /// # Note
    ///
    /// This method allocates memory. For zero-allocation polling, use
    /// [`poll_into`](Self::poll_into) or [`poll_each`](Self::poll_each) instead.
    #[must_use]
    pub fn poll(&self) -> Vec<Output> {
        let mut outputs = Vec::new();
        for core in &self.cores {
            outputs.extend(core.poll_outputs(1024));
        }
        outputs
    }

    /// Polls all cores for outputs into a pre-allocated buffer (zero-allocation).
    ///
    /// Returns the total number of outputs collected across all cores.
    ///
    /// # Arguments
    ///
    /// * `buffer` - Pre-allocated buffer to receive outputs. Use [`OutputBuffer`]
    ///   for optimal performance.
    /// * `max_per_core` - Maximum outputs to collect from each core.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use laminar_core::tpc::OutputBuffer;
    ///
    /// // Create buffer once
    /// let mut buffer = OutputBuffer::with_capacity(4096);
    ///
    /// // Poll loop - no allocation after warmup
    /// loop {
    ///     let count = runtime.poll_into(&mut buffer, 256);
    ///
    ///     for output in buffer.iter() {
    ///         process(output);
    ///     }
    ///
    ///     buffer.clear(); // Reuse buffer
    /// }
    /// ```
    pub fn poll_into(&self, buffer: &mut OutputBuffer, max_per_core: usize) -> usize {
        let start_len = buffer.len();

        for core in &self.cores {
            // Check remaining capacity to avoid overflowing
            let remaining = buffer.remaining();
            if remaining == 0 {
                break;
            }

            let max = max_per_core.min(remaining);
            core.poll_outputs_into(buffer.as_vec_mut(), max);
        }

        buffer.len() - start_len
    }

    /// Polls all cores with a callback for each output (zero-allocation).
    ///
    /// Processing continues until:
    /// - All cores have been polled up to `max_per_core`
    /// - The callback returns `ControlFlow::Break`
    ///
    /// Returns the total number of outputs processed.
    ///
    /// # Arguments
    ///
    /// * `max_per_core` - Maximum outputs to process from each core.
    /// * `f` - Callback function for each output. Return `true` to continue, `false` to stop.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Process outputs without any allocation
    /// let count = runtime.poll_each(256, |output| {
    ///     match output {
    ///         Output::Event(event) => {
    ///             send_to_sink(event);
    ///         }
    ///         _ => {}
    ///     }
    ///     true // Continue processing
    /// });
    ///
    /// // Or stop early on condition
    /// let count = runtime.poll_each(256, |output| {
    ///     if should_stop() {
    ///         false // Stop processing
    ///     } else {
    ///         process(output);
    ///         true
    ///     }
    /// });
    /// ```
    pub fn poll_each<F>(&self, max_per_core: usize, mut f: F) -> usize
    where
        F: FnMut(Output) -> bool,
    {
        let mut total = 0;
        let mut should_continue = true;

        for core in &self.cores {
            if !should_continue {
                break;
            }

            let count = core.poll_each(max_per_core, |output| {
                let result = f(output);
                if !result {
                    should_continue = false;
                }
                result
            });

            total += count;
        }

        total
    }

    /// Polls a specific core for outputs.
    ///
    /// # Note
    ///
    /// This method allocates memory. For zero-allocation polling, use
    /// [`poll_core_into`](Self::poll_core_into) or [`poll_core_each`](Self::poll_core_each) instead.
    #[must_use]
    pub fn poll_core(&self, core_id: usize) -> Vec<Output> {
        if core_id < self.cores.len() {
            self.cores[core_id].poll_outputs(1024)
        } else {
            Vec::new()
        }
    }

    /// Polls a specific core into a pre-allocated buffer (zero-allocation).
    ///
    /// Returns the number of outputs collected.
    pub fn poll_core_into(
        &self,
        core_id: usize,
        buffer: &mut OutputBuffer,
        max_count: usize,
    ) -> usize {
        if core_id < self.cores.len() {
            self.cores[core_id].poll_outputs_into(buffer.as_vec_mut(), max_count)
        } else {
            0
        }
    }

    /// Polls a specific core with a callback for each output (zero-allocation).
    ///
    /// Returns the number of outputs processed.
    pub fn poll_core_each<F>(&self, core_id: usize, max_count: usize, f: F) -> usize
    where
        F: FnMut(Output) -> bool,
    {
        if core_id < self.cores.len() {
            self.cores[core_id].poll_each(max_count, f)
        } else {
            0
        }
    }

    /// Returns statistics for all cores.
    #[must_use]
    pub fn stats(&self) -> RuntimeStats {
        let core_stats: Vec<CoreStats> = self
            .cores
            .iter()
            .map(|core| CoreStats {
                core_id: core.core_id(),
                numa_node: core.numa_node(),
                events_processed: core.events_processed(),
                inbox_len: core.inbox_len(),
                outbox_len: core.outbox_len(),
                is_running: core.is_running(),
            })
            .collect();

        RuntimeStats {
            num_cores: self.cores.len(),
            total_events_processed: core_stats.iter().map(|s| s.events_processed).sum(),
            cores: core_stats,
        }
    }

    /// Returns the key router.
    #[must_use]
    pub fn router(&self) -> &KeyRouter {
        &self.router
    }

    /// Shuts down the runtime gracefully.
    ///
    /// Signals all cores to stop and waits for them to finish.
    ///
    /// # Errors
    ///
    /// Returns an error if any core cannot be joined cleanly.
    pub fn shutdown(mut self) -> Result<(), TpcError> {
        self.is_running.store(false, Ordering::Release);

        // Signal all cores to shut down
        for core in &self.cores {
            core.shutdown();
        }

        // Join all cores
        let cores = std::mem::take(&mut self.cores);
        for core in cores {
            core.join()?;
        }

        Ok(())
    }

    /// Runs the runtime with a custom output handler.
    ///
    /// This is a convenience method that polls outputs and passes them
    /// to the handler in a loop until shutdown is signaled.
    ///
    /// # Arguments
    ///
    /// * `handler` - Function called with batches of outputs
    /// * `shutdown` - Atomic flag to signal shutdown
    pub fn run_with_handler<F>(&self, mut handler: F, shutdown: &AtomicBool)
    where
        F: FnMut(Vec<Output>),
    {
        while !shutdown.load(Ordering::Acquire) && self.is_running() {
            let outputs = self.poll();
            if outputs.is_empty() {
                std::thread::yield_now();
            } else {
                handler(outputs);
            }
        }
    }
}

impl Drop for ThreadPerCoreRuntime {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Release);

        // Cores will be dropped and their threads joined in CoreHandle::drop
    }
}

impl std::fmt::Debug for ThreadPerCoreRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadPerCoreRuntime")
            .field("num_cores", &self.cores.len())
            .field("is_running", &self.is_running())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Statistics for the runtime.
#[derive(Debug, Clone)]
pub struct RuntimeStats {
    /// Number of cores
    pub num_cores: usize,
    /// Total events processed across all cores
    pub total_events_processed: u64,
    /// Per-core statistics
    pub cores: Vec<CoreStats>,
}

/// Statistics for a single core.
#[derive(Debug, Clone)]
pub struct CoreStats {
    /// Core ID
    pub core_id: usize,
    /// NUMA node for this core
    pub numa_node: usize,
    /// Events processed by this core
    pub events_processed: u64,
    /// Current inbox queue length
    pub inbox_len: usize,
    /// Current outbox queue length
    pub outbox_len: usize,
    /// Whether the core is running
    pub is_running: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::{OperatorState, OutputVec, Timer};
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;
    use std::time::Duration;

    // Simple passthrough operator for testing
    struct PassthroughOperator {
        #[allow(dead_code)]
        core_id: usize,
    }

    impl Operator for PassthroughOperator {
        fn process(&mut self, event: &Event, _ctx: &mut crate::operator::OperatorContext) -> OutputVec {
            let mut output = OutputVec::new();
            output.push(Output::Event(event.clone()));
            output
        }

        fn on_timer(&mut self, _timer: Timer, _ctx: &mut crate::operator::OperatorContext) -> OutputVec {
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

    fn make_event(user_id: i64, timestamp: i64) -> Event {
        let user_ids = Arc::new(Int64Array::from(vec![user_id]));
        let batch = RecordBatch::try_from_iter(vec![
            ("user_id", user_ids as _),
        ]).unwrap();
        Event::new(timestamp, batch)
    }

    #[test]
    fn test_config_builder() {
        let config = TpcConfig::builder()
            .num_cores(4)
            .key_columns(vec!["user_id".to_string()])
            .cpu_pinning(false)
            .inbox_capacity(1024)
            .build()
            .unwrap();

        assert_eq!(config.num_cores, 4);
        assert!(!config.cpu_pinning);
        assert_eq!(config.inbox_capacity, 1024);
    }

    #[test]
    fn test_config_validation() {
        // Zero cores should fail
        let result = TpcConfig::builder()
            .num_cores(0)
            .build();
        assert!(result.is_err());

        // Zero inbox capacity should fail
        let result = TpcConfig::builder()
            .inbox_capacity(0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_runtime_creation() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).unwrap();
        assert_eq!(runtime.num_cores(), 2);
        assert!(runtime.is_running());

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_runtime_with_factory() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new_with_factory(config, &|core_id| {
            vec![Box::new(PassthroughOperator { core_id }) as Box<dyn Operator>]
        }).unwrap();

        assert_eq!(runtime.num_cores(), 2);

        // Submit events
        for i in 0..10 {
            runtime.submit(make_event(i, i * 1000)).unwrap();
        }

        // Wait for processing
        std::thread::sleep(Duration::from_millis(100));

        // Poll outputs
        let outputs = runtime.poll();
        assert!(!outputs.is_empty());

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_key_based_routing() {
        let config = TpcConfig::builder()
            .num_cores(4)
            .key_columns(vec!["user_id".to_string()])
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).unwrap();

        // Same user_id should always route to same core
        let event1 = make_event(100, 1000);
        let event2 = make_event(100, 2000);

        let core1 = runtime.router().route(&event1).unwrap();
        let core2 = runtime.router().route(&event2).unwrap();

        assert_eq!(core1, core2);
        assert!(core1 < 4);

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_submit_batch() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).unwrap();

        let events: Vec<Event> = (0..100)
            .map(|i| make_event(i, i * 1000))
            .collect();

        let (submitted, error) = runtime.submit_batch(events);
        assert_eq!(submitted, 100);
        assert!(error.is_none());

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_runtime_stats() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new_with_factory(config, &|core_id| {
            vec![Box::new(PassthroughOperator { core_id }) as Box<dyn Operator>]
        }).unwrap();

        // Submit some events
        for i in 0..50 {
            runtime.submit(make_event(i, i * 1000)).unwrap();
        }

        // Wait for processing
        std::thread::sleep(Duration::from_millis(100));

        // Poll to clear outboxes
        let _ = runtime.poll();

        let stats = runtime.stats();
        assert_eq!(stats.num_cores, 2);
        assert!(stats.total_events_processed > 0);

        for core_stat in &stats.cores {
            assert!(core_stat.is_running);
        }

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_submit_to_specific_core() {
        let config = TpcConfig::builder()
            .num_cores(4)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).unwrap();

        // Submit to specific cores
        runtime.submit_to_core(0, make_event(1, 1000)).unwrap();
        runtime.submit_to_core(1, make_event(2, 2000)).unwrap();
        runtime.submit_to_core(2, make_event(3, 3000)).unwrap();
        runtime.submit_to_core(3, make_event(4, 4000)).unwrap();

        // Invalid core should fail
        let result = runtime.submit_to_core(10, make_event(5, 5000));
        assert!(result.is_err());

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_poll_specific_core() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new_with_factory(config, &|core_id| {
            vec![Box::new(PassthroughOperator { core_id }) as Box<dyn Operator>]
        }).unwrap();

        // Submit to core 0
        runtime.submit_to_core(0, make_event(1, 1000)).unwrap();

        // Wait for processing
        std::thread::sleep(Duration::from_millis(100));

        // Poll core 0
        let outputs = runtime.poll_core(0);
        assert!(!outputs.is_empty());

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_runtime_debug() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).unwrap();

        let debug_str = format!("{runtime:?}");
        assert!(debug_str.contains("ThreadPerCoreRuntime"));
        assert!(debug_str.contains("num_cores"));

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_shutdown_stops_submission() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).unwrap();

        // Shutdown via drop
        drop(runtime);

        // Create new runtime and shutdown properly
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).unwrap();
        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_numa_aware_config() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .numa_aware(true)
            .build()
            .unwrap();

        assert!(config.numa_aware);

        let runtime = ThreadPerCoreRuntime::new(config).unwrap();
        assert_eq!(runtime.num_cores(), 2);

        // Stats should include NUMA node info
        let stats = runtime.stats();
        for core_stat in &stats.cores {
            // On any system, numa_node should be valid (0 on non-NUMA)
            assert!(core_stat.numa_node < 64);
        }

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_output_buffer_basic() {
        let mut buffer = OutputBuffer::with_capacity(100);

        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 100);
        assert_eq!(buffer.remaining(), 100);

        // Push some items
        let event = make_event(1, 1000);
        buffer.push(Output::Event(event));

        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.remaining(), 99);

        // Clear and reuse
        buffer.clear();
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 100); // Capacity preserved
    }

    #[test]
    fn test_output_buffer_iteration() {
        let mut buffer = OutputBuffer::with_capacity(10);

        for i in 0..5 {
            buffer.push(Output::Event(make_event(i, i * 1000)));
        }

        // Test iter()
        let count = buffer.iter().count();
        assert_eq!(count, 5);

        // Test Deref to slice
        assert_eq!(buffer.as_slice().len(), 5);

        // Test IntoIterator for reference
        let mut ref_count = 0;
        for _ in &buffer {
            ref_count += 1;
        }
        assert_eq!(ref_count, 5);
    }

    #[test]
    fn test_output_buffer_into_vec() {
        let mut buffer = OutputBuffer::with_capacity(10);

        for i in 0..3 {
            buffer.push(Output::Event(make_event(i, i * 1000)));
        }

        let vec = buffer.into_vec();
        assert_eq!(vec.len(), 3);
    }

    #[test]
    fn test_poll_into() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new_with_factory(config, &|core_id| {
            vec![Box::new(PassthroughOperator { core_id }) as Box<dyn Operator>]
        }).unwrap();

        // Submit events
        for i in 0..20 {
            runtime.submit(make_event(i, i * 1000)).unwrap();
        }

        // Wait for processing
        std::thread::sleep(Duration::from_millis(100));

        // Poll into buffer
        let mut buffer = OutputBuffer::with_capacity(100);
        let count = runtime.poll_into(&mut buffer, 256);

        assert!(count > 0);
        assert_eq!(buffer.len(), count);

        // Reuse buffer - no new allocation
        let cap_before = buffer.capacity();
        buffer.clear();
        let _ = runtime.poll_into(&mut buffer, 256);
        assert_eq!(buffer.capacity(), cap_before);

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_poll_each() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new_with_factory(config, &|core_id| {
            vec![Box::new(PassthroughOperator { core_id }) as Box<dyn Operator>]
        }).unwrap();

        // Submit events
        for i in 0..20 {
            runtime.submit(make_event(i, i * 1000)).unwrap();
        }

        // Wait for processing
        std::thread::sleep(Duration::from_millis(100));

        // Poll with callback
        let mut event_count = 0;
        let count = runtime.poll_each(256, |output| {
            if matches!(output, Output::Event(_)) {
                event_count += 1;
            }
            true
        });

        assert!(count > 0);
        assert!(event_count > 0);

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_poll_each_early_stop() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new_with_factory(config, &|core_id| {
            vec![Box::new(PassthroughOperator { core_id }) as Box<dyn Operator>]
        }).unwrap();

        // Submit many events
        for i in 0..50 {
            runtime.submit(make_event(i, i * 1000)).unwrap();
        }

        // Wait for processing
        std::thread::sleep(Duration::from_millis(100));

        // Poll with early stop after 10 items
        let mut processed = 0;
        let count = runtime.poll_each(256, |_| {
            processed += 1;
            processed < 10 // Stop after 10
        });

        assert_eq!(count, 10);
        assert_eq!(processed, 10);

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_poll_core_into() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new_with_factory(config, &|core_id| {
            vec![Box::new(PassthroughOperator { core_id }) as Box<dyn Operator>]
        }).unwrap();

        // Submit to specific core
        runtime.submit_to_core(0, make_event(1, 1000)).unwrap();
        runtime.submit_to_core(0, make_event(2, 2000)).unwrap();

        // Wait for processing
        std::thread::sleep(Duration::from_millis(100));

        // Poll core 0 into buffer
        let mut buffer = OutputBuffer::with_capacity(100);
        let count = runtime.poll_core_into(0, &mut buffer, 100);

        assert!(count > 0);
        assert_eq!(buffer.len(), count);

        // Invalid core returns 0
        let count = runtime.poll_core_into(99, &mut buffer, 100);
        assert_eq!(count, 0);

        runtime.shutdown().unwrap();
    }

    #[test]
    fn test_poll_core_each() {
        let config = TpcConfig::builder()
            .num_cores(2)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new_with_factory(config, &|core_id| {
            vec![Box::new(PassthroughOperator { core_id }) as Box<dyn Operator>]
        }).unwrap();

        // Submit to specific core
        runtime.submit_to_core(1, make_event(1, 1000)).unwrap();

        // Wait for processing
        std::thread::sleep(Duration::from_millis(100));

        // Poll core 1 with callback
        let mut event_count = 0;
        let count = runtime.poll_core_each(1, 100, |output| {
            if matches!(output, Output::Event(_)) {
                event_count += 1;
            }
            true
        });

        assert!(count > 0);
        assert!(event_count > 0);

        // Invalid core returns 0
        let count = runtime.poll_core_each(99, 100, |_| true);
        assert_eq!(count, 0);

        runtime.shutdown().unwrap();
    }
}
