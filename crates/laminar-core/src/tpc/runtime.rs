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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::operator::{Event, Operator, Output};
use crate::reactor::Config as ReactorConfig;

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
    #[must_use]
    pub fn poll(&self) -> Vec<Output> {
        let mut outputs = Vec::new();
        for core in &self.cores {
            outputs.extend(core.poll_outputs(1024));
        }
        outputs
    }

    /// Polls a specific core for outputs.
    #[must_use]
    pub fn poll_core(&self, core_id: usize) -> Vec<Output> {
        if core_id < self.cores.len() {
            self.cores[core_id].poll_outputs(1024)
        } else {
            Vec::new()
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
        Event { timestamp, data: batch }
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

        let debug_str = format!("{:?}", runtime);
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
}
