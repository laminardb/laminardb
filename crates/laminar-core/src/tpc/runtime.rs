//! Thread-per-core runtime configuration and output buffer.
//!
//! Provides [`TpcConfig`] for configuring per-core reactors and
//! [`OutputBuffer`] for zero-allocation output collection.

use std::ops::Deref;

use crate::operator::Output;
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

use super::router::KeySpec;
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
            num_cores: std::thread::available_parallelism().map_or(1, std::num::NonZero::get),
            key_spec: KeySpec::RoundRobin,
            cpu_pinning: false,
            cpu_start: 0,
            inbox_capacity: 8192,
            outbox_capacity: 8192,
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

    /// Creates configuration with automatic detection.
    ///
    /// Detects system capabilities and generates an optimal configuration:
    /// - Uses all available physical cores (minus 1 on systems with >8 cores)
    /// - Enables CPU pinning on multi-core systems
    /// - Enables NUMA-aware allocation on multi-socket systems
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use laminar_core::tpc::TpcConfig;
    ///
    /// let config = TpcConfig::auto();
    /// println!("Using {} cores", config.num_cores);
    /// ```
    #[must_use]
    pub fn auto() -> Self {
        let caps = crate::detect::SystemCapabilities::detect();
        let recommended = caps.recommended_config();

        Self {
            num_cores: recommended.num_cores,
            key_spec: KeySpec::RoundRobin,
            cpu_pinning: recommended.cpu_pinning,
            cpu_start: 0,
            inbox_capacity: recommended.queue_capacity,
            outbox_capacity: recommended.queue_capacity,
            reactor_config: ReactorConfig::default(),
            numa_aware: recommended.numa_aware,
        }
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
            return Err(TpcError::InvalidConfig(
                "inbox_capacity must be > 0".to_string(),
            ));
        }
        if self.outbox_capacity == 0 {
            return Err(TpcError::InvalidConfig(
                "outbox_capacity must be > 0".to_string(),
            ));
        }
        if self.cpu_pinning {
            let available = std::thread::available_parallelism()
                .map(std::num::NonZero::get)
                .unwrap_or(1);
            let max_cpu = self.cpu_start + self.num_cores;
            if max_cpu > available {
                return Err(TpcError::InvalidConfig(format!(
                    "cpu_start({}) + num_cores({}) = {max_cpu} exceeds available CPUs ({available})",
                    self.cpu_start, self.num_cores,
                )));
            }
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
            num_cores: self.num_cores.unwrap_or_else(|| {
                std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
            }),
            key_spec: self.key_spec.unwrap_or_default(),
            cpu_pinning: self.cpu_pinning.unwrap_or(false),
            cpu_start: self.cpu_start.unwrap_or(0),
            inbox_capacity: self.inbox_capacity.unwrap_or(8192),
            outbox_capacity: self.outbox_capacity.unwrap_or(8192),
            reactor_config: self.reactor_config.unwrap_or_default(),
            numa_aware: self.numa_aware.unwrap_or(false),
        };
        config.validate()?;
        Ok(config)
    }
}
