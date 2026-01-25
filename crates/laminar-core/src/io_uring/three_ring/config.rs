//! Configuration for three-ring I/O architecture.

use crate::io_uring::IoUringError;

/// Configuration for three-ring reactor.
///
/// Each ring has separate configuration for entries and SQPOLL timeout.
/// The latency ring uses shorter timeouts for responsiveness, while the
/// main ring uses longer timeouts to allow efficient sleep when idle.
#[derive(Debug, Clone)]
pub struct ThreeRingConfig {
    /// Number of entries for the latency ring (smaller, always polled).
    /// Default: 256
    pub latency_entries: u32,

    /// Number of entries for the main ring (larger, can block).
    /// Default: 1024
    pub main_entries: u32,

    /// Number of entries for the poll ring (NVMe storage).
    /// Default: 256
    pub poll_entries: u32,

    /// Enable the poll ring (requires NVMe).
    /// Default: false
    pub enable_poll_ring: bool,

    /// SQPOLL idle timeout for latency ring (ms).
    /// Short timeout for responsiveness.
    /// Default: 100
    pub latency_sqpoll_idle_ms: u32,

    /// SQPOLL idle timeout for main ring (ms).
    /// Longer timeout, can idle.
    /// Default: 1000
    pub main_sqpoll_idle_ms: u32,

    /// CPU to pin SQPOLL threads to (optional).
    /// Usually same NUMA node as the core.
    pub sqpoll_cpu: Option<u32>,

    /// Enable cooperative task running.
    /// Default: true
    pub coop_taskrun: bool,

    /// Optimize for single-threaded submission (thread-per-core).
    /// Default: true
    pub single_issuer: bool,

    /// Size of each buffer in the pools.
    /// Default: 64KB
    pub buffer_size: usize,

    /// Number of buffers per pool.
    /// Default: 64
    pub buffer_count: usize,
}

impl Default for ThreeRingConfig {
    fn default() -> Self {
        Self {
            latency_entries: 256,
            main_entries: 1024,
            poll_entries: 256,
            enable_poll_ring: false,
            latency_sqpoll_idle_ms: 100,
            main_sqpoll_idle_ms: 1000,
            sqpoll_cpu: None,
            coop_taskrun: true,
            single_issuer: true,
            buffer_size: 64 * 1024,
            buffer_count: 64,
        }
    }
}

impl ThreeRingConfig {
    /// Create a new builder for `ThreeRingConfig`.
    #[must_use]
    pub fn builder() -> ThreeRingConfigBuilder {
        ThreeRingConfigBuilder::default()
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<(), IoUringError> {
        // Ring entries must be power of 2
        if !self.latency_entries.is_power_of_two() {
            return Err(IoUringError::InvalidConfig(format!(
                "latency_entries must be power of 2, got {}",
                self.latency_entries
            )));
        }

        if !self.main_entries.is_power_of_two() {
            return Err(IoUringError::InvalidConfig(format!(
                "main_entries must be power of 2, got {}",
                self.main_entries
            )));
        }

        if !self.poll_entries.is_power_of_two() {
            return Err(IoUringError::InvalidConfig(format!(
                "poll_entries must be power of 2, got {}",
                self.poll_entries
            )));
        }

        // Must have at least 2 entries
        if self.latency_entries < 2 {
            return Err(IoUringError::InvalidConfig(
                "latency_entries must be at least 2".to_string(),
            ));
        }

        if self.main_entries < 2 {
            return Err(IoUringError::InvalidConfig(
                "main_entries must be at least 2".to_string(),
            ));
        }

        // Buffer validation
        if self.buffer_size == 0 {
            return Err(IoUringError::InvalidConfig(
                "buffer_size must be positive".to_string(),
            ));
        }

        if self.buffer_size > 16 * 1024 * 1024 {
            return Err(IoUringError::InvalidConfig(
                "buffer_size cannot exceed 16MB".to_string(),
            ));
        }

        Ok(())
    }

    /// Total memory used by all buffer pools.
    #[must_use]
    pub const fn total_buffer_memory(&self) -> usize {
        // Latency + Main pools, plus Poll pool if enabled
        let base = self.buffer_size * self.buffer_count * 2;
        if self.enable_poll_ring {
            base + self.buffer_size * self.buffer_count
        } else {
            base
        }
    }
}

/// Builder for `ThreeRingConfig`.
#[derive(Debug, Default)]
pub struct ThreeRingConfigBuilder {
    config: ThreeRingConfig,
}

impl ThreeRingConfigBuilder {
    /// Set the number of entries for the latency ring.
    #[must_use]
    pub const fn latency_entries(mut self, entries: u32) -> Self {
        self.config.latency_entries = entries;
        self
    }

    /// Set the number of entries for the main ring.
    #[must_use]
    pub const fn main_entries(mut self, entries: u32) -> Self {
        self.config.main_entries = entries;
        self
    }

    /// Set the number of entries for the poll ring.
    #[must_use]
    pub const fn poll_entries(mut self, entries: u32) -> Self {
        self.config.poll_entries = entries;
        self
    }

    /// Enable the poll ring for NVMe storage.
    #[must_use]
    pub const fn enable_poll_ring(mut self, enable: bool) -> Self {
        self.config.enable_poll_ring = enable;
        self
    }

    /// Set the SQPOLL idle timeout for the latency ring.
    #[must_use]
    pub const fn latency_sqpoll_idle_ms(mut self, ms: u32) -> Self {
        self.config.latency_sqpoll_idle_ms = ms;
        self
    }

    /// Set the SQPOLL idle timeout for the main ring.
    #[must_use]
    pub const fn main_sqpoll_idle_ms(mut self, ms: u32) -> Self {
        self.config.main_sqpoll_idle_ms = ms;
        self
    }

    /// Set the CPU for SQPOLL threads.
    #[must_use]
    pub const fn sqpoll_cpu(mut self, cpu: u32) -> Self {
        self.config.sqpoll_cpu = Some(cpu);
        self
    }

    /// Enable cooperative task running.
    #[must_use]
    pub const fn coop_taskrun(mut self, enable: bool) -> Self {
        self.config.coop_taskrun = enable;
        self
    }

    /// Enable single-issuer optimization.
    #[must_use]
    pub const fn single_issuer(mut self, enable: bool) -> Self {
        self.config.single_issuer = enable;
        self
    }

    /// Set the buffer size for all pools.
    #[must_use]
    pub const fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set the buffer count for all pools.
    #[must_use]
    pub const fn buffer_count(mut self, count: usize) -> Self {
        self.config.buffer_count = count;
        self
    }

    /// Build the configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn build(self) -> Result<ThreeRingConfig, IoUringError> {
        self.config.validate()?;
        Ok(self.config)
    }

    /// Build the configuration without validation (for testing).
    #[must_use]
    pub fn build_unchecked(self) -> ThreeRingConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ThreeRingConfig::default();
        assert_eq!(config.latency_entries, 256);
        assert_eq!(config.main_entries, 1024);
        assert_eq!(config.poll_entries, 256);
        assert!(!config.enable_poll_ring);
        assert_eq!(config.latency_sqpoll_idle_ms, 100);
        assert_eq!(config.main_sqpoll_idle_ms, 1000);
        assert!(config.coop_taskrun);
        assert!(config.single_issuer);
    }

    #[test]
    fn test_builder() {
        let config = ThreeRingConfig::builder()
            .latency_entries(128)
            .main_entries(512)
            .poll_entries(64)
            .enable_poll_ring(true)
            .latency_sqpoll_idle_ms(50)
            .main_sqpoll_idle_ms(500)
            .sqpoll_cpu(4)
            .buffer_size(128 * 1024)
            .buffer_count(128)
            .build()
            .unwrap();

        assert_eq!(config.latency_entries, 128);
        assert_eq!(config.main_entries, 512);
        assert_eq!(config.poll_entries, 64);
        assert!(config.enable_poll_ring);
        assert_eq!(config.sqpoll_cpu, Some(4));
        assert_eq!(config.buffer_size, 128 * 1024);
        assert_eq!(config.buffer_count, 128);
    }

    #[test]
    fn test_validation_latency_entries() {
        let config = ThreeRingConfig {
            latency_entries: 100, // Not power of 2
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_main_entries() {
        let config = ThreeRingConfig {
            main_entries: 1000, // Not power of 2
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_buffer_size_zero() {
        let config = ThreeRingConfig {
            buffer_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_total_buffer_memory() {
        let config = ThreeRingConfig {
            buffer_size: 64 * 1024,
            buffer_count: 64,
            enable_poll_ring: false,
            ..Default::default()
        };
        // Latency + Main = 2 pools
        assert_eq!(config.total_buffer_memory(), 64 * 1024 * 64 * 2);

        let config_with_poll = ThreeRingConfig {
            buffer_size: 64 * 1024,
            buffer_count: 64,
            enable_poll_ring: true,
            ..Default::default()
        };
        // Latency + Main + Poll = 3 pools
        assert_eq!(config_with_poll.total_buffer_memory(), 64 * 1024 * 64 * 3);
    }
}
