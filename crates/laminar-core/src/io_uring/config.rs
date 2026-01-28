//! Configuration types for `io_uring`.

/// Ring operation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RingMode {
    /// Standard mode with interrupt-based completions.
    #[default]
    Standard,
    /// SQPOLL mode with kernel polling thread (no syscalls under load).
    SqPoll,
    /// IOPOLL mode for `NVMe` devices (polls completions from device).
    /// Cannot be used with socket operations.
    IoPoll,
    /// Combined SQPOLL + IOPOLL for maximum performance on `NVMe`.
    SqPollIoPoll,
}

impl RingMode {
    /// Returns true if SQPOLL is enabled.
    #[must_use]
    pub const fn uses_sqpoll(&self) -> bool {
        matches!(self, Self::SqPoll | Self::SqPollIoPoll)
    }

    /// Returns true if IOPOLL is enabled.
    #[must_use]
    pub const fn uses_iopoll(&self) -> bool {
        matches!(self, Self::IoPoll | Self::SqPollIoPoll)
    }
}

/// Configuration for `io_uring` operations.
#[derive(Debug, Clone)]
pub struct IoUringConfig {
    /// Number of submission queue entries (power of 2, typically 256-4096).
    pub ring_entries: u32,
    /// Ring operation mode.
    pub mode: RingMode,
    /// SQPOLL idle timeout in milliseconds before kernel thread sleeps.
    /// Only used when mode uses SQPOLL.
    pub sqpoll_idle_ms: u32,
    /// CPU to pin the SQPOLL kernel thread to (usually same NUMA node as core).
    /// Only used when mode uses SQPOLL.
    pub sqpoll_cpu: Option<u32>,
    /// Size of each registered buffer in bytes (typically 64KB).
    pub buffer_size: usize,
    /// Number of buffers in the registered pool.
    pub buffer_count: usize,
    /// Enable cooperative task running to reduce kernel-userspace transitions.
    pub coop_taskrun: bool,
    /// Optimize for single-threaded submission (thread-per-core model).
    pub single_issuer: bool,
    /// Enable direct file descriptor table for faster operations.
    pub direct_table: bool,
    /// Number of direct file descriptor slots.
    pub direct_table_size: u32,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            ring_entries: 256,
            mode: RingMode::Standard,
            sqpoll_idle_ms: 1000,
            sqpoll_cpu: None,
            buffer_size: 64 * 1024, // 64KB per buffer
            buffer_count: 256,      // 256 buffers = 16MB total
            coop_taskrun: true,
            single_issuer: true,
            direct_table: false,
            direct_table_size: 256,
        }
    }
}

impl IoUringConfig {
    /// Create a new builder for `IoUringConfig`.
    #[must_use]
    pub fn builder() -> IoUringConfigBuilder {
        IoUringConfigBuilder::default()
    }

    /// Total buffer pool size in bytes.
    #[must_use]
    pub const fn total_buffer_size(&self) -> usize {
        self.buffer_size * self.buffer_count
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<(), super::IoUringError> {
        // Ring entries must be power of 2
        if !self.ring_entries.is_power_of_two() {
            return Err(super::IoUringError::InvalidConfig(format!(
                "ring_entries must be power of 2, got {}",
                self.ring_entries
            )));
        }

        // Must have at least 2 entries
        if self.ring_entries < 2 {
            return Err(super::IoUringError::InvalidConfig(
                "ring_entries must be at least 2".to_string(),
            ));
        }

        // Buffer size must be positive and reasonable
        if self.buffer_size == 0 {
            return Err(super::IoUringError::InvalidConfig(
                "buffer_size must be positive".to_string(),
            ));
        }

        if self.buffer_size > 16 * 1024 * 1024 {
            return Err(super::IoUringError::InvalidConfig(
                "buffer_size cannot exceed 16MB".to_string(),
            ));
        }

        // Buffer count must be reasonable
        if self.buffer_count == 0 {
            return Err(super::IoUringError::InvalidConfig(
                "buffer_count must be positive".to_string(),
            ));
        }

        if self.buffer_count > 65536 {
            return Err(super::IoUringError::InvalidConfig(
                "buffer_count cannot exceed 65536".to_string(),
            ));
        }

        Ok(())
    }
}

/// Builder for `IoUringConfig`.
#[derive(Debug, Default)]
pub struct IoUringConfigBuilder {
    config: IoUringConfig,
}

impl IoUringConfigBuilder {
    /// Set the number of ring entries.
    #[must_use]
    pub const fn ring_entries(mut self, entries: u32) -> Self {
        self.config.ring_entries = entries;
        self
    }

    /// Set the ring mode.
    #[must_use]
    pub const fn mode(mut self, mode: RingMode) -> Self {
        self.config.mode = mode;
        self
    }

    /// Enable SQPOLL mode with the specified idle timeout.
    #[must_use]
    pub const fn enable_sqpoll(mut self, idle_ms: u32) -> Self {
        self.config.mode = RingMode::SqPoll;
        self.config.sqpoll_idle_ms = idle_ms;
        self
    }

    /// Set the CPU for the SQPOLL kernel thread.
    #[must_use]
    pub const fn sqpoll_cpu(mut self, cpu: u32) -> Self {
        self.config.sqpoll_cpu = Some(cpu);
        self
    }

    /// Enable IOPOLL mode (for `NVMe` devices).
    #[must_use]
    pub const fn enable_iopoll(mut self) -> Self {
        self.config.mode = match self.config.mode {
            RingMode::SqPoll | RingMode::SqPollIoPoll => RingMode::SqPollIoPoll,
            _ => RingMode::IoPoll,
        };
        self
    }

    /// Set the size of each buffer in the pool.
    #[must_use]
    pub const fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set the number of buffers in the pool.
    #[must_use]
    pub const fn buffer_count(mut self, count: usize) -> Self {
        self.config.buffer_count = count;
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

    /// Enable direct file descriptor table.
    #[must_use]
    pub const fn direct_table(mut self, size: u32) -> Self {
        self.config.direct_table = true;
        self.config.direct_table_size = size;
        self
    }

    /// Build the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn build(self) -> Result<IoUringConfig, super::IoUringError> {
        self.config.validate()?;
        Ok(self.config)
    }

    /// Build the configuration without validation (for testing).
    #[must_use]
    pub fn build_unchecked(self) -> IoUringConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = IoUringConfig::default();
        assert_eq!(config.ring_entries, 256);
        assert_eq!(config.mode, RingMode::Standard);
        assert_eq!(config.buffer_size, 64 * 1024);
        assert_eq!(config.buffer_count, 256);
        assert!(config.coop_taskrun);
        assert!(config.single_issuer);
    }

    #[test]
    fn test_builder() {
        let config = IoUringConfig::builder()
            .ring_entries(512)
            .enable_sqpoll(2000)
            .sqpoll_cpu(4)
            .buffer_size(128 * 1024)
            .buffer_count(512)
            .build()
            .unwrap();

        assert_eq!(config.ring_entries, 512);
        assert!(config.mode.uses_sqpoll());
        assert_eq!(config.sqpoll_idle_ms, 2000);
        assert_eq!(config.sqpoll_cpu, Some(4));
        assert_eq!(config.buffer_size, 128 * 1024);
        assert_eq!(config.buffer_count, 512);
    }

    #[test]
    fn test_ring_mode() {
        assert!(!RingMode::Standard.uses_sqpoll());
        assert!(!RingMode::Standard.uses_iopoll());

        assert!(RingMode::SqPoll.uses_sqpoll());
        assert!(!RingMode::SqPoll.uses_iopoll());

        assert!(!RingMode::IoPoll.uses_sqpoll());
        assert!(RingMode::IoPoll.uses_iopoll());

        assert!(RingMode::SqPollIoPoll.uses_sqpoll());
        assert!(RingMode::SqPollIoPoll.uses_iopoll());
    }

    #[test]
    fn test_total_buffer_size() {
        let config = IoUringConfig {
            buffer_size: 64 * 1024,
            buffer_count: 256,
            ..Default::default()
        };
        assert_eq!(config.total_buffer_size(), 16 * 1024 * 1024); // 16MB
    }

    #[test]
    fn test_validation_ring_entries_power_of_two() {
        let config = IoUringConfig {
            ring_entries: 100, // Not power of 2
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_buffer_size_zero() {
        let config = IoUringConfig {
            buffer_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_buffer_count_zero() {
        let config = IoUringConfig {
            buffer_count: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_enable_iopoll_combines_with_sqpoll() {
        let config = IoUringConfig::builder()
            .enable_sqpoll(1000)
            .enable_iopoll()
            .build_unchecked();

        assert_eq!(config.mode, RingMode::SqPollIoPoll);
    }
}
