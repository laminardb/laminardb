//! `io_uring` ring creation and configuration.
//!
//! Provides functions to create optimized `io_uring` instances with various modes:
//! - SQPOLL: Kernel polling thread eliminates syscalls
//! - IOPOLL: Poll completions from `NVMe` device queue
//! - Combined: Maximum performance for `NVMe` storage

use io_uring::squeue::Entry;
use io_uring::IoUring;

use super::config::{IoUringConfig, RingMode};
use super::error::IoUringError;

/// Type alias for standard `IoUring` with default entry types.
pub type StandardIoUring = IoUring<Entry, io_uring::cqueue::Entry>;

/// Wrapper around `io_uring` with mode information.
pub struct IoUringRing {
    ring: StandardIoUring,
    mode: RingMode,
    entries: u32,
}

impl std::fmt::Debug for IoUringRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoUringRing")
            .field("mode", &self.mode)
            .field("entries", &self.entries)
            .finish_non_exhaustive()
    }
}

impl IoUringRing {
    /// Create a new ring from the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the ring cannot be created.
    pub fn new(config: &IoUringConfig) -> Result<Self, IoUringError> {
        config.validate()?;

        let ring = match config.mode {
            RingMode::Standard => create_standard_ring(config)?,
            RingMode::SqPoll => create_sqpoll_ring(config)?,
            RingMode::IoPoll => create_iopoll_ring_internal(config)?,
            RingMode::SqPollIoPoll => create_sqpoll_iopoll_ring(config)?,
        };

        // Check for FEAT_NODROP: without this feature, the kernel may silently
        // drop CQEs when the completion queue overflows, causing completed I/O
        // operations to be lost without notification.
        if !ring.params().is_feature_nodrop() {
            tracing::warn!(
                "io_uring: FEAT_NODROP not supported — CQE overflow may silently \
                 drop completions. Consider upgrading to Linux 5.5+."
            );
        }

        Ok(Self {
            ring,
            mode: config.mode,
            entries: config.ring_entries,
        })
    }

    /// Get a reference to the underlying `io_uring`.
    #[must_use]
    pub fn ring(&self) -> &StandardIoUring {
        &self.ring
    }

    /// Get a mutable reference to the underlying `io_uring`.
    #[must_use]
    pub fn ring_mut(&mut self) -> &mut StandardIoUring {
        &mut self.ring
    }

    /// Get the ring mode.
    #[must_use]
    pub const fn mode(&self) -> RingMode {
        self.mode
    }

    /// Get the number of entries.
    #[must_use]
    pub const fn entries(&self) -> u32 {
        self.entries
    }

    /// Check if SQPOLL is enabled.
    #[must_use]
    pub const fn uses_sqpoll(&self) -> bool {
        self.mode.uses_sqpoll()
    }

    /// Check if IOPOLL is enabled.
    #[must_use]
    pub const fn uses_iopoll(&self) -> bool {
        self.mode.uses_iopoll()
    }
}

/// Create a standard `io_uring` ring.
fn create_standard_ring(config: &IoUringConfig) -> Result<StandardIoUring, IoUringError> {
    let mut builder = IoUring::builder();

    if config.coop_taskrun {
        builder.setup_coop_taskrun();
    }

    if config.single_issuer {
        builder.setup_single_issuer();
    }

    builder
        .build(config.ring_entries)
        .map_err(IoUringError::RingCreation)
}

/// Create a SQPOLL-enabled ring.
fn create_sqpoll_ring(config: &IoUringConfig) -> Result<StandardIoUring, IoUringError> {
    let mut builder = IoUring::builder();

    // Enable SQPOLL - kernel thread polls submission queue
    builder.setup_sqpoll(config.sqpoll_idle_ms);

    // Pin SQPOLL thread to specific CPU if requested
    if let Some(cpu) = config.sqpoll_cpu {
        builder.setup_sqpoll_cpu(cpu);
    }

    if config.coop_taskrun {
        builder.setup_coop_taskrun();
    }

    if config.single_issuer {
        builder.setup_single_issuer();
    }

    builder.build(config.ring_entries).map_err(|e| {
        if e.raw_os_error() == Some(libc::EPERM) {
            IoUringError::FeatureNotSupported {
                feature: "SQPOLL".to_string(),
                required_version: "5.11+ with CAP_SYS_NICE (or kernel.io_uring_group sysctl)"
                    .to_string(),
            }
        } else {
            IoUringError::RingCreation(e)
        }
    })
}

/// Create an IOPOLL-enabled ring.
fn create_iopoll_ring_internal(config: &IoUringConfig) -> Result<StandardIoUring, IoUringError> {
    let mut builder = IoUring::builder();

    // Enable IOPOLL - poll completions from device
    builder.setup_iopoll();

    if config.coop_taskrun {
        builder.setup_coop_taskrun();
    }

    if config.single_issuer {
        builder.setup_single_issuer();
    }

    builder
        .build(config.ring_entries)
        .map_err(IoUringError::RingCreation)
}

/// Create a ring with both SQPOLL and IOPOLL.
fn create_sqpoll_iopoll_ring(config: &IoUringConfig) -> Result<StandardIoUring, IoUringError> {
    let mut builder = IoUring::builder();

    // Enable both SQPOLL and IOPOLL
    builder.setup_sqpoll(config.sqpoll_idle_ms);
    builder.setup_iopoll();

    if let Some(cpu) = config.sqpoll_cpu {
        builder.setup_sqpoll_cpu(cpu);
    }

    if config.coop_taskrun {
        builder.setup_coop_taskrun();
    }

    if config.single_issuer {
        builder.setup_single_issuer();
    }

    builder.build(config.ring_entries).map_err(|e| {
        if e.raw_os_error() == Some(libc::EPERM) {
            IoUringError::FeatureNotSupported {
                feature: "SQPOLL+IOPOLL".to_string(),
                required_version: "5.11+ with CAP_SYS_NICE (or kernel.io_uring_group sysctl)"
                    .to_string(),
            }
        } else {
            IoUringError::RingCreation(e)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_standard_ring() {
        let config = IoUringConfig {
            ring_entries: 32,
            mode: RingMode::Standard,
            ..Default::default()
        };
        let ring = IoUringRing::new(&config);
        // May fail in CI or containers without io_uring support
        if let Ok(r) = ring {
            assert_eq!(r.mode(), RingMode::Standard);
            assert_eq!(r.entries(), 32);
            assert!(!r.uses_sqpoll());
            assert!(!r.uses_iopoll());
        }
    }

    #[test]
    fn test_ring_wrapper() {
        let config = IoUringConfig::default();
        if let Ok(ring) = IoUringRing::new(&config) {
            assert_eq!(ring.mode(), RingMode::Standard);
            assert_eq!(ring.entries(), 256);
        }
    }
}
