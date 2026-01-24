//! io_uring ring creation and configuration.
//!
//! Provides functions to create optimized io_uring instances with various modes:
//! - SQPOLL: Kernel polling thread eliminates syscalls
//! - IOPOLL: Poll completions from NVMe device queue
//! - Combined: Maximum performance for NVMe storage

use io_uring::IoUring;
use std::io;

use super::config::{IoUringConfig, RingMode};
use super::error::IoUringError;

/// Wrapper around io_uring with mode information.
#[derive(Debug)]
pub struct IoUringRing {
    ring: IoUring,
    mode: RingMode,
    entries: u32,
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

        Ok(Self {
            ring,
            mode: config.mode,
            entries: config.ring_entries,
        })
    }

    /// Get a reference to the underlying io_uring.
    #[must_use]
    pub fn ring(&self) -> &IoUring {
        &self.ring
    }

    /// Get a mutable reference to the underlying io_uring.
    #[must_use]
    pub fn ring_mut(&mut self) -> &mut IoUring {
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

/// Create a standard io_uring ring.
fn create_standard_ring(config: &IoUringConfig) -> Result<IoUring, IoUringError> {
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

/// Create an io_uring ring with SQPOLL mode.
///
/// SQPOLL eliminates syscalls by using a dedicated kernel polling thread
/// that continuously polls the submission queue. The kernel thread is
/// optionally pinned to a specific CPU.
///
/// # Arguments
///
/// * `entries` - Number of submission queue entries (power of 2)
/// * `sqpoll_idle_ms` - Idle timeout before kernel thread sleeps
/// * `sqpoll_cpu` - Optional CPU to pin the SQPOLL thread to
///
/// # Returns
///
/// An optimized io_uring instance configured for SQPOLL mode.
///
/// # Errors
///
/// Returns an error if the ring cannot be created.
pub fn create_optimized_ring(
    entries: u32,
    sqpoll_idle_ms: u32,
    sqpoll_cpu: Option<u32>,
) -> Result<IoUring, IoUringError> {
    let config = IoUringConfig {
        ring_entries: entries,
        mode: RingMode::SqPoll,
        sqpoll_idle_ms,
        sqpoll_cpu,
        ..Default::default()
    };
    create_sqpoll_ring(&config)
}

/// Create a SQPOLL-enabled ring.
fn create_sqpoll_ring(config: &IoUringConfig) -> Result<IoUring, IoUringError> {
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

    builder
        .build(config.ring_entries)
        .map_err(IoUringError::RingCreation)
}

/// Create an io_uring ring with IOPOLL mode for NVMe storage.
///
/// IOPOLL polls completions directly from the NVMe device queue instead of
/// using interrupts. This provides lower latency for storage operations.
///
/// **Note**: IOPOLL rings cannot be used with socket operations. Use a
/// separate ring for network I/O.
///
/// # Arguments
///
/// * `entries` - Number of submission queue entries (power of 2)
///
/// # Returns
///
/// An io_uring instance configured for IOPOLL mode.
///
/// # Errors
///
/// Returns an error if the ring cannot be created.
pub fn create_iopoll_ring(entries: u32) -> Result<IoUring, IoUringError> {
    let config = IoUringConfig {
        ring_entries: entries,
        mode: RingMode::IoPoll,
        ..Default::default()
    };
    create_iopoll_ring_internal(&config)
}

/// Create an IOPOLL-enabled ring.
fn create_iopoll_ring_internal(config: &IoUringConfig) -> Result<IoUring, IoUringError> {
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
fn create_sqpoll_iopoll_ring(config: &IoUringConfig) -> Result<IoUring, IoUringError> {
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

    builder
        .build(config.ring_entries)
        .map_err(IoUringError::RingCreation)
}

/// Probe kernel for supported io_uring features.
#[must_use]
pub fn probe_features() -> SupportedFeatures {
    // Try to create a minimal ring to probe features
    let ring = match IoUring::builder().build(8) {
        Ok(r) => r,
        Err(_) => {
            return SupportedFeatures {
                available: false,
                sqpoll: false,
                iopoll: false,
                registered_buffers: false,
                registered_files: false,
                direct_descriptors: false,
            }
        }
    };

    // Check for SQPOLL support
    let sqpoll = IoUring::builder().setup_sqpoll(1000).build(8).is_ok();

    // Check for IOPOLL support
    let iopoll = IoUring::builder().setup_iopoll().build(8).is_ok();

    // Registered buffers and files are available on all io_uring versions
    let registered_buffers = true;
    let registered_files = true;

    // Direct descriptors (requires newer kernel)
    let direct_descriptors = false; // Would need actual probe

    drop(ring);

    SupportedFeatures {
        available: true,
        sqpoll,
        iopoll,
        registered_buffers,
        registered_files,
        direct_descriptors,
    }
}

/// Supported io_uring features on the current system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedFeatures {
    /// io_uring is available.
    pub available: bool,
    /// SQPOLL mode is supported.
    pub sqpoll: bool,
    /// IOPOLL mode is supported.
    pub iopoll: bool,
    /// Registered buffers are supported.
    pub registered_buffers: bool,
    /// Registered files are supported.
    pub registered_files: bool,
    /// Direct file descriptors are supported.
    pub direct_descriptors: bool,
}

impl SupportedFeatures {
    /// Check if all advanced features are available.
    #[must_use]
    pub const fn has_advanced_features(&self) -> bool {
        self.sqpoll && self.iopoll && self.registered_buffers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_probe_features() {
        let features = probe_features();
        // On Linux with io_uring, this should be true
        // On other platforms or containers, may be false
        println!("io_uring features: {:?}", features);
    }

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
    fn test_create_optimized_ring() {
        // May fail without root privileges or in containers
        let result = create_optimized_ring(32, 1000, None);
        if let Ok(ring) = result {
            // Ring created successfully
            drop(ring);
        }
    }

    #[test]
    fn test_create_iopoll_ring() {
        // May fail without NVMe or in containers
        let result = create_iopoll_ring(32);
        if let Ok(ring) = result {
            drop(ring);
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
