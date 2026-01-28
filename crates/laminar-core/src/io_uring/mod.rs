//! # `io_uring` Advanced Optimization (F067)
//!
//! Implements advanced `io_uring` optimizations including SQPOLL mode, registered buffers,
//! IOPOLL mode, and per-core ring management for sub-microsecond I/O latency.
//!
//! ## Features
//!
//! - **SQPOLL Mode**: Eliminates syscalls by using a dedicated kernel polling thread
//! - **Registered Buffers**: Avoids per-operation buffer mapping overhead
//! - **IOPOLL Mode**: Polls completions directly from `NVMe` queue (no interrupts)
//! - **Per-Core Rings**: Each core has its own `io_uring` instance for thread-per-core
//! - **Three-Ring I/O (F069)**: Separate latency/main/poll rings for optimal scheduling
//!
//! ## Research Background
//!
//! From TU Munich (Dec 2024): Basic `io_uring` yields only 1.06-1.10x improvement,
//! while careful optimization achieves 2.05x or more.
//!
//! ## Platform Support
//!
//! This module is Linux-only (`io_uring` requires Linux 5.10+, advanced features need 5.19+).
//! On other platforms, a no-op fallback is provided.
//!
//! ## Example
//!
//! ```rust,ignore
//! use laminar_core::io_uring::{IoUringConfig, CoreRingManager};
//!
//! let config = IoUringConfig::builder()
//!     .ring_entries(256)
//!     .enable_sqpoll(true)
//!     .sqpoll_idle_ms(1000)
//!     .buffer_size(64 * 1024)
//!     .buffer_count(256)
//!     .build();
//!
//! let mut manager = CoreRingManager::new(0, &config)?;
//!
//! // Submit and poll I/O operations
//! let user_data = manager.submit_read(fd, offset, len)?;
//! let completions = manager.poll_completions();
//! ```
//!
//! ## Three-Ring I/O (F069)
//!
//! For optimal latency/throughput balance, use the three-ring reactor:
//!
//! ```rust,ignore
//! use laminar_core::io_uring::three_ring::{ThreeRingConfig, ThreeRingReactor};
//!
//! let config = ThreeRingConfig::builder()
//!     .latency_entries(256)
//!     .main_entries(1024)
//!     .build()?;
//!
//! let mut reactor = ThreeRingReactor::new(0, config)?;
//! // Latency ring for network, main ring for WAL, poll ring for NVMe
//! ```

mod config;
mod error;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod buffer_pool;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod manager;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod ring;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod sink;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod three_ring;

pub use config::{IoUringConfig, IoUringConfigBuilder, RingMode};
pub use error::IoUringError;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use buffer_pool::RegisteredBufferPool;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use manager::{Completion, CompletionKind, CoreRingManager, PendingOp, RingMetrics};
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use ring::{create_iopoll_ring, create_optimized_ring, IoUringRing};
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use sink::IoUringSink;

/// Check if `io_uring` is available on this platform.
#[must_use]
pub const fn is_available() -> bool {
    cfg!(all(target_os = "linux", feature = "io-uring"))
}

/// Get the minimum kernel version required for basic `io_uring` features.
#[must_use]
pub const fn min_kernel_version() -> &'static str {
    "5.10"
}

/// Get the kernel version required for advanced features (IOPOLL, passthrough).
#[must_use]
pub const fn advanced_kernel_version() -> &'static str {
    "5.19"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_availability() {
        // This test passes on any platform
        let _ = is_available();
    }

    #[test]
    fn test_kernel_versions() {
        assert_eq!(min_kernel_version(), "5.10");
        assert_eq!(advanced_kernel_version(), "5.19");
    }
}
