//! # Three-Ring I/O Architecture (F069)
//!
//! Implements the three-ring I/O pattern from Seastar/Glommio where each core uses
//! multiple `io_uring` instances serving different latency requirements:
//!
//! - **Latency Ring**: Always polled, never blocks. For network receives and urgent ops.
//! - **Main Ring**: Can block when idle. For WAL writes and normal I/O.
//! - **Poll Ring**: IOPOLL for NVMe storage (optional).
//!
//! ## Key Insight
//!
//! When the main ring blocks waiting for I/O, the latency ring's fd is registered
//! for polling, so any latency-critical completion immediately wakes the thread.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Per-Core I/O Architecture                │
//! ├─────────────────────────────────────────────────────────────┤
//! │   ┌─────────────────┐                                       │
//! │   │  Latency Ring   │  ← Network receives, urgent ops       │
//! │   │  (non-blocking) │    Always polled first                │
//! │   └────────┬────────┘                                       │
//! │            │ eventfd registered on main ring for wake-up    │
//! │            ▼                                                │
//! │   ┌─────────────────┐                                       │
//! │   │   Main Ring     │  ← WAL writes, normal I/O             │
//! │   │  (can block)    │    Blocks when idle                   │
//! │   └────────┬────────┘                                       │
//! │            │                                                │
//! │            ▼                                                │
//! │   ┌─────────────────┐                                       │
//! │   │   Poll Ring     │  ← IOPOLL for NVMe (optional)         │
//! │   │  (storage only) │    Cannot have sockets                │
//! │   └─────────────────┘                                       │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use laminar_core::io_uring::three_ring::{
//!     ThreeRingConfig, ThreeRingReactor, RingAffinity, OperationType,
//! };
//!
//! // Create configuration
//! let config = ThreeRingConfig::builder()
//!     .latency_entries(256)
//!     .main_entries(1024)
//!     .enable_poll_ring(true)
//!     .build()?;
//!
//! // Create reactor
//! let mut reactor = ThreeRingReactor::new(0, config)?;
//!
//! // Submit operations to appropriate rings
//! reactor.submit_to_latency(recv_entry)?;
//! reactor.submit_to_main(wal_write_entry)?;
//! reactor.submit_to_poll(storage_read_entry)?;
//!
//! // Poll completions (latency ring always first)
//! let completions = reactor.poll_all()?;
//! ```
//!
//! ## Platform Support
//!
//! This module is Linux-only (requires Linux 5.10+, SQPOLL needs 5.11+).
//! On other platforms, a no-op fallback is provided.
//!
//! ## Research References
//!
//! - [Seastar I/O Architecture](https://seastar.io/tutorial/#io-scheduling)
//! - [Glommio Rings](https://docs.rs/glommio/latest/glommio/)
//! - [Thread-Per-Core 2026 Research](../../docs/research/laminardb-thread-per-core-2026-research.md)

mod affinity;
mod config;
mod handler;
mod reactor;
mod router;
mod stats;

pub use affinity::{OperationType, RingAffinity};
pub use config::{ThreeRingConfig, ThreeRingConfigBuilder};
pub use handler::{RingHandler, SimpleRingHandler};
pub use reactor::ThreeRingReactor;
pub use router::{CompletionRouter, PendingOperation, RoutedCompletion};
pub use stats::ThreeRingStats;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_affinity_classification() {
        assert_eq!(
            RingAffinity::for_operation(&OperationType::NetworkRecv),
            RingAffinity::Latency
        );
        assert_eq!(
            RingAffinity::for_operation(&OperationType::WalWrite),
            RingAffinity::Main
        );
        assert_eq!(
            RingAffinity::for_operation(&OperationType::StorageRead),
            RingAffinity::Poll
        );
    }

    #[test]
    fn test_default_config() {
        let config = ThreeRingConfig::default();
        assert_eq!(config.latency_entries, 256);
        assert_eq!(config.main_entries, 1024);
        assert_eq!(config.poll_entries, 256);
        assert!(!config.enable_poll_ring);
    }

    #[test]
    fn test_config_builder() {
        let config = ThreeRingConfig::builder()
            .latency_entries(128)
            .main_entries(512)
            .poll_entries(64)
            .enable_poll_ring(true)
            .latency_sqpoll_idle_ms(50)
            .main_sqpoll_idle_ms(500)
            .build_unchecked();

        assert_eq!(config.latency_entries, 128);
        assert_eq!(config.main_entries, 512);
        assert_eq!(config.poll_entries, 64);
        assert!(config.enable_poll_ring);
        assert_eq!(config.latency_sqpoll_idle_ms, 50);
        assert_eq!(config.main_sqpoll_idle_ms, 500);
    }

    #[test]
    fn test_stats_default() {
        let stats = ThreeRingStats::default();
        assert_eq!(stats.latency_completions, 0);
        assert_eq!(stats.main_completions, 0);
        assert_eq!(stats.poll_completions, 0);
        assert_eq!(stats.main_ring_sleeps, 0);
        assert_eq!(stats.latency_wake_ups, 0);
    }
}
