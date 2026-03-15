//! Zero-allocation enforcement for Ring 0 hot path.
//!
//! This module provides tools to detect and prevent heap allocations in
//! latency-critical code paths. In Ring 0 (hot path), a single allocation
//! can blow the <500ns latency budget.
//!
//! # Features
//!
//! - **Hot Path Guard**: RAII guard to mark hot path sections
//! - **Ring Buffer**: Fixed-capacity ring buffer for event queues
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_core::alloc::HotPathGuard;
//!
//! fn process_event(event: &Event) {
//!     let _guard = HotPathGuard::enter("process_event");
//!     // Hot path section marked for documentation and profiling
//! }
//! ```

mod guard;
mod ring_buffer;
/// Lock-free SPSC queue and cache-line padding.
pub mod spsc;

pub use guard::HotPathGuard;
pub use ring_buffer::RingBuffer;
pub use spsc::{CachePadded, SpscQueue};
