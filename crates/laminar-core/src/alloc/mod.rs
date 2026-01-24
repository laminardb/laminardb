//! Zero-allocation enforcement for Ring 0 hot path.
//!
//! This module provides tools to detect and prevent heap allocations in
//! latency-critical code paths. In Ring 0 (hot path), a single allocation
//! can blow the <500ns latency budget.
//!
//! # Features
//!
//! - **Allocation Detector**: Custom allocator that panics on allocation in marked sections
//! - **Hot Path Guard**: RAII guard to mark hot path sections
//! - **Object Pool**: Pre-allocated object pool for zero-allocation acquire/release
//! - **Ring Buffer**: Fixed-capacity ring buffer for event queues
//! - **Scratch Buffer**: Thread-local temporary storage
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_core::alloc::{HotPathGuard, ObjectPool};
//!
//! // Mark a section as hot path (panics on allocation in debug builds)
//! fn process_event(event: &Event) {
//!     let _guard = HotPathGuard::enter("process_event");
//!     // Any allocation here will panic in debug builds with allocation-tracking
//! }
//!
//! // Use pre-allocated pools instead of heap allocation
//! let mut pool: ObjectPool<MyType, 64> = ObjectPool::new();
//! let obj = pool.acquire().unwrap();
//! pool.release(obj);
//! ```
//!
//! # Feature Flags
//!
//! Enable `allocation-tracking` feature to activate allocation detection:
//!
//! ```toml
//! [dependencies]
//! laminar-core = { version = "0.1", features = ["allocation-tracking"] }
//! ```

mod detector;
mod guard;
mod object_pool;
mod ring_buffer;
mod scratch;

pub use detector::AllocationStats;
pub use guard::HotPathGuard;
pub use object_pool::ObjectPool;
pub use ring_buffer::RingBuffer;
pub use scratch::ScratchBuffer;

// Re-export feature-gated items
#[cfg(feature = "allocation-tracking")]
pub use detector::{is_hot_path_enabled, set_panic_on_alloc, HotPathDetectingAlloc};
