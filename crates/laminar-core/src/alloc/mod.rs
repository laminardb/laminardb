//! Allocation guards and priority class enforcement for the data path.
//!
//! This module provides tools for latency-critical code paths:
//!
//! - **Hot Path Guard**: RAII marker for allocation-sensitive sections
//! - **Priority Guard**: Debug-build enforcement of priority class contracts
//! - **Ring Buffer**: Fixed-capacity ring buffer for event queues
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_core::alloc::{HotPathGuard, PriorityGuard, PriorityClass};
//!
//! fn process_event(event: &Event) {
//!     let _guard = HotPathGuard::enter("process_event");
//!     let _priority = PriorityGuard::enter(PriorityClass::EventProcessing);
//!     // Hot path code — debug builds verify priority class
//! }
//! ```

mod guard;
mod ring_buffer;
/// Lock-free SPSC queue and cache-line padding.
pub mod spsc;

pub use guard::{HotPathGuard, PriorityClass, PriorityGuard};
pub use ring_buffer::RingBuffer;
pub use spsc::{CachePadded, SpscQueue};
