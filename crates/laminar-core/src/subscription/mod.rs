//! # Reactive Subscription System
//!
//! Foundation types for the push-based subscription system that delivers
//! change events from materialized views and streaming queries to consumers.
//!
//! ## Architecture
//!
//! The subscription system spans all three rings:
//!
//! - **Ring 0**: `NotificationRef` — zero-allocation, cache-line-aligned notification
//! - **Ring 1**: `ChangeEvent` — data delivery with `Arc<RecordBatch>` payloads
//! - **Ring 2**: Subscription lifecycle management (future F-SUB-003+)
//!
//! ## Types
//!
//! - [`EventType`] — Discriminant for change event kinds (Insert/Delete/Update/Watermark/Snapshot)
//! - [`NotificationRef`] — 64-byte cache-aligned Ring 0 notification slot
//! - [`ChangeEvent`] — Rich change event with Arrow data for Ring 1 delivery
//! - [`ChangeEventBatch`] — Coalesced batch of change events

mod backpressure;
mod batcher;
mod callback;
mod dispatcher;
mod event;
mod filter;
mod handle;
mod notification;
mod registry;
mod stream;

pub use backpressure::{BackpressureController, DemandBackpressure, DemandHandle};
pub use batcher::{BatchConfig, NotificationBatcher};
pub use callback::{
    subscribe_callback, subscribe_fn, CallbackSubscriptionHandle, SubscriptionCallback,
};
pub use dispatcher::{
    DispatcherConfig, DispatcherMetrics, NotificationDataSource, SubscriptionDispatcher,
};
pub use event::{ChangeEvent, ChangeEventBatch, EventType, NotificationRef};
pub use filter::{
    compile_filter, FilterCompileError, Ring0Predicate, Ring1Predicate, ScalarValue,
    StringInternTable, SubscriptionFilter,
};
pub use handle::{PushSubscription, PushSubscriptionError};
pub use notification::{NotificationHub, NotificationRing, NotificationSlot};
pub use registry::{
    BackpressureStrategy, SubscriptionConfig, SubscriptionEntry, SubscriptionId,
    SubscriptionMetrics, SubscriptionRegistry, SubscriptionState,
};
pub use stream::{
    subscribe_stream, subscribe_stream_with_errors, ChangeEventResultStream, ChangeEventStream,
};
