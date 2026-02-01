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

mod event;

pub use event::{ChangeEvent, ChangeEventBatch, EventType, NotificationRef};
