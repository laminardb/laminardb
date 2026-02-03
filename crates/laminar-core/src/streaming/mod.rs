//! # Streaming API
//!
//! In-memory streaming API for `LaminarDB` - embedded Kafka Streams-like semantics
//! with zero external dependencies.
//!
//! ## Overview
//!
//! This module provides a type-safe streaming API with:
//!
//! - **Source**: Entry point for data into a pipeline
//! - **Sink**: Consumption endpoint for data from a pipeline
//! - **Subscription**: Interface for receiving records from a sink
//! - **Channels**: Lock-free SPSC/MPSC communication
//!
//! ## Key Design Principles
//!
//! 1. **Channel type is auto-derived** - Never user-specified
//! 2. **SPSC → MPSC upgrade** - Automatic on `source.clone()`
//! 3. **Zero allocations on hot path** - Arena and batch operations
//! 4. **Checkpointing is optional** - Zero overhead when disabled
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use laminar_core::streaming::{self, Record, Source, Sink, Subscription};
//!
//! // Define your event type
//! #[derive(Clone)]
//! struct MyEvent {
//!     id: i64,
//!     value: f64,
//!     timestamp: i64,
//! }
//!
//! impl Record for MyEvent {
//!     fn schema() -> SchemaRef { /* ... */ }
//!     fn to_record_batch(&self) -> RecordBatch { /* ... */ }
//!     fn event_time(&self) -> Option<i64> { Some(self.timestamp) }
//! }
//!
//! // Create source and sink
//! let (source, sink) = streaming::create::<MyEvent>(1024);
//!
//! // Push data (single producer = SPSC mode)
//! source.push(event)?;
//!
//! // Clone for multiple producers (triggers MPSC upgrade)
//! let source2 = source.clone();
//! assert!(source.is_mpsc());
//!
//! // Subscribe to receive data
//! let subscription = sink.subscribe();
//!
//! // Consume via polling
//! while let Some(batch) = subscription.poll() {
//!     process(batch);
//! }
//!
//! // Or iterate
//! for batch in subscription {
//!     process(batch);
//! }
//! ```
//!
//! ## Module Structure
//!
//! - [`config`]: Configuration types for channels, sources, and sinks
//! - [`error`]: Error types for streaming operations
//! - [`ring_buffer`]: Lock-free ring buffer implementation
//! - [`channel`]: SPSC/MPSC channel with automatic upgrade
//! - [`source`]: Source API and Record trait
//! - [`sink`]: Sink API with subscription support
//! - [`subscription`]: Subscription API for consuming records
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────────┐
//! │   Source    │────▶│   Channel   │────▶│      Sink       │
//! │             │     │ (SPSC/MPSC) │     │                 │
//! │ push()      │     │             │     │ subscribe()     │
//! │ watermark() │     │             │     │                 │
//! └─────────────┘     └─────────────┘     └────────┬────────┘
//!       │                                          │
//!       │ clone()                                  │
//!       │ (triggers MPSC)                          ▼
//!       │                                 ┌─────────────────┐
//!       │                                 │  Subscription   │
//!       │                                 │                 │
//!       │                                 │ poll()          │
//!       │                                 │ recv()          │
//!       │                                 │ Iterator        │
//!       │                                 └─────────────────┘
//! ```
//!
//! ## Performance
//!
//! | Operation | Target | Notes |
//! |-----------|--------|-------|
//! | Ring buffer push | < 20ns | Power-of-2, cache-padded |
//! | SPSC channel push | < 50ns | Lock-free, batch support |
//! | MPSC channel push | < 150ns | CAS-based slot claiming |
//! | Source push | < 100ns | Includes watermark update |
//! | Arrow batch push | < 1μs | Zero-copy schema validation |
//!
//! ## Backpressure
//!
//! Configurable strategies when buffers are full:
//!
//! - **Block**: Wait until space is available (default, exactly-once friendly)
//! - **`DropOldest`**: Overwrite oldest data (real-time systems)
//! - **Reject**: Return error immediately (caller decides)

pub mod broadcast;
pub mod channel;
pub mod checkpoint;
pub mod config;
pub mod error;
pub mod ring_buffer;
pub mod sink;
pub mod source;
pub mod subscription;

// Re-export key types
pub use channel::{channel, channel_with_config, ChannelMode, Consumer, Producer};
pub use checkpoint::{
    CheckpointError, StreamCheckpoint, StreamCheckpointConfig, StreamCheckpointManager, WalMode,
};
pub use config::{
    BackpressureStrategy, ChannelConfig, ChannelStats, SinkConfig, SourceConfig, WaitStrategy,
};
pub use error::{RecvError, StreamingError, TryPushError};
pub use ring_buffer::RingBuffer;
pub use sink::{Sink, SinkMode};
pub use source::{create, create_with_config, Record, Source};
pub use subscription::{Subscription, SubscriptionMessage};

// Broadcast channel re-exports
pub use broadcast::{
    BroadcastChannel, BroadcastConfig, BroadcastConfigBuilder, BroadcastError,
    SlowSubscriberPolicy, SubscriberInfo,
};
