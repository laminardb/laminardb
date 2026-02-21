//! WebSocket source and sink connectors for LaminarDB.
//!
//! Provides four connector modes:
//! - **Source client**: Connects to an external WebSocket server (e.g., exchange feeds)
//! - **Source server**: Listens for incoming WebSocket connections (e.g., `IoT` sensors)
//! - **Sink server**: Fans out streaming query results to connected subscribers
//! - **Sink client**: Pushes streaming query output to an external WebSocket server
//!
//! # Delivery Guarantees
//!
//! WebSocket is a **non-replayable** transport. Source connectors provide
//! **at-most-once** or **best-effort** delivery — there are no offsets to
//! seek to on recovery. Sink connectors optionally support a replay buffer
//! for client-side resume, but this is bounded and best-effort.
//!
//! # Architecture
//!
//! All WebSocket I/O runs in Ring 2 (Tokio tasks). Parsed Arrow
//! `RecordBatch` data crosses to Ring 0 via bounded channels.

pub mod backpressure;
pub mod checkpoint;
pub mod connection;
pub mod fanout;
pub mod metrics;
pub mod parser;
pub mod protocol;
pub mod serializer;
pub mod sink;
pub mod sink_client;
pub mod sink_config;
pub mod sink_metrics;
pub mod source;
pub mod source_config;
pub mod source_server;

pub use backpressure::BackpressureStrategy;
pub use checkpoint::WebSocketSourceCheckpoint;
pub use metrics::WebSocketSourceMetrics;
pub use protocol::{ClientMessage, ServerMessage};
pub use sink::WebSocketSinkServer;
pub use sink_client::WebSocketSinkClient;
pub use sink_config::{SinkFormat, SinkMode, SlowClientPolicy, WebSocketSinkConfig};
pub use sink_metrics::WebSocketSinkMetrics;
pub use source::WebSocketSource;
pub use source_config::{
    EventTimeFormat, MessageFormat, ReconnectConfig, SourceMode, WebSocketSourceConfig,
    WsAuthConfig,
};
pub use source_server::WebSocketSourceServer;
