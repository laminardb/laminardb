//! In-memory streaming API — Source, Sink, Subscription with broadcast fan-out.

mod arrow_admission;
pub mod channel;
pub mod checkpoint;
pub mod config;
pub mod error;
pub mod sink;
pub mod source;
pub mod subscription;

pub use arrow_admission::{
    retained_arrow_bytes, validate_source_max_queued_bytes, DEFAULT_SOURCE_MAX_QUEUED_BYTES,
    MAX_SOURCE_QUEUED_BYTES,
};
pub use channel::{channel, AsyncConsumer, Producer};
pub use checkpoint::StreamCheckpointConfig;
pub use config::{BackpressureStrategy, ChannelConfig, SourceConfig, WaitStrategy};
pub use error::{RecvError, StreamingError, TryPushError};
pub use sink::Sink;
pub use source::{create, create_with_config, Record, Source};
pub use subscription::Subscription;
