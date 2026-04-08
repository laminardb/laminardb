//! In-memory streaming API — Source, Sink, Subscription backed by crossfire channels.

pub mod channel;
pub mod checkpoint;
pub mod config;
pub mod error;
pub mod sink;
pub mod source;
pub mod subscription;

pub use channel::{channel, Consumer, Producer};
pub use checkpoint::{CheckpointError, StreamCheckpointConfig, WalMode};
pub use config::{BackpressureStrategy, ChannelConfig, SourceConfig, WaitStrategy};
pub use error::{RecvError, StreamingError, TryPushError};
pub use sink::{Sink, SinkMode};
pub use source::{create, create_with_config, Record, Source};
pub use subscription::{Subscription, SubscriptionMessage};
