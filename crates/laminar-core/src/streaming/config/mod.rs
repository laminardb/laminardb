//! Configuration types for channels, sources, and sinks.

/// Default buffer size for channels.
pub const DEFAULT_BUFFER_SIZE: usize = 2048;

/// Minimum buffer size.
pub const MIN_BUFFER_SIZE: usize = 4;

/// Maximum buffer size.
pub const MAX_BUFFER_SIZE: usize = 1 << 20; // 1M entries

/// Backpressure strategy when buffer is full.
///
/// Stored in source/sink configs and exposed in SQL DDL (`BACKPRESSURE = '...'`).
/// Source pushes are always nonblocking and reject on count or Arrow-byte saturation.
/// Snapshot history evicts its oldest entries independently of this setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressureStrategy {
    /// Block until space is available (default).
    #[default]
    Block,
    /// Drop the oldest item to make room.
    DropOldest,
    /// Reject the push immediately.
    Reject,
}

impl std::str::FromStr for BackpressureStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "block" | "blocking" => Ok(Self::Block),
            "drop" | "drop_oldest" | "dropoldest" => Ok(Self::DropOldest),
            "reject" | "error" => Ok(Self::Reject),
            _ => Err(format!(
                "invalid backpressure strategy: '{s}'. Valid values: block, drop_oldest, reject"
            )),
        }
    }
}

/// Wait strategy — parsed from SQL DDL for forward-compatibility.
/// Currently unused; crossfire handles its own backoff internally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WaitStrategy {
    /// Spin-loop.
    Spin,
    /// Spin with yields (default).
    #[default]
    SpinYield,
    /// Park the thread.
    Park,
}

impl std::str::FromStr for WaitStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "spin" => Ok(Self::Spin),
            "spin_yield" | "spinyield" | "yield" => Ok(Self::SpinYield),
            "park" | "parking" => Ok(Self::Park),
            _ => Err(format!(
                "invalid wait strategy: '{s}'. Valid values: spin, spin_yield, park"
            )),
        }
    }
}

/// Channel configuration. Only `buffer_size` is used by the channel;
/// `backpressure` and `wait_strategy` are stored for higher-level consumers.
#[derive(Debug, Clone)]
pub struct ChannelConfig {
    /// Buffer size.
    pub buffer_size: usize,
    /// Backpressure strategy (used by catalog/snapshot ring, not the channel).
    pub backpressure: BackpressureStrategy,
    /// Wait strategy (reserved for future use).
    pub wait_strategy: WaitStrategy,
    /// Whether to track statistics (reserved for future use).
    pub track_stats: bool,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER_SIZE,
            backpressure: BackpressureStrategy::Block,
            wait_strategy: WaitStrategy::SpinYield,
            track_stats: false,
        }
    }
}

impl ChannelConfig {
    /// Creates a config with the specified buffer size.
    #[must_use]
    pub fn with_buffer_size(buffer_size: usize) -> Self {
        Self {
            buffer_size: buffer_size.clamp(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE),
            ..Default::default()
        }
    }
}

/// Configuration for a Source.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Channel configuration.
    pub channel: ChannelConfig,
    /// Name of the source (for debugging/metrics).
    pub name: Option<String>,
    /// Arrow-byte limit shared by cloned producers, the input ring and queued broadcast data.
    /// Defaults to 64 MiB. Generic `Record<T>` pushes remain count bounded; their arbitrary
    /// heap storage is not measured. Invalid limits make `push_arrow` return `InvalidConfig`.
    pub max_queued_bytes: usize,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            channel: ChannelConfig::default(),
            name: None,
            max_queued_bytes: super::DEFAULT_SOURCE_MAX_QUEUED_BYTES,
        }
    }
}

impl SourceConfig {
    /// Creates a source config with the specified buffer size.
    #[must_use]
    pub fn with_buffer_size(buffer_size: usize) -> Self {
        Self {
            channel: ChannelConfig::with_buffer_size(buffer_size),
            ..Self::default()
        }
    }

    /// Creates a named source config.
    #[must_use]
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests;
