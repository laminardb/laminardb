//! Streaming API configuration types.
//!
//! This module defines configuration types for channels, sources, and sinks.

/// Default buffer size for channels (128KB worth of 64-byte cache lines).
pub const DEFAULT_BUFFER_SIZE: usize = 2048;

/// Minimum buffer size (must hold at least a few items).
pub const MIN_BUFFER_SIZE: usize = 4;

/// Maximum buffer size (prevent excessive memory usage).
pub const MAX_BUFFER_SIZE: usize = 1 << 20; // 1M entries

/// Backpressure strategy when buffer is full.
///
/// This determines what happens when a producer tries to push
/// to a full channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressureStrategy {
    /// Block until space is available.
    ///
    /// Best for exactly-once semantics where no data loss is acceptable.
    #[default]
    Block,

    /// Drop the oldest item to make room for the new one.
    ///
    /// Best for real-time systems where freshness matters more than completeness.
    DropOldest,

    /// Reject the push and return an error immediately.
    ///
    /// Lets the caller decide what to do with the rejected item.
    Reject,
}

/// Wait strategy for consumers when channel is empty.
///
/// This determines how a consumer waits for data when the channel is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WaitStrategy {
    /// Spin-loop without yielding (lowest latency, highest CPU).
    ///
    /// Best for ultra-low-latency scenarios with dedicated cores.
    Spin,

    /// Spin with occasional thread yields (balanced).
    ///
    /// Good balance between latency and CPU usage.
    #[default]
    SpinYield,

    /// Park the thread and wait for notification (lowest CPU).
    ///
    /// Best for batch processing where latency is less critical.
    Park,
}

/// Configuration for a streaming channel.
#[derive(Debug, Clone)]
pub struct ChannelConfig {
    /// Buffer size (will be rounded up to power of 2).
    pub buffer_size: usize,

    /// Backpressure strategy when buffer is full.
    pub backpressure: BackpressureStrategy,

    /// Wait strategy for consumers.
    pub wait_strategy: WaitStrategy,

    /// Whether to track statistics (small overhead).
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
    /// Creates a new channel configuration with the specified buffer size.
    #[must_use]
    pub fn with_buffer_size(buffer_size: usize) -> Self {
        Self {
            buffer_size: buffer_size.clamp(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE),
            ..Default::default()
        }
    }

    /// Creates a builder for custom configuration.
    #[must_use]
    pub fn builder() -> ChannelConfigBuilder {
        ChannelConfigBuilder::default()
    }

    /// Returns the effective buffer size (rounded to power of 2).
    #[must_use]
    pub fn effective_buffer_size(&self) -> usize {
        self.buffer_size
            .clamp(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE)
            .next_power_of_two()
    }
}

/// Builder for `ChannelConfig`.
#[derive(Debug, Default)]
pub struct ChannelConfigBuilder {
    buffer_size: Option<usize>,
    backpressure: Option<BackpressureStrategy>,
    wait_strategy: Option<WaitStrategy>,
    track_stats: Option<bool>,
}

impl ChannelConfigBuilder {
    /// Sets the buffer size.
    #[must_use]
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    /// Sets the backpressure strategy.
    #[must_use]
    pub fn backpressure(mut self, strategy: BackpressureStrategy) -> Self {
        self.backpressure = Some(strategy);
        self
    }

    /// Sets the wait strategy.
    #[must_use]
    pub fn wait_strategy(mut self, strategy: WaitStrategy) -> Self {
        self.wait_strategy = Some(strategy);
        self
    }

    /// Enables statistics tracking.
    #[must_use]
    pub fn track_stats(mut self, enabled: bool) -> Self {
        self.track_stats = Some(enabled);
        self
    }

    /// Builds the configuration.
    #[must_use]
    pub fn build(self) -> ChannelConfig {
        ChannelConfig {
            buffer_size: self
                .buffer_size
                .unwrap_or(DEFAULT_BUFFER_SIZE)
                .clamp(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE),
            backpressure: self.backpressure.unwrap_or_default(),
            wait_strategy: self.wait_strategy.unwrap_or_default(),
            track_stats: self.track_stats.unwrap_or(false),
        }
    }
}

/// Configuration for a Source.
#[derive(Debug, Clone, Default)]
pub struct SourceConfig {
    /// Channel configuration.
    pub channel: ChannelConfig,

    /// Name of the source (for debugging/metrics).
    pub name: Option<String>,
}

impl SourceConfig {
    /// Creates a new source configuration with the specified buffer size.
    #[must_use]
    pub fn with_buffer_size(buffer_size: usize) -> Self {
        Self {
            channel: ChannelConfig::with_buffer_size(buffer_size),
            name: None,
        }
    }

    /// Creates a named source configuration.
    #[must_use]
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            channel: ChannelConfig::default(),
            name: Some(name.into()),
        }
    }
}

/// Configuration for a Sink.
#[derive(Debug, Clone, Default)]
pub struct SinkConfig {
    /// Channel configuration for each subscriber.
    pub channel: ChannelConfig,

    /// Name of the sink (for debugging/metrics).
    pub name: Option<String>,

    /// Maximum number of subscribers (0 = unlimited).
    pub max_subscribers: usize,
}

impl SinkConfig {
    /// Creates a new sink configuration with the specified buffer size.
    #[must_use]
    pub fn with_buffer_size(buffer_size: usize) -> Self {
        Self {
            channel: ChannelConfig::with_buffer_size(buffer_size),
            name: None,
            max_subscribers: 0,
        }
    }

    /// Creates a named sink configuration.
    #[must_use]
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            channel: ChannelConfig::default(),
            name: Some(name.into()),
            max_subscribers: 0,
        }
    }
}

/// Statistics for a channel.
#[derive(Debug, Clone, Default)]
pub struct ChannelStats {
    /// Total items pushed.
    pub items_pushed: u64,

    /// Total items popped.
    pub items_popped: u64,

    /// Times push was blocked due to full buffer.
    pub push_blocked: u64,

    /// Items dropped due to backpressure.
    pub items_dropped: u64,

    /// Times pop found empty buffer.
    pub pop_empty: u64,
}

impl ChannelStats {
    /// Returns the number of items currently in flight.
    #[must_use]
    pub fn in_flight(&self) -> u64 {
        self.items_pushed.saturating_sub(self.items_popped)
    }

    /// Returns the drop rate (0.0 to 1.0).
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // Stats are approximate, precision loss is acceptable
    pub fn drop_rate(&self) -> f64 {
        if self.items_pushed == 0 {
            0.0
        } else {
            self.items_dropped as f64 / self.items_pushed as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ChannelConfig::default();
        assert_eq!(config.buffer_size, DEFAULT_BUFFER_SIZE);
        assert_eq!(config.backpressure, BackpressureStrategy::Block);
        assert_eq!(config.wait_strategy, WaitStrategy::SpinYield);
        assert!(!config.track_stats);
    }

    #[test]
    fn test_config_builder() {
        let config = ChannelConfig::builder()
            .buffer_size(1024)
            .backpressure(BackpressureStrategy::DropOldest)
            .wait_strategy(WaitStrategy::Park)
            .track_stats(true)
            .build();

        assert_eq!(config.buffer_size, 1024);
        assert_eq!(config.backpressure, BackpressureStrategy::DropOldest);
        assert_eq!(config.wait_strategy, WaitStrategy::Park);
        assert!(config.track_stats);
    }

    #[test]
    fn test_effective_buffer_size() {
        let config = ChannelConfig::with_buffer_size(100);
        assert_eq!(config.effective_buffer_size(), 128); // Next power of 2

        let config = ChannelConfig::with_buffer_size(1);
        assert_eq!(config.effective_buffer_size(), MIN_BUFFER_SIZE.next_power_of_two());
    }

    #[test]
    fn test_buffer_size_clamping() {
        let config = ChannelConfig::with_buffer_size(0);
        assert_eq!(config.buffer_size, MIN_BUFFER_SIZE);

        let config = ChannelConfig::with_buffer_size(usize::MAX);
        assert_eq!(config.buffer_size, MAX_BUFFER_SIZE);
    }

    #[test]
    fn test_source_config() {
        let config = SourceConfig::with_buffer_size(512);
        assert_eq!(config.channel.buffer_size, 512);
        assert!(config.name.is_none());

        let config = SourceConfig::named("my_source");
        assert_eq!(config.name.as_deref(), Some("my_source"));
    }

    #[test]
    fn test_sink_config() {
        let config = SinkConfig::with_buffer_size(256);
        assert_eq!(config.channel.buffer_size, 256);
        assert!(config.name.is_none());
        assert_eq!(config.max_subscribers, 0);

        let config = SinkConfig::named("my_sink");
        assert_eq!(config.name.as_deref(), Some("my_sink"));
    }

    #[test]
    fn test_channel_stats() {
        let mut stats = ChannelStats::default();
        stats.items_pushed = 100;
        stats.items_popped = 80;
        stats.items_dropped = 5;

        assert_eq!(stats.in_flight(), 20);
        assert!((stats.drop_rate() - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn test_channel_stats_empty() {
        let stats = ChannelStats::default();
        assert_eq!(stats.in_flight(), 0);
        assert!((stats.drop_rate() - 0.0).abs() < f64::EPSILON);
    }
}
