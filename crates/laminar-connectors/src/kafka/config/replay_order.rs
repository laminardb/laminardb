//! Explicit Kafka input ordering profiles.

use crate::error::ConnectorError;

/// Ordering and batch cuts reproduced from engine-owned Kafka positions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum KafkaReplayOrder {
    /// Preserve ordinary broker arrival order without a cross-partition replay guarantee.
    #[default]
    Unspecified,
    /// Emit one record from every fixed partition, sorted by topic and partition.
    /// An idle partition holds the entire round. Cluster intake follows vnode zero.
    PartitionRounds,
}

str_enum!(fromstr KafkaReplayOrder, lowercase, ConnectorError, "invalid replay.order",
    Unspecified => "unspecified";
    PartitionRounds => "partition_rounds"
);

impl KafkaReplayOrder {
    pub(super) fn validate(self, config: &super::KafkaSourceConfig) -> Result<(), ConnectorError> {
        if self == Self::Unspecified {
            return Ok(());
        }
        if config.format != crate::serde::Format::Json
            || !matches!(config.subscription, super::TopicSubscription::Topics(_))
            || !matches!(
                config.startup_mode,
                super::StartupMode::Earliest | super::StartupMode::Latest
            )
            || config.fetch_max_bytes.is_some_and(|bytes| bytes <= 0)
        {
            return Err(ConnectorError::ConfigurationError(
                "Kafka partition_rounds requires JSON, fixed topics, earliest or sealed latest positions, and a positive fetch.max.bytes bound".into(),
            ));
        }
        Ok(())
    }
}
