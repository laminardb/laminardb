//! Named latency presets over the existing source fetch and producer batching settings.
//!
//! Precedence is: connector defaults, then the profile, then explicit options, then validation.
//! A profile never changes acknowledgements, idempotence, isolation, transactions or the delivery
//! guarantee, so a faster profile cannot weaken a guarantee.

/// `latency.profile` option shared by the Kafka source and sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LatencyProfile {
    /// Smallest batching delays and per-cycle batches.
    LowLatency,
    /// The connector defaults.
    #[default]
    Balanced,
    /// Larger fetches and producer batches with compression.
    Throughput,
}

str_enum!(LatencyProfile, lowercase, ConnectorError, "invalid latency.profile",
    LowLatency => "low_latency", "low-latency";
    Balanced => "balanced";
    Throughput => "throughput"
);

/// Source settings a profile supplies when the option is not set explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceProfileDefaults {
    pub(crate) max_poll_records: usize,
    pub(crate) fetch_min_bytes: Option<i32>,
    pub(crate) fetch_max_wait_ms: Option<i32>,
}

/// Producer settings a profile supplies when the option is not set explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SinkProfileDefaults {
    pub(crate) linger_ms: u64,
    pub(crate) batch_size: usize,
    pub(crate) compression: super::CompressionType,
}

impl LatencyProfile {
    pub(crate) const fn source_defaults(self) -> SourceProfileDefaults {
        match self {
            Self::LowLatency => SourceProfileDefaults {
                max_poll_records: 256,
                fetch_min_bytes: Some(1),
                fetch_max_wait_ms: Some(10),
            },
            Self::Balanced => SourceProfileDefaults {
                max_poll_records: 1000,
                fetch_min_bytes: None,
                fetch_max_wait_ms: None,
            },
            Self::Throughput => SourceProfileDefaults {
                max_poll_records: 8192,
                fetch_min_bytes: Some(64 * 1024),
                fetch_max_wait_ms: Some(100),
            },
        }
    }

    pub(crate) const fn sink_defaults(self) -> SinkProfileDefaults {
        match self {
            Self::LowLatency => SinkProfileDefaults {
                linger_ms: 0,
                batch_size: 16_384,
                compression: super::CompressionType::None,
            },
            Self::Balanced => SinkProfileDefaults {
                linger_ms: 5,
                batch_size: 16_384,
                compression: super::CompressionType::None,
            },
            Self::Throughput => SinkProfileDefaults {
                linger_ms: 50,
                batch_size: 1024 * 1024,
                compression: super::CompressionType::Lz4,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_names_and_aliases() {
        assert_eq!(
            "low_latency".parse::<LatencyProfile>().unwrap(),
            LatencyProfile::LowLatency
        );
        assert_eq!(
            "LOW-LATENCY".parse::<LatencyProfile>().unwrap(),
            LatencyProfile::LowLatency
        );
        assert_eq!(
            "throughput".parse::<LatencyProfile>().unwrap(),
            LatencyProfile::Throughput
        );
        assert!("fastest".parse::<LatencyProfile>().is_err());
    }

    #[test]
    fn balanced_is_the_historical_default() {
        let source = LatencyProfile::Balanced.source_defaults();
        assert_eq!(source.max_poll_records, 1000);
        assert_eq!(source.fetch_min_bytes, None);
        let sink = LatencyProfile::Balanced.sink_defaults();
        assert_eq!(sink.linger_ms, 5);
        assert_eq!(sink.batch_size, 16_384);
    }
}
