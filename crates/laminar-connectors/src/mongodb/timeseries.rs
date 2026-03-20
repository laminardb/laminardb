//! `MongoDB` time series collection configuration and validation.
//!
//! Time series collections in `MongoDB` use automatic bucketing for efficient
//! storage and querying of time-stamped measurement data. This module
//! provides typed configuration for creating and validating time series
//! collections.
//!
//! # Important Constraints
//!
//! - Time series collections only accept `insert` operations; other write
//!   modes are rejected at the sink level.
//! - Granularity can only be increased (seconds → minutes → hours), never
//!   decreased after collection creation.
//! - `MongoDB` does not support `watch()` (change streams) on time series
//!   collections — the source pre-flight guard rejects these.

use crate::error::ConnectorError;

/// Whether the target collection is a standard or time series collection.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CollectionKind {
    /// Standard `MongoDB` collection.
    #[default]
    Standard,
    /// Time series collection with bucketing configuration.
    TimeSeries(TimeSeriesConfig),
}

/// Configuration for a `MongoDB` time series collection.
///
/// Maps to `MongoDB`'s `timeseries` collection option. The `time_field`
/// is required; `meta_field` and TTL are optional.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TimeSeriesConfig {
    /// The field in each document that contains the date.
    pub time_field: String,

    /// An optional field that labels the data source (e.g., sensor ID).
    /// Documents with the same meta value are bucketed together.
    pub meta_field: Option<String>,

    /// Bucketing granularity.
    pub granularity: TimeSeriesGranularity,

    /// Optional TTL: automatically delete documents after this many seconds.
    pub expire_after_seconds: Option<u64>,
}

/// Time series bucketing granularity.
///
/// Controls the bucket span for time series collections:
///
/// | Granularity | Bucket Span |
/// |-------------|-------------|
/// | Seconds     | 1 hour      |
/// | Minutes     | 24 hours    |
/// | Hours       | 30 days     |
/// | Custom      | User-defined (`MongoDB` ≥ 6.3) |
///
/// Granularity can only increase (seconds → minutes → hours) on an
/// existing collection. Attempting to decrease returns
/// `GranularityDecreaseDenied`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TimeSeriesGranularity {
    /// Bucket span = 1 hour (default).
    #[default]
    Seconds,
    /// Bucket span = 24 hours.
    Minutes,
    /// Bucket span = 30 days.
    Hours,
    /// Custom bucketing (`MongoDB` ≥ 6.3).
    ///
    /// **Invariant**: `bucket_max_span_seconds` must equal
    /// `bucket_rounding_seconds`. This is enforced at construction via
    /// [`TimeSeriesGranularity::custom`].
    Custom {
        /// Maximum span of a single bucket in seconds.
        bucket_max_span_seconds: u32,
        /// Rounding boundary in seconds (must equal `bucket_max_span_seconds`).
        bucket_rounding_seconds: u32,
    },
}

impl TimeSeriesGranularity {
    /// Creates a `Custom` granularity, enforcing the invariant that
    /// `bucket_max_span_seconds == bucket_rounding_seconds`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the values differ.
    pub fn custom(
        bucket_max_span_seconds: u32,
        bucket_rounding_seconds: u32,
    ) -> Result<Self, ConnectorError> {
        if bucket_max_span_seconds != bucket_rounding_seconds {
            return Err(ConnectorError::ConfigurationError(format!(
                "time series custom granularity requires bucket_max_span_seconds ({bucket_max_span_seconds}) \
                 == bucket_rounding_seconds ({bucket_rounding_seconds})"
            )));
        }
        if bucket_max_span_seconds == 0 {
            return Err(ConnectorError::ConfigurationError(
                "time series custom granularity bucket_max_span_seconds must be > 0".to_string(),
            ));
        }
        Ok(Self::Custom {
            bucket_max_span_seconds,
            bucket_rounding_seconds,
        })
    }

    /// Returns an ordinal for comparison (higher = coarser granularity).
    /// Custom granularity returns the span as its ordinal.
    #[must_use]
    fn ordinal(self) -> u32 {
        match self {
            Self::Seconds => 1,
            Self::Minutes => 2,
            Self::Hours => 3,
            Self::Custom {
                bucket_max_span_seconds,
                ..
            } => bucket_max_span_seconds,
        }
    }

    /// Returns `true` if `self` is a finer (or equal) granularity than `other`.
    ///
    /// Used to validate that granularity changes only increase.
    #[must_use]
    pub fn is_finer_or_equal(self, other: Self) -> bool {
        self.ordinal() <= other.ordinal()
    }

    /// Validates that changing from `current` to `requested` is allowed.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the change would
    /// decrease granularity.
    pub fn validate_change(current: Self, requested: Self) -> Result<(), ConnectorError> {
        if !current.is_finer_or_equal(requested) {
            return Err(ConnectorError::ConfigurationError(format!(
                "cannot decrease time series granularity from {current:?} to {requested:?}"
            )));
        }
        Ok(())
    }
}

impl std::fmt::Display for TimeSeriesGranularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Seconds => f.write_str("seconds"),
            Self::Minutes => f.write_str("minutes"),
            Self::Hours => f.write_str("hours"),
            Self::Custom {
                bucket_max_span_seconds,
                ..
            } => write!(f, "custom({bucket_max_span_seconds}s)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_granularity_default() {
        assert_eq!(
            TimeSeriesGranularity::default(),
            TimeSeriesGranularity::Seconds
        );
    }

    #[test]
    fn test_custom_granularity_valid() {
        let g = TimeSeriesGranularity::custom(3600, 3600).unwrap();
        assert!(matches!(g, TimeSeriesGranularity::Custom { .. }));
    }

    #[test]
    fn test_custom_granularity_mismatch() {
        let err = TimeSeriesGranularity::custom(3600, 1800).unwrap_err();
        assert!(err.to_string().contains("bucket_max_span_seconds"));
    }

    #[test]
    fn test_custom_granularity_zero() {
        let err = TimeSeriesGranularity::custom(0, 0).unwrap_err();
        assert!(err.to_string().contains("must be > 0"));
    }

    #[test]
    fn test_granularity_ordering() {
        assert!(TimeSeriesGranularity::Seconds.is_finer_or_equal(TimeSeriesGranularity::Minutes));
        assert!(TimeSeriesGranularity::Minutes.is_finer_or_equal(TimeSeriesGranularity::Hours));
        assert!(!TimeSeriesGranularity::Hours.is_finer_or_equal(TimeSeriesGranularity::Seconds));
        assert!(TimeSeriesGranularity::Seconds.is_finer_or_equal(TimeSeriesGranularity::Seconds));
    }

    #[test]
    fn test_validate_change_increase_ok() {
        TimeSeriesGranularity::validate_change(
            TimeSeriesGranularity::Seconds,
            TimeSeriesGranularity::Minutes,
        )
        .unwrap();
    }

    #[test]
    fn test_validate_change_decrease_denied() {
        let err = TimeSeriesGranularity::validate_change(
            TimeSeriesGranularity::Hours,
            TimeSeriesGranularity::Seconds,
        )
        .unwrap_err();
        assert!(err.to_string().contains("decrease"));
    }

    #[test]
    fn test_collection_kind_default() {
        assert!(matches!(
            CollectionKind::default(),
            CollectionKind::Standard
        ));
    }

    #[test]
    fn test_granularity_display() {
        assert_eq!(TimeSeriesGranularity::Seconds.to_string(), "seconds");
        assert_eq!(TimeSeriesGranularity::Minutes.to_string(), "minutes");
        assert_eq!(TimeSeriesGranularity::Hours.to_string(), "hours");
        let custom = TimeSeriesGranularity::custom(7200, 7200).unwrap();
        assert_eq!(custom.to_string(), "custom(7200s)");
    }
}
