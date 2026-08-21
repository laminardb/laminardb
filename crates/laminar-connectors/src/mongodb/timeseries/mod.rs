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
//! - `MongoDB` does not support `watch()` (change streams) on time series
//!   collections — a source targeting one named collection rejects these.

use crate::error::ConnectorError;

const MAX_CUSTOM_BUCKET_SPAN_SECONDS: u32 = 31_536_000;

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

impl TimeSeriesConfig {
    /// Validate server-enforced time-series collection invariants before I/O.
    pub(crate) fn validate(&self) -> Result<(), ConnectorError> {
        validate_field_name(&self.time_field, "time_field")?;
        if let Some(meta_field) = self.meta_field.as_deref() {
            validate_field_name(meta_field, "meta_field")?;
            if meta_field == self.time_field {
                return Err(ConnectorError::ConfigurationError(
                    "time series meta_field must differ from time_field".into(),
                ));
            }
            if meta_field == "_id" {
                return Err(ConnectorError::ConfigurationError(
                    "time series meta_field must not be '_id'".into(),
                ));
            }
        }
        if self
            .expire_after_seconds
            .is_some_and(|ttl| ttl > u64::try_from(i64::MAX).expect("i64::MAX fits u64"))
        {
            return Err(ConnectorError::ConfigurationError(
                "time series expire_after_seconds exceeds MongoDB's signed 64-bit range".into(),
            ));
        }
        if let TimeSeriesGranularity::Custom {
            bucket_max_span_seconds,
            bucket_rounding_seconds,
        } = self.granularity
        {
            validate_custom_bucket(bucket_max_span_seconds, bucket_rounding_seconds)?;
        }
        Ok(())
    }
}

fn validate_field_name(field: &str, label: &str) -> Result<(), ConnectorError> {
    if field.trim().is_empty() {
        return Err(ConnectorError::ConfigurationError(format!(
            "time series {label} must not be empty"
        )));
    }
    if field.contains('\0') {
        return Err(ConnectorError::ConfigurationError(format!(
            "time series {label} must not contain NUL"
        )));
    }
    Ok(())
}

fn validate_custom_bucket(
    bucket_max_span_seconds: u32,
    bucket_rounding_seconds: u32,
) -> Result<(), ConnectorError> {
    if bucket_max_span_seconds != bucket_rounding_seconds {
        return Err(ConnectorError::ConfigurationError(format!(
            "time series custom granularity requires bucket_max_span_seconds ({bucket_max_span_seconds}) \
             == bucket_rounding_seconds ({bucket_rounding_seconds})"
        )));
    }
    if !(1..=MAX_CUSTOM_BUCKET_SPAN_SECONDS).contains(&bucket_max_span_seconds) {
        return Err(ConnectorError::ConfigurationError(format!(
            "time series custom granularity bucket_max_span_seconds must be between 1 and \
             {MAX_CUSTOM_BUCKET_SPAN_SECONDS}"
        )));
    }
    Ok(())
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
        validate_custom_bucket(bucket_max_span_seconds, bucket_rounding_seconds)?;
        Ok(Self::Custom {
            bucket_max_span_seconds,
            bucket_rounding_seconds,
        })
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
mod tests;
