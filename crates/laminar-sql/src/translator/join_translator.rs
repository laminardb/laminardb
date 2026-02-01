//! Join operator configuration builder
//!
//! Translates parsed join analysis into operator configurations
//! for stream-stream joins and lookup joins.

use std::time::Duration;

use crate::parser::join_parser::{AsofSqlDirection, JoinAnalysis, JoinType};

/// Configuration for stream-stream join operator
#[derive(Debug, Clone)]
pub struct StreamJoinConfig {
    /// Left side key column
    pub left_key: String,
    /// Right side key column
    pub right_key: String,
    /// Time bound for joining (max time difference between events)
    pub time_bound: Duration,
    /// Join type
    pub join_type: StreamJoinType,
}

/// Configuration for lookup join operator
#[derive(Debug, Clone)]
pub struct LookupJoinConfig {
    /// Stream side key column
    pub stream_key: String,
    /// Lookup table key column
    pub lookup_key: String,
    /// Join type
    pub join_type: LookupJoinType,
    /// Cache TTL for lookup results
    pub cache_ttl: Duration,
}

/// Stream-stream join types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamJoinType {
    /// Both sides required
    Inner,
    /// Left side always emitted
    Left,
    /// Right side always emitted
    Right,
    /// Both sides always emitted
    Full,
}

/// Lookup join types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupJoinType {
    /// Stream event required, lookup optional
    Inner,
    /// Stream event always emitted, lookup optional
    Left,
}

/// Configuration for ASOF join operator
#[derive(Debug, Clone)]
pub struct AsofJoinTranslatorConfig {
    /// Key column for partitioning (e.g., symbol)
    pub key_column: String,
    /// Left side time column
    pub left_time_column: String,
    /// Right side time column
    pub right_time_column: String,
    /// Join direction (Backward or Forward)
    pub direction: AsofSqlDirection,
    /// Maximum allowed time difference
    pub tolerance: Option<Duration>,
    /// ASOF join type (Inner or Left)
    pub join_type: AsofSqlJoinType,
}

/// ASOF join type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsofSqlJoinType {
    /// Both sides required
    Inner,
    /// Left side always emitted
    Left,
}

/// Union type for join operator configurations
#[derive(Debug, Clone)]
pub enum JoinOperatorConfig {
    /// Stream-stream join
    StreamStream(StreamJoinConfig),
    /// Lookup join
    Lookup(LookupJoinConfig),
    /// ASOF join
    Asof(AsofJoinTranslatorConfig),
}

impl JoinOperatorConfig {
    /// Create from join analysis.
    #[must_use]
    pub fn from_analysis(analysis: &JoinAnalysis) -> Self {
        if analysis.is_asof_join {
            return JoinOperatorConfig::Asof(AsofJoinTranslatorConfig {
                key_column: analysis.left_key_column.clone(),
                left_time_column: analysis
                    .left_time_column
                    .clone()
                    .unwrap_or_default(),
                right_time_column: analysis
                    .right_time_column
                    .clone()
                    .unwrap_or_default(),
                direction: analysis
                    .asof_direction
                    .unwrap_or(AsofSqlDirection::Backward),
                tolerance: analysis.asof_tolerance,
                join_type: AsofSqlJoinType::Left, // ASOF is always left-style
            });
        }

        if analysis.is_lookup_join {
            JoinOperatorConfig::Lookup(LookupJoinConfig {
                stream_key: analysis.left_key_column.clone(),
                lookup_key: analysis.right_key_column.clone(),
                join_type: match analysis.join_type {
                    JoinType::Inner => LookupJoinType::Inner,
                    _ => LookupJoinType::Left,
                },
                cache_ttl: Duration::from_secs(300), // Default 5 min
            })
        } else {
            JoinOperatorConfig::StreamStream(StreamJoinConfig {
                left_key: analysis.left_key_column.clone(),
                right_key: analysis.right_key_column.clone(),
                time_bound: analysis.time_bound.unwrap_or(Duration::from_secs(3600)),
                join_type: match analysis.join_type {
                    JoinType::Inner => StreamJoinType::Inner,
                    JoinType::Left | JoinType::AsOf => StreamJoinType::Left,
                    JoinType::Right => StreamJoinType::Right,
                    JoinType::Full => StreamJoinType::Full,
                },
            })
        }
    }

    /// Check if this is a stream-stream join.
    #[must_use]
    pub fn is_stream_stream(&self) -> bool {
        matches!(self, JoinOperatorConfig::StreamStream(_))
    }

    /// Check if this is a lookup join.
    #[must_use]
    pub fn is_lookup(&self) -> bool {
        matches!(self, JoinOperatorConfig::Lookup(_))
    }

    /// Check if this is an ASOF join.
    #[must_use]
    pub fn is_asof(&self) -> bool {
        matches!(self, JoinOperatorConfig::Asof(_))
    }

    /// Get the left key column name.
    #[must_use]
    pub fn left_key(&self) -> &str {
        match self {
            JoinOperatorConfig::StreamStream(config) => &config.left_key,
            JoinOperatorConfig::Lookup(config) => &config.stream_key,
            JoinOperatorConfig::Asof(config) => &config.key_column,
        }
    }

    /// Get the right key column name.
    #[must_use]
    pub fn right_key(&self) -> &str {
        match self {
            JoinOperatorConfig::StreamStream(config) => &config.right_key,
            JoinOperatorConfig::Lookup(config) => &config.lookup_key,
            JoinOperatorConfig::Asof(config) => &config.key_column,
        }
    }
}

impl StreamJoinConfig {
    /// Create a new stream-stream join configuration.
    #[must_use]
    pub fn new(
        left_key: String,
        right_key: String,
        time_bound: Duration,
        join_type: StreamJoinType,
    ) -> Self {
        Self {
            left_key,
            right_key,
            time_bound,
            join_type,
        }
    }

    /// Create an inner join configuration.
    #[must_use]
    pub fn inner(left_key: String, right_key: String, time_bound: Duration) -> Self {
        Self::new(left_key, right_key, time_bound, StreamJoinType::Inner)
    }

    /// Create a left join configuration.
    #[must_use]
    pub fn left(left_key: String, right_key: String, time_bound: Duration) -> Self {
        Self::new(left_key, right_key, time_bound, StreamJoinType::Left)
    }
}

impl LookupJoinConfig {
    /// Create a new lookup join configuration.
    #[must_use]
    pub fn new(
        stream_key: String,
        lookup_key: String,
        join_type: LookupJoinType,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            stream_key,
            lookup_key,
            join_type,
            cache_ttl,
        }
    }

    /// Create an inner lookup join configuration.
    #[must_use]
    pub fn inner(stream_key: String, lookup_key: String) -> Self {
        Self::new(
            stream_key,
            lookup_key,
            LookupJoinType::Inner,
            Duration::from_secs(300),
        )
    }

    /// Create a left lookup join configuration.
    #[must_use]
    pub fn left(stream_key: String, lookup_key: String) -> Self {
        Self::new(
            stream_key,
            lookup_key,
            LookupJoinType::Left,
            Duration::from_secs(300),
        )
    }

    /// Set the cache TTL.
    #[must_use]
    pub fn with_cache_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl = ttl;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_join_config() {
        let config = StreamJoinConfig::inner(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
        );

        assert_eq!(config.left_key, "order_id");
        assert_eq!(config.right_key, "order_id");
        assert_eq!(config.time_bound, Duration::from_secs(3600));
        assert_eq!(config.join_type, StreamJoinType::Inner);
    }

    #[test]
    fn test_lookup_join_config() {
        let config = LookupJoinConfig::inner("customer_id".to_string(), "id".to_string())
            .with_cache_ttl(Duration::from_secs(600));

        assert_eq!(config.stream_key, "customer_id");
        assert_eq!(config.lookup_key, "id");
        assert_eq!(config.cache_ttl, Duration::from_secs(600));
        assert_eq!(config.join_type, LookupJoinType::Inner);
    }

    #[test]
    fn test_from_analysis_lookup() {
        let analysis = JoinAnalysis::lookup(
            "orders".to_string(),
            "customers".to_string(),
            "customer_id".to_string(),
            "id".to_string(),
            JoinType::Inner,
        );

        let config = JoinOperatorConfig::from_analysis(&analysis);

        assert!(config.is_lookup());
        assert!(!config.is_stream_stream());
        assert_eq!(config.left_key(), "customer_id");
        assert_eq!(config.right_key(), "id");
    }

    #[test]
    fn test_from_analysis_stream_stream() {
        let analysis = JoinAnalysis::stream_stream(
            "orders".to_string(),
            "payments".to_string(),
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
        );

        let config = JoinOperatorConfig::from_analysis(&analysis);

        assert!(config.is_stream_stream());
        assert!(!config.is_lookup());

        if let JoinOperatorConfig::StreamStream(stream_config) = config {
            assert_eq!(stream_config.time_bound, Duration::from_secs(3600));
            assert_eq!(stream_config.join_type, StreamJoinType::Inner);
        }
    }

    #[test]
    fn test_from_analysis_asof() {
        let analysis = JoinAnalysis::asof(
            "trades".to_string(),
            "quotes".to_string(),
            "symbol".to_string(),
            "symbol".to_string(),
            AsofSqlDirection::Backward,
            "ts".to_string(),
            "ts".to_string(),
            Some(Duration::from_secs(5)),
        );

        let config = JoinOperatorConfig::from_analysis(&analysis);
        assert!(config.is_asof());
        assert!(!config.is_stream_stream());
        assert!(!config.is_lookup());
    }

    #[test]
    fn test_asof_config_fields() {
        let analysis = JoinAnalysis::asof(
            "trades".to_string(),
            "quotes".to_string(),
            "symbol".to_string(),
            "symbol".to_string(),
            AsofSqlDirection::Forward,
            "trade_ts".to_string(),
            "quote_ts".to_string(),
            Some(Duration::from_millis(5000)),
        );

        let config = JoinOperatorConfig::from_analysis(&analysis);
        if let JoinOperatorConfig::Asof(asof) = config {
            assert_eq!(asof.direction, AsofSqlDirection::Forward);
            assert_eq!(asof.left_time_column, "trade_ts");
            assert_eq!(asof.right_time_column, "quote_ts");
            assert_eq!(asof.tolerance, Some(Duration::from_millis(5000)));
            assert_eq!(asof.key_column, "symbol");
            assert_eq!(asof.join_type, AsofSqlJoinType::Left);
        } else {
            panic!("Expected Asof config");
        }
    }

    #[test]
    fn test_asof_is_asof() {
        let analysis = JoinAnalysis::asof(
            "a".to_string(),
            "b".to_string(),
            "id".to_string(),
            "id".to_string(),
            AsofSqlDirection::Backward,
            "ts".to_string(),
            "ts".to_string(),
            None,
        );

        let config = JoinOperatorConfig::from_analysis(&analysis);
        assert!(config.is_asof());
    }

    #[test]
    fn test_asof_key_accessors() {
        let analysis = JoinAnalysis::asof(
            "trades".to_string(),
            "quotes".to_string(),
            "sym".to_string(),
            "sym".to_string(),
            AsofSqlDirection::Backward,
            "ts".to_string(),
            "ts".to_string(),
            None,
        );

        let config = JoinOperatorConfig::from_analysis(&analysis);
        assert_eq!(config.left_key(), "sym");
        assert_eq!(config.right_key(), "sym");
    }

    #[test]
    fn test_join_types() {
        // Test all join types map correctly
        let left_analysis = JoinAnalysis::stream_stream(
            "a".to_string(),
            "b".to_string(),
            "id".to_string(),
            "id".to_string(),
            Duration::from_secs(60),
            JoinType::Left,
        );

        if let JoinOperatorConfig::StreamStream(config) =
            JoinOperatorConfig::from_analysis(&left_analysis)
        {
            assert_eq!(config.join_type, StreamJoinType::Left);
        }

        let right_analysis = JoinAnalysis::stream_stream(
            "a".to_string(),
            "b".to_string(),
            "id".to_string(),
            "id".to_string(),
            Duration::from_secs(60),
            JoinType::Right,
        );

        if let JoinOperatorConfig::StreamStream(config) =
            JoinOperatorConfig::from_analysis(&right_analysis)
        {
            assert_eq!(config.join_type, StreamJoinType::Right);
        }

        let full_analysis = JoinAnalysis::stream_stream(
            "a".to_string(),
            "b".to_string(),
            "id".to_string(),
            "id".to_string(),
            Duration::from_secs(60),
            JoinType::Full,
        );

        if let JoinOperatorConfig::StreamStream(config) =
            JoinOperatorConfig::from_analysis(&full_analysis)
        {
            assert_eq!(config.join_type, StreamJoinType::Full);
        }
    }
}
