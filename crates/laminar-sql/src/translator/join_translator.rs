//! Join operator configuration builder
//!
//! Translates parsed join analysis into operator configurations
//! for stream-stream joins and lookup joins.

use std::time::Duration;

use crate::parser::join_parser::{AsofSqlDirection, JoinAnalysis, JoinType, MultiJoinAnalysis};

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
    /// Left rows with at least one match (left columns only)
    LeftSemi,
    /// Left rows with no match (left columns only)
    LeftAnti,
    /// Right rows with at least one match (right columns only)
    RightSemi,
    /// Right rows with no match (right columns only)
    RightAnti,
}

/// Lookup join types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupJoinType {
    /// Stream event required, lookup optional
    Inner,
    /// Stream event always emitted, lookup optional
    Left,
}

/// Configuration for temporal join operator (FOR SYSTEM_TIME AS OF).
#[derive(Debug, Clone)]
pub struct TemporalJoinTranslatorConfig {
    /// Stream side key column
    pub stream_key_column: String,
    /// Table side key column
    pub table_key_column: String,
    /// Version column from FOR SYSTEM_TIME AS OF
    pub table_version_column: String,
    /// Temporal semantics: "event_time" or "process_time"
    pub semantics: String,
    /// Join type: "inner" or "left"
    pub join_type: String,
}

/// Configuration for ASOF join operator
#[derive(Debug, Clone)]
pub struct AsofJoinTranslatorConfig {
    /// Left side table name
    pub left_table: String,
    /// Right side table name
    pub right_table: String,
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
    /// Temporal join (FOR SYSTEM_TIME AS OF)
    Temporal(TemporalJoinTranslatorConfig),
}

impl std::fmt::Display for StreamJoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamJoinType::Inner => write!(f, "INNER"),
            StreamJoinType::Left => write!(f, "LEFT"),
            StreamJoinType::Right => write!(f, "RIGHT"),
            StreamJoinType::Full => write!(f, "FULL"),
            StreamJoinType::LeftSemi => write!(f, "LEFT SEMI"),
            StreamJoinType::LeftAnti => write!(f, "LEFT ANTI"),
            StreamJoinType::RightSemi => write!(f, "RIGHT SEMI"),
            StreamJoinType::RightAnti => write!(f, "RIGHT ANTI"),
        }
    }
}

impl std::fmt::Display for LookupJoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupJoinType::Inner => write!(f, "INNER"),
            LookupJoinType::Left => write!(f, "LEFT"),
        }
    }
}

impl std::fmt::Display for AsofSqlJoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsofSqlJoinType::Inner => write!(f, "INNER"),
            AsofSqlJoinType::Left => write!(f, "LEFT"),
        }
    }
}

impl std::fmt::Display for StreamJoinConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} JOIN ON left.{} = right.{} (bound: {}s)",
            self.join_type,
            self.left_key,
            self.right_key,
            self.time_bound.as_secs()
        )
    }
}

impl std::fmt::Display for LookupJoinConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} LOOKUP JOIN ON stream.{} = lookup.{} (cache_ttl: {}s)",
            self.join_type,
            self.stream_key,
            self.lookup_key,
            self.cache_ttl.as_secs()
        )
    }
}

impl std::fmt::Display for AsofJoinTranslatorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ASOF JOIN {}.{} = {}.{} ({}, {}.{} ~ {}.{}",
            self.join_type,
            self.left_table,
            self.key_column,
            self.right_table,
            self.key_column,
            self.direction,
            self.left_table,
            self.left_time_column,
            self.right_table,
            self.right_time_column,
        )?;
        if let Some(tol) = self.tolerance {
            write!(f, ", tolerance: {}s", tol.as_secs())?;
        }
        write!(f, ")")
    }
}

impl std::fmt::Display for TemporalJoinTranslatorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} TEMPORAL JOIN ON stream.{} = table.{} (version: {}, {})",
            self.join_type.to_uppercase(),
            self.stream_key_column,
            self.table_key_column,
            self.table_version_column,
            self.semantics,
        )
    }
}

impl std::fmt::Display for JoinOperatorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinOperatorConfig::StreamStream(c) => write!(f, "{c}"),
            JoinOperatorConfig::Lookup(c) => write!(f, "{c}"),
            JoinOperatorConfig::Asof(c) => write!(f, "{c}"),
            JoinOperatorConfig::Temporal(c) => write!(f, "{c}"),
        }
    }
}

impl JoinOperatorConfig {
    /// Create from join analysis.
    #[must_use]
    pub fn from_analysis(analysis: &JoinAnalysis) -> Self {
        if analysis.is_temporal_join {
            let join_type_str = match analysis.join_type {
                JoinType::Left => "left",
                _ => "inner",
            };
            return JoinOperatorConfig::Temporal(TemporalJoinTranslatorConfig {
                stream_key_column: analysis.left_key_column.clone(),
                table_key_column: analysis.right_key_column.clone(),
                table_version_column: analysis
                    .temporal_version_column
                    .clone()
                    .unwrap_or_default(),
                semantics: "event_time".to_string(),
                join_type: join_type_str.to_string(),
            });
        }

        if analysis.is_asof_join {
            return JoinOperatorConfig::Asof(AsofJoinTranslatorConfig {
                left_table: analysis.left_table.clone(),
                right_table: analysis.right_table.clone(),
                key_column: analysis.left_key_column.clone(),
                left_time_column: analysis.left_time_column.clone().unwrap_or_default(),
                right_time_column: analysis.right_time_column.clone().unwrap_or_default(),
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
                    JoinType::LeftSemi => StreamJoinType::LeftSemi,
                    JoinType::LeftAnti => StreamJoinType::LeftAnti,
                    JoinType::RightSemi => StreamJoinType::RightSemi,
                    JoinType::RightAnti => StreamJoinType::RightAnti,
                },
            })
        }
    }

    /// Create from a multi-join analysis, producing one config per join step.
    #[must_use]
    pub fn from_multi_analysis(multi: &MultiJoinAnalysis) -> Vec<Self> {
        multi.joins.iter().map(Self::from_analysis).collect()
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

    /// Check if this is a temporal join.
    #[must_use]
    pub fn is_temporal(&self) -> bool {
        matches!(self, JoinOperatorConfig::Temporal(_))
    }

    /// Get the left key column name.
    #[must_use]
    pub fn left_key(&self) -> &str {
        match self {
            JoinOperatorConfig::StreamStream(config) => &config.left_key,
            JoinOperatorConfig::Lookup(config) => &config.stream_key,
            JoinOperatorConfig::Asof(config) => &config.key_column,
            JoinOperatorConfig::Temporal(config) => &config.stream_key_column,
        }
    }

    /// Get the right key column name.
    #[must_use]
    pub fn right_key(&self) -> &str {
        match self {
            JoinOperatorConfig::StreamStream(config) => &config.right_key,
            JoinOperatorConfig::Lookup(config) => &config.lookup_key,
            JoinOperatorConfig::Asof(config) => &config.key_column,
            JoinOperatorConfig::Temporal(config) => &config.table_key_column,
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

    // -- Multi-way join translator tests --

    #[test]
    fn test_from_multi_analysis_single() {
        let analysis = JoinAnalysis::lookup(
            "a".to_string(),
            "b".to_string(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Inner,
        );
        let multi = MultiJoinAnalysis {
            joins: vec![analysis],
            tables: vec!["a".to_string(), "b".to_string()],
        };

        let configs = JoinOperatorConfig::from_multi_analysis(&multi);
        assert_eq!(configs.len(), 1);
        assert!(configs[0].is_lookup());
    }

    #[test]
    fn test_from_multi_analysis_two_lookups() {
        let j1 = JoinAnalysis::lookup(
            "a".to_string(),
            "b".to_string(),
            "id".to_string(),
            "a_id".to_string(),
            JoinType::Inner,
        );
        let j2 = JoinAnalysis::lookup(
            "b".to_string(),
            "c".to_string(),
            "id".to_string(),
            "b_id".to_string(),
            JoinType::Left,
        );
        let multi = MultiJoinAnalysis {
            joins: vec![j1, j2],
            tables: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };

        let configs = JoinOperatorConfig::from_multi_analysis(&multi);
        assert_eq!(configs.len(), 2);
        assert!(configs[0].is_lookup());
        assert!(configs[1].is_lookup());
        assert_eq!(configs[0].left_key(), "id");
        assert_eq!(configs[1].left_key(), "id");
    }

    #[test]
    fn test_from_multi_analysis_mixed_asof_lookup() {
        let j1 = JoinAnalysis::asof(
            "trades".to_string(),
            "quotes".to_string(),
            "symbol".to_string(),
            "symbol".to_string(),
            AsofSqlDirection::Backward,
            "ts".to_string(),
            "ts".to_string(),
            None,
        );
        let j2 = JoinAnalysis::lookup(
            "quotes".to_string(),
            "products".to_string(),
            "product_id".to_string(),
            "id".to_string(),
            JoinType::Inner,
        );
        let multi = MultiJoinAnalysis {
            joins: vec![j1, j2],
            tables: vec![
                "trades".to_string(),
                "quotes".to_string(),
                "products".to_string(),
            ],
        };

        let configs = JoinOperatorConfig::from_multi_analysis(&multi);
        assert_eq!(configs.len(), 2);
        assert!(configs[0].is_asof());
        assert!(configs[1].is_lookup());
    }

    #[test]
    fn test_from_multi_analysis_stream_stream_and_lookup() {
        let j1 = JoinAnalysis::stream_stream(
            "orders".to_string(),
            "payments".to_string(),
            "id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
        );
        let j2 = JoinAnalysis::lookup(
            "payments".to_string(),
            "customers".to_string(),
            "cust_id".to_string(),
            "id".to_string(),
            JoinType::Left,
        );
        let multi = MultiJoinAnalysis {
            joins: vec![j1, j2],
            tables: vec![
                "orders".to_string(),
                "payments".to_string(),
                "customers".to_string(),
            ],
        };

        let configs = JoinOperatorConfig::from_multi_analysis(&multi);
        assert_eq!(configs.len(), 2);
        assert!(configs[0].is_stream_stream());
        assert!(configs[1].is_lookup());
    }

    #[test]
    fn test_from_multi_analysis_order_preserved() {
        let j1 = JoinAnalysis::lookup(
            "a".to_string(),
            "b".to_string(),
            "k1".to_string(),
            "k1".to_string(),
            JoinType::Inner,
        );
        let j2 = JoinAnalysis::stream_stream(
            "b".to_string(),
            "c".to_string(),
            "k2".to_string(),
            "k2".to_string(),
            Duration::from_secs(60),
            JoinType::Left,
        );
        let j3 = JoinAnalysis::lookup(
            "c".to_string(),
            "d".to_string(),
            "k3".to_string(),
            "k3".to_string(),
            JoinType::Inner,
        );
        let multi = MultiJoinAnalysis {
            joins: vec![j1, j2, j3],
            tables: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ],
        };

        let configs = JoinOperatorConfig::from_multi_analysis(&multi);
        assert_eq!(configs.len(), 3);
        assert!(configs[0].is_lookup());
        assert!(configs[1].is_stream_stream());
        assert!(configs[2].is_lookup());
        assert_eq!(configs[0].left_key(), "k1");
        assert_eq!(configs[1].left_key(), "k2");
        assert_eq!(configs[2].left_key(), "k3");
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

    #[test]
    fn test_display_stream_join() {
        let config = StreamJoinConfig::inner(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
        );
        assert_eq!(
            format!("{config}"),
            "INNER JOIN ON left.order_id = right.order_id (bound: 3600s)"
        );
    }

    #[test]
    fn test_display_lookup_join() {
        let config = LookupJoinConfig::left("cust_id".to_string(), "id".to_string());
        assert_eq!(
            format!("{config}"),
            "LEFT LOOKUP JOIN ON stream.cust_id = lookup.id (cache_ttl: 300s)"
        );
    }

    #[test]
    fn test_display_asof_join() {
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
        let s = format!("{config}");
        assert!(s.contains("ASOF JOIN"), "got: {s}");
        assert!(s.contains("BACKWARD"), "got: {s}");
        assert!(s.contains("tolerance: 5s"), "got: {s}");
    }

    #[test]
    fn test_display_join_types() {
        assert_eq!(format!("{}", StreamJoinType::Inner), "INNER");
        assert_eq!(format!("{}", StreamJoinType::Left), "LEFT");
        assert_eq!(format!("{}", StreamJoinType::Right), "RIGHT");
        assert_eq!(format!("{}", StreamJoinType::Full), "FULL");
        assert_eq!(format!("{}", StreamJoinType::LeftSemi), "LEFT SEMI");
        assert_eq!(format!("{}", StreamJoinType::LeftAnti), "LEFT ANTI");
        assert_eq!(format!("{}", StreamJoinType::RightSemi), "RIGHT SEMI");
        assert_eq!(format!("{}", StreamJoinType::RightAnti), "RIGHT ANTI");
        assert_eq!(format!("{}", LookupJoinType::Inner), "INNER");
        assert_eq!(format!("{}", LookupJoinType::Left), "LEFT");
        assert_eq!(format!("{}", AsofSqlJoinType::Inner), "INNER");
        assert_eq!(format!("{}", AsofSqlJoinType::Left), "LEFT");
    }

    #[test]
    fn test_from_analysis_semi_anti() {
        let semi = JoinAnalysis::stream_stream(
            "a".to_string(),
            "b".to_string(),
            "id".to_string(),
            "id".to_string(),
            Duration::from_secs(60),
            JoinType::LeftSemi,
        );
        if let JoinOperatorConfig::StreamStream(config) =
            JoinOperatorConfig::from_analysis(&semi)
        {
            assert_eq!(config.join_type, StreamJoinType::LeftSemi);
        } else {
            panic!("Expected StreamStream config");
        }

        let anti = JoinAnalysis::stream_stream(
            "a".to_string(),
            "b".to_string(),
            "id".to_string(),
            "id".to_string(),
            Duration::from_secs(60),
            JoinType::RightAnti,
        );
        if let JoinOperatorConfig::StreamStream(config) =
            JoinOperatorConfig::from_analysis(&anti)
        {
            assert_eq!(config.join_type, StreamJoinType::RightAnti);
        } else {
            panic!("Expected StreamStream config");
        }
    }

    #[test]
    fn test_from_analysis_temporal() {
        let analysis = JoinAnalysis::temporal(
            "orders".to_string(),
            "products".to_string(),
            "product_id".to_string(),
            "id".to_string(),
            "order_time".to_string(),
            JoinType::Inner,
        );

        let config = JoinOperatorConfig::from_analysis(&analysis);
        assert!(config.is_temporal());
        assert!(!config.is_asof());
        assert!(!config.is_lookup());
        assert!(!config.is_stream_stream());
        assert_eq!(config.left_key(), "product_id");
        assert_eq!(config.right_key(), "id");

        if let JoinOperatorConfig::Temporal(tc) = config {
            assert_eq!(tc.table_version_column, "order_time");
            assert_eq!(tc.semantics, "event_time");
            assert_eq!(tc.join_type, "inner");
        } else {
            panic!("Expected Temporal config");
        }
    }

    #[test]
    fn test_temporal_left_join() {
        let analysis = JoinAnalysis::temporal(
            "orders".to_string(),
            "products".to_string(),
            "product_id".to_string(),
            "id".to_string(),
            "order_time".to_string(),
            JoinType::Left,
        );

        let config = JoinOperatorConfig::from_analysis(&analysis);
        if let JoinOperatorConfig::Temporal(tc) = config {
            assert_eq!(tc.join_type, "left");
        } else {
            panic!("Expected Temporal config");
        }
    }

    #[test]
    fn test_display_temporal_join() {
        let analysis = JoinAnalysis::temporal(
            "orders".to_string(),
            "products".to_string(),
            "product_id".to_string(),
            "id".to_string(),
            "order_time".to_string(),
            JoinType::Inner,
        );
        let config = JoinOperatorConfig::from_analysis(&analysis);
        let s = format!("{config}");
        assert!(s.contains("TEMPORAL JOIN"), "got: {s}");
        assert!(s.contains("order_time"), "got: {s}");
    }
}
