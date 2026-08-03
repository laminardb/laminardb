//! Join operator configuration builder
//!
//! Translates parsed join analysis into operator configurations
//! for stream-stream joins and lookup joins.

use std::time::Duration;

use crate::parser::join_parser::{JoinAnalysis, JoinType, MultiJoinAnalysis};
use crate::temporal::{TemporalJoinKind, TemporalProbeSchedule};

/// Configuration for stream-stream join operator
#[derive(Debug, Clone)]
pub struct StreamJoinConfig {
    /// SQL join kind.
    pub join_type: JoinType,
    /// Ordered left-side equality key columns.
    pub left_keys: Vec<String>,
    /// Ordered right-side equality key columns.
    pub right_keys: Vec<String>,
    /// Left side time column for interval matching
    pub left_time_column: String,
    /// Right side time column for interval matching
    pub right_time_column: String,
    /// Left side table name
    pub left_table: String,
    /// Right side table name
    pub right_table: String,
    /// Time bound for joining (max time difference between events)
    pub time_bound: Duration,
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
    /// Left input relation.
    pub left_table: String,
    /// Versioned right input relation.
    pub right_table: String,
    /// Left equality key.
    pub left_key_column: String,
    /// Right equality key.
    pub right_key_column: String,
    /// Left event-time column.
    pub left_time_column: String,
    /// Explicit right event-time/version column.
    pub right_time_column: String,
    /// INNER or LEFT output semantics.
    pub join_kind: TemporalJoinKind,
    /// One canonical target-time schedule.
    pub probe_schedule: TemporalProbeSchedule,
    /// Alias exposing multi-horizon `offset_ms` and `probe_time` columns.
    pub probe_alias: Option<String>,
}

/// Union type for join operator configurations
#[derive(Debug, Clone)]
pub enum JoinOperatorConfig {
    /// Stream-stream join
    StreamStream(StreamJoinConfig),
    /// Lookup join
    Lookup(LookupJoinConfig),
    /// Temporal join (FOR SYSTEM_TIME AS OF)
    Temporal(TemporalJoinTranslatorConfig),
}

impl std::fmt::Display for LookupJoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupJoinType::Inner => write!(f, "INNER"),
            LookupJoinType::Left => write!(f, "LEFT"),
        }
    }
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Left => write!(f, "LEFT"),
            JoinType::Right => write!(f, "RIGHT"),
            JoinType::Full => write!(f, "FULL"),
            JoinType::LeftSemi => write!(f, "LEFT SEMI"),
            JoinType::LeftAnti => write!(f, "LEFT ANTI"),
            JoinType::RightSemi => write!(f, "RIGHT SEMI"),
            JoinType::RightAnti => write!(f, "RIGHT ANTI"),
        }
    }
}

impl std::fmt::Display for StreamJoinConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} JOIN ON ", self.join_type)?;
        for (index, (left, right)) in self.left_keys.iter().zip(&self.right_keys).enumerate() {
            if index != 0 {
                write!(f, " AND ")?;
            }
            write!(
                f,
                "{}.{} = {}.{}",
                self.left_table, left, self.right_table, right
            )?;
        }
        write!(
            f,
            " (bound: {}s, time: {} ~ {})",
            self.time_bound.as_secs(),
            self.left_time_column,
            self.right_time_column,
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

impl std::fmt::Display for TemporalJoinTranslatorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} TEMPORAL JOIN ON {}.{} = {}.{} ({} -> {}, probes: {})",
            self.join_kind,
            self.left_table,
            self.left_key_column,
            self.right_table,
            self.right_key_column,
            self.left_time_column,
            self.right_time_column,
            self.probe_schedule.len(),
        )
    }
}

impl std::fmt::Display for JoinOperatorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinOperatorConfig::StreamStream(c) => write!(f, "{c}"),
            JoinOperatorConfig::Lookup(c) => write!(f, "{c}"),
            JoinOperatorConfig::Temporal(c) => write!(f, "{c}"),
        }
    }
}

impl JoinOperatorConfig {
    /// Create from join analysis.
    ///
    /// # Errors
    /// Returns an error when the analyzed join has no supported, complete runtime contract.
    pub fn from_analysis(analysis: &JoinAnalysis) -> Result<Self, String> {
        if analysis.is_temporal_join() {
            if !analysis.additional_key_columns.is_empty() {
                return Err(
                    "temporal joins support exactly one equality key; composite predicates are not implemented"
                        .to_string(),
                );
            }
            let join_kind = match analysis.join_type {
                JoinType::Inner => TemporalJoinKind::Inner,
                JoinType::Left => TemporalJoinKind::Left,
                unsupported => {
                    return Err(format!(
                        "temporal joins support only INNER or LEFT joins; {unsupported:?} is unsupported"
                    ));
                }
            };
            let left_time_column = analysis.left_time_column.clone().ok_or_else(|| {
                "temporal joins require an explicit left event-time column".to_string()
            })?;
            let right_time_column = analysis.right_time_column.clone().ok_or_else(|| {
                "temporal joins require an explicit right event-time column from the source contract"
                    .to_string()
            })?;
            let probe_schedule = analysis
                .temporal_probe_schedule
                .clone()
                .ok_or_else(|| "temporal join probe schedule is missing".to_string())?;
            if probe_schedule.is_multi_horizon() && analysis.temporal_probe_alias.is_none() {
                return Err("multi-horizon temporal probes require an output alias".into());
            }
            return Ok(JoinOperatorConfig::Temporal(TemporalJoinTranslatorConfig {
                left_table: analysis.left_table.clone(),
                right_table: analysis.right_table.clone(),
                left_key_column: analysis.left_key_column.clone(),
                right_key_column: analysis.right_key_column.clone(),
                left_time_column,
                right_time_column,
                join_kind,
                probe_schedule,
                probe_alias: analysis.temporal_probe_alias.clone(),
            }));
        }

        if analysis.is_lookup_join {
            if !analysis.additional_key_columns.is_empty() {
                return Err(
                    "lookup joins support exactly one equality key; composite predicates are not implemented"
                        .to_string(),
                );
            }
            let join_type = match analysis.join_type {
                JoinType::Inner => LookupJoinType::Inner,
                JoinType::Left => LookupJoinType::Left,
                unsupported => {
                    return Err(format!(
                        "lookup joins support only INNER or LEFT joins; {unsupported:?} is unsupported"
                    ));
                }
            };
            Ok(JoinOperatorConfig::Lookup(LookupJoinConfig {
                stream_key: analysis.left_key_column.clone(),
                lookup_key: analysis.right_key_column.clone(),
                join_type,
                cache_ttl: Duration::from_secs(300), // Default 5 min
            }))
        } else {
            let time_bound = analysis.time_bound.ok_or_else(|| {
                "stream-stream joins require an explicit finite time bound".to_string()
            })?;
            if time_bound.is_zero() {
                return Err("stream-stream joins require a positive finite time bound".to_string());
            }
            let mut left_keys = Vec::with_capacity(1 + analysis.additional_key_columns.len());
            let mut right_keys = Vec::with_capacity(1 + analysis.additional_key_columns.len());
            left_keys.push(analysis.left_key_column.clone());
            right_keys.push(analysis.right_key_column.clone());
            for (left, right) in &analysis.additional_key_columns {
                left_keys.push(left.clone());
                right_keys.push(right.clone());
            }
            Ok(JoinOperatorConfig::StreamStream(StreamJoinConfig {
                join_type: analysis.join_type,
                left_keys,
                right_keys,
                left_time_column: analysis.left_time_column.clone().unwrap_or_default(),
                right_time_column: analysis.right_time_column.clone().unwrap_or_default(),
                left_table: analysis.left_table.clone(),
                right_table: analysis.right_table.clone(),
                time_bound,
            }))
        }
    }

    /// Create from a multi-join analysis, producing one config per join step.
    ///
    /// # Errors
    /// Returns an error when any join step has no supported, complete runtime contract.
    pub fn from_multi_analysis(multi: &MultiJoinAnalysis) -> Result<Vec<Self>, String> {
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

    /// Check if this is a temporal join.
    #[must_use]
    pub fn is_temporal(&self) -> bool {
        matches!(self, JoinOperatorConfig::Temporal(_))
    }

    /// Get the ordered left-side equality key columns.
    #[must_use]
    pub fn left_keys(&self) -> &[String] {
        match self {
            JoinOperatorConfig::StreamStream(config) => &config.left_keys,
            JoinOperatorConfig::Lookup(config) => std::slice::from_ref(&config.stream_key),
            JoinOperatorConfig::Temporal(config) => std::slice::from_ref(&config.left_key_column),
        }
    }

    /// Get the ordered right-side equality key columns.
    #[must_use]
    pub fn right_keys(&self) -> &[String] {
        match self {
            JoinOperatorConfig::StreamStream(config) => &config.right_keys,
            JoinOperatorConfig::Lookup(config) => std::slice::from_ref(&config.lookup_key),
            JoinOperatorConfig::Temporal(config) => std::slice::from_ref(&config.right_key_column),
        }
    }
}

impl StreamJoinConfig {
    /// Create a new stream-stream join configuration.
    #[must_use]
    pub fn new(
        join_type: JoinType,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        time_bound: Duration,
    ) -> Self {
        Self {
            join_type,
            left_keys,
            right_keys,
            left_time_column: String::new(),
            right_time_column: String::new(),
            left_table: String::new(),
            right_table: String::new(),
            time_bound,
        }
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
        let config = StreamJoinConfig::new(
            JoinType::RightAnti,
            vec!["tenant_id".to_string(), "order_id".to_string()],
            vec!["account_id".to_string(), "payment_order_id".to_string()],
            Duration::from_secs(3600),
        );

        assert_eq!(config.join_type, JoinType::RightAnti);
        assert_eq!(config.left_keys, ["tenant_id", "order_id"]);
        assert_eq!(config.right_keys, ["account_id", "payment_order_id"]);
        assert_eq!(config.time_bound, Duration::from_secs(3600));
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

        let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();

        assert!(config.is_lookup());
        assert!(!config.is_stream_stream());
        assert_eq!(config.left_keys(), ["customer_id"]);
        assert_eq!(config.right_keys(), ["id"]);
    }

    #[test]
    fn test_from_analysis_stream_stream() {
        let mut analysis = JoinAnalysis::stream_stream(
            "orders".to_string(),
            "payments".to_string(),
            "tenant_id".to_string(),
            "account_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Full,
        );
        analysis
            .additional_key_columns
            .push(("order_id".to_string(), "payment_order_id".to_string()));

        let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();

        assert!(config.is_stream_stream());
        assert!(!config.is_lookup());

        if let JoinOperatorConfig::StreamStream(stream_config) = config {
            assert_eq!(stream_config.join_type, JoinType::Full);
            assert_eq!(stream_config.left_keys, ["tenant_id", "order_id"]);
            assert_eq!(stream_config.right_keys, ["account_id", "payment_order_id"]);
            assert_eq!(stream_config.time_bound, Duration::from_secs(3600));
        }
    }
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

        let configs = JoinOperatorConfig::from_multi_analysis(&multi).unwrap();
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
            JoinType::Inner,
        );
        let multi = MultiJoinAnalysis {
            joins: vec![j1, j2],
            tables: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };

        let configs = JoinOperatorConfig::from_multi_analysis(&multi).unwrap();
        assert_eq!(configs.len(), 2);
        assert!(configs[0].is_lookup());
        assert!(configs[1].is_lookup());
        assert_eq!(configs[0].left_keys(), ["id"]);
        assert_eq!(configs[1].left_keys(), ["id"]);
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

        let configs = JoinOperatorConfig::from_multi_analysis(&multi).unwrap();
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
            JoinType::Inner,
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

        let configs = JoinOperatorConfig::from_multi_analysis(&multi).unwrap();
        assert_eq!(configs.len(), 3);
        assert!(configs[0].is_lookup());
        assert!(configs[1].is_stream_stream());
        assert!(configs[2].is_lookup());
        assert_eq!(configs[0].left_keys(), ["k1"]);
        assert_eq!(configs[1].left_keys(), ["k2"]);
        assert_eq!(configs[2].left_keys(), ["k3"]);
    }

    #[test]
    fn test_display_stream_join() {
        let mut config = StreamJoinConfig::new(
            JoinType::LeftSemi,
            vec!["tenant_id".to_string(), "order_id".to_string()],
            vec!["account_id".to_string(), "payment_order_id".to_string()],
            Duration::from_secs(3600),
        );
        config.left_table = "orders".to_string();
        config.right_table = "payments".to_string();
        config.left_time_column = "ts".to_string();
        config.right_time_column = "ts".to_string();
        assert_eq!(
            format!("{config}"),
            "LEFT SEMI JOIN ON orders.tenant_id = payments.account_id AND orders.order_id = payments.payment_order_id (bound: 3600s, time: ts ~ ts)"
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
    fn test_display_join_types() {
        assert_eq!(format!("{}", LookupJoinType::Inner), "INNER");
        assert_eq!(format!("{}", LookupJoinType::Left), "LEFT");
        for (join_type, sql) in [
            (JoinType::Inner, "INNER"),
            (JoinType::Left, "LEFT"),
            (JoinType::Right, "RIGHT"),
            (JoinType::Full, "FULL"),
            (JoinType::LeftSemi, "LEFT SEMI"),
            (JoinType::LeftAnti, "LEFT ANTI"),
            (JoinType::RightSemi, "RIGHT SEMI"),
            (JoinType::RightAnti, "RIGHT ANTI"),
        ] {
            assert_eq!(join_type.to_string(), sql);
        }
    }

    #[test]
    fn test_from_analysis_temporal() {
        let mut analysis = JoinAnalysis::temporal(
            "orders".to_string(),
            "products".to_string(),
            "product_id".to_string(),
            "id".to_string(),
            "order_time".to_string(),
            JoinType::Inner,
        );
        analysis.right_time_column = Some("version_time".into());

        let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();
        assert!(config.is_temporal());
        assert!(!config.is_lookup());
        assert!(!config.is_stream_stream());
        assert_eq!(config.left_keys(), ["product_id"]);
        assert_eq!(config.right_keys(), ["id"]);

        if let JoinOperatorConfig::Temporal(tc) = config {
            assert_eq!(tc.left_time_column, "order_time");
            assert_eq!(tc.right_time_column, "version_time");
            assert_eq!(tc.join_kind, TemporalJoinKind::Inner);
            assert_eq!(tc.probe_schedule.offsets_ms(), [0]);
        } else {
            panic!("Expected Temporal config");
        }
    }

    #[test]
    fn test_temporal_left_join() {
        let mut analysis = JoinAnalysis::temporal(
            "orders".to_string(),
            "products".to_string(),
            "product_id".to_string(),
            "id".to_string(),
            "order_time".to_string(),
            JoinType::Left,
        );
        analysis.right_time_column = Some("version_time".into());

        let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();
        if let JoinOperatorConfig::Temporal(tc) = config {
            assert_eq!(tc.join_kind, TemporalJoinKind::Left);
        } else {
            panic!("Expected Temporal config");
        }
    }

    #[test]
    fn test_display_temporal_join() {
        let mut analysis = JoinAnalysis::temporal(
            "orders".to_string(),
            "products".to_string(),
            "product_id".to_string(),
            "id".to_string(),
            "order_time".to_string(),
            JoinType::Inner,
        );
        analysis.right_time_column = Some("version_time".into());
        let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();
        let s = format!("{config}");
        assert!(s.contains("TEMPORAL JOIN"), "got: {s}");
        assert!(s.contains("order_time"), "got: {s}");
    }

    #[test]
    fn stream_join_requires_explicit_time_bound() {
        let mut analysis = JoinAnalysis::stream_stream(
            "orders".to_string(),
            "payments".to_string(),
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(1),
            JoinType::Inner,
        );
        analysis.time_bound = None;

        let error = JoinOperatorConfig::from_analysis(&analysis).unwrap_err();
        assert!(error.contains("explicit finite time bound"));

        analysis.time_bound = Some(Duration::ZERO);
        let error = JoinOperatorConfig::from_analysis(&analysis).unwrap_err();
        assert!(error.contains("positive finite time bound"));
    }

    #[test]
    fn unsupported_join_analysis_fails_closed() {
        let unsupported_lookup_types = [
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];
        for join_type in unsupported_lookup_types {
            let lookup = JoinAnalysis::lookup(
                "orders".to_string(),
                "customers".to_string(),
                "customer_id".to_string(),
                "id".to_string(),
                join_type,
            );
            assert!(JoinOperatorConfig::from_analysis(&lookup)
                .unwrap_err()
                .contains("only INNER or LEFT"));

            let mut temporal = JoinAnalysis::temporal(
                "orders".to_string(),
                "customers".to_string(),
                "customer_id".to_string(),
                "id".to_string(),
                "order_time".to_string(),
                join_type,
            );
            temporal.right_time_column = Some("version_time".into());
            assert!(JoinOperatorConfig::from_analysis(&temporal)
                .unwrap_err()
                .contains("only INNER or LEFT"));
        }

        let mut composite = JoinAnalysis::lookup(
            "orders".to_string(),
            "customers".to_string(),
            "customer_id".to_string(),
            "id".to_string(),
            JoinType::Inner,
        );
        composite
            .additional_key_columns
            .push(("tenant_id".to_string(), "tenant_id".to_string()));
        assert!(JoinOperatorConfig::from_analysis(&composite)
            .unwrap_err()
            .contains("exactly one equality key"));
    }
}
