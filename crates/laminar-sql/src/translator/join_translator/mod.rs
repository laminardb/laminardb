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
    /// Ordered left-side equality key columns.
    pub left_key_columns: Vec<String>,
    /// Ordered right-side equality key columns.
    pub right_key_columns: Vec<String>,
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
        write!(f, "{:?} TEMPORAL JOIN ON ", self.join_kind)?;
        for (index, (left, right)) in self
            .left_key_columns
            .iter()
            .zip(&self.right_key_columns)
            .enumerate()
        {
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
            " ({} -> {}, probes: {})",
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
            let (left_key_columns, right_key_columns) = ordered_key_columns(analysis);
            validate_temporal_key_columns(&left_key_columns, &right_key_columns)?;
            return Ok(JoinOperatorConfig::Temporal(TemporalJoinTranslatorConfig {
                left_table: analysis.left_table.clone(),
                right_table: analysis.right_table.clone(),
                left_key_columns,
                right_key_columns,
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
            let (left_keys, right_keys) = ordered_key_columns(analysis);
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
            JoinOperatorConfig::Temporal(config) => &config.left_key_columns,
        }
    }

    /// Get the ordered right-side equality key columns.
    #[must_use]
    pub fn right_keys(&self) -> &[String] {
        match self {
            JoinOperatorConfig::StreamStream(config) => &config.right_keys,
            JoinOperatorConfig::Lookup(config) => std::slice::from_ref(&config.lookup_key),
            JoinOperatorConfig::Temporal(config) => &config.right_key_columns,
        }
    }
}

fn ordered_key_columns(analysis: &JoinAnalysis) -> (Vec<String>, Vec<String>) {
    let mut left = Vec::with_capacity(1 + analysis.additional_key_columns.len());
    let mut right = Vec::with_capacity(1 + analysis.additional_key_columns.len());
    left.push(analysis.left_key_column.clone());
    right.push(analysis.right_key_column.clone());
    for (left_column, right_column) in &analysis.additional_key_columns {
        left.push(left_column.clone());
        right.push(right_column.clone());
    }
    (left, right)
}

fn validate_temporal_key_columns(left: &[String], right: &[String]) -> Result<(), String> {
    if left.is_empty()
        || left.len() != right.len()
        || left.iter().chain(right).any(String::is_empty)
    {
        return Err(
            "temporal join equality keys must be non-empty and have matching cardinality".into(),
        );
    }
    Ok(())
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
mod tests;
