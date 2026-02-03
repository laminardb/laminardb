//! Channel type derivation from query plan analysis.
//!
//! This module analyzes query plans to automatically determine whether sources
//! need SPSC (single consumer) or broadcast (multiple consumer) channels.
//!
//! # Key Principle
//!
//! **Users never specify broadcast mode.** The planner analyzes MVs and sources
//! to derive the correct channel type automatically:
//!
//! - If a source is consumed by exactly 1 MV → SPSC (optimal)
//! - If a source is consumed by 2+ MVs → Broadcast
//!
//! # Example
//!
//! ```sql
//! CREATE SOURCE trades (...);
//!
//! CREATE MATERIALIZED VIEW vwap AS
//!   SELECT symbol, SUM(price * volume) / SUM(volume) AS vwap
//!   FROM trades
//!   GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE);
//!
//! CREATE MATERIALIZED VIEW max_price AS
//!   SELECT symbol, MAX(price)
//!   FROM trades
//!   GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE);
//! ```
//!
//! In this example, `trades` source has 2 consumers (`vwap` and `max_price`),
//! so the planner derives `Broadcast { consumer_count: 2 }` for `trades`.

use std::collections::HashMap;

/// Channel type derived from query analysis.
///
/// This enum represents the automatically-derived channel configuration
/// for a source based on how many downstream consumers it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedChannelType {
    /// Single consumer - use SPSC channel.
    ///
    /// Optimal for sources with exactly one downstream MV.
    /// No cloning overhead, lock-free single producer/consumer.
    Spsc,

    /// Multiple consumers - use Broadcast channel.
    ///
    /// Used when a source feeds multiple downstream MVs.
    /// Values are cloned to each consumer.
    Broadcast {
        /// Number of downstream consumers.
        consumer_count: usize,
    },
}

impl DerivedChannelType {
    /// Returns true if this is a broadcast channel.
    #[must_use]
    pub fn is_broadcast(&self) -> bool {
        matches!(self, DerivedChannelType::Broadcast { .. })
    }

    /// Returns the consumer count.
    #[must_use]
    pub fn consumer_count(&self) -> usize {
        match self {
            DerivedChannelType::Spsc => 1,
            DerivedChannelType::Broadcast { consumer_count } => *consumer_count,
        }
    }
}

/// Source definition for channel derivation.
///
/// Represents a registered streaming source that can be consumed by MVs.
#[derive(Debug, Clone)]
pub struct SourceDefinition {
    /// Source name (e.g., "trades", "orders").
    pub name: String,
    /// Optional watermark column for event time processing.
    pub watermark_column: Option<String>,
}

impl SourceDefinition {
    /// Creates a new source definition.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            watermark_column: None,
        }
    }

    /// Creates a source definition with a watermark column.
    #[must_use]
    pub fn with_watermark(name: impl Into<String>, watermark_column: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            watermark_column: Some(watermark_column.into()),
        }
    }
}

/// Materialized view definition for channel derivation.
///
/// Represents a continuous query that consumes from one or more sources.
#[derive(Debug, Clone)]
pub struct MvDefinition {
    /// MV name (e.g., "vwap", "max_price").
    pub name: String,
    /// Sources this MV reads from.
    pub source_refs: Vec<String>,
}

impl MvDefinition {
    /// Creates a new MV definition.
    #[must_use]
    pub fn new(name: impl Into<String>, source_refs: Vec<String>) -> Self {
        Self {
            name: name.into(),
            source_refs,
        }
    }

    /// Creates an MV definition that reads from a single source.
    #[must_use]
    pub fn from_source(name: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source_refs: vec![source.into()],
        }
    }
}

/// Analyzes query plan to determine channel types per source.
///
/// Examines how many MVs consume each source and determines whether
/// SPSC or Broadcast channels are needed.
///
/// # Arguments
///
/// * `sources` - Registered source definitions
/// * `mvs` - Materialized view definitions
///
/// # Returns
///
/// A map from source name to derived channel type.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_sql::planner::channel_derivation::*;
///
/// let sources = vec![
///     SourceDefinition::new("trades"),
///     SourceDefinition::new("orders"),
/// ];
///
/// let mvs = vec![
///     MvDefinition::from_source("vwap", "trades"),
///     MvDefinition::from_source("max_price", "trades"),
///     MvDefinition::from_source("order_count", "orders"),
/// ];
///
/// let channel_types = derive_channel_types(&sources, &mvs);
///
/// // trades has 2 consumers → Broadcast
/// assert_eq!(
///     channel_types.get("trades"),
///     Some(&DerivedChannelType::Broadcast { consumer_count: 2 })
/// );
///
/// // orders has 1 consumer → SPSC
/// assert_eq!(
///     channel_types.get("orders"),
///     Some(&DerivedChannelType::Spsc)
/// );
/// ```
#[must_use]
pub fn derive_channel_types(
    sources: &[SourceDefinition],
    mvs: &[MvDefinition],
) -> HashMap<String, DerivedChannelType> {
    let consumer_counts = count_consumers_per_source(mvs);

    sources
        .iter()
        .map(|source| {
            let count = consumer_counts.get(&source.name).copied().unwrap_or(0);
            let channel_type = if count <= 1 {
                DerivedChannelType::Spsc
            } else {
                DerivedChannelType::Broadcast {
                    consumer_count: count,
                }
            };
            (source.name.clone(), channel_type)
        })
        .collect()
}

/// Counts how many MVs read from each source.
fn count_consumers_per_source(mvs: &[MvDefinition]) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();

    for mv in mvs {
        for source_ref in &mv.source_refs {
            *counts.entry(source_ref.clone()).or_insert(0) += 1;
        }
    }

    counts
}

/// Analyzes a single MV to extract its source references.
///
/// This is a helper for parsing SQL queries to find referenced sources.
/// In practice, this would integrate with the SQL parser to extract
/// table references from FROM clauses.
///
/// # Arguments
///
/// * `mv_name` - The MV name
/// * `source_tables` - Tables referenced in the query
///
/// # Returns
///
/// An `MvDefinition` with the extracted source references.
#[must_use]
pub fn analyze_mv_sources(mv_name: &str, source_tables: &[&str]) -> MvDefinition {
    MvDefinition::new(
        mv_name.to_string(),
        source_tables.iter().map(|s| (*s).to_string()).collect(),
    )
}

/// Channel derivation result with additional metadata.
#[derive(Debug, Clone)]
pub struct ChannelDerivationResult {
    /// Derived channel types per source.
    pub channel_types: HashMap<String, DerivedChannelType>,
    /// Sources with no consumers (orphaned).
    pub orphaned_sources: Vec<String>,
    /// Total broadcast channels needed.
    pub broadcast_count: usize,
    /// Total SPSC channels needed.
    pub spsc_count: usize,
}

/// Derives channel types with additional analysis metadata.
///
/// Returns a result that includes orphaned sources (sources with no consumers)
/// and counts of each channel type.
#[must_use]
pub fn derive_channel_types_detailed(
    sources: &[SourceDefinition],
    mvs: &[MvDefinition],
) -> ChannelDerivationResult {
    let channel_types = derive_channel_types(sources, mvs);

    let orphaned_sources: Vec<String> = channel_types
        .iter()
        .filter(|(_, ct)| matches!(ct, DerivedChannelType::Spsc))
        .filter(|(name, _)| {
            // Check if this source actually has any consumers
            !mvs.iter()
                .any(|mv| mv.source_refs.contains(*name))
        })
        .map(|(name, _)| name.clone())
        .collect();

    let broadcast_count = channel_types
        .values()
        .filter(|ct| ct.is_broadcast())
        .count();

    let spsc_count = channel_types.len() - broadcast_count;

    ChannelDerivationResult {
        channel_types,
        orphaned_sources,
        broadcast_count,
        spsc_count,
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_single_consumer_spsc() {
        let sources = vec![SourceDefinition::new("trades")];
        let mvs = vec![MvDefinition::from_source("vwap", "trades")];

        let channel_types = derive_channel_types(&sources, &mvs);

        assert_eq!(
            channel_types.get("trades"),
            Some(&DerivedChannelType::Spsc)
        );
    }

    #[test]
    fn test_derive_multiple_consumers_broadcast() {
        let sources = vec![SourceDefinition::new("trades")];
        let mvs = vec![
            MvDefinition::from_source("vwap", "trades"),
            MvDefinition::from_source("max_price", "trades"),
        ];

        let channel_types = derive_channel_types(&sources, &mvs);

        assert_eq!(
            channel_types.get("trades"),
            Some(&DerivedChannelType::Broadcast { consumer_count: 2 })
        );
    }

    #[test]
    fn test_derive_mixed_sources() {
        let sources = vec![
            SourceDefinition::new("trades"),
            SourceDefinition::new("orders"),
        ];
        let mvs = vec![
            MvDefinition::from_source("vwap", "trades"),
            MvDefinition::from_source("max_price", "trades"),
            MvDefinition::from_source("order_count", "orders"),
        ];

        let channel_types = derive_channel_types(&sources, &mvs);

        // trades: 2 consumers → Broadcast
        assert_eq!(
            channel_types.get("trades"),
            Some(&DerivedChannelType::Broadcast { consumer_count: 2 })
        );

        // orders: 1 consumer → SPSC
        assert_eq!(
            channel_types.get("orders"),
            Some(&DerivedChannelType::Spsc)
        );
    }

    #[test]
    fn test_derive_no_consumers() {
        let sources = vec![SourceDefinition::new("orphan")];
        let mvs: Vec<MvDefinition> = vec![];

        let channel_types = derive_channel_types(&sources, &mvs);

        // No consumers → SPSC (default)
        assert_eq!(
            channel_types.get("orphan"),
            Some(&DerivedChannelType::Spsc)
        );
    }

    #[test]
    fn test_derive_mv_with_multiple_sources() {
        let sources = vec![
            SourceDefinition::new("orders"),
            SourceDefinition::new("payments"),
        ];
        let mvs = vec![MvDefinition::new(
            "order_payments",
            vec!["orders".to_string(), "payments".to_string()],
        )];

        let channel_types = derive_channel_types(&sources, &mvs);

        // Both sources have 1 consumer → SPSC
        assert_eq!(
            channel_types.get("orders"),
            Some(&DerivedChannelType::Spsc)
        );
        assert_eq!(
            channel_types.get("payments"),
            Some(&DerivedChannelType::Spsc)
        );
    }

    #[test]
    fn test_derived_channel_type_methods() {
        let spsc = DerivedChannelType::Spsc;
        assert!(!spsc.is_broadcast());
        assert_eq!(spsc.consumer_count(), 1);

        let broadcast = DerivedChannelType::Broadcast { consumer_count: 3 };
        assert!(broadcast.is_broadcast());
        assert_eq!(broadcast.consumer_count(), 3);
    }

    #[test]
    fn test_source_definition() {
        let source = SourceDefinition::new("trades");
        assert_eq!(source.name, "trades");
        assert!(source.watermark_column.is_none());

        let source_wm = SourceDefinition::with_watermark("trades", "event_time");
        assert_eq!(source_wm.name, "trades");
        assert_eq!(source_wm.watermark_column, Some("event_time".to_string()));
    }

    #[test]
    fn test_mv_definition() {
        let mv = MvDefinition::from_source("vwap", "trades");
        assert_eq!(mv.name, "vwap");
        assert_eq!(mv.source_refs, vec!["trades"]);

        let mv_multi = MvDefinition::new(
            "join_result",
            vec!["orders".to_string(), "payments".to_string()],
        );
        assert_eq!(mv_multi.name, "join_result");
        assert_eq!(mv_multi.source_refs.len(), 2);
    }

    #[test]
    fn test_analyze_mv_sources() {
        let mv = analyze_mv_sources("my_mv", &["table1", "table2"]);
        assert_eq!(mv.name, "my_mv");
        assert_eq!(mv.source_refs, vec!["table1", "table2"]);
    }

    #[test]
    fn test_detailed_derivation() {
        let sources = vec![
            SourceDefinition::new("trades"),
            SourceDefinition::new("orders"),
            SourceDefinition::new("orphan"),
        ];
        let mvs = vec![
            MvDefinition::from_source("vwap", "trades"),
            MvDefinition::from_source("max_price", "trades"),
            MvDefinition::from_source("order_count", "orders"),
        ];

        let result = derive_channel_types_detailed(&sources, &mvs);

        assert_eq!(result.broadcast_count, 1); // trades
        assert_eq!(result.spsc_count, 2); // orders, orphan
        assert!(result.orphaned_sources.contains(&"orphan".to_string()));
    }

    #[test]
    fn test_three_consumers() {
        let sources = vec![SourceDefinition::new("events")];
        let mvs = vec![
            MvDefinition::from_source("mv1", "events"),
            MvDefinition::from_source("mv2", "events"),
            MvDefinition::from_source("mv3", "events"),
        ];

        let channel_types = derive_channel_types(&sources, &mvs);

        assert_eq!(
            channel_types.get("events"),
            Some(&DerivedChannelType::Broadcast { consumer_count: 3 })
        );
    }
}
