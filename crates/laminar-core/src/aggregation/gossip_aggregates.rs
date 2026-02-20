//! Gossip-based partial aggregate replication across nodes.
//!
//! Reuses the chitchat gossip protocol from the gossip discovery module to disseminate
//! partial aggregates across the delta. Each node publishes
//! its local partials to chitchat, and a merger reads both local and
//! remote partials to produce cluster-wide aggregates.
//!
//! ## Key Format
//!
//! Chitchat key namespace: `agg/{pipeline_id}/{aggregate_name}/{window_scope}`
//!
//! ## Watermark-Gated Windows
//!
//! A window is considered FINAL when ALL nodes have a watermark >= window_end.

use std::collections::HashMap;
use std::hash::BuildHasher;

use serde::{Deserialize, Serialize};

use crate::delta::discovery::NodeId;

/// Aggregate state for a single partial.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateState {
    /// Count aggregate.
    Count(i64),
    /// Sum aggregate.
    Sum(f64),
    /// Minimum value.
    Min(f64),
    /// Maximum value.
    Max(f64),
    /// Average (running sum + count).
    Avg {
        /// Running sum.
        sum: f64,
        /// Running count.
        count: i64,
    },
    /// Custom aggregate with opaque serialized state.
    Custom(Vec<u8>),
}

impl AggregateState {
    /// Merge another partial into this one.
    pub fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => *a += b,
            (Self::Sum(a), Self::Sum(b)) => *a += b,
            (Self::Min(a), Self::Min(b)) => {
                if *b < *a {
                    *a = *b;
                }
            }
            (Self::Max(a), Self::Max(b)) => {
                if *b > *a {
                    *a = *b;
                }
            }
            (Self::Avg { sum: s1, count: c1 }, Self::Avg { sum: s2, count: c2 }) => {
                *s1 += s2;
                *c1 += c2;
            }
            _ => {} // Type mismatch — ignore
        }
    }

    /// Finalize the aggregate to a scalar value.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn finalize(&self) -> f64 {
        match self {
            Self::Count(n) => *n as f64,
            Self::Sum(s) | Self::Min(s) | Self::Max(s) => *s,
            Self::Avg { sum, count } => {
                if *count > 0 {
                    sum / (*count as f64)
                } else {
                    0.0
                }
            }
            Self::Custom(_) => f64::NAN,
        }
    }
}

/// A gossip-published aggregate value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipAggregateValue {
    /// Source node.
    pub node_id: NodeId,
    /// Watermark at time of publication (millis since epoch).
    pub watermark_ms: i64,
    /// Epoch at time of publication.
    pub epoch: u64,
    /// The partial aggregate state.
    pub state: AggregateState,
}

/// Chitchat key builder for aggregate namespacing.
#[derive(Debug, Clone)]
pub struct AggregateKeyspace {
    /// Pipeline identifier.
    pub pipeline_id: String,
    /// Aggregate name.
    pub aggregate_name: String,
}

impl AggregateKeyspace {
    /// Create a new aggregate keyspace.
    #[must_use]
    pub fn new(pipeline_id: String, aggregate_name: String) -> Self {
        Self {
            pipeline_id,
            aggregate_name,
        }
    }

    /// Build a chitchat key for a global aggregate.
    #[must_use]
    pub fn global_key(&self) -> String {
        format!("agg/{}/{}/global", self.pipeline_id, self.aggregate_name)
    }

    /// Build a chitchat key for a windowed aggregate.
    #[must_use]
    pub fn window_key(&self, window_start: i64, window_end: i64) -> String {
        format!(
            "agg/{}/{}/window/{window_start}_{window_end}",
            self.pipeline_id, self.aggregate_name
        )
    }

    /// Parse a chitchat key to extract its scope.
    #[must_use]
    pub fn parse_scope(key: &str) -> Option<AggregateScope> {
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() < 4 || parts[0] != "agg" {
            return None;
        }
        match parts[3] {
            "global" => Some(AggregateScope::Global),
            "window" if parts.len() >= 5 => {
                let window_parts: Vec<&str> = parts[4].split('_').collect();
                if window_parts.len() == 2 {
                    let start = window_parts[0].parse().ok()?;
                    let end = window_parts[1].parse().ok()?;
                    Some(AggregateScope::Window { start, end })
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// The scope of a gossip aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateScope {
    /// Global (un-windowed) aggregate.
    Global,
    /// Windowed aggregate.
    Window {
        /// Window start (millis since epoch).
        start: i64,
        /// Window end (millis since epoch).
        end: i64,
    },
}

/// Watermark-gated window completion status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatermarkGateStatus {
    /// All nodes have watermark >= `window_end`.
    Complete,
    /// Some nodes have not yet advanced past `window_end`.
    Incomplete {
        /// Nodes that haven't advanced.
        lagging_nodes: Vec<NodeId>,
        /// Minimum watermark across all nodes.
        min_watermark: i64,
    },
    /// Not enough information (e.g., unknown node set).
    Unknown,
}

/// Checks watermark convergence for window completion.
#[must_use]
pub fn check_watermark_gate<S: BuildHasher>(
    window_end: i64,
    node_watermarks: &HashMap<NodeId, i64, S>,
) -> WatermarkGateStatus {
    if node_watermarks.is_empty() {
        return WatermarkGateStatus::Unknown;
    }

    let mut lagging = Vec::new();
    let mut min_wm = i64::MAX;

    for (node_id, &wm) in node_watermarks {
        min_wm = min_wm.min(wm);
        if wm < window_end {
            lagging.push(*node_id);
        }
    }

    if lagging.is_empty() {
        WatermarkGateStatus::Complete
    } else {
        WatermarkGateStatus::Incomplete {
            lagging_nodes: lagging,
            min_watermark: min_wm,
        }
    }
}

/// Result of merging aggregates across the cluster.
#[derive(Debug, Clone)]
pub struct ClusterAggregateResult {
    /// The merged aggregate state.
    pub state: AggregateState,
    /// Number of nodes that contributed.
    pub contributing_nodes: usize,
    /// Whether the result is complete (all nodes reported).
    pub is_complete: bool,
    /// Maximum staleness across contributing nodes (millis).
    pub max_staleness_ms: i64,
}

/// Merges partial aggregates from multiple nodes.
#[must_use]
pub fn merge_cluster_aggregates(
    partials: &[GossipAggregateValue],
    expected_nodes: usize,
) -> Option<ClusterAggregateResult> {
    if partials.is_empty() {
        return None;
    }

    let mut merged = partials[0].state.clone();
    let now = chrono::Utc::now().timestamp_millis();
    let mut max_staleness: i64 = 0;

    for partial in &partials[1..] {
        merged.merge(&partial.state);
        let staleness = now.saturating_sub(partial.watermark_ms);
        max_staleness = max_staleness.max(staleness);
    }

    Some(ClusterAggregateResult {
        state: merged,
        contributing_nodes: partials.len(),
        is_complete: partials.len() >= expected_nodes,
        max_staleness_ms: max_staleness,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_state_merge_count() {
        let mut a = AggregateState::Count(10);
        a.merge(&AggregateState::Count(5));
        assert_eq!(a, AggregateState::Count(15));
    }

    #[test]
    fn test_aggregate_state_merge_sum() {
        let mut a = AggregateState::Sum(1.5);
        a.merge(&AggregateState::Sum(2.5));
        assert_eq!(a, AggregateState::Sum(4.0));
    }

    #[test]
    fn test_aggregate_state_merge_min() {
        let mut a = AggregateState::Min(10.0);
        a.merge(&AggregateState::Min(5.0));
        assert_eq!(a, AggregateState::Min(5.0));
    }

    #[test]
    fn test_aggregate_state_merge_max() {
        let mut a = AggregateState::Max(5.0);
        a.merge(&AggregateState::Max(10.0));
        assert_eq!(a, AggregateState::Max(10.0));
    }

    #[test]
    fn test_aggregate_state_merge_avg() {
        let mut a = AggregateState::Avg {
            sum: 10.0,
            count: 2,
        };
        a.merge(&AggregateState::Avg {
            sum: 20.0,
            count: 3,
        });
        match a {
            AggregateState::Avg { sum, count } => {
                assert!((sum - 30.0).abs() < f64::EPSILON);
                assert_eq!(count, 5);
            }
            _ => panic!("expected Avg"),
        }
    }

    #[test]
    fn test_aggregate_state_finalize() {
        assert!((AggregateState::Count(42).finalize() - 42.0).abs() < f64::EPSILON);
        assert!(
            (AggregateState::Sum(std::f64::consts::PI).finalize() - std::f64::consts::PI).abs()
                < f64::EPSILON
        );
        let avg = AggregateState::Avg {
            sum: 10.0,
            count: 4,
        };
        assert!((avg.finalize() - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_aggregate_keyspace_global() {
        let ks = AggregateKeyspace::new("pipe1".into(), "total_sales".into());
        assert_eq!(ks.global_key(), "agg/pipe1/total_sales/global");
    }

    #[test]
    fn test_aggregate_keyspace_window() {
        let ks = AggregateKeyspace::new("pipe1".into(), "hourly_count".into());
        assert_eq!(
            ks.window_key(1000, 2000),
            "agg/pipe1/hourly_count/window/1000_2000"
        );
    }

    #[test]
    fn test_parse_scope_global() {
        let scope = AggregateKeyspace::parse_scope("agg/pipe1/total/global").unwrap();
        assert_eq!(scope, AggregateScope::Global);
    }

    #[test]
    fn test_parse_scope_window() {
        let scope = AggregateKeyspace::parse_scope("agg/pipe1/hourly/window/1000_2000").unwrap();
        assert_eq!(
            scope,
            AggregateScope::Window {
                start: 1000,
                end: 2000
            }
        );
    }

    #[test]
    fn test_parse_scope_invalid() {
        assert!(AggregateKeyspace::parse_scope("invalid").is_none());
        assert!(AggregateKeyspace::parse_scope("agg/a/b").is_none());
        assert!(AggregateKeyspace::parse_scope("agg/a/b/unknown").is_none());
    }

    #[test]
    fn test_watermark_gate_complete() {
        let mut wms = HashMap::new();
        wms.insert(NodeId(1), 2000);
        wms.insert(NodeId(2), 1500);
        assert_eq!(
            check_watermark_gate(1000, &wms),
            WatermarkGateStatus::Complete
        );
    }

    #[test]
    fn test_watermark_gate_incomplete() {
        let mut wms = HashMap::new();
        wms.insert(NodeId(1), 2000);
        wms.insert(NodeId(2), 500);
        let status = check_watermark_gate(1000, &wms);
        match status {
            WatermarkGateStatus::Incomplete {
                lagging_nodes,
                min_watermark,
            } => {
                assert_eq!(lagging_nodes, vec![NodeId(2)]);
                assert_eq!(min_watermark, 500);
            }
            _ => panic!("expected Incomplete"),
        }
    }

    #[test]
    fn test_watermark_gate_empty() {
        assert_eq!(
            check_watermark_gate(1000, &HashMap::new()),
            WatermarkGateStatus::Unknown
        );
    }

    #[test]
    fn test_merge_cluster_aggregates() {
        let partials = vec![
            GossipAggregateValue {
                node_id: NodeId(1),
                watermark_ms: chrono::Utc::now().timestamp_millis(),
                epoch: 1,
                state: AggregateState::Count(10),
            },
            GossipAggregateValue {
                node_id: NodeId(2),
                watermark_ms: chrono::Utc::now().timestamp_millis(),
                epoch: 1,
                state: AggregateState::Count(20),
            },
        ];

        let result = merge_cluster_aggregates(&partials, 2).unwrap();
        assert_eq!(result.state, AggregateState::Count(30));
        assert_eq!(result.contributing_nodes, 2);
        assert!(result.is_complete);
    }

    #[test]
    fn test_merge_cluster_aggregates_empty() {
        assert!(merge_cluster_aggregates(&[], 3).is_none());
    }

    #[test]
    fn test_merge_cluster_aggregates_incomplete() {
        let partials = vec![GossipAggregateValue {
            node_id: NodeId(1),
            watermark_ms: chrono::Utc::now().timestamp_millis(),
            epoch: 1,
            state: AggregateState::Sum(42.0),
        }];

        let result = merge_cluster_aggregates(&partials, 3).unwrap();
        assert!(!result.is_complete);
        assert_eq!(result.contributing_nodes, 1);
    }

    #[test]
    fn test_gossip_aggregate_value_serialization() {
        let val = GossipAggregateValue {
            node_id: NodeId(1),
            watermark_ms: 1000,
            epoch: 5,
            state: AggregateState::Count(42),
        };
        let json = serde_json::to_string(&val).unwrap();
        let back: GossipAggregateValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back.node_id, NodeId(1));
        assert_eq!(back.epoch, 5);
    }

    #[test]
    fn test_aggregate_type_mismatch_merge_noop() {
        let mut a = AggregateState::Count(10);
        a.merge(&AggregateState::Sum(5.0));
        // Type mismatch should be a no-op
        assert_eq!(a, AggregateState::Count(10));
    }

    #[test]
    fn test_avg_zero_count_finalize() {
        let avg = AggregateState::Avg { sum: 0.0, count: 0 };
        assert!((avg.finalize()).abs() < f64::EPSILON);
    }
}
