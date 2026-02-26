//! gRPC Aggregate Fan-Out.
//!
//! Sends aggregate queries to all (or a subset of) delta nodes,
//! collects partial results, merges them, and returns a unified response.
//!
//! ## Routing Strategies
//!
//! - **Full fan-out**: Query is broadcast to every node. The coordinator
//!   waits for a quorum (configurable via `QuorumPolicy`) before returning.
//! - **Partition affinity**: If the `FanOutRequest` includes a `key_filter`,
//!   the coordinator hashes the key to a single owning node and routes the
//!   query directly — bypassing the fan-out entirely (<1ms).
//!
//! ## Epoch Pinning
//!
//! Each request may specify an `epoch`. When set, the target node waits
//! until it has checkpointed at or beyond the requested epoch before
//! responding, guaranteeing read-after-write consistency.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::gossip_aggregates::AggregateState;
use crate::delta::discovery::NodeId;

// ── QuorumPolicy ────────────────────────────────────────────────────

/// Policy that determines when enough node responses have been received.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuorumPolicy {
    /// All nodes must respond.
    #[default]
    All,
    /// A strict majority (> total/2) must respond.
    Majority,
    /// At least `n` nodes must respond.
    AtLeast(usize),
}

impl QuorumPolicy {
    /// Check whether the quorum is satisfied.
    ///
    /// Returns `true` if `received` responses out of `total` nodes
    /// satisfies this policy.
    #[must_use]
    pub fn is_satisfied(&self, received: usize, total: usize) -> bool {
        match self {
            Self::All => received >= total,
            Self::Majority => {
                if total == 0 {
                    return false;
                }
                received > total / 2
            }
            Self::AtLeast(n) => received >= *n,
        }
    }
}

impl fmt::Display for QuorumPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "All"),
            Self::Majority => write!(f, "Majority"),
            Self::AtLeast(n) => write!(f, "AtLeast({n})"),
        }
    }
}

// ── FanOutRequest ───────────────────────────────────────────────────

/// A fan-out aggregate query to send to the delta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanOutRequest {
    /// Unique identifier for this query.
    pub query_id: String,
    /// Name of the aggregate to query.
    pub aggregate_name: String,
    /// If set, the target node waits for a checkpoint at or beyond this epoch.
    pub epoch: Option<u64>,
    /// If set, route to the single node that owns this key (partition affinity).
    pub key_filter: Option<Vec<u8>>,
    /// Quorum policy for this query.
    pub quorum: QuorumPolicy,
}

impl FanOutRequest {
    /// Create a new fan-out request.
    #[must_use]
    pub fn new(query_id: String, aggregate_name: String) -> Self {
        Self {
            query_id,
            aggregate_name,
            epoch: None,
            key_filter: None,
            quorum: QuorumPolicy::default(),
        }
    }

    /// Set the epoch pin for this request.
    #[must_use]
    pub fn with_epoch(mut self, epoch: u64) -> Self {
        self.epoch = Some(epoch);
        self
    }

    /// Set the key filter for partition-affinity routing.
    #[must_use]
    pub fn with_key_filter(mut self, key: Vec<u8>) -> Self {
        self.key_filter = Some(key);
        self
    }

    /// Set the quorum policy.
    #[must_use]
    pub fn with_quorum(mut self, quorum: QuorumPolicy) -> Self {
        self.quorum = quorum;
        self
    }

    /// Returns `true` if this request should use partition-affinity routing
    /// instead of a full fan-out.
    #[must_use]
    pub fn is_affinity_routed(&self) -> bool {
        self.key_filter.is_some()
    }

    /// Returns `true` if this request is epoch-pinned.
    #[must_use]
    pub fn is_epoch_pinned(&self) -> bool {
        self.epoch.is_some()
    }
}

// ── NodeAggregateResult ─────────────────────────────────────────────

/// Result from a single node in the delta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAggregateResult {
    /// The node that produced this result.
    pub node_id: NodeId,
    /// The aggregate state from this node.
    pub state: AggregateState,
    /// The epoch at which this result was computed.
    pub epoch: u64,
    /// Round-trip latency in microseconds.
    pub latency_us: u64,
}

// ── FanOutResponse ──────────────────────────────────────────────────

/// Aggregated response from a fan-out query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanOutResponse {
    /// The query this response corresponds to.
    pub query_id: String,
    /// Per-node results that were collected.
    pub results: Vec<NodeAggregateResult>,
    /// Whether all expected nodes responded.
    pub is_complete: bool,
    /// Whether the quorum policy was satisfied.
    pub quorum_met: bool,
}

impl FanOutResponse {
    /// Merge all per-node aggregate states into a single combined state.
    ///
    /// Returns `None` if the response contains no results.
    #[must_use]
    pub fn merged_state(&self) -> Option<AggregateState> {
        if self.results.is_empty() {
            return None;
        }
        let mut merged = self.results[0].state.clone();
        for result in &self.results[1..] {
            merged.merge(&result.state);
        }
        Some(merged)
    }

    /// Maximum latency across all node responses, in microseconds.
    #[must_use]
    pub fn max_latency_us(&self) -> u64 {
        self.results.iter().map(|r| r.latency_us).max().unwrap_or(0)
    }

    /// Number of nodes that contributed results.
    #[must_use]
    pub fn contributing_nodes(&self) -> usize {
        self.results.len()
    }
}

// ── FanOutConfig ────────────────────────────────────────────────────

/// Configuration for the fan-out coordinator.
#[derive(Debug, Clone)]
pub struct FanOutConfig {
    /// Maximum time to wait for all responses.
    pub timeout: Duration,
    /// Maximum number of concurrent in-flight requests.
    pub max_concurrent: usize,
    /// Number of retry attempts per node on transient failure.
    pub retry_count: u32,
}

impl Default for FanOutConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            max_concurrent: 64,
            retry_count: 2,
        }
    }
}

// ── FanOutMetrics ───────────────────────────────────────────────────

/// Atomic counters for fan-out operations.
pub struct FanOutMetrics {
    /// Total number of fan-out queries initiated.
    pub queries_sent: AtomicU64,
    /// Total number of fan-out queries that completed successfully.
    pub queries_completed: AtomicU64,
    /// Number of queries where quorum was not met.
    pub quorum_failures: AtomicU64,
    /// Number of queries that timed out.
    pub timeouts: AtomicU64,
}

impl FanOutMetrics {
    /// Create a new zeroed metrics instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            queries_sent: AtomicU64::new(0),
            queries_completed: AtomicU64::new(0),
            quorum_failures: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
        }
    }

    /// Take a point-in-time snapshot of all counters.
    #[must_use]
    pub fn snapshot(&self) -> FanOutMetricsSnapshot {
        FanOutMetricsSnapshot {
            queries_sent: self.queries_sent.load(Ordering::Relaxed),
            queries_completed: self.queries_completed.load(Ordering::Relaxed),
            quorum_failures: self.quorum_failures.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
        }
    }

    /// Record that a query was sent.
    pub fn record_sent(&self) {
        self.queries_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a query completed successfully.
    pub fn record_completed(&self) {
        self.queries_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a quorum failure.
    pub fn record_quorum_failure(&self) {
        self.quorum_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timeout.
    pub fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for FanOutMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for FanOutMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FanOutMetrics")
            .field("queries_sent", &self.queries_sent.load(Ordering::Relaxed))
            .field(
                "queries_completed",
                &self.queries_completed.load(Ordering::Relaxed),
            )
            .field(
                "quorum_failures",
                &self.quorum_failures.load(Ordering::Relaxed),
            )
            .field("timeouts", &self.timeouts.load(Ordering::Relaxed))
            .finish()
    }
}

/// Point-in-time snapshot of [`FanOutMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanOutMetricsSnapshot {
    /// Total queries sent.
    pub queries_sent: u64,
    /// Total queries completed.
    pub queries_completed: u64,
    /// Total quorum failures.
    pub quorum_failures: u64,
    /// Total timeouts.
    pub timeouts: u64,
}

// ── FanOutError ─────────────────────────────────────────────────────

/// Errors that can occur during fan-out operations.
#[derive(Debug, thiserror::Error)]
pub enum FanOutError {
    /// The fan-out query timed out before quorum was reached.
    #[error("fan-out query timed out after {0:?}")]
    Timeout(Duration),

    /// Not enough nodes responded to satisfy the quorum policy.
    #[error(
        "quorum not met: received {received} of {total} \
         (policy: {policy})"
    )]
    QuorumNotMet {
        /// Number of responses received.
        received: usize,
        /// Total number of nodes queried.
        total: usize,
        /// The quorum policy that was not satisfied.
        policy: QuorumPolicy,
    },

    /// No nodes are available to query.
    #[error("no nodes available in the delta")]
    NoNodes,

    /// A specific node failed to respond.
    #[error("node {node_id} failed: {reason}")]
    NodeFailed {
        /// The node that failed.
        node_id: NodeId,
        /// Human-readable failure reason.
        reason: String,
    },
}

// ── FanOutCoordinator ───────────────────────────────────────────────

/// Coordinates fan-out aggregate queries across delta nodes.
///
/// The coordinator maintains a configuration and metrics. The actual
/// gRPC transport is injected when `execute` is wired up.
/// For now the coordinator provides routing logic, quorum evaluation,
/// and result merging.
pub struct FanOutCoordinator {
    /// Configuration for timeouts, concurrency, retries.
    config: FanOutConfig,
    /// Operational metrics.
    metrics: FanOutMetrics,
}

impl FanOutCoordinator {
    /// Create a new coordinator with the given configuration.
    #[must_use]
    pub fn new(config: FanOutConfig) -> Self {
        Self {
            config,
            metrics: FanOutMetrics::new(),
        }
    }

    /// Create a coordinator with default configuration.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(FanOutConfig::default())
    }

    /// Access the coordinator's configuration.
    #[must_use]
    pub fn config(&self) -> &FanOutConfig {
        &self.config
    }

    /// Access the coordinator's metrics.
    #[must_use]
    pub fn metrics(&self) -> &FanOutMetrics {
        &self.metrics
    }

    /// Determine which node(s) to query for the given request.
    ///
    /// If the request has a `key_filter`, the key is hashed to a single
    /// partition and the owning node is returned (affinity routing).
    /// Otherwise, all nodes are returned for full fan-out.
    #[must_use]
    pub fn resolve_targets(
        &self,
        request: &FanOutRequest,
        all_nodes: &[NodeId],
        partition_map: &HashMap<u32, NodeId>,
        num_partitions: u32,
    ) -> Vec<NodeId> {
        if let Some(key) = &request.key_filter {
            // Partition-affinity routing: hash key to partition, find owner.
            let partition = key_to_partition(key, num_partitions);
            if let Some(&owner) = partition_map.get(&partition) {
                return vec![owner];
            }
        }
        // Full fan-out to all nodes.
        all_nodes.to_vec()
    }

    /// Evaluate a set of node results against the request's quorum policy.
    ///
    /// Returns a [`FanOutResponse`] with `quorum_met` and `is_complete` set
    /// based on the policy and the number of results received.
    #[must_use]
    pub fn evaluate_results(
        &self,
        request: &FanOutRequest,
        results: Vec<NodeAggregateResult>,
        total_nodes: usize,
    ) -> FanOutResponse {
        let received = results.len();
        let quorum_met = request.quorum.is_satisfied(received, total_nodes);
        let is_complete = received >= total_nodes;

        if quorum_met {
            self.metrics.record_completed();
        } else {
            self.metrics.record_quorum_failure();
        }

        FanOutResponse {
            query_id: request.query_id.clone(),
            results,
            is_complete,
            quorum_met,
        }
    }

    /// Validate that an epoch-pinned request has consistent results.
    ///
    /// All node results must have an epoch >= the requested epoch.
    /// Returns the list of nodes whose epoch is behind.
    #[must_use]
    pub fn validate_epoch_pin(
        request: &FanOutRequest,
        results: &[NodeAggregateResult],
    ) -> Vec<NodeId> {
        let Some(target_epoch) = request.epoch else {
            return Vec::new();
        };

        results
            .iter()
            .filter(|r| r.epoch < target_epoch)
            .map(|r| r.node_id)
            .collect()
    }
}

impl fmt::Debug for FanOutCoordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FanOutCoordinator")
            .field("config", &self.config)
            .field("metrics", &self.metrics)
            .finish()
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Hash a key to a partition index using FNV-1a.
///
/// This must be consistent with the partition assignment used by the
/// delta's partition manager.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
fn key_to_partition(key: &[u8], num_partitions: u32) -> u32 {
    // FNV-1a 64-bit
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in key {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    (hash % u64::from(num_partitions)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── QuorumPolicy tests ──────────────────────────────────────────

    #[test]
    fn test_quorum_all_satisfied() {
        assert!(QuorumPolicy::All.is_satisfied(3, 3));
    }

    #[test]
    fn test_quorum_all_not_satisfied() {
        assert!(!QuorumPolicy::All.is_satisfied(2, 3));
    }

    #[test]
    fn test_quorum_majority_satisfied() {
        assert!(QuorumPolicy::Majority.is_satisfied(2, 3));
        assert!(QuorumPolicy::Majority.is_satisfied(3, 5));
    }

    #[test]
    fn test_quorum_majority_not_satisfied() {
        assert!(!QuorumPolicy::Majority.is_satisfied(1, 3));
        assert!(!QuorumPolicy::Majority.is_satisfied(2, 5));
    }

    #[test]
    fn test_quorum_majority_zero_total() {
        assert!(!QuorumPolicy::Majority.is_satisfied(0, 0));
    }

    #[test]
    fn test_quorum_at_least_satisfied() {
        assert!(QuorumPolicy::AtLeast(2).is_satisfied(2, 5));
        assert!(QuorumPolicy::AtLeast(1).is_satisfied(3, 5));
    }

    #[test]
    fn test_quorum_at_least_not_satisfied() {
        assert!(!QuorumPolicy::AtLeast(3).is_satisfied(2, 5));
    }

    #[test]
    fn test_quorum_display() {
        assert_eq!(QuorumPolicy::All.to_string(), "All");
        assert_eq!(QuorumPolicy::Majority.to_string(), "Majority");
        assert_eq!(QuorumPolicy::AtLeast(3).to_string(), "AtLeast(3)");
    }

    #[test]
    fn test_quorum_default_is_all() {
        assert_eq!(QuorumPolicy::default(), QuorumPolicy::All);
    }

    // ── FanOutConfig tests ──────────────────────────────────────────

    #[test]
    fn test_config_defaults() {
        let config = FanOutConfig::default();
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(config.max_concurrent, 64);
        assert_eq!(config.retry_count, 2);
    }

    // ── FanOutMetrics tests ─────────────────────────────────────────

    #[test]
    fn test_metrics_initial_zeroed() {
        let m = FanOutMetrics::new();
        let snap = m.snapshot();
        assert_eq!(snap.queries_sent, 0);
        assert_eq!(snap.queries_completed, 0);
        assert_eq!(snap.quorum_failures, 0);
        assert_eq!(snap.timeouts, 0);
    }

    #[test]
    fn test_metrics_recording() {
        let m = FanOutMetrics::new();
        m.record_sent();
        m.record_sent();
        m.record_completed();
        m.record_quorum_failure();
        m.record_timeout();

        let snap = m.snapshot();
        assert_eq!(snap.queries_sent, 2);
        assert_eq!(snap.queries_completed, 1);
        assert_eq!(snap.quorum_failures, 1);
        assert_eq!(snap.timeouts, 1);
    }

    // ── FanOutCoordinator tests ─────────────────────────────────────

    #[test]
    fn test_coordinator_creation() {
        let coord = FanOutCoordinator::with_defaults();
        assert_eq!(coord.config().timeout, Duration::from_secs(5));
        assert_eq!(coord.metrics().snapshot().queries_sent, 0);
    }

    #[test]
    fn test_coordinator_custom_config() {
        let config = FanOutConfig {
            timeout: Duration::from_millis(500),
            max_concurrent: 16,
            retry_count: 5,
        };
        let coord = FanOutCoordinator::new(config);
        assert_eq!(coord.config().timeout, Duration::from_millis(500));
        assert_eq!(coord.config().max_concurrent, 16);
        assert_eq!(coord.config().retry_count, 5);
    }

    // ── FanOutRequest / Response tests ──────────────────────────────

    #[test]
    fn test_request_builder() {
        let req = FanOutRequest::new("q1".into(), "total_sales".into())
            .with_epoch(42)
            .with_key_filter(vec![1, 2, 3])
            .with_quorum(QuorumPolicy::Majority);

        assert_eq!(req.query_id, "q1");
        assert_eq!(req.aggregate_name, "total_sales");
        assert_eq!(req.epoch, Some(42));
        assert_eq!(req.key_filter, Some(vec![1, 2, 3]));
        assert_eq!(req.quorum, QuorumPolicy::Majority);
        assert!(req.is_affinity_routed());
        assert!(req.is_epoch_pinned());
    }

    #[test]
    fn test_request_defaults() {
        let req = FanOutRequest::new("q2".into(), "count".into());
        assert!(req.epoch.is_none());
        assert!(req.key_filter.is_none());
        assert_eq!(req.quorum, QuorumPolicy::All);
        assert!(!req.is_affinity_routed());
        assert!(!req.is_epoch_pinned());
    }

    #[test]
    fn test_response_merged_state() {
        let resp = FanOutResponse {
            query_id: "q1".into(),
            results: vec![
                NodeAggregateResult {
                    node_id: NodeId(1),
                    state: AggregateState::Count(10),
                    epoch: 5,
                    latency_us: 100,
                },
                NodeAggregateResult {
                    node_id: NodeId(2),
                    state: AggregateState::Count(20),
                    epoch: 5,
                    latency_us: 200,
                },
            ],
            is_complete: true,
            quorum_met: true,
        };

        let merged = resp.merged_state().unwrap();
        assert_eq!(merged, AggregateState::Count(30));
        assert_eq!(resp.max_latency_us(), 200);
        assert_eq!(resp.contributing_nodes(), 2);
    }

    #[test]
    fn test_response_empty_results() {
        let resp = FanOutResponse {
            query_id: "q1".into(),
            results: vec![],
            is_complete: false,
            quorum_met: false,
        };
        assert!(resp.merged_state().is_none());
        assert_eq!(resp.max_latency_us(), 0);
        assert_eq!(resp.contributing_nodes(), 0);
    }

    // ── Key filter / affinity routing tests ─────────────────────────

    #[test]
    fn test_affinity_routing_to_single_node() {
        let coord = FanOutCoordinator::with_defaults();
        let all_nodes = vec![NodeId(1), NodeId(2), NodeId(3)];

        let key = b"customer_42";
        let partition = key_to_partition(key, 4);
        let mut partition_map = HashMap::new();
        partition_map.insert(partition, NodeId(2));

        let req = FanOutRequest::new("q1".into(), "agg".into()).with_key_filter(key.to_vec());

        let targets = coord.resolve_targets(&req, &all_nodes, &partition_map, 4);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0], NodeId(2));
    }

    #[test]
    fn test_full_fanout_without_key_filter() {
        let coord = FanOutCoordinator::with_defaults();
        let all_nodes = vec![NodeId(1), NodeId(2), NodeId(3)];
        let partition_map = HashMap::new();

        let req = FanOutRequest::new("q1".into(), "agg".into());

        let targets = coord.resolve_targets(&req, &all_nodes, &partition_map, 4);
        assert_eq!(targets.len(), 3);
    }

    // ── Epoch pinning validation tests ──────────────────────────────

    #[test]
    fn test_epoch_pin_all_up_to_date() {
        let req = FanOutRequest::new("q1".into(), "agg".into()).with_epoch(10);
        let results = vec![
            NodeAggregateResult {
                node_id: NodeId(1),
                state: AggregateState::Count(5),
                epoch: 10,
                latency_us: 50,
            },
            NodeAggregateResult {
                node_id: NodeId(2),
                state: AggregateState::Count(3),
                epoch: 12,
                latency_us: 80,
            },
        ];

        let lagging = FanOutCoordinator::validate_epoch_pin(&req, &results);
        assert!(lagging.is_empty());
    }

    #[test]
    fn test_epoch_pin_some_behind() {
        let req = FanOutRequest::new("q1".into(), "agg".into()).with_epoch(10);
        let results = vec![
            NodeAggregateResult {
                node_id: NodeId(1),
                state: AggregateState::Count(5),
                epoch: 10,
                latency_us: 50,
            },
            NodeAggregateResult {
                node_id: NodeId(2),
                state: AggregateState::Count(3),
                epoch: 8,
                latency_us: 80,
            },
        ];

        let lagging = FanOutCoordinator::validate_epoch_pin(&req, &results);
        assert_eq!(lagging, vec![NodeId(2)]);
    }

    #[test]
    fn test_epoch_pin_none_requested() {
        let req = FanOutRequest::new("q1".into(), "agg".into());
        let results = vec![NodeAggregateResult {
            node_id: NodeId(1),
            state: AggregateState::Count(5),
            epoch: 1,
            latency_us: 50,
        }];

        let lagging = FanOutCoordinator::validate_epoch_pin(&req, &results);
        assert!(lagging.is_empty());
    }

    // ── Error display tests ─────────────────────────────────────────

    #[test]
    fn test_error_display_timeout() {
        let err = FanOutError::Timeout(Duration::from_secs(5));
        assert_eq!(err.to_string(), "fan-out query timed out after 5s");
    }

    #[test]
    fn test_error_display_quorum_not_met() {
        let err = FanOutError::QuorumNotMet {
            received: 1,
            total: 3,
            policy: QuorumPolicy::All,
        };
        assert_eq!(
            err.to_string(),
            "quorum not met: received 1 of 3 (policy: All)"
        );
    }

    #[test]
    fn test_error_display_no_nodes() {
        let err = FanOutError::NoNodes;
        assert_eq!(err.to_string(), "no nodes available in the delta");
    }

    #[test]
    fn test_error_display_node_failed() {
        let err = FanOutError::NodeFailed {
            node_id: NodeId(7),
            reason: "connection refused".into(),
        };
        assert_eq!(err.to_string(), "node node-7 failed: connection refused");
    }

    // ── Evaluate results tests ──────────────────────────────────────

    #[test]
    fn test_evaluate_results_quorum_met() {
        let coord = FanOutCoordinator::with_defaults();
        let req = FanOutRequest::new("q1".into(), "agg".into()).with_quorum(QuorumPolicy::Majority);

        let results = vec![
            NodeAggregateResult {
                node_id: NodeId(1),
                state: AggregateState::Count(10),
                epoch: 5,
                latency_us: 100,
            },
            NodeAggregateResult {
                node_id: NodeId(2),
                state: AggregateState::Count(20),
                epoch: 5,
                latency_us: 150,
            },
        ];

        let resp = coord.evaluate_results(&req, results, 3);
        assert!(resp.quorum_met);
        assert!(!resp.is_complete);
        assert_eq!(resp.contributing_nodes(), 2);

        let snap = coord.metrics().snapshot();
        assert_eq!(snap.queries_completed, 1);
        assert_eq!(snap.quorum_failures, 0);
    }

    #[test]
    fn test_evaluate_results_quorum_not_met() {
        let coord = FanOutCoordinator::with_defaults();
        let req = FanOutRequest::new("q1".into(), "agg".into()).with_quorum(QuorumPolicy::All);

        let results = vec![NodeAggregateResult {
            node_id: NodeId(1),
            state: AggregateState::Count(10),
            epoch: 5,
            latency_us: 100,
        }];

        let resp = coord.evaluate_results(&req, results, 3);
        assert!(!resp.quorum_met);
        assert!(!resp.is_complete);

        let snap = coord.metrics().snapshot();
        assert_eq!(snap.queries_completed, 0);
        assert_eq!(snap.quorum_failures, 1);
    }

    // ── key_to_partition determinism ────────────────────────────────

    #[test]
    fn test_key_to_partition_deterministic() {
        let p1 = key_to_partition(b"hello", 8);
        let p2 = key_to_partition(b"hello", 8);
        assert_eq!(p1, p2);
        assert!(p1 < 8);
    }

    #[test]
    fn test_key_to_partition_different_keys() {
        // Different keys may or may not hash to different partitions,
        // but they must always be in range.
        for i in 0..100u32 {
            let key = i.to_le_bytes();
            let p = key_to_partition(&key, 16);
            assert!(p < 16);
        }
    }
}
