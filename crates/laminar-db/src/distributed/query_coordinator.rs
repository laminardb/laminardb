//! Distributed query coordinator — fan-out, execute, gather, merge.
//!
//! The [`DistributedQueryCoordinator`] decides how to execute a query
//! based on partition placement, fans out sub-queries to the owning
//! nodes, and merges partial results at the coordinator.

#![allow(clippy::disallowed_types)] // cold path: query coordination is control-plane

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow::compute;

use laminar_core::delta::discovery::NodeId;

use super::partition_map::PartitionMap;
use crate::error::DbError;
use crate::stream_executor::extract_table_references;

/// Network-addressable peer node.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Unique identifier.
    pub node_id: NodeId,
    /// RPC address (host:port).
    pub address: String,
}

/// How to execute a distributed query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionStrategy {
    /// All partitions are local — execute on this node only.
    LocalOnly,
    /// Partitions span multiple nodes — fan out and merge.
    FanOut {
        /// Node IDs that must participate.
        target_nodes: Vec<NodeId>,
    },
    /// Broadcast to all nodes (e.g., lookup table queries).
    Broadcast,
}

impl fmt::Display for ExecutionStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalOnly => write!(f, "local-only"),
            Self::FanOut { target_nodes } => {
                write!(f, "fan-out({} nodes)", target_nodes.len())
            }
            Self::Broadcast => write!(f, "broadcast"),
        }
    }
}

/// How to merge partial results from multiple nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeStrategy {
    /// Simple concatenation — no aggregation needed.
    Concat,
    /// Merge partial aggregates by group key columns.
    PartialAggregate {
        /// Indices of the group-key columns in the result schema.
        group_key_indices: Vec<usize>,
        /// Per aggregate column: (`column_index`, `aggregate_kind`).
        aggregates: Vec<(usize, AggregateKind)>,
    },
}

/// The kind of aggregate for partial merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateKind {
    /// SUM: sum of partial sums.
    Sum,
    /// COUNT: sum of partial counts.
    Count,
    /// MIN: min of partial mins.
    Min,
    /// MAX: max of partial maxes.
    Max,
}

impl fmt::Display for AggregateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sum => write!(f, "SUM"),
            Self::Count => write!(f, "COUNT"),
            Self::Min => write!(f, "MIN"),
            Self::Max => write!(f, "MAX"),
        }
    }
}

/// Per-node execution statistics gathered during a distributed query.
#[derive(Debug, Clone)]
pub struct NodeExecutionStats {
    /// Which node executed.
    pub node_id: NodeId,
    /// Wall-clock time for execution on this node.
    pub elapsed: Duration,
    /// Number of rows returned.
    pub rows_returned: usize,
    /// Number of batches returned.
    pub batches_returned: usize,
}

/// The merged result of a distributed query.
#[derive(Debug)]
pub struct GatherResult {
    /// Merged `RecordBatch`es from all participating nodes.
    pub batches: Vec<RecordBatch>,
    /// Per-node execution statistics.
    pub stats: Vec<NodeExecutionStats>,
    /// The execution strategy that was used.
    pub strategy: ExecutionStrategy,
}

impl GatherResult {
    /// Total rows across all batches.
    #[must_use]
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Total rows across all node stats (pre-merge).
    #[must_use]
    pub fn total_rows_pre_merge(&self) -> usize {
        self.stats.iter().map(|s| s.rows_returned).sum()
    }
}

/// Coordinates distributed query execution across cluster nodes.
///
/// Given a SQL query and the current partition map, the coordinator:
/// 1. Determines which nodes own the referenced partitions
/// 2. Chooses an execution strategy (local, fan-out, broadcast)
/// 3. Collects partial results from each node
/// 4. Merges partial results into a final result set
pub struct DistributedQueryCoordinator {
    /// This node's identity.
    local_node_id: NodeId,
    /// Known peer nodes for fan-out.
    peers: Vec<PeerInfo>,
    /// Partition-to-node mapping.
    partition_map: Arc<PartitionMap>,
}

impl DistributedQueryCoordinator {
    /// Create a new coordinator.
    #[must_use]
    pub fn new(
        local_node_id: NodeId,
        peers: Vec<PeerInfo>,
        partition_map: Arc<PartitionMap>,
    ) -> Self {
        Self {
            local_node_id,
            peers,
            partition_map,
        }
    }

    /// This node's identity.
    #[must_use]
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Update the partition map (e.g., after a rebalance).
    pub fn set_partition_map(&mut self, map: Arc<PartitionMap>) {
        self.partition_map = map;
    }

    /// Update the peer list.
    pub fn set_peers(&mut self, peers: Vec<PeerInfo>) {
        self.peers = peers;
    }

    /// The current partition map epoch.
    #[must_use]
    pub fn partition_epoch(&self) -> u64 {
        self.partition_map.epoch()
    }

    /// Determine the execution strategy for a SQL query.
    ///
    /// Extracts table references from the SQL, looks up their partition
    /// assignments, and decides whether execution is local-only, fan-out,
    /// or broadcast.
    #[must_use]
    pub fn plan_execution(&self, sql: &str) -> ExecutionStrategy {
        let table_refs = extract_table_references(sql);
        if table_refs.is_empty() {
            // No FROM clause — e.g., `SELECT 1` — execute locally.
            return ExecutionStrategy::LocalOnly;
        }

        let source_names: Vec<&str> = table_refs.iter().map(String::as_str).collect();

        // Check if all partitions for all referenced sources are local.
        let all_local = source_names.iter().all(|s| {
            self.partition_map
                .all_local_for_source(s, &self.local_node_id)
        });

        if all_local {
            return ExecutionStrategy::LocalOnly;
        }

        // Find all distinct nodes that own partitions for the referenced sources.
        let target_nodes = self.partition_map.nodes_for_sources(&source_names);

        if target_nodes.is_empty() {
            // No partitions registered — treat as local (source might be empty).
            return ExecutionStrategy::LocalOnly;
        }

        ExecutionStrategy::FanOut { target_nodes }
    }

    /// Merge partial result batches from multiple nodes using simple
    /// concatenation.
    ///
    /// All batches must share the same schema. Empty batches are skipped.
    ///
    /// # Errors
    ///
    /// Returns `DbError::InvalidOperation` if schemas are incompatible.
    pub fn merge_concat(
        &self,
        partial_results: Vec<Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut merged = Vec::new();
        for batches in partial_results {
            for batch in batches {
                if batch.num_rows() > 0 {
                    merged.push(batch);
                }
            }
        }
        Ok(merged)
    }

    /// Merge partial aggregate results from multiple nodes.
    ///
    /// Given partial GROUP BY results from N nodes, this:
    /// 1. Concatenates all partial batches into one
    /// 2. Groups by the key columns
    /// 3. Merges each aggregate column according to its kind
    ///    (SUM of sums, SUM of counts, MIN of mins, MAX of maxes)
    ///
    /// # Errors
    ///
    /// Returns `DbError::InvalidOperation` if the merge strategy references
    /// columns that do not exist in the input batches.
    pub fn merge_partial_aggregates(
        &self,
        partial_results: Vec<Vec<RecordBatch>>,
        strategy: &MergeStrategy,
    ) -> Result<Vec<RecordBatch>, DbError> {
        match strategy {
            MergeStrategy::Concat => self.merge_concat(partial_results),
            MergeStrategy::PartialAggregate {
                group_key_indices,
                aggregates,
            } => {
                // Flatten all partials into one list.
                let all_batches: Vec<RecordBatch> = partial_results
                    .into_iter()
                    .flatten()
                    .filter(|b| b.num_rows() > 0)
                    .collect();

                if all_batches.is_empty() {
                    return Ok(Vec::new());
                }

                // Concatenate into a single batch for grouping.
                let schema = all_batches[0].schema();
                let concatenated = compute::concat_batches(&schema, &all_batches)
                    .map_err(|e| DbError::InvalidOperation(format!("concat error: {e}")))?;

                if concatenated.num_rows() == 0 {
                    return Ok(Vec::new());
                }

                merge_grouped_aggregates(&concatenated, group_key_indices, aggregates)
            }
        }
    }

    /// Execute a distributed query, collecting partial results from the
    /// provided per-node batches and merging them.
    ///
    /// This is the in-process gather path — each node's partial result is
    /// already available as `Vec<RecordBatch>`. In production, the fan-out
    /// would happen over RPC; this method handles the merge side.
    ///
    /// # Errors
    ///
    /// Returns `DbError::InvalidOperation` on merge failure.
    pub fn gather(
        &self,
        node_results: Vec<(NodeId, Vec<RecordBatch>, Duration)>,
        merge_strategy: &MergeStrategy,
    ) -> Result<GatherResult, DbError> {
        let mut stats = Vec::with_capacity(node_results.len());
        let mut partial_results = Vec::with_capacity(node_results.len());

        for (node_id, batches, elapsed) in node_results {
            let rows_returned: usize = batches.iter().map(RecordBatch::num_rows).sum();
            let batches_returned = batches.len();
            stats.push(NodeExecutionStats {
                node_id,
                elapsed,
                rows_returned,
                batches_returned,
            });
            partial_results.push(batches);
        }

        let strategy = if stats.len() <= 1 {
            ExecutionStrategy::LocalOnly
        } else {
            ExecutionStrategy::FanOut {
                target_nodes: stats.iter().map(|s| s.node_id).collect(),
            }
        };

        let batches = self.merge_partial_aggregates(partial_results, merge_strategy)?;

        Ok(GatherResult {
            batches,
            stats,
            strategy,
        })
    }
}

impl fmt::Debug for DistributedQueryCoordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedQueryCoordinator")
            .field("local_node_id", &self.local_node_id)
            .field("peers", &self.peers.len())
            .field("partition_epoch", &self.partition_map.epoch())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Internal: grouped aggregate merge
// ---------------------------------------------------------------------------

/// Build a key string from the group-key columns for the given row.
///
/// This is a simple approach suitable for moderate cardinality. For
/// high-cardinality production use, a hash-based approach would be better.
fn row_group_key(batch: &RecordBatch, row: usize, group_key_indices: &[usize]) -> String {
    let mut key = String::new();
    for (i, &col_idx) in group_key_indices.iter().enumerate() {
        if i > 0 {
            key.push('\x00');
        }
        let col = batch.column(col_idx);
        // Use the array's string representation for the value.
        let val = arrow::util::display::array_value_to_string(col, row);
        match val {
            Ok(s) => key.push_str(&s),
            Err(_) => key.push_str("NULL"),
        }
    }
    key
}

/// Merge pre-grouped partial aggregates into a final result.
///
/// Groups rows by key columns, then for each aggregate column applies
/// the corresponding merge function (sum-of-sums, sum-of-counts, etc.).
fn merge_grouped_aggregates(
    concatenated: &RecordBatch,
    group_key_indices: &[usize],
    aggregates: &[(usize, AggregateKind)],
) -> Result<Vec<RecordBatch>, DbError> {
    let num_rows = concatenated.num_rows();
    let schema = concatenated.schema();

    // Group rows by key.
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for row in 0..num_rows {
        let key = row_group_key(concatenated, row, group_key_indices);
        groups.entry(key).or_default().push(row);
    }

    let num_groups = groups.len();
    if num_groups == 0 {
        return Ok(Vec::new());
    }

    // For each output column, build an array.
    let num_cols = schema.fields().len();
    let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    // Collect groups into a stable order.
    let group_entries: Vec<(String, Vec<usize>)> = groups.into_iter().collect();

    for col_idx in 0..num_cols {
        let col = concatenated.column(col_idx);

        // Is this column an aggregate column?
        let agg_spec = aggregates.iter().find(|(idx, _)| *idx == col_idx);

        if let Some((_, kind)) = agg_spec {
            output_columns.push(merge_agg_column(col, &group_entries, *kind)?);
        } else {
            // Group key or passthrough — take the first row from each group.
            let indices: Vec<usize> = group_entries.iter().map(|(_, rows)| rows[0]).collect();
            let idx_array =
                UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
            let taken = compute::take(col, &idx_array, None)
                .map_err(|e| DbError::InvalidOperation(format!("take error: {e}")))?;
            output_columns.push(taken);
        }
    }

    let result = RecordBatch::try_new(schema, output_columns)
        .map_err(|e| DbError::InvalidOperation(format!("batch build error: {e}")))?;

    Ok(vec![result])
}

/// Merge a single aggregate column across groups.
fn merge_agg_column(
    col: &dyn Array,
    groups: &[(String, Vec<usize>)],
    kind: AggregateKind,
) -> Result<ArrayRef, DbError> {
    // Try f64 first, then i64 — covers most numeric aggregates.
    if let Some(f64_arr) = col.as_any().downcast_ref::<Float64Array>() {
        let values: Vec<Option<f64>> = groups
            .iter()
            .map(|(_, rows)| merge_f64_group(f64_arr, rows, kind))
            .collect();
        return Ok(Arc::new(Float64Array::from(values)));
    }

    if let Some(i64_arr) = col.as_any().downcast_ref::<Int64Array>() {
        let values: Vec<Option<i64>> = groups
            .iter()
            .map(|(_, rows)| merge_i64_group(i64_arr, rows, kind))
            .collect();
        return Ok(Arc::new(Int64Array::from(values)));
    }

    if let Some(u64_arr) = col.as_any().downcast_ref::<UInt64Array>() {
        let values: Vec<Option<u64>> = groups
            .iter()
            .map(|(_, rows)| merge_u64_group(u64_arr, rows, kind))
            .collect();
        return Ok(Arc::new(UInt64Array::from(values)));
    }

    Err(DbError::InvalidOperation(format!(
        "unsupported data type for {kind} aggregate merge: {:?}",
        col.data_type()
    )))
}

fn merge_f64_group(arr: &Float64Array, rows: &[usize], kind: AggregateKind) -> Option<f64> {
    let mut result: Option<f64> = None;
    for &row in rows {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row);
        result = Some(match (result, kind) {
            (None, _) => v,
            (Some(acc), AggregateKind::Sum | AggregateKind::Count) => acc + v,
            (Some(acc), AggregateKind::Min) => acc.min(v),
            (Some(acc), AggregateKind::Max) => acc.max(v),
        });
    }
    result
}

fn merge_i64_group(arr: &Int64Array, rows: &[usize], kind: AggregateKind) -> Option<i64> {
    let mut result: Option<i64> = None;
    for &row in rows {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row);
        result = Some(match (result, kind) {
            (None, _) => v,
            (Some(acc), AggregateKind::Sum | AggregateKind::Count) => acc.saturating_add(v),
            (Some(acc), AggregateKind::Min) => acc.min(v),
            (Some(acc), AggregateKind::Max) => acc.max(v),
        });
    }
    result
}

fn merge_u64_group(arr: &UInt64Array, rows: &[usize], kind: AggregateKind) -> Option<u64> {
    let mut result: Option<u64> = None;
    for &row in rows {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row);
        result = Some(match (result, kind) {
            (None, _) => v,
            (Some(acc), AggregateKind::Sum | AggregateKind::Count) => acc.saturating_add(v),
            (Some(acc), AggregateKind::Min) => acc.min(v),
            (Some(acc), AggregateKind::Max) => acc.max(v),
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::super::partition_map::NodeAssignment;
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_partition_map(assignments: Vec<(u32, u64, &str)>) -> Arc<PartitionMap> {
        let mut map = PartitionMap::new();
        for (pid, node, source) in assignments {
            map.insert(
                pid,
                NodeAssignment {
                    node_id: NodeId(node),
                    address: format!("127.0.0.1:{}", 9000 + node),
                    source_name: source.to_string(),
                },
            );
        }
        Arc::new(map)
    }

    fn make_coordinator(
        local: u64,
        assignments: Vec<(u32, u64, &str)>,
    ) -> DistributedQueryCoordinator {
        DistributedQueryCoordinator::new(NodeId(local), vec![], make_partition_map(assignments))
    }

    // -----------------------------------------------------------------------
    // Execution strategy tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_local_only_all_partitions_local() {
        let coord = make_coordinator(1, vec![(0, 1, "trades"), (1, 1, "trades")]);
        let strategy = coord.plan_execution("SELECT * FROM trades");
        assert_eq!(strategy, ExecutionStrategy::LocalOnly);
    }

    #[test]
    fn test_fan_out_partitions_on_multiple_nodes() {
        let coord = make_coordinator(1, vec![(0, 1, "trades"), (1, 2, "trades")]);
        let strategy = coord.plan_execution("SELECT * FROM trades");
        match strategy {
            ExecutionStrategy::FanOut { target_nodes } => {
                assert_eq!(target_nodes.len(), 2);
            }
            other => panic!("expected FanOut, got {other}"),
        }
    }

    #[test]
    fn test_local_only_no_from_clause() {
        let coord = make_coordinator(1, vec![]);
        let strategy = coord.plan_execution("SELECT 1");
        assert_eq!(strategy, ExecutionStrategy::LocalOnly);
    }

    #[test]
    fn test_local_only_unknown_source() {
        let coord = make_coordinator(1, vec![(0, 1, "trades")]);
        // Query references a source not in the partition map.
        let strategy = coord.plan_execution("SELECT * FROM unknown_table");
        assert_eq!(strategy, ExecutionStrategy::LocalOnly);
    }

    #[test]
    fn test_fan_out_join_across_nodes() {
        let coord = make_coordinator(1, vec![(0, 1, "trades"), (1, 2, "orders")]);
        let strategy = coord.plan_execution(
            "SELECT t.symbol, o.qty FROM trades t JOIN orders o ON t.id = o.trade_id",
        );
        match strategy {
            ExecutionStrategy::FanOut { target_nodes } => {
                assert_eq!(target_nodes.len(), 2);
            }
            other => panic!("expected FanOut, got {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // Merge tests
    // -----------------------------------------------------------------------

    fn make_agg_batch(symbols: Vec<&str>, sums: Vec<f64>, counts: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("total_volume", DataType::Float64, false),
            Field::new("trade_count", DataType::Int64, false),
        ]));

        let symbol_arr: ArrayRef = Arc::new(arrow::array::StringArray::from(symbols));
        let sum_arr: ArrayRef = Arc::new(Float64Array::from(sums));
        let count_arr: ArrayRef = Arc::new(Int64Array::from(counts));

        RecordBatch::try_new(schema, vec![symbol_arr, sum_arr, count_arr]).unwrap()
    }

    #[test]
    fn test_merge_concat() {
        let coord = make_coordinator(1, vec![]);

        let b1 = make_agg_batch(vec!["AAPL"], vec![100.0], vec![10]);
        let b2 = make_agg_batch(vec!["GOOG"], vec![200.0], vec![20]);

        let result = coord.merge_concat(vec![vec![b1], vec![b2]]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[1].num_rows(), 1);
    }

    #[test]
    fn test_merge_concat_skips_empty() {
        let coord = make_coordinator(1, vec![]);

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let empty = RecordBatch::new_empty(schema);
        let non_empty = make_agg_batch(vec!["A"], vec![1.0], vec![1]);

        let result = coord
            .merge_concat(vec![vec![empty], vec![non_empty]])
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_merge_partial_aggregates_sum_count() {
        let coord = make_coordinator(1, vec![]);

        // Node 1 partial: AAPL sum=100, count=10; GOOG sum=200, count=20
        let b1 = make_agg_batch(vec!["AAPL", "GOOG"], vec![100.0, 200.0], vec![10, 20]);
        // Node 2 partial: AAPL sum=50, count=5; GOOG sum=300, count=30
        let b2 = make_agg_batch(vec!["AAPL", "GOOG"], vec![50.0, 300.0], vec![5, 30]);

        let strategy = MergeStrategy::PartialAggregate {
            group_key_indices: vec![0], // symbol
            aggregates: vec![
                (1, AggregateKind::Sum),   // total_volume
                (2, AggregateKind::Count), // trade_count
            ],
        };

        let result = coord
            .merge_partial_aggregates(vec![vec![b1], vec![b2]], &strategy)
            .unwrap();
        assert_eq!(result.len(), 1);

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);

        // Extract values and sort by symbol for deterministic assertions.
        let symbols: Vec<String> = (0..batch.num_rows())
            .map(|i| arrow::util::display::array_value_to_string(batch.column(0), i).unwrap())
            .collect();
        let sums: &Float64Array = batch.column(1).as_any().downcast_ref().unwrap();
        let counts: &Int64Array = batch.column(2).as_any().downcast_ref().unwrap();

        // Find the row for AAPL and GOOG.
        for i in 0..batch.num_rows() {
            if symbols[i] == "AAPL" {
                assert!(
                    (sums.value(i) - 150.0).abs() < f64::EPSILON,
                    "AAPL sum: {}",
                    sums.value(i)
                );
                assert_eq!(counts.value(i), 15, "AAPL count");
            } else if symbols[i] == "GOOG" {
                assert!(
                    (sums.value(i) - 500.0).abs() < f64::EPSILON,
                    "GOOG sum: {}",
                    sums.value(i)
                );
                assert_eq!(counts.value(i), 50, "GOOG count");
            }
        }
    }

    #[test]
    fn test_merge_partial_aggregates_min_max() {
        let coord = make_coordinator(1, vec![]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("min_price", DataType::Float64, false),
            Field::new("max_price", DataType::Float64, false),
        ]));

        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
                Arc::new(Float64Array::from(vec![150.0])),
                Arc::new(Float64Array::from(vec![155.0])),
            ],
        )
        .unwrap();

        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
                Arc::new(Float64Array::from(vec![148.0])),
                Arc::new(Float64Array::from(vec![160.0])),
            ],
        )
        .unwrap();

        let strategy = MergeStrategy::PartialAggregate {
            group_key_indices: vec![0],
            aggregates: vec![(1, AggregateKind::Min), (2, AggregateKind::Max)],
        };

        let result = coord
            .merge_partial_aggregates(vec![vec![b1], vec![b2]], &strategy)
            .unwrap();
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);

        let mins: &Float64Array = batch.column(1).as_any().downcast_ref().unwrap();
        let maxes: &Float64Array = batch.column(2).as_any().downcast_ref().unwrap();
        assert!((mins.value(0) - 148.0).abs() < f64::EPSILON);
        assert!((maxes.value(0) - 160.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_merge_empty_results() {
        let coord = make_coordinator(1, vec![]);
        let strategy = MergeStrategy::PartialAggregate {
            group_key_indices: vec![0],
            aggregates: vec![(1, AggregateKind::Sum)],
        };
        let result = coord
            .merge_partial_aggregates(vec![vec![], vec![]], &strategy)
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_gather() {
        let coord = make_coordinator(1, vec![]);
        let b1 = make_agg_batch(vec!["AAPL"], vec![100.0], vec![10]);
        let b2 = make_agg_batch(vec!["GOOG"], vec![200.0], vec![20]);

        let node_results = vec![
            (NodeId(1), vec![b1], Duration::from_millis(5)),
            (NodeId(2), vec![b2], Duration::from_millis(8)),
        ];

        let result = coord.gather(node_results, &MergeStrategy::Concat).unwrap();

        assert_eq!(result.batches.len(), 2);
        assert_eq!(result.stats.len(), 2);
        assert_eq!(result.total_rows(), 2);
        assert_eq!(result.total_rows_pre_merge(), 2);
        assert!(matches!(result.strategy, ExecutionStrategy::FanOut { .. }));
    }

    #[test]
    fn test_gather_single_node_is_local() {
        let coord = make_coordinator(1, vec![]);
        let b1 = make_agg_batch(vec!["AAPL"], vec![100.0], vec![10]);

        let node_results = vec![(NodeId(1), vec![b1], Duration::from_millis(5))];

        let result = coord.gather(node_results, &MergeStrategy::Concat).unwrap();
        assert_eq!(result.strategy, ExecutionStrategy::LocalOnly);
    }

    #[test]
    fn test_execution_strategy_display() {
        assert_eq!(ExecutionStrategy::LocalOnly.to_string(), "local-only");
        assert_eq!(ExecutionStrategy::Broadcast.to_string(), "broadcast");
        assert_eq!(
            ExecutionStrategy::FanOut {
                target_nodes: vec![NodeId(1), NodeId(2)]
            }
            .to_string(),
            "fan-out(2 nodes)"
        );
    }

    #[test]
    fn test_coordinator_debug() {
        let coord = make_coordinator(1, vec![]);
        let debug = format!("{coord:?}");
        assert!(debug.contains("DistributedQueryCoordinator"));
        assert!(debug.contains("NodeId(1)"));
    }

    #[test]
    fn test_set_partition_map() {
        let mut coord = make_coordinator(1, vec![]);
        assert_eq!(coord.partition_epoch(), 0);

        let mut new_map = PartitionMap::new();
        new_map.set_epoch(42);
        coord.set_partition_map(Arc::new(new_map));
        assert_eq!(coord.partition_epoch(), 42);
    }

    #[test]
    fn test_set_peers() {
        let mut coord = make_coordinator(1, vec![]);
        coord.set_peers(vec![PeerInfo {
            node_id: NodeId(2),
            address: "127.0.0.1:9002".into(),
        }]);
        // Just verify it doesn't panic.
        assert_eq!(coord.local_node_id(), NodeId(1));
    }
}
