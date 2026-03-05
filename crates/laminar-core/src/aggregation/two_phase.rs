//! Two-phase cross-partition aggregation.
//!
//! # Architecture
//!
//! ```text
//! Partition 0: COUNT(*) GROUP BY symbol → partial(AAPL: 500)
//! Partition 1: COUNT(*) GROUP BY symbol → partial(AAPL: 300)  → Ring 2 merge → final(AAPL: 800)
//! Partition 2: COUNT(*) GROUP BY symbol → partial(AAPL: 200)
//! ```
//!
//! Phase 1 (Partial): Each partition computes local aggregates and produces
//! `PartialAggregate` entries — one per group key per partition.
//!
//! Phase 2 (Merge): A coordinator collects partials from all partitions,
//! merges them via `MergeAggregator`, and produces final results.
//!
//! ## Supported Functions
//!
//! | Function | Partial State | Merge Logic |
//! |----------|--------------|-------------|
//! | COUNT | `count: i64` | sum of counts |
//! | SUM | `sum: f64` | sum of sums |
//! | AVG | `sum: f64, count: i64` | total\_sum / total\_count |
//! | MIN | `min: Option<f64>` | min of mins |
//! | MAX | `max: Option<f64>` | max of maxes |
//! | APPROX\_DISTINCT | HLL sketch bytes | HLL union |
//!
//! ## Arrow IPC
//!
//! Partial results can be shipped between nodes as Arrow IPC-encoded
//! `RecordBatch`es via `encode_batch_to_ipc` / `decode_batch_from_ipc`.

use std::io::Cursor;

use rustc_hash::FxHashMap;

use arrow::ipc;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::CrossPartitionAggregateStore;

// ── Errors ──────────────────────────────────────────────────────────

/// Errors from two-phase aggregation.
#[derive(Debug, thiserror::Error)]
pub enum TwoPhaseError {
    /// Arrow error during IPC encoding or decoding.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Serialization or deserialization error.
    #[error("serialization error: {0}")]
    Serde(String),

    /// Mismatched function count during merge.
    #[error("function count mismatch: expected {expected}, got {actual}")]
    FunctionCountMismatch {
        /// Expected number of functions.
        expected: usize,
        /// Actual number of functions found.
        actual: usize,
    },

    /// Unknown aggregate function name.
    #[error("unsupported two-phase function: {0}")]
    UnsupportedFunction(String),
}

// ── TwoPhaseKind ────────────────────────────────────────────────────

/// Aggregate function kind that supports two-phase execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TwoPhaseKind {
    /// COUNT aggregate.
    Count,
    /// SUM aggregate.
    Sum,
    /// AVG aggregate (carried as sum + count).
    Avg,
    /// MIN aggregate.
    Min,
    /// MAX aggregate.
    Max,
    /// Approximate distinct count via `HyperLogLog`.
    ApproxDistinct,
}

impl TwoPhaseKind {
    /// Resolve a function kind from its SQL name.
    ///
    /// # Examples
    ///
    /// ```
    /// use laminar_core::aggregation::two_phase::TwoPhaseKind;
    ///
    /// assert_eq!(TwoPhaseKind::from_name("COUNT"), Some(TwoPhaseKind::Count));
    /// assert_eq!(TwoPhaseKind::from_name("avg"), Some(TwoPhaseKind::Avg));
    /// assert_eq!(TwoPhaseKind::from_name("MEDIAN"), None);
    /// ```
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "COUNT" => Some(Self::Count),
            "SUM" => Some(Self::Sum),
            "AVG" => Some(Self::Avg),
            "MIN" => Some(Self::Min),
            "MAX" => Some(Self::Max),
            "APPROX_COUNT_DISTINCT" | "APPROX_DISTINCT" => Some(Self::ApproxDistinct),
            _ => None,
        }
    }
}

// ── PartialState ────────────────────────────────────────────────────

/// Intermediate partial state for a single aggregate function.
///
/// Each variant carries the minimum state needed for the merge step
/// to produce a correct final result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartialState {
    /// Partial count.
    Count(i64),
    /// Partial sum.
    Sum(f64),
    /// Partial average as (running sum, running count).
    Avg {
        /// Running sum.
        sum: f64,
        /// Running count.
        count: i64,
    },
    /// Partial minimum.
    Min(Option<f64>),
    /// Partial maximum.
    Max(Option<f64>),
    /// `HyperLogLog` sketch bytes for approximate distinct count.
    ApproxDistinct(Vec<u8>),
}

impl PartialState {
    /// Create an empty state for the given function kind.
    #[must_use]
    pub fn empty(kind: TwoPhaseKind) -> Self {
        match kind {
            TwoPhaseKind::Count => Self::Count(0),
            TwoPhaseKind::Sum => Self::Sum(0.0),
            TwoPhaseKind::Avg => Self::Avg { sum: 0.0, count: 0 },
            TwoPhaseKind::Min => Self::Min(None),
            TwoPhaseKind::Max => Self::Max(None),
            TwoPhaseKind::ApproxDistinct => Self::ApproxDistinct(HllSketch::new().to_bytes()),
        }
    }

    /// Merge another partial state into this one (in place).
    ///
    /// Type-mismatched merges are silently ignored.
    pub fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => *a += b,
            (Self::Sum(a), Self::Sum(b)) => *a += b,
            (Self::Avg { sum: s1, count: c1 }, Self::Avg { sum: s2, count: c2 }) => {
                *s1 += s2;
                *c1 += c2;
            }
            (Self::Min(a), Self::Min(b)) => {
                *a = match (*a, *b) {
                    (None, v) | (v, None) => v,
                    (Some(x), Some(y)) => Some(x.min(y)),
                };
            }
            (Self::Max(a), Self::Max(b)) => {
                *a = match (*a, *b) {
                    (None, v) | (v, None) => v,
                    (Some(x), Some(y)) => Some(x.max(y)),
                };
            }
            (Self::ApproxDistinct(a), Self::ApproxDistinct(b)) => {
                if let (Ok(mut hll_a), Ok(hll_b)) =
                    (HllSketch::from_bytes(a), HllSketch::from_bytes(b))
                {
                    hll_a.merge(&hll_b);
                    *a = hll_a.to_bytes();
                }
            }
            _ => {} // type mismatch — no-op
        }
    }

    /// Finalize the partial state to an `f64` result.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn finalize(&self) -> f64 {
        match self {
            Self::Count(n) => *n as f64,
            Self::Sum(s) => *s,
            Self::Avg { sum, count } => {
                if *count > 0 {
                    sum / (*count as f64)
                } else {
                    0.0
                }
            }
            Self::Min(v) | Self::Max(v) => v.unwrap_or(f64::NAN),
            Self::ApproxDistinct(bytes) => {
                HllSketch::from_bytes(bytes).map_or(0.0, |h| h.estimate())
            }
        }
    }
}

// ── PartialAggregate ────────────────────────────────────────────────

/// A partial aggregate entry for one group from one partition.
///
/// Contains the serialized group key, source partition ID,
/// one [`PartialState`] per aggregate function, and metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartialAggregate {
    /// Serialized group key.
    pub group_key: Vec<u8>,
    /// Source partition.
    pub partition_id: u32,
    /// One partial state per aggregate function.
    pub states: Vec<PartialState>,
    /// Watermark at computation time (millis since epoch).
    pub watermark_ms: i64,
    /// Epoch at computation time.
    pub epoch: u64,
}

// ── MergeAggregator ─────────────────────────────────────────────────

/// Combines partial aggregates from multiple partitions into final results.
///
/// The merge step runs on Ring 2 (control plane). It groups partials by
/// group key and merges each group's partial states into a single combined
/// state per function.
pub struct MergeAggregator {
    kinds: Vec<TwoPhaseKind>,
}

impl MergeAggregator {
    /// Create a new merge aggregator for the given function kinds.
    #[must_use]
    pub fn new(kinds: Vec<TwoPhaseKind>) -> Self {
        Self { kinds }
    }

    /// Merge partials for a single group key.
    ///
    /// # Errors
    ///
    /// Returns [`TwoPhaseError::FunctionCountMismatch`] if any partial
    /// has a different number of states than expected.
    pub fn merge_group(
        &self,
        partials: &[&PartialAggregate],
    ) -> Result<Vec<PartialState>, TwoPhaseError> {
        let mut merged: Vec<PartialState> =
            self.kinds.iter().map(|k| PartialState::empty(*k)).collect();

        for partial in partials {
            if partial.states.len() != self.kinds.len() {
                return Err(TwoPhaseError::FunctionCountMismatch {
                    expected: self.kinds.len(),
                    actual: partial.states.len(),
                });
            }
            for (target, source) in merged.iter_mut().zip(&partial.states) {
                target.merge(source);
            }
        }

        Ok(merged)
    }

    /// Merge all partials into per-group final states.
    ///
    /// Groups the partials by `group_key`, merges each group, and returns
    /// a map of `group_key → merged_states`.
    ///
    /// # Errors
    ///
    /// Returns [`TwoPhaseError::FunctionCountMismatch`] if any partial
    /// has a mismatched number of states.
    pub fn merge_all(
        &self,
        partials: &[PartialAggregate],
    ) -> Result<FxHashMap<Vec<u8>, Vec<PartialState>>, TwoPhaseError> {
        let mut by_group: FxHashMap<&[u8], Vec<&PartialAggregate>> = FxHashMap::default();
        for partial in partials {
            by_group
                .entry(&partial.group_key)
                .or_default()
                .push(partial);
        }

        let mut result = FxHashMap::with_capacity_and_hasher(by_group.len(), rustc_hash::FxBuildHasher);
        for (key, group_partials) in by_group {
            let merged = self.merge_group(&group_partials)?;
            result.insert(key.to_vec(), merged);
        }

        Ok(result)
    }

    /// Finalize a vector of merged partial states to `f64` results.
    #[must_use]
    pub fn finalize(states: &[PartialState]) -> Vec<f64> {
        states.iter().map(PartialState::finalize).collect()
    }

    /// Number of aggregate functions.
    #[must_use]
    pub fn num_functions(&self) -> usize {
        self.kinds.len()
    }

    /// The function kinds in order.
    #[must_use]
    pub fn kinds(&self) -> &[TwoPhaseKind] {
        &self.kinds
    }
}

// ── Store Integration ───────────────────────────────────────────────

/// Publish partial aggregates to a [`CrossPartitionAggregateStore`].
///
/// Each partial is serialized to JSON and published under its
/// `(group_key, partition_id)`.
///
/// # Errors
///
/// Returns [`TwoPhaseError::Serde`] if serialization fails.
pub fn publish_partials(
    store: &CrossPartitionAggregateStore,
    partials: &[PartialAggregate],
) -> Result<(), TwoPhaseError> {
    for partial in partials {
        let serialized =
            serde_json::to_vec(partial).map_err(|e| TwoPhaseError::Serde(e.to_string()))?;
        store.publish(
            Bytes::copy_from_slice(&partial.group_key),
            partial.partition_id,
            Bytes::from(serialized),
        );
    }
    Ok(())
}

/// Collect and deserialize partials for a group key from a
/// [`CrossPartitionAggregateStore`].
#[must_use]
pub fn collect_partials(
    store: &CrossPartitionAggregateStore,
    group_key: &[u8],
) -> Vec<PartialAggregate> {
    store
        .collect_partials(group_key)
        .into_iter()
        .filter_map(|(_pid, bytes)| serde_json::from_slice(&bytes).ok())
        .collect()
}

// ── Arrow IPC ───────────────────────────────────────────────────────

/// Encode a `RecordBatch` to Arrow IPC file format bytes.
///
/// # Errors
///
/// Returns [`TwoPhaseError::Arrow`] on encoding failure.
pub fn encode_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, TwoPhaseError> {
    let mut buf = Vec::new();
    {
        let mut writer = ipc::writer::FileWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Decode a `RecordBatch` from Arrow IPC file format bytes.
///
/// # Errors
///
/// Returns [`TwoPhaseError::Arrow`] on decoding failure, or
/// [`TwoPhaseError::Serde`] if the IPC stream contains no batches.
pub fn decode_batch_from_ipc(bytes: &[u8]) -> Result<RecordBatch, TwoPhaseError> {
    let cursor = Cursor::new(bytes);
    let mut reader = ipc::reader::FileReader::try_new(cursor, None)?;
    reader
        .next()
        .ok_or_else(|| TwoPhaseError::Serde("empty IPC stream".into()))?
        .map_err(TwoPhaseError::Arrow)
}

/// Serialize partial aggregates to JSON bytes.
///
/// # Errors
///
/// Returns [`TwoPhaseError::Serde`] on serialization failure.
pub fn serialize_partials(partials: &[PartialAggregate]) -> Result<Vec<u8>, TwoPhaseError> {
    serde_json::to_vec(partials).map_err(|e| TwoPhaseError::Serde(e.to_string()))
}

/// Deserialize partial aggregates from JSON bytes.
///
/// # Errors
///
/// Returns [`TwoPhaseError::Serde`] on deserialization failure.
pub fn deserialize_partials(bytes: &[u8]) -> Result<Vec<PartialAggregate>, TwoPhaseError> {
    serde_json::from_slice(bytes).map_err(|e| TwoPhaseError::Serde(e.to_string()))
}

// ── Detection ───────────────────────────────────────────────────────

/// Check if all functions in a query support two-phase execution.
///
/// Returns `false` for empty input or if any function is not two-phase
/// compatible.
///
/// # Examples
///
/// ```
/// use laminar_core::aggregation::two_phase::can_use_two_phase;
///
/// assert!(can_use_two_phase(&["COUNT", "SUM", "AVG"]));
/// assert!(!can_use_two_phase(&["COUNT", "MEDIAN"]));
/// assert!(!can_use_two_phase(&[]));
/// ```
#[must_use]
pub fn can_use_two_phase(function_names: &[&str]) -> bool {
    !function_names.is_empty()
        && function_names
            .iter()
            .all(|n| TwoPhaseKind::from_name(n).is_some())
}

// ── HyperLogLog Sketch ─────────────────────────────────────────────

/// Minimal `HyperLogLog` sketch for approximate distinct counting.
///
/// Uses `2^precision` registers. Default precision is 8 (256 registers,
/// ~256 bytes of memory, ~2% standard error).
///
/// ## Wire Format
///
/// `[precision: u8][registers: u8 * (2^precision)]`
#[derive(Debug, Clone)]
pub struct HllSketch {
    registers: Vec<u8>,
    precision: u8,
}

const DEFAULT_HLL_PRECISION: u8 = 8;

impl HllSketch {
    /// Create a new empty sketch with default precision (8).
    #[must_use]
    pub fn new() -> Self {
        Self::with_precision(DEFAULT_HLL_PRECISION)
    }

    /// Create a new empty sketch with the given precision.
    ///
    /// Allocates `2^precision` registers.
    ///
    /// # Panics
    ///
    /// Panics if `precision` is not in the range 4..=18.
    #[must_use]
    pub fn with_precision(precision: u8) -> Self {
        assert!(
            (4..=18).contains(&precision),
            "HLL precision must be 4..=18, got {precision}"
        );
        let num_registers = 1usize << precision;
        Self {
            registers: vec![0; num_registers],
            precision,
        }
    }

    /// Add a pre-hashed 64-bit value to the sketch.
    #[allow(clippy::cast_possible_truncation)]
    pub fn add_hash(&mut self, hash: u64) {
        let p = u32::from(self.precision);
        let idx = (hash >> (64 - p)) as usize;
        let w = hash << p;
        let rho = if w == 0 {
            64 - p + 1
        } else {
            w.leading_zeros() + 1
        } as u8;
        if rho > self.registers[idx] {
            self.registers[idx] = rho;
        }
    }

    /// Merge another sketch into this one (HLL union).
    ///
    /// # Panics
    ///
    /// Panics if the two sketches have different precisions.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            self.precision, other.precision,
            "HLL precision mismatch: {} vs {}",
            self.precision, other.precision
        );
        for (a, &b) in self.registers.iter_mut().zip(&other.registers) {
            *a = (*a).max(b);
        }
    }

    /// Estimate the cardinality (number of distinct elements).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha = match self.precision {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        let harmonic_sum: f64 = self
            .registers
            .iter()
            .map(|&r| 2.0_f64.powi(-i32::from(r)))
            .sum();

        let raw_estimate = alpha * m * m / harmonic_sum;

        // Small-range correction (linear counting)
        if raw_estimate <= 2.5 * m {
            #[allow(clippy::naive_bytecount)] // few registers; no bytecount dep needed
            let zeros = self.registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                m * (m / zeros).ln()
            } else {
                raw_estimate
            }
        } else {
            raw_estimate
        }
    }

    /// Serialize the sketch to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + self.registers.len());
        buf.push(self.precision);
        buf.extend_from_slice(&self.registers);
        buf
    }

    /// Deserialize a sketch from bytes.
    ///
    /// # Errors
    ///
    /// Returns [`TwoPhaseError::Serde`] if the bytes are malformed.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TwoPhaseError> {
        if bytes.is_empty() {
            return Err(TwoPhaseError::Serde("empty HLL bytes".into()));
        }
        let precision = bytes[0];
        if !(4..=18).contains(&precision) {
            return Err(TwoPhaseError::Serde(format!(
                "HLL precision {precision} out of range 4..=18"
            )));
        }
        let expected_len = 1 + (1usize << precision);
        if bytes.len() != expected_len {
            return Err(TwoPhaseError::Serde(format!(
                "HLL bytes length mismatch: expected {expected_len}, got {}",
                bytes.len()
            )));
        }
        Ok(Self {
            precision,
            registers: bytes[1..].to_vec(),
        })
    }

    /// Number of registers (`2^precision`).
    #[must_use]
    pub fn num_registers(&self) -> usize {
        self.registers.len()
    }

    /// Precision (log2 of register count).
    #[must_use]
    pub fn precision(&self) -> u8 {
        self.precision
    }
}

impl Default for HllSketch {
    fn default() -> Self {
        Self::new()
    }
}

// ── Conversion: PartialState ↔ AggregateState ──────────────────────

#[cfg(feature = "delta")]
mod delta_bridge {
    use super::PartialState;
    use crate::aggregation::gossip_aggregates::AggregateState;

    impl PartialState {
        /// Convert to the gossip [`AggregateState`] for cluster replication.
        #[must_use]
        pub fn to_aggregate_state(&self) -> AggregateState {
            match self {
                Self::Count(n) => AggregateState::Count(*n),
                Self::Sum(s) => AggregateState::Sum(*s),
                Self::Avg { sum, count } => AggregateState::Avg {
                    sum: *sum,
                    count: *count,
                },
                Self::Min(v) => AggregateState::Min(v.unwrap_or(f64::NAN)),
                Self::Max(v) => AggregateState::Max(v.unwrap_or(f64::NAN)),
                Self::ApproxDistinct(bytes) => AggregateState::Custom(bytes.clone()),
            }
        }

        /// Convert from a gossip [`AggregateState`].
        #[must_use]
        pub fn from_aggregate_state(state: &AggregateState) -> Self {
            match state {
                AggregateState::Count(n) => Self::Count(*n),
                AggregateState::Sum(s) => Self::Sum(*s),
                AggregateState::Avg { sum, count } => Self::Avg {
                    sum: *sum,
                    count: *count,
                },
                AggregateState::Min(v) => Self::Min(if v.is_nan() { None } else { Some(*v) }),
                AggregateState::Max(v) => Self::Max(if v.is_nan() { None } else { Some(*v) }),
                AggregateState::Custom(bytes) => Self::ApproxDistinct(bytes.clone()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // ── TwoPhaseKind ────────────────────────────────────────────────

    #[test]
    fn test_kind_from_name() {
        assert_eq!(TwoPhaseKind::from_name("COUNT"), Some(TwoPhaseKind::Count));
        assert_eq!(TwoPhaseKind::from_name("sum"), Some(TwoPhaseKind::Sum));
        assert_eq!(TwoPhaseKind::from_name("Avg"), Some(TwoPhaseKind::Avg));
        assert_eq!(TwoPhaseKind::from_name("MIN"), Some(TwoPhaseKind::Min));
        assert_eq!(TwoPhaseKind::from_name("max"), Some(TwoPhaseKind::Max));
        assert_eq!(
            TwoPhaseKind::from_name("APPROX_COUNT_DISTINCT"),
            Some(TwoPhaseKind::ApproxDistinct)
        );
        assert_eq!(
            TwoPhaseKind::from_name("APPROX_DISTINCT"),
            Some(TwoPhaseKind::ApproxDistinct)
        );
        assert_eq!(TwoPhaseKind::from_name("MEDIAN"), None);
        assert_eq!(TwoPhaseKind::from_name(""), None);
    }

    #[test]
    fn test_can_use_two_phase() {
        assert!(can_use_two_phase(&["COUNT", "SUM"]));
        assert!(can_use_two_phase(&["AVG"]));
        assert!(can_use_two_phase(&["MIN", "MAX", "COUNT"]));
        assert!(!can_use_two_phase(&["COUNT", "MEDIAN"]));
        assert!(!can_use_two_phase(&[]));
    }

    // ── PartialState merge ──────────────────────────────────────────

    #[test]
    fn test_merge_count() {
        let mut a = PartialState::Count(10);
        a.merge(&PartialState::Count(5));
        assert_eq!(a, PartialState::Count(15));
    }

    #[test]
    fn test_merge_sum() {
        let mut a = PartialState::Sum(1.5);
        a.merge(&PartialState::Sum(2.5));
        assert_eq!(a, PartialState::Sum(4.0));
    }

    #[test]
    fn test_merge_avg() {
        let mut a = PartialState::Avg {
            sum: 10.0,
            count: 2,
        };
        a.merge(&PartialState::Avg {
            sum: 20.0,
            count: 3,
        });
        match a {
            PartialState::Avg { sum, count } => {
                assert!((sum - 30.0).abs() < f64::EPSILON);
                assert_eq!(count, 5);
            }
            _ => panic!("expected Avg"),
        }
    }

    #[test]
    fn test_merge_min() {
        let mut a = PartialState::Min(Some(10.0));
        a.merge(&PartialState::Min(Some(5.0)));
        assert_eq!(a, PartialState::Min(Some(5.0)));

        // None + Some = Some
        let mut b = PartialState::Min(None);
        b.merge(&PartialState::Min(Some(3.0)));
        assert_eq!(b, PartialState::Min(Some(3.0)));

        // Some + None = Some
        let mut c = PartialState::Min(Some(7.0));
        c.merge(&PartialState::Min(None));
        assert_eq!(c, PartialState::Min(Some(7.0)));

        // None + None = None
        let mut d = PartialState::Min(None);
        d.merge(&PartialState::Min(None));
        assert_eq!(d, PartialState::Min(None));
    }

    #[test]
    fn test_merge_max() {
        let mut a = PartialState::Max(Some(5.0));
        a.merge(&PartialState::Max(Some(10.0)));
        assert_eq!(a, PartialState::Max(Some(10.0)));

        let mut b = PartialState::Max(None);
        b.merge(&PartialState::Max(Some(3.0)));
        assert_eq!(b, PartialState::Max(Some(3.0)));
    }

    #[test]
    fn test_merge_type_mismatch_noop() {
        let mut a = PartialState::Count(10);
        a.merge(&PartialState::Sum(5.0));
        assert_eq!(a, PartialState::Count(10));
    }

    // ── PartialState finalize ───────────────────────────────────────

    #[test]
    fn test_finalize_count() {
        assert!((PartialState::Count(42).finalize() - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_finalize_avg() {
        let avg = PartialState::Avg {
            sum: 10.0,
            count: 4,
        };
        assert!((avg.finalize() - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_finalize_avg_zero_count() {
        let avg = PartialState::Avg { sum: 0.0, count: 0 };
        assert!((avg.finalize()).abs() < f64::EPSILON);
    }

    #[test]
    fn test_finalize_min_none() {
        assert!(PartialState::Min(None).finalize().is_nan());
    }

    #[test]
    fn test_finalize_min_some() {
        assert!((PartialState::Min(Some(3.25)).finalize() - 3.25).abs() < f64::EPSILON);
    }

    // ── MergeAggregator ─────────────────────────────────────────────

    #[test]
    fn test_merge_single_group_three_partitions() {
        let aggregator = MergeAggregator::new(vec![TwoPhaseKind::Count, TwoPhaseKind::Sum]);

        let partials = vec![
            PartialAggregate {
                group_key: b"AAPL".to_vec(),
                partition_id: 0,
                states: vec![PartialState::Count(500), PartialState::Sum(75000.0)],
                watermark_ms: 1000,
                epoch: 1,
            },
            PartialAggregate {
                group_key: b"AAPL".to_vec(),
                partition_id: 1,
                states: vec![PartialState::Count(300), PartialState::Sum(45000.0)],
                watermark_ms: 1000,
                epoch: 1,
            },
            PartialAggregate {
                group_key: b"AAPL".to_vec(),
                partition_id: 2,
                states: vec![PartialState::Count(200), PartialState::Sum(30000.0)],
                watermark_ms: 1000,
                epoch: 1,
            },
        ];

        let result = aggregator.merge_all(&partials).unwrap();
        assert_eq!(result.len(), 1);

        let merged = &result[b"AAPL".as_ref()];
        assert_eq!(merged[0], PartialState::Count(1000));
        assert_eq!(merged[1], PartialState::Sum(150_000.0));

        let finals = MergeAggregator::finalize(merged);
        assert!((finals[0] - 1000.0).abs() < f64::EPSILON);
        assert!((finals[1] - 150_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_merge_multi_group() {
        let aggregator = MergeAggregator::new(vec![TwoPhaseKind::Count]);

        let partials = vec![
            PartialAggregate {
                group_key: b"AAPL".to_vec(),
                partition_id: 0,
                states: vec![PartialState::Count(10)],
                watermark_ms: 1000,
                epoch: 1,
            },
            PartialAggregate {
                group_key: b"GOOG".to_vec(),
                partition_id: 0,
                states: vec![PartialState::Count(20)],
                watermark_ms: 1000,
                epoch: 1,
            },
            PartialAggregate {
                group_key: b"AAPL".to_vec(),
                partition_id: 1,
                states: vec![PartialState::Count(30)],
                watermark_ms: 1000,
                epoch: 1,
            },
        ];

        let result = aggregator.merge_all(&partials).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[b"AAPL".as_ref()][0], PartialState::Count(40));
        assert_eq!(result[b"GOOG".as_ref()][0], PartialState::Count(20));
    }

    #[test]
    fn test_merge_avg_weighted() {
        let aggregator = MergeAggregator::new(vec![TwoPhaseKind::Avg]);

        // Partition 0: avg of [10, 20, 30] = sum=60, count=3
        // Partition 1: avg of [40, 50] = sum=90, count=2
        // Correct weighted avg = (60+90) / (3+2) = 150/5 = 30
        let partials = vec![
            PartialAggregate {
                group_key: b"g1".to_vec(),
                partition_id: 0,
                states: vec![PartialState::Avg {
                    sum: 60.0,
                    count: 3,
                }],
                watermark_ms: 1000,
                epoch: 1,
            },
            PartialAggregate {
                group_key: b"g1".to_vec(),
                partition_id: 1,
                states: vec![PartialState::Avg {
                    sum: 90.0,
                    count: 2,
                }],
                watermark_ms: 1000,
                epoch: 1,
            },
        ];

        let result = aggregator.merge_all(&partials).unwrap();
        let finals = MergeAggregator::finalize(&result[b"g1".as_ref()]);
        assert!((finals[0] - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_merge_min_max_global() {
        let aggregator = MergeAggregator::new(vec![TwoPhaseKind::Min, TwoPhaseKind::Max]);

        let partials = vec![
            PartialAggregate {
                group_key: b"g".to_vec(),
                partition_id: 0,
                states: vec![PartialState::Min(Some(10.0)), PartialState::Max(Some(90.0))],
                watermark_ms: 1000,
                epoch: 1,
            },
            PartialAggregate {
                group_key: b"g".to_vec(),
                partition_id: 1,
                states: vec![PartialState::Min(Some(5.0)), PartialState::Max(Some(100.0))],
                watermark_ms: 1000,
                epoch: 1,
            },
            PartialAggregate {
                group_key: b"g".to_vec(),
                partition_id: 2,
                states: vec![PartialState::Min(Some(15.0)), PartialState::Max(Some(80.0))],
                watermark_ms: 1000,
                epoch: 1,
            },
        ];

        let result = aggregator.merge_all(&partials).unwrap();
        let merged = &result[b"g".as_ref()];
        assert_eq!(merged[0], PartialState::Min(Some(5.0)));
        assert_eq!(merged[1], PartialState::Max(Some(100.0)));
    }

    #[test]
    fn test_merge_empty_partials() {
        let aggregator = MergeAggregator::new(vec![TwoPhaseKind::Count]);
        let result = aggregator.merge_all(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_merge_function_count_mismatch() {
        let aggregator = MergeAggregator::new(vec![TwoPhaseKind::Count, TwoPhaseKind::Sum]);

        let bad = PartialAggregate {
            group_key: b"g".to_vec(),
            partition_id: 0,
            states: vec![PartialState::Count(1)], // only 1, expected 2
            watermark_ms: 0,
            epoch: 0,
        };

        let refs: Vec<&PartialAggregate> = vec![&bad];
        let err = aggregator.merge_group(&refs).unwrap_err();
        match err {
            TwoPhaseError::FunctionCountMismatch {
                expected: 2,
                actual: 1,
            } => {}
            other => panic!("expected FunctionCountMismatch, got {other:?}"),
        }
    }

    // ── Arrow IPC ───────────────────────────────────────────────────

    #[test]
    fn test_arrow_ipc_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("count", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(Float64Array::from(vec![1000.0, 500.0])),
            ],
        )
        .unwrap();

        let ipc_bytes = encode_batch_to_ipc(&batch).unwrap();
        assert!(!ipc_bytes.is_empty());

        let decoded = decode_batch_from_ipc(&ipc_bytes).unwrap();
        assert_eq!(decoded.num_rows(), 2);
        assert_eq!(decoded.num_columns(), 2);
        assert_eq!(decoded.schema(), batch.schema());
    }

    #[test]
    fn test_ipc_decode_invalid() {
        let result = decode_batch_from_ipc(b"not valid ipc");
        assert!(result.is_err());
    }

    // ── Serialization ───────────────────────────────────────────────

    #[test]
    fn test_serialize_deserialize_partials() {
        let partials = vec![
            PartialAggregate {
                group_key: b"AAPL".to_vec(),
                partition_id: 0,
                states: vec![PartialState::Count(42), PartialState::Sum(100.5)],
                watermark_ms: 1000,
                epoch: 5,
            },
            PartialAggregate {
                group_key: b"GOOG".to_vec(),
                partition_id: 1,
                states: vec![PartialState::Count(10), PartialState::Sum(50.0)],
                watermark_ms: 1000,
                epoch: 5,
            },
        ];

        let bytes = serialize_partials(&partials).unwrap();
        let decoded = deserialize_partials(&bytes).unwrap();
        assert_eq!(decoded, partials);
    }

    #[test]
    fn test_deserialize_invalid() {
        let result = deserialize_partials(b"not json");
        assert!(result.is_err());
    }

    // ── Store Integration ───────────────────────────────────────────

    #[test]
    fn test_store_publish_collect_roundtrip() {
        let store = CrossPartitionAggregateStore::new(3);

        let partials = vec![
            PartialAggregate {
                group_key: b"AAPL".to_vec(),
                partition_id: 0,
                states: vec![PartialState::Count(100)],
                watermark_ms: 1000,
                epoch: 1,
            },
            PartialAggregate {
                group_key: b"AAPL".to_vec(),
                partition_id: 1,
                states: vec![PartialState::Count(200)],
                watermark_ms: 1000,
                epoch: 1,
            },
        ];

        publish_partials(&store, &partials).unwrap();

        let collected = collect_partials(&store, b"AAPL");
        assert_eq!(collected.len(), 2);

        // Verify content
        let total: i64 = collected
            .iter()
            .map(|p| match &p.states[0] {
                PartialState::Count(n) => *n,
                _ => panic!("expected Count"),
            })
            .sum();
        assert_eq!(total, 300);
    }

    // ── Full Pipeline ───────────────────────────────────────────────

    #[test]
    fn test_three_partition_full_pipeline() {
        let store = CrossPartitionAggregateStore::new(3);
        let aggregator = MergeAggregator::new(vec![
            TwoPhaseKind::Count,
            TwoPhaseKind::Sum,
            TwoPhaseKind::Avg,
        ]);

        // Phase 1: each partition publishes partials
        for pid in 0..3u32 {
            let count = (i64::from(pid) + 1) * 100; // 100, 200, 300
            let sum = (f64::from(pid) + 1.0) * 1000.0; // 1000, 2000, 3000
            let partial = PartialAggregate {
                group_key: b"AAPL".to_vec(),
                partition_id: pid,
                states: vec![
                    PartialState::Count(count),
                    PartialState::Sum(sum),
                    PartialState::Avg { sum, count },
                ],
                watermark_ms: 2000,
                epoch: 5,
            };
            publish_partials(&store, &[partial]).unwrap();
        }

        // Phase 2: collect and merge
        let collected = collect_partials(&store, b"AAPL");
        assert_eq!(collected.len(), 3);

        let result = aggregator.merge_all(&collected).unwrap();
        let merged = &result[b"AAPL".as_ref()];

        // COUNT: 100 + 200 + 300 = 600
        assert_eq!(merged[0], PartialState::Count(600));

        // SUM: 1000 + 2000 + 3000 = 6000
        assert_eq!(merged[1], PartialState::Sum(6000.0));

        // AVG: (1000+2000+3000) / (100+200+300) = 6000/600 = 10.0
        let finals = MergeAggregator::finalize(merged);
        assert!((finals[2] - 10.0).abs() < f64::EPSILON);
    }

    // ── HLL Sketch ──────────────────────────────────────────────────

    /// Bit mixer that distributes sequential integers across all 64 bits.
    fn test_hash(x: u64) -> u64 {
        let mut h = x.wrapping_mul(0x517c_c1b7_2722_0a95);
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
        h ^= h >> 33;
        h
    }

    #[test]
    fn test_hll_basic() {
        let mut hll = HllSketch::new();
        assert_eq!(hll.num_registers(), 256);
        assert_eq!(hll.precision(), 8);

        // Empty sketch should estimate ~0
        assert!(hll.estimate() < 1.0);

        // Add some values
        for i in 0..1000u64 {
            hll.add_hash(test_hash(i));
        }

        let est = hll.estimate();
        // HLL with precision=8 has ~6.5% standard error
        // 1000 ± 300 is a generous bound (~3-4 sigma)
        assert!(est > 700.0, "estimate {est} too low");
        assert!(est < 1400.0, "estimate {est} too high");
    }

    #[test]
    fn test_hll_merge() {
        let mut hll_a = HllSketch::new();
        let mut hll_b = HllSketch::new();

        // Add distinct sets
        for i in 0..500u64 {
            hll_a.add_hash(test_hash(i));
        }
        for i in 500..1000u64 {
            hll_b.add_hash(test_hash(i));
        }

        let est_a = hll_a.estimate();
        let est_b = hll_b.estimate();

        hll_a.merge(&hll_b);
        let est_merged = hll_a.estimate();

        // Merged should be roughly sum of distinct elements
        assert!(est_merged > est_a, "merged should be >= individual");
        assert!(est_merged > est_b, "merged should be >= individual");
        // Should be approximately 1000
        assert!(est_merged > 700.0, "merged {est_merged} too low");
        assert!(est_merged < 1400.0, "merged {est_merged} too high");
    }

    #[test]
    fn test_hll_serialization_roundtrip() {
        let mut hll = HllSketch::new();
        for i in 0..100u64 {
            hll.add_hash(i * 12345);
        }

        let bytes = hll.to_bytes();
        assert_eq!(bytes.len(), 1 + 256); // precision byte + 256 registers

        let restored = HllSketch::from_bytes(&bytes).unwrap();
        assert_eq!(restored.precision(), hll.precision());
        assert!(
            (restored.estimate() - hll.estimate()).abs() < f64::EPSILON,
            "estimate should be identical after roundtrip"
        );
    }

    #[test]
    fn test_hll_from_bytes_invalid() {
        assert!(HllSketch::from_bytes(&[]).is_err());
        assert!(HllSketch::from_bytes(&[3]).is_err()); // precision 3 < minimum 4
        assert!(HllSketch::from_bytes(&[8, 1, 2]).is_err()); // wrong length
    }

    #[test]
    fn test_hll_merge_partial_state() {
        let mut hll_a = HllSketch::new();
        let mut hll_b = HllSketch::new();

        for i in 0..500u64 {
            hll_a.add_hash(test_hash(i));
        }
        for i in 500..1000u64 {
            hll_b.add_hash(test_hash(i));
        }

        let mut state_a = PartialState::ApproxDistinct(hll_a.to_bytes());
        let state_b = PartialState::ApproxDistinct(hll_b.to_bytes());

        state_a.merge(&state_b);

        let est = state_a.finalize();
        assert!(est > 700.0, "HLL partial merge estimate {est} too low");
        assert!(est < 1400.0, "HLL partial merge estimate {est} too high");
    }

    // ── PartialState empty + kind ───────────────────────────────────

    #[test]
    fn test_partial_state_empty() {
        assert_eq!(
            PartialState::empty(TwoPhaseKind::Count),
            PartialState::Count(0)
        );
        assert_eq!(
            PartialState::empty(TwoPhaseKind::Sum),
            PartialState::Sum(0.0)
        );
        assert_eq!(
            PartialState::empty(TwoPhaseKind::Avg),
            PartialState::Avg { sum: 0.0, count: 0 }
        );
        assert_eq!(
            PartialState::empty(TwoPhaseKind::Min),
            PartialState::Min(None)
        );
        assert_eq!(
            PartialState::empty(TwoPhaseKind::Max),
            PartialState::Max(None)
        );

        // ApproxDistinct empty should be a valid HLL
        let empty_hll = PartialState::empty(TwoPhaseKind::ApproxDistinct);
        match &empty_hll {
            PartialState::ApproxDistinct(bytes) => {
                let sketch = HllSketch::from_bytes(bytes).unwrap();
                assert!(sketch.estimate() < 1.0);
            }
            _ => panic!("expected ApproxDistinct"),
        }
    }

    // ── MergeAggregator accessor ────────────────────────────────────

    #[test]
    fn test_merge_aggregator_accessors() {
        let aggregator = MergeAggregator::new(vec![TwoPhaseKind::Count, TwoPhaseKind::Avg]);
        assert_eq!(aggregator.num_functions(), 2);
        assert_eq!(
            aggregator.kinds(),
            &[TwoPhaseKind::Count, TwoPhaseKind::Avg]
        );
    }

    // ── PartialAggregate serde ──────────────────────────────────────

    #[test]
    fn test_partial_aggregate_json_roundtrip() {
        let pa = PartialAggregate {
            group_key: b"test".to_vec(),
            partition_id: 42,
            states: vec![
                PartialState::Count(100),
                PartialState::Avg {
                    sum: 500.0,
                    count: 10,
                },
                PartialState::Min(Some(1.5)),
                PartialState::Max(None),
            ],
            watermark_ms: 5000,
            epoch: 10,
        };

        let json = serde_json::to_string(&pa).unwrap();
        let back: PartialAggregate = serde_json::from_str(&json).unwrap();
        assert_eq!(back, pa);
    }
}
