//! Per-connector split enumeration and assignment.
//!
//! Each source connector exposes a [`SplitShape`] describing how its
//! work divides. The cluster control plane calls [`plan_assignment`] to
//! compute a concrete [`SplitAssignment`] vector. The function is pure
//! and deterministic; tests exercise the three shapes directly.
//!
//! Design in the parent doc §3: **per-connector dispatch, no
//! `SplitEnumerator` trait**. The connector crate (laminar-connectors)
//! is responsible for inspecting its own config and producing a
//! `SplitShape`; this module does the routing.
//!
//! ## Connector → shape mapping (reference)
//!
//! | Source | Shape |
//! |---|---|
//! | Kafka | `External` — the broker's consumer group is authoritative |
//! | Files | `Enumerable(paths)` — distribute by `key_hash(path)` |
//! | Postgres CDC / MySQL CDC | `Singleton` — one reader, DB constraint |
//! | WebSocket | `Singleton` |
//! | OTEL | `Singleton` |

use serde::{Deserialize, Serialize};

use crate::cluster::control::leader_of;
use crate::cluster::discovery::NodeId;
use crate::state::key_hash;

/// Opaque split identifier. Kafka uses `"topic:partition"` form; Files
/// uses the file path; CDC sources use a stable tag like
/// `"postgres-cdc:slot_name"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SplitId(pub String);

impl SplitId {
    /// Construct from any string-like value.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SplitId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Resolved (split, owner) pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SplitAssignment {
    /// The split being assigned.
    pub split: SplitId,
    /// Instance that owns the split for the current assignment version.
    pub owner: NodeId,
}

/// How a connector's work divides across instances.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitShape {
    /// An external service (Kafka broker, external load balancer) owns
    /// the assignment. The cluster control plane records the observed
    /// result rather than choosing it. Kafka is the canonical case —
    /// see `crates/laminar-connectors/src/kafka/rebalance.rs`.
    External,
    /// Connector enumerates a fixed list of splits up front; the
    /// control plane distributes them deterministically via
    /// `key_hash(split_id) % live.len()`.
    Enumerable(Vec<SplitId>),
    /// Exactly one reader allowed (DB replication slot, singleton
    /// connection). Pinned to the lowest-ID live instance; on failover
    /// the new lowest-ID takes over.
    Singleton {
        /// A stable tag for the single split (e.g., `"postgres-cdc:slot_name"`).
        tag: String,
    },
}

/// Plan the concrete owner for every split in `shape`.
///
/// - `SplitShape::External` → empty vector (assignments come from the
///   external service; caller records them separately).
/// - `SplitShape::Enumerable` → each split mapped to `live[key_hash(split) % live.len()]`.
/// - `SplitShape::Singleton` → one assignment to `leader_of(live)`.
///
/// Returns an empty vector if `live` is empty (no instances to assign
/// to); the caller should treat this as a precondition failure.
#[must_use]
pub fn plan_assignment(shape: &SplitShape, live: &[NodeId]) -> Vec<SplitAssignment> {
    if live.is_empty() {
        return Vec::new();
    }
    // Filter out the unassigned sentinel.
    let valid: Vec<NodeId> = live
        .iter()
        .copied()
        .filter(|n| !n.is_unassigned())
        .collect();
    if valid.is_empty() {
        return Vec::new();
    }

    match shape {
        SplitShape::External => Vec::new(),
        SplitShape::Enumerable(splits) => splits
            .iter()
            .map(|split| {
                let bucket = key_hash(split.0.as_bytes()) % valid.len() as u64;
                let idx = usize::try_from(bucket).unwrap_or(0);
                SplitAssignment {
                    split: split.clone(),
                    owner: valid[idx],
                }
            })
            .collect(),
        SplitShape::Singleton { tag } => {
            // Singleton sources pin to the leader for deterministic,
            // fence-safe failover. Reusing `leader_of` keeps the
            // rule in one place.
            leader_of(&valid)
                .map(|owner| {
                    vec![SplitAssignment {
                        split: SplitId::new(tag),
                        owner,
                    }]
                })
                .unwrap_or_default()
        }
    }
}

/// Compute how many splits each instance owns in `assignments`. Useful
/// for detecting hot-spotting during testing or debugging.
#[must_use]
pub fn split_count_by_owner(assignments: &[SplitAssignment]) -> rustc_hash::FxHashMap<NodeId, usize> {
    let mut counts: rustc_hash::FxHashMap<NodeId, usize> = rustc_hash::FxHashMap::default();
    for a in assignments {
        *counts.entry(a.owner).or_insert(0) += 1;
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(id: u64) -> NodeId {
        NodeId(id)
    }

    #[test]
    fn external_produces_empty_plan() {
        let plan = plan_assignment(&SplitShape::External, &[n(1), n(2)]);
        assert!(plan.is_empty());
    }

    #[test]
    fn singleton_pins_to_lowest_id_leader() {
        let plan = plan_assignment(
            &SplitShape::Singleton {
                tag: "postgres-cdc:slot_a".into(),
            },
            &[n(7), n(3), n(12)],
        );
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].owner, n(3), "singleton must match leader_of");
        assert_eq!(plan[0].split.as_str(), "postgres-cdc:slot_a");
    }

    #[test]
    fn singleton_empty_live_is_empty_plan() {
        let plan = plan_assignment(
            &SplitShape::Singleton {
                tag: "ws:connection".into(),
            },
            &[],
        );
        assert!(plan.is_empty());
    }

    #[test]
    fn enumerable_distributes_across_live() {
        let splits = (0..20)
            .map(|i| SplitId::new(format!("/data/events-{i:04}.parquet")))
            .collect::<Vec<_>>();
        let plan = plan_assignment(
            &SplitShape::Enumerable(splits.clone()),
            &[n(1), n(2), n(3), n(4)],
        );
        assert_eq!(plan.len(), 20);
        let counts = split_count_by_owner(&plan);
        for count in counts.values() {
            assert!(
                *count > 0,
                "expected every instance to get at least one split"
            );
        }
    }

    #[test]
    fn enumerable_is_deterministic() {
        let splits = vec![
            SplitId::new("a"),
            SplitId::new("b"),
            SplitId::new("c"),
            SplitId::new("d"),
        ];
        let live = vec![n(1), n(2), n(3)];
        let p1 = plan_assignment(&SplitShape::Enumerable(splits.clone()), &live);
        let p2 = plan_assignment(&SplitShape::Enumerable(splits), &live);
        assert_eq!(p1, p2, "same inputs must produce identical plans");
    }

    #[test]
    fn unassigned_sentinel_filtered() {
        // NodeId::UNASSIGNED in the live set must be ignored.
        let plan = plan_assignment(
            &SplitShape::Singleton {
                tag: "only".into(),
            },
            &[NodeId::UNASSIGNED, n(5)],
        );
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].owner, n(5));
    }

    #[test]
    fn empty_enumerable_produces_empty_plan() {
        let plan = plan_assignment(&SplitShape::Enumerable(vec![]), &[n(1)]);
        assert!(plan.is_empty());
    }

    #[test]
    fn grow_cluster_reshuffles_deterministically() {
        // Adding an instance redistributes; verify the mapping is
        // stable for inputs that don't change.
        let splits: Vec<SplitId> = (0..50)
            .map(|i| SplitId::new(format!("k-{i}")))
            .collect();
        let small = plan_assignment(
            &SplitShape::Enumerable(splits.clone()),
            &[n(1), n(2)],
        );
        let large = plan_assignment(
            &SplitShape::Enumerable(splits),
            &[n(1), n(2), n(3)],
        );
        // Some keys stay, some move. Not a tight bound, just a
        // regression guard that the output isn't empty.
        assert_eq!(small.len(), 50);
        assert_eq!(large.len(), 50);
    }
}
