//! Durable ownership of one stable cluster node identity.
//!
//! Each renewal appends a create-only sequence object. This gives local filesystems and object
//! stores the same compare-and-set boundary without relying on backend-specific entity tags.

mod authority;
mod manager;
mod store;

use object_store::path::Path as OsPath;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::cluster::discovery::NodeId;

pub use authority::ProcessLeaseAuthority;
pub use manager::{ProcessLeaseConfig, ProcessLeaseManager};
pub use store::ProcessLeaseStore;

use std::time::Duration;

const MAX_PROCESS_LEASE_RECORD_BYTES: u64 = 1024;
const MAX_PROCESS_LEASE_FENCE_BYTES: u64 = 2048;
const PROCESS_LEASE_HEAD_READ_ATTEMPTS: usize = 4;
const PROCESS_LEASE_HISTORY_TO_RETAIN: usize = 2;
const PROCESS_LEASE_MAX_LIST_RECORDS: usize = 4096;
const PROCESS_LEASE_PRUNE_BATCH_RECORDS: usize = 256;
const PROCESS_LEASE_PRUNE_READ_CONCURRENCY: usize = 32;
const PROCESS_LEASE_WRITES_PER_PRUNE: u64 = 64;
const PROCESS_LEASE_MAX_PRUNE_BATCHES: usize = 4;
const PROCESS_LEASE_PRUNE_IO_TIMEOUT: Duration = Duration::from_secs(5);
const PROCESS_LEASE_RENEW_RETRY_DELAY: Duration = Duration::from_millis(250);

fn lease_prefix(node: NodeId) -> String {
    format!("control/process-lease/node={}/", node.0)
}

fn lease_path(node: NodeId, seq: u64) -> OsPath {
    OsPath::from(format!("{}v{seq:016}.json", lease_prefix(node)))
}

fn fence_path(node: NodeId, predecessor: Uuid) -> OsPath {
    OsPath::from(format!(
        "control/process-lease-fences/v1/node={}/predecessor={predecessor}.json",
        node.0
    ))
}

fn successor_fence_path(node: NodeId, successor: Uuid, term: u64) -> OsPath {
    OsPath::from(format!(
        "control/process-lease-fences/v1/node={}/successor={successor}/term={term:016}.json",
        node.0
    ))
}

fn sequence_from_path(node: NodeId, path: &OsPath) -> Result<u64, ProcessLeaseError> {
    let prefix = lease_prefix(node);
    let raw = path
        .as_ref()
        .strip_prefix(&prefix)
        .and_then(|file| file.strip_prefix('v'))
        .and_then(|file| file.strip_suffix(".json"))
        .ok_or_else(|| {
            ProcessLeaseError::Invalid(format!("invalid process lease record path {path}"))
        })?;
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ProcessLeaseError::Invalid(format!(
            "invalid process lease sequence in {path}"
        )));
    }
    let sequence = raw.parse::<u64>().map_err(|error| {
        ProcessLeaseError::Invalid(format!("invalid process lease sequence in {path}: {error}"))
    })?;
    if sequence == 0 || lease_path(node, sequence) != *path {
        return Err(ProcessLeaseError::Invalid(format!(
            "noncanonical process lease record path {path}"
        )));
    }
    Ok(sequence)
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Durable owner of one stable node identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessLease {
    /// Stable node identity protected by this lease.
    pub node: NodeId,
    /// Boot-unique owner identity.
    pub owner: Uuid,
    /// Monotonic process term. It advances after every lease lapse.
    pub term: u64,
    /// Append-only compare-and-set sequence.
    pub seq: u64,
    /// Owner-written advisory expiry for diagnostics; takeover never compares client clocks.
    pub expires_at_ms: i64,
}

impl ProcessLease {
    pub(crate) fn validate(&self, expected_node: NodeId) -> Result<(), ProcessLeaseError> {
        if self.node != expected_node || self.node.is_unassigned() {
            return Err(ProcessLeaseError::Invalid(
                "lease node does not match its durable namespace".into(),
            ));
        }
        if self.owner.is_nil() || self.term == 0 || self.seq == 0 {
            return Err(ProcessLeaseError::Invalid(
                "lease owner, term, and sequence must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

/// Durable proof that one exact stable-node process term was superseded after a full lease
/// observation. The successor is the immediate create-only takeover record, not a wall-clock
/// expiry estimate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessLeaseFence {
    /// Last lease record owned by the process being fenced.
    pub predecessor: ProcessLease,
    /// Immediate different-owner term that revoked the predecessor.
    pub successor: ProcessLease,
}

impl ProcessLeaseFence {
    /// Build an exact process-term transition.
    ///
    /// # Errors
    /// Rejects different node namespaces, same-owner renewals, or a non-adjacent sequence/term.
    pub fn new(
        predecessor: ProcessLease,
        successor: ProcessLease,
    ) -> Result<Self, ProcessLeaseError> {
        let fence = Self {
            predecessor,
            successor,
        };
        if !fence.is_canonical() {
            return Err(ProcessLeaseError::Invalid(
                "process lease fence must bind an immediate different-owner takeover".into(),
            ));
        }
        Ok(fence)
    }

    /// Whether this is one exact create-only owner transition.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.predecessor.validate(self.predecessor.node).is_ok()
            && self.successor.validate(self.predecessor.node).is_ok()
            && self.predecessor.owner != self.successor.owner
            && self.predecessor.seq.checked_add(1) == Some(self.successor.seq)
            && self.predecessor.term.checked_add(1) == Some(self.successor.term)
    }
}

/// Result of one process-lease acquisition attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessLeaseOutcome {
    /// This boot incarnation owns the stable node identity.
    Acquired(ProcessLease),
    /// A different live boot incarnation owns it.
    Held(ProcessLease),
}

/// Candidate-local proof that one exact durable lease record stayed current for a full TTL.
#[derive(Debug)]
pub struct ProcessLeaseObservation {
    lease: ProcessLease,
    started: std::time::Instant,
}

/// Process lease storage failure.
#[derive(Debug, thiserror::Error)]
pub enum ProcessLeaseError {
    /// Underlying object-store failure.
    #[error("object store I/O: {0}")]
    Io(String),
    /// Invalid durable record.
    #[error("invalid process lease: {0}")]
    Invalid(String),
    /// JSON encoding or decoding failure.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Caller-provided monotonic deadline expired before fencing could be proven.
    #[error("process lease fencing deadline: {0}")]
    Deadline(String),
}

#[cfg(test)]
mod tests;
