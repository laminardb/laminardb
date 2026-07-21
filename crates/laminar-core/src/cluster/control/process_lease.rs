//! Durable ownership of one stable cluster node identity.
//!
//! Each renewal appends a create-only sequence object. This gives local filesystems and object
//! stores the same compare-and-set boundary without relying on backend-specific entity tags.

use std::collections::BinaryHeap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

use crate::cluster::discovery::NodeId;

use super::lease_deadline::LeaseDeadline;

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

/// Append-only object-store authority for one stable node identity.
pub struct ProcessLeaseStore {
    store: Arc<dyn ObjectStore>,
    node: NodeId,
    ttl_ms: i64,
    prune_running: Arc<AtomicBool>,
    prune_healthy: Arc<AtomicBool>,
    writes_since_prune: AtomicU64,
    sealed_term: AtomicU64,
}

impl std::fmt::Debug for ProcessLeaseStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessLeaseStore")
            .field("node", &self.node)
            .field("ttl_ms", &self.ttl_ms)
            .finish_non_exhaustive()
    }
}

impl ProcessLeaseStore {
    /// Create an authority for `node`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, node: NodeId, ttl_ms: i64) -> Self {
        Self {
            store,
            node,
            ttl_ms,
            prune_running: Arc::new(AtomicBool::new(false)),
            prune_healthy: Arc::new(AtomicBool::new(true)),
            writes_since_prune: AtomicU64::new(0),
            sealed_term: AtomicU64::new(0),
        }
    }

    async fn list_seqs_from(
        store: &Arc<dyn ObjectStore>,
        node: NodeId,
    ) -> Result<Vec<u64>, ProcessLeaseError> {
        let prefix_string = lease_prefix(node);
        let prefix = OsPath::from(prefix_string.clone());
        let mut entries = store.list(Some(&prefix));
        let mut sequences = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| ProcessLeaseError::Io(error.to_string()))?;
            if sequences.len() == PROCESS_LEASE_MAX_LIST_RECORDS {
                return Err(ProcessLeaseError::Invalid(format!(
                    "process lease history exceeds the fixed {PROCESS_LEASE_MAX_LIST_RECORDS}-record scan bound"
                )));
            }
            sequences.push(sequence_from_path(node, &entry.location)?);
        }
        sequences.sort_unstable();
        sequences.dedup();
        Ok(sequences)
    }

    async fn list_seqs(&self) -> Result<Vec<u64>, ProcessLeaseError> {
        Self::list_seqs_from(&self.store, self.node).await
    }

    async fn oldest_prune_window(
        store: &Arc<dyn ObjectStore>,
        node: NodeId,
    ) -> Result<(u64, Vec<u64>), ProcessLeaseError> {
        let prefix = OsPath::from(lease_prefix(node));
        let mut entries = store.list(Some(&prefix));
        let window_len = PROCESS_LEASE_PRUNE_BATCH_RECORDS
            .checked_add(1)
            .ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease prune window overflow".into())
            })?;
        let mut oldest = BinaryHeap::with_capacity(window_len);
        let mut count = 0_u64;
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| ProcessLeaseError::Io(error.to_string()))?;
            let sequence = sequence_from_path(node, &entry.location)?;
            count = count.checked_add(1).ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease history count exhausted".into())
            })?;
            if oldest.len() < window_len {
                oldest.push(sequence);
            } else if oldest.peek().is_some_and(|largest| sequence < *largest) {
                oldest.pop();
                oldest.push(sequence);
            }
        }
        Ok((count, oldest.into_sorted_vec()))
    }

    fn schedule_history_prune(&self, force: bool) {
        if !force
            && self.writes_since_prune.fetch_add(1, Ordering::AcqRel) + 1
                < PROCESS_LEASE_WRITES_PER_PRUNE
        {
            return;
        }
        if self
            .prune_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.writes_since_prune.store(0, Ordering::Release);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.prune_running.store(false, Ordering::Release);
            self.prune_healthy.store(false, Ordering::Release);
            return;
        };
        let store = Arc::clone(&self.store);
        let node = self.node;
        let prune_running = Arc::clone(&self.prune_running);
        let prune_healthy = Arc::clone(&self.prune_healthy);
        runtime.spawn(async move {
            let prune = Self::prune_history(&store, node).await;
            if let Err(error) = prune {
                prune_healthy.store(false, Ordering::Release);
                tracing::warn!(node = node.0, %error, "process lease history prune failed");
            } else {
                prune_healthy.store(true, Ordering::Release);
            }
            prune_running.store(false, Ordering::Release);
        });
    }

    async fn prune_history(
        store: &Arc<dyn ObjectStore>,
        node: NodeId,
    ) -> Result<(), ProcessLeaseError> {
        for _ in 0..PROCESS_LEASE_MAX_PRUNE_BATCHES {
            let done = tokio::time::timeout(
                PROCESS_LEASE_PRUNE_IO_TIMEOUT,
                Self::prune_history_batch(store, node),
            )
            .await
            .map_err(|_| ProcessLeaseError::Io("process lease history prune timed out".into()))??;
            if done {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(ProcessLeaseError::Io(
            "process lease history still exceeds the bounded prune budget".into(),
        ))
    }

    async fn repair_unhealthy_prune(&self) -> Result<(), ProcessLeaseError> {
        if self.prune_healthy.load(Ordering::Acquire) {
            return Ok(());
        }
        Self::prune_history(&self.store, self.node).await?;
        self.prune_healthy.store(true, Ordering::Release);
        self.writes_since_prune.store(0, Ordering::Release);
        Ok(())
    }

    async fn prune_history_batch(
        store: &Arc<dyn ObjectStore>,
        node: NodeId,
    ) -> Result<bool, ProcessLeaseError> {
        let (count, sequences) = Self::oldest_prune_window(store, node).await?;
        let retain = u64::try_from(PROCESS_LEASE_HISTORY_TO_RETAIN)
            .map_err(|_| ProcessLeaseError::Invalid("process lease retention overflow".into()))?;
        if count <= retain {
            return Ok(true);
        }
        let delete_count_u64 =
            (count - retain).min(u64::try_from(PROCESS_LEASE_PRUNE_BATCH_RECORDS).map_err(
                |_| ProcessLeaseError::Invalid("process lease prune batch overflow".into()),
            )?);
        let delete_count = usize::try_from(delete_count_u64).map_err(|_| {
            ProcessLeaseError::Invalid("process lease delete count overflow".into())
        })?;
        if sequences.len() <= delete_count {
            return Err(ProcessLeaseError::Invalid(
                "process lease prune window is missing its retained boundary".into(),
            ));
        }
        let mut reads = futures::stream::iter(sequences.iter().copied().map(|sequence| {
            let store = Arc::clone(store);
            async move {
                Self::read_record_from(&store, node, sequence)
                    .await
                    .map(|record| record.map(|record| (sequence, record)))
            }
        }))
        .buffer_unordered(PROCESS_LEASE_PRUNE_READ_CONCURRENCY);
        let mut records = Vec::with_capacity(sequences.len());
        while let Some(record) = reads.next().await {
            let Some(record) = record? else {
                return Ok(false);
            };
            records.push(record);
        }
        records.sort_unstable_by_key(|(sequence, _)| *sequence);
        // Seal every newly observed term transition under the predecessor boot identity before
        // deleting either side. The indexed certificate keeps recovery lookup O(1) and prevents
        // routine renewals or many later terms from erasing takeover evidence.
        for pair in records.windows(2) {
            let (left_sequence, left) = &pair[0];
            let (right_sequence, right) = &pair[1];
            if left_sequence.checked_add(1) != Some(*right_sequence) {
                return Err(ProcessLeaseError::Invalid(
                    "process lease history contains a noncontiguous sequence".into(),
                ));
            }
            if left.owner != right.owner || left.term != right.term {
                if left.term.checked_add(1) != Some(right.term) || left.owner == right.owner {
                    return Err(ProcessLeaseError::Invalid(
                        "process lease history contains a noncanonical term transition".into(),
                    ));
                }
                Self::seal_fence(store, &ProcessLeaseFence::new(left.clone(), right.clone())?)
                    .await?;
            }
        }
        let deletable = records
            .iter()
            .take(delete_count)
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>();
        let deletions = futures::stream::iter(
            deletable
                .into_iter()
                .map(move |sequence| Ok::<_, object_store::Error>(lease_path(node, sequence))),
        )
        .boxed();
        let mut results = store.delete_stream(deletions);
        while let Some(result) = results.next().await {
            if let Err(error) = result {
                if !matches!(error, object_store::Error::NotFound { .. }) {
                    return Err(ProcessLeaseError::Io(error.to_string()));
                }
            }
        }
        Ok(count.saturating_sub(delete_count_u64) <= retain)
    }

    async fn read_record_from(
        store: &Arc<dyn ObjectStore>,
        node: NodeId,
        sequence: u64,
    ) -> Result<Option<ProcessLease>, ProcessLeaseError> {
        let result = match store.get(&lease_path(node, sequence)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(ProcessLeaseError::Io(error.to_string())),
        };
        if result.meta.size == 0 || result.meta.size > MAX_PROCESS_LEASE_RECORD_BYTES {
            return Err(ProcessLeaseError::Invalid(format!(
                "process lease record is {} bytes; expected 1..={MAX_PROCESS_LEASE_RECORD_BYTES}",
                result.meta.size
            )));
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|error| ProcessLeaseError::Io(error.to_string()))?;
        let lease: ProcessLease = serde_json::from_slice(&bytes)?;
        lease.validate(node)?;
        if lease.seq != sequence {
            return Err(ProcessLeaseError::Invalid(
                "record sequence does not match its object name".into(),
            ));
        }
        let canonical = serde_json::to_vec(&lease)?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(ProcessLeaseError::Invalid(format!(
                "process lease record {sequence} does not use its canonical body"
            )));
        }
        Ok(Some(lease))
    }

    async fn read_history(
        store: &Arc<dyn ObjectStore>,
        node: NodeId,
    ) -> Result<Vec<(u64, ProcessLease)>, ProcessLeaseError> {
        for attempt in 0..PROCESS_LEASE_HEAD_READ_ATTEMPTS {
            let sequences = Self::list_seqs_from(store, node).await?;
            let mut reads = futures::stream::iter(sequences.iter().copied().map(|sequence| {
                let store = Arc::clone(store);
                async move {
                    Self::read_record_from(&store, node, sequence)
                        .await
                        .map(|record| record.map(|record| (sequence, record)))
                }
            }))
            .buffer_unordered(PROCESS_LEASE_PRUNE_READ_CONCURRENCY);
            let mut records = Vec::with_capacity(sequences.len());
            let mut changed = false;
            while let Some(record) = reads.next().await {
                match record? {
                    Some(record) => records.push(record),
                    None => changed = true,
                }
            }
            if !changed {
                records.sort_unstable_by_key(|(sequence, _)| *sequence);
                return Ok(records);
            }
            if attempt + 1 < PROCESS_LEASE_HEAD_READ_ATTEMPTS {
                tokio::task::yield_now().await;
            }
        }
        Err(ProcessLeaseError::Io(format!(
            "process lease history changed during {PROCESS_LEASE_HEAD_READ_ATTEMPTS} read attempts"
        )))
    }

    async fn load_fence_at(
        store: &Arc<dyn ObjectStore>,
        path: &OsPath,
    ) -> Result<Option<ProcessLeaseFence>, ProcessLeaseError> {
        let result = match store.get(path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(ProcessLeaseError::Io(error.to_string())),
        };
        if result.meta.size == 0 || result.meta.size > MAX_PROCESS_LEASE_FENCE_BYTES {
            return Err(ProcessLeaseError::Invalid(format!(
                "process lease fence is {} bytes; expected 1..={MAX_PROCESS_LEASE_FENCE_BYTES}",
                result.meta.size
            )));
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|error| ProcessLeaseError::Io(error.to_string()))?;
        let fence: ProcessLeaseFence = serde_json::from_slice(&bytes)?;
        if !fence.is_canonical() {
            return Err(ProcessLeaseError::Invalid(
                "indexed process lease fence is not canonical".into(),
            ));
        }
        let canonical = serde_json::to_vec(&fence)?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(ProcessLeaseError::Invalid(
                "indexed process lease fence does not use its canonical body".into(),
            ));
        }
        Ok(Some(fence))
    }

    async fn load_fence(
        store: &Arc<dyn ObjectStore>,
        node: NodeId,
        predecessor: Uuid,
    ) -> Result<Option<ProcessLeaseFence>, ProcessLeaseError> {
        let fence = Self::load_fence_at(store, &fence_path(node, predecessor)).await?;
        if fence.as_ref().is_some_and(|fence| {
            fence.predecessor.node != node || fence.predecessor.owner != predecessor
        }) {
            return Err(ProcessLeaseError::Invalid(
                "predecessor-indexed process lease fence has the wrong identity".into(),
            ));
        }
        Ok(fence)
    }

    async fn load_successor_fence(
        store: &Arc<dyn ObjectStore>,
        node: NodeId,
        successor: Uuid,
        term: u64,
    ) -> Result<Option<ProcessLeaseFence>, ProcessLeaseError> {
        let fence =
            Self::load_fence_at(store, &successor_fence_path(node, successor, term)).await?;
        if fence.as_ref().is_some_and(|fence| {
            fence.successor.node != node
                || fence.successor.owner != successor
                || fence.successor.term != term
        }) {
            return Err(ProcessLeaseError::Invalid(
                "successor-indexed process lease fence has the wrong identity".into(),
            ));
        }
        Ok(fence)
    }

    async fn seal_fence_at(
        store: &Arc<dyn ObjectStore>,
        path: &OsPath,
        fence: &ProcessLeaseFence,
    ) -> Result<(), ProcessLeaseError> {
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec(fence)?));
        let put_error = store.put_opts(path, payload, options).await.err();
        match Self::load_fence_at(store, path).await {
            Ok(Some(stored)) if stored == *fence => Ok(()),
            Ok(Some(_)) => Err(ProcessLeaseError::Invalid(
                "process fence index maps to conflicting takeover evidence".into(),
            )),
            Ok(None) => Err(ProcessLeaseError::Io(
                "process lease fence write was not durably visible".into(),
            )),
            Err(reconcile_error) => {
                if let Some(put_error) = put_error {
                    Err(ProcessLeaseError::Io(format!(
                        "process lease fence write failed ({put_error}); reconciliation failed ({reconcile_error})"
                    )))
                } else {
                    Err(reconcile_error)
                }
            }
        }
    }

    async fn seal_fence(
        store: &Arc<dyn ObjectStore>,
        fence: &ProcessLeaseFence,
    ) -> Result<(), ProcessLeaseError> {
        if !fence.is_canonical() {
            return Err(ProcessLeaseError::Invalid(
                "cannot seal a noncanonical process lease fence".into(),
            ));
        }
        Self::seal_fence_at(
            store,
            &fence_path(fence.predecessor.node, fence.predecessor.owner),
            fence,
        )
        .await?;
        Self::seal_fence_at(
            store,
            &successor_fence_path(
                fence.successor.node,
                fence.successor.owner,
                fence.successor.term,
            ),
            fence,
        )
        .await
    }

    async fn read_record(&self, sequence: u64) -> Result<Option<ProcessLease>, ProcessLeaseError> {
        Self::read_record_from(&self.store, self.node, sequence).await
    }

    async fn find_takeover_from(
        &self,
        owner: Uuid,
    ) -> Result<Option<ProcessLeaseFence>, ProcessLeaseError> {
        if let Some(fence) = Self::load_fence(&self.store, self.node, owner).await? {
            return Ok(Some(fence));
        }
        let records = Self::read_history(&self.store, self.node).await?;
        let mut found = None;
        for pair in records.windows(2) {
            let (predecessor_sequence, predecessor) = &pair[0];
            let (successor_sequence, successor) = &pair[1];
            if predecessor.owner != owner {
                continue;
            }
            if predecessor_sequence.checked_add(1) != Some(*successor_sequence) {
                continue;
            }
            let Ok(fence) = ProcessLeaseFence::new(predecessor.clone(), successor.clone()) else {
                continue;
            };
            if found.replace(fence).is_some() {
                return Err(ProcessLeaseError::Invalid(
                    "process owner appears in more than one durable takeover transition".into(),
                ));
            }
        }
        if let Some(fence) = found {
            Self::seal_fence(&self.store, &fence).await?;
            Ok(Some(fence))
        } else {
            // A concurrent pruner may have sealed the direct index and removed the history pair
            // after our first point read but before the scan completed.
            Self::load_fence(&self.store, self.node, owner).await
        }
    }

    async fn ensure_current_term_fence(
        &self,
        current: &ProcessLease,
    ) -> Result<(), ProcessLeaseError> {
        if current.term == 1 || self.sealed_term.load(Ordering::Acquire) == current.term {
            self.sealed_term.store(current.term, Ordering::Release);
            return Ok(());
        }
        if let Some(fence) =
            Self::load_successor_fence(&self.store, self.node, current.owner, current.term).await?
        {
            if fence.successor.seq > current.seq {
                return Err(ProcessLeaseError::Invalid(
                    "process term fence starts after the current lease head".into(),
                ));
            }
            self.sealed_term.store(current.term, Ordering::Release);
            return Ok(());
        }

        let records = Self::read_history(&self.store, self.node).await?;
        for pair in records.windows(2) {
            let (left_sequence, left) = &pair[0];
            let (right_sequence, right) = &pair[1];
            if left_sequence.checked_add(1) == Some(*right_sequence)
                && right.owner == current.owner
                && right.term == current.term
            {
                let fence = ProcessLeaseFence::new(left.clone(), right.clone())?;
                Self::seal_fence(&self.store, &fence).await?;
                self.sealed_term.store(current.term, Ordering::Release);
                return Ok(());
            }
        }
        Err(ProcessLeaseError::Invalid(
            "current process term has no durable takeover evidence".into(),
        ))
    }

    /// Load the highest durable sequence.
    ///
    /// # Errors
    /// Fails on object-store I/O, malformed JSON, or a record in the wrong node namespace.
    pub async fn load(&self) -> Result<Option<ProcessLease>, ProcessLeaseError> {
        let mut observed_head = false;
        for attempt in 0..PROCESS_LEASE_HEAD_READ_ATTEMPTS {
            let sequences = match self.list_seqs().await {
                Ok(sequences) => sequences,
                Err(error) => {
                    self.schedule_history_prune(true);
                    return Err(error);
                }
            };
            let Some(sequence) = sequences.last().copied() else {
                if !observed_head {
                    return Ok(None);
                }
                if attempt + 1 < PROCESS_LEASE_HEAD_READ_ATTEMPTS {
                    tokio::task::yield_now().await;
                    continue;
                }
                break;
            };
            observed_head = true;
            match self.read_record(sequence).await? {
                Some(lease) => return Ok(Some(lease)),
                None if attempt + 1 < PROCESS_LEASE_HEAD_READ_ATTEMPTS => {
                    tokio::task::yield_now().await;
                }
                None => break,
            }
        }
        Err(ProcessLeaseError::Io(format!(
            "process lease head changed during {PROCESS_LEASE_HEAD_READ_ATTEMPTS} read attempts"
        )))
    }

    /// Acquire or renew this stable node identity for `owner` at `now_ms`.
    ///
    /// # Errors
    /// Fails closed on object-store I/O or an invalid durable record.
    pub async fn try_acquire(
        &self,
        owner: Uuid,
        now_ms: i64,
    ) -> Result<ProcessLeaseOutcome, ProcessLeaseError> {
        if owner.is_nil() || self.node.is_unassigned() || self.ttl_ms <= 0 {
            return Err(ProcessLeaseError::Invalid(
                "node, owner, and lease TTL must be nonzero".into(),
            ));
        }
        self.repair_unhealthy_prune().await?;
        let current = self.load().await?;
        if let Some(lease) = current.as_ref().filter(|lease| lease.owner == owner) {
            self.ensure_current_term_fence(lease).await?;
        }
        let candidate = match current {
            None => ProcessLease {
                node: self.node,
                owner,
                term: 1,
                seq: 1,
                expires_at_ms: now_ms.saturating_add(self.ttl_ms),
            },
            Some(ref lease) if lease.owner == owner => ProcessLease {
                node: self.node,
                owner,
                term: lease.term,
                seq: lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| ProcessLeaseError::Invalid("lease sequence exhausted".into()))?,
                expires_at_ms: now_ms.saturating_add(self.ttl_ms),
            },
            Some(lease) => return Ok(ProcessLeaseOutcome::Held(lease)),
        };
        if candidate.term == 0 || candidate.seq == 0 {
            return Err(ProcessLeaseError::Invalid(
                "lease term or sequence exhausted".into(),
            ));
        }

        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&candidate)?));
        match self
            .store
            .put_opts(&lease_path(self.node, candidate.seq), payload, options)
            .await
        {
            Ok(_) => {
                self.sealed_term.store(candidate.term, Ordering::Release);
                self.schedule_history_prune(false);
                Ok(ProcessLeaseOutcome::Acquired(candidate))
            }
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                let winner = self.load().await?.ok_or_else(|| {
                    ProcessLeaseError::Io("CAS conflict but the winner was not readable".into())
                })?;
                self.schedule_history_prune(false);
                if winner.owner == owner {
                    self.ensure_current_term_fence(&winner).await?;
                    Ok(ProcessLeaseOutcome::Acquired(winner))
                } else {
                    Ok(ProcessLeaseOutcome::Held(winner))
                }
            }
            Err(error) => Err(ProcessLeaseError::Io(error.to_string())),
        }
    }

    /// Start a candidate-local monotonic observation of a rival lease.
    ///
    /// # Errors
    /// Rejects a malformed record or one from another node namespace.
    pub fn observe_rival(
        &self,
        lease: &ProcessLease,
    ) -> Result<ProcessLeaseObservation, ProcessLeaseError> {
        lease.validate(self.node)?;
        Ok(ProcessLeaseObservation {
            lease: lease.clone(),
            started: std::time::Instant::now(),
        })
    }

    /// Attempt takeover after the same sequence and owner have been observed unchanged for a
    /// full TTL on this candidate's monotonic clock.
    ///
    /// # Errors
    /// Fails closed on early observation, object-store I/O, or malformed durable state.
    pub async fn try_takeover(
        &self,
        owner: Uuid,
        observation: &ProcessLeaseObservation,
        now_ms: i64,
    ) -> Result<ProcessLeaseOutcome, ProcessLeaseError> {
        if owner.is_nil() || self.ttl_ms <= 0 {
            return Err(ProcessLeaseError::Invalid(
                "takeover owner and lease TTL must be nonzero".into(),
            ));
        }
        self.repair_unhealthy_prune().await?;
        observation.lease.validate(self.node)?;
        if observation.started.elapsed()
            < Duration::from_millis(u64::try_from(self.ttl_ms).unwrap_or(u64::MAX))
        {
            return Ok(ProcessLeaseOutcome::Held(observation.lease.clone()));
        }
        let current = self.load().await?.ok_or_else(|| {
            ProcessLeaseError::Invalid("observed process lease disappeared".into())
        })?;
        if current != observation.lease {
            return Ok(ProcessLeaseOutcome::Held(current));
        }
        let candidate = ProcessLease {
            node: self.node,
            owner,
            term: current
                .term
                .checked_add(1)
                .ok_or_else(|| ProcessLeaseError::Invalid("process term exhausted".into()))?,
            seq: current
                .seq
                .checked_add(1)
                .ok_or_else(|| ProcessLeaseError::Invalid("lease sequence exhausted".into()))?,
            expires_at_ms: now_ms.saturating_add(self.ttl_ms),
        };
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&candidate)?));
        match self
            .store
            .put_opts(&lease_path(self.node, candidate.seq), payload, options)
            .await
        {
            Ok(_) => {
                let fence = ProcessLeaseFence::new(current, candidate.clone())?;
                Self::seal_fence(&self.store, &fence).await?;
                self.sealed_term.store(candidate.term, Ordering::Release);
                self.schedule_history_prune(true);
                Ok(ProcessLeaseOutcome::Acquired(candidate))
            }
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                let winner = self.load().await?.ok_or_else(|| {
                    ProcessLeaseError::Io("takeover CAS winner was not readable".into())
                })?;
                self.schedule_history_prune(true);
                Ok(ProcessLeaseOutcome::Held(winner))
            }
            Err(error) => Err(ProcessLeaseError::Io(error.to_string())),
        }
    }
}

/// Shared authority for proving that an exact process incarnation has been durably revoked.
pub struct ProcessLeaseAuthority {
    store: Arc<dyn ObjectStore>,
    ttl: Duration,
    ttl_ms: i64,
}

impl std::fmt::Debug for ProcessLeaseAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessLeaseAuthority")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl ProcessLeaseAuthority {
    /// Bind all stable-node lease namespaces on one shared object store and TTL.
    ///
    /// # Errors
    /// Rejects a zero, sub-millisecond, fractional-millisecond, or oversized TTL.
    pub fn new(store: Arc<dyn ObjectStore>, ttl: Duration) -> Result<Self, ProcessLeaseError> {
        let ttl_ms = i64::try_from(ttl.as_millis())
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| ProcessLeaseError::Invalid("process lease TTL is invalid".into()))?;
        if Duration::from_millis(u64::try_from(ttl_ms).unwrap_or(u64::MAX)) != ttl {
            return Err(ProcessLeaseError::Invalid(
                "process lease TTL must use whole milliseconds".into(),
            ));
        }
        Ok(Self { store, ttl, ttl_ms })
    }

    /// Open one stable-node namespace over the shared authority.
    #[must_use]
    pub fn store_for(&self, node: NodeId) -> Arc<ProcessLeaseStore> {
        Arc::new(ProcessLeaseStore::new(
            Arc::clone(&self.store),
            node,
            self.ttl_ms,
        ))
    }

    /// Build a monotonic deadline that covers the mandatory full-TTL observation plus bounded
    /// authority I/O. Callers need not duplicate the process-lease TTL as another runtime knob.
    ///
    /// # Errors
    /// Rejects a zero I/O budget or monotonic-clock overflow.
    pub fn fencing_deadline(
        &self,
        io_budget: Duration,
    ) -> Result<tokio::time::Instant, ProcessLeaseError> {
        if io_budget.is_zero() {
            return Err(ProcessLeaseError::Invalid(
                "process lease fencing I/O budget must be nonzero".into(),
            ));
        }
        tokio::time::Instant::now()
            .checked_add(self.ttl)
            .and_then(|deadline| deadline.checked_add(io_budget))
            .ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease fencing deadline overflow".into())
            })
    }

    async fn bounded<T>(
        deadline: tokio::time::Instant,
        operation: &str,
        future: impl Future<Output = Result<T, ProcessLeaseError>>,
    ) -> Result<T, ProcessLeaseError> {
        if tokio::time::Instant::now() >= deadline {
            return Err(ProcessLeaseError::Deadline(format!(
                "deadline expired before {operation}"
            )));
        }
        tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| {
                ProcessLeaseError::Deadline(format!("deadline expired during {operation}"))
            })?
    }

    async fn recover_won_fence(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        head: ProcessLease,
        deadline: tokio::time::Instant,
    ) -> Result<ProcessLeaseFence, ProcessLeaseError> {
        let node = NodeId(participant.node_id);
        if head.node != node || head.owner == participant.boot_incarnation {
            return Err(ProcessLeaseError::Invalid(
                "process lease head has not superseded the requested incarnation".into(),
            ));
        }
        let store = self.store_for(node);
        let fence = Self::bounded(
            deadline,
            "locating process lease takeover evidence",
            store.find_takeover_from(participant.boot_incarnation),
        )
        .await?
        .ok_or_else(|| {
            ProcessLeaseError::Invalid(format!(
                "process lease takeover evidence for {} is missing",
                participant.boot_incarnation
            ))
        })?;
        if !self.verify_fence(&fence, deadline).await? {
            return Err(ProcessLeaseError::Invalid(
                "process lease takeover is no longer durably verifiable".into(),
            ));
        }
        Ok(fence)
    }

    /// Durably supersede an unchanged process incarnation after observing it for one full TTL.
    ///
    /// A retry after the create-only takeover won reconstructs the exact fence from the two
    /// retained history records. It never waits a second TTL for that already-durable result.
    ///
    /// # Errors
    /// Fails closed on renewal, deadline expiry, missing history, or any authority I/O failure.
    pub async fn fence_incarnation(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        deadline: tokio::time::Instant,
    ) -> Result<ProcessLeaseFence, ProcessLeaseError> {
        let node = NodeId(participant.node_id);
        if node.is_unassigned() || participant.boot_incarnation.is_nil() {
            return Err(ProcessLeaseError::Invalid(
                "process lease fence participant is not canonical".into(),
            ));
        }
        let store = self.store_for(node);
        let head = Self::bounded(deadline, "loading process lease fence head", store.load())
            .await?
            .ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease fence history is missing".into())
            })?;
        if head.owner != participant.boot_incarnation {
            return self.recover_won_fence(participant, head, deadline).await;
        }

        let observation = store.observe_rival(&head)?;
        let observation_until = tokio::time::Instant::now()
            .checked_add(self.ttl)
            .ok_or_else(|| ProcessLeaseError::Invalid("process lease TTL overflows time".into()))?;
        if observation_until >= deadline {
            return Err(ProcessLeaseError::Deadline(
                "deadline does not cover one full process lease TTL".into(),
            ));
        }
        tokio::time::sleep_until(observation_until).await;

        let mut revoker = Uuid::new_v4();
        while revoker.is_nil() || revoker == participant.boot_incarnation {
            revoker = Uuid::new_v4();
        }
        let outcome = Self::bounded(
            deadline,
            "publishing process lease takeover",
            store.try_takeover(revoker, &observation, now_millis()),
        )
        .await?;
        match outcome {
            ProcessLeaseOutcome::Acquired(successor) => {
                let fence = ProcessLeaseFence::new(head, successor)?;
                if !self.verify_fence(&fence, deadline).await? {
                    return Err(ProcessLeaseError::Invalid(
                        "won process lease takeover could not be verified".into(),
                    ));
                }
                Ok(fence)
            }
            ProcessLeaseOutcome::Held(current) if current.owner == participant.boot_incarnation => {
                Err(ProcessLeaseError::Invalid(
                    "process incarnation renewed during the full-TTL observation".into(),
                ))
            }
            ProcessLeaseOutcome::Held(current) => {
                self.recover_won_fence(participant, current, deadline).await
            }
        }
    }

    /// Verify that an exact boot incarnation is the current durable owner of its stable node.
    ///
    /// # Errors
    /// Fails closed on deadline expiry, missing takeover evidence, malformed state, or I/O.
    pub async fn verify_current_participant(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ProcessLeaseError> {
        self.verify_current_participant_identity(participant, None, deadline)
            .await
    }

    /// Verify an exact boot and process term against the current durable stable-node authority.
    ///
    /// # Errors
    /// Fails closed on deadline expiry, missing takeover evidence, malformed state, or I/O.
    pub async fn verify_current_participant_term(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        process_term: u64,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ProcessLeaseError> {
        if process_term == 0 {
            return Err(ProcessLeaseError::Invalid(
                "process lease term must be nonzero".into(),
            ));
        }
        self.verify_current_participant_identity(participant, Some(process_term), deadline)
            .await
    }

    async fn verify_current_participant_identity(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        process_term: Option<u64>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ProcessLeaseError> {
        let node = NodeId(participant.node_id);
        if node.is_unassigned() || participant.boot_incarnation.is_nil() {
            return Err(ProcessLeaseError::Invalid(
                "process lease participant is not canonical".into(),
            ));
        }
        let store = self.store_for(node);
        let Some(head) =
            Self::bounded(deadline, "loading current process lease", store.load()).await?
        else {
            return Ok(false);
        };
        if head.owner != participant.boot_incarnation
            || process_term.is_some_and(|term| head.term != term)
        {
            return Ok(false);
        }
        Self::bounded(
            deadline,
            "verifying current process term evidence",
            store.ensure_current_term_fence(&head),
        )
        .await?;
        let Some(after) =
            Self::bounded(deadline, "rechecking current process lease", store.load()).await?
        else {
            return Ok(false);
        };
        Ok(after.owner == head.owner
            && after.term == head.term
            && after.seq >= head.seq
            && process_term.is_none_or(|term| after.term == term))
    }

    /// Verify that both exact fence records remain present and the fenced owner is not current.
    ///
    /// # Errors
    /// Fails closed on deadline expiry, missing history, malformed records, or authority I/O.
    pub async fn verify_fence(
        &self,
        fence: &ProcessLeaseFence,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ProcessLeaseError> {
        if !fence.is_canonical() {
            return Err(ProcessLeaseError::Invalid(
                "process lease fence is not canonical".into(),
            ));
        }
        let store = self.store_for(fence.predecessor.node);
        let durable_fence = Self::bounded(
            deadline,
            "verifying indexed process lease fence",
            ProcessLeaseStore::load_fence(
                &store.store,
                fence.predecessor.node,
                fence.predecessor.owner,
            ),
        )
        .await?
        .ok_or_else(|| {
            ProcessLeaseError::Invalid("indexed process lease fence is missing".into())
        })?;
        let head = Self::bounded(deadline, "verifying process lease fence head", store.load())
            .await?
            .ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease fence durable head is missing".into())
            })?;
        Ok(durable_fence == *fence
            && head.seq >= fence.successor.seq
            && head.term >= fence.successor.term
            && (head.term != fence.successor.term || head.owner == fence.successor.owner)
            && head.owner != fence.predecessor.owner)
    }
}

/// Internal renewal timings for the stable-node lease.
#[derive(Debug, Clone, Copy)]
pub struct ProcessLeaseConfig {
    /// Lease lifetime.
    pub ttl: Duration,
    /// Renewal cadence.
    pub renew_interval: Duration,
}

impl Default for ProcessLeaseConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(15),
            renew_interval: Duration::from_secs(5),
        }
    }
}

/// Renews an already-acquired process lease and publishes terminal lease loss.
pub struct ProcessLeaseManager {
    store: Arc<ProcessLeaseStore>,
    owner: Uuid,
    config: ProcessLeaseConfig,
    initial_valid_until: std::time::Instant,
    live_tx: watch::Sender<bool>,
    deadline: Arc<LeaseDeadline>,
}

impl ProcessLeaseManager {
    /// Construct a renewal manager for an acquired lease.
    ///
    /// # Errors
    /// Rejects a lease that does not match the store namespace or owner.
    pub fn new(
        store: Arc<ProcessLeaseStore>,
        owner: Uuid,
        config: ProcessLeaseConfig,
        acquisition_started_at: Instant,
        initial: &ProcessLease,
    ) -> Result<Self, ProcessLeaseError> {
        initial.validate(store.node)?;
        let store_ttl = u64::try_from(store.ttl_ms)
            .ok()
            .filter(|ttl| *ttl > 0)
            .map(Duration::from_millis)
            .ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease TTL must be positive".into())
            })?;
        if initial.owner != owner
            || config.ttl.is_zero()
            || config.ttl != store_ttl
            || config.renew_interval.is_zero()
            || config.renew_interval >= config.ttl
        {
            return Err(ProcessLeaseError::Invalid(
                "renewal manager requires this boot's lease, the exact store TTL, and a renewal interval below TTL".into(),
            ));
        }
        let (live_tx, _live_rx) = watch::channel(true);
        let ttl = config.ttl;
        let now = Instant::now();
        if acquisition_started_at > now {
            return Err(ProcessLeaseError::Invalid(
                "process lease acquisition start is in the future".into(),
            ));
        }
        let initial_valid_until = acquisition_started_at
            .checked_add(ttl)
            .ok_or_else(|| ProcessLeaseError::Invalid("process lease TTL overflows time".into()))?;
        if now >= initial_valid_until {
            return Err(ProcessLeaseError::Invalid(
                "process lease acquisition response arrived after its local deadline".into(),
            ));
        }
        let deadline = Arc::new(LeaseDeadline::uninitialized());
        deadline.extend_until(initial_valid_until);
        if !deadline.is_live() {
            return Err(ProcessLeaseError::Invalid(
                "process lease acquisition response arrived after its local deadline".into(),
            ));
        }
        Ok(Self {
            store,
            owner,
            config,
            initial_valid_until,
            live_tx,
            deadline,
        })
    }

    /// Watch terminal ownership status.
    #[must_use]
    pub fn live_watch(&self) -> watch::Receiver<bool> {
        self.live_tx.subscribe()
    }

    /// Shared monotonic deadline for hot-path fencing.
    #[must_use]
    pub fn deadline(&self) -> Arc<LeaseDeadline> {
        Arc::clone(&self.deadline)
    }

    /// Spawn the renewal loop. Once ownership is uncertain past its expiry or a rival is observed,
    /// the watch becomes false and this manager never reacquires.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn spawn(
        self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut valid_until = self.initial_valid_until;
            let mut ticker = tokio::time::interval(self.config.renew_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The acquisition itself is the first successful tick.
            ticker.tick().await;
            loop {
                let now = std::time::Instant::now();
                if now >= valid_until {
                    self.deadline.fence();
                    self.live_tx.send_replace(false);
                    return;
                }
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        self.deadline.fence();
                        self.live_tx.send_replace(false);
                        return;
                    },
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(valid_until)) => {
                        self.deadline.fence();
                        self.live_tx.send_replace(false);
                        return;
                    }
                    _ = ticker.tick() => {}
                }

                let now = std::time::Instant::now();
                if now >= valid_until {
                    self.deadline.fence();
                    self.live_tx.send_replace(false);
                    return;
                }
                let attempt_started_at = Instant::now();
                let Some(attempt_valid_until) = attempt_started_at.checked_add(self.config.ttl)
                else {
                    self.deadline.fence();
                    self.live_tx.send_replace(false);
                    return;
                };
                let renewal = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(valid_until),
                    self.store.try_acquire(self.owner, now_millis()),
                )
                .await;
                match renewal {
                    Ok(Ok(ProcessLeaseOutcome::Acquired(_))) => {
                        let response_at = Instant::now();
                        if response_at >= valid_until || response_at >= attempt_valid_until {
                            self.deadline.fence();
                            self.live_tx.send_replace(false);
                            return;
                        }
                        valid_until = attempt_valid_until;
                        self.deadline.extend_until(attempt_valid_until);
                    }
                    Ok(Ok(ProcessLeaseOutcome::Held(rival))) => {
                        tracing::error!(
                            node = self.store.node.0,
                            owner = %rival.owner,
                            term = rival.term,
                            "stable node identity lease was lost"
                        );
                        self.deadline.fence();
                        self.live_tx.send_replace(false);
                        return;
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "stable node identity lease renewal failed");
                    }
                    Err(_) => {
                        self.deadline.fence();
                        self.live_tx.send_replace(false);
                        return;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use object_store::memory::InMemory;

    fn store(node: NodeId, ttl_ms: i64) -> ProcessLeaseStore {
        ProcessLeaseStore::new(Arc::new(InMemory::new()), node, ttl_ms)
    }

    #[tokio::test]
    async fn first_acquire() {
        let store = store(NodeId(7), 1_000);
        let owner = Uuid::from_u128(1);
        let ProcessLeaseOutcome::Acquired(lease) = store.try_acquire(owner, 10).await.unwrap()
        else {
            panic!("empty store must be acquired");
        };
        assert_eq!(lease.node, NodeId(7));
        assert_eq!(lease.owner, owner);
        assert_eq!(lease.term, 1);
        assert_eq!(lease.seq, 1);
        assert_eq!(lease.expires_at_ms, 1_010);
    }

    #[tokio::test]
    async fn same_incarnation_renews_without_changing_term() {
        let store = store(NodeId(7), 1_000);
        let owner = Uuid::from_u128(1);
        store.try_acquire(owner, 10).await.unwrap();
        let ProcessLeaseOutcome::Acquired(lease) = store.try_acquire(owner, 500).await.unwrap()
        else {
            panic!("live owner must renew");
        };
        assert_eq!(lease.term, 1);
        assert_eq!(lease.seq, 2);
        assert_eq!(lease.expires_at_ms, 1_500);
    }

    #[tokio::test]
    async fn live_rival_is_denied() {
        let store = store(NodeId(7), 1_000);
        let incumbent = Uuid::from_u128(1);
        store.try_acquire(incumbent, 10).await.unwrap();
        let ProcessLeaseOutcome::Held(lease) =
            store.try_acquire(Uuid::from_u128(2), 500).await.unwrap()
        else {
            panic!("live incumbent must not be replaced");
        };
        assert_eq!(lease.owner, incumbent);
        assert_eq!(lease.term, 1);
    }

    #[tokio::test]
    async fn expired_takeover_advances_term() {
        let store = store(NodeId(7), 10);
        store.try_acquire(Uuid::from_u128(1), 10).await.unwrap();
        let replacement = Uuid::from_u128(2);
        let ProcessLeaseOutcome::Held(incumbent) =
            store.try_acquire(replacement, 10_000).await.unwrap()
        else {
            panic!("rival timestamps must not authorize takeover");
        };
        let observation = store.observe_rival(&incumbent).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let ProcessLeaseOutcome::Acquired(lease) = store
            .try_takeover(replacement, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("expired identity must be replaceable");
        };
        assert_eq!(lease.owner, replacement);
        assert_eq!(lease.term, 2);
        assert_eq!(lease.seq, 2);
    }

    #[tokio::test]
    async fn shared_fencing_authority_fails_when_the_predecessor_renews() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority = Arc::new(
            ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(50)).unwrap(),
        );
        let store = authority.store_for(NodeId(7));
        let owner = Uuid::from_u128(71);
        store.try_acquire(owner, 1).await.unwrap();
        let participant = crate::checkpoint::CheckpointParticipant {
            node_id: 7,
            boot_incarnation: owner,
        };
        let fencing = authority.fence_incarnation(
            participant,
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
        tokio::pin!(fencing);
        tokio::select! {
            biased;
            result = &mut fencing => panic!("fencing completed before its full TTL: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        assert!(matches!(
            store.try_acquire(owner, 2).await.unwrap(),
            ProcessLeaseOutcome::Acquired(ProcessLease { seq: 2, .. })
        ));

        let error = fencing.await.unwrap_err();
        assert!(error.to_string().contains("renewed"), "{error}");
    }

    #[tokio::test]
    async fn shared_fencing_authority_recovers_and_verifies_its_exact_takeover() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority =
            ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(5)).unwrap();
        let store = authority.store_for(NodeId(7));
        let owner = Uuid::from_u128(71);
        store.try_acquire(owner, 1).await.unwrap();
        let participant = crate::checkpoint::CheckpointParticipant {
            node_id: 7,
            boot_incarnation: owner,
        };

        let fence = authority
            .fence_incarnation(
                participant,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(fence.predecessor.owner, owner);
        assert_ne!(fence.successor.owner, owner);
        assert!(authority
            .verify_fence(&fence, tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap());

        let recovered = authority
            .fence_incarnation(
                participant,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(recovered, fence);

        store
            .try_acquire(fence.successor.owner, now_millis())
            .await
            .unwrap();
        assert!(authority
            .verify_fence(&fence, tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap());
        assert_eq!(
            authority
                .fence_incarnation(
                    participant,
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap(),
            fence
        );
    }

    #[tokio::test]
    async fn pruning_preserves_every_takeover_boundary_but_removes_routine_renewals() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = ProcessLeaseStore::new(Arc::clone(&backing), NodeId(7), 1);
        let first = Uuid::from_u128(71);
        let second = Uuid::from_u128(72);
        let third = Uuid::from_u128(73);

        store.try_acquire(first, 1).await.unwrap();
        store.try_acquire(first, 2).await.unwrap();
        let ProcessLeaseOutcome::Acquired(first_head) = store.try_acquire(first, 3).await.unwrap()
        else {
            panic!("first process must renew");
        };
        let first_observation = store.observe_rival(&first_head).unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let ProcessLeaseOutcome::Acquired(second_start) = store
            .try_takeover(second, &first_observation, 4)
            .await
            .unwrap()
        else {
            panic!("second process must take over");
        };
        store.try_acquire(second, 5).await.unwrap();
        let ProcessLeaseOutcome::Acquired(second_head) =
            store.try_acquire(second, 6).await.unwrap()
        else {
            panic!("second process must renew");
        };
        let second_observation = store.observe_rival(&second_head).unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let ProcessLeaseOutcome::Acquired(third_start) = store
            .try_takeover(third, &second_observation, 7)
            .await
            .unwrap()
        else {
            panic!("third process must take over");
        };
        let ProcessLeaseOutcome::Acquired(third_head) = store.try_acquire(third, 8).await.unwrap()
        else {
            panic!("third process must renew");
        };

        ProcessLeaseStore::prune_history_batch(&backing, NodeId(7))
            .await
            .unwrap();
        assert_eq!(store.list_seqs().await.unwrap(), vec![7, 8]);
        let first_fence = ProcessLeaseFence::new(first_head, second_start).unwrap();
        assert_eq!(
            store.find_takeover_from(first).await.unwrap().unwrap(),
            first_fence
        );
        assert_eq!(
            store.find_takeover_from(second).await.unwrap().unwrap(),
            ProcessLeaseFence::new(second_head, third_start).unwrap()
        );
        assert_eq!(store.load().await.unwrap(), Some(third_head));
        let authority = ProcessLeaseAuthority::new(backing, Duration::from_millis(1)).unwrap();
        assert!(authority
            .verify_fence(
                &first_fence,
                tokio::time::Instant::now() + Duration::from_secs(1)
            )
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn oversized_history_can_prune_back_below_the_normal_scan_bound() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node = NodeId(7);
        let owner = Uuid::from_u128(71);
        let record_count = PROCESS_LEASE_MAX_LIST_RECORDS + 4;
        let expected_head = u64::try_from(record_count).unwrap();
        for sequence in 1..=record_count {
            let sequence = u64::try_from(sequence).unwrap();
            let lease = ProcessLease {
                node,
                owner,
                term: 1,
                seq: sequence,
                expires_at_ms: i64::try_from(sequence).unwrap(),
            };
            backing
                .put_opts(
                    &lease_path(node, sequence),
                    PutPayload::from(Bytes::from(serde_json::to_vec(&lease).unwrap())),
                    PutOptions {
                        mode: PutMode::Create,
                        ..PutOptions::default()
                    },
                )
                .await
                .unwrap();
        }
        let store = ProcessLeaseStore::new(Arc::clone(&backing), node, 1);
        assert!(store.list_seqs().await.is_err());

        assert!(!ProcessLeaseStore::prune_history_batch(&backing, node)
            .await
            .unwrap());

        let sequences = store.list_seqs().await.unwrap();
        assert_eq!(sequences.first(), Some(&257));
        assert_eq!(sequences.last(), Some(&expected_head));
        assert_eq!(store.load().await.unwrap().unwrap().seq, expected_head);
    }

    #[tokio::test]
    async fn shared_fencing_authority_rejects_missing_predecessor_history() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority =
            ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(5)).unwrap();
        let store = authority.store_for(NodeId(7));
        let owner = Uuid::from_u128(71);
        store.try_acquire(owner, 1).await.unwrap();
        let participant = crate::checkpoint::CheckpointParticipant {
            node_id: 7,
            boot_incarnation: owner,
        };
        let fence = authority
            .fence_incarnation(
                participant,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        ProcessLeaseStore::prune_history_batch(&backing, NodeId(7))
            .await
            .unwrap();
        backing
            .delete(&fence_path(NodeId(7), fence.predecessor.owner))
            .await
            .unwrap();
        backing
            .delete(&successor_fence_path(
                NodeId(7),
                fence.successor.owner,
                fence.successor.term,
            ))
            .await
            .unwrap();
        backing
            .delete(&lease_path(NodeId(7), fence.predecessor.seq))
            .await
            .unwrap();

        let error = authority
            .fence_incarnation(
                participant,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("missing"), "{error}");
    }

    #[tokio::test]
    async fn delayed_previous_owner_renewal_cannot_overwrite_takeover() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority = ProcessLeaseStore::new(Arc::clone(&object_store), NodeId(7), 1);
        let first_owner = Uuid::from_u128(1);
        let ProcessLeaseOutcome::Acquired(first) =
            authority.try_acquire(first_owner, 1).await.unwrap()
        else {
            panic!("first owner must acquire the lease");
        };
        let delayed_renewal = ProcessLease {
            node: first.node,
            owner: first.owner,
            term: first.term,
            seq: 2,
            expires_at_ms: 100,
        };
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let delayed_server_put = {
            let object_store = Arc::clone(&object_store);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                release.acquire().await.unwrap().forget();
                object_store
                    .put_opts(
                        &lease_path(NodeId(7), 2),
                        PutPayload::from(Bytes::from(
                            serde_json::to_vec(&delayed_renewal).unwrap(),
                        )),
                        PutOptions {
                            mode: PutMode::Create,
                            ..PutOptions::default()
                        },
                    )
                    .await
            })
        };

        let replacement = Uuid::from_u128(2);
        let ProcessLeaseOutcome::Held(incumbent) =
            authority.try_acquire(replacement, 10).await.unwrap()
        else {
            panic!("replacement must observe the incumbent");
        };
        let observation = authority.observe_rival(&incumbent).unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
        let ProcessLeaseOutcome::Acquired(takeover) = authority
            .try_takeover(replacement, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("replacement must win sequence two");
        };
        release.add_permits(1);
        assert!(matches!(
            delayed_server_put.await.unwrap(),
            Err(object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. })
        ));
        assert_eq!(authority.load().await.unwrap(), Some(takeover));
    }

    struct PutBarrierStore {
        inner: Arc<dyn ObjectStore>,
        path: OsPath,
        arrivals: Option<tokio::sync::Barrier>,
        delay_response: bool,
        committed: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
        conflict_as_precondition: bool,
    }

    impl std::fmt::Debug for PutBarrierStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("PutBarrierStore")
                .finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for PutBarrierStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("PutBarrierStore")
        }
    }

    #[async_trait]
    impl ObjectStore for PutBarrierStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if location == &self.path {
                if let Some(arrivals) = &self.arrivals {
                    arrivals.wait().await;
                }
            }
            let result = self.inner.put_opts(location, payload, options).await;
            if location == &self.path && self.delay_response {
                self.committed.add_permits(1);
                self.release
                    .acquire()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "PutBarrierStore",
                        source: Box::new(error),
                    })?
                    .forget();
            }
            if self.conflict_as_precondition
                && matches!(&result, Err(object_store::Error::AlreadyExists { .. }))
            {
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: Box::new(std::io::Error::other("injected create precondition")),
                });
            }
            result
        }

        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    struct GetBarrierStore {
        inner: Arc<dyn ObjectStore>,
        path: OsPath,
        armed: AtomicBool,
        entered: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
    }

    impl std::fmt::Debug for GetBarrierStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("GetBarrierStore")
                .finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for GetBarrierStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("GetBarrierStore")
        }
    }

    #[async_trait]
    impl ObjectStore for GetBarrierStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if location == &self.path && self.armed.swap(false, Ordering::AcqRel) {
                self.entered.add_permits(1);
                self.release
                    .acquire()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "GetBarrierStore",
                        source: Box::new(error),
                    })?
                    .forget();
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn participant_term_verification_rechecks_the_head_after_fence_evidence() {
        let node = NodeId(7);
        let first = Uuid::from_u128(71);
        let second = Uuid::from_u128(72);
        let third = Uuid::from_u128(73);
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = ProcessLeaseStore::new(Arc::clone(&backing), node, 1);
        let ProcessLeaseOutcome::Acquired(first_head) = store.try_acquire(first, 1).await.unwrap()
        else {
            panic!("first process must acquire the lease");
        };
        let first_observation = store.observe_rival(&first_head).unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let ProcessLeaseOutcome::Acquired(second_head) = store
            .try_takeover(second, &first_observation, 2)
            .await
            .unwrap()
        else {
            panic!("second process must take over the lease");
        };
        let second_observation = store.observe_rival(&second_head).unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;

        let gated = Arc::new(GetBarrierStore {
            inner: Arc::clone(&backing),
            path: successor_fence_path(node, second, second_head.term),
            armed: AtomicBool::new(true),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let authority_store: Arc<dyn ObjectStore> = gated.clone();
        let authority = Arc::new(
            ProcessLeaseAuthority::new(authority_store, Duration::from_millis(1)).unwrap(),
        );
        let participant = crate::checkpoint::CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: second,
        };
        let verify = {
            let authority = Arc::clone(&authority);
            tokio::spawn(async move {
                authority
                    .verify_current_participant_term(
                        participant,
                        second_head.term,
                        tokio::time::Instant::now() + Duration::from_secs(1),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), gated.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(matches!(
            store
                .try_takeover(third, &second_observation, 3)
                .await
                .unwrap(),
            ProcessLeaseOutcome::Acquired(ProcessLease { owner, term: 3, .. }) if owner == third
        ));
        gated.release.add_permits(1);

        assert!(!verify.await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn create_cas_has_one_winner() {
        let node = NodeId(7);
        let racing: Arc<dyn ObjectStore> = Arc::new(PutBarrierStore {
            inner: Arc::new(InMemory::new()),
            path: lease_path(node, 1),
            arrivals: Some(tokio::sync::Barrier::new(2)),
            delay_response: false,
            committed: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            conflict_as_precondition: true,
        });
        let first = ProcessLeaseStore::new(Arc::clone(&racing), node, 1_000);
        let second = ProcessLeaseStore::new(racing, node, 1_000);
        let (left, right) = tokio::join!(
            first.try_acquire(Uuid::from_u128(1), 10),
            second.try_acquire(Uuid::from_u128(2), 10)
        );
        let (left, right) = (left.unwrap(), right.unwrap());
        let winners = usize::from(matches!(&left, ProcessLeaseOutcome::Acquired(_)))
            + usize::from(matches!(&right, ProcessLeaseOutcome::Acquired(_)));
        assert_eq!(winners, 1);
        let durable = first.load().await.unwrap().unwrap();
        assert!(matches!(
            (left, right),
            (ProcessLeaseOutcome::Acquired(ref won), ProcessLeaseOutcome::Held(ref held))
                | (ProcessLeaseOutcome::Held(ref held), ProcessLeaseOutcome::Acquired(ref won))
                if won == &durable && held == &durable
        ));
    }

    #[tokio::test]
    async fn local_filesystem_supports_create_only_renewal() {
        let temp = tempfile::tempdir().unwrap();
        let filesystem: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp.path()).unwrap());
        let store = ProcessLeaseStore::new(filesystem, NodeId(7), 1_000);
        let owner = Uuid::from_u128(1);
        assert!(matches!(
            store.try_acquire(owner, 10).await.unwrap(),
            ProcessLeaseOutcome::Acquired(_)
        ));
        assert!(matches!(
            store.try_acquire(owner, 500).await.unwrap(),
            ProcessLeaseOutcome::Acquired(ProcessLease { seq: 2, .. })
        ));
    }

    #[tokio::test]
    async fn renewal_history_keeps_only_latest_and_predecessor() {
        let store = store(NodeId(7), 1_000);
        let owner = Uuid::from_u128(1);
        for now in 0..8 {
            assert!(matches!(
                store.try_acquire(owner, now).await.unwrap(),
                ProcessLeaseOutcome::Acquired(_)
            ));
        }
        ProcessLeaseStore::prune_history(&store.store, NodeId(7))
            .await
            .unwrap();
        assert_eq!(store.list_seqs().await.unwrap(), vec![7, 8]);
        assert_eq!(store.load().await.unwrap().unwrap().seq, 8);
    }

    #[tokio::test]
    async fn an_unhealthy_pruner_is_repaired_before_the_next_renewal() {
        let store = store(NodeId(7), 1_000);
        let owner = Uuid::from_u128(1);
        for now in 0..8 {
            store.try_acquire(owner, now).await.unwrap();
        }
        store.prune_healthy.store(false, Ordering::Release);

        assert!(matches!(
            store.try_acquire(owner, 9).await.unwrap(),
            ProcessLeaseOutcome::Acquired(ProcessLease { seq: 9, .. })
        ));
        assert_eq!(store.list_seqs().await.unwrap(), vec![7, 8, 9]);
        assert!(store.prune_healthy.load(Ordering::Acquire));
    }

    #[test]
    fn renewal_manager_requires_the_exact_store_ttl() {
        let store = Arc::new(ProcessLeaseStore::new(
            Arc::new(InMemory::new()),
            NodeId(7),
            10,
        ));
        let owner = Uuid::from_u128(1);
        let initial = ProcessLease {
            node: NodeId(7),
            owner,
            term: 1,
            seq: 1,
            expires_at_ms: 10,
        };
        for ttl in [
            Duration::from_millis(20),
            Duration::from_millis(10) + Duration::from_nanos(1),
        ] {
            let Err(error) = ProcessLeaseManager::new(
                Arc::clone(&store),
                owner,
                ProcessLeaseConfig {
                    ttl,
                    renew_interval: Duration::from_millis(2),
                },
                Instant::now(),
                &initial,
            ) else {
                panic!("mismatched manager TTL must be rejected");
            };
            assert!(error.to_string().contains("exact store TTL"), "{error}");
        }
    }

    #[tokio::test]
    async fn delayed_acquisition_response_cannot_publish_a_fresh_local_deadline() {
        let node = NodeId(7);
        let owner = Uuid::from_u128(1);
        let ttl = Duration::from_millis(30);
        let delayed = Arc::new(PutBarrierStore {
            inner: Arc::new(InMemory::new()),
            path: lease_path(node, 1),
            arrivals: None,
            delay_response: true,
            committed: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            conflict_as_precondition: false,
        });
        let object_store: Arc<dyn ObjectStore> = delayed.clone();
        let store = Arc::new(ProcessLeaseStore::new(object_store, node, 30));
        let acquisition_store = Arc::clone(&store);
        let acquisition_started_at = Instant::now();
        let acquisition =
            tokio::spawn(async move { acquisition_store.try_acquire(owner, 0).await });

        tokio::time::timeout(Duration::from_secs(1), delayed.committed.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(matches!(
            store.load().await.unwrap(),
            Some(ProcessLease { owner: current, .. }) if current == owner
        ));
        tokio::time::sleep(ttl + Duration::from_millis(20)).await;
        delayed.release.add_permits(1);
        let ProcessLeaseOutcome::Acquired(initial) =
            tokio::time::timeout(Duration::from_secs(1), acquisition)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        else {
            panic!("delayed durable acquisition must still return its committed lease");
        };

        let error = ProcessLeaseManager::new(
            store,
            owner,
            ProcessLeaseConfig {
                ttl,
                renew_interval: Duration::from_millis(5),
            },
            acquisition_started_at,
            &initial,
        )
        .err()
        .expect("an acquisition response after its TTL must fail closed");
        assert!(
            error.to_string().contains("response arrived after"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn delayed_first_poll_cannot_renew_after_initial_deadline() {
        let store = Arc::new(ProcessLeaseStore::new(
            Arc::new(InMemory::new()),
            NodeId(7),
            10,
        ));
        let owner = Uuid::from_u128(1);
        let acquisition_started_at = Instant::now();
        let ProcessLeaseOutcome::Acquired(initial) = store.try_acquire(owner, 0).await.unwrap()
        else {
            panic!("initial process lease must be acquired");
        };
        let manager = ProcessLeaseManager::new(
            Arc::clone(&store),
            owner,
            ProcessLeaseConfig {
                ttl: Duration::from_millis(10),
                renew_interval: Duration::from_millis(2),
            },
            acquisition_started_at,
            &initial,
        )
        .unwrap();
        let deadline = manager.deadline();
        let live = manager.live_watch();

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!deadline.is_live());
        tokio::time::timeout(
            Duration::from_millis(100),
            manager.spawn(tokio_util::sync::CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(!*live.borrow());
        assert_eq!(store.load().await.unwrap(), Some(initial));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn shutdown_fences_the_published_process_grant() {
        let store = Arc::new(ProcessLeaseStore::new(
            Arc::new(InMemory::new()),
            NodeId(7),
            100,
        ));
        let owner = Uuid::from_u128(1);
        let acquisition_started_at = Instant::now();
        let ProcessLeaseOutcome::Acquired(initial) = store.try_acquire(owner, 0).await.unwrap()
        else {
            panic!("initial process lease must be acquired");
        };
        let manager = ProcessLeaseManager::new(
            store,
            owner,
            ProcessLeaseConfig {
                ttl: Duration::from_millis(100),
                renew_interval: Duration::from_millis(20),
            },
            acquisition_started_at,
            &initial,
        )
        .unwrap();
        let deadline = manager.deadline();
        let live = manager.live_watch();
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();

        manager.spawn(shutdown).await.unwrap();

        assert!(!deadline.is_live());
        assert!(!*live.borrow());
    }
}
