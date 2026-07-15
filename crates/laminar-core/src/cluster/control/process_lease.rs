//! Durable ownership of one stable cluster node identity.
//!
//! Each renewal appends a create-only sequence object. This gives local filesystems and object
//! stores the same compare-and-set boundary without relying on backend-specific entity tags.

use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
const PROCESS_LEASE_HEAD_READ_ATTEMPTS: usize = 4;
const PROCESS_LEASE_HISTORY_TO_RETAIN: usize = 2;
const PROCESS_LEASE_MAX_LIST_RECORDS: usize = 4096;
const PROCESS_LEASE_PRUNE_BATCH_RECORDS: usize = 256;
const PROCESS_LEASE_MAX_PRUNE_BATCHES: usize = 4;
const PROCESS_LEASE_PRUNE_IO_TIMEOUT: Duration = Duration::from_secs(5);

fn lease_prefix(node: NodeId) -> String {
    format!("control/process-lease/node={}/", node.0)
}

fn lease_path(node: NodeId, seq: u64) -> OsPath {
    OsPath::from(format!("{}v{seq:016}.json", lease_prefix(node)))
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

fn retain_oldest_process_lease_record(
    oldest: &mut BinaryHeap<(u64, String)>,
    sequence: u64,
    path: &OsPath,
) {
    let candidate = (sequence, path.to_string());
    if oldest.len() < PROCESS_LEASE_PRUNE_BATCH_RECORDS {
        oldest.push(candidate);
    } else if oldest.peek().is_some_and(|largest| &candidate < largest) {
        oldest.pop();
        oldest.push(candidate);
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
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
}

/// Append-only object-store authority for one stable node identity.
pub struct ProcessLeaseStore {
    store: Arc<dyn ObjectStore>,
    node: NodeId,
    ttl_ms: i64,
    prune_running: Arc<AtomicBool>,
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
        }
    }

    async fn list_seqs(&self) -> Result<Vec<u64>, ProcessLeaseError> {
        let prefix_string = lease_prefix(self.node);
        let prefix = OsPath::from(prefix_string.clone());
        let mut entries = self.store.list(Some(&prefix));
        let mut sequences = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| ProcessLeaseError::Io(error.to_string()))?;
            if sequences.len() == PROCESS_LEASE_MAX_LIST_RECORDS {
                return Err(ProcessLeaseError::Invalid(format!(
                    "process lease history exceeds the fixed {PROCESS_LEASE_MAX_LIST_RECORDS}-record scan bound"
                )));
            }
            sequences.push(sequence_from_path(self.node, &entry.location)?);
        }
        sequences.sort_unstable();
        sequences.dedup();
        Ok(sequences)
    }

    fn schedule_history_prune(&self) {
        if self
            .prune_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.prune_running.store(false, Ordering::Release);
            return;
        };
        let store = Arc::clone(&self.store);
        let node = self.node;
        let prune_running = Arc::clone(&self.prune_running);
        runtime.spawn(async move {
            let prune = async {
                for _ in 0..PROCESS_LEASE_MAX_PRUNE_BATCHES {
                    let done = tokio::time::timeout(
                        PROCESS_LEASE_PRUNE_IO_TIMEOUT,
                        Self::prune_history_batch(&store, node),
                    )
                    .await
                    .map_err(|_| {
                        ProcessLeaseError::Io("process lease history prune timed out".into())
                    })??;
                    if done {
                        return Ok(());
                    }
                    tokio::task::yield_now().await;
                }
                Err(ProcessLeaseError::Io(
                    "process lease history still exceeds the bounded prune budget".into(),
                ))
            }
            .await;
            if let Err(error) = prune {
                tracing::warn!(node = node.0, %error, "process lease history prune failed");
            }
            prune_running.store(false, Ordering::Release);
        });
    }

    async fn prune_history_batch(
        store: &Arc<dyn ObjectStore>,
        node: NodeId,
    ) -> Result<bool, ProcessLeaseError> {
        let prefix_string = lease_prefix(node);
        let prefix = OsPath::from(prefix_string);
        let mut entries = store.list(Some(&prefix));
        let mut oldest = BinaryHeap::with_capacity(PROCESS_LEASE_PRUNE_BATCH_RECORDS);
        let mut total = 0usize;
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| ProcessLeaseError::Io(error.to_string()))?;
            let sequence = sequence_from_path(node, &entry.location)?;
            total = total.saturating_add(1);
            retain_oldest_process_lease_record(&mut oldest, sequence, &entry.location);
        }
        let delete_count = total
            .saturating_sub(PROCESS_LEASE_HISTORY_TO_RETAIN)
            .min(PROCESS_LEASE_PRUNE_BATCH_RECORDS);
        let deletions = futures::stream::iter(
            oldest
                .into_sorted_vec()
                .into_iter()
                .take(delete_count)
                .map(|(_, path)| Ok::<_, object_store::Error>(OsPath::from(path))),
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
        Ok(total.saturating_sub(delete_count) <= PROCESS_LEASE_HISTORY_TO_RETAIN)
    }

    async fn read_record(&self, sequence: u64) -> Result<Option<ProcessLease>, ProcessLeaseError> {
        let result = match self.store.get(&lease_path(self.node, sequence)).await {
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
        lease.validate(self.node)?;
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
                    self.schedule_history_prune();
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
        let current = self.load().await?;
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
                self.schedule_history_prune();
                Ok(ProcessLeaseOutcome::Acquired(candidate))
            }
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                let winner = self.load().await?.ok_or_else(|| {
                    ProcessLeaseError::Io("CAS conflict but the winner was not readable".into())
                })?;
                self.schedule_history_prune();
                if winner.owner == owner {
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
                self.schedule_history_prune();
                Ok(ProcessLeaseOutcome::Acquired(candidate))
            }
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                let winner = self.load().await?.ok_or_else(|| {
                    ProcessLeaseError::Io("takeover CAS winner was not readable".into())
                })?;
                self.schedule_history_prune();
                Ok(ProcessLeaseOutcome::Held(winner))
            }
            Err(error) => Err(ProcessLeaseError::Io(error.to_string())),
        }
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
        let initial_valid_until = std::time::Instant::now()
            .checked_add(ttl)
            .ok_or_else(|| ProcessLeaseError::Invalid("process lease TTL overflows time".into()))?;
        Ok(Self {
            store,
            owner,
            config,
            initial_valid_until,
            live_tx,
            deadline: Arc::new(LeaseDeadline::live_for(ttl)),
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
                let Some(remaining) = valid_until.checked_duration_since(now) else {
                    self.deadline.fence();
                    self.live_tx.send_replace(false);
                    return;
                };
                let renewal = tokio::time::timeout(
                    remaining,
                    self.store.try_acquire(self.owner, now_millis()),
                )
                .await;
                match renewal {
                    Ok(Ok(ProcessLeaseOutcome::Acquired(_))) => {
                        let Some(extended) = std::time::Instant::now().checked_add(self.config.ttl)
                        else {
                            self.deadline.fence();
                            self.live_tx.send_replace(false);
                            return;
                        };
                        valid_until = extended;
                        self.deadline.extend(self.config.ttl);
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

    #[test]
    fn prune_selection_finds_global_oldest_records_in_shuffled_order() {
        let node = NodeId(7);
        let mut oldest = BinaryHeap::new();
        for index in 0..300u64 {
            let sequence = (index * 73) % 300 + 1;
            retain_oldest_process_lease_record(&mut oldest, sequence, &lease_path(node, sequence));
        }
        assert_eq!(
            oldest
                .into_sorted_vec()
                .into_iter()
                .map(|(sequence, _)| sequence)
                .collect::<Vec<_>>(),
            (1..=u64::try_from(PROCESS_LEASE_PRUNE_BATCH_RECORDS).unwrap()).collect::<Vec<_>>()
        );
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
            Err(object_store::Error::AlreadyExists { .. })
                | Err(object_store::Error::Precondition { .. })
        ));
        assert_eq!(authority.load().await.unwrap(), Some(takeover));
    }

    struct PutBarrierStore {
        inner: Arc<dyn ObjectStore>,
        path: OsPath,
        arrivals: tokio::sync::Barrier,
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
                self.arrivals.wait().await;
            }
            let result = self.inner.put_opts(location, payload, options).await;
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

    #[tokio::test]
    async fn create_cas_has_one_winner() {
        let node = NodeId(7);
        let racing: Arc<dyn ObjectStore> = Arc::new(PutBarrierStore {
            inner: Arc::new(InMemory::new()),
            path: lease_path(node, 1),
            arrivals: tokio::sync::Barrier::new(2),
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
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let sequences = store.list_seqs().await.unwrap();
                if sequences == vec![7, 8] {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.load().await.unwrap().unwrap().seq, 8);
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
                &initial,
            ) else {
                panic!("mismatched manager TTL must be rejected");
            };
            assert!(error.to_string().contains("exact store TTL"), "{error}");
        }
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
