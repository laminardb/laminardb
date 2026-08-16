//! Append-only lease storage, history retention, and indexed takeover certificates.

use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use uuid::Uuid;

use crate::cluster::discovery::NodeId;

use super::{
    fence_path, lease_path, lease_prefix, sequence_from_path, successor_fence_path, ProcessLease,
    ProcessLeaseError, ProcessLeaseFence, ProcessLeaseObservation, ProcessLeaseOutcome,
    MAX_PROCESS_LEASE_FENCE_BYTES, MAX_PROCESS_LEASE_RECORD_BYTES,
    PROCESS_LEASE_HEAD_READ_ATTEMPTS, PROCESS_LEASE_HISTORY_TO_RETAIN,
    PROCESS_LEASE_MAX_LIST_RECORDS, PROCESS_LEASE_MAX_PRUNE_BATCHES,
    PROCESS_LEASE_PRUNE_BATCH_RECORDS, PROCESS_LEASE_PRUNE_IO_TIMEOUT,
    PROCESS_LEASE_PRUNE_READ_CONCURRENCY, PROCESS_LEASE_WRITES_PER_PRUNE,
};

/// Append-only object-store authority for one stable node identity.
pub struct ProcessLeaseStore {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) node: NodeId,
    pub(super) ttl_ms: i64,
    prune_running: Arc<AtomicBool>,
    pub(super) prune_healthy: Arc<AtomicBool>,
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

    pub(super) async fn list_seqs(&self) -> Result<Vec<u64>, ProcessLeaseError> {
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

    pub(super) async fn prune_history(
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

    pub(super) async fn prune_history_batch(
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

    pub(super) async fn load_fence(
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

    pub(super) async fn find_takeover_from(
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

    pub(super) async fn ensure_current_term_fence(
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
