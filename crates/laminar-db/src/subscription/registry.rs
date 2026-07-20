//! Per-object shared log for `SUBSCRIBE`.
//!
//! Publication and attachment use the same mutex. Portals retain only a
//! sequence cursor and a wake-up receiver. Arrow allocations are charged once
//! and shared by the byte-bounded log and any frame currently in transit.

#![allow(clippy::disallowed_types)] // subscription setup is a cold path

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use laminar_core::state::CheckpointAttempt;
use parking_lot::{Mutex, RwLock};
use tokio::sync::watch;

pub(super) const MAX_LIVE_BATCH_BYTES: usize = 8 * 1024 * 1024;

// Retention is a replay policy, not permission for unbounded live buffering.
// Two maximum-sized batches allow a portal to make progress while keeping the
// process-wide exposure finite when user retention is disabled.
const INTERNAL_LIVE_LOG_BYTES: usize = MAX_LIVE_BATCH_BYTES * 2;
const PROCESS_SUBSCRIPTION_BYTES: usize = 256 * 1024 * 1024;
const ENTRY_ACCOUNTING_BYTES: usize = 64;
const BARRIER_ENTRY_BYTES: usize = ENTRY_ACCOUNTING_BYTES + std::mem::size_of::<u64>() * 3;

static PROCESS_SUBSCRIPTION_BUDGET: OnceLock<Arc<SubscriptionMemoryBudget>> = OnceLock::new();

struct SubscriptionMemoryBudget {
    limit: usize,
    used: AtomicUsize,
}

struct ChargedUpdateInner {
    update: Option<MvUpdate>,
    bytes: usize,
    budget: Arc<SubscriptionMemoryBudget>,
}

impl Drop for ChargedUpdateInner {
    fn drop(&mut self) {
        {
            let _update = self.update.take();
        }
        self.budget.release(self.bytes);
    }
}

#[derive(Clone)]
pub(super) struct ChargedUpdate(Arc<ChargedUpdateInner>);

impl ChargedUpdate {
    /// Takes ownership of capacity that was reserved before this value was built.
    fn from_reserved(
        update: MvUpdate,
        bytes: usize,
        budget: Arc<SubscriptionMemoryBudget>,
    ) -> Self {
        Self(Arc::new(ChargedUpdateInner {
            update: Some(update),
            bytes,
            budget,
        }))
    }

    pub(super) fn as_ref(&self) -> &MvUpdate {
        self.0
            .update
            .as_ref()
            .expect("charged subscription update is present until final drop")
    }
}

impl SubscriptionMemoryBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                (bytes <= self.limit.saturating_sub(used)).then(|| used.saturating_add(bytes))
            })
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let released = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_sub(bytes)
            });
        debug_assert!(released.is_ok(), "subscription memory released twice");
    }

    #[cfg(test)]
    fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub(crate) enum MvUpdate {
    Batch(RecordBatch),
    Barrier {
        epoch: u64,
        checkpoint_id: u64,
        /// First shared-log sequence not certified by this checkpoint.
        through_sequence: u64,
    },
    Error(String),
}

/// Where a new subscriber should start reading.
#[derive(Clone, Copy, Debug)]
pub enum SubscribeStart {
    /// See only entries sequenced after attachment.
    Tail,
    /// Replay entries strictly after the retained barrier with `epoch == n`.
    AsOfEpoch(u64),
}

#[derive(Debug)]
pub(crate) enum SubscriptionOpenError {
    ReplayPruned {
        /// Earliest barrier epoch eligible for replay; `0` if none is retained.
        earliest_retained: u64,
    },
    EpochNotCommitted {
        requested: u64,
        latest_committed: Option<u64>,
    },
    Capacity {
        attached: usize,
        limit: usize,
    },
}

struct SequencedUpdate {
    sequence: u64,
    bytes: usize,
    update: ChargedUpdate,
}

struct StreamLog {
    inner: Mutex<StreamLogInner>,
    wake: watch::Sender<()>,
    budget: Arc<SubscriptionMemoryBudget>,
}

struct StreamLogInner {
    entries: VecDeque<SequencedUpdate>,
    bytes: usize,
    retention_cap: usize,
    retention_floor: u64,
    retention_bytes: usize,
    next_sequence: u64,
    next_reader_id: u64,
    readers: HashMap<u64, u64>,
    latest_committed_epoch: Option<u64>,
    terminal_error: Option<String>,
    reserved_marker: Option<CheckpointAttempt>,
}

#[must_use]
enum AppendOutcome {
    Stored,
    Unobserved,
    Rejected(String),
}

#[must_use]
enum ReservedAppendOutcome {
    Stored,
    Unobserved,
    Rejected(String),
}

impl StreamLog {
    fn new(
        retention_cap: usize,
        budget: Arc<SubscriptionMemoryBudget>,
        latest_committed_epoch: Option<u64>,
    ) -> Self {
        Self::new_at(retention_cap, budget, latest_committed_epoch, 0)
    }

    fn new_at(
        retention_cap: usize,
        budget: Arc<SubscriptionMemoryBudget>,
        latest_committed_epoch: Option<u64>,
        next_sequence: u64,
    ) -> Self {
        let (wake, _) = watch::channel(());
        Self {
            inner: Mutex::new(StreamLogInner {
                entries: VecDeque::new(),
                bytes: 0,
                retention_cap,
                retention_floor: next_sequence,
                retention_bytes: 0,
                next_sequence,
                next_reader_id: 0,
                readers: HashMap::new(),
                latest_committed_epoch,
                terminal_error: None,
                reserved_marker: None,
            }),
            wake,
            budget,
        }
    }

    fn append(&self, update: MvUpdate) -> AppendOutcome {
        let entry_bytes = approx_size(&update);
        let mut inner = self.inner.lock();
        debug_assert!(!matches!(&update, MvUpdate::Barrier { .. }));
        if let Some(message) = &inner.terminal_error {
            return AppendOutcome::Rejected(message.clone());
        }
        if inner.retention_cap == 0 && inner.readers.is_empty() {
            return AppendOutcome::Unobserved;
        }

        let storage_cap = storage_cap(inner.retention_cap);
        if entry_bytes > storage_cap {
            let message = format!(
                "subscription entry requires {entry_bytes} bytes, exceeding the internal shared-log limit of {storage_cap} bytes"
            );
            return self.reject_append(inner, message);
        }

        let required_sequence_space = if inner.reserved_marker.is_some() {
            2
        } else {
            1
        };
        if inner
            .next_sequence
            .checked_add(required_sequence_space)
            .is_none()
        {
            return self.reject_append(inner, "subscription sequence space exhausted".into());
        }
        let Some(next_sequence) = inner.next_sequence.checked_add(1) else {
            unreachable!("sequence-space preflight admitted the next sequence");
        };

        reclaim_consumed_prefix(&mut inner);
        let available_locally = storage_cap.saturating_sub(entry_bytes);
        evict_to_byte_target(&mut inner, available_locally);

        if !self.budget.try_reserve(entry_bytes) {
            let message = format!(
                "subscription process memory budget exhausted ({} byte limit); update was not delivered",
                self.budget.limit
            );
            return self.reject_append(inner, message);
        }

        let sequence = inner.next_sequence;
        inner.entries.push_back(SequencedUpdate {
            sequence,
            bytes: entry_bytes,
            update: ChargedUpdate::from_reserved(update, entry_bytes, Arc::clone(&self.budget)),
        });
        inner.next_sequence = next_sequence;
        inner.bytes = inner.bytes.saturating_add(entry_bytes);
        retain_appended_entry(&mut inner, sequence, entry_bytes);
        reclaim_consumed_prefix(&mut inner);
        drop(inner);
        self.wake.send_modify(|_| {});
        AppendOutcome::Stored
    }

    fn reject_append(
        &self,
        mut inner: parking_lot::MutexGuard<'_, StreamLogInner>,
        message: String,
    ) -> AppendOutcome {
        if inner.reserved_marker.is_some() {
            return AppendOutcome::Rejected(message);
        }
        self.terminate_locked(&mut inner, message.clone());
        drop(inner);
        self.wake.send_modify(|_| {});
        AppendOutcome::Rejected(message)
    }

    fn reserve_marker(&self, attempt: CheckpointAttempt) -> Result<Option<u64>, String> {
        let mut inner = self.inner.lock();
        if inner.terminal_error.is_some() || (inner.retention_cap == 0 && inner.readers.is_empty())
        {
            return Ok(None);
        }
        if let Some(existing) = inner.reserved_marker {
            return Err(format!(
                "subscription marker already reserved for epoch={} checkpoint_id={}",
                existing.epoch, existing.checkpoint_id
            ));
        }
        if inner.next_sequence.checked_add(1).is_none() {
            return Err("subscription sequence space cannot admit a checkpoint marker".into());
        }
        let through_sequence = inner.next_sequence;
        inner.reserved_marker = Some(attempt);
        Ok(Some(through_sequence))
    }

    fn cancel_marker(&self, attempt: CheckpointAttempt) -> bool {
        let mut inner = self.inner.lock();
        if inner.reserved_marker != Some(attempt) {
            return false;
        }
        inner.reserved_marker = None;
        true
    }

    fn append_reserved_marker(
        &self,
        attempt: CheckpointAttempt,
        through_sequence: u64,
    ) -> ReservedAppendOutcome {
        let mut inner = self.inner.lock();
        if inner.reserved_marker != Some(attempt) {
            return ReservedAppendOutcome::Rejected(format!(
                "subscription marker reservation mismatch for epoch={} checkpoint_id={}",
                attempt.epoch, attempt.checkpoint_id
            ));
        }
        inner.reserved_marker = None;

        if let Some(message) = &inner.terminal_error {
            return ReservedAppendOutcome::Rejected(format!(
                "subscription log terminated before checkpoint marker publication: {message}"
            ));
        }
        inner.latest_committed_epoch = Some(
            inner
                .latest_committed_epoch
                .map_or(attempt.epoch, |latest| latest.max(attempt.epoch)),
        );
        if inner.retention_cap == 0 && inner.readers.is_empty() {
            return ReservedAppendOutcome::Unobserved;
        }
        if through_sequence > inner.next_sequence {
            return ReservedAppendOutcome::Rejected(format!(
                "subscription checkpoint cut cursor {through_sequence} is ahead of log cursor {}",
                inner.next_sequence
            ));
        }
        let Some(next_sequence) = inner.next_sequence.checked_add(1) else {
            return ReservedAppendOutcome::Rejected(
                "subscription sequence space exhausted before checkpoint marker publication".into(),
            );
        };

        let storage_cap = storage_cap(inner.retention_cap);
        if BARRIER_ENTRY_BYTES > storage_cap {
            return ReservedAppendOutcome::Rejected(format!(
                "subscription checkpoint marker requires {BARRIER_ENTRY_BYTES} bytes, exceeding the internal shared-log limit of {storage_cap} bytes"
            ));
        }
        reclaim_consumed_prefix(&mut inner);
        evict_to_byte_target(&mut inner, storage_cap.saturating_sub(BARRIER_ENTRY_BYTES));

        let sequence = inner.next_sequence;
        inner.entries.push_back(SequencedUpdate {
            sequence,
            bytes: BARRIER_ENTRY_BYTES,
            update: ChargedUpdate::from_reserved(
                MvUpdate::Barrier {
                    epoch: attempt.epoch,
                    checkpoint_id: attempt.checkpoint_id,
                    through_sequence,
                },
                BARRIER_ENTRY_BYTES,
                Arc::clone(&self.budget),
            ),
        });
        inner.next_sequence = next_sequence;
        inner.bytes = inner.bytes.saturating_add(BARRIER_ENTRY_BYTES);
        retain_appended_entry(&mut inner, sequence, BARRIER_ENTRY_BYTES);
        reclaim_consumed_prefix(&mut inner);
        drop(inner);
        self.wake.send_modify(|_| {});
        ReservedAppendOutcome::Stored
    }

    fn subscribe(
        self: &Arc<Self>,
        start: SubscribeStart,
    ) -> Result<SubscriptionReader, SubscriptionOpenError> {
        let mut inner = self.inner.lock();
        let attached = inner.readers.len();
        if attached >= super::MAX_SUBSCRIBERS_PER_MV {
            return Err(SubscriptionOpenError::Capacity {
                attached,
                limit: super::MAX_SUBSCRIBERS_PER_MV,
            });
        }

        let (cursor, skip_barrier) = match (inner.terminal_error.is_some(), start) {
            (true, _) | (false, SubscribeStart::Tail) => (inner.next_sequence, None),
            (false, SubscribeStart::AsOfEpoch(epoch)) => {
                let (cursor, barrier_sequence) = cursor_after_retained_epoch(&inner, epoch)?;
                (cursor, Some((epoch, barrier_sequence)))
            }
        };
        let wake = self.wake.subscribe();
        let mut reader_id = inner.next_reader_id;
        while inner.readers.contains_key(&reader_id) {
            reader_id = reader_id.wrapping_add(1);
        }
        inner.next_reader_id = reader_id.wrapping_add(1);
        inner.readers.insert(reader_id, cursor);
        drop(inner);

        Ok(SubscriptionReader {
            log: Arc::clone(self),
            reader_id,
            cursor,
            skip_barrier,
            wake,
            registered: true,
        })
    }

    fn set_retention_cap(&self, retention_cap: usize) {
        let mut inner = self.inner.lock();
        let previous_head = head_sequence(&inner);
        inner.retention_cap = retention_cap;
        recompute_retention_suffix(&mut inner);
        reclaim_consumed_prefix(&mut inner);
        evict_to_byte_target(&mut inner, storage_cap(retention_cap));
        let head_changed = head_sequence(&inner) != previous_head;
        drop(inner);
        if head_changed {
            self.wake.send_modify(|_| {});
        }
    }

    fn terminate(&self, message: &str) {
        let mut inner = self.inner.lock();
        self.terminate_locked(&mut inner, message.to_owned());
        drop(inner);
        self.wake.send_modify(|_| {});
    }

    fn terminate_and_replace(&self, message: &str, latest_committed_epoch: Option<u64>) -> Self {
        let mut inner = self.inner.lock();
        debug_assert!(
            inner.reserved_marker.is_none(),
            "subscription generation invalidated before its checkpoint marker was released"
        );
        let retention_cap = inner.retention_cap;
        let next_sequence = inner.next_sequence;
        self.terminate_locked(&mut inner, message.to_owned());
        drop(inner);
        self.wake.send_modify(|_| {});
        Self::new_at(
            retention_cap,
            Arc::clone(&self.budget),
            latest_committed_epoch,
            next_sequence,
        )
    }

    fn subscriber_count(&self) -> usize {
        self.inner.lock().readers.len()
    }

    fn observe_committed_epoch(&self, epoch: u64) {
        let mut inner = self.inner.lock();
        inner.latest_committed_epoch = Some(
            inner
                .latest_committed_epoch
                .map_or(epoch, |latest| latest.max(epoch)),
        );
    }

    fn terminate_locked(&self, inner: &mut StreamLogInner, message: String) {
        if inner.terminal_error.is_none() {
            inner.terminal_error = Some(message);
        }
        clear_entries(inner);
    }
}

impl Drop for StreamLog {
    fn drop(&mut self) {
        let inner = self.inner.get_mut();
        debug_assert!(
            inner.reserved_marker.is_none(),
            "subscription log dropped with a live checkpoint marker reservation"
        );
    }
}

fn storage_cap(retention_cap: usize) -> usize {
    retention_cap.max(INTERNAL_LIVE_LOG_BYTES)
}

fn head_sequence(inner: &StreamLogInner) -> u64 {
    inner
        .entries
        .front()
        .map_or(inner.next_sequence, |entry| entry.sequence)
}

fn clear_entries(inner: &mut StreamLogInner) {
    inner.entries.clear();
    inner.bytes = 0;
    inner.retention_floor = inner.next_sequence;
    inner.retention_bytes = 0;
}

fn pop_front(inner: &mut StreamLogInner) -> Option<SequencedUpdate> {
    let entry = inner.entries.pop_front()?;
    inner.bytes = inner.bytes.saturating_sub(entry.bytes);
    if entry.sequence >= inner.retention_floor {
        debug_assert_eq!(
            entry.sequence, inner.retention_floor,
            "retention floor skipped a retained entry"
        );
        inner.retention_bytes = inner.retention_bytes.saturating_sub(entry.bytes);
        inner.retention_floor = entry.sequence.saturating_add(1);
        if inner.retention_bytes == 0 {
            inner.retention_floor = inner.next_sequence;
        }
    }
    Some(entry)
}

fn evict_to_byte_target(inner: &mut StreamLogInner, target: usize) {
    while inner.bytes > target {
        let Some(_evicted) = pop_front(inner) else {
            inner.bytes = 0;
            inner.retention_floor = inner.next_sequence;
            inner.retention_bytes = 0;
            break;
        };
    }
}

fn reclaim_consumed_prefix(inner: &mut StreamLogInner) {
    let reader_floor = inner
        .readers
        .values()
        .copied()
        .min()
        .unwrap_or(inner.next_sequence);
    let protected_floor = inner.retention_floor.min(reader_floor);

    while inner
        .entries
        .front()
        .is_some_and(|entry| entry.sequence < protected_floor)
    {
        let Some(_evicted) = pop_front(inner) else {
            break;
        };
    }
}

fn calculate_retention_suffix(inner: &StreamLogInner) -> (u64, usize) {
    if inner.retention_cap == 0 {
        return (inner.next_sequence, 0);
    }

    let mut bytes = 0usize;
    let mut floor = inner.next_sequence;
    for entry in inner.entries.iter().rev() {
        let with_entry = bytes.saturating_add(entry.bytes);
        if with_entry > inner.retention_cap {
            break;
        }
        bytes = with_entry;
        floor = entry.sequence;
    }
    (floor, bytes)
}

fn recompute_retention_suffix(inner: &mut StreamLogInner) {
    let (floor, bytes) = calculate_retention_suffix(inner);
    inner.retention_floor = floor;
    inner.retention_bytes = bytes;
}

fn retained_entry_bytes(inner: &StreamLogInner, sequence: u64) -> Option<usize> {
    let head = head_sequence(inner);
    if sequence < head || sequence >= inner.next_sequence {
        return None;
    }
    let index = usize::try_from(sequence.saturating_sub(head)).ok()?;
    inner.entries.get(index).map(|entry| entry.bytes)
}

fn retain_appended_entry(inner: &mut StreamLogInner, sequence: u64, bytes: usize) {
    if inner.retention_cap == 0 {
        inner.retention_floor = inner.next_sequence;
        inner.retention_bytes = 0;
        return;
    }

    if inner.retention_floor == sequence {
        debug_assert_eq!(inner.retention_bytes, 0);
        inner.retention_floor = sequence;
    }
    inner.retention_bytes = inner.retention_bytes.saturating_add(bytes);
    while inner.retention_bytes > inner.retention_cap {
        let Some(expired_bytes) = retained_entry_bytes(inner, inner.retention_floor) else {
            debug_assert!(false, "retention floor is outside the shared log");
            inner.retention_floor = inner.next_sequence;
            inner.retention_bytes = 0;
            return;
        };
        inner.retention_bytes = inner.retention_bytes.saturating_sub(expired_bytes);
        inner.retention_floor = inner.retention_floor.saturating_add(1);
    }
    if inner.retention_bytes == 0 {
        inner.retention_floor = inner.next_sequence;
    }
}

fn cursor_after_retained_epoch(
    inner: &StreamLogInner,
    requested_epoch: u64,
) -> Result<(u64, u64), SubscriptionOpenError> {
    let Some(latest_committed) = inner.latest_committed_epoch else {
        return Err(SubscriptionOpenError::EpochNotCommitted {
            requested: requested_epoch,
            latest_committed: None,
        });
    };
    if requested_epoch > latest_committed {
        return Err(SubscriptionOpenError::EpochNotCommitted {
            requested: requested_epoch,
            latest_committed: Some(latest_committed),
        });
    }
    if inner.retention_cap == 0 {
        return Err(SubscriptionOpenError::ReplayPruned {
            earliest_retained: 0,
        });
    }

    let mut cursor = None;
    let mut earliest_retained = u64::MAX;
    for entry in inner
        .entries
        .iter()
        .filter(|entry| entry.sequence >= inner.retention_floor)
    {
        if let MvUpdate::Barrier {
            epoch,
            through_sequence,
            ..
        } = entry.update.as_ref()
        {
            if *through_sequence < inner.retention_floor {
                continue;
            }
            earliest_retained = earliest_retained.min(*epoch);
            if *epoch == requested_epoch {
                cursor = Some((*through_sequence, entry.sequence));
            }
        }
    }

    if let Some(cursor) = cursor {
        return Ok(cursor);
    }
    if earliest_retained == u64::MAX || requested_epoch < earliest_retained {
        return Err(SubscriptionOpenError::ReplayPruned {
            earliest_retained: if earliest_retained == u64::MAX {
                0
            } else {
                earliest_retained
            },
        });
    }
    Err(SubscriptionOpenError::EpochNotCommitted {
        requested: requested_epoch,
        latest_committed: Some(latest_committed),
    })
}

pub(super) fn approx_size(update: &MvUpdate) -> usize {
    let payload = match update {
        MvUpdate::Batch(batch) => batch.get_array_memory_size(),
        MvUpdate::Barrier { .. } => return BARRIER_ENTRY_BYTES,
        MvUpdate::Error(message) => message.len(),
    };
    payload.saturating_add(ENTRY_ACCOUNTING_BYTES)
}

pub(super) enum SubscriptionRead {
    Update {
        sequence: u64,
        update: ChargedUpdate,
    },
    Lagged(u64),
    Terminal(String),
}

enum TryRead {
    Ready(SubscriptionRead),
    Pending,
}

pub(crate) struct SubscriptionReader {
    log: Arc<StreamLog>,
    reader_id: u64,
    cursor: u64,
    /// `(epoch, physical sequence)` of the retained progress marker that an
    /// AS-OF reader must skip while still replaying post-cut rows sequenced before it.
    skip_barrier: Option<(u64, u64)>,
    wake: watch::Receiver<()>,
    registered: bool,
}

impl std::fmt::Debug for SubscriptionReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubscriptionReader")
            .field("reader_id", &self.reader_id)
            .field("cursor", &self.cursor)
            .field("skip_barrier", &self.skip_barrier)
            .field("registered", &self.registered)
            .finish_non_exhaustive()
    }
}

impl SubscriptionReader {
    pub(super) async fn next(&mut self) -> SubscriptionRead {
        loop {
            match self.try_read() {
                TryRead::Ready(read) => return read,
                TryRead::Pending => {
                    if self.wake.changed().await.is_err() {
                        return SubscriptionRead::Terminal(
                            "subscription shared log closed unexpectedly".into(),
                        );
                    }
                }
            }
        }
    }

    fn try_read(&mut self) -> TryRead {
        let mut inner = self.log.inner.lock();
        if let Some(message) = &inner.terminal_error {
            return TryRead::Ready(SubscriptionRead::Terminal(message.clone()));
        }
        if inner.readers.get(&self.reader_id).copied() != Some(self.cursor) {
            return TryRead::Ready(SubscriptionRead::Terminal(
                "subscription reader cursor registration invariant failed".into(),
            ));
        }
        let head = head_sequence(&inner);
        if self.cursor < head {
            let mut skipped = head.saturating_sub(self.cursor);
            if self
                .skip_barrier
                .is_some_and(|(_, sequence)| sequence >= self.cursor && sequence < head)
            {
                skipped = skipped.saturating_sub(1);
                self.skip_barrier = None;
                if skipped == 0 {
                    self.cursor = head;
                    inner.readers.insert(self.reader_id, self.cursor);
                    reclaim_consumed_prefix(&mut inner);
                    drop(inner);
                    return self.try_read();
                }
            }
            return TryRead::Ready(SubscriptionRead::Lagged(skipped));
        }

        if self.cursor < inner.next_sequence {
            let Ok(index) = usize::try_from(self.cursor.saturating_sub(head)) else {
                return TryRead::Ready(SubscriptionRead::Terminal(
                    "subscription shared log index exceeds addressable memory".into(),
                ));
            };
            let Some(entry) = inner.entries.get(index) else {
                return TryRead::Ready(SubscriptionRead::Terminal(
                    "subscription shared log sequence invariant failed".into(),
                ));
            };
            if entry.sequence != self.cursor {
                return TryRead::Ready(SubscriptionRead::Terminal(
                    "subscription shared log is not contiguous".into(),
                ));
            }
            let sequence = entry.sequence;
            let update = entry.update.clone();
            let skip = self.skip_barrier.is_some_and(|(epoch, sequence)| {
                sequence == entry.sequence
                    &&
                matches!(update.as_ref(), MvUpdate::Barrier { epoch: seen, .. } if *seen == epoch)
            });
            self.cursor = self.cursor.saturating_add(1);
            inner.readers.insert(self.reader_id, self.cursor);
            reclaim_consumed_prefix(&mut inner);
            drop(inner);
            if skip {
                self.skip_barrier = None;
                return self.try_read();
            }
            return TryRead::Ready(SubscriptionRead::Update { sequence, update });
        }

        TryRead::Pending
    }

    fn release(&mut self) {
        if !self.registered {
            return;
        }
        let mut inner = self.log.inner.lock();
        inner.readers.remove(&self.reader_id);
        reclaim_consumed_prefix(&mut inner);
        drop(inner);
        self.registered = false;
    }
}

impl Drop for SubscriptionReader {
    fn drop(&mut self) {
        self.release();
    }
}

struct ReservedMarker {
    log: Arc<StreamLog>,
    through_sequence: u64,
}

struct ReservedCut {
    attempt: CheckpointAttempt,
    markers: Vec<ReservedMarker>,
    reserved_bytes: usize,
}

fn require_canonical_checkpoint_attempt(attempt: CheckpointAttempt) -> Result<(), String> {
    if attempt.is_canonical() {
        return Ok(());
    }
    Err(format!(
        "subscription checkpoint cut requires one nonzero canonical checkpoint ID; received epoch={} checkpoint_id={}",
        attempt.epoch, attempt.checkpoint_id
    ))
}

#[derive(Default)]
struct RegistryLifecycle {
    latest_committed_epoch: Option<u64>,
    pending_cut: Option<ReservedCut>,
}

pub(crate) struct SubscriptionRegistry {
    lifecycle: Mutex<RegistryLifecycle>,
    streams: RwLock<HashMap<String, Arc<StreamLog>>>,
    budget: Arc<SubscriptionMemoryBudget>,
}

impl SubscriptionRegistry {
    pub(crate) fn new() -> Self {
        let budget =
            Arc::clone(PROCESS_SUBSCRIPTION_BUDGET.get_or_init(|| {
                Arc::new(SubscriptionMemoryBudget::new(PROCESS_SUBSCRIPTION_BYTES))
            }));
        Self::with_budget(budget)
    }

    fn with_budget(budget: Arc<SubscriptionMemoryBudget>) -> Self {
        Self {
            lifecycle: Mutex::new(RegistryLifecycle::default()),
            streams: RwLock::new(HashMap::new()),
            budget,
        }
    }

    #[cfg(test)]
    pub(super) fn with_storage_budget(limit: usize) -> Self {
        Self::with_budget(Arc::new(SubscriptionMemoryBudget::new(limit)))
    }

    /// Set the AS-OF retention budget. A zero budget still keeps a small
    /// internal live suffix for attached portals but is never replay-eligible.
    pub(crate) fn configure(&self, name: &str, cap: usize) {
        let lifecycle = self.lifecycle.lock();
        let log = self.get_or_create_locked(name, &lifecycle);
        log.set_retention_cap(cap);
    }

    pub(crate) fn subscribe(
        &self,
        name: &str,
        start: SubscribeStart,
    ) -> Result<SubscriptionReader, SubscriptionOpenError> {
        self.get_or_create(name).subscribe(start)
    }

    pub(crate) fn send_batch(&self, name: &str, batch: RecordBatch) -> Result<(), String> {
        let Some(log) = self.streams.read().get(name).cloned() else {
            return Ok(());
        };
        let bytes = batch.get_array_memory_size();
        let outcome = if bytes > MAX_LIVE_BATCH_BYTES {
            log.append(MvUpdate::Error(format!(
                "subscription batch is {bytes} Arrow bytes, exceeding the internal live-batch limit of {MAX_LIVE_BATCH_BYTES} bytes; rows were not delivered"
            )))
        } else {
            log.append(MvUpdate::Batch(batch))
        };
        match outcome {
            AppendOutcome::Stored | AppendOutcome::Unobserved => Ok(()),
            AppendOutcome::Rejected(message) => Err(format!(
                "subscription update for '{name}' was rejected: {message}"
            )),
        }
    }

    /// Snapshot every live object's exact cursor at the aligned checkpoint cut.
    pub(crate) fn reserve_cut(&self, attempt: CheckpointAttempt) -> Result<(), String> {
        require_canonical_checkpoint_attempt(attempt)?;
        let mut lifecycle = self.lifecycle.lock();
        if let Some(existing) = &lifecycle.pending_cut {
            return Err(format!(
                "subscription cut already reserved for epoch={} checkpoint_id={}; cannot reserve epoch={} checkpoint_id={}",
                existing.attempt.epoch,
                existing.attempt.checkpoint_id,
                attempt.epoch,
                attempt.checkpoint_id
            ));
        }

        let streams = self.streams.read();
        let mut markers = Vec::with_capacity(streams.len());
        for log in streams.values() {
            match log.reserve_marker(attempt) {
                Ok(Some(through_sequence)) => markers.push(ReservedMarker {
                    log: Arc::clone(log),
                    through_sequence,
                }),
                Ok(None) => {}
                Err(error) => {
                    for marker in &markers {
                        let cancelled = marker.log.cancel_marker(attempt);
                        debug_assert!(cancelled);
                    }
                    return Err(format!(
                        "subscription cut reservation failed for epoch={} checkpoint_id={}: {error}",
                        attempt.epoch, attempt.checkpoint_id
                    ));
                }
            }
        }
        drop(streams);

        let Some(reserved_bytes) = markers.len().checked_mul(BARRIER_ENTRY_BYTES) else {
            for marker in &markers {
                let cancelled = marker.log.cancel_marker(attempt);
                debug_assert!(cancelled);
            }
            return Err("subscription checkpoint marker reservation size overflow".into());
        };
        if !self.budget.try_reserve(reserved_bytes) {
            for marker in &markers {
                let cancelled = marker.log.cancel_marker(attempt);
                debug_assert!(cancelled);
            }
            return Err(format!(
                "subscription checkpoint markers require {reserved_bytes} bytes, exceeding available process subscription memory ({} byte limit)",
                self.budget.limit
            ));
        }
        lifecycle.pending_cut = Some(ReservedCut {
            attempt,
            markers,
            reserved_bytes,
        });
        Ok(())
    }

    /// Resolve a previously reserved cut after the checkpoint is durable.
    pub(crate) fn commit_cut(&self, attempt: CheckpointAttempt) -> Result<(), String> {
        require_canonical_checkpoint_attempt(attempt)?;
        let mut lifecycle = self.lifecycle.lock();
        let Some(pending) = &lifecycle.pending_cut else {
            return Err(format!(
                "subscription cut missing for committed epoch={} checkpoint_id={}",
                attempt.epoch, attempt.checkpoint_id
            ));
        };
        if pending.attempt != attempt {
            return Err(format!(
                "subscription cut mismatch: reserved epoch={} checkpoint_id={}, committed epoch={} checkpoint_id={}",
                pending.attempt.epoch,
                pending.attempt.checkpoint_id,
                attempt.epoch,
                attempt.checkpoint_id
            ));
        }
        let cut = lifecycle
            .pending_cut
            .take()
            .expect("exact pending subscription cut was checked");

        // The lifecycle guard freezes membership while a shared streams guard
        // leaves existing-log lookups available during marker publication.
        let logs = self.streams.read();
        lifecycle.latest_committed_epoch = Some(
            lifecycle
                .latest_committed_epoch
                .map_or(attempt.epoch, |current| current.max(attempt.epoch)),
        );
        for log in logs.values() {
            log.observe_committed_epoch(attempt.epoch);
        }
        let reserved_bytes = cut.reserved_bytes;
        let mut unused_reserved_bytes = 0usize;
        let mut failures = Vec::new();
        for marker in cut.markers {
            match marker
                .log
                .append_reserved_marker(attempt, marker.through_sequence)
            {
                ReservedAppendOutcome::Stored => {}
                ReservedAppendOutcome::Unobserved => {
                    unused_reserved_bytes = unused_reserved_bytes
                        .checked_add(BARRIER_ENTRY_BYTES)
                        .expect("reserved subscription marker accounting overflowed");
                }
                ReservedAppendOutcome::Rejected(error) => {
                    unused_reserved_bytes = unused_reserved_bytes
                        .checked_add(BARRIER_ENTRY_BYTES)
                        .expect("reserved subscription marker accounting overflowed");
                    failures.push(error);
                }
            }
        }
        drop(logs);
        debug_assert!(unused_reserved_bytes <= reserved_bytes);
        self.budget.release(unused_reserved_bytes);
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "subscription checkpoint marker publication failed for epoch={} checkpoint_id={}: {}",
                attempt.epoch,
                attempt.checkpoint_id,
                failures.join("; ")
            ))
        }
    }

    pub(crate) fn abort_cut(&self, attempt: CheckpointAttempt) {
        if !attempt.is_canonical() {
            return;
        }
        let mut lifecycle = self.lifecycle.lock();
        if lifecycle
            .pending_cut
            .as_ref()
            .is_some_and(|cut| cut.attempt == attempt)
        {
            let cut = lifecycle
                .pending_cut
                .take()
                .expect("exact pending subscription cut was checked");
            release_reserved_cut(cut, &self.budget);
        }
    }

    /// End the current in-memory delivery generation before recovery can replay it.
    /// Existing readers receive a terminal error; replacement logs retain their
    /// configured byte caps and continue that object's current in-process cursor.
    pub(crate) fn invalidate_all(&self, reason: &str) {
        let mut lifecycle = self.lifecycle.lock();
        if let Some(cut) = lifecycle.pending_cut.take() {
            release_reserved_cut(cut, &self.budget);
        }
        let mut streams = self.streams.write();
        let latest_committed_epoch = lifecycle.latest_committed_epoch;
        for log in streams.values_mut() {
            let replacement = log.terminate_and_replace(reason, latest_committed_epoch);
            *log = Arc::new(replacement);
        }
    }

    #[cfg(test)]
    pub(crate) fn broadcast_barrier(&self, epoch: u64, checkpoint_id: u64) {
        let attempt = CheckpointAttempt::new(epoch, checkpoint_id);
        self.reserve_cut(attempt).unwrap();
        self.commit_cut(attempt).unwrap();
    }

    pub(crate) fn drop_name(&self, name: &str) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        let mut streams = self.streams.write();
        let Some(log) = streams.remove(name) else {
            return false;
        };
        if let Some(cut) = &mut lifecycle.pending_cut {
            let attempt = cut.attempt;
            let before = cut.markers.len();
            cut.markers.retain(|marker| {
                if Arc::ptr_eq(&marker.log, &log) {
                    let cancelled = marker.log.cancel_marker(attempt);
                    debug_assert!(cancelled);
                    false
                } else {
                    true
                }
            });
            let removed = before.saturating_sub(cut.markers.len());
            let released = removed
                .checked_mul(BARRIER_ENTRY_BYTES)
                .expect("reserved subscription marker accounting overflowed");
            cut.reserved_bytes = cut
                .reserved_bytes
                .checked_sub(released)
                .expect("released more subscription marker bytes than were reserved");
            self.budget.release(released);
        }
        log.terminate("object dropped");
        true
    }

    pub(crate) fn subscriber_count(&self, name: &str) -> usize {
        self.streams
            .read()
            .get(name)
            .map_or(0, |log| log.subscriber_count())
    }

    pub(crate) fn contains_name(&self, name: &str) -> bool {
        self.streams.read().contains_key(name)
    }

    fn get_or_create(&self, name: &str) -> Arc<StreamLog> {
        if let Some(log) = self.streams.read().get(name) {
            return Arc::clone(log);
        }
        let lifecycle = self.lifecycle.lock();
        self.get_or_create_locked(name, &lifecycle)
    }

    fn get_or_create_locked(&self, name: &str, lifecycle: &RegistryLifecycle) -> Arc<StreamLog> {
        Arc::clone(
            self.streams
                .write()
                .entry(name.to_owned())
                .or_insert_with(|| {
                    Arc::new(StreamLog::new(
                        0,
                        Arc::clone(&self.budget),
                        lifecycle.latest_committed_epoch,
                    ))
                }),
        )
    }

    #[cfg(test)]
    fn head_sequence(&self, name: &str) -> Option<u64> {
        self.streams
            .read()
            .get(name)
            .map(|log| head_sequence(&log.inner.lock()))
    }

    #[cfg(test)]
    fn next_sequence(&self, name: &str) -> Option<u64> {
        self.streams
            .read()
            .get(name)
            .map(|log| log.inner.lock().next_sequence)
    }

    #[cfg(test)]
    pub(super) fn charged_bytes(&self) -> usize {
        self.budget.used()
    }

    #[cfg(test)]
    fn assert_retention_cache(&self, name: &str) {
        let streams = self.streams.read();
        let log = streams.get(name).expect("test stream log must exist");
        let inner = log.inner.lock();
        assert_eq!(
            (inner.retention_floor, inner.retention_bytes),
            calculate_retention_suffix(&inner),
            "cached retention suffix diverged from a cold reverse scan"
        );
    }
}

fn release_reserved_cut(cut: ReservedCut, budget: &SubscriptionMemoryBudget) {
    debug_assert_eq!(
        cut.reserved_bytes,
        cut.markers
            .len()
            .checked_mul(BARRIER_ENTRY_BYTES)
            .expect("reserved subscription marker accounting overflowed"),
        "subscription marker reservation accounting diverged"
    );
    for marker in &cut.markers {
        let cancelled = marker.log.cancel_marker(cut.attempt);
        debug_assert!(cancelled);
    }
    budget.release(cut.reserved_bytes);
}

impl Drop for SubscriptionRegistry {
    fn drop(&mut self) {
        if let Some(cut) = self.lifecycle.get_mut().pending_cut.take() {
            release_reserved_cut(cut, &self.budget);
        }
    }
}

impl Default for SubscriptionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn batch(ids: Vec<i64>) -> RecordBatch {
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![StdArc::new(Int64Array::from(ids))]).unwrap()
    }

    fn earliest_retained(error: SubscriptionOpenError) -> u64 {
        match error {
            SubscriptionOpenError::ReplayPruned { earliest_retained } => earliest_retained,
            SubscriptionOpenError::EpochNotCommitted { .. } => {
                panic!("expected replay-pruned error")
            }
            SubscriptionOpenError::Capacity { .. } => panic!("expected replay-pruned error"),
        }
    }

    async fn next_update(reader: &mut SubscriptionReader) -> ChargedUpdate {
        match reader.next().await {
            SubscriptionRead::Update { update, .. } => update,
            SubscriptionRead::Lagged(skipped) => panic!("unexpected gap of {skipped} entries"),
            SubscriptionRead::Terminal(message) => panic!("unexpected terminal error: {message}"),
        }
    }

    #[tokio::test]
    async fn tail_starts_at_atomic_attach_cut() {
        let registry = SubscriptionRegistry::new();
        registry.configure("mv", 1 << 20);
        registry.send_batch("mv", batch(vec![1])).unwrap();
        let mut reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        registry.send_batch("mv", batch(vec![2])).unwrap();

        let update = next_update(&mut reader).await;
        assert!(
            matches!(update.as_ref(), MvUpdate::Batch(batch) if batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0) == 2)
        );
    }

    #[tokio::test]
    async fn as_of_starts_strictly_after_exact_retained_barrier() {
        let registry = SubscriptionRegistry::new();
        registry.configure("mv", 1 << 20);
        registry.broadcast_barrier(1, 1);
        registry.send_batch("mv", batch(vec![10])).unwrap();
        registry.broadcast_barrier(2, 2);

        let mut reader = registry
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap();
        assert!(matches!(
            next_update(&mut reader).await.as_ref(),
            MvUpdate::Batch(_)
        ));
        assert!(matches!(
            next_update(&mut reader).await.as_ref(),
            MvUpdate::Barrier {
                epoch: 2,
                checkpoint_id: 2,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn delayed_commit_preserves_the_aligned_cut_cursor() {
        let registry = SubscriptionRegistry::new();
        registry.configure("mv", 1 << 20);
        registry.send_batch("mv", batch(vec![10])).unwrap();
        let mut live = registry.subscribe("mv", SubscribeStart::Tail).unwrap();

        let attempt = CheckpointAttempt::canonical(1);
        registry.reserve_cut(attempt).unwrap();
        registry.send_batch("mv", batch(vec![20])).unwrap();
        registry.commit_cut(attempt).unwrap();

        assert!(matches!(
            next_update(&mut live).await.as_ref(),
            MvUpdate::Batch(batch)
                if batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0) == 20
        ));
        assert!(matches!(
            next_update(&mut live).await.as_ref(),
            MvUpdate::Barrier {
                epoch: 1,
                checkpoint_id: 1,
                through_sequence: 1,
            }
        ));

        let mut replay = registry
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap();
        assert!(matches!(
            next_update(&mut replay).await.as_ref(),
            MvUpdate::Batch(batch)
                if batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0) == 20
        ));
    }

    #[test]
    fn cut_reservation_fails_before_capture_when_marker_budget_is_full() {
        let sample = batch(vec![1, 2, 3]);
        let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
        let registry = SubscriptionRegistry::with_storage_budget(entry_bytes);
        registry.configure("mv", 1 << 20);
        registry.send_batch("mv", sample).unwrap();
        let attempt = CheckpointAttempt::canonical(1);

        let error = registry.reserve_cut(attempt).unwrap_err();

        assert!(error.contains("checkpoint markers require"));
        assert_eq!(registry.charged_bytes(), entry_bytes);
        let log = registry.streams.read().get("mv").cloned().unwrap();
        assert_eq!(log.inner.lock().reserved_marker, None);
        assert!(registry.lifecycle.lock().pending_cut.is_none());
    }

    #[test]
    fn abort_releases_marker_headroom_for_the_next_attempt() {
        let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
        registry.configure("mv", BARRIER_ENTRY_BYTES);
        let first = CheckpointAttempt::canonical(1);
        registry.reserve_cut(first).unwrap();
        assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);

        registry.abort_cut(first);
        assert_eq!(registry.charged_bytes(), 0);

        let second = CheckpointAttempt::canonical(2);
        registry.reserve_cut(second).unwrap();
        assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);
        registry.abort_cut(second);
        assert_eq!(registry.charged_bytes(), 0);
    }

    #[test]
    fn conflicting_attempt_cannot_steal_the_reserved_cut() {
        let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
        registry.configure("mv", BARRIER_ENTRY_BYTES);
        let reserved = CheckpointAttempt::canonical(1);
        let conflicting = CheckpointAttempt::canonical(2);
        registry.reserve_cut(reserved).unwrap();

        assert!(registry.reserve_cut(conflicting).is_err());
        assert!(registry.commit_cut(conflicting).is_err());
        assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);
        assert_eq!(
            registry
                .lifecycle
                .lock()
                .pending_cut
                .as_ref()
                .map(|cut| cut.attempt),
            Some(reserved)
        );

        registry.commit_cut(reserved).unwrap();
        assert_eq!(registry.next_sequence("mv"), Some(1));
        assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);
    }

    #[test]
    fn noncanonical_cut_attempts_cannot_mutate_registry_state() {
        let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
        registry.configure("mv", BARRIER_ENTRY_BYTES);
        let invalid = CheckpointAttempt::new(1, 2);
        let log = registry.streams.read().get("mv").cloned().unwrap();

        let error = registry.reserve_cut(invalid).unwrap_err();

        assert!(error.contains("canonical checkpoint ID"));
        assert!(registry.lifecycle.lock().pending_cut.is_none());
        assert_eq!(log.inner.lock().reserved_marker, None);
        assert_eq!(registry.charged_bytes(), 0);

        let canonical = CheckpointAttempt::canonical(1);
        registry.reserve_cut(canonical).unwrap();
        assert!(registry.commit_cut(invalid).is_err());
        registry.abort_cut(invalid);

        assert_eq!(
            registry
                .lifecycle
                .lock()
                .pending_cut
                .as_ref()
                .map(|cut| cut.attempt),
            Some(canonical)
        );
        assert_eq!(log.inner.lock().reserved_marker, Some(canonical));
        assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);

        registry.abort_cut(canonical);
        assert!(registry.lifecycle.lock().pending_cut.is_none());
        assert_eq!(log.inner.lock().reserved_marker, None);
        assert_eq!(registry.charged_bytes(), 0);
    }

    #[test]
    fn sequence_exhaustion_rejects_cut_reservation_without_claiming_budget() {
        let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
        registry.configure("mv", BARRIER_ENTRY_BYTES);
        let log = registry.streams.read().get("mv").cloned().unwrap();
        {
            let mut inner = log.inner.lock();
            inner.next_sequence = u64::MAX;
            inner.retention_floor = u64::MAX;
        }

        let error = registry
            .reserve_cut(CheckpointAttempt::canonical(1))
            .unwrap_err();

        assert!(error.contains("sequence space"));
        assert_eq!(registry.charged_bytes(), 0);
        assert_eq!(log.inner.lock().reserved_marker, None);
    }

    #[test]
    fn terminal_logs_do_not_claim_checkpoint_marker_capacity() {
        let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
        registry.configure("mv", BARRIER_ENTRY_BYTES);
        let log = registry.streams.read().get("mv").cloned().unwrap();
        log.terminate("injected terminal state");
        let attempt = CheckpointAttempt::canonical(1);

        registry.reserve_cut(attempt).unwrap();

        assert_eq!(registry.charged_bytes(), 0);
        assert_eq!(
            registry
                .lifecycle
                .lock()
                .pending_cut
                .as_ref()
                .map(|cut| cut.markers.len()),
            Some(0)
        );
        registry.commit_cut(attempt).unwrap();
    }

    #[test]
    fn post_cut_data_cannot_consume_the_reserved_marker_sequence() {
        let sample = batch(vec![1]);
        let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
        let registry = SubscriptionRegistry::with_storage_budget(
            BARRIER_ENTRY_BYTES.saturating_add(entry_bytes),
        );
        registry.configure("mv", 1 << 20);
        let log = registry.streams.read().get("mv").cloned().unwrap();
        {
            let mut inner = log.inner.lock();
            inner.next_sequence = u64::MAX - 1;
            inner.retention_floor = u64::MAX - 1;
        }
        let attempt = CheckpointAttempt::canonical(1);
        registry.reserve_cut(attempt).unwrap();

        let error = registry.send_batch("mv", sample).unwrap_err();

        assert!(error.contains("sequence space exhausted"));
        {
            let inner = log.inner.lock();
            assert_eq!(inner.next_sequence, u64::MAX - 1);
            assert_eq!(inner.reserved_marker, Some(attempt));
            assert!(inner.terminal_error.is_none());
        }
        registry.commit_cut(attempt).unwrap();
        let inner = log.inner.lock();
        assert_eq!(inner.next_sequence, u64::MAX);
        assert_eq!(inner.reserved_marker, None);
    }

    #[test]
    fn reserved_marker_survives_process_budget_contention() {
        let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
        registry.configure("mv", BARRIER_ENTRY_BYTES);
        let log = registry.streams.read().get("mv").cloned().unwrap();
        let attempt = CheckpointAttempt::canonical(1);
        registry.reserve_cut(attempt).unwrap();

        let error = registry.send_batch("mv", batch(vec![1])).unwrap_err();

        assert!(error.contains("process memory budget exhausted"));
        {
            let inner = log.inner.lock();
            assert_eq!(inner.next_sequence, 0);
            assert_eq!(inner.reserved_marker, Some(attempt));
            assert!(inner.terminal_error.is_none());
        }
        registry.commit_cut(attempt).unwrap();
        assert_eq!(registry.next_sequence("mv"), Some(1));
        assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);
    }

    #[test]
    fn unobserved_commit_releases_reserved_marker_bytes() {
        let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
        let reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        let attempt = CheckpointAttempt::canonical(1);
        registry.reserve_cut(attempt).unwrap();
        assert_eq!(registry.charged_bytes(), BARRIER_ENTRY_BYTES);

        drop(reader);
        registry.commit_cut(attempt).unwrap();

        assert_eq!(registry.charged_bytes(), 0);
        assert_eq!(registry.next_sequence("mv"), Some(0));
    }

    #[test]
    fn rejected_marker_commit_releases_reserved_bytes_and_reports_failure() {
        let registry = SubscriptionRegistry::with_storage_budget(BARRIER_ENTRY_BYTES);
        registry.configure("mv", BARRIER_ENTRY_BYTES);
        let log = registry.streams.read().get("mv").cloned().unwrap();
        let attempt = CheckpointAttempt::canonical(1);
        registry.reserve_cut(attempt).unwrap();
        log.inner.lock().terminal_error = Some("injected terminal state".into());

        let error = registry.commit_cut(attempt).unwrap_err();

        assert!(error.contains("terminated before checkpoint marker publication"));
        assert_eq!(registry.charged_bytes(), 0);
        assert_eq!(log.inner.lock().reserved_marker, None);
    }

    #[test]
    fn invalidation_and_object_drop_release_exact_marker_reservations() {
        let budget = StdArc::new(SubscriptionMemoryBudget::new(BARRIER_ENTRY_BYTES));
        let registry = SubscriptionRegistry::with_budget(StdArc::clone(&budget));
        registry.configure("mv", BARRIER_ENTRY_BYTES);
        let first = CheckpointAttempt::canonical(1);
        registry.reserve_cut(first).unwrap();

        registry.invalidate_all("injected recovery");
        assert_eq!(budget.used(), 0);
        assert!(registry.lifecycle.lock().pending_cut.is_none());

        let second = CheckpointAttempt::canonical(2);
        registry.reserve_cut(second).unwrap();
        assert!(registry.drop_name("mv"));
        assert_eq!(budget.used(), 0);
        registry.commit_cut(second).unwrap();
    }

    #[tokio::test]
    async fn recreated_object_is_outside_the_dropped_objects_reserved_cut() {
        let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
        registry.configure("mv", 1 << 20);
        let mut dropped_reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        let attempt = CheckpointAttempt::canonical(1);
        registry.reserve_cut(attempt).unwrap();

        assert!(registry.drop_name("mv"));
        assert!(matches!(
            dropped_reader.next().await,
            SubscriptionRead::Terminal(ref error) if error == "object dropped"
        ));

        registry.configure("mv", 1 << 20);
        let mut recreated_reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        registry.commit_cut(attempt).unwrap();
        assert_eq!(registry.next_sequence("mv"), Some(0));
        assert!(matches!(recreated_reader.try_read(), TryRead::Pending));

        registry.send_batch("mv", batch(vec![7])).unwrap();
        assert!(matches!(
            recreated_reader.next().await,
            SubscriptionRead::Update {
                sequence: 0,
                update,
            } if matches!(update.as_ref(), MvUpdate::Batch(_))
        ));
    }

    #[test]
    fn registry_drop_releases_an_unresolved_marker_reservation() {
        let budget = StdArc::new(SubscriptionMemoryBudget::new(BARRIER_ENTRY_BYTES));
        {
            let registry = SubscriptionRegistry::with_budget(StdArc::clone(&budget));
            registry.configure("mv", BARRIER_ENTRY_BYTES);
            registry
                .reserve_cut(CheckpointAttempt::canonical(1))
                .unwrap();
            assert_eq!(budget.used(), BARRIER_ENTRY_BYTES);
        }
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn recovery_replacement_continues_the_current_object_sequence() {
        let registry = SubscriptionRegistry::new();
        registry.configure("mv", 1 << 20);
        let mut before_recovery = registry.subscribe("mv", SubscribeStart::Tail).unwrap();

        registry.send_batch("mv", batch(vec![10])).unwrap();
        assert!(matches!(
            before_recovery.next().await,
            SubscriptionRead::Update {
                sequence: 0,
                update,
            } if matches!(update.as_ref(), MvUpdate::Batch(_))
        ));
        let abandoned = CheckpointAttempt::canonical(1);
        registry.reserve_cut(abandoned).unwrap();

        registry.invalidate_all("injected recovery");
        assert!(matches!(
            before_recovery.next().await,
            SubscriptionRead::Terminal(message) if message == "injected recovery"
        ));
        assert!(registry.commit_cut(abandoned).is_err());

        let mut after_recovery = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        registry.send_batch("mv", batch(vec![10])).unwrap();
        assert!(matches!(
            after_recovery.next().await,
            SubscriptionRead::Update {
                sequence: 1,
                update,
            } if matches!(update.as_ref(), MvUpdate::Batch(_))
        ));

        let committed = CheckpointAttempt::canonical(2);
        registry.reserve_cut(committed).unwrap();
        registry.commit_cut(committed).unwrap();
        assert!(matches!(
            after_recovery.next().await,
            SubscriptionRead::Update {
                sequence: 2,
                update,
            } if matches!(
                update.as_ref(),
                MvUpdate::Barrier {
                    epoch: 2,
                    checkpoint_id: 2,
                    through_sequence: 2,
                }
            )
        ));
    }

    #[test]
    fn as_of_classifies_future_missing_and_pruned_epochs() {
        let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
        registry.configure("mv", 1 << 20);
        registry.broadcast_barrier(5, 5);
        registry.broadcast_barrier(7, 7);

        assert!(matches!(
            registry
                .subscribe("mv", SubscribeStart::AsOfEpoch(8))
                .unwrap_err(),
            SubscriptionOpenError::EpochNotCommitted {
                requested: 8,
                latest_committed: Some(7)
            }
        ));
        assert!(matches!(
            registry
                .subscribe("mv", SubscribeStart::AsOfEpoch(6))
                .unwrap_err(),
            SubscriptionOpenError::EpochNotCommitted {
                requested: 6,
                latest_committed: Some(7)
            }
        ));

        registry.configure(
            "mv",
            approx_size(&MvUpdate::Barrier {
                epoch: 0,
                checkpoint_id: 0,
                through_sequence: 0,
            }),
        );
        assert_eq!(
            earliest_retained(
                registry
                    .subscribe("mv", SubscribeStart::AsOfEpoch(5))
                    .unwrap_err()
            ),
            7
        );
    }

    #[test]
    fn as_of_knows_latest_epoch_without_a_stored_log_entry() {
        let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
        registry.broadcast_barrier(11, 11);

        assert!(matches!(
            registry
                .subscribe("late", SubscribeStart::AsOfEpoch(12))
                .unwrap_err(),
            SubscriptionOpenError::EpochNotCommitted {
                requested: 12,
                latest_committed: Some(11)
            }
        ));
        assert_eq!(
            earliest_retained(
                registry
                    .subscribe("late", SubscribeStart::AsOfEpoch(11))
                    .unwrap_err()
            ),
            0
        );
    }

    #[test]
    fn zero_retention_never_enables_as_of() {
        let registry = SubscriptionRegistry::new();
        let _reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        registry.broadcast_barrier(1, 1);
        registry.send_batch("mv", batch(vec![1])).unwrap();

        let error = registry
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap_err();
        assert_eq!(earliest_retained(error), 0);
    }

    #[tokio::test]
    async fn cached_retention_floor_matches_cold_scan_after_hot_path_updates() {
        let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
        registry.configure("mv", 512);
        let mut reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();

        for value in 0..32_i64 {
            registry.send_batch("mv", batch(vec![value])).unwrap();
            registry.assert_retention_cache("mv");
        }
        for _ in 0..16 {
            let _ = next_update(&mut reader).await;
            registry.assert_retention_cache("mv");
        }

        registry.configure("mv", 4096);
        registry.assert_retention_cache("mv");
        registry.configure("mv", 128);
        registry.assert_retention_cache("mv");
    }

    #[test]
    fn disabling_retention_without_readers_releases_the_log() {
        let registry = SubscriptionRegistry::new();
        registry.configure("mv", 1 << 20);
        let values: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2, 3]));
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        registry
            .send_batch(
                "mv",
                RecordBatch::try_new(schema, vec![StdArc::clone(&values)]).unwrap(),
            )
            .unwrap();
        assert_eq!(StdArc::strong_count(&values), 2);

        registry.configure("mv", 0);

        assert_eq!(StdArc::strong_count(&values), 1);
    }

    #[test]
    fn as_of_readers_do_not_clone_retained_arrow_batches() {
        let registry = SubscriptionRegistry::new();
        registry.configure("mv", 1 << 20);
        registry.broadcast_barrier(1, 1);
        let values: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2, 3]));
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        registry
            .send_batch(
                "mv",
                RecordBatch::try_new(schema, vec![StdArc::clone(&values)]).unwrap(),
            )
            .unwrap();
        let owners_before_attach = StdArc::strong_count(&values);

        let readers = (0..super::super::MAX_SUBSCRIBERS_PER_MV)
            .map(|_| {
                registry
                    .subscribe("mv", SubscribeStart::AsOfEpoch(1))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        assert_eq!(readers.len(), super::super::MAX_SUBSCRIBERS_PER_MV);
        assert_eq!(StdArc::strong_count(&values), owners_before_attach);
    }

    #[tokio::test]
    async fn terminal_drop_releases_retained_storage_immediately() {
        let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
        registry.configure("mv", 1 << 20);
        registry.broadcast_barrier(1, 1);
        let values: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2, 3]));
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        registry
            .send_batch(
                "mv",
                RecordBatch::try_new(schema, vec![StdArc::clone(&values)]).unwrap(),
            )
            .unwrap();
        let mut reader = registry
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap();
        assert!(registry.charged_bytes() > 0);
        assert_eq!(StdArc::strong_count(&values), 2);

        assert!(registry.drop_name("mv"));

        assert_eq!(registry.charged_bytes(), 0);
        assert_eq!(StdArc::strong_count(&values), 1);
        assert!(matches!(
            reader.next().await,
            SubscriptionRead::Terminal(message) if message == "object dropped"
        ));
    }

    #[tokio::test]
    async fn local_log_reclaims_while_the_charge_follows_reader_updates() {
        let registry = SubscriptionRegistry::with_storage_budget(1 << 20);
        let mut first = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        let mut second = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        registry.send_batch("mv", batch(vec![1, 2, 3])).unwrap();
        let log = registry.streams.read().get("mv").cloned().unwrap();
        let retained = registry.charged_bytes();
        assert!(retained > 0);

        let first_frame = next_update(&mut first).await;
        assert!(matches!(first_frame.as_ref(), MvUpdate::Batch(_)));
        assert_eq!(registry.charged_bytes(), retained);
        assert_eq!(log.inner.lock().bytes, retained);

        let second_frame = next_update(&mut second).await;
        assert!(matches!(second_frame.as_ref(), MvUpdate::Batch(_)));
        assert_eq!(registry.charged_bytes(), retained);
        assert_eq!(log.inner.lock().bytes, 0);

        drop(first_frame);
        assert_eq!(registry.charged_bytes(), retained);
        drop(second_frame);
        assert_eq!(registry.charged_bytes(), 0);
    }

    #[tokio::test]
    async fn process_budget_contention_fails_without_claim_and_release_is_reusable() {
        let sample = batch(vec![1, 2, 3]);
        let entry_bytes = approx_size(&MvUpdate::Batch(sample.clone()));
        let budget = StdArc::new(SubscriptionMemoryBudget::new(entry_bytes));
        let first_registry = SubscriptionRegistry::with_budget(StdArc::clone(&budget));
        let contender_registry = SubscriptionRegistry::with_budget(budget);
        first_registry.configure("first", entry_bytes.saturating_mul(4));
        contender_registry.configure("contender", entry_bytes.saturating_mul(4));

        first_registry.send_batch("first", sample.clone()).unwrap();
        assert_eq!(first_registry.charged_bytes(), entry_bytes);
        let contender_sequence = contender_registry.next_sequence("contender").unwrap();
        assert!(contender_registry
            .send_batch("contender", sample.clone())
            .is_err());
        assert_eq!(
            contender_registry.next_sequence("contender"),
            Some(contender_sequence),
            "failed admission must not claim a sequence"
        );
        let mut contender = contender_registry
            .subscribe("contender", SubscribeStart::Tail)
            .unwrap();
        assert!(matches!(
            contender.next().await,
            SubscriptionRead::Terminal(message)
                if message.contains("process memory budget exhausted")
        ));

        assert!(first_registry.drop_name("first"));
        assert_eq!(contender_registry.charged_bytes(), 0);
        contender_registry.configure("replacement", entry_bytes.saturating_mul(4));
        contender_registry
            .send_batch("replacement", sample)
            .unwrap();
        assert_eq!(contender_registry.charged_bytes(), entry_bytes);
        assert_eq!(contender_registry.next_sequence("replacement"), Some(1));
    }

    #[tokio::test]
    async fn as_of_cursor_reports_exact_gap_after_live_byte_eviction() {
        let registry = SubscriptionRegistry::new();
        registry.configure("mv", 1024);
        registry.broadcast_barrier(1, 1);
        let mut reader = registry
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap();

        let values_per_batch = (MAX_LIVE_BATCH_BYTES / 2) / std::mem::size_of::<i64>();
        for value in 0..6_i64 {
            registry
                .send_batch("mv", batch(vec![value; values_per_batch]))
                .unwrap();
        }

        let head = registry.head_sequence("mv").unwrap();
        let expected = head.saturating_sub(1);
        assert!(
            expected > 0,
            "test must evict entries beyond the AS-OF cursor"
        );
        assert!(matches!(
            reader.next().await,
            SubscriptionRead::Lagged(skipped) if skipped == expected
        ));
    }

    #[tokio::test]
    async fn dropping_name_is_a_visible_terminal_error() {
        let registry = SubscriptionRegistry::new();
        let mut reader = registry.subscribe("mv", SubscribeStart::Tail).unwrap();
        assert!(registry.drop_name("mv"));
        assert!(matches!(
            reader.next().await,
            SubscriptionRead::Terminal(message) if message == "object dropped"
        ));
    }

    #[tokio::test]
    async fn oversized_batch_is_one_explicit_sequence_not_claim_then_evict() {
        let registry = SubscriptionRegistry::new();
        registry.configure("mv", INTERNAL_LIVE_LOG_BYTES);
        registry.broadcast_barrier(1, 1);
        let mut reader = registry
            .subscribe("mv", SubscribeStart::AsOfEpoch(1))
            .unwrap();
        let before = registry.next_sequence("mv").unwrap();
        let values = vec![0_i64; MAX_LIVE_BATCH_BYTES / std::mem::size_of::<i64>() + 1];

        registry.send_batch("mv", batch(values)).unwrap();

        assert_eq!(registry.next_sequence("mv"), Some(before + 1));
        assert!(matches!(
            next_update(&mut reader).await.as_ref(),
            MvUpdate::Error(message) if message.contains("rows were not delivered")
        ));
    }

    #[test]
    fn subscriber_cap_is_atomic_across_65_simultaneous_attempts() {
        let registry = StdArc::new(SubscriptionRegistry::new());
        let attempts = super::super::MAX_SUBSCRIBERS_PER_MV + 1;
        let start = StdArc::new(std::sync::Barrier::new(attempts));
        let handles = (0..attempts)
            .map(|_| {
                let registry = StdArc::clone(&registry);
                let start = StdArc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    registry.subscribe("mv", SubscribeStart::Tail)
                })
            })
            .collect::<Vec<_>>();

        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let successes = results.iter().filter(|result| result.is_ok()).count();
        let capacity_failures = results
            .iter()
            .filter(|result| matches!(result, Err(SubscriptionOpenError::Capacity { .. })))
            .count();
        assert_eq!(successes, super::super::MAX_SUBSCRIBERS_PER_MV);
        assert_eq!(capacity_failures, 1);
    }
}
