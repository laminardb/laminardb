//! Per-object shared log for `SUBSCRIBE`.
//!
//! Publication and attachment use the same mutex. Portals retain only a
//! sequence cursor and a wake-up receiver. Arrow allocations are charged once
//! and shared by the byte-bounded log and any frame currently in transit.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::RecordBatch;
use laminar_core::checkpoint::CheckpointAttempt;
use parking_lot::Mutex;
use tokio::sync::watch;

mod lifecycle;
mod reader;

pub(crate) use lifecycle::SubscriptionRegistry;
pub(super) use reader::SubscriptionRead;
pub(crate) use reader::SubscriptionReader;
#[cfg(test)]
use reader::TryRead;

pub(super) const MAX_LIVE_BATCH_BYTES: usize = 8 * 1024 * 1024;

// Retention is a replay policy, not permission for unbounded live buffering.
// Two maximum-sized batches allow a portal to make progress while keeping the
// process-wide exposure finite when user retention is disabled.
const INTERNAL_LIVE_LOG_BYTES: usize = MAX_LIVE_BATCH_BYTES * 2;
const ENTRY_ACCOUNTING_BYTES: usize = 64;
const BARRIER_ENTRY_BYTES: usize = ENTRY_ACCOUNTING_BYTES + std::mem::size_of::<u64>() * 3;

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
        self.wake.send_modify(|()| {});
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
        Self::terminate_locked(&mut inner, message.clone());
        drop(inner);
        self.wake.send_modify(|()| {});
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
        self.wake.send_modify(|()| {});
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

        Ok(SubscriptionReader::attached(
            Arc::clone(self),
            reader_id,
            cursor,
            skip_barrier,
            wake,
        ))
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
            self.wake.send_modify(|()| {});
        }
    }

    fn terminate(&self, message: &str) {
        let mut inner = self.inner.lock();
        Self::terminate_locked(&mut inner, message.to_owned());
        drop(inner);
        self.wake.send_modify(|()| {});
    }

    fn terminate_and_replace(&self, message: &str, latest_committed_epoch: Option<u64>) -> Self {
        let mut inner = self.inner.lock();
        debug_assert!(
            inner.reserved_marker.is_none(),
            "subscription generation invalidated before its checkpoint marker was released"
        );
        let retention_cap = inner.retention_cap;
        let next_sequence = inner.next_sequence;
        Self::terminate_locked(&mut inner, message.to_owned());
        drop(inner);
        self.wake.send_modify(|()| {});
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

    fn terminate_locked(inner: &mut StreamLogInner, message: String) {
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

#[cfg(test)]
mod tests;
