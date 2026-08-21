//! Registry membership, checkpoint cuts, and delivery-generation ownership.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use laminar_core::checkpoint::CheckpointAttempt;
use parking_lot::{Mutex, RwLock};

#[cfg(test)]
use super::{calculate_retention_suffix, head_sequence};
use super::{
    AppendOutcome, MvUpdate, ReservedAppendOutcome, StreamLog, SubscribeStart,
    SubscriptionMemoryBudget, SubscriptionOpenError, SubscriptionReader, BARRIER_ENTRY_BYTES,
    MAX_LIVE_BATCH_BYTES,
};

const PROCESS_SUBSCRIPTION_BYTES: usize = 256 * 1024 * 1024;

static PROCESS_SUBSCRIPTION_BUDGET: OnceLock<Arc<SubscriptionMemoryBudget>> = OnceLock::new();

pub(super) struct ReservedMarker {
    log: Arc<StreamLog>,
    through_sequence: u64,
}

pub(super) struct ReservedCut {
    pub(super) attempt: CheckpointAttempt,
    pub(super) markers: Vec<ReservedMarker>,
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
pub(super) struct RegistryLifecycle {
    latest_committed_epoch: Option<u64>,
    pub(super) pending_cut: Option<ReservedCut>,
}

pub(crate) struct SubscriptionRegistry {
    pub(super) lifecycle: Mutex<RegistryLifecycle>,
    pub(super) streams: RwLock<HashMap<String, Arc<StreamLog>>>,
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

    pub(super) fn with_budget(budget: Arc<SubscriptionMemoryBudget>) -> Self {
        Self {
            lifecycle: Mutex::new(RegistryLifecycle::default()),
            streams: RwLock::new(HashMap::new()),
            budget,
        }
    }

    #[cfg(test)]
    pub(in crate::subscription) fn with_storage_budget(limit: usize) -> Self {
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
            release_reserved_cut(&cut, &self.budget);
        }
    }

    /// End the current in-memory delivery generation before recovery can replay it.
    /// Existing readers receive a terminal error; replacement logs retain their
    /// configured byte caps and continue that object's current in-process cursor.
    pub(crate) fn invalidate_all(&self, reason: &str) {
        let mut lifecycle = self.lifecycle.lock();
        if let Some(cut) = lifecycle.pending_cut.take() {
            release_reserved_cut(&cut, &self.budget);
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
    pub(super) fn head_sequence(&self, name: &str) -> Option<u64> {
        self.streams
            .read()
            .get(name)
            .map(|log| head_sequence(&log.inner.lock()))
    }

    #[cfg(test)]
    pub(super) fn next_sequence(&self, name: &str) -> Option<u64> {
        self.streams
            .read()
            .get(name)
            .map(|log| log.inner.lock().next_sequence)
    }

    #[cfg(test)]
    pub(in crate::subscription) fn charged_bytes(&self) -> usize {
        self.budget.used()
    }

    #[cfg(test)]
    pub(super) fn assert_retention_cache(&self, name: &str) {
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

fn release_reserved_cut(cut: &ReservedCut, budget: &SubscriptionMemoryBudget) {
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
            release_reserved_cut(&cut, &self.budget);
        }
    }
}

impl Default for SubscriptionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
