//! Bounded process-local evidence for cluster checkpoint barrier pauses.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_core::cluster::control::LocalProcessAuthorityIdentity;
use laminar_core::state::CheckpointAttempt;

/// Process-lifetime barrier observations retained before incremental collection must run.
pub const CHECKPOINT_BARRIER_TIMING_CAPACITY: usize = 1_024;
/// Maximum records copied by one bounded snapshot.
pub const MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS: usize = 64;

/// Local role performed for one checkpoint barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointBarrierRole {
    /// This process coordinated the attempt.
    Leader,
    /// This process followed another process's announcement.
    Follower,
}

/// One exact process-local observation corresponding to one pipeline-stall histogram sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct CheckpointBarrierTimingRecord {
    /// Monotonic process-local sequence. Zero is never emitted.
    pub sequence: u64,
    /// Exact local process generation that performed the barrier.
    pub process: LocalProcessAuthorityIdentity,
    /// Canonical runtime checkpoint attempt.
    pub attempt: CheckpointAttempt,
    /// Local role in this attempt.
    pub role: CheckpointBarrierRole,
    /// Exact assignment version admitted for this attempt.
    pub assignment_version: u64,
    /// Digest of the complete admitted assignment certificate.
    pub assignment_digest: [u8; 32],
    /// Complete pipeline-pause observation in nanoseconds.
    pub pipeline_stall_ns: u64,
    /// Local barrier work in nanoseconds.
    pub local_barrier_ns: u64,
    /// Aligned-resume wait, when a cluster shuffle ran that stage.
    pub aligned_resume_ns: Option<u64>,
    /// Whether capture produced a durable-tail handoff before the local stage closed.
    pub durable_tail_handoff: bool,
    /// Whether the attempt's absolute deadline was exhausted when the pause closed.
    pub deadline_exhausted: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckpointBarrierTimingObservation {
    pub process: LocalProcessAuthorityIdentity,
    pub attempt: CheckpointAttempt,
    pub role: CheckpointBarrierRole,
    pub assignment_version: u64,
    pub assignment_digest: [u8; 32],
    pub pipeline_stall_ns: u64,
    pub local_barrier_ns: u64,
    pub aligned_resume_ns: Option<u64>,
    pub durable_tail_handoff: bool,
    pub deadline_exhausted: bool,
}

/// Canonical non-timing dimensions captured before one barrier pause.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckpointBarrierTimingContext {
    pub process: LocalProcessAuthorityIdentity,
    pub attempt: CheckpointAttempt,
    pub role: CheckpointBarrierRole,
    pub assignment_version: u64,
    pub assignment_digest: [u8; 32],
}

impl CheckpointBarrierTimingObservation {
    fn is_canonical(self) -> bool {
        self.process.is_canonical()
            && self.attempt.is_canonical()
            && self.assignment_version != 0
            && self.assignment_digest != [0; 32]
            && self.local_barrier_ns <= self.pipeline_stall_ns
            && self
                .aligned_resume_ns
                .is_none_or(|duration| duration <= self.pipeline_stall_ns)
            && self
                .local_barrier_ns
                .checked_add(self.aligned_resume_ns.unwrap_or(0))
                .is_some_and(|duration| duration <= self.pipeline_stall_ns)
            && (self.durable_tail_handoff || self.aligned_resume_ns.is_none())
    }

    fn with_sequence(self, sequence: u64) -> CheckpointBarrierTimingRecord {
        CheckpointBarrierTimingRecord {
            sequence,
            process: self.process,
            attempt: self.attempt,
            role: self.role,
            assignment_version: self.assignment_version,
            assignment_digest: self.assignment_digest,
            pipeline_stall_ns: self.pipeline_stall_ns,
            local_barrier_ns: self.local_barrier_ns,
            aligned_resume_ns: self.aligned_resume_ns,
            durable_tail_handoff: self.durable_tail_handoff,
            deadline_exhausted: self.deadline_exhausted,
        }
    }
}

struct CheckpointBarrierTimingState {
    slots: Box<[Option<CheckpointBarrierTimingRecord>]>,
    first: usize,
    len: usize,
    next_sequence: u64,
    overwritten_record_count: u64,
    process: Option<LocalProcessAuthorityIdentity>,
}

/// Fixed-capacity, process-lifetime checkpoint barrier timing ledger.
///
/// Writers never wait: contention, invalid metadata, and counter exhaustion increment explicit
/// loss state. The only allocation occurs during construction and bounded reader snapshots.
pub(crate) struct CheckpointBarrierTimingLedger {
    state: parking_lot::Mutex<CheckpointBarrierTimingState>,
    recording_loss_count: AtomicU64,
    metadata_exhausted: AtomicBool,
}

/// Drop-observing timer that feeds the existing histograms and one exact ledger record.
pub(crate) struct CheckpointBarrierTimingGuard {
    stall_histogram: prometheus::Histogram,
    local_histogram: prometheus::Histogram,
    aligned_histogram: prometheus::Histogram,
    ledger: Arc<CheckpointBarrierTimingLedger>,
    context: Option<CheckpointBarrierTimingContext>,
    stall_started: Instant,
    local_started: Instant,
    local_barrier_duration: Option<Duration>,
    aligned_started: Option<Instant>,
    aligned_resume_duration: Option<Duration>,
    durable_tail_handoff: bool,
    absolute_deadline: tokio::time::Instant,
    invalid_duration_state: bool,
}

impl CheckpointBarrierTimingGuard {
    pub(crate) fn start_with_context(
        context: impl FnOnce() -> Option<CheckpointBarrierTimingContext>,
        metrics: &crate::engine_metrics::EngineMetrics,
        ledger: &Arc<CheckpointBarrierTimingLedger>,
        absolute_deadline: tokio::time::Instant,
    ) -> Self {
        let started = Instant::now();
        let context = context();
        Self {
            stall_histogram: metrics.checkpoint_pipeline_stall_duration.clone(),
            local_histogram: metrics.checkpoint_barrier_local_duration.clone(),
            aligned_histogram: metrics.checkpoint_aligned_resume_wait.clone(),
            ledger: Arc::clone(ledger),
            context,
            stall_started: started,
            local_started: started,
            local_barrier_duration: None,
            aligned_started: None,
            aligned_resume_duration: None,
            durable_tail_handoff: false,
            absolute_deadline,
            invalid_duration_state: false,
        }
    }

    pub(crate) fn finish_local_barrier_with_handoff(&mut self) {
        self.durable_tail_handoff = true;
        self.finish_local_barrier();
    }

    pub(crate) fn begin_aligned_resume(&mut self) {
        if self.aligned_started.is_some() || self.aligned_resume_duration.is_some() {
            self.invalid_duration_state = true;
            return;
        }
        self.aligned_started = Some(Instant::now());
    }

    pub(crate) fn finish_aligned_resume(&mut self) {
        let Some(started) = self.aligned_started.take() else {
            self.invalid_duration_state = true;
            return;
        };
        self.aligned_resume_duration = Some(started.elapsed());
    }

    fn finish_local_barrier(&mut self) {
        if self.local_barrier_duration.is_some() {
            return;
        }
        self.local_barrier_duration = Some(self.local_started.elapsed());
    }
}

impl Drop for CheckpointBarrierTimingGuard {
    fn drop(&mut self) {
        self.finish_local_barrier();
        if self.aligned_started.is_some() {
            self.finish_aligned_resume();
        }
        let stall = self.stall_started.elapsed();
        let local_barrier_duration = self
            .local_barrier_duration
            .expect("local barrier duration is closed before observation");

        // Publish every duration only when the complete barrier scope closes. In particular, a
        // process killed after the durable-tail handoff must not leave a local/aligned sample
        // without the corresponding stall sample and exact ledger record.
        self.local_histogram
            .observe(local_barrier_duration.as_secs_f64());
        if let Some(aligned_resume_duration) = self.aligned_resume_duration {
            self.aligned_histogram
                .observe(aligned_resume_duration.as_secs_f64());
        }
        self.stall_histogram.observe(stall.as_secs_f64());
        let Some(pipeline_stall_ns) = duration_ns(stall) else {
            self.ledger.note_recording_loss();
            return;
        };
        let Some(local_barrier_ns) = duration_ns(local_barrier_duration) else {
            self.ledger.note_recording_loss();
            return;
        };
        let aligned_resume_ns = if let Some(duration) = self.aligned_resume_duration {
            let Some(duration_ns) = duration_ns(duration) else {
                self.ledger.note_recording_loss();
                return;
            };
            Some(duration_ns)
        } else {
            None
        };
        if self.invalid_duration_state {
            self.ledger.note_recording_loss();
            return;
        }
        let Some(context) = self.context else {
            self.ledger.note_recording_loss();
            return;
        };
        self.ledger.try_record(CheckpointBarrierTimingObservation {
            process: context.process,
            attempt: context.attempt,
            role: context.role,
            assignment_version: context.assignment_version,
            assignment_digest: context.assignment_digest,
            pipeline_stall_ns,
            local_barrier_ns,
            aligned_resume_ns,
            durable_tail_handoff: self.durable_tail_handoff,
            deadline_exhausted: tokio::time::Instant::now() >= self.absolute_deadline,
        });
    }
}

fn duration_ns(duration: Duration) -> Option<u64> {
    u64::try_from(duration.as_nanos()).ok()
}

impl CheckpointBarrierTimingLedger {
    pub(crate) fn new() -> Self {
        Self::with_capacity(CHECKPOINT_BARRIER_TIMING_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "checkpoint timing capacity must be nonzero");
        Self {
            state: parking_lot::Mutex::new(CheckpointBarrierTimingState {
                slots: vec![None; capacity].into_boxed_slice(),
                first: 0,
                len: 0,
                next_sequence: 1,
                overwritten_record_count: 0,
                process: None,
            }),
            recording_loss_count: AtomicU64::new(0),
            metadata_exhausted: AtomicBool::new(false),
        }
    }

    /// Attempt one O(1), allocation-free record write without waiting for a reader or writer.
    pub(crate) fn try_record(&self, observation: CheckpointBarrierTimingObservation) -> bool {
        if !observation.is_canonical() {
            self.note_recording_loss();
            return false;
        }
        let Some(mut state) = self.state.try_lock() else {
            self.note_recording_loss();
            return false;
        };
        if state.next_sequence == u64::MAX {
            self.metadata_exhausted.store(true, Ordering::Release);
            drop(state);
            self.note_recording_loss();
            return false;
        }
        if state.len == state.slots.len() && state.overwritten_record_count == u64::MAX {
            self.metadata_exhausted.store(true, Ordering::Release);
            drop(state);
            self.note_recording_loss();
            return false;
        }
        if state
            .process
            .is_some_and(|process| process != observation.process)
        {
            drop(state);
            self.note_recording_loss();
            return false;
        }

        let sequence = state.next_sequence;
        state.next_sequence += 1;
        state.process.get_or_insert(observation.process);
        let index = if state.len < state.slots.len() {
            let index = (state.first + state.len) % state.slots.len();
            state.len += 1;
            index
        } else {
            let index = state.first;
            state.first = (state.first + 1) % state.slots.len();
            state.overwritten_record_count += 1;
            index
        };
        state.slots[index] = Some(observation.with_sequence(sequence));
        true
    }

    pub(crate) fn note_recording_loss(&self) {
        if self
            .recording_loss_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .is_err()
        {
            self.metadata_exhausted.store(true, Ordering::Release);
        }
    }

    /// Copy one bounded, exclusive-cursor page without performing I/O.
    pub(crate) fn snapshot_after(
        &self,
        after_sequence: u64,
        limit: usize,
    ) -> Result<CheckpointBarrierTimingSnapshot, CheckpointBarrierTimingSnapshotError> {
        if !(1..=MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS).contains(&limit) {
            return Err(CheckpointBarrierTimingSnapshotError::InvalidLimit { limit });
        }
        let mut records = Vec::with_capacity(limit);
        let Some(state) = self.state.try_lock() else {
            return Err(CheckpointBarrierTimingSnapshotError::Busy);
        };
        if after_sequence >= state.next_sequence {
            return Err(CheckpointBarrierTimingSnapshotError::CursorAhead {
                after_sequence,
                next_sequence: state.next_sequence,
            });
        }

        let oldest_retained_sequence = (state.len != 0).then(|| {
            state.slots[state.first]
                .expect("a retained timing slot must be populated")
                .sequence
        });
        if oldest_retained_sequence.is_some_and(|oldest| after_sequence.saturating_add(1) < oldest)
        {
            return Err(CheckpointBarrierTimingSnapshotError::CursorOverwritten {
                after_sequence,
                oldest_retained_sequence: oldest_retained_sequence.unwrap_or(1),
            });
        }

        let logical_record = |offset: usize| {
            let index = (state.first + offset) % state.slots.len();
            state.slots[index].expect("a retained timing slot must be populated")
        };
        let mut low = 0;
        let mut high = state.len;
        while low < high {
            let middle = low + (high - low) / 2;
            if logical_record(middle).sequence <= after_sequence {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let end = low.saturating_add(limit).min(state.len);
        for offset in low..end {
            records.push(logical_record(offset));
        }
        let snapshot = CheckpointBarrierTimingSnapshot {
            process: state.process,
            capacity: state.slots.len(),
            oldest_retained_sequence,
            next_sequence: state.next_sequence,
            overwritten_record_count: state.overwritten_record_count,
            recording_loss_count: self.recording_loss_count.load(Ordering::Acquire),
            metadata_exhausted: self.metadata_exhausted.load(Ordering::Acquire),
            has_more: end < state.len,
            records,
        };
        drop(state);
        Ok(snapshot)
    }
}

/// One bounded in-memory ledger page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CheckpointBarrierTimingSnapshot {
    /// Process identity that owns this sequence domain, or `None` before the first record.
    pub process: Option<LocalProcessAuthorityIdentity>,
    /// Fixed process-lifetime record capacity.
    pub capacity: usize,
    /// Oldest successful record still retained, or `None` before the first record.
    pub oldest_retained_sequence: Option<u64>,
    /// Next successful sequence; the last accepted sequence is one less.
    pub next_sequence: u64,
    /// Successful records evicted by fixed-capacity overwrite.
    pub overwritten_record_count: u64,
    /// Observations lost to contention, invalid metadata/durations, or exhausted counters.
    pub recording_loss_count: u64,
    /// Sequence or loss metadata can no longer advance without ambiguity.
    pub metadata_exhausted: bool,
    /// More retained records follow this page.
    pub has_more: bool,
    /// Ordered records whose sequence is strictly greater than the requested cursor.
    pub records: Vec<CheckpointBarrierTimingRecord>,
}

/// Failure to take one trustworthy bounded ledger page.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CheckpointBarrierTimingSnapshotError {
    /// The caller supplied a zero or oversized page.
    #[error("checkpoint timing page limit {limit} is outside the supported range")]
    InvalidLimit {
        /// Requested record count.
        limit: usize,
    },
    /// The nonblocking reader found another ledger operation in progress.
    #[error("checkpoint timing ledger is busy")]
    Busy,
    /// Fixed retention already evicted records after the supplied cursor.
    #[error(
        "checkpoint timing cursor {after_sequence} precedes oldest retained sequence {oldest_retained_sequence}"
    )]
    CursorOverwritten {
        /// Caller-supplied exclusive cursor.
        after_sequence: u64,
        /// First record still retained.
        oldest_retained_sequence: u64,
    },
    /// The supplied cursor is outside the current numeric sequence range.
    #[error(
        "checkpoint timing cursor {after_sequence} is at or beyond next sequence {next_sequence}"
    )]
    CursorAhead {
        /// Caller-supplied exclusive cursor.
        after_sequence: u64,
        /// Next sequence this process may issue.
        next_sequence: u64,
    },
}

/// One process-bound, bounded in-memory ledger page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CheckpointBarrierTimingPage {
    /// Live local process identity sampled around the ledger read.
    pub process: LocalProcessAuthorityIdentity,
    /// Bounded page from that process's sequence domain.
    pub snapshot: CheckpointBarrierTimingSnapshot,
}

/// Failure to read a process-bound checkpoint timing page.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CheckpointBarrierTimingReadError {
    /// A nonzero sequence cursor omitted its process-generation identity.
    #[error("checkpoint timing continuation cursor requires a process identity")]
    ProcessIdentityRequired,
    /// A live local process identity could not be sampled without waiting.
    #[error("checkpoint timing process identity is unavailable")]
    ProcessIdentityUnavailable,
    /// The supplied cursor belongs to a different process generation.
    #[error("checkpoint timing cursor process identity does not match the live process")]
    ProcessIdentityMismatch {
        /// Process identity supplied with the cursor.
        expected: LocalProcessAuthorityIdentity,
        /// Live process identity observed before the read.
        actual: LocalProcessAuthorityIdentity,
    },
    /// Process authority changed while the in-memory ledger was read.
    #[error("checkpoint timing process identity changed during the read")]
    ProcessIdentityChanged {
        /// Identity observed before the ledger read.
        before: LocalProcessAuthorityIdentity,
        /// Identity observed after the ledger read.
        after: LocalProcessAuthorityIdentity,
    },
    /// The ledger's immutable sequence domain contradicts current process authority.
    #[error("checkpoint timing ledger belongs to a different process identity")]
    LedgerProcessMismatch {
        /// Live process identity sampled around the read.
        expected: LocalProcessAuthorityIdentity,
        /// Process identity bound to the ledger's sequence domain.
        actual: LocalProcessAuthorityIdentity,
    },
    /// The bounded ledger snapshot itself failed.
    #[error(transparent)]
    Snapshot(#[from] CheckpointBarrierTimingSnapshotError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_core::checkpoint::CheckpointParticipant;
    use uuid::Uuid;

    fn observation(checkpoint_id: u64) -> CheckpointBarrierTimingObservation {
        CheckpointBarrierTimingObservation {
            process: LocalProcessAuthorityIdentity {
                participant: CheckpointParticipant {
                    node_id: 7,
                    boot_incarnation: Uuid::from_u128(77),
                },
                process_term: 3,
            },
            attempt: CheckpointAttempt::canonical(checkpoint_id),
            role: CheckpointBarrierRole::Follower,
            assignment_version: 9,
            assignment_digest: [11; 32],
            pipeline_stall_ns: checkpoint_id * 100,
            local_barrier_ns: checkpoint_id * 50,
            aligned_resume_ns: Some(checkpoint_id * 10),
            durable_tail_handoff: true,
            deadline_exhausted: false,
        }
    }

    fn context(checkpoint_id: u64) -> CheckpointBarrierTimingContext {
        let observation = observation(checkpoint_id);
        CheckpointBarrierTimingContext {
            process: observation.process,
            attempt: observation.attempt,
            role: observation.role,
            assignment_version: observation.assignment_version,
            assignment_digest: observation.assignment_digest,
        }
    }

    #[test]
    fn composite_guard_keeps_histogram_and_ledger_counts_in_lockstep() {
        let registry = prometheus::Registry::new();
        let metrics = crate::engine_metrics::EngineMetrics::new(&registry);
        let ledger = Arc::new(CheckpointBarrierTimingLedger::with_capacity(4));
        {
            let mut guard = CheckpointBarrierTimingGuard::start_with_context(
                || Some(context(1)),
                &metrics,
                &ledger,
                tokio::time::Instant::now() + Duration::from_secs(1),
            );
            guard.finish_local_barrier_with_handoff();
            guard.begin_aligned_resume();
            guard.finish_aligned_resume();
            assert_eq!(
                metrics
                    .checkpoint_pipeline_stall_duration
                    .get_sample_count(),
                0
            );
            assert_eq!(
                metrics.checkpoint_barrier_local_duration.get_sample_count(),
                0
            );
            assert_eq!(metrics.checkpoint_aligned_resume_wait.get_sample_count(), 0);
        }
        {
            let _early_return = CheckpointBarrierTimingGuard::start_with_context(
                || Some(context(2)),
                &metrics,
                &ledger,
                tokio::time::Instant::now(),
            );
        }

        assert_eq!(
            metrics
                .checkpoint_pipeline_stall_duration
                .get_sample_count(),
            2
        );
        assert_eq!(
            metrics.checkpoint_barrier_local_duration.get_sample_count(),
            2
        );
        assert_eq!(metrics.checkpoint_aligned_resume_wait.get_sample_count(), 1);
        let snapshot = ledger.snapshot_after(0, 4).unwrap();
        assert_eq!(snapshot.process, Some(context(1).process));
        assert_eq!(snapshot.next_sequence, 3);
        assert_eq!(snapshot.recording_loss_count, 0);
        assert_eq!(snapshot.records.len(), 2);
        assert!(snapshot.records[0].durable_tail_handoff);
        assert!(snapshot.records[0].aligned_resume_ns.is_some());
        assert!(!snapshot.records[0].deadline_exhausted);
        assert!(!snapshot.records[1].durable_tail_handoff);
        assert_eq!(snapshot.records[1].aligned_resume_ns, None);
        assert!(snapshot.records[1].deadline_exhausted);
        assert!(snapshot
            .records
            .iter()
            .all(|record| record.local_barrier_ns <= record.pipeline_stall_ns));
    }

    #[test]
    fn context_loss_preserves_histograms_and_is_explicit() {
        let registry = prometheus::Registry::new();
        let metrics = crate::engine_metrics::EngineMetrics::new(&registry);
        let ledger = Arc::new(CheckpointBarrierTimingLedger::with_capacity(2));

        {
            let _guard = CheckpointBarrierTimingGuard::start_with_context(
                || None,
                &metrics,
                &ledger,
                tokio::time::Instant::now() + Duration::from_secs(1),
            );
        }

        assert_eq!(
            metrics
                .checkpoint_pipeline_stall_duration
                .get_sample_count(),
            1
        );
        assert_eq!(
            metrics.checkpoint_barrier_local_duration.get_sample_count(),
            1
        );
        let snapshot = ledger.snapshot_after(0, 2).unwrap();
        assert_eq!(snapshot.process, None);
        assert_eq!(snapshot.recording_loss_count, 1);
        assert!(snapshot.records.is_empty());
    }

    #[test]
    fn ledger_sequences_and_pages_are_exact_and_exclusive() {
        let ledger = CheckpointBarrierTimingLedger::with_capacity(4);
        assert!(ledger.try_record(observation(1)));
        assert!(ledger.try_record(observation(2)));
        assert!(ledger.try_record(observation(3)));

        let first = ledger.snapshot_after(0, 2).unwrap();
        assert_eq!(first.capacity, 4);
        assert_eq!(first.oldest_retained_sequence, Some(1));
        assert_eq!(first.next_sequence, 4);
        assert!(first.has_more);
        assert_eq!(
            first
                .records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let second = ledger.snapshot_after(2, 2).unwrap();
        assert!(!second.has_more);
        assert_eq!(second.records, vec![observation(3).with_sequence(3)]);
        assert_eq!(ledger.snapshot_after(3, 1).unwrap().records, Vec::new());
    }

    #[test]
    fn overwrite_and_stale_cursor_are_explicit() {
        let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
        assert!(ledger.try_record(observation(1)));
        assert!(ledger.try_record(observation(2)));
        assert!(ledger.try_record(observation(3)));

        assert_eq!(
            ledger.snapshot_after(0, 2),
            Err(CheckpointBarrierTimingSnapshotError::CursorOverwritten {
                after_sequence: 0,
                oldest_retained_sequence: 2,
            })
        );
        let retained = ledger.snapshot_after(1, 2).unwrap();
        assert_eq!(retained.overwritten_record_count, 1);
        assert_eq!(
            retained
                .records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn writer_contention_loses_evidence_without_advancing_sequence() {
        let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
        let guard = ledger.state.lock();
        assert!(!ledger.try_record(observation(1)));
        drop(guard);
        assert!(ledger.try_record(observation(2)));

        let snapshot = ledger.snapshot_after(0, 2).unwrap();
        assert_eq!(snapshot.recording_loss_count, 1);
        assert_eq!(snapshot.next_sequence, 2);
        assert_eq!(snapshot.records[0].sequence, 1);
        assert_eq!(snapshot.records[0].attempt, CheckpointAttempt::canonical(2));
    }

    #[test]
    fn invalid_observation_and_sequence_exhaustion_fail_closed() {
        let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
        let mut invalid = observation(1);
        invalid.local_barrier_ns = invalid.pipeline_stall_ns + 1;
        assert!(!ledger.try_record(invalid));
        let mut overlapping = observation(2);
        overlapping.local_barrier_ns = 150;
        overlapping.aligned_resume_ns = Some(100);
        assert!(!ledger.try_record(overlapping));
        {
            let mut state = ledger.state.lock();
            state.next_sequence = u64::MAX;
        }
        assert!(!ledger.try_record(observation(3)));
        let snapshot = ledger.snapshot_after(u64::MAX - 1, 1).unwrap();
        assert_eq!(snapshot.recording_loss_count, 3);
        assert!(snapshot.metadata_exhausted);
        assert!(snapshot.records.is_empty());
    }

    #[test]
    fn ledger_rejects_a_second_process_identity() {
        let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
        let first = observation(1);
        assert!(ledger.try_record(first));
        let mut other_process = observation(2);
        other_process.process.process_term += 1;
        assert!(!ledger.try_record(other_process));

        let snapshot = ledger.snapshot_after(0, 2).unwrap();
        assert_eq!(snapshot.process, Some(first.process));
        assert_eq!(snapshot.recording_loss_count, 1);
        assert_eq!(snapshot.records, vec![first.with_sequence(1)]);
    }

    #[test]
    fn bounds_and_cursor_ahead_are_rejected() {
        let ledger = CheckpointBarrierTimingLedger::with_capacity(2);
        assert_eq!(
            ledger.snapshot_after(0, 0),
            Err(CheckpointBarrierTimingSnapshotError::InvalidLimit { limit: 0 })
        );
        assert_eq!(
            ledger.snapshot_after(1, 1),
            Err(CheckpointBarrierTimingSnapshotError::CursorAhead {
                after_sequence: 1,
                next_sequence: 1,
            })
        );
        assert!(std::mem::size_of::<CheckpointBarrierTimingRecord>() <= 192);
    }
}
