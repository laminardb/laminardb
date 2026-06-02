//! Bounded in-memory log of inference calls, surfaced as `laminar.ai_calls`.
//!
//! One record per batch call — local and remote alike. Remote calls carry
//! tokens and cost; local calls report [`Usage::ZERO`](crate::ai::provider::Usage::ZERO).
//! The log is written from the Ring 1 inference worker and read by queries
//! against `laminar.ai_calls`, so it is behind a lock; it is never touched on
//! Ring 0. The buffer is bounded — the oldest record is dropped once full — and
//! a monotonic counter records how many calls were ever logged.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::ai::provider::Usage;
use crate::ai::registry::{BackendKind, Task};

/// Outcome of a batch call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallOutcome {
    /// The batch completed.
    Success,
    /// The batch failed; carries the surfaced error message.
    Failure(String),
}

impl CallOutcome {
    /// Short status string for the `laminar.ai_calls` view (`ok` / `error`).
    #[must_use]
    pub fn status(&self) -> &'static str {
        match self {
            CallOutcome::Success => "ok",
            CallOutcome::Failure(_) => "error",
        }
    }
}

/// One logged inference call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiCallRecord {
    /// Wall-clock time the call completed (epoch milliseconds).
    pub timestamp_ms: i64,
    /// Registry model name (e.g. `finbert`, `haiku`).
    pub model: String,
    /// Backend-kind identity reported by the provider (e.g. `anthropic`).
    pub provider: &'static str,
    /// The task performed.
    pub task: Task,
    /// The backend kind.
    pub kind: BackendKind,
    /// Number of rows in the batch.
    pub batch_size: u32,
    /// Token/cost accounting (zero for local).
    pub usage: Usage,
    /// End-to-end latency of the batch call.
    pub latency_ms: u64,
    /// Outcome.
    pub outcome: CallOutcome,
}

/// Bounded ring buffer of [`AiCallRecord`]s.
#[derive(Debug)]
pub struct AiCallLog {
    records: Mutex<VecDeque<AiCallRecord>>,
    capacity: usize,
    recorded: AtomicU64,
}

impl AiCallLog {
    /// Create a log retaining at most `capacity` of the most recent records.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            records: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
            capacity: capacity.max(1),
            recorded: AtomicU64::new(0),
        }
    }

    /// Create a log with a default capacity of 10,000 records.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(10_000)
    }

    /// Append a record, evicting the oldest if at capacity.
    pub fn record(&self, record: AiCallRecord) {
        let mut records = self.records.lock();
        if records.len() >= self.capacity {
            records.pop_front();
        }
        records.push_back(record);
        self.recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot the retained records, oldest first. Backs `laminar.ai_calls`.
    #[must_use]
    pub fn snapshot(&self) -> Vec<AiCallRecord> {
        self.records.lock().iter().cloned().collect()
    }

    /// Number of records currently retained.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    /// Whether the log currently holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.lock().is_empty()
    }

    /// Total calls ever logged, including those evicted from the buffer.
    #[must_use]
    pub fn total_recorded(&self) -> u64 {
        self.recorded.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(model: &str, ts: i64, outcome: CallOutcome) -> AiCallRecord {
        AiCallRecord {
            timestamp_ms: ts,
            model: model.to_string(),
            provider: "anthropic",
            task: Task::Classify,
            kind: BackendKind::Remote,
            batch_size: 4,
            usage: Usage {
                input_tokens: 10,
                output_tokens: 2,
                cost_micros: 5,
            },
            latency_ms: 42,
            outcome,
        }
    }

    #[test]
    fn records_and_snapshots_in_order() {
        let log = AiCallLog::with_defaults();
        assert!(log.is_empty());
        log.record(record("haiku", 1, CallOutcome::Success));
        log.record(record("haiku", 2, CallOutcome::Failure("timeout".into())));
        let snap = log.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].timestamp_ms, 1);
        assert_eq!(snap[1].outcome, CallOutcome::Failure("timeout".into()));
        assert_eq!(log.total_recorded(), 2);
    }

    #[test]
    fn evicts_oldest_when_full() {
        let log = AiCallLog::new(2);
        log.record(record("m", 1, CallOutcome::Success));
        log.record(record("m", 2, CallOutcome::Success));
        log.record(record("m", 3, CallOutcome::Success));
        let snap = log.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].timestamp_ms, 2, "oldest dropped");
        assert_eq!(snap[1].timestamp_ms, 3);
        assert_eq!(log.total_recorded(), 3, "monotonic count survives eviction");
    }
}
