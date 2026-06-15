//! Bounded in-memory log of inference calls, surfaced as `laminar.ai_calls`.
//! One record per batch; written from Ring 1, never Ring 0. Oldest record is
//! dropped when full; a monotonic counter tracks lifetime call count.

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
    /// Epoch milliseconds at call completion.
    pub timestamp_ms: i64,
    /// Registry model name.
    pub model: String,
    /// Provider name (e.g. `anthropic`).
    pub provider: &'static str,
    /// Task performed.
    pub task: Task,
    /// Backend kind.
    pub kind: BackendKind,
    /// Number of rows in the batch.
    pub batch_size: u32,
    /// Zero for local calls.
    pub usage: Usage,
    /// End-to-end latency.
    pub latency_ms: u64,
    /// Outcome.
    pub outcome: CallOutcome,
}

/// Bounded ring buffer of call records.
#[derive(Debug)]
pub struct AiCallLog {
    records: Mutex<VecDeque<AiCallRecord>>,
    capacity: usize,
    recorded: AtomicU64,
}

impl AiCallLog {
    /// Retain at most `capacity` of the most recent records.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            records: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
            capacity: capacity.max(1),
            recorded: AtomicU64::new(0),
        }
    }

    /// Create a log with a default capacity of 2 000 records.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(2_000)
    }

    /// Append a record; evicts the oldest if at capacity.
    pub fn record(&self, record: AiCallRecord) {
        let mut records = self.records.lock();
        if records.len() >= self.capacity {
            records.pop_front();
        }
        records.push_back(record);
        self.recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot retained records (oldest first). Backs `laminar.ai_calls`.
    #[must_use]
    pub fn snapshot(&self) -> Vec<AiCallRecord> {
        self.records.lock().iter().cloned().collect()
    }

    /// Number of retained records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    /// Whether the log holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.lock().is_empty()
    }

    /// Total calls ever logged, including evicted records.
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
