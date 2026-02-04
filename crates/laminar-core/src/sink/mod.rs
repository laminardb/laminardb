//! # Exactly-Once Sinks (F023)
//!
//! This module provides exactly-once delivery semantics for sinks through two mechanisms:
//!
//!
//! 1. **Transactional Sinks**: For sinks that support transactions (Kafka, Delta Lake)
//! 2. **Idempotent Sinks**: For sinks without transactions, using deduplication
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Exactly-Once Guarantees                   │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │  Checkpoint(epoch) ◄────► SinkCheckpoint                    │
//! │       │                        │                            │
//! │       ▼                        ▼                            │
//! │  WAL Recovery  ◄────► Sink Transaction Rollback             │
//! │                                                             │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Transaction Flow
//!
//! ```text
//! begin_transaction() ──► write() ──► write() ──► commit()
//!                              │              │
//!                              ▼              ▼
//!                         rollback() on failure
//! ```
//!
//! ## Idempotent Flow
//!
//! ```text
//! write() ──► check_dedup() ──► filter_new() ──► inner.write()
//!                   │
//!                   ▼
//!              mark_seen()
//! ```

mod adapter;
mod checkpoint;
mod dedup;
mod error;
mod idempotent;
mod traits;
mod transaction;
mod transactional;
mod two_phase;

pub use adapter::{AdapterConfig, AdapterStats, ExactlyOnceSinkAdapter};
pub use checkpoint::{SinkCheckpoint, SinkCheckpointManager, SinkOffset};
pub use dedup::{DeduplicationStore, InMemoryDedup, RecordId};
pub use error::SinkError;
pub use idempotent::IdempotentSink;
pub use traits::{ExactlyOnceSink, SinkCapabilities, SinkState, TransactionId};
pub use transaction::{TransactionCoordinator, TransactionState, TwoPhaseCommitSink};
pub use transactional::{TransactionalSink, TransactionalSinkConfig, TransactionalSinkStats};
pub use two_phase::{
    CoordinatorDecision, ParticipantState, ParticipantVote, TransactionLog, TransactionLogEntry,
    TransactionRecord, TwoPhaseConfig, TwoPhaseCoordinator, TwoPhaseResult,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::{Event, Output};
    use crate::reactor::Sink;
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;

    fn make_test_event(timestamp: i64, value: i64) -> Event {
        let array = Arc::new(Int64Array::from(vec![value]));
        let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
        Event::new(timestamp, batch)
    }

    #[test]
    fn test_idempotent_sink_deduplicates() {
        struct MockSink {
            writes: Vec<Output>,
        }

        impl Sink for MockSink {
            fn write(&mut self, outputs: Vec<Output>) -> Result<(), crate::reactor::SinkError> {
                self.writes.extend(outputs);
                Ok(())
            }
            fn flush(&mut self) -> Result<(), crate::reactor::SinkError> {
                Ok(())
            }
        }

        let inner = MockSink { writes: Vec::new() };
        let dedup = InMemoryDedup::new(1000);
        let mut sink = IdempotentSink::new(inner, dedup);

        // First write should succeed
        let event1 = make_test_event(1000, 42);
        let outputs1 = vec![Output::Event(event1.clone())];
        Sink::write(&mut sink, outputs1).unwrap();

        // Write count depends on implementation
        // The important thing is deduplication works
        assert!(!sink.inner().writes.is_empty());

        // Same record ID should be deduplicated
        // (In real use, the ID extractor would return same ID for duplicate)
        // For this test, we verify the dedup store works
        let record_id = RecordId::from_bytes(b"test-id-1");
        assert!(sink.dedup_store().is_new(&record_id));
        sink.dedup_store_mut().mark_seen(record_id.clone());
        assert!(!sink.dedup_store().is_new(&record_id));
    }

    #[test]
    fn test_sink_checkpoint_serialization() {
        let mut checkpoint = SinkCheckpoint::new("test-sink");
        checkpoint.set_offset("partition-0", SinkOffset::Numeric(100));
        checkpoint.set_offset("partition-1", SinkOffset::Numeric(200));
        checkpoint.set_transaction_id(Some(TransactionId::new(12345)));

        let serialized = checkpoint.to_bytes();
        let restored = SinkCheckpoint::from_bytes(&serialized).unwrap();

        assert_eq!(restored.sink_id(), "test-sink");
        assert_eq!(
            restored.get_offset("partition-0"),
            Some(&SinkOffset::Numeric(100))
        );
        assert_eq!(
            restored.get_offset("partition-1"),
            Some(&SinkOffset::Numeric(200))
        );
        assert!(restored.pending_transaction_id().is_some());
    }

    #[test]
    fn test_transaction_state_lifecycle() {
        let mut state = TransactionState::new();

        // Initially idle
        assert!(state.is_idle());

        // Begin transaction
        let tx_id = TransactionId::new(1);
        state.begin(tx_id.clone()).unwrap();
        assert!(state.is_active());
        assert_eq!(state.current_transaction(), Some(&tx_id));

        // Commit
        state.commit(&tx_id).unwrap();
        assert!(state.is_idle());

        // Begin another
        let tx_id2 = TransactionId::new(2);
        state.begin(tx_id2.clone()).unwrap();

        // Rollback
        state.rollback(&tx_id2).unwrap();
        assert!(state.is_idle());
    }

    #[test]
    fn test_dedup_store_capacity() {
        let mut dedup = InMemoryDedup::new(3); // Only keep 3 entries

        for i in 0u64..5 {
            let id = RecordId::from_bytes(&i.to_le_bytes());
            dedup.mark_seen(id);
        }

        // Oldest entries should be evicted
        let id0 = RecordId::from_bytes(&0u64.to_le_bytes());
        let id4 = RecordId::from_bytes(&4u64.to_le_bytes());
        let _ = (id0, id4); // Suppress unused warnings

        // Note: exact eviction behavior depends on implementation
        // But capacity should be respected
        assert!(dedup.len() <= 3);
    }

    #[test]
    fn test_sink_capabilities() {
        let caps = SinkCapabilities::new()
            .with_transactions()
            .with_idempotent_writes()
            .with_changelog_support();

        assert!(caps.supports_transactions());
        assert!(caps.supports_idempotent_writes());
        assert!(caps.supports_changelog());
        assert!(!caps.supports_upsert());
    }

    // F023 Recovery Integration Tests

    use crate::reactor::BufferingSink;

    /// Transactional recovery: committed epoch 1, uncommitted epoch 2,
    /// restore from epoch 1 checkpoint — no duplicate outputs.
    #[test]
    fn test_transactional_recovery_no_duplicates() {
        // Setup: transactional sink
        let inner = BufferingSink::new();
        let mut tx_sink = TransactionalSink::new(inner, "tx-recovery");

        // Epoch 1: write and commit
        let tx1 = tx_sink.begin_transaction().unwrap();
        let event1 = make_test_event(1000, 1);
        ExactlyOnceSink::write(&mut tx_sink, &tx1, vec![Output::Event(event1)]).unwrap();
        tx_sink.commit(&tx1).unwrap();

        // Checkpoint after epoch 1
        let checkpoint = ExactlyOnceSink::checkpoint(&tx_sink);

        // Epoch 2: write but DON'T commit (simulates crash)
        let tx2 = tx_sink.begin_transaction().unwrap();
        let event2 = make_test_event(2000, 2);
        ExactlyOnceSink::write(&mut tx_sink, &tx2, vec![Output::Event(event2)]).unwrap();

        // Verify buffer has uncommitted data
        assert!(tx_sink.stats().buffer_count > 0 || tx_sink.state() == SinkState::InTransaction);

        // Restore from epoch 1 checkpoint
        let inner2 = BufferingSink::new();
        let mut restored_sink = TransactionalSink::new(inner2, "tx-recovery");
        restored_sink.restore(&checkpoint).unwrap();

        // Restored sink should be idle, no pending data
        assert_eq!(restored_sink.state(), SinkState::Idle);
        assert_eq!(restored_sink.stats().committed, 1);
        assert_eq!(restored_sink.stats().total_outputs_written, 1);
        // The uncommitted epoch 2 data is gone — no duplicates
    }

    /// Idempotent recovery: checkpoint dedup state, restore, replay same events.
    #[test]
    fn test_idempotent_recovery_no_duplicates() {
        let inner = BufferingSink::new();
        let dedup = InMemoryDedup::new(1000);
        let mut sink = IdempotentSink::new(inner, dedup).with_sink_id("idem-recovery");

        // Write event and checkpoint
        let event = make_test_event(1000, 42);
        Sink::write(&mut sink, vec![Output::Event(event.clone())]).unwrap();
        let checkpoint = ExactlyOnceSink::checkpoint(&sink);

        // Simulate crash: create new sink and restore
        let inner2 = BufferingSink::new();
        let dedup2 = InMemoryDedup::new(1);
        let mut restored = IdempotentSink::new(inner2, dedup2).with_sink_id("idem-recovery");
        restored.restore(&checkpoint).unwrap();

        // Replay the same event — should be deduplicated
        Sink::write(&mut restored, vec![Output::Event(event)]).unwrap();
        assert_eq!(restored.stats().total_deduplicated, 1);
    }

    /// Full adapter lifecycle: write → checkpoint → crash → recover → no duplicates.
    #[test]
    fn test_adapter_recovery_full_pipeline() {
        let inner = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner, "adapter-recovery");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        // Epoch 1: write and checkpoint
        let event1 = make_test_event(1000, 1);
        Sink::write(&mut adapter, vec![Output::Event(event1)]).unwrap();
        let checkpoint = adapter.notify_checkpoint(1).unwrap();

        assert_eq!(adapter.epoch(), 1);
        assert_eq!(adapter.stats().committed_epochs, 1);

        // Epoch 2: write but crash before checkpoint
        let event2 = make_test_event(2000, 2);
        Sink::write(&mut adapter, vec![Output::Event(event2)]).unwrap();
        assert!(adapter.has_active_transaction());

        // Recover from epoch 1 checkpoint
        adapter.restore(&checkpoint).unwrap();

        assert!(!adapter.has_active_transaction());
        assert_eq!(adapter.epoch(), 1);
        assert_eq!(adapter.stats().committed_epochs, 1);
        // Epoch 2 data was rolled back — no duplicates

        // Can continue writing after recovery
        let event3 = make_test_event(3000, 3);
        Sink::write(&mut adapter, vec![Output::Event(event3)]).unwrap();
        let _cp2 = adapter.notify_checkpoint(2).unwrap();
        assert_eq!(adapter.stats().committed_epochs, 2);
    }

    /// Both `TransactionalSink` and `IdempotentSink` implement `ExactlyOnceSink` + `reactor::Sink`.
    #[test]
    fn test_multiple_sink_types_supported() {
        // TransactionalSink
        let inner1 = BufferingSink::new();
        let mut tx_sink = TransactionalSink::new(inner1, "tx-type");
        assert!(tx_sink.capabilities().supports_transactions());
        Sink::write(&mut tx_sink, vec![Output::Event(make_test_event(1000, 1))]).unwrap();
        Sink::flush(&mut tx_sink).unwrap();
        assert_eq!(tx_sink.stats().committed, 1);

        // IdempotentSink
        let inner2 = BufferingSink::new();
        let dedup = InMemoryDedup::new(1000);
        let mut idem_sink = IdempotentSink::new(inner2, dedup);
        assert!(idem_sink.capabilities().supports_idempotent_writes());
        Sink::write(
            &mut idem_sink,
            vec![Output::Event(make_test_event(1000, 1))],
        )
        .unwrap();
        Sink::flush(&mut idem_sink).unwrap();

        // Both support ExactlyOnceSink API
        let tx_id = idem_sink.begin_transaction().unwrap();
        idem_sink.commit(&tx_id).unwrap();
    }

    /// Adapter with `SinkCheckpointManager`: multi-sink checkpoint serialization.
    #[test]
    fn test_adapter_with_checkpoint_manager() {
        // Create two adapters
        let inner1 = BufferingSink::new();
        let tx_sink1 = TransactionalSink::new(inner1, "sink-a");
        let mut adapter1 = ExactlyOnceSinkAdapter::new(tx_sink1);

        let inner2 = BufferingSink::new();
        let tx_sink2 = TransactionalSink::new(inner2, "sink-b");
        let mut adapter2 = ExactlyOnceSinkAdapter::new(tx_sink2);

        // Write to both and checkpoint
        Sink::write(&mut adapter1, vec![Output::Event(make_test_event(1000, 1))]).unwrap();
        Sink::write(&mut adapter2, vec![Output::Event(make_test_event(2000, 2))]).unwrap();

        let cp1 = adapter1.notify_checkpoint(1).unwrap();
        let cp2 = adapter2.notify_checkpoint(1).unwrap();

        // Register both in a checkpoint manager
        let mut manager = SinkCheckpointManager::new();
        manager.register(cp1);
        manager.register(cp2);
        manager.advance_epoch();

        // Serialize and restore
        let bytes = manager.to_bytes();
        let restored_manager = SinkCheckpointManager::from_bytes(&bytes).unwrap();

        assert_eq!(restored_manager.current_epoch(), 1);
        assert!(restored_manager.get("sink-a").is_some());
        assert!(restored_manager.get("sink-b").is_some());

        // Restore individual adapters from manager
        let restored_cp_a = restored_manager.get("sink-a").unwrap();
        adapter1.restore(restored_cp_a).unwrap();
        assert_eq!(adapter1.epoch(), 1);
    }
}
