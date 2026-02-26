//! Idempotent sink wrapper for exactly-once delivery without transactions

use super::checkpoint::SinkCheckpoint;
use super::dedup::{DeduplicationStore, RecordId};
use super::error::SinkError;
use super::traits::{ExactlyOnceSink, SinkCapabilities, SinkState, TransactionId};
use super::transaction::TransactionState;
use crate::operator::Output;
use crate::reactor::Sink;

/// Extracts record IDs from outputs for deduplication
pub trait RecordIdExtractor: Send {
    /// Extract record IDs from a batch of outputs
    fn extract_ids(&self, outputs: &[Output]) -> Vec<RecordId>;
}

/// Default ID extractor that uses event timestamp + data hash
#[derive(Debug, Clone, Default)]
pub struct DefaultIdExtractor;

impl RecordIdExtractor for DefaultIdExtractor {
    fn extract_ids(&self, outputs: &[Output]) -> Vec<RecordId> {
        outputs
            .iter()
            .filter_map(|output| {
                match output {
                    Output::Event(event) => {
                        // Use timestamp + batch memory address as ID
                        // In production, this should use a proper record ID field
                        let mut bytes = event.timestamp.to_le_bytes().to_vec();
                        bytes.extend_from_slice(
                            &(event.data.get_array_memory_size() as u64).to_le_bytes(),
                        );
                        Some(RecordId::from_hash(&bytes))
                    }
                    Output::Changelog(record) => {
                        // Use emit timestamp + operation + event timestamp
                        let mut bytes = record.emit_timestamp.to_le_bytes().to_vec();
                        bytes.push(record.operation.to_u8());
                        bytes.extend_from_slice(&record.event.timestamp.to_le_bytes());
                        Some(RecordId::from_hash(&bytes))
                    }
                    _ => None, // Watermarks and side outputs don't need dedup
                }
            })
            .collect()
    }
}

/// Column-based ID extractor that uses specific column values
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ColumnIdExtractor {
    /// Column names to use as the ID
    columns: Vec<String>,
}

#[allow(dead_code)]
impl ColumnIdExtractor {
    /// Create a new column-based extractor
    #[must_use]
    pub fn new(columns: Vec<String>) -> Self {
        Self { columns }
    }

    /// Create an extractor for a single column
    #[must_use]
    pub fn single(column: impl Into<String>) -> Self {
        Self {
            columns: vec![column.into()],
        }
    }
}

impl RecordIdExtractor for ColumnIdExtractor {
    fn extract_ids(&self, outputs: &[Output]) -> Vec<RecordId> {
        outputs
            .iter()
            .filter_map(|output| {
                let batch = match output {
                    Output::Event(event) => &event.data,
                    Output::Changelog(record) => &record.event.data,
                    _ => return None,
                };

                // Build composite ID from column values
                let mut parts: Vec<Vec<u8>> = Vec::with_capacity(self.columns.len());
                for col_name in &self.columns {
                    if let Ok(col_idx) = batch.schema().index_of(col_name) {
                        let col = batch.column(col_idx);
                        // Convert column to bytes (simplified - in production use proper Arrow conversion)
                        let bytes = format!("{col:?}").into_bytes();
                        parts.push(bytes);
                    }
                }

                if parts.is_empty() {
                    None
                } else {
                    let part_refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
                    Some(RecordId::composite(&part_refs))
                }
            })
            .collect()
    }
}

/// Idempotent sink wrapper that provides exactly-once delivery via deduplication
///
/// This wrapper is used for sinks that don't support transactions. It works by:
/// 1. Tracking record IDs that have been written
/// 2. Filtering out duplicates before writing
/// 3. Persisting the dedup store in checkpoints for recovery
///
/// # Example
///
/// ```ignore
/// use laminar_core::sink::{IdempotentSink, InMemoryDedup};
///
/// let inner_sink = MySink::new();
/// let dedup = InMemoryDedup::new(100_000);
/// let mut sink = IdempotentSink::new(inner_sink, dedup);
///
/// // Writes are deduplicated
/// sink.write(outputs)?;
/// ```
pub struct IdempotentSink<S, D, E = DefaultIdExtractor>
where
    S: Sink,
    D: DeduplicationStore,
    E: RecordIdExtractor,
{
    /// The underlying sink
    inner: S,

    /// Deduplication store
    dedup: D,

    /// Record ID extractor
    id_extractor: E,

    /// Transaction state (for API compatibility)
    tx_state: TransactionState,

    /// Sink identifier
    sink_id: String,

    /// Statistics
    total_received: u64,
    total_deduplicated: u64,
    total_written: u64,
}

impl<S, D> IdempotentSink<S, D, DefaultIdExtractor>
where
    S: Sink,
    D: DeduplicationStore,
{
    /// Create a new idempotent sink with default ID extraction
    pub fn new(inner: S, dedup: D) -> Self {
        Self::with_extractor(inner, dedup, DefaultIdExtractor)
    }
}

impl<S, D, E> IdempotentSink<S, D, E>
where
    S: Sink,
    D: DeduplicationStore,
    E: RecordIdExtractor,
{
    /// Create a new idempotent sink with custom ID extraction
    pub fn with_extractor(inner: S, dedup: D, id_extractor: E) -> Self {
        Self {
            inner,
            dedup,
            id_extractor,
            tx_state: TransactionState::new(),
            sink_id: format!("idempotent-sink-{}", uuid::Uuid::new_v4()),
            total_received: 0,
            total_deduplicated: 0,
            total_written: 0,
        }
    }

    /// Set the sink identifier
    #[must_use]
    pub fn with_sink_id(mut self, sink_id: impl Into<String>) -> Self {
        self.sink_id = sink_id.into();
        self
    }

    /// Get a reference to the inner sink
    #[must_use]
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get a mutable reference to the inner sink
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Get the deduplication store
    #[must_use]
    pub fn dedup_store(&self) -> &D {
        &self.dedup
    }

    /// Get a mutable reference to the deduplication store
    pub fn dedup_store_mut(&mut self) -> &mut D {
        &mut self.dedup
    }

    /// Get statistics
    #[must_use]
    pub fn stats(&self) -> IdempotentSinkStats {
        IdempotentSinkStats {
            total_received: self.total_received,
            total_deduplicated: self.total_deduplicated,
            total_written: self.total_written,
            dedup_store_size: self.dedup.len(),
        }
    }

    /// Write outputs with deduplication
    fn write_deduplicated(&mut self, outputs: Vec<Output>) -> Result<(), SinkError> {
        if outputs.is_empty() {
            return Ok(());
        }

        self.total_received += outputs.len() as u64;

        // Extract record IDs
        let ids = self.id_extractor.extract_ids(&outputs);

        // Find new (non-duplicate) records
        let mut new_outputs = Vec::with_capacity(outputs.len());
        let mut new_ids = Vec::with_capacity(ids.len());

        for (output, id) in outputs.into_iter().zip(ids.into_iter()) {
            if self.dedup.is_new(&id) {
                new_outputs.push(output);
                new_ids.push(id);
            } else {
                self.total_deduplicated += 1;
            }
        }

        if new_outputs.is_empty() {
            return Ok(());
        }

        // Write to inner sink
        self.inner
            .write(&mut new_outputs)
            .map_err(|e| SinkError::WriteFailed(e.to_string()))?;

        // Mark IDs as seen after successful write
        self.dedup.mark_seen_batch(new_ids);
        self.total_written += 1;

        Ok(())
    }
}

impl<S, D, E> Sink for IdempotentSink<S, D, E>
where
    S: Sink,
    D: DeduplicationStore,
    E: RecordIdExtractor,
{
    fn write(&mut self, outputs: &mut Vec<Output>) -> Result<(), crate::reactor::SinkError> {
        let owned = std::mem::take(outputs);
        self.write_deduplicated(owned)
            .map_err(crate::reactor::SinkError::from)
    }

    fn flush(&mut self) -> Result<(), crate::reactor::SinkError> {
        self.inner.flush()
    }
}

impl<S, D, E> ExactlyOnceSink for IdempotentSink<S, D, E>
where
    S: Sink,
    D: DeduplicationStore,
    E: RecordIdExtractor,
{
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::new().with_idempotent_writes()
    }

    fn state(&self) -> SinkState {
        self.tx_state.state()
    }

    fn begin_transaction(&mut self) -> Result<TransactionId, SinkError> {
        // Idempotent sinks simulate transactions for API compatibility
        self.tx_state.begin_new()
    }

    fn write(&mut self, _tx_id: &TransactionId, outputs: Vec<Output>) -> Result<(), SinkError> {
        // For idempotent sinks, we write immediately (deduplication handles safety)
        self.write_deduplicated(outputs)
    }

    fn commit(&mut self, tx_id: &TransactionId) -> Result<(), SinkError> {
        // Commit is essentially a no-op for idempotent sinks
        // The deduplication store ensures no duplicates on recovery
        self.tx_state.commit(tx_id)?;
        self.inner
            .flush()
            .map_err(|e| SinkError::FlushFailed(e.to_string()))?;
        Ok(())
    }

    fn rollback(&mut self, tx_id: &TransactionId) -> Result<(), SinkError> {
        // For idempotent sinks, rollback doesn't undo writes
        // (they're already deduplicated, so retry is safe)
        self.tx_state.rollback(tx_id)
    }

    fn checkpoint(&self) -> SinkCheckpoint {
        let mut checkpoint = SinkCheckpoint::new(&self.sink_id);

        // Store dedup state in checkpoint metadata
        let dedup_bytes = self.dedup.to_bytes();
        checkpoint.set_metadata("dedup_store", dedup_bytes);

        // Store stats
        checkpoint.set_metadata("total_written", self.total_written.to_le_bytes().to_vec());

        checkpoint
    }

    fn restore(&mut self, checkpoint: &SinkCheckpoint) -> Result<(), SinkError> {
        // Restore dedup state
        if let Some(dedup_bytes) = checkpoint.get_metadata("dedup_store") {
            self.dedup
                .restore(dedup_bytes)
                .map_err(SinkError::CheckpointError)?;
        }

        // Restore stats
        if let Some(stats_bytes) = checkpoint.get_metadata("total_written") {
            if stats_bytes.len() >= 8 {
                self.total_written = u64::from_le_bytes(stats_bytes[0..8].try_into().unwrap());
            }
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), SinkError> {
        self.inner
            .flush()
            .map_err(|e| SinkError::FlushFailed(e.to_string()))
    }

    fn close(&mut self) -> Result<(), SinkError> {
        ExactlyOnceSink::flush(self)
    }
}

/// Statistics for idempotent sink
#[derive(Debug, Clone, Copy)]
pub struct IdempotentSinkStats {
    /// Total outputs received
    pub total_received: u64,
    /// Total outputs deduplicated (dropped)
    pub total_deduplicated: u64,
    /// Total batches written
    pub total_written: u64,
    /// Current dedup store size
    pub dedup_store_size: usize,
}

impl IdempotentSinkStats {
    /// Calculate the deduplication rate (0.0 to 1.0)
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn dedup_rate(&self) -> f64 {
        if self.total_received == 0 {
            0.0
        } else {
            self.total_deduplicated as f64 / self.total_received as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::Event;
    use crate::reactor::{BufferingSink, Sink as ReactorSink};
    use crate::sink::InMemoryDedup;
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;

    fn make_event(timestamp: i64, value: i64) -> Event {
        let array = Arc::new(Int64Array::from(vec![value]));
        let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
        Event::new(timestamp, batch)
    }

    #[test]
    fn test_idempotent_sink_basic() {
        let inner = BufferingSink::new();
        let dedup = InMemoryDedup::new(1000);
        let mut sink = IdempotentSink::new(inner, dedup);

        let event = make_event(1000, 42);
        ReactorSink::write(&mut sink, &mut vec![Output::Event(event)]).unwrap();

        let stats = sink.stats();
        assert_eq!(stats.total_received, 1);
        assert_eq!(stats.total_deduplicated, 0);
    }

    #[test]
    fn test_idempotent_sink_deduplication() {
        let inner = BufferingSink::new();
        let dedup = InMemoryDedup::new(1000);
        let mut sink = IdempotentSink::new(inner, dedup);

        // First write
        let event = make_event(1000, 42);
        ReactorSink::write(&mut sink, &mut vec![Output::Event(event.clone())]).unwrap();

        // Same event again (will be deduplicated)
        ReactorSink::write(&mut sink, &mut vec![Output::Event(event)]).unwrap();

        let stats = sink.stats();
        assert_eq!(stats.total_received, 2);
        assert_eq!(stats.total_deduplicated, 1);
    }

    #[test]
    fn test_idempotent_sink_checkpoint_restore() {
        let inner = BufferingSink::new();
        let dedup = InMemoryDedup::new(1000);
        let mut sink = IdempotentSink::new(inner, dedup);

        // Write some data
        let event = make_event(1000, 42);
        ReactorSink::write(&mut sink, &mut vec![Output::Event(event)]).unwrap();

        // Checkpoint
        let checkpoint = ExactlyOnceSink::checkpoint(&sink);

        // Create new sink and restore
        let inner2 = BufferingSink::new();
        let dedup2 = InMemoryDedup::new(1);
        let mut sink2 = IdempotentSink::new(inner2, dedup2);
        sink2.restore(&checkpoint).unwrap();

        // The same event should be deduplicated
        let event = make_event(1000, 42);
        ReactorSink::write(&mut sink2, &mut vec![Output::Event(event)]).unwrap();

        let stats = sink2.stats();
        assert_eq!(stats.total_deduplicated, 1);
    }

    #[test]
    fn test_idempotent_sink_exactly_once_api() {
        let inner = BufferingSink::new();
        let dedup = InMemoryDedup::new(1000);
        let mut sink = IdempotentSink::new(inner, dedup);

        // Use the ExactlyOnceSink API
        let tx_id = sink.begin_transaction().unwrap();

        let event = make_event(1000, 42);
        ExactlyOnceSink::write(&mut sink, &tx_id, vec![Output::Event(event)]).unwrap();

        sink.commit(&tx_id).unwrap();

        assert_eq!(sink.state(), SinkState::Idle);
    }

    #[test]
    fn test_column_id_extractor() {
        let extractor = ColumnIdExtractor::single("id");

        let array = Arc::new(Int64Array::from(vec![123]));
        let batch = RecordBatch::try_from_iter(vec![("id", array as _)]).unwrap();
        let event = Event::new(1000, batch);

        let ids = extractor.extract_ids(&[Output::Event(event)]);
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn test_stats_dedup_rate() {
        let stats = IdempotentSinkStats {
            total_received: 100,
            total_deduplicated: 25,
            total_written: 75,
            dedup_store_size: 75,
        };

        assert!((stats.dedup_rate() - 0.25).abs() < 0.001);
    }
}
