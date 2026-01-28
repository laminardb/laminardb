# F023: Exactly-Once Sinks

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F023 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F008, F011B, F063 |
| **Owner** | TBD |
| **Research** | [Emit Patterns Research 2026](../../research/emit-patterns-research-2026.md) |

## Summary

Ensure exactly-once delivery semantics to sinks by tracking committed offsets atomically with output. Prevents duplicates on recovery.

## Critical Dependencies (from Research)

From the 2026 emit patterns research, two features are **required** for exactly-once:

1. **F011B (EMIT ON WINDOW CLOSE)**: For append-only sinks (Kafka, S3), must only emit when window closes to prevent duplicates. Without this, late data causes retraction emissions that cannot be handled by append-only sinks.

2. **F063 (Changelog/Retraction)**: For upsert-capable sinks, must track and emit retractions when late data arrives. Without this, late data corrections are lost.

## Goals

- Atomic offset + data commit
- Idempotent writes
- Transaction support where available
- Deduplication fallback

## Technical Design

```rust
pub trait ExactlyOnceSink {
    async fn begin_transaction(&mut self) -> Result<TxId>;
    async fn write(&mut self, tx: TxId, batch: RecordBatch) -> Result<()>;
    async fn commit(&mut self, tx: TxId, offsets: &Offsets) -> Result<()>;
    async fn rollback(&mut self, tx: TxId) -> Result<()>;
}

pub struct IdempotentSink {
    inner: Box<dyn Sink>,
    dedup_store: DeduplicationStore,
}

impl IdempotentSink {
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let ids: Vec<_> = batch.column("_id").collect();
        let new_ids = self.dedup_store.filter_unseen(&ids);
        
        if !new_ids.is_empty() {
            self.inner.write(batch.filter(&new_ids)).await?;
            self.dedup_store.mark_seen(&new_ids);
        }
        Ok(())
    }
}
```

## Completion Checklist

- [x] Transactional sinks working — `TransactionalSink<S>` buffers writes, flushes on commit
- [x] Idempotent fallback implemented — `IdempotentSink` with deduplication (pre-existing)
- [x] Recovery tested (no duplicates) — 5 integration tests in `sink/mod.rs`
- [x] Multiple sink types supported — TransactionalSink + IdempotentSink via `ExactlyOnceSinkAdapter`
