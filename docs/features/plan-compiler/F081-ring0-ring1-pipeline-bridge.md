# F081: Ring 0 / Ring 1 Pipeline Bridge

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F081 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 2.5 (Plan Compiler) |
| **Effort** | M (3-5 days) |
| **Dependencies** | F080, F014 (SPSC Queues) |
| **Owner** | TBD |

## Summary

The mechanism connecting compiled Ring 0 event-at-a-time processing with Ring 1 stateful operators (windows, joins, aggregations). Compiled pipelines in Ring 0 perform filtering, projection, and key extraction at < 100ns per event; surviving events cross the bridge into Ring 1 where batch-oriented stateful operators process them. This replaces the implicit batch-boundary-driven handoff with an explicit, watermark-aware handoff that fixes issues like partial session window emissions (Issue #55).

## Motivation

Today, LaminarDB's Ring 0 reactor processes events through operators sequentially — filters, windows, and joins are all in the same `poll()` loop. This means:

1. **Window operators see raw events** — they must inline filter and projection logic
2. **Batch boundaries leak** — when a RecordBatch ends mid-window, session windows emit partial results
3. **No separation of concerns** — stateless (fast) and stateful (slower) operations share the same event loop budget

The Pipeline Bridge creates an explicit boundary:

```
Ring 0 (compiled, < 100ns/event):
  Event → Compiled Filter+Project+KeyExtract → Bridge

Ring 1 (stateful, 1-100µs/event):
  Bridge → Window/Join/Aggregation → Output
```

Events that don't pass the filter never reach Ring 1. Events that do are pre-projected (only needed columns survive) and pre-keyed (partition key already computed). Ring 1 operators receive clean, pre-processed events — no redundant work.

### Fixing Issue #55 (Session Window Partial Results)

With the bridge, session window state is managed entirely in Ring 1:
- Events arrive in the bridge's handoff buffer, not in RecordBatch boundaries
- Ring 1 flushes on watermark advance or count threshold — both are query-level decisions, not transport-level artifacts
- Session merging happens in Ring 1 with full visibility into watermark state
- No more partial emissions at arbitrary batch boundaries

## Goals

1. `PipelineBridge` — lock-free SPSC handoff from Ring 0 to Ring 1
2. Configurable batch formation: flush on count threshold OR time/watermark threshold
3. Backpressure: Ring 0 detects when Ring 1 is behind and applies backpressure
4. Watermark forwarding: Ring 0 watermarks propagate through the bridge
5. Zero-allocation in Ring 0 side (arena-backed event rows)

## Non-Goals

- Replacing the existing SPSC queue implementation (reuse `tpc::SpscQueue`)
- Ring 1 operator redesign (they continue using existing `Operator` trait)
- Cross-core bridge (this is single-core Ring 0 → Ring 1)

## Technical Design

### Bridge Architecture

```
┌──────────────────────────────────────────────────────────┐
│                         Ring 0                            │
│                                                          │
│  Event bytes ──► CompiledPipeline ──► PipelineBridge     │
│                     (filter+project)    │                │
│                                         │ SPSC Queue     │
│                                         ▼                │
├──────────────────────────────────────────────────────────┤
│                         Ring 1                            │
│                                                          │
│  BridgeConsumer ──► RowBatchBridge ──► Ring 1 Operator   │
│    (batch formation)   (EventRow→RecordBatch)            │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### BridgeMessage

```rust
/// Messages sent from Ring 0 to Ring 1 through the pipeline bridge.
///
/// Carried over the SPSC queue. Must be small and Copy-friendly.
#[derive(Debug)]
pub enum BridgeMessage {
    /// A pre-processed event row (filter passed, projection applied).
    Event {
        /// Pointer to the event row bytes in the arena.
        /// Valid until the arena is reset (end of current poll cycle).
        row_data: SmallVec<[u8; 128]>,
        /// Event timestamp (microseconds since epoch).
        event_time: i64,
        /// Pre-computed partition key hash.
        key_hash: u64,
    },
    /// Watermark advance notification.
    Watermark {
        /// New watermark value (microseconds since epoch).
        timestamp: i64,
    },
    /// Checkpoint barrier — Ring 1 should snapshot state.
    CheckpointBarrier {
        /// Epoch number.
        epoch: u64,
    },
    /// End of stream.
    Eof,
}
```

### PipelineBridge (Ring 0 Producer)

```rust
/// Ring 0 side of the bridge. Accepts compiled pipeline output
/// and sends it to Ring 1 via SPSC queue.
pub struct PipelineBridge {
    /// SPSC queue to Ring 1. Capacity is power-of-2 for fast modulo.
    queue: SpscProducer<BridgeMessage>,
    /// Arena for temporary event row allocations.
    /// Reset at the end of each poll cycle after all messages are queued.
    arena: bumpalo::Bump,
    /// Output row schema from the compiled pipeline.
    output_schema: Arc<RowSchema>,
    /// Statistics.
    events_sent: u64,
    events_dropped_backpressure: u64,
}

impl PipelineBridge {
    /// Send a compiled pipeline result to Ring 1.
    ///
    /// Called from Ring 0 after CompiledPipeline::execute returns Emit.
    /// Zero-allocation: the row data is copied into a SmallVec (inline for < 128 bytes).
    #[inline(always)]
    pub fn send_event(
        &mut self,
        row: &EventRow<'_>,
        event_time: i64,
        key_hash: u64,
    ) -> Result<(), BridgeError> {
        let row_bytes = row.as_bytes();
        let msg = BridgeMessage::Event {
            row_data: SmallVec::from_slice(row_bytes),
            event_time,
            key_hash,
        };

        match self.queue.try_push(msg) {
            Ok(()) => {
                self.events_sent += 1;
                Ok(())
            }
            Err(_) => {
                self.events_dropped_backpressure += 1;
                Err(BridgeError::Backpressure)
            }
        }
    }

    /// Forward a watermark to Ring 1.
    #[inline(always)]
    pub fn send_watermark(&mut self, timestamp: i64) -> Result<(), BridgeError> {
        self.queue.try_push(BridgeMessage::Watermark { timestamp })
            .map_err(|_| BridgeError::Backpressure)
    }

    /// Send a checkpoint barrier to Ring 1.
    pub fn send_checkpoint(&mut self, epoch: u64) -> Result<(), BridgeError> {
        self.queue.try_push(BridgeMessage::CheckpointBarrier { epoch })
            .map_err(|_| BridgeError::Backpressure)
    }

    /// Check if Ring 1 has capacity for more events.
    #[inline(always)]
    pub fn has_capacity(&self) -> bool {
        !self.queue.is_full()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("Ring 1 backpressure: queue full")]
    Backpressure,
}
```

### BridgeConsumer (Ring 1 Side)

```rust
/// Ring 1 side of the bridge. Drains events from the SPSC queue,
/// forms batches, and feeds them to Ring 1 operators.
pub struct BridgeConsumer {
    /// SPSC queue from Ring 0.
    queue: SpscConsumer<BridgeMessage>,
    /// Accumulates event rows into Arrow RecordBatch.
    batch_bridge: RowBatchBridge,
    /// Batch formation policy.
    policy: BatchPolicy,
    /// Last watermark received.
    current_watermark: i64,
    /// Pending checkpoint epoch (if barrier received).
    pending_checkpoint: Option<u64>,
}

/// Controls when the batch is flushed to Ring 1 operators.
#[derive(Debug, Clone)]
pub struct BatchPolicy {
    /// Flush after this many rows (default: 1024).
    pub max_rows: usize,
    /// Flush after this duration since first row in batch (default: 10ms).
    pub max_latency: std::time::Duration,
    /// Flush immediately on watermark advance (for low-latency windows).
    pub flush_on_watermark: bool,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            max_rows: 1024,
            max_latency: std::time::Duration::from_millis(10),
            flush_on_watermark: true,
        }
    }
}

impl BridgeConsumer {
    /// Drain available messages from Ring 0 and form batches.
    ///
    /// Returns a list of actions for Ring 1 to process.
    pub fn drain(&mut self) -> Vec<Ring1Action> {
        let mut actions = Vec::new();

        self.queue.pop_each(self.policy.max_rows * 2, |msg| {
            match msg {
                BridgeMessage::Event { row_data, event_time, key_hash } => {
                    let row = EventRow::from_bytes(&row_data, &self.batch_bridge.row_schema());
                    if let Some(batch) = self.batch_bridge.append(&row) {
                        actions.push(Ring1Action::ProcessBatch(batch));
                    }
                }

                BridgeMessage::Watermark { timestamp } => {
                    self.current_watermark = timestamp;
                    if self.policy.flush_on_watermark {
                        // Flush any pending batch before advancing watermark
                        if self.batch_bridge.row_count() > 0 {
                            let batch = self.batch_bridge.flush();
                            actions.push(Ring1Action::ProcessBatch(batch));
                        }
                    }
                    actions.push(Ring1Action::AdvanceWatermark(timestamp));
                }

                BridgeMessage::CheckpointBarrier { epoch } => {
                    // Flush pending batch, then checkpoint
                    if self.batch_bridge.row_count() > 0 {
                        let batch = self.batch_bridge.flush();
                        actions.push(Ring1Action::ProcessBatch(batch));
                    }
                    actions.push(Ring1Action::Checkpoint(epoch));
                }

                BridgeMessage::Eof => {
                    if self.batch_bridge.row_count() > 0 {
                        let batch = self.batch_bridge.flush();
                        actions.push(Ring1Action::ProcessBatch(batch));
                    }
                    actions.push(Ring1Action::Eof);
                }
            }
            true // continue draining
        });

        actions
    }
}

/// Actions for Ring 1 operators to process.
pub enum Ring1Action {
    /// Process a batch of pre-filtered, pre-projected events.
    ProcessBatch(RecordBatch),
    /// Advance the watermark — may trigger window emissions.
    AdvanceWatermark(i64),
    /// Take a checkpoint at the given epoch.
    Checkpoint(u64),
    /// End of stream.
    Eof,
}
```

### Backpressure Strategy

```rust
/// Backpressure handling between Ring 0 and Ring 1.
///
/// Ring 0 must never block. When Ring 1 is slow, Ring 0 has three options:
pub enum BackpressureStrategy {
    /// Drop events when queue is full. Record dropped count in metrics.
    /// Use for best-effort analytics where losing events is acceptable.
    DropNewest,

    /// Slow down the source. Ring 0 signals the source to pause polling.
    /// Use for exactly-once pipelines where event loss is unacceptable.
    PauseSource,

    /// Spill to disk. Ring 0 writes overflow events to a WAL segment.
    /// Ring 1 reads from WAL when it catches up.
    /// Use for burst-tolerant pipelines.
    SpillToDisk {
        /// Maximum bytes to spill before switching to DropNewest.
        max_spill_bytes: usize,
    },
}
```

### Integration with Existing Reactor

```rust
/// Modified Reactor::poll() incorporating the pipeline bridge.
///
/// The reactor now has two modes:
/// 1. Legacy mode: operators process events directly (existing behavior)
/// 2. Compiled mode: compiled pipeline + bridge + Ring 1 operators
impl Reactor {
    /// Poll with compiled pipeline support.
    pub fn poll_compiled(&mut self) -> Vec<Output> {
        // Step 1: Fire expired timers (unchanged)
        self.fire_timers();

        // Step 2: Process events through compiled pipeline
        while let Some(event) = self.event_queue.pop_front() {
            // Deserialize event into EventRow
            let row = self.deserialize_to_row(&event);

            // Execute compiled pipeline
            let action = unsafe {
                self.compiled_pipeline.execute(
                    row.as_ptr(),
                    self.output_row_buf.as_mut_ptr(),
                )
            };

            match action {
                PipelineAction::Emit => {
                    let output_row = EventRow::from_bytes(
                        &self.output_row_buf,
                        &self.compiled_pipeline.output_schema,
                    );
                    // Send to Ring 1 via bridge
                    let _ = self.bridge.send_event(
                        &output_row,
                        event.timestamp,
                        self.compute_key_hash(&output_row),
                    );
                }
                PipelineAction::Drop => {
                    // Event filtered out — no Ring 1 work needed
                }
                PipelineAction::Error => {
                    // Log and continue
                    tracing::warn!("Compiled pipeline error for event");
                }
            }
        }

        // Step 3: Forward watermark
        if self.watermark_advanced {
            let _ = self.bridge.send_watermark(self.current_watermark());
        }

        // Step 4: Collect Ring 1 outputs (from previous cycle)
        std::mem::take(&mut self.output_buffer)
    }
}
```

## Performance Targets

| Metric | Target |
|--------|--------|
| Bridge send (Ring 0 side) | < 30ns per event |
| Bridge drain (Ring 1 side) | < 1µs per batch of 1024 |
| Backpressure detection | < 5ns (single atomic read) |
| Watermark forwarding | < 50ns |
| End-to-end latency (Ring 0 → Ring 1 batch) | < 100µs at default batch policy |

## Success Criteria

- [ ] Events flow from compiled pipeline through bridge to Ring 1 operators correctly
- [ ] Watermarks propagate through the bridge and trigger window emissions
- [ ] Checkpoint barriers propagate and Ring 1 snapshots state correctly
- [ ] Backpressure prevents Ring 0 from overflowing the queue
- [ ] Session windows no longer emit partial results at batch boundaries (Issue #55)
- [ ] Zero allocations on the Ring 0 (producer) side of the bridge
- [ ] Batch formation policy correctly flushes on count, time, and watermark

## Testing

| Module | Tests | What |
|--------|-------|------|
| `PipelineBridge` | 8 | Send event, send watermark, send checkpoint, backpressure, capacity check |
| `BridgeConsumer` | 10 | Drain events, batch formation, watermark flush, checkpoint flush, EOF |
| `BatchPolicy` | 6 | Count threshold, time threshold, watermark trigger, custom policy |
| `Backpressure` | 4 | Queue full → drop, queue full → pause source, spill to disk, recovery |
| Integration | 6 | End-to-end: compiled pipeline → bridge → window operator → output |
| Issue #55 regression | 2 | Session window: no partial emissions, correct watermark-driven output |

## Files

- `crates/laminar-core/src/compiler/bridge.rs` — MODIFY: Add `PipelineBridge`, `BridgeConsumer`, `BridgeMessage`
- `crates/laminar-core/src/compiler/policy.rs` — NEW: `BatchPolicy`, `BackpressureStrategy`
- `crates/laminar-core/src/reactor/mod.rs` — MODIFY: Add `poll_compiled()` method
- `crates/laminar-core/src/compiler/mod.rs` — MODIFY: Re-export bridge types
