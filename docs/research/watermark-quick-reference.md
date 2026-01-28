# LaminarDB Watermark Implementation - Quick Reference

> Use this file to quickly provide context in Claude Code sessions.
> Full research: `watermark-generator-research-2026.md`

## Current Status: Phase 1 Complete, Phase 2 Specs Ready

## Implementation Status

| Research Phase | Feature | Status | Spec |
|----------------|---------|--------|------|
| **Phase 1: Foundation** | Bounded Out-of-Orderness | ✅ Done | F010 |
| | Ascending Timestamps | ✅ Done | F010 |
| | Periodic Emission | ✅ Done | F010 |
| | Punctuated Watermarks | ✅ Done | F010 |
| | Source-Provided | ✅ Done | F010 |
| | Multi-Source Alignment | ✅ Done | F010 |
| | Idle Detection | ✅ Done | F010 |
| | Metrics Collection | ✅ Done | F010 |
| **Phase 2: Per-Partition** | Per-partition tracking | 📝 Draft | F064 |
| | Thread-per-core integration | 📝 Draft | F064 |
| **Phase 3: Keyed Watermarks** | Per-key tracking | 📝 Draft | F065 |
| | Memory-efficient storage | 📝 Draft | F065 |
| **Phase 4: EMIT ON WINDOW CLOSE** | OnWindowClose strategy | 📝 Draft | F011B |
| | Changelog emission | 📝 Draft | F011B |
| **Phase 5: Alignment Groups** | Multi-source coordination | 📝 Draft | F066 |
| | Bounded drift enforcement | 📝 Draft | F066 |
| **Phase 6: Adaptive** | ML-based prediction | Not planned | Future |

## Target Constraints
- **Hot path latency:** <500ns (watermarks in Ring 1)
- **Zero allocations** in Ring 0 event processing
- **Lock-free** watermark observation from hot path

## Core Implementation (F010 - Done)

```rust
// Available generators
use laminar_core::time::{
    BoundedOutOfOrdernessGenerator,  // Default, allows lateness
    AscendingTimestampsGenerator,     // Strictly ordered
    PeriodicGenerator,                // Wall-clock emission
    PunctuatedGenerator,              // Event-based emission
    SourceProvidedGenerator,          // External watermarks
    WatermarkTracker,                 // Multi-source alignment
    MeteredGenerator,                 // Metrics wrapper
};

// Example usage
let mut gen = BoundedOutOfOrdernessGenerator::new(1000); // 1s lateness
let wm = gen.on_event(5000); // Returns Some(Watermark(4000))

// Multi-source tracking
let mut tracker = WatermarkTracker::new(2);
tracker.update_source(0, 5000);
tracker.update_source(1, 3000);
assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));

// Idle detection
tracker.mark_idle(1);
assert_eq!(tracker.current_watermark(), Some(Watermark::new(5000)));
```

## Accuracy Comparison

| Watermark Type | Accuracy | Data Loss | Feature |
|----------------|----------|-----------|---------|
| Global (current) | 63-67% | 33-37% | F010 |
| **Per-Partition** | Better | Lower | F064 |
| **Keyed** | **99%+** | **<1%** | F065 |

## Phase 2 Features (Specs Ready)

### F064: Per-Partition Watermarks (P1)
- Track watermarks per Kafka partition
- Required for Kafka source integration
- Thread-per-core integration

### F065: Keyed Watermarks (P1)
- Per-key watermark tracking
- 99%+ accuracy vs 63-67% global
- Memory-efficient for high cardinality

### F066: Watermark Alignment Groups (P2)
- Multi-source coordination with bounded drift
- Pause fast sources to prevent state growth
- Required for stream-stream joins

## Ring Integration Pattern

```
Ring 0 (Hot Path)           Ring 1 (Background)
─────────────────           ───────────────────
observe_event_time() ──────▶ WatermarkGenerator
  (atomic, lock-free)         - compute watermark
                              - check idle
                              - emit to operators
```

## Key Formulas

```
watermark = max_event_time_seen - bounded_delay
global_watermark = min(partition_watermarks) // exclude idle
keyed_watermark = min(key_watermarks[k] for k in active_keys)
```

## SQL Syntax

```sql
CREATE SOURCE orders (
    order_id BIGINT,
    order_time TIMESTAMP,
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (...);
```

## Key Files

| File | Purpose |
|------|---------|
| `crates/laminar-core/src/time/watermark.rs` | Core implementation (995 lines) |
| `crates/laminar-core/src/time/mod.rs` | Module exports, Watermark type |
| `crates/laminar-core/src/operator/window.rs` | Window integration |
| `docs/features/phase-1/F010-watermarks.md` | Phase 1 spec |
| `docs/features/phase-2/F064-per-partition-watermarks.md` | Partition spec |
| `docs/features/phase-2/F065-keyed-watermarks.md` | Keyed spec |
| `docs/features/phase-2/F066-watermark-alignment-groups.md` | Alignment spec |

## Research Highlights

1. **Keyed watermarks** achieve 99%+ accuracy vs 63-67% with global (Phase 3)
2. **Idle detection** prevents pipeline stalls - critical for Kafka sources
3. **EMIT ON WINDOW CLOSE** reduces write amplification
4. **Watermark alignment** prevents unbounded state growth in joins
5. Keep watermark gen **off hot path** for <500ns latency

---
*Updated: 2026-01-24 | Phase 1 Complete, Phase 2 Specs Ready*
