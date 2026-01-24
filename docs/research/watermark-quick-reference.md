# LaminarDB Watermark Implementation - Quick Reference

> Use this file to quickly provide context in Claude Code sessions.  
> Full research: `watermark-generator-research-2026.md`

## Current Phase: 1 (Bounded Out-of-Orderness)

## Target Constraints
- **Hot path latency:** <500ns (watermarks must be in Ring 1)
- **Zero allocations** in Ring 0 event processing
- **Lock-free** watermark observation from hot path

## Phase 1 Scope
- [x] Bounded out-of-orderness strategy
- [x] Single global watermark per source
- [x] Basic idle source detection
- [x] SQL: `WATERMARK FOR <col> AS <col> - INTERVAL 'N' SECOND`

## Future Phase Requirements (Design For)
| Phase | Feature | Key Requirement |
|-------|---------|-----------------|
| 2 | Per-partition tracking | `HashMap<PartitionId, Watermark>` |
| 3 | Keyed watermarks | Per-key tracking, 99%+ accuracy |
| 4 | EMIT ON WINDOW CLOSE | Append-only output mode |
| 5 | Multi-source alignment | Pause fast sources |
| 6 | Adaptive watermarks | ML-based delay tuning |

## Core Trait (Extensible Design)

```rust
pub trait WatermarkGenerator: Send + Sync {
    /// Update with new event - called from Ring 1
    fn on_event(&mut self, event_time: Timestamp);
    
    /// Current watermark value
    fn current_watermark(&self) -> Option<Timestamp>;
    
    /// Check idle status
    fn check_idle(&mut self) -> bool;
    
    /// Periodic emission hook (Ring 1)
    fn on_periodic_emit(&mut self) -> Option<Timestamp>;
}
```

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

## SQL Syntax Target

```sql
CREATE SOURCE orders (
    order_id BIGINT,
    order_time TIMESTAMP,
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (...);
```

## Common Prompts for Claude Code

### Design the trait:
```
@watermark-generator-research-2026.md

Design the WatermarkGenerator trait for LaminarDB Phase 1. 
Must be extensible for keyed watermarks (Phase 3) and 
integrate with Ring 0/1 architecture. Zero allocations in hot path.
```

### Implement bounded OOO:
```
@watermark-quick-reference.md

Implement BoundedOutOfOrdernessWatermark struct:
- Track max_event_time
- Configurable bounded_delay  
- Idle detection with timeout
- Lock-free observation from Ring 0
```

### Add SQL parsing:
```
@watermark-generator-research-2026.md (Section 10)

Add WATERMARK FOR clause to SQL parser.
Syntax: WATERMARK FOR <col> AS <col> - INTERVAL 'N' <unit>
Units: SECOND, MINUTE, HOUR, DAY
```

### Test idle detection:
```
@watermark-quick-reference.md

Write tests for idle source detection:
1. Source goes idle after timeout
2. Idle source excluded from global watermark
3. Source reactivates on new event
4. Multiple partitions with mixed idle state
```

## Research Highlights

1. **Keyed watermarks** achieve 99%+ accuracy vs 63-67% with global (Phase 3)
2. **Idle detection** prevents pipeline stalls - critical for Kafka sources
3. **EMIT ON WINDOW CLOSE** reduces write amplification 
4. **Watermark alignment** prevents unbounded state growth in joins
5. Keep watermark gen **off hot path** for <500ns latency

## Files to Reference

| File | Use For |
|------|---------|
| `watermark-generator-research-2026.md` | Full research, detailed patterns |
| `watermark-quick-reference.md` | Quick context in prompts |

---
*Updated: 2026-01-24 | Phase: 1*
