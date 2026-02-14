# Anti-Pattern Review & Fix Plan

> **Date**: 2026-02-14
> **Branch**: feature/anti_patterns
> **Total Issues Found**: 71 (14 CRITICAL, 17 HIGH, 28 MEDIUM, 12 LOW/TRIVIAL)

## Implementation Plan (Priority Order)

### Week 1 — CRITICAL Path

| # | Fix | File(s) | Impact | Status |
|---|-----|---------|--------|--------|
| 1 | Eliminate unnecessary config cloning in execute_cycle | `stream_executor.rs:317-324` | 5-10us/cycle | ✅ Done |
| 2 | Remove topo_order clone in loop | `stream_executor.rs:317` | <1us/cycle | ✅ Done |
| 3 | Cache timestamp format inference | `stream_executor.rs:765-777` | O(n_cols) per batch | ✅ Done |
| 4 | Single-pass EOWC filtering (eliminate double filter) | `stream_executor.rs:522-557, 614-633` | 25-50us/cycle | ✅ Done |
| 5 | SIMD batch filtering via Arrow compute | `batch_filter.rs:49-156` | 10x faster filtering | ✅ Done |
| 6 | Add EOWC memory bounds + late data rejection | `stream_executor.rs:121-133, 480-491` | Prevents OOM + EOWC violation | ✅ Done |
| 7 | Add backpressure return from execute_cycle | `stream_executor.rs:300-379` | Flow control | ✅ Done |
| 8 | Checkpoint store fsync ordering | `checkpoint_store.rs:179-204` | Durability correctness | ✅ Done |
| 9 | Changelog drain → WAL write in prepare_wal | `checkpoint_coordinator.rs:288-323` | Exactly-once correctness | ✅ Done |
| 10 | Pre-allocate reactor completion Vecs | `three_ring/reactor.rs:408-512` | Eliminate 3 allocs/poll | ✅ Done |

### Week 2 — HIGH Path

| # | Fix | File(s) | Impact | Status |
|---|-----|---------|--------|--------|
| 11 | Manifest/sink commit atomicity (per-sink tracking) | `checkpoint_coordinator.rs:436-477, 531-554` | Exactly-once | |
| 12 | WAL synced position tracking | `per_core_wal/writer.rs:75-105` | Data loss prevention | |
| 13 | Recovery manifest validation | `recovery_manager.rs:120-141` | State corruption prevention | |
| 14 | Session window LRU cache eviction | `session_window.rs:288-534` | Memory leak prevention | |
| 15 | Add timeout to two-phase commit pre-commit | `checkpoint_coordinator.rs:517-528` | Availability | |
| 16 | Changelog drainer max_pending bounds | `changelog_drainer.rs:21-68` | Memory bounds | |
| 17 | Feature-gate DAG executor metrics | `dag/executor.rs:509-576` | 5-10ns/event | |
| 18 | Separate PipelineCounters by ring (cache lines) | `metrics.rs:40-62` | False sharing | |

### Week 3 — Hardening & Observability

| # | Fix | File(s) | Impact | Status |
|---|-----|---------|--------|--------|
| 19 | Fix window boundary computation for session/sliding | `stream_executor.rs:740-762` | Correctness | |
| 20 | Replace reactor yield_now() with condvar | `reactor/mod.rs:473-475` | 500-1000ns saved | |
| 21 | Move Arrow IPC serialization out of operator hot path | `lookup_join.rs:268-282, asof_join.rs:272-289` | 100-500us saved | |
| 22 | Pre-allocate operator scratch buffers | `lag_lead.rs:142, lookup_join.rs, asof_join.rs` | 50-100ns/event | |
| 23 | OutputVec with_capacity hints | Multiple operators | 50-100ns saved | |
| 24 | Add watermark persistence to WAL | `checkpoint_coordinator.rs` | Watermark regression | |
| 25 | Operator checkpoint phase ordering | `checkpoint_coordinator.rs:364-497` | Event loss prevention | |

### Deferred (Architectural)

| # | Fix | Notes |
|---|-----|-------|
| 26 | Query plan caching (DataFusion LogicalPlan) | Requires DataFusion API research — high ROI but complex |
| 27 | Arc\<RecordBatch\> for MemTable registration | Requires custom TableProvider — high ROI but complex |
| 28 | EXPLAIN command + per-operator profiling | New feature, not a fix |
| 29 | Sliding window retraction handling | Design decision needed |
| 30 | Ring 0 type-level boundary enforcement | Long-term architectural |

---

## Detailed Issue Catalog

### Category 1: EMIT ON WINDOW CLOSE & Window Aggregation (14 issues)

| Severity | Issue | File | Lines |
|----------|-------|------|-------|
| CRITICAL | Unbounded EOWC memory accumulation | stream_executor.rs | 121-133, 480-491 |
| CRITICAL | Late data handling gap after window closes | stream_executor.rs | 480-491, 505-536 |
| HIGH | Excessive cloning on hot path (550K allocs/sec) | stream_executor.rs | 317-573 |
| HIGH | O(n) row-by-row filtering (no SIMD) | batch_filter.rs | 49-156 |
| HIGH | Session window state never evicted | session_window.rs | 288-534 |
| HIGH | No backpressure mechanism in execute_cycle | stream_executor.rs | 300-379 |
| MEDIUM | Ambiguous window boundary for session/sliding | stream_executor.rs | 740-762 |
| MEDIUM | Missing retraction for sliding windows | sliding_window.rs | 49-72 |
| MEDIUM | No max batch size enforcement | stream_executor.rs | 480-491 |
| MEDIUM | Timestamp format inferred per batch | stream_executor.rs | 765-777 |
| MEDIUM | Query metadata cloned per cycle | stream_executor.rs | 317-324 |
| LOW-MED | Unnecessary coalesce + double clone | stream_executor.rs | 538-584 |
| LOW-MED | Redundant window state serialization tags | window.rs | 3044-3063 |
| MEDIUM | SessionIndex cache cloning (double clone) | session_window.rs | 526-534 |

### Category 2: Checkpointing & Recovery (11 issues)

| Severity | Issue | File | Lines |
|----------|-------|------|-------|
| CRITICAL | Manifest persisted before sink commit completes | checkpoint_coordinator.rs | 436-477 |
| CRITICAL | Lost changelog entries between drain and WAL | checkpoint_coordinator.rs | 288-323 |
| CRITICAL | No fsync on manifest/latest.txt writes | checkpoint_store.rs | 179-204 |
| HIGH | Unbounded changelog drainer memory | changelog_drainer.rs | 21-68 |
| HIGH | WAL position based on buffered (not synced) data | per_core_wal/writer.rs | 75-105 |
| HIGH | No manifest consistency validation in recovery | recovery_manager.rs | 120-141 |
| HIGH | Incomplete sink commit error handling | checkpoint_coordinator.rs | 531-554 |
| MEDIUM | No timeout on two-phase commit pre-commit | checkpoint_coordinator.rs | 517-528 |
| MEDIUM | Phase ordering race between snapshot and drain | checkpoint_coordinator.rs | 364-497 |
| MEDIUM | Watermark not persisted to WAL | checkpoint_coordinator.rs | 422 |
| LOW | Epoch overflow not checked | checkpoint_coordinator.rs | 476-477 |

### Category 3: Ring 0/1/2 Architecture (27 issues)

| Severity | Issue | File | Lines |
|----------|-------|------|-------|
| CRITICAL | Vec allocations in three-ring reactor poll | reactor.rs | 408-512 |
| CRITICAL | Vec allocations in operator hot paths | lag_lead, lookup_join, asof_join | Various |
| CRITICAL | Reactor yield_now() system call on idle | reactor/mod.rs | 473-475 |
| CRITICAL | String format! in operator IDs | asof_join, lookup_join | Various |
| HIGH | Arrow IPC serialization on hot path | lookup_join.rs, asof_join.rs | 268-289 |
| HIGH | Mutex access from LaminarDB struct | db.rs | 68-95 |
| MEDIUM | MPSC lock contention (sleep backoff) | channel.rs | 135-159 |
| MEDIUM | Task budget Instant::now() overhead | task_budget.rs | 88 |
| MEDIUM | No yield points in long-running operators | All operators | - |
| MEDIUM | OutputVec without capacity hints | Multiple operators | Various |
| MEDIUM | SPSC pop_batch allocates Vec | spsc.rs | 313-323 |
| MEDIUM | TPC runtime Vec allocations | runtime.rs | 570, 697 |
| MEDIUM | Allocation tracking feature-gated off | alloc/detector.rs | 54-100 |
| LOW | Atomic counter overhead in channel stats | channel.rs | 183-215 |
| LOW | Periodic shutdown check modulo | reactor/mod.rs | 478 |

### Category 4: SQL Handling & Micro-Batching (20 issues)

| Severity | Issue | File | Lines |
|----------|-------|------|-------|
| CRITICAL | No query plan caching (DataFusion re-plans every cycle) | stream_executor.rs | 350 |
| CRITICAL | Per-batch RecordBatch cloning for MemTable | stream_executor.rs | 362, 397, 573 |
| CRITICAL | Per-batch topological sort recomputation | stream_executor.rs | 233-308 |
| CRITICAL | Planning result not cached from DDL | db.rs | 871-915 |
| HIGH | Config values cloned per batch | stream_executor.rs | 317-324 |
| HIGH | EOWC state cloning (deep clone) | stream_executor.rs | 487 |
| HIGH | EOWC double-filtering | stream_executor.rs | 522-557, 614-633 |
| HIGH | Batch filter row-by-row (no SIMD) | batch_filter.rs | 48-170 |
| MEDIUM | Table register/deregister churn per cycle | stream_executor.rs | 364-425 |
| MEDIUM | SQL re-parsing for ASOF detection | stream_executor.rs | 779-816 |
| MEDIUM | Extract table refs via full SQL parse | stream_executor.rs | 35-91 |
| MEDIUM | HashMap alloc in compute_topo_order | stream_executor.rs | 235-245 |
| MEDIUM | Compiler cache contention fallback | db.rs | 1177-1203 |
| MEDIUM | Arena allocator not cleared between batches | db.rs | 1245 |
| LOW | SessionContext recreated per-pipeline start | db.rs | 1728 |
| LOW | Pipeline hash via Debug representation | compiler/cache.rs | 102-120 |
| TRIVIAL | Topo order cloned in loop | stream_executor.rs | 317 |
| TRIVIAL | Window config clone when ref used | stream_executor.rs | 324 |
| TRIVIAL | AsofConfig clone when ref used | stream_executor.rs | 321 |

### Category 5: Profiling & Observability (9 issues)

| Severity | Issue | File | Lines |
|----------|-------|------|-------|
| HIGH | Missing EXPLAIN command | All SQL modules | - |
| HIGH | Missing per-operator statistics | operator/mod.rs | - |
| MEDIUM | Per-event metrics not feature-gated | dag/executor.rs | 509-576 |
| MEDIUM | PipelineCounters false sharing | metrics.rs | 40-62 |
| MEDIUM | Missing watermark lag tracking | time/watermark.rs | - |
| MEDIUM | Missing task budget observability | budget/ | - |
| MEDIUM | Compilation metrics shared atomic | compilation_metrics.rs | 42-80 |
| LOW | Checkpoint timing acceptable | checkpoint_coordinator.rs | 389 |

---

## Performance Impact Summary

### Current Per-Event Overhead (Target: <1us)

| Category | Current | After Week 1 | After Week 2 |
|----------|---------|-------------|-------------|
| Config cloning | 5-10ns | ~0ns | ~0ns |
| Batch filtering | 20-50ns | ~2-5ns | ~2-5ns |
| Reactor allocations | 30-50ns | ~0ns | ~0ns |
| Metrics overhead | 5-10ns | 5-10ns | ~0ns |
| **Total measurable** | **60-120ns** | **~7-15ns** | **~2-5ns** |

### Exactly-Once Risk After Fixes

| Issue | Before | After Week 1 |
|-------|--------|-------------|
| Manifest/sink race | Partial commit possible | Per-sink tracking |
| Lost changelog entries | Silent state loss | Drain→WAL atomic |
| No fsync on manifest | Crash loses checkpoint | Ordered fsync |
| WAL position unsynced | Recovery from garbage | Synced position only |
