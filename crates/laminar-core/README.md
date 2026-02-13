# laminar-core

Core streaming engine for LaminarDB -- reactor, operators, state stores, and streaming infrastructure.

## Overview

This is the Ring 0 crate containing all latency-critical components. Everything here is designed for sub-microsecond execution with zero heap allocations on the hot path.

## Key Modules

| Module | Purpose |
|--------|---------|
| `reactor` | Single-threaded event loop, CPU-pinned |
| `operator` | Windows (tumbling, sliding, hopping, session), joins (stream, ASOF, temporal, lookup), changelog, lag/lead, ranking |
| `state` | `StateStore` trait, `InMemoryStateStore` (FxHashMap), `ChangelogAwareStore` wrapper |
| `time` | Event time processing, watermarks (partitioned, keyed, alignment groups) |
| `streaming` | Ring buffer, SPSC/MPSC channels, source/sink abstractions, checkpoint manager |
| `dag` | DAG pipeline topology, multicast routing, executor, checkpointing |
| `subscription` | Reactive push-based subscriptions: events, notifications, registry, dispatcher |
| `tpc` | Thread-per-core runtime: SPSC queues, key router, core handles, backpressure |
| `sink` | Exactly-once transactional sink, epoch adapter |
| `mv` | Cascading materialized views |
| `compiler` | JIT compiler: EventRow, Cranelift expressions, pipeline extraction/compilation, cache |
| `alloc` | Zero-allocation enforcement (bump/arena allocators) |
| `numa` | NUMA-aware memory allocation |
| `io_uring` | io_uring integration, three-ring I/O architecture |
| `budget` | Task budget enforcement |

## Feature Flags

| Flag | Purpose |
|------|---------|
| `jit` | Cranelift JIT compilation for Ring 0 query execution |
| `allocation-tracking` | Panic on heap allocation in marked sections |
| `io-uring` | Linux 5.10+ io_uring integration |
| `hwloc` | Enhanced NUMA topology discovery |
| `xdp` | Linux eBPF/XDP network optimization |

## Ring Placement

This crate operates in **Ring 0 (Hot Path)**. All code must:
- Make zero heap allocations
- Use no locks (SPSC queues for communication)
- Avoid system calls on the fast path
- Stay within task budget limits

## Benchmarks

```bash
cargo bench -p laminar-core --bench state_bench       # State lookup latency
cargo bench -p laminar-core --bench throughput_bench   # Throughput per core
cargo bench -p laminar-core --bench latency_bench      # p99 latency
cargo bench -p laminar-core --bench window_bench       # Window operations
cargo bench -p laminar-core --bench dag_bench          # DAG pipeline
cargo bench -p laminar-core --bench join_bench         # Join throughput
```
