# laminar-core

Core streaming engine for LaminarDB -- reactor, operators, state stores, and streaming infrastructure.

## Overview

This is the Ring 0 crate containing all latency-critical components. Everything here is designed for sub-microsecond execution with zero heap allocations on the hot path.

## Key Modules

| Module | Purpose |
|--------|---------|
| `reactor` | Single-threaded event loop, CPU-pinned |
| `operator` | Windows (tumbling, sliding, hopping, session), joins (stream, ASOF, temporal, lookup), changelog, lag/lead, ranking |
| `state` | `StateStore` trait, `InMemoryStateStore` (AHashMap), `ChangelogAwareStore` wrapper |
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
| `checkpoint` | Distributed checkpoint barrier protocol, barrier alignment |
| `lookup` | Lookup table trait, predicate types, foyer caches (memory + hybrid), CDC-to-cache adapter |
| `index` | redb secondary indexes for non-primary-key lookups |
| `aggregation` | Cross-partition lock-free HashMap (papaya) |
| `detect` | Platform detection utilities |
| `xdp` | Linux eBPF/XDP network optimization |
| `delta` | Distributed mode: discovery (gossip, Kafka), Raft consensus, partition ownership, gRPC RPC |

## Feature Flags

| Flag | Purpose |
|------|---------|
| `jit` | Cranelift JIT compilation for Ring 0 query execution |
| `allocation-tracking` | Panic on heap allocation in marked sections |
| `io-uring` | Linux 5.10+ io_uring integration |
| `hwloc` | Enhanced NUMA topology discovery (hwlocality) |
| `xdp` | Linux eBPF/XDP network optimization (libbpf-rs) |
| `dag-metrics` | Per-event DAG executor metrics (adds ~5-10ns/event overhead) |
| `delta` | Distributed mode (tonic, openraft, chitchat, prost) |

## Ring Placement

This crate operates in **Ring 0 (Hot Path)**. All code must:
- Make zero heap allocations
- Use no locks (SPSC queues for communication)
- Avoid system calls on the fast path
- Stay within task budget limits

## Key Abstractions

### StateStore Trait

```rust
pub trait StateStore: Send {
    fn get(&self, key: &[u8]) -> Option<&[u8]>;
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
    fn prefix_scan(&self, prefix: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])>;
}
```

### Reactor

Single-threaded event loop that pulls batches from sources, runs them through the operator DAG, and emits results. CPU-pinned via the thread-per-core runtime.

### Operator Types

- **Stateless**: map, filter, project
- **Windowed**: tumbling, sliding, hopping, session (with merge detection)
- **Join**: stream-stream (time-bounded), ASOF (point-in-time), temporal, lookup
- **Analytics**: LAG/LEAD, ROW_NUMBER/RANK/DENSE_RANK
- **Changelog**: Z-set retraction (+1 insert, -1 retract)

## Benchmarks

```bash
cargo bench -p laminar-core --bench state_bench       # State lookup latency
cargo bench -p laminar-core --bench throughput_bench   # Throughput per core
cargo bench -p laminar-core --bench latency_bench      # p99 latency
cargo bench -p laminar-core --bench window_bench       # Window operations
cargo bench -p laminar-core --bench dag_bench          # DAG pipeline
cargo bench -p laminar-core --bench join_bench         # Join throughput
cargo bench -p laminar-core --bench lookup_join_bench  # Lookup join throughput
cargo bench -p laminar-core --bench cache_bench        # foyer cache hit/miss
cargo bench -p laminar-core --bench compiler_bench     # JIT compilation
cargo bench -p laminar-core --bench streaming_bench    # Streaming channels
cargo bench -p laminar-core --bench subscription_bench # Subscription dispatch
cargo bench -p laminar-core --bench reactor_bench      # Reactor loop
cargo bench -p laminar-core --bench tpc_bench          # Thread-per-core
cargo bench -p laminar-core --bench checkpoint_bench   # Checkpoint cycle
cargo bench -p laminar-core --bench delta_checkpoint_bench  # Delta checkpoint
cargo bench -p laminar-core --bench io_uring_bench     # io_uring operations
cargo bench -p laminar-core --bench dag_stress         # DAG stress test
```

## Related Crates

- [`laminar-sql`](../laminar-sql) -- SQL parser that produces operator configurations
- [`laminar-storage`](../laminar-storage) -- WAL and checkpointing that consumes changelog entries
- [`laminar-db`](../laminar-db) -- Database facade that orchestrates the reactor
