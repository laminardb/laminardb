# laminar-core

Core streaming engine for LaminarDB -- operators, checkpoint barriers, and streaming infrastructure.

## Overview

Foundation crate providing window assigners, checkpoint barrier protocol, streaming channels, and time/watermark primitives.

## Key Modules

| Module | Purpose |
|--------|---------|
| `operator` | Window assigners (tumbling, sliding), table cache, changelog types, operator state |
| `time` | Event time processing, watermarks (partitioned, keyed, alignment groups) |
| `streaming` | SPSC/MPSC channels, source/sink abstractions, backpressure |
| `checkpoint` | Distributed checkpoint barrier protocol, barrier injection |
| `subscription` | Reactive push-based subscriptions: events, notifications, registry, dispatcher |
| `mv` | Cascading materialized views |
| `alloc` | Priority-class allocation tracking |
| `lookup` | Lookup table trait, predicate types, foyer caches |
| `serialization` | Shared Arrow IPC serialization for RecordBatch |
| `error_codes` | Structured `LDB-NNNN` error code registry and zero-alloc `HotPathError` |
| `delta` | Distributed mode: discovery (gossip, Kafka), coordination, partition ownership |

## Feature Flags

| Flag | Purpose |
|------|---------|
| `io-uring` | Linux 5.10+ io_uring integration |
| `hwloc` | Enhanced NUMA topology discovery (hwlocality) |
| `delta` | Distributed mode (chitchat gossip, coordination) |

## Benchmarks

```bash
cargo bench -p laminar-core --bench latency_bench      # p99 latency
cargo bench -p laminar-core --bench window_bench       # Window operations
cargo bench -p laminar-core --bench lookup_join_bench  # Lookup join throughput
cargo bench -p laminar-core --bench cache_bench        # foyer cache hit/miss
cargo bench -p laminar-core --bench streaming_bench    # Streaming channels
cargo bench -p laminar-core --bench subscription_bench # Subscription dispatch
```

## Related Crates

- [`laminar-sql`](../laminar-sql) -- SQL parser that produces operator configurations
- [`laminar-storage`](../laminar-storage) -- Checkpoint persistence
- [`laminar-db`](../laminar-db) -- Database facade that orchestrates execution
