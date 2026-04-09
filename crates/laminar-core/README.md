# laminar-core

Core streaming engine for LaminarDB — operators, checkpoint barriers, and streaming infrastructure.

## Modules

| Module | Purpose |
|--------|---------|
| `operator` | Window assigners, table cache, changelog/Z-set types |
| `time` | Event time extraction, watermark generators, timer service |
| `streaming` | Source/Sink/Subscription API backed by crossfire channels |
| `checkpoint` | Barrier protocol for consistent snapshots |
| `lookup` | Lookup table trait, predicate pushdown, foyer cache |
| `mv` | Cascading materialized views |
| `alloc` | Priority-class enforcement (debug builds) |
| `error_codes` | Structured `LDB-NNNN` error codes and `HotPathError` |
| `serialization` | Shared Arrow IPC serialization |
| `delta` | (optional) Distributed coordination, gossip discovery |

## Benchmarks

```bash
cargo bench -p laminar-core --bench streaming_bench    # Channel and source throughput
cargo bench -p laminar-core --bench window_bench       # Window operations
cargo bench -p laminar-core --bench lookup_join_bench   # Lookup join throughput
cargo bench -p laminar-core --bench cache_bench         # foyer cache hit/miss
cargo bench -p laminar-core --bench latency_bench       # p99 latency
```
