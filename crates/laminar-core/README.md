# laminar-core

Core streaming engine for LaminarDB: operators, checkpoint barriers, and streaming infrastructure.

## Modules

| Module | Purpose |
|--------|---------|
| `operator` | Window assigners, table cache, changelog/Z-set types |
| `time` | Event time extraction, watermark generators |
| `streaming` | Bounded Source queue with one Sink consumer, backed by crossfire channels |
| `checkpoint` | Barrier protocol for consistent snapshots |
| `lookup` | Lookup table trait, predicate pushdown, in-memory cache |
| `mv` | Cascading materialized views |
| `alloc` | Priority-class enforcement (debug builds) |
| `error_codes` | Structured `LDB-NNNN` error codes and `HotPathError` |
| `serialization` | Shared Arrow IPC serialization |
| `delta` | (optional) Distributed coordination, gossip discovery |

## In-process source admission

`Source::push_arrow` has a **64 MiB** retained-Arrow-byte limit by default, configured by
`SourceConfig::max_queued_bytes`. Cloned producers share it. A reservation follows each
accepted batch until the source's single `Sink` receives it, and releases on failed
admission. `queued_arrow_bytes()` reports this charge. Batches returned to callers have
separate ownership. If the sink is dropped, Crossfire retains unread queue values and their
charges until the last producer handle drops; further pushes return `Disconnected`.

Pushes never wait for queue capacity. `ChannelFull` reports count or byte saturation;
`BatchTooLarge { bytes, limit }` rejects a batch larger than the whole budget, and
`Disconnected` reports a dropped sink. Rejection leaves sequence and watermark unchanged.
Constructors remain infallible; zero or limits above `MAX_SOURCE_QUEUED_BYTES` make Arrow
pushes return `InvalidConfig`. A slow sink never loses accepted messages: they stay queued
and producers see `ChannelFull`.

Charges use Arrow-reported retained array storage plus batch/column descriptors, including
backing buffers of slices, nested arrays and views. Aliased buffers are charged independently.
Schema metadata and allocator overhead are excluded. The standalone generic `push`, `try_push`
and record-batch helpers are **count bounded only**: no arbitrary user-defined `Record<T>` heap
size is inferred. DB typed source handles convert to Arrow before admission and therefore do
use the byte limit. These limits do not establish a process RSS bound.

## Benchmarks

```bash
cargo bench -p laminar-core --bench streaming_bench    # Channel and source throughput
cargo bench -p laminar-core --bench window_bench       # Window operations
cargo bench -p laminar-core --bench lookup_join_bench  # Lookup join throughput
cargo bench -p laminar-core --bench cache_bench        # lookup cache hit/miss
cargo bench -p laminar-core --bench latency_bench      # End-to-end event latency
```
