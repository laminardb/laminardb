# F-DAG-002: Multicast & Routing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DAG-002 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-DAG-001 (Core Topology), F071 (Zero-Allocation) |
| **Blocks** | F-DAG-003 (Executor) |
| **Owner** | TBD |
| **Created** | 2026-01-30 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/dag/multicast.rs`, `laminar-core/src/dag/routing.rs` |

## Summary

Zero-copy multicast buffer for shared intermediate stages and pre-computed routing table for O(1) hot path dispatch. The multicast buffer uses reference-counted pre-allocated slots (Ring 0 safe). The routing table is cache-line aligned and computed once at topology construction time (Ring 2).

## Goals

- `MulticastBuffer<T>` with per-slot reference counting
- `publish()` sets refcount to consumer_count, `consume(idx)` decrements
- Slot reuse when refcount reaches 0 (no allocation)
- Backpressure: publish fails when slowest consumer hasn't freed a slot
- `RoutingTable` with `RoutingEntry` per (node, port), cache-line aligned (64 bytes)
- O(1) `route()` lookup via array index
- `SharedStageMetadata` for tracking consumer counts and watermarks

## Non-Goals

- Event execution (F-DAG-003)
- Dynamic routing table updates at runtime
- NUMA-aware slot placement (future optimization)

## Technical Design

See [Full Design Spec, Sections 4-5](../F-DAG-001-dag-pipeline.md#4-shared-intermediate-stages-zero-copy-multicast) for detailed implementation.

### MulticastBuffer<T>

- Pre-allocated `Box<[UnsafeCell<MaybeUninit<T>>]>` slots
- Power-of-2 capacity with bitmask indexing
- Single `write_pos: AtomicU64` (single writer)
- Per-consumer `read_positions: Vec<AtomicU64>`
- Per-slot `refcounts: Box<[AtomicU32]>`
- `publish()`: check refcount == 0, write slot, set refcount = consumer_count
- `consume(idx)`: read value, advance read_pos, decrement refcount

### RoutingTable

- `routes: Box<[RoutingEntry]>` indexed by `node_id * MAX_PORTS + port`
- `#[repr(C, align(64))]` RoutingEntry: targets[8], target_count, is_multicast
- `route(source, port) -> &RoutingEntry` via `get_unchecked` (bounds checked at build time)

### Module Structure

```
crates/laminar-core/src/dag/
+-- multicast.rs    # MulticastBuffer<T>, SharedStageMetadata
+-- routing.rs      # RoutingTable, RoutingEntry
```

## Performance Targets

| Operation | Target |
|-----------|--------|
| `publish()` | <100ns |
| `consume()` | <50ns |
| `route()` lookup | <50ns |
| Multicast to N consumers | <100ns total |

## Test Plan

### Unit Tests (12+)

- [ ] `test_multicast_single_consumer` - Publish/consume roundtrip
- [ ] `test_multicast_multiple_consumers` - All consumers read same value
- [ ] `test_multicast_backpressure` - Full buffer returns error
- [ ] `test_multicast_slot_reuse` - Slot freed after all consumers read
- [ ] `test_multicast_partial_consume` - Some consumers read, slot not freed
- [ ] `test_multicast_wrap_around` - Sequence wraps past capacity
- [ ] `test_multicast_empty_consume` - Consume on empty returns None
- [ ] `test_routing_table_build` - Build from DAG topology
- [ ] `test_routing_table_linear` - Linear DAG, single target per entry
- [ ] `test_routing_table_fan_out` - Fan-out, multiple targets, is_multicast=true
- [ ] `test_routing_table_terminal` - Sink node, target_count=0
- [ ] `test_routing_entry_cache_alignment` - Verify 64-byte alignment

### Benchmarks

- [ ] `bench_multicast_publish` - Target: <100ns
- [ ] `bench_multicast_consume` - Target: <50ns
- [ ] `bench_routing_lookup` - Target: <50ns

## Completion Checklist

- [ ] `MulticastBuffer<T>` implemented in `dag/multicast.rs`
- [ ] `RoutingTable` implemented in `dag/routing.rs`
- [ ] 12+ unit tests passing
- [ ] Benchmarks meet targets
- [ ] `cargo clippy` clean
