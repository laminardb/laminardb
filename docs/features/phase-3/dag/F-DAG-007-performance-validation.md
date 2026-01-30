# F-DAG-007: Performance Validation

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DAG-007 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-DAG-003 (Executor), F-DAG-004 (Checkpointing), F-DAG-005 (SQL/MV) |
| **Owner** | TBD |
| **Created** | 2026-01-30 |
| **Crate** | `laminar-core` |
| **Module** | `benches/dag_bench.rs` |

## Summary

Criterion benchmarks and stress tests validating that DAG pipeline execution meets LaminarDB's performance targets. Covers routing latency, multicast throughput, end-to-end hot path latency, checkpoint overhead, and recovery time.

## Goals

- Criterion benchmarks for all DAG hot path operations
- Verify <500ns p99 end-to-end latency with DAG routing
- Verify <5% checkpoint throughput overhead
- Verify <5s recovery time for 1GB state
- Stress test with 20-node complex DAG topology
- Memory overhead profiling (DAG metadata <10% vs linear)

## Benchmarks

| Benchmark | Target | Method |
|-----------|--------|--------|
| `bench_routing_lookup` | <50ns | Criterion, single route() call |
| `bench_multicast_publish` | <100ns | Criterion, single publish() |
| `bench_multicast_consume` | <50ns | Criterion, single consume() |
| `bench_executor_linear_3` | <500ns p99 | Criterion, 3-node linear DAG |
| `bench_executor_fan_out_3` | <500ns p99 | Criterion, 1->3 fan-out |
| `bench_executor_diamond` | <500ns p99 | Criterion, diamond topology |
| `bench_executor_complex_10` | <1us p99 | Criterion, 10-node mixed DAG |
| `bench_throughput_linear` | >500K/sec/core | Sustained throughput test |
| `bench_throughput_fan_out` | >500K/sec/core | Fan-out throughput |
| `bench_checkpoint_overhead` | <5% | Throughput with/without checkpoints |
| `bench_recovery_1gb` | <5s | Recovery time for 1GB state |

## Stress Tests

- [ ] 20-node DAG with mixed fan-in/fan-out, sustained load
- [ ] 8-way fan-out from single shared stage
- [ ] Deep linear pipeline (10 operators in sequence)
- [ ] Diamond DAG with heavy aggregation operators
- [ ] Checkpoint during sustained load (no latency spikes >10us)

## Completion Checklist

- [ ] Criterion benchmark suite in `benches/dag_bench.rs`
- [ ] All latency targets met
- [ ] Throughput targets met
- [ ] Checkpoint overhead verified
- [ ] Recovery time verified
- [ ] Stress tests passing
- [ ] Results documented in `docs/benchmarks/`
