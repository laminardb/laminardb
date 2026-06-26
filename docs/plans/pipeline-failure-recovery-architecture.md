# Pipeline failure recovery — remaining work: 1B Phase 2 (shared-source isolation)

The original roadmap had four items; all are landed except **1B Phase 2**: isolating
queries that *share* a source. The blast-radius mechanism (failure domains) and the
disjoint-source win (1B v1) are in (commit `6d907a34`).

## Already landed (context, no action)
- **1A single-node** — a fatal `execute_cycle` error under exactly-once faults and recovers
  from the last checkpoint; opt-in auto-restart supervisor + `RestartPolicy`
  (`pipeline_lifecycle.rs` `spawn_supervised_restart`, `pipeline_callback.rs`
  `fault_on_cycle_error`). ExactlyOnce → recover, AtLeastOnce → continue + metric.
- **1A-cluster** — leader-coordinated global restart-to-epoch (`coordinated_recovery.rs`): on a
  fault every node rewinds to the highest cluster-wide committed epoch. Soak-validated.
- **Convergence gate (Notes 2/3)** — the leader publishes a converged verdict off the hot path
  through a `watch`; the checkpoint gate is a local borrow, no per-checkpoint gossip.
- **1B v1 — failure domains + disjoint-source isolation** (commit `6d907a34`).
  `compute_node_domains` (`operator_graph.rs`) union-finds the DAG into connected components
  (`node_domain`); sources join the domains that read them, so disjoint queries are separate
  domains and shared-source queries are one. `execute_cycle` scopes a fatal operator error to its
  domain (skips the rest, finishes siblings) and reports `take_cycle_failures()` → `CycleOutcome`.
  The coordinator commits healthy domains' offsets and holds back the faulted domain's via
  `commit_pending_offsets_except`. Under exactly-once / coordinated recovery any domain fault
  still rewinds the whole pipeline, so the isolation is the at-least-once / per-sink-EO path. No
  offset-model change.

## 1B Phase 2 — isolate queries that share a source

### Problem
Shared-source queries are fused into one domain because the engine has **one source consumer
with one cursor per source name**, drained once per cycle into a shared buffer that every query
reads lock-step. So a faulted query can't rewind its view of the source without re-feeding the
healthy sibling (→ duplicates). Splitting them needs each `(source, domain)` to own its cursor,
checkpoint, and recovery — the RisingWave "database isolation" / Flink "region failover" model.

### What this touches (5 coupled subsystems — verified against the tree)
Effort is **XL**, not L: every layer below assumes "all queries on a source = one failure
domain", and each is load-bearing for exactly-once.
1. **Source consumers** — one connector/cursor per source name (`connector_manager.rs:209`,
   `streaming_coordinator.rs:226`); `register_source_tables`/`LiveSourceHandle`
   (`operator_graph.rs`) swaps one batch set into all queries. Needs per-`(source,domain)`
   consumption (per-domain consumer, or an engine-side per-domain replay buffer).
2. **Offset model** — `committed_offsets`/`pending_offsets` keyed by `source_idx`
   (`streaming_coordinator.rs:89`); manifest `source_offsets` keyed by source **name**
   (`checkpoint_manifest.rs:47`). Both must key by `(source, domain)`.
3. **Barrier alignment is global — the biggest blocker.** A checkpoint commits only when *all*
   sources align (`streaming_coordinator.rs:955`, `sources_aligned >= sources_total`); one
   faulted/deferred domain stalls the whole pipeline's 2PC. EO isolation needs **per-domain
   barriers + per-domain 2PC** (`PendingBarrier` per domain).
4. **Cycle buffering** — single `source_batches_buf` per source name
   (`streaming_coordinator.rs:81`); needs per-`(source,domain)` buffering so a domain drains only
   its slice.
5. **Recovery is whole-pipeline** — a fault drops the coordinator and re-seeks *all* sources
   (`pipeline_lifecycle.rs` restart, `coordinated_recovery.rs` global epoch). Needs scoped
   (region) recovery: restart only the faulted domain's operators + consumers.

### Open design fork (decide first)
- **Per-domain consumers** (Flink): register a shared source as N logical consumers (Kafka
  supports independent cursors / seek). Cleanest; explodes connector count and needs the
  connector trait to carry a domain id. Hard for single-cursor sources (CDC binlog).
- **Engine replay buffer** (no connector change): one consumer, but the engine keeps a bounded
  per-`(source,domain)` buffer and re-feeds a faulted domain its slice. Avoids the connector
  rebuild; bounded-buffer + checkpoint interaction is the cost.

### Constraints (from the owner, 2026-06-26)
- **No backward compatibility** — zero external users; change the manifest schema / offset keying
  freely, no migration path needed.
- Every increment must be **production-correct and reviewed**; no unused code or scaffolding —
  build each layer **with its consumer** (an earlier unwired 1B foundation was removed for this).
- Land flag-gated (default-OFF) slices and **soak each** before the next.

### Validation
Two queries sharing one source, one erroring: the healthy query commits and advances while the
erroring one recovers independently, and the shared-source offset never skips the faulted query's
rows (no gap/dup on either sink). Plus the disjoint-source v1 case stays green.

### Don't repeat (from the prior work)
- Don't "retain `source_batches_buf` + replay" on cycle error → re-runs `execute_cycle` →
  re-writes sinks → DUPLICATES. Recover, don't replay-in-place.
- Don't add per-cycle/per-barrier gossip on the hot path → regresses alignment timeouts.
- Soak harness: `crates/laminar-server/tests/cluster_soak.rs` (recreate Redpanda first;
  system-OpenSSL + ORT env). The **EO-Kafka** soak (`LAMINAR_SOAK_KAFKA_BROKERS`) is currently
  flaky/red on this box *on baseline too* (barrier-alignment stall) — not a regression signal;
  the **plain liveness** and **kill-9 file-checkpoint** soaks (`LAMINAR_SOAK_KILLS=4`, no Kafka)
  are the reliable greens. Validate EO over ≥8 runs once the broker env is healthy.

## Sources
- [Apache Flink 2.0.0: A New Era of Real-Time Data Processing](https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/)
- [Disaggregated State Management in Apache Flink 2.0 (VLDB 2025)](https://www.vldb.org/pvldb/vol18/p4846-mei.pdf)
- [Task Failure Recovery — region failover (Apache Flink docs)](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/disaggregated_state/)
- [Workload Isolation in RisingWave: Database Isolation & Resource Groups](https://risingwave.com/blog/workload-isolation-in-risingwave/)
- [Let It Recover: HA and Fault Tolerance in RisingWave](https://risingwave.com/blog/let-it-recover-dive-into-the-high-availability-and-fault-tolerance-in-risingwave/)
