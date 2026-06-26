# Pipeline failure recovery — remaining work: 1B Phase 2 (shared-source isolation)

The original roadmap had four items; all are landed except **1B Phase 2**: isolating
queries that *share* a source. The blast-radius mechanism (failure domains) and the
disjoint-source win (1B v1) are in.

## Already landed (context, no action)
- **1A single-node** — a fatal `execute_cycle` error under exactly-once faults and recovers
  from the last checkpoint; opt-in auto-restart supervisor + `RestartPolicy`
  (`pipeline_lifecycle.rs` `spawn_supervised_restart`, `pipeline_callback.rs`
  `fault_on_cycle_error`). ExactlyOnce → recover, AtLeastOnce → continue + metric.
- **1A-cluster** — leader-coordinated global restart-to-epoch (`coordinated_recovery.rs`): on a
  fault every node rewinds to the highest cluster-wide committed epoch. Soak-validated.
- **Convergence gate (Notes 2/3)** — the leader publishes a converged verdict off the hot path
  through a `watch`; the checkpoint gate is a local borrow, no per-checkpoint gossip.
- **1B v1 — failure domains + disjoint-source isolation.** `compute_node_domains`
  (`operator_graph.rs`) union-finds the DAG into connected components (`node_domain`); sources
  join the domains that read them, so disjoint queries are separate domains and shared-source
  queries are one. `execute_cycle` scopes a fatal operator error to its domain (skips the rest
  of that domain, finishes siblings) and reports `take_cycle_failures()` → `CycleOutcome`. The
  coordinator commits healthy domains' offsets and holds back the faulted domain's via
  `commit_pending_offsets_except`; under exactly-once / coordinated recovery any domain fault
  still rewinds the whole pipeline (single-node recovery is whole-pipeline), so the new
  isolation is the at-least-once / per-sink-EO path. No offset-model change.

## 1B Phase 2 — isolate queries that share a source

### Problem
Today shared-source queries are deliberately fused into one domain: re-seeking a shared source
for one query would re-feed the other → duplicates. So a fault in one still recovers its
siblings. To split them, each `(source, domain)` pair needs its own offset cursor.

### What 2026 engines do
- **Flink 2.0** region failover restarts only the failed pipelined region; over disaggregated
  state (S3 primary, local cache) per-region restore is cheap and size-independent.
- **RisingWave v2.3** database isolation: recovery **and checkpointing are per-database**;
  resource groups pin domains to compute nodes.

Both give every failure domain **independent checkpoint + source-offset state** — the
load-bearing requirement. The graph partitioning is easy; per-domain offset tracking is the cost.

### Design
The coordinator's **global** `committed_offsets` / `pending_offsets`
(`streaming_coordinator.rs`) become per-`(source, domain)` so a faulted domain rewinds its own
view of a shared source without re-feeding a sibling. A fault then scopes recovery to the
faulted domain's sources instead of the whole pipeline, and the EO/coordinated path can stop
rewinding healthy domains. Disaggregated/per-domain checkpoint state is the enabler if
per-domain restore cost matters.

### Effort / risk
**L / medium-high** — the global offset model is load-bearing; per-`(source, domain)` offsets +
per-domain recovery scoping is the rework.

### Validation
Two queries sharing one source, one erroring: the healthy query commits and advances while the
erroring one recovers independently, and the shared-source offset never skips the faulted
query's rows (no gap/dup on either sink).

### Don't repeat (from the prior work)
- Don't "retain `source_batches_buf` + replay" on cycle error → re-runs `execute_cycle` →
  re-writes sinks → DUPLICATES. Recover, don't replay-in-place.
- Don't add per-cycle/per-barrier gossip on the hot path → regresses alignment timeouts.
- Build the partition *with* its consumer (the `execute_cycle` scoping + coordinator commit),
  not ahead — unwired domain scaffolding was removed once already.
- Soak harness + env: `crates/laminar-server/tests/cluster_soak.rs` (recreate Redpanda first;
  system-OpenSSL + ORT env). EO-gap/alignment are flaky — validate over ≥8 runs, delta-off/on.

## Sources
- [Apache Flink 2.0.0: A New Era of Real-Time Data Processing](https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/)
- [Disaggregated State Management in Apache Flink 2.0 (VLDB 2025)](https://www.vldb.org/pvldb/vol18/p4846-mei.pdf)
- [Task Failure Recovery — region failover (Apache Flink docs)](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/disaggregated_state/)
- [Workload Isolation in RisingWave: Database Isolation & Resource Groups](https://risingwave.com/blog/workload-isolation-in-risingwave/)
- [Let It Recover: HA and Fault Tolerance in RisingWave](https://risingwave.com/blog/let-it-recover-dive-into-the-high-availability-and-fault-tolerance-in-risingwave/)
