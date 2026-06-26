# Pipeline failure recovery — remaining work: 1B failure-domain isolation

Cold-start-ready. The original failure-recovery roadmap had four items; three have landed.
**1B (blast-radius isolation) is the only outstanding piece.** Anchors verified against the
tree at `1c45f510`.

## Already landed (context, no action)
- **1A single-node** — a fatal `execute_cycle` error under exactly-once faults and recovers
  from the last checkpoint; opt-in auto-restart supervisor + `RestartPolicy`
  (`pipeline_lifecycle.rs:131` `spawn_supervised_restart`, `pipeline_callback.rs:1557`
  `fault_on_cycle_error`). The open question "is continue-past-error ever desired?" is
  resolved: ExactlyOnce → recover, AtLeastOnce → continue + metric.
- **1A-cluster** — leader-coordinated global restart-to-epoch (`coordinated_recovery.rs`): on a
  fault every node rewinds to the highest cluster-wide committed epoch. Soak-validated.
- **Convergence gate (Notes 2/3)** — the leader publishes a converged verdict off the hot path
  through a `watch`; the checkpoint gate is a local borrow, no per-checkpoint gossip.

## 1B — independent failure domains (blast-radius isolation)

### Problem
Recovery (1A) and the recoverable `ShuffleNotReady` defer both act at **whole-pipeline**
granularity. Co-located queries share only source nodes, yet a fatal error in one query faults
the entire pipeline, and a recoverable defer in one query stalls the cycle for its siblings.
`OperatorGraph` is a flat `nodes` + `topo_order` with no component boundary (`source_node_ids`
`operator_graph.rs:276`, `input_sources` `:282`).

### What 2026 engines do (and the requirement they share)
- **Flink 2.0** (Mar 2025) — *region failover*: restart only the failed pipelined region and
  its dependents, not the job. Over disaggregated state (S3 primary, local cache), per-region
  restore is cheap and independent of state size.
- **RisingWave v2.3** (2025) — *database isolation*: a streaming-job failure is contained to
  its database; recovery **and checkpointing are per-database**; resource groups pin domains to
  compute nodes.

Both give every failure domain **independent checkpoint + source-offset state**. That is the
load-bearing requirement — not the graph partitioning (easy), but per-domain offset tracking so
a domain can rewind its own source view without re-feeding a sibling.

### Design
**Domain = a connected query component.** Union-find over the DAG once in `compute_topo_order`
(`operator_graph.rs:1505`) → `node_domain: Vec<usize>`.

The source-sharing question decides isolation-vs-correctness and resolves the contradiction
between the two prior drafts (one said "exclude sources", one said "include"):

- **v1 — sources join the domains that read them.** Queries with *disjoint* sources become
  separate domains → clean isolation today, no offset rework (each domain already owns its
  source). Queries that *share* a source stay one domain → recover together, which is correct
  (re-seeking a shared source for one query would re-feed the other → duplicates). The free,
  safe win.
- **Phase 2 — split shared-source queries via per-(source,domain) offsets.** To isolate queries
  that share a source (Flink per-region consumers / RisingWave per-database checkpoint), the
  coordinator's **global** `committed_offsets`/`pending_offsets`
  (`streaming_coordinator.rs:90,92`) must become per-(source,domain) so a faulted domain rewinds
  its slice independently. This is the real cost and what makes full 1B an **L**.

**Per-domain cycle outcome.** `execute_cycle` (`operator_graph.rs:1787`) already iterates
`topo_order`; the defer seam is `execute_single_operator:1654`
(`depends_on_stream || is_shuffle_not_ready`). Generalize: track `failed_domains` for the cycle,
gate `execute_single_operator` + `route_output` (`:1743`) on the node's domain not being failed;
sibling domains finish and their sinks commit. `commit_pending_offsets`
(`streaming_coordinator.rs:874`) commits only offsets whose every consuming domain succeeded.

### Steps
1. `operator_graph.rs`: build `node_domain` in `compute_topo_order` (`:1505`); expose
   `domain_of(node)` / `domains_of_source(source)`.
2. `operator_graph.rs` `execute_cycle`: carry `failed_domains: FxHashSet`; gate operator run +
   `route_output` on it; return `{committed_domains, failed_domains}` instead of one `Err`.
3. `streaming_coordinator.rs`: commit only fully-succeeded sources' offsets; scope a fault →
   recover to the faulted domain's sources. (Phase 2: per-(source,domain) offset vectors.)

### Effort / risk
- v1 (disjoint-source isolation): **M**, no offset-model change.
- Full (shared-source isolation): **L / medium-high** — the global offset model is load-bearing;
  per-(source,domain) offsets is the rework. Disaggregated/per-domain checkpoint state (à la
  Flink 2.0 / RisingWave) is the enabler if per-domain restore cost matters.

### Validation
Two queries sharing one source, one erroring: the healthy query commits and advances while the
erroring one defers/recovers independently, and the shared-source offset never skips the faulted
query's rows (no gap/dup on either sink). Add a disjoint-source variant for the v1 win.

### Don't repeat (from the prior work)
- Don't "retain `source_batches_buf` + replay" on cycle error → re-runs `execute_cycle` →
  re-writes sinks → DUPLICATES. Recover, don't replay-in-place.
- Don't add per-cycle/per-barrier gossip on the hot path → regresses alignment timeouts.
- Don't land unwired domain scaffolding: an earlier 1B foundation fed only a log line and was
  removed. Build the partition *with* its consumer (the `execute_cycle` scoping), not ahead.
- Soak harness + env: `crates/laminar-server/tests/cluster_soak.rs` (recreate Redpanda first;
  system-OpenSSL + ORT env). EO-gap/alignment are flaky — validate over ≥8 runs, delta-off and
  delta-on.

## Sources
- [Apache Flink 2.0.0: A New Era of Real-Time Data Processing](https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/)
- [Disaggregated State Management in Apache Flink 2.0 (VLDB 2025)](https://www.vldb.org/pvldb/vol18/p4846-mei.pdf)
- [Task Failure Recovery — region failover (Apache Flink docs)](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/disaggregated_state/)
- [Workload Isolation in RisingWave: Database Isolation & Resource Groups](https://risingwave.com/blog/workload-isolation-in-risingwave/)
- [Let It Recover: HA and Fault Tolerance in RisingWave](https://risingwave.com/blog/let-it-recover-dive-into-the-high-availability-and-fault-tolerance-in-risingwave/)
