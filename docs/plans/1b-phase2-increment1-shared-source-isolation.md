# 1B Phase 2 — Increment 1: shared-source failure isolation (engine replay buffer)

Status: **planned** (not started). Branch `feat/shuffle-barrier-after-kill-recovery`.
Parent plan: `docs/plans/pipeline-failure-recovery-architecture.md`. This doc is the detailed,
file-anchored implementation plan for **Increment 1** only. Increment 2 (per-domain barrier
alignment + per-domain 2PC for the full exactly-once path) is previewed at the end, out of scope
here.

## Goal

Make two queries that **share a source** recover independently the same way disjoint-source
queries already do under 1B v1: a fault in one shared-source query holds back and replays **only
its** rows, while the healthy sibling commits and advances — **no gap/dup on either sink**, and
the source is still read **once**.

This is the at-least-once / per-sink-EO path. Under full pipeline exactly-once a domain fault
still rewinds the whole pipeline (unchanged) until Increment 2.

## Decision (settled with the owner)

**Engine replay buffer, in-memory.** One physical consumer per source name (unchanged); the
engine keeps a bounded, in-memory, append-only buffer per source and feeds each failure domain
its own slice via a per-`(source, domain)` cursor. A faulted domain replays its slice from the
buffer; healthy siblings only ever see new data.

Rejected for now (recorded in the parent plan):
- **Per-domain consumers** (Flink/RisingWave): N reads of the source, connector-trait change,
  expensive for single-cursor CDC (binlog / logical replication, both first-class here).
- **Durable shared log** (Materialize `persist`): the strictly-correct answer (crash-durable,
  unbounded isolation) but pays an object-store write on the ingest hot path. The per-`(source,
  domain)` cursor shape below does **not** foreclose swapping a durable backend in later; we just
  don't build that abstraction now (no unused scaffolding).

### Semantics this buys (and the one it does not)

- **Performance:** read-once preserved; per-domain fan-out is cursor arithmetic over `Arc`-shared
  `RecordBatch`es (no data copy); steady-state footprint ≈ one cycle of ingest; no new hot-path
  I/O.
- **Data / exactly-once durability:** **unchanged.** The buffer is a cache over the existing
  durable path (manifest offsets + upstream replay) and never holds the only copy of anything. It
  cannot make durability worse — worst case it is bypassed and you get today's recovery.
- **Online isolation:** a transient domain fault is isolated and replayed from the buffer; healthy
  siblings advance.
- **NOT crash-durable isolation:** after a process crash, or a fault that outlives the buffer cap,
  isolation collapses to today's **global-min recovery** (everyone rewinds to the conservative
  per-source offset). No data loss; ALO tolerates the re-emit, per-sink EO dedups via its own 2PC.
  Crash-durable isolation is the durable-log upgrade, deferred.

### Why this needs no opaque-offset comparison and no manifest change

The buffer is append-only and preserves arrival order; each entry carries the `SourceCheckpoint`
captured at production time (already on `SourceMsg::Batch`). A domain's cursor is an **integer
index** into the buffer. Therefore:

- The conservative durable offset for a source = the checkpoint stored at the entry just before
  `min` over domains of the **committed** cursor. Integer `min`, then index — **no comparing
  opaque connector offsets.**
- That single value is exactly what the manifest already stores per source today
  (`checkpoint_manifest.rs:47`, `source_offsets: HashMap<String, ConnectorCheckpoint>`), so
  **the manifest schema and the recovery path are unchanged.** Per-domain progress beyond the min
  lives only in memory (lost on crash → global-min recovery, as above).

This shrinks the "5 coupled subsystems" of the parent plan: for Increment 1, subsystems **2
(offset model / manifest)** and **5 (recovery)** stay as-is, and **3 (global barrier alignment)**
is untouched (that is Increment 2). Increment 1 is subsystems **1 (source consumption)** and **4
(cycle buffering)** plus the domain split.

## Feeding path — SPIKE RESOLVED (2026-06-26)

Source data reaches operators by **two** paths, confirmed against the tree:

1. **Graph fan-out / `input_bufs`** — `prime_sources` (`operator_graph.rs:1968`) pushes a source's
   batches into `input_bufs[source_node][0]`; the source node (`SourcePassthrough`) routes to
   downstream ops via `output_routes` (`operator_graph.rs:1827`); `execute_single_operator` passes
   them to `operator.process(inputs, …)` (`operator_graph.rs:1679`).
2. **Live providers / DataFusion table scans** — `register_source_tables`
   (`operator_graph.rs:1629`) `swap`s the batch set into a `LiveSourceProvider` registered in the
   **shared `SessionContext`** keyed by source **name** (`operator_graph.rs:763-779`). Query
   *outputs* are likewise swapped into name-keyed providers in `route_output`
   (`operator_graph.rs:1816`) so chained MVs can scan them.

**Which operators use which:** `SqlQueryOperator::process` (`operator/sql_query.rs:826`) reads its
`inputs` (= `input_bufs`) for the **Compiled** (vectorized projection/filter) and **Agg**
(incremental aggregation) states. The **CachedPlan / CachedPhysical** states (`sql_query.rs:868/885`)
and the **join / asof / temporal / lookup** operators execute a DataFusion plan that **SQL-scans the
name-keyed live providers** in the shared context. `input_bufs` non-emptiness is only a *gate* for
those.

**Consequence for the design:**
- The live provider is **global-by-name in one shared `SessionContext`** — two domains reading
  `trades` scan the *same* swapped batches. So per-domain *data* slicing (replay, Slice 2) cannot
  go through it unchanged; it needs **per-`(name, domain)` providers** or forcing isolated
  SQL-scan operators onto `input_bufs`-only execution. This is the Slice 2 decision.
- Per-domain *offset accounting* (Slice 1) needs **no** divergent feeding: every domain sees the
  same batches each cycle (read-once fan-out), and only the *committed offset* diverges when one
  domain faults. So Slice 1 ships without touching the live-provider path.

## Domain-split mechanism

Two viable shapes; **recommended: cut-at-source** (smaller blast radius), with per-consumer source
nodes as the documented fallback if the live-provider path makes the cut awkward.

- **Recommended — cut at the source in `compute_node_domains`** (`operator_graph.rs:1590`): keep
  one shared source node per name (no change to `ensure_source_node` / `source_map` /
  construction), but make union-find **not** merge two downstream subgraphs through a shared source
  node. Each downstream query becomes its own domain; the shared source node is its own (inert)
  domain. Per-domain feeding then happens at the source node's fan-out: each `output_route` gets
  the slice from that route's domain cursor instead of the same batches for all routes.
  - Cursor state keyed by `(source node id, downstream domain)`.
  - A fault in the source node itself (rare; it is a passthrough) is treated as faulting **all**
    domains reading it — correct, since they all depend on it.
- **Fallback — per-consumer source nodes**: give each query its own logical source node for the
  shared name (all backed by the one physical consumer + shared buffer). Domains then fall out of
  the existing union-find with **no** `compute_node_domains` change, and per-domain feeding falls
  out of the existing per-node `input_bufs`/`prime_sources`. Cost: `ensure_source_node`
  (`operator_graph.rs:792`) stops deduping by name; `source_map` (name→single node, used at
  `:793/:1151/:1578`) must split into a physical-source registry vs per-consumer logical nodes.
  More construction surgery, but no special source fan-out logic.

When the feature flag is **OFF**, neither change is active: `compute_node_domains` unions through
sources as today and shared-source queries stay one domain — byte-identical to current behavior.

## Data structures (in `StreamingCoordinator`, `streaming_coordinator.rs`)

```text
ReplayBuffer (one per source name)
  base_index : u64                       // global index of entries[0] after eviction
  entries    : VecDeque<(RecordBatch, SourceCheckpoint)>   // Arc-shared batches
  bytes      : usize                     // sum of get_array_memory_size, for the cap

Per-(source, domain) cursors (replaces the meaning of committed/pending_offsets):
  committed_cursor : FxHashMap<(Arc<str>, DomainKey), u64>  // next entry this domain will read
  pending_cursor   : FxHashMap<(Arc<str>, DomainKey), u64>  // staged this cycle; commit on success
```

- `DomainKey` is **deterministic and restart-stable** — not the current insertion-order integer
  from `compute_node_domains` (`root_to_domain.len()` at `:1621`, which depends on node iteration
  order). Derive it from the **sorted set of output (MV/sink) names** in the component (fall back
  to sorted source names for an output-less component). Stable because recovery rebuilds the same
  graph from the same SQL. (Only strictly required if/when per-domain offsets become durable; for
  Increment 1 the durable offset is the global min, but use the stable key from the start so the
  in-memory maps survive a topology recompute within a run.)
- Per-source durable offset for the manifest = checkpoint at index `min(committed_cursor over
  domains of that source) − 1` (or the seed/restore checkpoint if min == base). Computed in
  `current_source_offsets` (`streaming_coordinator.rs:878`).
- **Eviction:** drop `entries` before `min(committed_cursor)` (every domain durably past them),
  advancing `base_index`. Safe ahead of the manifest because crash recovery re-reads from the
  connector at the last-checkpointed min.
- **Cap:** new `PipelineConfig` field `max_replay_buffer_bytes` (per source). On overflow → log +
  metric + **fall back to global recovery** (fault for recovery / drop buffer), the documented
  degradation. Isolation window ≈ `cap / ingest_rate`.

## Flag

Add to `PipelineConfig` (`crates/laminar-db/src/pipeline/config.rs`):
```rust
/// Isolate queries that share a source into independent failure domains (1B Phase 2).
/// Default off; when off, shared-source queries fault and recover together (1B v1).
pub shared_source_isolation: bool,   // default: false
pub max_replay_buffer_bytes: usize,  // per-source cap; only used when the above is on
```
Thread it to `OperatorGraph` (gates the `compute_node_domains` cut) and to the coordinator (gates
the buffer + per-domain cursors). Default-OFF everywhere; server never sets it yet.

## Slices (each: flag-gated, production-correct, reviewed, soaked before the next)

**Slice 1 — domain split with conservative whole-source hold-back (DONE, 2026-06-26).**
The minimal unit with a real runtime consumer; matches the already-soaked 1B v1 semantics,
extended from disjoint to shared sources. Implementation note: per-`(source,domain)` offsets
turned out **not** to be needed here — the existing conservative whole-source hold-back already
delivers the win, so it ships with **zero coordinator/manifest change** (moved the granular offset
work to Slice 2).
- Flag `shared_source_isolation` on `PipelineConfig` + `LaminarConfig`; threaded to `OperatorGraph`
  (`set_shared_source_isolation`) + coordinator. Default OFF = byte-identical to today. ✅
- Cut-at-source in `compute_node_domains` (`operator_graph.rs:1590`): under the flag, skip union of
  edges leaving a source node, and leave source nodes **unassigned** (`node_domain = usize::MAX`) so
  `domain_count` stays equal to the number of query domains (the all-domains-failed check stays
  exact). Two shared-source queries → distinct domains. ✅
- Failed-source mapping (`source_feeds_failed_domain`, `operator_graph.rs`): a source is held back
  when its own domain faulted (flag off) **or** any domain it directly feeds faulted (flag on) —
  since the source is cut out of every consumer's domain. Reuses the existing
  `cycle_failed_sources` / `commit_pending_offsets_except` path unchanged. ✅
- Consumer (the shippable win): when shared-source query A faults, only A's domain is skipped;
  sibling B still executes and **writes its sink this cycle** instead of the whole shared-source
  group stalling. The shared source is held back conservatively (it feeds A), so on recovery both
  rewind to that offset and B re-reads (ALO dup tolerated / per-sink-EO dedups). Online, A's faulted
  cycle is dropped (not yet replayed) — same as 1B v1.
- Unit tests (operator_graph): `test_node_domains_shared_source_isolated` (split + source=MAX) and
  `test_execute_cycle_isolates_shared_source_sibling` (healthy sibling emits, shared source held
  back). Flag-off covered by the existing `test_node_domains_shared_source_joined`. ✅
- Remaining for Slice 1: DB-level integration test (two MVs sharing a source, one faults) + soak
  (plain liveness + kill-9 file-checkpoint, `LAMINAR_SOAK_KILLS=4`, no Kafka).

**Slice 2 — online replay + per-domain feeding (the core, addresses the live-provider wall).**
- Raise buffer retention to "until every domain has replayed past it"; on a domain fault, **re-feed
  that domain its held slice next cycle** without re-running healthy domains (per-domain, never a
  whole-`execute_cycle` re-run — that re-wrote healthy sinks and dup'd in prior work; see
  Guardrails).
- Resolve the **live-provider wall**: isolated SQL-scan operators (CachedPlan/CachedPhysical, joins)
  must read a per-`(name,domain)` view, not the shared global-by-name provider. Decide between
  per-`(name,domain)` live providers vs `input_bufs`-only execution for isolated operators; record
  the decision here.
- Eviction + cap (`max_replay_buffer_bytes`) + overflow → fall back to global recovery.
- Consumer: a faulted shared-source query replays and catches up with the healthy sibling
  undisturbed — no gap/dup on either sink online.
- Soak: as Slice 1, plus the validation scenario below.

**Slice 3 — hardening + observability.**
- Metrics: `replay_buffer_bytes{source}`, `replay_buffer_pinned_domain`, replay/eviction counters,
  cap-overflow→global-recovery counter.
- Crash-recovery path test: kill-9 with one domain ahead of another; assert recovery to global min,
  no dup (ALO re-emit tolerated / EO sink dedup), no gap.
- Principal review pass.

## Validation (the parent plan's acceptance test)

Two queries sharing one source, one erroring: the healthy query commits and advances while the
erroring one recovers independently; the shared-source offset never skips the faulted query's rows
(no gap/dup on either sink). Plus the disjoint-source v1 case stays green, and **flag-OFF behavior
is byte-identical to today** (regression guard).

## Guardrails (from prior work — do not repeat)

- **Never replay by retaining `source_batches_buf` and re-running `execute_cycle`** — it re-writes
  *all* sinks (incl. healthy ones) → duplicates; this was tried and reverted. The buffer exists
  precisely to replay **one domain** without re-running the others.
- **No per-cycle / per-barrier gossip on the hot path** — regresses alignment timeouts.
- **No unwired foundation** — build each layer with its consumer (an earlier 1B foundation was
  removed for reading as dead code). Slices above each ship a consumer.
- **Stable `DomainKey`** — never key persistent/in-memory cross-recompute state by the
  insertion-order domain integer.
- Soak harness: `crates/laminar-server/tests/cluster_soak.rs`; recreate Redpanda first
  (`down -v`/`up`); system-OpenSSL + ORT env on PATH. The **EO-Kafka** soak is flaky on this box
  *on baseline too* (barrier-alignment stall) — not a regression signal. Use the **plain liveness**
  and **kill-9 file-checkpoint** soaks as the green bars.

## Key anchors (verified against the tree)

| Concern | Location |
| --- | --- |
| Domain union-find (cut here) | `operator_graph.rs:1590` `compute_node_domains` |
| Domain id (make deterministic) | `operator_graph.rs:1621` `root_to_domain` |
| Source-node dedup by name | `operator_graph.rs:792` `ensure_source_node` |
| Source fan-out (per-domain slice) | `operator_graph.rs:1827` push to `output_routes` |
| `input_bufs` feed | `operator_graph.rs:1968` `prime_sources` |
| Live-provider feed | `operator_graph.rs:1629` `register_source_tables` |
| Per-cycle execute + offset commit | `streaming_coordinator.rs:569-594` |
| Buffer accumulator (today: cleared) | `streaming_coordinator.rs:81,504,692,736` |
| Offset maps (today: by `source_idx`) | `streaming_coordinator.rs:89-92` |
| Hold-back commit | `streaming_coordinator.rs:903` `commit_pending_offsets_except` |
| Durable per-source offset | `streaming_coordinator.rs:878` `current_source_offsets` |
| Manifest offsets (unchanged) | `checkpoint_manifest.rs:47` `source_offsets` |
| `CycleOutcome` (add failed domains) | `pipeline_callback.rs:63` |
| Config flag | `pipeline/config.rs` `PipelineConfig` |

## Increment 2 (preview — out of scope here)

Per-domain barrier alignment (`PendingBarrier` per domain, decoupling the global
`sources_aligned >= sources_total` gate at `streaming_coordinator.rs:955`) + per-domain 2PC + scoped
(region) recovery, so a faulted domain does not stall or rewind healthy domains' checkpoints under
full exactly-once. Required under *every* design fork; the buffer does not simplify it.
