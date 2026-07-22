# Phase 0 execution plan: distributed keyed state

- **Status:** In progress; contracts and inventory only
- **Started:** 2026-07-22
- **Parent plan:** [distributed keyed/stateful operators](distributed-keyed-stateful-operators.md)
- **Decision:** [ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md)
- **Baseline:** `1e2f8429`; working branch `feature/distributed-keyed-state-adr`

## Outcome

Phase 0 selects and proves the contracts needed before a production working-state implementation
can begin. It does not add an LSM to the runtime, relax `[LDB-4007]`, enable a materialized view, or
change the cluster delivery guarantee.

The phase is complete only when maintainers can answer, with versioned evidence:

1. which physical operators retain state and what distribution model each requires;
2. which key/state ABI is durable across restore, rescale, and rolling upgrade;
3. what numerical workload, latency, resource, checkpoint, and RTO limits define success;
4. which one local LSM passed the same workload and fault contract;
5. which source/operator/output/sink scenario is being certified at-least-once; and
6. how an independent black-box soak will prevent an unearned production-ready claim.

## Existing substrate to reuse

- `laminar_core::state::PARTITIONING_ABI_VERSION` and raw-key hash golden vectors already exist.
  Extend them for canonical typed SQL-key encoding; do not create a second partition ABI.
- `VnodeRegistry`, assignment/process fencing, shuffle alignment, per-vnode artifact sealing, and
  source handoff remain the ownership/checkpoint authority.
- `crates/laminar-server/tests/cluster_soak.rs` already demonstrates three-process control, broker
  input cuts, frozen output cuts, and Kafka output oracles. Reuse those concepts for engineering
  tests, not as independent production certification.
- `.github/workflows/checkpoint-fault-soak.yml` remains a short weekly regression. It builds a
  debug-asserting test binary on a hosted runner and is not renamed as the production soak.
- The removed `tools/state-tier-bench` is historical workload evidence only. Its single-point
  Fjall wrapper and results are not restored.

## Workstream A — Mandatory operator capability inventory

### A1. Compile-time declaration

Add a required, non-default method to `GraphOperator`. The descriptor separates:

- implementation identity;
- state class: `Stateless`, `GlobalSingleton`, `VnodeKeyed`, `RebuildableReplicated`, or
  `LocalOnly`; and
- current cluster status: `DdlGuarded`, `Rejected(reason)`, or `InternalOnly`.

`DdlGuarded` is intentionally not called “certified”: existing DDL validation remains the sole
admission authority in Phase 0. `Rejected` must carry a non-empty fail-closed reason. No default is
allowed, so a new operator cannot compile without being inventoried.

`SqlQueryOperator` is classified once from its SQL shape: projection is `Stateless`, a direct
ungrouped aggregate is `GlobalSingleton`, `GROUP BY` is `VnodeKeyed`, and any parse/shape ambiguity
is `LocalOnly + Rejected`. It must never infer safety from an uninitialized runtime state or from
the presence of default vnode hooks.

### A2. Inventory and tests

Inventory all production implementations and all test probes. Focused tests cover projection,
global aggregate, grouped aggregate, and ambiguous SQL, plus descriptor invariants. Compiler errors
are the exhaustiveness check for new implementations.

**Exit:** both cluster and non-cluster feature builds pass; admission regression tests remain
byte-for-byte equivalent in outcome; no DDL path consumes the descriptor yet.

## Workstream B — Frozen workload, ABI, and delivery contracts

### B1. Numerical target profile

Before either backend is measured, named workload and operations owners must approve a target
profile containing:

- exact Linux/kernel/filesystem, CPU, RAM, NVMe, cgroup, and object-store/network environment;
- cache-resident and larger-than-RAM state sizes;
- fixed/variable key and value widths, cardinality, Zipf skew, batch rows, offered rate, and vnode
  count;
- aggregate, timer/window, and bounded two-input join operation mixes;
- p50/p95/p99/p99.9 end-to-end and state-service latency limits;
- maximum compute/event-loop stall, checkpoint freeze/tail/seal time, sink flush time, and RTO;
- hard RSS, local disk, FD, queue, frozen-generation, snapshot, compaction-debt, and write
  amplification limits; and
- repetition count, warm-up, fixed operation count, and invalid-run policy.

Numerical fields cannot be `TBD` in a qualification run and cannot be changed after candidate
results are visible. If production targets are not yet known, first measure the existing
stateless/global baseline; those measurements inform an owner decision but do not set the gate
automatically.

### B2. Typed partition ABI

Freeze the existing ABI v1 rather than introduce another encoder: Arrow row bytes using default
sort fields, xxh3-64 with the existing seed, then modulo the nonzero vnode count. Put construction,
the supported-type gate, encoding, hashing, and vnode mapping behind one vectorized codec shared by
shuffle and state. ABI v1 admits the explicitly enumerated scalar types and integer-indexed
dictionaries over those types; it rejects floats (therefore NaN and signed-zero semantics), nested
types, and run-end encoding. Cluster planning must reject an unsupported key before data reaches
the hot path even where embedded mode can group it.

Golden vectors record exact encoded bytes, the full 64-bit hash, and vnode result for nulls,
booleans, every signed/unsigned integer width and boundary, decimal precision/scale, UTF-8/binary
representations, timestamp unit/timezone metadata, dictionary hydration, and composite boundary
cases. A handwritten schema descriptor records arity/order and all type parameters; neither
`DataType::to_string` nor a vnode-only vector is an ABI proof. Persist the encoding/hash version,
schema fingerprint, vnode count, operator/table identity, and state schema version together.

Every base and delta restore artifact retains its declared vnode. Before any live mutation, decode
and validate the complete chain against the planned key schema, require every group and
`last_emitted` key to hash to that vnode, and require global state to use vnode 0. Any changed bytes
or widened type semantics require an ABI version bump plus an explicit replay/migration and
rollback policy; there is no implicit N/N-1 reader window.

The keyed checkpoint format must gain a versioned envelope containing partition ABI, vnode count,
claimed vnode, canonical key-schema fingerprint, operator fingerprint, accumulator-state schema
fingerprint, and payload version. Preflight uses expected key and accumulator schemas cached from
the plan, never schemas inferred only from the payload. It rejects empty state for an accumulator
that declares state fields, coercive or empty keyed emission keys, a changed vnode count, and any
decoded group/byte reservation above the frozen limits. A FULL image replaces the complete target
vnode namespace, including removal of live keys absent from the image.

Represent one uninitialized restore as tagged base-plus-delta chains rather than separate flat
vectors. Prepare all chains, prove cross-vnode key disjointness, and reserve resources before any
chain mutates the temporary aggregate; publish that aggregate only after every preflight succeeds.
The later graph-level lifecycle must likewise prevent a failed late operator from exposing earlier
partial application. Keep legacy raw checkpoint compatibility limited to the admitted global
vnode-0 aggregate until an explicit keyed migration policy exists.

### B3. Delivery scenario

Freeze the first vertical as:

```text
Kafka splittable/replayable source, fixed topics/partitions and replay start
  -> grouped COUNT(*) plus SUM append-result snapshots
  -> Kafka durable at-least-once multiwriter envelope=append sink
  -> shared object-store checkpoint authority
```

The oracle permits bit-identical external replay duplicates but rejects missing input, internal
double application, impossible aggregate state, stale-owner output, or durable/external source
progress published before sink flush succeeds. Capturing a source position before the flush is
expected. FullChangelog, mutable-key sinks, MVs, and exactly-once remain outside this vertical.

The output is a repeated full running snapshot of every group on each processing cycle, not a
monotonic append aggregate or changed-group stream. `COUNT(*)` is mandatory because it supplies a
batching-independent logical state version for the narrow COUNT/SUM identity; `SUM` is initially
limited to exact integer/decimal input and result semantics. The producer must route every logical
group to exactly one fixed Kafka input partition so its broker offsets define group-local order.
Freeze the input and run-specific output topic inventory, partition counts, explicit replay
offsets, canonical group-key partitioning, `acks=all`, replication/min-ISR, unclean-election, DLQ,
and evidence-retention settings as part of the contract.

Do not certify the path from connector flags or Kafka producer idempotence alone. Current records
lack replay-stable operation identity and ownership provenance. Before the scenario is eligible,
add golden-tested `operation_id_v1` derived from domain, deployment/pipeline/operator identities,
canonical group key, and count state version; carry its separate canonical payload digest, vnode
and partition ABI, assignment version, node ID, boot UUID, and process term. Excluding the payload
from the identity makes two payloads for one logical state version a detectable conflict. The same
ID and payload is a legal replay; the same ID with a different payload is a failure. “Stale” means
work computed or admitted after the writer's authority was fenced. A previously admitted in-flight
append arriving later is evaluated under the ordinary at-least-once duplicate rules.
Checkpoint-attempt identity is not stable across source replay.

The expected checkpoint order is source position capture followed by a FIFO sink synchronization
fence and successful durable flush before manifest readiness, durable decision, and external source
progress notification. The asynchronous tail may overflush post-cut output, which may replay. A
cross-layer test must block and fail sink flush to prove that no durable decision or source
notification escapes early.

## Workstream C — Evidence-only LSM qualification

Create a standalone, unpublished tool at `tools/state-backend-qual` with its own workspace and
committed lockfile. The root workspace and `crates/**` must gain no candidate dependency during the
spike.

### C1. Backend-neutral model first

The first tool commit contains only:

- a validated profile format with mandatory numerical gates;
- deterministic counter-seeded aggregate, timer/window, and join request generation;
- Arrow-batch-sized logical multi-read and atomic mutation batches;
- an in-memory semantic model and digest oracle; and
- structured run identity and result output.

It contains no Fjall or RocksDB adapter and no CI workflow. Tests prove profile rejection,
deterministic request bytes, batch atomicity, and model results.

### C2. Candidate adapters

Add exact, optional candidate pins in separate commits: Fjall 3.1.8 and one reviewed RocksDB Rust
binding/version. Build exactly one candidate per binary. The private qualification contract covers
batched reads, atomic write/delete/timer mutations, bounded range scans, consistent snapshots,
vnode cleanup, sorted restore, explicit crash persistence, and resource/operability statistics. It
is not the future production trait.

Run identical fixed-operation workloads, alternate candidate order across repetitions, and record
service latency separately from queue latency. Required outputs include raw p50/p90/p99/p99.9/max,
throughput, CPU, RSS/PSS, cache/memtable/journal/compaction pressure, physical writes, disk/FD use,
snapshot/export overlap, restore/cleanup RTO, oracle digest, binary/lock/profile hashes, and target
hardware identity.

### C3. Fault and endurance gates

For each candidate, exercise kill during atomic write, snapshot/export, restore, and cleanup;
explicit persistence recovery; corruption/truncation; wrong identity/schema; concurrent open; FD
pressure; scoped Linux `ENOSPC`; complete local loss plus portable restore; and N/N-1 behavior. A
24–72-hour backend churn/TTL soak measures compaction and resource slopes but is not the independent
product soak.

Select one backend in a reviewed report, delete the rejected adapter/dependency, and preserve raw
evidence by immutable URI and digest. Phase 1 may start only after this decision.

## Workstream D — Independent production-soak contract

The detailed charter is [distributed-state production soak](../testing/distributed-state-production-soak-charter.md).
Phase 0 freezes its identities, numerical gates, scenarios, oracle, fault schedule, artifact set,
invalid-run rules, and independent reviewer before implementation results can influence them.

The final soak must consume an immutable release archive or OCI digest and may not build the SUT.
Its oracle has no LaminarDB library dependency and derives expected results from a broker-acknowledged
input ledger plus frozen source/sink cuts. Every failed and invalid attempt is retained; retrying
until green is prohibited. A relevant binary, chart, configuration, charter, or oracle change
requires a complete rerun.

The current Dockerfile toolchain mismatch (`rust:1.93` versus workspace `rust-version = 1.95`) must
be resolved before an OCI artifact can be eligible. This is a release prerequisite, not permission
for a drive-by Docker change in the capability-inventory commit.

## Commit sequence

1. `docs: detail phase zero keyed-state execution`
   - this plan and draft independent-soak charter;
2. `refactor: inventory operator cluster capabilities`
   - mandatory descriptor, all implementations, focused tests, no admission consumer;
3. `test: freeze keyed-state workload and ABI gates`
   - approved numerical profile, typed key vectors, delivery/fault oracle contract;
4. `tools: define state backend qualification model`
   - standalone model-only harness;
5. `tools: qualify Fjall state backend candidate`;
6. `tools: qualify RocksDB state backend candidate`;
7. `test: exercise backend crash and resource gates`;
8. `docs: select managed-state backend from evidence`;
9. `tools: remove rejected state backend spike`; and
10. `docs: review distributed keyed state cycle 1`.

Each commit runs its affected feature matrix and keeps the worktree reviewable. Backend candidate
commits do not touch runtime crates. The capability commit does not change admission. The first
guard-removal commit is reserved for the later grouped-aggregate vertical after Phase 1 passes.

## Phase 0 exit review

The Cycle 1 reviewer must conclude `APPROVE`, `APPROVE WITH OWNED FOLLOW-UPS`, or `BLOCK` across the
six required passes. Any unowned correctness, numerical, backend, connector, independent-soak,
upgrade, or evidence-retention gap is `BLOCK` for Phase 1 and leaves `[LDB-4007]` unchanged.

Required attached evidence:

- named human owners and approvals for the target profile and soak charter;
- operator capability inventory and exact test commands;
- typed ABI vectors and compatibility/rollback decision;
- raw and summarized Fjall/RocksDB results with hashes and rejected-candidate rationale;
- fault/endurance results including every failed or invalid attempt;
- source/operator/sink ALO oracle specification;
- AI-slop, overengineering, unused-code, production-readiness, documentation, and test review; and
- explicit confirmation that no keyed/window/join/MV admission or exactly-once guarantee changed.
