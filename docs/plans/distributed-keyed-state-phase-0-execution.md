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

The later physical checkpoint contract is a second, positive proof. Queuing bytes while the SQL
operator is `Uninit` is not validation and cannot produce an activation token. Restore preparation
must construct enough of the exact incremental physical plan to select registered codecs and
decode private state. Contract derivation failure is recorded as cluster `Unavailable(reason)` and
keeps `[LDB-4007]`; it must not make an otherwise supported embedded aggregate fail merely because
the binary includes the cluster feature.

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
  amplification limits;
- hard encoded artifact/descriptor/payload bytes, chain-link/delta depth, operators/vnodes per
  transition, groups/accumulators/IPC streams/emitted rows, decoded Arrow variable bytes, and
  restore-staging limits; and
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
claimed vnode, explicit global/keyed mode, canonical routing-key schema, exact stored-key schema,
operator fingerprint, state-table/accumulator schema, payload codec, and `FULL`, `DELTA`, or
`EMPTY` kind. Preflight uses expected schemas cached from the plan, never schemas inferred only
from the payload. It rejects empty state for an accumulator that declares state fields, coercive or
empty keyed emission keys, a changed vnode count, and any decoded group/byte reservation above the
frozen limits. A FULL image replaces the complete target vnode namespace, including removal of
live keys absent from the image; a missing operator entry never means empty.

Freeze two schema layers deliberately. `PartitionKeySchemaV1` is the hydrated logical routing
identity; artifact codec v1 separately records exact physical non-dictionary Arrow fields and
nullability, hydrating dictionary inputs before writing. Accumulator contracts retain semantic
state fields and exact stored fields. Encoding must use planned fields rather than synthesize
all-nullable `c0...cN` fields. Before Arrow parsing, the decoder reserves from the global restore
budget, bounds and validates IPC framing/metadata/body/nodes/buffers, matches the cached schema
frame, and requires exactly `Schema -> RecordBatch -> EOS`, exact arity/type/nullability and row
count, and no casts/default state. Initial v1 rejects compression and dictionary messages. The
physical descriptor lands with the bounded DTO/writer/decoder and covers or rejects every relevant
metadata scope; routing ABI v1 rejects field metadata.

The aggregate codec registry is keyed by concrete reviewed builtin implementation, Laminar codec
ID, and explicit version—not by a spoofable UDAF name or a floating dependency version. Exact
DataFusion pins, fresh and populated state goldens, and explicit COUNT/SUM/AVG/MIN/MAX and
retractable invariants precede a writer. The persisted payload is a bounded wire DTO; direct rkyv
of the live checkpoint struct is not declared stable. Enforce artifact and decoded-size ceilings
before allocation/hashing and benchmark cold-path hashing/copying separately from the record path.
Cache the immutable contract at plan/init time: UDF introspection, schema canonicalization,
dependency-version formatting, SHA-256, IPC/rkyv parsing, and compatibility selection never run
per row or per processing batch, and post-freeze artifact encoding stays off the event-loop thread.

Represent restore as one assignment-scoped transition, not separate acquire/revoke maps or flat
payload vectors. It binds the exact committed cut and checkpoint assignment fence to the target
assignment version, vnode count, owner-map digest, acquired chains, revoked set, and authoritative
operator-lifecycle inventory. Snapshot rather than drain it; preflight all vnodes/operators;
prepare shadow state for every lifecycle participant; then enter exclusive graph/callback
publication with intake closed, revalidate assignment authority under the rotation fence, and
publish only infallible shard/generation swaps. Retain old handles for destruction after the short
section. A failed late prepare aborts all shadows, retains the identical transition, mutates no
live operator, and activates no vnode.
An uninitialized SQL operator must build and validate its exact incremental plan during prepare.
Keep legacy raw checkpoint compatibility limited to the admitted global vnode-0 aggregate until
an explicit keyed migration policy exists.

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

For Fjall, test consistency and power-loss durability separately: ordinary buffered writes versus
the proposed grouped `SyncData`/`SyncAll` boundary, retained slices, snapshot/iterator reclamation,
prefix cleanup without a range tombstone, and every stable pressure counter. For RocksDB, test the
chosen Rust binding's actual MultiGet behavior, DeleteRange tombstone/read cost,
cross-column-family stall propagation, rate-limiter scope, native memory accounting, and
SST-ingest write pauses. Pin the exact RocksDB engine and wrapper before results are accepted.

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

## Progress and remaining commit sequence

Completed Phase 0 slices now include the operator capability inventory, partition ABI v1 and its
bounded routing-schema identity, source/sink and output-identity contracts, independent-soak
charter and ineligible validator scaffold, plus aggregate/graph restore audits. None is an
admission consumer. A reviewed Cycle 3 experiment removed the generic strict IPC helper because
Arrow 57.2 can allocate from attacker-declared lengths before proving input availability; the
artifact-specific bounded decoder remains part of the next format-contract slice.

Remaining commits are kept reviewable in this order:

1. `test: freeze keyed-state numerical qualification profile`
   - named owner approvals and complete latency/resource/RTO gates;
2. `test: freeze aggregate artifact and codec contract`
   - dedicated bounded DTO, exact dependency pins, concrete builtin registry, semantic/physical
     schema goldens, artifact-specific hostile-input IPC preflight, authoritative roster/explicit-
     empty vectors, and no live restore wiring;
3. `tools: define state backend qualification model`
   - standalone backend-neutral model, deterministic workload, digest oracle, and validated output;
4. separate exact-pin Fjall and RocksDB adapter commits behind the private spike contract;
5. `test: exercise backend crash resource and endurance gates`;
6. `docs: select managed-state backend from evidence`;
7. `tools: remove rejected state backend spike`; and
8. `docs: review distributed keyed state phase zero`.

Each commit runs its affected feature matrix. Backend candidates do not touch runtime crates, and
the first graph-lifecycle implementation remains a Phase 1 change. The first guard-removal commit
is reserved for the later grouped-aggregate vertical after Phase 1 passes.

## Phase 0 exit review

The final Phase 0 reviewer must conclude `APPROVE`, `APPROVE WITH OWNED FOLLOW-UPS`, or `BLOCK`
across the six required passes. Any unowned correctness, numerical, backend, connector,
independent-soak, upgrade, or evidence-retention gap is `BLOCK` for Phase 1 and leaves
`[LDB-4007]` unchanged.

Required attached evidence:

- named human owners and approvals for the target profile and soak charter;
- operator capability inventory and exact test commands;
- typed ABI vectors and compatibility/rollback decision;
- raw and summarized Fjall/RocksDB results with hashes and rejected-candidate rationale;
- fault/endurance results including every failed or invalid attempt;
- source/operator/sink ALO oracle specification;
- AI-slop, overengineering, unused-code, production-readiness, documentation, and test review; and
- explicit confirmation that no keyed/window/join/MV admission or exactly-once guarantee changed.
