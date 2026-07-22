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

The later physical checkpoint contract is a second, positive proof. Before artifact fetch, a pure,
fallible graph-construction phase builds and caches enough of the exact incremental physical plan to
select registered implementations/codecs and derive the immutable state contract. `Uninit` means no
working shards are installed and no data-plane callback may run; it does not mean the plan contract
is absent. Queuing bytes is not validation and cannot produce an activation token. Contract
derivation failure is recorded as cluster `Unavailable(reason)` and keeps `[LDB-4007]`; it must not
make an otherwise supported embedded aggregate fail merely because the binary includes the cluster
feature.

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
- hard encoded artifact/descriptor/payload bytes, total resolved parent links, operators/vnodes per
  transition, groups/accumulators/rows, canonical key/state bytes, output buffering, and
  restore-staging limits; and
- repetition count, warm-up, fixed operation count, and invalid-run policy.

Numerical fields cannot be `TBD` in a qualification run and cannot be changed after candidate
results are visible. If production targets are not yet known, first measure the existing
stateless/global baseline; those measurements inform an owner decision but do not set the gate
automatically.

The machine-readable [`linux-nvme-v1` candidate](../../tools/state-backend-qual/profiles/linux-nvme-v1.candidate.json)
is the sole source for proposed numerical gates. Its evidence-ownership map assigns backend,
artifact-conformance, and product-integration sections to different executors, so an LSM run cannot
claim sink/checkpoint/failover gates. Its validator deliberately reports
`VALID_INELIGIBLE_PROFILE`: workload/operations owners and an immutable image/package identity are
unset, no candidate has run, and it is not qualification evidence. The source/object-store/sink
deployment profile remains a separate required part of the product scenario and independent soak;
an LSM result cannot certify those boundaries.

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
and validate the complete chain against the planned key schema, require every canonical group key
to hash to that vnode, and require global state to use vnode 0. Key bytes remain opaque unless a
separately reviewed, panic-free decoder validates them; untrusted bytes must never be passed to
Arrow `RowParser`. Any changed bytes or widened type semantics require an ABI version bump plus an
explicit replay/migration and rollback policy; there is no implicit N/N-1 reader window.

The keyed checkpoint format must gain a manually parsed, allocation-bounded envelope containing
partition ABI, vnode count and claimed vnode, explicit global/keyed mode, canonical routing-key
schema, stable operator/table identities, state contract, payload codec, checkpoint ancestry and
assignment/owner-map provenance, exact section lengths and counts, digests, and `FULL`, `DELTA`, or
`EMPTY` kind. Preflight uses the immutable expected contract cached from the plan, never a contract
inferred from the payload. It rejects a changed vnode count, wrong ancestry/provenance, duplicate or
out-of-order keys, cross-vnode rows, trailing bytes, nonzero reserved fields, and any byte/count
reservation above the approved profile. A FULL image replaces the complete target vnode namespace,
including removal of live keys absent from the image; a missing operator entry never means empty.

The first managed aggregate payload is not Arrow IPC. It is a Laminar-owned sorted row format:
length-delimited canonical key bytes followed by fixed-width state selected by a cached contract.
Zero encoded key bytes remain valid for a nonempty ABI-v1 `Null` grouping schema; EMPTY is an
artifact kind, not an inference from key length.
The first candidate contract is append-only `COUNT(*)` plus `SUM(Int64)`: checked count and non-null
count plus a signed `i64` accumulator, with nullable `Int64` SQL output. The executor checks every
group-local input prefix in fixed source order, preflights the complete Arrow batch, then publishes
one atomic state/output mutation. Any count/SUM overflow, including in a late group, faults with no
part of that batch applied; split and coalesced replay therefore agree. Decode requires
`1 <= COUNT <= i64::MAX`, non-null count no greater than COUNT, and canonical zero/SQL NULL when it
is zero; otherwise the exact signed `i64` accumulator is the nullable `Int64` result. This checked
Laminar implementation must also execute the
exact embedded/reference shape before cluster admission; DataFusion 52.3's wrapping SUM and
release-dependent count overflow are not durable semantics. Decimal, unsigned, floating, AVG,
MIN/MAX, retraction, UDAF, and changelog codecs remain unavailable. Current `last_updated_ms` has no
aggregate semantic consumer and is omitted; changed-group append output needs no persisted
`last_emitted` value.

The aggregate codec registry is keyed by concrete reviewed implementation, Laminar codec ID, and
explicit version—not by a spoofable UDAF name or floating dependency. Fresh/populated state goldens,
arithmetic boundary tests, and deterministic bytes across insertion orders precede a writer.
Direct rkyv of the live checkpoint struct and hash-map order are not stable. The existing
`VnodePartial` rkyv container must be replaced for managed state by an allocation-bounded
`VnodePartialV2` directory selected by manifest magic/version with no decode fallback. Its sorted,
unique operator/table/vnode roster uses `BODY` for a checked FULL/DELTA/EMPTY slice and `REFERENCE`
for an exact unchanged parent entry. Absence is corruption and REFERENCE is never treated as EMPTY.
BODY ranges are in bounds, non-overlapping, and exactly cover the unpadded body region. Legacy v1
requires manifest/type proof of the admitted global vnode-0 path. Cache the immutable contract at
plan/init time;
introspection, canonicalization, dependency selection, SHA-256, rkyv/format parsing, sorting, and
post-freeze encoding never run per row or per processing batch.

From the trusted checkpoint pointer, obtain the inventory object's encoded length and expected
digest. Reject a length above `transition_metadata_bytes_max`, acquire the global encoded-byte
charge, then stream the inventory to the exact cap before parsing it. Its verified declarations bind
artifact lengths/digests, provenance, and decoder dispatch; each artifact repeats reserve-before-GET
and exact-length/digest enforcement. Per-artifact/per-chain encoded caps, the per-artifact
directory-entry cap, the global encoded pool, and per-task/global decoder/ingestion scratch are
distinct candidate-profile fields. "Per artifact" means one complete `VnodePartialV2` object: rows,
key bytes, state bytes, and encoded bytes are summed across its BODY entries rather than reset for
each inner payload. The resolved-parent limit counts both outer REFERENCE and inner DELTA edges.

Represent restore as one assignment-scoped transition, not separate acquire/revoke maps or flat
payload vectors. It binds the exact committed cut and checkpoint assignment fence to the target
assignment version, vnode count, owner-map digest, acquired chains, revoked set, and authoritative
operator-lifecycle inventory. Snapshot rather than drain it; reserve the inventory-declared spool
bytes against the local disk governor, then preflight every chain and row into a bounded immutable
spool before any operator callback; prepare shadow state for every
lifecycle participant from that spool; then enter exclusive graph/callback
publication with intake closed, revalidate assignment authority under the rotation fence, and
publish only infallible shard/generation swaps. Retain old handles for destruction after the short
section. A failed late prepare aborts all shadows, retains the identical transition, mutates no
live operator, and activates no vnode. Prepare uses the previously cached plan/codec contract only
to consume the validated spool into abortable shadow state; it cannot select a different decoder.
The transition digest namespaces the spool. Retry retains an atomically completed spool; restart
removes incomplete/unreferenced spools and rebuilds from remote authority; success or terminal
rejection reclaims it outside the ownership fence.
Keep legacy raw checkpoint compatibility limited to the admitted global vnode-0 aggregate until
an explicit keyed migration policy exists.

The initial positive plan proof admits exactly one `COUNT(*)`, one `SUM` over a direct `Int64` input
column, and one or more direct partition-ABI-v1 grouping columns. Aggregate FILTER/DISTINCT/order,
explicit null treatment, HAVING, derived group/aggregate expressions, multiple aggregates, and
retractions stay closed. Every upstream projection/filter must be replay-deterministic; processing
time, watermark-relative `now()`, random/volatile functions, AI calls, and unclassified UDFs fail
with `[LDB-4007]` before runtime.

### B3. Delivery scenario

Freeze the first vertical as:

```text
Kafka splittable/replayable source, fixed topics/partitions and replay start
  -> grouped COUNT(*) plus SUM(Int64) changed-group append snapshots
  -> externally fenced, transactional Kafka envelope=append sink (end-to-end at-least-once)
  -> shared object-store checkpoint authority
```

The oracle permits bit-identical external replay duplicates but rejects missing input, internal
double application, impossible aggregate state, stale-owner output, or durable/external source
progress published before sink flush succeeds. Capturing a source position before the flush is
expected. FullChangelog, mutable-key sinks, MVs, and exactly-once remain outside this vertical.

After each atomically applied input batch, output contains one current row per distinct group
touched by that batch. Rows for one group may be coalesced, so legal intermediate count versions may
be absent; output cost is proportional to changed input rather than total resident cardinality.
Versions increase within a writer-authority interval. Recovery from an older sealed cut may append
lower legal prefixes after an unsealed higher version that reached Kafka under the prior interval;
provenance must show the fence/recovery boundary, and the same version must retain one operation ID
and bit-identical payload. `COUNT(*)` is mandatory because its checked value is the
batching-independent logical state version; `SUM` is initially only nullable `Int64` with checked
Laminar arithmetic. The producer must route every logical group to exactly one fixed Kafka input
partition so its broker offsets define group-local order.
Freeze the input and run-specific output topic inventory, partition counts, explicit replay
offsets, canonical group-key partitioning, `acks=all`, replication/min-ISR, unclean-election, DLQ,
and evidence-retention settings as part of the contract.

Do not certify the path from connector flags or Kafka producer idempotence alone. Current records
lack replay-stable operation identity and ownership provenance. Before the scenario is eligible,
add golden-tested `operation_id_v1` derived from domain, deployment/pipeline/operator identities,
pipeline incarnation, canonical group key, and count state version; carry its separate canonical
payload digest, vnode and partition ABI, assignment version, node ID, boot UUID, and process term.
Excluding the payload
from the identity makes two payloads for one logical state version a detectable conflict. The same
ID and payload is a legal replay; the same ID with a different payload is a failure. “Stale” means
work computed or admitted after the writer's authority was fenced. A predecessor transaction
committed before the successor's partition marker remains an ordinary at-least-once record even if
its acknowledgement arrives later; fencing aborts any still-open transaction, so it is invisible.
Checkpoint-attempt identity is not stable across source replay. Ordinary recovery retains pipeline
incarnation; an intentional rewind/recreate changes it.

Metadata cannot prove whether admission preceded a fence. Each output therefore carries a
writer-interval ID and a sink-admission sequence that starts at zero and strictly increases within
each `(sink-writer shard, writer interval)`, and the certified sink supplies an external fence cut.
The Kafka candidate derives a
stable transactional ID from deployment, pipeline incarnation, sink, and bounded writer shard. The
successor initializes it to broker-fence the predecessor, then commits deterministic
predecessor/successor markers to all affected output partitions in one confirmed transaction before
admitting data. Every output record then uses transactions from that fenced producer. An ambiguous
marker commit kills that writer; a new interval fences it before retry. A read-committed oracle
rejects any old-interval row after the marker and the fault suite brackets
producer initialization and marker commit. This is provider-enforced stale-writer exclusion, not
exactly-once; source cursor/state and Kafka transaction remain separate commits. Plain multiwriter
append without this proof stays closed.

The input oracle likewise cannot use acknowledgement callbacks as its ledger: Kafka may persist a
record whose acknowledgement is lost. The producer durably records stable intents, then the
controller reads and reconciles every actual input record through the frozen partition cuts. The
model consumes that broker-derived ledger, including physical retries, and rejects unknown or
conflicting records.

The expected checkpoint order is source position capture followed by a FIFO sink synchronization
fence and successful durable flush before manifest readiness, durable decision, and external source
progress notification. The asynchronous tail may overflush post-cut output, which may replay. A
cross-layer test must block and fail sink flush to prove that no durable decision or source
notification escapes early.

## Workstream C — Evidence-only LSM qualification

Create a standalone, unpublished tool at `tools/state-backend-qual` with its own workspace and
committed lockfile. The root workspace and `crates/**` must gain no candidate dependency during the
spike.

### C1. Profile, then backend-neutral model

The first tool commit contains only the candidate profile, schema, and an ineligible-profile
validator. It has no backend, workload generator, result vocabulary, or candidate-execution
workflow. The next tool commit adds:

- deterministic counter-seeded aggregate, timer/window, and join request generation;
- Arrow-batch-sized logical multi-read and atomic mutation batches;
- an in-memory semantic model and digest oracle; and
- structured run identity and result output.

That second slice still contains no Fjall or RocksDB adapter and no candidate-execution workflow.
Tests prove deterministic request bytes, batch atomicity, model results, and rejection of malformed
profiles/results.

### C2. Candidate adapters

Add exact, optional candidate pins in separate commits: Fjall `=3.1.8` and RocksDB Rust wrapper
`=0.24.0` with its bundled RocksDB 10.4.2 engine. Build exactly one candidate per binary. The
private qualification contract covers
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
Its oracle has no LaminarDB library dependency and derives expected results from durable producer
intents reconciled against every actual broker record through frozen source cuts, plus frozen sink
cuts. Every failed and invalid attempt is retained; retrying
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
initial aggregate artifact is therefore specified to use a bounded Laminar row codec instead. Its
writer/decoder and the allocation-bounded outer `VnodePartialV2` directory remain future
admission-neutral slices.

Remaining commits are kept reviewable in this order:

1. `docs: approve keyed-state qualification profile`
   - named workload/operations owners approve the unchanged candidate thresholds and immutable
     runner identity; a separately reviewed approved-profile schema/status records signatures and
     candidate-profile hash. The current validator intentionally accepts only null approvals and
     `qualification_eligible=false` and cannot be edited in place after results exist;
2. `test: freeze aggregate artifact and codec contract`
   - dedicated bounded row DTO, concrete checked COUNT/SUM registry, semantic/state goldens,
     hostile-input preflight, authoritative roster plus BODY/REFERENCE/EMPTY vectors, and no live
     restore wiring;
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
