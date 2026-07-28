# Phase 0 execution plan: distributed keyed state

- **Status:** Certification/qualification work paused; backend-neutral core reference work resumed;
  stock official Fjall 3.1.8 is the preferred worker-local qualification-entry subject; no runtime
  dependency, backend selection, qualification evidence, independent production-soak result, or
  admission change
- **Started:** 2026-07-22
- **Last reconciled:** 2026-07-28 during Core Cycle 10
- **Parent plan:** [distributed keyed/stateful operators](distributed-keyed-stateful-operators.md)
- **Decision:** [ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md)
- **Baseline:** `1e2f8429`; working branch `feature/distributed-keyed-state-adr`

## Outcome

Phase 0 proves the contracts needed before any stock-Fjall working-state implementation can be
considered for production admission. Exact-source review finds the required atomic-batch, snapshot
and ordered-range primitive shapes but leaves concurrent/crash semantics, pressure bounds,
synchronous-call deadlines, maintenance/error health, prefix cleanup, tail latency and fault
behavior to prove. Backend-neutral
Laminar lifecycle/checkpoint, publication-boundary, resource-admission and truthful health-
composition work may continue without adding a backend dependency.
Cycle 20 does not split the existing Phase 0 review gate;
Phase 1 remains blocked until that gate completes or an accepted ADR/plan amendment defines a
smaller owner-approved entry gate. This phase does not add a state backend to the runtime, relax
`[LDB-4007]`, enable a materialized view, or change the cluster delivery guarantee.

The [2026-07-27 ADR reset](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#2026-07-27-workstream-reset)
pauses Cycle 69 and later certification work and authorizes only the private reference core slice.
It is not Phase 0 completion, backend qualification, or permission to execute a candidate.

The [2026-07-28 Fjall amendment](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#2026-07-28-fjall-318-priority-amendment)
supersedes the former TidesDB work order. TidesDB-specific T0/T1 and campaign text retained below is
historical Cycle 40/41 evidence unless the current numbered implementation order explicitly says
otherwise. It does not schedule a package wait, fork, adapter, or run.

Core Cycles 2–10 add a fail-closed runtime containment path for staged local vnode transitions and a
narrow audited, committed final-owner revoke path. Cycles 4–6 pin restore to the capsule-validated
seal, enforce immutable attempt identity and bounded chain traversal, and replace split staging with
one transition bound to exact checkpoint, assignment, process, pipeline, acquired, and revoked
authority. Cycle 7 initializes managed participants in embedded, single-node, and cluster modes and
makes the cached graph capability plus exact participant roster authoritative for capture, restore,
and revoke. Cycle 8 connects the SQL aggregate to prepare-all/abort-all, authority-fenced
unit-returning publication, and post-lock retirement.

Cycle 9 reserves aggregate capacity from checked net final live growth and adds immutable immediate-
parent/transitive raw-payload and artifact lineage to the current legacy path. It preflights the
requested subset before body reads, single-flights successful parent seal loads, proves exact parent
arithmetic and decoded base identity, and validates a verified-body receipt at immutable staging.
The outer wrapper is now `LDBVP3` version 3 with a 164-byte header and seals are version 8; older
V2/seal-7 cuts require an explicit reset until a migration bridge exists. These cycles do not impose
the production keyed budget or hold the acquired raw-body envelope as a transition-lifetime
reservation. Cycle 10 binds participant-agreed current-profile limits and exact metadata-verified
cluster totals into capsule v6 before Commit and reproduces them before restore body reads. Complete
wrapper/request/spool/decode/RSS/pause limits remain open; these cycles do not consume the proposed
managed V2 format, add a hot-state backend, authorize a stateful query, or change delivery.

The phase is complete only when maintainers can answer, with versioned evidence:

1. which physical operators retain state and what distribution model each requires;
2. which key/state ABI is durable across restore, rescale, and rolling upgrade;
3. what numerical workload, latency, resource, checkpoint, and RTO limits define success;
4. which disk-backed working-state backend passed the intended general local-spill workload and
   fault contract; the in-memory subject remains reference/conformance-only;
5. which source/operator/output/sink scenario is being certified at-least-once; and
6. how an independent black-box soak will prevent an unearned production-ready claim.

The Cycle 20 [working-state placement analysis](../reports/state-working-state-options-2026-07-24.md)
supports that product decision; ADR-008 and this plan retain decision and sequencing authority. A
local database is capacity and latency infrastructure, not checkpoint, ownership or exactly-once
authority.

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
- hard RSS, local disk, FD, queue, frozen-generation, snapshot, applicable background-maintenance,
  and write
  amplification limits;
- hard encoded artifact/descriptor/payload bytes, total resolved parent links, operators/vnodes per
  transition, groups/accumulators/rows, canonical key/state bytes, output buffering, and
  restore-staging limits; and
- repetition count, warm-up, fixed operation count, and invalid-run policy.

Numerical fields cannot be `TBD` in a qualification run and cannot be changed after candidate
results are visible. If production targets are not yet known, first measure the existing
stateless/global baseline; those measurements inform an owner decision but do not set the gate
automatically.

The machine-readable [`linux-nvme-v3` candidate](../../tools/state-backend-qual/profiles/linux-nvme-v3.candidate.json)
is the current sole source for proposed numerical gates. Its evidence-ownership map assigns backend,
artifact-conformance, and product-integration sections to different executors, so an LSM run cannot
claim sink/checkpoint/failover gates. Its validator deliberately reports
`VALID_INELIGIBLE_PROFILE`: workload/operations owners and an immutable image/package identity are
unset, no candidate has run, and it is not qualification evidence. The source/object-store/sink
  deployment profile remains a separate required part of the product scenario and independent soak;
  an LSM result cannot certify those boundaries. The v1 and v2 profiles remain immutable
  validator/model regression fixtures and are not eligible for a new runner plan.

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
schema fingerprint, vnode count, operator/state-table identity, and state schema version together.

Every base and delta restore artifact retains its declared vnode. Before any live mutation, decode
and validate the complete chain against the planned key schema, require every canonical group key
to hash to that vnode, and require global state to use vnode 0. Key bytes remain opaque unless a
separately reviewed, panic-free decoder validates them; untrusted bytes must never be passed to
Arrow `RowParser`. Any changed bytes or widened type semantics require an ABI version bump plus an
explicit replay/migration and rollback policy; there is no implicit N/N-1 reader window.

The keyed checkpoint format must gain a manually parsed, allocation-bounded envelope containing
partition ABI, vnode count and claimed vnode, explicit global/keyed mode, canonical routing-key
schema, stable operator/state-table identities, state contract, payload codec, checkpoint ancestry and
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
`1 <= COUNT <= i64::MAX`, non-null count no greater than COUNT, equality between those counts when
the cached contract marks the SUM input non-nullable, and canonical zero/SQL NULL when the non-null
count is zero; otherwise the exact signed `i64` accumulator is the nullable `Int64` result. This
checked Laminar implementation must also execute the
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
unique operator/state-table/vnode roster uses `BODY` for a checked FULL/DELTA/EMPTY slice and
`REFERENCE` for an exact unchanged parent entry. Absence is corruption and REFERENCE is never
treated as EMPTY. BODY ranges are in bounds, non-overlapping, and exactly cover the unpadded body
region. Legacy v1 requires manifest/type proof of the admitted global vnode-0 path. Cache the
immutable contract at plan/init time;
introspection, canonicalization, dependency selection, SHA-256, rkyv/format parsing, sorting, and
post-freeze encoding never run per row or per processing batch.

Core Cycle 9's V3 wrapper lineage around the unchanged legacy raw-rkyv body is not this future
allocation-bounded managed keyed envelope or its manifest selector. It contains the current restore
path while managed V2 composition and admission remain closed.

Cycle 5 freezes the normative layout and goldens with private, admission-neutral borrowed aggregate
and outer-structural `VnodePartialV2` readers plus full-buffer fixture encoders. Core Cycle 1 promotes
only the inner aggregate encoder into private release code for the reference vnode shard; the outer
encoder remains test-only. The 160-byte V2 header hashes the
directory; each BODY entry hashes its exact slice, so there is no redundant whole-body hash. The
outer reader does not authenticate the object or validate aggregate semantics. Production restore
must first match the complete payload to the trusted seal/inventory digest, select the format from
the manifest, and run every BODY through its expected inner reader while sharing the object budget.
Production managed streaming writing, managed-V2 bounded fetch and chain resolution, shadow
ingestion, and publication remain future work. Neither private codec is an admission consumer.

From the trusted checkpoint pointer, obtain the inventory object's encoded length and expected
digest. Reject a length above `transition_metadata_bytes_max`, acquire the global encoded-byte
charge, then stream the inventory to the exact cap before parsing it. This remains future managed-
format work. Cycle 9 preflights only the current path's requested transitive raw-payload/artifact
lineage; it does not charge inventory/wrapper/seal bytes, an aggregate object/request count, or
decoder scratch. The verified managed declarations must bind artifact lengths/digests, provenance,
and decoder dispatch; each artifact repeats reserve-before-GET and exact-length/digest enforcement.
Per-artifact/per-chain encoded caps, the per-artifact directory-entry cap, the global encoded pool,
and per-task/global decoder/ingestion scratch are distinct candidate-profile fields. "Per artifact"
means one complete raw `VnodePartialV2` payload, excluding the current 164-byte V3 provenance
wrapper: rows, key bytes, state bytes, and encoded bytes are
summed across its BODY entries rather than reset for each inner payload. Every aggregate BODY decode
must consume one caller-owned, non-`Copy` mutable `AggregateObjectBudget` ledger for the whole V2
object. Wrapper plus payload is checked before fetch. The resolved-parent limit counts both outer
REFERENCE and inner DELTA edges.

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
Keep legacy raw body compatibility limited to the admitted global vnode-0 aggregate until an
explicit keyed migration policy exists. The V3 wrapper and seal-8 change is nevertheless an
incompatible durable reset boundary; no rolling-upgrade claim follows from the unchanged body.

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
Versions increase within a writer-authority interval. Recovery from an older coordinator-admitted
cut with a durable terminal Commit may append lower legal prefixes after an uncommitted higher
version that reached Kafka under the prior interval;
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
pipeline incarnation, canonical group key, and count state version. Its compact data header carries
only envelope version/kind, operation ID, writer-interval ID, and checked admission sequence. The
serialized Kafka payload bytes are authoritative; the independent reader computes their SHA-256,
so payload digest, vnode/ABI, assignment, and process provenance are not repeated on every row.
Excluding payload and checkpoint attempt from the identity makes two byte payloads for one logical
state version a detectable conflict across replay. The same ID and bytes is only a byte-consistent
replay candidate; it is legal only when the resolved interval marker and recovery cut permit it. The
same ID with different bytes is a failure. “Stale” means work computed or admitted after the writer's
authority was fenced. A predecessor transaction committed before the successor's partition marker
remains an ordinary at-least-once record even if its acknowledgement arrives later; fencing aborts
any still-open transaction, so it is invisible. Ordinary recovery retains pipeline incarnation; an
intentional rewind/recreate changes it.

Metadata cannot prove whether admission preceded a fence. Each output therefore carries a
writer-interval ID and a sink-admission sequence that starts at zero and strictly increases within
each `(sink-writer shard, writer interval)`, and the certified sink supplies an external fence cut.
Before the first interval, open sources only far enough to resolve exact partition inventory and
numeric exclusive start baselines. Keep readiness, source delivery, graph execution, and sink-write
admission closed; commit a zero-input bootstrap checkpoint/capsule with those baselines, empty state/
timers, pipeline identity, and assignment certificate. The unactivated sink may satisfy that
checkpoint's flush only after proving that no output was computed, queued, or accepted. The first
marker has `predecessor = none`, references the bootstrap authority, and must commit before data
admission opens. A source unable to expose a checkpointable baseline before delivery stays closed.
The Kafka candidate derives a
stable transactional ID from deployment, pipeline incarnation, sink, and bounded writer shard. The
successor initializes it to broker-fence the predecessor, then commits deterministic
predecessor/successor markers to all affected output partitions in one confirmed transaction before
admitting data. Each marker carries deployment; pipeline incarnation and identity; operator/output
and sink identity; partition ABI; sink shard and owned vnode set; assignment certificate/digest;
owner node, boot incarnation, and durable process term; predecessor/successor interval IDs; and the
exact recovery-base `{epoch, checkpoint_id}` plus recovery-capsule digest. The oracle resolves that
reference through the immutable checkpoint-evidence view, derives each row's expected vnode from
its canonical key and the frozen ABI, and verifies marker ownership before classifying replay. Every
output record then uses transactions from that fenced producer. An ambiguous marker commit kills
that writer; a new interval fences it before retry. A read-committed oracle rejects any old-interval
row after the marker and any replay causally before the resolved sealed cut; the fault suite brackets
producer initialization and marker commit. This is provider-enforced stale-writer exclusion, not
exactly-once; source cursor/state and Kafka transaction remain separate commits. Plain multiwriter
append without this proof stays closed.

Failure before bootstrap Commit retries startup. Failure after Commit but before a confirmed marker
restores the bootstrap cut and creates/fences a new interval against it. An ambiguous marker commit
terminates the writer; its successor fences it and references that same cut. The one-time bootstrap
and initial-marker latency is part of startup/RTO qualification, not hidden from the profile.

The input oracle likewise cannot use acknowledgement callbacks as its ledger: Kafka may persist a
record whose acknowledgement is lost. The producer durably records stable intents, then the
controller reads and reconciles every actual input record through the frozen partition cuts. The
model consumes that broker-derived ledger, including physical retries, and rejects unknown or
conflicting records.

Cycle 50's implementation audit freezes the dependency order and prevents connector-first work:

1. extend the execution-ineligible independent semantic fixture with writer intervals, checked
   sequences, ownership/shard checks, predecessor/successor marker rules, and an exact marker-to-
   recovery-base/capsule binding over its existing source cuts, including the zero-input bootstrap
   base and `predecessor = none` first interval;
2. freeze compact data-header and marker bytes, version/cap limits, and hostile decoding;
3. prove operation identity and assignment-authority propagation with pure tests;
4. prove initialization, marker-before-data, ambiguous-commit failure, batching, and overflow with a
   fake transactional-producer state machine;
5. prove broker fencing, atomic marker/data visibility, aborted predecessor invisibility, and
   `read_committed` behavior against real Kafka/Redpanda; and
6. only then wire the three-node engineering harness. The independent release-binary soak remains a
   later gate.

Cycles 51 through 53 complete items 1 through 3. Cycle 54 implements only the validation-model
subset of item 4. Fixture v1 remains unchanged, while explicit v2 stays
synthetic, root-workspace-excluded, and `certification_eligible=false`. Its positive case covers a
zero-input bootstrap, a later recovery cut, assignment 7 to 8 ownership change, cross-interval
byte-identical replay at the raw source offset equal to that cut, predecessor output before the
successor marker, sequence gaps, and exact final grouped state. Mutation tests distinguish absent/
incomplete evidence (`RUN_INVALID`) from complete conflicting product output (`PRODUCT_FAIL`). The
fixture now consumes the frozen standalone data-header and marker envelopes and rejects malformed,
noncanonical, unknown, or semantically contradictory bytes. The sole normative layout is in
[ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#distributed-output-envelope-v1);
no Kafka connector or runtime code uses it. Cycle 53 adds the sole grouped `COUNT(*)`/`SUM(Int64)`
[operation-ID definition](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#grouped-countsum-operation-identity-v1),
independently derives every v2 data ID from explicit ABI-v1 group bytes, and pure-tests projection
from a current assignment certificate plus complete owner/participant view, separate live process
lease, and immutable recovery Commit. The standalone projection independently recomputes the full
certificate digest. Pipeline incarnation and interval allocation still have no production
lifecycle. Cycle 54 adds a unit-test-only synchronous writer model around that projection. It
requires a confirmed marker over every supplied affected partition before data, plans but does not
hide transaction splits,
checks one global sequence range through `u64::MAX`, retries only confirmed-aborted attempts inside
the same borrowed call, and poisons every ambiguous phase. It neither selects the correct successor
after an ambiguous marker, nor proves a complete broker partition inventory, nor rejects interval
reuse across fake chains/restarts.

Cycle 55 freezes
[`transactional_id_v1`](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#kafka-transactional-identity-v1-and-real-broker-evidence-boundary)
and exercises the deterministic subset of item 5 with a root-workspace-excluded synchronous
`rdkafka` probe against the repository's one-node Redpanda service. On a unique RF=1 topic it proves
the exact pre/post partition inventory `[0,1,2]`, byte-identical marker fanout, confirmed abort plus
byte-identical retry, fatal same-ID predecessor fencing, and separate `read_uncommitted`/
`read_committed` visibility. It also verifies the exact reserved header, an unrelated preserved
header, marker null key and empty non-null payload, unchanged data key/payload, predecessor-visible
data, and interval-reset replay. This closes neither a production topology nor item 5 as a whole.
No protocol-aware actuator dropped a matched `EndTxn` response after request delivery, so
ambiguous-marker/data reconciliation remains the next item-5 slice. Broker size/timeout/pressure,
replication/failover, TLS/auth, runtime interval non-reuse, item 6, latency, and the independent soak
remain open.

Cycle 56 timeboxes that slice around the ADR's validation-only matched-`EndTxn` contract. The exact
pinned client negotiates non-flexible `EndTxn` v1, allowing one bounded Rust TCP actuator to retain
and correlate only the selected request/response while forwarding complete non-target frames during
the active lifecycle. The host exposes the broker origin at loopback port 19194 while the broker
advertises the actuator at 19192, so metadata and coordinator reconnects cannot bypass it. Existing
proxy frameworks remain out of scope because none supplies this exact fault without a custom
extension and substantially larger codec/routing/runtime dependencies.

Implementation order is fail-closed: parser/state-machine and fake-socket tests; route-inventory
preflight; then four unique-topic real-broker cases for `{marker,data} x {applied,unapplied}`. The
applied case requires a full same-connection/correlation success response withheld byte-for-byte;
the unapplied case requires a full request and zero upstream target bytes. Both require a retriable
caller timeout, target-producer connection retirement before same-ID successor initialization, and
separate RU/RC consumers reaching a frozen high-watermark cut on every partition. No three-node,
runtime, backend, limit, latency, or certification work starts if any part of that proof is absent.

The completed [Cycle 56 evidence](../reviews/distributed-keyed-state-cycle-56.md) passes all four
isolated cases on the frozen one-node PLAINTEXT subject. The applied actuator retained the exact
same-connection/correlation error-zero response and forwarded zero response bytes; the unapplied
actuator retained the complete request and forwarded zero request bytes. After every target-client
connection closed, the same-ID successor reconciled complete frozen cuts. Marker selection chose
the candidate only in the applied case. `read_committed` exposed the predecessor only in the
applied case and the successor replay in both cases; `read_uncommitted` retained both staged
branches. This closes the controlled single-broker ambiguity slice of item 5, not generic broker
or runtime correctness. Durable interval allocation/non-reuse,
item 6 integration, production topology/limits/security, replicated failover, source/state/sink
atomicity, hot-path/tail-latency evidence, a qualified backend, and independent soak remain open.

Cycle 57 completes only the stable-serving local-adoption projection and its admission-neutral
engineering consumer. Authenticated `GET /api/v1/cluster/local-evidence` returns a 4-KiB-capped,
cache-disabled current-process sample: node/boot/process term, exact boot-bound durable assignment
adoption, and no inferred recovery state. It reads no shared `/vnodes` head, requires the adoption to
match the locally audited assignment fence, and rechecks the process lease and fence after the one
bounded checked-KV current-slot operation. The three-node kill/rejoin harness compares every expected live
assignment participant with a durable-before/after assignment cut and requires a new boot plus
higher process term on restart.
Current recovery phase and exact
committed-`Release` consumption are not retained and are deliberately absent. Checkpoint outcome/
capsule projection, exact latency-attempt/max evidence, durable writer intervals, transactional
runtime integration, backend qualification, and independent soak all remain open.

The new convergence assertions passed in two real Windows/WSL2 engineering runs. The full tests did
not: the first deliberately short run lacked the required latency sample count, and the 90-second
rerun measured 98.81% rather than the required 99.00% of node1 checkpoint stalls within 1024 ms.
This result is retained; aggregate histograms cannot identify the exact violating attempts, so
exact attempt/timing projection is the next authority audit rather than a threshold adjustment.

The [Cycle 58 audit](../reports/checkpoint-attempt-evidence-audit-2026-07-26.md) added no route.
Cycle 59 implements its bounded process-local ledger for pipeline stall, local barrier, and aligned
resume plus the protected existing-harness consumer. Exact records are process- and assignment-
bound and loss-detecting; assignment-certificate authority covers sampled converged versions. They
are reconciled at coherent observed Prometheus cuts, streamed through each observed cut to
per-generation JSONL, and bounded in memory. A corrected one-kill engineering run at `7782a032`
reconciled 392 records across four process generations with no deadline exhaustion; the existing
pipeline-stall gate passed and every three-family diagnostic `le=1.024`
bucket contained 100% of its samples. The post-run `1a6dff80` defense has focused deterministic
coverage only. This was not an instrumentation A/B or an independent immutable release-binary soak.
Full-checkpoint/restorable-gate evidence and a non-creating, read-only same-snapshot durable audit
remain separate blockers.

Cycle 60 closes only the deterministic coherent-cut retry interleaving: metrics-only instability
and exact cursor/metadata-only instability independently retry before one stable finalization. It
adds no runtime code. A direct nonempty HTTP route test remains deliberately deferred rather than
adding a ledger injector or duplicate live-cluster fixture. The
[engineering A/B v1](../testing/distributed-state-production-soak-charter.md#cycle-60-instrumentation-ab-protocol)
now freezes separate recorder and polling contrasts with a fixed trace, time window, manual-
checkpoint fault ordinal, common metrics, and balanced order. It has not run, the current harness
cannot execute it without treatment-dependent control flow, and v1 cannot support equivalence.

Cycle 61 adds only the [chartered prebuilt-executable binding](../testing/distributed-state-production-soak-charter.md#cycle-61-executable-binding-seam)
to the existing engineering harness. It does not decouple diagnostics from control flow. No
standalone observer, A/B, or real-process soak was added or run.

Cycle 62 adds the isolated
[`tools/distributed-state-ab`](../../tools/distributed-state-ab/) schedule scaffold and no product
dependency. One raw-manifest-bound base plan fixes 104 driver actions and 58 observer slots. The
driver's private end seal exists only after the exact materialized trace is validated and
file-synced; observer outcome and retained capped output are consumed afterward. Ten Windows tests
pass with one ignored subprocess fixture, including identical C/D traces across success, exit,
hang, and malformed output and bounded kill/reap. This is logical schedule non-feedback evidence,
not execution of the opaque trace/control artifacts, a live observer, A/B, or a real-process soak.
Live work remains blocked until diagnostic reads can be delegated without also delegating the
console bearer's checkpoint/pipeline mutation authority.

Cycle 63 resolves that design choice without adding code. The
[diagnostic-read authority decision](../testing/distributed-state-production-soak-charter.md#cycle-63-diagnostic-read-authority-decision)
selects a disjoint, startup-bound, loopback-only server credential for exactly the two local
diagnostic GETs and rejects a broker holding the console bearer. Console fallback, query-token
authentication, implicit `HEAD`, CORS access, remote plaintext exposure, and live rotation are
prohibited for the observer; the console bearer retains administrator access to the GETs. Before
enabling that credential, the implementation must redact substituted TOML input
from parse errors and make both reload paths retain all restart-only active configuration rather
than republishing changed server secrets. The bounded implementation cycle includes the auth policy,
router split, and exhaustive route/config/reload tests only; the observer client and any live run
remain blocked. Since the main HTTP address is also advertised for checkpoint RPC, this loopback
slice cannot support a multi-host A/B or production soak; that later gate needs a separate local
diagnostic listener or native TLS/mTLS design.

Cycle 64 implements and verifies that slice in `3a0d3b5c` and `cf0f5aa4`. The shared validator enforces canonical
split credentials, cluster mode, and a loopback bind before startup side effects; an immutable
policy and typed principal isolate both diagnostic routes from every console mutation/read and
WebSocket route. One post-auth permit, fixed rolling window, and two-second deadline are local to
the diagnostic control plane. Substituted TOML input is stripped from parse errors, and the POST
and watcher paths now retain every restart-only value across pure and mixed successful/failed
reloads. Full no-cluster and cluster server tests pass (238 and 316), with warnings-denied Clippy in
both configurations. This authorizes only the next fake-server observer-protocol cycle: no live
request, A/B, backend package, admission, delivery, multi-host transport, or soak gate changed.

Cycle 65 completes that standalone fake-server protocol component in `46b9c3fd` through
`553c933d`, still outside the root workspace and product runtime. It binds a canonical sanitized
plan and stdin-provisioned diagnostic authority to bounded direct loopback requests, validates
process/cursor/assignment history, rejects restart when the last observed timing page advertised
unread evidence, reports
incomplete collection explicitly, and supports bounded bootstrap and cancellation. Owned Windows
fake servers prove zero C connections and a complete 348-response D child. This is not a LaminarDB
HTTP sample: the tests own the loopback listeners, while the implementation enforces loopback but
cannot attest process identity. The sealed driver still consumes only its old dry-run observer, so
network-mode non-feedback and result consumption remain the next fake-only gate before any live
request. Backend qualification, delivery, admission, multi-host transport, A/B, and independent
soak status are unchanged. The fake path deliberately runs its 58 logical slots immediately; a
separately versioned paced integration path must honor `at_ns` and the server start-rate limit
before live polling can be authorized. Because the ledger is process-local, records appended after
the last poll and then lost at restart remain unknowable; live integration also needs durable
continuity/handoff or an explicitly reviewed bounded observation interpretation.

Cycle 66 connects only that accelerated fake protocol to the sealed driver in `4a95a3db`,
`76c43064`, and `8108966c`. A typed
endpoint/secret ingress, fresh UUIDv4 bootstrap/result binding, manifest-pinned child, bounded
capture/cancel/kill/reap, strict result equations, and post-end-seal-only consumption pass 38 tests.
C/D common plan and trace bytes remain identical and raw child streams are not persisted. This
closes the fake-only consuming-supervisor gate, not the live observer: `at_ns`, the server start-rate
limit, durable timing-tail authority, multi-host transport, effect estimation, powered A/B,
backend qualification, delivery, admission, and independent soak remain blocked.

Cycle 67 is the code-free paced/evidence gate. Its
[normative contract](../testing/distributed-state-production-soak-charter.md#cycle-67-paced-observer-and-evidence-decision)
requires distinct live schemas; pre-`t0` readiness and a bounded monotonic start acknowledgement;
three concurrent node lanes at absolute 0..285-second targets; absolute slot deadlines, no overlap
or catch-up; cross-slot rate shaping; ambiguous-delivery lane quarantine; and a complete slot vector
plus sealed transcript. It chooses
an explicitly open/unsealed process-local prefix claim, because abrupt restart can erase records,
loss metadata, or in-flight guards after the last poll. Exact continuity is deferred to a separate
durable-journal decision that must account for checkpoint-control-path latency.

This certification sequence is suspended. If explicitly resumed, work would proceed in this order:
(1) paced owned-fake implementation and deterministic plus one real-
time schedule test; (2) launcher-prebound loopback socket, trusted release-process descriptor, and
nonce-bound v2 response preflight, still not
an A/B sample; (3) engineering effect-estimation under Cycle 60 only after review; (4) a separate
provider-neutral diagnostics-only TLS 1.3 mTLS listener and hostile identity tests for multi-host;
and (5) powered equivalence and the independently operated production soak only after their own
frozen inputs. None authorizes candidate construction, a runtime backend dependency, keyed-state
admission, or delivery-guarantee change.

Cycle 68 completes only the control-primitives prefix of step (1), in `1b6a06ed`. The isolated tool
now has externally bound `paced-owned-fake` plan/READY fixtures, byte-golden START/ACK frames,
post-decode monotonic anchoring, 49/50/51-ms release and ACK classification, absolute 4.5/4.75-
second cuts, and atomic seven-start rolling admission. The full suite has 58 active passing tests
and one intentionally ignored subprocess fixture on Windows. No executable mode, evidence stream,
result, transcript, lane, HTTP interaction, or real-time 290-second test exists, so step (1) remains
open.

The former Cycle 69 scope is paused. A separately reauthorized cycle could add library-level
owned-fake evidence contracts: bounded sequenced framing,
supervisor-spool-compatible transcript validation, a complete ordered 174-node-slot result, checked
totals, and honest open/unsealed generation coverage. It must remain incapable of contacting
LaminarDB. Transport delivery-stage extraction, persistent lanes, child/supervisor integration, the
actual-limiter test, and the 290-second pair follow as separately reviewed gates rather than one
large rewrite.

The existing output path satisfies none of the new provenance/fence fields: it passes only a batch
and deadline and uses an idempotent, non-transactional Kafka producer. The supported evidence APIs
now include the three-family local barrier-pause ledger, but still need exact full-checkpoint/
restorable-gate evidence and, later, the same-snapshot immutable outcome/capsule projection. Raw
object-store records, provider-specific connector offsets, and tracing fields are implementation
evidence, not a frozen external API. These tasks do not depend on which local working-state engine
eventually qualifies.

The expected checkpoint order is source position capture followed by a FIFO sink synchronization
fence and successful durable flush before manifest readiness, durable decision, and external source
progress notification. The asynchronous tail may overflush post-cut output, which may replay. A
cross-layer test must block and fail sink flush to prove that no durable decision or source
notification escapes early.

## Workstream C — Evidence-only disk-backend qualification

Create a standalone, unpublished tool at `tools/state-backend-qual` with its own workspace and
committed lockfile. The root workspace and `crates/**` must gain no candidate dependency during the
spike.

### C1. Profile, then backend-neutral model

The first tool commit contains only the candidate profile, schema, and an ineligible-profile
validator. It has no backend, workload generator, result vocabulary, or candidate-execution
workflow. The next tool commit adds:

- deterministic counter-seeded aggregate, timer/window, and join request generation;
- profile-sized logical multi-read and atomic mutation batches;
- an in-memory semantic model and digest oracle; and
- structured deterministic model-replay identity and result output.

That second slice still contains no backend adapter and no candidate-execution workflow.
Tests prove deterministic request bytes, batch atomicity, model results, and rejection of malformed
profiles/results.

The C1 model is resource-bounded before payload construction: model-only width, batch, logical-row
work, per-request, and cumulative 64 MiB replay ceilings are normative in the protocol. Exact
aggregate and join accounting occurs after logical deduplication so the safety gate does not reject
a valid compact request merely because its raw input batch was wide.

The C1 reference implementation is complete against the provisional v1 contract. It contains
deterministic aggregate, timer/window, and join generation; bounded preflight; an in-memory
semantic and lifecycle oracle; occurrence-addressed lifecycle cuts; immutable snapshot export;
independently checked literal wire/result fixtures; strict deterministic result regeneration; and
a validation-only CLI. The CLI cannot execute a backend candidate. The v1 profile/protocol remain
historical provisional inputs, and every checked-in profile remains `qualification_eligible=false`.
Cycle 38 accepts runner v2 only as permission for validation implementation. C1 selects no backend, supplies no candidate
performance, resource, fault, endurance, checkpoint, source/sink, admission, or exactly-once
evidence, and changes neither `[LDB-4007]` nor `[LDB-0013]`.

The provisional C1 semantics, encoding, digest, result, and fault vocabulary are specified in
[state backend qualification model v1](../architecture-decisions/state-backend-qualification-model-v1.md).
Model/conformance and candidate-neutral validation scaffolding may be implemented because it cannot
produce qualification evidence. Contract acceptance alone never authorizes a candidate dependency,
source or native build, adapter, conformance run, benchmark, or execution command. Each needs the
separate candidate-specific source and exact-run authorities below; performance, resource, fault,
endurance, selection, and qualification execution remain prohibited until those gates close.
There is no implicit workload cross-product or pacing policy in C1.

### C2. Runner contract, then candidate adapters

The provisional candidate-neutral measurement and evidence policy is specified in
[state backend qualification runner v1](../architecture-decisions/state-backend-qualification-runner-v1.md).
Its strict plan schema always represents a complete, nonempty exact matrix; synthetic fixtures may
exercise validators, but no real plan is checked in while its DKS-Q2 blockers remain open. The
runner contract freezes open-loop pacing, raw latency samples, gate populations, resource formulas,
physical layout, attempt classification, fault identity, provenance, and immutable retention before
candidate observations exist. The CLI exposes validation only and has no candidate-execution
command.

Cycle 13 wires the previously isolated mechanism parsers through
`validate-mechanism-bundle`. Its content-addressed validation input is structurally fixed to a
synthetic, qualification-ineligible fixture and streams common resource samples/cuts plus applicable
debt, stall, and target-device artifacts through a fixed 64 KiB buffer. It enforces raw hashes,
population equality, canonical stall censoring, claimed clock/cut chronology, and the bounded
mechanism gates. A successfully validated bundle can report only a non-authoritative absence or
presence of adverse mechanism signals; malformed input instead remains invalid. Claimed write stop
and clock source are not derived or attested, and this is not the future runner-plan/approval
verifier. It cannot execute a candidate. DKS-Q2-005/006 therefore remain open.

Cycle 13 also detaches the existing Zipf feasibility literals into a record explicitly marked
non-independent and ineligible, adds Windows release coverage, and provisions native Linux arm64
debug/release coverage. The independent numerical corpus, MPFR/interval audit, retry proof,
interference evidence, workload/case registry, licensing record, and named-owner sampler decision
remain DKS-Q2-001 blockers; configured CI is not a passing target result until it runs.

Cycle 14 adds a bounded, standalone MPFR interval prototype without adding a candidate dependency
or execution command. Its 14 tests enforce fixed domains/grid points, explicit context and runtime
identity, outward rounding, precision escalation, caps, canonical bytes, and non-evidence flags.
Local Windows x86_64 and pinned Docker Linux x86_64 generate the same 865,397-byte observation and
SHA-256 `8ad14317bdb1f12d67b9f823bea0759d33034e4c01164c2dbac90ad870f2474b`. Required CI now exercises
that prototype on Linux x86_64, Windows x86_64, and native Linux arm64 and participates in
`ci-success`. The prototype does not consume candidate output or compute approved
distribution/rejection/retry metrics and has not been independently operated by the workload and
operations owners. It therefore closes no DKS-Q2 item, does not authorize a backend run, and cannot
be reused as production or soak evidence.

Cycle 15 publishes commit `1cc095bc` and runs the configured workflow as [CI run
30047503740](https://github.com/laminardb/laminardb/actions/runs/30047503740). On attempt 1, hosted
Linux x86_64, Windows x86_64, and native Linux arm64 pass the standalone oracle tests; the arm job
also passes the 111-test standalone-validator library suite with `zipf-feasibility` enabled in debug
and release. Attempt 1 fails only in an unchanged broad Windows recovery test after 3,240 other
tests pass. The exact test then passes 1,250 local Windows stress runs and attempt 2's complete
5,772-test Windows suite; aggregate `CI Success` passes. Attempt 2 reruns the failed and dependent
jobs, not the already-green target jobs. The local stress uses a pre-existing test binary. A
current-source integration-target build fails with Windows OS error 1455 when the host exhausts its
paging file; a reduced lib-only build then reaches its five-minute time limit without a result. The
pre-existing binary's SHA-256 is
`8330213baee1ab67fc1d38c96daf5ab6084a3ffdc9c559a35019a94585a49848`. The intermittent first
failure remains recorded and is not a fixed defect, soak, or qualification result. Hosted execution
closes only the prototype's configured-platform gap; all DKS-Q2-001 policy, numerical, provenance,
independent-operation, and candidate-comparison blockers remain.

redb 4.1.0 remains outside C2. Cycle 16 added a separately authorized, isolated
`construction-only-no-decision` workspace and CI lane; it cannot consume approval, classify a
prescreen, or contribute selection evidence. The optional
[bounded redb prescreen](../testing/state-backend-redb-prescreen-v1.md) is **PARKED after Cycle 34**;
this is an administrative stop, not a formal `DEFER` result. Its design timebox is exhausted, and
its descriptor-root schemas remain synthetic-only
regression shapes. No further protocol, provider, Docker, IPC, schema, collector, mechanism, or
adapter work is scheduled. It may reopen only through the protocol's one-page, two-engineering-day/
four-machine-hour, separately versioned micro-prescreen charter and separate candidate-execution
authority. A favorable observation merely funds mechanism/persistence mapping, an additive profile/
schema proposal, and adapter review; no prescreen artifact may satisfy or be pooled into C1/C2/C3.

**Historical candidate record:** The following Cycle 17–41 RocksDB/TidesDB sequence and its
TidesDB-specific facade/campaign details are retained as exact decision provenance. The Fjall
priority amendment and current implementation order below supersede their future imperatives.

Cycle 17 stopped the proposed RocksDB stall-only workspace at read-only source proof: the stall
observer appeared bounded but v1's debt arm required broader engine instrumentation/configuration
proof. That work remains historical v4/reference provenance and is not the active product track.
The Cycle 18
[maintenance-health v2 proposal](../architecture-decisions/state-backend-maintenance-health-v2-proposal.md)
replaces only that debt arm. Cycle 21 records the direction approval; Cycle 38 removes the proposed
protected-workflow ceremony, accepts the
[consolidated v2 validation contract](../architecture-decisions/state-backend-qualification-runner-v2-draft.md)
  for standalone validation-only implementation, and makes TidesDB the preferred local-spill product
  candidate instead of RocksDB. Cycle 40 records the official `tidesdb/tidesdb-rs` binding,
  published as Cargo package `tidesdb`, as the only selected integration line. Cycle 41 then stops
  exact v0.11.1 at T0 without changing that policy.
  The choice is not package validation, qualification, runtime admission, or a production claim.

Do not add a candidate dependency to a Laminar runtime crate, freeze successor profile/mapping
identities, add an adapter, or add a qualification command before the preceding gates pass and the
specific later scope is reviewed. Cycle 40 completes the bounded
[TidesDB package design](../architecture-decisions/tidesdb-local-state-successor-design.md) for exact
official Cargo package `tidesdb v0.11.1` and its native 9.3.6 source path. The build script's
`pkg-config` probe is unavoidable; accepting a system-library match is prohibited, so a future T1
must make the probe miss and capture link provenance. The design permits only a restricted
package facade: one database, one retained fixed prefixed CF, a dedicated bounded blocking lane,
copied values, transaction-scoped iterators, deterministic child-before-parent shutdown, and no
callbacks or package types crossing the facade. Private FFI, raw handles, package/native patches or
forks, native/system-library substitution, and unsafe workarounds are prohibited. Native existing-
directory state, checkpoint, and remote storage remain outside the safe product surface; the initial
  profile does not require unified WAL, strict native replay, or per-batch `FULL` acknowledgement.
Cycle 41 completed the bounded zero-machine-hour T0 with
`STOP_WAIT_FOR_UPSTREAM`: relevant later native correctness/memory-safety fixes are absent; the
package can acknowledge a short partial one-CF transaction; and the general cgroup envelope and
mandatory public maintenance-health facts remain unclosed. Restricted owner/lifetime containment
passes. A pre-output verified-commit/fail-stop protocol may later address only the short-batch gap
and must use a fresh verification transaction, check every distinct final key/delete, poison on any
ambiguity, and pass fault/p99.9/maximum gates. It cannot repair arena, flush-rotation, or iterator
defects. T1 is cancelled, no dependency/adapter/profile identity is added, and the exact evidence is in the
[T0 source closure](../reports/tidesdb-rs-t0-source-closure-2026-07-25.md). Only a new official Cargo
package followed by a complete T0 pass may fund T1. V4 is never reused or relabelled.
LaminarDB's `object_store` path remains provider-neutral transport for local, S3, GCS, and Azure
artifacts; native TidesDB filesystem/remote modes remain disabled, cluster recovery authority
requires an admitted cluster-shared `StateBackend`, and `file://` is node-local by default.

Cycle 19's [paper mappings](../reports/state-backend-maintenance-health-mapping-designs-2026-07-24.md)
  remain reviewed vocabulary and frozen Fjall/RocksDB reference provenance; they add no TidesDB profile
  or wire identity. TidesDB needs its own mapping only after the package correctness, containment,
  resource, and health closures above. Redb's candidate-specific profile translation, durability mapping, close
outcome and task/thread N/A proof are unscheduled while it is parked. Patch effort and interference
remain unknown, so this ordering is a gate sequence rather than qualification evidence.

Build exactly one
  admitted candidate per binary. The candidate qualification contract covers
batched reads, atomic write/delete/timer mutations, bounded range scans, consistent snapshots,
vnode cleanup, sorted restore, an explicit candidate-applicable persistence disposition, portable
fresh restore, and resource/operability statistics. It is not the future production trait.

For any later package-admitted TidesDB subject, test the exact official Rust/native pair and configuration,
one-CF logical ordering, exact-count batch results, pre/post visibility, fail-stop unknown outcomes,
immutable logical cuts, delete/cleanup cost, compaction and write-stall propagation, allocator and
page-cache accounting, cgroup pressure, checkpoint/export overlap, corruption, complete local loss,
  and cache loss. Process/cache-loss cases must prove that a prior native directory is never opened or
served and that portable export -> exclusive fresh root -> restore reproduces the logical digest.
The successor explicitly marks native persistence/reopen arms unsupported and replaces them with
  that portable-restore truth table. Pin the exact package, bundled engine, transitive build inputs, features,
and asynchronous WAL policy before results are accepted. Frozen Fjall/RocksDB v4 checks remain
regression/reference coverage only and cannot qualify TidesDB.

Only after a separate project-owner authorization binds the exact candidate, profile, complete
runner-plan hashes, target, isolation, limits, and cost, run the fixed-operation single-TidesDB
workloads in the frozen successor case order and record offered end-to-end, service, and queue
latency separately. Retain the exact raw-sample wire populations required by the
ultimately approved runner contract and derive
p50/p90/p95/p99/p99.9/max, throughput, CPU, RSS/PSS,
common external resource-v2 observations, conditionally applicable approved mechanism artifacts,
physical writes, disk/FD use, snapshot/export overlap, restore/cleanup RTO, distinct C1-oracle-
expected and TidesDB-actual roots/counters, binary/lock/profile hashes, and target hardware
identity. Candidate-native cache/memtable/journal/compaction fields exist only when a reviewed
mapping requires them; unsupported is never zero.

Every quantile and maximum named under a profile latency gate is enforced; p90 is retained as an
additional diagnostic. Candidate-local
snapshot/export/restore/cleanup timings are diagnostic primitive observations and cannot satisfy
the separately owned artifact-conformance, checkpoint, or recovery gates.

### C3. Fault and endurance gates

Before TidesDB can qualify for production, run a separately frozen shared-database concurrency
matrix; C2's
single-worker service evidence is necessary but insufficient. Use deterministic disjoint-vnode
lanes with a sequential oracle per lane, including a hot writer and latency-victim lane, concurrent
point/write/range traffic, and snapshot/export overlap. Gate victim and aggregate p99/p99.9/max,
global stalls, CPU/memory/I/O and resource tails. Barrier-addressed cases race normal operations
with restore activation, cleanup, and pinned-snapshot release while preserving the lifecycle-fence
oracle. TidesDB uses the frozen successor lane schedule, one-CF layout, seeds, and barriers. Any
optional reference comparison has a separately frozen paired order, is non-gating, and cannot change
the absolute TidesDB verdict.

For the admitted TidesDB subject, exercise kill during atomic write, snapshot/export, restore, and
cleanup; prove native process-death retention/reopen unsupported and portable fresh recovery
authoritative; force corruption/truncation, wrong identity/schema, concurrent open, FD pressure,
scoped Linux `ENOSPC`, complete local loss, and N/N-1 portable-format behavior. A
24–72-hour backend churn/TTL soak measures compaction and resource slopes but is not the independent
product soak.

Record the TidesDB pass or disqualification verdict in a reviewed report and preserve raw evidence
by immutable URI and digest. A failure removes any spike code and returns alternatives to an
explicit owner decision; it does not activate a fallback. Phase 1 may start only after a passing
verdict and every other Phase 0 gate.

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

The release Dockerfile now uses `rust:1.95-bookworm`, matching workspace `rust-version = 1.95`.
Release eligibility still requires the workflow to resolve and record the exact base-image digest,
build the immutable multi-architecture OCI artifact, and pass the release/soak gates; a matching tag
alone is not provenance or production evidence.

The current workstation's WSL/Docker lane is classified separately in the
[local capability report](../reports/wsl-docker-state-qualification-capability-2026-07-23.md). It may
run Linux correctness and process-crash smoke tests, but its overlayfs/ext4/VHDX/NTFS stack cannot
satisfy XFS project-quota, dedicated-device, NVMe latency, cache-loss, endurance, or independent-soak
evidence.

## Progress and remaining commit sequence

Completed Phase 0 slices now include the operator capability inventory, partition ABI v1 and its
bounded routing-schema identity, source/sink and output-identity contracts, independent-soak
charter and ineligible validator scaffold, aggregate/graph restore audits, and the provisional C1
model, generator, literal fixtures, deterministic result regeneration, lifecycle fault cuts, and
validation-only CLI. None is an admission consumer. A reviewed Cycle 3 experiment removed the
generic strict IPC helper because
Arrow 57.2 can allocate from attacker-declared lengths before proving input availability; the
initial aggregate artifact therefore uses the bounded Laminar row codec and wire contract frozen in
[managed state artifact format v1](../architecture-decisions/managed-state-artifact-format-v1.md).
Cycle 37 freezes the aggregate-v1 journal/checkpoint-transition contract and a private, disconnected
`BTreeMap` oracle nested under the existing artifact tests. Its literal vectors cover atomic PUT
coalescing, immutable capture/re-emission, Commit-admitted ancestry, aborted-attempt retention,
outcome-less allocated-ID gaps, pre/post-seal DecisionInDoubt, exact generation release,
deterministic ordering, and the existing aggregate/V2 codec seam. It is a reference model, not a
runtime implementation or performance result. Core Cycles 6–9 additionally land exact transition
authority, all-mode aggregate initialization, aggregate prepare/publication, and the current raw-
lineage reader/staging receipt. The managed artifact-v1 and `VnodePartialV2` readers remain unwired;
`[LDB-4007]` remains unchanged.

Remaining work is kept reviewable in this dependency order:

1. retain the Cycle 40 TidesDB selection and Cycle 41 T0 stop as historical evidence; no package
   wait, fork, PR, adapter, or qualification work remains scheduled for that line;
2. `docs: prioritize stock official Fjall 3.1.8 for bounded qualification-entry review` — the
   2026-07-28 amendment changes candidate priority without adding or executing the crate. Source
   review finds required KV primitive shapes and disproves a hard global write-buffer cap and hard
   journal cap; the one-engineer-day, zero-candidate-machine-hour source/contract closure remains;
3. complete the remaining backend-neutral Laminar gaps required regardless of the engine.
   Core Cycle 10 now metadata-traverses every required parent seal, verifies exact child-parent
   lineage before the pre-Commit cluster-global payload/artifact sum, persists participant-agreed
   limits and verified totals in capsule v6, and makes restore reproduce that contract before body
   reads. Next, make the acquired-subset raw budget a held transition reservation with bounded
   in-flight response/request permits and an absolute acquisition deadline/cancellation, and
   separately close wrapper/seal/request/spool/decode/RSS/pause reservations, vnode sharding, minimum truthful
   health signals, and a second state-family consumer. The outcome, publication,
   checkpoint/rebalance fencing, fresh-root, capability, and current raw-lineage authority already
   landed in Core Cycles 6–10 and must remain regression coverage rather than be reimplemented.
   Do not add or execute Fjall in this slice. The completed containment increments and their tests are summarized in the
   [validation report](../reports/cluster-keyed-state-validation-2026-07-22.md). Core Cycles 6–10
   complete the immutable transition, aggregate prepare/publication, and current raw-lineage
   authority subsets. The next backend-neutral slice is the held acquired-subset raw transition
   reservation; wrapper/seal/request/spool/decode/RSS/pause limits, vnode sharding, and a second
   state family remain open. The latest exact boundary is recorded in the
   [Cycle 10 review](../reviews/distributed-keyed-state-core-cycle-10.md);
4. use the one-engineer-day, zero-candidate-machine-hour read-only closure to prove truthful stable
   sources for every required Fjall pressure, progress, error, resource and fail-stop fact. Freeze
   the smallest testable entry contract only on a pass; the first unavailable internal fact or
   required fork stops the candidate;
5. only after that pass and under a separately reviewed implementation scope, build the smallest adapter/conformance
   vertical required to prove that contract in embedded, single-node, and cluster-with-admission-
   closed modes. Remove it if entry correctness/resource containment fails; do not build a generic
   runtime-selectable backend framework or speculative alternate adapters;
6. `docs: authorize an exact keyed-state qualification run`
   - the project owner may revise the candidate before explicitly authorizing exact thresholds, case
     matrix, Zipf sampler, runner source/build identity, target/isolation/limits/cost, and evidence
     rules. The current validator continues to accept only null approvals and
     `qualification_eligible=false` until that separate execution design lands;
7. `test: exercise Fjall uniform/Zipf aggregate, timer/window and join-family C2/C3 plus checkpoint,
   cleanup, rebalance, crash, resource and endurance gates` using only the authorized artifacts;
8. `docs: record the Fjall qualification verdict`; a hard failure or required fork disqualifies the
   target and returns alternatives to an explicit owner decision rather than silently activating
   one;
9. run the existing cluster failover, ALO and EO-eligible regression matrices, then complete the
   final code/unused-helper/document cleanup before an independently operated immutable RC soak; and
10. `docs: review distributed keyed state phase zero and authorize only the operator families whose
    full distribution/state/delivery evidence passes`.

The parked redb prescreen is not a prerequisite or active side branch in this numbered candidate
sequence. If a future bounded charter yields a favorable administrative recommendation, a later
explicit scope decision and additive profile/schema revision are still required before any redb
adapter commit.

Phase 1 tracks three temporary release dead-code allowances as **DKS-P1-001**. Owner:
distributed-state lifecycle implementation. Deadline: 2026-08-31 or the first applicable runtime
consumer, whichever comes first. The trusted manifest-selected outer-plus-inner consumer removes
the allowances in `aggregate_state/artifact_v1.rs` and `vnode_partial/v2.rs`; a real
cluster graph/lifecycle consumer removes the allowance in `aggregate_state/managed_v1.rs`, or that
reference module is removed. All three exits must land before any admission guard is relaxed. The
outer-directory fixture encoder remains test-only; the promoted inner reference encoder is not a
manifest-selected or production streaming writer.

Each commit runs its affected feature matrix. Backend candidates do not touch runtime crates before
the separately reviewed adapter step. The
[Cycle 10 review](../reviews/distributed-keyed-state-core-cycle-10.md) records the cumulative current
Phase 1 containment boundary; it neither completes Phase 1 nor bypasses the Phase 0 gates. The
first guard-removal commit is reserved for the later grouped-aggregate vertical after Phase 1
passes.

## Phase 0 exit review

The final Phase 0 reviewer must conclude `APPROVE`, `APPROVE WITH OWNED FOLLOW-UPS`, or `BLOCK`
across the six required passes. Any unowned correctness, numerical, backend, connector,
independent-soak, upgrade, or evidence-retention gap is `BLOCK` for Phase 1 and leaves
`[LDB-4007]` unchanged.

Required attached evidence:

- named human owners and approvals for the target profile and soak charter;
- operator capability inventory and exact test commands;
- typed ABI vectors and compatibility/rollback decision;
- raw and summarized results for every admitted candidate, with hashes and rejected-candidate
  rationale;
- fault/endurance results including every failed or invalid attempt;
- source/operator/sink ALO oracle specification;
- AI-slop, overengineering, unused-code, production-readiness, documentation, and test review; and
- explicit confirmation that no keyed/window/join/MV admission or exactly-once guarantee changed.
