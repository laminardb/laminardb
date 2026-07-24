# ADR-008: Managed vnode-keyed working state for distributed operators

- **Status:** Proposed; implementation requires the Phase 0 review gate
- **Date:** 2026-07-22
- **Amended:** 2026-07-24; Cycle 16 carry-forward recommendation pending owner decision
- **Decision scope:** Cluster `CREATE STREAM` aggregates, windows, and joins
- **Related:** [validation report](../reports/cluster-keyed-state-validation-2026-07-22.md),
  [implementation plan](../plans/distributed-keyed-stateful-operators.md)

## Decision

LaminarDB will add a common, byte-governed, spillable **working-state service** for keyed
operators. Phase 0 originally scoped Fjall `=3.1.8` and RocksDB Rust wrapper `=0.24.0` (bundled
RocksDB 10.4.2) behind the same narrow service and workload/fault harness. The Cycle 16
[carry-forward matrix](../reports/state-backend-carry-forward-matrix-2026-07-24.md) recommends
RocksDB's bounded mechanism-closure task first and redb 4.1.0 only as a separate prescreen
contingency, pending the owner's carry-forward decision. Unmodified Fjall can re-enter only after
its DKS-Q2-006 patch. This recommendation is work allocation, not backend selection or candidate
admission. Any candidates admitted later still run the common campaign, which records one
production backend rather than maintaining multiple implementations. An in-memory implementation
remains a semantic reference and a small local-mode option. Cluster-shared object storage and the
existing `StateBackend` remain the
authoritative checkpoint/recovery layer; no local working-state backend is remote recovery
authority.

The [exact-source static audit](../reports/state-backend-static-audit-2026-07-23.md) finds that
unmodified Fjall 3.1.8 cannot supply the required stable compaction-debt/write-stall telemetry and
that the current RocksDB binding also lacks a complete stall signal. Both remain blocked before a
gate-bearing run. redb 4.1.0 was also screened and deferred before implementation: its sole
database-wide blocking writer plus not-yet-approved durability, cache, and telemetry mappings need
a cheaper writer/commit/recovery microprobe and contract decision before expanding the bake-off.
The proposed [bounded redb prescreen](../testing/state-backend-redb-prescreen-v1.md) is explicitly
non-gating and can only fund later candidate-mapping review; it cannot admit redb by itself.
SurrealKV 0.21.2 is rejected unmodified because its exact source breaks snapshot-registration
bookkeeping used by compaction and also has unresolved drain/telemetry risks; any reconsideration
requires a correctness fork and bounded prescreen before candidate admission. These are risk-based
scope decisions, not an unmeasured C3 failure or a selection by API checklist.

The existing fixed vnode ABI, bounded shuffle, assignment/process fencing, aligned barriers,
per-vnode checkpoint artifacts, and exact-attempt seal are retained. Cluster admission will move
from SQL-shape exclusions plus permissive operator hooks to a planner-certified distribution/state
descriptor. Each stateful operator must declare its partition key, stable state tables, timers,
retention, output mode, checkpoint schema, and acquire/revoke behavior.

The implementation order is grouped aggregates, fixed event-time windows, then bounded interval
joins. Stateful streams may be enabled before cluster materialized views. Cluster exactly-once is
a separate connector/commit problem and remains rejected by `[LDB-0013]`; the first keyed-state
release targets the currently advertised cluster at-least-once contract.

## Context

Cluster SQL admission is correctly fail-closed today. Stateless streams and one direct ungrouped
aggregate stage are admitted; keyed aggregates, all windowed aggregates, all joins, and all
materialized views are rejected with nested `[LDB-4007]`. Embedded mode has local implementations
for these operators.

The error description is not a complete diagnosis for aggregates. `SqlQueryOperator` already
shuffles aggregate keys and captures, restores, rebases, and revokes aggregate state by vnode. The
live groups nevertheless remain in operator-owned maps behind a group-count guard that cannot
bound variable-width keys, accumulator state, changelog output, dirty generations, or allocator
retention. Window-close and join operators additionally lack per-vnode timers/state and input
co-partitioning. See the validation report for the code and empirical evidence.

`laminar_core::state::StateBackend` is deliberately an artifact backend. It writes immutable
checkpoint-attempt vnode payloads, inventories them, and seals an exact attempt. It has no hot
get, scan, atomic batch, timer, spill, or generation-freeze API. Expanding that interface would
mix remote recovery authority with latency-sensitive working state and obscure both contracts.

LaminarDB has prior Fjall experience, but it is important not to overstate it. The v0.26-era
`state-tier` path introduced at `7b6ad7aa` stored demoted `(operator, vnode)` checkpoint slices and
later individual cold groups behind an asynchronous promotion worker. The current baseline
`1e2f8429` removed that feature and has no Fjall dependency in its workspace or lockfile. That cold
tier was rebuildable capacity, not the always-current managed working state designed here. Its
benchmarks and failure lessons are useful; restoring the deleted implementation would not close
the present lifecycle gap.

## Decision drivers

1. Preserve LaminarDB's low-latency Arrow-batch execution: no remote call, blocking LSM operation,
   future, transaction, or fsync per record. Cold I/O is coalesced at Arrow-batch granularity and
   runs off the compute/event-loop thread.
2. Bound memory and local disk before admitting previously unbounded SQL.
3. Reuse the checkpoint and ownership invariants already present rather than building a second
   coordinator.
4. Make operator distribution explicit and mechanically auditable.
5. Restore from portable, versioned per-vnode artifacts even after local disk loss or backend
   upgrades.
6. Keep checkpoint barrier interruption independent of total state size.
7. Enable one narrow operator vertical at a time, with fail-closed admission for everything else.

## Architecture

### 1. Planner-certified operator capability

Every physical streaming operator will expose one mandatory capability descriptor:

| Class | Meaning | Cluster rule |
|---|---|---|
| `Stateless` | No retained data influences a later batch | Admit when every input distribution requirement is satisfied |
| `GlobalSingleton` | Retained state is intentionally owned by vnode 0 | Admit only with singleton routing and the managed lifecycle or the existing certified global path |
| `VnodeKeyed` | State and output are owned by a canonical key's vnode | Admit only with certified exchanges, tables, timers, checkpoint, and ownership hooks |
| `RebuildableReplicated` | Read-only state can be reconstructed from a versioned source snapshot | Local/lookup-specific path; not a substitute for mutable keyed state |
| `LocalOnly` | Operator has local state but no distributed contract | Reject with `[LDB-4007]` and its declared reason |

A `VnodeKeyed` descriptor contains:

- the partition-key expression fingerprint and partitioning ABI version;
- stable operator and state-table identifiers;
- state key/value schema versions and compatibility readers;
- input exchange requirements and the expected vnode count;
- event-time or processing-time timer tables and watermark inputs;
- logical retention/cleanup rules;
- append, update, retraction, or unmatched-output semantics; and
- capture, restore, acquire, revoke, and local-rebuild capabilities.

Operator IDs must derive from a canonical, persisted topology identity, not allocation order or a
process-local pointer. Admission compares the planned descriptor with the runtime's exchange,
backend, and ownership configuration. Runtime assertions reject a row for a vnode outside the
operator's active assignment. A stateful operator can no longer inherit no-op vnode hooks and
silently appear cluster-capable.

SQL pre-analysis remains a defensive early rejection, but the physical descriptor becomes the
positive proof. A DataFusion fallback is local-only until it produces the same descriptor and uses
the managed state service.

### 2. Stable partition and state ABI

The resolved vnode count remains fixed for a deployment/pipeline namespace. ABI v1 freezes the
existing Arrow `RowConverter` bytes using default sort fields, xxh3-64 with its existing seed, and
modulo vnode mapping; the new codec is a single authority around those semantics, not a second
encoder. The following are one versioned ABI and are persisted in catalog identity, shuffle
handshakes, checkpoint descriptors, and local-state metadata:

- canonical typed key encoding and schema identity, including null, decimal precision/scale,
  timestamp unit/timezone, and binary collation semantics;
- key hash algorithm/seed and vnode mapping;
- the vnode-0 convention for singleton global state;
- operator/state-table ID derivation;
- state-key ordering and value encoding; and
- timer-key ordering.

ABI v1 rejects floating keys, including every NaN and signed-zero representation, rather than
inventing equality semantics different from embedded Arrow grouping. It also rejects nested and
run-end-encoded keys. Integer-indexed dictionaries hydrate to their admitted logical value type;
index width is representation, not key identity. Strings and binary use raw bytes with no Unicode
normalization, decimal keys use the unscaled coefficient with precision/scale in schema identity,
and timestamps hash their stored epoch integer while unit and exact timezone remain schema
identity.

Golden vectors cover exact encoded bytes, the complete hash, and vnode for every supported family
and rejection class. Restore additionally validates the planned typed schema and every decoded key
against the artifact's claimed vnode before mutation. Any incompatible change requires an ABI bump
and explicit replay/migration or rejection; it must never silently reinterpret a checkpoint. ABI
v1 has no implicit mixed-version reader window. Assignment generation and worker identity fence
access but are not part of logical state keys, so ownership can change without rewriting every
key.

The physical key prefix is ordered by pipeline, operator/state-table, and vnode before the logical
key. This permits bounded vnode scans, bulk restore, range deletion, and quota attribution. The
initial layout uses one worker-local database with a small fixed set of physical keyspaces;
pipeline, operator, table, and vnode are logical prefixes, never separate databases. Phase 0 must
confirm that this layout gives acceptable failure isolation and cleanup cost. A database or keyspace per
vnode is forbidden; Fjall keyspaces are physical LSM trees with their own write buffers.

### 3. Batched local working-state service

The service contract is local and batch-oriented:

- multi-get and existence checks;
- ordered prefix/range scan with explicit result byte/row limits;
- one atomic write/delete/timer batch per processed Arrow batch;
- bulk ingest for restore;
- snapshot/freeze at a storage sequence;
- install, validate, and drop vnode ranges; and
- metrics plus resource reservations.

There is no `await`, database call, or object-store request per row. Operators deduplicate encoded
keys while evaluating an Arrow batch, reserve resources, and submit one state request. Cache-only
reads may complete inline; any operation that can fault on disk runs on a long-lived bounded
blocking-worker pool. Independent vnode/table lanes may run in parallel, while mutations for one
lane preserve order. A cold batch is deferred as a unit with bounded input and watermark holds;
the compute/event-loop thread never executes LSM I/O and no `spawn_blocking` task is created per
key. An aligned barrier waits for all pre-cut state requests before freezing the cut.

State changes that must agree—such as an accumulator and its last-emitted value, a window result
and timer deletion, or both sides of a changelog join—share one atomic write batch. Hot values use a
compact schema-versioned binary codec with schema metadata hoisted out of each value. Per-group
Arrow IPC streams are prohibited; the prior tier audit found their framing could dominate small
accumulator payloads. Checkpoint export may re-columnarize many logical records together.

#### Evidence-based disk-backend qualification

[Fjall 3.1.8](https://docs.rs/fjall/3.1.8/fjall/) is a credible fit on paper: a Rust-native API and no
C++ storage engine, atomic
cross-keyspace write batches, consistent cross-keyspace snapshots, forward/reverse prefix and range
iteration, sorted bulk ingestion, a configurable block-cache capacity, configurable
memtables/journals and worker threads, and a documented stable disk-format policy. It also avoids
RocksDB's C++ build and opaque native allocator. LaminarDB already has Fjall-shaped benchmark and
operational experience.

It is not accepted on API shape alone. The current public API has no native multi-get or range
tombstone. Slices can retain backing buffers including cache blocks; snapshots and iterators retain
old MVCC versions and delay reclamation. Each keyspace has a separate memtable, while the O(1)
`Keyspace::clear()` cannot clean one vnode prefix in the shared-keyspace layout. Public counters
cover items such as write-buffer bytes, journals, disk use, approximate length, and fragmented blob
bytes, but stable cache-hit, compaction-debt/backlog, stall, and total-pressure telemetry is still
insufficient. The configured block-cache capacity is not a hard process-memory governor.

An atomic Fjall batch defines consistency, not power-loss durability: ordinary writes reach OS
buffers, and the selected policy must explicitly call `persist(SyncData|SyncAll)` or prove an
equivalent group-durability boundary. Version 3.1.8 was four days old at this ADR date and followed
recent [recovery](https://github.com/fjall-rs/fjall/releases/tag/3.1.1),
[clear/recovery](https://github.com/fjall-rs/fjall/releases/tag/3.1.4), and
[poisoned/buffered-write](https://github.com/fjall-rs/fjall/releases/tag/3.1.7) fixes. The service
may coalesce point gets and perform bounded scan/delete, but Phase 0 must prove tail latency,
cleanup RTO, crash behavior, and upgrade compatibility rather than treating the disk-format policy
as a substitute for N/N-1 testing.

Historical Windows/consumer-NVMe results from `7b6ad7aa` are warning data, not qualification:
with 300 million 240-byte values (74 GB), Fjall cold-read p99 was about 0.55 ms at 100 writes/s,
1.43 ms at 10k writes/s, and 6.9–7.7 ms near ingest saturation. That confirms write-pressure and
compaction can dominate the tail. The new harness must use the actual always-current state
workload—batched group updates, timer-range scans, snapshots, and checkpoint export—on target Linux
NVMe and report p99.9 as well as p99.

Any later patched/admitted Fjall subject passes only if it meets the precommitted performance
profile, exposes stable cache/memtable/journal/disk/compaction telemetry, obeys hard memory/disk/
queue bounds, survives the crash/corruption matrix, and supports the required portable restore/
upgrade policy. Every admitted candidate runs the same logical batches, timer scans, snapshot/export
overlap, restore, cleanup, and fault schedule rather than comparing unrelated vendor microbenchmarks;
no adapter is authorized before candidate-specific mechanism closure. RocksDB's
multi-get, range delete, rate limiting, mature operational telemetry, and physical
[checkpoint](https://github.com/facebook/rocksdb/wiki/Checkpoints)/SST-ingest primitives are
advantages; its C++ build, native-memory accounting, platform burden, and compaction tuning are
costs. MultiGet benefits depend on table/filter configuration and the chosen Rust binding.
DeleteRange tombstones can degrade reads, one column family's write stall can stall the database,
and the generic rate limiter governs background flush/compaction I/O rather than WAL durability or
per-vnode QoS. Physical checkpoints are whole-database mirrors; column-family export is still not a
portable logical vnode artifact. SST ingest needs sorted input and may flush overlapping memtables
or briefly block writes, so it is a measured restore optimization. Qualification pins an exact
RocksDB release and Rust wrapper before relying on any API behavior.

The in-memory backend is required for model/differential tests and may serve explicitly bounded
local workloads. It is not the cluster production fallback: inability to open or govern the
qualified disk-backed backend keeps keyed cluster admission closed.

### 4. Resource governance

One worker-level governor owns reservations across:

- LSM block cache, memtables/write buffers, journals, pinned values/snapshots/iterators, background
  workers, OS page cache, and any native overhead of a fallback backend;
- operator scratch data, decoded keys/values, Arrow input/output, and retained output;
- active, frozen, and not-yet-committed mutation generations;
- timer indexes and window/join side metadata;
- shuffle queues and bounded acquire/replay buffers; and
- local state bytes, temporary checkpoint files, compaction debt, and restore staging, including
  artifact/descriptor/payload bytes, chain links/depth, operators/vnodes per transition,
  groups/accumulators/rows, canonical key/state bytes, and output-buffer bytes.

Cardinality counters remain metrics, not safety limits. Reservations happen before mutation with a
documented maximum one-batch slack. Pressure first triggers safe flush/compaction and bounded
backpressure. If capacity is not recovered within the configured deadline, the pipeline faults in
a controlled way and recovers from the last committed cut. It must not OOM, silently drop state, or
invent eviction. TTL/retention deletes state only when it is part of the SQL/operator semantics.

Memory and disk have separate hard limits. Compaction debt and write amplification are explicit
admission/health signals because free disk alone does not prove that the state path can sustain its
write rate. Fjall slices must release/copy retained backing buffers before a batch ends, and
snapshots/iterators have bounded lifetimes so old MVCC versions can be reclaimed. If RocksDB is
selected, native allocations are included; [Kafka Streams' 2026 RocksDB leak
fix](https://kafka.apache.org/blog/2026/06/25/apache-kafka-4.3.1-release-announcement/) is a useful
warning against relying on Rust heap metrics alone.

### 5. Checkpoint bridge

The working copy is local and rebuildable. Cluster recovery authority remains the exact sealed
checkpoint in shared storage.

Each state write batch atomically updates the logical tables and a coalescing mutation journal for
the owning vnode. At an already-aligned barrier:

1. stop admitting a new operator batch;
2. finish the bounded in-flight batch;
3. freeze a backend sequence and rotate the active journal generation;
4. capture watermarks, timers, and operator metadata at the same logical cut; and
5. resume post-cut processing once the immutable handles are safe.

Encoding, checksumming, and uploading per-vnode full/delta artifacts then occur asynchronously
before the existing exact-attempt seal is allowed. The synchronous barrier work is proportional to
the number of participating tables/generations and the bounded in-flight batch, not total state.

Checkpoint artifacts use LaminarDB's versioned portable logical encoding rather than raw LSM
directories. Delta artifacts contain latest values/tombstones from the frozen journal; scheduled
full bases range-scan a snapshot asynchronously. Restore may use Fjall sorted ingestion or build
SSTs as a backend-specific optimization, but correctness is defined by portable records and
descriptor digests.

Keyed restore is a prepare/publish transaction, not an incremental merge of trusted bytes. Its
versioned envelope binds the partition ABI, vnode count and claimed vnode, key mode, canonical key
schema, operator identity, state-table/accumulator schema, payload codec, and explicit `FULL`,
`DELTA`, or `EMPTY` kind. Preflight uses schemas cached by the physical plan, validates every state
and emission key against the claimed vnode, checks complete-chain ordering and cross-vnode
disjointness, reserves the bounded restore budget, and computes an authoritative replacement for
the entire vnode namespace. Emission keys require exact arity and types; restore never casts them.
The current raw aggregate checkpoint is legacy input for the admitted global vnode-0 aggregate
only; a query fingerprint alone is not a keyed-state compatibility contract.

#### Artifact and schema contract

Routing compatibility and stored-state compatibility are separate identities. A grouped artifact
persists the hydrated `PartitionKeySchemaV1` used by the partition ABI and a Laminar-owned state
contract containing key mode, stable operator/state-table identity, ordered accumulator codec IDs
and versions, and exact logical input/output/null semantics. Dictionary inputs are hydrated before key
encoding; dictionary index width is never durable identity. Global state has an explicit
`GlobalSingleton` mode and is valid only on vnode 0—it is not inferred from an empty descriptor.

The first managed aggregate payload does **not** use Arrow IPC. COUNT/SUM state is a compact,
length-delimited Laminar row record, sorted strictly by canonical encoded key off the event-loop.
The contract determines a fixed state width: checked `u64` `COUNT(*)`, followed for each admitted
`SUM(Int64)` by checked `u64` non-null count and signed `i64` accumulator, all in canonical
big-endian form. COUNT's SQL result remains non-null `Int64` and SUM's remains nullable `Int64`.
For each group, the executor evaluates rows in fixed source order and checks count, non-null count,
and SUM at **every input prefix**, not just the coalesced batch result. It preflights all groups in
the Arrow batch before publishing one atomic state/output mutation; overflow in a late row or group
faults with no group or output from that batch applied. Thus `[MAX, +1, -1]` cannot pass merely
because it was coalesced into one batch. The same Laminar checked implementation becomes the
embedded/reference executor for this exact shape before cluster admission, with a compatibility
note for the former DataFusion 52.3 wrapping/profile-dependent behavior. Decimal, unsigned,
floating, `AVG`, `MIN`/`MAX`, retractions, and UDAFs remain codec-unavailable until separately
specified and tested.

Each non-empty payload row is `u32_be key_length | key_bytes | fixed_state_bytes`. A zero key length
is valid: ABI v1 admits a nonempty `Null` grouping schema, for which Arrow 57.2 encodes the row as
zero bytes; the persisted nonempty routing-schema descriptor distinguishes that keyed row from a
global singleton. `EMPTY` is identified only by artifact kind and row count. Key length, row count,
aggregate key bytes, state bytes, descriptor bytes, artifact bytes, and full-plus-delta chain
bytes are checked against the approved profile with checked arithmetic before allocation. Keys are
unique and strictly increasing, and every keyed row must hash to the claimed vnode. `EMPTY` is the
only zero-row representation. A `FULL` image replaces the entire operator/state-table/vnode namespace;
`DELTA` carries sorted latest values; append-only v1 has no deletes or tombstones. Restored routing
keys remain opaque hash/LSM identity and are never passed to Arrow `RowParser`, whose format assumes
converter-produced input; any future materializing consumer requires a strict, panic-free decoder.
Decode rejects a zero-row `FULL`/`DELTA` payload, a stored row with `COUNT(*) == 0` or
`COUNT(*) > i64::MAX`, a non-null SUM count greater than COUNT, and zero non-null count with a
nonzero accumulator. When the cached contract declares a non-nullable SUM input, its non-null count
must equal COUNT. Zero non-null count has canonical zero accumulator bytes and evaluates to SQL
`NULL`; otherwise the exact signed `i64` accumulator is the nullable `Int64` result. Cycle 5
publishes one normative binary layout with every magic, field offset, width, byte order, and digest
range plus frozen goldens, a private borrowed reader, and a test-only full-buffer fixture encoder.
The fixture encoder is not release code or the production streaming writer; this ADR does not treat an archived
Rust type or prose alone as the wire specification.
Current `last_updated_ms` is not part of v1 because no aggregate execution path consumes it.
Changed-group append output derives its stable
operation identity from the canonical key and checked count version, so v1 also carries no
`last_emitted` value. Timer/TTL or changelog codecs must add their own named state rather than
silently extending this record.

The payload sits inside the manually parsed, allocation-bounded
[managed state artifact v1 envelope](managed-state-artifact-format-v1.md). It binds exact
format/header length, inner `FULL`/`DELTA`/`EMPTY`, key mode, partition ABI, codec ID/version, total and
section lengths, row/key/state totals, checkpoint and parent identity, assignment version, vnode
count and vnode, owner-map certificate digest, stable operator/state-table/contract digests, and payload
digest. Reserved fields must be zero; total length must equal the supplied slice with no trailing
bytes. Descriptor bytes are compared byte-for-byte with the immutable plan-time contract after
their digest is checked. SHA-256 supplies corruption evidence, not authentication; trust and
encryption remain checkpoint-store/deployment properties.

Managed artifacts cannot be nested in the current `VnodePartial` rkyv object, whose attacker-sized
vectors are materialized before an inner payload can reserve memory. `VnodePartialV2` has an
unambiguous magic/version selected by the checkpoint manifest—decoders never try v2 and then fall
back to rkyv. Its canonical directory has a 160-byte header and one sorted, unique
operator/state-table/vnode entry for the authoritative roster. An entry is either `BODY`, naming an
exact inner FULL/DELTA/EMPTY slice, or `REFERENCE`, naming the exact parent checkpoint and entry
digest for unchanged state. Absence is corruption, `REFERENCE` is not empty state, and `EMPTY`
remains an authoritative zero-row base. BODY ranges are non-overlapping, in bounds, and exactly
cover the declared body region with no padding. The directory digest covers the directory and each
BODY entry covers its exact slice; a redundant whole-body digest is deliberately omitted.

Cycle 5 lands a private borrowed outer-structural V2 reader and a test-only full-buffer fixture
encoder. The outer reader validates checked layout, roster, ranges, entry kinds, ancestry shape, and
per-entry BODY digests against its expected source context. It does not authenticate the complete
object or establish aggregate-state semantics. Production composition must first match the complete
payload to the trusted seal/inventory digest and manifest selector, then invoke the expected inner
reader for every BODY with exact identity, kind, parent, codec, routing-schema, and state-contract
context. The fixture encoders compile only in tests, allocate complete vectors, and are not
production streaming writers.

The trusted checkpoint pointer first identifies the inventory object and its expected digest. A
metadata/HEAD request must expose its encoded length; restore rejects a value above
`transition_metadata_bytes_max` and acquires the global encoded-byte charge before GET, response
buffering, transport reassembly, alignment copy, or parsing. The inventory is streamed to that exact
cap, digest-verified, and then authenticates expected artifact type/version, content lengths and
digests, checkpoint/assignment provenance, and legacy/managed dispatch. Each artifact repeats the
same reserve-before-GET protocol. Short, long, or digest-mismatched bodies fail closed.

That trusted sealed-object composition, manifest dispatch, and bounded fetch path are not wired in
Cycle 5. The private structural reader must not be called directly on bytes fetched by the current
whole-object `read_partial` path and must not be used as evidence to relax cluster admission.

V2 parsing runs under the encoded charge; separate task/global **scratch** reservations cover
directory metadata, key/state bytes, rows, shadow ingestion, operators, and vnodes. Transition-owned
validated spool bytes are charged to the local-disk governor and retained until prepare completes;
they are never an unbounded in-memory copy. The candidate profile separately names per-artifact and
per-chain encoded caps, a directory-entry cap, a global encoded-byte pool, and per-task/global
scratch caps; that machine-readable profile remains the sole numerical source. One "artifact" is
the complete raw `VnodePartialV2` payload (excluding the existing fixed provenance wrapper), not
each inner BODY: its row, key-byte, state-byte, and encoded-byte caps are cumulative across all BODY
entries. Inner aggregate decodes consume one caller-owned, non-`Copy` mutable
`AggregateObjectBudget` ledger for the complete V2 object; the ledger is never reset per BODY.
Wrapper plus payload is checked before fetch. `resolved_parent_links_max` counts every
outer `REFERENCE` and inner `DELTA` parent edge; a FULL/EMPTY base has depth zero, exactly the
maximum is accepted, and maximum-plus-one is rejected. Whole-transition preflight resolves every
REFERENCE and validates every chain and row into that immutable spool before any operator callback.
Prepare then consumes the spool into abortable shadow LSM state.
Legacy rkyv/Arrow decoding is selected only by manifest/type proof for the currently admitted global
vnode-0 path.

Before the first artifact GET, checked inventory arithmetic reserves the transition's declared spool
bytes against the local state-disk governor and project quota. Spools are namespaced by transition
digest and become readable only through an atomic completion marker. A retry retains the same
completed spool; process restart discards incomplete/unreferenced spools and can rebuild them from
remote authority. Successful publication or terminal transition rejection releases and removes the
spool outside the ownership fence. Pressure/hard-stop policy includes live state, LSM amplification,
and all retained spools; restore backpressures rather than exceeding the reservation.

The initial codec registry admits only concrete reviewed Laminar implementations. Function names
are not codec identity because a UDAF can reuse a built-in name. Direct rkyv of live Rust/DataFusion
types and hash-map iteration order are not durable ABI. Fresh/populated, null-only, per-prefix
overflow, split/coalesced, late-group rollback, and impossible-restored-state goldens; truncation and
every-limit/max-plus-one vectors; duplicate/out-of-order/cross-vnode keys; checksum/provenance
failures; and N/N-1 compatibility tests precede a writer. Contract derivation is plan-time;
sorting, encoding, hashing, and restore decoding run on bounded blocking workers. None runs on the
record/event-loop hot path, and qualification still measures their CPU, memory, pause, and tail
effects.

The landed borrowed readers and frozen fixture vectors are admission-neutral conformance
primitives. They do not resolve REFERENCE/DELTA chains, produce the authoritative replacement namespace, ingest
shadow state, publish a graph transition, alter a checkpoint manifest, or relax `[LDB-4007]`.

#### Whole-graph publication boundary

One staged transition binds the exact committed checkpoint attempt and checkpoint assignment fence
to one target assignment version, nonzero vnode count, and owner-map digest. It contains acquired
base-plus-delta chains, revoked vnodes, and a digest of the complete state-lifecycle operator
inventory. Every required operator/vnode pair has an explicit `BODY` or `REFERENCE` entry, and each
resolved chain terminates in an authoritative `FULL` or `EMPTY` base; absence is corruption. The
old split staging maps and successful default vnode hooks are not eligible for the managed path.

Restore follows this protocol:

1. snapshot the exact staged transition without removing it;
2. preflight the whole batch—attempt/parent links, assignment provenance, vnode ranges,
   acquire/revoke disjointness, duplicate-free canonical operator inventory, explicit bases, plan
   contracts, key membership, resource limits, and topology—before any operator is called;
3. ask every lifecycle participant to prepare all of its acquired and revoked vnodes into shadow
   state; on any error, abort prepared shadows in reverse order and retain the identical staged
   transition with every acquired vnode still `Restoring`;
4. enter the graph's exclusive callback/publication section with source, shuffle, checkpoint, and
   output intake closed; acquire the existing rotation fence to freeze assignment authority, then
   revalidate the target owner-map digest, registry states, transport scope, and exact transition;
5. complete every fallible action before publication, then perform only unit-returning pointer or
   generation swaps, mark the complete acquired set `Active`, and remove that exact transition;
   and
6. leave the short publication section, destroy retired shard handles asynchronously, and open
   source/shuffle/output intake only after complete publication.

Sequential in-memory swaps are logically atomic because exclusive graph/callback serialization and
closed intake prevent a source row, shuffle row, checkpoint, or output from observing the graph
between them; the rotation fence separately prevents assignment change. Swaps retain old handles
so refcount drops and destructors cannot extend the fenced section. A process crash in that short
publication section discards the process image and reconstructs from the unchanged durable cut. A
changed assignment during asynchronous prepare aborts the shadows and publishes nothing.

Before any artifact fetch, graph construction runs a pure, fallible state-contract derivation for
every lifecycle participant and caches the exact incremental physical plan, implementation/codec
IDs, schemas, and digest. `Uninit` thereafter means that no working shards are installed and no
data-plane callback may run; it does not mean that the plan contract is unknown. A missing or
changed contract blocks the transition before row preflight. Prepare consumes the already validated
spool using that cached implementation and builds only abortable shadow state; DataFusion node-local
fallback remains rejected. The current flat aggregate maps are only diagnostic substrate:
production publication first shards
groups, emission state, dirty generations, timers, and deduplication metadata by vnode so prepare
can build a replacement shard and publish/revoke it with bounded pointer swaps instead of scanning
all groups.

Frozen generations remain referenced until a later committed base/delta chain contains them. An
aborted or failed capture cannot clear its changes; the next attempt includes their union or emits
a full rebase. Limits on concurrent attempts, frozen bytes, and delta-chain length apply
backpressure. A mutable-capture or encoding error faults the pipeline rather than retrying against
partially consumed dirty state.

Capture and revoke operate on vnode-prefixed ranges and frozen mutation journals. They must not
scan every live group once per vnode on the barrier or ownership-transfer path. Qualification
therefore measures checkpoint/rebalance CPU time and scheduler stalls as well as storage latency;
constant-time generation freeze is not sufficient if record encoding, range deletion, or cleanup
later steals the operator hot path.

Local WAL/fsync policy is a recovery-time optimization, not cluster authority. Correctness must
hold after complete local-disk loss by restoring the sealed cut and replaying source input. A local
cache may accelerate restart only after its pipeline identity, ABI, decided checkpoint, and
assignment are validated.

### 6. Ownership and rebalance state machine

Initial rescaling uses checkpoint-cut transfer, not dual writes or record-by-record migration:

```text
owner:      Active -> Frozen/Draining -> Revoked -> local range eligible for deletion
successor:  Unowned -> Acquiring -> Restoring -> Validated -> Active
```

The old assignment token is fenced before a successor publishes output. The successor validates
the decided checkpoint, assignment/process provenance, ABI, schema, and full-plus-delta chain;
installs the vnode; then opens its input/output gate. Rows targeting a restoring vnode are held in
a byte/time-bounded replay buffer or kept at the upstream barrier. They are never processed against
partial state. Other vnodes should continue where the graph can isolate their gates.

Acquire and revoke for one assignment change publish as the whole-graph transition defined above.
No acquired vnode becomes `Active` merely because its own operator loop finished while another
operator or vnode in that transition can still fail.

After acquisition, the successor's next checkpoint emits a full base for that vnode, bounding
cross-owner delta dependencies. Local range deletion is asynchronous and only follows durable
revocation; stale local data can never authorize ownership.

If cut-over measurements miss the agreed pause/RTO objective, a later ADR may add Megaphone-style
fine-grained logical-time migration or standby replicas. Neither complexity is on the initial
correctness path.

## Operator state models and rollout order

### Grouped aggregate

Managed tables hold the encoded group key and versioned accumulator state. Any timestamp, timer,
or emitted-value field requires a named semantic consumer; vestigial map-era fields are not copied
into the durable schema. Accumulator and output-enqueue mutations are atomic, and dirty tracking
belongs to the state service rather than a second operator map.

The first candidate is one append-only stage with one mandatory `COUNT(*)`, one `SUM` over a direct
`Int64` input column, and one or more direct grouping columns accepted by partition ABI v1. Output
aliases are naming only. Aggregate `FILTER`, `DISTINCT`, `ORDER BY`, explicit null treatment,
`HAVING`, derived aggregate/group expressions, multiple COUNT/SUM calls, and retractions remain
closed. Any preceding projection/filter expression must have a positive replay-determinism proof;
processing time, watermark-relative `now()`, volatile/random functions, AI calls, and unclassified
UDFs keep `[LDB-4007]`. Broader `COUNT`/`SUM` shapes, `AVG`, append-only or changelog `MIN`/`MAX`,
and arbitrary UDAFs remain closed until their arithmetic, null, state-growth, determinism, and
portable encoding contracts are reviewed. A UDAF must declare a stable serializer, merge/restore
compatibility, and resource behavior before it can be cluster-capable.

The existing global aggregate remains vnode 0. Distributed partial/global aggregation is a
separate optimization; it is not required to admit grouped state.

### Event-time window

Fixed tumbling windows are first, hopping windows second, and merging session windows last. State
is keyed by vnode, logical group, and window identity. A vnode-owned ordered timer table is keyed by
fire/cleanup time and window key. The checkpoint includes input watermarks/frontiers, allowed
lateness, trigger/accumulation mode, emitted/retraction state, and pending timers.

Timer firing atomically updates or deletes state, records output/retraction bookkeeping, and removes
or advances the timer. Recovery may re-fire an uncommitted output under at-least-once delivery, but
cannot lose a window, fire before its persisted frontier, or retain it indefinitely after cleanup.
Late-data and allowed-lateness behavior are explicit SQL semantics, not an implementation TTL.

Processing-time timers and custom triggers remain local-only until their restart semantics are
specified. Session merging requires atomic range lookup, merge, timer replacement, and retraction,
so it follows fixed windows rather than sharing their initial admission flag.

### Stateful join

The first distributed join is an append-only bounded inner equi-interval join. Both inputs exchange
on the same canonical encoded join key. Each vnode owns two ordered multiset tables indexed by join
key, event time, and stable row identity, plus side watermarks and eviction timers. A match probes a
bounded time range and commits buffered rows and required output bookkeeping atomically.

Watermarks and interval bounds determine when each side can no longer match and may be deleted.
Unbounded stream joins remain rejected unless the user selects a documented finite retention
contract whose semantic effect is visible; an internal cache TTL is not correctness.

Outer/semi/anti joins follow only after unmatched-row timers and emitted/retraction identity are
checkpointed. Changelog joins additionally require signed multiplicities and deterministic
retraction. ASOF, temporal, and session-like joins need ordered-history/version rules and arrive
later. Lookup enrichment stays a distinct versioned replicated/read-through design.

### Materialized output

Enabling a stateful named stream does not enable `CREATE MATERIALIZED VIEW`. Cluster MVs need their
own planner-certified output partitioning, assignment-fenced writes, restore, routed or distributed
reads, and subscription ordering. MV work is a separate phase and keeps the blanket `[LDB-4007]`
guard until all of those pieces pass.

## Delivery, source, and sink composition

Operator-state correctness and end-to-end delivery are separate contracts that must compose. The
initial cluster release remains **at-least-once**. At the current baseline, cluster admission
rejects `BestEffort` and `ExactlyOnce`; `[LDB-0013]` continues to guard the latter. A certified
cluster source must be non-ephemeral and `Splittable`, with assignment-scoped checkpoint/handoff.
Kafka is currently the only built-in external source with that topology. Kafka source partitions
govern input ownership, while the SQL group/join key governs operator vnodes; checkpoint metadata
must bind both assignment versions and must never assume they are the same partitioning.

The existing source handoff already binds the exact checkpoint attempt, source assignment,
cursors, per-source watermarks, cluster watermark, and recovery frontier. Window support extends
that cut with vnode timer/frontier state rather than creating a second watermark authority. A
source drain or reassignment cannot advance the frontier past input that has not reached managed
state.

Certification does not infer the input set from producer acknowledgements: Kafka can persist a
record and lose its acknowledgement. The workload producer first durably records a stable event
intent, then the independent controller reads back every actual broker record through the frozen
partition high-watermarks and reconciles event ID/payload/offset against those intents. The oracle
models the reconciled broker log, including any physical retry records; an unknown or conflicting
record makes the run fail or invalid according to the charter.

At-least-once recovery restores operator state and the source cursor from one sealed cut. It must
not double-apply replay within recovered state, lose timer/output bookkeeping, or skip a result;
external results flushed after that cut may appear again after a crash. The checkpoint tail keeps
the existing ordering: enqueue operator output, flush every durable sink, then seal source
positions. State capture and real sink-flush latency share the checkpoint deadline. A stable output
identity and provenance envelope must be added before the initial release. For the narrow
append-only `COUNT(*)`/`SUM(Int64)` vertical, the checked count is the batching-independent logical
state version;
identity binds that version and canonical group to deployment, pipeline **incarnation**, and
operator identity,
while a separate canonical payload digest detects conflicting values at one version. The input
contract maps each logical group to one Kafka partition so group-local broker order is stable, and
the planner rejects any expression that cannot reproduce the same group/SUM prefix from replay.
Intentional rewind/recreate gets a new incarnation; ordinary crash recovery retains it. SUM checks
every source-ordered prefix and faults the whole input batch atomically rather than wrapping.
Vnode and partition ABI, assignment
version, node ID, boot UUID, and process term accompany the identity. A checkpoint attempt alone is
insufficient because replay can cross attempts and owners. This is evidence for at-least-once
correctness; it is not presented as exactly-once.

A cluster sink used by this release must be `DurableAtLeastOnce + MultiWriter` and accept the
operator's declared output mode. The first candidate is Kafka `envelope=append`; broker topic,
partitioning, acknowledgement, replication/min-ISR, election, DLQ, and retention settings are part
of the certified contract. The first managed aggregate emits one current result for each distinct
group changed by an atomically applied input batch. Multiple rows for one group may be
coalesced, so intermediate count versions may be absent; output never scans or republishes every
resident group merely because another group changed. Versions increase within one writer-authority
interval. After a crash, an unsealed higher version may already be external while recovery starts
from an older sealed cut, so a new fenced writer interval may append lower legal prefixes before it
reaches the final version again. The same version always has the same operation ID and bit-identical
payload. Kafka producer idempotence cannot deduplicate recovery from a new producer incarnation.
There is currently no
built-in cluster-admissible `FullChangelog` sink. Any retraction/full-changelog output remains
fail-closed until either a multiwriter changelog-log sink is certified or mutable sinks gain
key-affine assignment, old-writer fencing, deterministic operation IDs, and vnode handoff. Merely
marking a mutable sink `MultiWriter` is not sufficient.

A stale-owner append is defined by computation or sink admission after the writer lost process or
vnode authority, not by broker acknowledgement time. Assignment/node/process metadata alone cannot
prove that boundary. Every output carries a writer-interval ID and a sink-admission sequence that
starts at zero and strictly increases within each `(sink-writer shard, writer interval)`. Activation
requires an externally auditable fence proof. For the Kafka candidate, a bounded sink-writer shard
has a stable
transactional ID derived from deployment, pipeline incarnation, sink, and shard—not the ephemeral
writer interval—so successor initialization broker-fences the old producer. In one confirmed
transaction the successor then writes a deterministic predecessor/successor interval marker to
every affected output partition; it admits no data before that commit is known successful. All
subsequent output also uses transactions from that fenced producer and is captured read-committed;
transaction batching is bounded and must meet the latency profile.

The oracle reads committed records, uses the first valid marker for the successor as the immutable
partition cut, and rejects an old-interval record after it. A predecessor transaction committed
before the marker remains legal even if its acknowledgement arrived later; an in-flight transaction
aborted by fencing is invisible. Ambiguous marker commit is fatal to that writer process and a new
interval must fence it before retry; crash/fault tests bracket initialization and marker commit.
Broker configuration, transactional-ID derivation, markers, read-committed capture, and forced old
producer rejection form the retained proof—timestamps or Laminar logs alone do not. This
provider-enforced fencing is qualified for latency and failure behavior but does not make delivery
exactly-once because source cursor, managed state, and Kafka transaction are not one atomic commit.
If this topology cannot meet the profile, the Kafka scenario stays closed.

End-to-end exactly-once is a later certification per concrete source/state/sink combination. It
requires an exact-certified source and a checkpoint-committable external sink whose transaction
atomically consumes the predecessor cursor and is fenced by deployment, pipeline/sink namespace,
checkpoint attempt, and live leader term. The engine already binds checkpoint decisions to leader
and assignment proofs; the missing portion is connector/provider operations that consume the same
authority and recover ambiguous commits. Local LSM WAL/fsync policy cannot supply that guarantee.

## Correctness and low-latency gates

Before Phase 1, maintainers must check in a reproducible workload profile with hardware, state
size relative to RAM, key/value distributions, hot-key skew, batch size, ingress rate, window/join
bounds, checkpoint cadence, and numerical p99/p99.9 latency, throughput, pause, and recovery goals.
Targets are chosen before optimization results are known; “fast on a laptop” is not a release gate.

The checked-in [`linux-nvme-v3` candidate](../../tools/state-backend-qual/profiles/linux-nvme-v3.candidate.json)
is the current proposed numerical contract, not a benchmark result or approval. Its evidence-ownership map
assigns backend, artifact-conformance, and product-integration sections to different executors; an
LSM run cannot satisfy sink/checkpoint/failover gates. Its standalone validator accepts only the
explicitly ineligible form, rejects measured/result fields, and has no runtime or backend dependency.
Named owner approval, immutable runner identity, evidence for every admitted candidate, the product
connector/object-store profile, and the independent release soak all remain outstanding. The v1
and v2 profiles are retained only as immutable validation/model regression fixtures.

Regardless of the profile, these architecture invariants are mandatory:

- no network or object-store I/O per state access;
- no per-row future, fsync, or database transaction;
- no total-state scan in the synchronous checkpoint barrier section;
- no uncharged unbounded map, timer heap, acquire buffer, or checkpoint generation;
- no successor output before state validation and ownership activation;
- no admission based only on a SQL string or a default no-op hook;
- no silent state eviction, partial restore, checkpoint downgrade, or guarantee widening; and
- no claimed production support when the fault/latency suite or independent release-candidate soak
  is skipped.

Release evidence reports p50/p95/p99/p99.9 end-to-end latency and event-loop stall, throughput,
checkpoint align/freeze/upload/seal times, RSS/native/cache/memtable bytes, local bytes and
compaction debt, write amplification, state size/cardinality/timer count per vnode, restore rate,
and vnode unavailability during rebalance. Results include steady state, checkpointing,
backpressure, spill, hot skew, node loss, object-store delay, disk pressure, and `1 -> 3 -> 2`
ownership changes.

Correctness uses differential output/state comparison with embedded/reference execution plus
deterministic crash points before and after state batch, timer fire, freeze, upload, seal,
assignment publish, revoke, install, and activation. A PGVal-style matrix varies data rate,
partitions, topology, parallelism, skew, and fault timing; one happy-path recovery test is not a
guarantee.

Production certification also requires a black-box soak that is independent of the implementation
and its in-process model tests. It runs the release-candidate binary in a production-like
multi-process deployment with real certified source, shared object store, and sink. An external
oracle—not LaminarDB's operator state—checks output/state progress, allowed duplicates, checkpoint
recovery, and stale-owner exclusion for every source/operator/output/sink scenario proposed for
production. The duration, event volume, fault/rebalance schedule, resource leak slopes, and pass
thresholds are committed before the run; raw logs, metrics, manifests, output digests,
configuration, and binary identity are retained for an independent reviewer. An
unexpected harness gap, unexplained anomaly, assertion failure, or relevant binary/configuration
change invalidates the evidence and requires a complete clean rerun. A canary, benchmark, or the
backend qualification soak is not a substitute.

## Alternatives considered

### Keep operator maps and improve the group-count limit — rejected

Entry counts do not bound variable-sized state, allocator retention, timers, output history, or
checkpoint copies. Per-operator accounting would also duplicate spill, snapshot, restore, and
pressure policy and leave joins/windows inconsistent.

### Turn `StateBackend` into the live-state API — rejected

Its remote artifact/seal semantics are intentionally attempt-scoped and asynchronous. Combining
them with hot point/range operations would put remote latency on the data path and weaken the
meaning of checkpoint authority.

### Object-store-primary LSM now — rejected

RisingWave Hummock demonstrates versioned and pinned snapshots, cache tiers, and explicit
compaction/vacuum ownership. Flink ForSt demonstrates remote SSTs, local file caching, asynchronous
State V2 access, and lightweight incremental checkpoints, but remains experimental and its current
implementation is being replaced. Materialize Persist demonstrates durable object-store
collections with transactional consensus metadata feeding cluster-local hydrated arrangements.
Together they show that remote state is a storage subsystem—not a backend setting—and needs
version authority, caches, compaction/GC ownership, async execution, and failure isolation.
LaminarDB does not currently have that subsystem. Reconsider only if measured local-disk operations
or recovery objectives justify its cost.

### One database or checkpoint file per vnode — rejected

Hundreds of databases/files multiply caches, background threads, descriptors, compactions, and
small checkpoint objects. Vnodes are logical ordered ranges inside a bounded number of worker
databases and portable artifacts.

### Kafka changelog topics as the generic authority — rejected

Kafka Streams is coherent because tasks, input partitions, changelogs, and transactions all live
in Kafka. LaminarDB supports non-Kafka sources and already has vnodes and object-store recovery.
Kafka consumer groups or changelogs would create a second assignment/commit authority.

### Remote shared state to avoid migration — deferred

It exchanges restore pauses for steady-state network/tail latency and the remote-LSM system above.
Checkpoint-cut restore is simpler and must be measured first.

### Unaligned checkpoints or fine-grained live migration first — deferred

Flink's unaligned checkpoints capture in-flight buffers and help when measured alignment dominates;
they add artifact I/O and watermark/recovery complexity. CheckMate shows workload-dependent tradeoffs.
Megaphone reduces migration latency but adds routing/time-frontier machinery. Instrument the aligned
cut and initial restore before adopting either.

### Block keyed state on cluster exactly-once — rejected

Correct restore/rebalance is required for at-least-once too. External exactly-once additionally
needs term-fenced source handoff and sink cursor commits. Conflating the programs delays state
correctness and risks falsely advertising end-to-end exactly-once.

### Restore the former Fjall cold tier or preselect an LSM — rejected

The removed tier cached checkpoint slices and used point operations; it did not own always-current
state, and its dirty-state coupling was unsafe. Restoring it would preserve the missing lifecycle.
Conversely, API checklists and old single-insert benchmarks are insufficient to preselect Fjall or
RocksDB. A bounded bake-off is justified because backend choice directly affects the tail,
resource governor, restore, cleanup, and operational surface; a permanent two-backend product is
not.

## Consequences and risks

Positive consequences:

- one bounded state lifecycle replaces operator-specific maps and snapshot rules;
- checkpoints retain their existing durable authority and portable artifacts;
- each operator can be admitted independently by a positive planner/runtime proof;
- local NVMe preserves the low-latency path while shared storage tolerates node/disk loss; and
- window timers and join cleanup become explicit, testable state rather than side structures.

Costs and risks:

- either LSM adds corruption, disk, tuning, and compaction risk; Fjall additionally needs proof for
  batched reads, cleanup, and stable governance telemetry, while RocksDB adds native build and
  allocator accounting;
- portable state encodings and stable operator IDs become long-lived compatibility contracts;
- asynchronous full/delta materialization needs strict generation retention and pressure control;
- hot keys can still serialize one vnode/operator even when storage is bounded;
- rebalance RTO grows with restored bytes until standby or incremental migration is justified; and
- embedded execution must not regress while maps are replaced incrementally.

Mitigations are the backend qualification spike, whole-process resource governor, version/golden
tests, bounded frozen generations, per-vnode skew metrics, checkpoint-cut rollout, delivery-matrix
checks, and admission flags that remain disabled until each vertical passes its evidence gate.

## State of the art considered (facts and LaminarDB inference)

| System/research | Relevant fact | Decision taken here |
|---|---|---|
| [Fjall 3.1.8 API](https://docs.rs/fjall/3.1.8/fjall/) and [RocksDB operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations) | Fjall offers a Rust-native API without a C++ storage engine plus batches/snapshots/ranges, but lacks native multi-get/range tombstones and sufficient governance telemetry; RocksDB offers broader batch/operations controls at native-build/accounting cost | Run one workload/fault contract, including explicit durability and pressure tests, select one production backend, and retain portable Laminar artifacts |
| [Flink 2.3 keyed state](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/stateful-stream-processing/) and [state backends](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/state_backends/) | Key groups are the atomic redistribution unit; heap is low-latency but memory-bound; EmbeddedRocksDB supports large local state and incremental checkpoints | Keep fixed vnodes; start with a local disk-backed backend and portable checkpoints |
| [Flink ForSt](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/disaggregated_state/) | Remote SSTs, local cache and async State V2 access enable lightweight checkpoints, but ForSt remains experimental; synchronous state is local unless explicitly overridden, and the current implementation is slated for replacement | Do not put object storage on LaminarDB's initial hot path |
| [Flink checkpoint backpressure guidance](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpointing_under_backpressure/) | Unaligned capture is useful when measured alignment delay is the problem and captures in-flight data | Keep aligned barriers; instrument before adding unaligned state |
| [RisingWave architecture](https://docs.risingwave.com/get-started/architecture), [v3.0 vnode mapping](https://github.com/risingwavelabs/risingwave/blob/v3.0.0/docs/dev/src/design/consistent-hash.md), and [v3.0 checkpoints](https://github.com/risingwavelabs/risingwave/blob/v3.0.0/docs/dev/src/design/checkpoint.md) | Key-to-vnode mapping/storage encoding stay invariant while Meta changes vnode ownership during scaling; barriers combine with Hummock cache, version, and compaction services | Reuse fixed vnode identity/barriers; do not imitate Hummock without its control/storage services |
| [Materialize views](https://materialize.com/docs/concepts/views/), [clusters](https://materialize.com/docs/concepts/clusters/), and [arrangements](https://materialize.com/docs/get-started/arrangements/) | Materialized results are durable/cross-cluster, while indexes and shared arrangements are cluster-local; views also hydrate maintenance state proportional to inputs/outputs | Keep materialized output separate and budget hydration; consider shared arrangements only after the common store works |
| [Spark 4.2 state](https://spark.apache.org/docs/4.2.0/streaming/apis-on-dataframes-and-datasets.html) and [streaming modes](https://spark.apache.org/docs/4.2.0/streaming/index.html) | Persisted hash state depends on stable shuffle partitions; RocksDB changelog checkpointing is optional, locality is preferred rather than fenced, and bounded memory is soft. Stateful checkpointed execution remains batch/cut-oriented; Continuous Processing is at-least-once | Borrow the local store/journal pattern and compatibility discipline, not Spark's ownership or latency model |
| [Kafka Streams runtime](https://kafka.apache.org/43/streams/developer-guide/running-app/), [rebalance protocol](https://kafka.apache.org/43/streams/developer-guide/streams-rebalance-protocol/), and [configuration](https://kafka.apache.org/43/streams/developer-guide/config-streams/) | Partition-derived tasks restore local stores from changelogs. Standbys exist; classic assignment's High Availability Assignor has warmups and rack-aware placement, while opt-in broker-authoritative `group.protocol=streams` in 4.3 does not yet provide that assignor. ALO is default and Kafka EOS is opt-in/Kafka-scoped | Learn from local restore/standby, but keep LaminarDB's connector-independent vnode and external-delivery authority |
| [Asynchronous Barrier Snapshotting](https://arxiv.org/abs/1506.08603) and [Flink state management](https://www.vldb.org/pvldb/vol10/p1718-carbone.pdf) | A short consistent cut can be separated from asynchronous state materialization | Freeze state at the existing aligned cut and upload outside the pause |
| [Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) and [MillWheel](https://research.google/pubs/millwheel-fault-tolerant-stream-processing-at-internet-scale/) | Event time, watermarks, triggers, persistent per-key state, and timers are correctness concepts | Make timers/frontiers first-class managed state |
| [Differential Dataflow](https://www.microsoft.com/en-us/research/publication/differential-dataflow/) | Incremental collections carry changes/differences | Preserve explicit weights/multiplicity for changelog joins rather than overwriting rows |
| [Megaphone](https://www.vldb.org/pvldb/vol12/p1002-hoffmann.pdf) | Fine-grained migration can reduce latency spikes | Defer until checkpoint-cut migration is measured |
| [Disaggregated State Management in Flink](https://www.vldb.org/pvldb/vol18/p4846-mei.pdf) (2025) | Remote-primary state required asynchronous access, local caching, streamed updates, and lightweight checkpoint/recovery machinery | Keep synchronous object storage off the initial hot path; treat any future remote state as a subsystem |
| [CheckMate](https://arxiv.org/abs/2403.13629) | Coordinated checkpoints performed best under uniform load, while uncoordinated checkpoints could benefit skewed workloads and cyclic dataflows | Treat checkpoint mode as an empirical decision |
| [PGVal](https://www.vldb.org/pvldb/vol18/p585-tahir.pdf) | Observed end-to-end reliability varied with input rate, partition count, topology, parallelism, and fault type | Require a multidimensional output-oracle fault matrix including network faults |
| [Timely and Accurate Prefetching](https://arxiv.org/abs/2603.19890) (ICDE 2026) | Known future state keys can be prefetched to reduce cold-state latency | Defer; reconsider only if qualification shows cold reads dominate p99/p99.9 |

[Flink 2.3.0](https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/)
(2026-06-25), [Spark 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)
(2026-07-14), [Kafka 4.3.1](https://kafka.apache.org/blog/2026/06/25/apache-kafka-4.3.1-release-announcement/)
(2026-06-25), [RisingWave 3.0](https://github.com/risingwavelabs/risingwave/releases/tag/v3.0.0),
and Materialize's current 2026 documentation were checked for this decision. Vendor documentation
establishes what those systems do; the LaminarDB choices above are explicit design inferences, not
claims that another system proves this implementation correct.

## Revisit conditions

Reopen this ADR if backend qualification cannot meet the committed latency/resource profile, local
restore cannot meet the production RTO at the target state size, the vnode/partition ABI is found
incompatible with required SQL key semantics, or a shared-state design becomes justified by
measured operations rather than analogy. Any replacement must preserve positive admission proofs,
portable sealed recovery, byte governance, and fenced ownership.
