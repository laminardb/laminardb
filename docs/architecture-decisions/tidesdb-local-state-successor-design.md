# TidesDB local working-state remediation and successor design

- **Status:** accepted Cycle 39 selected-target design; source construction and candidate execution
  prohibited
- **Date:** 2026-07-25
- **Product direction:** TidesDB is the selected target for worker-local state, conditional on every
  qualification and product gate in this document
- **Production qualification/admission:** none
- **Exact native reference:** TidesDB `v9.3.14`, commit
  `6fe1e83104b70255a694239d360a14bae51d0c70`
- **Rejected current Rust subject:** `tidesdb-rs v0.11.1`, commit
  `e2febbc548e7f0158d1c09ea487aa0bb7c343616`, which selects native `9.3.6`
- **Evidence basis:** [static prescreen](../reports/tidesdb-static-prescreen-2026-07-25.md),
  [managed-state ADR](ADR-008-managed-vnode-keyed-state.md), and
  [runner v2 reference contract](state-backend-qualification-runner-v2-draft.md)
- **Candidate downloaded, built, linked, modified, or executed:** no
- **Runtime/admission effect:** none; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Production verdict:** **NO-GO**; independent product soak has not run

## Decision

The initial TidesDB product shape is:

```text
one worker-local database
+ one fixed physical managed-state column family
+ Laminar table/vnode/generation prefixes
+ asynchronous disposable local persistence
+ always restore into a new local directory from a Commit-admitted Laminar checkpoint
+ no existing-directory state, native checkpoint, or native remote storage in the safe surface
```

This is a deliberate successor to, not a relabelling of, the frozen four-keyspace Fjall/RocksDB v4
profile. It removes mechanisms that the initial product does not need and keeps the low-latency path
free of a per-batch `FULL` durability fence. TidesDB is local capacity and latency infrastructure;
Laminar's portable artifacts, exact-attempt seal, coordinator decision, ownership fence, and source/
sink protocol remain recovery and delivery authority.

The product choice does not make TidesDB safe or production-admitted. A narrow exact-current Rust
integration, exact-count mutation success, fail-stop unknown outcomes, atomic visibility, immutable
logical cuts, cgroup/resource control, maintenance health, concurrency, latency, faults, endurance,
integration, and independently operated product soak all remain conjunctive gates.

## Authority and bounded investment

This document authorizes only documentation and source review. It creates no dependency, native
source tree, fork, wrapper, build script, adapter, schema, profile, command, benchmark, or run.
No exact TidesDB source tree is present in this workspace; source facts are carried from the dated
exact-source prescreen.

A later source-construction task must bind the exact source/fork, isolated workspace, targets,
toolchain, time, machine cost, and stop conditions. To avoid another open-ended candidate exercise,
it has two kill-fast stages:

1. at most half an engineering day and no candidate machine time for exact-source/build identity,
   ownership/close feasibility, licensing/distribution, and immediate stop conditions; then
2. only if that passes, at most one engineering day and four machine hours for the smallest narrow-
   wrapper feasibility slice: represent a create-new root, fixed CF, exact-count batch/read, and
   deterministic shutdown under the proposed ownership model. Compilation, linkage, or dynamic
   smoke execution occurs only when the separately bound task expressly authorizes each action;
   otherwise the corresponding questions remain open for later proof.

Reaching either cap without closing its questions returns `INSUFFICIENT_CLOSURE`; it does not
silently continue or admit the candidate. One-CF visibility, fresh restore, cgroup fixes, health
instrumentation, and the complete proof matrix belong to a separately estimated construction
package after both stages pass, not to an expanding prescreen.

No GitHub approval workflow is required for the later task. The project owner must still explicitly
bind its scope and cost. Exact candidate execution requires a still-later authorization over the
candidate, profile, plan, target, isolation, limits, and cost.

## Initial physical and recovery profile

### One fixed column family

All logical state tables share one fixed physical column family. The durable Laminar key contract
continues to identify pipeline, operator, logical table, vnode, generation, and opaque logical key.
The qualification layout may use the compact form below only when the database root and signed
profile identity bind exactly one pipeline/operator namespace. Otherwise the full pipeline and
operator identifiers remain in every key; omitting both would permit namespace collisions.

```text
vnode_u32_be || record_kind || generation_u64_be || table_tag_u8 || opaque_key
```

The table tag distinguishes aggregate, window, timer, join-left, join-right, and output-bookkeeping
records. Lifecycle records retain their distinct record kind. The safe integration exposes no
runtime column-family create, drop, rename, comparator, or configuration mutation.

One CF removes the source-proved cross-CF sequential-WAL problem and the need for unified mode in
the initial profile. It does not excuse silent short writes, partial visibility, snapshot defects,
or global interference. Shared compaction/stalls, timer and join prefix scans, cleanup cost, and
hot-writer/disjoint-vnode victim latency remain measured vetoes.

One database or CF per vnode remains forbidden. One admitted state-service request represents one
processed Arrow input batch. Before native entry, it is partitioned into sorted canonical mutation
units, each containing exactly one vnode, and the adapter validates ownership and reserves resources
for every unit. The complete Arrow batch—not an individual vnode unit—is the atomic publication and
output unit. Reads, checkpoint capture, result publication, and output stay behind its outer gate
until every unit succeeds. If any post-entry unit becomes ambiguous, the entire worker-local
database incarnation is poisoned and none of the batch is published; physically applied but hidden
units are never served and the incarnation is later restored fresh.

### Always-fresh portable restore

The first product profile never opens a prior TidesDB directory, including after a clean restart.
Every non-genesis process incarnation:

1. selects only a sealed, durable-Commit-admitted Laminar checkpoint and live assignment fence;
2. creates a new, never-reused, empty local root under the approved quota;
3. restores and validates portable records while the generation remains unservable;
4. verifies the complete logical digest and source cursor/frontier;
5. proves the committed source cursor still exists and initializes the fenced source assignment;
6. atomically publishes the restored generation under the ownership fence; and
7. opens input and output only after publication, then replays and catches up from the committed
   cursor under the claimed delivery protocol.

The first deployment follows the same admission shape through a coordinator-committed empty genesis
cut. That cut binds the exact source starting cursors, assignment/process fence, state ABI, empty
logical digest, and attempt identity. Absent, duplicate, ambiguous, wrong-source, wrong-assignment,
or wrong-incarnation genesis evidence leaves the vnode inactive; there is no special unfenced empty-
database bootstrap.

A crash during restore abandons that root. A valid, corrupt, newer, permission-denied, or poisoned
old root is quarantined or garbage-collected under a bounded policy but never opened or read, and a
root referenced by an unquiesced native call is never deleted or reused.
Failure to create or restore a fresh root leaves the vnode inactive; it never falls back to local
files.

Native checkpoint, opening an existing state directory, serving recovered WAL state, cache-loss
prefix recovery, and native remote storage are unsupported and unreachable through the initial safe
API. Construction must prove exclusive create-new semantics and an empty root before native open.
The engine may internally enter recovery code while creating an empty database; the narrower product
claim is that no existing WAL or recovered native state can become servable. Call-graph
unreachability may be claimed only if exact source proves it. Current native-recovery defects are not
recorded as passing features or `N/A` observations.

The production profile has no per-batch native durability fence. A successful local mutation means
complete in-process installation and visibility, not power-loss durability, checkpoint commit,
source-offset commit, sink commit, or exactly-once. A future local-fast-restart profile would require
a new lineage and would reopen `TDB_SYNC_FULL`, strict WAL replay, directory/cache-loss durability,
acknowledgement, corruption, and local-identity gates.

## Narrow exact-current Rust integration

Do not repair or expose the broad `tidesdb-rs 0.11.1` API. A later isolated construction package
should build a small project-private integration containing a private unsafe module, a narrow C ABI
shim compiled against exact native 9.3.14, and a safe facade containing only Laminar-required
operations. Any patch creates a distinct subject whose identity binds the base commit, ordered patch
digests, resulting source-tree/archive digest, ABI revision, features, build inputs, and SBOM; it
cannot retain the unpatched subject identity.

The design requires:

- content-addressed native source and static linkage of the exact native object set, with no network
  fetch, `pkg-config` substitution, runtime library substitution, or unpinned system TidesDB library;
- a checked-in allowlisted shim/binding surface, exact header/ABI probes, embedded native/source/
  feature identity, link-map proof, lockfile, build flags, allocator, SBOM, licences, and notices;
- identities for every native transitive input, including codecs, allocator, generated files,
  compiler/linker/build tools, and the exact libc/system DSO floor; plus MPL covered/modified source,
  third-party licence inventory, notices, distribution evidence, and legal review;
- one parent database owner; child handles retain parent ownership; transactions are unique and
  non-`Clone`; iterators borrow their transaction and cannot escape it;
- no raw handle, engine-owned slice, arbitrary option, runtime CF deletion, commit hook, custom
  comparator, or Rust callback in the initial surface;
- copied key/value bytes before a service interval ends; exact null/length, integer-narrowing,
  buffer/error ownership, status/enum, and unknown-status fail-closed rules; and
- a creator/owner/borrower/destroyer/concurrency ledger for database, CF, transaction, iterator,
  snapshot, error, and buffer objects. Duplicate CF acquisition must intern/share ownership or prove
  that wrappers never destroy database-owned CF pointers; and
- explicit `Open -> Closing -> Closed` admission and child-before-parent shutdown. Close rejects
  live children and must source-prove that every background worker joined before freeing the
  database. Failed or unproved close never frees optimistically from `Drop`.

Every unsafe block needs an inventory entry and local safety argument. Unknown statuses/enums and
panic paths fail closed; no panic or invalid conversion crosses the FFI boundary. Golden/source proof
must show that the built-in comparator is Laminar's canonical unsigned-byte lexicographic order. A
different ordering stops the narrow package because the initial surface permits no Rust comparator.

Native-owning types begin `!Send + !Sync`; no blanket unsafe trait is inherited from the official
wrapper. Production concurrency requires a source-backed per-type/per-operation ledger and native
TSAN evidence before selectively sharing the database/CF. Transactions and iterators remain worker-
confined unless exact proof shows otherwise. If safe parallel disjoint-vnode service cannot be
proved, or a single confined lane misses the frozen C3/latency gates, the candidate stops.

Miri covers only the pure-Rust ownership/lifecycle model. The actual native subject still requires
ASAN, LSAN, TSAN, and UBSAN coverage. If a future callback is unavoidable, it needs install-once
stable ownership, quiescent teardown, panic containment, and a fail-closed native error path; a Rust
comparator callback without an error return remains prohibited.

Synchronous native admission is non-cancellable. After entry, caller timeout or cancellation places
the incarnation in an admission/output hold while the worker resolves the result; it cannot abandon
the call. An exact result published into the stable result slot under the visibility gate may release
the hold. Any unresolvable outcome poisons the incarnation. Ownership of the database, child handles,
directory, and native buffers is retained until the call definitively returns and all workers
quiesce. Nothing may free, delete, restore over, or reuse that root. Shutdown waits only under a
bounded policy; if quiescence cannot be proved, it leaks/fail-stops safely or terminates the
containing process. A hung call that can outlive ownership or defeat the recovery deadline is a
candidate stop.

## Mutation and visibility contract

Before native entry, the adapter validates the complete Arrow-batch request, partitions and orders
its vnode units, rejects duplicates or a key placed in the wrong unit, checks every assignment plus
row/key/value/result/time limit, and reserves every bounded Rust and native byte. Every fallible
validation possible happens before the first unit mutates.

The externally observable state machine is:

```text
Ready
  -> AllVnodesValidatedAndReserved
  -> NativeAdmissionMayHaveOccurred
  -> CompleteHiddenInstall
  -> AllResultSlotsAndStatePublished
  -> SuccessObserved

NativeAdmissionMayHaveOccurred -> UnknownCommit -> IncarnationPoisoned
AllResultSlotsAndStatePublished -> ResponseLost -> StoredCommittedOutcome
```

- A validation/rejection before native admission leaves state unchanged and may return normally.
- Success is permitted only when every unit's native result proves that exactly its requested
  operation count was installed and the outer gate publishes all units; accepting any nonnegative
  short count or publishing a successful prefix is prohibited.
- Any short apply, native error, allocation/I/O failure, or other ambiguous outcome before
  publication is `UnknownCommit` and poisons the entire local incarnation before the visibility gate
  is released.
- Once native admission begins, caller timeout/cancellation cannot abandon the worker or result. An
  exact-count result, stable request identity, result slot, state publication, and cancellation
  disposition are serialized under the same gate. A waiter lost after publication may recover only
  that stored committed outcome; it may not resubmit the mutation. If the result slot cannot be
  recovered unambiguously, the adapter atomically blocks later admission and output and poisons the
  incarnation before any dependent effect escapes.
- `UnknownCommit` permits no later read, write, scan, snapshot, export, checkpoint, output, or blind
  retry from the poisoned incarnation.
- Recovery restores the last committed Laminar cut into a new directory and replays the certified
  source. A silent successful partial apply is always disqualifying.

One-CF application is still entry-by-entry in the inspected source. Construction must either stage
the complete Arrow batch privately and publish once or put every admitted point/range/iterator/
snapshot/mutation operation behind its outer Laminar visibility gate. All output and result slots
remain hidden with state. Same-vnode logical tables share one ordered lane; a multi-vnode batch
acquires its lanes in canonical vnode order and releases them only after whole-batch publication or
poisoning. Disjoint batches may run concurrently only after exact source/thread-safety and C3
evidence. The aligned checkpoint barrier drains every pre-cut outer batch before acquiring the
immutable cut.

Every successful read sees one complete pre-mutation or post-mutation state; no point/iterator path
may bypass the publication rule. If the only correct gate is database-global, it remains acceptable
only if hot-writer/victim, throughput, queue, p99.9, maximum, and checkpoint-overlap limits pass.

## Immutable cut, export, and restore

Checkpoint acquisition captures one committed single-CF sequence/frontier covering lifecycle and
all logical table tags. It excludes hidden/poisoned work, survives later mutation/flush/compaction,
retains safe ownership for a bounded charged lifetime, and expires fail-closed without publishing a
partial artifact.

Export uses bounded half-open vnode/table scans, canonical unsigned-byte order, exact row/byte caps
plus one lookahead, deterministic FULL/DELTA/EMPTY records, and existing Laminar artifact, inventory,
attempt, and assignment digests. A lost upload response retries identical frozen bytes; it never
recaptures mutable state under the same attempt.

Only Laminar's portable writer and Rust `object_store` path publish local/S3/GCS/Azure artifacts.
Native SST, WAL, manifest, checkpoint directory, or remote object never becomes a portable artifact,
restore selector, ownership proof, or exactly-once receipt.

Restore validates the complete stream and budget before publication, builds an unservable fresh
generation, verifies its logical digest, and publishes exactly once under the assignment/process
fence. Missing, corrupt, unordered, duplicate, wrong-vnode, wrong-ABI, wrong-epoch, over-budget,
Sealed-Abort, undecided, or DecisionInDoubt input publishes nothing.

## Resource, health, and hot-path obligations

Laminar resolves the effective cgroup-v2 budget and passes an explicit internal managed-memory
limit below the external hard limit. TidesDB must honor the explicit value without its current
host-RAM-derived five-percent floor or upward clamp. Auto memory mode is prohibited. The profile
reserves Rust/Arrow/request/result/queue/snapshot, native allocator, page-cache, and temporary
headroom separately; the cgroup remains hard authority.

Qualification binds fixed, non-auto cache/memtable/write-buffer/worker/file settings and observes:

- process-tree PSS/RSS, allocator identity, `memory.current`, `memory.peak`, file/dirty/writeback,
  event deltas, and swap-off proof;
- every WAL/SST/manifest/obsolete/compaction/export/temp path below one XFS project-quota root;
- RLIMIT, lower engine FD cap, actual FD peak, quota/ENOSPC, cgroup/device I/O, disk growth, and
  write/space amplification; and
- snapshot/iterator lifetime, restore staging, frozen generations, and post-write maintenance-tail
  clearance.

The maintenance mapping must inventory every enabled local mechanism. A bounded production surface
provides exact queue-plus-in-flight gauges and a lossless general local background-failure counter.
Qualification additionally records exact per-reason foreground stall start/end/count/duration in a
fixed preallocated ring updated only on the slow stall path. Overflow/loss invalidates evidence.
An off-event-loop bounded sampler reads one native snapshot; it does not scan files/CFs, allocate per
record, lock/log/query metrics on every request, or infer failure from logs. Missing coverage is
BLOCK, never zero.

The state service exposes one bounded operation per processed Arrow batch, internally containing
canonical per-vnode units. No per-row future, task, transaction, allocation, log, metric query, FFI
call, fsync, or object-store operation is permitted.
Later open-loop evidence separates queue, preparation, native service, and end-to-end latency and
gates every repetition's p99.9 and maximum under resident, true-spill/cold, timer/range/join, moving-
hotspot, sustained-compaction, checkpoint-overlap, and hot-writer/disjoint-victim cases. TidesDB's
built-in `uint8_t` Zipf helper is ineligible.

## Successor qualification lineage

The v2 runner freezes four physical keyspaces and native persistence/reopen semantics. TidesDB needs
a semantic successor, not only a new candidate field. Exact successor version labels are assigned
only after source closure identifies every changed wire; this design freezes the roles that require
new identities:

| Artifact | Required lineage action |
|---|---|
| runner contract | new identity |
| profile schema and TidesDB instance | new identities |
| physical layout and physical-fault semantics | new identities |
| mechanism mapping | new identity |
| profile-use binding | new identity |
| synthetic bundle | new identity and permanently non-evidentiary |

The successor may retain the C1 logical model, workload v2 operations unrelated to persistence,
latency v1, common resource samples/cuts v2, candidate-health samples v1, stall intervals v1,
target-device I/O v1, resource formulas v3, and candidate-neutral classification wires only where
their exact bytes and evaluator semantics remain unchanged. Before profile freeze, a field-by-field
ledger must decide each wire, including physical faults.

The initial TidesDB profile explicitly marks runner-v2 native `persist_data`, `persist_all`, setup
persist-close-reopen, process-death retention, existing-directory reopen, and cache-loss prefix
equation as unsupported—not passed, skipped, or `N/A`. Their setup and fault cases are replaced by
portable logical export -> close/abandon -> exclusive new root -> restore -> logical verification.
The asynchronous native policy must later bind exact WAL enablement, `TDB_SYNC_NONE` versus a frozen
interval/background policy, rotation and error handling, and every periodic WAL worker in the
maintenance inventory. `FULL` remains non-gating and future-lineage-only.

Mapping v2, profile-use v1, bundle v2, and v4-specific physical-layout and persistence semantics are
not consumable by TidesDB. V1-v4 remain immutable regression/reference history. Any changed reused
wire receives a new identity; no compatibility translator or mixed-lineage reader is permitted.
Successor synthetic input remains permanently `NOT QUALIFICATION EVIDENCE`.

## Recovery RTO, source retention, delivery, and soak

The restore-to-active RTO includes failure detection, old-owner fencing, fresh-root creation,
artifact fetch/validation, full/delta load, committed-cursor availability and seek, fenced source
assignment initialization, and atomic state activation. A separate replay/catch-up SLO runs from
activation until current head; a combined recovery deadline may gate both. They cover one-vnode
acquisition, multi-vnode rotation, complete worker/local-volume loss, one failed restore attempt,
object-store delay, and maximum admitted state/chain depth. Cursor availability must remain true
through successful seek and catch-up, not merely at checkpoint and activation. Replay throughput
must exceed admitted ingress by a frozen margin with bounded buffering or upstream pause.

Source retention must cover:

```text
maximum checkpoint age
+ failure and fencing delay
+ object-store outage/retry budget
+ restore, replay, and catch-up deadline
+ rollback/fallback window
+ safety margin
```

Both time and byte/offset headroom are proved. At checkpoint and before activation, every partition's
earliest available cursor must precede or equal the selected committed cursor. A retention gap leaves
the vnode inactive. Checkpoint GC retains every required base/delta parent and fallback cut while an
owner, restore, rollback, or decision-in-doubt lineage can reference it.

For at-least-once, output after the restored cut may reappear with replay-stable identity. Later
exactly-once still requires an exact-certified replayable/fenced source and a checkpoint-committable,
ambiguity-recoverable, old-writer-fenced sink transaction bound to the same coordinator decision.
Async local WAL does not weaken or create that composition.

After source proof, successor-profile freeze, separate run authorization, the common correctness/
latency/resource/fault/recovery/C3 and 24/72-hour campaign, and reviewed selection, integration must
still prove vnode restore/rebalance and the exact source/sink delivery scenario. The independently
operated black-box soak then tests the unchanged release artifact, independent oracle, cold cache,
complete local loss, provider-neutral checkpoint faults, repeated process loss, fencing/rebalance,
and every claimed delivery mode. Backend endurance is not the product soak.

## Construction proof matrix

| ID | Later proof | Pass condition |
|---|---|---|
| TSC-01 | exact build and safe facade | one hermetic exact subject; ABI/build/SBOM/licence bound; ownership and shutdown pass Miri/sanitizers; forbidden APIs absent |
| TSC-02 | one-CF layout | logical ordering, all six table tags, lifecycle/generation, scans, cleanup, and restore match the C1 oracle |
| TSC-03 | exact mutation success | every whole-batch success applies every unit's exact N operations; injected pre-entry failures leave the digest unchanged; no short or successful-prefix publication |
| TSC-04 | atomic visibility | concurrent point/range/iterator/snapshot readers and checkpoint capture observe only complete whole-Arrow-batch pre/post sentinels across all vnode units |
| TSC-05 | ambiguity and response latch | every pre-publication ambiguity poisons before output/read/export/retry; post-publication response loss recovers only the exact stored committed outcome or blocks and poisons before dependent output |
| TSC-06 | immutable cut/export | concurrent mutation, flush, compaction, and long export cannot change the captured digest; retries are byte-identical |
| TSC-07 | genesis and fresh restore | genesis is coordinator-committed/fenced; hostile streams publish nothing; valid restore publishes one new verified fenced generation; old roots are never opened |
| TSC-08 | corruption/faults | checksum/I/O/iterator/ENOSPC/allocation errors are fatal, never absence/success; complete local loss restores portable state |
| TSC-09 | resources and health | explicit cgroup budget is honored; all memory/FD/disk/temp/background paths bounded; exact stalls/errors observable off hot path |
| TSC-10 | latency/concurrency | batch API, disjoint-vnode C3, hot-victim, spill, maintenance, and checkpoint-overlap tails meet frozen gates |
| TSC-11 | delivery/recovery | source retention/replay/RTO, ownership fencing, provider faults, and certified ALO scenario pass |
| TSC-12 | independent soak | unchanged selected release passes the separately operated product charter before any production claim |

No current evidence satisfies this matrix.

## Hard stops

Stop the TidesDB track before a successor profile or run when any of these holds:

- acceptable RTO requires reopening existing TidesDB state in the initial profile;
- the one-CF path still reaches unified replay, native checkpoint, native remote, runtime CF drop,
  unsafe callback, or unpinned library substitution;
- a short insert can return success, partial state can escape the visibility gate, or a post-entry
  ambiguity permits continued service or retry;
- a timed-out/hung native call can outlive ownership, permit optimistic close/delete, or defeat the
  bounded recovery policy;
- no immutable all-table snapshot exists, corruption becomes missing data/success, or restore can
  serve before complete validation and fencing;
- the fix requires a new WAL/on-disk transaction protocol or a large permanent native-engine fork
  without a new owner decision;
- useful concurrency requires undocumented `Send`/`Sync`, or the correct visibility gate misses the
  frozen throughput/p99.9/maximum/C3 limits;
- explicit memory is still host-derived/clamped, any native/resource path is unbounded, or exact
  background failure/stall coverage requires normal-request-path metrics work;
- licensing, offline hermetic build, ABI, sanitizer, source-retention, RTO, target isolation, or
  provider-neutral artifact gates fail;
- any v4 identity/evidence is relabelled, candidate execution lacks exact authority, or the
  independent unchanged-release soak is omitted.

Failure records the selected TidesDB target as disqualified and returns backend selection to an
explicit owner decision; it does not automatically activate RocksDB, Fjall, redb, bounded memory,
or cluster admission.
