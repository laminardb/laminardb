# TidesDB local working-state design

- **Status:** Accepted direction; package prescreen not yet passed
- **Date:** 2026-07-25
- **Last reconciled:** 2026-07-25 during Cycle 40
- **Selected integration line:** the official `tidesdb-rs` package only
- **Current exact prescreen subject:** `tidesdb-rs v0.11.1`, commit
  `e2febbc548e7f0158d1c09ea487aa0bb7c343616`, with its default
  `tidesdb-src-v9-3-6 = 0.1` native payload
- **Runtime dependency added:** no
- **Production verdict:** **NO-GO** until package closure, qualification, integration, and the
  independent soak all pass
- **Related evidence:** [TidesDB static prescreen](../reports/tidesdb-static-prescreen-2026-07-25.md)
  and [ADR-008](ADR-008-managed-vnode-keyed-state.md)

## Decision

LaminarDB will pursue TidesDB for worker-local keyed state through the official
[`tidesdb-rs`](https://github.com/tidesdb/tidesdb-rs) package. The package is a private
implementation dependency behind a Laminar-owned safe facade; its database, column-family,
transaction, iterator, callback, and raw-pointer types do not become Laminar APIs.

This decision explicitly rejects:

- a project-private TidesDB FFI wrapper;
- a TidesDB or `tidesdb-rs` fork or vendored patch series;
- replacing the package's native payload with a newer system or locally built TidesDB library;
- using native TidesDB object storage, checkpoint, or reopen as recovery authority; and
- adding the package to a runtime crate before the bounded package prescreen passes.

The current release is a **starting subject, not an admitted backend**. Its manifest enables only
native 9.3.6, while later native releases contain correctness and recovery fixes. The first gate
must determine whether the restricted Laminar surface is unaffected. If it cannot prove that, work
stops until an official `tidesdb-rs` release carries an acceptable native version and safe surface.
Choosing the package does not waive this gate.

## Product boundary

TidesDB supplies disposable local capacity and latency infrastructure. It does not own:

- vnode assignment, leases, fencing, or rebalance decisions;
- source positions or input replay;
- checkpoint cuts, coordinator Commit decisions, or restore admission;
- sink transactions or end-to-end delivery guarantees; or
- object-store provider selection.

Authoritative state remains a Laminar portable checkpoint written through Rust `object_store` to
S3, Google Cloud Storage, Azure, or a separately qualified shared/durable filesystem. `file://` is
node-local by default and cannot prove recovery from complete local-volume loss. TidesDB's native
remote backend is S3-only and is disabled; its filesystem mode is not cluster authority. A worker
restart initially creates a fresh TidesDB root and restores only a
Commit-admitted Laminar checkpoint, then replays the source from its admitted position. Existing
TidesDB directories are never a correctness shortcut in the first product profile.

Local TidesDB durability is therefore not the exactly-once boundary. Per-batch `FULL`/fsync is not
required for correctness and must not be added to the hot path merely to imitate that boundary.
Exactly-once remains a composition of replayable sources, aligned/checkpointed operator state,
coordinator commit, and a sink with a compatible transactional or idempotent contract. The first
grouped-aggregate vertical may ship only under its explicitly qualified at-least-once connector
matrix; windows, joins, and exactly-once are separate gates.

## Initial physical profile

The first profile has:

- one TidesDB database per worker-local managed-state service;
- one fixed column family, opened once and retained for the database lifetime;
- logical prefixes for pipeline, operator, state table, vnode, ownership generation, and key;
- one coalesced backend command per admitted Arrow batch;
- a single bounded blocking lane, never an async runtime or compute/event-loop thread;
- bounded scans, transactions, iterators, and copied return values; and
- no runtime column-family create/drop, duplicate handle acquisition, callbacks, commit hooks,
  native remote mode, native checkpoints, or prior-directory reopen.

One database or column family per vnode is forbidden. The one-CF shape contains the current
native multi-CF crash-atomicity gap and keeps cleanup and resource attribution in logical prefixes.
Range cleanup may start as bounded scan plus point deletion only if the latency, reclamation, and
disk-amplification gates pass.

## Restricted package facade

The adapter may use only public safe APIs from the accepted official package. No package type may
escape the adapter module. The facade owns a strict tree:

```text
TidesLocalState
└── database
    └── one retained column family
        └── transaction
            └── iterator
```

The facade must make child-before-parent destruction structural, not a comment:

- all package-object construction, use, and destruction occurs on the same owner lane;
- the owner remains alive for the complete blocking-lane lifetime;
- the one column-family handle is acquired once and is never dropped by name during service life;
- transaction and iterator values are confined to one command scope;
- iterator results are copied before that scope ends;
- shutdown stops admission and drains the queue; the owner lane then destroys any iterator/
  transaction, the retained CF, and the database in child-before-parent order, exits, and is joined
  by the caller;
- package values are neither constructed outside nor moved/shared across the lane, even if the
  package declares `Send` or `Sync`;
- panics do not cross native callbacks because callbacks are not registered; and
- stalled shutdown is bounded, observed, and escalated to a process fail-stop rather than unsafe
  forced destruction.

T0 must also prove from exact source that this permitted call sequence is internally safe; usage
containment cannot repair an unsafe package implementation. If these invariants cannot be expressed
and proved using only the package's public safe API, the exact package release fails. Laminar will
not compensate with raw handles, `unsafe`, private FFI, intentional leaks, or a wrapper fork.

## Mutation and visibility contract

One logical backend mutation covers all state changes caused by one admitted Arrow batch. Success
means every intended point mutation, timer/index mutation, and batch marker is visible together.
No output derived from that batch may escape until complete state publication succeeds. Failure
means none is published, or the service enters a fail-stop state before that batch's output, any
later output, or a checkpoint can be admitted. A timeout or unknown native outcome is fatal for that
worker attempt.

The v0.11.1/native-9.3.6 path currently has an unresolved source issue: native multi-operation apply
can return a short non-negative count while callers treat only negative values as failure. It must
be proved that the package's one-CF transactional surface cannot acknowledge a partial Laminar
batch. T0 must obtain source proof, followed by adversarial evidence in T1/qualification, that the
restricted transaction is atomic and its success result covers every operation. A final marker does
not prove preceding mutations landed after a silent short apply. Per-key read-back, a marker-only
publication check, a second database, global serialization across unrelated workers, or a full
checkpoint per batch are not acceptable production repairs. If exact success and atomic visibility
cannot be closed without them, the release fails and Laminar waits for an official upstream fix.

Reads use one bounded command and a transaction/snapshot whose lifetime cannot outlive its owner.
The adapter must prove repeatable point/range visibility, stable iterator bounds, no stale generation
publication, and bounded cancellation/shutdown behavior.

## Hot-path and resource rules

No TidesDB call runs on the compute/event-loop thread. State requests are coalesced per Arrow batch,
sent through a bounded queue, and executed in a dedicated blocking lane. The accepted profile has
exactly one owner lane per worker-local database. Any increase in package concurrency requires a new
profile and fresh thread-safety/source proof, not only a tuning change. Queue wait,
backend service time, state-commit time, checkpoint overlap, and end-to-end batch latency are
measured separately at p50, p95, p99, p99.9, and maximum.

This deliberately exposes one global head-of-line-blocking risk across pipelines and vnodes on a
worker. The C2/C3 hot-writer/disjoint-victim and queue-saturation gates are therefore package kill
criteria: if the single-lane profile misses throughput, p99.9, maximum, or victim-isolation limits,
the release fails. The design does not silently add lanes, databases, or cross-thread package use.

The facade must not allocate or sample expensive engine statistics per key. Metrics are sampled at
a bounded control-plane cadence, except lightweight counters and actual stall transitions. Any
commit-marker check permitted by the mutation contract is one bounded check per Arrow batch and
must pass the same tail-latency gates.

The inspected current engine derives memory configuration from host RAM and applies a five-percent
floor. T0 must verify the exact bundled 9.3.6 behavior; any such behavior is not cgroup-safe.
Qualification must account for and constrain native allocations, cache,
memtables, pinned values/iterators, journals, compression scratch, background jobs, allocator
retention, mmap/page cache, files, temporary checkpoint data, Arrow buffers, and adapter queues.
The container hard limit is authoritative. A package release fails if its public configuration
cannot maintain a proven safety margin under the target cgroup without native modification.

Disk has independent soft and hard watermarks. Admission/backpressure must react before `ENOSPC`;
checkpoint creation, compaction, obsolete files, failed deletion, and restore temporary space all
count. No vendor memory or cache percentage is accepted as a hard limit without cgroup evidence.

## Maintenance-health contract

The accepted maintenance-health v2 semantics remain the product contract. Candidate-native signals
may have a new mapping identity, but cannot be renamed into stronger facts. At minimum the product
needs enough source-proved, low-interference signals to distinguish:

- flush and compaction backlog, running work, progress, and persistent non-progress;
- foreground pressure/stall intervals and reasons;
- background errors, recovery disposition, and poison/fail-stop state;
- obsolete/failed-deletion backlog and disk-growth pressure; and
- memory/disk/FD/queue consumption versus configured limits.

The current TidesDB surface has useful queue, pressure, and progress observations, but exact stalls,
general background errors, and some cleanup/resource facts remain unclosed. The prescreen must map
only signals exposed through the package's safe public API; OS/process/cgroup observations may
supplement them but cannot invent internal errors or stalls. If the mandatory contract needs a package fork, native
patch, callback race, raw handle, or private FFI, the release fails and waits upstream. The contract
is not weakened to make the chosen dependency pass.

## Bounded package prescreen

### T0 — exact-source and safe-subset closure

- **Cap:** one working day, at most eight engineer-hours, zero candidate machine-hours.
- Freeze the release tag, commit, crate checksum, lock resolution, features, native source archive,
  native tag/commit, toolchain, license/SBOM, and supported target matrix.
- Reconcile every native correctness/recovery fix after 9.3.6 against the one-DB/one-CF,
  fresh-root, no-native-checkpoint/reopen surface.
- Prove the owner tree, duplicate-CF prohibition, transaction/iterator scope, thread confinement,
  close order, atomic-success semantics, memory settings, and required maintenance signals from
  exact source.

T0 stops immediately if any required invariant depends on a fork, patch, private FFI, raw handle,
callback, system-library substitution, or an assumed upgrade to native 9.3.14. A relevant missing
post-9.3.6 correctness fix is an automatic stop for v0.11.1.

### T1 — isolated official-package feasibility

- **Cap:** two working days and four machine-hours.
- **Location:** a disposable isolated workspace, not a Laminar runtime crate.
- Build and verify one exact release-configuration package/features subject in an isolated Linux
  feasibility environment. WSL2 or a Linux container is acceptable for T1, but cannot satisfy the
  later native Linux/XFS/NVMe qualification or soak target evidence. Native Windows is non-gating
  unless separately made a product target.
- Exercise same-lane fresh-root construction, one retained CF, basic point/batch operations, bounded
  iteration with copied outputs, deterministic child-before-parent close, and repeated create/drop.
- Run one separately identified instrumented diagnostic build within the cap. Its toolchain, flags,
  and digest must not be confused with the exact release-config subject; sanitizer success cannot
  qualify that production binary. Unsupported or additional sanitizers become explicit later
  qualification gaps, not passes.
- Measure only feasibility and gross interference; this is not a benchmark or qualification result.

T1 stops on a crash, leak, race, hang, ambiguous acknowledged mutation, unsafe workaround, package
type escaping the facade, or inability to build the exact official subject reproducibly. A stopped
release is not replaced silently. Wait for a newer official `tidesdb-rs`, freeze it, and repeat T0.

Passing T0/T1 authorizes only a separately reviewed adapter/profile proposal. It does not authorize
runtime integration, candidate qualification, or cluster admission.

## Qualification after T0/T1

For an accepted exact package release:

1. freeze a TidesDB successor profile and signal mapping with new identities; immutable v1-v4
   Fjall/RocksDB artifacts remain regression history and cannot be relabelled;
2. implement the smallest one-CF facade and in-memory differential oracle;
3. run model/differential tests and open-loop C1/C2/C3 workloads, including Zipf/hot-key skew,
   hot-writer/disjoint-vnode victim isolation, compaction, checkpoint overlap, queue saturation, and
   p99.9/maximum gates;
4. run cgroup memory, disk/FD, `ENOSPC`, corruption, externally injectable process/storage/syscall
   boundary kills, torn/short operation,
   local-directory loss, portable restore, and stale-owner fencing tests;
5. run 24-hour and 72-hour backend endurance with leak, debt, cleanup, and latency-slope gates;
6. integrate one grouped-aggregate at-least-once vertical behind disabled admission, then prove
   recovery/rebalance before considering windows, joins, or exactly-once; and
7. freeze a release candidate and have an independent operator run the full soak from immutable
   instructions and artifacts. The implementation team cannot self-certify this gate.

Every phase produces raw evidence, an explicit pass/fail/blocked verdict, and a six-part review for
AI slop, overengineering/hot-path cost, unused code, production readiness, documentation, and tests.
A benchmark win, successful smoke test, or vendor Zipf result cannot replace any gate.

## Stop and fallback policy

The TidesDB track stops for the exact release when:

- the official package cannot provide a safe contained owner/lifetime surface;
- any relevant native 9.3.6 correctness fix is missing;
- acknowledged partial mutation or ambiguous visibility remains possible;
- memory, maintenance health, shutdown, or target-platform control requires non-public/native work;
- a backend-attributable common absolute latency, isolation, resource, fault, restore, endurance, or
  independent-soak gate fails.

A soak failure caused by coordinator, connector, environment, or harness defects invalidates the
run and blocks production pending cause analysis; it disqualifies the package only when the failure
is attributable to the package/backend path.

On stop, `[LDB-4007]` and `[LDB-0013]` remain fail-closed. No alternative activates automatically.
The next action is either to wait for and re-audit a newer official `tidesdb-rs` release or record a
new owner decision to reconsider RocksDB or another candidate. Bounded memory remains reference-only.

## Consequences

This direction honors the selected TidesDB package and removes the maintenance burden of a private
FFI or engine fork. It also makes upstream package quality and release cadence explicit schedule
dependencies. One-CF/fresh-root confinement reduces the first safety surface but does not prove
atomicity, memory governance, maintenance health, low tail latency, exactly-once delivery, or
production readiness. Those remain measured gates, culminating in the independent soak.
