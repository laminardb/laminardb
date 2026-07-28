# TidesDB local working-state design

- **Status:** Historical stopped candidate design; superseded by ADR-008's Fjall 3.1.8 priority
  amendment
- **Date:** 2026-07-25
- **Last reconciled:** 2026-07-27 during Core Cycle 1
- **Selected integration line:** the official `tidesdb/tidesdb-rs` binding, published on crates.io
  as package and library `tidesdb`; the unrelated `tidesdb-rs` crate is excluded
- **Stopped exact prescreen subject:** Cargo package `tidesdb v0.11.1`, tag commit
  `e2febbc548e7f0158d1c09ea487aa0bb7c343616`, with its default
  `tidesdb-src-v9-3-6 = 0.1` native source path
- **Runtime dependency added:** no
- **Production verdict:** **NO-GO** until package closure, qualification, integration, and the
  independent soak all pass
- **Related evidence:** [TidesDB T0 source closure](../reports/tidesdb-rs-t0-source-closure-2026-07-25.md),
  [TidesDB static prescreen](../reports/tidesdb-static-prescreen-2026-07-25.md), and
  [ADR-008](ADR-008-managed-vnode-keyed-state.md)

**2026-07-28 supersession:** [ADR-008](ADR-008-managed-vnode-keyed-state.md#2026-07-28-fjall-318-priority-amendment)
makes stock official Fjall 3.1.8 the sole preferred worker-local qualification-entry subject. This
file preserves the exact Cycle 40 design and Cycle 41 T0 stop as decision evidence; its
present-tense TidesDB work order is no longer current. No TidesDB package, fork, upstream-wait task,
adapter, or qualification run is scheduled.

## Decision

At Cycle 40 LaminarDB chose to pursue TidesDB for worker-local keyed state through the official
[`tidesdb/tidesdb-rs`](https://github.com/tidesdb/tidesdb-rs) binding, whose Cargo package and
library are named `tidesdb`. The package is a private
implementation dependency behind a Laminar-owned safe facade; its database, column-family,
transaction, iterator, callback, and raw-pointer types do not become Laminar APIs.

The crates.io package literally named `tidesdb-rs` is a separate third-party wrapper from
`0x6flab`. It is not selected, copied into Laminar, or combined with the official binding.

This decision explicitly rejects for LaminarDB's dependency graph:

- a project-private TidesDB FFI wrapper;
- a TidesDB or `tidesdb-rs` fork or vendored patch series;
- replacing the package's native payload with a newer system or locally built TidesDB library;
- using native TidesDB object storage, checkpoint, or reopen as recovery authority; and
- adding the package to a runtime crate before the bounded package prescreen passes.

A short-lived contributor fork is permitted only to propose a focused upstream PR to the official
`tidesdb/tidesdb-rs` repository. LaminarDB never depends on that fork, vendors it, or treats its CI
as qualification. PR preparation is capped at one engineer-day and one local machine-hour and may
cover only the native-version update, exact transaction outcome, and minimum fatal/stall/close
signals. Only a merged, officially published successor package can re-enter T0. A successor release
is the planned path, but neither the contributor branch nor that expectation has qualification or
runtime authority.

The current release was a **prescreen subject, not an admitted backend**, and failed T0 in Cycle 41.
Its native 9.3.6 payload predates published memtable-corruption, stats/read concurrency, recovery,
flush-rotation, and iterator fixes that an outer adapter cannot supply. Exact source also proves
that a multi-operation transaction can acknowledge a short partial batch. A Laminar pre-output
verification/fail-stop protocol could theoretically contain that acknowledgement gap, but it is an
unaccepted, latency-sensitive design option rather than a fix. Host-derived memory resolution and
the incomplete public maintenance-health surface are additional stops. Work waits for a new
official Cargo package with a reconciled native payload and a repeated T0. T1, a runtime dependency,
and TidesDB adapter code are not authorized for 0.11.1.

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

The adapter may use only public safe APIs from the accepted official binding. No package type may
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

Cycle 41 resolved the v0.11.1/native-9.3.6 source issue as a failure: native multi-operation apply
can return a short non-negative count, its one-CF transaction callers treat only negative values as
failure, and the Rust package returns `Ok(())`. The restricted transaction therefore can acknowledge
a partial Laminar batch. A final marker alone does not prove preceding mutations landed after a
silent short apply. Two containment designs remain possible only because local state is disposable
and the owner lane can forbid output/checkpoint publication until verification finishes: read back
every distinct final key and fail-stop on mismatch, or use single-mutation transactions and
fail-stop on any error/crash. The former adds O(touched keys) point reads and copies per Arrow batch;
the latter multiplies transaction/WAL overhead. Neither is accepted without source proof, fault
tests, and strict p99.9/maximum evidence. A second database, global serialization across unrelated
workers, or a full checkpoint per batch remains rejected. Native 9.3.14 retains the mismatch.

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
a bounded control-plane cadence, except lightweight counters and actual stall transitions. The only
contemplated verified-commit shim opens a fresh transaction and reads/copies every distinct touched
key; it is not a marker check or part of the admitted normal path. Its extra queue occupancy, FFI
calls, allocations, bytes, service time, and end-to-end tails must pass the same latency gates before
that shim can be accepted.

The inspected current engine derives memory configuration from host RAM and applies a five-percent
floor; Cycle 41 verified that behavior for the bundled 9.3.6 source. It is not a general cgroup
governor. A future constrained profile must use the exact `H`/`C`/`F`/`E`/`R` admission formula in
the [T0 source closure](../reports/tidesdb-rs-t0-source-closure-2026-07-25.md), prohibit auto mode,
reject missing/unlimited cgroup-v2 authority, verify the resolved limit at startup, and fail closed
unless qualification has proved the complete non-engine and unaccounted-engine reserve.
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

The official binding has useful queue, pressure, and progress observations, but exact stalls,
general background errors, and some cleanup/resource facts remain unclosed. Laminar may add truthful
owner-queue, call-latency, cgroup, PSI, disk, and FD facts; it may not relabel those as missing native
error facts. The prescreen must map
only signals exposed through the package's safe public API; OS/process/cgroup observations may
supplement them but cannot invent internal errors or stalls. If the mandatory contract needs a package fork, native
patch, callback race, raw handle, or private FFI, the release fails and waits upstream. The contract
is not weakened to make the chosen dependency pass.

## Bounded package prescreen

### T0 — exact-source and safe-subset closure

- **Cap:** one working day, at most four engineer-hours, zero candidate machine-hours.
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

**Cycle 41 result: STOP_WAIT_FOR_UPSTREAM.** Crate/tag/nested-archive attribution and restricted
owner/lifetime containment closed. The exact package is stopped by relevant post-9.3.6 native
correctness/memory-safety fixes that Laminar cannot add, plus the short-transaction acknowledgement,
the inability to guarantee the general cgroup envelope, and missing mandatory
stall/background-error/cleanup/reaper facts. A verified-commit/fail-stop shim is retained only as a
future measured option for the short-transaction gap; it does not cure the other stops. The full
evidence and re-entry contract are in the
[T0 source-closure report](../reports/tidesdb-rs-t0-source-closure-2026-07-25.md).

### T1 — isolated official-binding feasibility (not authorized for v0.11.1)

- **Cap:** one engineer-day and one candidate machine-hour.
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
release is not replaced silently. Wait for a newer official Cargo package `tidesdb`, freeze it, and
repeat T0.

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

## Historical Cycle 41 stop and fallback policy

The TidesDB track stops for the exact release when:

- the official package cannot provide a safe contained owner/lifetime surface;
- any relevant native 9.3.6 correctness fix is missing;
- acknowledged partial mutation or ambiguous visibility remains possible without an accepted
  pre-output verification/fail-stop boundary;
- memory, maintenance health, shutdown, or target-platform control requires non-public/native work;
- a backend-attributable common absolute latency, isolation, resource, fault, restore, endurance, or
  independent-soak gate fails.

A soak failure caused by coordinator, connector, environment, or harness defects invalidates the
run and blocks production pending cause analysis; it disqualifies the package only when the failure
is attributable to the package/backend path.

On stop, `[LDB-4007]` and `[LDB-0013]` remain fail-closed. No alternative activates automatically.
At Cycle 41 the next action was to wait for a new official Cargo package `tidesdb`, freeze that exact
pair, and repeat T0. The 2026-07-28 Fjall amendment cancels that scheduled wait. A new owner decision
is required to restart TidesDB or reconsider another candidate. Bounded memory remains
reference-only.

## Consequences

This historical direction honored the selected TidesDB package and removed the maintenance burden of a private
FFI or engine fork. It makes upstream transaction semantics, package surface, and release cadence
explicit schedule dependencies. Cycle 41 proves that one-CF/fresh-root confinement can contain the
ownership tree. It can support a future fail-stop verification design for ambiguous mutation, but
cannot repair native memory corruption or manufacture missing internal maintenance-health facts;
host-derived memory behavior also requires a constrained startup-admission formula. Low tail
latency, exactly-once delivery, qualification, and the independent soak remain unstarted later
gates, not implied failures or passes.
