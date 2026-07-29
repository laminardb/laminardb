# Fjall 3.1.8 worker-lifecycle empirical closure

- **Date:** 2026-07-29
- **Scope:** one deterministic stock lifecycle reproduction and one validation-only counterpatch;
  no adapter, benchmark, soak, or runtime dependency
- **Stock subject:** official `fjall = "=3.1.8"` with default features, tag commit
  `6debe706dbc53d6d0eb666aae5057671d5c1370f`
- **Stock result:** **CONFIRMED RELEASED WORKER-ERROR/DROP DEFECT**
- **Patch result:** **CLOSES THE REPRODUCED COUNTER-LEAK BRANCH ONLY**
- **Backend/fork verdict:** **NO BACKEND SELECTED; DO NOT CARRY A PRODUCTION FORK**
- **Production/admission:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision

The source-derived Fjall 3.1.8 lifecycle hazard is now empirically confirmed. A public compaction
filter returned `LsmError::Unrecoverable` from Fjall's sole background worker. The database became
observably poisoned, but destruction did not return within the parent's 15-second absolute
deadline. The parent killed and reaped the child after it had emitted all four required pre-drop
markers and no `DROP_RETURNED` marker:

```text
FIRST_COMPACTION_COMPLETE
FILTER_ERROR_RETURNED
WORKER_POISON_OBSERVED
DROP_BEGIN
```

The ordinary stock control wrote, returned from Drop, reopened the same root, verified its value,
and exited zero. The result therefore distinguishes the worker-error teardown defect from a general
Fjall, filesystem, process-supervisor, or Windows harness failure.

A validation-only patch based on the exact 3.1.8 tag counts each spawn attempt independently and
uses a worker-lifetime RAII guard to decrement on normal return, returned error, or unwind. The same
fault child then emitted `DROP_RETURNED` and exited zero. The final patch touches only
`src/worker_pool.rs` (44 insertions, 17 deletions; stable patch ID
`9c4f77cb68b152e76f7afa27988bb4bfe93d01ec`). It adds no record-path branch, lock, allocation, or
I/O: the extra work is one relaxed atomic increment per worker spawn and one decrement per worker
exit.

That counterpatch is suitable material for a small upstream PR, but it does **not** satisfy
LaminarDB's complete all-mode lifecycle contract. `DatabaseInner::drop` still has no absolute
deadline, fallible close result, or joined worker outcome. It can still wait indefinitely if a
live worker is stuck in synchronous I/O or cannot consume the bounded work queue. The separate
warning-only maintenance-error signal gap also remains. Process termination is containment for a
server deployment, not reusable embedded/single-node lifecycle. Stock 3.1.8 therefore remains
ineligible, and a long-lived Laminar fork would make this project responsible for broader database
lifecycle work without closing the production gate.

## Frozen subject and environment

| Item | Identity |
|---|---|
| Fjall crate | 3.1.8; SHA-256 `420a84699b8ccbb1ed573e38e88f4f23637b45beab6432066452f834be469c57` |
| Fjall source tag | `3.1.8`; commit `6debe706dbc53d6d0eb666aae5057671d5c1370f` |
| `lsm-tree` crate | 3.1.8; SHA-256 `055a908d502129cf63bedae52f2db222e4436d2da32a69df9b84ac9fb9147761` |
| Harness source | SHA-256 `f52a77fabbe75083017299bb6c4bb4e133f0d8f6db79a9c362d62dcad8a7896b` |
| Rust toolchain | rustc/cargo 1.96.0, `x86_64-pc-windows-msvc` |
| Host | Windows 11 Pro 10.0.26200 build 26200; AMD Ryzen 9 7900X, 12 cores / 24 logical CPUs |

The isolated harness had its own lockfile and resolved Fjall and `lsm-tree` at exact 3.1.8. The stock
crate was built from the registry checksum above. The patched countertrial used a shallow clone of
the official tag. The repair diff and stable patch ID above are computed over
`src/worker_pool.rs` only. An empty `[workspace]` table was also added to the cloned manifest solely
so Cargo could isolate the nested validation build; the whole validation-clone diff has patch ID
`a9fab4dbfd3c2f7d29c42acb10039dd20a92e9db`, while that non-behavioral manifest marker is excluded
from the proposed upstream repair. The final executable SHA-256 was
`4c60c0668352d19e0a8bdd2cb0ce52a2ebb5ae3c8b10219ae0abc49cd187c585`.
No root workspace manifest or lockfile changed.

## Deterministic protocol

Both arms used one worker and a public compaction-filter factory on one keyspace. Leveled
compaction used an L0 threshold of one. The first write/rotation established one completed
compaction. That compaction may use `lsm-tree`'s legitimate metadata-move optimization, so filter
invocation was not required at this phase. The second write used the same key, forcing overlap with
the first table. After arming, the filter emitted its marker and returned
`LsmError::Unrecoverable` from that actual merge.

The child then polled only the public `Database::persist(PersistMode::Buffer)` result until it
observed `fjall::Error::Poisoned`, dropped the keyspace, emitted `DROP_BEGIN`, and destroyed the
database. Marker writes were flushed. Each case ran in a fresh root as a real child process. Build
and link time were excluded from the 15-second absolute execution deadline; a timed-out child was
force-terminated and reaped. An internal five-second absolute deadline classified failure to reach
the filter/poison branch as a harness failure rather than a Fjall verdict.

Direct Windows execution was sufficient. Docker/WSL was not needed because the public filter makes
the exact worker-error branch deterministic without filesystem permission or capacity injection.
These elapsed times are harness diagnostics, not latency measurements.

## Results

| Subject/case | Result | Evidence |
|---|---:|---|
| Stock baseline | PASS; exit 0 in 411 ms | `BASELINE_WRITE_COMPLETE`, `DROP_BEGIN`, `DROP_RETURNED`, `REOPEN_OK`; empty stderr |
| Stock fault | DEFECT CONFIRMED; parent timeout/kill at 15,030 ms | All four required pre-drop markers; no `DROP_RETURNED`; empty stderr; child reaped |
| Final patched baseline | PASS; exit 0 in 385 ms | Same baseline markers and clean reopen; empty stderr |
| Final patched fault | NARROW PATCH PASS; exit 0 in 359 ms | Same fault markers plus `DROP_RETURNED`; empty stderr |

An earlier fault attempt exited with a harness timeout before the first marker because it
incorrectly required the first metadata-move compaction to invoke the filter. It produced no Fjall
lifecycle verdict. The corrected overlap protocol was rebuilt and both stock arms were rerun from
fresh roots before the patch countertrial.

## Patch boundary and upstream requirement

The smallest reviewed repair is entirely Rust code in Fjall's private worker pool:

1. prepare a worker's captured state;
2. increment the active counter immediately before that individual spawn attempt;
3. roll back the increment if the spawn fails;
4. install one RAII exit guard at the top of every successfully spawned worker;
5. on unwind, publish poison before decrementing the active count; and
6. remove the normal-close-only decrement so the guard owns every exit path.

Before an upstream PR is production-complete it needs deterministic returned-error, panic, and
partial-spawn tests. The returned-error test must use an external child deadline so a regression
cannot wedge CI. The panic and partial-spawn branches were source-reviewed but were not executed in
this bounded Laminar validation.

Even after that patch lands, an official successor must separately address or explicitly expose the
remaining lifecycle boundary: cooperative shutdown independent of the work queue, observable
worker results, a fallible close operation, and a deadline/cancellation contract. No in-process
Rust API can forcibly and safely reclaim a thread stuck in an uninterruptible system call; if hard
termination is the only guarantee, the architecture must acknowledge process isolation and its IPC,
deployment, latency, checkpoint, and delivery consequences. LaminarDB does not adopt that larger
architecture here.

The official [Fjall 3.1.8 release](https://github.com/fjall-rs/fjall/releases/tag/3.1.8), immutable
[`WorkerPool::start`](https://github.com/fjall-rs/fjall/blob/6debe706dbc53d6d0eb666aae5057671d5c1370f/src/worker_pool.rs#L58-L114), and
[`DatabaseInner::drop`](https://github.com/fjall-rs/fjall/blob/6debe706dbc53d6d0eb666aae5057671d5c1370f/src/db.rs#L63-L78)
remain the stock source authority. The separate queue-pressure teardown defect remains tracked in
[Fjall issue 260](https://github.com/fjall-rs/fjall/issues/260).

## Scope exclusions and next gate

This was not backend qualification. It did not test atomic state-table semantics, portable export/
restore, vnode cleanup/reclamation, memory/disk/FD limits, uniform or Zipf load, windows/timers,
joins, checkpoint overlap, rebalance, crash recovery, p99/p99.9 latency, endurance, source/sink
delivery, existing cluster failover/ALO/EO soaks, or the independent release-candidate soak. It
does not authorize a Fjall dependency, fork, adapter, runtime selector, object-store coupling, or
admission change.

The actionable result is narrow: submit the exit-accounting repair upstream if external repository
work is authorized, but do not wait for it or build a Laminar fork. Continue backend-neutral core
work. A future official Fjall release may receive a bounded re-entry only after its released source
closes the complete lifecycle and maintenance-health entry gates; passing those gates would permit
qualification, not production admission.

## End-of-cycle review

- **AI slop and overengineering:** pass after correction. The first invalid trigger is disclosed and
  contributes no Fjall verdict. Work stopped after one deterministic returned-error reproduction
  and one one-file repair countertrial; no benchmark, generic adapter, observer, sidecar, or backend
  survey was added.
- **Hot path and latency:** unchanged in LaminarDB. The proposed Fjall change runs only at worker
  spawn/exit. The four elapsed values are functional supervisor diagnostics, not throughput or tail-
  latency evidence.
- **Unused code and cleanup:** pass. The harness, clone, lockfile, roots, and transcripts lived only
  in ignored `/tools/fjall-lifecycle`; they are removed after evidence capture. No runtime helper,
  dependency, feature, public API, or unused patch is tracked in LaminarDB.
- **Production readiness:** **BLOCK**. The patch does not create a fallible/deadline-bound close or
  joined worker result and does not close queue pressure, maintenance visibility, resource,
  checkpoint/rebalance, delivery, or all-mode qualification. No backend or fork is selected.
- **Documentation:** pass after reconciliation. This is the single new empirical authority; the
  dated source-only report retains its original scope with a follow-up pointer, and current ADR/
  plan/selection surfaces carry only the changed disposition.
- **Tests:** pass for the exact returned-error question. Stock baseline/fault and final patched
  baseline/fault ran as fresh child processes; formatting, exact resolution, build, and harness
  warnings-denied Clippy passed. The earlier first-marker timeout was harness-invalid. Panic,
  partial-spawn, multi-worker, real I/O failure, broad backend qualification, failover/ALO/EO
  recertification, performance, endurance, and independent soak were not run.

## Next-cycle review plan

The next backend-neutral core cycle or future official-release re-entry must again remove
speculative/unused helpers, audit batch and lifecycle hot paths for blocking and allocation, verify
all-mode checkpoint/rebalance and delivery failure boundaries, keep one current evidence authority,
run only the affected focused/broad matrices actually authorized, and leave independent immutable-
candidate soak as a separate final production gate.
