# Fjall 3.1.8 adapter-entry source closure

- **Date:** 2026-07-28
- **Scope:** one engineer-day, read-only source/contract closure; zero candidate machine-hours
- **Subject:** stock official `fjall` 3.1.8, tag commit
  `6debe706dbc53d6d0eb666aae5057671d5c1370f`
- **Result:** **STOP before dependency, adapter, or candidate execution**
- **Disposition:** `OBSERVED_DESIGN_UNSUPPORTED_IN_STOCK_SOURCE`
- **Production/admission:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Decision

Stock Fjall 3.1.8 is disqualified from LaminarDB's managed local-spill adapter entry. This is not a
claim that Fjall cannot store data, that every use of it is unsafe, or that a newer official release
cannot qualify. It means this exact stock release cannot meet LaminarDB's all-mode production
lifecycle contract without an upstream change, a private patch/fork, or a new process-isolation
architecture.

The smallest unavoidable veto is bounded teardown after a background-worker failure. Fjall counts
the configured worker pool before spawning it. A worker decrements that count only when
`worker_tick` returns `Ok(true)`—normally the `Close` path—while the ordinary `Err` path poisons the
database and exits without decrementing.
`DatabaseInner::drop` then waits without a deadline until the count reaches zero. A real background
I/O or maintenance error can therefore poison the database and make orderly database destruction
wait forever. A partial thread-spawn failure can leave the same accounting mismatch.

LaminarDB cannot repair that invariant through Fjall's stock public API. A foreground timeout does
not cancel the already-started native call or the database destructor. A Laminar-owned sticky error
latch can stop new work but cannot reconcile Fjall's private worker count. Leaking the database or
aborting the process is not an acceptable lifecycle for embedded and single-node modes, and a
storage sidecar would be a materially different architecture rather than the smallest adapter.

The review therefore stops before a build or runtime experiment. A stock Fjall child process plus
an induced filesystem/I/O failure and a bounded parent timeout could corroborate a teardown hang,
but that run was outside this authorized zero-machine-hour closure and cannot negate the source
proof. Uniform or Zipf workloads would measure only the common path and cannot create the missing
exit guard.

## Exact source trace

The official [Fjall 3.1.8 release](https://github.com/fjall-rs/fjall/releases/tag/3.1.8) resolves to
the commit above. Its manifest declares Rust 1.90 and `lsm-tree = "~3.1.8"`; this review inspected
the stock source with `lsm-tree` 3.1.8. Any later re-entry must freeze both exact sources rather than
inherit a moving compatible dependency resolution.

The decisive path is:

1. [`WorkerPool::start`](https://github.com/fjall-rs/fjall/blob/6debe706dbc53d6d0eb666aae5057671d5c1370f/src/worker_pool.rs#L58-L114)
   adds the complete configured pool size to `active_thread_counter` before spawning threads.
2. The normal `Close` branch decrements the counter, but the `worker_tick` `Err` branch logs,
   poisons, and returns without decrementing it. `PoisonDart` also poisons on panic but is not an
   exit-count guard.
3. [`DatabaseInner::drop`](https://github.com/fjall-rs/fjall/blob/6debe706dbc53d6d0eb666aae5057671d5c1370f/src/db.rs#L63-L78)
   repeatedly sends `Close` and sleeps while that private counter is nonzero. It has no deadline,
   cancellation, fallible close result, or public recovery hook.
4. The pre-increment also makes an early partial spawn failure unsafe: the failed spawn subtracts
   one, but collection stops at the first error, leaving later not-yet-attempted slots represented
   in the count. Failure on the final (or sole) slot does not create those phantom slots.

This is a source-proven reachable invariant violation: `worker_tick` propagates fallible journal,
rotation, flush, compaction, and journal-maintenance operations. A particular filesystem fault is
not required to prove that the returned-error branch fails to discharge its worker lifetime.

## Frozen maintenance-health contract result

The existing v2 contract separately requires a truthful production-minimal pressure signal and a
complete background-failure signal for every enabled asynchronous maintenance mechanism. Whole-arm
N/A is false because Fjall starts maintenance workers.

Fjall 3.1.8 also fails that precommitted contract. During automatic memtable rotation,
[`inner_rotate_memtable`](https://github.com/fjall-rs/fjall/blob/6debe706dbc53d6d0eb666aae5057671d5c1370f/src/keyspace/mod.rs#L755-L787)
catches a fallible version-history maintenance error, emits a warning, and continues successfully.
The error therefore does not reach the worker poison path. The stable public API exposes neither
that failure nor an exhaustive background-error state/counter. `Database::persist()` can reveal the
private poison state indirectly, but this warning-only path never poisons it.

A separate adversarial maintainability analysis challenged treating every cleanup warning as a
universal engine veto: dedicated-root bytes/file counts, quotas, tail behavior, and endurance tests can expose
the consequential resource growth. That is a reasonable simplification for a future contract. It
does not reverse this decision because the bounded-teardown defect above remains under the narrower
contract.

## Supporting risks, not the primary veto

These findings raise the cost of a future qualification but are not used alone to reject 3.1.8:

- automatic rotation and post-flush compaction notifications use best-effort
  `try_send(...).ok()` on a private bounded queue;
- `max_write_buffer_size` stores a deprecated configuration value with no enforcement reader in the
  exact source, while `max_journaling_size` is a maintenance trigger rather than a hard disk cap;
- foreground write-halt loops have no cancellation/deadline check;
- useful pressure counters are explicitly experimental/`#[doc(hidden)]` and remain incomplete as a
  worker-liveness or background-error surface; and
- some foreground journal-write errors are not converted into Fjall poison, although a Laminar
  adapter could conservatively latch every returned backend error.

Laminar admission, one composite-key keyspace, a bounded blocking lane, cgroup/filesystem limits,
foreground latency, and a wrapper-owned fatal latch could contain parts of this list. They cannot
make the private teardown accounting correct.

## Workaround assessment

| Proposed stock-public workaround | Result |
|---|---|
| Poll `Database::persist(PersistMode::Buffer)` | Detects propagated poison only; it neither detects warning-only maintenance failure nor repairs worker accounting. |
| Pin hidden pressure counters | Useful candidate diagnostics, but no count represents a departed worker or makes `drop` bounded. |
| Latch every returned Fjall error in Laminar | Correct fail-stop policy for a future adapter; cannot terminate/reclaim the poisoned Fjall instance safely. |
| Bound the adapter queue and call duration | Bounds work before dispatch and detects a late call; cannot cancel the synchronous call or destructor. |
| Enforce cgroup and filesystem quotas | Provides last-resort process/resource containment, not orderly all-mode database lifecycle. |
| Capture warning logs | Not a stable, typed, exhaustive error API and unrelated to the counter leak. |
| Leak the database or abort the process | Violates reusable embedded/single-node lifecycle and graceful failover requirements. |
| Put Fjall in a killable sidecar | Could contain hangs, but adds IPC, deployment, failure, latency, and exactly-once boundaries; not a minimal backend adapter. |
| Carry a Laminar fork/private patch | Could fix the source but conflicts with the explicit no-fork decision and creates ongoing database ownership. |

## Gate result

| Entry question | Result | Consequence |
|---|---|---|
| Required KV primitive shapes | **PASS on source shape** | Atomic batches, consistent snapshots, point access, and ordered ranges justify considering the engine. |
| Bounded failure and close lifecycle | **FAIL** | Fatal worker exit can leave `DatabaseInner::drop` unbounded. This is the decisive stock-source veto. |
| Frozen v2 background-failure coverage | **FAIL** | Warning-only async maintenance failures have no complete stable public signal. |
| Hard native memory/disk bounds | **FAIL natively; partly wrapper-containable** | Laminar admission and external limits would still require empirical proof. |
| Hot-path, crash, resource, and endurance evidence | **NOT RUN** | A source veto prevents spending target-machine time. Absence of a run is not a performance result. |
| Adapter/runtime admission | **NOT AUTHORIZED** | No dependency or losing adapter is added. |

## Re-entry conditions

A later official Fjall release may receive a new bounded source check if it provides all of the
following in stock source:

1. count only successfully spawned workers;
2. use an exit guard that decrements on normal close, returned error, and panic;
3. join workers or otherwise prove a bounded, observable shutdown result; and
4. include fault tests for worker error, panic, and partial spawn failure.

The maintenance-health mapping must then be re-evaluated against the contract current at that time.
Only a source pass may authorize a minimal adapter and empirical uniform/Zipf aggregate,
window/timer, join-family, checkpoint/restore, rebalance, crash, pressure, tail-latency, and
endurance qualification. Existing cluster failover, ALO, and EO-eligible regressions and a separate
independently operated release-candidate soak remain mandatory after integration.

The next backend decision is separate. This stop does not select TidesDB or prove its current
official Rust/native package suitable. It returns work to the owner's stated TidesDB pivot, which
still needs a separately scoped, bounded re-entry rather than silently activating a fallback.

## Review record

Three parallel read-only agent analyses reached **STOP**. One selected the warning-only
maintenance error as the first frozen-contract veto, one selected native resource/cancellation
gaps, and one deliberately relaxed the telemetry interpretation. The relaxed review still found
the worker-exit/drop accounting defect unavoidable. This report therefore uses that shared narrow
finding as the primary decision and preserves the other observations as contract-specific or
supporting evidence.

No Rust source, manifest, lockfile, runtime behavior, admission rule, soak artifact, or
qualification runner changed during this closure.
