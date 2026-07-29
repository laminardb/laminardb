# Distributed keyed state — Core Cycle 11 review

- **Date:** 2026-07-29
- **Implementation commits:** `8a4ed834`, `ca86af54`
- **Scope:** own the current acquired-subset raw restore-input envelope through load, staging,
  publication, terminal failure, launch failure, and replacement
- **Slice verdict:** **PASS FOR CURRENT-PROFILE RAW RESTORE-INPUT OWNERSHIP**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and result

The existing versioned restore contract remains the durable authority. Core Cycle 11 adds one
private worker resource owner for its current `global_singleton_compatibility` profile:

- metadata preflight derives the exact requested vnode subset's declared lineage payload bytes and
  artifact count before the first body GET;
- both dimensions are atomically reserved from the committed worker limits, or the load fails
  without reading a body;
- one non-cloneable RAII charge follows the loaded `Bytes` into the immutable
  `PendingVnodeTransition`;
- every body read shares one 32-permit worker semaphore and the caller's original absolute deadline
  and cancellation token; and
- exact-`Arc` cleanup releases abandoned input without consuming a callback- or watcher-installed
  replacement.

Boot recovery now derives one absolute deadline from the configured checkpoint timeout. Live
assignment adoption reuses its already-established absolute deadline. Dropping either future also
drops an in-flight backend future; `StateBackend::read_sealed_partial_bounded` now documents that
implementations must not detach work after future cancellation.

The reservation transfers through these ownership states:

`worker budget -> LoadedVnodeChains -> PendingVnodeTransition -> publication or exact retirement`

Ordinary validation/read errors drop the loaded owner. Successful publication clears the exact
pending slot. For transitions that own raw restore input, terminal callback/publication failure
poisons the graph, clears installed authority, and retires only the exact failed transition. A graph
that fails after recovery but before launch retires its exact transition while leaving vnodes
`Restoring`; the next generation must reload the durable cut before intake can open. A revoke-only
final-owner exit owns no restore-input charge and retains its durable staging authority after an
indeterminate failure. Boot replacement validates durable target/backend authority before retiring
prior input, then releases the last old observer before acquiring the replacement charge, avoiding
full-budget self-deadlock.

Recovery publication is the last await/fallible boundary before the caller installs the failed-
launch guard. A coordinator-less fresh-start check was moved ahead of publication to close the
cancellation gap.

No row hot path changed. An independent review rejected an initial implementation that locked the
pending-transition slot and retained its `Arc` on every cluster graph cycle. Cleanup is now scoped
to the synchronous rare transition path and reuses the snapshot that transition preparation
already requires.

No state backend, runtime selector, public API, compatibility bridge, generic quota framework,
operator admission, source/sink mode, delivery guarantee, or soak/certification helper was added.

## Verification

| Check | Result |
|---|---:|
| `cargo check -p laminar-db --no-default-features --features cluster` | passed |
| `cargo check -p laminar-db --no-default-features` | passed |
| serialized cluster lib-test build, `--lib --no-run -j 1` | passed |
| restore-input resource lifecycle | passed, 6/6 |
| vnode-transition staging | passed, 12/12 |
| boot vnode-recovery lifecycle | passed, 16/16 |
| terminal publication/restore poison filter | passed, 3/3 |
| transport-drift exact retirement | passed, 1/1 |
| callback-installed replacement preservation | passed, 1/1 |
| successful publication charge release | passed, 1/1 |
| revoke-only final-owner authority | passed, 11/11 |
| cluster library Clippy with `-D warnings` | passed |
| no-default library Clippy with `-D warnings` | passed |
| formatting and diff hygiene | passed |
| broad full workspace/integration matrix | **not run** |
| prior cluster failover/ALO/EO engineering soaks on this binary | **not run; paused** |
| independent immutable release-candidate soak | **not run; required before production** |

The resource tests prove zero body reads when bytes or artifacts cannot be reserved; atomic retry
after owner drop; shared permit exhaustion/reuse; entry cancellation/deadline behavior; and
cancellation after a blocking backend body read has entered. The last case completes under a hard
test timeout and observes zero active backend reads, zero reserved bytes/artifacts, and reuse of all
32 request permits.

The Windows debug unit binary linked successfully in a serialized `--lib -j 1` build. A broad
integration-target invocation is intentionally not claimed: this host previously exhausted its
paging file when Cargo linked unrelated targets in parallel.

## End-of-cycle review

- **AI slop and overengineering:** pass after correction. Review removed the new steady-state lock/
  `Arc` retention, unified two duplicate unwind guards, and moved cleanup to the exact transition
  ownership seam. A follow-up review also restricted terminal cleanup to transitions that actually
  own raw restore input, preserving revoke-only retry authority. The implementation is one focused
  private module, not a reusable quota service.
- **Hot path and latency:** pass for scope only. Record processing has no new lock, allocation,
  task, I/O, or branch. Restore adds a short worker-budget mutex before body reads and a semaphore
  acquisition per body. Restore p99/p99.9, allocation pressure, and pause duration remain
  unmeasured and cannot be inferred from unit tests.
- **Unused code and maintainability:** pass. Production types are cluster-private; test controls
  remain under `cfg(test)`. The resource owner and exact-retirement helper have direct runtime
  consumers. No backend façade, unused configuration, migration layer, or alternate code path was
  introduced. `DKS-CLEANUP-001` remains the final whole-workstream cleanup gate.
- **Production readiness:** **BLOCK**. The charge covers declared raw lineage payload/artifact
  ownership. It does not bound wrapper/seal metadata, allocator/response overhead, decoder scratch
  or expansion, decoded/live/prepared/retired RSS, publication/retirement pause, or vnode-scale
  swap cost. No qualified hot-state backend, window/timer or join consumer, delivery composition,
  failover/ALO/EO recertification, latency/resource profile, or independent soak exists for this
  binary.
- **Documentation and overdocumentation:** pass. The ADR, implementation plans, validation report,
  artifact-format boundary, and changelog are reconciled; this review is the only new cycle-history
  document.
- **Tests:** pass for the affected boundaries. Pointer identity, retryable versus terminal cleanup,
  in-flight cancellation, exact charge lifetime, replacement self-deadlock avoidance, and failed-
  launch fencing are covered. Broad feature/integration suites and all current-binary soaks remain
  explicitly unclaimed.

## Next bounded work

Run the missing stock Fjall lifecycle experiment as an isolated validation-only program under a
hard external process deadline. It must exercise the exact successful-spawn/worker-failure/close
sequence cited by the current source rejection and distinguish a stock released failure from a
test or harness defect. Only if stock Fjall reproduces the gap should a concise upstreamable patch
be prototyped and rerun. A patch may inform an upstream PR, but it must not silently add a Laminar
runtime dependency, relax admission, or become an unowned permanent fork.

After that bounded decision, return to the remaining backend-neutral resource/RSS/pause and vnode-
scalability gates. Independent soak remains the final cleaned-release-candidate gate, not part of
this cycle.
