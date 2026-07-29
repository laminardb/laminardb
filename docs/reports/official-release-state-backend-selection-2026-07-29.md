# Official-release local state backend selection

- **Date:** 2026-07-29
- **Scope:** worker-local state for embedded, single-node, and cluster execution
- **Decision:** carry only `rocksdb = 0.24.0` with its bundled RocksDB 10.4.2 into the next
  bounded adapter-entry work
- **Production status:** **NO-GO**; no backend is production-qualified and `[LDB-4007]` and
  `[LDB-0013]` remain fail-closed
- **Runtime change:** none; no dependency or adapter is added by this decision

## Outcome

LaminarDB will stop the broad backend search. The sole released candidate to carry is the canonical
[`rocksdb` 0.24.0](https://github.com/rust-rocksdb/rust-rocksdb/releases/tag/v0.24.0) crate with
`librocksdb-sys 0.17.3+10.4.2` and its bundled upstream RocksDB 10.4.2. It is selected for one small,
fail-fast adapter-entry slice, not qualification or production admission.

The provenance caveat is material: `rust-rocksdb/rust-rocksdb` is the canonical maintained Rust
binding, but it is a community project rather than a Meta-published official Rust binding. In this
decision, **official release path** means an upstream-published, non-yanked crates.io release tied
to an immutable released engine, with no fork, git dependency, local native patch, or unreleased
fix. If project policy instead requires a vendor-authored Rust binding, the eligible set is empty
and work must stop. The unrelated crates.io package named `rust-rocksdb` is not eligible.

No runtime fallback is selected. If the exact RocksDB path fails its absolute contract, LaminarDB
stays fail-closed and returns to an explicit owner decision; it does not silently activate another
backend.

The frozen v4 validation contract keeps its original Fjall/RocksDB subjects and historical
TidesDB-preference metadata byte-for-byte as regression provenance. Those fields have no backend-
selection authority after this report; they must not be rewritten or relabelled as a RocksDB
product run.

| Selected component | Immutable identity |
|---|---|
| `rocksdb-0.24.0.crate` | SHA-256 `ddb7af00d2b17dbd07d82c0063e25411959748ff03e8d4f96134c2ff41fce34f`; release commit `bb7d2168eab1bc7849f23adbcb825e3aba1bd2f4` |
| `librocksdb-sys-0.17.3+10.4.2.crate` | SHA-256 `cef2a00ee60fe526157c9023edab23943fae1ce2ab6f4abb2a807c1746835de9`; same wrapper/sys release commit |
| bundled RocksDB | upstream commit `410c5623195ecbe4699b9b5a5f622c7325cec6fe`, tag `v10.4.2` |

The root manifest and lockfile do not yet contain these packages. The adapter-entry commit must pin
the exact release and capture its resolved source/build identity before executing a test subject.
Cargo features, compression, allocator, build flags, and RocksDB options are not selected here;
they must be frozen before any result is called candidate performance evidence. A minimal smoke
feature set cannot be relabelled as the production subject.

## Mandatory Laminar contract

The local database is disposable working-state capacity. It must provide:

- byte-keyed point access and ordered bounded ranges;
- one atomic batch across the state tables touched by an operator batch;
- a consistent snapshot/generation view for deterministic portable export;
- deterministic restore into a fresh local root and bounded vnode/generation cleanup;
- bounded cache, write-buffer, queue, file, thread, and disk configuration, with truthful pressure,
  foreground-call, maintenance-progress, and failure observations; and
- a lifecycle that can be quiesced and bounded without acknowledging ambiguous state as reusable.

Laminar—not the local database—continues to own vnode assignment and epoch fencing, checkpoint
inventory and the durable Commit decision, restore-before-activate, rebalance, source-position
sealing, and sink delivery composition. Local database durability cannot create exactly-once
delivery. Shared checkpoints remain on the provider-neutral Rust `object_store` path for Azure,
S3, GCS, and supported local/test stores; no engine object-store tier is required.

Every backend call remains coalesced per Arrow batch and runs on bounded blocking/state-owner lanes,
never per row on a compute/event-loop thread. The same state service and portable format must work
in embedded, single-node, and cluster modes; only cluster admission remains closed while evidence is
incomplete.

## Released-candidate veto matrix

| Candidate | Exact released path | Primitive fit | Current veto or caveat | Decision |
|---|---|---|---|---|
| RocksDB | `rocksdb 0.24.0` -> `librocksdb-sys 0.17.3+10.4.2` -> bundled RocksDB 10.4.2 | Atomic cross-CF batches, snapshots, ordered ranges, flush/WAL, checkpoint, cache/write-buffer/thread/WAL controls, properties, timed compaction wait, and background-work cancellation | Canonical community binding, not Meta official; wrapper trails current native 11.1.2; void `rocksdb_close` hides native close status; C++ build/native memory and tails require measurement | **SOLE CARRY CANDIDATE.** Bounded adapter entry only; qualification remains a later, separately authorized gate |
| TidesDB | official Cargo package `tidesdb 0.11.1`, declaring only native-version feature `v9_3_6` with bundled 9.3.6 source fallback | Required point/range/transaction shapes and active cross-platform upstream | Released path predates relevant native fixes; current native 9.3.14 still reproduced acknowledged success after an incomplete batch apply; PR 664 is open and no released Rust feature selects a fixed successor | **REJECT CURRENT RELEASE.** Watch only for a fixed native successor and matching official source/Rust releases |
| Fjall | `fjall 3.1.8`, exact official release | Rust-native atomic cross-keyspace batches, snapshots, ordered ranges, and explicit persistence | A worker can exit on error without reconciling the pre-counted worker total; database destruction can then wait indefinitely on private state. No wrapper can repair it under the no-fork rule | **REJECT 3.1.8.** Re-enter only after an official release fixes/tests bounded teardown and the maintenance/error surface is re-evaluated |
| redb | `redb 4.1.0`, exact official release | Rust-native ACID transactions, MVCC reads, ordered ranges, simple deployment | Freed pages were reused after barriers, but physical allocation remained 3.014x versus the 2.5x reference until offline compaction reduced it to 1.94x. There is no public fallible database close; Drop performs maintenance/close and discards errors. The page-reuse improvement is merged but unreleased | **STOP 4.1.0.** Re-enter only on an official successor and repeat the bounded resource/lifecycle control |

RocksDB has the strongest operating history and broadest controls, but also the heaviest build and
native-memory surface; its binding is Apache-2.0 and upstream CI covers the major target platforms.
Fjall and redb are lighter Rust-native MIT/Apache-2.0 projects, while TidesDB's wrapper/native path
is MPL-2.0 and C-backed. All have active upstreams and cross-platform CI. These are tradeoffs after
the veto gates, not scores that can override a correctness or lifecycle failure; bundled native
redistribution still needs the normal license/SBOM review before release.

The current native [RocksDB 11.1.2](https://github.com/facebook/rocksdb/releases/tag/v11.1.2)
cannot be substituted into this selection: the latest released Rust wrapper bundles 10.4.2. A
system library, git revision, or locally modified binding would violate the selected provenance and
would be a different subject.

The official TidesDB repository is named `tidesdb-rs`, but the Cargo package is `tidesdb`. The
official [0.11.1 release](https://github.com/tidesdb/tidesdb-rs/releases/tag/v0.11.1) declares only
feature `v9_3_6`; its build first probes exact system 9.3.6 and otherwise uses the bundled 9.3.6
source fallback. It cannot select current
[native 9.3.14](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.14).
[PR 664](https://github.com/tidesdb/tidesdb/pull/664) is useful upstream work but is not an eligible
artifact until an equivalent fix appears in a fixed native tag, matching `tidesdb-src-vX-Y-Z`
crate, and official `tidesdb` release/feature.

## Why RocksDB may enter despite its close-status gap

The local root is never recovery authority. A candidate adapter can stop admission, drain its
single owner lane, complete and verify the Laminar checkpoint, explicitly flush and report errors,
request a timed maintenance wait or cancellation, then abandon and quarantine the root after an
indeterminate close or process failure. Restore starts from a fresh root and the last
Commit-admitted portable checkpoint.

Those APIs do **not** enforce bounded in-process teardown. `wait_for_compact` can time out without
stopping jobs, `cancel_all_background_work(true)` has neither timeout nor returned status, and Rust
`Drop` calls void `rocksdb_close`. A timed Rust-thread join would only leak a live native handle. The
entry fault test must therefore own open/use/drop in a child process; its parent may apply a hard
deadline, terminate the child, and quarantine the root. That proves process-level containment only.
Disposable roots remove recovery-authority ambiguity, not shutdown liveness or leaked-resource
risk. Production remains blocked unless process termination/restart is explicitly accepted for all
supported modes or an official wrapper release exposes a sufficiently bounded/fallible lifecycle.

The previously approved maintenance-health v2 direction applies: record candidate-native estimates,
counters, properties, and Laminar-owned call/resource evidence with their true scope and units.
Do not add hot-path native instrumentation, call an estimate exact, or encode unsupported data as
zero merely to satisfy an engine-neutral schema.

## Next bounded slice

Timebox the next backend-specific work to one engineer-day and zero soak/qualification
machine-hours against the exact released pair. Spend at most the first four hours on release-delta,
build-provenance, and lifecycle closure; the first hard veto stops the cycle before an adapter. It
may add only the minimum private owner needed to prove:

1. a bounded 10.4.2-to-current-native delta screen for correctness, security, lifecycle, and
   resource fixes that would veto use of the older bundled engine;
2. bundled-build provenance: set `ROCKSDB_COMPILE=1`, reject `ROCKSDB_LIB_DIR` and relevant native
   library overrides, freeze Cargo features/options, and verify the linked engine identity;
3. open/fresh-root identity, deterministic configuration capture, atomic two-table batch, point
   read, ordered bounded scan, consistent snapshot export, explicit flush, nominal Drop followed by
   successful reopen, and restore into a new root;
4. vnode-prefix range deletion with a held-snapshot visibility check, post-delete logical absence,
   and quota-visible physical reclamation through an explicit flush/compaction policy; stop if the
   released API cannot supply a bounded policy without per-key hot-path work;
5. child-process quiesce/wait/cancel/Drop under a parent deadline, forced termination, root
   quarantine, and fresh-root restore, explicitly classified as process containment only;
6. injected foreground I/O/full-disk error propagation without publishing ambiguous state;
7. compilation and focused conformance on Windows plus WSL2/Linux; and
8. no row-hot-path I/O, runtime backend selector, generic multi-backend framework, cluster admission
   change, qualification execution, or soak/certification work.

Any required fork, git dependency, native instrumentation, reusable-root assumption after an
indeterminate close, or unbounded teardown stops the slice and removes the adapter. A pass only
permits a request for separately reviewed qualification authority; performance, failover/ALO/EO
regression, cleanup, and the independent immutable-release soak remain mandatory before production.

## Evidence retained, not active alternatives

- [RocksDB mechanism/source closure](rocksdb-mechanism-source-closure-2026-07-24.md)
- [TidesDB empirical entry validation](tidesdb-current-package-reentry-2026-07-28.md)
- [Fjall 3.1.8 adapter-entry source closure](fjall-3.1.8-adapter-entry-source-closure-2026-07-28.md)
- [redb 4.1.0 bounded review](redb-4.1.0-bounded-review-2026-07-29.md)
- [working-state placement analysis](state-working-state-options-2026-07-24.md)

These reports remain correctness and decision provenance. They no longer define a live multi-engine
research queue. No additional backend may enter without a newly released artifact and a concrete
reason it can beat or replace the selected path.

The stopped redb construction job is removed from required CI in this decision cycle. Remaining
redb qualification schemas/code and the oversized prescreen protocol are cleanup debt to remove
before a RocksDB adapter dependency lands; only report-owned minimal reproduction material may be
retained, with no execution or candidate authority.
