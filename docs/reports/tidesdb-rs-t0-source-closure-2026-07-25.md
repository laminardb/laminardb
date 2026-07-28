# Official TidesDB Rust binding T0 source closure

**2026-07-28 supersession:** [ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#2026-07-28-fjall-318-priority-amendment)
makes stock Fjall 3.1.8 the preferred qualification-entry subject. This T0 stop remains historical
evidence; no package wait or re-entry is scheduled.

- **Cycle:** 41
- **Date:** 2026-07-25
- **Scope:** read-only package/source closure; no build, link, install, or candidate execution
- **Selected binding:** [`tidesdb/tidesdb-rs`](https://github.com/tidesdb/tidesdb-rs), published
  on crates.io as package and library `tidesdb`
- **Exact subject:** Cargo package `tidesdb v0.11.1`, default features `v9_3_6` and
  `compression`, using its native TidesDB 9.3.6 source payload
- **T0 verdict:** **STOP_WAIT_FOR_UPSTREAM** for this release
- **T1 disposition:** cancelled; Laminar-only design work may continue, but the package must not be
  added or executed
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Package-name correction

The official repository is named `tidesdb-rs`, but its manifest declares the Cargo
[package](https://github.com/tidesdb/tidesdb-rs/blob/v0.11.1/Cargo.toml#L1-L3) and Rust
[library](https://github.com/tidesdb/tidesdb-rs/blob/v0.11.1/Cargo.toml#L39-L41) name as `tidesdb`.
The dependency intended by this ADR is therefore:

```toml
tidesdb = "=0.11.1"
```

The distinct crates.io package [`tidesdb-rs`](https://crates.io/crates/tidesdb-rs) is an
independently maintained wrapper from `0x6flab`, currently version 0.1.3 and imported as
`tidesdb_rs`. It is not the official TidesDB binding and is not the selected dependency. A bounded
source check found that it vendors a post-v9.2.0 native commit and omits safe iteration, statistics,
and memory-limit configuration from its public wrapper. Laminar will not combine it with, copy code
from, or substitute it for the official binding.

## Decision

LaminarDB can implement the integration concerns it owns on top of the official public safe API:
vnode-prefixed keys, one-owner-lane lifetime containment, copied bounded scans, portable logical
checkpoint export/restore plumbing, pre-output fail-stop boundaries, connector coordination, cgroup
admission checks, and external resource/latency observations. This is call-surface sufficiency only:
native 9.3.6's integrity and iterator defects prevent sound logical export. None of the plumbing
requires a private FFI or a package fork.

The current release must nevertheless not be added. Its native 9.3.6 payload predates published
fixes for memtable corruption, stats/read concurrency, error-path memory safety, flush rotation, and
other one-CF paths. Those defects cannot be repaired by a Laminar facade. Native transaction batch
success is also ambiguous: a short partial apply is acknowledged as success. That particular gap
may be containable by an explicit Laminar pre-output verification/fail-stop protocol, but only after
source proof and measurement; it is not silently treated as fixed.

The integration direction remains the official Rust binding. The exact 0.11.1/native-9.3.6 pair is
stopped, T1 is cancelled, and no runtime dependency or adapter is authorized. Re-entry requires a
new official package with a reconciled native payload, followed by a repeated T0.

## Exact artifact ledger

Source packages were downloaded and unpacked in an isolated temporary directory for inspection.
No package build script, native compiler, linker, database binary, Docker/WSL workload, or Laminar
candidate path ran.

| Artifact | Frozen identity | Reconciliation |
|---|---|---|
| [`tidesdb` crate](https://crates.io/crates/tidesdb), version 0.11.1 | SHA-256 `84b46549f2fc7b1a1afd3c8898d3aee285cddef6687acd8d842956d18b5581a8`; published 2026-06-22; not yanked | Manifest selects default `v9_3_6` plus `compression`; no alternate native-version feature exists |
| Published Rust VCS identity | `.cargo_vcs_info.json` records `78956cc331651830e8d1cd31512aef462caa51c5` | The [v0.11.1 tag](https://github.com/tidesdb/tidesdb-rs/releases/tag/v0.11.1) is merge commit `e2febbc548e7f0158d1c09ea487aa0bb7c343616`; both commits have tree `3bfccb0fffb8e363a1a278a43d00a6cb2a06ab58`. The tag is one zero-file-diff merge ahead; the tag manifest equals published `Cargo.toml.orig`, and ten manifest/build/Rust-source pairs are byte-identical |
| [`tidesdb-src-v9-3-6` crate](https://crates.io/crates/tidesdb-src-v9-3-6), version 0.1.0 | SHA-256 `4bbead8c005eb5bbba378338ba356501ecd68f6f0f5f5615575d99bca0be9779`; published 2026-06-10; not yanked | Declares `TIDESDB_VERSION = "9.3.6"` and archive `tidesdb-9.3.6.tar.gz` |
| Bundled native archive | SHA-256 `81894657d862d1006e1340b706f90416e01f7de444031ac566f659d635535ef1` | Byte-identical to the official [native v9.3.6 tag archive](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.6); 78 files, zero content differences; tag commit `1414af858506d251411eb36ea4fd0bbdf40f306f` |

The package [build script](https://github.com/tidesdb/tidesdb-rs/blob/e2febbc548e7f0158d1c09ea487aa0bb7c343616/build.rs#L185-L198)
always performs an exact-version `pkg-config` probe for 9.3.6 and otherwise builds the source
archive. Accepting an ambient system library is outside the selected identity. Any future T1 must
make that probe miss and capture final link provenance.

This closes crate/tag/nested-archive attribution only. Lockfile, target, toolchain, redistribution,
and complete license/SBOM closure stopped at the first source veto; they are not passes.

## Native batch-success gap

The exact source establishes this chain:

1. Native [`skip_list_put_batch`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/skip_list.h#L440-L453)
   specifies a non-negative insertion count that may be less than `count` and tells the caller to
   compare the two.
2. Its [implementation](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/skip_list.c#L2260-L2586)
   skips individual allocation/insertion failures, retains preceding successes, and returns
   `success_count`.
3. The one-CF transaction helper's [small path](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L26993-L27067)
   and [larger path](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L27162-L27217)
   test only `< 0`; a short count becomes `TDB_SUCCESS`.
4. The commit path [writes the WAL and applies the helper](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L28733-L28912),
   then [marks committed and returns success](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L28908-L28979).
5. The [official Rust wrapper](https://github.com/tidesdb/tidesdb-rs/blob/78956cc331651830e8d1cd31512aef462caa51c5/src/transaction.rs#L187-L195)
   receives only `TDB_SUCCESS` and returns `Ok(())`.

One owner lane prevents another Laminar command or logical checkpoint from observing a commit while
it is being verified. Always-fresh restore means a process crash can discard incomplete local
state. Consequently, two Laminar containment designs are theoretically possible:

- commit the coalesced transaction, open a fresh `READ_COMMITTED` transaction, read back every
  distinct final mutation before output or checkpoint publication, require exact canonical bytes
  for puts and exact `NotFound` for intended deletes, and poison/fail-stop on every other result or
  ambiguous read; or
- use serialized single-mutation transactions, publish no output/checkpoint until all succeed, and
  fail-stop/fresh-restore after any error or crash.

Neither is accepted yet. Full read-back adds O(distinct touched keys) point reads and copies to every
state batch; single-mutation transactions multiply transaction/WAL overhead and remove native batch
efficiency. Both require exact ordering with source acknowledgement, output release, and checkpoint
cuts, plus fault injection and strict p99.9/maximum latency evidence. They are valid bounded design
subjects, not a reason to admit 0.11.1.

Reusing the committing transaction is forbidden: [native transaction reads](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L25592-L25695)
consult that transaction's pending write buffer before storage and could return the intended value
even when the memtable omitted it. Except for exact `NotFound` while verifying an intended delete,
any error, timeout, cancellation, panic, or mismatch poisons the whole process. The old root is
quarantined and never reopened. Recovery may restore only the exact state/source/output lineage of
the latest coordinator-admitted cut with a durable terminal `CheckpointVerdict::Commit`, into a
fresh root under a new owner epoch. The prior owner and source/sink epochs must be fenced, and an
ambiguous sink decision reconciled, before replacement activation.

## Independent current-release stops

The native payload also fails gates that an outer adapter cannot repair:

| Gate | Exact-source result | Disposition |
|---|---|---|
| Non-containable general-profile defects | Native [v9.3.7](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.7) fixes a thread-local skip-list arena ABA that could corrupt the memtable when an arena address was reused; one owner lane does not remove a thread-local address-reuse defect. Native [v9.3.11](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.11) fixes flush-rotation enqueue-failure dangling/use-after-free state that can hide committed data. Native [v9.3.14](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.14) fixes iterator rebuild across a reaper eviction window that can omit a live SST from logical export | **STOP**; touched-key verification cannot certify untouched state or an incomplete iterator export, and Laminar cannot patch the native payload |
| Excluded later defects | v9.3.7 also fixes a stats/flush concurrency crash and open/recovery double-free/double-destroy paths | Same-lane quiescence and the prohibition on native reopen can exclude those specific paths, but do not weaken the independent STOP above |
| Owner/lifetime containment | The broad official wrapper omits DB -> CF -> transaction -> iterator lifetimes, but a private same-thread facade can keep one CF, lexical transactions/iterators, copied outputs, and strict child-before-parent destruction using public safe APIs. A stuck close escalates to whole-process fail-stop | **PASS for the restricted facade only**; unrestricted package use remains rejected |
| Resource governance | Public tuning and stats exist, but native Linux discovery reads host `/proc/meminfo`/`sysinfo`; automatic mode uses 75% of host RAM and an explicit smaller limit is raised to 5% of host RAM. It is not a general cgroup/RSS/page-cache governor | **STOP for the general profile**; a future constrained profile must pass the exact formula and observability rules below, otherwise startup fails |
| Maintenance health v2 | The safe public surface exposes useful backlog, running methods, and progress counters. It does not expose exact internal stall intervals/reasons, general local background errors, cleanup/deferred-free failures/backlog, or reaper failure; upload-failure counters apply only to the prohibited native object-store mode | **STOP**; Laminar can add external queue/cgroup/disk/FD observations but cannot invent missing sticky native error facts |
| Native object storage | The package exposes optional filesystem/S3-oriented modes but no safe Azure/GCS/custom Rust `object_store` connector | **PASS only as an exclusion**; native remote/filesystem modes remain disabled and cannot replace provider-neutral Laminar checkpoints |

The resource finding is deliberately bounded: the source does not prove that every cgroup-aligned
configuration is impossible. It proves that the required general container envelope cannot be
guaranteed. A future constrained Linux profile is admissible only if all of these conditions are
machine-checked without integer overflow:

- `H` is physical host memory, `C` is the finite effective cgroup-v2 `memory.max`,
  `F = ceil(0.05 * H)`, and `E` is the explicit non-auto engine limit;
- `R` is a profile-frozen, qualification-derived upper reserve for Laminar/Arrow queues and
  checkpoints, allocator retention, native memory omitted by TidesDB pressure accounting, mmap/page
  cache, and safety headroom;
- `E >= F`, `E + R <= C`, and post-open `resolved_memory_limit == E`; and
- missing/unreadable cgroup-v2 data, `memory.max=max`, an unproved `R`, or any failed equality or
  inequality rejects startup.

The profile must also freeze process-FD soft/hard limits, open-file reserve, disk-byte and inode
soft/hard watermarks, and compaction/checkpoint/restore temporary-space reserves. Off-hot-path
supervision samples cgroup `memory.current`/`memory.events`, PSI, process RSS, FDs, disk bytes/inodes,
and I/O tails. Crossing a soft threshold backpressures; a hard threshold or lost observability
poisons the attempt. Qualification and the independent soak must prove `R` and resource plateaus;
published RSS alone is not that proof.

### Resource and health source ledger

| Finding | Exact source proof |
|---|---|
| Host-derived limit and floor | Native constants set auto mode to 75% and the minimum to 5% of host memory ([`tidesdb.c`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L411-L421)); Linux available/total memory comes from `/proc/meminfo` and `sysinfo`, not cgroups ([`compat.h`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/compat.h#L2350-L2381), [`compat.h`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/compat.h#L2429-L2458)); database open clamps an explicit limit and publishes the resolved value ([`tidesdb.c`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L20750-L20787)) |
| Pressure-accounting scope | The native pressure total adds selected memtables, estimated active-compaction memory, auxiliary SST memory, caches, and transaction memory ([`tidesdb.c`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L19663-L19820)); it is not process RSS, allocator retention, or filesystem page-cache accounting |
| Hidden exact stalls | Native write stalls inspect a private flush heartbeat and emit exact no-progress outcomes ([`tidesdb.c`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L24677-L24728)); the public Rust [`DbStats`](https://github.com/tidesdb/tidesdb-rs/blob/78956cc331651830e8d1cd31512aef462caa51c5/src/stats.rs#L80-L173) has queue/progress fields but no heartbeat, stall interval/reason, or local asynchronous-error record |
| Lost background failure facts | Flush/disk/SST/manifest failures are logged, retried or removed from pending work without a sticky public error record ([`tidesdb.c`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L18577-L18785)); compaction failures are logged before pending state is cleared ([`tidesdb.c`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L19223-L19258)) |
| Unreported durability, reaper, and cleanup health | Flush paths discard SST fsync results ([`tidesdb.c`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L18660-L18667)); reaper-thread creation failure is non-fatal, while safe stats expose neither reaper liveness nor cleanup backlog/oldest age ([`tidesdb.c`](https://github.com/tidesdb/tidesdb/blob/1414af858506d251411eb36ea4fd0bbdf40f306f/src/tidesdb.c#L21435-L21446), [`DbStats`](https://github.com/tidesdb/tidesdb-rs/blob/78956cc331651830e8d1cd31512aef462caa51c5/src/stats.rs#L80-L173)) |

## Laminar-owned versus upstream-owned gaps

| Gap | Laminar action | Upstream/package requirement |
|---|---|---|
| Keyed layout and vnode lifecycle | Prefix keys with pipeline/operator/table/vnode/generation; fence owners; restore into an inactive fresh generation; publish atomically; clean up incrementally | Ordered point/seek/iterator behavior must remain safe and exact |
| Package ownership safety | Confine all package objects to one bounded blocking owner lane; copy results; prohibit callbacks/raw handles; fail-stop on stuck shutdown | Public safe APIs must preserve the required calls without use-after-free or hidden global lifecycle hazards |
| Portable checkpoint and providers | Export deterministic logical records through Laminar's Rust `object_store` path for S3/GCS/Azure/qualified shared FS | Native object-store/checkpoint formats remain non-authoritative |
| Delivery and exactly-once | Align source positions, state cut, coordinator Commit, fencing, and transactional/idempotent sink capability; keep ALO and EO matrices separate | The local engine need not own connector transactions, but must provide unambiguous state outcomes or support an accepted fail-stop verification boundary |
| Memory and latency envelope | Configure a non-auto budget, verify resolved limit at startup, account Arrow/queue/checkpoint memory, observe cgroup memory/PSI/disk/FD, backpressure early, and measure p99.9/max under skew | Native must not contain known corruption bugs; missing internal hard-bound behavior constrains which deployment envelopes can be admitted |
| Maintenance health | Derive only truthful Laminar queue/service/checkpoint and OS-level facts; poison on foreground errors and persistent proven non-progress | Sticky internal background/cleanup/reaper failure facts require an upstream safe surface; logs are not a correctness substitute |

## Re-entry contract

A new official-binding attempt starts only when:

1. `tidesdb/tidesdb-rs` publishes a new Cargo package `tidesdb` with a native payload containing all
   one-CF/fresh-root-relevant fixes after 9.3.6; merely finding a newer system library is prohibited;
2. transaction success is exact upstream, or a separately accepted Laminar verified-commit/
   fail-stop protocol has source proof and explicit latency/fault gates;
3. memory satisfies the explicit constrained-profile formula above, or the native API supplies a
   stronger cgroup-aware hard-bound contract; and the public safe surface supplies loss-detecting
   worker/reaper liveness, exact stall interval/reason, asynchronous-error sequence/code/time,
   cleanup backlog/oldest-age, and propagated fsync/flush/manifest/compaction failure facts;
4. Laminar freezes the crate, VCS, features, nested source, target, toolchain, link, legal, and SBOM
   identities and repeats the bounded T0 from the beginning; and
5. only a complete T0 pass authorizes a scoped T1 build/feasibility run.

No RocksDB, Fjall, redb, bounded-memory, third-party `tidesdb-rs`, or other fallback activates
automatically. The official TidesDB Rust binding remains the intended integration line, but its
current release has no runtime or production admission path.
