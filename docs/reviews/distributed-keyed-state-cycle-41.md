# Distributed keyed state Cycle 41 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** exact official Cargo package `tidesdb v0.11.1`/native 9.3.6 stopped at T0;
  official Rust-binding direction retained; T1 cancelled
- **Package identity:** official repository `tidesdb/tidesdb-rs`; Cargo package/import `tidesdb`;
  unrelated package `tidesdb-rs` excluded
- **T0 verdict:** `STOP_WAIT_FOR_UPSTREAM`
- **Candidate installed, built, linked, or executed:** no; source packages only were downloaded and
  unpacked outside the repository
- **Runtime dependency, backend, adapter, workflow, command, or admission change:** none
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

The [T0 source closure](../reports/tidesdb-rs-t0-source-closure-2026-07-25.md) reconciles the
official published crate, repository tag/tree, nested native source crate/archive, and native
v9.3.6 tag. The strict owner-thread/one-CF/no-callback facade can contain the official wrapper's
unencoded parent/child lifetimes, and the public safe API has the calls needed for Laminar-owned
point, transaction, iterator, configuration, stats, copied-output, and checkpoint-export plumbing.
That is API-shape evidence only; native 9.3.6's integrity and iterator defects make its logical
export unsound.

The exact release still fails before T1:

1. native 9.3.6 predates relevant memtable-corruption, stats/read concurrency, recovery,
   flush-rotation, and iterator fixes that an outer Laminar facade cannot supply;
2. one-CF multi-operation transactions may apply a subset, accept the short non-negative count,
   mark committed, and make official `Transaction::commit()` return `Ok(())`;
3. host-derived memory resolution and its 5%-of-host floor cannot guarantee the required general
   cgroup envelope; and
4. the public safe surface omits exact internal stall/background-error/cleanup/reaper health facts.

A fresh-transaction readback of every distinct touched key, followed by whole-process poison/
fail-stop on any ambiguity, can theoretically contain the short-batch defect because output and
logical checkpoint publication remain behind the owner lane and every restart uses a fresh root.
It costs O(touched keys) FFI reads/allocations/copies per Arrow batch and cannot detect corruption
of untouched state or incomplete iterator export. Single-mutation transactions are also
attempt-boundary-correct under the same fail-stop rules but multiply WAL/transaction calls and are
not a viable hot-path direction. Neither option repairs the other native defects or is accepted for
production.

The work is therefore split honestly: backend-neutral Laminar lifecycle, publication-boundary,
checkpoint, resource-admission, health-capability, and fake-fault contracts may proceed; no
TidesDB dependency or adapter is added until a new official package repeats and passes T0.

## Six-part cycle review

### 1. AI slop, evidence, and consistency

**Pass after correction.** The documents now distinguish repository name, Cargo package/import,
and the unrelated third-party crate. Claims are bound to crate/archive checksums and exact commits.
Crate/tag/nested-archive attribution is not mislabeled as complete lock/toolchain/link/legal
identity. The build script's unavoidable `pkg-config` probe is recorded accurately: future T1 must
make it miss rather than claiming the probe itself is prohibited.

The original “no Laminar containment is possible” wording was narrowed. Full-key verification or
single-mutation fail-stop can contain only the known short-apply acknowledgement at an attempt
boundary; arena, flush-rotation, untouched-state, iterator/export, resource, and silent-background-
error issues remain independent stops. No source-only result is presented as latency or endurance
evidence.

Independent review also corrected four boundary errors before commit: restore now requires a durable
terminal Commit rather than Seal alone; delete verification accepts only the intended `NotFound`;
resource admission uses an executable formula; and backend-neutral work no longer pre-implements a
TidesDB owner lane or verified-readback state.

### 2. Overengineering, hot path, and latency

**Pass by separating design from adoption.** No per-key verifier, marker protocol, second database,
private FFI, raw handle, patch, fork, native substitution, or adapter was implemented. The design
records the verifier's unavoidable O(K) point-read/copy cost and the single-mutation path's O(K)
transactions/WAL writes. Any future verifier must be measured against p99.9, maximum, victim-lane,
queueing, checkpoint-overlap, and Zipf gates; correctness does not exempt it from the low-latency
bar.

Backend-neutral complete-success/failed-before-apply/unknown-poison publication semantics and fake
fault injection are the next useful work. TidesDB's owner lane and verified readback are deferred
until an admitted profile requires them.

### 3. Unused code and scope

**Pass.** No Rust code, dependency, feature flag, adapter, candidate profile, mapping, workflow,
binary, or run command was added. Source artifacts live outside the repository. The unrelated
`tidesdb-rs` package was identified and excluded rather than partially integrated. Frozen
Fjall/RocksDB validators remain reference history; redb stays parked and bounded memory remains
reference/conformance-only. No fallback code activated.

### 4. Production readiness, delivery, and soak

**Pass only as an explicit stop/NO-GO.** A verified local transition would still not manufacture
exactly-once. At-least-once requires a replayable/fenced/checkpointable source and compatible sink;
exactly-once separately composes a coordinator-admitted state/output cut with a durable terminal
Commit, replay-stable source cursor, sink transaction or durable idempotency, and ownership epoch.
Ambiguous native outcomes poison the whole attempt; native reopen is forbidden.

A future constrained Linux memory profile must machine-check the source report's
`H`/`C`/`F`/`E`/`R` formula and freeze FD/disk/inode reserves; unknown cgroup authority or an
unproved reserve fails startup. External supervision cannot manufacture missing native health.
Production re-entry requires the official safe API to expose loss-detecting worker/reaper, exact
stall, asynchronous-error, cleanup, and durability-failure facts.

Native TidesDB filesystem/S3 object-store modes remain disabled. Laminar's provider-neutral Rust
`object_store` path and cluster-shared `StateBackend` remain portable checkpoint authority for S3,
GCS, Azure, or a separately qualified shared filesystem. Immutable cuts, fresh restore, C2/C3
p99.9/maximum latency, faults, 24/72-hour endurance, connector delivery, and the independently
operated unchanged-release product soak were not reached. Production remains closed.

### 5. Documentation and research hygiene

**Pass.** Current authority documents use the unambiguous phrase “official
`tidesdb/tidesdb-rs` binding, published as Cargo package `tidesdb`.” The source report owns detailed
evidence and the Laminar/upstream gap split; other documents link to it. The brief source check of
the unrelated literal package is retained only as an exclusion fact, not another candidate track.
Dated backend reports remain relevant selection/rejection history; no obsolete workflow or private-
FFI design was reintroduced.

### 6. Tests and reproducibility

**Pass for source/documentation-only scope.** These checks do not qualify TidesDB or provide
candidate runtime evidence.

- `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed; one explicit non-gating throughput/RSS observation ignored.
- `cargo fmt --all -- --check` and the standalone qualifier format check: pass.
- `git diff --check`: pass; local links across all 18 changed/new Markdown documents: zero
  missing.
- Frozen runner-v2: 66,870 bytes, SHA-256
  `661102f9dcb46934f966f25cc91934ea0d1fa6a487323fb383cbabd0a5e166e3`.
- Frozen v4 profile: 7,838 bytes, SHA-256
  `94652d30153d998628d4e1d2b5da87bce59f5064192eeba9e9331f3f40507392`.
- Published official `tidesdb 0.11.1` crate SHA-256:
  `84b46549f2fc7b1a1afd3c8898d3aee285cddef6687acd8d842956d18b5581a8`.
- Published `tidesdb-src-v9-3-6 0.1.0` crate SHA-256:
  `4bbead8c005eb5bbba378338ba356501ecd68f6f0f5f5615575d99bca0be9779`.
- Nested/official native v9.3.6 archive SHA-256:
  `81894657d862d1006e1340b706f90416e01f7de444031ac566f659d635535ef1`; 78 files, zero
  content differences.
- Literal third-party `tidesdb-rs 0.1.3` exclusion check: crate SHA-256
  `0f18108d0a1647b338ee5957c72d691a3a5a587e2c2b0a8a186150c8eb6e0ab1`; its 75 vendored native
  files were byte-identical to gitlink commit `83983a1e95ef12aca7f400d697e5d6c85a4ca204`.
- No TidesDB build/link/workload, Docker/WSL workload, provider API, cloud resource, cluster soak,
  or independent production soak ran.

## Next-cycle review plan

Before Cycle 42's backend-neutral gap work is committed, review:

1. **AI slop:** every complete-success, failed-before-apply, unknown-poison, checkpoint,
   source-output, and owner-epoch transition has one normative definition and an executable negative
   case; no TidesDB-specific claim leaks into the engine-neutral contract.
2. **Overengineering/hot path:** the normal path adds no per-record allocation, full-state scan,
   fsync, or remote I/O; any verification hook is batch-scoped, disabled without an admitted
   backend profile, and has an explicit cost counter.
3. **Unused code:** every new state/variant is consumed by the fake fault tests and a named future
   integration seam; otherwise remove it.
4. **Production readiness:** ambiguous outcome poisons before output/checkpoint publication; crash
   always means fresh-root restore and old-owner fencing; ALO/EO/source/sink capabilities remain
   explicit and fail-closed.
5. **Documentation:** keep the implementation plan and ADR concise; do not duplicate the TidesDB
   source audit or revive obsolete GitHub approval workflow text.
6. **Tests:** cover commit success, explicit failure, timeout, panic, silent-short simulation,
   verification mismatch, crash at every publication boundary, checkpoint exclusion, stale owner,
   bounded queue/resource admission, and deterministic replay. TidesDB execution remains forbidden
   until a new official package passes repeated T0.
