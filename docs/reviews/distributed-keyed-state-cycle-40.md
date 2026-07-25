# Distributed keyed state Cycle 40 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** official `tidesdb-rs` selected as the only TidesDB integration line; private
  FFI, engine/package forks, patches, and native substitution rejected
- **Exact starting subject:** `tidesdb-rs v0.11.1`, commit
  `e2febbc548e7f0158d1c09ea487aa0bb7c343616`, with bundled native 9.3.6
- **Candidate installed, built, linked, or executed:** no
- **Runtime dependency, backend, adapter, workflow, command, or admission change:** none
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

The [TidesDB package design](../architecture-decisions/tidesdb-local-state-successor-design.md)
replaces Cycle 39's private-FFI proposal. The official package is selected as an implementation line,
not approved as a broad API or production backend. Laminar may expose only a restricted package-
backed facade: one database, one retained fixed CF, one same-thread owner lane, copied inputs and
outputs, transaction-scoped iterators, and deterministic child-before-parent destruction. No package
type crosses that facade.

The current package's bundled native 9.3.6 is the exact subject. Native 9.3.14 is comparison/source
evidence and cannot be substituted. A relevant missing post-9.3.6 fix, uncontainable lifetime or
thread-safety defect, silent short apply, partial visibility, missing cgroup/resource control, or
unexposed mandatory health signal stops v0.11.1 pending a newer official package. Laminar will not
repair it through private FFI, raw handles, callbacks, `unsafe`, leaks, package/native patches or
forks, `pkg-config` substitution, or a system native library.

Local TidesDB files remain disposable. Cluster recovery uses a coordinator-admitted portable
Laminar checkpoint through a cluster-shared `StateBackend`; Rust `object_store` remains the provider-
neutral local/S3/GCS/Azure transport, while `file://` is node-local by default. TidesDB native
checkpoint, prior-directory reopen, and native remote mode are prohibited in the initial profile.

## Bounded next work

T0 is next: at most one working day/eight engineer-hours and zero candidate machine-hours for exact
package/native identity, post-9.3.6 fix relevance, safe call/lifetime/close containment, exact atomic-
success semantics, cgroup controls, stock package health signals, and build/legal provenance.

Only a T0 pass may permit separately scoped T1: at most two working days/four machine-hours in an
isolated WSL2/Linux environment. T1 performs an exact release-config build/link identity smoke,
same-lane one-CF lifecycle/operations/close checks, and one separately identified instrumented
diagnostic build. It is not runtime integration, qualification, or native target evidence.

## Six-part cycle review

### 1. AI slop, evidence, and consistency

**Pass after correction.** Independent reviews caught and removed an invalid marker-only atomicity
escape hatch, corrected owner-lane shutdown order, separated release and sanitizer identities, and
made the single-lane head-of-line risk a disqualification gate. Statements now distinguish inspected
source facts, design requirements, and unproved evidence. Vendor Zipf results remain hypotheses.

### 2. Overengineering, hot path, and latency

**Pass for the timeboxed design.** The initial shape is one DB/one CF/one owner lane with one bounded
command per Arrow batch. Per-row FFI, futures, tasks, metrics queries, fsync, and object-store calls
remain forbidden. No copy-on-write/marker subsystem was invented to mask silent short apply; exact
atomic success is a hard package gate. If one-lane queueing or hot-writer/disjoint-victim p99.9,
maximum, throughput, or isolation fails, the package fails rather than silently adding concurrency.

### 3. Unused code and scope

**Pass.** No Rust code, dependency, adapter, schema, fixture, feature flag, workflow, candidate binary,
or run command was added. T0 now precedes speculative validation tooling. The frozen runner-v2 and
v4 profile were not edited.

### 4. Production readiness, delivery, and soak

**Pass only as an explicit NO-GO.** Package safety, atomic success/visibility, immutable logical cut,
cgroup resource governance, maintenance health, open-loop latency, faults, portable recovery,
rebalance fencing, source/sink delivery composition, endurance, and the independent unchanged-
release soak remain conjunctive gates. A soak failure disqualifies TidesDB only when root-caused to
the backend; any failure still blocks production until resolved. Exactly-once remains a separate
source/checkpoint/coordinator/sink composition, not a TidesDB WAL property.

### 5. Documentation and research hygiene

**Pass.** The obsolete private-FFI design was replaced in place; Git history retains its provenance.
Dated backend reports remain relevant negative/source evidence and now contain short pointers to the
current ADR rather than duplicate work orders. RocksDB/Fjall/redb evidence stays historical; no Fjall
fork or fallback is activated. The current authority is concentrated in ADR-008, the package design,
the active plan, the TidesDB prescreen, and this review.

### 6. Tests and reproducibility

**Pass for documentation-only scope.** No TidesDB safety, correctness, latency, or production claim
comes from these checks.

- `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed; one explicit non-gating throughput/RSS observation ignored.
- Frozen runner-v2: 66,870 bytes, SHA-256
  `661102f9dcb46934f966f25cc91934ea0d1fa6a487323fb383cbabd0a5e166e3`.
- Frozen v4 profile: 7,838 bytes, SHA-256
  `94652d30153d998628d4e1d2b5da87bce59f5064192eeba9e9331f3f40507392`.
- `cargo fmt --all -- --check` and the standalone qualifier format check: pass.
- `git diff --check`: pass; local Markdown links across 14 changed/new documents: zero missing.
- No TidesDB, Docker/WSL workload, provider API, cloud resource, cluster soak, or independent
  production soak ran.

## Cycle 41 review plan

Before committing T0, repeat the six reviews:

1. reject any source claim without exact package, native archive, checksum, feature, and line-level
   proof; distinguish inference from fact;
2. stop at the one-day cap and reject a private wrapper, patch, fork, callback, marker subsystem, or
   speculative hot-path repair;
3. add no runtime dependency or unused facade/tooling when the exact release already fails a gate;
4. preserve fail-closed admission, provider-neutral portable recovery, connector delivery, and
   independent-soak requirements;
5. record one compact T0 verdict and update only current authority documents; and
6. run no candidate machine work in T0, verify frozen artifacts remain unchanged, and obtain an
   independent source/safety review before commit.
