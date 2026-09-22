# Production hardening execution plan

**Status:** S1–S3, S4a–S4e and S5–S11 implemented and validated locally. S4 workspace audit/deny checks pass locally with temporary exceptions expiring 2026-10-21; registry crate publication is permitted under an explicit temporary exception for consumers that do not inherit the XML workspace patch. S6a has a documented sort-latency cost; S8 has admission/concurrent-burst costs; S9 has a measured, placement-sensitive wide-fan-out cost; S10 has table-write/refresh costs and variable wide-checkpoint timings; S11 has MV preflight/staging costs and variable snapshot/checkpoint timings. These are scoped local results, not production qualification. S12 has a workload observer, a Kafka startup fix, RSS evidence per process generation and passing local release diagnostics; production workload qualification remains open. S13 remains unimplemented.
**Date:** 2026-09-21. **Base:** `b429d0dfd02a435219f1b5977a442da9972e0c3d` (`0.30.0`).
**Evidence:** [production-readiness review](production-readiness.md). Its G1–G9 identifiers are used below.

Implement the smallest correctness fixes first. Establish performance evidence before changing
memory ownership or publication, then qualify the resulting build against a declared workload.
Treat each session below as one bounded change, normally one PR. This is an execution plan, not
a claim that every fix, performance target, or release qualification has been achieved.

The initial planning assumption is **single-node qualification first, then cluster**; embedded
correctness remains in scope throughout. Production throughput, latency, memory and recovery
targets remain to be selected. Their absence does not block the two P0 fixes.

## Order and dependencies

1. Run **S1 and S2 independently** to close the two configuration-dependent P0s. Start B0's
   workload definition alongside them; neither cold-path fix needs a performance project first.
2. Correct public support claims in **S3**. Security enforcement **S4** and telemetry **S5** can
   proceed in separate ownership lanes. Both must finish before production release qualification.
3. Complete B0's measurements, then implement **S6 → S7 → S8 → S9 → S10 → S11** in order. These
   sessions share configuration, lifecycle and memory ownership, so concurrent implementations
   would increase integration risk. Refresh the baseline immediately before each hot-path change.
4. Qualify the integrated build in **S12**. Prepare upgrade fixtures early, but complete **S13**
   against the final candidate before advertising a supported upgrade path.

S10/S11 cover local tables and MVs. A first qualification may exclude those features explicitly;
that qualifies the narrower workload and leaves G3 open. Cluster admission must stay fail-closed.

## Bounded implementation sessions

### B0 — define the workload and capture performance baselines (G5)

**Output:** a recorded workload/configuration, exact build identity, commands, raw Criterion
results and allocation/CPU profiles. No engine redesign or speculative optimization.

For memory work, record each domain's owner, admission/release points, transient allowance and
overflow behavior: source queues, parked/staged cycles, graph ports, DataFusion consumers, live
tables/MVs and checkpoint scratch. This is a short ownership contract, not a new accounting framework.

Choose the source/sink composition, SQL, row widths, batch sizes, key distribution, pipeline
count, event rate, checkpoint interval, retention, hardware and object-store location. Record
numerical latency/recovery ceilings and the RSS envelope before a certifying run. Diagnostic
runs with unset targets remain observational. Use the benchmark map below; add a focused
measurement only where existing benches do not exercise the changed path.

### S1 — require authentication for remote HTTP (G1)

**Owns:** server auth configuration/tests, directly affected examples and Helm documentation.
**Modes:** single-node and cluster. **Risk:** low runtime risk; intentional startup compatibility change.

- Add the rule in `collect_http_auth_errors` in [validation.rs](../crates/laminar-server/src/config/validation.rs).
  `load_config`/`validate_config` and `run_server` already share this guard. Reject missing console
  credentials on non-loopback binds before bootstrap, lease acquisition or listener creation.
- Preserve token rules, diagnostic-only scope and startup/recovery fencing. Cover wildcard,
  routable, loopback and IPv4-mapped IPv6 addresses explicitly; fail closed for ambiguous cases.
- Update the root/server READMEs, `examples/laminardb-cluster.toml`, and Helm quickstarts together.
  Reuse `consoleToken.existingSecret` and environment substitution. Provide no static default token.
  A chart using a remote bind must explain the missing credential before deployment or startup;
  custom configuration remains subject to server validation.

**Exit:** TOML and programmatic starts reject remote anonymous configuration before other work;
valid credentials and loopback development still work. Protected HTTP/WS routes reject invalid,
missing and duplicate credentials when serving is open; preserve earlier 503 responses while
fenced. Validate Helm rendering with/without a secret and with configuration overrides. Existing
cluster plaintext/mTLS tests retain their transport assertions with HTTP credentials supplied.

### S2 — enforce delivery-policy admission in every constructor (G9)

**Owns:** DB configuration, builder/shared-constructor validation and regression tests.
**Modes:** all DB construction paths. **Risk:** low; cold-path only. **Precedes:** S6–S11.

- Permit `ShedOldest` only with BestEffort. Keep lossless Backpressure and Fail behavior.
- Put the rule on [LaminarConfig](../crates/laminar-db/src/config.rs), reused by builder validation
  and `open_with_config_and_vars_and_rules`. Preserve early builder rejection before cluster lease
  binding. Two calls to the same validator are preferable to two implementations of the rule.
- Extract the existing cohesive validation/default-normalization phase from the shared constructor
  when wiring this in: `db/mod.rs`, this constructor and builder `build` have frozen readability
  baselines. Preserve source-idle, future-skew, state and checkpoint-limit validation semantics.
  Error messages and rustdoc must say “BestEffort only.”

**Exit:** test all three guarantees × all three policies through builder and direct configuration,
effective-cap edge cases, and inferred cluster ALO. Preserve cluster BestEffort rejection. Prove
invalid policy invokes no registered connector callback. Existing shedding, lossless deferral,
Fail-at-cap and cursor/recovery tests pass. Keep external overload/restart ledger tests in S12;
this fix does not need a new replay protocol or changes to the record path.

### S3 — correct supported-surface claims (G6)

**Owns:** root/crate READMEs and the smallest supporting example/admission tests.
**Modes:** all. **Risk:** low. **Depends on:** S1 for shared README/example edits.

Describe rejected PostgreSQL/MongoDB CDC admission, Delta reader versus admitted streaming
routes, supported managed cluster windows, the single compute runtime, and local/cluster
subscription boundaries. Link existing typed-contract tests instead of building another registry.

**Exit:** each positive example has a named parse/admission check; unsupported compositions still
fail before connector I/O. No feature flag or checkpoint alone implies exactly-once delivery.

### S4 — enforce existing security checks (G8)

**Owns:** existing audit/deny jobs, `deny.toml`, narrowly necessary dependency fixes.
**Modes:** shipped artifacts. **Risk:** medium dependency/build risk; no broad upgrades.

Run current audit/deny tools against the locked graph and current advisory data; triage findings
before changing gate behavior. Any accepted exception needs a specific reason, owner and review
date. Require both jobs in `ci-success` and remove advisory-only failure handling. The release
workflow already calls this CI workflow: preserve and verify that dependency rather than adding
a second scanner or release pipeline.

**Exit:** clean or explicitly accepted findings, plus a nonpublishing failure exercise proving
audit/deny failures prevent CI/release success. External branch-protection settings are a separate
verification; do not infer their state from workflow YAML.

### S5 — expose Kafka progress and freshness (G4)

**Owns:** Kafka metrics/background collection, required engine progress export and dashboard.
**Modes:** Kafka in all modes. **Risk:** medium semantic/observability risk.

Reuse `LaminarConsumerContext`, assigned partitions, the metrics registry and librdkafka statistics
or bounded metadata tasks. Define the offset convention before implementation: fetched/consumed
position, settled processing position and committed checkpoint recovery position are different.
Broker advisory commits cannot substitute for Laminar's committed recovery frontier. Coordinate
any DB callback changes with the single DB integration owner.

**Exit:** pause consumption while continuing broker writes: lag rises within two collection
intervals. Test source freshness age separately when production stops. Stale/unavailable measurements
are distinguishable from zero; revoked assignments disappear; label count is bounded. Dashboard
expressions match emitted metrics. Checkpoint/source freshness is alertable while readiness keeps
its authority/lifecycle meaning. No network polling or per-row metric allocation on compute.

### S6 — bound participating DataFusion working memory (G2)

**Progress (2026-09-20):** implemented and verified locally. See S6a below for the cached-plan
repair and measured sort cost, and S6b for the shared reservation pool, no-spill configuration,
correctness gates and matched performance evidence. Wider memory bounds remain S7–S11 work.

**Owns:** cached physical-plan reuse, configuration and DataFusion runtime/context creation.
**Modes:** all DB modes. **Risk:** medium; execution-state reuse, query failures and spilling behavior
change. **Depends on:** S2 and B0.

Reuse DataFusion 53.1's bounded pool and runtime APIs. Establish the budget scope explicitly and
share the intended per-DB pool with both the main context and the separately built connector
operator-graph context in [operator_graph.rs](../crates/laminar-db/src/pipeline_lifecycle/operator_graph.rs).
Inspect other public context constructors so the documented scope has no unbounded bypass.
The cluster-only `collect_local_table` diagnostic also constructs a fresh context; include it
in the per-DB scope. Standalone `laminar-sql` context factories and the expression-only lambda
context need an explicit scope decision rather than an implicit whole-process guarantee.

Disable disk spilling for execution on the compute runtime using the existing disk-manager API;
the upstream default can use OS temporary files. Do not introduce a new spill subsystem. The
pool governs participating fallible reservations, not every Arrow allocation or whole-process RSS.

**Exit:** expensive real plans hit a typed allocation/query error at the intended reservation
boundary, release reservations on error/cancel, create no compute-path spill files, and preserve
source progress/recovery. Test concurrent contexts against the shared budget and cached-plan
reuse after failure. Baseline and rerun representative DataFusion/coordinator workloads.

### S7 — bound connector-to-coordinator queued bytes (G2)

**Progress (2026-09-20):** implemented and verified locally. See the dated S7 record below for
the ownership contract, correctness gates, corrected burst fixtures and matched performance evidence.

**Owns:** `SourceMsg` admission and source-task handoff, including shutdown-tail sends.
**Modes:** all connector pipelines. **Risk:** high correctness/performance. **Depends on:** S6 + fresh B0.

Trace reservation ownership through normal, pending-cursor and direct shutdown `try_send` paths,
drain, parked messages, recovery and cancellation. The count-bounded source channel is shared
across sources. A queue permit released at dequeue does not bound graph retention; document the
handoff into separately bounded owners and complete prospective graph enforcement in S9.

Reuse existing caps/backpressure with finite documented defaults chosen from B0. Reserve before
queue admission; reject a single oversized batch instead of waiting forever. Control/barrier
traffic must retain FIFO ordering and a path to make progress when data admission is saturated.
Use batch-level accounting; no new per-row allocations, locks or hashing without benchmark evidence.

**Exit:** wide batches, multiple sources, slow sinks, parked input and cancellation respect the
queue budget; accepted work is not silently dropped and refused input is not settled. All charges
release exactly once. Saturated queues permit shutdown, checkpoint and recovery progress or their
existing bounded errors. This session alone does not close the whole G2 memory bound.

### S8 — close the embedded push-queue bypass (G2)

**Progress (2026-09-20):** implemented and verified locally, with documented admission and
concurrent-burst costs. See the dated S8 record below for ownership, compatibility and evidence.

**Owns:** public core streaming source admission and its DB configuration plumbing.
**Modes:** embedded/in-process sources. **Risk:** high API/performance. **Depends on:** S7 + fresh B0.

`Source::push_arrow` first puts an arbitrary batch in a separate count-bounded `SourceMessage`
ring. S7 alone cannot bound it. Inspect DB typed-to-Arrow conversion, `SourceEntry::push_and_buffer`,
snapshot retention, broadcast transfer and cloned producers too. Reuse the existing source owner
and nonblocking API; share the budget across clones. Document rejection and ownership semantics
without requiring caller-created reservation wrappers or an async push API. Do not expand this
into generic heap-size accounting for every possible user-defined `Record<T>`.

**Exit:** the documented Arrow/DB-handle paths respect their stated limits, including variable-width
records. Tests cover concurrent producers, full/closed queues, failed pushes, flush and cancellation
without leaked charges; unsuccessful admission leaves sequence/snapshot state unchanged. Explicitly
state the standalone generic-record API's accounting scope. Push/streaming benchmarks pass the gate.

### S9 — enforce prospective graph-buffer limits (G2)

**Progress (2026-09-20):** implemented and locally validated. Prospective source/output admission,
deferred input/frontier ownership and terminal failure handling pass correctness/static gates.
Matched timings and profiles retain a wide-fan-out caveat: +17% in an unrestricted comparison,
versus +3.9% in the longer fixed-core comparison. See the dated S9 record below.

**Owns:** source priming, graph-port admission, output fan-out and deferred input ownership.
**Modes:** all. **Risk:** high correctness/performance. **Depends on:** S7/S8 + fresh B0.

Existing gates inspect current usage before routing; the next output can overshoot a byte cap.
Inspect `is_downstream_at_capacity`, `gate_decision`, `push_to_port`, `route_output` and
`prime_sources` together. Preflight the incoming size with explicit fan-out/shared-buffer charging.
Preserve accepted input across deferral. An operator may already have mutated state before its
output size is known: never blindly rerun it on admission failure. Use the existing fault/recovery
boundary where deferral cannot safely preserve the executed result.

**Exit:** empty/nearly-full ports cannot overshoot the declared retained-byte limit; oversized
operator output has a typed, tested failure path. Fan-out, checkpoint/restart and retry tests find
no lost input or duplicate state transitions. Operator results match an independent reference;
benchmark queueing and output admission as well as the core execution loop.

### S10 — quota live reference-table state (G3)

**Progress (2026-09-20):** implemented and validated locally, with measured write/refresh costs.
See the S10 execution evidence below for the accounting contract, tests and performance caveats.

**Owns:** table storage/refresh/restore and quota configuration.
**Modes:** embedded and single-node; retain cluster rejection.
**Risk:** high accounting/atomicity. **Depends on:** S9 + fresh B0 for any hot-path changes.

Preflight the final state of an upsert/refresh/restore before mutation. Count encoded keys and
retained Arrow storage with explicit treatment of shared buffers and capacity. A row slice can
retain its original large allocation; existing checkpoint capture estimates are not an exact
live counter. Avoid a whole-map clone per update. Measure retention amplification before choosing
compaction; no eviction of live SQL keys or generic state-backend layer.
Reuse `TableStore::prepare_snapshot`, `install_prepared_snapshots` and the existing staged
checkpoint replacement, validating quota changes before their atomic installation boundary.

**Exit:** growth and over-limit restore fail atomically; replacements/deletes release the expected
retention; wide/nested/dictionary/view arrays and one surviving slice of a large batch are covered.
Failed multi-table refresh preserves the previous complete installation and readiness. Lookup,
refresh and checkpoint-under-load measurements remain acceptable.

### S11 — quota all local MV storage modes (G3)

**Progress (2026-09-21):** implemented and validated locally, with measured MV preflight/staging
costs and variable snapshot/checkpoint timings documented in the S11 execution evidence below.
Fresh baseline source, binaries and measurements are recorded under ignored `target/s11-mv/`;
prior uncommitted S8–S10 work is preserved. S12 workload qualification remains outstanding.

**Owns:** MV storage, publication preflight and restore.
**Modes:** embedded and single-node; retain cluster rejection.
**Risk:** high publication/performance. **Depends on:** S10 + fresh B0.

Cover Aggregate, Append, Upsert and Multiset modes, including one oversized append batch, owned
scalar/key bytes and multiplicity errors. Preserve intentional append retention semantics.
Reuse staged deltas, but validate all affected MV quota updates before applying/publishing any
of them. `update_mv_stores` currently updates and sends each MV sequentially; adding a later
quota error without this preparation could expose a partial cycle. This is a change-design
hazard, not evidence that local subscriptions offer transactional delivery today.

**Exit:** a failing second MV leaves both stores and quota-dependent publications unchanged;
successful cycles, replacements, deletes, restore and snapshot materialization match independent
expected results. State and cursor fault handling stay consistent. Reuse staged deltas; any new
state clone or lock needs bounded transient memory and benchmark evidence. MV update/materialization
and coordinator benchmarks/profiles pass.

### S12 — qualify the integrated workload (G5)

**Status (2026-09-21):** the workload observer, local release smoke matrix and repeated recovery
diagnostics pass, including the Kafka multi-source startup fix and RSS measurement correction
found during preparation. No production workload has been
qualified. See the S12 execution evidence and the
[workload harness instructions](../tests/qualification/README.md).

**Owns:** extensions to existing soak/oracle tooling and immutable evidence bundles.
**Depends on:** S1–S9, and S10/S11 when tables/MVs are advertised.

Use existing process-kill/checkpoint/rejoin harnesses and external sink readers. Measure arrival
through external visibility separately from compute-cycle and checkpoint-stall time. Record
p50/p95/p99/p99.9 with sufficient sample counts and tail resolution, queue slope, RSS and recovery.
Use a consistent observer clock or explicitly bound inter-host clock error; record offered load
and backpressure so a stalled producer cannot make latency look artificially healthy.

Run at least one-hour steady workloads, three repetitions, one/four pipelines and declared
uniform/skewed/hot-key distributions. Include slow/failed sinks, paused sources, checkpoint under
load, process/leader loss, rejoin/scale changes where supported, corrupt cuts and expired replay.
Add the G9 durable Backpressure/Fail saturation → checkpoint → restart external-ledger cases.

**Exit:** declared latency/recovery ceilings pass; queues stabilize below advertised capacity; RSS
plateaus inside the declared envelope. An independent oracle finds no missing ALO records and
no duplicate committed effects for exact compositions. Qualify each mode/composition separately:
Kafka output remains ALO; cluster exact candidates retain the existing Kafka → direct-S3 Delta
or supported REST-Iceberg admission. Native provider ALO evidence and ineligible standalone
contract scaffolds cannot certify another composition. Publish limitations with the evidence.

### S13 — qualify one supported upgrade pair (G7)

**Owns:** cross-version process/fixture tests and upgrade/rollback instructions.
**Modes:** persistent deployments. **Risk:** high authority/replay risk.

Select an actual predecessor release and inspect its state/partition/pipeline ABIs. An old binary
writes the committed cut; the candidate restores and continues against an external oracle.
Rehearse stop/checkpoint/upgrade/restart, incompatible-cut rejection before intake, and leadership
loss during transition. A rollback must respect authority and external effects after new commits;
copying an old checkpoint over current authority is not a safe generic rollback.

**Exit:** list the tested pair, supported directions and required retained artifacts. Either the
pair preserves continuity or it fails closed with an actionable unsupported-transition result.
No blanket rolling-upgrade, state-migration or arbitrary previous-version compatibility promise.

## Common verification and performance gates

Each implementation starts with a failing regression or a measured reproducer for its stated
problem, then uses existing coverage before adding tests. Do not add tests that mirror private
implementation. A refactor must preserve current error ordering, cleanup and admission boundaries.

For each completed code change run the relevant targeted tests, then the repository gates:

```powershell
$env:RUST_MIN_STACK = '4194304'
cargo test --workspace --lib --locked -j1 -- --test-threads=2
cargo clippy --workspace --all-features --all-targets --locked -j1 -- -D warnings
cargo clippy --workspace --no-default-features --locked -j1 -- -D warnings
cargo +nightly fmt --all -- --check
cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .
python tools/check_analytical_dependencies.py
```

S1 also requires the server binary suite, which `--lib` does not run. The server's default
features include cluster support. Run the suites together with the same stack setting:

```powershell
cargo test --workspace --lib --bin laminardb --locked -j1 -- --test-threads=2
```

S2 must similarly include the feature-gated DB cluster-admission regressions; a passing local-only
suite cannot cover inferred cluster ALO. Run integration targets named by the affected session.

Use `--offline` only when dependencies are already cached; S4 advisory data must be current.
The previous review passed these gates with documented Windows resource settings. That evidence
belongs to the base SHA and does not substitute for tests after implementation. Run heavy builds
serially on this machine; prior parallel linking exhausted available paging resources.

| Changed path | Required focused performance evidence |
|---|---|
| Constructor/auth/config only (S1/S2) | No hot-path benchmark required while edits remain cold |
| DataFusion/coordinator/queued bytes (S6/S7/S9) | `latency_bench`, `stream_executor_bench`, relevant `hot_path_micro` cases, plus changed admission/cancel/fan-out measurements |
| Core push/ring admission (S8) | `latency_bench`, `streaming_bench`, focused typed/Arrow push cases |
| Table/MV lookup and publication (S10/S11) | `latency_bench`, `lookup_join_bench`, relevant graph/hot-path cases; add meaningful update/refresh/MV cases if absent |
| Checkpoint/recovery affected by any change | `recovery_bench` plus existing real-process recovery tests; a microbenchmark is not a cloud recovery SLO |

Run `cargo bench --bench latency_bench` and the applicable benches before and after each hot-path
change on the same quiet hardware, release profile, feature set and workload. Confirm requested
feature-gated cases actually ran. Save raw distributions, throughput, allocation/retention and CPU
profiles (including IPC where the target hardware exposes it). Explain counter limitations.
The repository's IPC > 2 guideline is diagnostic, not a substitute for latency measurements.

**A regression over 5% blocks landing until removed or explained with evidence.** Do not hide it
by widening thresholds, dropping difficult inputs or benchmarking during other builds/soaks.
Statistical uncertainty requires another controlled measurement. Numeric defaults must fit the
measured envelope; limits on queues, DataFusion reservations and state are not additive proofs
of total RSS because buffers may be shared and some allocations remain outside those domains.

## Agent/session coordination

Use separate worktrees for simultaneous implementation sessions and one integration owner.
Adjacent small sessions may share a task while retaining separate reviewable changes; the list
does not require thirteen simultaneous agents or thirteen new tasks.
Do not commit unrelated `.zcode/` work. Suggested concurrency is S1 + S2, then S4 + S5 alongside
the serial DB lane where file ownership permits. Run one benchmark/soak at a time per host.
Independent review can run while another owner prepares an unrelated change; review does not
replace the owner running tests. Shared files are assigned before editing, not resolved by racing
agents. Never increase frozen readability baselines to accommodate growth.

Use this brief for each fresh session:

> Implement only session **S#** from `docs/production-hardening-plan.md`, using the current source
> and `docs/production-readiness.md` as evidence. Record the starting SHA and completed dependencies;
> recheck relevant assumptions if HEAD moved. Preserve the stated mode restrictions. Reproduce the
> problem, make the smallest change, run the session exit tests and common gates, and inspect the
> final diff for duplication and unrelated edits. For hot-path work, capture before/after benchmarks
> and profiles before claiming completion. Report changed APIs/config defaults, tests, benchmark
> deltas, residual risks and evidence paths. Split newly discovered problems into bounded follow-up
> work; do not silently widen this session or weaken an admission/test to make it pass.

At handoff record: base/result SHA or uncommitted diff, affected modes, reproduction, test results,
performance evidence, any compatibility change, outstanding blockers, and the next dependency.
A passing PR closes its scoped gap only; production readiness requires the corresponding S12/S13
evidence. S1–S3, S4a–S4e and S5 are **implemented and verified locally**. S4 is **partially
implemented**; the enforced scans still fail on unresolved dependency findings. B0 now has local
diagnostic timings and CPU/allocation profiles. S6a repairs cached-plan retention; S6b reservation
limits, S7 source queue byte bounds and S8 embedded push bounds are **implemented and verified
locally**. S8's admission and concurrent-burst regressions are characterized below; this is not
a blanket latency or RSS qualification. S9–S13 remain **not implemented**. Production
qualification is open.

## Implementation progress

S1/S2 changes were made against the base SHA above. Before production edits, five new delivery-policy
tests and five new HTTP tests failed for the expected admission/parser problems. Shared policy
validation now rejects durable shedding, and the existing server guard requires a console token
for non-loopback HTTP. Duplicate WS query tokens, including bare duplicate keys, fail authentication.
Docker, Helm and remote examples use environment/Secret references; no default credential was added.

The independent code review's bare-query-key finding was corrected and rechecked; no further
actionable findings remained. Verification on Windows passed:

- Workspace libraries and server binary: **6,003 passed, zero failed, one ignored**. This includes
  all five new delivery-policy regressions and existing graph/recovery coverage. The ignored test
  downloads an ONNX model and requires an external runtime; it is unrelated to these changes.
- Clippy with all features/all targets and with no default features, both with `-D warnings`.
- Nightly formatting, readability, analytical-dependency generation and diff whitespace checks.
- Strict Helm lint for defaults and all three CI values files; six render cases covered defaults,
  a token Secret, standalone/cluster/full values and a custom configuration/Secret key. Docker's
  bundled TOML parsed with the required token environment reference. No image build or Kubernetes
  deployment was run.

The full test command was `cargo test --workspace --lib --bin laminardb --locked --offline -j1 --
--test-threads=2`, with `RUST_MIN_STACK=4194304`. The default server features include cluster, and the
cluster delivery-admission test ran. These changes stay in configuration/startup and HTTP
authentication, outside the streaming record path; no hot-path benchmark claim is made.

Compatibility: non-loopback HTTP now requires `server.console_token`; durable delivery now rejects
`ShedOldest` through builder and direct configuration. Use Backpressure or Fail for durable delivery.
Regression logs and gate output are under `target/p0-hardening/`. S12/S13 release and upgrade
qualification remain outstanding.

### S3 — supported-surface documentation

Completed against `a16c5a6d` on `codex/p0-production-hardening`. The root and crate READMEs now
describe rejected PostgreSQL/MongoDB CDC admission, Delta reader versus streaming-route
capability, managed direct-source cluster windows, the single compute runtime, and local versus
cluster subscription replay. The root README links named admission examples and rejection tests.
The SQL README no longer presents PostgreSQL CDC as a positive connector example. `SECURITY.md`
now reflects the existing latest-minor support policy for 0.30 and the S1 HTTP authentication rule.

This is documentation only, affecting guidance for all modes. It changes no API, admission rule,
dependency or execution path. Existing contract coverage was reused; no duplicate capability
registry or tests of document wording were added. An independent source/test review found no
actionable inaccuracies, and local Markdown links resolve.

Validation: `cargo test --workspace --lib --bin laminardb --test cdc_admission --locked --offline
-j1 -- --test-threads=2` with `RUST_MIN_STACK=4194304` passed **6,005 tests, zero failed, one ignored**.
All 15 admission/replay/config checks linked from the corrected sections ran and passed. Both
Clippy gates, nightly formatting, readability, analytical-dependency generation and whitespace
checks passed. Test linking reported cached OpenSSL debug-symbol warnings; the builds and tests
succeeded. The ignored ONNX model test and proc-macro future-compatibility notice are unchanged.
Logs are under `target/s3-hardening/`. No performance or external-system qualification is claimed.

### S4 — initial dependency triage before enforcement

Read-only triage used cargo-audit **0.22.2**, cargo-deny **0.20.2**, and freshly fetched RustSec
revision `d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16` on 2026-09-19 against the unchanged lockfile.
Audit reported **seven vulnerability findings** in the lockfile; this is not a claim that all
seven are reachable in each deployed binary. In particular, the old rkyv entry has no selected
edge in the all-features/all-targets graph inspected during triage.

| Locked dependency | Finding / fixed range | Next bounded work |
|---|---|---|
| crossbeam-epoch 0.9.18 | RUSTSEC-2026-0204; fixed >=0.9.20 | Compatible update and regression checks |
| h2 0.4.14 | RUSTSEC-2026-0258; fixed >=0.4.16 | Compatible update and transport checks |
| quick-xml 0.39.4 | RUSTSEC-2026-0194 and RUSTSEC-2026-0195; fixed >=0.41.0 | object_store 0.13.2 and OpenDAL 0.57.0 constrain XML to 0.39; review a backport or coordinated dependency migration. Cloud response parsing reaches the affected reader. |
| rkyv 0.7.46 | RUSTSEC-2026-0235; fixed >=0.8.17 | Dormant optional rust_decimal dependency; review parent resolution/feature removal without changing the active 0.8 state ABI |
| rsa 0.9.10 | RUSTSEC-2023-0071; no patched release listed | Finish transitive signing/decryption reachability review before considering any exception |
| rustls 0.23.40 | RUSTSEC-2026-0285; fixed >=0.23.45 | Exact-version dry run succeeds but also upgrades aws-lc native crypto and webpki; validate all TLS feature sets |

Additional findings: unsound `event-listener` 5.4.1 (fixed >=5.4.2) and `lru` 0.16.3
(fixed >=0.18.2, constrained by chitchat), yanked `chacha20` 0.10.0, and unmaintained transitive
dependencies. Compatible-update dry runs succeeded for crossbeam-epoch, event-listener, h2 and
chacha20. A generic rustls update selected a still-affected version, so it is insufficient.

The checked-in `deny.toml` fails the current tool's schema before scanning. A temporary modernized
configuration also exposed missing license allow-list entries for Unicode-3.0, bzip2-1.0.6,
CDLA-Permissive-2.0 and BSL-1.0. License review, schema migration, finding remediation and the
nonpublishing failure exercise remain part of S4. The release workflow already depends on reusable
CI; audit/deny remain advisory-only and absent from `ci-success` until this work is completed.

No dependency versions, scanner policy, exceptions or workflows changed during this triage.
Full scan output, parent graphs and dry runs are under `target/s4-hardening/`. Follow up with
compatible dependency repairs first, then the constrained XML/LRU and RSA work, before enabling
the required audit/deny gates. Preserve the analytical-generation invariant and do not hide
unresolved findings behind blanket ignores. S5 can proceed independently; B0 remains required
before the serial S6–S11 memory changes.

### S4a — compatible dependency repairs and PEM migration

Implemented against `aa68f16e` on `codex/p0-production-hardening`. This bounded part of S4 repairs
dependencies compatible with the current analytical generation and removes the unmaintained
server PEM wrapper. It does not complete S4 or accept any advisory/license exceptions.

| Dependency | Previous | Updated |
|---|---|---|
| crossbeam-epoch | 0.9.18 | 0.9.21 |
| event-listener | 5.4.1 | 5.4.2 |
| h2 | 0.4.14 | 0.4.19 |
| chacha20 | 0.10.0 | 0.10.2 |
| rustls | 0.23.40 | 0.23.45 |
| rustls-webpki | 0.103.13 | 0.103.15 |
| aws-lc-rs | 1.17.0 | 1.18.1 |
| aws-lc-sys | 0.41.0 | 0.45.0 |

`concurrent-queue` leaves the graph with the upstream event-listener update. `rustls-pemfile` is
removed from the server and lockfile; pgwire now calls the existing rustls-pki-types `PemObject`
API through tokio-rustls. The former wrapper already used that parser. File-open ordering,
empty-file diagnostics, first supported private key selection (PKCS#1/PKCS#8/SEC1), certificate
and CA bundles, key permission warnings, expiry checks and failed-reload retention are preserved.
Underlying malformed-PEM detail strings now use the maintained parser's formatting; the server's
contextual error labels are unchanged.

The dependency updates affect builds using this workspace lockfile in all modes; the PEM change
affects the single-node and cluster pgwire listeners. No coordinator/operator code or state ABI
changes. Updated native crypto and transport behavior still require shipped-platform CI and
workload qualification; no hot-path benchmark or production latency claim is made here.

Cargo initially reselected unrelated parking_lot, socket2 and Windows dependency edges. The
committed selections for unchanged packages were preserved and locked metadata revalidated.
Independent review confirmed exactly eight version changes and two removals, with no unrelated
edge changes. The only changed edge on an unchanged package is the removed server PEM dependency.
Analytical dependency generations remain unchanged.

Against the same current RustSec revision recorded above, the scoped audit comparison verifies
five resolved advisory IDs: RUSTSEC-2026-0204, RUSTSEC-2026-0221, RUSTSEC-2026-0258,
RUSTSEC-2026-0285 and RUSTSEC-2025-0134. Vulnerability findings fall from seven to four. The two
quick-xml advisories, RSA and dormant rkyv remain; unsound LRU and three unmaintained dependencies
remain warnings in cargo-audit. The strict temporary cargo-deny configuration still fails on
active unresolved advisories and the existing license allow-list gaps. Scanner policies and CI
workflows are unchanged. Raw before/after reports and dependency evidence are in
`target/s4-compatible/`.

Validation: the focused TLS run passed **44 tests**. `cargo test --workspace --lib --bin laminardb
--test cluster_tls_integration --locked --offline -j1 -- --test-threads=2` with
`RUST_MIN_STACK=4194304` passed **6,007 tests, zero failed, one ignored**. This includes the three
new PEM regressions, both roots in the expanded CA-bundle test and the real cluster mTLS exchange.
Both Clippy gates, nightly formatting, readability, analytical-dependency generation, locked
metadata and whitespace checks passed. Independent source and dependency review found no
actionable issues. Cached OpenSSL debug-symbol warnings, the ignored ONNX model
test and the proc-macro future-compatibility notice are unchanged. Logs are under
`target/s4-compatible/`.

Next S4 work is the constrained XML/LRU dependency remediation, dormant-rkyv scope and RSA review,
then license/schema policy and required CI gate enforcement. No clean audit, release qualification
or branch-protection result is claimed.

### S4b — remove dormant legacy serialization dependency

The targeted Cargo update from rust_decimal 1.41.0 to 1.43.0 removes its optional rkyv 0.7
support and ten unused packages from the lockfile. Laminar's active rkyv 0.8.18 codec and the
analytical dependency generations are unchanged. The upstream release also includes decimal
arithmetic and formatting changes, so this is validated as a dependency update, not merely
manual lockfile cleanup. [Upstream release](https://github.com/paupino/rust-decimal/releases/tag/1.43.0).

Audit against the current RustSec snapshot now reports three vulnerability findings: the two
quick-xml advisories and RSA. The removed rkyv finding described an inactive optional dependency;
this update does not claim to repair an active Laminar checkpoint vulnerability. No advisory
exception or scanner policy change was made. Logs are under `target/s4-remaining/`.

Focused pgwire text/binary decimal checks with postgres-types 0.2.14 pass six cases on both
rust_decimal versions, covering signs, zero, extrema, scale 28, trailing zeros, known wire bytes,
invalid text and truncated/special binary values. A seventh check fails on both versions:
upstream `Decimal::from_sql` panics for an out-of-range binary NUMERIC value (`10^32`). This
pre-existing helper defect is retained as a reproducible finding, not suppressed. Current Laminar
handlers do not call that decoder; review it before adding typed NUMERIC parameter decoding or
qualifying downstream uses of the maintained pgwire helper. Workspace validation passed with
the S5 changes as recorded below; the exploratory decoder failure remains open.

The remaining XML fixes require an analytical-generation migration or maintained upstream
backport. LRU first resolves through Chitchat 0.13, which also changes the transport API, gossip
envelopes and dead-node retention; that requires a separate cluster upgrade and mixed-version
tests. Current Chitchat keys have no panicking destructor, but no advisory exception is accepted.
RSA still has no patched compatible parent; active reqsign uses randomized signing, not decryption,
which narrows the observed surface without proving absence of timing leakage. Preserve these
findings while proceeding with the independent S5 work; avoid local forks or compatibility wrappers.

### S4c — require dependency checks in CI and release

Implemented from `a8aedeee`. This changes repository/release policy for all shipped modes;
runtime code, dependency versions and hot-path behavior are unchanged. Enforcement now proceeds
before the constrained dependency migrations: the existing findings deliberately block CI and
release instead of being hidden by successful aggregate results. S4 remains incomplete.

The existing audit/deny jobs no longer tolerate failure, and both are required by `ci-success`.
Audit treats warnings as errors; deny checks the locked, all-features graph across the existing
six target triples and rejects vulnerability, unsoundness, unmaintained and yanked findings.
The old `instant` advisory ignore was removed; no advisory exceptions were added. The existing
release dependency chain already requires reusable CI before artifact builds, release creation,
crate publication and manifest updates, so it needs no additional scanner or workflow.

The deny policy uses the [current configuration schema](https://embarkstudios.github.io/cargo-deny/checks/advisories/cfg.html).
Unlicensed and unlisted licenses remain errors under the tool's default-deny license policy.
Four permissive license identifiers used by the locked dependencies are now explicitly allowed,
after checking package license files against their SPDX texts:

| License | Existing dependency examples | Conditions relevant to redistribution |
|---|---|---|
| [Unicode-3.0](https://spdx.org/licenses/Unicode-3.0.html) | ICU4X, unicode-ident | Retain copyright/permission notices in copies or documentation; no unauthorized name promotion. |
| [bzip2-1.0.6](https://spdx.org/licenses/bzip2-1.0.6.html) | libbz2-rs-sys | Retain source notices, identify altered sources and respect origin/endorsement restrictions. |
| [CDLA-Permissive-2.0](https://spdx.org/licenses/CDLA-Permissive-2.0.html) | webpki-roots, webpki-root-certs | Include the agreement with shared data. |
| [BSL-1.0](https://spdx.org/licenses/BSL-1.0.html) | xxhash-rust | Retain notices and license text, subject to the license's object-code exception. |

This allow-list decision does not verify notice packaging in every release artifact. Package
license paths/hashes and scan output are retained under `target/s4-gates/`.

Fresh scans with cargo-audit 0.22.2 and cargo-deny 0.20.2 used RustSec revision
`d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16`. Both exit **1** for substantive advisory findings.
Deny's license, source and ban checks have no errors; duplicate-version and one unused-license
warning remain. Its seven advisory errors are the two quick-xml vulnerabilities, RSA, unsound
LRU, and unmaintained number_prefix, paste and proc-macro-error2. Audit additionally reports
unmaintained `instant` in its broader lockfile scope. These are remediation/review work, not a
clean security scan or newly accepted exceptions.

The nonpublishing failure exercise executes the actual aggregate Bash step extracted from CI:
**65 cases passed**, including every required job failing, cancelling, skipping or missing its
result, all-success, and all audit/deny status combinations. The release dependency chain was
checked locally; actionlint 1.7.12 accepts both workflows. Three isolated license probes confirm
MIT is accepted while an unlisted GPL license and an unlicensed package are rejected. No hosted
CI/release run or external branch-protection setting was changed or verified. The exercise is
local evidence, not a publishing rehearsal.

Workspace library tests passed **5,672 tests, zero failed, one ignored**, using the locked,
offline graph, `RUST_MIN_STACK=4194304`, one build job and two test threads. Both Clippy gates,
nightly formatting, readability and whitespace checks passed. The ignored ONNX test and cached
OpenSSL debug-symbol warning remain unchanged. Validation logs are under `target/s4-gates/`.

Next S4 work remains constrained dependency remediation or explicit, owned, time-bounded
advisory review; B0 still blocks the serial memory changes. These passing regression and policy
checks do not override the failing security scans.

### S4d — remove unmaintained download-progress dependency

Implemented from `3d8b502d`. The targeted `hf-hub` update from
0.4.3 to 0.5.0 replaces indicatif 0.17.11 with 0.18.6 and console 0.15.11 with 0.16.6,
removing number_prefix 0.4.0 in favor of unit-prefix 0.5.2. The selected Tokio/Rustls client
features remain the same, as recorded in the [published 0.5.0 manifest](https://docs.rs/crate/hf-hub/0.5.0/source/Cargo.toml).
The upgrade retains Laminar's existing async-client and cache APIs.

This applies to builds enabling local AI, including the server. Cluster SQL admission is
unchanged. Production loader/inference code and the analytical dependency generations are
unchanged. Two focused regressions cover the existing Hugging Face snapshot layout and a local
HTTP download with progress reporting, label-cache publication and a second read without HTTP.
They need neither an external model download nor an ONNX Runtime installation.

Fresh audit/deny scans against RustSec revision `d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16`
remove only `RUSTSEC-2025-0119`, with no new findings. Deny retains six advisory errors;
audit also reports unmaintained `instant` in its broader lockfile scope. Both scans still
exit 1. No scanner policy or advisory exception changed. Evidence is under `target/s4-hub/`.

Validation passed: **six focused local-backend tests** and **5,674 workspace library tests**,
zero failures, plus both Clippy gates, nightly formatting, readability, locked metadata,
analytical-dependency generation and whitespace checks. The existing model-download/ONNX test
remains ignored; this is cache/client compatibility evidence, not a new inference qualification.
The Windows feature change required a wider rebuild; existing OpenSSL debug-symbol warnings
are unchanged. No coordinator/core-operator code changed, and no hot-path performance claim is made.

### S4e — remove the unsound gossip cache dependency

Implemented from `0c78edc0`. Chitchat 0.10.1 → 0.13.0 permits the fixed LRU 0.18.4,
removing `RUSTSEC-2026-0253`. The only added package is itertools 0.15.0. LRU now enables
default allocator/hasher features on the existing hashbrown 0.17.1; no other existing package's
selected features change. The analytical dependency generations and Tokio version are unchanged.

This affects cluster gossip discovery. Embedded, single-node and static discovery behavior are
unchanged. Chitchat's new [protocol selector](https://docs.rs/chitchat/0.13.0/chitchat/struct.ChitchatConfig.html)
is explicitly V0, preserving the existing uncompressed wire format. The existing partition-test
transport forwards the new envelope/outcome types and socket address, and reports zero bytes
when it drops a simulated packet. KV lookup compares the new shared node-ID type through borrowed
strings. No new wrapper, configuration knob or compatibility layer is added.
Callers supplying their own Chitchat transports must adopt the updated upstream socket API.
Laminar's software-version, discovery-protocol and process-generation admission checks remain intact.

A regression checks actual outbound V0 bytes, replies to legacy SYN vectors and rejection of
a foreign cluster. The vectors were checked against the published 0.10.1 UDP encoder in an
isolated probe. A second probe runs published 0.10.1 and 0.13.0 peers together over loopback UDP:
both directions pass live membership, initial values, updates, tombstones, dead-peer collection
and higher-generation rejoin. Its separate old dependency graph is test evidence under
`target/s4-gossip/mixed/`, not part of the workspace or shipped lockfile. This is protocol evidence,
not an S13 cross-release upgrade or rollback qualification.

Upstream also increases the bounded garbage-collected-node history from 500 to 5,000 entries and
uses shared node-ID strings. Laminar's explicit failure-detector and tombstone grace periods are
preserved. The larger history can reserve and retain more control-plane memory; production
RSS qualification remains open. No coordinator/core-operator code changed and no hot-path
performance claim is made.

Fresh scans at RustSec revision `d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16` remove only the
LRU advisory, with no new findings. Both scans still exit 1: deny retains five advisory errors
(two quick-xml findings, RSA, paste and proc-macro-error2), and audit additionally reports instant.
No advisory exception or scanner policy changed. Raw dependency, scan and probe evidence is
under `target/s4-gossip/`.

The locked/offline workspace library and cluster integration run passed **5,685 tests, zero
failed, two ignored**, using `RUST_MIN_STACK=4194304`, one build job and two test threads. This
includes **5,675 library tests** with the new V0 regression and ten core/DB cluster integration cases.
The normally ignored same-ID rejoin case also passed when explicitly selected; its existing
test ignore remains because one successful run does not establish repeatability. The ONNX
model test remains ignored. Both Clippy gates, nightly formatting, readability, locked metadata,
analytical-dependency generations and whitespace checks passed. Readability exceptions did not grow.

The test command was `cargo test --workspace --lib --test cluster_integration --locked --offline
-j1 -- --test-threads=2`; the rejoin check used the same targets with the
`killed_node_can_rejoin -- --ignored --exact --test-threads=1` filter. The two isolated compatibility
probe tests also passed. These local Windows results do not replace shipped-platform CI or S12/S13.

Windows linking initially exhausted disk space. Removing older generated incremental caches
freed approximately 110 GiB, preserving benchmark baselines, compiled outputs and evidence;
the same validation then passed. Existing OpenSSL debug-symbol warnings remain unchanged.

A 2026-09-20 registry/manifest follow-up found no additional narrow repair: object_store 0.13.2
and OpenDAL 0.57.0 remain the latest releases in their permitted series and still require XML
0.39. Delta Lake 0.32.4 still requires validator 0.19. Published reqsign-core 3.3.1,
reqsign-google 3.1.1 and reqsign-azure-storage 3.2.1 still depend on RSA 0.9; their archives were
checked against the registry checksums. No dependency or advisory policy changed. This was a
manifest review, not a fresh security scan; evidence is under `target/s4-next/`.

### S5 — Kafka reader progress and delivery freshness

Implemented from `598a656c` alongside S4b. This applies to Kafka sources in embedded, single-node
and cluster modes when a metrics registry and canonical source name are supplied.

Implementation reuses the existing canonical source name, source task tracker, bounded Kafka
metadata lookup and Prometheus registry. A source-owned sampler runs every ten seconds,
independently of the reader queue and connector polling. Samples use fresh broker high watermarks
and the next offset handed to the Kafka reader, not broker advisory commits. No new coordinator
or core-operator work, connector trait hook, dependency, or per-record allocation is introduced.

New source-labelled metrics expose reader offset distance, sample availability and timestamp,
and the timestamp of the last nonempty successful connector poll. Unknown positions, failed
queries and assignment changes cannot manufacture zero lag. Partition labels follow the current
assignment; source registration has one cleanup owner so retired workers cannot overwrite a
replacement's metrics. Sampling uses the existing shutdown budget and retains cancelled native
work through the existing blocking-task owner.

The overview dashboard distinguishes unavailable/stale samples from zero lag and shows source
delivery age separately. Checkpoint-stall guidance uses the existing successful-completion counter
for explicitly selected periodically checkpointed pipelines, preserving readiness semantics.
Reader lag may include offset gaps and uncommitted transactions; these signals do not measure
settled SQL progress, committed recovery offset lag or external sink visibility.

Eight focused regressions passed, including real librdkafka MockCluster writes while the reader
is paused, each assignment-generation fence, revocation, failed lookup, bounded close, startup
wiring and source replacement. The initial focused run caught a missing canonical source name
in the test fixture; the corrected fixture uses the existing validated startup contract.

Validation: `cargo test --workspace --lib --bin laminardb --test cluster_tls_integration --locked
--offline -j1 -- --test-threads=2` with `RUST_MIN_STACK=4194304` passed **6,015 tests, zero failed,
one ignored**. Both Clippy gates, nightly formatting, readability, locked metadata, analytical
dependency generations and whitespace checks passed. Final Clippy cleanup only renamed a test
binding. Dashboard JSON, all 60 dashboard/documentation PromQL expressions and local links were
validated. Independent source/dependency review found no blocking issue. Logs are under
`target/s4-remaining/`; the query validator is under `target/s5-query-validation/`.

No coordinator or core-operator code changed, so no hot-path performance claim is made. MockCluster
is local protocol evidence, not production broker qualification; the metadata-query overhead at
production partition counts remains S12 work. The existing ignored ONNX model test, cached OpenSSL
debug-symbol warnings and proc-macro future-compatibility notice are unchanged. Next is B0 before S6.

### B0 — Windows timing baseline (2026-09-19)

Started from `42657355`. Reuse `latency_bench` and the existing `stream_executor_bench` cases
`plain_select`, `agg_group_by`, `sort_limit` and `query_chain`. The SQL diagnostic uses one local,
ephemeral DB, 1,024-row batches with four numeric columns and one short string column, four uniformly
distributed region keys, and the in-process source/subscription path. Input is closed-loop:
push a batch, then wait for output. This does not measure an independently offered event rate,
external sink visibility, durable recovery or Kafka overhead. Checkpointing is disabled and no
external object store is involved. Production latency/recovery ceilings and an RSS envelope remain unset.

Host inventory records an AMD Ryzen 9 7900X (12 cores / 24 logical processors), approximately
31 GiB usable RAM, Windows 11 Pro build 26200 and Rust 1.98.0. Use the same optimized bench profile
with `CARGO_PROFILE_BENCH_DEBUG=1` and `CARGO_PROFILE_BENCH_STRIP=none` for before/after runs, so
profiles retain symbols. Builds and benchmark runs use `RUST_MIN_STACK=4194304`.
Raw build/host evidence is under `target/b0/`.

The original SQL smoke run failed source-schema admission: the fixture registered a two-column mock
connector for a five-column source, then pushed batches through the separate embedded input path.
The four selected cases now use the existing in-process source directly, create Tokio's timeout
inside its runtime and reuse one warmed pipeline. Setup and graceful shutdown are outside timing;
each measured iteration includes the shallow input clone, push, scheduling and first output decoding.
This avoids concurrent fixtures and timed teardown from the previous batched harness. The separate
high-cardinality cases are unchanged and excluded from this baseline. All four corrected optimized
smoke cases passed. All-features/all-targets Clippy, nightly formatting and readability passed;
the earlier workspace and no-default-features gates cover the unchanged production code.
No engine execution code or dependency changed.

Uninstrumented Criterion runs completed against source commit
`54b551b14f7bb8b36b0ab7504e8a7c0f48a66c70`, using the matching working-tree binaries built before
that commit. `target/b0/baseline-identity.json` records executable paths, SHA-256 hashes and run time.
Raw samples and estimates are saved under `target/criterion/**/s6-before/`; the host remains on its
existing Balanced power scheme. Each case collected 100 samples after a three-second warmup, with
a five-second measurement target automatically extended for the slower SQL cases.

| Diagnostic | Criterion mean | 95% confidence interval |
|---|---:|---:|
| Tumbling-window assignment | 1.414 ns | 1.409–1.421 ns |
| Projection/filter, 1,024 input rows | 0.921 ms | 0.795–1.051 ms |
| Four-group aggregate, 1,024 input rows | 1.476 ms | 1.265–1.702 ms |
| Sort/top ten, 1,024 input rows | 1.161 ms | 0.989–1.336 ms |
| Three-query chain, 1,024 input rows | 1.434 ms | 1.285–1.584 ms |

These are mean estimates from `estimates.json`, not Criterion's displayed regression slopes or
event-latency percentiles. SQL intervals are too wide to resolve a 5% regression confidently;
repeat matched before/after measurements on a stable target host before accepting hot-path changes.
The window-assignment microbenchmark does not measure full event processing.

The reproducible commands, with the two executable paths from `target/b0/executables.json`, are:

```text
cargo bench -p laminar-core -p laminar-db --no-default-features --bench latency_bench --bench stream_executor_bench --no-run --message-format=json --locked --offline -j1
<latency_bench.exe> --bench --noplot --save-baseline s6-before
<stream_executor_bench.exe> --bench "^(plain_select|agg_group_by|sort_limit|query_chain)/" --noplot --save-baseline s6-before
```

CPU/IPC and allocation profiles were not captured in this Windows run. WPR rejected the named
CPU/PMC capture with `0xc5585011` (could not enable the profiling policy), and image-specific heap tracing with
`0x80070005` (access denied). The execution token lacks the profiling privilege. Final checks show
the task's WPR instance idle and heap tracing disabled; no profile workload ran. Capture logs and
the validated PMC profile were recorded under `target/b0/`. This timing-only run did not close B0.
The Linux follow-up below supplies diagnostic profiles. The previous ignored Windows evidence
directories were no longer present at that follow-up; the numbers above remain historical results.
No production workload targets or memory defaults have been inferred from them.

The memory ownership contract for the later serial work is deliberately limited to existing owners:

| Domain | Admission, ownership and release | Current bound / transient gap |
|---|---|---|
| Connector queue | Source actor transfers a batch through `SourceMsg`; dequeue transfers ownership to the coordinator, not necessarily to free memory. | Count-bounded plus a shared 64 MiB Arrow-byte budget, retained through parking and released at staging/discard. Each source may additionally hold one validated waiting batch up to the same limit; connector decode scratch is outside this budget. |
| Embedded input | `SourceEntry::push_and_buffer` admits into the core source channel and retains snapshot/broadcast references until their owners release them. | Count limits do not cover arbitrary Arrow width or all retained snapshots. S8 owns this separate path. |
| Parked/staged cycles | The coordinator retains parked messages and cycle buffers until execution, retry, recovery or cleanup resolves them. Cursor settlement follows successful publication. | These retained references outlive dequeue; a released queue permit cannot serve as their memory budget. |
| Graph ports | `OperatorGraph` admits and retains input/output batches, then releases port ownership when consumed or cleared. | Existing Backpressure/Fail/BestEffort shedding applies; pre-route current-usage checks can overshoot on the next batch. S9 owns prospective admission and fan-out treatment. |
| DataFusion reservations | The per-DB runtime pool owns participating reservations until the consumer releases/drops them; main, graph and auxiliary contexts share it. | S6b adds a 256 MiB configurable fallible-reservation limit and disables DB-context spilling. Direct Arrow/expression allocations remain outside reservations. |
| Reference tables (S10) | Stores retain live keys/rows through upsert, refresh and restore; replacement or drop releases ownership. | Per-table row/retained-byte preflight counts shared Arrow capacity once per table and preserves the previous installation on failure. Prepared replacements, input scratch and pinned checkpoint/query snapshots have separate ownership and need headroom. |
| MVs | Stores retain aggregate/append/upsert/multiset results through publication and restore; replacement/delete or configured append retention releases them. | No comprehensive live-byte quota across storage modes. S11 owns preflight and failure atomicity. |
| Checkpoint scratch | Capture retains immutable frames while background serialization/persistence overlaps live state, then releases them on completion/cleanup. | Existing checkpoint data limits do not establish a whole-process or transient scratch-memory allowance. |

No values in this ownership inventory are an additive RSS guarantee. Establish measured headroom
before selecting memory defaults; keep the S6 pool scoped to participating reservations.

### B0 — Linux timings, profiles and retained-plan finding (2026-09-20)

Measured production/benchmark source at `9bb1e996f9ff2b7287d2decc80d55d3b482bda4b`, with only
documentation edits during the run. The existing Ubuntu 24.04.4 WSL2 environment supplies
user-process profiling without changing kernel profiling permissions. It exposes 24 logical CPUs
on the same Ryzen 9 7900X, approximately 15.2 GiB RAM and 4 GiB swap. Windows reported Balanced
power at run start. Rust 1.95.0 built the locked graph with one build job, opt-level 3, thin LTO,
one codegen unit, debug level 1, no debug stripping and `RUST_MIN_STACK=4194304`. This is a benchmark build
at the declared MSRV, not an all-feature MSRV qualification.

Reused the workload described above. All five optimized smoke cases passed. Each timing case
collected 100 samples after a three-second warmup; SQL measurement targets were twenty seconds,
and the assignment target was five seconds. Timings ran before profiling, with no concurrent build.
The table reports Criterion **means and 95% confidence intervals**, not regression slopes or
per-event percentiles. Different OS/toolchain and source identities prevent comparison with the
Windows numbers as a regression/improvement claim.

| Diagnostic | Mean | 95% confidence interval | User-process IPC | Profile peak heap, MB |
|---|---:|---:|---:|---:|
| Tumbling-window assignment | 5.057 ns | 5.030–5.082 ns | — | — |
| Projection/filter, 1,024 rows | 136.15 µs | 134.53–137.82 µs | 0.453 | 1.47 |
| Four-group aggregate, 1,024 rows | 144.17 µs | 142.66–145.67 µs | 1.134 | 1.49 |
| Sort/top ten, 1,024 rows | 150.69 µs | 148.40–153.24 µs | 0.390 | 16.74 |
| Three-query chain, 1,024 rows | 161.12 µs | 158.79–163.89 µs | 0.646 | 40.49 |

Perf 6.8.12 recorded grouped user cycles/instructions for requested fifteen-second SQL profiles;
both counters report 100% running time. These aggregate process counters include runtime/harness
work under virtualization. They are below the IPC > 2 heuristic and do not establish an optimized
compute kernel. Kernel scheduling counters were excluded; their reported zeros are not evidence
of no context switches. Separate CPU stack captures use 16 KiB DWARF stacks at 199 Hz. The first
chain capture lost samples and is superseded by a 99 Hz repeat. All four accepted captures report
zero lost samples and contain resolved application stacks. Perf writes on the Windows mount
failed; captures succeeded on Linux's native filesystem and were copied into the workspace.

[Heaptrack](https://github.com/KDE/heaptrack) 1.5.0 profiled the same SQL cases separately with a
requested five-second duration. The table's rounded decimal MB values measure intercepted heap
allocations across fixture setup, warmup, execution and teardown using the system allocator.
They are not DataFusion reservation totals or production RSS limits. The uninstrumented SQL timing
process peaked at **165,056 KiB RSS**; this short diagnostic does not establish a steady plateau.

**New G2 evidence:** a fifteen-second chain allocation profile peaked at **143.27 MB**, compared
with 40.49 MB in the five-second profile. Its exported live-heap timeline rises through roughly
40, 61, 82, 105 and 127 MB during the long execution, then falls to about 134 KB on teardown.
Retained stacks point to DataFusion metric labels/values and their registry under cached projection
and sort execution. Source inspection matches the observation: `execute_cached_plan` repeatedly
collects the same plan; `ProjectionExec::execute` registers new metrics, and
`ExecutionPlanMetricsSet::register` appends to its shared set. This is retained execution state
until the plan is released, outside participating memory reservations. S6 therefore starts with
a focused reuse repair before adding the pool. No engine fix or safe memory default is claimed here.

Raw samples, profiles, reports, exact commands, hashes and the retention timeline are under
`target/b0-wsl/`; `summary.json` identifies the accepted and superseded CPU reports. The native
build/profiles remain in `/home/sujit/.cache/laminardb-b0-9bb1e996` inside Ubuntu. Executable hashes:

- `stream_executor_bench-a84282b6a7c6d167`: `7b478bdb422666695b3d79aff3c122b3aa8cac2de114921332652dbb17a8fd41`.
- `latency_bench-c15ce6117c8edbe1`: `41cf9196f2b4a8f65dd4ee6e693c4f53c2859b059bdea0e4cadf1eb7f66d0b9a`.

Reproduction uses the prior build command with `cargo +1.95.0`, the documented profile variables
and native `CARGO_TARGET_DIR`. Run the two binaries with `--bench --noplot --save-baseline
s6-before-wsl`; add the existing four-case SQL filter and `--measurement-time 20` for SQL.
Profiles use that filter narrowed to one case with `--profile-time 15` (perf) or `5` (heaptrack).
Refresh the matched Linux baseline immediately before S6; retain the >5% regression gate and
repeat uncertain measurements. Production workload targets, external visibility, recovery,
other feature combinations and shipped-platform qualification remain S12/S13 work.

All 32 recorded benchmark/capture/export commands exited successfully; trace quality review rejected
the lossy first chain capture despite its zero exit status. Formatting, readability, analytical
dependency, local-link and whitespace checks passed. This follow-up changes documentation only;
prior workspace test/Clippy
results are historical, and no new full-workspace regression run is claimed.

### S6a — release cached execution state between batches (2026-09-20)

Implemented for all DB modes. Cached plans remain unexecuted templates; each collection uses
DataFusion's [`reset_plan_states`](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/execution_plan.rs)
to give metrics and join build state one execution lifetime, including errors and cancellation.
Live source slots stay shared. All SQL, aggregate/window pre-projection and post-projection cache
paths use the same execution helper. No dependency, public setting or cache framework was added.

Preparation disables dynamic-filter pushdown only in its planning-state copy and rejects recursive
plans after view expansion. Direct file scans are also rejected because DataFusion 53.1's
[`DataSourceExec`](https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource/src/source.rs)
returns the same leaf on reset, retaining file-source metrics. Connector I/O supplies live Arrow
batches to streaming plans; ordinary one-shot DataFusion queries keep their existing behavior.

**Correctness:** nine new regressions cover changing ascending/descending Top-K inputs, live join
inputs, running aggregates, window closure, compiled fallback after an error, repeated query errors,
cancellation and subsequent execution, and recursive/file-plan rejection without changing ad-hoc
queries. The workspace library suite with cluster features passed **5,684 tests, 0 failures,
1 ignored** (the existing external ONNX-model test). Both required Clippy configurations, nightly
formatting, readability (19 module / 195 function exceptions), analytical dependency and whitespace
checks passed. The diff review found no new unused code, unnecessary abstraction or unrelated cleanup.

**Performance:** refreshed the existing Linux baseline before editing and preserved both binaries.
Same WSL host, Rust 1.95.0, bench profile, five workloads and 100-sample protocol as B0; builds were
finished before measurements. Initial Criterion means:

| Workload | Before | After | Change |
|---|---:|---:|---:|
| Window assignment | 4.872 ns | 5.107 ns | +4.83% |
| Plain select | 131.49 µs | 137.69 µs | +4.72% |
| Four-group aggregate | 142.97 µs | 150.00 µs | +4.91% |
| Sort / top 10 | 143.37 µs | 175.83 µs | +22.64% |
| Three-query chain | 157.17 µs | 163.24 µs | +3.87% |

The sort result exceeds the 5% gate and is retained as an **explained correctness cost**, not a
sub-5% performance claim. Two matched repeats, with reversed run order, measured sort changes of
**+4.23%** (167.59 → 174.69 µs) and **+15.29%** (153.48 → 176.95 µs). The unchanged select control
varied −4.07% / +4.92%; even the identical core binary varied +4.83% in the initial comparison.
CPU profiles attribute 28.81% of post-fix samples to Top-K heap maintenance. An independent probe
confirmed that the old cached plan returned `[1000, 900]` for one batch, then incorrectly returned
no rows for `[100, 90, 80]`; resetting returned `[100, 90]`. The old retained cutoff skipped valid
sorting work. Keeping that shortcut would preserve incorrect results. This does not establish a
production latency budget; target-workload qualification remains S12.

Heaptrack peaks changed from **16.74 MB → 1.66 MB** for sort and **40.49 MB → 1.73 MB** for the
five-second chain profile. The longer chain profile changed from **143.27 MB → 1.73 MB**; sampled
live heap stayed near 1.49–1.71 MB after warmup and returned to 134 KB at teardown. Participating
reservation limits and whole-process memory bounds are still separate work. Both 99 Hz CPU captures
had zero lost samples; process IPC ranged 0.47–1.33 on WSL, below the 2.0 kernel guideline.

Evidence is in `target/s6-cache/`: commands, test/Clippy logs, Criterion samples and confidence
intervals, CPU/heap traces, the isolated Top-K probe, source hashes and `summary.json`. The final
SQL benchmark SHA-256 is `65a2b170a048949b7553d20715f9cfbf625292adf0d35fe09acda6a56291fd19`;
the core binary is unchanged from B0. Starting HEAD was `23666ebf`. Native binaries and profiles
remain under `/home/sujit/.cache/laminardb-b0-9bb1e996/s6-cache` for the next bounded change.

### S6b — share bounded DataFusion reservations (2026-09-20)

**Status:** implemented and verified locally. Starting HEAD:
`07d0e6b45b001732274ab81a9d6387d7bc88b7b5`; result is an uncommitted diff.

All DB modes now create one DataFusion 53.1 `GreedyMemoryPool` per `LaminarDB`, shared through
the runtime used by main queries, connector operator graphs, sink-filter contexts and the
cluster local-table diagnostic. New graph generations retain the same pool. Separate DBs have
separate budgets. Catalogs remain separate where they were previously separate.

`LaminarConfig::datafusion_memory_limit_bytes`, the matching builder method, and
`[server].datafusion_memory_limit_bytes` select the finite limit; the default is **256 MiB**.
Zero is rejected, including direct server/cluster startup before discovery or lease acquisition.
Changing the server setting requires restart. DB-owned contexts use `DiskManagerMode::Disabled`,
including ad-hoc queries. Participating allocation failures retain DataFusion's resource error
or the existing `DbError::QueryPipeline`; translation identifies resource exhaustion as query
execution failure (`LDB-9001`) rather than an internal bug.

The budget covers fallible DataFusion reservations, not every Arrow allocation or process RSS.
Queues, managed state, tables/MVs and checkpoint scratch remain separately owned. Connector-owned
I/O contexts, standalone `laminar-sql` factories and its thread-local lambda context retain their
existing defaults and are explicitly outside the per-DB scope. The 256 MiB policy does not establish
a production memory envelope or close G2. S7–S11 and S12 workload sizing remain separate work.

Before implementation, regressions demonstrated that a default DB admitted a reservation larger
than the proposed cap and enabled temporary spill files. Fourteen new tests cover default/explicit
limits, independent DBs and concurrent contexts, real sort/aggregate/join exhaustion, cached-plan
retry and cancellation with live reservations, the cluster diagnostic, connector-graph failure
source reporting and reconstruction, configuration/startup validation, and error translation.
All **6,040 workspace library/server tests passed, zero failed, one ignored** (the existing
external ONNX-model test). Both Clippy configurations with `-D warnings`, nightly formatting,
readability, analytical-dependency and whitespace checks passed. Tests used one Cargo build job,
two test threads and `RUST_MIN_STACK=8388608`; existing cached OpenSSL debug-symbol warnings did
not prevent linking. No production cursor/recovery behavior or cluster SQL admission was changed.

**Performance:** same WSL host, Rust 1.95.0, release settings, five workloads and 100-sample
protocol as S6a, with a refreshed baseline before implementation. SQL measurement targets were
20 seconds, with three-second warmups; builds and profiling did not overlap measurements.
The table reports Criterion means; raw estimates include their 95% confidence intervals.

| Workload | Before | After | Change |
|---|---:|---:|---:|
| Window assignment | 5.160 ns | 5.107 ns | −1.02% |
| Plain select, 1,024 rows | 137.42 µs | 139.06 µs | +1.19% |
| Four-group aggregate, 1,024 rows | 149.71 µs | 151.96 µs | +1.51% |
| Sort / top 10, 1,024 rows | 179.24 µs | 178.00 µs | −0.69% |
| Three-query chain, 1,024 rows | 164.28 µs | 169.45 µs | +3.15% |

The chain's initial approximate 95% change interval overlapped the 5% gate, so it and the select
control were repeated with reversed run order and 30-second measurement targets. The chain
measured **170.05 → 168.02 µs (−1.19%)**, with an approximate change interval of **−2.86% to +0.48%**;
select measured **141.82 → 143.18 µs (+0.96%)**. No measured mean exceeded the 5% gate. This is local
diagnostic evidence, not a production latency or RSS qualification. The unchanged `hot_path_micro`
kernels passed optimized smoke checks; they do not construct a DataFusion runtime and do not
measure this pool change. No record-path kernel, queue admission or fan-out implementation changed.

Separate Heaptrack profiles measured peak heap **1.66 → 1.63 MB** for sort and **1.73 → 1.69 MB**
for the 15-second chain; both returned to the same roughly 134 KB process teardown remainder as
their before runs. These figures include fixture/runtime allocations and are not pool accounting.
Four 99 Hz CPU captures had zero lost samples. Grouped user counters ran 100% of the requested
time: sort IPC **1.286 → 1.284**, chain **0.657 → 0.678**. These virtualized process counters include
runtime/harness work and remain below the IPC > 2 kernel guideline; no kernel optimization or
target-hardware qualification is claimed.

`target/s6-memory/` contains commands, source/binary hashes, regression and gate logs, Criterion
samples/intervals, CPU/heap traces and `summary.json`. Native artifacts remain under
`/home/sujit/.cache/laminardb-b0-9bb1e996/s6-memory`. The before binaries were preserved from S6a and
verified by hash. The after SQL benchmark SHA-256 is
`a6cb9365e40cc25eb4dafaebf6e546bd8b165c34ae19653b10cd1a1d4a4f4ed3`; the core binary is unchanged.
The source/diff review found no new dependencies, per-row bookkeeping, unused abstractions or
unrelated edits. The next serial session is S7; release qualification and remaining S4 findings
remain open.

### S7 — bound connector-to-coordinator queued bytes (2026-09-20)

**Status:** implemented and verified locally; correctness gates pass and the final matched timing
means have no regression above 5%.
Starting HEAD: `014b997f07ffcef443f95da978e4a2a07fdebe3d`.
**Modes:** all connector pipelines in embedded, single-node and cluster deployments. Cluster
SQL/delivery admission, checkpoint formats and committed cursor semantics remain unchanged.

`LaminarConfig::source_queue_max_bytes`, its builder method, `PipelineConfig::source_queue_max_bytes`
and `[server].source_queue_max_bytes` configure a **64 MiB** default shared by all source senders
in one coordinator generation. The existing 64-message default also remains in force. Zero and
values above the platform semaphore/u32 range (`MAX_SOURCE_QUEUE_BYTES`) fail before connector
startup; both server entry points validate before discovery/leases. Server changes require restart.

The source channel owns one Tokio byte semaphore. An owned permit travels with each queued
message through dequeue and intake parking until staging or discard. Closing the receiver wakes
byte waiters even when a parked message still holds capacity. Normal sends, pending-cursor sends
and both shutdown-tail `try_send` paths share admission. A batch larger than the entire budget
fails promptly, before a pending cursor can retain it. Refused input never advances the
coordinator's recovery cursor. Cancelled acquisition, failed count admission and dropped messages
return their charges. Barriers remain in the mixed FIFO and bypass only the data-byte semaphore;
per-source ordering and existing bounded checkpoint/shutdown failure paths are preserved.

Accounting uses Arrow-reported retained array storage plus fixed batch/column charges. Slices,
views and nested arrays retain their backing storage in this accounting; aliases are charged
independently, without a per-row allocator or deduplication map. Each source can additionally
hold one validated batch while waiting for capacity or a cursor, up to the configured limit.
Connector decode scratch and schema/cursor metadata are outside this queue charge. Staging
transfers Arrow ownership into the cycle/graph; it does not prove that storage was freed.
Embedded push rings, staged/graph buffers, replay, tables/MVs, sink buffers and checkpoint scratch
remain separate owners. S8/S9 and the remaining memory/qualification sessions are still required.

**Correctness:** the pre-change regression admitted a batch exceeding 64 MiB. Eighteen new tests
cover configuration/startup in each mode, oversized data and deferred cursor capture, parked input
through actual coordinator staging, multiple producers with a slow consumer, FIFO barriers while
bytes are saturated, partial reservation cancellation, lease loss, shutdown-tail admission,
receiver closure and backing storage retained by wide slices/views. Targeted validation passed
**404 tests**. All **6,058 workspace library/server tests passed, zero failed, one existing ignored**
ONNX-model test. Both Clippy configurations with `-D warnings`, nightly formatting, readability,
analytical-dependency and whitespace checks passed. Builds used one Cargo job, two test threads
and `RUST_MIN_STACK=8388608`; cached OpenSSL debug-symbol warnings did not prevent linking.

**Performance:** standard core/SQL baselines were refreshed before implementation. Final SQL
comparisons use the same B0 WSL/Linux host, Rust 1.95.0, no default features, optimized builds
with debug symbols, 100 Criterion samples, a 3-second warmup and a 30-second measurement target.
Candidate runs precede baseline runs; builds, timings and profiles run serially. The unchanged
core benchmark binary measured 5.368 → 5.176 ns in the initial matched check; optimized core and
hot-path kernel smoke checks passed.

Three new public-API burst cases exercise narrow rows, 4 KiB strings and four sources. Each
sends 64 batches of 256 rows per source and waits for every row at the subscription. Setup and
shutdown are untimed; input clones share Arrow backing storage. A nullable-schema mismatch was
corrected in both fixtures. The wide fixture also explicitly retains 128 MiB of output in both
versions: its original 16 MiB live-log default could evict unread output from a 64 MiB burst,
under tracing and during a longer timing run. Those incomplete runs are retained as invalid
evidence. Both rebuilt versions use the identical corrected fixture, all 64 batches and the
same source-admission configuration. The fixture's output retention is separate from S7's budget.

| Criterion mean | Before | After | Change |
|---|---:|---:|---:|
| Plain SELECT, 1,024 rows | 142.39 µs | 140.88 µs | −1.06% |
| GROUP BY, 1,024 rows / 4 groups | 154.41 µs | 151.67 µs | −1.77% |
| Sort / top 10, 1,024 rows | 182.92 µs | 181.03 µs | −1.03% |
| Three-query chain | 168.75 µs | 168.23 µs | −0.31% |
| Narrow burst | 174.33 µs | 177.00 µs | +1.53% |
| Wide burst with 128 MiB output history | 10.310 ms | 6.723 ms | −34.79% |
| Four-source burst | 437.32 µs | 431.71 µs | −1.28% |

The initial four-source (+33.4%) and aggregate (+15.5%) slowdowns did not recur in this longer
matched comparison. Raw samples, mean confidence intervals and approximate change intervals
are retained. The wide case has substantial variance; its observed improvement is diagnostic,
not a promised speedup. No production code was changed to make a timing pass.

CPU and heap captures passed for four-source bursts, wide bursts and GROUP BY in both versions.
CPU sampling used 49 Hz DWARF stacks; final reports disable the failing inline-symbol lookup.
Some frames remain unresolved, so attribution is qualitative. Arrow concatenation dominates
the burst profiles (about 56% and 91–93% inclusive CPU respectively). Source publication is
about 1.5% in the candidate four-source profile. IPC remains below the >2 heuristic in both
versions: 0.41 → 0.55 for four sources, 0.17 → 0.17 for wide bursts and 1.16 → 1.16 for GROUP BY.

Heaptrack peak allocated memory was 19.79 → 20.34 MB for four sources, 149.23 → 191.22 MB for
wide bursts and 1.84 → 1.84 MB for GROUP BY. These fixed-time profiles execute different amounts
of work and include connector concatenation, output history and runtime allocations. The wide
heap increase is retained in the evidence; a source-queue limit does not constrain those other
owners. Instrumented RSS also includes profiler overhead. These local diagnostics do not qualify
a production RSS or external-visibility envelope. S8/S9 and workload qualification remain necessary.

Commands, gate/regression logs, source/binary identities, raw Criterion samples and profiles are
under `target/s7-queue/`, including `final-summary.json`, `final-perf-commands.json` and final
source/binary hashes. The pre-change source snapshot and corrected fixture are preserved there;
native binaries remain under `/home/sujit/.cache/laminardb-b0-9bb1e996/s7-queue`. Final candidate
SQL benchmark SHA-256: `a256a6042c7db3fe6f1503c262838b76e4d79b975c3865fb9b0b551c96ab836b`.
All 36 final build/smoke/timing/profile commands passed. Runtime source hashes are unchanged
since the correctness gates; formatting and all-target Clippy passed again after the fixture fix.
The final diff review found no dependencies, readability exceptions, per-row bookkeeping or
unrelated changes. The next serial session is S8. S4 dependency findings and production
release/upgrade qualification remain open.


### S8 — bound embedded push queues and snapshot history (2026-09-20)

**Status:** implemented and verified locally; admission and concurrent-burst costs are recorded below.
Starting HEAD: `3688fab2ad8282cad8f7c1f2cc7270ed54ec361e`. Changes are uncommitted.
**Modes:** embedded/in-process source admission, including those handles where admitted by other
runtime modes. Cluster SQL/delivery admission and checkpoint formats are unchanged.

`SourceConfig::max_queued_bytes` gives each core source a **64 MiB** Arrow-byte budget shared
by all producer clones, its input ring and queued broadcast references. A private batch owner
retains one semaphore reservation until the last queued reference is delivered, evicted or
dropped. Single-subscriber delivery can move the original batch instead of cloning its column
vector. Count saturation and byte saturation return `ChannelFull`; an individually oversized
batch returns `BatchTooLarge { bytes, limit }`. Both are nonblocking with respect to capacity.
Closed drain tasks return `Disconnected`. Failed pushes do not advance sequence or watermark.
Core constructors remain infallible; invalid byte limits reject Arrow pushes with `InvalidConfig`.

`LaminarConfig::push_source_max_bytes` and the corresponding builder method configure each
registered source. DB construction rejects zero and values above the core semaphore/u32 maximum.
Typed handles convert records to Arrow before admission, so variable-width records cannot bypass
the limit. Raw handles, SQL inserts and API writers share the same admission path. Handles and
source metrics include byte saturation in `is_backpressured`; utilization remains count based.

Snapshot history is a separate owner capped by the **same byte limit**, plus its existing count
limit. Admission and snapshot publication share the source's history mutex; rejected pushes leave
history and notifications unchanged, and concurrent successful pushes preserve broadcast/history
order. Successful pushes evict oldest history until both bounds fit, without dropping queued input.
The accounting helper reuses S7's retained-array calculation, now shared from core; S7's charge
and admission semantics are unchanged. Aliases are charged independently, and slices/views/nested
arrays retain their backing-storage charge.

Each caller can hold input/conversion scratch before admission. Snapshot readers and returned
subscription batches have separate ownership and may outlive queue/history release. Schema metadata,
allocator overhead, downstream graph/state/sink retention and process RSS are outside these caps.
The standalone generic `Record` API remains count bounded without arbitrary heap-size estimation.
The query-result bridge also retains its separate count-bounded output ownership; a regression
verifies that a query result larger than 64 MiB is not silently truncated by an input budget.
Existing broadcast lag/eviction semantics remain; this is not a new delivery guarantee.

Cancellation follows actual ownership. Dropping a subscriber releases its queued references;
writer flush is still a no-op and writer close does not release another owner's data. If the core
drain task is cancelled, Crossfire retains unread ring values while producer handles survive.
Those values keep their charges until the last producer drops, while further pushes fail closed.
The cancellation test checks this lifetime rather than clearing a counter while storage remains.

**Compatibility:** synchronous push signatures and infallible core constructors are preserved.
Existing exhaustive `SourceConfig`/`LaminarConfig` literals must supply the new field or use
`..Default::default()`; exhaustive `StreamingError` matches must handle `BatchTooLarge`.
Callers that previously queued more than the new default must configure a suitable limit, retry
`ChannelFull` after consumers progress, or split an individually oversized batch. Snapshot history
can now evict by bytes before reaching its count limit.

**Correctness:** before the change, the new regression admitted a 72 MiB Arrow batch. Twenty new
tests cover exact limits, wide slices/views/nested arrays, shared concurrent producers, count/byte
saturation, schema/configuration errors, failed watermark updates, broadcast eviction, cancelled
receives, closed runtimes, final-producer cleanup, typed conversions, snapshot ordering/eviction,
writer flush/close and query-output scope. Focused runs passed **52 core streaming tests** and
**65 DB admission tests**. All **6,078 workspace library/server tests passed, zero failed, one
existing ignored** ONNX test. Both Clippy configurations with `-D warnings`, nightly formatting,
readability, analytical-dependency and whitespace checks passed. Validation used one Cargo job,
two test threads and `RUST_MIN_STACK=8388608`; existing cached OpenSSL symbol warnings did not
prevent linking. No dependencies or readability baseline allowances were added.

**Performance:** matched optimized builds use the same B0 Ryzen 9 7900X / WSL2 host, Rust 1.95.0,
the combined core/DB no-default-features build, thin LTO, one codegen unit and debug symbols.
Builds, timings and profiles ran serially. Final cases run in separate processes, candidate then
baseline, with 100 Criterion samples, a three-second warmup and a twenty-second measurement
target. The table reports means, not slopes or event-latency percentiles. Setup/shutdown are
untimed and repeated per Criterion sample; profiling includes them.

Core focused cases enqueue and deliver 32 messages to a subscriber: Arrow batches contain 256
rows, generic records one row. DB-handle cases enqueue 32 one-row batches, drain intake with no
broadcast subscriber and retain snapshot history. Both DB fixtures explicitly configure 64 history
entries in source DDL; the earlier builder-only setting was overridden by the DDL default. Original
runs remain in the evidence but are superseded by the matching corrected fixtures.

The existing wide pipeline burst can exceed 64 MiB once Arrow metadata is charged. It now retries
only `ChannelFull`, with a deadline, and still verifies every output row. Both versions use this
same fixture and its existing 128 MiB output-history allowance. The initial unhandled rejection
is retained as invalid evidence. No input budget was widened and no accepted work was omitted.

| Criterion mean | Before | After | Change |
|---|---:|---:|---:|
| Window-assignment control | 5.18 ns | 5.23 ns | +0.94% |
| Core Arrow, 16-byte values, 32 batches | 3.588 µs | 5.417 µs | +50.97% |
| Core Arrow, 4 KiB values, 32 batches | 3.596 µs | 5.425 µs | +50.87% |
| Core generic records, 16-byte values, 32 records | 9.675 µs | 8.085 µs | −16.43% |
| Core generic records, 4 KiB values, 32 records | 15.351 µs | 14.490 µs | −5.60% |
| DB Arrow, 16-byte values, 32 batches | 3.543 µs | 4.541 µs | +28.17% |
| DB Arrow, 4 KiB values, 32 batches | 3.523 µs | 4.601 µs | +30.58% |
| DB typed handles, 16-byte values, 32 records | 10.653 µs | 9.883 µs | −7.22% |
| DB typed handles, 4 KiB values, 32 records | 14.766 µs | 16.680 µs | +12.96% |
| Plain SELECT, 1,024 rows | 142.815 µs | 141.424 µs | −0.97% |
| GROUP BY, 1,024 rows / 4 groups | 156.795 µs | 155.048 µs | −1.11% |
| Sort / top 10, 1,024 rows | 188.514 µs | 184.906 µs | −1.91% |
| Three-query chain, longer confirmation | 172.015 µs | 170.791 µs | −0.71% |
| Narrow burst | 179.610 µs | 175.955 µs | −2.03% |
| Four-source burst, longer confirmation | 1.028 ms | 1.176 ms | +14.37% |
| Wide burst, matched linear sampling | 6.470 ms | 5.954 ms | −7.96% |

The Arrow microbenchmarks add about **57 ns per core batch**, **31–34 ns per DB Arrow batch**,
and **60 ns per wide record converted by a DB handle**. Retained-byte measurement, semaphore
reservation, a shared queued owner and final-release work are deliberate additions. CPU samples
attribute 8.4% inclusive time to core admission and 16.9% to batch ownership transfer/release in
the wide core case. In the wide typed DB case, admission contributes 5.2%, queued-owner destruction
3.7%, and overall source publication grows from 29.0% to 32.9% of sampled CPU. Parent/child
percentages overlap and must not be summed. These profiles explain the fixed per-batch cost;
there is no per-row bookkeeping loop. An initial generic-record slowdown prompted isolated runs
and a profile-guided inlining hint on the existing typed send helper. Final generic cases show
no regression; their observed improvements are diagnostic, not promised speedups.

The first query-chain result (+4.71%, approximate change interval +0.20% to +10.21%) was uncertain.
A 45-second confirmation, baseline then candidate, measured −0.71% (−2.44% to +1.04%). The
four-source slowdown persisted: +13.16% initially and **+14.37%** in the longer run (approximate
interval +4.19% to +25.59%). This concurrent-ingestion cost is retained, not described as noise.

A separate counter diagnostic records warmup and measured iteration counts, including each
sample's untimed initial burst. Both versions completed exactly **29,457 bursts** per four-source
run. Unrestricted execution added **3.16% instructions** and **8.62% user cycles** per burst;
its mean latency increased 8.29%. Restricting both processes to virtual CPUs `0,2,4,6` (distinct
reported cores) retained the additional work (+3.34% instructions), while cycles rose 1.93% and
mean latency changed −9.27%. This supports additional per-batch accounting work with scheduling
and cache effects amplifying the concurrent result; it does not establish a universal 14% cost
or remove the unrestricted regression. The diagnostic uses the same binaries, inputs and limits,
100 samples and 1,000 bootstrap resamples, with profiling/harness work included in the counters.
No host-wide affinity or power setting was changed.

Wide-burst timing remains variable. Shorter repeats changed sign and some adaptive runs selected
different flat/linear sampling modes. The final pair uses a 120-second target, 100 samples and
verified linear sampling with **20,200 measured bursts in each version**. Its −7.96% mean change
still has an approximate interval of −25.92% to +14.98%; it is not evidence of a speedup or a
less-than-5% worst-case change. The unchanged catalog bridge opportunistically concatenates
available batches and skips copying when only one is available, so arrival/poll timing matters.
CPU captures attribute about 55–56% of four-source CPU and 92–94% of wide-burst CPU to Arrow
concatenation. The ingress ownership cost and this batching/scheduling sensitivity are recorded
as a performance tradeoff of bounded admission, not hidden by dropping the wide workload.

| Profile | IPC before → after | Peak allocated heap before → after |
|---|---:|---:|
| Core Arrow, 4 KiB | 2.75 → 2.01 | 3.43 → 2.89 MB |
| Core generic records, 16 bytes | 3.43 → 3.16 | 3.43 → 2.88 MB |
| DB typed handles, 4 KiB | 2.59 → 2.51 | 1.73 → 1.73 MB |
| Four-source burst | 0.61 → 0.62 | 20.06 → 19.19 MB |
| Wide burst | 0.19 → 0.20 | 157.63 → 144.88 MB |

CPU sampling used 49 Hz DWARF stacks with no lost samples; reports disable inline-symbol lookup.
Grouped user cycles/instructions ran for 100% of their requested time. Virtualized counters include
runtime/harness work. Kernel scheduling counters were not used; no zero-context-switch claim is
made. Some frames remain unresolved, so attribution is qualitative.
The memory-heavy pipeline profiles remain below the IPC > 2 heuristic in both versions. Heaptrack
runs execute differing work counts and include setup, concatenation, output history and runtime
allocations. Wide-burst instrumented RSS increased **334.43 → 399.79 MB**, despite lower peak
intercepted heap allocation. This observation remains in the evidence: profiler/allocator overhead
and other owners are outside the source caps, and these captures do not qualify a process RSS bound.

The 64 MiB default accommodates the focused core 32-batch burst (256 rows per batch, up to 4 KiB
per row; about 32 MiB conservatively charged), while the regression rejects a 72 MiB batch.
This is a diagnostic configuration choice, not production workload qualification. The >5% ingress
and concurrent-burst costs above are explained and retained as the bounded-ownership tradeoff.
S12 must qualify throughput, latency and RSS for the selected production composition.

All **106 final build/smoke/timing/profile commands passed**. Source hashes match the final
correctness/static-gate run; no runtime changes followed it. Commands, failed pre-change regression,
raw samples, confidence intervals, CPU/heap reports and identities are under `target/s8-push/`,
including `final-summary.json`, `confirm-summary.json`, `wide-linear-summary.json`,
`diagnostic-summary.json`, `final-profiles-summary.json` and `final-source-hashes.json`.
The baseline snapshot and identical fixtures are preserved there; native binaries/profiles remain
under `/home/sujit/.cache/laminardb-b0-9bb1e996/s8-push`. Final SQL benchmark SHA-256:
`0be35ab4be65ddb5e0157a7352c16e37075492279508b467c38c3deae1c4e9e3`.
Final diff review found no dependencies, readability exceptions or unrelated edits. The next
serial session is **S9**. Remaining memory owners, S4 dependency findings and production
release/upgrade qualification remain open.

### S9 — enforce prospective graph-buffer limits (2026-09-20)

**Status:** implemented and locally validated, with a documented wide-fan-out performance caveat.
**Applies to:** embedded, single-node and cluster graph execution. Cluster SQL and delivery
admission are unchanged. Starting HEAD was `3688fab2ad8282cad8f7c1f2cc7270ed54ec361e`, with the
verified S8 working-tree changes retained. The before snapshot includes S8 and an identical
S9 benchmark fixture; `.zcode/` remains unrelated and untouched.

**Ownership and failure contract.** Source priming and every output destination now validate
the prospective per-port batch count and conservative retained Arrow charge before retaining
input or publishing the producer's result. All source views and all fan-out destinations are
preflighted before any of that admission mutates the graph. Charges reuse S8's backing-storage
accounting, including slices, nested/view arrays and fixed metadata. Each port pays the full
charge even when buffers are shared; these limits do not measure unique process heap or RSS.
Exact duplicate edge registration is normalized while building the graph.

A source passthrough can know its output size before execution, so Backpressure defers it when
the destination cannot fit the accepted input. The input, per-source progress and watermark
remain held until consumption. A general operator can change state before its output size is
known. If its result cannot fit, the typed `GraphBufferBudgetExceeded` poisons that generation,
halts normal/checkpoint execution, and prevents retry or capture of the mutated state. Startup
error capture preserves that terminal disposition. Ordinary stop/start cannot clear it; cluster
terminal authority also survives process replacement. Recovery requires explicit terminal-fault
resolution and a valid committed cut after changing the batch size or configured capacity.

Cached SQL providers bind the exact accepted inputs immediately before their consumer runs,
including input retained across cycles. Separate dynamic sources have separate graph ports;
static/reference lookup providers remain separate. An empty port clears an earlier branch's
provider view. This prevents both losing deferred data when cycle cleanup clears providers and
rereading another branch's input. Single-input execution borrows its existing batch roster;
multi-input local SQL retains its existing combined input semantics.

Best-effort `ShedOldest` removes the oldest existing/incoming batches before retaining the
suffix, including an individually oversized batch. Existing discarded-row metrics account for
that loss; durable delivery continues to reject shedding. The default count cap remains 256;
zero disables the count cap. The byte cap remains optional (`None`), and a configured zero-byte
cap is rejected. Embedded configuration and both server startup paths apply the same limits;
the server TOML options are startup settings, not hot-reloaded limits. A finite graph byte cap
must be selected for a bounded workload; S9 does not establish a default process-memory envelope.

The cap covers graph-held input only. Caller/source ownership, staged coordinator input, operator
working/output construction, result publication, live DataFusion provider snapshots, subscription
history and checkpoint scratch have separate lifetimes and may overlap. S7/S8 and shared
DataFusion reservations remain separate controls, not additive proofs of total memory usage.

**Correctness evidence.** The added source-priming regression fails against the pre-change graph.
Coverage includes atomic multi-source/visible/positioned admission, exact and nearly-full limits,
wide slices and nested/view arrays, prospective count limits, multi-port fan-out, duplicate edge
normalization, shedding, source/frontier deferral, cached multi-input SQL and branch isolation.
A stateful SUM test compares results with an independent expected value, rejects output after
mutation, verifies retry/drain/capture fencing, restores the prior cut in a fresh graph, and
replays once. Public stop/start, startup error round trips and normal/checkpoint callback mapping
exercise the terminal fault boundary.

Final Windows correctness gates pass: **6,097 workspace/server tests**, with one existing ONNX
model-download test ignored; all **eight** checkpoint/recovery/source-isolation integration
tests pass. The workspace run explicitly enables `laminar-db/cluster` and the `laminardb` binary.
Both workspace Clippy configurations (`--all-features --all-targets` and `--no-default-features`,
with `-D warnings`), nightly formatting, analytical dependency checks and whitespace validation
pass. Readability reports 19 module / 194 function exceptions; the now-smaller error formatter's
obsolete exception was removed. Windows builds use one Cargo job, two test threads and an 8 MiB
test stack. Existing cached OpenSSL debug-symbol and proc-macro future-compatibility notices are
unchanged. The 1,005 recorded source/manifest hashes match the final passing run.

Final commands, raw benchmark samples, profiles and source/binary identities are recorded under
ignored `target/s9-graph/`; native Linux binaries and captures are under
`/home/sujit/.cache/laminardb-b0-9bb1e996/s9-graph`. Performance results and their limits follow.

**Measurement protocol.** Before/after optimized binaries use the same B0 Ryzen 9 7900X / WSL2
host, Rust 1.95.0, no default features, thin LTO, one codegen unit and debug symbols. Existing
SELECT, GROUP BY, sort/Top-K and query-chain cases exercise execution alongside core assignment,
grouping and checkpoint-manifest controls. The new graph cases send sixteen 256-row batches per
source through an intermediate stream and then one or four consumers. Payloads are 16 bytes or
4 KiB; a two-source UNION case checks distinct SQL input providers. Each burst consumes and
asserts every expected output row. Graph ports use 64 batches / 32 MiB, with 32 MiB output history
per stream. Setup and shutdown are untimed; profiles include runtime/setup costs.

The initial fan-out fixture mistakenly reserved five 64 MiB output histories against the existing
256 MiB process subscription budget. That warmup failure remains in the evidence. Both final
fixtures use 32 MiB histories, enough for the complete 16 MiB wide burst; graph and ingress limits
were not enlarged to make it run. The public terminal-fault test also retains its initial failure
logs: it now joins the compute thread before reading the diagnostic and asserts the existing
shutdown error after a terminal halt.

CPU/heap profiles cover all four graph cases in both versions. Sampling uses 49 Hz DWARF stacks
with inline-symbol lookup disabled; all captures report zero lost samples. Grouped user cycles
and instructions ran for 100% of their requested time. IPC includes the harness and runtimes on
this virtualized host. Some frames remain unresolved; inclusive parent/child percentages overlap
and cannot be summed. Wide fan-out spends about 86–87% of sampled CPU in existing Arrow
concatenation in both versions.

| Graph profile | IPC before → after | Peak allocated heap before → after | Instrumented RSS before → after |
|---|---:|---:|---:|
| Single input | 0.580 → 0.571 | 35.27 → 35.27 MB | 105.56 → 104.52 MB |
| Four-way fan-out | 0.735 → 0.725 | 35.93 → 35.99 MB | 108.09 → 107.49 MB |
| Wide four-way fan-out | 0.283 → 0.286 | 44.22 → 44.23 MB | 137.44 → 148.13 MB |
| Two-input UNION | 0.671 → 0.681 | 35.46 → 35.43 MB | 120.46 → 110.17 MB |

All profiles remain below the IPC > 2 heuristic in both versions. These measurements include
asynchronous ingress, copying, output history and runtime work; they do not qualify hardware
efficiency for a production workload. Heaptrack runs perform different work counts and include
profiler/setup overhead. Wide-burst instrumented RSS rises about 7.8% despite nearly unchanged
peak intercepted heap; this is retained in the evidence, not treated as proof of an RSS bound.

Timings use 100 Criterion samples and three-second warmup targets. Initial control measurements
target ten seconds, pipeline measurements twenty seconds. Four cases whose approximate change
interval reached above 5% were repeated for 45 seconds in reversed order (baseline then candidate).
The table reports Criterion means, not slopes or event-latency percentiles. All original samples
and individual confidence intervals remain in the evidence.

| Criterion mean | Before | After | Change |
|---|---:|---:|---:|
| Window assignment | 5.25 ns | 5.25 ns | +0.13% |
| Owned grouping keys | 22.977 µs | 23.238 µs | +1.14% |
| Borrowed grouping keys | 7.135 µs | 7.011 µs | −1.73% |
| Manifest load, 2 sources | 110.989 µs | 112.691 µs | +1.53% |
| Manifest load, 10 sources | 115.415 µs | 117.929 µs | +2.18% |
| Manifest load, 50 sources | 154.193 µs | 154.962 µs | +0.50% |
| SELECT, 1,024 rows | 146.031 µs | 146.935 µs | +0.62% |
| GROUP BY, 1,024 rows / 4 groups | 157.365 µs | 157.670 µs | +0.19% |
| Sort / top 10, 1,024 rows | 187.500 µs | 183.931 µs | −1.90% |
| Three-query chain, 45-second confirmation | 172.894 µs | 172.101 µs | −0.46% |
| Graph single-input burst, confirmation | 160.209 µs | 160.619 µs | +0.26% |
| Graph four-way fan-out, confirmation | 188.054 µs | 182.420 µs | −3.00% |
| Graph two-input UNION | 173.254 µs | 174.703 µs | +0.84% |
| Wide four-way fan-out, unrestricted confirmation | 1.098 ms | 1.286 ms | +17.04% |
| Wide four-way fan-out, fixed cores / 120 seconds | 1.138 ms | 1.182 ms | +3.91% |

The unrestricted wide case cannot be called regression-free: its initial result was +11.24%
(approximate interval −1.84% to +28.31%), and the longer comparison was +17.04% (+8.49% to +26.47%).
The other three confirmations have upper interval bounds below 1.8%. Wide-case allocation
captures show almost identical concatenation allocations per admitted input batch (1.7507 vs
1.7506) and nearly identical retained heap. Provider rebinding does add small roster/Arc
allocations per consumer; it is necessary for deferred-input and branch correctness.

To isolate the wide-case gap, matched 45-second runs also recorded hardware counters per
completed burst, including warmup and setup bursts. With CPU affinity fixed to `0,2,4,6`,
the latency mean changes +1.73%, instructions/burst +1.57%, cycles/burst −0.29%, and user CPU
time/burst +1.62%. A separate unrestricted diagnostic changes the latency mean +3.96%,
instructions/burst +2.11%, cycles/burst +3.97%, and CPU time/burst +2.94%. These diagnostics
support sensitivity to execution placement and the memory-copy-heavy workload; they do not
erase the earlier +17% result or establish a production latency guarantee. A longer fixed-core
confirmation targets 120 seconds per version, using the original bootstrap settings and the
same 105,207 completed bursts in both processes. It measures +3.91% latency, +1.59% instructions,
+4.14% cycles and +2.61% CPU time per burst. Its approximate latency-change interval is
+0.36% to +7.59%; it does not prove a universal below-5% bound.

The extra work is at batch/port boundaries: prospective charges and exact provider bindings
add fixed bookkeeping and roster/Arc ownership. Cached SQL needs those bindings even on its
compiled path because evaluation can fall back to a cached plan within the same call. The
profiles and controlled counters explain a small added execution cost alongside substantial
placement sensitivity in a workload dominated by Arrow copying. They do not isolate a single
cause of the entire unrestricted +17% gap. That observed slowdown is retained as a capacity
tradeoff of the correctness fix and must be included in S12's workload qualification; it is
not represented as a regression-free result. No byte limit, workload width or regression
threshold was increased to obtain the later measurements.

All **88 final build/smoke/timing/profile/diagnostic commands passed**. The performance verdict
is qualified as above. Raw samples, confidence intervals, allocation counts, profiles and
identities are in `target/s9-graph/`, including `final-summary.json`, `confirm-summary.json`,
`diagnostic-summary.json`, `pinned-long-summary.json`, `final-profiles-summary.json`,
`performance-verdict.json` and `final-source-hashes.json`. Both benchmark fixtures are
byte-identical (SHA-256 `01f955a6de5f7dcfa14caa7cf1d0a417c61cd3b0d8b3b27d6da4a35b0cc4ff70`).
Candidate SQL benchmark SHA-256:
`1f1111f3e50b28cbc088dc8c851c3eac8168fcd7065a3f48c796196a546f921f`.

The next serial implementation session is **S10**, using the existing prepared table-snapshot
installation and restore boundaries, then S11 for local MVs. S4 dependency findings still block
release. S12 must select and qualify throughput, latency, RSS and recovery bounds on target
hardware, including this wide-input case; S13 must verify the chosen release upgrade pair.
These local tests and no-default-features embedded benchmarks do not certify a native cluster
or external-broker/cloud workload. Changes remain uncommitted.

### S10 — reference-table live quotas (2026-09-20)

**Implemented and validated locally, with the performance qualifications below.** Embedded
and single-node tables now have independent per-table limits, configured through
`reference_table_max_rows` (default 1,000,000) and `reference_table_max_bytes` (default 256 MiB).
Both must be nonzero; the server validates and passes them through its shared builder path.
Cluster table admission remains fail-closed.

Upsert preparation retains only the incoming distinct keys and affected allocation deltas.
It charges encoded keys, conservative row/array descriptors and original Arrow allocation
capacity. Shared allocations count once per table across columns, rows and admitted batches;
replacing the last live reference releases the allocation charge. Last-key-wins upserts and
unique-key snapshots keep their existing semantics. There is no whole-map clone, live-key
eviction, buffer compaction or alternate backend. Nested children, dictionary/view storage and
validity buffers use the same recursive accounting. Custom buffers use the allocation extent
reported by the pinned Arrow implementation; an opaque owner's unreported memory is excluded.

Startup snapshot loading now validates each polled batch into a bounded candidate, stops on
failure and closes every source. Restore validates the declared row limit before decoding and
checks each decoded batch before reading the next. Both paths preserve their existing atomic
installation boundaries; a failing second table cannot change either table or its readiness.
Installation rechecks current limits and captured table identities before any replacement.
The checkpoint format remains version 2. A selected checkpoint that exceeds current limits is
rejected without falling back to another cut.

Checkpoint capture estimates now cover at least the live retained-memory charge, so a small
surviving row cannot pin an uncharged large allocation after replacement. Live, candidate and
checkpoint ownership remain separate: refresh/restore may retain the old installation plus
one quota-checked candidate per table and one incoming decoded batch. Encoded checkpoint
storage, input/key/delta scratch, query snapshots, hash-map spare capacity, schema/allocator
overhead and opaque external owners require separate headroom. These are not process RSS caps.

The regression suite covers exact byte-boundary replacement, duplicate upserts, encoded wide
keys, shared columns/batches, spare capacity, empty slices, nested/dictionary/view/validity
buffers, multi-table quota failures, readiness preservation, bounded snapshot polling and
source cleanup. A deterministic sequence of accepted/rejected updates is compared with an
independent ordered map. Streaming restore brought `restore_checkpoint` below the readability
exception threshold; its obsolete baseline entry was removed.

**Correctness and static validation.** The final Windows run passed **6,112 workspace library
and server-binary tests**, with one existing ignored model-download test, plus **8 checkpoint,
process-crash/restart and shared-source integration tests**. It enabled `laminar-db/cluster`
to retain cluster rejection/recovery coverage. Focused table, source-cleanup and public
configuration regressions also pass. Both required workspace Clippy configurations pass
(`--all-features --all-targets` and `--no-default-features`, warnings denied), as do nightly
formatting, readability, analytical-dependency consistency and whitespace checks. Readability
now retains 19 module and 193 function exceptions. No dependency versions changed.

Performance uses the same Ryzen 9 7900X / WSL2 host, Rust 1.95.0, optimized thin-LTO profile
and fixed virtual CPUs `0,2,4,6` for both versions. The baseline is the starting S9 working
tree with the new table benchmark fixture added; candidate runtime changes are S10 only.
Both fixtures execute the same operations; their source differs only in formatting. The
`benchmark-internals` feature implies cluster support, but these measurements exercise local
tables, not distributed table admission. Final timings run without concurrent compilation
or tests, using 100 Criterion samples, three-second warmup targets and fifteen-second table
measurement targets. They measure means, not event-latency percentiles or production SLOs.

| Operation, 1,024 rows | Before | After | Change |
|---|---:|---:|---:|
| Upsert, 16-byte payload | 92.400 µs | 136.494 µs | +47.72% |
| Upsert, 4-KiB payload | 89.559 µs | 135.183 µs | +50.94% |
| Scan, 16-byte payload | 30.519 µs | 25.884 µs | −15.19% |
| Scan, 4-KiB payload | 112.998 µs | 111.015 µs | −1.75% |
| Refresh, 16-byte payload | 166.432 µs | 207.328 µs | +24.57% |
| Refresh, 4-KiB payload | 164.562 µs | 194.598 µs | +18.25% |
| Capture/update/encode, 16-byte payload | 444.694 µs | 496.618 µs | +11.68% |
| Capture/update/encode, 4-KiB payload, initial | 5.620 ms | 6.951 ms | +23.68% |
| Capture/update/encode, 4-KiB payload, longer confirmation | 5.579 ms | 5.029 ms | −9.86% |
| Refresh/replace/scan with one surviving old row, 16-byte payload | 306.419 µs | 382.464 µs | +24.82% |
| Refresh/replace/scan with one surviving old row, 4-KiB payload | 292.830 µs | 377.064 µs | +28.77% |

The update/refresh costs are real. Atomic admission stages distinct keys and checks affected
row/allocation deltas before mutating the existing map; each stored row also retains its batch
descriptor. This adds approximately 44–46 µs per 1,024-row upsert in these cases, with similar
cost for narrow and wide payloads. No payload-compaction copy was added. Public table writes
and snapshot loading run outside the coordinator record path; the table-scan measurements
exercise the changed representation used by query materialization. The checkpoint case
captures a cut, applies an update, then encodes the pinned cut in one benchmark iteration.
It does not establish a concurrent ingestion or external-storage checkpoint SLO.

The longer checkpoint comparison targets 45 seconds per version, reversing the initial order
to baseline then candidate. Its −9.86% result does not erase the initial +23.68% result or prove
a universal below-5% bound. The encoding-heavy workload is variable on this host; neither a
stable checkpoint regression nor a stable speedup is established. Both comparisons and their
individual mean confidence intervals remain in the evidence.

Window assignment changes +0.16%. The lookup controls range from −2.92% to +16.47% initially,
but the complete lookup executables have identical SHA-256 hashes before and after S10:
`d178bf80ed6404bf023130709bc3976fcc3deddd4149dc3846c3e068fd647057`.
They generate fresh random key sets in each process. Reversed-order, 45-second confirmations
change the ten-key batch from +16.47% to −1.83%, and the nominal 1%-hit case from +13.07% to
+0.56%. These differences between identical executables cannot establish an S10 code regression.

CPU and heap profiles cover narrow upserts, wide refreshes, wide capture/update/encoding and
the wide surviving-slice case in both versions. All eight 49-Hz DWARF captures have zero lost
samples; grouped user cycles/instructions ran for 100% of the requested time. Inclusive
stack percentages overlap and some frames remain unresolved. Upsert profiles show admission,
row slicing, map growth and cleanup; checkpoint encoding dominates its wide case in both
versions (approximately 75% candidate / 87% baseline of sampled CPU).

| Profile | IPC, before → after | Peak intercepted heap, before → after | Instrumented RSS, before → after |
|---|---:|---:|---:|
| Narrow upsert | 2.87 → 2.92 | 10.62 → 10.62 MB | 15.72 → 21.24 MB |
| Wide refresh | 3.24 → 2.99 | 10.60 → 10.60 MB | 24.13 → 24.31 MB |
| Wide capture/update/encode | 0.68 → 0.99 | 25.82 → 25.84 MB | 38.18 → 38.45 MB |
| Wide surviving slice | 3.00 → 2.89 | 10.60 → 10.60 MB | 24.46 → 24.66 MB |

Upsert/refresh profiles exceed the IPC > 2 heuristic; wide checkpoint encoding remains below
it in both versions. Peak intercepted heap is almost unchanged, while narrow-upsert
instrumented RSS rises about 35%. This is retained as a capacity consideration. These captures
include fixture input/setup and profiler overhead, perform different work counts and do not
prove an RSS bound or a reduction in allocations per operation. Buffer retention is deliberate:
one surviving wide row keeps the original large allocation charged until its last reference
is replaced. Query/checkpoint ownership can outlive that live-table charge.

Existing `recovery_bench` binaries were built for both versions with the same feature/profile
settings; every smoke case passed. Matched selected-manifest and verified-state-range controls
target fifteen seconds per case, with 100 samples for manifests and the existing ten-sample
override for state reads. These are local filesystem measurements with warmup, separate from
the table-specific capture/update benchmark and the passing process-crash/restart tests.

| Recovery control | Before | After | Change |
|---|---:|---:|---:|
| Exact manifest load, 2 sources | 111.540 µs | 106.050 µs | −4.92% |
| Exact manifest load, 10 sources | 116.677 µs | 117.253 µs | +0.49% |
| Exact manifest load, 50 sources | 153.352 µs | 155.244 µs | +1.23% |
| Verified state read, 1 KiB | 154.087 µs | 157.154 µs | +1.99% |
| Verified state read, 1 MiB | 679.223 µs | 663.445 µs | −2.32% |
| Verified state read, 10 MiB | 5.865 ms | 5.756 ms | −1.85% |

The final timing/profile run (**64 commands**) and follow-up run (**17 commands**) passed.
Raw commands, logs, source/binary hashes, samples, confidence intervals and profiles are under
ignored `target/s10-tables/`, including `final-summary.json`, `followup-summary.json`,
`profiles-summary.json`, `performance-verdict.json`, `final-review.json` and
`final-source-hashes.json`. Native artifacts use the existing WSL B0 build directory.
Benchmark production source matches the final correctness/static-gate source; the only later
source edit was a semicolon in a test, included in the final validation run.

The local S10 verdict retains the 48–51% upsert and 18–25% refresh costs as the cost of atomic
admission. It does not claim regression-free writes, a stable wide-checkpoint speedup or a
process RSS guarantee. No quota, workload width or hot-path regression threshold was relaxed.
S12 must qualify the resulting table workload and its input/candidate/checkpoint headroom on
target hardware. The next serial session is **S11**, covering all local MV storage modes.
S4 dependency findings still block release, and S12/S13 qualification remains outstanding.
Changes remain uncommitted.

### S11 — local materialized-view quotas and cycle preflight (2026-09-21)

**Implemented and validated locally, with the performance costs recorded below.** Embedded and
single-node MVs have independent defaults of **1,000,000 live rows / 256 MiB** through
`LaminarConfig`, builder methods and the server's shared `[server]` construction path.
Both limits must be nonzero. Cluster MV admission remains rejected.

Aggregate mode admits an entire replacement snapshot; upsert mode accounts for final keyed
replacements/deletes; multiset mode charges distinct encoded full rows and their counts.
Append mode keeps its oldest-batch eviction policy within batch, row and byte limits, but
rejects an individual batch that cannot fit. Empty aggregate cycles preserve the prior
snapshot. Restore checks a private candidate and rejects oversized committed images instead
of evicting rows. `MaterializedViewQuotaExceeded` carries the view, projected usage and limits.

`update_mv_stores` now preflights every affected MV under its existing write lock before
installing any store or publishing any MV output. Aggregate/append candidates borrow input
batches; upsert and multiset candidates retain their existing staged deltas. Up to four
prepared entries use inline storage. There is no full live-map clone or additional lock.
All cycle publications also preflight the multiset's existing expansion guards, even when
no subscriber is currently attached; cached expanded row/byte totals avoid scanning untouched
rows for admission. Counted checkpoint encoding and guarded snapshot reads are preserved.
The existing recovery fault path still prevents cursor settlement and downstream publication.

The accounting contract is per MV. Arrow modes conservatively charge Arrow-reported backing
capacity and batch/column metadata, including slices, nested arrays, dictionaries and views.
Upserts include owned scalar allocations, vector capacity, encoded keys and row metadata;
multisets include complete encoded rows and counts. Shared Arrow storage is charged per
stored batch/scalar. Replacements, deletes and append eviction release the corresponding
live charge. Hash-map spare capacity, schema/converter/allocator and opaque-owner overhead
are outside this charge. Preflight simultaneously holds touched-key deltas for all affected
MVs, proportional to the cycle's output rows and widths; source/graph admission bounds
remain separate. Restore holds old state, decoded input and private candidates together.
Query/subscriber materialization and pinned checkpoint captures require additional headroom.
These quotas are not an RSS bound or production capacity qualification.

The starting source is `3688fab2ad8282cad8f7c1f2cc7270ed54ec361e` plus the preserved S8–S10
working tree. The source snapshot and binary identities are under ignored `target/s11-mv/`.
A standalone regression linked against that starting build fails because it admits a
1,000,001-row aggregate snapshot. The baseline Criterion suite exercises two MVs across
all four storage modes, with 1,024-row inputs and 16/4,096-byte strings. Each update iteration
applies two alternating cycles to both views. Snapshot reads materialize one view (1,024
rows, or 8,192 retained rows for append mode); checkpoint cases capture both views, update,
encode the old cut and apply the reverse cycle. These are local operation timings.

Fresh controls cover `latency_bench`, all `lookup_join_bench` cases, selected graph/SQL
`stream_executor_bench` cases, owned-row grouping in `hot_path_micro`, and exact-manifest /
verified-state-read `recovery_bench` cases. All benchmark targets pass their smoke cases.
Before/after builds use the existing WSL Rust 1.95.0 cache, the same optimized profile with
debug symbols, no default features, and `laminar-db/benchmark-internals` (which includes
cluster). Timings use CPUs 0/2/4/6, two-second warmup, ten-second target measurements and
100 samples, except the existing ten-sample state-read override. Candidate builds, regression
probes, benchmark smoke cases, matched timings and CPU/allocation profiles all completed.

**Correctness and static validation.** Native Windows Rust 1.98.0 passed **6,131 workspace
library/server tests** (one existing ignored test) and all **eight** checkpoint, recovery and
shared-source integration tests. The targeted runs passed 46 MV-store tests, two publication/
coordinator fault tests, one configuration/builder test, four MV recovery lifecycle tests and
two server configuration/runtime tests; these also run in the workspace suite. Coverage includes
all four modes, row/byte limits, wide and encoded keys, nested/dictionary/view arrays, surviving
Arrow slices, append eviction and oversized batches, replacement/deletion release, independent
randomized row oracles, failed second-view publication, multiset expansion and over-limit restore.
The real coordinator failure test covers all three delivery guarantees, preserved committed
positions and suppressed sink output. The real restart test leaves both prior MV stores intact
and refuses to enter Running when the selected cut exceeds the lowered quota.

Both workspace Clippy configurations (`--all-features --all-targets` and `--no-default-features`,
each with `-D warnings`), nightly formatting, syntax-aware readability, analytical-dependency
policy and whitespace checks pass. No readability exception was added or enlarged. Exact
commands, complete logs and the latest passing outcomes are in `target/s11-mv/gates.json`;
`validated-source-hashes.json` records the 1,129 source/manifest hashes. Earlier server fixture
failures are retained in the log history: its generator schema and expected shutdown fault were
corrected before the passing targeted and workspace runs.

**CPU/allocation profiles.** Eight before/after workload pairs completed under user-space
`perf stat` (cycles/instructions, 100% counter coverage), 49 Hz DWARF CPU sampling and separate
five-second heaptrack runs. Hardware is an AMD Ryzen 9 7900X, 24 logical CPUs, under WSL2;
timings and profiles use CPUs 0/2/4/6. Heaptrack values below are whole-fixture peaks in decimal
MB, including setup and inputs, not live MV charges or production RSS. Narrow/Arrow update
peaks are dominated by the fixture's other-mode setup and cannot resolve tiny local differences.

| Profile (1,024 rows; string width in bytes) | IPC before → after | Peak heap MB before → after |
|---|---|---|
| Aggregate update, 16 | 3.86 → 3.60 | 21.84 → 21.79 |
| Append update, 4,096 | 3.70 → 3.46 | 21.84 → 21.79 |
| Upsert update, 16 | 3.41 → 2.85 | 21.84 → 21.79 |
| Upsert update, 4,096 | 1.64 → 1.26 | 21.99 → 26.45 |
| Multiset update, 4,096 | 3.06 → 2.84 | 30.75 → 39.53 |
| Multiset snapshot, 16 | 4.80 → 4.79 | 21.84 → 21.79 |
| Upsert checkpoint during updates, 4,096 | 0.65 → 0.59 | 38.61 → 38.61 |
| Append checkpoint during updates, 4,096 | 0.18 → 0.17 | 80.07 → 80.07 |

Wide keyed updates hold both views' staged deltas until all preflight checks succeed. The
additional peak is **4.46 MB for upserts / 8.78 MB for multisets** in this two-view workload.
Allocation stacks remain the existing owned scalar strings, row vectors and encoded keys;
there is no full live-state clone. Upsert sampling includes scalar extraction, allocation,
hash-map operations and the added prepare phase. Multiset sampling is dominated by hashing
the wide encoded rows (74% self samples in the candidate's SipHasher write routine). The wider
working set is consistent with the observed update costs; these samples do not isolate every
cache effect. Wide upserts and checkpoints remain below the IPC > 2 rule of thumb, including
the baseline. Wide append checkpoint samples spend over 99% of cycles beneath Arrow IPC
writing in both builds, with unchanged peak heap; copying/growing encoded buffers dominates.
No production latency or memory ceiling is inferred from these local profiles.

**Timing evidence and regression review.** The first pass produced **47 matched means**,
including all 24 MV cases, with raw distributions and 95% confidence intervals retained in
`timing-summary.json` and `criterion-before` / `criterion-after`. Sixteen selected cases then
ran in close after→before pairs, with a three-second warmup, twenty-second measurement target
and 100 samples. This reverses the original order and reduces drift between compared processes.
`confirmation-summary.json` retains these distributions. Timings, profiles and builds ran
serially; no workload width, quota or 5% regression threshold was relaxed.

| Confirmed MV case (string width in bytes) | Before → after mean | Change |
|---|---|---|
| Aggregate update, 16 | 0.137 → 0.244 µs | +78.8% |
| Append update, 4,096 | 0.137 → 0.280 µs | +103.8% |
| Upsert update, 16 | 0.876 → 1.179 ms | +34.6% |
| Upsert update, 4,096 | 2.042 → 2.515 ms | +23.2% |
| Multiset update, 16 | 1.116 → 1.165 ms | +4.3% |
| Multiset update, 4,096 | 34.090 → 37.021 ms | +8.6% |
| Aggregate checkpoint during updates, 16 | 14.987 → 15.246 µs | +1.7% |
| Aggregate checkpoint during updates, 4,096 | 4.158 → 4.216 ms | +1.4% |
| Upsert checkpoint during updates, 16 | 1.445 → 1.622 ms | +12.3% |
| Upsert checkpoint during updates, 4,096 | 12.584 → 12.934 ms | +2.8% |
| Multiset checkpoint during updates, 16 | 1.330 → 1.399 ms | +5.2% |
| Multiset checkpoint during updates, 4,096 | 42.007 → 44.645 ms | +6.3% |
| Upsert snapshot, 4,096 | 3.388 → 3.252 ms | −4.0% |
| Append checkpoint during updates, 4,096 | 29.843 → 32.200 ms | +7.9% |

Each update still means two alternating cycles across two 1,024-row views, not one input
record. Aggregate/append preflight adds about **0.11–0.14 µs** per complete measured operation:
checks, retained-byte accounting and the prepare/install traversal replace direct mutation.
Upsert admission adds a pass over the staged map and checked final-state accounting, while
both keyed modes retain multiple views' deltas simultaneously. These are measured costs of
the required all-view admission boundary. The upsert increase was 16.6% narrow / 42.3% wide
in the initial pass and 34.6% / 23.2% in the closer pairs; it is not dismissed as noise.
Multiset update increases were 5.3% / 12.0% initially and 4.3% / 8.6% in the pairs. The
remaining keyed checkpoint increases include the same update work as well as capture/encoding.

Isolated filters skip earlier update/snapshot benchmark stages. Their allocator history and
stored-row layout can therefore differ from the full suite; do not compare absolute means
across those two protocols. In particular, the initial wide-upsert snapshot was 251.6 →
308.7 µs (+22.7%), while the isolated pair above reverses direction at a different absolute
level. This does not establish a stable snapshot regression or improvement. Aggregate checkpoint
increases of 9.0% / 11.3% in the full pass narrowed to 1.7% / 1.4% in the isolated pairs;
wide-upsert checkpoint's 11.3% narrowed to 2.8%. These observations and the copy/allocation
profiles require S12 measurements with the actual sequence of updates, reads and checkpoints.

Control outliers were checked separately. `latency_bench` and `lookup_join_bench` have
**byte-identical** before/after executables; the lookup deltas up to +8.9% cannot come from
an S11 code change. Their binary hashes are retained with the results. Plain SELECT changed
from +6.2% initially to +3.1% in the close pair; four-way graph fan-out changed from +5.2%
to −2.9%. The other graph, grouping, latency and recovery controls stayed below +5% in the
first pass. This explains the control outliers without attributing the measured MV costs
to those controls or claiming an end-to-end latency SLO.

The remaining wide-append checkpoint outlier received two further forty-second/100-sample
pairs, first before→after and then after→before. Means were **28.856 → 29.712 ms (+3.0%)**
and **29.581 → 32.011 ms (+8.2%)**, compared with +23.4% in the first full-suite pass and
+7.9% in the twenty-second pair. `append-repeat-summary.json` retains the intervals; none
of these results was discarded. The small fixed admission cost does not explain millisecond
changes in a workload whose profiles spend over 99% beneath existing Arrow IPC writes and
whose peak heap is unchanged. These results are consistent with sensitivity to process/allocation
history and run conditions; they do not isolate every source of the remaining variation.
The observed 3–8% longer-pair range remains a checkpoint workload risk for S12, rather than
a claim of a stable speedup or that every measured checkpoint delta is below 5%.

**Local S11 verdict:** correctness, recovery and static gates pass. The required performance
review is complete with explicit costs: additional quota/preflight work, simultaneous staged
deltas and the measured materialization/encoding sensitivity. The larger upsert and multiset
working sets are retained to establish the required all-view failure boundary; no full-state
clone, new lock, async boundary, silent eviction of keyed state or relaxed limit was introduced.
This is not regression-free publication or a qualified production capacity envelope. S12 must
measure the actual update/read/checkpoint sequence, staging headroom, RSS and tail latency on
target hardware. `final-source-hashes.json` matches all 1,129 validated source/manifest hashes;
only documentation changed after the passing gates and candidate build.

The next serial session is **S12**. S4 dependency findings still block release, and S12/S13
workload and upgrade qualification remain outstanding. Changes remain uncommitted.

### S12 — workload observer and release smoke matrix (2026-09-21)

**Preparation implemented and locally validated; S12 remains open.** The selected scope is single-node
Kafka → Kafka ALO projection, with one/four independent pipelines and uniform/Zipf/hot-key
inputs. Production hardware, offered load and numerical acceptance ceilings remain unselected.
Tables/MVs, stateful SQL, exact delivery and cluster execution are outside this workload.
The first narrower qualification allowed by this plan would therefore leave G3 open.

The first release matrix exposed a Kafka startup defect before the four-pipeline workload could
accept input. Prometheus 0.14 identifies a composite collector by the wrapping sum of its
descriptor IDs; the four progress descriptors for `input_1`, `input_2` and `input_3` have the same
sum despite distinct descriptors. The earlier two-source test used names that did not collide.
The failed process evidence is retained under `target/s12-workload/smoke-v4-4-uniform-none`,
and `kafka-registration-before.log` reproduces the failure without a broker. Progress now registers
each metric family separately, rolls back only successful registrations on failure, and preserves
the primary error with any rollback failure attached. Source-owned cleanup still leaves late worker
clones unable to unregister a replacement. Metric names, labels and sampling semantics are unchanged.
This cold startup/cleanup fix applies to named Kafka sources sharing a metrics registry in embedded,
single-node and cluster modes; it does not widen any delivery or SQL admission.

The existing `cluster_soak` target now contains a workload module that reuses its verified
server executable, process control, Kafka topic setup and checkpoint observations. Its independent
Kafka reader checks every expected source origin, ID, key, arithmetic result and transformed string; it counts
ALO duplicates and requires a drained, stable public output boundary. Producer and observer use
one monotonic clock. Latency starts at the original scheduled arrival, including producer stalls,
and ends at the first verified external observation. Offered, enqueued, acknowledged and observed
counts are retained independently. An intentional five-second source pause adjusts only the
declared offered schedule; a broker or process stall never resets it.

The origin travels through Kafka and the SQL projection as input data. The oracle rejects a record
whose origin disagrees with its output topic, even when all pipelines share identical IDs, keys and
payloads. `cross-routing-before.log` demonstrates that the earlier oracle accepted that mismatch;
the added regression prevents another pipeline's records from satisfying its frontier. The raw
visibility ledger also retains origin, key and projected arithmetic value for independent checking.
Earlier `smoke-v4-*` and `smoke-v5-*` observations predate this stronger check and are retained as
superseded preparation evidence.

The existing Prometheus histogram implementation supplies p50/p95/p99/p99.9 bucket upper bounds
per pipeline, at one-percent spacing with a 1 μs minimum bucket. Overflow and missing evidence
cannot satisfy declared limits. Such runs require one hour of offered load after warmup,
at least 60 seconds of warmup and
100,000 measured records per pipeline. Full engine scrapes retain compute-cycle and checkpoint
stall measurements separately. Resource observations report sampled RSS and the second-half
RSS/backlog slopes; backlog means offered minus externally observed rows, not internal queue
bytes. Process-kill recovery requires a continuous externally visible prefix including an input
scheduled after confirmed process death, a newer committed checkpoint, and a completion counter
from the restarted process. Raw fault-event timestamps permit independent recomputation. Shorter
runs remain observations.

`tools/run_workload_qualification.py` builds a release server and the existing process test,
archives exact source files, the dirty diff, SHA, lockfile, features, toolchain and binary hashes,
then runs the chosen profile serially. Each new evidence directory retains input acknowledgements,
output observations, config, checkpoints, metric scrapes, logs, report and a final SHA-256 index.
Existing output directories are rejected. The reports always retain `s12_qualified: false`;
passing a run's limits cannot certify an unexecuted matrix or another mode/composition.
See [the harness instructions](../tests/qualification/README.md) for its controls and limitations.

**Starting identity and preservation:** `3688fab2ad8282cad8f7c1f2cc7270ed54ec361e` plus the existing
uncommitted S8–S11 implementation. Comparing the 1,129 S11 source/manifest hashes finds only Kafka
progress registration and its tests, plus the `cluster_soak.rs` test entry point changed; the new
workload modules and runner are additional files. Dependencies, admission boundaries, API/config
defaults and the S8–S11 implementation remain unchanged. The production edit is limited to connector
startup/cleanup; no coordinator-cycle or core-operator edit requires another before/after Criterion run.

**Validation:** Windows Rust 1.98 passed 6,133 workspace library/server tests (one pre-existing
ignored test), all eight checkpoint/recovery/shared-source integration tests, and 84 non-ignored
soak-harness tests. All nine measurement tests also pass on the final Linux release harness.
The new measurement regressions cover cross-routing, corrupt/missing output,
ALO duplicates, separate pipeline frontiers, p99.9 resolution and overflow, stalled schedules,
explicit source pause, sample floors, post-death recovery targets, mandatory recovery evidence
even without numerical limits, and growth slopes. Two further connector regressions cover the
reproduced multi-source registration collision and rollback at
each metric family, including preservation of existing collectors and later source replacement.
Both Clippy configurations, nightly formatting, readability, analytical-dependency policy and
whitespace gates pass. The workspace suite was rerun on the final production source after Clippy
removed an extra allocation in rollback-error formatting. Logs and command
history are retained in `target/s12-workload/gates.json`; earlier measurement-test and lint failures
are retained alongside the corrected passing runs.

**Local release smoke matrix:** all eight `smoke-v6-*` runs passed against real Kafka boundaries.
Each used a 30-second schedule, two seconds of latency warmup, 200 offered rows/second per pipeline,
128-byte payloads, 1,024 keys, four input partitions, one output partition and 500 ms checkpoints.
The pause case includes five seconds without offered input. The independent ledger check found
all **134,000 expected unique records**, correct source routing and arithmetic, and no gaps.
It recomputed per-pipeline quantiles from raw timestamps, checked recovery authority and timestamps,
and verified every indexed artifact plus identical server/harness hashes across all eight bundles.

| Pipelines / distribution | Fault | Unique rows | ALO replays | Peak sampled RSS | Highest pipeline p99.9 upper bound |
|---|---|---:|---:|---:|---:|
| 1 / uniform | None | 6,000 | 0 | 56.2 MiB | 90.5 ms |
| 1 / Zipf | None | 6,000 | 0 | 55.3 MiB | 128.2 ms |
| 1 / hot key | None | 6,000 | 0 | 55.1 MiB | 79.5 ms |
| 4 / uniform | None | 24,000 | 0 | 62.9 MiB | 140.2 ms |
| 4 / Zipf | None | 24,000 | 0 | 62.8 MiB | 118.4 ms |
| 4 / hot key | None | 24,000 | 0 | 63.4 MiB | 110.4 ms |
| 4 / Zipf | Process kill | 24,000 | 324 | 64.6 MiB | 956.7 ms |
| 4 / hot key | Source pause | 20,000 | 0 | 61.8 MiB | 125.7 ms |

The kill case recovered in **2,180.136 ms**, including a post-death input on every pipeline and
a checkpoint completed by the restarted process. Its 324 duplicates are allowed by the declared
ALO composition. A metrics scrape was unavailable during restart, so the RSS-growth result is
explicitly unavailable and cannot pass a declared limit. These short observations do not establish
tail SLOs, stable queues, an RSS plateau, capacity, or long-term checkpoint behavior. Generation-aware
RSS growth and internal queue-byte/capacity evidence remain qualification work.

The diagnostic host was an AMD Ryzen 9 7900X with 24 logical CPUs, Ubuntu under WSL2
6.18.33.2 and about 15 GiB guest RAM. A same-host Docker Redpanda 26.1.13 broker used one CPU and
1 GiB, plaintext and replication factor one. Linux Rust 1.95.0 built the optimized release with
`--no-default-features --features cluster,kafka`, debug information retained and the system
allocator; actual server mode was single-node. This is not broker-HA or cluster qualification.
The server SHA-256 was `d174602bcf096dd97d3a7362eb7adc604099c904b3169e391683dac39666f4ef`.
Full toolchains, manifests, configurations and identities are in each bundle.

Evidence is retained in `target/s12-workload/smoke-v6-matrix.json`,
`smoke-v6-verified-summary.json`, the eight `smoke-v6-*` directories and
`linux-observer-smoke-v6-tests.json`. All reports retain `status: observed` and
`s12_qualified: false`. Earlier failed builds, startup failure, source-mutation rejections and
superseded observations remain available; none was substituted for a passing run.

Final review added a guard against reporting a successful process-kill run with no verified
post-death recovery when numerical limits are unset. `missing-recovery-before.log` reproduces
the missing guard, and its regression now passes. The eight-case matrix already satisfies the
stronger rule, as checked independently. The final guard changes only the workload module and its
tests; the production server remains byte-identical. The focused `smoke-v7-4-zipf-process_kill`
release rerun also passed: 24,000 unique records, 466 allowed ALO replays, recovery in 2,395.513 ms,
64.4 MiB peak sampled RSS and a 1,088.8 ms pipeline p99.9 upper bound. RSS growth was again
unavailable during restart. `smoke-v7-verified-summary.json` independently checks this run;
`linux-observer-smoke-v7-tests.json` binds all nine passing measurement tests to its exact harness.

`final-verification.json` confirms all nine bundles' source snapshots, the two-file harness delta,
the identical production executable, passing gates and preservation of the S8–S11 implementation.
`final-source-hashes.json` records all 1,102 selected final source/manifest files. Only root
documentation changed after that source freeze. The disposable broker was stopped; topics,
volumes, checkpoints and evidence were retained. Changes remain uncommitted.

**Remaining S12 exit work:** select the launch workload and numerical targets, then perform the
three hour-long repetitions across the declared load/distribution/pipeline matrix on target
hardware, including sufficient tail samples, usable RSS growth and internal queue/capacity evidence.
Add the slow/failed-sink, corrupt-cut, expired-replay and G9 durable Backpressure/Fail
saturation → checkpoint → restart external-ledger cases. Qualify tables/MVs, stateful SQL and
each additional mode/composition separately, including applicable leader/rejoin/scale scenarios.
These remain required evidence; neither the new observer nor the existing correctness soaks close
them. S4 dependency findings still block release. S12 remains the active session; S13 is not started.

#### RSS evidence across process loss (2026-09-21)

**Status:** measurement correction implemented and validated locally; S12 remains open.
**Starting SHA:** `c6f3dbb429dd9a60e0f8d0f7b5160489e31faaf3`. The selected scope remains
single-node Kafka → Kafka ALO projection. The user selected continued diagnostic fault testing;
production workload targets remain unset. Production code, dependencies and admission are unchanged.

Two failing regressions reproduced distinct measurement defects: a missing startup scrape after
restart made RSS growth unavailable, while a healthy restarted process could hide growth in its
predecessor because the original fit covered only the second half of the whole run. The failed
regressions are retained in `target/s12-recovery-resources/restart-before.log`.

Version 2 resource reports fit RSS separately for every observed process generation. Each fit
uses the latter half of that generation's sampled post-warmup offered-load interval. The configured
warmup is applied from each generation's first load sample; only missing RSS in that startup
interval is excluded. Any missing RSS after warmup during offered load invalidates that
generation's growth result, including gaps before the fitting window. Final drain cannot provide
missing load evidence. The report retains per-generation intervals, fitting sample counts,
missing-sample counts, peaks, slopes and final checkpoint p99. Overall RSS growth is the highest
generation slope and is unavailable if any generation lacks usable evidence. Checkpoint p99 still
retains the worst final histogram across generations.

Diagnostic fits need three samples. Declared-limit fits additionally need at least 60 samples
spanning 60 seconds per generation; the existing hour, warmup, latency-sample and checkpoint-sample
requirements remain. Five new regressions cover startup gaps, a leaking predecessor, missing
running-process RSS, drain/short-lived generations and the declared sampling floors. These changes
are confined to the workload harness and its documentation, so no engine hot-path benchmark is
required. Version 1 evidence remains available with its original measurement limitations.

**Retained preparation failures:** the first Windows-mounted control delivered all 48,000 rows
but overlapped an integration build and missed one RSS sample at 8.05 seconds. Its growth result
is correctly unavailable. The following fault run was rejected: verification of the 803 MB
executable preceded a kill at 63.08 seconds, after its 60-second input schedule had ended. The
required post-death target was therefore beyond the input range, and the existing recovery guard
refused success. Both bundles remain under `target/s12-recovery-resources/{steady,recovery}`.
The replacement diagnostics use native Linux storage for executable copies and local checkpoints;
this storage placement differs from the earlier Windows-mounted smoke matrix. These failures do
not justify weakening the missing-sample or post-death recovery checks.

**Validation:** 6,133 workspace library/server tests, eight checkpoint/recovery/shared-source
integration tests and 89 non-ignored soak-harness tests passed. All 14 measurement tests also
passed on the final Linux release harness. Both Clippy configurations, nightly formatting,
readability, analytical-dependency policy and whitespace gates passed. Commands and logs are
retained in `target/s12-recovery-resources/gates.json`.

**Release diagnostics:** after the validation builds completed, a native-Linux steady control
and three process-kill repetitions each ran a 60-second schedule with three seconds of latency
warmup, four Zipf-distributed pipelines, 200 rows/second per pipeline, 128-byte payloads, 1,024
keys, four source partitions and 500 ms checkpoints. The independent checker verified all
**192,000 unique records**, no gaps, source routing, arithmetic, external offsets, latency
quantiles, recovery events, RSS fits and every indexed artifact. The 965 replay duplicates are
allowed by the declared ALO composition.

| Run | ALO replays | Peak sampled RSS | Highest generation RSS growth | Recovery | Highest pipeline p99.9 upper bound |
|---|---:|---:|---:|---:|---:|
| Steady control | 0 | 86.31 MiB | 118.24 KiB/s | — | 5,457.9 ms |
| Process kill 1 | 280 | 84.69 MiB | 0.00 KiB/s | 2,610.3 ms | 2,462.1 ms |
| Process kill 2 | 273 | 83.66 MiB | 65.00 KiB/s | 1,603.4 ms | 593.4 ms |
| Process kill 3 | 412 | 84.96 MiB | 20.17 KiB/s | 1,629.9 ms | 570.3 ms |

Every process generation has usable diagnostic RSS evidence: 28 fitting samples in the control,
and 14/13 before/after each restart, with no missing RSS after the configured warmup. These short
fits do **not** establish an RSS plateau; the positive slopes and latency tails remain workload
qualification concerns. Numerical limits were unset and all reports retain `s12_qualified: false`.

The two immutable bundles are under `target/s12-recovery-resources/native/{steady,recovery}`;
`native/resources-verified-summary.json` records independent verification. `final-verification.json`
checks all 1,102 selected source/manifest files and confirms only the three workload Rust files
and its README differ from the prior smoke source snapshot. The production executable remains
byte-identical (`d174602bcf096dd97d3a7362eb7adc604099c904b3169e391683dac39666f4ef`). The disposable
broker was stopped; topics, checkpoints and evidence were retained.

**Next S12 work:** slow/failed sinks, corrupt cuts, expired replay and G9 durable saturation/restart
external-ledger cases remain unexecuted in this workload. Production targets, three hour-long
repetitions across the declared matrix, internal queue/capacity evidence and other mode/composition
qualifications remain open. S4 still blocks release; S13 has not started.

### PR #540 — confirmed review fixes (2026-09-21)

Reviewed all 40 Cubic inline comments against source and existing evidence. Six additional
findings have scoped fixes, alongside the S12 RSS correction committed as `fb76edd5`:

- Cluster DDL rejects ordinary SQL with multiple source frontiers before catalog mutation.
  A two-source `UNION ALL` reproduced the admission bug before the fix. Managed joins retain
  their separate validation; embedded and single-node query admission is unchanged.
- Kafka startup has one failed-start cleanup owner at the connector lifecycle boundary. Consumer
  creation, assignment and schema-prefetch errors release progress registration and retire
  consumer work consistently. This applies to all deployment modes.
- Operator contexts inherit the root logical optimizer sequence once; auxiliary contexts register
  streaming and custom functions, including aliases. Both retain the DB's shared runtime/budget.
- Reference-table quota errors retain their public Query classification and detailed message.
- Helm ServiceMonitor uses the service application-name label as its scrape job, so the default
  `job="laminardb"` alert selector is independent of the Helm release name. A custom `nameOverride`
  also changes this application label and requires corresponding custom alert selectors.

The CI failures are actual S4 dependency findings: the two quick-xml advisories, RSA, unmaintained
paste and proc-macro-error2; audit additionally rejects unmaintained instant in the broader locked
graph. Cargo Deny's bans, licenses and sources pass. Current registry metadata still offers no
compatible object_store 0.13/OpenDAL 0.57 repair, and RSA has no patched release listed. No dependency
exception, scanner suppression or analytical-generation upgrade is included in this follow-up.

Comment-by-comment verdicts and the downloaded CI/review evidence are retained locally under
`target/pr540-followup/`. Temporary MV staging remains unbounded by its retained-state quota;
unchecked upsert input schemas and other confirmed findings in that report also remain open.
Historical baseline findings and intentional evidence floors were not rewritten as defects.
No coordinator-cycle or core-operator code changed. S4 and production qualification remain open.

Validation: the two-source admission regression failed before the fix. The final workspace library
and server-binary suite, with `laminar-db/cluster` enabled, passed **6,136 tests, zero failed,
one existing ignored**. This includes the startup cleanup, custom-function, optimizer-sequence
and API-error regressions. Helm lint and rendered service/monitor/alert label checks pass for two
different release names. Startup cleanup preserves pre-I/O validation retryability and running-source
state while releasing failed `Initializing` attempts.
Both Clippy configurations, nightly formatting, readability, analytical dependency consistency and
whitespace checks pass. The CI security/advisory findings above remain release-blocking.

### S4/S11 — dependency remediation and bounded MV staging (2026-09-21)

This follow-up supersedes the open XML, temporary MV staging and input-schema findings above.
The two quick-xml 0.39.4 security fixes are backported from upstream without migrating the
Arrow/DataFusion/object_store generation. Provenance, compatibility adjustments and exact source
verification are recorded in `vendor/quick-xml/SECURITY-BACKPORT.md`. The backport applies to all
three modes built from this workspace. Published embedded-library consumers must apply the patch
in their application's root manifest: Cargo does not propagate this workspace patch to them.
Registry crate publication is consequently blocked before upload while the backport is required;
the release guard prevents a green workspace scan from publishing an unfixed XML dependency.

The four residual findings (RSA, paste, proc-macro-error2 and instant) were explicitly accepted on
2026-09-21 until **2026-10-21 UTC**, owned by the LaminarDB maintainers responsible for PR #540.
RSA remains a real residual confidentiality risk; signing-only reachability does not prove the
absence of timing leakage. The two XML version exceptions require the verified patched source.
See [`security/dependency-exceptions.md`](../security/dependency-exceptions.md). Both required CI
scanner jobs now reject expiry, package/source/checksum drift, backport modification and ignore-list
disagreement before scanning. There is no severity relaxation or automatic renewal.

Embedded and single-node MV preflight now validates declared column names, order, types and
non-null values before key indexing or scalar conversion, including empty batches. Keyed inputs
require exactly one Int64 `__weight` column; upsert keys index the plain columns even when the
weight appears first. All affected views remain unchanged on any schema or quota rejection.
Cluster MV admission remains closed.

Keyed staging admits at most twice the live row and byte limits plus bounded entry metadata,
allowing replacement and retraction before final-state admission. Replacement releases the old
staged charge and a cancelled multiset delta is removed immediately. A separate conservative
input estimate rejects oversized batches and repeated dictionary/view expansion before Arrow
row conversion. View lengths and the largest dictionary value are charged without multiplying
descriptor/key buffers per row; ordinary 16,384-row dictionary/view batches remain admissible.
Oversized net-neutral cycles can now fail even when their final state would fit.
This remains a per-view charge, not a whole-process RSS bound: live stores, staged views, input
batches, one candidate scalar row, converter scratch, map spare capacity and publication or
checkpoint materialization can coexist.

Security validation: cargo-audit 0.22.2 with `--deny warnings` and cargo-deny 0.20.2 pass using
RustSec revision `57ad4063bb49c1deb04b6fcee30cfbac6b508474`. Seven policy fault tests cover expiry,
scanner disagreement, version/source/checksum/duplicate drift, source tampering and rejection
of registry publication with a workspace-only patch. The XML
backport passes 1,450 upstream/local unit tests (six existing ignores), including the default
256/257 namespace boundary through both reader APIs and Serde response deserialization.
All 43 Cubic comments were classified in `target/security-mv-fixes/cubic-validation.md`.
The latest three follow-ups correct the backlog-window wording, clarify the RSS section heading
and independently test the RSS duration floor. All 14 qualification tests pass. Other confirmed
findings outside this follow-up retain their prior open status.
The full workspace library and server-binary suite with `laminar-db/cluster` enabled passes
6,142 tests, zero failures and one existing ignore. Both Clippy configurations, nightly formatting,
readability, analytical dependency consistency and whitespace checks pass. The 52 MV tests include
schema and multi-view atomicity, staging bounds, ordinary dictionary/view batches and aliased
payload rejection. A focused Arrow probe reproduced the earlier overestimate before its correction.
Performance validation compares `90151cd8` with the final source using Rust 1.95.0 in WSL,
CPU affinity `0,2,4,6`, 100 Criterion samples, two-second warmup and ten-second measurement.
Each update measurement contains two 1,024-row cycles. Final means in microseconds:

| Mode | 16-byte values, before → after | 4,096-byte values, before → after |
| --- | --- | --- |
| Aggregate | 0.256 → 0.284 (+11.09%) | 0.251 → 0.282 (+12.56%) |
| Append | 0.293 → 0.307 (+4.86%) | 0.292 → 0.308 (+5.65%) |
| Upsert | 1,049.090 → 1,075.586 (+2.53%) | 2,761.553 → 2,590.633 (-6.19%) |
| Multiset | 1,276.241 → 1,278.428 (+0.17%) | 38,410.663 → 33,599.206 (-12.53%) |

The changes above 5% are explained by mandatory per-batch schema preflight: Aggregate adds
28–32 ns per pair of cycles across both widths; wide Append adds about 16 ns. The Aggregate
CPU profile shows preflight self time increasing from 4.24% to 8.35%, with schema datatype
comparison appearing in the final profile. This closes malformed-input admission without
adding per-row work to those two modes. Their profiled peak heap remains 21.79 MB.
The earlier wide Upsert snapshot outlier reverses to -7.29%; narrow Aggregate checkpoint
changes by +2.51%. The broader initial run also covered core latency and four coordinator
query controls, all within 5%; those unchanged controls were not repeated.

Final CPU/allocation profiles cover all four modes. Final IPC is 3.77 Append, 3.57 Aggregate,
1.32 wide Upsert and 2.71 wide Multiset. Wide Upsert was already below the 2.0 rule of thumb
(1.35 baseline), with scalar conversion/allocation prominent in both profiles; this remains
an optimization opportunity despite the lower measured update time. Its peak heap changes
from 26.45 to 26.49 MB, including bounded staging metadata; Multiset remains 39.53 MB.
Heap profiles include fixture setup and are not a production RSS envelope. The existing Append
fixture retains eight batches before snapshot cases, so its 1,024-row label is not an isolated
snapshot size. No benchmark fixture was changed for this comparison.

All 43 profile commands passed. Commands, binary/source hashes, comparisons, CPU samples,
allocation summaries and the Cubic verdicts are retained under `target/security-mv-fixes/`.
Dictionary-typed Multiset snapshot reconstruction still has a pre-existing schema mismatch;
the staging regression checks retained state directly and does not qualify that materializer.
S12 production qualification remains open.

### S11/S12 — dictionary reconstruction and fault diagnostics (2026-09-21)

This entry supersedes the dictionary snapshot defect and unconditional registry-publication block
above. Embedded and single-node Multiset snapshots and counted checkpoints restore the declared
Arrow types after row decoding hydrates dictionaries. The shared conversion uses checked casts:
unrepresentable values fail instead of becoming null. Direct and nested dictionary regressions
failed before the fix and now preserve schemas, nulls, multiplicities and retractions after restore.
The ordinary 16,384-row dictionary case now checks the materialized snapshot as well as retained
state. Cluster MV admission remains closed.

The dependency policy now explicitly accepts the unpatched registry XML denial-of-service risks
through the existing exception deadline; the verified workspace backport remains mandatory.
The publication guard passes only with this acceptance, exact reviewed dependencies and unexpired
policy. No crate was uploaded. Current DataFusion/Delta still require object_store 0.13, and the
current Iceberg adapter still requires OpenDAL 0.57: a direct version bump would retain the affected
transitive graph. The upstream review and residual risks are in
[`security/dependency-exceptions.md`](../security/dependency-exceptions.md). CI rejects these
exceptions on **2026-10-21 UTC** unless they are removed or explicitly re-reviewed beforehand.

The new embedded Kafka overflow test establishes a committed prefix, expands an intermediate
batch beyond a 64 KiB graph port, checks halt/checkpoint rejection, then explicitly increases
capacity and restarts. Both Backpressure and Fail recover all 40 acknowledged input IDs without
duplicates. The independent Kafka reader checks values and a stable stopped-writer boundary.
Earlier small-batch/query-budget fixtures did not demonstrate saturation and are retained as
failed probes, not passing evidence. This closes the indivisible-output overflow/replay diagnostic;
**gradual queue saturation, deferral and restart still need external-ledger evidence**, separately
from cluster terminal-fault authority.

Single-node Kafka ALO diagnostics use four projection pipelines at 1,000 rows/s each for 60 seconds,
separate source/sink brokers, 500 ms checkpoints and a process kill halfway through load. A
10-second sink-broker pause preserves all 240,000 source-acknowledged IDs with 1,796 allowed ALO
duplicates and 2,784 ms verified recovery. Corrupting the selected manifest fails its committed
digest check. Advancing the retained Kafka prefix to 60,001 past the committed next position 60,000
fails replay validation. Both recovery failures exit before readiness and leave every external sink
boundary unchanged, including after independent canary inputs. No fallback checkpoint is accepted.

The broker-kill diagnostic exposed a test-observer limitation: its consumer exited on transport
loss. The observer now retains its ledger and original deadline across the two connection-loss
codes only; malformed output and other errors still fail. The failed initial run is retained.
The rerun survives a 10-second broker outage and process restart. A fresh consumer audit of the
final stable broker cut verifies all 240,000 IDs and payloads, with 3,371 stored ALO duplicates and
12,640 ms verified recovery. Broker restart reused 59 offsets with different payloads, so the
live observer's 3,430 duplicate observations are not the stored duplicate count; the independent
final scan establishes the result. This broker behavior and the observed recovery time must be
included when selecting production durability and recovery targets.

Validation: 6,144 workspace library/server tests pass with one existing ignore. Two parallel
attempts hit different one-second cluster-test deadlines; the complete serial rerun passes and
both earlier failure logs are retained. Both Clippy configurations, nightly formatting,
readability, analytical dependency consistency and whitespace checks pass. All 14 qualification
unit tests, the new Kafka overflow/restart diagnostic, seven exception-policy tests, the
publication guard, cargo-audit and cargo-deny pass. The fault workloads use the same verified
server executable; only the independent observer was rebuilt for the broker-kill rerun.

Performance: matched cached binaries compare the six Multiset update/snapshot/checkpoint cases
and the core latency control with 100 Criterion samples, two-second warmup and five-second
measurement targets, pinned to WSL CPUs 0,2,4,6. Initial narrow-update (+9.05%) and wide-checkpoint
(+5.08%) outliers accompanied an +8.49% slowdown of the byte-identical control binary. Repeating
only these cases in reversed order with three-second warmup and fifteen-second measurements gives
-1.43%, +2.63% and -0.91%, respectively. The other four initial cases range from -2.22% to +3.43%.
No unexplained regression above 5% remains. Snapshot IPC changes from 4.85 to 4.95. Both initial
and confirmation measurements, source/binary identities and commands are retained.

Local evidence and diagnostic controllers are retained under `target/s12-diagnostics/`. Native
release executables remain in the WSL evidence directories named by each bundle's
`executables.json`; bundle hashes were verified there before copying the nonbinary evidence.
Production acceptance ceilings remain unset. The one-hour runs, three repetitions, workload/mode
matrix, normal saturation cases and S13 upgrade qualification remain open; these short fault
diagnostics do not establish production latency, RSS or recovery limits.

### S12 — deferred source scheduling (2026-09-22)

Fixed two replay defects in the shared embedded, single-node and cluster runtime. Buffered source
ports now count as deferred work, so a source whose downstream queue has drained is retried.
The coordinator leaves newer input in its existing bounded FIFO until retained work settles,
preserving the original source cursors and frontier pins across data and control wakeups.
The redundant replay-wakeup flag was removed; no new queue, state or dependency was added.

Both regressions failed before the fix and pass afterward. Coverage includes ordinary graph
drain after partial queue saturation and queued successor input during runnable replay, timer
retries and manual checkpoint wakeups. The external-ledger saturation/checkpoint/restart cases
remain open; these regressions do not close S12 production qualification.

Validation: 6,145 workspace library/server tests pass with one existing ignore. Both Clippy
configurations, nightly formatting, readability, analytical dependency consistency and whitespace
checks pass. No readability baseline was expanded.

Matched WSL Criterion means cover 12 core-latency, group-key, source-queue and graph cases with
100 samples. The single-graph case initially measured +5.15%; reversed-order 30-second runs
measure -3.68%, with the byte-identical latency control at -0.68%. The other initial cases range
from -27.89% to +2.63%; no unexplained regression above 5% remains. Fanout IPC is 0.70 before and
0.69 after, below the 2.0 rule of thumb in both builds. CPU samples retain allocation, Arrow
concatenation and scheduling costs. Commands, source/binary hashes, failed/passing regressions,
measurements and profile reports are retained under `target/s12-gradual-replay/`.

### S12 — external-ledger saturation, checkpoint and restart (2026-09-22)

The embedded Kafka → Kafka ALO diagnostic now covers the count- and byte-capacity cases left
open by the deferred-scheduling entry above. Both Backpressure and Fail fill a graph port with
two individually admissible one-row batches from one completed producer invocation. Count
capacity is two batches. Byte capacity is independently calibrated from the seed's retained
Arrow charge, with count admission disabled: each batch charges 8,592 bytes against a 17,184-byte
port. A bounded identity UDF retains the backing allocation without changing the visible IDs or
values. The one-nanosecond query budget forces deferral; another test-only identity UDF holds the
downstream consumer so checkpoint and process-loss boundaries can be observed deterministically.
No production runtime hook or admission change was added.

Backpressure holds the original source cursors while two successor records are acknowledged by
Kafka. A manual checkpoint remains pending and the durable decision stays unchanged. Releasing
the gate drains both batches and the successors, then permits a new committed cut. A second
worker is killed with another saturated port and a pending checkpoint. Restart keeps the same
capacity, restores the committed cut, and uses single-message intake and the ordinary query budget.
Fail instead reports the specific downstream-capacity fault, rejects checkpointing, and leaves
the independent sink boundary at the committed prefix. Its explicit recovery configuration
increases the relevant capacity by 128 times before replay.

Independent Kafka readers audit stopped-writer source and sink cuts, validate every ID and value,
and count ALO duplicates. Each Backpressure case verifies ten source-acknowledged IDs; each Fail
case verifies eight, including two post-restart canaries. The final four-case matrix passes three
times: 108 acknowledged records across twelve cases, with no missing records, incorrect values or
duplicates. A separate evidence audit verifies all 405 bundle files and 1,151 source-file hashes.

Validation: 6,145 workspace library/server tests pass with one existing ignore. Both Clippy
configurations, nightly formatting, readability, analytical dependency consistency and whitespace
checks pass. Final fixture corrections use the repository's existing `parking_lot` dependency,
place support code below the integration-test root, and reuse the initially validated broker
address instead of repeatedly invoking the optional test suite's 500 ms availability probe.
One earlier run stopped at that short probe while the broker remained running and subsequently
reported healthy; its log and broker inspection are retained. Actual producer and independent
consumer deadlines and error handling remain enforced. The collector's UTF-8 corrections and
invalidated intermediate builds are also retained, separately from passing evidence.

The fixture and repeat command are documented in [`tests/qualification/README.md`](../tests/qualification/README.md).
Final evidence is retained under `target/s12-saturation/complete/`, with the verified result in
`target/s12-saturation/completion.json`. The bundle includes its source snapshot, build identity,
worker logs, acknowledgement offsets, committed decisions, pressure reports and independent ledger
scans. This is native Windows test-profile evidence with Rust 1.98.0 and default features plus
`cluster`; the runtime mode is embedded. It closes this bounded embedded ALO diagnostic.
Single-node server, cluster, exact compositions and
the declared production load/latency/RSS/recovery matrix still require separate qualification.
