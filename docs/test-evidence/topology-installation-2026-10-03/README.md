# Held committed runtime installation, 2026-10-03

This continuation starts clean at `53e63fad8b83e84076d045d6ede11242127a538c`
on `feature/cluster-topology-migrations`. The original baseline is
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC, Rust/Cargo 1.98 and
the workspace Rust 1.95 minimum remain unchanged. Cargo.lock and dependency
metadata are unchanged: DataFusion 53.1.0, Arrow 58.4.0, Tokio 1.53.1,
rdkafka 0.39.0 and async-trait 0.1.92.

`install_committed_cluster_topology(image)` consumes one exact committed private
image through the existing startup owner. It reuses observed parent retirement,
exact-Commit transport preparation, catalog replay, checkpoint initialization,
source/sink actor preparation and compute launch. Current full-roster authority,
process/adoption, assignment, source availability, catalog inventory, target
pipeline identity and environment must match. The decoded graph moves into the
runtime without another state restore; the compiler permit remains owned through
the handoff.

The target coordinator keeps the parent outcome/index/manifest as immutable
historical metadata. Its first target checkpoint must capture full vnode state
under the new pipeline identity. Preserved catalog handles, object incarnations,
subscription generations/frontiers and Resume positions survive. Added sources
use their sealed Initialized vector without a fabricated checkpoint attempt or
latest reevaluation. Source requests preflight before target sink I/O. Reference
tables without a migration initialization mapping reject.

Source/sink actors and the compute control loop become locally Running with
intake/cut held. Initial target sink epoch admission is deferred. No checkpoint,
source acknowledgement, readiness receipt, Release or locally active version is
published. Ordinary startup, manual gate opening and runtime DDL remain fenced.
One cooperative 45-second budget spans transport and owned startup, followed by
bounded terminal cleanup. The watcher enters the existing DB ownership registry
before any readiness wait; cancellation cannot discard an unowned compute thread.
Failed startup retains Commit, holds and namespace ownership and joins or retains
the existing actor owners. Retry reconstructs the immutable root.

No general framework, migration scheduler, authority encoding, dependency or
per-record operation is added. The shared startup ownership fix also covers normal
and coordinated-recovery startup; their existing lifecycle suites are included in
the full selected-feature run.

Eight focused cases cover:

- Exact target catalog/coordinator/graph binding, preserved source and stream Arc
  handles, incarnation 7, old Resume cursor 3 and sealed new cursor 91; held source
  controls, zero polls/acknowledgements/writes/epoch starts and observed terminal
  shutdown of both source and sink actors.
- Actual aggregate and target callback/sink execution after a fixture-only local
  gate opening: preserved total 30 plus three post-cut rows of value 5 yields 45;
  the added stream emits only its three supplied post-boundary rows of value 19.
- Caller disconnect during atomic source startup: the same registered startup
  owner continues, reaches held Running and resolves latest only once at staging.
- Atomic source-start failure with full owned cleanup, retained Commit/OS namespace
  lock and successful reconstruction/retry of all nine real operator frames.
- Unsupported initialized-start capability rejects before source starts or sink I/O.
- A short deadline passed through the same owned startup driver expires while a
  source is blocked; cleanup completes with no live watcher/actors and retains
  Commit, the root hold and OS namespace lock.
- Process lease loss during source startup rejects readiness and retires the graph
  claim without polling, output or acknowledgements.
- A separate Created DB installs from the root before any target checkpoint,
  without resolving a new source boundary.

The DB fixture has one configured control process with actual durable authority,
operator codecs, pipeline callback and owned actor machinery. Its connector I/O is
controlled and uses at-least-once delivery; it does not contact Kafka or certify
transactional sink installation. Each emitted batch carries its updated
assignment-bound fixture cursor, but no post-install checkpoint is taken. The
gate-opening case therefore
checks restored state and actual runtime wiring, not durable cursor recovery,
production Release or restart certification. The Created DB reuses the configured
fixture process identity, not a new boot or a full cluster restart.

Commands use the established sequential four-package feature union:

```powershell
$env:CARGO_BUILD_JOBS = '1'
$env:RUST_MIN_STACK = '4194304'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_install_ -- --nocapture
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
cargo check --locked -p laminar-db --no-default-features --features cluster,ffi
cargo fmt --all -- --check
git -c core.excludesFile= diff --check
git -c core.excludesFile= diff --cached --check
```

All eight focused cases pass (5.43 s). The full selected-feature suite passes
4,393 tests: 1,078 core (30.52 s), 921 connectors (45.16 s), 2,038 DB (12.15 s)
and 356 server (6.04 s). The same three cases remain ignored, including the separate
external-broker source-start case validated in the preceding increment. These
durations are test runtimes, not migration-pause measurements.

All-target Clippy with warnings denied passes (44.14 s), as do the minimal server
build (13.64 s), cluster/FFI build (9.83 s), formatting and working/staged diff
checks. All 19 changed Rust source hashes and Cargo.lock remain unchanged through
final verification and staging. No optional connector runtime suites are rerun.

Final results are recorded in [focused results](focused-tests.txt),
[full suite results](unit-results.txt), [build checks](build-checks.txt) and
[source identity](source-identity.json). The 19 changed Rust files and Cargo.lock
are frozen before final runtime validation and compared again after verification,
staging and commit, accounting for Git line-ending normalization. Raw local logs
remain under ignored `target/topology-evidence`. Checked-in logs omit the existing
large OpenSSL missing-PDB prelude. Earlier Clippy checks found a large startup future
and a test mutex lifetime; the future is boxed on the control path and the fixture
uses a lexical guard scope. No lint or delivery requirement was suppressed.
The first eight-case run also caught two fixture errors: a typed deadline-error
expectation ignored the existing sticky result wrapper, and emitted test batches
lacked the required assignment-bound cursor. Both fixtures now obey those existing
contracts; no production fence was bypassed to make the run pass.

No broker, optimized migration or comparative performance run is repeated because
the existing multi-process harness does not drive this held internal installer or
production Release. Validation durations do not measure a migration pause.
Current installation capabilities/readiness, stale sink completion fencing,
participant-complete Release, automatic post-Commit recovery, public submission
and the real stateful migration/restart/failure/performance oracle remain unfinished.
LDB-6043 remains. No push or pull request is made.
