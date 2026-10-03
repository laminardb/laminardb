# Installed runtime certification and Release, 2026-10-03

This continuation starts clean at `1b2fcf26b0308e4b5ed8d957d37edb575c501316`
on `feature/cluster-topology-migrations`. The original baseline is
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC, Rust/Cargo 1.98,
the workspace Rust 1.95 minimum and dependency metadata remain unchanged.
Cargo.lock retains DataFusion 53.1.0, Arrow 58.4.0, Tokio 1.53.1,
rdkafka 0.39.0 and async-trait 0.1.92.

Authority encoding 21 and installation protocol 5 implement exact runtime
observations and participant-complete Release. Every current owner/evidence
process, including zero-vnode participants, must certify its exact runtime UUID,
boot and process term. Historical private-restore/retirement receipts alone do
not authorize output. A leader replacement supersedes an unreleased installation
round only by recollecting all runtime observations. Published Release and Commit
remain immutable; identical retries resolve their original decision.

The existing DB control executor owns certification, leader Release and local
application with one existing compiler slot and a 45-second cooperative budget.
Caller disconnect does not drop that owner. Runtime readiness includes the exact
coordinator/vnode binding, compute watcher, live source/sink actors, actual sink
acknowledgements and receiver mesh. Application reconciles/admit sink epochs,
rechecks authority and opens intake last. Status distinguishes durable Active
from this process's applied, live runtime.

Every sink actor has a unique revocation token. Revocation precedes actor abort,
blocks connector admission and rejects same-poll completion or buffered successful
acknowledgements. A same-name successor cannot inherit the token. Existing connector
child trackers still prove terminal ownership; rejection of a late success does
not undo an external effect or reconcile an unknown transactional outcome.

Held assignment refresh retains the exact transport/controller certificate.
Released refresh can use the certified Release before the first target checkpoint
and coexist with an exact target checkpoint. Checkpoint artifact admission rejects
the historical parent pipeline after target Commit. Generic recovery cannot relabel
that cut. Release retains the migration root's parent checkpoint artifact pin;
explicit reference-aware root retirement and journal reclamation remain unfinished.

Twenty new cases cover:

- Nine core authority cases: complete current roster and stable retries; old
  capability/nil/replaced runtime or changed input rejection; unreleased leader
  replacement requiring complete recollection; immutable Release across a harmless
  leader change; successful installation/Release writes with lost or cancelled
  responses; exact target checkpoint coexistence and parent rejection; retained
  authority/root pins and missing-anchor rejection; malformed roster, duplicate
  append sequences, phase/format downgrade and evidence rewrites.
- Six DB cases with actual restored aggregate execution, callback routing and
  owned actors: certification holds intake; durable Release preserves total 30
  plus three future rows of value 5 as 45; an added pipeline emits only its three
  supplied future rows of value 19; no historical acknowledgements are invented;
  assignment refresh remains exact before a target checkpoint; dead actors with
  retained children cannot certify readiness; dead actors after Release cannot
  report local Active; caller cancellation leaves the bounded DB owner running;
  process loss keeps intake held and observes terminal cleanup.
- Five sink generation cases: no connector future after revocation, same-poll
  late success, revocation of a pending cancel-safe operation, buffered successful
  acknowledgement after revocation, and isolation of a same-name successor.

The core fixture has exact two-process durable authority. The DB fixture has one
configured process, real operator codecs/runtime callback/actor ownership and
controlled at-least-once source/sink I/O. It performs actual durable Release and
local application, but does not contact Kafka or certify transactional target sink
installation. No post-install target checkpoint, replacement-boot activation or
full cluster restart is exercised by the DB fixture. The core checkpoint case
admits an inventory; it does not persist/commit target state. These limits are
separate from the successful local state and fencing oracles.

Final commands use the established sequential four-package feature union:

```powershell
$env:CARGO_BUILD_JOBS = '1'
$env:RUST_MIN_STACK = '4194304'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_activation_ -- --nocapture
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins sink_generation_ -- --nocapture
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
cargo check --locked -p laminar-db --no-default-features --features cluster,ffi
cargo fmt --all -- --check
git -c core.excludesFile= diff --check
git -c core.excludesFile= diff --cached --check
```

All 20 new focused cases pass: nine core activation cases (0.23 s), six DB
activation cases (0.64 s), and five sink generation cases (0.01 s). The final
selected-feature suite passes 4,413 tests: 1,087 core (30.68 s), 921 connectors
(45.36 s), 2,049 DB (12.60 s), and 356 server (6.04 s), with the same three ignored
cases. These durations are test runtimes, not migration-pause measurements.
All-target Clippy with warnings denied passes (26.42 s incremental), as do minimal
server (25.19 s), cluster/FFI (61 s), formatting and working/staged diff checks.

Results are recorded in [focused cases](focused-tests.txt),
[full suite results](unit-results.txt), [build checks](build-checks.txt)
and [source identity](source-identity.json). Raw local logs remain under ignored
`target/topology-evidence`. Checked-in results omit the existing large OpenSSL
missing-PDB linker prelude. The source identity freezes all 34 changed Rust files
and Cargo.lock before final validation, with normalized hashes for Git line endings.

The initial focused run found an incorrect fixture expectation: intentionally
fencing a live process can make shutdown report the existing pipeline fault while
still terminating all owners. The corrected case validates that typed outcome and
terminal handles. Clippy prompted associated-function/closure cleanup and boxing
the large startup future on the control path; no lint or delivery requirement is
suppressed. The final source includes the root retention fix discovered during
review: durable Release must not allow cleanup past a still-referenced migration
root.

No broker, optimized multi-process migration, soak extension or comparative
performance run is repeated. Test runtimes are not migration-pause measurements.
Owned automatic orchestration, automatic target recovery, public SQL/atomic API,
removal/replacement contracts and the real migration/restart/failure/performance
oracle remain unfinished. LDB-6043 and startup guards remain. No push or pull
request is made.
