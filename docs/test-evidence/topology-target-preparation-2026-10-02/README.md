# Durable target preparation observations, 2026-10-02

This continuation starts clean at `7a9496889a68841d8fce6a79bd41cf2506273dfc`
on `feature/cluster-topology-migrations`. The original baseline is
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC and Rust/Cargo 1.98
remain unchanged; the workspace minimum is Rust 1.95. Locked versions include
DataFusion 53.1.0, Arrow 58.4.0, object_store 0.13.2, Tokio 1.53.1, rdkafka
0.39.0 and async-trait 0.1.92. No dependency or Cargo metadata changes.

`LaminarDB::certify_cluster_topology_target_preparation(&mut image)` connects the
opaque privately restored target to observed parent retirement and the existing
fenced authority append. It observes the existing runtime/connector owners even
on retry. The controller uses its actual process/assignment authorities and durable
local adoption. No caller-provided termination flag, process, root or receipt is
accepted by the DB method. The cooperative total budget is 45 seconds; the core
append allows 16 CAS attempts within 15 seconds.

The first receipt upgrades authority to format 19. Target preparation requires
protocol 3; the admitted candidate plan remains protocol 2. The sorted receipt
contains the exact frozen participant/boot/process term and first immutable append.
The enclosing operation binds the unchanged plan, compatibility, assignment,
root and old cut. Every frozen owner/evidence participant is required. Identical
retries append nothing; concurrent participants use the existing conditional-write
serialization. Status reads audit all anchors, and pruning retains each anchor,
including after a pre-Commit abort. Receipt counts remain capped at 129 participants
and complete authority bodies at 256 KiB. Earlier encodings omit empty receipts.

Receipts are historical preparation observations. They do not prove that a target
image is still resident, a sealed cursor is still available or target receivers
and sinks are installed. Dropping an image retains its historical receipt and the
old cut/namespace hold. Current restore inputs compare every immutable requirement
while allowing separately validated receipt/status progress from other participants.
No PipelineIdentity check or historical checkpoint is rewritten.
Current retries need the retained image; losing it before Commit requires pre-Commit
abort and coordinated parent recovery. Post-Commit reconstruction is future work.

The committed catalog remains topology 1, the runtime remains `ShuttingDown`, and
the target remains unstarted. Atomic target Commit, usable post-Commit recovery,
transport generation fencing, installation, participant-complete Release, detached
work and public SQL submission remain unfinished. LDB-6043 remains. This is a
reviewable preparation checkpoint, not the original migration definition of done.

Validation uses the existing exact-root fixtures and conditional-write fault store.
The core fixture freezes two exact participants and retains state/subscription
metadata. It tests full versus partial preparation, concurrent CAS, immutable
requirements, protocol rejection, all-process/assignment fencing even on retry,
lost responses, cancellation after successful create, pre-create deadline, leader
replacement and retained anchors after abort/pruning. Malformed receipt evidence
and rewrites fail closed.

The DB fixture restores the actual aggregate archive (sum 30), nine state frames,
eight local vnodes, preserved stream generation 7 and nonzero subscription sequences,
plus preserved cursor 3 and sealed new cursor 91. Its watcher is controlled, its
connector child uses the real terminal tracker, and its namespace uses a true OS
file lock. It checks receipt gating, no external connector effects, foreign/fault
rejection, process loss, deadline/cancellation, idempotent retry, image drop and
held public lifecycle. The previous retirement increment separately exercises the
real source wrapper and process-fenced sink actor; those tests remain in the suite.

Commands use the same four-package feature union and locked dependencies:

```powershell
$env:CARGO_BUILD_JOBS = '1'
$env:RUST_MIN_STACK = '4194304'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_target_preparation -- --nocapture
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
cargo check --locked -p laminar-db --no-default-features --features cluster,ffi
cargo fmt --all -- --check
git -c core.excludesFile= diff --check
git -c core.excludesFile= diff --cached --check
```

Final selected-feature results are 4,334 passed: 1,054 core (30.49 s),
914 connectors (45.38 s), 2,010 DB (11.77 s) and 356 server (6.04 s).
The broker-dependent connector test and existing model-download/ORT test remain
ignored. The final focused command passes all 15 tests: ten core (30.14 s) and
five DB (0.31 s). Core timing includes the deliberate full 30-second monotonic
process-takeover observation; it is not a migration-pause measurement.

All-target Clippy with warnings denied passes (23.19 s). The non-default server
check passes (8.48 s), as does cluster/FFI (48.38 s). Formatting and working/staged
diff checks pass. All 19 changed Rust working-tree source hashes and Cargo.lock
remain unchanged through final validation and staging. These identify tested
working-tree bytes; Git normalizes line endings, and no production binary identity
or performance certification is inferred from them.

[Focused results](target-preparation-tests.txt), [full suite results](unit-results.txt),
[build checks](build-checks.txt) and [source identity](source-identity.json) retain the
final evidence. Raw logs are under ignored `target/topology-evidence`. Native
OpenSSL missing-PDB linker warnings are preserved there and omitted from the
exported test prelude. Earlier failed compile/test/lint attempts remain in the
raw logs; the final results include the corrected duplicate-successor check and
proper observed process takeover.

No broker or optimized multi-process scenario is rerun for this increment: the
existing server/harness does not drive this new internal DB receipt method. The
earlier cut/abort/full-restart runs retain their own evidence and source/binary
identities; they do not certify target Commit or activation. No comparative
throughput, latency, RSS, allocation, pause or transactional-sink migration evidence
is claimed here. The real public stateful migration/restart/failure oracle and
performance work remain required.
