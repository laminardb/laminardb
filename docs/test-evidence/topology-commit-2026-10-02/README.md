# Atomic target Commit and private reconstruction, 2026-10-02

This continuation starts clean at `ca2674957e162bf508f6347529e70eea1a4b006b`
on `feature/cluster-topology-migrations`. The original baseline is
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC and Rust/Cargo 1.98
remain unchanged; the workspace minimum is Rust 1.95. Locked versions include
DataFusion 53.1.0, Arrow 58.4.0, object_store 0.13.2, Tokio 1.53.1, rdkafka
0.39.0 and async-trait 0.1.92. No dependency or Cargo metadata changes.

`LaminarDB::commit_cluster_topology_target(&mut image)` binds the DB's retained
private image to observed parent retirement, fresh sealed-source validation and
the configured leader's existing authority. It accepts no caller-supplied
termination flag, process term, assignment, root or catalog. Every frozen
owner/evidence process must have protocol-4 preparation and its original current
process/assignment fence. The core also rechecks the exact compiled image/root,
parent catalog, latest settled checkpoint cut and competing authority. Sixteen
CAS attempts share a 15-second budget; the DB's total cooperative budget is 45 seconds.

Authority format 20 publishes `lease.catalog_manifest` and the operation's
immutable `TopologyCommit` in one create-only append. The same bounded journal
supplies the committed version. There is no second head, scheduler, registry,
framework, dependency or per-record work. The original topology-1 baseline,
object incarnations, historical checkpoint/outcome links and epoch allocator
stay exact. Existing plan protocol 2 and root encodings 1/2 are unchanged.
Historical protocol-3 target observations remain readable but cannot be upgraded
in place or authorize Commit; all required binaries must be upgraded together.

Commit advances the committed catalog to topology 2, without local activation.
The parent stays ShuttingDown with intake/cut/namespace held. The target image
stays Created/unstarted. Leader replacement and recovery faults preserve the
decision; abort and parent recovery Release reject. Cached parent recovery-admission
snapshots become invalid. Commit/root/cut/receipt/adoption anchors and old state
artifacts remain pinned through authority and artifact pruning.

`recover_committed_cluster_topology(operation_id)` reuses the existing isolated
compiler, strict historical-parent loader, actual operator codecs and configured
state/payload limits. It reconstructs a private image before any target checkpoint
on a Created DB or the still-held retired parent. Current leader, complete process
terms, assignment and durable local adoption are audited around restore. New boots
can use a newer assignment only with the same vnode owners/domain/ABI and complete
stable roster; rescaling and changed ownership reject. Historical PipelineIdentity,
checksums, source attempts and subscription incarnation/frontiers stay exact.
Sealed new-source cursors are validated rather than resolved again. No processed
attempt, target checkpoint, input acknowledgement or output permit is invented.
Cancellation/deadline frees the partial image/compiler slot and retains Commit.

Committed catalog replay accepts the complete current inventory or the exact
complete adopted original bootstrap, whose ordered prefix is preserved by the
additive audit. Arbitrary subsets and changed definitions reject. Ordinary startup
fails closed on pending Commit until target installation and participant-complete
Release exist. LDB-6043 remains. Public submission, detached migration ownership,
generation fencing, installed target actors, automatic full-cluster target recovery
and Release remain unfinished. This is an internal, resumable Commit/reconstruction
checkpoint of the original plan.

The core fixture uses two exact frozen participants, historical state/subscription
metadata and the existing conditional-write fault store. It exercises atomicity,
complete protocol capability, immutable image/assignment checks, concurrent and lost
responses, cancellation after create, deadline before create, leader change before
and after Commit, recovery faults, cached parent Release rejection and retained
authority/artifact anchors. A real full 30-second monotonic process-takeover
observation changes a peer boot and assignment version while retaining historical
ownership. That wait is a fencing test, not a migration-pause measurement.

The DB fixture restores the actual aggregate archive (sum 30), nine state frames,
eight local vnodes, stream incarnation 7 with nonzero subscription sequences,
preserved cursor 3 and sealed new cursor 91. It verifies Commit/idempotence,
retained-image handoff, actual private state reconstruction (next sum 45), missing
or corrupt payload rejection, source-validation and reconstruction budgets,
cancellation and compiler/namespace ownership. Its watcher is controlled; existing
terminal actor tests remain in the full suite. A separate Created DB replays the
committed six-object inventory using the original three-object bootstrap, rejects
ordinary startup and restores privately. It keeps the configured control process;
the core test separately exercises actual fenced peer process takeover. This is
not a multi-process target runtime restart or target sink/transport activation test.

Commands use the same four-package feature union and locked dependencies:

```powershell
$env:CARGO_BUILD_JOBS = '1'
$env:RUST_MIN_STACK = '4194304'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_commit -- --nocapture
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
cargo check --locked -p laminar-db --no-default-features --features cluster,ffi
cargo fmt --all -- --check
git -c core.excludesFile= diff --check
git -c core.excludesFile= diff --cached --check
```

Final selected-feature results are 4,355 passed: 1,067 core (31.83 s),
914 connectors (45.89 s), 2,018 DB (12.04 s) and 356 server (6.04 s).
The existing broker-dependent connector test and model-download/ORT test remain
ignored. The final focused command passes all 21 tests: 13 core (30.16 s) and
eight DB (0.41 s). The deliberate monotonic takeover accounts for core timing;
these durations are validation runtimes, not migration-pause measurements.

All-target Clippy with warnings denied passes (41.66 s). The non-default server
check passes (17.23 s), as does cluster/FFI (59.49 s). Formatting and diff checks
pass. All 33 changed Rust working-tree source hashes and Cargo.lock stay unchanged
through final validation and staging. They identify tested working-tree bytes;
Git normalizes line endings. No production binary identity or performance
certification is inferred from them.

[Focused results](commit-tests.txt), [full suite results](unit-results.txt),
[build checks](build-checks.txt) and [source identity](source-identity.json) retain
the final evidence. Raw logs remain under ignored `target/topology-evidence`.
Native OpenSSL missing-PDB linker warnings are retained there and omitted from
the exported test prelude. Earlier compile/lint attempts and the initial full
run's startup error-code regression remain in the raw logs. The final code
preserves the existing bootstrap rejection wording and corrupt-catalog LDB-6003
recovery diagnostic while keeping the pending-Commit startup guard.

No broker or optimized multi-process scenario is rerun for this increment: the
existing server/harness does not drive these internal Commit/reconstruction methods.
Earlier cut/abort/full-restart runs retain their own evidence and source/binary
identities; they do not certify target activation. No comparative throughput,
latency, RSS, allocation, cutover pause or transactional-sink migration result is
claimed here. The real public stateful migration/restart/failure oracle and
performance work remain required. No changes are pushed and no pull request is created.
