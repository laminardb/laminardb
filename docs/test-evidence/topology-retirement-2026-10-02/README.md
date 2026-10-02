# Observed parent retirement, 2026-10-02

This continuation starts clean at `1430fe8cd251c2fcc227c1be303020eb99c15523`
on `feature/cluster-topology-migrations`. The original baseline is
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC and Rust/Cargo 1.98
remain unchanged; the workspace minimum is Rust 1.95. Locked versions include
DataFusion 53.1.0, Arrow 58.4.0, object_store 0.13.2, Tokio 1.53.1, rdkafka
0.39.0 and async-trait 0.1.92. No dependency or Cargo file changes.

`LaminarDB::retire_cluster_topology_parent(&mut image)` now retires the held parent
of this DB's opaque privately restored target. The existing owned compiler guard
identifies the originating DB. The exact published root, old Commit, complete
current participant preparation, leader, process and assignment are checked before
signalling stop and after observing terminal cleanup. Current local process and
recovery/draining fences are checked again after coordinator access.

The existing stop code joins the compute watcher, retires vnode-generation claims,
settles issued checkpoint decisions and the sink-open witness, and observes source
actors, sink actors and tracked connector children. Cancellation requests, a sink
close result or compute completion alone cannot prove the entire generation gone.
Unresolved owners stay in their existing DB registries across cancellation and the
45-second total request deadline. Retry with the same current image resumes cleanup.
Watcher failure remains sticky and requires recovery or terminal shutdown.

Successful retirement retains `ShuttingDown`, the intake/cut hold, checkpoint
namespace lock and parent catalog/coordinator identity. The restored target stays
unstarted and keeps the compiler permit. Public start/stop cannot release a held
cut. Dropping the image frees private state and the compiler without reopening
intake or releasing the namespace. Existing authorized coordinated recovery can
take over an aborted operation; terminal shutdown can finish cleanup.

`parent_retirement_observed()` is a local observation, not a durable readiness
receipt or target output permit. A future Commit/installer must revalidate it.
No authority encoding, phase, receipt, scheduler, generic framework, dependency
or per-record work is added. Process-owned shuffle handles remain; target transport
generation fencing, participant-complete readiness, atomic topology Commit,
installation, Release, post-commit recovery, detached submission and public SQL
activation are unfinished. LDB-6043 remains. This increment does not meet the
original runtime migration definition of done.

## Checks

All Cargo commands use `CARGO_BUILD_JOBS=1`, `RUST_MIN_STACK=4194304` and `--locked`.

```powershell
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_retirement -- --nocapture
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
cargo check --locked -p laminar-db --no-default-features --features cluster,ffi
cargo fmt --all -- --check
git -c core.excludesFile= diff --check
git -c core.excludesFile= diff --cached --check
```

Ten focused retirement tests pass. They cover retained namespace/cut/catalog
ownership and inactive target state; all three task/connector ownership registries;
blocked compute and connector children; cancellation followed by retry; deadline
exhaustion; foreign images and lost local holds; stale leader authority before stop;
leader change and coordinated recovery takeover after stop; process-lease loss
during cleanup; sticky watcher panic; and local recovery/fault fences.
The injected watcher panic in the focused output is expected and caught by the
production join/error path; the test passes only when it cannot certify the image.

The full selected-feature suite passes 1,044 core, 914 connector, 2,005 DB and
356 server tests: **4,319 passed**, no failures. The existing real-broker test
and one existing DB model-download test remain ignored in that suite. All-target
Clippy passes with warnings denied, both compatibility builds pass, and formatting
and working/staged diff checks pass. Exact outputs are in
[retirement-tests.txt](retirement-tests.txt), [unit-results.txt](unit-results.txt)
and [build-checks.txt](build-checks.txt). The existing MSVC OpenSSL missing-PDB
warning prelude is omitted from test exports; raw logs remain under ignored
`target/topology-evidence`. Source hashes are recorded in
[source-identity.json](source-identity.json) and checked after validation.

## Evidence limits

The new fixture uses the production exact-root compiler/recovery path, real source
terminal wrappers, a real process-fenced sink actor, connector child trackers and
an actual OS checkpoint namespace lock. Its old cut contains real operator codec
state and subscription frontiers. A controlled DB-owned watcher stands in for
compute completion; tracked child guards model work that outlives actor termination.
No actual new source/sink runtime or committed target starts. The full suite also
covers the existing lifecycle and recovery machinery reused by this change.

No broker or optimized three-process scenario was rerun for this increment:
the retirement API has no automatic server worker or HTTP route yet, and the
existing cut/abort/restart scenario does not invoke it. The preceding
[private restore evidence](../topology-restore-2026-10-02/README.md) records that
scenario at its own source/binary hashes. It is not current target-retirement or
post-commit migration certification. Comparative throughput, total RSS, allocation
amplification, pause-inclusive latency and transactional-sink migration evidence
remain required with the complete runtime migration path.
