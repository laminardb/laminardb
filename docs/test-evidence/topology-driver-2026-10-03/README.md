# Database-owned topology phase progress, 2026-10-03

This continuation starts clean at `02202d7910d3d85da6e1946c2c0097e4f2df4c77`
on `feature/cluster-topology-migrations`. The original baseline remains
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC, Rust/Cargo 1.98,
workspace Rust 1.95 minimum and Cargo.lock are unchanged. The lock retains
DataFusion 53.1.0, Arrow 58.4.0, Tokio 1.53.1, rdkafka 0.39.0 and
async-trait 0.1.92.

The existing DB-owned recovery supervisor drives already admitted operations.
Every healthy node independently compiles, prepares its private image, observes
retirement, installs held actors and certifies its exact runtime. The current
leader uses the existing manual checkpoint owner, stages the root, commits after
the complete protocol-four preparation roster and publishes Release after the
complete protocol-five installation roster. Followers apply that exact decision.
No scheduler, queue, dependency, authority format or generic workflow is added.

The supervisor owns at most one private image and the existing compiler permit.
A status observer cannot cancel that ownership. Current local faults, recovery,
drain and process fencing release private preparation. An aborted held cut requests
coordinated parent recovery without opening intake. A phase with no durable
progress for 180 seconds requests coordinated recovery. Each phase retains its
existing I/O/deadline/CAS bounds; exact checkpoint owners finish terminal cleanup.

The latest operation UUID/status sequence is a polling hint, never permission.
Each phase uses definitive audited status and the existing authority/lifecycle
methods. Held local boundaries take precedence over later journal entries. Idle
polling does not copy the installed manifest/root or repeatedly audit its blobs.
An Active operation never reconstructs a missing runtime from the parent cut.

Nine new cases cover:

- Hint progress through admission/abort; a missing immutable plan still rejects
  definitive status and cannot authorize phase work.
- Independent local compilation and the existing checkpoint route; a missing
  live route cannot manufacture a cut, and a stalled phase requests recovery.
- Exact-root state through private preparation, observed retirement, Commit,
  actual held actors and participant-complete Release. The existing aggregate
  retains 30 and produces 45 from three future rows of value 5. New source
  initialization remains sealed at 91 and is resolved once.
- Lost private image after retirement reconstructs from the same held cut and
  reaches Release without resuming the parent or starting duplicate actors.
- Pre-Commit abort drops the compiler permit, queues coordinated parent recovery
  and retains intake, root and durable status.
- A queued fault drops private preparation and cannot publish Commit.
- Failed atomic installation observes cleanup and retries the same committed
  root, source positions and namespace ownership.
- An Active operation with a missing process-owned binding cannot borrow the
  original Release or restart from the parent root.
- The actual DB-owned supervisor is enabled once, survives observer cancellation
  during blocked installation, reaches local Active and joins its monitor and
  actors at shutdown.

The first focused run passed the hint, stalled route, fault and abort cases, then
overflowed the 4 MiB test-thread stack while building a private target image.
The correction puts the single image and active restore/phase futures on the
heap. The long-lived monitor future is pinned once per generation. The test stack,
fixed two-worker control executor and its existing 4 MiB stacks are unchanged.
These allocations occur only on the control path; no per-record work or Arrow
batch deep copy is introduced. The failed raw log remains under ignored
`target/topology-evidence/topology-driver-focused-initial-raw.txt`.

Final validation uses `CARGO_BUILD_JOBS=1`, `RUST_MIN_STACK=4194304`, `--locked`,
and the established `cluster,aws,kafka` union for all four selected packages.
The final source is frozen before full unit validation. Raw and normalized Git
hashes for ten changed Rust sources and Cargo.lock are in
[source-identity.json](source-identity.json). Commands and exact outcomes are
recorded in [focused-tests.txt](focused-tests.txt),
[unit-results.txt](unit-results.txt) and [build-checks.txt](build-checks.txt).

The full selected lib/bin suite passes 4,422 tests: 921 connectors, 1,088 core,
2,057 DB and 356 server, with two connector and one DB case ignored. All nine
focused cases pass on the same final source. All-target Clippy denies warnings;
minimal server and cluster/FFI builds, formatting and diff/source checks are
recorded separately. Debug MSVC links report the existing OpenSSL missing-PDB
LNK4099 warnings; they are not suppressed.

The runtime fixture is one process with controlled ALO connectors and a configured
original process identity. It does not certify a new boot, automatic target
recovery, a complete live parent-to-target checkpoint scenario, transactional
migration, public SQL/API submission, multiple real server processes or broker
faults. No optimized soak or comparative performance run is repeated. Automatic
target recovery, whole-cluster startup, reference-aware root retirement, bounded
journal reclamation, public writes and removal/replacement remain unfinished.
LDB-6043 and startup guards remain until those paths are implemented and certified.
