# Target checkpoint and private recovery selection, 2026-10-03

This continuation starts clean at `482b20334c6e01aa00e394795719f094020cd325`
on `feature/cluster-topology-migrations`. The original baseline remains
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Rust/Cargo 1.98, the workspace
Rust 1.95 minimum and Cargo.lock are unchanged. No dependency, authority format,
scheduler or per-record work is added.

The first target checkpoint extends its historical parent only through the
audited, released migration root and exact descriptor. Deployment, pipeline
identities, source inventories, assignment/ABI and watermark continuity remain
explicit. Ordinary predecessor validation still requires equal pipeline identity.
Subscription continuity checks the sealed parent certificate and exclusive
frontiers, then uses the exact target certificate in the comparison view. It never
changes historical manifest bytes, hashes or sequence ranges.

An opaque authority input chooses the greatest target Commit, or the migration
root before the first target checkpoint. It audits the current target, leader,
full stable owner roster, current process leases and exact assignment. Damaged
or foreign newer progress fails instead of falling back. Selection and rechecks
share a 15-second budget. Private reconstruction uses the existing compiler,
checkpoint readers and operator codecs with their 45-second and memory bounds.
Sources resume the selected checkpoint; new sources cannot borrow their original
latest initialization after target progress has committed.

Eight core cases cover root selection, greatest target checkpoint, later Abort,
damaged target evidence, new boots and old Release rejection, a newer Commit
during selection, cancellation/deadline ownership, and the first checkpoint
bridge's exact identities. Three DB cases load real state frames and actual
stored subscription output segments, reject corrupt target state without parent
fallback, and prevent a selected recovery image from borrowing the original
installation/Release path.

The DB fixture keeps the original parent aggregate at 30, captures a target
checkpoint at 45, then restores that checkpoint and processes three future rows
of value 5 to produce 60. It checks source cursors 6 and 94, watermark 100,
subscription certificate/frontiers, one initialization resolution, no sink
effects, no live actors, and no authority append from private reconstruction.
Its Release is authority fixture evidence; it does not certify live actor
installation or a broker scenario.

Initial tests exposed a missing first-target predecessor contract and two fixture
errors. Process takeover must observe the real monotonic process-lease TTL;
advancing subscription ranges must reference stored output segments. The final
fixtures meet those existing contracts. No lease, manifest or output validator
was relaxed. Failed raw logs remain under ignored `target/topology-evidence`.

Final checks use `CARGO_BUILD_JOBS=1`, `RUST_MIN_STACK=4194304`, `--locked`, and
the established four-package `cluster,aws,kafka` feature union. The 19 changed
Rust sources and unchanged lockfile are frozen in
[source-identity.json](source-identity.json). Exact commands and results are in
[focused-tests.txt](focused-tests.txt), [unit-results.txt](unit-results.txt) and
[build-checks.txt](build-checks.txt).

All 11 focused cases pass. The selected lib/bin suite passes 4,433 tests:
1,096 core, 921 connectors, 2,060 DB and 356 server, with the same three existing
ignored cases. All-target Clippy denies warnings; minimal server, cluster/FFI,
formatting and diff/source identity checks pass. Debug MSVC links report the
existing OpenSSL LNK4099 missing-PDB warnings; they remain unsuppressed.

Automatic recovery Start/Release, whole-cluster startup, public SQL/atomic API
submission, replay/retention across multiple migrations, removal/replacement,
real multi-process target migration/restart and comparative performance remain
unfinished. Runtime topology writes and ordinary committed-target startup remain
guarded. This increment certifies private state selection and checkpoint
continuity, not the requested end-to-end migration.
