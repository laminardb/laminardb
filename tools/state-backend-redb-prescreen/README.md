# redb 4.1.0 construction workspace

This unpublished standalone workspace is **CONSTRUCTION ONLY / NO DECISION / NOT PRESCREEN OR
QUALIFICATION EVIDENCE**. It is outside the LaminarDB workspace and runtime dependency graph.

- `candidate` is the only package that depends on exact `redb =4.1.0` with default features off.
- `gate` has no redb dependency. Its redb-free construction oracle reconstructs the expected state
  and verifies a canonical export record by record. The caller, not the gate, is responsible for
  launching `scan` as a fresh process and keeping the report/export in one private run directory.

The primary construction lane is `construction-only-no-decision`. It creates a
disposable 64 MiB logical four-table fixture, executes one returned transaction in each frozen
`I1`, `I2`, and `QR` mode, exercises the database-wide writer with a 250 ms `HOLD`, and exports the
complete state from a fresh candidate process. The gate accepts no backend disposition and repeats
all qualification, selection, production, delivery, and soak fields as false.

This is not the approved Docker or native prescreen harness. In particular, it does not implement
the detached two-principal protected-review gate, crash actuator, native target preflight,
steady-state latency matrix, recovery comparison, or final disposition algorithm. Do not reuse its
construction wall-times as latency evidence. It operates only on disposable files in a trusted,
runner-owned directory: it is not hardened for attacker-controlled paths, and a failed command can
leave partial files that the caller must discard. An external runner must impose a hard deadline;
redb's database-wide writer wait has no cancellation or timeout API.

Run from this directory after building both binaries, with all three outputs inside a new private
temporary directory. Arbitrary output locations are not automatically ignored by Git:

```text
state-backend-redb-prescreen-candidate construction-only-no-decision run <new-db> <new-report>
state-backend-redb-prescreen-candidate construction-only-no-decision scan <existing-db> <new-export>
state-backend-redb-prescreen-gate verify-construction <report> <export>
```

All output paths must have an existing parent and must not already exist.

`candidate/src/bin/resource_review.rs` is the smaller, Linux-aware reproducer retained for the
2026-07-29 bounded no-select decision. It reports physical allocation using `st_blocks`, and remains
elimination evidence rather than a benchmark or qualification runner. From this workspace, run its
buffered and baseline-barrier scenarios with separate existing empty directories:

```text
cargo run --locked --release -p state-backend-redb-prescreen-candidate --bin resource_review -- <empty-directory>
cargo run --locked --release -p state-backend-redb-prescreen-candidate --bin resource_review -- <empty-directory> baseline-barrier
```
