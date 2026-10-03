# Public topology submission, 2026-10-03

Continued from `1b168570b9df4a359fafb56a9a16d09d5a73d67b` on
`feature/cluster-topology-migrations`, with public-submission changes already
present. Original baseline is `5d81ba9b18d80343373ecfaec4793df8c5caccf1`.
[Source identity](source-identity.json) freezes 37 changed Rust files and the
unchanged lockfile. This evidence certifies unit/library behavior, not a production
binary or multi-process performance run.

## Implemented contract

Running-cluster CREATE SOURCE/STREAM/SINK uses durable protocol-six admission and
the existing monitor. The atomic array API retains raw SQL, caller UUID and exact
parent, validates the entire graph separately, freezes the current assignment and
requires full participant preparation. Authority format 23 is required before the
old cut. Old private protocol-two fixtures remain compatible with their original
scope. Mixed binaries require coordinated process termination and upgrade.

Identical retry resolves the original operation before compilation, runtime or
newer-parent gates; changed bytes conflict. A follower uses one bounded authenticated
HTTP hop to the durable leader; the forwarded receiver cannot forward again.
Explicit legacy adoption preserves the sealed manifest and existing deployment.
SQL admission returns applied=false and a durable UUID receipt; uncertain errors
retain that UUID and the original registry error code.

No new scheduler, generic workflow, dependency, per-row lookup or record allocation
is introduced. Public paths do not use startup bootstrap. Commit and full installed
Release retain the previously implemented exact root, actor and recovery fences.

## Tests actually run

```powershell
$env:CARGO_BUILD_JOBS = '1'
$env:RUST_MIN_STACK = '4194304'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_ -- --test-threads=8
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --test-threads=8
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
```

Focused: 277 pass (core 133, connectors 12, DB 126, server 6), two existing
connector tests ignored. Full suite: 4,459 pass (1,104/921/2,075/359), three existing
tests ignored. Clippy, minimal server, cluster/FFI, formatting, diff and frozen
source checks pass. The DB and all-source tests retain 4 MiB stacks, two control
workers and unchanged one-second listener/filesystem deadlines.

The actual actor case starts the parent, processes aggregate 30 and commits a real
checkpoint, then submits an independent source/stream/sink array. Its owned monitor
prepares, cuts, restores, retires, commits, installs and releases without request
ownership. Actual target input advances aggregate to 45 and source cursors to 6/94.
An ordinary SQL downstream migration preserves that state and advances it to 60 and
old-source cursor 9. All target checkpoint epochs increase; latest is resolved once.
The test asserts immutable admission does not change the live parent catalog before
Commit. A compiler-held identical Active retry still resolves its original receipt.

Additional cases cover two competing parents, terminal retry with compiler held
and shutdown flagged, changed whitespace payload, missing owned coordinator,
expired forwarding, exact authenticated request bytes, typed fence propagation,
oversized responses and redirects, malformed HTTP identity/body/unknown fields,
console authorization and queryable uncertain SQL identity.

Initial failures exposed incomplete test fixtures: Created state, missing bound
coordinator, uncompleted startup intake and absent leased barrier transport. Those
fixtures now satisfy the real contracts. An old assertion now verifies typed
coordinator rejection with unchanged catalog and authority sequence. No guard was
weakened. A native link failure from low disk space was resolved by removing only
obsolete generated debug symbols from this worktree; no linker override was used.

Raw final logs are under ignored `target/topology-evidence/`:
`topology-public-final-focused-raw.txt`, `topology-public-suite-raw.txt`,
`topology-public-final-clippy-raw.txt`, `topology-public-minimal-raw.txt` and
`topology-public-ffi-raw.txt`. Earlier failed runs remain available there.

## Limits and next qualification

The controlled source/sink case is one-process at-least-once evidence. The new
ignored three-process Kafka/S3 soak is included and Clippy-compiled, but its
optimized build/run is pending. Earlier cut/abort/restart runs do not certify
public target activation or full target restart. Transactional migration,
comparative performance, safe removal/replacement and root/journal reclamation
are not claimed by this increment. Roots stay pinned and the journal rejects
admission at 64 retained identities. See [progress](../../cluster-topology-migrations-progress.md).
