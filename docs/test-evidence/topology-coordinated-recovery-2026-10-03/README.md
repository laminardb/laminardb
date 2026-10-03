# Coordinated target recovery and cold startup, 2026-10-03

This increment starts clean at `fedf5d7eceae25937be6fc537fefcbd9cecff525`
on `feature/cluster-topology-migrations`. The original baseline remains
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Toolchain, Rust minimum and
Cargo.lock are unchanged. The 42 changed Rust sources are frozen in
[source-identity.json](source-identity.json).

Authority encoding 22 and recovery protocol 6 bind the existing recovery round
to the exact topology Commit and full current process terms. Replacement actors
receive a new runtime UUID. Selection happens after complete stopped receipts
and checkpoint/sink settlement, then uses the greatest exact target checkpoint
or the original migration root. A missing or corrupt newer target fails without
parent fallback. Actual actor/state/source/sink/receiver readiness is checked
before restored/Ready receipts and again before Release.

A first coordinated recovery Release also publishes a pending target's topology
Release atomically. Recovery of an already Active target retains its original
activation evidence. That original receipt cannot authorize the replacement UUID.
Cold startup reconstructs the committed catalog without starting actors and
queues this same recovery owner. Missing deployment identity fails closed;
checkpoint reads, including cached readers, cannot create a replacement identity.
The original non-cluster recovery lifecycle remains available.

Six new authority tests exercise full replacement rosters, first Release,
superseded installation rounds, exact target cut selection, stale original
Release and explicit assignment/process namespaces. Six DB tests run actual
source/sink/graph actors and the existing recovery monitor. They cover a fault
before first Release, a fault after Active, recovery from a real target checkpoint,
cold root startup, cold target-checkpoint startup and missing deployment identity.
An additional checkpoint reader test covers both cached and fresh readers after
identity deletion.

The state oracle preserves aggregate 30, produces 45, restores the selected
checkpoint and processes further input to reach 60. The real checkpoint case
checks source cursors 6/94 then 9/97 and publishes a strict successor checkpoint.
Root cases admit no invented acknowledgements; target recovery accepts only
previous acknowledgements covered by the selected checkpoint. The original
Release/checkpoint of the cold target fixture is authority fixture evidence;
replacement actor startup and recovery Release in that case are real DB paths.
These are controlled connector tests, without broker or multi-process migration
certification.

Initial failures exposed a missing encoding gate, large nested recovery futures,
an incomplete barrier fixture, late fixture input and a non-cluster regression.
Corrections retain the original validators, heap-pin large futures and keep the
same control worker count and 4 MiB stack limit. Clippy additionally required
boxing optional control bindings and test fixture futures. One existing server
live-listener test exceeded its unchanged one-second deadline on its first full
run; the same suite passed on retry. The final unrestricted run also hit the
existing filesystem conditional-put probe's one-second deadline. The final full
suite passes with eight test threads; no timeout was increased. Failed raw logs remain under ignored
`target/topology-evidence`.

Final checks use `CARGO_BUILD_JOBS=1`, `RUST_MIN_STACK=4194304`, `--locked`,
and the four-package `cluster,aws,kafka` feature union. Final results are recorded
in [unit-results.txt](unit-results.txt) and [build-checks.txt](build-checks.txt).
All 4,446 tests pass: 1,103 core, 921 connectors, 2,066 DB and 356 server, with
the same three existing ignored cases. All-target Clippy denies warnings;
minimal server, cluster/FFI, formatting and diff/source checks pass.
Debug MSVC links retain the existing OpenSSL LNK4099 missing-PDB warnings.
No dependency, additional scheduler or per-record migration work is introduced.

Subscription readers and retention still fail closed across a historical pipeline
transition; the checkpoint writer's audited bridge is already implemented. Public
SQL/atomic submission, safe removal/replacement, reference-aware root/journal
reclamation, real multi-process migration/restart and comparative performance
remain subsequent work. Runtime topology mutation guards remain in place.
