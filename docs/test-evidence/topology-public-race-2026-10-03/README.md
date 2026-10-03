# Public admission contention repair — 2026-10-03

The stock optimized server from `d7297d653f8b97a205ab1f52f268d3072ffa325c`
started three real processes after the owned MinIO test bucket was restored.
Public exact-inventory legacy adoption succeeded. The first migration request
returned HTTP 409 without admission while periodic checkpoint 49 was reserved.
The final authority record retains that exact checkpoint inventory, no topology
operation, no assignment reservation, no artifact cleanup and no recovery fault.
The response body was not retained by the initial harness. These are failed
qualification runs, not evidence of target activation or restart success.

The repair reports checkpoint/cleanup reservations as typed contention and retries
the same compiled plan, UUID, parent and process/leader proof within the existing
45-second submission deadline. Other unresolved operations/recovery/assignment,
changed payloads and stale parents retain their definitive conflict/fence checks.
A checkpoint Abort does not release its retained artifacts: submission waits for
exact artifact cleanup too. No barrier or runtime guard is bypassed.

The selected four-package `cluster,aws,kafka` focused suite passes 278 tests:
core 133, connectors 12 (two existing ignored), DB 127 and server 6. The four public
DB tests pass, including actual parent/target actors and checkpoint cleanup wait.
All-target Clippy passes with warnings denied. Input test import failure was
corrected; its earlier raw log remains under `target/topology-evidence`.

Commands:

```powershell
$env:CARGO_BUILD_JOBS='1'
$env:RUST_MIN_STACK='4194304'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_ -- --test-threads=8
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
```

Source identity binds the five changed Rust files and unchanged Cargo.lock against
the public parent commit. The optimized failed-run binary is unmodified, with a
1 MiB PE main-stack reserve. Runtime control workers/stacks remain unchanged.
Fresh repaired-server process qualification and matched performance are pending.
