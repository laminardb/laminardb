# Committed topology transport preparation, 2026-10-02

This continuation starts clean at `646fab31e19db2d24c5d3bfbcefa0175d7992e72`
on `feature/cluster-topology-migrations`. The original baseline is
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC, Rust/Cargo 1.98 and
the workspace Rust 1.95 minimum remain unchanged. Locked versions include
DataFusion 53.1.0, Arrow 58.4.0, object_store 0.13.2, Tokio 1.53.1, rdkafka
0.39.0 and async-trait 0.1.92. No dependency or Cargo metadata changes.

`LaminarDB::prepare_cluster_topology_transport(&mut image)` binds this DB's
committed private graph and both directions of its process-owned shuffle fabric
to the exact logical topology and catalog digest. It audits current complete
authority/process/assignment/adoption around publication, reobserves actual parent
retirement, validates sealed cursors and holds the existing execution/assignment
locks. Created recovery requires empty runtime/connector ownership. Its total
cooperative budget is 45 seconds. No caller-supplied readiness, authority, Commit
or termination flag can replace these checks.

The small Copy transport identity lives inside the existing installed assignment.
Existing assignment locks publish both endpoint bindings; the existing delivery
mutex serializes loss auditing and pending admissions with sequence-domain reset.
Old scope tokens, connections, blocked sends and handshake tokens are retired.
Predecessor queued/staged data, frontiers and barriers are filtered before loss
accounting. Genuine unrepaired loss blocks installation and is never forgiven.
Expired processes, inactive assignments, incompatible endpoint pairs, conflicting
same-version digests and downgrades reject before publication. Assignment/recovery
changes retain the topology floor; identical installation retries preserve sequences.

The request/response handshake and leading Hello carry the exact version/digest.
Zero plus empty explicitly represents legacy fabric. Partial, malformed, divergent
or legacy identities cannot open migrated streams. New clients verify the echo,
so old binaries ignoring the fields cannot open target streams. Data/frontier/barrier
payloads and Arrow schemas remain unchanged. Graph/operator checks run at existing
batch/ownership boundaries using version atomics; the stream checks the full immutable
identity. Retained async send plans capture their fixed graph binding. No new per-row
lookup, hashing, serialization, lock, allocation or task is introduced. There is no
general framework, scheduler, registry or secondary authority head.

Success keeps the operation Committed, target private/unstarted, parent ShuttingDown
and cut/intake/namespace held. It does not replace the local catalog/coordinator,
start source/sink actors, acknowledge input, publish an authority append or grant
Release/output. Cancellation after local publication retains the target binding;
retry the image or reconstruct from the immutable root. Private reconstruction
rejects a divergent retained fabric. Authority format 20, preparation protocol 4,
plan protocol 2 and root encodings 1/2 are unchanged. Protocol-4 preparation does
not prove installation capability or readiness on every current process. Those
must be certified before future participant-complete Release. LDB-6043 remains.

Eleven core cases include nine tests using real loopback gRPC with two process
incarnations, unchanged assignment version 1 and recovery generation 0. They cover
reconnect/identical-install sequence continuity, divergent and legacy peers, staged
and queued predecessor controls/data, budget-blocked send cancellation, exact pair
publication, inactive assignment refusal, retained floors after assignment/recovery,
real burned-sequence loss, old handshake tokens and monotonic lease expiry. Two
delivery/wire tests cover malformed identities and late predecessor data/barrier
admissions without target loss or sequence advancement.

Seven DB cases reuse actual aggregate decoding (nine frames, old sum 30 then 45),
eight local vnodes, subscription incarnation 7/nonzero exclusive sequences, preserved
cursor 3 and sealed new cursor 91. They cover exact Commit/private graph binding,
idempotence, old graph rejection before input, strict reconstruction after fencing,
refusal to rebind an executed image,
foreign/uncommitted/faulted images, cursor cancellation/deadline, unresolved watcher
ownership, divergent fabric and separate Created recovery. The watcher is controlled;
existing real actor terminal-observation tests remain in the full suite. Direct
private codec execution is not a runtime/output test. Created recovery retains the
configured control process; it is not a full multi-process target restart.

Commands use the established sequential four-package feature union:

```powershell
$env:CARGO_BUILD_JOBS = '1'
$env:RUST_MIN_STACK = '4194304'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_transport -- --nocapture
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
cargo check --locked -p laminar-db --no-default-features --features cluster,ffi
cargo fmt --all -- --check
git -c core.excludesFile= diff --check
git -c core.excludesFile= diff --cached --check
```

The final focused command passes all 18 tests: 11 core (1.33 s) and seven DB
(1.24 s). The selected-feature suite passes 4,373 tests: 1,078 core (30.56 s),
914 connectors (45.08 s), 2,025 DB (11.90 s) and 356 server (6.05 s).
The same broker-dependent connector and model-download/ORT tests remain ignored.
These durations are validation runtimes, not migration-pause measurements.

All-target Clippy with warnings denied passes (1 min 27 s), as do the non-default
server check (1 min 07 s), cluster/FFI check (1 min), formatting and working/staged
diff checks. All 30 changed source hashes (29 Rust and one protobuf) and Cargo.lock
remain unchanged through final validation and staging. They identify tested
working-tree bytes; Git normalizes line endings. No production binary identity
or performance certification is inferred.

[Focused results](transport-tests.txt), [full suite results](unit-results.txt),
[build checks](build-checks.txt) and [source identity](source-identity.json) retain
the final evidence. Raw logs remain under ignored `target/topology-evidence`.
Native OpenSSL missing-PDB warnings remain there and are omitted from the exported
test prelude. Earlier compile/lint attempts and the initial focused run's private
binding failure remain in the raw logs. Decoding closes the one-shot restore window;
the corrected binding guard separately tracks whether execution has begun, and
rejects rebinding permanently after an execution attempt is armed.

No broker or optimized multi-process scenario is rerun: the existing server/harness
does not drive this internal exact-Commit transport method. Earlier cut/abort/restart
and performance results retain their own source/binary identities. They cannot
certify activated migration. No throughput, latency, RSS, allocation, migration
pause or transactional-sink result is claimed. Runtime catalog/coordinator/actors,
participant-complete Release, automatic post-Commit recovery and the real stateful
migration/restart/failure/performance oracle remain unfinished. No changes are
pushed and no pull request is created.
