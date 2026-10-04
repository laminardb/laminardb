# Cluster topology migration implementation report

## Source and scope

Starting clean commit: `5d81ba9b18d80343373ecfaec4793df8c5caccf1` on
`feature/cluster-topology-migrations`. The current candidate extends qualified
commit `4a972885d747d1054be57d27bcede710b8453c22` with twelve frozen Rust sources.
Its optimized build passes; native attempt 19 fails its third replacement. No user changes,
catalogs, checkpoints, namespaces, topics or volumes were reset. No push or PR
has been made. Cargo.lock and dependency versions are unchanged.

The former runtime LDB-6043 guard lacked a replicated logical topology barrier.
Supported running-cluster SQL now enters one checkpoint-bound migration through
the existing conditional append authority and DB recovery monitor. Startup
bootstrap retains its separate guard. The implementation does not add a
scheduler, consensus system or general workflow framework.

## Implemented contract

| Operation | Classification and support |
| --- | --- |
| Independent supported source → stateless stream → sink | Additive, future-only; atomic multi-object candidate. |
| Compatible stateless downstream stream or sink | Additive, future-only; explicit cut and Release. |
| Existing supported aggregates, joins and windows | Exact definitions, incarnations, state/codecs, timers, source/watermark and output progress preserved. |
| Process replacement or whole cluster restart | Same complete owner map/node slots; reconstruct greatest exact target checkpoint, otherwise its authorized root before any target checkpoint. |
| Replacement, removal or DROP/recreate | Unsupported: current descriptors lack retired-incarnation, dependency projection, sink-settlement and replay evidence. |
| Keys, schemas, window/aggregate semantics, source identity, sink semantics or new stateful operators | Unsupported without a certified state transformation or initialization contract. |
| Membership changes or survivor rescaling | Outside this logical topology contract; complete-map admission rejects them. |

The durable phases are Planned → Preparing → Quiescing → CutPrepared → Committed
→ Activating → Active. A single irreversible authority append commits the
target catalog with the exact old checkpoint, root, state mapping and sealed
source initialization. Target input/output remain held until the complete
installed process roster certifies readiness and a separate durable Release is
applied locally. Post-Commit failure cannot roll back or Abort the target.

New Kafka latest positions are resolved once and persisted. Unsupported
connector pause/replay/start contracts reject before activation. Existing
transactional/idempotent or at-least-once delivery contracts remain distinct;
the native qualification uses at-least-once Kafka sinks. It does not certify
arbitrary external exactly-once effects.

Fresh boot recovery after Commit, including partial installation before a
target checkpoint, requires the exact root handoff pin and unchanged owner map.
Pre-Commit recovery and graceful drain during pending activation remain
excluded. Stale boots, changed assignments and incomplete installation cannot
authorize Release. Portable restore retains historical checkpoint identities
while installing the certified current assignment.

Routine target-checkpoint cleanup protects every irreversible migration root's
metadata, state chunks and output references. Pending migrations block cleanup;
Active cleanup stops before retained roots/prior cleanup anchors. Corrupt or
missing protected bytes prevent deletion. State preflight is bounded by 8192
objects, 4 GiB, eight concurrent 256 KiB reads and a 15-second deadline; combined
root/target manifest metadata is capped at 16 MiB. Root consumption and journal
reclamation remain unsupported, with admission failing closed at 64 retained
operation identities.

## Upgrade and verified interfaces

Stop and observe termination of every old process, upgrade all binaries together,
and retain the deployment identity, configuration and durable namespaces. Mixed
binary operation is unsupported. Explicit legacy adoption binds the exact sealed
manifest and existing deployment UUID without rewriting historical bytes. Public
protocol 6 raises authority to format 23 before the cut; capability advertisements
alone cannot retire cached old actors.

The [operator guide](cluster-topology-operations.md) contains adoption, dry-run,
atomic array submission, UUID retry/status and recovery procedures. Restart may
use the exact complete original adopted bootstrap; durable target authority takes
precedence and cannot be reverted by that old configuration.

The public harness verifies ordinary `POST /api/v1/sql` with:

```json
{"sql":"CREATE STREAM topology_live_downstream AS SELECT join_key, match_count, max_right_id FROM soak_join_aggregate"}
```

The response is HTTP 202 with `result_type = "TOPOLOGY MIGRATION"` and a durable
operation receipt. The array API is `POST /api/v1/cluster/topology/operations`;
dry-run uses `/api/v1/cluster/topology/validate`, and definitive status uses
`GET /api/v1/cluster/topology/operations/{operation_id}`. Committed and locally
active versions remain separate. Exact exercised source/stream/sink statements
are in [the existing native harness](../crates/laminar-server/tests/cluster_soak/topology_migration.rs).

## Verification

Windows MSVC, Rust/Cargo 1.98.0, one Cargo build job and 4 MiB test-thread stacks.
These commands pass for the candidate:

```powershell
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_ -- --test-threads=8
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --test-threads=8
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
```

Focused: 288 passed, two existing ignored. Full: 4475 passed, three existing
ignored (core 1110, connectors 921, DB 2085, server 359). Formatting and diff
checks pass. The unchanged stock optimized build passes in 24m 00s; retained
server SHA-256 is `a2eabeadc2c3eb9802ea50c64d6235527e6bbac025647276f73051f225387c6c`.
The binary keeps the original Windows 1 MiB main stack. The
[verification summary](test-evidence/topology-public-process-2026-10-03/root-boot-verification-summary.json)
binds exact commands, source hashes and build logs. Two authority regressions failed before the repair; the first repair
still failed pending-root handoff validation and an obsolete blanket cleanup
assertion. Exact source identities and both failures are retained.

The earlier qualified stock server passes native attempt 18: both public
migrations, one leader and two follower hard kills, the whole original-bootstrap
cold restart and every independent final stateful, sink and sequence oracle.
Native attempt 19 passes both migrations and two replacements, then fails the
third full-roster Release at the unchanged 90-second ceiling after 284.86 s.
Root metadata remains exact after the artifact floor advances to 52. The
surviving fabric retains delivery-loss incidents: installation rejects them,
but their repair floor cannot advance until Release. That recovery ordering
defect is being repaired against the existing prepared generation cutoff.
Whole cold restart and final oracles are not reached. Matched pair three did
not start. Historical results do not qualify newer source changes.

[Process evidence](test-evidence/topology-public-process-2026-10-03/README.md)
records exact source/binary identities, authority/checkpoint references and
deterministic fault boundaries. [Performance evidence](test-evidence/topology-performance-2026-10-04/README.md)
records three alternating stock queue trials, matched paced-load process pairs,
pause-inclusive consumer observations, RSS and artifact endpoints. No production
latency, throughput capacity, statistical equivalence or growth-rate claim is made.

Default connector variants and native exactly-once Delta/S3 scenarios have not
been run for this candidate; they require those features and fixtures. Native
kills at every durable migration phase are not claimed; deterministic injected
write/cancellation/ownership failures cover their stated boundaries. Windows
allocation tracing requires an elevated tracing session. The retained harness
does not isolate queue item counts, preparing-only consumer latency or pure state
restore duration. Existing buffer/state gauges and observed control windows are
reported separately.

The [engineering guide](cluster-topology-engineering.md) describes serialization,
state compatibility, fences, bounded ownership, recovery and the failure matrix.
The [progress file](cluster-topology-migrations-progress.md) records the current
qualification state and remaining unsupported contracts.
