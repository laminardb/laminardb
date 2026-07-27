# Distributed keyed state — Core Cycle 1 review

- **Date:** 2026-07-27
- **Scope:** private in-memory managed `COUNT(*)`/nullable `SUM(Int64)` vnode reference,
  FULL/EMPTY capture and restore, and caller-supplied lifecycle-batch publication
- **Code commits:** `fa0a47ab`, `7080a031`
- **Slice verdict:** **APPROVE WITH OWNED FOLLOW-UPS**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Implemented boundary

`aggregate_state/managed_v1.rs` now provides one concrete, private reference shard. It validates an
entire append batch before mutating live state, preserves source order for duplicate keys, rejects
wrong-vnode keys and COUNT/SUM overflow, enforces nullable/non-nullable SUM semantics, and accounts
entry count, encoded key size, and logical key-plus-accumulator payload.

The shard binds routing schema, aggregate contract, operator, state table, vnode count/vnode, exact
assignment version and certificate, process-local shard incarnation, and lifecycle revision.
FULL/EMPTY restore builds an off-side replacement. Revoke leaves the local shard explicitly
non-serving; only an explicit replacement reactivates it. A retained vnode advances to a newer
assignment fence without copying its state.

`publish_prepared_changes` validates one caller-supplied batch before publication: every live shard
must still match its prepared incarnation/revision/fence/state, every replacement must match its
destination, and all actions must share one exact predecessor and target fence. Retirement capacity
is reserved before an allocation-free, infallible publication loop. A stale later participant or a
mixed target therefore leaves every supplied shard unchanged. The helper does not know the
authoritative vnode roster; proving that the batch is complete remains the graph owner's job.

The artifact-v1 reference encoder is now private release code for this reference shard. The
outer-directory encoder remains test-only. No TidesDB package, generic backend trait, runtime graph
consumer, manifest selector, admission option, or source/sink guarantee was added.

## Review corrections

Independent correctness, maintainability, and documentation reviews initially requested changes.
The final implementation and plans now:

- reject same-version/different-certificate authority;
- bind prepared changes to a unique live shard incarnation and lifecycle revision;
- make revoked state non-serving and retained-vnode fence advancement explicit;
- preflight every supplied transition participant and require common exact fences before mutation;
- distinguish caller-supplied atomicity from authoritative-roster completeness;
- use direct names for limits, SUM inputs, and lifecycle revisions, and keep the large replacement
  behind an off-hot-path `Box`;
- enumerate all three temporary release dead-code allowances under **DKS-P1-001** with their
  distinct removal conditions; and
- keep the upstream TidesDB contribution exception separate from candidate qualification and any
  LaminarDB dependency.

All three final independent verdicts are **APPROVE**.

## Cycle review

- **AI slop — pass for this slice:** names and claims distinguish logical payload from resident
  memory, supplied participants from a complete roster, reference encoding from a production
  writer, and an upstream contribution from a qualified backend.
- **Overengineering/hot path — pass for this slice:** no generic working-state abstraction or disk
  adapter was added. Live state is a concrete `BTreeMap`; this is a correctness reference, not a
  latency claim. Replacement boxing, decode, sorting, and publication preparation are outside row
  mutation. A production hot path still needs measurement and a qualified bounded store.
- **Unused/dead code — approved with owned debt:** `artifact_v1.rs`, `vnode_partial/v2.rs`, and
  `managed_v1.rs` remain private release-dead code under **DKS-P1-001**. Before admission, the first
  two must gain the trusted manifest-selected restore consumer, while `managed_v1.rs` must gain a
  real graph/lifecycle consumer or be removed.
- **Production readiness — block:** there is no authoritative-roster graph integration, trusted
  manifest composition, bounded resident-memory model, structured operational error/health API,
  drop-only retired-image type, qualified TidesDB successor, portable restore through object store,
  source/state/sink delivery proof, exactly-once composition, measured tail latency, rebalance fault
  run, or independent release-binary soak.
- **Documentation — pass:** Cycle 68 remains preserved; Cycle 69 and later certification work are
  explicitly paused. ADR, plans, validation report, artifact format, and TidesDB design describe the
  implemented boundary without relaxing admission.
- **Tests — pass with one disclosed retry:** formatting, diff checks, focused codec/lifecycle,
  warnings-denied Clippy, exact fail-closed admission, and both full library feature matrices pass on
  Windows. The first no-default full run had one unrelated 60-second ASOF MV timing failure after
  1,301 passes; its first isolated rerun passed in 0.08 seconds and the next complete run passed.

## Verification record

| Command/suite | Result |
|---|---:|
| managed-v1 focused, `--no-default-features` | 15 passed |
| managed-v1 focused, `--features cluster` | 15 passed |
| artifact-v1 focused | 21 passed |
| VnodePartialV2 focused | 12 passed |
| exact cluster query-shape admission | 1 passed |
| Clippy `-D warnings`, no default features | passed |
| Clippy `-D warnings`, cluster | passed |
| full library, no default features, final run | 1,302 passed; 1 ignored |
| full library, cluster | 1,723 passed; 1 ignored |

## Next core cycle

Integrate this lifecycle shape with the graph's authoritative assignment roster. Stage and validate
the complete acquired/retained/revoked set in deterministic order, publish only under the exclusive
graph lifecycle fence, activate only after every participant is ready, and make any unexpected
post-mutation failure poison the whole graph generation. Keep `[LDB-4007]` closed. Backend
qualification and all paused certification/soak tooling remain outside that cycle.
