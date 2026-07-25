# Distributed-state Cycle 36 owner decision packet

- **Date:** 2026-07-25
- **Status:** review input; not an approval record
- **Scope:** maintenance-health v2 final-contract decisions and the next candidate-neutral state step
- **Backend selected or executed:** none
- **Runtime dependency, adapter, or admission change:** none
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

The v4 profile bytes and preparatory independent exact-delta evidence are ready for owner review. Four
contract decisions and the final protected two-owner approval remain open. No backend should be
chosen or implemented while those gates are open.

The working-state design is also not ready to become a public runtime trait. Its next safe slice is
a disconnected aggregate-v1 state-machine oracle that freezes mutation-journal and checkpoint-cut
semantics without selecting or exercising a storage engine.

## Final-contract decision matrix

| Gate | Prepared evidence or proposed decision | Owner action | Authority created now |
|---|---|---|---|
| PF1 exact v4 | Accept the reconstructed 7,838-byte freeze candidate and exact three-change delta | Accept or return with an exact correction | None until final approval |
| PF2 first observation | After setup persist/close/reopen and independent setup verification complete successfully, the first gate-bearing common bracket and its paired health bracket both complete before the first warmup mutation; both observations then remain uninterrupted through the resource-tail cut | Accept or replace with one unambiguous baseline rule | None |
| PF3 threshold ownership | Accept the four-way split below | Accept or identify a conflicting value owner | None |
| PF4 independent review | Two distinct reviewers inspect the same immutable contract/profile hashes: schema/evidence correctness and operations/hot-path correctness; both return immutable outcome receipts with no unresolved stop | Name reviewers, accept the procedure, then obtain both completed receipts | Review evidence only; no implementation or execution authority |
| PF5 final approval | Distinct authenticated workload and operations principals approve the same bytes through protected-provider receipts or a preapproved detached-signature process | Approve only after PF1-PF4 close | Validation-only implementation; no source, adapter, execution, selection, runtime, or admission authority |

The recommended PF3 split is exact:

| Artifact | Values it owns |
|---|---|
| runner contract | closed types, units, predicates, formula semantics, validity/failure rules, and wire bounds |
| v4 profile | retained common v3 numerical gates |
| candidate mapping | candidate-native limits, threshold basis, and safety margins |
| approved run plan | service/cadence/skew/tail/calibration/occupancy/observer-overhead values |

All exact values are bound before results. A result, repository field, or runner process cannot
approve itself or fill a missing threshold after observation.

## Exact v4 freeze-candidate evidence

The prepared
[linux-nvme-v4 freeze candidate](../../tools/state-backend-qual/profiles/linux-nvme-v4.freeze-candidate.json)
has:

- byte length `7838`;
- lowercase SHA-256 `94652d30153d998628d4e1d2b5da87bce59f5064192eeba9e9331f3f40507392`;
- UTF-8 without BOM, LF-only line endings, and one trailing LF; and
- exactly three decoded changes from v3: schema identity v3→v4, profile identity v3→v4, and removal
  of `background_maintenance_debt_max_bytes`.

Independent reconstruction from v3 produced identical bytes and zero residual semantic differences.
`notice=NOT QUALIFICATION EVIDENCE`, `status=candidate_unapproved`,
`qualification_eligible=false`, all owner/approval fields, and image/package fields remain
unchanged and ineligible. The existing validator is expected to reject v4 until final approval
authorizes validation-only implementation. This reconstruction is preparatory: the named PF4
schema/evidence reviewer must reproduce or explicitly accept it over the final immutable head.

## Production-minimal maintenance visibility

Every observed maintenance mechanism must expose production-minimal signals covering both backlog
or in-flight pressure and background failure. Tail-quiescence measurement may be
qualification-only. A qualification run samples the production-minimal surface plus any additional
qualification-only signals. This prevents a candidate from qualifying with health evidence that
operators cannot observe in production, while keeping expensive diagnostics off the hot path.

## Provider and checkpoint boundary

| Concern | Decision |
|---|---|
| Provider support | LaminarDB's Rust `object_store` builder supports local, S3, GCS, and Azure targets |
| Cluster checkpoint authority | A namespace-proof-admitted, genuinely cluster-shared store plus LaminarDB's exact-attempt inventory and seal; cloud targets provide the current shared paths, while `file://` remains embedded/test/node-durable unless separately proved shared |
| Worker-local spill backend | Disposable local latency/capacity mechanism; no native remote-object-store feature is required |
| TidesDB remote mode | Zero backend-selection weight and disabled for any local-only assessment; shipped filesystem/S3-compatible paths, no native GCS/Azure path, and no Rust `object_store` injection cannot replace the provider-neutral checkpoint layer; the low-level C callback seam would be new connector engineering |
| Future remote working-state tier | Separate ADR only after a local engine qualifies and measurements justify it; if required, provider neutrality including Azure becomes a hard gate |

TidesDB's native read cut, checkpoint, or remote SST/WAL/manifest set could only be a local
capture/restore optimization or input to LaminarDB's portable export. It cannot become vnode,
checkpoint, coordinator, rebalance, or exactly-once authority. This keeps object-store network I/O
off the record and event-loop hot paths.

## Candidate-neutral working-state readiness

The ADR describes the required behavior, and the C1 tool model validates a useful subset, but a
production `ManagedWorkingState` API would still leave these contracts ambiguous:

1. typed namespace/authority handles, stable operator/table identity, and logical-versus-physical
   key encoding;
2. atomicity scope across tables/vnodes, preconditions, and ambiguous-after-commit handling;
3. bounded scan continuation, direction/endpoints, and reservation ownership;
4. freeze-token/cut identity, journal coalescing, post-freeze isolation, abort/rearm, and release;
5. bounded streaming FULL/DELTA export plus shadow restore prepare/publish/abort/drop; and
6. memory/disk reservation lifetime and ownership.

Artifact v1 intentionally covers only append-only grouped `COUNT(*)`/`SUM(Int64)`. Timers, joins,
tombstones/deletes, window frontiers, manifest selection, and restore installation are not yet
encoded. A public trait now would freeze guesses and invite unused backend abstractions.

One checkpoint transition is now explicit: every allocated checkpoint ID is permanently burned; a
numeric gap may have no outcome, capture, or seal. When the immediately preceding attempt has no
admitted entry, including an outcome-less gap, unsealed attempt, or sealed-Abort attempt, the first
later changed capture emits FULL, or EMPTY for authoritative empty state; an unchanged nonempty
vnode may REFERENCE an older admitted nonempty BODY, while unchanged empty state emits EMPTY. A later
DELTA may name an immediately preceding admitted REFERENCE entry and resolution follows both edges.
Cycle 37 defines admitted as exact sealed inventory plus durable Commit; sealed-Abort state remains
retained but is not parent authority in initial managed v1.

## Smallest safe next implementation slice

First freeze a short normative aggregate-v1 journal/checkpoint-transition contract. Then add a
disconnected BTreeMap state-machine oracle nested under the existing `#[cfg(test)]` artifact tests.
Literal vectors
must prove:

- one batch reads a pre-mutation cut and publishes all writes atomically;
- repeated PUTs coalesce deterministically;
- freeze isolates later mutations;
- failed capture retains dirty generations;
- a lost materialization/upload response reuses the identical immutable cut without lifecycle retry;
- a normal adjacent admitted parent permits DELTA, including an admitted REFERENCE parent;
- a burned immediately preceding checkpoint ID, including a gap with no outcome, forces FULL for
  the first later changed state;
- unchanged state may REFERENCE an older admitted nonempty BODY;
- early release is rejected, and seal followed by terminal Abort does not release generations;
- pre-seal DecisionInDoubt blocks progress until observed Abort retains the generations, while an
  observed Commit still requires the exact seal;
- generations release only after the exact containing attempt has both a sealed inventory and the
  durable terminal `CheckpointVerdict::Commit` decision;
- per-vnode output ordering is deterministic and round-trips through existing test-only aggregate
  and `VnodePartialV2` encoders/readers.

This slice adds no public runtime trait, dependency, backend, adapter, candidate command, manifest
dispatch, restore installation, or admission consumer. It also does not execute Fjall, RocksDB,
redb, TidesDB, Docker/WSL, a provider API, or a cloud resource.

## Backend and production implications

| Subject | Current disposition |
|---|---|
| Fjall 3.1.8 | v4 comparison subject; stock scheduler/lifecycle/governance gaps remain; not runnable or selected |
| RocksDB 10.4.2 via rocksdb 0.24.0 | v4 comparison subject; source/binding/operability closure remains; not runnable or selected |
| redb 4.1.0 | PARKED after the bounded Cycle 34 design timebox; reopening needs a new micro-charter |
| TidesDB | current exact subject STOP; remote mode does not cure local correctness/integration vetoes |

A qualified worker-local spill backend is eventually required because bounded memory is
reference-only, but a backend is not required for this next semantic slice. Backend qualification
still cannot prove vnode ownership, rebalance, connector replay/commit/fencing, end-to-end exactly
once, or production readiness. Those remain independent integration gates, followed by the
separately operated immutable-release soak before any production claim.
