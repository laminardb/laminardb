# Distributed keyed state Cycle 36 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** exact v4 freeze-candidate evidence and a concise owner packet prepared;
  provider-neutral checkpoint and aggregate-v1 transition boundaries reconciled
- **Backend/candidate/provider executed:** no
- **Runtime backend, adapter, public state trait, schema, or admission change:** none
- **Bounded memory:** reference/conformance-only
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

The [Cycle 36 owner packet](../reports/distributed-state-cycle-36-owner-decision-packet-2026-07-25.md)
reduces the maintenance-health v2 contract to PF1-PF5 decisions. Exact v4 freeze-candidate bytes are
prepared and a separate read-only reconstruction confirmed their length, digest, three-change
semantic delta, line endings, and retained ineligibility. This is preparatory evidence only: named
formal reviewers and distinct authenticated workload/operations approval remain required.

The architecture now states that LaminarDB's provider-neutral object-store checkpoint/state handles
remain authoritative. TidesDB's FS/S3 remote mode has zero selection weight, supplies no Azure
path, and cannot replace LaminarDB's local/S3/GCS/Azure builder surface or its namespace proof,
sealed inventory, durable decision, and restore fencing. Provider parity is not required of a
disposable local working-state engine; a future remote working-state tier would require a separate
provider-neutral ADR.

The artifact contract also closes the failed-attempt numeric-gap edge: DELTA requires the
immediately preceding canonical sealed BODY or REFERENCE entry. A burned/unsealed predecessor forces
FULL/EMPTY for the first later changed capture. Frozen dirty generations release only after the
containing attempt has both a sealed inventory and durable terminal `CheckpointVerdict::Commit`;
seal followed by Abort cannot release them.

## Six-pass cycle review

### 1. AI slop, evidence, and consistency

**Pass after three independent read-only reviews and correction.** The reviews independently
reconstructed v4, audited Azure/provider authority against code, and checked the contract/owner
packet. Corrections removed stale active-redb language, reconciled proposal versus normative runner,
separated contract/mapping/plan threshold ownership, required production-minimal pressure and
failure visibility, distinguished preparatory evidence from formal PF4 receipts, and made both
independent completed review receipts inputs to final approval.

The checkpoint review caught two nontrivial ambiguities before implementation: DELTA after a sealed
REFERENCE is legal, while DELTA across an unsealed immediate predecessor is not; and inventory seal
alone cannot clear dirty state before the durable Commit decision. No placeholder or conflicting
authority remains in the reviewed Cycle 36 delta.

### 2. Overengineering, hot path, and latency

**Pass.** No generic storage facade, public runtime trait, backend adapter, remote tier, or
qualification implementation was added. The next slice is deliberately contract-first and limited
to a disconnected aggregate-v1 BTreeMap oracle. Production-minimal maintenance signals must use
bounded, allocation-free request-path updates, while gate-bearing reads remain off the event loop.
Remote object-store calls remain outside record-level and event-loop working-state paths.

### 3. Unused code and dependencies

**Pass.** The only Rust edit corrects existing object-store builder documentation to list the
already-supported `abfss://` scheme. No dependency, feature, module, API, schema, fixture consumer,
backend code, workflow, or cloud resource was added. The v4 JSON is intentionally unconsumable by
the current validator until final contract approval.

### 4. Production readiness, delivery, exactly once, and soak

**NO-GO, correctly fail-closed.** A worker-local engine still cannot establish vnode assignment,
restore-before-activate, old-owner fencing, aligned source/state/output cuts, coordinator recovery,
sink ambiguity handling, or end-to-end exactly once. Delivery remains certified per exact
source/operator/output/sink combination. Backend qualification and all Cycle 36 evidence remain
insufficient for production. Independent operators must still run the unchanged release artifact
through the separately approved soak before any production-ready claim.

### 5. Documentation and research hygiene

**Pass.** The owner packet is the single concise decision surface. The maintenance-health proposal
is explicitly rationale; the consolidated runner is the sole normative successor. The Cycle 21
record remains chronological but points to current state, and redb is consistently PARKED. The
TidesDB report remains relevant rejection/re-entry evidence and now closes the Azure authority leak.
No research document was removed because every retained dated record supports an active gate,
historical decision, rejected path, or reproducibility boundary.

### 6. Tests and empirical boundary

**Pass for this documentation/contract slice; no backend or product evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed; one explicitly ignored non-gating throughput/RSS observation.
- Exact v4 check: 7,838 bytes; SHA-256
  `94652d30153d998628d4e1d2b5da87bce59f5064192eeba9e9331f3f40507392`; UTF-8 without BOM; LF-only;
  trailing LF; JSON parse pass; independent exact-delta reconstruction pass.
- `git diff --check`, 155 tracked relative Markdown links, and exact `N01`-`N29` / `D01`-`D21`
  registry sets: pass.
- No candidate, backend, Docker/WSL workload, object-store provider API, cloud resource, cluster
  soak, or independent production soak ran.

## Cycle 37 entry boundary

Freeze a short normative aggregate-v1 journal/checkpoint-transition contract, then add only the
disconnected tool/test BTreeMap oracle and literal vectors listed in the Cycle 36 packet. Include
same-live retry, adjacent sealed BODY/REFERENCE DELTA, burned-predecessor FULL, abort-after-seal
retention, Commit-gated release, and deterministic vnode ordering. Do not add a public state trait,
runtime backend/dependency, adapter, candidate execution, manifest dispatch, restore installation,
or admission consumer. End Cycle 37 with these same six review passes.
