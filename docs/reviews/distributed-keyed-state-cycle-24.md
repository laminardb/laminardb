# Distributed keyed state Cycle 24 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle commit:** `24249a77`
- **Cycle outcome:** `REDB_RESULT_FINALIZATION_CONTRACT_FROZEN_IMPLEMENTATION_BLOCKED`
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; exact runner-v2 contract and implementation remain gated
- **Current product target:** local spill; backend not selected
- **Candidate construction or execution authorized:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 24 freezes the semantic result, cleanup and authority hierarchy in the
[redb 4.1.0 prescreen protocol](../testing/state-backend-redb-prescreen-v1.md). It adds no schema,
classifier, provider client, storage client, dispatcher, candidate dependency or execution path.

The native lifecycle now separates:

- the campaign-scoped `TERMINAL_CORRECTNESS_STOP_LATCHED` operational stop;
- the content-only `derived_outcome` (`PRESCREEN_PASS`, `PRESCREEN_NO_GO`, `DEFER`, or
  `REJECT_EXACT_PIN`);
- copied post-run review bindings, which remain authority-unverified;
- live native-run provenance, owner-review and immutable-storage capabilities; and
- the registry-held `FINAL_PRESCREEN_RESULT_SEALED` state.

Pre-run Docker and native dispatch capabilities are disjoint. Native dispatch additionally consumes
the verified Docker prerequisite. All dispatch/finalization capabilities and the sealed state remain
explicitly unconstructible until exact freshness, single-use, replay, TOCTOU, provider, storage and
registry rules are frozen. Review or storage failure after payload freeze blocks finalization; it
does not rewrite the derived candidate outcome.

The retained-evidence DAG is now acyclic. `R` is the closed pre-cleanup evidence set; `M` is the
durable evidence-close manifest; `J` is the crash-recoverable cleanup journal whose first frame binds
`M`; `C` is the cleanup report; and `F` is the final index over exactly `R + M + J + C`. The result
payload and post-run receipt follow `F`. The index summarizes but does not duplicate per-database
cleanup records. The manifest is only a content precondition; deletion can use only pre-authorized
supervisor-owned, handle-relative scratch-root authority.

Pre-classification retained accounting reserves the complete 256-KiB result and 64-KiB receipt caps:

```text
retained_artifact_bytes + actual_final_index_bytes + 256 KiB + 64 KiB <= 2 GiB
```

The later live storage verifier checks the exact actual closure. Docker uses a disjoint
`DOCKER_SMOKE_PASS`/`DOCKER_SMOKE_INCOMPLETE` no-decision union and can never emit a native outcome,
rejection or final seal. Its verified prerequisite must prove the protected Docker run consumed the
exact Docker dispatch capability; reviewed/stored content alone is insufficient.

The five-hour native watchdog now ends at local payload freeze. Its reordered allocation is 291.5
minutes with 8.5 minutes slack; human review, immutable publication, live revalidation and registry
finalization are asynchronous governance work and cannot extend or rerun the campaign.

The protocol's final SHA-256 is
`51be6f9b37bec7e115d58285d10e6753b11b364ccceaf588d09322f32d9f98f2`.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after three independent blocking reviews and repair.** The first draft incorrectly treated
owner review plus storage as sufficient to seal internally consistent fabricated bytes, let the
content classifier depend on an opaque Docker capability, conflated an early stop with a final seal,
and left a cleanup-index cycle. Reviews required live native-run provenance, an independent strict
reclassification over the stored closure, registry-held final state, disjoint Docker/native dispatch
capabilities, and the explicit `R -> M -> J -> C -> F -> payload -> receipt` dependency chain.

Follow-up reviews caught the wrong attempt-local latch scope, unbound cleanup-journal recovery,
ambiguous cleanup authority, stale pre-run `DEFER` wording, QR precedence conflict, unsafe live-review
ordering around a possible 2-GiB scan, and missing Docker dispatch provenance. All were corrected.
Final governance, protocol and adversarial reviews returned PASS. No trusted state can be created by
deserializing JSON, matching a hash, or printing a status line.

### 2. Overengineering, hot path and latency

**Pass for contract-only scope.** The cycle defines only the minimum authority and evidence joins
needed to prevent false results. It does not implement a generic PKI, ledger, storage abstraction,
provider plugin or cleanup framework. The finalization registry is a required future trust boundary,
not new infrastructure selected or built in this cycle.

No LaminarDB record, Arrow-batch, state, checkpoint, rebalance, source or sink path changed. The
2-GiB scan and review/storage calls are offline prescreen control-plane work, not runtime hot-path
operations. Candidate writer acquisition, commit and recovery latency remain unmeasured.

### 3. Unused code and dependencies

**Pass.** Cycle 24 changed no source file, Cargo manifest, lockfile, feature flag or dependency and
added no redb, Fjall, RocksDB or other backend. It introduced no result/index schema, classifier,
dispatcher, provider/storage client, process launch, deletion implementation, trusted-state
constructor or seal command. Older synthetic schemas remain regression inputs only.

The existing Cycle 22/23 bytes-only validators remain the only code implementing the formal
prescreen validation contract and still return only content-valid/ineligible/authorization-
unverified results. The separate Cycle 16 construction-only tool remains explicitly no-decision and
outside that formal result path.

### 4. Production readiness, delivery, exactly once and independent soak

**NO-GO, correctly fail-closed.** Cycle 24 executed no redb candidate, Docker/WSL smoke or native
workload. It produced no writer-rate, tail-latency, crash, recovery, resource, fault, endurance,
C1/C2/C3 or production evidence. No runtime backend has been selected.

LaminarDB still lacks vnode ownership epochs, checkpoint freeze/export/seal,
restore-before-activate, rebalance fencing and retention-safe cleanup. Cluster keyed aggregates,
windows and stateful joins therefore remain rejected by `[LDB-4007]`.

Exactly once still requires a compatible replayable source/offset cut, a sealed vnode-state cut,
recoverable coordinator decision, and a transactional or idempotent fenced sink. This cycle changes
no source or sink capability and cannot satisfy `[LDB-0013]`. An immutable release candidate must
still pass both the separately approved backend endurance campaign and an independently operated
production-like soak whose charter and duration remain unapproved. A redb prescreen result can never
substitute for either gate.

### 5. Documentation, stale research and overdocumentation

**Pass after one supersession repair.** The authority and cleanup rules remain in the existing redb
protocol rather than creating another ADR. The detail is justified by destructive cleanup,
cross-provider authority and hash-DAG invariants; executable wire details that are not yet decided
are listed as blockers instead of being invented.

The carry-forward matrix's stale Cycle 16 offline-signature direction contradicted the later
protected-provider decision. It now has a dated supersession note and points to the normative
protocol. The rest of that matrix and the exact-source/backend research remain relevant audit and
decision history, so no document was deleted merely to reduce file count. No Claude-memory claim was
treated as evidence.

### 6. Tests and empirical boundary

**Regression validation passes; candidate evidence remains absent.** In the isolated qualification
workspace, `cargo test --all-targets --all-features` reports 148 passed and one explicitly ignored
non-gating parser throughput/RSS observation. `cargo fmt --all -- --check`,
`cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check` pass.

These tests exercise the unchanged validation tooling only. They do not parse or implement the new
result/index contracts, run Docker, open redb, launch a candidate, dereference an indexed artifact,
delete a database, call a provider/store, derive a prescreen outcome or create a final seal.

## Cycle 25 entry boundary

Continue validation-only contract work without candidate construction or execution:

1. freeze the exact terminal-finding, raw-run-manifest and conditional evidence matrix before a
   result schema or classifier exists;
2. freeze the evidence-close manifest and cleanup journal/report wire, recovery, scratch-root,
   no-follow deletion, durability, absence and failure semantics before any deletion code exists;
3. freeze the retained-role registry, cardinalities and numeric caps, then independently review the
   complete result/index descriptor hierarchy; and
4. keep live provider, Docker/native run provenance, immutable storage and final registry integration
   separate and unconstructible until a security/operations review approves their exact contracts.

Do not add a runtime backend, candidate dependency, execution command, trusted dispatcher,
provider/storage integration, deletion path, backend selection, admission change or production claim.
