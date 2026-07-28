# Distributed keyed state — Core Cycle 7 review

- **Date:** 2026-07-28
- **Implementation commits:** `ec22dafc`, `2f3ed324`, `fa40c1f3`, `e477f622`,
  `512eda88`, `344e5815`, `67e6db24`, `6ec919ae`, `1c5f44ad`, `a353050b`,
  `74881645`, `39ca1b70`, `ea8c3c56`, `32c3f8f1`, `8c7a5ab1`, `46bad493`,
  `c443c91a`, `142dadf7`
- **Scope:** initialize managed operators before recovery, derive exact vnode/state participation
  from the live graph, recertify replacement owners from durable authority, and prevent assignment
  chaining until every exact participant reports installed vnode state
- **Slice verdict:** **PASS FOR DETERMINISTIC INITIALIZATION, RECOVERY AUTHORITY, AND READINESS
  GATES; POST-`142dadf7` ENGINEERING SOAK PASSED**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and implementation

Every runtime mode now plans and initializes declared managed aggregate state before checkpoint
recovery. Catalog-bridged and intermediate source schemas are registered before that planning
boundary. The graph caches the resulting managed-state capability, so row processing does not
repeat plan, schema, or codec classification.

The live graph is the participant authority. One ownership snapshot determines an exact capture
roster: global state participates only on vnode zero and vnode-keyed state participates on every
locally owned vnode. A required participant with no rows emits a named FULL payload whose decoded
state is empty. Restore rejects a missing or unexpected participant, a duplicate name within one
artifact link, and invalid global/keyed placement before invoking callbacks; repetition through a
valid FULL/delta lineage remains legal. Revoke receives only placement-relevant vnodes and a
stateful operator cannot inherit a successful default that discards state.

The raw `VnodePartial` rkyv layout is unchanged. The operator-graph checkpoint/state ABI advances
from 4 to 5 because the old graph encoding did not prove these semantics. Version 4 is rejected;
this project currently promises no API compatibility, and durable upgrade requires an explicit
state reset until a separately designed migration reader exists.

Replacement startup begins fenced, publishes no unowned registry state, announces the current
process, and lets the watcher/rebalancer derive an audited recovery successor from the durable
committed head. Assignment zero is only a pristine bootstrap sentinel. A stale-process drain
terminal is materialized without installing its predecessor-incarnation owner locally; a later pass
performs boot recovery. The obsolete server pre-Active direct-adoption loop was removed.

`CheckpointAssignmentAdoption` now carries `vnode_state_ready`. Transport activation accepts an
exact adoption while this flag is false, but successor assignment publication requires true reports
from every exact participant. Startup and recovery publish false before clearing or staging state.
True requires the exact registry fence and, for a bound pipeline, the exact installed-state binding;
fresh no-work startup installs that audited binding before launching the graph. Assignment-zero
bootstrap skips an impossible predecessor withdrawal. The adoption lock serializes local staging,
reporting, and publication preflight, and one absolute checkpoint deadline covers its waits, the
remote report scan, and object-store CAS.

The readiness scan is deliberately described as a best-current preflight, not an atomic distributed
lease. A remote participant can withdraw after the scan and before CAS; source drain plus forced-
checkpoint abort/recovery must contain that race. CAS timeout or error is outcome-unknown, so a
subsequent rebalance reloads the durable head before proceeding. A deterministic
withdrawal-between-scan-and-CAS fault test is still required. The watcher currently republishes its
bounded control-KV slot every two-second poll so a stale local cache cannot mask a direct false
write. That protects correctness, but its write/readback cost must be measured and safely coalesced
before production admission.

## Engineering soak evidence and fixes

The pre-fix three-node ALO hard-kill run failed and remains negative evidence:

- **server binary SHA-256:**
  `bef33cb62c397dc6486d062d09d4c0f5df808eb8124a11e986614d8629a41904`
- **logs:** `target/tmp/soak-942164-1785198920573674300`
- checkpoint 17 on assignment 2 was incorrectly rejected against acquisition-handoff checkpoint 2
  on assignment 1; recovery now accepts newer cuts within the exact installed assignment history;
- materialized assignment 3 pruned assignment 1, after which a recursive audit of assignment 2
  incorrectly required the lost predecessor; audits are now bounded to the retained predecessor and
  preserve the current assignment cut; and
- assignment 4 was published while node 1 still had unapplied assignment-3 state; assignment
  chaining now requires exact `vnode_state_ready` reports as described above.

The first attempt to exercise the resulting binary with a pinned SHA-256 prefix of `5ee865...` was
rejected by harness preflight after 0.09 seconds: Cargo had overwritten that executable with one
whose digest was
`91fe9d2c2ef734e1fad4f148da9e69a2eebed92057a74b70702b9b77fb490b10`. No server process or
runtime soak ran, so this is build-hygiene evidence, not a soak result.

A direct real run of the replacement binary produced a second failed engineering result:

- **server binary SHA-256:**
  `91fe9d2c2ef734e1fad4f148da9e69a2eebed92057a74b70702b9b77fb490b10`
- **logs:** `target/tmp/soak-989812-1785220503643922100`
- after round 1, the two survivors committed checkpoint 5 in 42.489 seconds, but the restarted
  ownerless node timed out after 71.907 seconds; and
- the logs support a specific, evidence-only cause: Release E forced the ownerless path back to an
  exact optional target E after the live owners had committed E+1. This repeated across transitions,
  including 24 to 25. `[LDB-6041]` correctly kept the inconsistent transition fail-closed.

Commit `142dadf7` changes only that role-dependent recovery rule: while still gated, an ownerless
process resolves the latest committed authority and clears its stale optional target; an active
owner continues to require its exact assigned cut. The coordinated-recovery suite passes 38/38,
including the strict stale-target sentinel. A deterministic forced E/E+1 interleaving remains open.

The post-`142dadf7` three-node ALO kill/rejoin engineering rerun **PASSED** in 339.97 seconds:

- **code:** `142dadf7`
- **server binary SHA-256:**
  `089197fa82d20cc9c83118036d8820d3168f6c3752125f05c6677ac6418f81d5`
- **harness SHA-256:**
  `b88df679bc4acc93739a4d4462bf208ac9c3b684bc58afc3f09a62a5d096b7b6`
- **logs:** `target/tmp/soak-996548-1785223178826043700`
- three kill/rejoin rounds covered leader node 2 and follower nodes 0 and 1. Their survivor/rejoin
  times were respectively 43.721/36.441, 37.089/30.020, and 34.586/31.053 seconds;
- end-of-steady-soak progress was checkpoint/epoch 170/170, and the frozen durable input cut was
  checkpoint/epoch 187/187;
- all 132,899 IDs were observed, with 7,898 permitted at-least-once duplicates; and
- at 400 records per second, all 514 measured stalls were at most 1,024 milliseconds, with no
  deadline or soak-SLO violation.

This pass does not reclassify either earlier failure and is engineering evidence, not production
certification. The independently operated immutable-release-candidate soak has **NOT RUN**. The
deterministic forced E/E+1 interleaving and passive-connector no-poll/no-write proof also remain
open.

## Stateful-join target

The managed-state target covers every persistent LaminarDB streaming join family: bounded interval,
two-sided incremental/changelog, ASOF, temporal probe, system-time temporal, replicated/full-
snapshot lookup, partial/on-demand lookup, and changelog-input plus static-reference enrichment.
ASOF and temporal probe each have two live cursors; temporal and lookup families have one live
cursor plus checkpoint-bound reference state. The bounded interval target includes watermark-final
`LEFT` as well as the existing append-only `INNER` family. Finite ad hoc ASOF and generic DataFusion
batch/fallback plans are not separate managed streaming-state families.

The target also includes deliberate finite `RIGHT`/`FULL`/left-right semi/anti, schema-frozen
natural, bounded cross, and pairwise multiway implementations. Current supported local stateful
joins that reach cluster admission remain rejected by `[LDB-4007]`. Other forms commonly fail
earlier in planning, although temporal/lookup translation can currently coerce some unsupported
join types into a candidate that reaches `[LDB-4007]`. Arbitrary dual-unbounded joins and correlated
apply remain fail-closed unless a bounded, checkpointable semantic contract is designed. No join
implementation or cluster admission changes in this cycle.

## End-of-cycle review

- **AI slop and overengineering:** pass for this runtime slice. No state backend, selector, generic
  storage façade, connector guarantee, feature flag, or admission bypass was added. Existing graph,
  assignment, and control-store authority carry the new invariants.
- **Hot path and latency:** pass for scope, not production. Classification is cached and all new
  state/readiness work is off the row path. The unconditional two-second control-KV publication is
  a measurable control-plane cost, not a free operation; safe coalescing and tail measurement remain
  open. A passive-connector test proving no readiness polling or writes also remains open. No
  p99/p99.9 claim is made.
- **Unused code and maintainability:** pass for touched paths, with `DKS-CLEANUP-001` open. Large DB,
  graph-transition, rebalance, soak, and test modules; stale rehydration terminology; test-only
  fault hooks; duplicate placement classification; and parked qualification prose still require
  human-oriented cleanup. `ManagedStateContract` remains a marker whose identity must be bound to
  validation or removed before more state classes are added.
- **Production readiness:** block. Prepare-all/abort plus infallible whole-graph publish, and one
  transition-wide reservation/deadline for transitive encoded bytes, object requests, decoder
  scratch, decoded RSS, and apply pause, are absent. Dual-live source/state/timer/output/sink cuts,
  one-live/reference cuts, bounded hot-key/asymmetric-join behavior, delivery compatibility, rolling
  format policy, and a qualified optional state backend also remain open.
- **Health and fault observability:** block. The required bounded-cardinality transition
  count/duration/apply-duration, active-transition timestamp/restoring-vnode count, restore/payload
  byte, and retention-lag/failure signals are not yet implemented. Readiness must close during an
  active/restoring/poisoned transition while liveness remains independent, and structured start and
  single terminal events must identify phase and disposition without keys or state bytes. Tests must
  prove one terminal outcome, abandoned-restoring detection, readiness composition, report
  withdrawal between scan and CAS, outcome-unknown CAS reconciliation, and no cardinality leak.
- **Join correctness:** block before migration. Local paths still need explicit rejection instead
  of join-type coercion, composite-key preservation/rejection, checked signed multiplicity,
  temporal-probe overflow without data loss, forward/nearest ASOF finality, versioned lookup
  recovery, and bounded interval-LEFT finalization/retraction tests.
- **Documentation:** pass after correction. Current behavior, target behavior, failed soak evidence,
  and certification status are separated. Living documents remain candidates for reduction rather
  than further certification-tooling expansion.
- **Tests:** the deterministic feature/integration matrix and post-`142dadf7` engineering soak are
  green below. A deterministic forced E/E+1 recovery interleaving is still required, the passive-
  connector no-poll/no-write proof remains open, and the independently operated immutable-RC soak
  has not run.

## Verification

| Check | Result |
|---|---:|
| full no-cluster `laminar-db` library suite | passed, 1,310; failed, 0; ignored, 1 profiling test |
| full cluster-feature `laminar-db` library suite | passed, 1,811; failed, 0; ignored, 1 profiling test |
| serialized cluster integration suite | passed, 23/23 |
| coordinated-recovery suite | passed, 38/38 |
| strict stale optional-target recovery sentinel | passed |
| embedded crash/restart recovery integration suite | passed, 3/3 |
| `laminar-server` binary suite without cluster | passed, 238/238 |
| `laminar-server` binary suite with cluster | passed, 316/316 |
| warnings-denied DB/server Clippy, cluster and no-cluster | passed, 4/4 configurations |
| exact `[LDB-0013]`, `[LDB-4007]`, inventory, and source/sink sentinels | passed, 6/6 |
| formatting, diff hygiene, and relative-link scan | passed; 106 local links resolved |
| pre-fix three-node ALO hard-kill engineering soak | **failed; retained as diagnosed negative evidence** |
| pinned `5ee865...` attempt | **preflight rejection in 0.09 s; no runtime soak occurred** |
| direct `91fe9d2c...` three-node ALO kill/rejoin engineering soak | **failed after round 1; retained as negative evidence** |
| post-`142dadf7` three-node ALO kill/rejoin engineering soak | **passed in 339.97 s; 3/3 kill/rejoin rounds, no deadline or SLO violation** |
| independent immutable-release-candidate soak | **not run; certification remains unavailable** |

## Next core slice

Retain the passed engineering-soak result without promoting it to certification. Add an abortable
prepared replacement for one real managed aggregate participant and an infallible whole-graph
publish step. Prove that corruption, a late participant failure, or an outcome-unknown CAS leaves
the transition recoverable and does not expose mixed graph state. Add transition-wide resource
reservations and the minimum health contract before any working-state backend or cluster admission.
Keep the API concrete enough for the listed join families; add no speculative backend abstraction
before a second real consumer proves the boundary.
