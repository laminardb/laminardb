# Phased plan: distributed keyed and stateful operators

- **Status:** Core reference implementation resumed; stock Fjall 3.1.8 is the preferred local-spill
  qualification-entry subject, but no backend is selected; production qualification/certification
  is paused and no new cluster operator is admitted
- **Date:** 2026-07-22
- **Last reconciled:** 2026-07-28 during Core Cycle 10
- **Decision:** [ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md)
- **Baseline evidence:** [validation report](../reports/cluster-keyed-state-validation-2026-07-22.md)
- **Phase 0 execution:** [file-level implementation plan](distributed-keyed-state-phase-0-execution.md)

## Objective

Admit distributed keyed aggregates, event-time windowed aggregates, and stateful joins one vertical
at a time without regressing LaminarDB's low-latency path or weakening its fail-closed guarantees.
The deliverable is production evidence—bounded resources, portable recovery, fenced rebalance,
fault correctness, and measured tail latency—not merely removal of `[LDB-4007]`.

The critical path is:

```text
0. contracts/evidence
        |
1. managed working state
        |
2. grouped aggregates
        |
3. event-time windows
        |
4. stateful joins
        |
5. materialized output (separate admission)
        |
6. production certification and gradual rollout
```

Performance, fault injection, compatibility, operational telemetry, and end-of-cycle review run
through every phase; they are not a final cleanup sprint.

### Core workstream reset

The owner reset is recorded normatively in
[ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#2026-07-27-workstream-reset).
Cycle 69 and later certification work are paused. Core Cycles 1–10 add a private reference shard and
a fail-closed graph containment path: exact owned/restoring vnode-roster and chain preflight,
deterministic callbacks, delayed activation, sticky poison after indeterminate mutation, boot-cut
validation, predecessor-authority repair, control-only completion while source intake is closed,
an audited committed final-owner exit that does not invent a target participant. Cycle 4 also
retains the exact capsule-validated head seal through boot/adoption, requires built-in backends to
read exact sealed partials under a per-artifact bound, and carries the full attempt through graph
preflight. Its corrective scope leaves the admitted global vnode-0 aggregate's raw rkyv
FULL/reference/DELTA wire unchanged and freezes a representative DELTA fixture. Cycle 5, recorded
in the [cycle review](../reviews/distributed-keyed-state-core-cycle-05.md), makes a checkpoint
attempt an immutable, never-reused name within its bound deployment/state namespace:
the seal inventory is create-once while live and retirement is irreversible. A parent attempt
therefore binds one exact seal and digest-checked body without a payload-wire change. The reader
also rejects more than this binary's current writer maximum of six physical artifacts per vnode.
This prevents transitive substitution for retained artifacts on the built-ins and conforming
backends, and bounds traversal for the current writer policy. It is not a durable-format proof:
external deletion of a live seal or a mixed/future writer policy remains a backend-qualification or
version-fencing blocker. Ancestor retention still determines availability.

Cycle 5 deliberately did not add `max_restore_encoded_bytes`: its seals exposed only head sizes, so
a reader-only cap could make a valid committed cut unrestorable. That constraint remains the reason
the later Cycle 9 reader containment cannot by itself authorize keyed admission.

Cycle 6, recorded in the [cycle review](../reviews/distributed-keyed-state-core-cycle-06.md), replaces the
split mutable staging maps with one immutable record binding predecessor/target assignment, process
incarnation, pipeline identity, exact committed restore cut, and acquired/revoked vnode rosters. It
also fences reuse of installed heap state to the exact Running graph and clears that authority on
poison or lifecycle failure. Cycle 7 initializes declared managed state before recovery in every
runtime mode and makes cached graph capabilities authoritative for per-vnode capture, restore, and
revoke. Each required participant emits a named FULL payload whose decoded state is empty when it
has no rows; missing, extra, duplicate names within one artifact link, and placement-invalid
participants fail before callbacks. Repetition across valid FULL/delta lineage links is expected.
Global state is scoped to vnode zero and keyed state to the exact owned roster. At Cycle 7 the raw
rkyv `VnodePartial` payload layout was unchanged, while graph checkpoint/state ABI 5 deliberately
rejected older ambiguous graph state. Cycle 9 later changes only the outer wrapper/seal as recorded
below.
Cycle 7 also makes `vnode_state_ready` a mandatory field in each exact assignment-adoption report.
A process publishes `false` under assignment serialization before staging or recovery and, for a
bound managed pipeline, publishes `true` only for the exact installed graph binding, assignment,
and pipeline. A no-coordinator process can report ready only after exact registry-fence validation
and proof that no transition is pending; assignment-zero bootstrap has no predecessor report to
withdraw.
Assignment chaining requires exact `true` reports from every predecessor participant. Lock
acquisition, local validation, the report scan, and durable CAS share one absolute checkpoint
deadline; an error or timeout after CAS begins is outcome-unknown and the next pass reconciles the
reloaded durable head. The wire change deliberately rejects old/missing-field reports and is not
rolling-compatible without a separately designed bridge.
Cycle 8, recorded in the
[cycle review](../reviews/distributed-keyed-state-core-cycle-08.md), connects the existing managed
SQL aggregate to a graph-owned prepare-all/abort-all/infallible-publish lifecycle. Complete chains
are decoded and validated into a mutation plan with private replacement collections before logical
state changes; prepublication failure aborts every attempted participant, although live-map
capacity reserved for allocation-free publication can remain; exact authority is revalidated while
the pending and installed-state locks are held; and displaced state is retired after those locks
are released. Cluster initialization also rejects cached `Rejected` capability or
post-initialization descriptor drift with `[LDB-4007]`.
Cycle 9, recorded in the
[cycle review](../reviews/distributed-keyed-state-core-cycle-09.md), begins the resource slice:
aggregate prepare reserves only checked net final live growth. The outer object wrapper advances to
`LDBVP3` version 3 with a fixed 164-byte header and seals advance to version 8; V2/seal-7 cuts
require an explicit reset until a migration bridge exists. Immutable immediate-parent/transitive
raw-payload and artifact attestations, requested-subset pre-GET validation, exact child-parent
arithmetic before parent-body I/O, reference-only accounting, and a staging receipt contain the
current legacy path. Its `max_partial_bytes * 6` envelope is accepted only as interim fail-closed
containment for current global vnode-0 fixtures; it neither proves every committed cut restorable
nor supplies the production keyed or multi-vnode budget.

Cycle 10, recorded in the
[cycle review](../reviews/distributed-keyed-state-core-cycle-10.md), closes the Commit-domain raw-
lineage authority gap for the current admitted profile. Participant readiness v6 attests exact
payload/ancestry limits. After validating that roster and before capsule persistence or Commit, the
leader walks every required parent seal to a root, checks each lineage extension, and recomputes the
cluster-wide payload/artifact totals stored in recovery capsule v6. Recovery repeats the same
metadata-only proof, requires the local runtime to derive the exact committed limits, and constrains its
acquired subset before body reads. A rejected post-seal attempt is no longer successor-parent
eligible. The contract is deliberately tagged `global_singleton_compatibility`; it is not keyed
admission or a memory reservation. A staged-byte or retained-depth configuration change across a
live cut requires reset/new namespace until a capability-superset rule exists.

Core Cycle 11 should make raw transition acquisition resource-owning rather than comparison-only:
derive the exact acquired subset, reserve its raw body/artifact budget for the lifetime of the
transition, bind request concurrency to those permits, cap retained in-flight response/spool bytes,
apply an absolute restore/acquisition deadline with cancellation, and prove release on publish,
abort, poison, and graph replacement. Encoded wrapper/seal metadata, decoder scratch and expansion,
decoded/live/prepared/retired RSS, and apply/retirement pause remain separate limits.
Bounded working-state storage remains later.
Backend work does not block these slices. The bounded stock-Fjall entry and no-fork boundary are
owned by the
[ADR amendment](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#2026-07-28-fjall-318-priority-amendment).

**DKS-CLEANUP-001 — final maintainability gate.** The distributed keyed-state feature maintainer
owns this gate. Low-risk cleanup lands as each core slice stabilizes; the final sweep runs after the
core lifecycle and backend implementation are functionally complete and before any stateful
admission. The independent soak must exercise the cleaned release candidate, so the final sweep
also precedes that soak and any production-readiness claim. Its ordered inventory is:

1. remove the parked redb prescreen/qualification lane, required-CI dependency, and oversized
   parked protocol document, retaining only a short alternatives-considered decision;
2. remove or explicitly product-own the excluded `distributed-state-ab` fake observer and the
   certification-only collectors mixed into `cluster_soak.rs`; preserve the independent soak as a
   later release gate, not production helper code;
3. decide the checkpoint timing ledger, diagnostic credential, and HTTP evidence routes as one
   operational product surface—either give the whole slice stable security/operations ownership or
   remove it;
4. wire into a real runtime consumer or delete the release-dead `artifact_v1`, `managed_v1`,
   `vnode_partial/v2`, and unused partition-schema models before admission;
5. make the operator capability inventory authoritative or remove its duplicate SQL classification;
6. rename rehydration/staging and dry-run/legacy APIs by actual semantics, narrow test-only public
   hooks, delete zero-consumer façade methods, and split DB assignment, graph transition, soak, and
   their large test modules by coherent runtime/test ownership rather than line count;
7. remove or isolate debug filesystem fault-injection polling (`checkpoint_kill_gate` and
   `LAMINAR_FAULT_INJECT_TRIGGER_FILE`) behind an explicit test-only feature, then benchmark the
   empty transition path and cache shuffle routing codecs/schema work before changing hot-path
   locking or allocation;
8. revalidate generated research against current primary sources and the accepted ADRs, then delete
   superseded, irrelevant, or duplicated research/cycle prose instead of maintaining it as product
   documentation; and
9. finish with compiler/Clippy feature matrices plus reachability, public-API, naming, module-size,
   and dependency scans; every retained helper/config/model must have a human-identifiable owner and
   runtime, conformance, test, or operations consumer.

**DKS-OPS-001 — minimum maintenance and error signals before admission.** Keep labels bounded;
checkpoint IDs, assignment versions, vnode/operator names, digests, SQL, and error text belong in
structured logs, not metric labels.

| Signal | Required semantics |
|---|---|
| `vnode_transitions_total{kind,outcome}` | One terminal increment per startup, rebalance, or final-exit attempt; outcomes are `completed`, `failed_before_publish`, or `recovery_required` |
| `vnode_transition_duration_seconds{kind,outcome}` | End-to-end durable read through publication latency |
| `vnode_transition_apply_duration_seconds{outcome}` | Only graph preflight/callback/publication time while normal execution is withheld; the product latency budget must set the p99/p99.9 gate |
| `vnode_transition_started_timestamp_seconds` and `vnode_restoring_vnodes` | Zero timestamp when idle and an atomic locally owned restoring count, sufficient to detect an abandoned transition without hot-path polling |
| `vnode_restore_bytes_total` and `vnode_checkpoint_payload_bytes_total` | Physical verified bytes read and partial payload bytes written; retries count again |
| `vnode_retention_lag_epochs` and `vnode_retention_failures_total{component,reason}` | Difference between authorized and applied prune horizons, with bounded component/reason categories |
| `vnode_state_ready` and `vnode_state_readiness_publications_total{value,outcome}` | Local exact-binding readiness plus bounded success/failure/timeout accounting for the mandatory control report; `value` is only `true` or `false` |
| `assignment_rotation_blocked_total{reason}` | One increment per deferred rotation attempt, with bounded reasons such as `local_state_not_ready` and `participant_state_not_ready` and no participant/vnode labels |
| `assignment_publication_outcome_unknown_total` and `assignment_head_reconciliations_total{outcome}` | Expose ambiguous durable CAS outcomes; bounded reconciliation outcomes distinguish `predecessor_retained`, `materialized_adopted`, `materialized_aborted`, and `failed` |

Liveness stays independent. Readiness closes while a managed transition is active, any locally
owned vnode is `Restoring`, or graph state is poisoned/recovery-required. Emit one structured start
and terminal event with stable code, phase/disposition, assignment version, full checkpoint attempt,
vnode count, verified bytes, and duration; never log keys or state bytes. An increase in
`recovery_required`, or owned `Restoring` vnodes without an active transition, is immediately
critical. Repeated pre-publish failure and retention lag use deployment-specific time windows; no
universal apply-pause SLO is invented before the latency budget is approved. The current
`checkpoint_size_bytes` excludes separately written vnode partials and must be corrected or renamed
before it can support this operator family.

Completion requires both feature matrices, warnings-denied Clippy, focused fault tests, dead/public
API search, documentation-link checks, latency/resource evidence for touched hot paths, and
independent human review for AI slop, overengineering, unused code, production readiness,
documentation, and tests. Git preserves discarded experiments; maintained documentation keeps only
current decisions and evidence. This gate does not resume paused certification or soak execution.

The Cycle 20 [working-state placement analysis](../reports/state-working-state-options-2026-07-24.md)
separates the capability from a named engine but does not change sequencing authority. Phase 1
remains blocked by the existing Phase 0 review gate. Any later gate split requires an accepted ADR/
plan amendment with named scope and owners. The intended broad/variable-state production profile
  still waits for stock official Fjall 3.1.8 to pass bounded integration and absolute qualification.
  Source review finds the required KV primitive shapes, while concurrent/crash semantics,
  unenforced global write-buffer configuration, soft journal limits, synchronous stalls, incomplete
  maintenance visibility, prefix cleanup and tail/fault evidence remain open. Backend-neutral
  Laminar lifecycle, publication-boundary,
checkpoint, resource-admission and health-composition work may continue. Candidate priority is not
runtime authorization or production admission. The
  project decision keeps bounded memory as a reference/conformance implementation
  only; it has no cluster product schedule or production-soak matrix under this plan.

Cycle 42 completes one backend-neutral, runtime-consumed correction: a normal incremental
aggregate error after state application may have begun is recovery-required, and the coordinator
does not publish that cycle or start a due checkpoint. It does not add managed working state,
qualify a backend, or satisfy the future native-root poison/fresh-restore contract.

Cycle 43 makes analytic frame-history advancement transactional with its residual projection. The
candidate tail is installed only after projection succeeds, so an ordinary projection failure or
cancellation remains failed-before-apply and cannot double-append on replay. This changes neither
the cluster rejection nor any backend gate.

Cycle 44 makes returned ASOF failures replay-safe: ingest errors remain pre-apply; join/projection
errors become recovery-required only after current-cycle right state or schema changes; and a
returned eviction error is recovery-required after pruning begins. Panic/cancellation poisoning and
empty-buffer right-schema checkpoint/restore remain open. No cluster capability is admitted.

Cycle 45 versions the ASOF operator checkpoint without changing its v1 buffer body. A bounded schema
appendix preserves learned right-side shape when no rows remain; non-empty v1 checkpoints migrate by
deriving that shape, while ambiguous empty v1 LEFT checkpoints fail recovery-closed. Restore also
checks buffer/index/schema coherence before one atomic install, and live right-schema drift fails
before ingest. This does not solve cancellation/panic poisoning, distribution, rebalance, delivery,
backend qualification, or independent soak, and admits no cluster capability.

Cycle 46 closes the retained in-memory graph-generation ambiguity without adding a backend. A
deterministic ASOF test proved that dropping a borrowed graph future after right-state mutation and
downstream routing previously left the same graph checkpointable. A cycle-level cancellation guard
now permanently fences that generation on drop/unwind; execute, drain, whole-state capture, and
per-vnode capture all require a fresh graph restored from the last committed cut. Explicit returned
errors retain their existing recovery/halt classification, and cancellation while waiting for the
cluster rotation fence does not poison because no input/state has been admitted. Current production
already awaits graph passes non-preemptively and destroys the graph on compute-root panic, so this is
a direct-API/future-refactor invariant rather than evidence of a formerly reachable normal shutdown
fault. It adds one atomic check and one `Arc` clone/drop per graph cycle, not per row/operator. A
future cancellable checkpoint-delivery path and any native operation that can outlive the graph still
need their own owner-level poison/publication fence. Distribution, rebalance, source/sink delivery,
Fjall integration/qualification, cluster admission, and the independent soak remain open.

Cycle 47 closes the audit—not the implementation—of post-graph drain publication cancellation. A
one-slot real sink-actor probe reproduced an unclassified retained-callback drop after graph input
was consumed and while the third output batch awaited enqueue. No production owner cancels and
retains this future today. Supported deadline/lease interruption regains control: publication in
replay-required and cluster modes returns recovery, while local best-effort enqueue loss records a
sink timeout whose subsequent fence blocks capture: `Skipped` after successful FIFO sync, otherwise
`Failed`, while overall drain-deadline expiry returns recovery. A checkpoint-only guard would miss
the equivalent normal-cycle publication boundary and would not own source barriers or exact-attempt
cleanup, so it is not added. The callback contract now requires an explicit drain result or
destruction of the complete callback/coordinator generation. If a cancellable caller is introduced,
its prerequisite is a coordinator-owned attempt transaction over input-cut ownership, graph/MV/
stream/sink publication, offsets/barriers, and attempt cleanup. This adds no hot-path cost and makes
no exactly-once or native-backend cancellation claim.

Cycle 48 fixes a distinct live cut error. Staged vnode acquire/revoke work and a registry assignment
not yet observed by graph execution now force checkpoint drain and block both snapshot APIs.
Leader/source-less and immediate/deferred follower capture paths reuse the assignment rotation
read token from after sink FIFO fencing, through shuffle alignment and both whole/vnode mutable
images, with assignment-certificate checks at acquisition, post-alignment, and post-capture. The
token ends before encoding, durable tail I/O, or awaited cleanup. This closes assignment publication
between final quiescence and capture without adding row-path work or a second lock. Multiple
admitted global-aggregate queries also proved the former sequential vnode-0 partial apply was
structurally reachable; at Cycle 48 an explicit apply error forced whole-generation recovery and no
output/cut. Core Cycle 8 replaces production `SqlAggregateV1` application with roster-complete
off-side prepare-all/abort-all and authority-fenced publication. The legacy callback path remains
test-only. Cluster admission, at-least-once delivery, backend status, and source/sink capability
remain unchanged.

Cycle 49 completes the local follower and anomalous-contention probes. Both immediate and deferred
follower routes hold the rotation read token across whole/vnode mutable capture and release it before
their durable tails while preserving exact pending-attempt cleanup. The held-permit probe exposed a
latency inversion: a retained non-abortable encoder could make capture wait for the checkpoint
timeout under the token while an assignment writer had a shorter deadline. A contending capture now
uses a semaphore try-acquire and returns `[LDB-6017]` before state mutation; the original encoder
retains its permit and fault lifecycle. Removing the redundant async timeout wrappers does not make
synchronous snapshots preemptible, which was already the case. There is no row-path change, new
backend, or delivery widening. The independent soak charter now names exact source-less/sourceful
leader/follower rotation scenarios and latency distributions, but no qualifying soak has run.

Cycle 50 closes the output/evidence inventory without implementing it. Current Kafka output has no
replay-stable operation ID, Laminar provenance, checked admission sequence, transactional writer
fence, or successor marker bound to an exact recovery-base capsule. Supported HTTP and metrics
expose durable assignment publication and aggregate timing, but not each process's locally adopted
assignment, immutable attempt history,
exact attempt maxima, or deadline-exhaustion counts. Private object-store outcomes, capsules,
adoption reports, and process leases remain the internal authority; the production contract needs
small versioned projections rather than coupling an independent controller to private paths or text
logs. Delivery work starts with executable oracle semantics and byte/state-machine tests, then real
broker fencing, then engineering integration. Per-record headers stay limited to version/kind,
operation ID, interval ID, and sequence; interval-wide provenance lives in the referenced marker,
while the reader hashes payload bytes and derives vnode ownership from the canonical key/ABI. This
evidence gap is independent of the local-state backend gate and does not justify relaxing
`[LDB-4007]` or claiming exactly-once.

The first writer interval requires the same proof: resolve exact numeric source-start baselines
without delivering records, commit a zero-input bootstrap checkpoint/capsule with empty state and
the current pipeline/assignment identity, then confirm a `predecessor = none` marker before opening
readiness or data admission. Only that proved-empty bootstrap may bypass an unactivated sink flush.
Sources unable to expose a checkpointable pre-delivery baseline remain closed, and bootstrap/marker
time is measured as startup/RTO latency.

Cycle 51 completes the first executable-oracle slice without touching production. Standalone
fixture v2 freezes semantic relationships among exact source/sink inventories, bootstrap and
recovery cuts, historical assignment authority, writer intervals, cross-partition successor
markers, admission sequences, raw payload bytes, and independently derived vnode/shard ownership.
It permits byte-identical cross-interval replay only when the raw causal source offset is at or after
the resolved exclusive cut, while versions must rise within one interval. Evidence selected from a
wrong run or missing a required cut is `RUN_INVALID`; complete Kafka-shaped evidence proving a bad
marker, owner, replay, payload, or result is `PRODUCT_FAIL`. Assignment/process evidence is
synthetically reconciled. Cycle 52 then freezes and hostile-tests a minimal standalone binary
representation: a fixed 66-byte per-record header and a bounded marker whose common provenance is
not repeated per row. V2 consumes those exact bytes, but no runtime or Kafka connector does. The
sole normative byte table is
[ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#distributed-output-envelope-v1).
Cycle 53 then freezes one sink-scoped grouped `COUNT(*)`/`SUM(Int64)` operation identity, derives
fixture IDs from explicit ABI-v1 group bytes and checked count, and projects marker/data inputs only
after pure current-assignment, live-process, and committed-recovery checks. Pipeline incarnation and
writer intervals remain opaque inputs without a production lifecycle. These cycles close no
public-evidence, Kafka-fencing, backend, admission, exactly-once, latency, or production-soak gate.
Cycle 54 adds only the
[validation-only transactional writer model](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#validation-only-transactional-writer-model-v1):
confirmed marker-before-data, bounded explicit transaction slices, checked global sequence ranges,
terminal ambiguity, and confirmed-predecessor replay are executable without a broker. It explicitly
defers complete partition-inventory proof, durable interval non-reuse, ambiguous-marker
reconciliation, and real fencing.

Cycle 55 then freezes the 74-byte stable Kafka
[`transactional_id_v1`](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#kafka-transactional-identity-v1-and-real-broker-evidence-boundary)
and runs the smallest deterministic real-broker slice. Repeated clean runs, including an optimized
standalone binary, prove same-ID fatal predecessor fencing, confirmed-abort invisibility plus
byte-identical retry, fence-aborted open-transaction invisibility, exact marker fanout over a
synthetic three-partition inventory, and reserved/unrelated header and key/payload preservation on
the exact one-node Redpanda subject. This is only partial item-5 evidence. An ambiguous `EndTxn`
outcome needs a protocol-aware matched-response-loss actuator; generic timeouts or broker kills are
not accepted. Runtime connector wiring, durable interval allocation/non-reuse, production broker
limits, multi-broker durability, hot-path/latency evidence, source/state/sink atomicity, and the
independent soak remain absent.

Cycle 56 closes only that controlled ambiguity subset. A validation-only Rust actuator asserts the
exact pinned `EndTxn` v1 request and same-connection response, then deliberately proves either a
broker-accepted commit whose complete success response is withheld or a complete request held with
zero upstream bytes. Four isolated marker/data cases retire every target-client connection before
same-ID successor fencing and reconcile separate `read_uncommitted`/`read_committed` captures to
frozen per-partition cuts. The marker candidate and predecessor data are `read_committed`-visible
only in the applied cases while `read_uncommitted` sees both staged branches; replay under the
successor interval is `read_committed`-visible in both data cases. This is evidence
for one RF=1 Redpanda/PLAINTEXT/client-version subject, not runtime code or an exactly-once claim.
It leaves durable interval non-reuse, runtime integration, production broker limits/security,
replicated failover, source/state/sink atomicity, latency, backend qualification, and independent
soak unresolved.

Cycle 57 closes only the stable-serving local-adoption evidence portion of the public-observability
gap. Its authenticated, 4-KiB-capped, cache-disabled endpoint uses one bounded checked-KV operation
to read this stable-node slot's current-boot durable adoption, requires it to match the locally
audited assignment fence, then
rechecks that fence and the live node/boot/process-term identity. The existing three-node engineering
harness sandwiches every expected live assignment-participant sample between durable assignment
reads and requires old-boot removal, same-node new-boot adoption, and a higher process term across
hard kill/rejoin. It cannot report an exact current recovery phase or committed-`Release`
consumption because neither is retained. This adds no row/checkpoint hot-path work, state backend,
admission, delivery guarantee, qualifying latency result, or independent production-soak evidence.

Both Cycle 57 Windows/WSL2 engineering commands passed the new exact local convergence and
kill/rejoin assertions but failed their encompassing terminal profile gate. The substantive rerun
recorded 98.81% rather than 99.00% of one node's checkpoint stalls within 1024 ms. The miss is kept
as NO-GO evidence; aggregate metrics cannot identify its exact attempts or establish cause.

The [Cycle 58 checkpoint-attempt evidence audit](../reports/checkpoint-attempt-evidence-audit-2026-07-26.md)
timeboxed the gap without adding an endpoint. Cycle 59 implements the bounded three-family local
barrier-pause ledger and protected existing-harness consumer. Its exact process binding, sampled-
converged-version certificate binding, loss detection, fixed-memory diagnostics, streamed artifacts
through each coherent observed cut, and Prometheus reconciliation passed a one-kill engineering run
at `7782a032` over 392 records. The engineering oracle tolerated and counted 2,758 duplicate output
IDs but did not verify byte identity or sealed-cut replay legality, so it proves neither charter-
level at-least-once duplicate legality nor exactly-once composition. The post-run `1a6dff80`
defense has focused deterministic coverage only. Instrumentation A/B, exact full-checkpoint/
restorable-gate evidence, and a read-only same-snapshot durable audit remain separate blockers;
this is not the independent immutable release-binary soak.

Cycle 60 adds a test-target-only three-attempt proof for coherent-cut retries and freezes the
[engineering A/B v1](../testing/distributed-state-production-soak-charter.md#cycle-60-instrumentation-ab-protocol).
The protocol uses named recorder/polling contrasts, common metrics, fixed input/time/fault anchors,
and balanced temporal blocks. No A/B ran; the coupled soak harness is not its common driver and v1
is effect-estimation-only. The direct nonempty HTTP route test remains an explicit low-risk
composition gap rather than justification for a seeding API or duplicate live-cluster unit fixture.

Cycle 61 adds only the [chartered prebuilt-executable binding](../testing/distributed-state-production-soak-charter.md#cycle-61-executable-binding-seam)
to the ignored engineering harness. No real-process run or A/B occurred; the coupled target remains
ineligible as the common driver and cannot replace the independent release-binary soak.

Cycle 62 adds only the [standalone schedule scaffold](../testing/distributed-state-production-soak-charter.md#cycle-62-schedule-scaffold).
It proves, across C/D and four observer outcomes, that the reviewed driver materializes identical
plan/trace bytes before interpreting observer status or bounded output. It performs no HTTP,
workload, fault, checkpoint, latency measurement, A/B, or soak. Before live polling, design and
review a route-scoped read-only credential or a content-bound GET-only broker; the current console
bearer also authorizes checkpoint and pipeline control. Then add a loopback fake-server state
machine for exact method/path/origin, deadline, retry, page, cursor, identity, and response-bound
behavior before any real cluster is contacted.

Cycle 63 chooses the [server-enforced diagnostic-read credential](../testing/distributed-state-production-soak-charter.md#cycle-63-diagnostic-read-authority-decision)
and rejects the bearer-holding broker. The credential is disjoint from the console bearer,
startup-bound, loopback-only, and valid solely for exact `GET` access to the two local evidence
routes. The next bounded cycle first removes substituted TOML input from parse errors and fixes both
reload commit paths so restart-only server authority cannot change, then adds the split auth/router
boundary and its route matrix. It does not add the observer or run the experiment. The fake-server
observer protocol, live effect-estimation, powered equivalence, and independent release-binary soak
remain ordered gates after that control-plane work. The main HTTP port is also the advertised
checkpoint-RPC port, so the loopback slice is single-host engineering support only; multi-host and
production-soak use need a later separate local listener or native TLS/mTLS design.

Cycle 64 completes only that prerequisite boundary. `server.diagnostic_read_token` is validated at
file load and programmatic startup, snapshotted with console authority, and accepted only on the two
diagnostic GETs outside console CORS. Fixed post-auth concurrency/rate/deadline bounds and matched-
route logging contain the surface. Parse-error input is detached, and both reload paths retain all
restart-only active configuration while committing only successful source/lookup/pipeline/sink
changes. The no-cluster and cluster server matrices pass (238/238 and 316/316), including the
credential/alias/method matrix and reload success/failure paths. No observer or real-process HTTP
run exists yet. A loopback fake-server observer protocol was then the next bounded gate before the
certification workstream was suspended; multi-host transport, effect-estimation, powered
equivalence, and the independent release soak remain open.

Cycle 65 completes only that root-workspace-excluded loopback fake-protocol component. Typed stdin
bootstrap, exact bounded HTTP parsing, restart/cursor/assignment validation, explicit
incompleteness, cancellation, zero C sockets, and 348 successful D fake responses pass in unit and
real-child tests. No LaminarDB process or workload was used, and configured loopback alone is not
fake-process attestation. The sealed common driver has not yet been upgraded to launch and consume
the network-mode observer; that fake-only non-feedback proof was then the next gate before this
workstream was suspended. Runtime admission,
Fjall integration/qualification, source/state/sink delivery, multi-host security, latency A/B, and the
independent release soak remain open.

The fake path accelerates all 58 logical slots and does not validate the 0..285-second cadence or
the live server's per-process start limiter. A separately versioned paced integration contract is
therefore required before any live request. Restart detection covers only unread records already
advertised by the last observed page; an unobserved process-local tail requires durable
continuity/handoff or an explicitly reviewed bounded observation interpretation before live use.

Cycle 66 closes only the next fake-only gate. The sealed driver now provisions and supervises the
manifest-pinned child, binds each canonical result to a fresh invocation, and consumes it only after
the identical common trace has been file-synced and end-sealed. Bounded hostile child/input/output
tests pass without persisting raw streams. The accelerated 58-slot path remains ineligible for live
or A/B use; a separately versioned paced contract and an honest timing-tail decision are next.

Cycle 67 freezes those two decisions and still performs no live work. Paced v1 uses an acknowledged
monotonic start bound, absolute five-second targets, three parallel persistent node lanes, no late
catch-up or overlapping slot, a cross-slot client rolling limiter below the server's allowance, and
a complete slot/transcript result distinct from fake result v3. It adopts observed process-local
timing prefixes with explicit unsealed restart gaps; it does not add checkpoint-path persistence or
claim an unknown old-process tail is complete. A future exact option requires a fenced durable
intent/terminal/generation-seal journal and its own checkpoint-latency/outage decision.

Cycle 68 completes the first, deliberately smaller owned-fake slice in `1b6a06ed`: externally bound
canonical fixture plans, distinct fixture descriptor identities, fixed nonce-bound START/ACK,
anchor-after-decode ordering, absolute timing classifiers, and atomic cross-slot rate admission. Its
`paced-owned-fake-*` names cannot be relabelled as live readiness, and it has no executable, network,
lane, result, transcript, or timing-coverage path.

The following certification sequence is suspended. If explicitly resumed, its next owned-fake gate
would add a separately versioned bounded
frame stream, complete 174-node-slot result validation, append-only transcript validation, and
open/unsealed timing-generation classification without a CLI or socket. Then extract delivery-stage
HTTP behavior with accelerated-v3 regression coverage; implement three persistent lanes and
supervisor spooling; prove ambiguous post-delivery quarantine and the actual limiter; and run the
manual 290-second C/D pair. Only after those gates may work prove launcher-prebound socket adoption,
trusted release-process descriptors, and nonce-bound v2 responses in a non-measurement loopback
preflight before any A/B. Multi-host work remains a
separate diagnostics-only TLS 1.3 mTLS listener with signed node-specific roster/identity and
attempt-bound credentials; it cannot reuse current cluster mTLS as HTTP evidence. Live A/B,
powered equivalence, Fjall integration/qualification, runtime admission, delivery/exactly-once work, and the
independent release-binary soak remain separate later gates.

## Scope and non-goals

In scope:

- fixed-vnode managed working state on local memory/NVMe;
- portable full/delta checkpoint artifacts in shared storage;
- assignment-fenced acquire, restore, activate, revoke, and cleanup;
- positive physical-plan capability descriptors;
- grouped built-in aggregates, event-time windows, every persistent streaming-state physical join
  family, and finite well-defined outer/existence, natural, cross, and multiway additions, admitted
  independently only after each semantic and delivery contract passes;
- truthful byte/disk/timer accounting and controlled pressure behavior;
- local-vs-cluster differential, fault, rescale, and latency certification;
- source/operator/sink delivery compatibility and checkpoint-tail certification; and
- a separate distributed materialized-output lifecycle.

Not in the initial program:

- a new consensus service, Raft implementation, or control-plane rewrite;
- object-store-primary live state, a Hummock/Persist clone, or per-record remote state calls;
- unaligned checkpoints, dual-write migration, or Megaphone-style routing before measurements;
- cluster exactly-once certification, checkpoint-coupled transactional sink commits, or guarantee
  widening; bounded Kafka transactions used only for externally auditable writer fencing remain
  required by the initial at-least-once scenario;
- arbitrary UDAFs, unbounded joins, implicit semantic TTL, or best-effort state eviction;
- one database/file per vnode; or
- a new crate or generic framework before two concrete consumers demonstrate the boundary.

## Preconditions

The following are blocking inputs rather than tasks to hand-wave inside an operator phase:

1. The existing cluster at-least-once checkpoint, exact-attempt seal, assignment fence, and recovery
   capsule must pass their own deterministic fault gates. Stateful admission cannot mask a known
   durability-authority defect.
2. Cluster sources used by a certified scenario must be non-ephemeral, `Splittable`, and have a
   supported assignment-scoped checkpoint/handoff contract. At this baseline, Kafka is the only
   built-in external source that qualifies. A stateful operator does not make a singleton or
   consumer-group-only source safe.
3. A certified external-output scenario needs a `DurableAtLeastOnce + MultiWriter` sink that accepts
   the operator's output mode. A connector mismatch remains fail-closed before I/O.
4. The deployment's vnode count and partitioning ABI are immutable and present in pipeline identity.
5. Shared object storage remains required recovery authority; local state storage is configured and
   quota-controlled but may be lost completely.
6. Numerical production workload and latency/recovery targets are checked in during Phase 0.

## Admission progression

| Milestone | Newly eligible `CREATE STREAM` shapes | Still fail-closed |
|---|---|---|
| Current | Stateless projection/filter; one direct global aggregate stage | Every implemented local group/window/join candidate and all MVs; unsupported temporal/lookup forms may be coerced and reach `[LDB-4007]`, while other unsupported forms can fail earlier in planning |
| Phase 2 first gate | Append-only grouped `COUNT(*)` plus `SUM(Int64)` | All broader aggregates, retractions, windows/joins |
| Later Phase 2 gates | Reviewed broader `COUNT`/`SUM`, `AVG`, append-only `MIN`/`MAX` | `DISTINCT`, arbitrary UDAF, changelog min/max, windows/joins |
| Phase 3A/B | Certified tumbling then hopping event-time aggregates | Sessions, processing-time/custom triggers, analytic frames, joins |
| Phase 3C/D | Certified session windows and bounded analytic frames | Unbounded frames and uncertified trigger modes |
| Phase 4A | No cluster admission; harden existing local join planning/correctness | Every implemented local cluster join candidate; other forms remain planner-rejected |
| Phase 4B | Append-only bounded interval `INNER`, then finite bounded interval `LEFT` under separate admission | Incremental/changelog, `RIGHT`/`FULL`/semi/anti, ASOF, temporal/probe, lookup, and unbounded joins |
| Phase 4C+ | Independently certify existing physical families, then deliberately implement finite outer/existence, natural, cross, and pairwise multiway forms | Any shape without finite managed retention/distribution/output proof; arbitrary dual-unbounded and correlated apply forms |
| Phase 5 | Certified distributed MVs/read paths for already-supported operators | Global subscriptions/orderings without a sequencing proof |

Admission changes are granular capability flags tied to descriptors and tests. A later phase never
widens an earlier operator's function, update, timer, or output mode by accident.

## Delivery compatibility gate

The release unit is a certified source/operator/output/sink scenario, not an operator name in
isolation:

| Dimension | Initial cluster requirement | Consequence |
|---|---|---|
| Runtime guarantee | `AtLeastOnce` | `BestEffort` and `ExactlyOnce` remain rejected; the latter stays behind `[LDB-0013]` |
| Source | Non-ephemeral, `Splittable`, assignment-scoped handoff | Kafka is the only current built-in external source path; source partitions and SQL-key vnodes remain distinct |
| Operator state | One coordinator-admitted state/timer/output-bookkeeping cut with a durable terminal Commit and source cursor | Replay cannot double-apply internal state; externally flushed results may repeat |
| Changed-group append snapshots | `DurableAtLeastOnce + MultiWriter + AppendOnly` plus externally auditable writer fencing | One current row per touched group/batch; versions increase per authority interval, while a fenced recovery interval may replay an older committed prefix |
| Retraction/changelog output | `DurableAtLeastOnce + MultiWriter + FullChangelog`, or a new assignment-fenced mutable-sink contract | No current built-in cluster sink qualifies, so these combinations remain closed |

Checkpoint certification preserves CP-5 ordering: drain/enqueue operator output, flush every
durable sink, then seal source positions. It measures real sink flush and state-capture latency in
the same deadline. A stable output identity is part of the state ABI so replay can be recognized,
but at-least-once permits an externally visible duplicate after a crash. Exactly-once is a later
per-combination program requiring an exact-certified source and leader-term-fenced external commit;
local backend durability is neither necessary nor sufficient for that claim.

## Phase 0 — Contract and evidence freeze

**Purpose:** make correctness, compatibility, and performance measurable before backend code sets
accidental contracts.

Work:

1. Accept or revise ADR-008 through an owner review; record unresolved decisions explicitly.
2. Check in a reproducible benchmark profile:
   - CPU, RAM, local NVMe, OS/filesystem, object-store RTT/bandwidth;
   - state smaller than RAM and state larger than RAM;
   - fixed and variable-width keys/values, key cardinality, Zipf/hot-key skew;
   - Arrow batch sizes, input rate, checkpoint cadence, and failure/rebalance schedule;
   - source replay/handoff and sink-flush latency/backpressure profiles;
   - aggregate, window, and two-input join workloads; and
   - absolute p99/p99.9 latency, throughput, checkpoint-pause, RTO, RSS, disk, artifact/decode,
     chain-depth, operator/vnode-count, and restore-staging limits.
   The current candidate numerical gates live only in the machine-readable
   [`linux-nvme-v3` candidate](../../tools/state-backend-qual/profiles/linux-nvme-v3.candidate.json),
   which remains explicitly unapproved, execution-ineligible, and not evidence. Its ownership map assigns backend,
   artifact-conformance, and product-integration sections to different executors; an LSM run cannot
   satisfy sink, checkpoint, or failover gates. The v1 profile remains an immutable regression
   fixture and cannot be used by a new runner plan; v2 is likewise an immutable regression fixture.
3. Specify the partition/state ABI and add golden vectors for every admitted key type plus explicit
   rejection vectors for floating-point, nested, and other excluded types. Persist hydrated routing
   identity separately from the artifact's Laminar-owned state contract. Treat restored routing
   bytes as opaque unless a panic-free strict decoder has independently validated them.
4. Specify stable operator/table ID derivation, concrete builtin codec registry, Laminar-owned codec
   versions, checked arithmetic/null semantics, a dedicated bounded row DTO and outer artifact
   directory, populated-state goldens, and N/N-1 rolling upgrade/rollback behavior. Initial managed
   state is append-only `COUNT(*)` plus `SUM(Int64)`; no live DataFusion/rkyv type is durable ABI.
   The outer directory has manifest-selected magic/version, canonical BODY entries, explicit
   unchanged-parent REFERENCE entries, and no fallback decoder. Reserve encoded bytes before fetch;
   account decode/ingestion scratch separately.
5. Add the mandatory capability descriptor to design tests. Inventory every current operator as
   `Stateless`, `GlobalSingleton`, `VnodeKeyed`, `RebuildableReplicated`, or `LocalOnly` without
   changing admission.
6. Close the exact candidate's DKS-Q2-006 mechanism gate before adapter work. Cycle 17 stopped the
   Cycle 16 RocksDB stall-only recommendation at source proof because v1's maintenance-debt arm
   cannot be closed by that narrow binding. That work now remains frozen v4/reference provenance,
   not an active product track. The Cycle 18
   [decision matrix](../reports/state-backend-contract-decision-matrix-2026-07-24.md) recommends an
   additive maintenance-health successor. Cycle 21 records the direction approval, and Cycle 38
   accepts the consolidated contract for validation-only implementation without a GitHub approval
   workflow. The 2026-07-28 owner amendment makes stock official Fjall 3.1.8 the sole preferred
   qualification-entry subject; Cycle 40/41 TidesDB selection and stop remain historical. Exact-
   source review finds Fjall's atomic-batch, consistent-snapshot and ordered-range primitive shapes
   but disproves a hard global memtable cap and hard journal cap and retains synchronous-stall,
   maintenance-health and prefix-cleanup risks. The bounded recheck must prove that stable stock and
   directly observed Laminar facts truthfully cover every required mechanism. It may not substitute
   a watchdog for invisible engine debt, progress, stalls, or background errors. No missing signal
   is zero and no Fjall fork or git dependency is allowed. Cycle 19's reviewed
   [candidate mappings](../reports/state-backend-maintenance-health-mapping-designs-2026-07-24.md)
   define the historical RocksDB source/binding closure and Fjall scheduler/lifecycle closure used
   by the immutable v4 reference lineage. Redb 4.1.0 is parked after its Cycle 34 design timebox; it has no scheduled
   protocol or adapter work and may reopen only under the bounded micro-prescreen charter recorded
   in its canonical protocol. SurrealKV 0.21.2 remains rejected. These engine gates apply to the
   general local-spill profile. They are not an architectural need of the in-memory reference, but
   the current Phase 0 gate still blocks Phase 1. Run an authorized exact Fjall subject through an
   execution profile that binds stock 3.1.8, `lsm-tree`, adapter, configuration, target and limits:
   Arrow-batch-sized atomic requests, realistic
   hot/cold multi-key reads, timer scans, snapshot/export overlap, sorted restore, vnode drop/GC,
   maintenance pressure/write stalls, hard memory/disk/FD limits, `kill -9`, torn/corrupt data,
   `ENOSPC`, and N/N-1 format rehearsal. Include 24–72-hour churn/TTL soak. A pass qualifies the
   chosen target for later integration; a hard failure disqualifies it and returns backend choice to
   an explicit owner decision.
7. Record the complete delivery matrix: source consistency/topology and handoff; operator update
   mode and output identity; sink durability/topology/input mode; CP-5 ordering; permitted ALO
   duplicates; and combinations that remain closed. Benchmark at least one real certified source
   and sink rather than only an in-memory harness. Kafka output needs broker-enforced writer fencing
   plus partition fence markers bound to the exact recovery-base attempt, capsule digest, and
   assignment certificate; the source oracle derives its ledger by reconciling durable intents
   with actual broker records rather than acknowledgement callbacks. Cycle 57 freezes and consumes
   the supported stable-serving local-adoption projection. Cycle 58 freezes, and Cycle 59 implements,
   the bounded three-family local barrier-pause ledger and engineering consumer. Durable projection
   still waits for one read-only core audit that can return exact retained authority and both floors
   from a stable snapshot. Full-checkpoint and restorable-gate evidence plus provider-neutral
   source-cut normalization remain open. Polling the shared durable `/vnodes` head, aggregate
   histograms, private object paths, or text-log substrings is not proof.
8. Freeze a fault-point vocabulary and output-oracle format shared by later phases. Cross source
   drain/replay, state mutation/freeze, timer fire, output enqueue, sink flush, durable decision,
   external publication, assignment rotation, and ambiguous commit.
9. Freeze an independent production-soak charter: release artifact and topology, real connector and
   object-store dependencies, external oracle, minimum duration/event volume, scheduled faults and
   rebalances, leak-slope/latency/progress thresholds, raw-artifact retention, invalid-run rules,
   and a reviewer who did not implement the operator/backend under test.
10. Audit existing aggregate vnode code for reusable invariants versus map-specific logic. Resolve
   or document vestigial values such as the discarded `has_ownership_partitioned_state` result;
   do not carry dead compatibility scaffolding into the new API.

Exit gate:

- ADR accepted with named reviewers and no unresolved correctness decision;
- benchmark and numerical SLO/RTO profile is reproducible on a clean runner;
- golden ABI/schema vectors and compatibility policy pass;
- the placement-neutral service/lifecycle and in-memory conformance subject are reviewable without
  implying admission; before broad-profile admission, the exact stock-Fjall target passes
  reproducible conformance, latency, resource, fault and operability gates, or it is disqualified
  and the profile stays closed; the in-memory subject remains reference/conformance-only and
  supplies no admission evidence;
- at least one source/operator/append-sink scenario has a complete ALO oracle and every unsupported
  output/delivery combination has a fail-closed assertion;
- the independent production-soak charter is approved before implementation results can influence
  its duration, workload, oracle, or thresholds;
- every operator has an explicit current capability classification; and
- the latest Phase 0 cycle review contains no unowned blocker.

No DDL guard is relaxed in this phase.

## Phase 1 — Managed working-state substrate

**Purpose:** build and prove the shared lifecycle before attaching a production SQL operator.

The service and operator codecs are required to become identical in embedded, single-node, and
cluster modes. That is a target contract, not the current implementation: there is no runtime
backend selector or Fjall dependency today. Embedded and single-node currently use a local
one-key-group topology; cluster uses configured vnode ownership and adds exchange, fencing, and
transfer. The in-memory reference path and any future qualified local-spill backend must share SQL
semantics and portable recovery artifacts, while remaining different capacity/maintenance
profiles.

Work packages:

### 1A. Service and namespace

- Implement the smallest batched service from ADR-008 inside the existing state/database modules.
  Extract a crate only if dependency direction or a second non-DB consumer requires it.
- Add canonical logical prefixes and ABI/schema validation. The local-spill implementation also adds
  persisted metadata, process locking and safe cleanup scoped to one resolved pipeline directory.
- Provide the in-memory semantic/lifecycle implementation first and the Phase-0-selected local-
  spill backend behind the same contract and conformance suite. Neither implementation changes
  admission by existing; do not retain losing disk qualification adapters.
- If Fjall passes the ADR-008 entry closure, use only the reviewed exact official crates.io release
  behind the managed-state facade. Every Fjall API call uses a bounded foreground blocking lane;
  Fjall's internal maintenance remains engine-owned and un-cancellable. Freeze the exact database/
  keyspace layout and limits in the entry contract. Do not allocate a database or keyspace per
  vnode, expose Fjall types outside the facade, depend on a fork, or credit its deprecated global
  write-buffer option as a resource bound.
- Encode hot values with a compact schema-versioned binary format. Do not use per-group Arrow IPC,
  live DataFusion/rkyv checkpoint types, read-before-write accounting, or the removed cold-tier
  wrapper.
- Reject wrong deployment/pipeline/ABI/schema/decided-checkpoint identity before exposing any key.

### 1B. Hot-path scheduler

- Build and cache immutable codec/schema contracts at planning or initialization. Concrete-UDF
  checks, schema canonicalization, dependency-version selection, SHA-256, IPC/rkyv parsing, and
  compatibility lookup never run per row or per processing batch; post-freeze artifact work runs
  off the compute/event-loop thread.
- Deduplicate state keys per Arrow batch and submit one logical multi-read plus one atomic mutation
  batch; a backend without native multi-get must still satisfy the same batched latency contract.
- Complete only Laminar-owned in-memory cache hits inline. Route every Fjall or other disk-capable
  call to a long-lived bounded blocking-worker pool; do not create a future or `spawn_blocking` task
  per row.
- Preserve mutation order within each vnode/table lane while allowing independent lanes to run in
  parallel. Bound queue bytes, age, and concurrency, and propagate storage pressure to ingestion.
- Defer a cold Arrow batch as one unit with bounded input and watermark holds. Aligned barriers drain
  all pre-cut state requests before freezing; the compute/event-loop thread never performs LSM I/O.

### 1C. Resource governor

- Reserve before mutation across Rust/Arrow/operator buffers and, when applicable, engine cache/
  memtables/journal, snapshots/iterators/pinned values, OS page cache, and native memory.
- Enforce memory, restore-staging and frozen-generation limits for every profile. Local-spill
  profiles additionally enforce local-byte and maintenance-debt limits. Every limit has at most one
  documented batch of slack.
- Define pressure states, bounded backpressure, health transitions, and a typed controlled-fault
  error. Every profile tests reservation exhaustion without cursor/output advance or OOM; local-
  spill profiles additionally test disk full and applicable native allocation failure. Never rely
  on the OS OOM killer.

### 1D. Checkpoint bridge

- Record state mutations atomically per vnode and generation with the logical write batch; a local
  engine journal is an implementation detail, not cluster recovery authority.
- Rotate/freeze generations at an aligned barrier and materialize portable per-vnode deltas
  asynchronously through the existing artifact backend and exact-attempt seal.
- Build periodic full bases, chain limits, abort/failed-capture rearming, and retained-generation
  backpressure.
- Prove complete local working-state loss recovery, checksum/corruption rejection and N/N-1
  decoding; local-spill profiles additionally prove physical local-disk loss.

### 1E. Ownership lifecycle

Cycles 6–9 land the transition identity, structural preflight, authoritative roster, explicit
empty-state, aggregate prepare/publish, and current-path raw-lineage receipt subsets below. Phase 1E
remains open until the pre-Commit global budget, remaining transition resource/pause bounds,
vnode-sharded bounded-cost publication, a second real state-family consumer, and full lifecycle/
fault/performance evidence are complete.

- Implement `Unowned -> Acquiring -> Restoring -> Validated -> Active` and
  `Active -> Frozen/Draining -> Revoked` in the graph/runtime.
- **Landed in Cycle 6:** replace the split acquired/revoked staging maps with one exact transition
  identity containing the committed cut, predecessor/target assignment fences, process and
  pipeline identity, acquired chains, and revoked roster. Never convert a missing chain to empty.
- **Landed in Cycle 7:** initialize every declared managed participant before recovery; derive the
  exact per-vnode participant roster from cached graph capabilities and one ownership snapshot;
  require exact capture/restore rosters; represent empty state with a named FULL payload whose
  decoded state is empty; and scope global/keyed capture, restore, and revoke to vnode zero/exact
  owned vnodes respectively.
- **Landed in Cycle 7:** publish exact `vnode_state_ready=false` before exposing pending state or
  staging any startup, recovery, or assignment-adoption work. For a bound managed pipeline,
  publish `true` only for the exact `InstalledVnodeStateBinding`; a no-coordinator process first
  proves the exact registry fence and no pending transition. Require exact `true` reports for every
  predecessor participant before assignment chaining. One absolute checkpoint deadline covers
  assignment serialization,
  local validation, report read, and durable CAS; CAS timeout/error is outcome-unknown and requires
  durable-head reconciliation before another rotation. The mandatory report field intentionally
  makes old/missing-field control records incompatible.
- **Landed in Cycle 8 for `SqlAggregateV1`:** decode and validate every restore chain into private
  prepared state; prepare all applicable aggregate participants; abort and finish every attempted
  participant on a prepublication error; lock and revalidate the exact pending transition,
  installed binding, assignment, transport, registry, and roster; then run only unit-returning
  participant publication, activate the complete set, install the exact binding, and clear the
  exact inbox item. Retire displaced aggregate and binding state after releasing publication
  locks; poison and clear installed authority on publication unwind.
- **Landed in Cycle 9 for the current raw-rkyv path:** reserve aggregate live maps from checked net
  final growth; bind each immutable partial and seal to immediate-parent/transitive raw-payload and
  artifact lineage; preflight the requested subset before body reads; single-flight successful
  parent seal reads; verify exact parent arithmetic and decoded base identity; and validate the
  verified-body receipt at immutable staging. This is global-singleton compatibility containment,
  not the production keyed-transition reservation.
- **Remaining:** keep stateful capability fail-closed unless initialize, capture, prepare, publish,
  abort, revoke, and finish are complete. Generalize this boundary only when a window or join
  consumer proves its timer/cursor/output needs; stateless operators remain legitimate
  nonparticipants, and no successful default may discard named state.
- Evolve the current named FULL-with-empty-state encoding only if measurement justifies explicit
  `BODY`/`REFERENCE` and `FULL`/`DELTA`/`EMPTY` envelope tags. Preserve exact roster validation and
  ensure every resolved participant chain terminates in a semantic FULL/EMPTY base. Omission,
  duplicate names, mixed attempts, and topology drift fail before prepare.
- Build and cache an uninitialized SQL operator's exact plan/codec contract in a pure, fallible graph
  construction phase before artifact fetch. Prepare consumes only preflighted rows under that
  contract; activation cannot precede semantic validation or fall back to node-local DataFusion
  state.
- Reuse assignment/process fences and restoring-output suppression. Intake remains closed while
  the current assignment has restoring vnodes or a staged transition.
- Bound acquired-vnode input buffering, bulk install, post-acquire full rebase, and revoked-range
  cleanup. Prove stale owners cannot read/write/publish after rotation.
- Complete one transition-wide reservation before fetch/prepare for the exact Commit-admitted
  transitive payload/artifact total, wrapper/seal metadata, object and request count, retained spool,
  decode scratch, decoded RSS, simultaneous live/prepared/retired residency, and publication/
  retirement-pause work. The Cycle 9 compatibility receipt and per-vnode limits do not substitute
  for this aggregate budget.
- Introduce vnode-owned state shards before the aggregate migration so acquire/revoke publication
  is a bounded pointer swap rather than a full-map scan.

Tests:

- backend model/conformance tests over random atomic batches, scans, deletes, snapshots, and restore;
- crash before/after write, freeze, encode, upload, seal, install, activate, revoke, and range delete;
- late-operator and later-vnode prepare failure leaves logical rows, dirty/delta bookkeeping,
  ownership, and output unchanged, retains the exact staged transition, and activates no vnode;
  capacity/RSS growth is charged separately. An explicit `EMPTY` base removes stale state while
  missing/extra/duplicate roster entries fail before prepare;
- uninitialized operators cannot activate after byte staging alone; exact semantic/state-contract
  goldens, same-name custom UDAF rejection, global vnode-0, truncation at every envelope/row
  boundary, declared length/count max and max-plus-one, reserved fields, unknown versions, duplicate
  or out-of-order/cross-vnode keys, trailing bytes, and every restore reservation fail closed without
  passing managed artifact bytes to Arrow. Any future IPC codec owns its separate framing,
  compression, dictionary, decoded-expansion, and second-batch/EOS matrix;
- checksum, truncated artifact, wrong ABI/schema/owner, object-store stall and complete local state
  loss; local-spill profiles also cover disk full/corruption and maintenance stalls;
- deterministic withdrawal after the all-participant readiness scan but before assignment CAS;
  prove source drain/forced checkpoint aborts and retains the predecessor, then reconciles either
  possible durable CAS outcome before retry. Exercise shared-deadline exhaustion at every await and
  reject old/missing-field adoption records;
- measure the current two-second unconditional readiness write/readback load. Any later coalescing
  must prove that startup/recovery/adoption can publish `false` without a watcher cache hiding it;
- generation/iterator leak tests and resident/native byte accounting under sustained churn;
- scheduler saturation tests proving bounded queues, lane order, watermark holds, and no event-loop
  blocking; and
- microbenchmarks with concurrent checkpoint and restore; local-spill profiles cover state both
  inside and outside cache.

Exit gate:

- zero unbounded retained collection in the substrate or test harness;
- barrier freeze cost is independent of total state in complexity and confirmed by size scaling;
- latest coordinator-admitted cut with a durable terminal Commit restores after local loss with
  exact model state;
- applicable memory/disk/maintenance limits fail predictably without corruption or OOM;
- no stateful SQL capability is yet enabled; and
- Phase 1 cycle review is approved.

## Phase 2 — Grouped aggregate vertical

**Purpose:** replace the latent aggregate map lifecycle with managed state and certify the first
distributed keyed operator end to end.

Work:

1. Add the aggregate `VnodeKeyed` descriptor and make planner admission consume it.
   Cache the exact codec/schema contract on the physical operator; processing only reuses the
   existing encoded key and static vnode mapping.
2. Reuse the existing canonical pre-aggregate shuffle and ownership/barrier fences. Remove duplicate
   map-era dirty/full/delta tracking only after the managed path is equivalent and all callers move.
3. Encode only named semantic state. For the first vertical that is the canonical group key, checked
   count, and checked `Int64` SUM non-null count/accumulator; map-era `last_updated_ms` and `last_emitted`
   are not copied without a consumer. Apply one Arrow input batch with one grouped state read and one
   atomic mutation/output-enqueue batch; no record performs its own blocking LSM operation.
4. Implement the reviewed Laminar codec/executor for exactly one append-only `COUNT(*)`, one nullable
   `SUM` of a direct `Int64` column, and direct ABI-v1 grouping columns. Check every group-local input
   prefix in source order, preflight the whole Arrow batch, and fault with no mutation/output on a
   late overflow. Use the same implementation for this embedded/single-node reference shape before
   admission.
   Require fresh/populated, null-only, split/coalesced overflow, late-group rollback, and impossible
   restored-state goldens; a matching UDAF name is not codec identity.
5. Keep multiple aggregates, filters/HAVING/derived expressions, broader COUNT/SUM types, `AVG`,
   `MIN`/`MAX`, `DISTINCT`, retractions, UDAFs, multi-stage fallback, and cluster MVs closed. Require
   positive replay-determinism for all upstream expressions; reject volatile/time-relative/AI UDFs.
6. Add per-operator/vnode keys, bytes, dirty bytes, cache hit rate, batch read/write, skew, checkpoint,
   restore, and pressure metrics.
7. Remove the one-million-group safety fiction once the new hard byte policy is the only admitted
   cluster path. Embedded/single-node compatibility may retain the old implementation temporarily behind an
   explicit local-only path, with a removal issue and owner.

Correctness matrix:

- random append-only batches versus the shared checked embedded/single-node reference implementation;
- all admitted Arrow key types, null-only SUM, prefix overflow/error paths, hash golden vectors,
  unsupported multiple aggregates, deterministic-expression proofs, volatile-UDF rejection, and
  hot keys;
- multi-node remote shuffle with every vnode boundary and zero-vnode workers;
- checkpoints during dirty state, failed capture, owner death before/after seal, `1 -> 3 -> 2`
  rotation, stale messages, and repeated acquire/revoke;
- output oracle under the advertised at-least-once boundary: no lost state/result, documented
  replay duplicates only, and no double-application inside restored state.
- Kafka assignment handoff plus at least one admitted durable multiwriter append sink, including
  broker-enforced old-writer fencing, partition fence markers resolvable to exact recovery-base
  checkpoint/capsule authority, ambiguous source acknowledgements, and crash before/after sink
  flush and source-position seal.
- every selected profile's cache-resident and near-capacity skew, frozen-generation, allocator/RSS
  retention and controlled-exhaustion latency/throughput profile; and
- local-spill-only cold-cache, spill-heavy and maintenance-pressure profiles.

Exit gate:

- the newly admitted aggregate matrix is exactly enumerated and all other shapes still produce
  `[LDB-4007]` before mutation;
- fault/differential suites report zero state divergence;
- numerical p99/p99.9, checkpoint, resource, and RTO targets pass on the Phase 0 profile;
- the exact selected working-state profile has a reviewed performance regression result;
- changed-group append-snapshot versus full-changelog modes are explicit: the certified append
  scenario passes, while every unsupported retraction/changelog sink combination remains
  fail-closed;
- rolling upgrade/rollback and checkpoint compatibility pass; and
- Phase 2 review approves removal of obsolete map code and docs.

Rollout starts internal/experimental, then a canary cluster allowlist, then default admission only
after at least one release cycle of telemetry. Rollback disables new DDL while retaining the reader
needed to drain or restore already-created pipelines.

## Phase 3 — Event-time windows and timers

**Purpose:** add managed time/frontier semantics instead of treating a window as only another group
key.

### 3A. Tumbling windows

- Implement vnode-owned event-time timer tables, input watermark/frontier checkpointing, allowed
  lateness, trigger state, output/retraction markers, and atomic fire/cleanup.
- Extend the existing committed source-handoff cut, which already binds source cursors and
  watermarks; do not create a competing watermark authority. Source drain/reassignment cannot move
  the frontier past unprocessed input.
- Unify running and window-close state on the managed representation; do not preserve two unrelated
  map/checkpoint paths.
- Certify append-only tumbling aggregates first.

### 3B. Hopping windows

- Add bounded fan-out accounting, timer coalescing, incremental panes only if benchmarks justify
  them, and cleanup proof for overlapping windows.

### 3C. Session windows

- Add ordered range lookup, deterministic merge, timer replacement, late merge/retraction, and
  atomic multi-window mutation. Session support has its own admission bit and fault matrix.

### 3D. Analytic frames

- Classify bounded row/range frames separately from event-time grouping windows. Require a stable
  ordering/partition proof and byte-bounded frame state. Unbounded frames remain rejected unless
  their resource contract is explicit.

Tests and exit gates:

- differential event-time oracle across out-of-order rows, equal timestamps, empty windows, nulls,
  watermark stalls/regression attempts, allowed lateness, late drops, and session merges;
- crash at timer selection, output, deletion, watermark checkpoint, and post-restore refire;
- timer selection, state mutation, timer removal/advance, emission identity, and output bookkeeping
  are one atomic transition; ALO recovery may re-fire an externally visible output but cannot lose
  or internally double-apply it;
- skewed windows, millions of timers, checkpoint/rebalance with pending timers, owner change exactly
  at close time, and local-spill disk pressure;
- no premature fire, lost fire, unbounded retained closed window, or silent late-data policy;
- each subphase independently meets the Phase 0 tail/resource/RTO profile and completes its cycle
  review before its admission bit changes.

Processing-time/custom-trigger support is not implied by event-time certification.
Any subphase that emits retractions remains closed to external cluster sinks until the delivery
gate has a certified `FullChangelog` path.

## Phase 4 — Stateful joins

**Purpose:** migrate every persistent streaming-state physical join family onto the same
mode-neutral managed state used by embedded and single-node execution, then add cluster
co-partitioning/ownership without changing its SQL semantics. New SQL forms are implemented
deliberately after that migration. Each family retains a separate admission and delivery gate.

### 4A. Harden existing local join planning and correctness

- Reject unsupported temporal and lookup join types instead of coercing them to `INNER`.
- Preserve every analyzed equi key in interval/temporal execution or reject composite keys before
  pipeline mutation; never route using only the first key silently.
- Replace temporal-probe's pending-row overflow drop with bounded backpressure, local spill, or a
  typed controlled failure that advances neither input nor output.
- Use checked signed multiplicity arithmetic in the incremental join and define overflow as a
  pre-publication failure.
- Retain left rows for forward/nearest ASOF until a right-side frontier proves finality; backward
  ASOF remains the first coherent direction.
- Bind system-time/full/partial lookup recovery to an append-only immutable source, an exact
  snapshot/version, or a durable resolved response. A changing unversioned refetch is rejected.

This subphase changes no cluster admission. Its regressions become the semantic oracle for later
mode-parity vectors.

### 4B. Bounded interval `INNER` and `LEFT`

- Canonicalize one equi-join key ABI and install required exchanges on both inputs.
- Store two vnode-owned, time-ordered multisets keyed by join key/event time/row identity.
- Persist both watermarks, bounds, eviction timers, and row multiplicity in the same checkpoint cut.
- Probe bounded ranges in batches and atomically store input/output bookkeeping.
- Admit append-only `INNER` first, then finite bounded interval `LEFT` independently. `LEFT` persists
  matched counts and stable matched/unmatched output identity; only the opposite-side frontier may
  authorize null-padded unmatched emission and cleanup. Any admitted late-data or changelog policy
  that can change a published result emits a deterministic retraction/update.

### 4C. Changelog joins

- Migrate the incremental `INNER`/`LEFT` implementation and changelog-input plus static-dimension
  enrichment.
  Add signed multiplicities, unique/deterministic row identity, join-result weights, reference
  snapshot identity where applicable, and retraction state. Test negative/zero multiplicity and
  cross-cycle duplicate inputs.
- Keep external publication fail-closed until a multiwriter FullChangelog log sink or an
  assignment/key-affine mutable-sink lifecycle is certified. A mutable path must expose vnode
  assignment, fence its previous writer, and use deterministic operation IDs; `MultiWriter` alone
  is not a correctness proof.

### 4D. ASOF, temporal, and temporal-probe variants

- Treat ASOF as two live input cursors. Add versioned ordered history and direction/tie/finality
  rules; forward/nearest matching retains left rows until the right frontier proves the result final.
- Keep the one-live-input temporal table/snapshot version in the checkpoint cut for
  `FOR SYSTEM_TIME AS OF` joins.
- Treat temporal probe as two live input cursors. Persist offsets, pending probes, timers, exact
  progress, and watermark holds; backpressure, spill, or controlled failure replaces any silent
  pending-row drop.

### 4E. Lookup and enrichment variants

- Full-snapshot lookup `INNER`/`LEFT` binds the exact replicated or partitioned snapshot version to
  the checkpoint cut.
- Partial/on-demand lookup `INNER`/`LEFT` owns pending work by vnode and assignment generation. A
  changing source requires a versioned re-fetch contract or durable resolved response; cache
  contents alone are not correctness state.
- Changelog-input plus static-dimension enrichment checkpoints the reference snapshot identity and
  output multiset together.
- Choose `RebuildableReplicated` only for a bounded, atomically published snapshot; otherwise use
  vnode-keyed mutable state. Remote lookup or object storage is never called per input row on the
  compute thread.

### 4F. Deliberately implement outer and existence forms

- Add streaming physical operators for finite `FULL`, left/right semi, and left/right anti joins
  rather than treating syntax or translator configuration as support. Normalize finite `RIGHT`
  from the certified `LEFT` implementation only when the swap preserves original output order and
  nullability and correctly inverts interval predicates, temporal bounds, and frontier ownership;
  otherwise require its own physical operator and gate.
- Add matched counts/bits, opposite-side frontier timers, null-padding or existence-output
  identity, delayed-output rules, and deterministic retractions.
- Prove that opposite-side watermarks, not wall-clock polling, authorize unmatched output and
  cleanup. Keep each form behind its own physical-capability and delivery gate.

### 4G. Finite natural, cross, and multiway forms

- Lower `NATURAL` to an explicitly validated finite equi-join only after freezing its input schemas
  and derived key list in the plan/state ABI.
- Admit `CROSS` only when one side is a bounded reference relation or an explicit finite
  window/retention contract bounds the product, state, and finality.
- Lower both explicit join chains and implicit comma multi-source forms to named pairwise stages
  with stable intermediate identities, per-stage distribution/state contracts, and one validated
  query checkpoint/delivery cut.
- Keep arbitrary dual-unbounded joins and correlated `APPLY` fail-closed.

Tests and exit gates:

- differential SQL oracle over match cardinality, nulls, duplicates, equal timestamps, interval
  boundaries, out-of-order data, watermarks, and changelog weights;
- bounded interval `LEFT` proves no unmatched output before the opposite frontier, stable
  matched/unmatched identity, bounded cleanup, and deterministic retraction/update under every
  admitted late-data or changelog policy;
- normalized `RIGHT` differentials cover asymmetric interval bounds, frontier movement,
  nullability, duplicates, and original left/right output-column order;
- for interval, incremental/changelog, ASOF, temporal-probe, and future two-live-input forms, one
  fenced cut binds both source cursors and assignments, both sides' state, frontiers/timers, output
  identity, and the sink transaction or ALO publication boundary; both sources independently prove
  replay, splitting, and handoff;
- for system-time, replicated/full-snapshot, partial/on-demand, and changelog-input plus
  static-dimension enrichment, one cut binds the sole live cursor/assignment plus the exact reference
  snapshot/version or durable response/pending-work state to output and sink publication;
- two-input barrier/replay permutations, one-side pause/failure, network reorder/loss, owner change,
  crash around unmatched output/eviction, and local-spill disk pressure;
- mode-parity checkpoint/restore vectors for embedded, single-node, and cluster, plus compatible and
  incompatible source/sink combinations for every admitted join family;
- hot join key and asymmetric-rate profiles with bounded probe/result batches and backpressure;
- finite state follows from declared interval/watermark/retention semantics—an internal TTL is never
  the proof;
- each join family has a separate admission flag, compatibility vector, production metrics, and
  approved cycle review; compute support and external-output support are reported separately.

Arbitrary dual-unbounded joins and correlated `APPLY` remain fail-closed until a separately reviewed
finite semantic retention and execution contract exists. Unsupported temporal/lookup forms may
currently be coerced and reach `[LDB-4007]`; other unsupported shapes can return `InvalidQuery`
before cluster admission. Phase 4A makes those routes explicit before any join gate opens.

## Phase 5 — Distributed materialized output

**Purpose:** remove the independent blanket cluster MV rejection only after retained output and
reads have a distributed lifecycle.

Work:

- define output partitioning and stable row identity for append and changelog/upsert MVs;
- write output through assignment-fenced managed tables and checkpoint it with upstream operator
  state;
- add an assignment-aware sink/read topology rather than reusing `MultiWriter` as a mutable-key
  ownership claim;
- route point/range reads to owners or implement a reviewed distributed merge;
- specify read snapshot/epoch consistency during rebalance and recovery;
- restore/activate MV output before serving it;
- define cluster subscription ordering, replay, and backpressure separately; and
- prevent a stateless query from bypassing MV output-state admission.

Exit gate:

- stateless and certified stateful MVs survive node loss and `1 -> 3 -> 2` rotation with a read
  oracle and no stale-owner response;
- checkpoint and read consistency are documented without claiming external exactly-once;
- query/subscribe latency and retained-output quotas pass; and
- Phase 5 review approves the exact MV matrix. Named stateful streams may ship earlier.

## Phase 6 — Production certification and rollout

This phase does not add operator semantics. It closes cross-cutting evidence:

1. Run the common PGVal-style matrix over data rate, topology, partitions, skew, checkpoints,
   process death, network disruption, object-store stalls and rolling upgrade/rollback. The
   local-spill profile additionally forces cold cache, disk full/corruption, maintenance stalls and
   complete local-disk loss.
2. Run the Phase 0-chartered independent black-box soak for each exact scenario and
   working-state-profile identity against the unchanged release-candidate
   binary in a production-like multi-process environment. Use real certified source, object store,
   and sink; an external oracle must check progress, output/state correctness, allowed ALO
   duplicates, recovery, and ownership fencing for every scenario proposed for GA. Track leak
   slopes for Rust heap and, when applicable, engine cache/memtables/journal and native allocation,
   file descriptors, iterators/snapshots, frozen generations, timers and checkpoint artifacts plus
   local bytes when applicable. Archive
   raw evidence and obtain independent reviewer sign-off. The backend spike, ordinary integration
   suite, or canary cannot satisfy this gate.
3. Publish reproducible p50/p95/p99/p99.9 and RTO results for cache-resident, near-capacity, skewed,
   checkpointing and rebalancing workloads plus spill-heavy results when applicable. A skipped
   profile-applicable external test is reported as missing evidence, never a pass.
4. Exercise operational alerts, capacity exhaustion, corrupt checkpoint, failed upgrade and
   admission rollback runbooks plus local-disk replacement for local-spill profiles.
5. Audit credentials, artifact encryption/integrity, log/error redaction and tenant/pipeline quota
   isolation plus local-state-directory security when applicable.
6. Remove experimental flags only per operator matrix, with staged canary percentages and automatic
   rollback thresholds.

General availability requires zero correctness-oracle failures, all committed numerical gates, a
valid independently reviewed release-candidate soak, approved production/operations review, and no
unresolved severity-1/2 issue. Any relevant binary/configuration change or unexplained soak anomaly
requires a complete rerun. This does not remove `[LDB-0013]`; cluster exactly-once has its own plan
and evidence.

## End-of-cycle review contract

Every numbered phase and lettered operator-admission subphase ends with a committed review under
`docs/reviews/distributed-keyed-state-cycle-<n>.md`. The review is written after tests and before the
admission/phase merge. It must name evidence and owners rather than checking boxes by assertion.

Required passes:

1. **AI-slop:** verify every symbol/path/config/source claim against the tree; remove speculative
   APIs, fake precision, duplicated prose, cargo-cult architecture, stale TODOs, generated filler,
   and names that promise more than the implementation guarantees.
2. **Over-engineering:** challenge every abstraction, dependency, feature flag, migration mode, and
   public option; record what is deliberately deferred and why the smallest vertical is insufficient
   without any retained mechanism.
3. **Unused/dead code:** run compiler/clippy feature matrices plus reachability/search review; remove
   superseded maps, hooks, adapters, metrics, configs, test helpers, and ignored return values, or
   assign a dated removal issue. No unowned `allow(dead_code)` may reach an admission change: staged
   code must gain its real consumer, move to test/conformance scope, or be removed.
4. **Production readiness:** review failure containment, resource bounds, security, upgrades,
   rollback, observability, on-call actions, data compatibility, and evidence against numerical
   SLO/RTO gates. For a production claim, verify the independent soak's release identity, external
   oracle, raw artifacts, reviewer independence, and complete valid pass without unexplained gaps.
5. **Documentation:** keep ADR as decision authority and this file as sequencing authority; update
   public capability docs, remove superseded diaries/research, test every link, and cut repetition.
   Split oversized source/test files only at coherent ownership boundaries; do not preserve vague
   helper names or create line-count-only modules.
6. **Tests:** list exact commands/results, skipped suites and prerequisites, nondeterminism/retry
   counts, fault coverage, differential oracle, performance environment, and coverage gaps. A test
   that matches zero cases or needs unrecorded temporary instrumentation is a failure.

The reviewer must conclude `APPROVE`, `APPROVE WITH OWNED FOLLOW-UPS`, or `BLOCK`. A block leaves the
admission flag closed. Reviews are cumulative; use the highest numbered completed
`docs/reviews/distributed-keyed-state-cycle-*.md` review as the current cycle boundary.

Before any production-ready claim, run one repository-wide maintainability audit over runtime and
soak/tooling callers, names, dead-code allowances, module ownership, and superseded documentation.
That audit removes unused code rather than converting it into permanent compatibility surface.

## Commit and change discipline

- Commit contract/test scaffolding, substrate slices, operator migration, and admission changes
  separately; do not combine a backend rewrite with a guard removal.
- Each commit must build its affected feature matrix and preserve fail-closed behavior.
- Admission is the last commit in an operator cycle, after recovery/performance evidence.
- Compatibility readers land before writers; rollback readers remain until every supported cut is
  beyond the old format.
- Destructive local cleanup is namespace-scoped, assignment-fenced, and independently tested.
- Avoid drive-by control-plane, connector, or SQL syntax changes; create a separate ADR/plan if
  measurements reveal that expansion is necessary.
