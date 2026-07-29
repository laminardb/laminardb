# Cluster keyed/stateful operator validation — 2026-07-22

**Branch:** `feature/distributed-keyed-state-adr`

**Baseline:** `1e2f8429` (`main`, 2026-07-22)

**Scope:** admission and lifecycle validation only; no cluster capability is enabled by this work.

**2026-07-29 core update (through Core Cycle 14):** The
[ADR reset](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#2026-07-27-workstream-reset)
pauses later certification tooling. Core Cycle 1 adds a private reference managed vnode shard and
caller-supplied lifecycle publication. Core Cycles 2–4 contain the existing runtime staging path
with exact local roster/chain preflight, deterministic callbacks, delayed activation, sticky
poison, boot-target validation, predecessor-authority repair, and an audited committed final-owner
revoke when the live local process has the committed predecessor boot incarnation and is absent
from the target assignment. Recovery authority, no-store adoption, abort, and unaudited
transitions cannot authorize that cleanup. Cycle 4 also proves and closes a head-seal substitution
window between source/capsule validation and vnode loading, requires exact attestation and a
per-artifact bound on built-in backend reads, and rejects mixed full-attempt staging before
callbacks. The corrective scope preserves the admitted global vnode-0 aggregate's established raw
rkyv FULL/reference/DELTA wire. Cycle 5 contains transitive parent substitution while artifacts
remain under the built-ins' or a conforming backend's immutable namespace/retirement contract, and
bounds each vnode chain to the current writer maximum of six artifacts. External deletion of a live
seal and mixed/future writer-policy compatibility remain production gates. Aggregate encoded bytes,
RSS/decode limits, and publication pause remain open. Cycle 6 replaces split mutable restore/revoke
staging with one immutable transition binding the exact predecessor/target assignments, current
process incarnation, pipeline identity, committed restore cut, and acquired/revoked vnode rosters.
It structurally preflights the complete raw-chain batch, revalidates authority after callbacks,
delays activation until every callback succeeds, and invalidates installed-state reuse before graph
poison is visible. Cycle 7 initializes declared aggregate state before recovery in embedded,
single-node, and cluster pipelines and makes cached graph capabilities authoritative for the exact
managed participant roster. Capture uses one ownership snapshot, global state is scoped to vnode
zero, keyed state is scoped to every owned vnode, and each required empty participant emits a named
FULL payload whose decoded aggregate image is empty. Restore rejects omitted, unexpected,
placement-invalid, or duplicate-within-link participants before callbacks; revoke receives only
placement-relevant vnodes. The raw rkyv vnode payload layout remains unchanged, while graph
checkpoint/state ABI 5 deliberately rejects older ambiguous graph state. Cycle 8 gives the real
managed SQL aggregate participant complete decode/validate preparation, graph-wide prepare-all and
abort-all, unit-returning publication under exact transition authority, and post-lock retirement.
Prepublication failure leaves logical rows/bookkeeping and graph authority unchanged, although
live-map capacity reserved for publication may remain; publication unwind poisons the graph and
clears installed authority. Cluster graph initialization also rejects cached `Rejected` capability
or post-initialization descriptor drift with `[LDB-4007]`. Cycle 9 reserves aggregate live-map
capacity from checked net final growth and adds immutable immediate-parent/transitive raw-payload
and artifact lineage to the built-in checkpoint path. It preflights the requested subset before
body reads, single-flights successful parent seal inventories, verifies exact child-parent
arithmetic and decoded base identity before a parent body, and counts reference-only bodies in the
receipt validated at immutable staging. The raw rkyv body is unchanged, but the outer wrapper is
`LDBVP3` version 3 with a 164-byte header and seals are version 8. V2/seal-7 cuts require an
explicit state/checkpoint reset until a migration bridge exists. Cycle 10 binds participant-agreed
current-profile payload and ancestry limits plus exact verified cluster-wide lineage totals into
recovery capsule v6. After complete readiness validation and before capsule persistence or Commit,
the leader metadata-walks every required vnode through exact parent seals to a root. Restore repeats
the complete proof before body reads and requires exact equality with the target runtime's derived
limits. A post-seal failure publishes Abort when authority is available; an unresolved Abort
requires recovery. Neither path acknowledges or makes that attempt a later reference/delta parent;
leader parent promotion occurs only after the exact Commit wins. Readiness v5, capsule v5, and a
staged-byte/retained-depth configuration change across a live cut require an explicit reset or new
namespace until a capability-superset rule exists.

Cycle 11 reserves the exact acquired subset's declared lineage payload bytes and artifact count
from one worker-wide current-profile budget before its first body GET. The non-cloneable charge
follows loaded bodies into the immutable pending transition; 32 shared request permits and the
caller's absolute deadline/cancellation scope bound body reads. In-flight cancellation drops the
backend future, request permit, collected bodies, and charge. For transitions that own restore
input, exact-Arc cleanup releases abandoned input on publication, terminal poison, failed launch,
and replacement without consuming newer work. Revoke-only final-owner exits own no charge and
retain their durable staging authority after an indeterminate failure. Review removed an initial
steady-state transition-slot lock/Arc retention, so record cycles have no new synchronization or
allocation.

Cycle 12 validates the legacy raw-rkyv outer archive through a checked borrowed view and enforces
the committed `global_singleton_compatibility` one-entry ceiling before owned operator-name or
payload-vector deserialization. Both sealed-chain traversal decode sites and graph preflight use
that boundary. Over-roster heads fail before parent body I/O, over-roster parents fail before a
chain reaches operator preparation, and callbacks/activation remain untouched. The persisted wire
and record hot path are unchanged.

Cycle 13 makes the aggregate key-group count immutable construction identity. Embedded and single-
node state binds `LOCAL_KEY_GROUP_COUNT`; cluster lazy initialization and DDL/startup preflight use
the exact registry/checkpoint count. A global aggregate retains that full count while routing its
sole key to vnode zero. Full and delta capture reject a mismatch before `prev_owned` or delta-chain
bookkeeping changes, and transition preparation rejects it before payload decode or private state
staging. The former optional delta count is now a baseline-activation boolean, leaving inactive
record processing with one guard and no tracking hash, insertion, allocation, lock, or I/O.

Cycle 14 replaces the aggregate's flat working collections with one coherent state object per
vnode. A fixed topology-sized table contains lazy boxed slots, while a sorted active-vnode roster
makes whole-state traversal proportional to resident shards rather than configured key groups.
Each shard owns its groups, changelog deduplication state, emit/checkpoint dirty keys, delta-chain
depth, and forced-full-rebase marker. Whole restore constructs a complete off-side slot set before
replacement. Managed restore/revoke prepares complete replacement shards, then publication swaps
only transitioned slot pointers and the active roster under the existing graph fence; displaced
state remains retired until after the fence. Per-vnode capture is a direct slot operation rather
than a flat-map scan.

The existing local route and remote routed-vnode metadata now provide an exact uniform vnode hint
to aggregate processing. This removes the redundant partition hash for local and singleton-remote
batches without adding a row sidecar or changing the transport wire. Live in-memory aligned replay
retains a singleton remote hint; checkpoint-restored replay, owner-coalesced mixed routes, and
configless batches still hash once per unique encoded group. Independent review caught and fixed
an idle delta that interpreted an empty emitted-dirty set as the complete `last_emitted` map. The
checkpoint structs, body schema, codec, envelope, fingerprint, and routing ABI remain unchanged,
but serialized ordering and byte-identical compatibility have not been independently demonstrated.

These cycles do not add a bounded working-state backend or runtime selector, validate the complete
production keyed budget, bound wrapper/seal/allocator/archive-alignment/inner-decode/RSS/
publication cost, admit a keyed/windowed/join operator, or relax `[LDB-4007]` or `[LDB-0013]`.

Cycle 7 also separates durable assignment adoption from installed vnode-state readiness.
`CheckpointAssignmentAdoption.vnode_state_ready` is published `false` before startup/recovery clears
or stages state. It becomes `true` only for the exact registry fence and, when a coordinator and
pipeline identity exist, the exact installed vnode-state binding. A no-work graph installs that
audited binding before runtime launch; a failed launch removes only its exact marker. Transport
fencing can recognize an adopted participant in either readiness state, but successor assignment
publication requires `true` from every exact target participant. Pristine assignment zero has no
predecessor readiness to withdraw.

The snapshot watcher republishes its bounded adoption report every two seconds so a direct
`false` withdrawal is not hidden behind a local cache. Adoption-lock acquisition, local readiness,
the remote participant scan, and object-store compare-and-swap all consume one absolute checkpoint
timeout. A timed-out or failed compare-and-swap is treated as outcome-unknown: the next pass reloads
the durable head and aborts an already-materialized drain when reconciliation requires it.

The next real three-node ALO engineering run retained one more failure. Subject SHA-256
`91fe9d2c2ef734e1fad4f148da9e69a2eebed92057a74b70702b9b77fb490b10`, logs
`target/tmp/soak-989812-1785220503643922100`, reached checkpoint 5 on the surviving owners after
the leader was killed, but the restarted ownerless process did not converge before the harness
timeout. It repeatedly consumed a Release for epoch E after the owner quorum had already produced
the newer durable Commit E+1, armed E as an exact recovery target, and correctly hit the strict
`[LDB-6041]` no-rewind rejection. The rejection is a required safety check; relaxing it would make
the older cut eligible despite the newer authoritative Commit.

Commit `142dadf7` fixes the caller instead. Only a process outside the frozen participant roster
recovers the latest durable head while its assignment and intake remain closed, and its previous
recovery target is explicitly cleared. Owners named by the round still restore the exact Release
cut. Focused recovery gates passed 38/38, the strict newer-Commit sentinel passed, cluster
warnings-denied Clippy passed, and the serialized cluster integration target passed 23/23 after
the change. A deterministic connector-bearing regression that freezes Release E, advances the
owner quorum to Commit E+1, and then exercises a passive nonparticipant is still open.

The post-`142dadf7` real-process rerun passed its engineering gate. Server SHA-256
`089197fa82d20cc9c83118036d8820d3168f6c3752125f05c6677ac6418f81d5`, harness SHA-256
`b88df679bc4acc93739a4d4462bf208ac9c3b684bc58afc3f09a62a5d096b7b6`, and logs
`target/tmp/soak-996548-1785223178826043700` bind the executed subject. Runtime was 339.97 seconds;
the three kill/rejoin convergence pairs were 43.721/36.441, 37.089/30.020, and 34.586/31.053
seconds. End-of-steady-soak progress was checkpoint 170/epoch 170, and the frozen durable input cut
was checkpoint 187/epoch 187. At a 400 rps target, all 132,899 expected IDs were observed, with 7,898
at-least-once duplicates counted. All 514 observed stalls were at or below 1,024 ms, and there
were zero deadline or SLO violations. This is engineering-only evidence, not an exactly-once,
production-latency, or production-readiness result. The independent immutable release-candidate
soak remains **NOT RUN**.

A separate attempted launch used an executable whose SHA-256 began `5ee...`, but the configured
digest did not match the file at preflight. The harness stopped before spawning LaminarDB. This is
evidence that immutable-subject protection worked, not runtime failure or soak evidence.

This is a best-current preflight, not an atomic distributed lease across scan and compare-and-swap.
A process can stop, fault, or poison its graph after the scan; drain and forced checkpoint must then
abort and retain the predecessor. The deterministic withdrawal-between-scan-and-CAS fault remains
to be added. The unconditional two-second control-KV write/readback cost is unmeasured and may need
safe coalescing. There is not yet a production health contract for false-readiness duration,
refusal reason, last successful report, or outcome-unknown reconciliation. Fixed assignment,
recovery, and startup-checkpoint budgets also lack large-state validation. None of this is a row
hot-path change or production-readiness evidence.

**Current backend authority:** the
[2026-07-29 RocksDB source closure](rocksdb-0.24.0-adapter-entry-source-closure-2026-07-29.md)
stops released `rocksdb` 0.24.0 before dependency or adapter. Its range-compaction API discards
native failure status and cannot be deadline-bound; close/cancellation are also void and unbounded.
Current RocksDB, TidesDB, Fjall, and redb releases are not live fallbacks. No Cargo dependency,
bounded working-state integration, runtime backend selector, or cluster admission has landed.
[Cycle 42](../reviews/distributed-keyed-state-cycle-42.md)
corrects current aggregate failure classification and proves synchronous output/checkpoint
exclusion after an indeterminate apply. [Cycle 43](../reviews/distributed-keyed-state-cycle-43.md)
keeps analytic frame history unchanged until residual projection succeeds. [Cycle 44](../reviews/distributed-keyed-state-cycle-44.md)
classifies returned ASOF failures after right-state mutation while retaining ordinary errors before
mutation. [Cycle 45](../reviews/distributed-keyed-state-cycle-45.md) preserves the learned ASOF
right schema after full eviction through a bounded, conditionally v1-compatible checkpoint v2 and
validates restored index/schema coherence. [Cycle 46](../reviews/distributed-keyed-state-cycle-46.md)
reproduces retained-owner cancellation/panic after real ASOF mutation and permanently fences that
in-memory graph generation from execution or checkpoint capture. The future native-backend owner,
checkpoint-delivery cancellation, and independent soak remain open. [Cycle 47](../reviews/distributed-keyed-state-cycle-47.md)
empirically reproduces the post-graph sink-publication drop on a retained private callback, proves
that no production owner performs that cancellation today, and freezes the owner contract instead
of adding a checkpoint-only fence. [Cycle 48](../reviews/distributed-keyed-state-cycle-48.md)
then closes a live assignment/capture race: staged vnode transitions now enter checkpoint drain,
and the existing rotation fence spans shuffle alignment plus whole/vnode mutable capture.
[Cycle 49](../reviews/distributed-keyed-state-cycle-49.md) proves the same span on both follower
routes and removes a rebalance latency inversion by rejecting an overlapping mutable capture before
state mutation instead of awaiting a retained encoder while holding that fence. [Cycle 50](../reviews/distributed-keyed-state-cycle-50.md)
audits Kafka output and supported public evidence end to end: current ALO records cannot identify a
logical operation or fenced writer interval, and the public checkpoint/assignment surfaces cannot
yet prove every process's local adoption or reconstruct exact per-attempt latency. None of these
decisions changes cluster admission. [Cycle 51](../reviews/distributed-keyed-state-cycle-51.md)
implements only a synthetic v2 semantic oracle for the missing output-authority relationships; it
is not runtime or certification evidence. [Cycle 52](../reviews/distributed-keyed-state-cycle-52.md)
freezes its compact standalone wire representation and hostile decoder without changing Kafka,
runtime admission, delivery guarantees, or certification status. [Cycle 53](../reviews/distributed-keyed-state-cycle-53.md)
adds standalone grouped operation-ID derivation and pure authority projection, while leaving
pipeline-incarnation, interval, producer, and public-evidence lifecycles unwired. [Cycle 54](../reviews/distributed-keyed-state-cycle-54.md)
adds only a unit-test transactional protocol model; real Kafka fencing and ambiguous-marker
reconciliation remain open. [Cycle 55](../reviews/distributed-keyed-state-cycle-55.md) freezes a
stable transactional-ID encoding and proves the deterministic fencing/visibility subset on one
real disposable Redpanda broker, leaving ambiguous `EndTxn` open at that point. [Cycle 56](../reviews/distributed-keyed-state-cycle-56.md)
then proves applied and unapplied matched-`EndTxn` marker/data reconciliation on that same bounded
one-node subject; it adds no runtime connector, durable interval authority, production topology,
exactly-once composition, backend qualification, qualifying latency result, or soak evidence.
[Cycle 57](../reviews/distributed-keyed-state-cycle-57.md) adds an authenticated, 4-KiB-capped
stable-serving projection of the exact process identity and current-boot durable assignment
adoption only while it matches the locally audited assignment fence. The existing three-node
engineering harness consumes it across hard kill/rejoin. No retained record proves current recovery
phase or exact committed-`Release` consumption, and this slice changes no data hot path, admission,
delivery guarantee, backend, qualifying latency result, or independent-soak status.
The new convergence assertions passed twice against real processes, Kafka, and MinIO, but both
encompassing Windows/WSL2 engineering tests remained red on their existing latency-profile terminal
gate; details are retained below and in the Cycle 57 review.
[Cycle 58](checkpoint-attempt-evidence-audit-2026-07-26.md) audits the resulting exact-attempt gap
and deliberately adds no endpoint. Recovery outcomes/capsules and anonymous aggregate timing could
not form a truthful stable response, so it froze a bounded process-local three-family barrier-pause
ledger consumed by the existing harness. That slice was then implemented in Cycle 59. Exact full-
checkpoint and restorable-gate evidence plus a non-creating, read-only same-snapshot durable audit
remain open.
[Cycle 59](../reviews/distributed-keyed-state-cycle-59.md) implements that bounded first slice and
passes a corrected one-kill engineering run: 392 exact records across four process generations,
zero deadline exhaustion, 100% membership in each three-family diagnostic `le=1.024` bucket, a
passing existing pipeline-stall gate, loss-detecting JSONL through each observed cut, and exact
reconciliation with the corresponding observed Prometheus cut. It anchors sampled converged
assignment versions rather than unsampled history and adds no full-checkpoint/restorable-gate,
durable outcome/capsule, exactly-once, backend, admission, or independent-soak authority.
[Cycle 60](../reviews/distributed-keyed-state-cycle-60.md) adds a deterministic three-attempt test
which independently forces metrics-only and exact-cursor/metadata-only coherent-cut retries. It
also freezes an engineering effect-estimation protocol which separates recorder installation from
diagnostic polling; no A/B run or common external driver exists yet. A direct nonempty HTTP route
test remains deliberately deferred: deterministic ledger pagination, process-bound DB continuation,
HTTP mapping/bounds, and real Cycle 59 nonempty continuation polling are covered, while a valid
direct seed would duplicate a live checkpoint-capable cluster. The run does not prove that one HTTP
snapshot returned a `has_more` page. No seeding API was added.
[Cycle 61](../reviews/distributed-keyed-state-cycle-61.md) adds a SHA-bound prebuilt-executable seam
to both ignored real-process harness paths. No real-process run or A/B occurred; it is not the
common driver or independent release-binary soak.
[Cycle 62](../reviews/distributed-keyed-state-cycle-62.md) adds a separate, root-workspace-excluded
driver/observer schedule scaffold. Its Windows matrix proves byte-identical common plans and traces
across C/D and observer success, exit, hang, and malformed output, with bounded capture and process
cleanup. The driver only materializes a static schedule and D only emits planned probes: no SUT,
HTTP, workload, fault, latency, A/B, backend, or soak was executed.
[Cycle 63](../reviews/distributed-keyed-state-cycle-63.md) then audits the current HTTP/reload boundary and selects a server-enforced, disjoint,
startup-bound, loopback-only diagnostic credential instead of a console-bearer broker. It is a
design decision only: no credential, router split, observer, HTTP request, or empirical result has
landed.
[Cycle 64](../reviews/distributed-keyed-state-cycle-64.md) implements that bounded control-plane
slice: shared startup validation, immutable split authority, exact diagnostic routing and
availability bounds, parse-error redaction, and reloadable-section-only publication. In-process
route/config/reload matrices pass, but no live observer, networked A/B, backend trial, or soak ran.
[Cycle 65](../reviews/distributed-keyed-state-cycle-65.md) implements only the standalone loopback
fake-server protocol. Canonical plan/secret framing, bounded direct HTTP, restart/cursor/assignment
validation, explicit incompleteness, bootstrap timeout, and cancellation pass unit and real-child
tests; C opens zero sockets and D parses 348 owned fake responses. The sealed driver does not yet
consume this network mode. The fake path also accelerates all slots rather than exercising the
wall-clock cadence or server rate limiter; no LaminarDB request, A/B, backend trial, or soak ran.
Restart-loss rejection also covers only unread records advertised by the last observed page. An
old process can lose records appended after that poll without the client knowing, so durable
continuity/handoff or an explicitly reviewed bounded observation interpretation remains a live
integration blocker.
[Cycle 66](../reviews/distributed-keyed-state-cycle-66.md) adds only the sealed driver's fake-only
consumer. Fresh invocation/result binding, bounded supervision, strict aggregate validation, and
post-end-seal outcome consumption preserve byte-identical C/D plans and traces across success and
hostile child paths. No live pacing, LaminarDB request, A/B, backend, delivery change, or soak ran.
[Cycle 67](../reviews/distributed-keyed-state-cycle-67.md) is design-only. It freezes a distinct
monotonic paced protocol, complete slot/transcript evidence, an observed-prefix timing claim, and
separate single-host versus multi-host transport profiles. It runs no request or experiment.
[Cycle 68](../reviews/distributed-keyed-state-cycle-68.md) implements only domain-separated
owned-fake control primitives: exact external plan binding, fixed START/ACK framing, post-frame
monotonic anchoring, absolute deadline classifiers, and atomic cross-slot rate admission. The path
has no executable, socket, result/transcript, live schema, LaminarDB request, or empirical sample.

## Verdict

The reported fail-closed boundary is genuine, with three qualifications:

1. It applies to cluster `CREATE STREAM`. Cluster materialized views are rejected with
   `[LDB-4007]` even when their query is stateless.
2. The diagnostic's generic wording is older than part of the implementation. A grouped aggregate
   already has pre-aggregate vnode shuffle and aggregate-specific per-vnode capture, restore, and
   revoke code. It remains rejected because its live groups are held in unbounded operator-owned
   maps with no enforceable byte budget or spillable hot-state tier. Windows and joins have larger
   lifecycle gaps.
3. Restoring operator admission alone would not certify every runnable pipeline. Cluster delivery
   is currently at-least-once only, source/sink topology is checked independently, and there is no
   built-in cluster-admissible FullChangelog sink for retraction output.

The current cluster admission matrix below is canonical for the named physical routes, but DDL
coverage is partial. It must not be read as proof that every syntactic join spelling reaches the
same error boundary:

| SQL shape | Cluster `CREATE STREAM` | Embedded/single-node | Exact cluster reason |
|---|---:|---:|---|
| Projection/filter | Admitted | Supported | Stateless path |
| One ungrouped aggregate stage containing direct `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` calls | Admitted, subject to the restrictions below | Supported | Singleton global state is routed to vnode 0 and uses the exact incremental path |
| `GROUP BY`, without a window | `[LDB-4007]` | Supported | Group map has no hard live-state byte budget or spill tier |
| Running tumbling/hopping aggregate | `[LDB-4007]` | Supported | Window columns become grouping keys and hit the same grouped-state rejection |
| `EMIT ON WINDOW CLOSE` / `EMIT FINAL` | `[LDB-4007]` | Supported | Whole-operator window state has no vnode lifecycle |
| Analytic/window-frame operator | `[LDB-4007]` | Supported | No vnode-keyed state lifecycle |
| Bounded stream interval `INNER` join | `[LDB-4007]` | Physical implementation exists for an append-only, positive finite, single-key shape | Both time indexes, frontiers, and output are not vnode-managed together; additional equi keys are not yet rejected before the single-key runtime config |
| Bounded stream interval non-`INNER` join | Planner `InvalidQuery` before cluster admission | No current physical interval route | The interval planner accepts only `INNER`; this is an earlier physical-route rejection, not `[LDB-4007]` evidence |
| Incremental/changelog `INNER` or `LEFT` join | `[LDB-4007]` | Physical composite-key signed-multiset implementation exists | Retraction output has no vnode/delivery lifecycle or hard state bound, and output-weight multiplication is not checked |
| ASOF join | `[LDB-4007]` | Backward matching over two live inputs is locally tested; SQL is left-preserving | Forward/nearest are not streaming-final because left rows are not retained for later right arrivals; both live cursors, ordered history, and output identity are not bound in one vnode cut |
| Temporal-probe join | `[LDB-4007]` | Physical backward-probe implementation over two live inputs exists, but is not production-correct at its current pending limit | Above 100,000 pending probes it drains oldest rows and logs data loss; both live cursors, history, probes, and timers are not bound in one vnode cut |
| System-time temporal lookup | `[LDB-4007]` when a physical candidate reaches cluster admission, including some coerced unsupported join types | Intended local single-key `INNER`/`LEFT` implementation has one live input plus versioned reference state | Translation does not consistently reject unsupported join types; the live cursor and exact reference version are not bound together, and reference changes do not retract prior output |
| Replicated/full-snapshot lookup | `[LDB-4007]` when its physical candidate reaches cluster admission, including some coerced unsupported join types | Local `INNER`/`LEFT` implementation has one live input plus a registry snapshot | Translation does not consistently reject unsupported join types; exact reference snapshot identity is not bound to the live cursor, assignment, output state, and sink cut |
| Partial/on-demand lookup | `[LDB-4007]` when its physical candidate reaches cluster admission, including some coerced unsupported join types | Dedicated local single-key `INNER`/`LEFT` implementation has one live input plus fetched/cache rows and pending-request state | Current cached rows and in-flight request state are not assignment-fenced in one checkpoint; versioned refetch or durable resolved responses are possible future contracts, not current behavior |
| Changelog-input plus static-dimension enrich | `[LDB-4007]` | Local composite pure-equi `INNER`/`LEFT` implementation has one live changelog input plus a static reference | Reference snapshot identity and output multiset are not bound to the live cursor and assignment cut |
| Other current `RIGHT`/`FULL` or semi/anti streaming forms | Route-dependent: commonly planner `InvalidQuery`; temporal/lookup coercion may instead reach `[LDB-4007]` | Syntax/translator cases exist, but no deliberate general streaming implementation | Syntax/configuration is not an execution or state-lifecycle capability; named-route DDL coverage is partial |
| Current natural/cross, arbitrary unbounded, or explicit/implicit multiway streaming join | Planner-rejected or unsupported in inspected paths | Rejected or unsupported | No current finite distribution/retention/output contract; correlated apply is also rejected; this is not an exhaustive DDL matrix |
| Any materialized view | `[LDB-4007]` | Supported | Distributed materialized output and read ownership are not certified |

The finite bounded-interval target has deliberately stronger semantics than a side tag on the
current `INNER` operator:

- `LEFT` retains each left row, its stable input identity, match count, emitted pair identities, and
  unmatched-output identity until the right frontier plus the declared lateness policy proves that
  no valid match can still arrive. Unmatched output is frontier-authorized, not timeout-by-wall-
  clock. State cleanup occurs only after that output decision and its sink admission are covered by
  a checkpoint. A correction/retraction-capable input must retract a prior pair or unmatched result
  deterministically rather than silently changing history.
- `RIGHT` may lower to `LEFT` only through a validated side swap which preserves interval direction,
  predicates, canonical keys, input/frontier roles, output projection order, and nullability.
- `FULL` carries the symmetric identities and frontier obligations for both sides. Semi joins retain
  existence counts and emit once on the first valid match; changelog updates retract on the last
  match. Anti joins use the same frontier-final unmatched rule as outer joins for append-only output,
  or explicit insert/retract transitions under a compatible changelog sink.

No such outer/existence interval implementation is present today. Natural, bounded cross, and
pairwise multiway targets likewise require their own reviewed physical contracts; arbitrary
dual-unbounded joins and correlated `APPLY` remain intentionally fail-closed.

“One global aggregate” means one logical aggregate node per stream query, not one aggregate
expression or one such stream in the cluster. That node may contain multiple direct aggregate
calls. It must be constructible as `IncrementalAggState`; have no group columns or `DISTINCT`; and
use functions classified as `count`, `sum`, `avg`, `min`, or `max`. `MIN`/`MAX` over a changelog is
also rejected because the counted multiset is unbounded. Derived or nested shapes that create
multiple stages or miss the exact incremental path fail closed.

## Code validation

### Admission is mode-derived and pre-mutation

`LaminarDB::handle_create_stream` calls both cluster validators after reserving the catalog name
but before committing catalog, planner, subscription, connector, or table state. The reservation is
rolled back on error. The validator checks configured runtime mode, not the current number of
owners, so a configured one-owner cluster has the same safety boundary as a multi-owner cluster.

Evidence:

- [`handle_create_stream`](../../crates/laminar-db/src/ddl.rs) calls
  `validate_cluster_query_shape_before_plan` and `validate_cluster_query_shape` before the planned
  query is registered.
- [`validate_cluster_query_shape_before_plan`](../../crates/laminar-db/src/ddl.rs) rejects
  `DISTINCT`, window-close/final emission, analytic frames, AI in-flight state, temporal filters,
  and named specialized join routes. It is not exhaustive across all join DDL/translation paths.
- [`validate_cluster_query_shape`](../../crates/laminar-db/src/ddl.rs) rejects every join plan and
  DataFusion join fallback that reaches that cluster gate, plus multi-stage aggregates, incomplete
  shuffle/ownership configuration, and aggregates outside the exact incremental execution path.
- [`CLUSTER_STATE_LIFECYCLE_UNSUPPORTED`](../../crates/laminar-core/src/error_codes.rs) is
  `[LDB-4007]`.
- `handle_create_materialized_view` has an independent blanket cluster rejection because an MV's
  retained output is state too.

Startup is a second enforcement point. Persisted catalog entries are revalidated before connector
creation, and residual cluster materialized state prevents startup. A cluster graph rejects any
cached `Rejected` physical capability before managed-state initialization and requires the
post-initialization descriptor to equal the cached implementation, state class, status, and
managed-state contract exactly. Thus a conservative/classifier drift such as aggregate plus
`LIMIT` remains `[LDB-4007]` instead of installing mismatched state. This is a fail-closed duplicate
check, not proof that DDL and physical classification have one positive admission certificate.

### Graph execution cancellation and panic are now generation-fenced

The production owner chain is intentionally non-preemptive at this boundary: the coordinator owns
its callback and graph, directly awaits normal, shutdown-drain, and checkpoint graph passes, and
checks shutdown/deadlines only between complete passes. A process panic unwinds through that owner
chain; the outer compute-thread boundary catches it only after the callback and graph generation are
destroyed, then fences intake and restores from durable authority. Therefore ordinary production
shutdown did not cancel and retain a dirty graph before Cycle 46.

The borrowed graph API had a distinct correctness hole. `execute_single_operator` moves graph input
buffers into future-local vectors before awaiting `GraphOperator::process`. ASOF can install right
rows and later await projection; a downstream operator can also suspend after ASOF routed its
output. Dropping the borrowed cycle at that point discarded the taken inputs and partial results but
left advanced ASOF state in a reusable graph. The Cycle 46 red test first checkpointed one right-side
cut, admitted a newer quote plus a trade, waited for the real ASOF output at a downstream pending
probe, dropped the cycle, and demonstrated that the same graph still accepted `snapshot_state`.
A caught downstream panic reproduced the same retained-owner result.

One graph-generation `Arc<AtomicBool>` and an armed cycle guard now close that boundary. The guard is
created after the cluster rotation read fence is acquired and before restore closure, vnode apply,
source priming, or operator execution. Any explicit result disarms it, while cancellation or unwind
sets it permanently. In cluster mode, a terminal recovery/halt result after operator input admission,
an indeterminate vnode callback, or post-callback authority drift uses the same poison path. Cycle 6
clears the exact installed-state success binding before publishing poison with Release ordering, so
assignment adoption cannot trust state from an unusable graph generation. Errors while only waiting
for the rotation fence or performing non-mutating preflight remain unpoisoned. Normal execution,
checkpoint drain, whole-graph capture, and per-vnode capture check the fence and return
`StatefulOperatorPartialApply`, which the existing callback maps to recovery. The callback's existing
`take_pipeline_fault` consumer also observes the sticky graph condition. The fixed test proves that
the old graph cannot execute or checkpoint and that a fresh graph restored from the preceding cut
emits the newer ASOF match on one explicit replay invocation. It does not exercise a source cursor,
callback output publication, sink epoch, or end-to-end exactly-once.

The normal cluster graph cycle adds one installed-state-handle `Arc` clone/drop to the existing
guard work, never per row/operator. The installed-state mutex is acquired only when lifecycle or
fault handling retires authority; there is no normal-row timeout, task spawn, lock, or I/O from this
change. It does not add bounded working state, make a checkpoint-delivery await cancellable, or fence
a future TidesDB operation that can complete after its Rust request/graph is gone. Those require the
callback/native owner to poison the complete publication/root generation. Cluster delivery also
remains at-least-once only: best-effort and exactly-once cluster configurations are still rejected
independently.

### Callback drain publication is owner-non-cancellable today

Cycle 47 traced the complete checkpoint-drain pass after graph execution: materialized views and
subscriptions publish synchronously, sink filter compilation and bounded actor enqueue await, the
sink FIFO fence completes before capture, and source offsets are materialized only after operator
state capture. The durable tail later performs the contract-required phase-one flush or pre-commit
before persisting that source cut; checkpoint-committable sinks finalize only after the durable
decision. A transient red probe used three real output batches and a one-slot gated sink actor.
The first write blocked in the connector, the second occupied the queue, the third remained pending,
and dropping the borrowed drain future left the retained callback without a fault after its graph
input had been consumed. At-least-once recovery could duplicate the already accepted first two
batches; the unsafe result is allowing a later cut to omit the third.

That drop is not reachable through the current production owner chain. Leader, source-less, and
follower checkpoints await the complete drain directly. The nested checkpoint-deadline and cluster-
lease wrappers are supported because control returns to `write_to_sinks`. Publication in replay-
required and cluster modes records a sink/checkpoint fault and returns `Recovery`; local best-effort
enqueue loss records `sink_timed_out`, and the subsequent fence blocks capture: `Skipped` after a
successful FIFO sync, otherwise `Failed`; overall drain-deadline expiry returns `Recovery`. The pre-
capture FIFO sync rejects an unresolved accepted command; the later phase-one flush or pre-commit
establishes the contract-required sink fence before the source cut is persisted. Transactional
sinks finalize only after the durable decision. Root panic destroys the callback and graph before
supervisor recovery.

No runtime guard was added. A drain-only graph fence would leave the analogous normal-cycle output
publication boundary and coordinator source-barrier/exact-attempt ownership unresolved. The trait
contract now requires callers to await an explicit drain result or destroy the complete owner
generation. Before any outer timeout, `select!`, or task abort is introduced, one coordinator-owned
attempt transaction must cover the frozen input cut, graph/MV/stream/sink publication, source
barriers, offsets, and exact-attempt cleanup. This is not exactly-once certification and does not
address native work that can complete after its Rust owner is gone.

### Checkpoint capture is now atomic with vnode assignment publication

Cycle 48 found a live gap in today's admitted global-aggregate path. Checkpoint quiescence checked
only graph input buffers. An idle, source-less, or follower pipeline could therefore stage vnode
acquire/revoke work, report quiescence before a graph pass applied it, and capture the old or absent
vnode state. A transient red probe removed the fix and reproduced the defect: the staged-acquire
assertion failed in 0.02 seconds because the checkpoint drain loop did not run.

The immutable pending-transition slot, restoring lifecycle, and registry's current assignment
version are now checkpoint work. A stable transition must complete one graph drain pass before
either whole-state or per-vnode snapshot APIs will capture. Cycle 6 replaces the former two-map
sample with one slot read, so checkpoint inspection cannot observe split revoke/restore staging.

The final quiescence sample alone was insufficient because assignment adoption could still win
between it and mutable capture. Leader, source-less leader, immediate follower, and deferred
follower now reuse the existing assignment-rotation read fence after sink FIFO synchronization,
before shuffle alignment, and retain it through both whole and vnode snapshots. The admitted
assignment certificate is revalidated after token acquisition, after shuffle staging, and after
mutable capture. Any change after staging is recovery-required; a proven pre-staging supersession
is cancelled without capture. Follower vnode capture was moved out of the durable-tail constructor,
so both images are made under the same token. The token is dropped before encoding, durable
checkpoint-tail I/O, or async cleanup. Alignment remains inside it and may perform deadline-bounded
transport and authority-settlement reads because its staged channel state belongs to the cut.

A callback-level source-less leader regression drives the production checkpoint method. Its audit
operator proves that assignment write publication is excluded inside both `checkpoint()` and
`checkpoint_by_vnode()`, then proves the write token is available after capture returns. Sourceful
leader uses the same method. The two follower routes have separate cleanup contracts but now pass
already-captured vnode images into the tail; explicit awaited cleanup first drops the token.

The Cycle 48 capture change affects only checkpoint/rotation work; it added no normal graph/row
branch, lock, allocation, task, or I/O. Checkpoint sampling reads the pending-transition slot and
restoring lifecycle, compares the assignment version, and takes one deadline-bounded read
acquisition on an existing lock. Cycle 49 found that the
serialization permit was then awaited under that read token for up to the checkpoint/serialization
deadline (120 seconds by default), while the default rebalance writer deadline is 15 seconds. A
timed-out blocking encoder is intentionally non-abortable and can retain the sole permit, so this
was a real recovery-path latency inversion.

Healthy admission permits only one checkpoint tail and callback capture already has exclusive
`&mut self` ownership. Permit contention therefore denotes the retained encoder or an invariant
breach, not useful steady-state queuing. Capture now uses one synchronous `try_acquire_owned` before
either snapshot: `NoPermits` returns `[LDB-6017]` immediately without touching operator state or
creating a second sticky fault, while the old encoder keeps its permit until it exits. Dropping the
capture read token then lets a queued assignment writer proceed. The removed async timeout wrappers
could not preempt the synchronous snapshot calls and supplied no stronger cancellation guarantee.
Deterministic tests cover this rejection and the leader, immediate-follower, and deferred-follower
token spans. The independent checkpoint-versus-rotation soak remains required for latency
distributions. Cluster delivery remains at-least-once: recovery can duplicate already accepted
output, exactly-once remains rejected, and no source/sink capability widened.

The admitted multi-operator reachability question is also now explicit. More than one stream query
may each contain its one permitted global aggregate, so one graph can contain multiple vnode-0
aggregate operators. Cycle 8 prepares every applicable aggregate participant in canonical order
and aborts/finishes all attempted participants if a later prepare or authority check fails. Only
after all fallible work completes does the graph publish each prepared participant, activate the
complete roster, install the exact binding, and remove the exact pending transition. Publication
unwind remains indeterminate and therefore invalidates installed authority and poisons the graph.
Future keyed/window/join/MV admission still requires transition resource/pause bounds,
vnode-sharded bounded-cost publication, and bounded working state; this aggregate protocol alone
does not admit another state family.

### Aggregate support is partial substrate, not an admitted feature

[`IncrementalAggState::cluster_state_rejection`](../../crates/laminar-db/src/aggregate_state.rs)
rejects every `num_group_cols != 0` with the current, precise reason: operator-owned map state has
no live-state byte budget. That check is the immediate blocker for plain and running-window keyed
aggregates.

The same type nevertheless implements:

- a stable key hash to vnodes and pre-aggregate row shuffle;
- per-vnode full and delta checkpoint encoding;
- base-plus-delta restore and acquisition;
- state removal for revoked vnodes; and
- assignment-aware shuffle/barrier integration through `SqlQueryOperator`.

Cycle 7 removes the earlier uninitialized-restore gap for the current aggregate path. Every
connector-pipeline runtime mode initializes declared aggregate state before recovery; cached graph
capabilities then define the exact managed participant roster. Aggregate apply validates each
decoded key against the declared vnode and current registry vnode count before mutation. This
closes early activation and omitted empty-participant ambiguity for the current aggregate
implementation, but does not make grouped aggregate state bounded or cluster-admissible.

Cycle 8 replaces production live callback mutation for this participant. It decodes every FULL and
ordered DELTA chain, validates authoritative vnode replacement/removal, reserves the resulting
collections, and checks exact final group cardinality before publishing anything. A restored group
introduced only by a DELTA is marked dirty for changelog emission; revoked vnodes lose stale
failed-generation rebase markers so a later capture cannot loop on removed ownership. Dropping or
explicitly aborting prepared state leaves logical aggregate rows, dirty/delta state, and ownership
bookkeeping unchanged, but capacity reserved on live maps may remain and increase RSS. Publication
consumes only the prepared mutation plan, while retired aggregate and ownership state is destroyed
after graph authority locks are released.

The feature branch began that work in `562cc590`: capture, delta tracking, `last_emitted`
bucketing, recovery bookkeeping, and revoke now call one allocation-free mapping over the existing
encoded `OwnedRow`, and an active delta generation rejects a changed or zero vnode count before
capture/revoke mutation. This is partition-path hardening only. It does not add a persisted vnode
count, a count-rotation lifecycle, or a bounded versioned semantic payload. The Cycle 6 transition
tag and Cycle 7 graph ABI are authority metadata around the established raw payload, not that
payload envelope.

The branch later experimented with a generic Arrow schema descriptor (`b0ac1a7a`) and strict
single-record-batch IPC helper (`12a34c38`). Cycle 3 review (`1e8b1a59`, `f4ded97b`) narrowed the
former to the bounded,
opaque routing identity actually consumed by `PartitionKeySchemaV1` and removed the latter. The
generic helper used Arrow 57.2's `StreamReader`, which can allocate from IPC-declared metadata/body
lengths before proving those bytes exist. The reviewed first `COUNT(*)`/`SUM(Int64)` artifact is
specified to use a bounded Laminar row codec and will not call Arrow on restored state. Any future
IPC codec must first reserve a global restore budget and preflight hostile framing. These changes
remain admission-neutral and do not close the runtime lifecycle gap.

The audit found that vnode membership is coupled to several other restore invariants and therefore
must not be patched as an isolated assertion:

- the raw aggregate payload has no envelope carrying partition ABI, vnode count and claimed vnode,
  canonical key-schema fingerprint, or accumulator-state schema/version;
- the payload directly archives the live `AggStateCheckpoint` Rust type rather than a bounded,
  independently versioned wire DTO, so its compatibility surface is implicit;
- FULL and DELTA rows inherit `AHashMap`/`AHashSet` iteration order, so identical logical state does
  not have canonical bytes across insertion order, process hash seed, or restart;
- columnar checkpoint writers synthesize every Arrow IPC field as nullable `c0...cN` instead of
  preserving the accumulator's declared semantic fields. For example, a non-nullable COUNT state
  is physically described as nullable, so one descriptor cannot honestly identify both forms;
- the ordinary IPC helper returns the first record batch and does not require exactly one batch,
  the end-of-stream marker, or complete byte consumption. Exact persisted restore must reject a
  second batch/trailing bytes and validate declared lengths/decoded expansion before Arrow parsing;
- accumulator validation checks payload count and row count, but an empty payload can be decoded as
  absent state and rebuild a stateful accumulator at its default unless expected state fields are
  known independently;
- `last_emitted` keys are cast toward planned types and an empty keyed tuple can take the global
  sentinel path, so exact arity/type checks must precede conversion;
- `last_updated_ms` is written and round-tripped but has no aggregate execution, TTL, or eviction
  reader, so copying it into a durable managed schema would ossify vestigial state;
- the resolved DataFusion 52.3 non-distinct integer/decimal SUM uses wrapping addition, while COUNT
  uses ordinary integer addition and the release profile does not enable overflow checks. The soak
  profile does enable them, so behavior can differ by build profile. A portable artifact cannot
  repair that arithmetic; the first managed vertical needs Laminar-owned checked semantics or an
  independently enforced input/state bound before a writer or admission proof exists;
- Arrow 57.2's row parser assumes converter-produced well-formed bytes and may panic on malformed
  input. Restored partition-key bytes must remain opaque for hash/LSM identity, or pass a separate
  strict decoder before any materialization; artifact bytes cannot be fed directly to `RowParser`;
- the former initialized FULL apply merged only keys present in its payload, allowing a stale live
  key in the same vnode to survive. Cycle 6 replaces the complete initialized vnode namespace and
  rejects decoded keys for another vnode atomically. Cycle 7 initializes the aggregate before
  recovery, so the runtime restore path no longer relies on uninitialized raw-byte staging;
- at the validated baseline, delta bucketing remembered one vnode count while capture accepted
  another. `562cc590` now rejects that local drift, but artifacts still do not bind their vnode
  count or define an explicit generation-rotation lifecycle;
- Cycle 8 retains exact raw-chain/cut identity, completes graph structural preflight, and then
  performs aggregate semantic decode/validation into a mutation plan with private replacement
  collections for every participant before logical publication; and
- ordinary grouped batch processing mutates groups and accumulators sequentially, so a late error
  can leave an in-memory partial batch. Cluster execution must recover from the sealed cut rather
  than retry that batch locally until the managed state write is one atomic transaction.

Cycles 6–13 close staged-material loss, activation before complete validation, mixed live
aggregate publication. The graph snapshots one exact pending-transition `Arc`, structurally
resolves the complete raw-chain inventory against its cached participant roster, and asks every
applicable SQL aggregate to prepare a mutation plan with private replacement collections. A
preparation, exact-slot, or authority failure aborts and finishes all attempted participants
without poisoning or logical row/bookkeeping mutation; reserved live-map capacity may remain.
After exact assignment, transport, registry, roster, pending-slot, and installed-binding
revalidation, participant publication cannot return an error; the graph activates the complete
roster, installs the binding, clears the pending item, releases both locks, and retires displaced
state. A publication unwind clears installed authority and poisons the graph so recovery uses the
committed cut.

This is atomic semantic installation for the current aggregate participant, not production
qualification. Cycle 9 removes avoidable live-map growth by reserving only checked net final group
growth and adds a requested-subset receipt for transitive raw body bytes/artifacts on the present
global-singleton path. Preparation still scans the full unsharded live aggregate maps to discover
transitioned keys, clones those keys, can hold live state and prepared replacement collections
simultaneously, and can leave successfully reserved capacity after abort. Publication and
retirement remain proportional to transitioned state. Cycle 10 supplies the current-profile
pre-Commit raw-lineage authority, and Cycle 11 owns each acquired subset's declared bodies/
artifacts under bounded body-read concurrency and one restore deadline. Cycle 12 caps legacy outer
entries before owned deserialization. Cycle 13 closes first-capture selection of aggregate routing
cardinality and checks the immutable count before capture/transition work. There is still no
complete production keyed or transition-wide reservation for wrapper/seal metadata, allocator/
response overhead, retained spool, archive validation/alignment, inner decoder scratch/expansion,
decoded RSS, live/prepared/retired residency, or apply pause. The remaining boundary is therefore
resource-governed, vnode-sharded bounded-cost publication plus a second real state-family
consumer—not another validator around the raw keyed payload.

This code is useful implementation substrate, but it does not make the operator production safe.
The current one-million-group guard counts entries, not bytes. The live `groups`, changelog
emission state, distinct/counting side state, dirty generations, Arrow buffers, and allocator
retention are not governed by one truthful byte budget, and there is no local spillable
working-state engine. Capture also walks and serializes operator maps rather than freezing a
state-engine generation in constant time. Initial delta FULL capture can scan all groups and all
`last_emitted` entries once per touched vnode; delta discovery and revoke also scan live maps
synchronously. That approaches `O(vnodes x groups)` work at the checkpoint/rebalance cut and is a
direct tail-latency blocker, not merely an implementation detail.

### Windows and joins have additional gaps

`EowcQueryOperator`/`CoreWindowState` checkpoint local whole-operator state, but do not override the
graph's per-vnode capture/apply/drop hooks. Distributed windows also need vnode-owned event-time
timers, watermark/frontier state, late-data policy, firing/output bookkeeping, and atomic cleanup.

The persistent streaming-state physical join families are interval stream-stream, two-sided
incremental/changelog, ASOF, temporal probe, system-time temporal lookup, replicated/full-snapshot
lookup, partial/on-demand lookup, and changelog-input plus static-dimension enrichment. Finite ad hoc
ASOF and generic DataFusion batch/fallback plans are not separate managed operator families. None of
the streaming families declares managed vnode state. The current interval physical route accepts
only `INNER`, so its non-`INNER` shapes fail during planning. General `RIGHT`, `FULL`, semi, and anti
cases have no deliberate supported streaming implementation, but their observed error boundary is
route-dependent: temporal/lookup translation can coerce an unsupported join type and let the
resulting candidate reach cluster `[LDB-4007]`. Natural/cross, arbitrary unbounded,
explicit/implicit multiway, and correlated apply shapes remain planner-rejected or unsupported in
the inspected paths; DDL coverage is partial.

Local execution also has correctness gates before distributed migration: forward/nearest ASOF is
not streaming-final; temporal probe drops oldest pending work above its fixed cap; temporal/lookup
translation can coerce unsupported join types; interval/temporal planning can discard extra equi
keys; and incremental output multiplicity can overflow unchecked. These must fail closed or gain
correct bounded semantics rather than being inherited by cluster state.

A production dual-live-input join additionally needs both inputs shuffled with one canonical
join-key encoding and one fenced checkpoint cut covering both source positions and assignments,
side-specific vnode tables/time indexes, watermarks/retention, multiset weights, matched or emitted
output identity, and assignment-fenced output ownership. Interval, incremental/changelog, ASOF, and
temporal probe use two live cursors; ASOF/probe asymmetry does not turn the right input into a
checkpoint-independent reference snapshot. Stream-reference joins instead bind one live cursor and
assignment to an exact reference snapshot/version in the same output/sink cut. The current partial
lookup path is specifically fetched/cache rows plus pending-request state; a future implementation
must checkpoint that work or adopt a reviewed versioned-refetch/durable-response contract. Merely
serializing current hash tables per vnode would leave routing, bounds, tail latency, and recovery
correctness unresolved. Rejecting default vnode methods and the managed capability roster prevent a
new stateful operator from silently opting out, while DDL remains explicitly fail-closed.

The target semantics for finite interval `LEFT`/`RIGHT`/`FULL` and left/right semi/anti are specified
with the canonical matrix above; schema-frozen natural, bounded cross, and explicit/implicit
pairwise multiway forms remain later deliberate targets. Arbitrary dual-unbounded joins and
correlated apply remain fail-closed; a state backend alone cannot supply their missing semantics.

### Delivery and connector constraints are independent guards

Cluster runtime admission currently rejects `BestEffort` and `ExactlyOnce`; only `AtLeastOnce` is
accepted. A cluster source must be non-ephemeral and `Splittable`, and a cluster sink must be
`DurableAtLeastOnce + MultiWriter`. At this baseline, Kafka is the only built-in external source
with the required cluster topology. Several built-in append sinks qualify, but no built-in
cluster-admissible sink accepts `FullChangelog`.

Evidence is the typed capability vocabulary in
[`connector.rs`](../../crates/laminar-connectors/src/connector.rs), its exhaustive compatibility
checks in [`pipeline_lifecycle.rs`](../../crates/laminar-db/src/pipeline_lifecycle.rs), and each
connector's registered descriptor—not an inference from connector names.

That does not block every grouped stream. Ordinary `CREATE STREAM` queries are registered as
non-incremental and emit append-result rows, so their compatible append-sink paths can be certified.
The current aggregate emits a repeated full running snapshot of all groups each processing cycle,
not only changed groups or a final monotonic result. It does mean retraction/full-changelog modes
cannot be called production-ready merely because the operator can compute them. A mutable cluster
sink also needs key-affine assignment and stale-writer fencing; a `MultiWriter` flag alone does not
establish ownership.

The current source handoff already binds checkpoint attempt, source assignment version, cursors,
per-source watermarks, cluster watermark, and recovery frontier. SQL-key vnode placement is a
separate shuffle from Kafka source-partition placement, so a stateful checkpoint must preserve both
authorities. Under at-least-once, recovered state and the source cursor share a coherent sealed cut,
but external records flushed after that cut may repeat. Local LSM durability cannot turn this into
end-to-end exactly-once; that later guarantee needs connector/provider commits fenced by the same
leader/assignment proof already used by the checkpoint coordinator.

### Current Kafka output and independent-evidence boundary

Cycle 50 traced the first Kafka-to-Kafka candidate from graph output to the broker. Graph execution
returns batches by stream name, and publication still selects the configured sink input and output
contract. At the connector boundary, however, `SinkCommand` wraps `SinkOperation::WriteBatch` with
only a `RecordBatch` and deadline; the current epoch remains actor-local error/checkpoint state. The
Kafka append connector serializes each row and sets a payload, optional configured key, and optional
partition. Main records have no Laminar headers. The single bounded actor supplies implicit
process-local FIFO, but no externally visible sink admission sequence.

Kafka source is replayable and splittable, and the append sink truthfully declares durable
at-least-once multiwriter capability. Its producer uses `enable.idempotence=true`, `acks=all`, and a
bounded in-flight count. It deliberately has no `transactional.id`, blocks users from injecting
one, and does not expose the broker-assigned producer identity as Laminar authority. Each successful
write already awaits every accepted delivery report, so checkpoint `flush()` is then a no-op. An
ambiguous write retires that connector generation. These are useful durability/failure properties,
but producer idempotence covers one producer session; it neither fences a successor nor identifies
recovery replay.

The exact evidence inventory is:

| Required fact | Current internal authority | External evidence today | Disposition |
|---|---|---|---|
| Logical operation and payload | User row only; no engine operation ID reaches `SinkCommand` | Broker payload/key; the engineering soak parses user `seq` but does not retain bytes by identity | Missing as a product contract; workload `seq` is not a general operation identity |
| Pipeline/operator/sink identity | Pipeline digest, graph/operator names, and configured sink name exist separately | None is attached to the main Kafka record | Missing |
| Assignment and writer process | Checkpoint assignment fence binds version/digest, node, and boot incarnation; process lease fences local sink work | Current assignment and membership are queryable, but output carries none of them | Missing from output |
| Sink shard/writer interval | Kafka partition is selected at send; actor/connector generations are local lifetimes | Broker topic/partition/offset only | No stable Laminar shard, predecessor/successor interval, or interval marker |
| Sink admission order | One actor consumes a bounded FIFO | Broker offset orders actual appends per partition | No checked Laminar sequence and no cross-writer authority boundary |
| Legal replay interval | Committed recovery capsule binds connector-specific retained positions and assignment | Engineering soak observes advisory consumer-group commits | The provider-specific recovery position is authoritative, but no provider-neutral exclusive-cut projection exists and the current oracle consumes neither form |
| Checkpoint terminal/assignment evidence | Create-once outcome, seal inventory, and recovery capsule bind the exact attempt and assignment | `/api/v1/cluster/checkpoints` exposes only the latest ID/epoch/time/names/count; current internal reads cannot supply one read-only same-snapshot outcome/floors/capsule classification | Partially observable; no stable per-attempt evidence view |
| Local assignment convergence | Each assignment participant durably publishes a boot-bound canonical `CheckpointAssignmentAdoption`, while the watcher separately maintains the current locally audited assignment fence | Cycle 57's authenticated `/api/v1/cluster/local-evidence` reads the exact local adoption through one bounded checked-KV operation, requires it to match that live fence, rechecks fence/process authority, caps/no-stores the response, and is compared with a durable-before/after assignment sandwich by the engineering harness | Stable-serving local adoption is now observable; zero-vnode nonparticipants have no current matching adoption evidence and are outside the positive-evidence set, while recovery phase and committed-`Release` consumption remain absent |
| Latency attempt/max evidence | Cycle 59's preallocated process-local ledger retains exact pipeline-stall, local-barrier, and optional aligned-resume records with process, attempt, assignment-certificate, sequence, overwrite/loss, handoff, and deadline fields | A protected bounded/paginated local route is consumed by the engineering harness and reconciled at coherent observed Prometheus cuts; records through each cut stream to per-generation JSONL while memory remains bounded | Implemented and green for those three families on one engineering run only; exact full-checkpoint/restorable-gate evidence, instrumentation A/B, complete historical assignment coverage, durable attempt/outcome correlation, and independent qualification remain open |

The live engineering oracle currently accepts any number of duplicate `seq` values once the
expected set is present. It does not compare bytes for repeated IDs, bind a duplicate to the sealed
source cut, read assignment evidence, or identify a predecessor record after successor activation.
The legacy standalone fixture v1 compares duplicate bytes, aggregate prefixes, and frozen versus
durable source-cut completeness, but invents fixture operation IDs and has no assignment, writer
interval, marker, or recovery-cut-to-writer-interval binding. Cycle 51's fixture v2 adds those
relationships synthetically: explicit source and sink inventories, exact pre-delivery baselines,
bootstrap/recovery checkpoint references, separately resolved current writer assignments,
predecessor/successor markers, checked per-interval sequences, raw-offset replay causality, and
independently derived vnode/shard ownership. It also proves that missing or wrong-run authority is
`RUN_INVALID`, while complete evidence of conflicting or stale output is `PRODUCT_FAIL`. Its
assignment/process view is pre-reconciled test data and its ABI stops at vnode-to-shard semantics.
Cycle 52 adds exact standalone envelope bytes, literal goldens, strict caps, and hostile decoding,
but the current Kafka path still emits none of those bytes and no supported evidence endpoint or
broker fence exists. Cycle 53 replaces v2's arbitrary operation-ID bytes with the sole ADR-defined
grouped `COUNT(*)`/`SUM(Int64)` derivation over deployment, pipeline identity/incarnation,
sink/operator/output scope, exact ABI-v1 group bytes, and checked count. A pure projection rejects
contradictory current assignment, live process, recovery Commit, shard ownership, and interval
inputs before producing the already-frozen marker/data headers. It independently reconstructs the
production owner-map and full assignment-certificate digests, including canonical participant
boots, rather than trusting an arbitrary hash label. Production still supplies none of these new
lifecycle inputs to the sink command. Neither fixture version is independent production evidence.

Cycle 54's synchronous fake consumes the Cycle 53 authority and Cycle 52 bytes. It models confirmed
marker-before-data over a supplied partition set, unsplit marker fanout, explicit bounded data transactions, global checked
sequences including `u64::MAX`, terminal ambiguity, and immediate confirmed-predecessor replay. It
does not contact Kafka, serialize a transactional ID, establish actual visibility, allocate durable
intervals, prove a complete broker partition inventory, reject reuse across fake chains/restarts,
or change the current nontransactional sink. An ambiguous marker can be present or absent;
the successor must reconcile the read-committed log before choosing its predecessor.

Cycle 55's standalone probe freezes `transactional_id_v1` over `(deployment, pipeline
incarnation, sink, shard)` and contacts only the repository's disposable one-node Redpanda service.
On a newly created RF=1 topic, metadata before and after returned exactly partitions `[0,1,2]` with
one in-sync replica each. A second producer initialized with the same stable ID; the old producer's
later commit and fatal state both reported `Fenced`. Separate captures showed committed markers
and data visible under both isolation levels; a flushed confirmed-abort attempt and a flushed open
transaction aborted by fencing were physical under `read_uncommitted` and absent under
`read_committed`; the confirmed-abort retry was byte-identical and visible. Marker values matched
the frozen 325-byte first/successor literals on all partitions with one `__ldb`, null key, and empty
non-null payload. Data retained its exact key/payload and an unrelated `trace-id` header while
carrying exactly one 66-byte `__ldb` value. The successor replay retained operation/key/payload,
changed interval, and restarted sequence zero.

Cycle 56 extends only this standalone probe boundary with a matched `EndTxn` v1 actuator. On four
isolated topics it retained the exact target request plus either the same-connection/correlation
error-zero broker response with zero response bytes sent downstream (`applied`) or zero target
bytes sent upstream (`unapplied`). The expected retriable local timeout completed, all connections
observed for producer A's exact client ID closed, and producer B initialized the same transactional
ID before separate consumers reached frozen cuts on all three partitions. Read-committed evidence
selected the applied marker and fell back to the last confirmed predecessor for the unapplied
marker. Applied data was read-committed-visible before replay, unapplied data was absent there, and
successor replay was read-committed-visible in both cases; read-uncommitted retained both staged
branches.

This proves only the exact tested Kafka-protocol behavior, not generic production inventory or
atomic source/state/sink delivery. The probe still omits replication/failover, restart/disk
durability, TLS/auth, broker limit and pressure cases, durable runtime interval non-reuse,
production hot-path/tail latency, and the independent release-binary soak. Redpanda's success
response was treated as an accepted coordinator decision; semantic claims came from bounded
read-committed reconciliation, not the timeout. The current connector remains nontransactional and
unchanged.

The missing minimum is not another state backend. It is the already-designed delivery/evidence
vertical: an operator-specific replay-stable operation ID; compact data-record provenance; a stable
sink-writer shard and checked per-interval admission sequence; broker-enforced predecessor fencing
plus a committed successor marker bound to the exact recovery-base attempt, capsule digest, and
assignment certificate; and a versioned read-only projection of that existing checkpoint authority
which the oracle can resolve. The data header contains only version/kind, operation ID, interval ID,
and sequence; interval-wide pipeline/operator/sink, ABI, shard/vnode, assignment, process, and
recovery provenance lives in the marker. The reader hashes the raw payload bytes and derives the
expected vnode from the canonical key and frozen ABI rather than trusting duplicated row metadata.
Cycle 57 closes the public stable-serving local-adoption identity only. Cycle 58 proves that
per-attempt timing must first come from a bounded local ledger and that durable joining cannot use
the current racy/inferred lookup composition. Cycle 59 implements and exercises the bounded
three-family ledger; the durable same-snapshot join remains required for the rotation charter. The
initial interval must first
reference a zero-input bootstrap checkpoint/capsule created
from exact pre-delivery source baselines while readiness, graph work, and sink writes remain closed;
there is no null/genesis authority shortcut. Implementation must start with semantic model and byte-
golden tests, then a fake producer state machine, deterministic real-broker fencing/isolation,
and controlled ambiguous-outcome reconciliation. Cycle 55 closes the deterministic broker slice;
Cycle 56 closes the deliberately controlled one-broker ambiguity slice, and Cycle 57 makes the
current three-node engineering harness consume exact local adoption evidence. Durable interval
authority and admission-neutral transactional runtime integration remain open. The independent
soak remains later and separately operated.

### Fjall failed adapter entry; its former cold tier remains historical

The current baseline contains no Fjall dependency or state-tier module. Stock Fjall 3.1.8 failed
the bounded source gate before integration; its worker-error/drop trace is now both source-audited
and empirically confirmed in the
[lifecycle closure](fjall-3.1.8-worker-lifecycle-empirical-closure-2026-07-29.md). Fjall 3.1 was previously
used by the v0.26-era optional `state-tier` feature as a rebuildable cold cache for demoted
checkpoint slices/groups. The current [`CHANGELOG.md`](../../CHANGELOG.md) records why commit
`1e2f8429` removed it: a correctness defect allowed demotion to clear vnode dirtiness before
durable checkpoint authority, so a failed attempt could recover older bytes. The deleted tier was
not always-current working state and is not safe substrate to restore wholesale.

Its archived benchmark is useful warning data, not production qualification. It used individual
point inserts and uniform cold reads; the wrapper also read before write/remove for byte gauges and
copied returned values. Formal target-Linux/NVMe testing never ran. Current Fjall 3.1.8 has useful
atomic batches, snapshots, range scans, and sorted ingestion, but lacks native multi-get/range
delete and a mature supported memory/compaction observability surface. The ADR therefore requires
the same real state workload and fault gates before selecting any backend rather than assuming a
historical dependency is fit. The later targeted child reproduced the exact fatal worker-exit/drop
hang; a small RAII counterpatch made that child return but remains unreleased and does not supply a
complete bounded/fallible lifecycle or maintenance-health surface. There
is no `fjall` or `tidesdb` dependency or selectable bounded working-state
implementation in the current manifests or runtime. The existing injectable `StateBackend` is
instead checkpoint-artifact storage, as distinguished below.

## Empirical validation

The validation uses a real configured cluster builder with a controller, shared object-store
checkpoint backend, shuffle sender/receiver, and an eight-vnode registry. It intentionally assigns
all vnodes to one owner: rejection in this setup proves that admission follows configured mode and
does not accidentally become permissive when only one process is present.

The repository test `cluster_query_shape_admission_is_pre_mutation_and_mode_derived` exercises:

- `[LDB-4007]` for a plain keyed aggregate, global and keyed windows, window-close emission,
  bounded interval join, temporal filter, derived/nested aggregate, stateless MV, and aggregate MV;
- successful cluster creation of a filtered projection and one global `SUM` stage;
- absence of catalog/planner/connector/subscription residue after every rejected DDL;
- startup rejection of residual materialized state; and
- separation from embedded mode, where local query rules apply instead of `[LDB-4007]`.

The rendered error is an outer generic invalid-operation `[LDB-0005]` containing the specific
`[LDB-4007]` cluster lifecycle code and reason. Tests search for the specific nested code.

The integration test `sealed_materialized_view_manifest_is_rejected_by_every_node_after_restart`
injects a persisted keyed MV and verifies that each node rejects both initial startup and restart
with `[LDB-4007]`.
Local unit/integration tests separately execute grouped aggregates, window-close aggregates, a
backward ASOF path, temporal lookup, and incremental changelog joins. Those tests establish only
the named local paths; they do not close the ASOF finality, temporal-probe loss, join-type coercion,
extra-key, overflow, or distributed-lifecycle blockers above.

Clean targeted commands use `--no-default-features --features cluster` throughout. Results:

| Test filter | Result | Evidence |
|---|---:|---|
| `db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived` (`--lib --exact`) | PASS, 1/1 | Named-route admission matrix and no-residue rollback; explicit plain-keyed and global-window assertions are covered, but specialized join DDL coverage remains partial |
| `pipeline_lifecycle::connector_admission_tests::source_contract_admission_matrix_is_fail_closed` (`--lib --exact`) | PASS, 1/1 | Exhaustive delivery/consistency/topology source matrix |
| `pipeline_lifecycle::connector_admission_tests::sink_contract_admission_matrix_is_fail_closed` (`--lib --exact`) | PASS, 1/1 | Exhaustive delivery/consistency/topology/output-mode sink matrix |
| `incremental_emit_snapshot_matches_full_recompute` (`--test incremental_emit --exact`) | PASS, 1/1 | Embedded grouped aggregate matches full recomputation across four multi-key batches |
| `db::tests::test_nullif_float_with_int_literal_runs_without_error` (`--lib --exact`) | PASS, 1/1 | Embedded tumbling window closes and emits |
| `db::tests::asof_join_in_materialized_view_emits_backward_match` (`--lib --exact`) | PASS, 1/1 | Embedded ASOF MV produces expected backward matches |
| `operator::interval_join::tests::test_checkpoint_roundtrip` (`--lib --exact`) | PASS, 1/1 | Local interval-join buffered state restores and matches a later batch |
| `rebalance::dead_aggregate_owner_advances_to_a_successor_recovery_quorum` (`--test cluster_integration --exact`) | PASS, 1/1 | Multi-node global aggregate checkpoints, loses vnode-0 owner, and reaches successor recovery quorum |
| `failures::zero_vnode_workers_start_idle_without_joining_assignment_quorum` (`--test cluster_integration --exact`) | PASS, 1/1 | Three-node stateless stream runs with zero-vnode workers |
| `failures::sealed_materialized_view_manifest_is_rejected_by_every_node_after_restart` (`--test cluster_integration --exact`) | PASS, 1/1 | Persisted unsupported MV is rejected by every node before and after restart |

The exact invocations and timings are repeated in the Cycle 0 review. At that baseline, real
multi-process soak, MinIO/object-store integration, Kafka/Docker, and server HTTP/Flight admission
suites were not run. Cycle 55 later runs only the narrow one-broker Kafka protocol probe recorded
below; it is not a LaminarDB pipeline or soak. The ADR/plan consequently forbid a production-ready
claim until an independently reviewed black-box release-candidate soak passes with real
source/object-store/sink dependencies.

The current branch's admission-neutral hardening was then checked separately:

| Current-branch check | Result | Evidence |
|---|---:|---|
| Core Cycle 4 corrective-tree feature checks | PASS | `cargo check -p laminar-db --no-default-features` and the same command with `--features cluster` passed |
| Core Cycle 4 raw vnode-partial wire tests | PASS, 7/7 | Includes a frozen established global-aggregate DELTA fixture that must remain byte-exact until a version-fenced reader/writer migration exists |
| Core Cycle 4 sealed chain/backend tests | PASS, 15/15 DB and 3/3 core | Exact validated-head reuse, attempt substitution rejection, per-artifact read bounds, full/reference/delta chains, missing seals, retention, and built-in exact sealed reads |
| Core Cycle 4 graph and boot staging tests | PASS, 112/112 graph and 8/8 boot | Complete-attempt preflight, mixed-attempt rejection, callback/activation containment, and exact boot roster/attempt/assignment staging |
| Core Cycle 4 focused coordinator regressions | PASS, 4/4 | Reference reuse, bounded delta depth, mixed-delta reference exclusion, and overlapping-attempt isolation retain the established raw wire |
| Core Cycle 4 warnings-denied Clippy and formatting gates | PASS | Both no-default and cluster Clippy matrices plus `cargo fmt --all -- --check`, current diff, and five-file relative-link checks pass |
| Core Cycle 4 full DB/default-feature suite | **NOT RUN / NO PASS CLAIM** | A broad Windows invocation did not yield a suite result; it timed out/broke its pipe only after the test binary linked. No full-suite, latency, RSS, backend, or soak evidence follows |
| Core Cycle 5 immutable-attempt contract | PASS for Laminar-controlled built-in lifecycle | The trait fixes one inventory per namespace-scoped attempt and makes retirement irreversible; object-store CAS/floor regressions and the in-process reference floor reject reseal/republish. External live-seal deletion and custom-backend conformance remain production gates |
| Core Cycle 5 sealed lineage reader | PASS, 17/17 | Includes retained-child restore after a conflicting parent reseal, exact FULL/reference/delta recovery, missing/pruned-parent failure, max-four/max-three artifact enforcement, and the current six/seven production boundary |
| Core Cycle 5 feature/lint checks | PASS | Cluster test targets compile; warnings-denied cluster Clippy, focused policy tests, formatting, and diff checks pass |
| Core Cycle 5 aggregate restore bytes/RSS/latency/soak | **OPEN / NO PASS CLAIM** | No public cap was added because current seal metadata cannot prove transitive bytes; no full suite, latency, RSS, backend qualification, or independent soak ran |
| Core Cycle 6 immutable transition model | PASS, 9/9 | Exact boot/live origin, cut/pipeline/assignment/roster binding, incarnation replacement, missing-chain rejection, and committed final-owner authority |
| Core Cycle 6 graph transition module | PASS, 119/119 | Complete structural preflight, exact-Arc retention, delayed activation, authority/lifecycle drift, callback failure, cancellation/panic, terminal backpressure, and installed-state poison invalidation |
| Core Cycle 6 boot transition module | PASS, 10/10 | Prepare-before-replace, exact target/cut publication, and audited zero-owned target retirement |
| Core Cycle 6 initialized aggregate authoritative replacement | PASS, 2/2 focused | Full restore removes post-cut keys absent from the committed vnode image; a chain labeled for another vnode is rejected without mutation |
| Core Cycle 6 full cluster-feature library suite | PASS, 1,787 passed / 0 failed / 1 profiling test ignored | Includes durable-authority fixtures for final-owner rejection and boot recovery assignment history |
| Core Cycle 6 cluster test-target compilation | PASS | `cargo check -p laminar-db --tests --no-default-features --features cluster` passed after the integrated transition and poison changes |
| Core Cycle 6 rebalance module and final lint matrix | PASS | Rebalance passed 31/31; warnings-denied cluster/no-cluster Clippy, formatting, and diff checks passed after final fixes |
| Core Cycle 7 full no-cluster library suite | PASS, 1,310 passed / 0 failed / 1 profiling test ignored | Embedded/single-node deterministic regression coverage after the shared managed-state initialization changes; not mode-parity or soak evidence |
| Core Cycle 7 full cluster-feature library suite | PASS, 1,811 passed / 0 failed / 1 profiling test ignored | Deterministic library recertification after managed-state initialization, exact participant rosters, replacement-process adoption, stale-process drain fixes, and vnode-state readiness gating; not soak evidence |
| Core Cycle 7 serialized full cluster integration target | PASS, 23 passed / 0 failed | Rerun with one Cargo job after `142dadf7`; includes committed global aggregate and source-cut restore across a stale-drain Abort generation, continuation under fresh process incarnations, and a subsequent checkpoint bound to the replacement assignment; not an independent soak |
| Core Cycle 7 serialized embedded recovery integration target | PASS, 3 passed / 0 failed | Run with one Cargo job; checkpoint hard-kill restart and no-loss recovery stayed green without cluster features; not mode-parity or soak evidence |
| Core Cycle 7 serialized server binary suites | PASS, 238/238 without cluster and 316/316 with cluster | Run with one Cargo job; covers both configuration/startup surfaces, including fail-closed delivery policy and replacement startup choreography; not a real-process soak |
| Core Cycle 7 final lint/document hygiene | PASS | Warnings-denied DB/server Clippy passed with and without cluster; formatting and diff checks were green at the recorded gate |
| Core Cycle 7 exact fail-closed sentinels | PASS, 6/6 | Cluster EO, query-shape admission, fixed operator inventory, server durability scope, and source/sink capability matrices still reject before unsupported runtime work |
| Core Cycle 7 ownerless-latest recovery correction | PASS, 38/38 focused tests plus strict sentinel and cluster Clippy | `142dadf7` makes only a nonparticipant rebuild its gated connector from the latest durable head and clear a stale target; owners remain exact. The strict test still rejects an explicit recovery target E when Commit E+1 is authoritative. The complete connector-bearing E/E+1 race remains open |
| Core Cycle 7 pre-fix real three-node ALO hard-kill/rejoin engineering soak | **FAIL** | Subject SHA-256 `bef33cb62c397dc6486d062d09d4c0f5df808eb8124a11e986614d8629a41904`; logs `target/tmp/soak-942164-1785198920573674300`. It exposed an over-strict acquisition-handoff comparison, a recursive audit inconsistent with bounded retained lineage, and successor publication overtaking remote unapplied state. This failed attempt is retained and is not certification evidence |
| Core Cycle 7 second real three-node ALO hard-kill/rejoin engineering soak | **FAIL** | Subject SHA-256 `91fe9d2c2ef734e1fad4f148da9e69a2eebed92057a74b70702b9b77fb490b10`; logs `target/tmp/soak-989812-1785220503643922100`. Survivors reached checkpoint 5 after the leader kill, but the restarted ownerless process timed out in a Release-E/newer-Commit-E+1 recovery loop. Strict `[LDB-6041]` rejected the rewind as designed; this is retained failure evidence, not certification |
| Core Cycle 7 executable-digest preflight rejection | PASS for harness protection; **NO RUNTIME RESULT** | An attempted subject whose SHA-256 began `5ee...` did not match the configured digest, so the harness stopped before spawning LaminarDB. It is neither a runtime failure nor soak evidence |
| Core Cycle 7 post-`142dadf7` real three-node ALO hard-kill/rejoin engineering rerun | **PASS engineering gate** | Server SHA-256 `089197fa82d20cc9c83118036d8820d3168f6c3752125f05c6677ac6418f81d5`; harness SHA-256 `b88df679bc4acc93739a4d4462bf208ac9c3b684bc58afc3f09a62a5d096b7b6`; logs `target/tmp/soak-996548-1785223178826043700`. Runtime 339.97 s; three kill/rejoin convergence pairs 43.721/36.441, 37.089/30.020, and 34.586/31.053 s; end-of-steady-soak checkpoint 170/epoch 170; frozen durable input cut checkpoint 187/epoch 187; 400 rps target; all 132,899 expected IDs observed with 7,898 ALO duplicates; 100% of 514 observed stalls at or below 1,024 ms; zero deadline/SLO violations. Engineering only; the deterministic E/E+1/passive-connector race and independent immutable RC soak remain open |
| Core Cycle 8 aggregate transition and full cluster library | PASS, 1,826 passed / 0 failed / 1 profiling test ignored | Includes prepare-all/abort-all, normal and final-owner publication, publication-panic poison/no-deadlock, corrupt-chain unchanged logical state, exact max-groups, changelog dirtiness, rebase cleanup, and post-initialization capability-drift regressions; not a resource, latency, backend, or admission result |
| Core Cycle 8 serialized cluster integration target | PASS, 23 passed / 0 failed | The first parallel run passed 21/23 but two tests timed out waiting for the harness shared-namespace proof; the complete serial rerun passed both affected cases and all 23 tests. This is deterministic integration evidence, not an independent soak |
| Core Cycle 8 feature/lint and fail-closed gates | PASS | Cluster and no-default DB checks and warnings-denied Clippy passed; the cluster server feature check passed; formatting/diff hygiene passed; exact `[LDB-4007]` query-shape and two `[LDB-0013]` delivery/build sentinels passed |
| Core Cycle 9 aggregate net-growth boundaries | PASS | Exact-limit, max-plus-one, equal-cardinality, arithmetic, and rollback regressions passed; this bounds avoidable live-map reserve growth, not total prepared/retired RSS |
| Core Cycle 9 V3/seal-8 built-in lineage contract | PASS, 133/133 core state tests | Includes immutable payload-plus-lineage idempotence, literal V3 header offsets/raw suffix, lineage shape/arithmetic/overflow, V2 and seal-7 reset rejection, and corrupt/truncated headers |
| Core Cycle 9 restore/staging lineage containment | PASS, 24/24 rehydration and 11/11 staging tests | Includes exact/max-plus-one rejection before body I/O, configured artifact boundary, exact child-parent and decoded-base checks, reference-only usage, success single-flight, transient retry, and inadequate-receipt rejection before graph/operator semantic decode or apply |
| Core Cycle 9 coordinator lifecycle | PASS, 5/5 focused regressions | Includes atomic promotion, revoke/reacquire reset, late old-attempt completion, and rejection of aborted/unsealed FULL or REFERENCE lineage as a successor base |
| Core Cycle 9 feature/lint/server gates | PASS for completed targets | Cluster DB test check; DB no-default and no-default-plus-cluster checks; warnings-denied core all-features and DB cluster library Clippy; minimal server checks/build-only cluster target; formatting and diff hygiene passed. The `[LDB-4007]`/`[LDB-0013]` admission markers have no diff |
| Core Cycle 9 broad Windows matrices | **NOT COMPLETED / NO PASS CLAIM** | A broad non-library DB build exhausted the Windows paging file; broad default-feature server commands stalled in Cargo and timed out. Neither produced a completed suite result; rerun on a sufficiently provisioned host |
| Core Cycle 10 commit-domain lineage authority | PASS for focused current-profile scope | Participant-agreed limits and exact cluster totals, complete pre-Commit and pre-body parent-seal traversal, Abort/non-promotion on invalid ancestry, Commit-only parent promotion, affected coordinator/committer/rehydration/staging suites, feature checks, and warnings-denied Clippy passed. Broad suites and current-binary soaks were not run |
| Core Cycle 11 raw restore-input ownership | PASS, 40 focused lifecycle/resource tests plus build/lint gates | Exact acquired-subset byte/artifact reservation before body GET, 32 shared body-read permits, in-flight cancellation/drop, publication/poison/failed-launch/replacement release, pointer-exact successor preservation, cluster and no-cluster checks, and warnings-denied Clippy passed. No backend, admission, latency/RSS result, or soak follows |
| Core Cycle 12 legacy outer-decode bound | PASS, 11 codec, 27 rehydration, 24 vnode-transition, and focused graph regressions plus build/lint gates | Checked borrowed rkyv validation and the committed one-entry ceiling precede owned outer deserialization at loader and graph boundaries. Real head/parent ancestry, zero-roster, duplicate-name, multi-participant test protocol, unaligned/truncated input, no-default/cluster checks, and warnings-denied Clippy passed. Inner decode/RSS/pause, backend, admission, delivery and soak remain open |
| Core Cycle 13 immutable aggregate key-group identity | PASS, 15 vnode-lifecycle, 19 SQL operator, 72 active cluster aggregate, 62 active local aggregate, and focused admission regressions plus build/lint gates | Typed construction identity is exact in local and cluster modes; global state keeps N while routing to vnode zero; first/full/delta/transition mismatch paths fail before bookkeeping, decode, or logical mutation. No wire, backend, delivery, or admission change. One broader operator-graph filter timed out during Windows linking and has no pass claim; soaks remain paused |
| Core Cycle 14 vnode-sharded aggregate working state (`32dcec29`) | PASS for the focused slice: 63 active no-default aggregate tests (1 ignored), 74 active cluster aggregate tests (1 ignored), 16 vnode lifecycle, 21 SQL delta/replay, exact `[LDB-4007]` and `[LDB-0013]` sentinels, two exact core transport tests, and one aligned-graph regression | Lazy coherent vnode slots, O(active) traversal, direct vnode capture, off-side restore, transitioned-slot pointer-swap publication, and uniform route-hint reuse pass. DB no-default/cluster and core cluster Clippy pass with `-D warnings`; formatting and diff hygiene are clean. No broad workspace/integration/soak run, backend, wire, delivery, or admission change. The no-default library-test target emitted only pre-existing warnings |
| Independent immutable release-candidate soak | **NOT RUN** | Production certification remains unavailable |
| Core Cycle 3 `operator_graph::tests` with cluster | PASS, 111/111 | Full graph unit module after opaque final-exit authority, rotation-fence, endpoint, restore-drift, assignment-drift, callback-failure, and sticky-poison checks |
| Current `final_owner_exit` focused filter | PASS, 11/11 | Audited committed last-vnode cleanup succeeds only for exact predecessor/target authority. Revoke-only exits own no raw restore-input charge; callback failure and post-callback restore, transport, or assignment drift poison the graph while retaining durable staging for retry |
| `snapshot_watcher_handles_draining_phase` multi-node integration | PASS, 1/1 | A zero-vnode follower completes committed revoke without polling its source, remains intake-fenced and non-authoritative, and exposes inactive transport endpoints; not an independent soak |
| Core Cycle 3 feature/lint matrices | PASS | `cargo check` and warnings-denied Clippy pass with no default features and with `cluster`; formatting and diff checks pass |
| `state::partition_key::tests` | PASS, 15/15 | Typed row/hash/vnode ABI plus bounded routing-only schema identity, every admitted family golden, alias/order/nullability policy, dictionary hydration, and exact resource/type gates |
| `cargo test -p laminar-core --lib` | PASS, 562/562 | Complete core library regression set after the Cycle 3 safety follow-up |
| Generic strict IPC experiment | REMOVED after review | Arrow 57.2 parses attacker-declared lengths too early; artifact-specific preflight, a global encoded-byte charge, and separate task/global scratch charges remain explicit Phase 0 blockers |
| `aggregate_state::vnode_partition_tests` (cluster lib-test binary) | PASS, 4/4 | Existing raw capture/merge/idempotence plus new shuffle/capture/drop parity and pre-mutation drift rejection; not keyed-envelope validation |
| `aggregate_state::tests::drop_vnodes_purges_revoked_keeps_sibling` | PASS, 1/1 | Revoke retains sibling-vnode state after the fallible count check was added |
| `aggregate_state::tests::global_changelog_delta_checkpoint_roundtrips` | PASS, 1/1 | The admitted global aggregate remains pinned to vnode 0 |
| `db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived` | PASS, 1/1 | The `[LDB-4007]` feature matrix remains closed while stateless/global shapes remain admitted; two independent global aggregate streams prove admission is per query |
| `aggregate_state::tests::embedded_float_grouping_remains_supported_without_partition_codec_gate` (`--no-default-features`) | PASS, 1/1 | Embedded planning and execution still accept a float key excluded from cluster partition ABI v1 |
| `operator_graph::tests::later_restore_callback_failure_poisons_and_retires_exact_transition` | PASS, 1/1 | Preserves containment for the legacy `cfg(test)` mutation probe: an earlier callback can mutate, but the exact failed transition and raw-input charge are retired, no acquired vnode activates, vnode lifecycle remains `Restoring`, installed authority is cleared, and the generation is sticky-poisoned. A newer replacement would be preserved. Production `SqlAggregateV1` uses the prepared protocol instead |
| `operator_graph::tests::checkpoint_quiescence_requires_pending_vnode_transitions_to_apply` | PASS, 1/1 | Acquire, revoke, and assignment-version changes block both snapshot APIs until graph drain applies the transition |
| `pipeline_callback::tests::source_less_leader_holds_rotation_fence_through_whole_and_vnode_capture` | PASS, 1/1 | Production leader/source-less callback holds the existing rotation read token across both mutable capture callbacks and releases it before the tail |
| `pipeline_callback::tests::source_less_immediate_follower_holds_rotation_fence_through_capture` | PASS, 1/1 | The real source-less `CaptureNow` follower route holds the token through both images and releases it before its blocked durable tail |
| `pipeline_callback::tests::retained_follower_capture_keeps_ownership_after_promotion` | PASS, 1/1 | The deferred source-barrier follower route preserves attempt cleanup while releasing the token before its blocked durable tail |
| `pipeline_callback::tests::busy_serialization_gate_rejects_synchronously_before_mutating_operator_state` | PASS, 1/1 | A retained encoder causes immediate `[LDB-6017]`; destructive graph state and the current fault slot are unchanged |
| `pipeline_callback::tests::busy_serialization_gate_does_not_hold_assignment_writer_until_timeout` | PASS, 1/1 | A queued assignment writer is blocked by capture ownership, then proceeds as soon as the rejected capture drops its token rather than after the serialization timeout |
| `pipeline::streaming_coordinator::tests::recovery_cycle_error_faults_best_effort` | PASS, 1/1 | A recovery-classified partial apply publishes no output/cut and destroys the callback/graph generation before returning |
| standalone Kafka transaction probe unit tests | PASS, 6/6 | Transactional-ID golden/axes/bounds, probe-local encoder checked against the copied frozen data-header golden, marker linkage, exact visibility transcript, hostile CLI/identity/topic bounds |
| standalone Kafka transaction probe, debug and optimized binaries | PASS, repeated | Redpanda v26.1.13/RF=1: exact three-partition inventory/fanout, fatal `Fenced`, confirmed abort/retry, fence-aborted open transaction, separate RC/RU visibility captures, and exact header/key/payload capture; protocol-only, not latency/durability/soak evidence |
| Cycle 57 core/HTTP local-authority gates | PASS | 5/5 core tests, 6/6 cluster HTTP tests, 1/1 non-cluster test, both server feature matrices and core warnings-denied Clippy; exact schema/auth/bounds/stale-boot/live-fence/lease-race coverage |
| Cycle 57 exact-cut classifier | PASS, 1/1 | Stable success; changing/draining durable head, trailing/missing/ahead/conflicting local adoption, and duplicate-process branches; `cluster,kafka` warnings-denied Clippy passed |
| Cycle 57 real static run, one leader kill, zero-second tail | **FAIL terminal gate** | Exact survivor/rejoin convergence passed in 44.95/34.21 s and all 43,473 IDs reached the ALO sink; only 72 node0 latency observations were available versus the existing minimum 100 |
| Cycle 57 real static run, one leader kill, 90-second tail | **FAIL terminal gate** | Exact survivor/rejoin convergence passed in 43.46/37.53 s and all 80,260 IDs reached the ALO sink; node1 met the 1024-ms stall bound for 98.81% of 168 observations versus the required 99.00% |
| Cycle 58 checkpoint-attempt authority audit | PASS for design boundary; no runtime claim | Exact outcome/capsule retention, two-floor compaction, orphan-capsule, source-offset, lookup-side-effect, timing, and response-bound limits were mapped; implementation stopped before an untruthful route |
| Cycle 59 exact barrier-timing implementation | PASS, focused/unit/lint gates | Preallocated three-family ledger, protected bounded route, exact process plus sampled-converged-version certificate binding, bounded-memory collector, JSONL streaming through coherent observed cuts, safe exported eviction, unread-loss rejection, and exact Prometheus reconciliation |
| Cycle 59 real static run, one leader kill, 90-second tail | **PASS engineering gate** | All 79,996 IDs present; 2,758 duplicate IDs tolerated/counted but not proven byte-identical legal replay; 392 exact records across four process generations; zero deadline exhaustion and 100% membership in all three diagnostic `le=1.024` buckets; existing pipeline-stall gate passed; 43.43-s failover and 34.51-s rejoin; not an instrumentation A/B or independent release soak |
| Cycle 60 coherent-cut retry proof | PASS, focused/full/lint gates | Metrics-only and exact-cursor/metadata-only instability independently force retries before one stable finalization; test-target seam only |
| Cycle 60 instrumentation A/B | DESIGN ONLY; not run | Frozen v1 separates recorder installation from polling with common metrics and fixed workload/fault anchors; it estimates effects only and still needs a separate driver plus powered v2 |
| Cycle 61 executable provenance seam | PASS, focused/full/lint gates; no real-process run | Canonical regular-file path plus exact SHA-256 fail closed on partial configuration, relative/missing/unreadable/non-regular paths, malformed or mismatched digest, and post-resolution mutation; executable permission/format/architecture remain OS spawn checks |
| Cycle 62 schedule scaffold | PASS for logical schedule isolation; 10 tests passed, one ignored subprocess fixture; no real-process run | Separate driver/observer bytes, raw-manifest-bound common plan, exact file-synced end seal, capped pipe retention, bounded kill/reap, and identical C/D traces across four observer outcomes; static planned probes only, with no HTTP, SUT, workload execution, timing, or A/B evidence |
| Cycle 63 diagnostic-read authority | DESIGN ONLY; no runtime or empirical sample | Selects a disjoint startup-bound credential on exactly two loopback diagnostic GETs; rejects the unrestricted-bearer broker and identifies parse-error disclosure plus restart-only reload publication as prerequisites |
| Cycle 64 diagnostic-read boundary | PASS for in-process configuration, route, race, and reload tests; no real-process sample | Implements the disjoint immutable credential, exact two-route allowlist, post-auth permit/rate/deadline bounds, route-template logs, parse-input redaction, and restart-only reload retention; 238 no-cluster and 316 cluster tests pass |
| Cycle 65 standalone observer protocol | PASS for the bounded fake-only component; live/driver integration blocked | Result v2 binds sanitized plan v2; bootstrap v3, strict direct HTTP/cursor/identity validation, explicit incomplete exit, and cancellation pass. Owned child-process tests prove zero C connections and 348 parsed D responses; no LaminarDB process was contacted |
| Cycle 66 sealed fake-protocol supervisor | PASS for bounded fake-only consumption; live/production blocked | Bootstrap v4/result v3 add fresh invocation binding; the manifest-pinned child is captured, cancelled, and reaped within bounds, and its result is consumed only after the identical common end seal. 38 tests pass; no LaminarDB request or measurement ran |
| Cycle 67 paced observer/evidence contract | DESIGN ONLY; live/production blocked | Freezes absolute monotonic pacing, parallel node lanes, cross-slot rate shaping, complete transcript evidence, observed-prefix timing coverage, launcher-prebound loopback sockets, and a separate future mTLS listener; no code, request, or measurement |
| Cycle 68 paced owned-fake primitives | PASS for the partial library boundary; live/production blocked | Externally bound fake plan/READY, byte-golden START/ACK, anchor-after-decode ordering, absolute timing cuts, and atomic seven-start shaping pass 20 focused tests; no executable, network, evidence/result framing, lane, or measurement |

For Cycle 7, the deterministic suite results above are claimed at their recorded scope. The first
three-node ALO engineering soak is a retained failure, not evidence to average away: subject
SHA-256 `bef33cb62c397dc6486d062d09d4c0f5df808eb8124a11e986614d8629a41904`, logs
`target/tmp/soak-942164-1785198920573674300`. Its checkpoint-17/assignment-2 recovery was compared
against the acquisition handoff at checkpoint 2/assignment 1; bounded retention then made a
recursive predecessor audit impossible, while a successor assignment could publish before one
remote participant had applied the current assignment.

The second real run is also retained as a failure: subject SHA-256
`91fe9d2c2ef734e1fad4f148da9e69a2eebed92057a74b70702b9b77fb490b10`, logs
`target/tmp/soak-989812-1785220503643922100`. Its survivors reached checkpoint 5 after the leader
kill, but the ownerless restarted process repeatedly tried to recover Release E after the owners
had durably committed E+1. Strict `[LDB-6041]` correctly refused that older explicit target. Commit
`142dadf7` makes only the passive nonparticipant clear the stale target and recover latest while
gated; owners remain pinned to their exact released cut. The 38/38 focused gates, strict sentinel,
cluster Clippy, and serialized 23/23 integration target pass at that scope. The full deterministic
Release-E/Commit-E+1/passive-connector interleaving is still open. The next real three-node run
passed its engineering gate: server SHA-256
`089197fa82d20cc9c83118036d8820d3168f6c3752125f05c6677ac6418f81d5`, harness SHA-256
`b88df679bc4acc93739a4d4462bf208ac9c3b684bc58afc3f09a62a5d096b7b6`, logs
`target/tmp/soak-996548-1785223178826043700`, 339.97-second runtime, and three kill/rejoin pairs of
43.721/36.441, 37.089/30.020, and 34.586/31.053 seconds. End-of-steady-soak progress was checkpoint
170/epoch 170, and the frozen durable input cut was checkpoint 187/epoch 187. It observed all
132,899 expected IDs plus
7,898 ALO duplicates at a 400 rps target, kept all 514 observed stalls at or below 1,024 ms, and
recorded zero deadline/SLO violations. The earlier `5ee...` executable hash mismatch was a pre-spawn
harness rejection, not a third runtime result. This pass is engineering-only: it does not exercise an
admitted distributed keyed SQL operator or bounded working-state backend, prove exactly-once or
production latency/RSS, close complete delivery composition, or replace the **NOT RUN** independently
operated immutable release-candidate soak.

Cycle 57's real commands used a Windows optimized soak-profile binary with Docker Desktop's WSL2
engine, static discovery, MinIO, Redpanda v26.1.13, 64 key groups, 96 input partitions, shared
S3-compatible state/checkpoint prefixes, and 400 rps. They are engineering evidence, not the
independent immutable release subject. The second run's aggregate histogram cannot identify the two
violating attempts or correlate their stages with assignment and terminal outcome; rerunning merely
to dilute the ratio was rejected.

Cycle 59 retained the earlier failures and corrected the missing measurement authority instead of
changing the threshold. Its optimized Windows/WSL2 engineering subject at `7782a032` used the same
static three-node/MinIO/Redpanda class of environment with a unique S3 prefix. It passed exact
ledger/Prometheus reconciliation and emitted four per-generation JSONL files (392 records total).
The current Kafka sink remained nontransactional and the oracle tolerated and counted 2,758
duplicate output IDs without proving their byte identity or sealed-cut replay legality. The result
therefore does not satisfy the charter's at-least-once duplicate-legality oracle and does not close
`[LDB-0013]`. The later `1a6dff80` substitution defense has focused deterministic test/lint
coverage but was not part of the empirical subject.

Cycle 60 adds no empirical sample. Its deterministic finalizer test proves that metrics-only and
exact-cut-only instability independently force retry. The direct nonempty HTTP route test remains a
documented composition gap because production-path seeding would recreate an entire live cluster;
no injection API or fake handler was added. The frozen instrumentation A/B v1 uses common metrics,
a content-addressed input trace, fixed manual-checkpoint fault ordinal, balanced temporal blocks,
and named nonadditive contrasts. The current coupled soak harness cannot execute it, and v1 cannot
support an equivalence conclusion.

Cycle 61 also adds no empirical sample. Four executable-resolution tests and the complete harness
passed: 38 Kafka-feature tests were non-ignored and two real-process tests remained ignored. The
[charter](../testing/distributed-state-production-soak-charter.md#cycle-61-executable-binding-seam)
retains the timing, staging, and release-gate limitations.

Cycle 62 also adds no empirical product sample. Its standalone Windows suite passed ten tests with
one ignored subprocess fixture, strict Clippy, formatting, and diff checks. Those tests exercise
schedule serialization and supervisor failure isolation only. The
[scaffold contract](../testing/distributed-state-production-soak-charter.md#cycle-62-schedule-scaffold)
keeps live HTTP blocked because the current console bearer grants both diagnostic-read and
checkpoint/pipeline mutation authority. The independent immutable release-binary soak has still
not run.

Cycle 63 adds no empirical product sample either. Source inspection confirms that one protected
router currently grants the console bearer access to diagnostics and mutations; `[server]` is
labelled restart-only but can be republished by successful reload, while checkpoint forwarding
retains the startup console token; and TOML parse errors can preserve substituted source text before
`Secret` redaction exists. The
[selected contract](../testing/distributed-state-production-soak-charter.md#cycle-63-diagnostic-read-authority-decision)
requires parse-error redaction, reloadable-section-only commits, and a disjoint immutable
diagnostic policy before a live observer can be written. No route or guarantee changed, and the
independent immutable release-binary soak has still not run.

Cycle 64 adds executable boundary evidence but no empirical product sample. Full in-process server
matrices pass without and with the cluster feature (238/238 and 316/316), including both principals,
all registered console routes, hostile credential/target aliases, method/CORS isolation,
single-flight/rate/timeout/cancellation behavior, shared file/programmatic validation, and pure or
mixed successful/failed POST and watcher reload. Matched route templates replace raw request-target
logging, and stripped TOML input keeps substituted sentinels out of all downstream error formatting.
No socket-level observer, real cluster, workload, fault, latency A/B, backend, or independent soak
was exercised. Loopback still limits this slice to co-located engineering use, and
`certification_eligible` remains `false`.

Cycle 65 also adds no empirical product sample. The root-workspace-excluded observer and owned
Windows listeners exercised six bounded child-process cases: C with an open supervisor pipe and no
connections; complete D with 348 parsed responses; incomplete D with serialized disposition and
nonzero exit; bootstrap deadline; stalled-read cancellation; and unsupported-environment redaction.
Loopback addressing is enforced, but fake-process identity is a property of the test-owned setup,
not an executable attestation. The common sealed driver still uses its legacy dry-run observer, so
network-mode non-feedback and result consumption remain open. No LaminarDB server, workload, fault,
latency A/B, backend, or independent soak was exercised; `certification_eligible` remains `false`.
The accelerated 58-slot execution is not evidence for the 0..285-second cadence or live diagnostic
start-rate limit; those require a separately versioned paced integration path.
Likewise, the process-local timing ledger cannot reveal records appended after the last poll and
lost at restart; the fake result is not a durable-tail continuity certificate.

Cycle 66 also adds no empirical product sample. Owned local children prove fresh replay-resistant
fake result consumption after the same sealed trace, bounded failure cleanup, partial-secret
bootstrap deadlines, and absence of raw child streams from artifacts. Loopback ownership remains a
harness property, the path still accelerates all slots, and no LaminarDB server, workload, fault,
latency A/B, backend, delivery composition, or independent soak was exercised.

Cycle 67 adds no empirical sample. Source audit shows that serial 4.5-second node budgets cannot fit
the five-second cadence, per-slot connection caps do not protect the server's rolling window across
slot boundaries, and a drained process-local timing page cannot reveal a later record/loss/in-flight
guard erased by process death. The accepted contract uses parallel absolute node lanes, a persistent
client rate shaper, and explicitly open/unsealed process-prefix coverage. It also rejects loopback
address, gossip RPC address, current shared-name cluster mTLS, DNS, or PID alone as endpoint
identity. No code, LaminarDB request, workload, fault, A/B, backend, delivery change, or independent
soak ran; `certification_eligible` remains `false`.

Cycle 68 also adds no empirical product sample. The root-workspace-excluded tool gains a library-
only `paced-owned-fake` contract and clock/rate primitives in `1b6a06ed`; it cannot open a socket.
All 58 active standalone tests pass on Windows (one subprocess fixture remains intentionally
ignored), along with warnings-denied Clippy and formatting. Three independent reviews approve the
bounded primitive slice and explicitly exclude result/transcript validation, HTTP delivery-stage
handling, three persistent lanes, child/supervisor spooling, the 290-second C/D pair, release-
process preflight, A/B, backend, delivery, admission, and independent soak. Fixture READY is not
launcher listener-adoption evidence, and `certification_eligible` remains `false`.

These are admission and focused operator tests, not a production certification. No existing test
demonstrates a distributed keyed operator processing remote rows through crash, restore, and
rebalance, because that path is deliberately unreachable.

## The missing capability, precisely

LaminarDB does **not** need a second cluster coordinator or a generic “checkpoint system.” It needs
a managed keyed working-state capability connected to the distributed execution contract:

1. **Stable distribution ABI:** one canonical typed key codec shared by shuffle, state, and restore;
   exact encoded-byte/hash/vnode vectors; fixed vnode count; stable operator and state-table IDs;
   schema/version compatibility; artifact-to-vnode membership validation; and an explicit
   global-vnode convention.
2. **Batch hot-state API:** local low-latency point/multi-get, prefix/range scan, atomic write batch,
   timer index, range delete, and truthful memory/disk accounting. The public record path must not
   await object storage per row.
3. **Bounded resources:** reservations before mutation and hard limits covering state-engine
   caches/write buffers, operator side state, dirty/frozen generations, timers, retained output,
   Arrow batches, and compaction debt; bounded backpressure followed by a controlled fault instead
   of OOM.
4. **Checkpoint bridge:** atomic state mutation plus dirty-journal update, cheap generation freeze
   at an aligned barrier, asynchronous full/delta artifact production, exact-attempt sealing, and
   safe retry/rebase after failed capture.
5. **Ownership lifecycle:** preserve one exact committed-cut/target-assignment transition; consume
   the landed authoritative participant roster and named decoded-empty FULL payloads; preflight all
   acquired/revoked vnodes and prepare every operator off-side; publish only infallible graph-wide
   shard swaps under the execution fence; activate the complete set once; retain the exact
   transition on failure; fence the old owner before revoke; and drop local ranges only after
   authority changes.
6. **Operator contracts:** aggregates externalize accumulators/emission state; windows externalize
   event-time timers and firing state. Dual-live joins declare both inputs' distributions, indexed
   sides, and cursors; stream-reference joins declare one live distribution/cursor plus exact
   reference identity or durable pending/response state. Both declare applicable
   frontiers/timers/retention, matched or emitted-output state, and append/upsert/changelog delivery
   mode. This applies independently to every persistent streaming-state family and finite planned
   form; syntax or translator configuration alone is not an operator contract.
7. **Materialized output lifecycle:** separately partition, checkpoint, restore, and serve retained
   MV output before cluster MVs can be admitted.
8. **Delivery composition:** bind supported source handoff, operator state/timers/output identity,
   compatible sink mode, and checkpoint-tail ordering; keep unsupported changelog and exactly-once
   combinations fail-closed.
9. **Proof:** differential semantics, deterministic fault injection at every cut, skew and quota
   tests, process-death/rebalance output oracles, recovery compatibility, and published latency and
   resource profiles.

Core Cycles 6–14 implement the transition identity, authoritative participant roster, named FULL
payloads whose decoded aggregate image is empty, placement scoping, structural preflight, aggregate
semantic prepare-all/abort-all/infallible publication, net live-growth reserve, and current-path
sealed raw-lineage receipt and Commit-domain authority subsets of item 5. Cycle 10 validates the
complete cluster-global raw lineage before Commit and binds the exact totals and participant-agreed
limits into the durable capsule. Restore reproduces the full metadata proof before bodies and
preflights its acquired subset under the same contract. A rejected sealed attempt cannot become a
successor parent. Cycle 11 atomically reserves that acquired subset's declared raw lineage before
body reads and transfers the charge through loaded and pending-transition ownership until exact
publication or terminal retirement. One worker request pool and absolute deadline/cancellation
scope bound the read phase. Cycle 12 then enforces the current one-entry legacy outer roster through
a checked borrowed archive before owned deserialization at every production restore decode site.
Cycle 13 makes aggregate routing cardinality immutable across construction, capture, restore, and
rebalance, while preserving global vnode-zero semantics under a multi-key-group topology. Cycle 14
then gives the aggregate a coherent physical vnode layout, direct shard capture, off-side shard
construction, and fenced pointer-swap publication. Existing exact route metadata skips the
redundant aggregate partition hash for uniform batches; mixed/configless input retains a per-unique-
group hash fallback.

They do not complete the capability. The current resource-owning slice is deliberately limited to
the admitted global-singleton compatibility profile and its declared raw lineage. Wrapper/seal
metadata, allocator/response overhead, archive validation and alignment-copy memory, inner decode
scratch/expansion, decoded RSS, simultaneous live/prepared/retired residency, and publication/
retirement pause remain unreserved. The aggregate is vnode-sharded, but its fixed slot table and
active/replacement/retired working sets do not yet have a measured or enforced live-byte/RSS
envelope. Uniform route hints rely on trusted internal routing metadata; mixed/configless and
restart/replay paths still require explicit hash/latency characterization. No window/timer or join
state family consumes the lifecycle. The hot-state service/backend, delivery composition,
operator-specific contracts, byte-identical checkpoint compatibility proof, current-binary
failover EO/ALO recertification, and independent production proof in items 2–9 also remain open.

The existing `StateBackend` must not be mistaken for item 2. Its contract explicitly persists
immutable per-checkpoint-attempt vnode artifacts and an exact-attempt durability seal. It has no
hot get/put/scan/write-batch/timer interface and is correctly kept as remote recovery authority.

## Documentation audit

| Artifact | Disposition | Reason |
|---|---|---|
| former `docs/AGENT_KNOWLEDGE.md` | Removed in `0f4b37ff` | Stale Claude handoff memory described completed transport/barrier work as future work and was not authoritative project documentation |
| `docs/plans/checkpoint-production-correctness-2026.md` | Keep, cross-link | Current and relevant; correctly distinguishes checkpoint artifacts from hot keyed state |
| `docs/plans/cluster-production-readiness.md` | Keep, narrow | Relevant umbrella plan; the focused ADR/plan becomes the source of truth for keyed operators |
| `docs/ARCHITECTURE.md` | Corrected in `706270dc` | Now distinguishes immutable checkpoint artifacts from absent live keyed state |
| `docs/SQL_REFERENCE.md` and `README.md` | Corrected in `706270dc` | State the exact stream/global-aggregate/MV boundary |
| former `.claude/fix-plans/state-backend.md` | Removed outside the tracked tree | Stale Claude plan conflated artifact `StateBackend` with live state and proposed unrelated control-plane work; none of it is used as design authority |
| private `docs/research/extensible-schema-traits.md` and `schema-inference-design.md` | Reject as current evidence; public copies already removed in `52daf683` | They describe removed/unwired registries plus unsupported DDL, format, inference, zero-copy, and latency claims; neither reflects the current connector implementation |

No tracked `docs/research` artifact exists on this baseline, and the ignored `docs/research`
junction is currently empty. The two obsolete private files previously named here are no longer
present. Cycle 57's tracked-document audit found no further obsolete research file: backend reports
remain reverse-linked frozen decision/regression provenance rather than active recommendations.
Relevant primary sources live as citations in the ADR, not another research dump.

## Confidence and limitations

Confidence is high for the admission boundary because independent pre-plan, post-plan, startup,
and MV guards converge on the same error and targeted tests execute those paths. Confidence is
also high that a common managed hot-state tier is absent: the checkpoint backend explicitly denies
that role and no local LSM dependency or equivalent API is present in the current tree. Fjall's
deleted cold-cache history does not change that conclusion.

This report does not claim production behavior for an unreachable distributed stateful data path.
It also does not claim cluster exactly-once; cluster delivery remains at-least-once and
`[LDB-0013]` continues to guard exactly-once independently of this feature.
