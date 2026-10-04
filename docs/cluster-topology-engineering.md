# Cluster topology migration engineering guide

This guide describes the implemented control path. [Progress](cluster-topology-migrations-progress.md) and its source-bound evidence distinguish controlled actor tests, authority fixtures, real-process qualification and remaining work.

## Serialization and commit points

LaminarDB uses the existing append-only, fenced `LeaderLeaseStore`, with create-only sequence objects and conditional head publication/reconciliation. It does not use Raft. A topology operation shares this authority with checkpoint, assignment, process/recovery and retention transitions. There is no independently mutable catalog/migration head or second scheduler.

```text
LegacySealed --explicit identical-inventory adoption--> Versioned(1)

Planned -> Preparing -> Quiescing -> CutPrepared
        -> Committed -> Activating -> Active

Pre-Commit phases may become Aborted.
Committed/Activating/Active cannot become Aborted.
```

Adoption retains the original sealed manifest as the immutable topology-one baseline. The topology Commit is one authority append binding the target manifest, exact definitive parent checkpoint, sealed migration root, compatibility descriptor, participant observations and source initialization. The catalog reference advances in that same append. Target output stays held.

Release is a separate irreversible append after the full installed process roster certifies actual readiness. Each process then checks and applies that Release to its exact held runtime. `committed_version` can therefore advance while `locally_active_version` is null. Neither persistence nor a manifest fingerprint alone means activation.

## Identity domains

| Identity | Purpose |
| --- | --- |
| `TopologyVersion` | Checked nonzero logical version; legacy baseline is one. |
| `CatalogManifestRef.version` | Catalog serialization format, still one. |
| Object name, kind and `catalog_generation` | Catalog incarnation; retained for unchanged objects. |
| `PipelineIdentity` | Strict complete processing/recovery definition and ABI. |
| Checkpoint epoch/attempt | Monotonic data and publication cut; never reset by migration. |
| Assignment fence | Exact vnode owner map and full owner/evidence boot roster. |
| Leader proof and process term/boot | Current fenced durable authority and process incarnation. |
| Operation UUID | Idempotency identity, bound to immutable request evidence. |
| Runtime UUID and recovery round | Actual installed generation and replacement release owner. |

Object compatibility additionally binds resolved definition, Arrow schema, managed codec, connector/execution capability, global state/routing/delivery ABI and transitive dependency identities. Names or optimizer traversal indices alone cannot authorize state preservation. Graph state mappings use existing stable catalog names such as `graph:totals`; changed whole-pipeline identity remains strict.

The original baseline, every operation's plan/descriptor/root and its durable phase anchors are audited. Catalog bytes and historical checkpoint manifests/digests are never rewritten to disguise a mismatch. Bounds and canonical encodings are checked before decode/publication.

## Public admission and planning

Running cluster `LaminarDB::execute` intercepts topology DDL before taking the catalog write lock and submits through the coordinator. Direct parsed mutation and cold startup/replay retain their existing guards. Runtime requests cannot use the bootstrap exception. Single-node DDL remains synchronous.

`ClusterTopologyRequest` carries a nonzero UUID, exact expected parent and ordered individual statements. One array is one atomic candidate; ordinary SQL batches remain sequential. `POST /api/v1/cluster/topology/operations` returns HTTP 202 with the durable status. SQL returns a boxed operation receipt in `DdlInfo`, with `applied = false`; uncertain SQL errors retain the generated UUID.

An identical retry is looked up before running-state, current-parent or compiler gates. Its exact raw statements and expected parent are checked against the immutable plan/target before returning original terminal or in-progress status. Changed UUID payload reuse rejects. Admission itself atomically rechecks parent version, leader/process/assignment evidence and all serialization exclusions. It requires the live DB-owned checkpoint/recovery owner and released parent intake. A request owns no detached migration task.

Followers use the durable lease owner and existing membership HTTP address, preserving the console bearer, UUID, payload, parent and remaining total deadline. Forwarded receivers check durable local leadership and cannot forward again. Redirects are disabled and receipts are bounded. A receipt is an observation; it cannot grant actor or publication authority. Authorization and serving fences remain the existing server policy.

The isolated compiler replays the exact authoritative parent, builds candidate definitions/graphs separately, and checks the live parent again. It creates no source consumption, sink effects, durable head/checkpoint IDs or copied live state. Its one compiler slot, 30-second deadline, 64 statements, 256 KiB SQL, 256 catalog objects and 1 MiB descriptor bound planning. Submission has a 45-second total deadline; individual authority transitions use the existing 15-second/16-CAS bounds.

Public plans require protocol 6. Every frozen process, including required zero-vnode evidence participants, independently compiles and certifies that exact protocol/descriptor before a cut. Admission already raises authority to format 23, so an older format reader cannot continue appending a compatible head. Preparation advertisements alone do not retire cached actors; a coordinated binary upgrade and process/sink fencing remain required. Earlier protocol/format artifacts retain their canonical bytes and remain readable for historical audit/recovery.

## Cut, root and actor ownership

1. Admission freezes the complete assignment/process roster and reserves checkpoint/assignment transitions.
2. Every participant certifies the same immutable candidate. A reachable majority cannot substitute for missing state owners or evidence participants.
3. The existing checkpoint barrier runs under the parent pipeline. Sources remain able to produce their barrier; intake is held at the actual capture boundary, not prematurely closed.
4. The checkpoint persists state/channel/source/output progress. Old sink effects follow their definitive durable outcomes. A timeout or missing acknowledgement cannot be interpreted as failed commit.
5. The exact old cut reaches complete application receipts. A sealed root binds each preserved object's incarnation/state mapping, channel positions, source cursors, watermark/idle state and subscription publication frontiers. New latest positions are resolved once and durably sealed before target Commit.
6. Existing ownership handles observe superseded source, sink, coordinator and asynchronous work reaching terminal completion. Cancellation requests alone are insufficient. The namespace remains owned and output remains fenced.
7. One bounded private graph restores through existing codecs, retaining the exact cut. Encoded state is freed after decode. Preparation avoids keeping a second arbitrarily large live state graph while the parent remains resident.
8. Complete target preparation observations allow atomic Commit. The ordinary runtime startup owner installs the target held, including exact source startup positions, sink generation and receiver mesh.
9. Actual installation receipts bind each current process, new runtime UUID, exact assignment and target generation. Complete current readiness permits durable Release, followed by each participant's independent local application.

The existing recovery monitor drives this state machine. It keeps one private image, phase deadlines and cancellation-safe lifecycle ownership; it is not a generic workflow framework. An HTTP caller disappearing cannot revoke an admitted operation. Terminal faults hand off to existing coordinated recovery.

Control futures are heap-pinned at large lifecycle boundaries. Runtime worker counts and 4 MiB worker stacks are unchanged. Planning, serialization, hashing, root validation, authority I/O and migration allocations stay on control boundaries. Arrow batches retain existing ownership. No per-row catalog lookup, JSON, remote call, lock or task was added.

Shuffle sender/receiver generation and assignment envelopes reject stale graph work at transport/batch boundaries. Sink epoch/generation admission and asynchronous completion checks reject superseded publications. These fences complement, and do not replace, namespace, leader, process, assignment and terminal-fault authority.

## Recovery and full restart

Before target Commit, a replacement leader reconciles existing checkpoint/sink outcomes and durably aborts or resumes through the existing fault owner. If the old cut committed, recovery uses that cut rather than rewinding effects to an earlier checkpoint. Aborting a topology reservation does not directly reopen intake.

After Commit, rollback is forbidden. Recovery selects the greatest exact target checkpoint, or the explicitly authorized migration root before any target checkpoint. It never loads the newest arbitrary parent checkpoint under the target. Target checkpoints audit their exact predecessor edge through the root; unchanged identity paths retain strict existing validation.

Coordinated recovery binds the exact immutable Commit, full current process roster and new recovery round. After complete stopped receipts and artifact/sink settlement, participants independently reconstruct the same selected cut and start held actors. Ready/Release certify actual sources, state, sink generation and receiver mesh. A first recovery Release can atomically complete pending topology activation. Recovering an already Active target retains its original immutable activation evidence while using a new replacement runtime/round.

This recovery contract preserves the complete vnode owner map and stable node IDs;
only process incarnations and certified assignment versions may advance. The
public topology soak replaces a failed process before requiring full-roster
progress. The existing survivor-rescaling soak exercises a separate membership
contract. A reduced assignment is not evidence for topology Release; the partial
process run rejected it and stayed fenced.

An assignment-recovery handoff pin protects the exact selected restore cut until
the replacement assignment commits its first target checkpoint. Recovery
Start/install/Release may retain that pin only when its complete assignment fence
and full checkpoint reference equal the audited current assignment and selected
greatest checkpoint. The pin is rechecked after control I/O and remains present
through Release. A differing pin, unsettled checkpoint, cleanup cursor or drain
reservation still fences recovery; Release itself does not retire restore state.

Private topology restore selects the existing portable checkpoint bootstrap when
the selected cut has an older assignment version. The checkpoint index and state
payloads keep their historical assignment; they are not rewritten. Selection and
restore require the same complete owner digest, vnode count, partition ABI,
stable node roster and exact local vnode set. The ordinary bootstrap then audits
drained portable whole/vnode state, current transport/process authority and the
managed-state budget before publication. An identical assignment still uses
strict ordinary restore. This handles process replacement within the existing
topology contract and grants no survivor rescaling authority.

Cold startup accepts the complete current catalog or the exact complete original adopted bootstrap as an assertion. Durable target authority takes precedence over that original bootstrap. Arbitrary subsets and changed definitions reject. The target catalog is reconstructed, namespace ownership retained and the same coordinated recovery owner queued before actors can publish.

Missing deployment identity is an error, including through a cached decision store. Reads never recreate it. Source cursors for every target source come from the selected checkpoint; recovery cannot re-resolve latest or reuse an old initializer when target progress exists. Checkpoint allocation and publication frontiers continue monotonically.

## Sources, sinks and subscriptions

Unchanged sources preserve committed replay/snapshot/channel progress and acknowledgement rules. New Kafka explicit-topic sources can seal earliest/latest numeric positions, including never-read partitions; atomic initialized startup validates availability and adopts only the current assignment. Unsupported pause/replay/start contracts reject before the cut or target installation.

New stateless downstream objects are future-only. They do not implicitly replay retained history or claim historical completeness. New stateful operators, schema/key/window transformations and source/sink replacements remain rejected. The current implementation does not reuse dropped object incarnations or delete external tables/topics as a DDL side effect.

Old prepared sink effects settle against their durable logical checkpoint identities. Retired writers and late completions are fenced. Transactional/idempotent and at-least-once connectors retain their respective delivery contracts; no universal exactly-once claim is made.

Unchanged subscription certificates retain generation, schema, query, distribution, changelog, event-time and retention contracts. A reader crosses a historical pipeline boundary only through exact released roots and complete certificate equality for the mapped incarnation. Historical segment bindings remain unchanged. Audits occur at checkpoint boundaries and cache the selected certificate; there is no per-row remote audit. Reconnect and AS OF EPOCH retain existing no-silent-gap and bounded-consumer semantics, without inventing named-consumer acknowledgement storage.

Retention uses those exact predecessor edges and historical certificates. A cleanup horizon is checked by its full encoded reference, digest and length. Missing/corrupt roots or changed horizons stop deletion before output/state loss. Migration roots/state pins are still retained conservatively after Release; consumption and bounded topology-journal reclamation remain outstanding. The 64-operation bound fails closed rather than forgetting idempotency history.

Protected-cut artifact preflight verifies complete owned and incremental state
objects by length and SHA-256 before cluster cleanup publishes its floor/cursor.
Reads use 256 KiB ranges, at most eight concurrent objects, an 8192-object/4 GiB
aggregate bound and a 15-second state-read deadline. Duplicate references must
agree exactly; empty objects must still exist. This verifies stored bytes rather
than decoding every state codec. Existing local retention publishes its floor
before protected-cut loading and retains that ordering. Cleanup rechecks topology
and assignment-handoff references after preflight before its conditional append.

## Ordering and failure matrix

Compiler ownership precedes the catalog read lock during isolated validation. Public SQL submission does not hold the catalog write lock across control I/O. Short parking_lot guards copy handles/snapshots and release before awaits. Lifecycle, checkpoint and recovery ownership follow the existing coordinator ordering, with fresh durable/process/assignment checks around transfer. Cancellation is covered at authority publication, stopped-owner handoff, private reconstruction and held startup boundaries.

| Failure | Required behavior |
| --- | --- |
| Lost admission/certificate/Commit response | Resolve durable status using the same UUID/evidence; no replacement request. |
| Missing/divergent/mixed participant preparation | No parent cut or target output authorization. |
| Parent checkpoint timeout or unknown sink result | Reconcile definitive outcome; never infer Abort from timeout. |
| Leader/process loss before target Commit | Fenced abort/recovery; retain any successfully committed old cut. |
| Failure after Commit or during install/Release | Preserve target Commit; recover and activate target with a new valid runtime/round. |
| Stale process/assignment/graph/Release message | Reject exact identity mismatch; gates remain held. |
| Shutdown/cancellation during ownership transfer | Existing lifecycle owner retains cleanup obligations and namespace fences. |
| Damaged descriptor/root/manifest/state/output binding | Typed failure before mutation, intake release or deletion. |
| Retention racing admission/recovery/replay | Shared authority serialization and exact pins/horizons determine permitted cleanup. |

## Evidence and outstanding qualification

The linked evidence folders record the original baseline, tested source hashes, unchanged Cargo.lock, features, exact commands/results and each fixture's scope. Existing control/graph/sink/source tests cover malformed artifacts, stale evidence, lost responses, cancellation, checkpoint races and replacement release. Stored-segment tests cover reconnect identity and cleanup corruption.

The [boundary-test index](test-evidence/topology-public-process-2026-10-03/portable-bootstrap-fault-coverage.json)
binds 40 existing authority and owned-runtime fault regressions to the passing
4,470-test run. It covers lost responses, cancellation, stale leaders, failed
installation and held replacement Release at their stated boundaries. It does
not claim that native processes were killed at every migration phase.

The public migration soak extends the existing real three-process Kafka/S3 stateful harness. It adds an independent latest-source pipeline, SQL downstream stream, deterministic paused-input/output checks, post-migration hard kills and full target restart with the original bootstrap. Its consumer-visible timing starts before producing records during a held prepared cut, including the explicit test hold and subsequent Release. Existing independent join/window/aggregate oracles remain required. It must pass before it is cited as real-process migration evidence.

Safe removals/replacements require their own descriptor/projection and incarnation contracts. Root consumption, journal reclamation, the remaining fault matrix and matched baseline/current steady-state and pause-inclusive performance measurements remain tracked work. Local timings are observations of their stated workload/environment, not production guarantees.
