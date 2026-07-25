# redb prescreen external execution-term options — Cycle 34

- **Date:** 2026-07-25
- **Evidence class:** official interface/document review and protocol design only
- **Scope:** `D21.1` authority for a future redb-free dummy-mechanism/evidence VM
- **Recommendation:** if owners retain the renewable-term requirement, carry a GCP absolute
  deletion-trigger VM composed with an external etcd lease/revision term into a separate mechanism
  prescreen
- **Selected service, provider tuple, version, account, VM, controller, or client:** none
- **Provider/API, etcd, Docker, candidate, backend, or soak execution:** none
- **Runtime/backend authority:** none; this is not LaminarDB state, checkpoint, leader, or rebalance
  coordination
- **Production verdict:** **NO-GO**

## Decision boundary

No examined cloud primitive atomically binds protected workflow attempt, renewable single-writer
term, attested VM/boot/image, and bounded termination. The least speculative composition currently
worth owner review is an external etcd v3 lease plus compare-and-swap revision term, with a GCP
absolute `terminationTime` request, `DELETE` action, and read-back `terminationTimestamp` as an
independent provider-trigger backstop. This is an engineering recommendation for a later redb-free
mechanism probe, not a selection or permission to provision either service. Etcd fences its own
keys, not VM execution; the composition supplies evidence authority plus cooperative guest stop and
provider deletion under the existing trusted guest/hypervisor model, not a true external VM fence.

The simpler one-shot alternative is a fixed GCP absolute deadline with no renewable term. It is
attractive for a short dummy probe but does not meet the already frozen renewable single-writer
predicate. Owners may choose it only by explicitly amending `D21.1` and re-reviewing the weaker
early-stop/failure behavior; it cannot be silently described as a lease. GCP documents when
automatic stop/delete may begin, not an upper bound for execution cessation or final absence, so it
also remains conditional. No examined AWS or Azure primitive improves that to a proved hard expiry.

| Direction | What it proves if later qualified | Missing or adverse property | Disposition |
|---|---|---|---|
| fixed GCP absolute deadline only | one absolute automatic-deletion trigger can be bound and restart cannot extend that future time | no renewable monotonic term, no automatic early fence, and no documented cessation/final-absence bound | simplest alternative, but incompatible with current `D21.1` |
| GCP plus external etcd term | renewable evidence-authority term, CAS single writer, fail-closed guest observation, early delete path, and absolute provider trigger can be composed | not a VM execution fence and not atomic across etcd/GCP; exact deployment, pins, timing, IAM, failure injection, cessation, and final-absence proof are absent | **recommended for owner review; not selected** |
| AWS plus external etcd term | same external evidence term with useful instance/image attestation | no provider absolute deletion trigger or bounded cessation/final absence identified | conditional only |
| custom Rust consensus/lease service | could be shaped to the protocol | creates a new distributed system and operational/failure surface solely for a qualification control plane | reject as overengineering |

## Fixed one-shot alternative

If owners weaken `D21.1`, the nonrenewable authority receipt would bind exactly the protected
workflow/repository IDs, run ID/attempt, fresh challenge, project number, zone, instance name and
numeric instance ID, boot ID, image/attestation policy, evidence-grant deadline `E`, and requested/
observed GCP termination time `H`, with `E < H`.

One controller would create exactly one standalone VM with a stable UUID `requestId`, absolute
`terminationTime=H`, `instanceTerminationAction=DELETE`, `automaticRestart=false`, and
`deletionProtection=false`. MIG, autohealer, autoscaler, replacement, metadata/IAM mutation,
start/reset/resume, alternate deletion, and deadline-extension paths are denied. Bootstrap remains
inert until the create operation completes without error and an exact provider GET, numeric instance
ID, attestation, boot, image, and challenge all join. Authority expires at `E` and cannot renew.
Cancellation uses delete with one stable UUID `requestId`; ambiguous create/delete calls may repeat
only that documented idempotent request identity.

A successor remains forbidden until the delete operation is terminal without error, exact GET
returns genuine not-found, complete inventory excludes both name and numeric ID, and an independent
read-only verifier signs the close receipt. This is a single-use capability, not a monotonic term,
and GCP does not document the consistency/finality strength or time bound needed for that absence
oracle. The insert/delete `requestId` and operation handling are documented by the
[instance insert API](https://docs.cloud.google.com/compute/docs/reference/rest/v1/instances/insert),
[instance delete API](https://docs.cloud.google.com/compute/docs/reference/rest/v1/instances/delete),
and [Compute response guide](https://docs.cloud.google.com/compute/docs/api/how-tos/api-requests-responses).

## Recommended etcd construction

### Durable gate, live key, and term

A single lease-attached key is unsafe: expiry could remove the only guard and allow a successor
while the stale VM still exists. The minimum direction therefore uses two fixed keys:

| Key | Lease | Purpose |
|---|---|---|
| `G` durable gate | none | survives controller/current-lease loss within the frozen logical cluster and blocks every successor until provider absence is independently reconciled |
| `L` live authority | current attempt lease | disappears on lease expiry/revoke and gates current evidence acceptance |

The term is
`T = (etcd_cluster_id, acquisition_txn_revision, lease_id, attempt_nonce)`. A lease ID alone, etcd
Raft term, watch revision, guest clock, workflow run number, provider instance name, or copied value
is not an etcd fencing term. Even `T` governs evidence acceptance only: GCP VM execution does not
validate it and therefore is not externally fenced by etcd. The acquisition revision is monotonic
only inside the exact logical cluster named by `etcd_cluster_id`; terms from different cluster IDs
are never ordered or substituted.

Canonical `G,L` values bind the workflow/repository/run-attempt identity, policy and controller
digests, provider tuple, attestation challenge, evidence deadline, absolute GCP trigger, and exact
binary/configuration pins. Their activated values additionally bind numeric instance ID, boot ID,
image and verified attestation. Exact canonical serialization remains blocked.

### Roles and least privilege

1. The protected workflow submitter supplies a fresh challenge through short-lived OIDC/WIF and has
   no etcd or Compute mutation credential.
2. One external authority controller is the sole etcd writer/lease renewer and sole routine Compute
   create/delete principal. It cannot start/reset/resume a VM or extend the absolute trigger.
3. The guest evidence supervisor has read-only exact-key access to `G,L`; it cannot grant, renew,
   revoke, put or delete, and has no provider mutation authority. The candidate/redb child receives
   no credential.
4. An independent teardown verifier can read etcd and Compute state but cannot write/renew the term,
   mutate the VM, or close `G`.
5. Infrastructure administrators and any emergency delete-only path are a closed trusted set outside
   normal run flow. Their use poisons the attempt and is retained.

Mutual client/peer TLS, exact CA/server/client identities, endpoint and cluster/member allowlists,
etcd RBAC, provider IAM conditions, credential delivery/rotation/revocation, controller executable
identity, and audit-log retention are approval inputs. Audit logs remain retrospective evidence, not
live authority.

### Acquire, activate, renew, and observe

1. `LeaseGrant` returns a server-selected TTL and lease ID. A default-linearizable read first selects
   exactly one acquisition shape: bootstrap compares both `G,L` at version zero; successor compares
   `G` at the exact prior `CLOSED` value/mod revision and `L` at version zero. The selected transaction
   writes unleased `G=PREPARING` and leased `L=PREPARING`. Its top-level revision is the term revision.
   A following default-linearizable read must prove the exact values/mod revisions plus `G.lease=0`
   and `L.lease=T.lease_id`. A failure branch, any other population, or a stale/non-closed gate rejects
   the attempt; there is no logical-OR comparison hidden inside one transaction.
2. The controller creates one fresh VM with absolute `terminationTime` and `DELETE`, using the fixed
   one-shot restrictions above. Requested time and read-only `terminationTimestamp` are evidence;
   `maxRunDuration` is not the absolute trigger because restart recalculates it.
3. After provider create/GET, attestation, boot, image, and challenge validation, activation CASes
   the exact prior values and mod revisions of both keys, then writes unleased `G=ACTIVE` and leased
   `L=ACTIVE` in one transaction. Candidate release requires a default-linearizable read-only
   transaction returning exact `G,L` together. It proves `G.lease=0`, `L.lease=T.lease_id`, and exact
   values/mod revisions; activation comparisons alone cannot prove lease metadata. Only the
   transaction's top-level response header is authoritative.
4. The controller is the only renewer, permits one outstanding `LeaseKeepAlive` at a time, applies an
   explicit response deadline, and follows each valid positive-TTL response with a default-
   linearizable exact `G,L` read that revalidates both lease fields, values and mod revisions.
   Transparent indefinite retry is forbidden. No Watch operation is part of the safety path.
5. The guest performs bounded exact linearizable reads and uses a local monotonic watchdog. It stops
   candidate progress after an owner-approved maximum interval without a valid exact read of
   `G=ACTIVE`, `G.lease=0`, `L=ACTIVE` and `L.lease=T.lease_id`. Lease expiry deletes `L`, while
   non-closed `G` durably prevents a successor. False-positive stop/delete is safe; continuing on
   uncertainty is not.
6. Only after every possibly created provider identity is independently absent may the sole
   controller select one closure transaction. It compares `G` at the exact current `PREPARING`,
   `ACTIVE` or `POISONED` value/mod revision (one concrete branch, never an OR), compares `L` absent,
   and writes a new `CLOSED` value that binds the prior term and independent final-absence receipt
   identity/hash. No gate deletion/reuse is allowed.

Exact server-selected TTL `Tttl`, keepalive interval `K`, response/read deadline `R`, guest polling
period `P`, guest stop bound `Gstop`, evidence deadline `E`, provider trigger `H`, and measured
margin remain symbolic. At minimum `K + R + margin < Tttl`, `P < Gstop`, and no authority extends
past `E < H`. `Gstop` remains bounded post-loss grace, not instantaneous execution fencing.

etcd provides default-linearizable reads, atomic conditional transactions, one revision for all
modifications in a successful transaction, and lease-attached key deletion on expiry/revoke. Its
watches are not linearizable and may be delayed. A lease alone also does not establish external
mutual exclusion. The design therefore uses the acquisition revision and CAS state as the sequencer,
bounded linearizable reads as live evidence authority, and no Watch in the minimal path
([etcd API](https://etcd.io/docs/v3.7/learning/api/),
[API guarantees](https://etcd.io/docs/v3.7/learning/api_guarantees/),
[lease/sequencer comparison](https://etcd.io/docs/v3.7/learning/why/)). That last source explicitly
warns that an etcd lock cannot protect an external resource unless the resource validates the
version token.

### Ambiguity, cooperative stop, and provider deletion

Lease-grant, acquisition or activation ambiguity never releases a candidate. Keepalive EOF/timeout,
wrong lease/nonpositive TTL, missed deadline, exact-read timeout/mismatch, accidental serializable
read, TLS/auth/cluster/member drift, or any explicit revoke/end sets sticky poison, stops candidate
progress, and requests provider deletion. A timed-out mutation is never blindly retried or used to
resume the attempt; later linearizable reconciliation is teardown evidence only. Watch delay/loss is
irrelevant to authority because Watch is absent from the path.

When quorum remains reachable, the controller attempts one exact CAS of the current
`G=PREPARING`/`ACTIVE` value and mod revision to `G=POISONED`; CAS failure or inability to reach quorum
does not restore authority. Because the guest accepts only exact active `G,L`, a visible poison stops
it immediately, while loss of `L` stops it after the bounded watchdog path. Any controller restart
may reconcile and tear down only; it can never resume an open attempt.

GCP create/delete ambiguity may retry only the same UUID `requestId` and must reconcile the zonal
operation identity, client operation ID, target link, target numeric ID, terminal status and error.
A `403`, timeout, malformed response, empty list, accepted delete request, terminal operation alone,
guest shutdown, etcd key loss, or audit record is not provider absence. The absolute deletion
schedule is the last trigger backstop if controller/guest coordination fails, but GCP documents only
that automatic stop/delete may begin up to 30 seconds after the requested limit; this is neither a
bound on execution cessation nor on final absence
([GCP VM runtime limits](https://cloud.google.com/compute/docs/instances/limit-vm-runtime)).

Cluster restore/revision rollback, cluster-ID change, unapproved membership/configuration change,
stale endpoint, lost durable-gate history, expired credentials, or a new guest boot globally poisons
open attempts. No new term starts until every previously bound provider identity is reconciled absent
([etcd disaster recovery](https://etcd.io/docs/v3.7/op-guide/recovery/)).

## Pins, versioning, and remaining blockers

This packet deliberately does not pin an etcd release. Official releases v3.7.1 and v3.6.14 were
published on 2026-07-23, so v3.6.11 is already stale; the patch release includes security fixes and a
client lease-cache race correction
([official patch-release notice](https://etcd.io/blog/2026/july-23-patch-release/)). The current v3.7
API documentation is useful for interface reasoning, but exact server/client patch, release
signatures/SBOM, member count and failure domains, storage/fsync policy, quorum/corruption checks,
compaction/restore policy, TLS/RBAC configuration, client library, controller build, endpoints,
deadlines, and hostile fixtures all remain open. The lease API authorization behavior and chosen
client's retry/stream semantics require source audit. Stream clients need application deadlines
because transport keepalive alone can stay responsive through a partition
([etcd client design](https://etcd.io/docs/v3.7/learning/design-client/),
[transport security](https://etcd.io/docs/v3.7/op-guide/security/)).

The GCP project/region/zone, AMD-SEV machine and image, attestation trust material/PCR policy,
service accounts/IAM conditions, absolute horizon, delete API/absence oracle, network policy, and
controller failure domains also remain unselected. Trusted UTC source/drift, provider-clock
interpretation, guest monotonic-clock source, and the bounded UTC-to-monotonic conversion used for
`E,H` are unresolved; `E < H` alone does not prove enforcement. No provider or etcd availability/
SLO claim is inherited from documentation.

Adding the etcd trust bundle, controller executable, role identities, gate/live-key bytes, or
live evidence changes approval inputs. It therefore requires successor target/execution-plan and
payload/receipt identities; current `/v2` approval bytes cannot authorize it. If the sequenced-packet
N19 direction is selected first, these inputs must be included in its already required payload and
receipt `/v3` freeze rather than layered afterward.

All polling, renewal, attestation, and deletion work is qualification control-plane work, never a
LaminarDB record/state/timer/checkpoint hot-path call. Its CPU, network, scheduling, and failure
perturbation must nevertheless be measured during the dummy probe and later qualification. An
off-the-shelf external coordinator is preferable to implementing a new Rust consensus service; a
Rust client may be evaluated later, but language preference does not waive the distributed-systems
or operational proof. If no already-operated, independently administered etcd quorum exists,
provisioning one solely for this dummy probe requires a separate owner cost/risk decision against
explicitly weakening `D21.1` to the fixed one-shot alternative; infrastructure complexity is not
free evidence quality.

Until every blocker above closes through separate owner review and a redb-free hostile mechanism
probe, the etcd composition is not selectable, the provider workflow is not executable, and no
candidate, backend, cluster admission, production, exactly-once, or soak claim follows.
