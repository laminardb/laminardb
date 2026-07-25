# Distributed keyed state Cycle 33 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Protocol commit:** `63dffcf4`
- **Cycle outcome:** N19 owner-decision packet/recommendation, provider shortlist, and redb-free
  Docker/kernel probe predicates reviewed; owner acceptance remains absent
- **Bounded memory:** reference/conformance-only; no product or soak profile
- **Maintenance-health v2:** direction approved; candidate mapping remains gated
- **Current product target:** one qualified local-spill backend; no backend selected
- **Candidate, container, provider workflow, mechanism, or backend execution performed:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

The [canonical redb prescreen](../testing/state-backend-redb-prescreen-v1.md) now recommends rejecting
the Cycle 32 Linux/x86-64 shared-memory assembly ABI as the durable N19/crash-marker default and
selecting one-way `AF_UNIX`/`SOCK_SEQPACKET` publication. This is an engineering recommendation, not
the missing workload-owner and operations-owner decision. The exact design-only JSON choice is bound
under the future target identity and cannot authorize implementation or execution.

The recommendation is honest about its evidence cut. Successful packet enqueue is publication;
the supervisor timestamps later receipt. Exact receiver-namespace credentials, retained pidfd/full
process identity, sole endpoint-holder proof, and a fixed no-ancillary `sendto` profile are
conjunctive. A pre-call `commit_call_imminent` packet does not prove adapter or redb-internal entry
and cannot satisfy existing confirmed-in-commit coverage. The successor crash contract must choose
an independently versioned in-adapter marker or weaker post-imminent/no-return coverage. Until then,
the crash schemas, oracle, coverage and large-recovery campaign remain blocked. Selecting IPC would
require protocol/policy `/v2`, approval payload/receipt `/v3`, and successor marker-sensitive
identities; assembly-v1 evidence cannot cross that boundary.

For the redb-free Docker mechanism, GCP is the unselected provider shortlist leader because an
AMD-SEV attested VM can use absolute `terminationTime` plus deletion as a hard-expiry backstop. AWS
is an unselected conditional finalist for NitroTPM/Attestable-AMI and no-replacement controls, but no
provider-enforced hard expiry has been identified. Azure remains on hold. None supplies the whole
workflow-attempt, renewable-term, VM/boot/image, bounded-termination chain atomically; the external
single-writer term, provider identity, delete operation and final absence must be composed and
proved. GitHub-hosted Ubuntu is superseded as the evidence target, while the approved protected-
workflow orchestration direction remains separate.

The narrow source proposal is rootful Linux/amd64 Moby 29.6.2, containerd 2.2.6, its matching
`containerd-shim-runc-v2`, runc 1.3.6, and Engine API v1.55. Three preconnected sockets carry one
events stream, one wait, and 20 or 21 sequential control requests: 22 normal requests or 23 for the
single hostile kill case. Source proves only a logical runtime graph. Executable/image hashes,
external-containerd compatibility, keepalive behavior, exact configuration, helper reachability and
physical socket cardinalities remain absent.

The kernel/gate proposal now distinguishes each authority: allocation versus committed fork,
exec admission versus success, resolved AF_UNIX peer versus pathname, `file_receive` receiver versus
source process, and duplicate close versus final open-file-description release. Policy maps require
both program-read-only creation and user-space freeze. Mount aliases, classic AIO, epoll, io_uring,
descriptor duplication and ancillary transfer are explicit hostile cases. The verifier validates
and closes its own `pidfd_getfd` result; it never attributes that FD to the supervisor or claims
final file release. These are falsifiable dummy-probe predicates, not an implementation or passing
mechanism.

No runtime dependency, schema, fixture, workflow, provider resource, backend adapter, candidate
run, cluster-admission change, or production claim was added.

## Six-pass cycle review

### 1. AI slop, evidence, and consistency

**Pass after three independent adversarial reviews and correction rounds.** Review removed an
ill-typed credential/pidfd join, a false pre-call “commit entered” claim, incomplete owner bytes,
and unsupported version compatibility. Provider review separated absolute GCP expiry from
restart-relative duration, made AWS conditional, scoped comparisons to the three examined clouds,
and removed stale GitHub-host-target direction. Docker/kernel review corrected verifier FD
ownership, packaging claims, unimplemented-supervisor language, legitimate transient candidates,
mount-alias coverage, and duplicate-close semantics. N19, provider/hygiene, and Docker/kernel
reviewers independently returned commit-ready.

### 2. Overengineering, hot path, and latency

**Pass for a gated qualification control plane; no product mechanism is approved.** Packet markers,
provider fencing, BPF, seccomp, gates and the fixed Engine pool are outside LaminarDB's record,
timer, join, checkpoint and state-access hot paths. Marker overhead is unmeasured and must be probed;
no assembly speed claim substitutes for maintainability. The three Engine sockets are the minimum
for one lifetime event stream, one wait and serialized control. If the sealed-pool or kernel design
cannot be proved, the result is infeasible rather than another exception layer.

Backend latency remains a separate gate: open-loop p99/p99.9, hot-key/Zipf pressure, disjoint-vnode
interference, maintenance stalls, restore tails and bounded telemetry outside per-record critical
sections. This cycle supplies none of that evidence.

### 3. Unused code and dependencies

**Pass.** The protocol document was the only file changed before this review. No Cargo dependency,
feature, runtime API, schema, fixture, workflow, provider integration, Docker image, assembly/BPF
object, helper, adapter or generated artifact was added. There is no unused implementation.

### 4. Production readiness, delivery, exactly once, and soak

**NO-GO, correctly fail-closed.** No state engine, target, provider, executable tuple or mechanism is
eligible or executed. A local engine alone still cannot provide vnode ownership, restore-before-
activate, checkpoint sealing, rebalance fencing, retention-safe cleanup or source/sink delivery.

The first keyed-state release remains at-least-once. Exactly once still requires a replay-stable,
assignment-fenced source; one state/timer/output checkpoint cut; recoverable coordinator decision;
and a checkpoint-committable sink fenced by deployment, pipeline/sink namespace, checkpoint attempt
and live leader term, including ambiguous-commit recovery. Source and sink certification remains
separate from backend selection. An independent team/environment must complete the full production
soak before any production-ready claim; neither N19 qualification nor the D20/D21 dummy probe can
replace it.

### 5. Documentation and research hygiene

**Pass with a carry-forward warning.** Forty-nine added external-URL occurrences resolve to 45
unique primary provider, Docker, containerd, runc, Linux/POSIX or man-page targets. Stale GitHub-target and overbroad provider text was
replaced rather than layered. No standalone research document was removed: the backend audits,
redb mechanism history, maintenance-health work and Fjall/RocksDB/redb/SurrealKV/TidesDB records
still explain live decisions or rejected paths.

The canonical prescreen is now a large cumulative ledger. This cycle should be its last substantial
historical append. Cycle 34 must update a short current-state ADR/index and prefer replacement or
linked focused reports over more chronological prose.

### 6. Tests and empirical boundary

**The validation-only repository slice is green; no backend evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed; one explicitly ignored non-gating CLI throughput/RSS observation.
- `git diff --check`, exact `N01`--`N29` and `D01`--`D21` registry checks, relative Markdown links,
  exact N19 owner-literal uniqueness, and 45 unique added external-URL target resolutions (49
  occurrences): pass.
- No redb, TidesDB, RocksDB, Fjall, Docker daemon/container, WSL workload, native mechanism, cloud
  API/workflow, provider resource, backend candidate, or soak ran.

## Cycle 34 entry boundary

Continue validation-only closure without selecting or executing a backend:

1. replace the leading decision summary in ADR-008 with a compact current-state/backend matrix and
   links, preserving detailed history in existing reports rather than appending another ledger;
2. prepare the separate owner decision for in-adapter marker versus weaker post-imminent/no-return
   crash coverage, without implementing or running either branch;
3. statically narrow GCP and conditional AWS to exact eligible image/kernel/attestation/expiry/IAM
   predicates and one external-term construction, without provisioning or calling provider APIs;
4. source-audit the minimum containerd configuration, snapshotter/network/plugin set, complete
   legitimate helper population and exact Engine request sequence; do not download or run binaries;
5. derive raw roles/cardinalities and caps only after those populations close; and
6. repeat independent AI-slop, overengineering, unused-code, production-readiness,
   overdocumentation and test review while preserving bounded-memory, maintenance-health,
   source/sink, exactly-once, hot-path, latency and independent-soak gates.
