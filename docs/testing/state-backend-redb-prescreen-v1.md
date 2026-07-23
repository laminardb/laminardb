# redb 4.1.0 bounded state-backend prescreen v1

- **Identity:** `state-backend-redb-prescreen/v1`
- **Status:** proposed, non-gating protocol; strict detached schemas and source note exist, but owners,
  harness, validator, and execution remain absent
- **Evidence class:** `NOT C2/C3 QUALIFICATION EVIDENCE`
- **Scope:** decide whether a redb adapter is worth adding to the backend qualification bake-off
- **Production/admission effect:** none; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Detached contracts:** [pre-run approval schema](../../tools/state-backend-qual/schema/redb-prescreen-approval-v1.schema.json),
  [reviewed-result schema](../../tools/state-backend-qual/schema/redb-prescreen-result-v1.schema.json),
  and [exact-source mechanism note](../reports/redb-4.1.0-prescreen-mechanism-note-2026-07-23.md)

## Decision boundary

redb 4.1.0 is not a qualification candidate. This prescreen answers four cheaper questions before
LaminarDB pays for a third adapter:

1. does one database-wide writer create unacceptable acquisition or victim tails for the proposed
   state traffic;
2. what latency cost comes from one-phase Immediate, two-phase Immediate, and quick-repair commits;
3. do process-crash outcomes preserve atomic cross-table state and Immediate's return boundary; and
4. does quick repair materially bound reopen time without introducing correctness or resource
   failures?

`PRESCREEN_PASS` only funds the candidate mechanism mapping, persistence mapping, and adapter design
review. `PRESCREEN_NO_GO` means repeatable, valid architectural or performance evidence does not
justify that investment. `DEFER` means the experiment or environment could not decide.
`REJECT_EXACT_PIN` applies only to a correctness failure in the exact pinned build on a valid quiet
target. None supplies C1/C2/C3, fault, endurance, checkpoint, source/sink, exactly-once, selection,
or production evidence. The prescreen never selects redb by comparing its limits with Fjall or
RocksDB results.

## Frozen source and build scope

The only subject is:

- crate `redb =4.1.0`;
- SHA-256 of the exact `.crate` archive
  `8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839`; and
- packaged/upstream revision `6ed1f981ba4deab0b2adbdd7bccb46ec409b2191`.

The harness lives in an isolated, unpublished prescreen crate. Its exact lockfile, source archive,
SBOM, feature set, target, `1.95.0-x86_64-unknown-linux-gnu` toolchain, compiler flags, and binary
SHA-256 are recorded. Neither the LaminarDB root workspace nor runtime crates gain a redb dependency.
A different archive, source revision, feature set, or relevant build flag is a different subject and
cannot inherit the result.

One redb file contains four byte-key/byte-value tables named `state`, `timer`, `join_left`, and
`join_right`. The builder cache is 8 GiB. Measured attempts prohibit `WriteTransaction::stats`,
`Database::compact`, savepoints, and any other full-tree or exclusive maintenance call. Every
transaction uses 32-byte keys and 992-byte values, for 1,024 logical key-plus-value bytes per
mutation. A transaction's mutations are distributed deterministically across all four tables.

The three exact transaction modes are:

| Mode | redb 4.1.0 calls | Purpose |
|---|---|---|
| `I1` | `set_durability(Immediate)`, `set_two_phase_commit(false)`, `set_quick_repair(false)` | Immediate one-phase baseline |
| `I2` | `set_durability(Immediate)`, `set_two_phase_commit(true)`, `set_quick_repair(false)` | isolate two-phase commit cost |
| `QR` | `set_durability(Immediate)`, `set_two_phase_commit(false)`, `set_quick_repair(true)` | save allocator state; redb forces two-phase commit during commit |

The harness asserts and records the closed setter sequence before each attempt; pinned-source review
verifies that `QR` forces two-phase commit. `Durability::None` is outside this protocol because it
does not satisfy the persistence question.

### Separate pre-run authorization

This proposal does not authorize execution. Any Docker smoke or native prescreen command requires a
strict detached `state-backend-redb-prescreen-approval/v1` record signed by the named workload and
operations owners. The strict record schema now exists, but no external byte/attestation verifier,
semantic plan/result validator, or harness exists. The record must bind the
exact protocol bytes; harness/oracle source, binary, lockfile, SBOM, toolchain, target and flags; raw
wire/result schemas and literal goldens; complete seed/order schedule; target identity and preflight/
noise rules; clock/cgroup/cache-reset procedures; trigger and bounded adaptive-delay rule; every
deadline/resource/artifact cap; and all-false qualification/selection/production/admission fields.
The result schema restricts every `synthetic_fixture` to `fixture_ineligible=true` and disposition
`DEFER`; a fixture cannot encode even a smoke pass or a native prescreen decision. A real-shaped
record still has no authority until the future semantic verifier checks its referenced bytes,
attestations, run class, and disposition.
The command must verify the detached record before opening redb. Any bound input change requires a
new approval, and no prescreen approval can authorize or donate C1/C2/C3 evidence.

## Isolation and clocks

An external Linux supervisor owns the database directory and starts one child process. The child
owns exactly one `redb::Database`; lanes are OS threads in that child. The supervisor supplies
open-loop release times, process/cgroup sampling, intent/acknowledgement memory, `SIGKILL`, reopen,
and the independent expected-state oracle. The child cannot classify its own crash result.

The target is the runner's native Linux/XFS/dedicated-NVMe class with fixed CPU affinity, an
otherwise idle device, cgroup v2, synchronized monotonic clocks, and the same thermal and free-space
preflight used by the future candidate campaign. Virtual disks, overlay filesystems, shared host
NVMe, missing project quota, or missing device-write attribution cannot produce a prescreen outcome.

For every transaction the supervisor records scheduled, enqueue, dispatch, service-before-
`begin_write`, writer-acquired, commit-enter, candidate-return, and terminal timestamps. Queue,
writer-acquisition, service, and end-to-end populations are separate. Oracle and sampling work run
outside the candidate service interval; lag and result-ring overflow invalidate the attempt rather
than being subtracted. All clocks and raw records use bounded binary framing fixed before execution.

## Steady-state matrix

The fixed seeds are `2026072301`, `2026072302`, and `2026072303`. Each mode runs all four probes once
per seed: 27 measured `W0`--`W2` attempts and nine short `HOLD` attempts. Candidate mode and probe
order rotate by a precommitted schedule; failures are retained and never silently rerun under the
same slot identity.

Each `W0`--`W2` attempt has 15 seconds warmup, 60 seconds measured open-loop traffic, 10 seconds
drain, and 15 seconds resource tail. Its hard wall-clock cap is 120 seconds.

| Probe | Offered traffic | Question |
|---|---|---|
| `W0` | one lane, 100 transactions/s, 128 mutations/transaction (128 KiB) | uncontended commit cost |
| `W1` | two disjoint-vnode lanes, 50 transactions/s each, 128 mutations/transaction | global-writer acquisition and fairness |
| `W2` | hot lane: 8 transactions/s at 4,096 mutations (4 MiB); victim lane: 100 transactions/s at 128 mutations | victim tail behind large commits |
| `HOLD` | holder acquires the writer for 250 ms; a victim starts 50 ms later | prove the sole-writer acquisition boundary |

`HOLD` uses a supervisor barrier after the holder has acquired its write transaction. The victim must
not report writer acquisition during the supervisor-controlled 250-ms live-transaction hold and
must acquire within 500 ms after the supervisor releases the holder to commit. A holder return
marker is not used as the unlock instant because its thread can be descheduled after releasing the
writer. The 500-ms bound is a prescreen soft limit, not a claim that redb provides cancellation or a
timeout API. `HOLD` has a ten-second external process deadline. In `W1` and `W2`, both lane
threads call `begin_write` directly; an adapter queue or dispatcher must not serialize them before
the engine acquisition point. `W2` is an owner-approved synthetic stress point combining the
profile's 4-MiB target batch with victim work; its mutation rate is not equated with a scenario's
source-row throughput gate.

The following soft limits apply to every valid repetition. A single miss is retained and cannot be
`REJECT_EXACT_PIN`; the final `DEFER` versus repeatable `PRESCREEN_NO_GO` rule is below:

- no child, adapter, oracle, timeout, result-ring, or sampling errors and at least 95% of the offered
  rate completed independently by every lane;
- `W0`/`W1`: service p99 at most 10 ms, end-to-end p99 at most 25 ms, and end-to-end maximum at most
  250 ms;
- `W2`: victim writer-acquisition p99 at most 25 ms and maximum at most 250 ms; victim end-to-end
  p99 at most 100 ms and maximum at most 500 ms; hot service p99 at most 250 ms and maximum at most
  one second; and
- cgroup memory current/peak at most 16 GiB, process file descriptors at most 256, and every
  bracketed XFS project-quota allocation at most four times logical live bytes.

Latency populations contain every transaction released during the 60-second measured interval for
that lane; a completion during the ten-second drain stays in the population. Warmup is excluded, no
outlier is removed, and an unreturned measured release prevents pass. Raw integer nanoseconds are
sorted and p99 is nearest rank `ceil(990*N/1000)`; maximum is the exact largest sample. Achieved rate
uses measured releases and completions over the fixed 60-second horizon, not drain time.

The resource tail records basic cgroup memory/CPU/I/O, queue/writer/service timing, file allocated
bytes, process file descriptors, `/proc` dirty/writeback state, block-device writes, and any
source-proven optional redb cache counters. Physical/logical allocation and write ratios are retained
as diagnostics, not imported C2 gates. It does not recreate resource-v2 or invent LSM debt,
background compaction, write-stall, or pinned-snapshot values. Full-tree statistics may be collected
only after the attempt and are labelled offline diagnostics.

## Process-crash matrix

For each transaction mode and seed, six fresh atomicity trials run against a verified 256-MiB
logical base. The clean fixture is copied byte-for-byte without reflink into an independent file and
hash verified before use; copy method/time/bytes are recorded outside the attempt. Every
fixture includes deterministic insert, overwrite, and delete sentinels in all four tables, plus
bounded churn that creates allocated/free-page fragmentation. Its recipe and canonical digest are
recorded. Each trial applies one cross-table transaction and kills only the child at one of these
supervisor triggers:

1. after the child records the complete intent but before commit entry;
2. immediately after the commit-entry marker;
3. 250 microseconds after commit entry;
4. 2 milliseconds after commit entry;
5. 10 milliseconds after commit entry; or
6. after candidate return and durable supervisor acknowledgement.

After opening its private copy, the child first completes one returned priming transaction in the
trial's `I1`, `I2`, or `QR` mode and remains open. The post-prime state becomes that trial's old-state
oracle. This prevents the clean fixture's `Database::drop` quick-repair allocator state from making
an interrupted first `I1`/`I2` commit look artificially cheap. The target transaction then starts
without closing the database.

The intent contains the post-prime digest, complete intended mutation digest, transaction, mode,
seed, trigger identity, and sequence number. Shared memory exposes monotonic `intent`,
`commit_entered`, `candidate_returned`, and `acknowledged` transitions. Child marker stores use
release ordering. The supervisor writes acknowledgement only after observing candidate return, and
trigger 6 waits until acknowledgement is visible before kill.
The supervisor records the markers observed when it requests the signal, then after `waitid`/pidfd
exit rereads their final values with acquire ordering. Final markers classify the trial because
return may race signal delivery; a requested timed trigger is not silently called “in commit.”
Across triggers 2--5, each mode needs at least three finally confirmed
`commit_entered && !candidate_returned` kills. At most two extra, separately identified trials per
mode may vary only the delay to meet that coverage; otherwise the outcome is `DEFER`.

Shared memory is protocol state, not proof of database durability. `SIGKILL` is delivered by
PID/pidfd and the supervisor records delivery and observed exit. The child must die without unwind
or `Database::drop`: redb's clean drop makes a quick-repair/shrink commit and can mask the crash path.
A normal child exit is invalid. Graceful drop is measured separately as a clean-close control;
container stop, machine reboot, and power loss are different fault classes.

After each kill, a fresh process opens the file and independently scans all four tables in canonical
order. Open and full scan each have a 60-second cap and their durations are never combined. An
attempt killed before `commit_entered` must contain exactly the old state. Once commit was entered
but before `candidate_returned`, either exactly old or exactly complete new is allowed. Once
`candidate_returned` is visible, complete new is required whether or not supervisor acknowledgement
was written; acknowledged is therefore also complete new. A torn/mixed transaction, extra key/value,
missing post-return mutation, checksum/corruption error, candidate/redb panic attributable to valid
input, or non-canonical duplicate is `REJECT_EXACT_PIN` when the target and actuator evidence are
valid. Timeout, actuator ambiguity, host noise, harness/oracle panic, or resource-observation failure
is `DEFER`.

A separate large-recovery comparison uses one 4-GiB fragmented fixture per mode and seed: nine
trials total, each with a confirmed in-commit kill. Independent clean-control and crash copies start
from the same verified fixture. Both execute and retain the mode-specific priming commit while open;
the control then closes normally, while the crash copy begins the target transaction and is killed
without drop. It records clean-control reopen, crash reopen, and full-scan duration separately.
Before each reopen, the dedicated host follows the same reviewed, recorded file/device quiescence
and page-cache-reset procedure; no comparison is accepted if either side's cold state cannot be
established. Docker Desktop/WSL results are never used here. These nine trials answer recovery cost;
the smaller 54-trial matrix supplies broad atomicity timing coverage.

For `QR`, crash-reopen median must be at most two seconds and each crash-reopen at most five seconds.
If the matching `I2` crash-reopen median exceeds two seconds, `QR` median must also be no more than
half that `I2` median. These are soft investment gates; the full scan remains correctness-bearing
and cannot be replaced by reopen success or redb's internal statistics. Median and ratio are
complete-population gates; a valid miss is `PRESCREEN_NO_GO`. One seed exceeding the five-second
maximum is `DEFER`; two or more are `PRESCREEN_NO_GO`.

## Bounds and disposition

The performance base is 1 GiB logical, the atomicity base is 256 MiB, and the large-recovery base is
4 GiB. Each attempt starts from a new verified directory and has a 16-GiB physical allocation cap.
The complete prescreen is bounded to 250,000 transaction samples, 2 GiB of retained artifacts, 15
minutes of build/environment preflight, 120 seconds per steady attempt, ten seconds per `HOLD`, 120
seconds per crash open-and-scan pair, 60 seconds each for the nine large clean closes and clean
reopens, at most six replacement atomicity trials, 60 aggregate minutes for fixture copies, priming,
cache resets, and artifact finalization, 60 seconds for final cleanup, and five hours wall clock.
Reaching a bound fails closed as `DEFER`; no partial population may be called a pass. The enumerated
caps consume 287.5 minutes including shared overhead and cleanup, leaving 12.5 minutes of the hard
campaign budget.
These ceilings are materially cheaper than the 45-hour C2 sketch.

The single final outcome is:

- `REJECT_EXACT_PIN` for one fully attributable atomicity/durability/corruption invariant violation
  on a valid target; at most one bounded diagnostic repetition may follow, and it cannot downgrade
  that outcome;
- `PRESCREEN_NO_GO` when the same attributable candidate writer/latency/recovery/resource soft gate
  fails in all three valid seed repetitions for a mode/probe, when a complete-population QR rule
  above says so, or when deterministic sole-writer liveness prevents progress;
- `DEFER` for an isolated soft-limit miss, environmental invalidity, unexplained harness error,
  missing in-commit/cold-cache evidence, missing artifacts, or bound exhaustion; or
- `PRESCREEN_PASS` only when every scheduled slot passes and the named workload and operations
  owners approve the source, harness, raw artifacts, oracle, and disposition.

Harness, oracle, sampler, actuator, and observation faults always map to `DEFER`, even if repeated;
they never become candidate `PRESCREEN_NO_GO` by repetition.

Every artifact repeats `NOT C2/C3 QUALIFICATION EVIDENCE` and sets all qualification, selection,
production, and admission booleans false. A pass still leaves DKS-Q2-006's mechanism-map schema and
redb mapping, DKS-Q2-007 persistence/configuration work, the complete candidate adapter, C1/C2/C3,
physical fault/endurance qualification, and independent production soak open.

Before owners approve this prescreen, an exact-source mechanism note must inventory the sole-writer
wait, synchronous allocator reclamation, quick-repair allocator-state writes, clean-close
quick-repair/shrink commit, kernel writeback, and any thread/background activity. That proof—not the
absence of activity in a short run—decides which DKS-Q2-006 arms may be `not_applicable`. The probe
only corroborates the pinned source/configuration mapping. The separately bound execution,
preflight, and observation policies must freeze the XFS quota query, units, one-second/boundary cuts,
16-GiB hard-quota setup, and error/wrap behavior used by the allocation gate. The current profile v3
remains Fjall/RocksDB-specific, so even a pass needs an
additive redb profile/schema proposal rather than editing or reinterpreting `linux-nvme-v3`;
`linux-nvme-v2` remains an immutable regression fixture.

## Docker Desktop/WSL smoke subset

Docker Desktop on this Windows host may run a smoke-only subset using the exact pinned Linux build
and a Docker volume. It checks harness construction, the four-table layout, schema/golden/oracle
agreement, one transaction in each mode, one `HOLD`, and one trial at each kill trigger against a
64-MiB base. Optional two- and five-second lane bursts may catch gross deadlocks.

Every such artifact is labelled `SMOKE_ONLY / NO_DECISION`. A named-volume database uses Docker's
managed ext4/VHDX/NTFS/shared-NVMe path (while the container root also uses overlayfs); it cannot
validate XFS quota, direct device writes, physical amplification, native-NVMe latency,
power loss, endurance, C2/C3, or the prescreen disposition. Passing Docker smoke is a prerequisite
for spending target-host time, not evidence that redb is suitable.
