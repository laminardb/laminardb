# State-backend carry-forward matrix — Cycle 16

- **Date:** 2026-07-24
- **Decision scope:** choose where to spend the next backend-qualification effort
- **Production backend selected at Cycle 16:** none; Cycle 40 later selects official `tidesdb-rs`
  as the TidesDB integration line, not a qualified backend
- **Cluster admission:** unchanged; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Historical recommendation (superseded for redb):** carry RocksDB into a bounded DKS-Q2-006
  mechanism-closure task; carry redb only into its separately approved native prescreen; do not
  carry unmodified Fjall or SurrealKV

**Cycle 17 supersession:** the bounded
[RocksDB mechanism-source closure](rocksdb-mechanism-source-closure-2026-07-24.md) stopped at Stage
0. An apparently bounded stall observer is plausible but unproved, and this matrix's assumption
that the existing pending-compaction estimate could satisfy v1's direct/disjoint debt population
was wrong. Do not start the stall-only patch from this historical recommendation. The Cycle 17 next
step was an explicit choice between retaining v1 and funding broader native instrumentation and
configuration proof, or issuing an additive qualification contract with reviewed
candidate-specific health signals. No backend was selected.

**Cycle 34 redb supersession:** redb is now `PARKED`, an administrative stop rather than a formal
`DEFER` result. Its design timebox is exhausted and the native prescreen is no longer active work.
Reopening requires the separately recorded one-page, two-engineering-day/four-machine-hour,
separately versioned micro-prescreen charter and new candidate-execution authority. Every later redb
imperative, recommendation, work-order step, and “hedge” statement in this dated report is historical
and superseded. This matrix does not authorize further redb protocol or adapter work.

**Current authority:** this report remains dated source/gap history; its RocksDB work order is
superseded. The [Cycle 40 package design](../architecture-decisions/tidesdb-local-state-successor-design.md)
selects exact official `tidesdb-rs v0.11.1`/native 9.3.6 only as a restricted-facade prescreen
subject. RocksDB/Fjall remain inactive v4 references, and no runtime, adapter, execution, or
production authority follows.

## Historical Cycle 16 recommended judgment

The recommended next investment is the exact `rocksdb =0.24.0` / RocksDB 10.4.2 subject, but only
for closing its known observation and control gap. This is not a production selection and does not
authorize an adapter or candidate run. RocksDB already exposes most required state, iteration,
restore, pressure, and operational primitives. Its known candidate-specific DKS-Q2-006 source/
binding gap is narrower than the published Fjall gap: prove or expose complete database/write-
buffer-manager stall episodes and account for native memory and shared controllers. The common
DKS-Q2-006 mapping, validator, XFS/cgroup/device/process/lifecycle observations, synchronous FFI
tail latency, and C3 concurrency all remain open. Engineering effort and elapsed cost have not been
measured.

redb 4.1.0 is the bounded contingency. Its Rust-native, no-C++ engine and lack of background LSM
maintenance are attractive, but its single database-wide, non-cancellable writer is an architectural
risk for low-latency disjoint-vnode lanes. Exact-source review establishes the sole-writer contract;
the new construction runner only exercises the pinned crate, four-table layout, durability-mode
calls, synchronized writer scenario, read-only scan, and redb-free row oracle on Windows and Linux
Docker. It supplies no latency, crash, recovery, native-device, or backend-selection evidence. redb
has no formal prescreen outcome today. A future `PRESCREEN_PASS` would fund, not supply, its profile
and mechanism mapping; candidate admission remains **DEFER** until those later artifacts are approved.

Unmodified Fjall 3.1.8 is not carried because it fails the published DKS-Q2-006 contract, not because
RocksDB won a benchmark. Reconsider it only as an explicitly funded pinned fork or upstream release
that adds the complete stable signals and global controls before adapter work. SurrealKV 0.21.2 is
not carried because its exact-source snapshot-registration failure is correctness-bearing; it would
first require a correctness and liveness fork, then the same observation work.

If the organization has a hard Rust-native/no-C++-engine policy, it should not silently promote redb
or Fjall. It should choose and fund either the Fjall patch path or the approved redb prescreen, while
keeping cluster admission closed. Under the current production/latency policy, RocksDB is the most
appropriate primary track because it has the narrowest currently known candidate-specific mechanism
gap; redb is a bounded architectural hedge. This is a qualitative recommendation from source gaps,
not a measured engineering-cost ranking.

## Absolute-gate comparison

`Present` below means exact-source capability, not a passing qualification result. `Blocked` and
`unknown` are not converted into scores; a weighted total could wrongly let convenient features
cancel a correctness, durability, latency, or observability veto.

| Candidate | State/restore primitives | Hot path and concurrency | Governance and resources | Durability/recovery | Current evidence | Absolute disposition |
|---|---|---|---|---|---|---|
| Fjall 3.1.8 | Atomic cross-keyspace batch, consistent snapshot, ordered range/prefix scan, sorted ingest present; no native multi-get, range tombstone, physical checkpoint, or backup | Rust-native/no C++ engine; slices can pin blocks; ingestion holds the global journal mutex across flush/sync; C3 unmeasured | Missing stable complete debt, stall, compaction-I/O, cache/pinned-memory, applied-option, snapshot-retention observations, and enforceable global write-buffer control | Explicit journal persistence APIs exist; physical cache-loss truth table, N/N-1 restore, and fatal error handling unproved | Exact-source audit plus historical non-qualifying warning data | **FAIL as published.** Stop unless a telemetry/control patch is funded first. |
| RocksDB 10.4.2 via `rocksdb` 0.24.0 | Atomic cross-CF batch, snapshots, bounded iterators, multi-get, WAL flush, checkpoint, SST ingest, and range delete present | Synchronous, non-cancellable FFI; database-wide controllers and WBM are shared; native allocation and hot-writer/victim tails require C2/C3 evidence | Broad properties and controls exist, but the exposed stall ticker does not prove the WBM/database-scope stall path; native memory remains externally authoritative | Credible WAL primitives, but exact fsync/fdatasync mapping, cache-loss truth table, complete configuration, corruption policy, and N/N-1 restore unproved | Exact-source audit; no approved candidate run | **BLOCK, primary closure track.** Expose/prove the missing stall path before adapter or campaign execution. |
| redb 4.1.0 | Atomic cross-table write, MVCC read snapshots, ordered ranges, and portable full scan present; no multi-get, range tombstone, online backup, native checkpoint, or bulk ingest | Exactly one database-wide writer; `begin_write` blocks without try/timeout/cancel; synchronous cache eviction, sync, resize, repair, and close work; C3 unmeasured | No background LSM worker in the pinned build, so debt/stall arms may become N/A only after source-bound review and native corroboration; full-tree stats are forbidden during measurement | Immediate I1/I2 and QR source mappings exist; directory/power-loss boundary, crash atomicity, reopen cost, shrink ordering, and N/N-1 unproved | Exact-source audit plus non-gating 64-MiB construction checks only | **DEFER, prescreen only.** Do not build a full adapter unless the approved native prescreen passes. |
| SurrealKV 0.21.2 | One-tree atomic transactions and snapshot-filtered reads exist, but snapshot tracking can unregister a still-live sequence used by compaction | Async paths perform synchronous filesystem work; drain/wakeup liveness and concurrent commit behavior need repair and proof | Public complete debt/stall/cache/pinned/applied-option observations absent | WAL ordering, strict recovery, oversized-batch admission, and close behavior need a pinned correctness fork | Exact-source audit; no candidate execution | **REJECT unmodified.** No qualification spend before correctness/liveness fixes. |

The detailed source basis is the [exact-source backend audit](state-backend-static-audit-2026-07-23.md),
the [redb mechanism note](redb-4.1.0-prescreen-mechanism-note-2026-07-23.md), and the complete
[DKS-Q2-001 through DKS-Q2-009 checklist](../architecture-decisions/state-backend-qualification-runner-v1.md#blocking-execution-and-selection-issues).

## Historical Cycle 16 cost to the next honest decision

| Track | Smallest decision-producing work | Stop condition | What a pass permits |
|---|---|---|---|
| RocksDB primary | Pin the exact build/SBOM/options; add a reviewed source/binding for complete WBM/database stall episodes; prove native-memory and observer mappings without changing workload behavior | Any stall class remains unobservable, mapping overhead changes the measured population beyond its gate, or shared controls cannot be bounded | Admission to the common C1/C2/C3 qualification sequence after the other DKS-Q2 approvals close; not production selection |
| redb contingency | Repair and freeze the contract; implement and bind the external approval/verifier, native supervisor/harness, crash actuator, oracle, and result classifier; obtain two-owner approval; only then run the bounded native writer/commit/crash/recovery prescreen | Invalid authorization/environment/actuator evidence; torn post-return state; deterministic sole-writer liveness failure; repeatable valid latency/recovery/resource miss; or final `DEFER` | Funding for a redb-specific profile, persistence/mechanism mapping, and adapter design review; not C1/C2/C3 admission by itself |
| Fjall alternative | Patch/upstream stable cheap complete debt/stall/compaction-I/O/cache/pinned/snapshot/applied-option observations plus global pressure controls | Any applicable signal is still private, sampled too slowly, incomplete, or encoded as a false zero/N/A | Re-entry as a candidate subject; not inheritance of old benchmarks |
| SurrealKV alternative | First fix reference-counted snapshot retention, range-snapshot ownership, wakeup/drain, oversized-batch WAL ordering, close/recovery; only then add governance telemetry | Any forced-compaction snapshot or recovery invariant fails | A new bounded prescreen proposal, not candidate admission |

**Cycle 24 supersession note (2026-07-25):** the redb protocol remains unready for approval or
execution, but the Cycle 16 offline-signature work item is no longer the selected direction. The
protected-provider design deliberately avoids inventing a local signature preimage, trust root or
revocation service. Current blockers are the exact result/evidence wires, cleanup recovery and
deletion-safety contract, live dispatch/run/review provenance, immutable-storage proof and trusted
finalization-registry linearization. The construction lane does not paper over those blockers; see
the current [redb prescreen protocol](../testing/state-backend-redb-prescreen-v1.md) for the normative
boundary.

The recommended work order is therefore:

1. run a bounded source/binding closure for RocksDB's DKS-Q2-006 gap, with its effort and elapsed-time
   caps approved before execution;
2. in parallel only after the named approvals exist, run redb's bounded native prescreen;
3. if RocksDB closes its mechanism contract, carry it into the common campaign and let C1/C2/C3,
   fault/endurance, restore, and tail evidence decide;
4. if RocksDB cannot close and redb prescreen passes, design the additive redb profile before any
   adapter; and
5. if both paths fail, stop and explicitly fund a Fjall fork/upstream effort or keep the feature
   unavailable. Do not weaken DKS-Q2-006 or select by elimination.

## Cycle 16 redb construction result

The isolated workspace pins redb 4.1.0 with default features disabled and leaves the LaminarDB root
workspace and lockfile redb-free. Its gate has no redb dependency and reconstructs the expected four
tables record by record. Candidate and gate forbid unsafe code in their own source; this is not a
claim that the redb dependency contains no unsafe implementation.

On local Windows and pinned Linux/amd64 Docker, the final construction lane:

- created exactly 64 MiB logical state across `state`, `timer`, `join_left`, and `join_right`;
- checked the exact prior value for every insert, overwrite, and delete;
- executed one returned I1, I2, and QR transaction and a synchronized sole-writer HOLD;
- scanned from a separate process using `open_read_only` while the database was marked read-only;
- left the database SHA-256 unchanged across scan; and
- produced 65,536 canonical rows, 67,174,456 export bytes, and SHA-256
  `a82240b51daf373ce03bbff9cd70bede90eda1b8433ef39e6be0754dd76e7290`.

The Docker run used the network-disabled Linux/amd64 image
`rust@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1`,
Rust/Cargo 1.95.0, a 6-GiB container limit, a 512-MiB per-file limit, and 60-second external process
deadlines. The disposable database/output directory was tmpfs inside Docker Desktop/WSL, not native
XFS or dedicated NVMe. The construction report hard-codes every qualification, selection,
production, delivery, fault, and soak eligibility field to false. Its construction wall-times and
file-size ratios are deliberately excluded from this recommendation.

The hosted `ubuntu-24.04` job passed on its first attempt in [run
30072229655](https://github.com/laminardb/laminardb/actions/runs/30072229655) against commit
`c26763a2`. The runner used Ubuntu 24.04.4, runner 2.336.0, image `20260720.247.2`, and exact Rust
1.95.0. It reproduced the canonical row count, export size, and digest above; the complete workflow
and required `CI Success` gate also passed on the first attempt. This proves only that the same
bounded construction mechanics work on that hosted runner. No artifact was uploaded, and the job
cannot become prescreen, qualification, backend-selection, latency, or production evidence.

## Delivery, exactly-once, and backend independence

The backend choice cannot create distributed correctness. All four local stores lack vnode
ownership, epoch fencing, checkpoint decisions, source-offset sealing, sink commit, rebalance
transfer, and an authoritative remote artifact. LaminarDB must keep these responsibilities separate:

| Boundary | Required capability | Backend consequence |
|---|---|---|
| Local working state | Atomic state/timer/join mutations at one fenced vnode epoch; bounded reads/scans and memory/disk admission | Selected store is replaceable and disposable after a committed checkpoint; no native directory becomes cluster authority |
| Checkpoint | Freeze one vnode generation at the aligned barrier; write versioned portable full/delta artifacts; seal exact inventory and coordinator decision | Snapshot/export pause and retention are measured; native checkpoint/SST features are optimizations only |
| Source | Replayable offsets or cursors, partition/vnode mapping, term-fenced handoff, and offset sealing in the same checkpoint decision | Only source profiles separately certified for the requested delivery contract may run; otherwise the combination remains fail-closed |
| Sink | Transactional commit composed with checkpoint/source-cursor decisions, live-term fencing, and ambiguous-commit recovery, or a proven idempotent/fenced write protocol | Only sink/update-mode profiles separately certified for ALO or exactly-once may run; a local durable commit cannot upgrade them |
| Recovery/rebalance | Restore to an unservable generation, verify, atomically publish ownership, fence the old epoch, and garbage-collect only after retention permits | Backend must pass restore/cleanup/hot-victim C3 races but does not own distributed authority |
| Release | Immutable artifact, pre-approved duration/event-count charter, target hardware, black-box oracle, and a genuinely independent operator | The separate 24–72-hour backend endurance run is insufficient; no cluster feature is production-ready before independent product soak and fault review |

Therefore the recommended Cycle 16 judgment is intentionally two-level: **carry RocksDB for the
next closure gate, keep redb as a bounded prescreen hedge, and select no production backend yet**.
