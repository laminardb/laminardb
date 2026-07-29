# redb 4.1.0 bounded backend review

Date: 2026-07-29

Subject: upstream `redb` 4.1.0, tag commit
[`6ed1f981`](https://github.com/cberner/redb/commit/6ed1f981ba4deab0b2adbdd7bccb46ec409b2191)

Disposition: **stop 4.1.0; reconsider only an official successor**

## Decision

Do not select redb 4.1.0 or build its Laminar adapter. Its narrow semantics are plausible for a
single-owner local state lane, and no correctness veto appeared in the reviewed upstream tests or
existing construction smoke. The bounded executable review instead exposed unresolved physical
space, maintenance, and shutdown risks under Laminar's disposable-state profile.

On WSL2 Ubuntu/ext4, a 60 MiB logical fixture occupied 2.01x physical bytes at baseline. A scenario
with no baseline durability barrier reached 8.23x while a snapshot was retained and 8.28x after
release. After the first `Immediate` barrier, two equal churn/barrier rounds remained at 8.28x:
redb reused freed space but did not return it to the filesystem. Starting with a durable baseline
reduced the retained-snapshot result to 2.60x; two later churn/barrier rounds settled at 3.01x. An
offline `Database::compact()` reduced both scenarios to 1.94x.

These are physical allocations from Linux `st_blocks * 512`, not apparent file lengths. The current
v4 reference continuation threshold is 2.5x, but this was not its full workload, collector, target
host, or qualification process. Therefore this is a no-select prescreen result, not a formal backend
qualification failure. It establishes that 4.1.0 needs a candidate-specific proof for its durability
barrier cadence, peak checkpoint allocation, offline compaction policy, and shutdown boundary before
an adapter would be responsible engineering.

The review stops there. It does not fund fault injection, A/B work, soak, runtime code, or a
production claim. `[LDB-4007]` and `[LDB-0013]` remain fail-closed.

## What the old evidence proves

The existing `tools/state-backend-redb-prescreen` lane is a construction smoke, not a prescreen or
qualification. It pins exact 4.1.0 with default features disabled, creates four tables, runs one clean
transaction in three durable modes, shows that a second writer blocks, reopens, and deterministically
exports 64 MiB. Its two candidate tests, seven gate tests, and release build passed again on Windows.

It does not cover normal point/range traffic, dirty recovery, checkpoint/restore, rebalance, crash or
ENOSPC, bounded memory, physical allocation, skew, tail latency, or resource stability. Its 8 GiB
cache also makes its timings unusable for selection.

Selected upstream 4.1.0 tests passed locally:

```text
previous_io_error                         1 passed
non_durable_commit_persistence/isolation  2 passed
concurrent_write_transactions_block       1 passed
```

An earlier local exploratory harness also found no atomic state-plus-journal, retained-snapshot,
logical export/import digest, or vnode-range mismatch. That harness was not retained, so these are
supporting observations rather than decision authority. The tracked review below reproduces only the
resource result that drives the stop.

## Source fit

| Requirement | redb 4.1.0 behaviour | Laminar consequence |
|---|---|---|
| Ordered point/range state | Byte keys are lexicographically ordered; `get`, `insert`, `remove`, and double-ended `range` cover the narrow surface | Source pass |
| Atomic batch | One write transaction can cover state and journal tables | Source pass; construction observed |
| Concurrency | One database-wide writer; `begin_write()` blocks without try, timeout, or cancellation | Conditional: keep the database and every transaction on one bounded owner lane |
| Buffered local state | `Durability::None` gives no crash-persistence guarantee but still writes through redb's file buffer | Never use it as source-offset, sink-output, checkpoint, or EO authority |
| Page reuse | Non-durable commits pin a durable ancestor until a later `Immediate` commit | A bounded barrier cadence is part of the resource lifecycle even for disposable state |
| I/O failure | The first I/O error is returned and latched; later operations return `PreviousIo` | Poison the Laminar generation, never retry it, and restore a fresh root |
| Cache | Default 1 GiB can be overridden, but is not a process-RSS bound; metrics need an optional feature | Laminar still owns queue, RSS, disk, FD, and file-growth governors |
| Close | No fallible public database close. `Drop` performs a quick-repair/shrink commit and backend close | Reviewed default-feature build silently discards close errors; optional logging in another build does not make them actionable |
| Dirty recovery | Default repair may scan the database; quick repair increases durable-commit work | Do not depend on dirty-root reopen; restore a portable committed checkpoint |

The one-writer and durability contracts are documented by redb's
[`Database`](https://docs.rs/redb/4.1.0/redb/struct.Database.html) and
[`Durability`](https://docs.rs/redb/4.1.0/redb/enum.Durability.html) APIs. The storage interface returns
operation errors, but its close is invoked by database drop; see
[`StorageBackend`](https://docs.rs/redb/4.1.0/redb/trait.StorageBackend.html). Upstream issue
[#1072](https://github.com/cberner/redb/issues/1072) remains open for database drop blocking when a
write transaction outlives it. A lexical Laminar owner API can prevent that misuse but cannot create
a close result or cancellation API.

Upstream merged page-reuse change
[#1201](https://github.com/cberner/redb/pull/1201) nine days after 4.1.0. It is relevant to a future
repeat, but this review does not assume that it fixes the observed workload.

## Reproducible resource control

The tracked [`resource_review.rs`](../../tools/state-backend-redb-prescreen/candidate/src/bin/resource_review.rs)
uses the existing isolated lockfile: exact `redb = 4.1.0`, default features off, Rust 1.95, one
database, one owner, 256 MiB cache, 262,144 32-byte keys and 208-byte values across 256 vnodes. It
holds one read snapshot across bounded non-durable get/overwrite batches, measures physical bytes,
then runs three durability-barrier/churn observations, offline compaction, and clean drop.

The two WSL2 Ubuntu/ext4 commands require separate existing empty directories:

```bash
CARGO_TARGET_DIR=/tmp/laminardb-redb-resource-target \
  cargo +1.95.0 run --locked --release \
  -p state-backend-redb-prescreen-candidate --bin resource_review -- \
  /tmp/laminardb-redb-buffered

CARGO_TARGET_DIR=/tmp/laminardb-redb-resource-target \
  cargo +1.95.0 run --locked --release \
  -p state-backend-redb-prescreen-candidate --bin resource_review -- \
  /tmp/laminardb-redb-barriered baseline-barrier
```

Recorded physical results:

| Phase | No baseline barrier | Durable baseline |
|---|---:|---:|
| Baseline | 126,418,944 B / 2.009x | 126,418,944 B / 2.009x |
| Snapshot held | 518,098,944 B / 8.229x | 163,586,048 B / 2.598x |
| Snapshot released plus 50,000 writes | 521,224,192 B / 8.278x | 164,110,336 B / 2.606x |
| Churn after first `Immediate` barrier | 521,228,288 B / 8.278x | 189,435,904 B / 3.009x |
| Equal churn after second barrier | 521,228,288 B / 8.278x | 189,800,448 B / 3.014x |
| Offline compact | 122,159,104 B / 1.940x | 122,159,104 B / 1.940x |
| After clean drop | 122,191,872 B physical | 122,191,872 B physical |

The apparent file length after drop was 244,305,920 bytes while physical allocation stayed near
122 MiB, demonstrating why file length is not the space gate. The post-barrier rounds show effective
reuse once the durable ancestry advanced; compaction was needed to return physical allocation below
the 2.5x reference threshold in this control, not to prevent immediate further growth.

Maintenance timings on WSL2 varied too much for a latency verdict. In the final two commands,
barriers ranged from 0.315 to 3.385 seconds and compaction from 2.336 to 2.496 seconds; earlier warm
runs were much shorter. Only target-host workload testing can set the latency policy.

## Fit matrix

| Decision question | Result |
|---|---|
| Narrow keyed records and atomic logical roles | Source pass; existing construction only |
| One serialized owner lane | Architecturally possible; queue, skew, and tail fit unqualified |
| Portable checkpoint and fresh vnode restore | Plausible logical design; not retained decision evidence |
| No-baseline-barrier physical allocation | **8.278x before the first barrier in this control** |
| Physical allocation below the 2.5x reference after checkpoint churn | **No in this control** |
| Page reuse after durability barriers | Effective in this bounded repeat; 8.278x stayed flat and 3.009x moved to 3.014x during equal churn |
| Bounded, observable maintenance and close | Unresolved; compaction is offline and close errors are not returned |
| Source/sink exactly-once authority | No, by design; coordinator and connector protocols remain authoritative |
| Ready for adapter or production selection | **No** |

## Re-entry boundary

Do not test a git fork or unreleased master. A newer official redb release may repeat this exact
resource control, including both durability profiles. Continue only if a candidate mapping explains
and meets the approved physical-allocation, barrier-latency, compaction, and shutdown limits without
putting blocking maintenance on the event loop. No individual upstream change is pre-credited.

A favorable repeat would authorize only an adapter proposal. Full Linux NVMe/XFS qualification,
failure injection, crash/ENOSPC ambiguity, concurrent export-under-write load, open-loop queue and
hot-skew behaviour, vnode revoke/staged activation, EO/ALO and failover regressions, and an
independent cleaned-release soak would still be mandatory.

## End-of-cycle review

- **Code/unused surface:** no Laminar runtime code, backend trait, runtime dependency, workflow,
  schema, or certification helper was added. One 240-line isolated reproducer is retained because it
  is the minimum source needed to reproduce the decision-critical physical result.
- **AI slop/over-engineering:** the archived 3,976-line protocol was not revived. Work stopped at the
  bounded resource concern; no adapter or speculative sharding was built.
- **Production readiness:** this is a stop/no-select result, not qualification or production evidence;
  fail-closed admission is unchanged.
- **Tests:** the isolated workspace builds/tests and both resource scenarios completed as observations.
  Target hardware, faults, long-run growth, integration, regression, and soak remain unrun by design.
- **Documentation:** this report replaces the current parked narrative. `DKS-CLEANUP-001` should still
  remove the oversized protocol and required construction CI lane while retaining this decision and
  its minimal reproducer until a successor repeat or backend selection closes the alternative.
