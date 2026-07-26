# Distributed keyed state Cycle 59 review

- **Date:** 2026-07-26
- **Scope:** bounded exact checkpoint barrier-pause evidence and engineering-harness consumption
- **Code outcome:** accepted for engineering diagnostics in commits `40e3637b`, `6084462b`,
  `034b14e9`, `3909f4d4`, `136d6a6d`, `4c62c4dd`, `7782a032`, and `1a6dff80`
- **Empirical outcome:** corrected optimized one-kill engineering run passed
- **Runtime/backend outcome:** no keyed operator, working-state backend, admission flag, source/sink
  contract, or delivery guarantee changed
- **Production verdict:** **NO-GO**; `[LDB-4007]`, `[LDB-0013]`, backend qualification, complete
  latency authority, instrumentation A/B, and the independent immutable release-binary soak remain
  open

## Accepted boundary

Cycle 59 implements exactly three local timing families: pipeline stall, local barrier, and optional
aligned resume. A preallocated fixed-capacity ledger receives one O(1), nonblocking record under the
same drop-guard scope as the corresponding histograms. Recording performs no row-path, network,
filesystem, object-store, serialization, state-backend, or blocking work. Failure to retain a
record is explicit evidence loss, never a silent omission.

Authenticated `GET /api/v1/cluster/local-checkpoint-barrier-timings` exposes a cache-disabled,
bounded page after an exclusive process-local sequence. After bootstrap, every request binds node,
boot incarnation, and process term. The response reports exact ring capacity, oldest and next
sequence, physical overwrite count, recording-loss count, metadata exhaustion, continuation, and
fixed records. Stale identity/cursor, unread-window overwrite, malformed algebra, or serving-gate
loss fails closed. Physical eviction is valid only after the collector exported the affected
records.

The engineering collector:

- binds local adoption to a converged owner-map fence, then binds each sampled converged version to
  the distinct full assignment-certificate digest carried by its runtime timing records;
- rejects gaps, duplicate/regressed/conflicting attempts, identity drift, loss, exhaustion,
  impossible stage relationships, and exact histogram count/diagnostic-bucket disagreement;
- finalizes a generation only after exact-ledger and Prometheus samples form a coherent observed
  cut;
- streams every record collected through that cut to schema-versioned per-generation JSONL and
  closes each writer at finalization; and
- keeps RAM bounded by 1,024 process generations, 1,024 observed assignment versions per process,
  fixed counters/maxima, and eight diagnostic witnesses per class.

Disk evidence intentionally grows with observations. Assignment anchoring covers only versions
independently sampled at converged harness cuts, not every historical version that existed between
samples. Neither limitation may be relabelled as complete lifetime authority.

## Engineering run

The subject was the optimized soak-profile test binary built from `7782a032` on Windows, using
Docker Desktop's WSL2 engine, MinIO `RELEASE.2024-11-07T00-52-20Z`, and Redpanda v26.1.13. It used
static three-node discovery, 64 key groups, 96 Kafka partitions, a unique S3-compatible prefix,
400 records/s, one checkpoint-gated leader `kill -9`, a 90-second configured tail, and a 90-second
recovery ceiling. This was an implementer-run engineering test, not the chartered independent
release subject.

| Evidence | Result |
|---|---:|
| Test duration | PASS, 207.20 s |
| Producer acknowledgements | 79,996 in 200.0 s |
| Frozen output prefix | All 79,996 IDs present |
| Duplicate output IDs | 2,758 tolerated and counted; byte identity and sealed-cut replay legality were not proved |
| Failover / rejoin | 43.43 s / 34.51 s, within the engineering ceiling |
| Exact timing records | 392 across four process generations |
| Pipeline/local/aligned observations at or below 1,024 ms | 100% / 100% / 100% |
| Maximum pipeline stall | 962.8988 ms |
| Deadline exhaustion / missing handoff / recording loss | 0 / 0 / 0 |

Workspace-local evidence is under
`target/tmp/soak-570208-1785098187039362300`; it is not an immutable retained evidence store.

| Artifact | Records | SHA-256 |
|---|---:|---|
| `checkpoint-timing-node0-generation1.jsonl.log` | 139 | `697b42ce332f0ec83259e1132da7bcc5346a6d1977633c35ada83960e2fe4ed4` |
| `checkpoint-timing-node1-generation1.jsonl.log` | 139 | `9422c934680a723e295d084f3f3adb60374761751563b00fd4a46c6ca6f210f0` |
| `checkpoint-timing-node2-generation1.jsonl.log` | 4 | `ff069f2a72a52f86bc2a7f34a0d603f371cabd1effc477ad5309e6ee9f7f1120` |
| `checkpoint-timing-node2-generation2.jsonl.log` | 110 | `edbb8ae68c411e90658098eac9c87d509d8b61133aea4dabc17d0b47041a42ac` |

Focused verification on the final code tree:

| Gate | Result |
|---|---:|
| Ledger/metrics API tests | PASS, 10/10 |
| HTTP route/auth/bounds/error tests | PASS, 7/7 |
| Non-ignored `cluster_soak` tests | PASS, 33/33; 2 real-process tests remain ignored by default |
| Certificate-domain and substitution tests | PASS, 2/2 within the harness suite |
| `cluster,kafka` harness Clippy with warnings denied | PASS |
| Rust formatting and changed-document relative links | PASS; 8 documents checked |

The later `1a6dff80` defense against reporter/owner-map/boot substitution has focused deterministic
test and lint coverage but was committed after these artifacts and was not empirically rerun. The
earlier Cycle 57 failures remain retained. This green run validates the new exact evidence and
its current workload result; it does not identify the earlier two slow attempts, excuse them, or
establish a preapproved production RTO/latency profile.

## Cycle review

- **AI slop — pass:** claims are limited to the three recorded families and observed converged
  cuts. No outcome/capsule, complete history, source cut, sink transaction, exactly-once, keyed
  runtime, backend, or production claim is inferred.
- **Overengineering and hot path — pass with an open measurement gate:** the recorder is a
  preallocated O(1) checkpoint-control-path write, and exact evidence is read only by a low-cadence
  diagnostic endpoint. It is not on the row path. A controlled instrumentation A/B is still
  required; this run alone cannot establish negligible tail impact.
- **Unused code — pass:** the real three-node harness consumes the endpoint, every process
  generation expected by the harness is finalized once, and every record collected through its
  observed cut feeds both artifact and reconciliation paths. No backend/runtime/admission
  dependency was introduced.
- **Production readiness — NO-GO:** no qualified TidesDB package or other backend, admitted keyed
  path, transactional Kafka source/state/sink composition, exact full-checkpoint/restorable-gate
  evidence, durable same-snapshot attempt audit, approved numerical profile, or independent soak
  exists. The 2,758 duplicate output IDs were tolerated by the engineering oracle; without byte and
  sealed-cut causality checks they prove neither charter-level legal replay nor exactly once.
- **Documentation — pass with one canonical schema owner:** the Cycle 58/59 audit owns detailed
  timing semantics. ADR, plans, validation report, and soak charter summarize and link that boundary
  without treating JSONL or the endpoint as a public product API. No newly obsolete research file
  was found or removed.
- **Tests — pass for the implemented slice:** ledger/timer, HTTP auth/bounds/error mapping, exact
  collector, certificate-domain/substitution, formatting, and warnings-denied lint gates pass. The
  direct HTTP suite still lacks a nonempty multi-page response test, and the final coherent-cut retry
  loop lacks a deterministic interleaving test; collector pagination and maximum-envelope
  serialization are tested separately.

## Cycle 60 review plan

1. **AI slop:** separate recorder-cost measurement from endpoint-polling perturbation and from the
   earlier Cycle 57 red result; none may be called production certification.
2. **Overengineering/hot path:** freeze a minimal controlled A/B before measurement. Do not add a
   permanent runtime mode, generic telemetry bus, per-attempt metric labels, or row-path branch just
   to manufacture a baseline.
3. **Unused code:** close the nonempty paginated HTTP and deterministic coherent-cut test gaps before
   adding another evidence surface.
4. **Production readiness:** preserve the short engineering run as diagnostic evidence only;
   full-checkpoint/restorable-gate authority and the independent immutable release soak remain hard
   gates.
5. **Documentation:** retain raw A/B inputs, exact binary/config identities, run order, and all
   results, including regressions; avoid copying the ledger schema again.
6. **Tests:** require identical frozen workloads, randomized or alternating run order, warm-up,
   repeated samples, confidence intervals/effect sizes, and fail-closed invalid-run rules before
   interpreting any instrumentation overhead.
