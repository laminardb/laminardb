# Public three-process migration qualification

Attempt 15 passes public adoption, both additive migrations, a complete cold
restart with the original bootstrap, and all independent final stateful, sink
and sequence oracles. The stock server is `54e3a3d2...`; its exact frozen nine-source
identity, unchanged Cargo.lock/profile and Windows PE stack are recorded in
`public-soak-15-binary-identity.json`. The retained harness is `cbdf3983...`, with
unchanged final oracles and production library dependencies at `4cfcb2cd`.

The run passed after 280.45 seconds with idle compilers and zero extra kills.
Target epochs advanced through 18 and 22 before the whole restart. Every cold
replacement used the same node slot and acquired a new process identity.
Recovery consumed a fresh full-roster Release and produced nine expected new
logical pairs; cold restart to fresh output took 48,421 ms. The original Active
receipts remained immutable. Final authority and checkpoint bytes were verified
against their exact SHA-256 and encoded lengths by the read-only collector.

The second cut's consumer delay includes the deliberate two-second checkpoint
hold: nine observations yield nearest-rank p50 361.56 ms and p95/p99 18,444.08 ms.
Sampled combined server RSS peaked at 749,150,208 bytes. These small observations
are not a production latency distribution. Allocations and queue depth were not
profiled. `portable-installation-verification-summary.json` binds 284 focused and
4,471 full tests, three existing ignored tests, Clippy with warnings denied, the
minimal server check and the stock build. The 42-test fault index records the
deterministic library boundaries separately from native process failures.

Attempt 16 failed before topology submission or injected kills: creation of its
new input topic exceeded the retained Kafka fixture's 1,000-partition limit.
The raw broker rejection and verified adjustment to 2,000 are recorded in
`attempt-16-fixture-capacity.json`. The task-owned fixture retains its 4 GiB,
one-shard configuration, all historical topics and its original volume. The
failed result is retained; a retry uses the same server and harness.

Attempt 17 passed both public migrations and the first leader replacement,
resuming target checkpoint 36 after 43.86 seconds. The second injected follower
failure triggered automatic assignment recovery to two survivors while the
replacement boot was arriving. Final authority sequence 334 retains that
assignment-3 handoff pin and exact checkpoint-38 reference. The topology recovery
owner-completeness check correctly refused the reduced map; the existing
90-second Release ceiling expired and the test failed after 261.50 seconds.
The third kill, final cold restart and final stateful/sink/sequence oracles were
not reached. This identifies assignment admission as the next repair, rather
than granting survivor rescaling through a recovery identity bypass.

Earlier attempts below describe the independently observed defects and their
repair history; their failures remain part of the evidence.

Attempt 09 completed public adoption and both additive migrations on the existing
Kafka/S3 stateful soak. It failed after killing the leader, before replacement or
full restart. This is partial evidence, not a passing end-to-end qualification.

The server uses production commit `4cfcb2cd60e864c96d714db822751d87092a3e81`,
unchanged stock optimized `soak` profile and Windows PE stack settings. The harness
adds test-only consumer latency and observed-phase measurements. Exact source,
Cargo.lock and executable identities are in `public-soak-09-binary-identity.json`.

```powershell
$env:CARGO_BUILD_JOBS = '1'
cargo test --locked --profile soak -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --test cluster_soak --no-run
```

The build passed. The run used three native server processes, owned MinIO and
Redpanda fixtures, 12 original Kafka partitions, 400 offered records/second,
4,096 keys, Zipf 1.2, 500 ms checkpoints and at-least-once delivery. Both existing
checkpoint and latency SLO modes were `observe`; no production SLO is inferred.

The exact existing sealed inventory was adopted without changing its bytes or
deployment identity. An independent latest-source pipeline committed topology 2
and released all three installed processes. Ordinary SQL then attached a
stateless stream to an existing aggregate, committed topology 3, and released
the complete roster. Target checkpoints advanced from epoch 23 to 26. The six
expected new-pipeline logical pairs appeared; historical records were excluded.

The independent activation took 26,473 ms. A proven checkpoint gate held the
second cut for 2,023 ms. The following three records became consumer-visible
after 15,014 ms, including that hold and Release. Six deterministic observations
are retained; their nearest-rank p50/p95/p99 are 111.236/15,014.340/15,014.340 ms.
This small oracle is not a steady-state or production latency distribution.
Observed durable status transitions include polling and I/O delay; missing
transitions have no inferred duration.

The leader was killed in checkpoint 28's final sink fence. The original soak
then requested progress on two survivors before restarting the victim. The
committed topology requires the complete unchanged owner map, and recovery
rejected that reduced assignment. The test timed out at the existing 90-second
recovery ceiling and exited 101 after 205.96 seconds. Full restart and the final
independent stateful/sink/sequence oracles were not reached. The failed run did
not roll back either committed topology or manufacture a smaller Release.

The subsequent harness correction replaces the killed process before requiring
progress, asserts a new boot/process term and the unchanged complete owner map,
and checks a fresh durable recovery Release. An already Active operation retains
its original immutable activation evidence. Ordinary survivor-rescaling tests
remain separate. Its standard optimized build completed in 32m 45s; its hash and
source binding are in `public-soak-10-binary-identity.json`.

Attempt 10 stopped before server startup because the 1 GiB broker rejected new
partitions at its memory limit (532 requested total against a 524-replica limit).
Only the owned broker was restarted with 4 GiB; its exact data volume and all 112
topic definitions were verified unchanged. No topic, authority or checkpoint
namespace was reset. Subsequent comparison runs use the same fixture allocation.

Attempt 11 used the corrected harness with zero injected kills to isolate full
cold restart. Both public migrations passed, reaching target epochs 52 and 55;
independent activation took 16,880 ms and pause-inclusive observation took
16,456 ms with a 2,023 ms explicit hold. These are correctness observations under
concurrent compilation, not a compiler-idle performance comparison.

All three replacement processes replayed the original configuration. The cold
recovery driver then repeatedly rejected source-drain settlement: the new DB was
Created with a live runtime token, and recovery stop returned early without
observed teardown. The existing 90-second Release deadline expired; the test
failed after 181.47 seconds. Original target Commit/activation evidence remains
immutable at authority sequence 437; no recovery Release was manufactured. Final
stateful/sink/sequence oracles were not reached. A narrow recovery-owned Created
teardown fix and an owned-task/drain regression are being verified; no passing
cold-restart qualification is claimed yet.

The narrow Created recovery fix uses the existing cancellation and observed
teardown owner. Its regression installs a task that waits for runtime cancellation,
then verifies the task was observed before the exact stopped-source settlement.
Two additional red-to-green authority tests reproduce cleanup before a Planned
cut exists and admission during artifact preflight. The fixes retain all existing
root/state/replay pins. The focused `--lib topology` run passed 298 tests with two
ignored (connectors 12, Core 144, DB 142). This command excludes server binary
tests. All-target Clippy passed with warnings denied. The subsequent full
`--lib --bins` run passed 4,463 tests (connectors 921, Core 1,106, DB 2,077,
server 359) with three ignored. Native restart qualification for the repaired
executable remains pending.

Attempt 12 used that repaired stock server (`cfb9b732…`) and the same corrected
stock harness (`cbdf3983…`) with zero injected kills and idle compilers. Both
public migrations completed, reaching exact target epochs 27 and 33. All three
cold replacement processes used the original bootstrap and acquired new boot
identities/process terms. Observed Created teardown and source-drain settlement
succeeded. Recovery then blocked Start because the assignment-handoff pin was
still present, although that pin cannot retire until the recovered assignment
produces its first target checkpoint. The test failed after 204.60 seconds at the
unchanged 90-second Release ceiling; final stateful/sink/sequence oracles were
not reached. This is a second independently observed recovery defect, not a
passing full-restart qualification.

Final authority sequence 323 retained the exact checkpoint-33 reference and full
three-process assignment-2 handoff pin, with no active checkpoint artifacts,
cleanup cursor or drain reservation. The two original Active operation receipts
remained immutable; no fresh recovery Release was published. The checked-in
attempt-12 authority summary and binary identity bind those observations.
Activation was observed at 22,584 ms, the deliberate checkpoint hold was 2,022 ms,
and the three paused input pairs were first consumed after 16,599 ms. The six
consumer samples yield nearest-rank p50 608.90 ms and p95/p99 16,599.49 ms. These
small correctness-oracle samples do not establish a production latency budget.
The sampled combined server RSS maximum was 563,736,576 bytes across both process
generations; allocation events were not profiled.

The subsequent handoff fix accepts a pin only after auditing the exact current
assignment and complete selected checkpoint reference. It preserves the pin
through installation/Release and preserves the original Active receipt. A
regression rejects altered assignment and checkpoint pins, exercises the real
process takeover and assignment-recovery decision, then requires the next exact
target checkpoint to retire the pin. The original guard failed that regression
after 30.21 seconds. The subsequent full suite passed 4,464 tests, and the
retained-state extension passed 4,467 tests with three ignored. All-target Clippy
and the minimal server check passed; the final focused retention run also passed
the later empty-object and final-range cases. Qualification of the newly built
native executable is still pending. No blanket handoff, fingerprint or ownership
bypass was added.

Attempt 13 used the frozen 14-source repair and retained stock executable
`f9a05d5f…`, built in 23m 53s. With zero injected kills and idle compilers, both
public migrations reached exact target checkpoints 36 and 42 and all six new
pipeline pairs matched. All three cold replacements acquired fresh process
identities. Recovery passed the exact handoff-pin Start guard, then failed while
decoding the interval join's archived assignment-1 state against live assignment
2. Intake stayed held, the original Active receipts stayed immutable, and no
replacement Release was published. The existing 90-second deadline expired;
the test failed after 203.32s and did not reach the final stateful/sink/sequence
oracles. The final authority summary binds sequence 341 and exact checkpoint 42.
This identifies the next repair; it does not qualify full cold restart.

Attempt 13's sampled combined server working-set maximum was 594,001,920 bytes
across 103 samples and six processes (the initial and cold replacement rosters).
The original processes' observed peak working sets were 192,360,448, 168,738,816
and 235,720,704 bytes; replacement peaks were 75,440,128, 76,386,304 and
78,524,416 bytes.
The resource JSON retains sampling intervals, private bytes and available
input-buffer/managed-state gauges. Allocation events were not profiled and are
reported as unavailable. Raw node/build logs remain under ignored
`target/topology-evidence` and `target/tmp/soak-576636-1791078946966558900`.

Attempt 14 used stock server `60169d45…`, built in 22m 18s from `582b7cf0`
plus the frozen three-source portable-bootstrap repair. The focused run passed
283 tests (two ignored), the full selected suite passed 4,470 (three ignored),
all-target Clippy denied warnings and passed, and the minimal server check passed.
The verification summary binds those logs and exact sources. The checkpoint
decoder now retains the historical assignment and uses ordinary portable
bootstrap for the certified newer assignment with the same complete owner map.

With idle compilers and zero injected kills, both public migrations reached
exact target checkpoints 35 and 42 and all six new-pipeline pairs matched.
All three cold replacements used the original bootstrap. Private managed-state
restoration passed, including the interval join that failed in attempt 13.
Held runtime startup then rejected `recovered.reassigned` in the target
checkpoint coordinator. The existing 90-second Release deadline expired; the
test failed after 197.93 seconds and reached none of its final stateful, sink
or sequence oracles. This is another separately observed recovery defect.
Intake remained held, with no replacement Release or rewritten Active receipt.
The result, exact final authority/checkpoint references and artifact endpoint
are recorded in the attempt-14 summaries. Sampled combined server RSS peaked
at 571,621,376 bytes. No successful cold-restart qualification is claimed.
