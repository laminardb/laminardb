# Distributed keyed state Cycle 55 review

- **Date:** 2026-07-26
- **Scope:** deterministic real-broker subset of step 5 in the Cycle 50 delivery-evidence sequence
- **Cycle outcome:** stable Kafka transactional ID v1, confirmed cross-partition marker/data
  visibility, confirmed-abort retry, provider fencing, and exact broker capture are executable
- **Runtime/backend outcome:** unchanged; the probe is root-workspace-excluded and no connector,
  cluster admission, state backend, endpoint, metric, or delivery guarantee changed
- **Production verdict:** **NO-GO**; ambiguous commit reconciliation, runtime interval/source/state/
  sink composition, replicated durability, production limits/latency, and the independent soak
  remain absent

## Result and exact boundary

Cycle 55 adds `tools/kafka-transaction-probe`, a standalone synchronous validation executable with
its own lockfile. It uses the repository's exact `rdkafka 0.39.0` line and does not depend on any
LaminarDB runtime crate. The CLI is manual and fail-closed, creates one unique delete-retained RF=1
topic with three partitions, disables producer/consumer auto-creation, prints `NOT CERTIFICATION
EVIDENCE`, and refuses to represent its elapsed runtime as latency evidence.

The ADR now freezes one broker fencing identity per stable `(deployment UUID, pipeline
incarnation, sink ID, shard ID)`:

```text
"ldb.tx.v1." || lowerhex(SHA256(
    "laminardb/kafka/transactional-id/v1\0"
    || deployment[16] || incarnation[16]
    || u8(sink_len) || sink_utf8
    || u8(shard_len) || shard_utf8
))
```

It is exactly 74 ASCII bytes. The frozen sample is
`ldb.tx.v1.c49ace6d02eb21ec7a2dc4424d8c3b9680fc3cd828cd754fec079b800a37411a`.
Assignment, process, checkpoint, and writer interval do not enter the hash because a replacement
must reproduce the ID to fence its predecessor. Operator/output provenance remains in the marker.
The future runtime must make shard IDs unique among concurrent writers in one stable scope.

The real-broker scenario performs these ordered transactions:

1. producer A initializes the stable ID and commits the frozen 325-byte first marker to all three
   inventoried partitions in one transaction;
2. A commits one `stable-key`/`stable-payload` data record with operation `[0xa1;32]`, first interval
   `[0x11;16]`, and sequence zero;
3. A sends and provider-confirms one three-partition range with global sequences 1, 2, and 3, then
   confirms abort; it immediately sends byte-identical records and commits the retry;
4. A begins another range with global sequences 4, 5, and 6, sends to every partition, and receives
   successful delivery reports without ending the transaction;
5. producer B initializes the same transactional ID; A's later commit returns fatal `Fenced`, and
   A's client fatal state independently reports `Fenced`;
6. B commits the frozen successor marker to all partitions and replays the stable data with the
   same operation/key/payload, interval `[0x12;16]`, and sequence reset to zero; and
7. separately assigned `read_uncommitted` and `read_committed` consumers drain all partitions from
   the beginning through partition EOF, then metadata is rechecked.

The uncommitted capture contains application-record counts `[7,5,5]`; the committed capture
contains `[5,3,3]`. The difference is exactly the confirmed-aborted attempt and the predecessor's
fence-aborted open transaction. The byte-identical confirmed retry remains visible. Both markers
appear exactly once on every partition in both captures. Each has exactly one case-sensitive,
non-null `__ldb` header with the frozen bytes, no key, and a present empty payload. Every data
record has exactly one 66-byte `__ldb` value, preserves an unrelated `trace-id` header, and retains
the fixture key/payload. Metadata before and after is exactly `[0,1,2]`, one non-negative leader,
one replica, and one matching ISR; the configured and observed leader was node 0. This proves a
complete inventory only for that synthetic topic.

## Subject and retained identities

- Docker Desktop 4.83.0; client/engine 29.6.2; Compose 5.3.1; WSL2 kernel
  `6.18.33.2-microsoft-standard-WSL2`; Linux/amd64, 24 CPUs, 16,290,336,768 Docker bytes.
- Existing [`tests/docker/compose.yml`](../../tests/docker/compose.yml), SHA-256
  `ba366bc4f3c8d9f79e4a55136b589b5639af3ccbf907e9270800af7834ef9ac6`.
- Redpanda `v26.1.13`, Git ref `90d2d87a52e31c0441e93a06986b5eda8afe77f7`, image/repository
  digest `sha256:ae0a858eddd0538dacbba5696f9be1f590de1fe51283964afc85f5510fa8f32e`.
  Effective service: one node, one core, 1 GiB, plaintext broker listener inside the container,
  loopback-only host publication and advertisement at `127.0.0.1:19092`, RF=1.
- `rdkafka 0.39.0`, `rdkafka-sys 4.10.0+2.12.1`, dynamically reported librdkafka `2.12.1`
  (`0x20c01ff`); Rust/Cargo 1.96.0 on `x86_64-pc-windows-msvc`.
- Final loopback-bound optimized probe topic
  `ldb-tx-probe-cycle55-loopback-final-18c5dc266cd6872c-6db98`; post-run broker high
  watermarks `[21,15,15]`. These include transaction control records and are not substituted for
  application counts or a read-committed last-stable-offset proof.
- Optimized executable SHA-256
  `ecf46dd042a33cb8743cc254ed76e12361c477c11ec9601efb7611125b37b0a5`; lockfile SHA-256
  `5c4522029ef37d9f033227622a20cd5d3e9314d6f856276d1f71586073741a50`; source SHA-256
  `7daa92bd152aa60acab30d32169dfe700177717b2c7c40e128dad52a555a0b55`.

The exact image is a useful Redpanda compatibility subject, not an Apache Kafka result. The broker
has no TLS/auth, replicated transaction coordinator, min-ISR/election case, restart, disk-loss, or
remote-recovery fault in this cycle.

## Deliberate scope reduction from the Cycle 54 plan

Cycle 54 asked Cycle 55 to bracket ambiguous marker/data commits. The pre-implementation threat
review rejected the available actuators. A broker kill, tiny timeout, socket disconnect, or generic
Toxiproxy rule cannot prove both that a particular `EndTxn` request reached the broker and that only
its matching response was lost. Inferring broker application from such a timeout would be a false
positive. Cycle 55 therefore proves only deterministic fencing/isolation and leaves ambiguity open.

A later valid ambiguity test needs protocol-aware evidence: identify one `EndTxn` request and
correlation ID, forward it, drop that response, retire A, initialize B with the same stable ID, and
drain the complete read-committed topic. The successor selects the ambiguous interval only if the
exact marker is visible on every partition, selects the last visible predecessor only if absent
everywhere, and opens no data on partial/conflicting/incomplete evidence. Both applied and unapplied
branches must be deliberately actuated; otherwise the attempt is invalid.

## Independent review disposition

Three independent cycle-end reviews examined protocol claims, scope/isolation, and production,
test, and documentation claims. The protocol reviewer also reran the real-broker probe. The final
evidence set therefore has four successful unique-topic runs: debug, optimized, reviewer, and a
final optimized run after changing the Compose host publication from all interfaces to loopback.
The latter retained exact RU `[7,5,5]`, RC `[5,3,3]`, fatal `Fenced`/`Fenced`, and high watermarks
`[21,15,15]`.

Review findings were resolved by distinguishing separate same-executable RU/RC captures from
independent evidence, describing the data encoder as checked against a copied frozen golden,
separating the configured/observed node-0 leader from the probe's non-negative-leader assertion,
and removing the stale promise that Cycle 55 would resolve ambiguity. An inbound-link audit also
confirmed that the two parked redb execution reports remain referenced by the redb prescreen
contract as historical audit provenance. They were therefore retained rather than misclassified
as obsolete research.

## AI slop review

**Pass.** The change adds one executable, one ADR contract, and evidence updates. It does not create
a generic broker abstraction, connector framework, async runtime, retry library, interval allocator,
metrics subsystem, fault-proxy framework, or state-backend adapter. Dynamic facts live in this
review rather than being duplicated throughout normative documents. Ambiguity is left unresolved
instead of filled with timing assumptions.

## Overengineering and hot-path review

**Pass for a validation probe.** The executable uses synchronous `BaseProducer`/`BaseConsumer`, one
bounded three-record range, finite queue/byte/time settings, explicit pre-commit flushes, and no
per-row future. Inline 66-byte data headers and one shared producer transaction preserve the design
direction, but this code is not on LaminarDB's hot path. `max.in.flight=1`, zero linger, single-node
elapsed time, allocator behavior, and debug/release run duration are test settings—not production
tuning or latency evidence. Broker/request framing and maximum limits remain unqualified.

## Unused-code review

**Pass.** The crate is excluded from the root workspace, has three direct dependencies, and every
CLI option, vector, identity field, transaction helper, delivery callback counter, capture field,
and visibility branch is exercised by unit or real-broker runs. No runtime feature flag, connector
hook, endpoint, metric, backend dependency, or dormant ambiguity hook was added.

## Production-readiness review

**NO-GO.** The probe establishes atomicity only for records in one Kafka transaction. It does not
atomically include a Laminar source cursor, managed state/checkpoint decision, or object-store cut,
and it does not call `send_offsets_to_transaction` as a Laminar commit. The current connector has no
transactional ID and remains at-least-once behind `[LDB-0013]`. Runtime pipeline-incarnation and
durable interval-ID allocation/non-reuse are absent; `A -> B -> A` is still possible in a constructed
model. Ambiguous commits, generic topology races, partition expansion/deletion prohibition, broker
limits/pressure, three-broker durability/failover, security, hot-path p99/p99.9 latency, qualified
TidesDB working state, end-to-end faults, and an independent release-binary soak remain blockers.
`[LDB-4007]` is unchanged for grouped aggregates, windows, and stateful joins.

## Documentation review

**Pass.** ADR-008 is the sole normative transactional-ID definition. The implementation plan,
phase plan, validation report, and soak charter summarize only the evidence boundary and link the
detail here. Cycle 54 remains immutable so its requested matrix and this cycle's justified scope
reduction are both auditable. Primary behavior is checked against the `rdkafka 0.39.0` producer API,
[Apache Kafka transaction design](https://kafka.apache.org/43/design/design/),
[Apache Kafka consumer isolation](https://kafka.apache.org/43/configuration/consumer-configs/#isolation.level),
[librdkafka 2.12.1 transactional fencing](https://github.com/confluentinc/librdkafka/blob/v2.12.1/INTRODUCTION.md#transactional-producer),
and [Redpanda transactions](https://docs.redpanda.com/streaming/current/develop/transactions/).
The research-retention audit found no obsolete document to remove. In particular, the two parked
redb execution reports remain inbound-linked from the redb prescreen contract as audit provenance;
they are not active backend recommendations.

## Test review

**Pass for the deterministic protocol subset.** Final checks cover:

- 6/6 standalone unit tests and warnings-denied all-target Clippy;
- exact transactional-ID golden, length, input bounds, stability, and four scope axes;
- probe-local encoder checked against the copied frozen data-header golden and exact linked marker
  literals;
- hostile manual CLI/topic/identity inputs and visibility transcript structure;
- four unique-topic real-broker passes: debug, optimized, independent reviewer, and final optimized
  after loopback-only host publication;
- provider-confirmed delivery before commit/abort/fence, same-ID fatal fencing from two error
  surfaces, complete pre/post test-topic inventory, confirmed abort and identical retry;
- separate RU/RC direct-assignment captures through every partition EOF, exact per-partition
  order/counts, marker fanout/cardinality/null-versus-empty semantics, and data header/key/payload/
  unrelated-header preservation; and
- standalone/root formatting, dependency isolation, Markdown links, and `git diff --check`.

These tests are not ambiguous-fault, broker-limit, multi-broker, runtime connector, backend,
benchmark, or independent-soak evidence.

## Cycle 56 review plan

Timebox the ambiguous-outcome preflight and implementation separately. Do not begin the three-node
engineering harness until the ambiguity actuator has a reviewed proof boundary.

1. **AI slop:** first audit Redpanda/Kafka/librdkafka supported test hooks and existing Rust protocol
   proxies; implement only a minimal `EndTxn` correlation-aware actuator if none is sufficient.
2. **Overengineering/hot path:** keep the actuator outside runtime and parse only the Kafka request/
   response framing needed to evidence one matched fault; do not build a general proxy product.
3. **Unused code:** require every mode to actuate either proved pre-apply rejection or forwarded-
   request/lost-response, and remove any hook that cannot produce externally checkable evidence.
4. **Production readiness:** reconcile first/successor markers and data after producer retirement;
   partial fanout or incomplete capture stays closed. Retain **NO-GO** for limits, runtime lifecycle,
   replicated durability, latency, backend, and independent soak.
5. **Documentation:** record exact request API key/version/correlation, proxy/broker identities, and
   packet/event evidence once; do not infer application from a timeout.
6. **Tests:** deliberately observe both marker applied/unapplied outcomes and ambiguous data replay,
   then repeat cleanly. If the actuator cannot distinguish them within the timebox, record `BLOCK`
   for this slice rather than weakening the requirement.
