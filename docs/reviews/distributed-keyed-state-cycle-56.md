# Distributed keyed state Cycle 56 review

- **Date:** 2026-07-26
- **Scope:** controlled matched-`EndTxn` ambiguity subset of step 5 in the Cycle 50
  delivery-evidence sequence
- **Cycle outcome:** applied and unapplied marker/data outcomes are deliberately actuated and
  reconciled through a fenced successor on the frozen one-node subject
- **Runtime/backend outcome:** unchanged; the actuator and Compose topology are validation-only,
  root-workspace-excluded, and add no connector, state backend, admission, endpoint, or metric
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain unchanged, and runtime
  interval authority, source/state/sink composition, production topology/limits/latency, backend
  qualification, and the independent soak remain absent

## Result and exact boundary

Cycle 56 adds a bounded Rust TCP actuator to `tools/kafka-transaction-probe` and an isolated
loopback-only Redpanda Compose topology. The broker origin is host-published at `127.0.0.1:19194`
but advertises the actuator at `127.0.0.1:19192`; the probe rejects any different or additional
advertised broker endpoint. Coordinator discovery therefore cannot silently bypass the actuator.
The runtime-reported subject is `rdkafka 0.39.0`, `rdkafka-sys 4.10.0+2.12.1`, and librdkafka
`2.12.1` (`0x20c01ff`). It must negotiate non-flexible `EndTxn` v1.

The actuator selects exactly API key 26, version 1, request-header v1, correlation ID, exact client
ID, transactional ID, producer ID, producer epoch, and `committed=true`. Correlation is scoped to
one accepted connection generation. It retains the complete bounded request/response and their
SHA-256 values. Complete non-target frames are forwarded byte-for-byte during the active
lifecycle. Unexpected partial frames outside signalled teardown, retries, identity/version drift,
unknown or duplicate correlations, nonzero broker errors, target downstream bytes, and target
client traffic after evidence finalization all invalidate the run.

Only two actuation classifications pass:

- `FORWARDED_SUCCESS_RESPONSE_LOST`: the full target request is written upstream; the exact
  same-connection/correlation error-zero response is read; zero response bytes go downstream.
- `PRE_FORWARD_REJECTION`: the full target request is retained; zero target bytes go upstream; no
  target response exists.

Both classifications produce the expected local `OperationTimedOut` with `retriable=true`,
`fatal=false`, and `abortable=false`. Actuation and timeout both complete before producer A is
destroyed. Every connection observed for A's exact client ID closes before producer B initializes
the same transactional ID. A timeout or connection close alone is never used as broker-outcome
evidence.

## Four-case broker matrix

Each case uses a fresh three-partition RF=1 topic. Separate directly assigned `read_uncommitted`
and `read_committed` consumers start at the beginning and must reach the same frozen final
high-watermark cut on every partition. Counts below are application records; high watermarks also
include transaction-control records.

| Case | Target disposition | Final high watermarks | RU / RC application counts | Read-committed verdict |
|---|---|---:|---:|---|
| Marker applied | 137 request bytes upstream; exact 14-byte success response withheld | `[9,6,6]` | `[3,2,2]` / `[3,2,2]` | candidate marker selected |
| Marker unapplied | 139-byte request retained; zero upstream bytes | `[9,6,6]` | `[3,2,2]` / `[2,1,1]` | last confirmed marker selected |
| Data applied | 135 request bytes upstream; exact 14-byte success response withheld | `[12,6,6]` | `[4,2,2]` / `[4,2,2]` | predecessor and successor replay visible |
| Data unapplied | 137-byte request retained; zero upstream bytes | `[12,6,6]` | `[4,2,2]` / `[3,2,2]` | predecessor absent; successor replay visible |

The pre-successor frozen high watermarks were `[6,6,6]` in both marker cases and `[6,3,3]` in
both data cases. The marker oracle selects the candidate only when it is read-committed-visible
exactly once on all affected partitions; uniform absence selects the last confirmed predecessor.
Partial, duplicate, conflicting, or incomplete evidence admits no data. The data cases deliberately
commit a successor marker and byte-identical logical replay under the successor interval. This
validates reconciliation behavior; it is explicitly not a Laminar exactly-once result.

## Retained subject and wire identities

- Docker Desktop 4.83.0; Docker client/engine 29.6.2; Compose 5.3.1; WSL2 kernel
  `6.18.33.2-microsoft-standard-WSL2`; Linux/amd64, 24 CPUs, 16,290,336,768 Docker bytes.
- Redpanda `v26.1.13`, Git ref `90d2d87a52e31c0441e93a06986b5eda8afe77f7`, image/repository
  digest `sha256:ae0a858eddd0538dacbba5696f9be1f590de1fe51283964afc85f5510fa8f32e`,
  cluster UUID `5440395a-e97d-4a97-aabf-70478ade9d62`.
- Compose SHA-256
  `ecc3dcd1bcb134650f42d84bb917e68778acbb62b4a924bf82f6c27e069303fd`; one node,
  one core, 1 GiB, PLAINTEXT, loopback host publication, RF=1.
- Final optimized executable SHA-256
  `dac8c8a2e76bbd0543caa9d7115c53abcdee2687377686a2e482c16463ee77ad`; `main.rs`
  `145bf39747d5f1d455f2940ca4afac9354b7b4957e266711e8e119cfe3fb9c15`;
  `endtxn_proxy.rs` `880b02e0090e305ff08cd330f15c5e88791ea71a9e4dfea5de06e264b827731f`;
  unchanged lockfile `5c4522029ef37d9f033227622a20cd5d3e9314d6f856276d1f71586073741a50`.

The final four wire subjects were:

| Case | Topic / transactional ID | PID/epoch | Request bytes / SHA-256 |
|---|---|---:|---|
| Marker applied | `ldb-tx-probe-cycle56-sealed-marker-applied-18c5dfaa20c3be40-73e84` / `ldb.tx.v1.c3dec917599288ddc4b3ee3e99adfae6c438f522aeb103181f4d94fb949d3821` | 21/0 | 137 / `d325ecef383d71b3cad687f85d6b307ed3234a6e1a7c401ae236f3cd5c0f25fd` |
| Marker unapplied | `ldb-tx-probe-cycle56-sealed-marker-unapplied-18c5dfaeb3b52ec4-75610` / `ldb.tx.v1.dd5e80326e70a38aae4376597a92ce3fe0b3be63669c4eba395190213c582f34` | 22/0 | 139 / `99e5cbad11d81df9aaeefa96f3691478669710348a50b2b2fb99b9e25b825d24` |
| Data applied | `ldb-tx-probe-cycle56-sealed-data-applied-18c5dfb042ba1778-754dc` / `ldb.tx.v1.96f3da4b24e5d938ea66b762a7f368de821152d44c306b5e9e4e3523fff854b6` | 23/0 | 135 / `90e8c0d8f2bb0f841d449938c91145c0a3ff2bb7d463ec740c618221e7df351f` |
| Data unapplied | `ldb-tx-probe-cycle56-sealed-data-unapplied-18c5dfb5ab517038-70f70` / `ldb.tx.v1.6b4be2b9f03212405e580b6dd8ddcdb1eb743b771f5b11132d2330f42245cf42` | 24/0 | 137 / `c7fd50408237e5d32c8b39692632a1bc1c98feece380cd250c90726ee3676a06` |

Both applied cases retained the exact raw response
`0000000a00000007000000000000`: frame length 10, correlation ID 7, throttle 0, error 0. Its
SHA-256 is `deb0a2f003df16906f53171c68d337104aa967dbe4ad2decfd06b72b23904101`.
All four requests used correlation ID 7. Their exact target client IDs were
`ldb-kafka-ambiguity-marker-applied-a`, `ldb-kafka-ambiguity-marker-unapplied-a`,
`ldb-kafka-ambiguity-data-applied-a`, and `ldb-kafka-ambiguity-data-unapplied-a`.
Applied event order was arm, complete request buffer, complete upstream write, matching success
response buffer, zero downstream response bytes, all target-client connections closed, then
finalization. Unapplied order replaced the write/response events with exact zero upstream request
bytes before the same close/finalize sequence. A pre-arm target-client connection also closed in
every run and was retained in the transcript. The probe printed the complete raw request/response
hex, byte counts, hashes, connection generations, event timestamps, runtime library versions, and
reconciliation transcript for each run.

## Actuator choice and protocol preflight

The preflight checked the exact pinned protocol implementation before writing a parser.
Librdkafka 2.12.1 advertises `EndTxn` versions 0 through 1, while the tested Redpanda handler
supports 0 through 3, so version 1 is the only valid negotiated subject. The parser follows the
Apache request/response schemas and librdkafka request-header selection. Redpanda's success response
is treated as coordinator acceptance of the commit decision, not instantaneous data-partition
marker visibility; only bounded read-committed reconciliation establishes the semantic outcome.

A dated 2026-07-26 survey of exact [Shotover v0.7.3](https://github.com/shotover/shotover-proxy/releases/tag/v0.7.3),
[Tansu v0.6.0](https://github.com/tansu-io/tansu/tree/079c4c334a0e9240933d208ecd38c1127526e89c),
[Kroxylicious v0.23.0](https://github.com/kroxylicious/kroxylicious/tree/v0.23.0),
[Toxiproxy v2.12.0](https://github.com/Shopify/toxiproxy/blob/3ccd6a79cbc6c6a72b884d295ad314b75cdf3962/README.md#toxics),
and [`kafka-protocol` 0.17.0](https://github.com/tychedelia/kafka-protocol-rs/tree/0.17.0)
found no ready-made matched-`EndTxn` actuator. Shotover was the closest Rust option, but still
required a custom transform and a substantially larger general proxy/routing TCB. Tansu required a
custom layer; Kroxylicious required a custom JVM filter/plugin; Toxiproxy was byte-stream-only; and
`kafka-protocol` was a codec rather than the required actuator. The bounded validation-only parser
is accepted only for the frozen one-broker PLAINTEXT v1 subject. Multi-broker routing, TLS/SASL,
address rewriting, or arbitrary protocol versions trigger a Shotover/framework re-evaluation
instead of growing this parser.

Primary protocol references:

- [librdkafka 2.12.1 EndTxn request versions and body](https://github.com/confluentinc/librdkafka/blob/e1db7eaa517f0a6438bc846a9c49ede73b9ea211/src/rdkafka_request.c#L6454-L6495)
- [librdkafka request header/version handling](https://github.com/confluentinc/librdkafka/blob/e1db7eaa517f0a6438bc846a9c49ede73b9ea211/src/rdkafka_broker.c#L237-L292)
- [librdkafka transactional commit state machine](https://github.com/confluentinc/librdkafka/blob/e1db7eaa517f0a6438bc846a9c49ede73b9ea211/src/rdkafka_txnmgr.c#L2118-L2320)
- [Redpanda v26.1.13 EndTxn handler](https://github.com/redpanda-data/redpanda/blob/90d2d87a52e31c0441e93a06986b5eda8afe77f7/src/v/kafka/server/handlers/end_txn.h#L17)
- [Redpanda commit-decision path](https://github.com/redpanda-data/redpanda/blob/90d2d87a52e31c0441e93a06986b5eda8afe77f7/src/v/cluster/tx_gateway_frontend.cc#L2032-L2078)

## Caught harness defects

The first debug marker-applied run proved the wire actuation and frozen-cut transcript, then failed
shutdown because one pump interpreted its peer pump's deliberate socket close as an unexpected
partial frame. The reader now returns cancellation only after signalled local/global teardown and
still rejects a true partial EOF. A deterministic fake-socket regression covers both paths.

Independent protocol review then found a finish race: a new target-client connection arriving after
evidence finalization could have been forwarded. Connection admission and finalization are now one
atomic state decision. A connection observed before finalization joins the set that must close; an
exact target client observed after finalization is fatal before forwarding. A regression exercises
that boundary. These were caught validation-harness defects, not broker or Laminar product
failures, and the final four-case matrix ran after both fixes.

## Independent review disposition

The protocol reviewer inspected the framing, connection/correlation authority, applied/unapplied
classifications, teardown order, and visibility oracle, found the late-client race, and reran all
four post-fix broker cases independently. The final disposition is **APPROVE** with no protocol
blocker. The quality reviewer independently ran a data-applied case, audited concurrency and lock
ordering, repeated the cancellation regression 50/50 times, and confirmed 16/16 tests plus
warnings-denied Clippy. The final quality disposition is **APPROVE**. A separate claims review
checked the RU/RC wording and production boundary; its precision corrections are incorporated.

## AI slop review

**Pass.** The change solves one exact evidence gap. It does not add a reusable proxy product,
protocol abstraction, async runtime, connector framework, interval allocator, backend adapter,
metrics system, or dormant production hook. Dynamic identities live here; ADR-008 contains only the
normative validation boundary. Timing is never substituted for wire or broker-log evidence.

## Overengineering and hot-path review

**Pass for validation-only scope.** The actuator is large because it implements bounded full-duplex
framing, per-connection correlation, exact byte dispositions, lifecycle retirement, four semantic
oracles, and hostile parsing tests. Generalizing or extracting it further would add surface. It is
excluded from LaminarDB's workspace and hot path. Single-node elapsed time, blocking sockets,
`max.in.flight=1`, and zero linger are test controls, not production latency guidance. No p99/p99.9
or broker-pressure claim is made.

## Unused-code review

**Pass.** No dependency or lockfile changed. All modes, evidence fields, lifecycle states, and
safety-critical parser/error paths are exercised by unit or broker tests. Root metadata still
contains only the six Laminar packages. No runtime feature flag, endpoint, metric, connector hook,
backend dependency, or unarmed fault mode was added.

## Production-readiness review

**NO-GO.** This is one Redpanda RF=1 PLAINTEXT compatibility subject, not Apache Kafka coverage or a
production topology. There is no durable writer-interval allocation/non-reuse, runtime pipeline
incarnation, partition expansion/deletion policy, three-broker durability/failover, coordinator or
disk restart, TLS/SASL, size/timeout/pressure qualification, supported authority API, hot-path or
tail-latency result, qualified TidesDB state, or independent release-binary soak. The current Kafka
connector remains nontransactional and at-least-once. No transaction atomically combines a
Laminar source cursor, managed state/checkpoint decision, and external sink publication.
`[LDB-0013]` continues to reject cluster exactly-once; `[LDB-4007]` continues to reject grouped
aggregates, windows, stateful joins, and materialized views.

## Documentation review

**Pass.** ADR-008 owns the actuator contract; the validation report, parent plan, Phase 0 plan, and
soak charter summarize its evidence boundary and link here. Cycle 55 remains immutable so the
prior open gap is auditable. The research-retention audit found no newly obsolete document. Parked
redb reports remain inbound-linked historical provenance, not active backend recommendations, and
were retained. No source claimed that object storage or a local LSM supplies delivery atomicity.

## Test review

**Pass for the controlled one-broker ambiguity subset.** Final checks cover:

- 16/16 standalone unit tests, standalone formatting, and warnings-denied all-target Clippy;
- exact v1 request/response parsing, bounds, fragmentation, malformed lengths, version/identity/
  correlation drift, nonzero errors, retry/duplicate targets, targeted byte accounting, and true
  partial EOF versus signalled teardown;
- fatal late target-client traffic and complete target-client connection retirement;
- hostile CLI, topic, transactional-ID, NUL, Unicode/no-normalization, and shard bounds;
- four final optimized unique-topic cases plus four independently rerun post-fix cases;
- exact request/response bytes and hashes, broker route assertion, frozen RU/RC cuts, marker
  selection/fallback, applied/unapplied data distinction, and successor replay; and
- unchanged root dependency metadata and lockfile, local Markdown links, and diff hygiene.

These are not runtime connector, durable interval, multi-broker, security, broker-limit, backend,
benchmark, source/state/sink atomicity, or independent-soak tests.

## Cycle 57 review plan

Begin item 6 with the smallest production-consumed evidence slice: a versioned, bounded local-node
authority view and a three-node engineering-harness consumer. It may project only this process's
existing immutable node/boot/process-lease identity, exact locally adopted assignment identity,
local recovery phase, and last locally acknowledged recovery round. It must not reread the shared
`/cluster/vnodes` snapshot and relabel it as local convergence, expose private object-store paths or
owner vectors, invent missing authority, or introduce a generic event/log API. If the exact source
authority is not already retained, stop and amend the design rather than synthesize evidence.

1. **AI slop:** add one fixed/bounded schema and route using existing authority types; reject a
   generic evidence framework, event bus, or duplicate lifecycle state.
2. **Overengineering/hot path:** keep projection work on explicit control-plane reads with zero
   row, state-mutation, or checkpoint hot-path work; do not extend the ambiguity proxy.
3. **Unused code:** require the three-node engineering oracle to consume the view and distinguish
   durable assignment publication from each live process's adoption; no unconsumed endpoint fields.
4. **Production readiness:** authenticate the route, fail closed on startup/recovery/stale lease,
   preserve `[LDB-4007]` and `[LDB-0013]`, and retain **NO-GO** for delivery, backend, limits,
   latency, failover certification, and soak.
5. **Documentation:** freeze field bounds and authority provenance once, including any narrowly
   justified startup-gate observability exception; do not expose private storage layout.
6. **Tests:** cover non-cluster/auth, stale or missing adoption, durable-versus-local divergence,
   same-node restart with new boot/process term, malformed/contradictory evidence, bounded response,
   and a focused three-node kill/rejoin convergence case.

Durable pipeline-incarnation and monotonically burned writer-interval authority follow only with
their first admission-neutral runtime consumer. A disconnected allocator in Cycle 57 would be
unused model code and is deliberately excluded.
