# Distributed keyed state Cycle 50 review

- **Date:** 2026-07-26
- **Scope:** current Kafka output provenance, independently observable checkpoint/assignment facts,
  and the smallest ordered delivery-evidence implementation sequence
- **Cycle outcome:** the existing Kafka append and public telemetry paths are insufficient for the
  frozen independent stale-writer/replay oracle; the exact missing contract and ownership split are
  now recorded
- **Admission/backend outcome:** unchanged; no TidesDB dependency, runtime backend, cluster
  capability, output envelope, endpoint, or metric was added
- **Production verdict:** **NO-GO** pending managed vnode ownership, qualified working state,
  externally fenced/provenanced delivery, bounded public evidence, fault/latency gates, and an
  independently operated release-candidate soak

## Result and exact boundary

Cycle 50 follows the first Kafka-to-Kafka candidate from graph output to broker evidence. Graph
execution returns a stream-name-to-batches map, and publication retains internal sink selection and
output-contract checks. The external identity is lost at `SinkCommand::WriteBatch`, which carries
only a `RecordBatch` under a deadline; the single bounded actor provides process-local FIFO. The
Kafka connector emits one payload per row, with an optional configured key and optional broker
partition, and awaits every accepted delivery result. Main records carry no Laminar headers.

The producer correctly enforces `enable.idempotence=true`, `acks=all`, and at most five in-flight
requests. It deliberately has no `transactional.id`, and pass-through configuration cannot inject
one. Ambiguous writes retire the connector generation. Those are valuable durable at-least-once
properties, but a producer session is not a Laminar writer-authority interval and cannot fence its
successor.

The current external record therefore lacks all of the facts needed to distinguish legal recovery
replay from stale-owner output: a replay-stable logical operation ID, writer interval, checked sink
admission sequence, stable Laminar sink shard, assignment/process provenance, and committed
predecessor/successor marker. Broker offset orders actual records within one Kafka partition; it
does not state which Laminar writer was authorized when work entered the sink.

This is a delivery and certification gap, not a state-backend gap. It does not alter the existing
at-least-once claim, and it does not authorize exactly-once.

## Public evidence inventory

The durable internals are stronger than the supported projections:

- `/api/v1/cluster/vnodes` reads the shared `AssignmentSnapshotStore`. Its version, vnode map, and
  participant boot-incarnation roster prove publication, but polling every node proves only that
  each HTTP process can read the same durable head—not that its local `VnodeRegistry` adopted it.
- `/api/v1/cluster/checkpoints`/`SHOW CHECKPOINT STATUS` returns only the latest checkpoint ID,
  epoch, timestamp, source/sink names, and total count. It does not expose terminal outcome,
  assignment fence, exact source offsets, or attempt history.
- Prometheus checkpoint histograms expose buckets, count, and sum. They support approximate
  quantiles but not the charter's exact maximum, per-attempt correlation, or deadline-exhaustion
  counts; values reset with each process.
- Reservation, completion, recovery, and some assignment facts are structured `tracing` fields in
  code, but the server emits human-formatted text with no versioned event schema. The current soak's
  substring parser is engineering scaffolding, not a production evidence contract.
- Private object-store outcomes, recovery capsules, process leases, adopted-assignment reports, and
  recovery records contain much of the exact truth. Their paths, envelopes, retention, and pruning
  are implementation details and must not become an accidental external API.

The minimum supported additions are a bounded/versioned local-node evidence view; a bounded/
versioned projection of immutable checkpoint outcomes and capsules; and exact per-process-
generation maximum/deadline evidence for the five frozen checkpoint latency families. Delivery
also needs the already-designed data provenance and broker-enforced writer marker. That marker must
bind its interval to the exact recovery-base `{epoch, checkpoint_id}`, capsule digest, and
assignment certificate/digest, which the oracle resolves through the immutable view before deciding
whether replay is legal. Missing evidence classifies an attempt as `INVALID`; complete evidence
showing stale or conflicting output is a product `FAIL`.

There is no null recovery base for the first writer. Source partitions and exact numeric exclusive
start baselines are resolved without delivery while readiness, graph execution, and sink writes stay
closed. A zero-input bootstrap checkpoint/capsule seals those cuts, empty state/timers, pipeline
identity, and assignment. Only that proved-empty checkpoint may flush an unactivated sink. The first
marker uses `predecessor = none` and references the bootstrap capsule; data admission opens only
after its confirmed transaction. Failure before Commit retries startup; failure or ambiguity after
Commit creates/fences a new interval against the same cut. Startup/marker latency is a measured gate.

## Frozen implementation order

1. Extend only the independent, execution-ineligible semantic fixture with writer intervals,
   checked sequences, assignment/shard ownership, successor markers, and marker-to-recovery-base
   binding over its existing frozen/durable source-cut model, including a zero-input bootstrap base
   and a `predecessor = none` first interval.
2. Freeze compact data-header and marker bytes, version/size caps, and hostile decoding.
3. Add pure tests for stable operation identity and exact authority propagation.
4. Add a fake transactional-producer state machine proving marker-before-data, terminal ambiguous
   commits, contiguous batch sequence reservation, overflow rejection, and stable producer ID.
5. Add real Kafka/Redpanda tests for producer fencing, `read_committed` visibility, marker atomicity,
   and invisible aborted predecessor data.
6. Upgrade the three-node engineering harness only after those layers pass. An independently
   operated release-binary soak remains a later, separate gate.

The hot path is constrained up front: common interval provenance is resolved from one marker per
affected partition; each data record carries only a compact version/kind, operation ID, interval
ID, and sequence; and a batch reserves one checked contiguous sequence range rather than performing
per-row locking, allocation, or coordination. The independent reader hashes the already-serialized
payload bytes; Laminar does not transmit a duplicate payload digest. The oracle derives the expected
vnode from the canonical key and frozen ABI, then verifies it against the marker's certified shard/
vnode set rather than trusting a per-row vnode field.

## AI slop review

**Pass.** The audit traces the existing graph, actor, connector, HTTP, metrics, logs, and durable
records before naming additions. It does not invent a second checkpoint authority, sink framework,
generic event bus, or state backend. The implementation order reuses the existing independent
semantic fixture and exact outcome/capsule authority.

## Overengineering and hot-path review

**Pass.** The proposed minimum keeps common provenance at writer-interval scope, reserves sequence
ranges per batch, and uses Kafka's existing producer fencing/partition order. Public evidence is a
bounded read-only projection rather than high-cardinality metrics or direct access to private object
paths. No row-path code changed in this cycle.

## Unused-code review

**Pass.** This cycle adds documentation only. Every proposed future field has a named independent
oracle consumer and a deterministic pass/fail case. No runtime placeholder, feature flag, backend
adapter, unused metric, or speculative connector type was added.

## Production-readiness review

**NO-GO.** Current Kafka ALO durability and actor FIFO are real but do not provide external
predecessor/successor exclusion. Current endpoints and histograms cannot satisfy the frozen
assignment-convergence and exact-latency evidence. No production scenario, delivery guarantee,
backend, or SQL admission changed, and no independent soak ran.

## Documentation review

**Pass.** The validation report, ADR-008, phased plan, Phase 0 execution order, and soak charter now
agree on the current record shape, public/private evidence boundary, smallest missing contract,
implementation order, and unchanged NO-GO. Existing backend research remains correctly labelled as
historical, parked, rejected, selected-but-stopped, or validation-only; no document became obsolete
or misleading enough to remove in this cycle.

## Test review

**Pass, with two recorded command/build corrections.** Final validation is:

- `cargo test -p laminar-connectors --lib --features kafka
  kafka::sink_config::tests::test_rdkafka_config_at_least_once -- --exact`: one passed; confirms
  idempotence, `acks=all`, max in-flight five, and no `transactional.id`;
- `cargo test -p laminar-connectors --lib --features kafka
  kafka::sink::tests::contract_is_multi_writer_durable_at_least_once -- --exact`: one passed;
- an initial server invocation with `--lib` was rejected before testing because `laminar-server` has
  no library target. The corrected first all-feature invocation exceeded its 240-second Windows
  compile window while its child compiler continued; after artifact completion, the exact rerun of
  `http::tests::test_cluster_vnodes_fails_when_durable_snapshot_is_missing` passed one test, and
  `http::tests::test_cluster_checkpoints_returns_metadata` passed one test;
- `cargo clippy -p laminar-connectors --lib --features kafka -- -D warnings`: pass. Its first
  180-second Windows native build window also expired while child compilation continued; the exact
  completed-artifact rerun passed;
- `cargo fmt --all -- --check`, `git diff --check`, all local links across six changed documents,
  and backend/dependency hygiene: pass; and
- independent sink-path, soak-oracle, and public-evidence audits supplied the factual inventory.
  A correctness review initially rejected the source-cut wording, graph-boundary wording, and
  unbound recovery marker; all three were corrected and its re-review approved. Scope review
  approved. A follow-up correctness edge audit then found the missing initial-interval authority;
  the zero-input bootstrap ordering and failure rules were added. Documentation review's field-
  placement, replay-sufficiency, and test-record objections were corrected before final approval.

No Rust source changed, so the full LaminarDB suites that passed in Cycle 49 were not rerun. No
backend candidate or soak ran. These focused tests validate the audited interfaces, not distributed
keyed-state production readiness.

## Cycle 51 review plan

Implement only step 1 of the frozen sequence in `tools/independent-soak-contract`; it remains
synthetic and `certification_eligible=false`:

1. **AI slop:** extend the existing schema/oracle/fixture rather than adding another tool or model;
2. **Overengineering/hot path:** model only facts consumed by the independent verdict, with no
   Kafka client, runtime envelope, backend, endpoint, or production metric;
3. **Unused code:** require at least one deterministic passing or failing fixture for every new
   field and rule;
4. **Production readiness:** distinguish incomplete evidence (`INVALID`) from complete evidence
   proving illegal replay/stale output (`FAIL`), while retaining the overall NO-GO;
5. **Documentation:** keep the fixture schema, charter, ADR, validation report, and execution plan
   synchronized without repeating the full design; and
6. **Tests:** cover legal identical replay at/after a sealed cut, replay before the cut, conflicting
   bytes, predecessor records on both sides of the marker, missing/unknown markers, sequence reuse
   or regression, wrong owner/shard, missing source/checkpoint evidence, valid bootstrap/first-marker
   ordering, data before bootstrap/marker, and a non-empty unactivated-sink flush. Run no backend
   candidate, relax no admission guard, and make no production-ready claim.
