# NATS connector — production readiness

Five small, independently-mergeable PRs that take `feat/nats-connector-source-sink`
from MVP to a v1 an operator would deploy. The branch today is a working
reference implementation with zero auth, zero metrics, unbounded retry,
no chaos coverage, and no throughput numbers.

## Guiding principles

- Match existing patterns. Metrics mirror `KafkaSourceMetrics` /
  `KafkaSinkMetrics`. Auth follows the enum + validation shape used by
  the Kafka connector's `SecurityProtocol` / `SaslMechanism`.
- Block-merge only what's in the "required for v1" list below. Anything
  else goes on the deferred list and stays there.
- Every PR ships with tests. Unit for parsing. Integration where
  behavior involves a real server.

## PR A — Auth + TLS

### Scope
- Config: `auth.mode = none | user_pass | token | tls` (in v1).
  Additional keys: `user`, `password`, `token`, `tls.ca.location`,
  `tls.cert.location`, `tls.key.location`, `tls.insecure_skip_verify`.
- Wire into `async_nats::ConnectOptions`: `.user_and_password()`,
  `.token()`, `.add_root_certificates()`, `.add_client_certificate()`,
  `.require_tls()`.
- Validation: `auth.mode=user_pass` requires both `user` and
  `password`; mutually-exclusive keys caught at parse time. New LDB
  codes `LDB-5060`–`LDB-5065`.

### Deferred (not v1)
- `nkey`, `jwt`, `creds_file` — real production features, niche.
- OAuth / JWT callback signers.

### Tests
- Unit: every mutual-exclusion rule, every required-when rule.
- Integration: extend `docker-compose.nats.yml` with a secured profile
  (`--profile secure`) that starts `nats-server --user alice --pass bob`
  + TLS certs; round-trip under each mode.

### Merge criterion
All new unit tests + at least `user_pass` and `tls` integration tests
green. ~400 LOC.

## PR B — Metrics + HealthStatus

### Scope
- `NatsSourceMetrics` and `NatsSinkMetrics` modeled on Kafka metrics.
- Source counters: `nats_source_records_total`,
  `nats_source_fetch_errors_total`, `nats_source_acks_total`,
  `nats_source_ack_errors_total`. Gauges: `nats_source_pending_acks`,
  `nats_source_channel_depth`.
- Sink counters: `nats_sink_records_total`,
  `nats_sink_publish_errors_total`, `nats_sink_ack_errors_total`,
  `nats_sink_dedup_total` (from `PubAck.duplicate`). Gauges:
  `nats_sink_pending_futures`.
- `HealthStatus` impls: `Healthy` while data flowing + pending within
  budget; `Degraded` on sustained back-pressure (>50% pending
  saturation over a rolling window); `Unhealthy` after N consecutive
  fetch errors (see PR C).
- Thread `Option<&prometheus::Registry>` through `NatsSource::new` and
  `NatsSink::new`. Update factory in `mod.rs`.

### Open questions
- Label cardinality. `records_total{subject}` is a NATS footgun given
  wildcard subject spaces. Default to **no per-subject label**; add a
  `metrics.per.subject.label=false` switch for opt-in.
- `Degraded` threshold (50%, rolling window size) — expose as config
  or hardcode?

### Tests
- Unit: metric increments via a direct `prometheus::Registry`.
- Integration: verify `nats_sink_dedup_total = 1` after the existing
  dedup integration test.

### Merge criterion
Operator can `curl /metrics` and see every new counter. ~450 LOC.

## PR C — Bounded reconnect + drift handling

### Scope
- Source fetch loop: track consecutive errors, exponential backoff
  500ms → 5s cap, flip `HealthStatus::Unhealthy` after
  `fetch.error.threshold` (default 10) consecutive errors. Reset on
  success.
- Consumer config drift: on `create_consumer` error, parse async-nats
  error kind; surface
  `LDB-5070 consumer '{name}' exists with incompatible config;
   rotate the durable name or delete the consumer out-of-band`.
- Shutdown latency: replace the `AtomicBool` + between-fetch check
  with `tokio::select!` that cancels `fetch()` directly. Lag drops
  from `fetch.max.wait.ms` to sub-ms.

### Deferred
- Periodic `duplicate_window` revalidation.

### Tests
- Unit: drift-error parsing against mocked error variants.
- Integration: delete the stream behind a running source, observe
  `HealthStatus::Unhealthy` within bounded time.

### Merge criterion
Fetch-error threshold test green. ~250 LOC.

## PR D — Chaos integration test

### Scope
- `exactly_once_under_broker_restart`: publish a burst,
  `docker compose restart nats` mid-burst, resume, verify stream
  contains the exact set of ids (no loss, no dup).
- `source_acks_only_after_commit`: publish, consume up to barrier,
  simulate `checkpoint()` without `notify_epoch_committed`, kill the
  source, restart, verify redelivery.

### Deferred
- Full-pipeline kill-mid-epoch — that's a `laminar-db` test, not a
  connector test.

### Tests
Shell out to `docker compose restart` / `stop` via
`std::process::Command`. `#[ignore]`-gated.

### Merge criterion
Both tests green 10 consecutive runs locally.
Label as Linux-CI-only (Docker Desktop on Windows has known SIGKILL
flakiness). ~300 LOC.

## PR E — Throughput benchmark

### Scope
- `benches/nats_throughput.rs` using `criterion`, gated
  `required-features = ["nats"]`.
- `sink_publish_jetstream` — baseline for regression detection.
- `source_consume_jetstream`.
- Workflow documented, numbers captured in `docs/BENCHMARKS.md`.

### Merge criterion
Benches compile + run + produce numbers. No threshold assertion
(criterion surfaces regressions across runs). ~200 LOC.

## Sequencing

```
A (Auth)    ──┐
              ├──→ D (Chaos) ──→ E (Bench)
B (Metrics) ──┤
              │
C (Reconnect)─┘
```

A, B, C independent. D depends on B (metrics are what chaos tests
assert on) and C (bounded reconnect is what "survives broker restart"
means). E last — benches against a hardened connector.

## Start order

**B first** — broadest utility, surfaces runtime behavior we've only
inferred from code reading, unblocks D. Operator visibility is the
single largest current gap.

Then A, C, D, E in that order.
