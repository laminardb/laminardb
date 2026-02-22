# laminar-observe

Observability for LaminarDB -- metrics, tracing, and health checks.

## Status: Planned (Phase 5)

This crate exists as a workspace member with module stubs, but **no implementation exists yet**. The source files (`metrics.rs`, `tracing.rs`, `health.rs`) contain only module-level doc comments.

Implementation is planned for Phase 5 (Admin & Observability). See the [Feature Index](../../docs/features/INDEX.md) for details on features F048-F054.

Note: LaminarDB already has internal pipeline observability via `PipelineMetrics` and `PipelineCounters` in `laminar-db`. This crate will add external export capabilities (Prometheus, OpenTelemetry).

## Planned Components

- **Real-Time Metrics** (F048) -- Pipeline throughput, latency histograms, state store sizes
- **Prometheus Export** (F050) -- `/metrics` endpoint in Prometheus exposition format
- **OpenTelemetry Tracing** (F051) -- Distributed trace spans for query execution
- **Health Check Endpoints** (F052) -- Liveness and readiness probes

## Architecture

This crate operates in **Ring 2 (Control Plane)** with no latency requirements.

## Related Crates

- [`laminar-admin`](../laminar-admin) -- Will mount metrics and health endpoints
- [`laminar-core`](../laminar-core) -- Pipeline counters and metrics sources
