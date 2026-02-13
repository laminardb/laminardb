# laminar-observe

Observability for LaminarDB -- metrics, tracing, and health checks.

## Overview

Ring 2 crate providing production observability. Exports metrics via Prometheus and traces via OpenTelemetry.

## Key Components

- **Prometheus Metrics** -- Pipeline throughput, latency histograms, state store sizes
- **OpenTelemetry Tracing** -- Distributed trace spans for query execution
- **Health Checks** -- Liveness and readiness endpoints

## Dependencies

- `prometheus` 0.14
- `opentelemetry` 0.31
- `tracing` + `tracing-opentelemetry`
- `axum` 0.8 (health/metrics endpoints)

## Related Crates

- [`laminar-admin`](../laminar-admin) -- Mounts metrics and health endpoints
- [`laminar-core`](../laminar-core) -- Pipeline counters and metrics sources
