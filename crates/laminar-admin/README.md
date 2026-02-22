# laminar-admin

Administration interface for LaminarDB.

## Status: Planned (Phase 5)

This crate exists as a workspace member with module stubs, but **no implementation exists yet**. The source files (`api.rs`, `dashboard.rs`, `cli.rs`) contain only module-level doc comments.

Implementation is planned for Phase 5 (Admin & Observability). See the [Feature Index](../../docs/features/INDEX.md) for details on features F046-F055.

## Planned Components

- **Admin REST API** (F046) -- Pipeline management endpoints via Axum
- **Web Dashboard** (F047) -- Browser-based admin dashboard
- **SQL Query Console** (F049) -- Interactive SQL console
- **CLI Tools** (F055) -- Command-line administration

## Architecture

This crate operates in **Ring 2 (Control Plane)** with no latency requirements.

## Related Crates

- [`laminar-auth`](../laminar-auth) -- Authentication and authorization
- [`laminar-observe`](../laminar-observe) -- Metrics and health endpoints
- [`laminar-server`](../laminar-server) -- Server binary that will mount the admin API
