# laminar-admin

Administration interface for LaminarDB.

## Overview

Ring 2 crate providing a REST API for pipeline management, built on Axum with auto-generated OpenAPI documentation via utoipa/Swagger UI.

## Key Components

- **REST API** -- Pipeline management endpoints (Axum 0.8)
- **Swagger UI** -- Interactive API documentation (utoipa-swagger-ui)

## Related Crates

- [`laminar-auth`](../laminar-auth) -- Authentication and authorization
- [`laminar-observe`](../laminar-observe) -- Metrics and health endpoints
- [`laminar-server`](../laminar-server) -- Server binary that mounts the admin API
