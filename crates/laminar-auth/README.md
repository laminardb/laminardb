# laminar-auth

Authentication and authorization for LaminarDB.

## Overview

Ring 2 crate providing security infrastructure. Handles JWT-based authentication and policy-based authorization (RBAC and ABAC).

## Key Components

- **JWT Authentication** -- Token validation and claims extraction via `jsonwebtoken`
- **RBAC** -- Role-based access control with role hierarchies
- **ABAC** -- Attribute-based access control with policy evaluation
- **Row-Level Security** -- Per-user data filtering (planned)

## Related Crates

- [`laminar-admin`](../laminar-admin) -- REST API that uses auth for endpoint protection
- [`laminar-server`](../laminar-server) -- Server binary that configures auth
