# laminar-auth

Authentication and authorization for LaminarDB.

## Status: Planned (Phase 4)

This crate exists as a workspace member with module stubs, but **no implementation exists yet**. The source files (`authn.rs`, `authz.rs`, `rls.rs`) contain only module-level doc comments.

Implementation is planned for Phase 4 (Enterprise & Security). See the [Feature Index](../../docs/features/INDEX.md) for details on features F035-F045.

## Planned Components

- **JWT Authentication** (F036) -- Token validation and claims extraction
- **mTLS Authentication** (F037) -- Mutual TLS client certificate auth
- **LDAP Integration** (F038) -- Directory-based authentication
- **RBAC** (F039) -- Role-based access control with role hierarchies
- **ABAC** (F040) -- Attribute-based access control with policy evaluation
- **Row-Level Security** (F041) -- Per-user data filtering
- **Column-Level Security** (F042) -- Per-user column masking
- **Audit Logging** (F043) -- Tamper-evident audit trail
- **Encryption at Rest** (F044) -- State store and WAL encryption
- **Key Management** (F045) -- Key rotation and vault integration

## Architecture

This crate operates in **Ring 2 (Control Plane)** with no latency requirements.

## Related Crates

- [`laminar-admin`](../laminar-admin) -- REST API that will use auth for endpoint protection
- [`laminar-server`](../laminar-server) -- Server binary that will configure auth
