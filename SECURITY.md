# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.12.x (latest) | Yes |
| < 0.12 | No |

## Reporting a Vulnerability

Please report security vulnerabilities by emailing **security@laminardb.io**.

**Do NOT open a public GitHub issue for security vulnerabilities.**

We will:
- Acknowledge receipt within 48 hours
- Provide a detailed response within 7 days
- Work with you to understand and address the issue
- Credit you in the security advisory (unless you prefer otherwise)

## Security Considerations

LaminarDB is an embedded database. Security considerations depend on your deployment model:

- **Embedded (library)**: Security is inherited from the host application. LaminarDB does not open network ports unless you use the admin API or server binary.
- **Server binary**: The standalone server exposes a network interface. Use `laminar-auth` for JWT authentication and RBAC/ABAC authorization.
- **Connectors**: Kafka, PostgreSQL, and MySQL connectors handle credentials. Connector configurations support TLS and are subject to secret masking in logs.

## Unsafe Code

All `unsafe` blocks in LaminarDB are documented with `// SAFETY:` comments explaining the invariants. Unsafe code is concentrated in performance-critical paths (Ring 0) and FFI boundaries.

We run `cargo +nightly miri test` in CI to detect undefined behavior in unsafe code.
