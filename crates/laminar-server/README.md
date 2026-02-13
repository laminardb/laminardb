# laminar-server

Standalone server binary for LaminarDB.

## Overview

Provides the `laminardb` binary that runs LaminarDB as a standalone server process. Combines all crates (core, SQL, storage, connectors, auth, admin, observe) into a single deployable binary.

## Running

```bash
# Build and run
cargo run --release --bin laminardb

# With configuration file
cargo run --release --bin laminardb -- --config config.toml
```

## Configuration

Configuration is loaded from (in order of precedence):
1. CLI arguments (via clap)
2. Environment variables
3. Configuration file (TOML)
4. Default values

## Binary

- **Name**: `laminardb`
- **Entry point**: `src/main.rs`

## Related Crates

This crate depends on all other LaminarDB crates and serves as the integration point:

- [`laminar-db`](../laminar-db) -- Database facade
- [`laminar-admin`](../laminar-admin) -- REST API
- [`laminar-observe`](../laminar-observe) -- Metrics and health endpoints
- [`laminar-auth`](../laminar-auth) -- Authentication
