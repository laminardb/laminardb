# laminar-server

Standalone server binary for LaminarDB.

## Status: Skeleton

The server binary exists with CLI argument parsing and logging initialization, but the core server logic (configuration loading, reactor initialization, admin API mounting) is not yet implemented. The `main.rs` contains TODO comments for Phase 5 features.

## What Works

- CLI argument parsing via clap (`--config`, `--log-level`, `--admin-bind`)
- Tracing/logging initialization via tracing-subscriber
- Prints version and config file path

## What is Planned (Phase 6c)

- **TOML Configuration** (F-SERVER-001) -- Load pipeline definitions from TOML
- **Engine Construction** (F-SERVER-002) -- Initialize reactor from config
- **HTTP API** (F-SERVER-003) -- Admin and query endpoints
- **Hot Reload** (F-SERVER-004) -- Reload config without restart
- **Delta Server Mode** (F-SERVER-005) -- Multi-node distributed operation
- **Graceful Rolling Restart** (F-SERVER-006) -- Zero-downtime upgrades

## Running

```bash
# Build and run (currently just prints startup info)
cargo run --release --bin laminardb

# With configuration file
cargo run --release --bin laminardb -- --config config.toml

# Custom log level and bind address
cargo run --release --bin laminardb -- --log-level debug --admin-bind 0.0.0.0:8080
```

## Related Crates

This crate will depend on all other LaminarDB crates and serve as the integration point:

- [`laminar-db`](../laminar-db) -- Database facade
- [`laminar-admin`](../laminar-admin) -- REST API (planned)
- [`laminar-observe`](../laminar-observe) -- Metrics and health endpoints (planned)
- [`laminar-auth`](../laminar-auth) -- Authentication (planned)
