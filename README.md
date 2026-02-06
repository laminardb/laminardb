# LaminarDB

A high-performance embedded streaming database in Rust. Think "SQLite for stream processing" with sub-microsecond latency.

## Features

- **Embedded-first**: Single binary deployment, no cluster required
- **Sub-microsecond latency**: < 1μs event processing on the hot path
- **SQL-native**: Full SQL support with streaming extensions
- **Thread-per-core**: Linear scaling with CPU cores
- **Exactly-once semantics**: End-to-end delivery guarantees

## Quick Start

```bash
# Build all crates
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench

# Start the server
cargo run --bin laminardb
```

## Architecture

LaminarDB uses a ring architecture to separate concerns:

- **Ring 0 (Hot Path)**: Event processing with zero allocations
- **Ring 1 (Background)**: Checkpointing, WAL, compaction
- **Ring 2 (Control Plane)**: Admin API, metrics, configuration

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for details.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Development Roadmap](docs/ROADMAP.md)
- [Contributing Guide](CONTRIBUTING.md)
- [API Documentation](https://laminardb.io/api/)

## Performance

| Metric | Target | Status |
|--------|--------|--------|
| Throughput | 500K events/sec/core | 🚧 In Progress |
| State Lookup | < 500ns p99 | 🚧 In Progress |
| Event Processing | < 1μs | 🚧 In Progress |
| Recovery Time | < 10s | 🚧 In Progress |

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.