# Contributing to LaminarDB

Thank you for your interest in contributing to LaminarDB!

## Development Setup

1. Clone the repository
2. Install Rust (stable channel)
3. Run `cargo build` to build all crates
4. Run `cargo test` to run tests

## Code Style

- Format code with `cargo fmt`
- Lint with `cargo clippy -- -D warnings`
- All public APIs must be documented
- Add tests for new functionality

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Performance Guidelines

For Ring 0 (hot path) code:
- Zero heap allocations
- No locks (use lock-free structures)
- Avoid system calls
- Profile and benchmark changes

## Documentation

- Update relevant documentation
- Add entries to CHANGELOG.md
- Update feature status in docs/features/

## Questions?

Feel free to open an issue for any questions!