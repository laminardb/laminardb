# Multi-stage build for LaminarDB demo (embedded + Kafka modes).
#
# Usage:
#   docker build -f examples/demo/Dockerfile -t laminardb-demo .
#   docker run --rm -it laminardb-demo                    # embedded mode
#   docker run --rm -it -e DEMO_MODE=kafka laminardb-demo # Kafka mode

# -- Build stage --
FROM rust:1.93-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake make g++ libssl-dev libcurl4-openssl-dev pkg-config clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cargo build --release -p laminardb-demo --features kafka

# -- Runtime stage --
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/demo /usr/local/bin/demo
COPY --from=builder /build/target/release/producer /usr/local/bin/producer

ENV TERM=xterm-256color

ENTRYPOINT ["demo"]
