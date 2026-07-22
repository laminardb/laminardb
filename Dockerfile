# syntax=docker/dockerfile:1
#
# Multi-stage Dockerfile for laminardb-server.
# Supports linux/amd64 and linux/arm64 via Docker Buildx.

# --------------------------------------------------------------------------
# Stage 1: Builder
# --------------------------------------------------------------------------
FROM rust:1.93-bookworm AS builder

ARG TARGETARCH
ARG VERSION=0.0.0-dev
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    cmake \
    pkg-config \
    libssl-dev \
    libelf-dev \
    zlib1g-dev \
    libcurl4-openssl-dev \
    libsasl2-dev \
    libudev-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Override release profile for Docker builds: thin LTO + 2 CGUs to reduce
# peak memory during linking (fat LTO needs 8-12GB, QEMU arm64 amplifies 2-4x).
ENV CARGO_PROFILE_RELEASE_LTO=thin \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=2 \
    CARGO_PROFILE_RELEASE_STRIP=symbols

# The server workspace uses vendored path dependencies, so both trees must be
# present before Cargo resolves the build.
COPY Cargo.toml Cargo.lock ./

# Strip example crates from workspace members (not needed for server build)
RUN sed -i '/"examples\//d' Cargo.toml

COPY crates/ crates/
COPY vendor/ vendor/

# Build the server binary in release mode
RUN cargo build --release -p laminar-server

# --------------------------------------------------------------------------
# Stage 2: Runtime
# --------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

ARG VERSION=0.0.0-dev
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown

# OCI image spec labels
LABEL org.opencontainers.image.source="https://github.com/laminardb/laminardb"
LABEL org.opencontainers.image.title="laminardb-server"
LABEL org.opencontainers.image.description="LaminarDB - Embedded Streaming SQL Database Server"
LABEL org.opencontainers.image.vendor="LaminarDB"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${GIT_SHA}"
LABEL org.opencontainers.image.created="${BUILD_DATE}"

# Install minimal runtime dependencies.
# `apt-get upgrade` pulls security-patched versions of base-image packages
# (e.g. gnutls) so the Trivy scan doesn't flag CVEs that Debian has already
# fixed but the stale base layer still carries.
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    tini \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -g 1000 laminardb \
    && useradd -u 1000 -g laminardb -m -s /bin/false laminardb

# Create data directories
RUN mkdir -p /etc/laminardb /var/lib/laminardb/state /var/lib/laminardb/checkpoints \
    && chown -R laminardb:laminardb /etc/laminardb /var/lib/laminardb

# Copy binary from builder
COPY --from=builder /build/target/release/laminardb /usr/local/bin/laminardb

# Copy default configuration template
COPY <<'EOF' /etc/laminardb/laminardb.toml
# LaminarDB Server Configuration
# See https://github.com/laminardb/laminardb for documentation.

[server]
mode = "single"
bind = "0.0.0.0:8080"
workers = 0
log_level = "info"

[state]
backend = "local"
path = "/var/lib/laminardb/state"

[checkpoint]
url = "file:///var/lib/laminardb/checkpoints"
interval = "30s"
EOF

# Expose ports
EXPOSE 8080
EXPOSE 5432
EXPOSE 7946
EXPOSE 7947

# Volumes for persistent data and configuration
VOLUME ["/var/lib/laminardb"]
VOLUME ["/etc/laminardb"]

# Health check
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Disable file watcher by default in containers (use POST /api/v1/reload instead)
ENV LAMINAR_DISABLE_FILE_WATCH=1

# Run as non-root user
USER laminardb

ENTRYPOINT ["tini", "--", "laminardb"]
CMD ["--config", "/etc/laminardb/laminardb.toml"]
