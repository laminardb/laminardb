FROM rust:1.95-bookworm AS builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY . .
RUN RUSTUP_TOOLCHAIN=1.95.0 cargo build \
    --manifest-path examples/demos/markout_lab/Cargo.toml \
    --release \
    --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /workspace/examples/demos/markout_lab/target/release/markout-lab /usr/local/bin/markout-lab

EXPOSE 8088
ENTRYPOINT ["/usr/local/bin/markout-lab"]
CMD ["--host", "0.0.0.0", "--port", "8088", "--maker-fee-bps", "0.0"]
