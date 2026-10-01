FROM rust:1.95-bookworm AS example-builder

WORKDIR /build
ENV CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0
RUN apt-get update && apt-get install -y --no-install-recommends cmake pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY vendor/ ./vendor/
COPY examples/process_python/manifest.json ./examples/process_python/manifest.json
RUN cargo build --locked -p laminar-db --no-default-features --features process-remote --example process_python

FROM debian:bookworm-slim AS example

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o 10001 -g 10001 /var/lib/laminardb-process
COPY --from=example-builder /build/target/debug/examples/process_python /usr/local/bin/process_python
ENV LAMINAR_PROCESS_ENDPOINT=http://127.0.0.1:50051
USER 10001:10001
ENTRYPOINT ["process_python"]

FROM python:3.13.1-slim AS worker

WORKDIR /opt/laminardb-process
COPY python/laminardb_process/requirements.lock ./requirements.lock
RUN python -m pip install --no-cache-dir -r requirements.lock

COPY python/laminardb_process/pyproject.toml ./pyproject.toml
COPY python/laminardb_process/laminardb_process ./laminardb_process
RUN python -m pip install --no-cache-dir --no-build-isolation --no-deps .

COPY examples/process_python/handler.py ./handler.py
COPY examples/process_python/manifest.json ./manifest.json
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD python -c "import grpc; c = grpc.insecure_channel('127.0.0.1:50051'); grpc.channel_ready_future(c).result(timeout=1); c.close()"
USER 10001:10001
ENTRYPOINT ["laminardb-process-worker"]
CMD ["--manifest", "manifest.json", "--handler", "handler:handle", "--handler-file", "handler.py", "--bind", "127.0.0.1:50051"]
