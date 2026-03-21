# LaminarDB Deployment Guide

Three ways to run LaminarDB: pre-built binary, Docker, or Helm on Kubernetes.

---

## 1. Binary Download

Download pre-built binaries from [GitHub Releases](https://github.com/laminardb/laminardb/releases).

Available platforms:
| Platform | Archive |
|----------|---------|
| Linux x86_64 | `laminardb-server-x86_64-unknown-linux-gnu-v*.tar.gz` |
| Linux ARM64 | `laminardb-server-aarch64-unknown-linux-gnu-v*.tar.gz` |
| Linux x86_64 (musl) | `laminardb-server-x86_64-unknown-linux-musl-v*.tar.gz` |
| macOS Intel | `laminardb-server-x86_64-apple-darwin-v*.tar.gz` |
| macOS Apple Silicon | `laminardb-server-aarch64-apple-darwin-v*.tar.gz` |
| Windows x86_64 | `laminardb-server-x86_64-pc-windows-msvc-v*.zip` |

```bash
# Download and extract (Linux x86_64 example)
curl -LO https://github.com/laminardb/laminardb/releases/latest/download/laminardb-server-x86_64-unknown-linux-gnu-v0.18.12.tar.gz
tar xzf laminardb-server-x86_64-unknown-linux-gnu-v*.tar.gz

# Verify checksums
curl -LO https://github.com/laminardb/laminardb/releases/latest/download/SHA256SUMS
sha256sum -c SHA256SUMS --ignore-missing

# Run with example config
./laminardb --config examples/laminardb.toml
```

---

## 2. Docker

Images are published to both GHCR and Docker Hub on every push to `main` and tagged releases.

```bash
# Pull from GHCR
docker pull ghcr.io/laminardb/laminardb-server:latest

# Or from Docker Hub
docker pull laminardb/laminardb-server:latest

# Run with default config
docker run -d --name laminardb \
  -p 8080:8080 \
  -v laminardb-data:/var/lib/laminardb \
  ghcr.io/laminardb/laminardb-server:latest

# Check health
curl http://localhost:8080/health
```

Available tags:
- `latest` - latest build from `main`
- `0.18.12` - specific version
- `0.18` - latest patch for minor version
- `sha-abc1234` - specific commit

Multi-arch support: `linux/amd64` and `linux/arm64`.

### Docker Compose

```bash
docker compose up -d
```

See the root `docker-compose.yml` for a full example with Kafka and PostgreSQL.

---

## 3. Helm (Kubernetes)

### Install from GHCR OCI registry

```bash
helm install my-laminardb oci://ghcr.io/laminardb/charts/laminardb \
  --set image.tag=0.18.12
```

### Quick start (standalone embedded mode)

```bash
helm install my-laminardb oci://ghcr.io/laminardb/charts/laminardb \
  -f deploy/helm/laminardb/ci/standalone-values.yaml
```

### Connect after install

```bash
# Port-forward the HTTP API
kubectl port-forward svc/my-laminardb-laminardb 8080:8080

# Check health
curl http://localhost:8080/health

# Execute SQL
curl -X POST http://localhost:8080/api/v1/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SHOW SOURCES"}'
```

### Configuration

All values are documented in [`helm/laminardb/values.yaml`](helm/laminardb/values.yaml). Key sections:

| Section | Description |
|---------|-------------|
| `laminardb.mode` | `embedded` (single node) or `delta` (multi-node cluster) |
| `laminardb.checkpoint` | Checkpoint interval, storage URL, strategy |
| `sources` / `sinks` / `pipelines` | Streaming pipeline definitions (rendered to TOML) |
| `persistence` | PVC sizes for state and checkpoints |
| `serviceMonitor` | Prometheus Operator integration |

### CI test values

- `ci/standalone-values.yaml` - minimal single-node deployment
- `ci/constellation-values.yaml` - 3-node delta-mode cluster
- `ci/full-values.yaml` - all features enabled (ingress, monitoring, network policy)

---

## CI/CD Workflows

| Workflow | File | Triggers | Purpose |
|----------|------|----------|---------|
| CI | `ci.yml` | push, PR | Format, clippy, test, MSRV, feature matrix |
| Release | `release.yml` | `v*` tags | Build binaries, create GitHub Release, publish to crates.io |
| Docker | `docker.yml` | push to main, `v*` tags, PR | Multi-arch Docker images to GHCR + Docker Hub |
| Helm Publish | `helm-publish.yml` | push to main (helm changes) | Lint and publish Helm chart to GHCR OCI |
| Benchmarks | `bench.yml` | push to main | Run and track performance benchmarks |
| Coverage | `coverage.yml` | push, PR | Code coverage with Codecov |
| Docs | `docs.yml` | push to main (docs/code changes) | Build and deploy API docs to GitHub Pages |
| Dependencies | `deps.yml` | weekly (Sunday) | Check for outdated deps, auto-create update PR |

## Required GitHub Secrets

| Secret | Required By | How to Obtain |
|--------|------------|---------------|
| `DOCKERHUB_USERNAME` | docker.yml | Docker Hub account username |
| `DOCKERHUB_TOKEN` | docker.yml | Docker Hub > Settings > Security > New Access Token |
| `CARGO_REGISTRY_TOKEN` | release.yml | crates.io > Settings > API Tokens |
| `CODECOV_TOKEN` | coverage.yml | codecov.io project settings |
| `GITHUB_TOKEN` | all workflows | Automatically provided by GitHub Actions |
