# LaminarDB build automation

set dotenv-load

# Default recipe
default:
    @just --list

# ── Docker ───────────────────────────────────────────────────────────────

# Build Docker image locally
docker-build:
    docker build -t laminardb/laminardb-server:dev \
        --build-arg VERSION=dev \
        --build-arg GIT_SHA=$(git rev-parse --short HEAD) \
        --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
        .

# Run with docker-compose (laminardb + kafka)
docker-run:
    docker compose up -d laminardb kafka

# Push to Docker Hub (requires auth)
docker-push tag="latest":
    docker tag laminardb/laminardb-server:dev laminardb/laminardb-server:{{tag}}
    docker push laminardb/laminardb-server:{{tag}}

# ── Helm ─────────────────────────────────────────────────────────────────

# Lint chart
helm-lint:
    helm lint deploy/helm/laminardb --strict

# Template chart with default values
helm-template:
    helm template laminardb deploy/helm/laminardb

# Template with all features enabled
helm-template-full:
    helm template laminardb deploy/helm/laminardb -f deploy/helm/laminardb/ci/full-values.yaml

# Package chart
helm-package:
    helm package deploy/helm/laminardb

# Install to local k8s (kind/minikube)
helm-install-local:
    helm install laminardb deploy/helm/laminardb \
        -f deploy/helm/laminardb/ci/standalone-values.yaml \
        --create-namespace -n laminardb

# Upgrade existing installation
helm-upgrade-local:
    helm upgrade laminardb deploy/helm/laminardb \
        -f deploy/helm/laminardb/ci/standalone-values.yaml \
        -n laminardb

# Uninstall from local k8s
helm-uninstall-local:
    helm uninstall laminardb -n laminardb

# ── Development ──────────────────────────────────────────────────────────

# Start full local stack (laminardb + kafka + monitoring)
dev-up:
    docker compose --profile monitoring up -d

# Stop local stack
dev-down:
    docker compose --profile monitoring down

# Tail laminardb logs
dev-logs:
    docker compose logs -f laminardb

# Check pipeline status via HTTP API
dev-status:
    @curl -s http://localhost:8080/health | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8080/health
