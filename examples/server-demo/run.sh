#!/usr/bin/env bash
# LaminarDB Server Demo — All-in-one launcher
#
# Starts Redpanda, the demo producer, and the LaminarDB server.
#
# Usage:
#   cd examples/server-demo && bash run.sh
#
# Prerequisites:
#   - Docker (with Compose v2)
#   - Rust toolchain (cargo)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
CONFIG_FILE="$SCRIPT_DIR/laminardb.toml"

PRODUCER_PID=""

cleanup() {
    echo ""
    echo "=== Shutting down ==="
    if [ -n "$PRODUCER_PID" ] && kill -0 "$PRODUCER_PID" 2>/dev/null; then
        echo "Stopping producer (PID $PRODUCER_PID)..."
        kill "$PRODUCER_PID" 2>/dev/null || true
        wait "$PRODUCER_PID" 2>/dev/null || true
    fi
    echo "Stopping Docker containers..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    echo "Done."
}

trap cleanup EXIT INT TERM

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              LaminarDB Server Demo                         ║"
echo "║                                                            ║"
echo "║  Kafka sources → SQL pipelines → Kafka sinks              ║"
echo "║  3 sources · 4 pipelines · 4 sinks                        ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Start Redpanda ──────────────────────────────────────────────

echo "[1/4] Starting Redpanda (Kafka broker)..."
docker compose -f "$COMPOSE_FILE" up -d

echo "[2/4] Waiting for topics to be created..."
# Wait for init-topics container to finish
for i in $(seq 1 30); do
    STATUS=$(docker inspect --format='{{.State.Status}}' laminardb-server-demo-init-topics 2>/dev/null || echo "missing")
    if [ "$STATUS" = "exited" ]; then
        EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' laminardb-server-demo-init-topics 2>/dev/null || echo "1")
        if [ "$EXIT_CODE" = "0" ]; then
            echo "  Topics ready."
            break
        else
            echo "  ERROR: init-topics failed (exit code $EXIT_CODE)"
            docker logs laminardb-server-demo-init-topics
            exit 1
        fi
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ERROR: Timed out waiting for topics"
        exit 1
    fi
    sleep 2
done

# ── Step 2: Start producer ──────────────────────────────────────────────

echo "[3/4] Starting demo producer (background)..."
cd "$REPO_ROOT"
cargo run -p laminardb-demo --bin producer --features kafka &
PRODUCER_PID=$!
echo "  Producer PID: $PRODUCER_PID"

# Give producer a moment to connect
sleep 3

# ── Step 3: Start server ────────────────────────────────────────────────

echo "[4/4] Starting LaminarDB server..."
echo ""
echo "┌──────────────────────────────────────────────────────────────┐"
echo "│  Server:           http://localhost:9000                     │"
echo "│  Metrics:          http://localhost:9000/metrics             │"
echo "│  Redpanda Console: http://localhost:8888                     │"
echo "│                                                              │"
echo "│  Test endpoints:   bash examples/server-demo/test.sh         │"
echo "│  Press Ctrl+C to stop everything.                            │"
echo "└──────────────────────────────────────────────────────────────┘"
echo ""

cargo run -p laminar-server --features kafka -- --config "$CONFIG_FILE"
