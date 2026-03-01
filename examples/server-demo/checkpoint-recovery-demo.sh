#!/usr/bin/env bash
# LaminarDB — Checkpoint & Recovery Demo
#
# Exercises the full crash-recovery lifecycle:
#   Start server → produce data → checkpoint → SIGKILL → restart → verify recovery
#
# Prerequisites:
#   - Docker (with Compose v2)
#   - Rust toolchain (cargo)
#   - jq (for JSON parsing)
#
# Usage:
#   bash examples/server-demo/checkpoint-recovery-demo.sh
#
# Environment:
#   LAMINARDB_PORT  — HTTP API port (default 9000)

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
CONFIG_FILE="$SCRIPT_DIR/laminardb.toml"

# Detect Windows (Git Bash / MSYS2).  The Rust server binary interprets
# file:///tmp/... as C:\tmp\... , but Git Bash maps /tmp to its own MSYS
# temp directory.  Use /c/tmp/... so both sides agree on the same path.
IS_WINDOWS=false
if [[ "$(uname -s)" == MINGW* ]] || [[ "$(uname -s)" == MSYS* ]] || [[ "$(uname -s)" == CYGWIN* ]]; then
    IS_WINDOWS=true
fi

if $IS_WINDOWS; then
    CHECKPOINT_DIR="/c/tmp/laminardb-server-demo/checkpoints"
else
    CHECKPOINT_DIR="/tmp/laminardb-server-demo/checkpoints"
fi

PORT="${LAMINARDB_PORT:-9000}"
BASE="http://localhost:$PORT"

SERVER_PID=""
PRODUCER_PID=""

# wait_for_pid_exit PID — poll until a process exits (or 10s timeout).
# On Windows, `wait` hangs after taskkill because bash doesn't know the
# process was reaped externally.
wait_for_pid_exit() {
    local pid="$1"
    for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 0.5
    done
}

# kill_server — force-kill all laminardb.exe processes (for crash simulation).
kill_server() {
    local pid="$1"
    if $IS_WINDOWS; then
        taskkill //F //T //PID "$pid" >/dev/null 2>&1 || true
        # Also kill any orphan laminardb.exe left by cargo
        taskkill //F //IM "laminardb.exe" >/dev/null 2>&1 || true
    else
        kill -9 "$pid" 2>/dev/null || true
    fi
    wait_for_pid_exit "$pid"
}

# graceful_kill PID — send SIGTERM (or taskkill on Windows).
# Only kills by PID tree, does NOT kill all laminardb.exe instances.
graceful_kill() {
    local pid="$1"
    if $IS_WINDOWS; then
        taskkill //F //T //PID "$pid" >/dev/null 2>&1 || true
    else
        kill "$pid" 2>/dev/null || true
    fi
    wait_for_pid_exit "$pid"
}

# ── Colors & helpers ──────────────────────────────────────────────────────

RED="\033[31m"
GREEN="\033[32m"
YELLOW="\033[33m"
CYAN="\033[36m"
BOLD="\033[1m"
DIM="\033[2m"
RESET="\033[0m"

phase() {
    echo ""
    echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}${CYAN}║  Phase $1${RESET}"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
}

info()    { echo -e "  ${GREEN}✓${RESET} $1"; }
warn()    { echo -e "  ${YELLOW}!${RESET} $1"; }
fail()    { echo -e "  ${RED}✗${RESET} $1"; }
detail()  { echo -e "  ${DIM}$1${RESET}"; }

wait_for_url() {
    local url="$1" max="$2" label="$3"
    for i in $(seq 1 "$max"); do
        if curl -sf "$url" >/dev/null 2>&1; then
            return 0
        fi
        if [ "$i" -eq "$max" ]; then
            fail "Timed out waiting for $label ($url)"
            return 1
        fi
        sleep 1
    done
}

get_metric() {
    local name="$1"
    curl -sf "$BASE/metrics" 2>/dev/null \
        | grep "^${name}" \
        | head -1 \
        | awk '{print $2}' || echo "0"
}

# ── Cleanup ───────────────────────────────────────────────────────────────

cleanup() {
    echo ""
    echo -e "${BOLD}Cleaning up...${RESET}"
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        detail "Stopping server (PID $SERVER_PID)"
        graceful_kill "$SERVER_PID"
    fi
    if [ -n "$PRODUCER_PID" ] && kill -0 "$PRODUCER_PID" 2>/dev/null; then
        detail "Stopping producer (PID $PRODUCER_PID)"
        graceful_kill "$PRODUCER_PID"
    fi
    echo -e "${DIM}  (Redpanda left running — use 'docker compose -f $COMPOSE_FILE down -v' to stop)${RESET}"
    echo ""
}

trap cleanup EXIT INT TERM

# ── Banner ────────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║         LaminarDB — Checkpoint & Recovery Demo              ║${RESET}"
echo -e "${BOLD}${CYAN}║                                                              ║${RESET}"
echo -e "${BOLD}${CYAN}║  Start → Produce → Checkpoint → SIGKILL → Restart → Verify  ║${RESET}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""

# ══════════════════════════════════════════════════════════════════════════
# Phase 1 — Setup
# ══════════════════════════════════════════════════════════════════════════

phase "1 — Setup"

# Clean checkpoint directory for a fresh demo run
if [ -d "$CHECKPOINT_DIR" ]; then
    detail "Removing old checkpoints at $CHECKPOINT_DIR"
    rm -rf "$CHECKPOINT_DIR"
fi
mkdir -p "$CHECKPOINT_DIR"
info "Checkpoint directory ready: $CHECKPOINT_DIR"

# Ensure Redpanda is running
if docker compose -f "$COMPOSE_FILE" ps --status running 2>/dev/null | grep -q redpanda; then
    info "Redpanda already running"
else
    detail "Starting Redpanda..."
    docker compose -f "$COMPOSE_FILE" up -d
    # Wait for init-topics to finish
    for i in $(seq 1 30); do
        STATUS=$(docker inspect --format='{{.State.Status}}' laminardb-server-demo-init-topics 2>/dev/null || echo "missing")
        if [ "$STATUS" = "exited" ]; then
            EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' laminardb-server-demo-init-topics 2>/dev/null || echo "1")
            if [ "$EXIT_CODE" = "0" ]; then
                break
            else
                fail "init-topics failed (exit code $EXIT_CODE)"
                exit 1
            fi
        fi
        if [ "$i" -eq 30 ]; then
            fail "Timed out waiting for topics"
            exit 1
        fi
        sleep 2
    done
    info "Redpanda started, topics created"
fi

# Start producer
detail "Starting demo producer..."
cd "$REPO_ROOT"
cargo run -p laminardb-demo --bin producer --features kafka > /dev/null 2>&1 &
PRODUCER_PID=$!
info "Producer running (PID $PRODUCER_PID)"
sleep 2

# ══════════════════════════════════════════════════════════════════════════
# Phase 2 — Run & Accumulate
# ══════════════════════════════════════════════════════════════════════════

phase "2 — Run & Accumulate"

detail "Starting LaminarDB server..."
cargo run -p laminar-server --features kafka -- --config "$CONFIG_FILE" > /dev/null 2>&1 &
SERVER_PID=$!
info "Server started (PID $SERVER_PID)"

detail "Waiting for server to become healthy..."
if ! wait_for_url "$BASE/health" 60 "server health"; then
    fail "Server did not become healthy"
    exit 1
fi
info "Server is healthy"

# Let data accumulate
ACCUMULATE_SECS=15
detail "Accumulating data for ${ACCUMULATE_SECS}s..."
sleep "$ACCUMULATE_SECS"

# Capture metrics snapshot
EVENTS_BEFORE=$(get_metric "laminardb_events_ingested_total")
info "Events ingested so far: $EVENTS_BEFORE"

echo ""
detail "Per-source metrics:"
curl -sf "$BASE/metrics" 2>/dev/null \
    | grep "^laminardb_source_events_total" \
    | while read -r line; do detail "  $line"; done

HEALTH=$(curl -sf "$BASE/health" 2>/dev/null)
PIPELINE_STATE=$(echo "$HEALTH" | grep -o '"pipeline_state":"[^"]*"' | cut -d'"' -f4)
info "Pipeline state: $PIPELINE_STATE"

if [ "${EVENTS_BEFORE%.*}" = "0" ] || [ -z "$EVENTS_BEFORE" ]; then
    warn "No events ingested yet — demo may not produce meaningful recovery data"
fi

# ══════════════════════════════════════════════════════════════════════════
# Phase 3 — Checkpoint
# ══════════════════════════════════════════════════════════════════════════

phase "3 — Checkpoint"

detail "Triggering manual checkpoint..."
CKPT_RESPONSE=$(curl -sf -X POST "$BASE/api/v1/checkpoint" 2>/dev/null || echo '{"success":false}')
echo ""
echo -e "  ${BOLD}Checkpoint response:${RESET}"
if command -v jq >/dev/null 2>&1; then
    echo "$CKPT_RESPONSE" | jq '.' | sed 's/^/    /'
else
    echo "    $CKPT_RESPONSE"
fi
echo ""

CKPT_SUCCESS=$(echo "$CKPT_RESPONSE" | grep -o '"success":true' || true)
if [ -z "$CKPT_SUCCESS" ]; then
    warn "Checkpoint may not have succeeded — continuing anyway"
fi

FIRST_EPOCH=$(echo "$CKPT_RESPONSE" | grep -o '"epoch":[0-9]*' | cut -d: -f2 || echo "?")
FIRST_CKPT_ID=$(echo "$CKPT_RESPONSE" | grep -o '"checkpoint_id":[0-9]*' | cut -d: -f2 || echo "?")
info "Checkpoint ID: $FIRST_CKPT_ID, Epoch: $FIRST_EPOCH"

# Show checkpoint files on disk
echo ""
detail "Checkpoint files on disk:"
if [ -d "$CHECKPOINT_DIR" ]; then
    find "$CHECKPOINT_DIR" -type f 2>/dev/null | sort | while read -r f; do
        SIZE=$(stat --format=%s "$f" 2>/dev/null || stat -f%z "$f" 2>/dev/null || echo "?")
        detail "  $(echo "$f" | sed "s|$CHECKPOINT_DIR/||")  ($SIZE bytes)"
    done
else
    warn "No checkpoint directory found"
fi

# Show manifest excerpt
LATEST_MANIFEST=$(find "$CHECKPOINT_DIR" -name "manifest.json" 2>/dev/null | sort | tail -1 || true)
if [ -n "$LATEST_MANIFEST" ] && [ -f "$LATEST_MANIFEST" ]; then
    echo ""
    detail "Manifest excerpt ($LATEST_MANIFEST):"
    if command -v jq >/dev/null 2>&1; then
        jq '{epoch, checkpoint_id, source_offsets, pipeline_hash}' "$LATEST_MANIFEST" 2>/dev/null | sed 's/^/    /' || cat "$LATEST_MANIFEST" | head -20 | sed 's/^/    /'
    else
        head -20 "$LATEST_MANIFEST" | sed 's/^/    /'
    fi
fi

# ══════════════════════════════════════════════════════════════════════════
# Phase 4 — Crash (SIGKILL)
# ══════════════════════════════════════════════════════════════════════════

phase "4 — Crash (force-kill)"

detail "Killing server process tree (simulating hard crash)..."
kill_server "$SERVER_PID"
info "Server killed (PID $SERVER_PID)"
SERVER_PID=""

# Verify checkpoint files survived
CKPT_FILE_COUNT=$(find "$CHECKPOINT_DIR" -type f 2>/dev/null | wc -l)
if [ "$CKPT_FILE_COUNT" -gt 0 ]; then
    info "Checkpoint files survived crash ($CKPT_FILE_COUNT files on disk)"
else
    fail "No checkpoint files found after crash!"
fi

# Brief pause to ensure port is released
sleep 2

# ══════════════════════════════════════════════════════════════════════════
# Phase 5 — Recovery
# ══════════════════════════════════════════════════════════════════════════

phase "5 — Recovery"

detail "Restarting server (same config, same checkpoint dir)..."
cargo run -p laminar-server --features kafka -- --config "$CONFIG_FILE" > /dev/null 2>&1 &
SERVER_PID=$!
info "Server restarted (PID $SERVER_PID)"

detail "Waiting for server to recover and become healthy..."
if ! wait_for_url "$BASE/health" 60 "server health after recovery"; then
    fail "Server did not recover"
    exit 1
fi

HEALTH=$(curl -sf "$BASE/health" 2>/dev/null)
PIPELINE_STATE=$(echo "$HEALTH" | grep -o '"pipeline_state":"[^"]*"' | cut -d'"' -f4)
info "Server recovered — pipeline state: $PIPELINE_STATE"

# ══════════════════════════════════════════════════════════════════════════
# Phase 6 — Verify
# ══════════════════════════════════════════════════════════════════════════

phase "6 — Verify"

VERIFY_SECS=20
detail "Waiting ${VERIFY_SECS}s for Kafka consumer group rebalance + new data..."
sleep "$VERIFY_SECS"

EVENTS_AFTER=$(get_metric "laminardb_events_ingested_total")
info "Events ingested after recovery: $EVENTS_AFTER"

echo ""
detail "Per-source metrics (post-recovery):"
curl -sf "$BASE/metrics" 2>/dev/null \
    | grep "^laminardb_source_events_total" \
    | while read -r line; do detail "  $line"; done

# Verify data is flowing
EVENTS_INT=${EVENTS_AFTER%.*}
if [ -n "$EVENTS_INT" ] && [ "$EVENTS_INT" != "0" ]; then
    info "Data is flowing after recovery"
else
    warn "No events ingested after recovery — check Kafka connectivity"
fi

# Trigger second checkpoint
echo ""
detail "Triggering second checkpoint..."
CKPT2_RESPONSE=$(curl -sf -X POST "$BASE/api/v1/checkpoint" 2>/dev/null || echo '{"success":false}')
echo ""
echo -e "  ${BOLD}Second checkpoint response:${RESET}"
if command -v jq >/dev/null 2>&1; then
    echo "$CKPT2_RESPONSE" | jq '.' | sed 's/^/    /'
else
    echo "    $CKPT2_RESPONSE"
fi
echo ""

SECOND_EPOCH=$(echo "$CKPT2_RESPONSE" | grep -o '"epoch":[0-9]*' | cut -d: -f2 || echo "?")
SECOND_CKPT_ID=$(echo "$CKPT2_RESPONSE" | grep -o '"checkpoint_id":[0-9]*' | cut -d: -f2 || echo "?")

if [ "$SECOND_EPOCH" != "?" ] && [ "$FIRST_EPOCH" != "?" ] && [ "$SECOND_EPOCH" -gt "$FIRST_EPOCH" ] 2>/dev/null; then
    info "Epoch advanced: $FIRST_EPOCH → $SECOND_EPOCH"
else
    warn "Epoch comparison: first=$FIRST_EPOCH, second=$SECOND_EPOCH"
fi

# Show all checkpoint files
echo ""
detail "All checkpoint files after recovery:"
find "$CHECKPOINT_DIR" -type f 2>/dev/null | sort | while read -r f; do
    SIZE=$(stat --format=%s "$f" 2>/dev/null || stat -f%z "$f" 2>/dev/null || echo "?")
    detail "  $(echo "$f" | sed "s|$CHECKPOINT_DIR/||")  ($SIZE bytes)"
done

# ══════════════════════════════════════════════════════════════════════════
# Phase 7 — Cleanup
# ══════════════════════════════════════════════════════════════════════════

phase "7 — Cleanup"

detail "Stopping server..."
if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    graceful_kill "$SERVER_PID"
    SERVER_PID=""
fi
info "Server stopped"

detail "Stopping producer..."
if [ -n "$PRODUCER_PID" ] && kill -0 "$PRODUCER_PID" 2>/dev/null; then
    graceful_kill "$PRODUCER_PID"
    PRODUCER_PID=""
fi
info "Producer stopped"

echo ""
echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${GREEN}║  Demo complete!                                              ║${RESET}"
echo -e "${BOLD}${GREEN}║                                                              ║${RESET}"
echo -e "${BOLD}${GREEN}║  Summary:                                                    ║${RESET}"
echo -e "${BOLD}${GREEN}║    First checkpoint:  ID=$FIRST_CKPT_ID  Epoch=$FIRST_EPOCH$(printf '%*s' $((27 - ${#FIRST_CKPT_ID} - ${#FIRST_EPOCH})) '')║${RESET}"
echo -e "${BOLD}${GREEN}║    Second checkpoint: ID=$SECOND_CKPT_ID  Epoch=$SECOND_EPOCH$(printf '%*s' $((26 - ${#SECOND_CKPT_ID} - ${#SECOND_EPOCH})) '')║${RESET}"
echo -e "${BOLD}${GREEN}║    Events pre-crash:  $EVENTS_BEFORE$(printf '%*s' $((37 - ${#EVENTS_BEFORE})) '')║${RESET}"
echo -e "${BOLD}${GREEN}║    Events post-recovery: $EVENTS_AFTER$(printf '%*s' $((34 - ${#EVENTS_AFTER})) '')║${RESET}"
echo -e "${BOLD}${GREEN}║                                                              ║${RESET}"
echo -e "${BOLD}${GREEN}║  To tear down Redpanda:                                      ║${RESET}"
echo -e "${BOLD}${GREEN}║    docker compose -f examples/server-demo/docker-compose.yml \\ ║${RESET}"
echo -e "${BOLD}${GREEN}║      down -v                                                 ║${RESET}"
echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""
