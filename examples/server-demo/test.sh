#!/usr/bin/env bash
# LaminarDB Server Demo — Endpoint tester
#
# Curls all HTTP API endpoints with formatted output.
# Run after the server is up (see run.sh).
#
# Usage:
#   bash examples/server-demo/test.sh

set -euo pipefail

BASE="http://localhost:9000"
BOLD="\033[1m"
GREEN="\033[32m"
CYAN="\033[36m"
RESET="\033[0m"

section() {
    echo ""
    echo -e "${BOLD}${CYAN}━━━ $1 ━━━${RESET}"
}

# ── Health checks ───────────────────────────────────────────────────────

section "GET /health"
curl -s "$BASE/health" | python3 -m json.tool 2>/dev/null || curl -s "$BASE/health"

section "GET /ready"
curl -s "$BASE/ready" | python3 -m json.tool 2>/dev/null || curl -s "$BASE/ready"

# ── Catalog ─────────────────────────────────────────────────────────────

section "GET /api/v1/sources"
curl -s "$BASE/api/v1/sources" | python3 -m json.tool 2>/dev/null || curl -s "$BASE/api/v1/sources"

section "GET /api/v1/streams"
curl -s "$BASE/api/v1/streams" | python3 -m json.tool 2>/dev/null || curl -s "$BASE/api/v1/streams"

section "GET /api/v1/sinks"
curl -s "$BASE/api/v1/sinks" | python3 -m json.tool 2>/dev/null || curl -s "$BASE/api/v1/sinks"

# ── Ad-hoc SQL ──────────────────────────────────────────────────────────

section "POST /api/v1/sql (ad-hoc query)"
curl -s -X POST "$BASE/api/v1/sql" \
    -H "Content-Type: application/json" \
    -d '{"sql": "SELECT 1 + 1 AS result"}' \
    | python3 -m json.tool 2>/dev/null \
    || curl -s -X POST "$BASE/api/v1/sql" \
           -H "Content-Type: application/json" \
           -d '{"sql": "SELECT 1 + 1 AS result"}'

# ── Checkpoint ──────────────────────────────────────────────────────────

section "POST /api/v1/checkpoint (trigger)"
curl -s -X POST "$BASE/api/v1/checkpoint" | python3 -m json.tool 2>/dev/null || curl -s -X POST "$BASE/api/v1/checkpoint"

# ── Metrics ─────────────────────────────────────────────────────────────

section "GET /metrics (Prometheus, first 30 lines)"
curl -s "$BASE/metrics" | head -30

# ── Summary ─────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}${GREEN}All endpoints tested.${RESET}"
echo ""
echo "Interactive exploration:"
echo "  curl $BASE/api/v1/streams/ohlc_bars     # Single stream detail"
echo "  curl $BASE/api/v1/streams/vwap           # VWAP stream detail"
echo ""
echo "Redpanda Console: http://localhost:8888"
echo "  Check output topics: ohlc-bars, volume-metrics, vwap-results, anomaly-alerts"
