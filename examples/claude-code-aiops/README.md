# Claude Code Cost Monitor

Real-time cost, token, and tool usage monitoring for Claude Code. Zero infrastructure.

## The Problem

You're spending real money on Claude Code but flying blind:
- How much did that last coding session cost?
- Which prompts burned the most tokens?
- Are you getting good cache hit rates or re-sending context?
- Which tools are slow or failing?

You find out from next month's invoice. Not in real time.

## The Solution

LaminarDB receives Claude Code's built-in OpenTelemetry events, runs streaming
SQL over them in real time, and pushes results to a live dashboard over WebSocket.

**One binary. One HTML file. Five env vars. That's it.**

```
Claude Code  --OTLP/gRPC-->  LaminarDB  --WebSocket-->  Dashboard
  (you)                      (1 binary)                 (1 file)
```

No Datadog. No Grafana. No Prometheus. No ClickHouse. No Docker.

## Quick Start

### 1. Start LaminarDB

**Using pre-compiled binary:**

```bash
# Download from https://github.com/laminardb/laminardb/releases
laminardb --config examples/claude-code-aiops/config.toml
```

**Or build from source:**

```bash
cargo run --release -p laminar-server --no-default-features \
    --features mimalloc,otel,websocket,files -- \
    --config examples/claude-code-aiops/config.toml
```

### 2. Open the dashboard

Open `examples/claude-code-aiops/dashboard.html` directly in your browser, or serve it:

```bash
python -m http.server 8000 -d examples/claude-code-aiops
# Open http://localhost:8000/dashboard.html
```

### 3. Start Claude Code with telemetry

```bash
CLAUDE_CODE_ENABLE_TELEMETRY=1 \
OTEL_LOGS_EXPORTER=otlp \
OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_LOGS_EXPORT_INTERVAL=5000 \
claude
```

Use Claude Code normally. The dashboard updates in real time.

**Windows (PowerShell):**

```powershell
$env:CLAUDE_CODE_ENABLE_TELEMETRY=1
$env:OTEL_LOGS_EXPORTER="otlp"
$env:OTEL_EXPORTER_OTLP_PROTOCOL="grpc"
$env:OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317"
$env:OTEL_LOGS_EXPORT_INTERVAL=5000
claude
```

## What You See

| Panel | Updates | What It Shows |
|-------|---------|---------------|
| **Prompt Lifecycle** | streaming | Stream join: prompt → api_request → tools correlated by prompt.id |
| **Session Cost** | 15s | Running total with sparkline and $/min burn rate |
| **Token Breakdown** | 15s | Input/output/cache-read/cache-create + cache hit rate |
| **Cost by Model** | 15s | Per-model cost bars (Opus vs Sonnet vs Haiku) |
| **Tool Usage** | 15s | Per-tool call counts, success rates, latency |
| **Tool Permissions** | 15s | Accept/reject decisions, auto-approval % |
| **Per-Prompt Breakdown** | 30s | Cost, model, tokens, tools, cache %, duration per prompt |

## Hero Feature: Stream-to-Stream Temporal Join

```sql
SELECT p.prompt_id, a.model, a.cost_usd, a.duration_ms
FROM enriched_events p
JOIN enriched_events a
    ON p.prompt_id = a.prompt_id
    AND a.ts_ms BETWEEN p.ts_ms AND p.ts_ms + INTERVAL '2' MINUTE
WHERE p.event_name = 'user_prompt'
    AND a.event_name = 'api_request'
```

Claude Code emits separate events for prompts, API calls, and tool results.
They share `prompt.id` but arrive independently. This SQL joins them in
real-time as they stream in — reconstructing "what happened for this prompt?"

No other observability tool does this. Prometheus stores counters (no joins).
Grafana queries retroactively (no streaming). AI platforms trace within
their SDK (no arbitrary SQL joins across event types).

## Pipelines

| Pipeline | Type | What It Computes |
|----------|------|------------------|
| `enriched_events` | pass-through | Extract flat columns from JSON for join keys |
| `prompt_lifecycle` | **stream join** | Prompt → API call (model, cost, latency) |
| `prompt_tools` | **stream join** | Prompt → tool execution (tool, success, duration) |
| `cost_by_model` | window (15s) | Cost, tokens, latency grouped by model |
| `tool_stats` | window (15s) | Tool invocations, success rate, latency |
| `tool_decisions` | window (15s) | Permission accept/reject by tool |
| `session_activity` | window (15s) | Event counts, cost velocity, speed mode |
| `prompt_analysis` | window (30s) | Per-prompt cost, model, tokens, tools, cache |
| `event_count` | window (5s) | Heartbeat |

## Persistence

Pipeline output is written to `./data/aiops/` as Parquet files (zstd compressed).
Survives server restarts. Query with DuckDB:

```bash
duckdb -c "SELECT model, SUM(total_cost_usd) FROM 'data/aiops/cost/*.parquet' GROUP BY model"
```

## Multi-User / Team

Multiple Claude Code instances can point at the same LaminarDB server. Events
include `user.account_uuid` and `user.email`, so cost is tracked per user.

## Claude Code Telemetry Reference

Verified against Claude Code v2.1.92.

| Event | Key Attributes |
|-------|----------------|
| `user_prompt` | `prompt.id`, `prompt_length` |
| `api_request` | `model`, `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_creation_tokens`, `cost_usd`, `duration_ms`, `speed` |
| `tool_decision` | `tool_name`, `decision`, `source` |
| `tool_result` | `tool_name`, `success`, `duration_ms`, `tool_result_size_bytes` |

All attributes are flat keys in the OTel `attributes` JSON column.

## Files

| File | Purpose |
|------|---------|
| `config.toml` | LaminarDB server config (9 pipelines + sinks) |
| `dashboard.html` | Single-file dashboard (no build step, no deps) |
| `pipelines.sql` | SQL reference (not loaded directly) |
