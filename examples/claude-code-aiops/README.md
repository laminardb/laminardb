# Claude Code Cost Monitor

Real-time cost, token, and tool usage monitoring for Claude Code. Zero infrastructure.

## The Problem

You're spending real money on Claude Code but flying blind:
- How much did that last coding session cost?
- Which prompts burned the most tokens?
- Are you getting good cache hit rates or re-sending context?
- Is Opus 4 worth the cost premium over Sonnet?
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

## Quick Start (3 steps)

### 1. Start the server

```bash
cargo run --release -p laminar-server --no-default-features \
    --features mimalloc,otel,websocket,files -- \
    --config examples/claude-code-aiops/config.toml
```

### 2. Open the dashboard

```bash
# From the repo root:
python -m http.server 8000 -d examples/claude-code-aiops
# Open http://localhost:8000/dashboard.html
```

### 3. Point Claude Code at it

```bash
CLAUDE_CODE_ENABLE_TELEMETRY=1 \
OTEL_LOGS_EXPORTER=otlp \
OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_LOGS_EXPORT_INTERVAL=5000 \
claude
```

Use Claude Code normally. The dashboard updates in real time.

## What You See

| Panel | Updates | What It Shows |
|-------|---------|---------------|
| **Session Cost** | 15s | Running total with sparkline and $/min burn rate |
| **Token Breakdown** | 15s | Input/output/cache-read/cache-create + cache hit rate |
| **Cost by Model** | 15s | Per-model cost bars (Opus vs Sonnet vs Haiku) |
| **Tool Usage** | 15s | Per-tool call counts, success rates, latency |
| **Tool Permissions** | 15s | Accept/reject decisions, auto-approval % |
| **Activity Timeline** | 15s | API and tool call volume over time |
| **Prompt Lifecycle** | streaming | Stream join: prompt → api_request correlated by prompt.id |
| **Per-Prompt Breakdown** | 30s | Cost, model, tokens, tools, cache %, duration per prompt |
| **Stat Cards** | 15s | API reqs, tool calls, avg latency, cost/req, fast mode %, $/hr |
| **Event Log** | live | Scrolling feed of all pipeline output |

## How It Works

Claude Code emits separate OTel events for prompts, API calls, and tool results —
correlated by `prompt.id` but arriving as independent log records. LaminarDB
receives these via OTLP/gRPC, runs **stream-to-stream temporal joins** to
correlate prompt → api_request → tool_result in real-time, plus windowed
aggregations for cost and tool metrics. Results push to the dashboard over
WebSocket. All output is persisted to Parquet files for historical analysis.

### Hero Feature: Stream-to-Stream Temporal Join

```sql
SELECT p.prompt_id, a.model, a.cost_usd, a.duration_ms
FROM otel_events p
JOIN otel_events a
    ON p.prompt_id = a.prompt_id
    AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '2' MINUTE
WHERE p.event_name = 'user_prompt'
    AND a.event_name = 'api_request'
```

This reconstructs "what happened when the developer submitted this prompt?" by
joining the prompt event with its API response — as events flow in real-time.
Prometheus cannot do this. Grafana cannot do this. No AI observability platform
offers streaming SQL joins over raw telemetry.

### Pipelines

| Pipeline | Type | What It Computes |
|----------|------|------------------|
| `prompt_lifecycle` | **stream join** | Prompt → API call correlation (model, cost, latency) |
| `prompt_tools` | **stream join** | Prompt → tool execution correlation (tool, success, duration) |
| `event_count` | window | Heartbeat (5s event count) |
| `cost_by_model` | window | Cost, tokens, latency grouped by model (15s) |
| `tool_stats` | window | Tool invocations, success rate, latency (15s) |
| `tool_decisions` | window | Permission accept/reject by tool (15s) |
| `session_activity` | window | Event counts, cost velocity, speed mode (15s) |
| `prompt_analysis` | window | Per-prompt cost, model, tokens, tools, cache (30s) |

### Persistence

All pipeline output is written to `./data/aiops/` as Parquet files with zstd
compression. These survive server restarts — query with DuckDB or pandas.

## Claude Code Telemetry Reference

Verified against Claude Code v2.1.90 (2026-04-02).

| Event (`event.name`) | Key Attributes |
|----------------------|----------------|
| `user_prompt` | `prompt.id`, `prompt_length` |
| `api_request` | `model`, `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_creation_tokens`, `cost_usd`, `duration_ms`, `speed` |
| `tool_decision` | `tool_name`, `decision`, `source` |
| `tool_result` | `tool_name`, `success`, `duration_ms`, `tool_result_size_bytes` |

All attributes are flat keys in the `attributes` JSON column (no `gen_ai.*` namespace).
Cost is pre-calculated by Claude Code — no pricing table needed.

## Multi-User / Team Setup

Multiple Claude Code instances can point at the same LaminarDB server. Events
include `user.account_uuid` and `user.email`, so the cost_by_model pipeline
groups by account. Point your team's Claude Code instances at the same endpoint
to get a unified cost view.

## Files

| File | Purpose |
|------|---------|
| `config.toml` | LaminarDB server configuration (pipelines + sinks) |
| `dashboard.html` | Single-file live dashboard (no build step, no dependencies) |
| `pipelines.sql` | Standalone SQL reference (not loaded directly) |
