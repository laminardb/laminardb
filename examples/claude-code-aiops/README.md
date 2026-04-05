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
| **Per-Prompt Breakdown** | 30s | Cost, model, tokens, tools, cache %, duration per prompt |
| **Stat Cards** | 15s | API reqs, tool calls, avg latency, cost/req, fast mode %, $/hr |
| **Event Log** | live | Scrolling feed of all pipeline output |

## How It Works

Claude Code emits [OpenTelemetry log events](https://opentelemetry.io/docs/specs/otel/logs/)
for every prompt, API call, tool decision, and tool result. These events contain
token counts, costs, latencies, and tool metadata.

LaminarDB receives these events via OTLP/gRPC, runs 5 streaming SQL pipelines
with tumbling windows, and pushes aggregated results to the dashboard over
WebSocket. All pipeline output is also persisted to local JSON files in
`./data/aiops/` for historical analysis.

### Pipelines

| Pipeline | Window | What It Computes |
|----------|--------|------------------|
| `event_count` | 5s | Heartbeat (event count) |
| `cost_by_model` | 15s | Cost, tokens, latency grouped by model |
| `tool_stats` | 15s | Tool invocations, success rate, latency |
| `tool_decisions` | 15s | Permission accept/reject by tool and source |
| `session_activity` | 15s | Event counts, cost velocity, speed mode |
| `prompt_analysis` | 30s | Per-prompt cost, model, tokens, tools, cache |

### Persistence

All pipeline output is written to `./data/aiops/` as append-mode JSON files.
These survive server restarts — you get historical data without any database.

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
