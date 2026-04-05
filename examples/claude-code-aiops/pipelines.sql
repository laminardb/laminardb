-- =============================================================================
-- LaminarDB AI Ops Demo: Streaming SQL Pipelines
-- =============================================================================
-- Verified against Claude Code v2.1.90 telemetry (2026-04-02).
--
-- Event types (event.name attribute):
--   user_prompt  — prompt.id, prompt_length
--   api_request  — model, input_tokens, output_tokens, cache_read_tokens,
--                  cache_creation_tokens, cost_usd, duration_ms, speed
--   tool_decision — tool_name, decision, source
--   tool_result   — tool_name, success, duration_ms, tool_result_size_bytes
--
-- All attributes are flat keys in the `attributes` JSON column.
-- Extract via: jsonb_get_text(from_json(attributes), 'key')
--
-- Use GROUP BY TUMBLE(col, interval) syntax — the planner only extracts
-- window config from the GROUP BY form.
-- =============================================================================

-- Pipeline 1: Cost per model (1-minute windows)
-- Uses pre-calculated cost_usd from Claude Code (no pricing lookup needed).
CREATE STREAM cost_by_model AS
SELECT
    jsonb_get_text(from_json(attributes), 'model') AS model,
    jsonb_get_text(from_json(attributes), 'user.account_uuid') AS account_uuid,
    COUNT(*) AS request_count,
    SUM(CAST(jsonb_get_text(from_json(attributes), 'input_tokens') AS BIGINT)) AS total_input_tokens,
    SUM(CAST(jsonb_get_text(from_json(attributes), 'output_tokens') AS BIGINT)) AS total_output_tokens,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'cache_read_tokens') IS NOT NULL
        THEN CAST(jsonb_get_text(from_json(attributes), 'cache_read_tokens') AS BIGINT) ELSE 0 END) AS total_cache_read_tokens,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'cache_creation_tokens') IS NOT NULL
        THEN CAST(jsonb_get_text(from_json(attributes), 'cache_creation_tokens') AS BIGINT) ELSE 0 END) AS total_cache_creation_tokens,
    SUM(CAST(jsonb_get_text(from_json(attributes), 'cost_usd') AS DOUBLE)) AS total_cost_usd,
    AVG(CAST(jsonb_get_text(from_json(attributes), 'duration_ms') AS DOUBLE)) AS avg_duration_ms
FROM otel_events
WHERE jsonb_get_text(from_json(attributes), 'event.name') = 'api_request'
GROUP BY
    TUMBLE(_laminar_received_at, INTERVAL '60' SECOND),
    jsonb_get_text(from_json(attributes), 'model'),
    jsonb_get_text(from_json(attributes), 'user.account_uuid')
EMIT ON WINDOW CLOSE;

-- Pipeline 2: Tool effectiveness (1-minute windows)
-- Uses tool_result events (tool_decision has no duration/success data).
CREATE STREAM tool_stats AS
SELECT
    jsonb_get_text(from_json(attributes), 'tool_name') AS tool_name,
    COUNT(*) AS invocation_count,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'success') = 'true' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'success') = 'false' THEN 1 ELSE 0 END) AS failure_count,
    AVG(CAST(jsonb_get_text(from_json(attributes), 'duration_ms') AS DOUBLE)) AS avg_duration_ms,
    MAX(CAST(jsonb_get_text(from_json(attributes), 'duration_ms') AS DOUBLE)) AS max_duration_ms,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'tool_result_size_bytes') IS NOT NULL
        THEN CAST(jsonb_get_text(from_json(attributes), 'tool_result_size_bytes') AS BIGINT) ELSE 0 END) AS total_result_bytes
FROM otel_events
WHERE jsonb_get_text(from_json(attributes), 'event.name') = 'tool_result'
GROUP BY
    TUMBLE(_laminar_received_at, INTERVAL '60' SECOND),
    jsonb_get_text(from_json(attributes), 'tool_name')
EMIT ON WINDOW CLOSE;

-- Pipeline 3: Per-prompt breakdown (30s windows)
-- Correlates user_prompt, api_request, and tool_result events by prompt.id.
CREATE STREAM prompt_analysis AS
SELECT
    jsonb_get_text(from_json(attributes), 'prompt.id') AS prompt_id,
    MAX(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'user_prompt'
        THEN CAST(jsonb_get_text(from_json(attributes), 'prompt_length') AS BIGINT) END) AS prompt_length,
    MAX(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'api_request'
        THEN jsonb_get_text(from_json(attributes), 'model') END) AS model,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'api_request'
        THEN CAST(jsonb_get_text(from_json(attributes), 'cost_usd') AS DOUBLE)
        ELSE 0.0 END) AS prompt_cost_usd,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'api_request'
        THEN CAST(jsonb_get_text(from_json(attributes), 'input_tokens') AS BIGINT)
        ELSE 0 END) AS total_input_tokens,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'api_request'
        THEN CAST(jsonb_get_text(from_json(attributes), 'output_tokens') AS BIGINT)
        ELSE 0 END) AS total_output_tokens,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'api_request'
        THEN CASE WHEN jsonb_get_text(from_json(attributes), 'cache_read_tokens') IS NOT NULL
            THEN CAST(jsonb_get_text(from_json(attributes), 'cache_read_tokens') AS BIGINT) ELSE 0 END
        ELSE 0 END) AS total_cache_read_tokens,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'api_request'
        THEN CAST(jsonb_get_text(from_json(attributes), 'duration_ms') AS BIGINT)
        ELSE 0 END) AS total_duration_ms,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'tool_result'
        THEN 1 ELSE 0 END) AS tools_invoked,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'tool_result'
        AND jsonb_get_text(from_json(attributes), 'success') = 'false'
        THEN 1 ELSE 0 END) AS tool_failures,
    SUM(CASE WHEN jsonb_get_text(from_json(attributes), 'event.name') = 'api_request'
        THEN 1 ELSE 0 END) AS api_calls
FROM otel_events
WHERE jsonb_get_text(from_json(attributes), 'prompt.id') IS NOT NULL
GROUP BY
    TUMBLE(_laminar_received_at, INTERVAL '30' SECOND),
    jsonb_get_text(from_json(attributes), 'prompt.id')
EMIT ON WINDOW CLOSE;
