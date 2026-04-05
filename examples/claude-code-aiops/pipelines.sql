-- LaminarDB AI Ops Demo — standalone SQL reference (see README.md for details).
-- Use GROUP BY TUMBLE(col, interval) syntax for windowed aggregation.

-- Cost per model
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

-- Tool effectiveness
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

-- Per-prompt breakdown
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

-- Stream joins — pre-extract flat columns, then join on simple refs

-- Enriched events — extract flat columns + convert nanos→millis for join bounds
CREATE STREAM enriched_events AS
SELECT
    _laminar_received_at / 1000000 AS ts_ms,
    jsonb_get_text(from_json(attributes), 'prompt.id') AS prompt_id,
    jsonb_get_text(from_json(attributes), 'event.name') AS event_name,
    jsonb_get_text(from_json(attributes), 'model') AS model,
    jsonb_get_text(from_json(attributes), 'prompt_length') AS prompt_length,
    jsonb_get_text(from_json(attributes), 'input_tokens') AS input_tokens,
    jsonb_get_text(from_json(attributes), 'output_tokens') AS output_tokens,
    jsonb_get_text(from_json(attributes), 'cost_usd') AS cost_usd,
    jsonb_get_text(from_json(attributes), 'duration_ms') AS duration_ms,
    jsonb_get_text(from_json(attributes), 'tool_name') AS tool_name,
    jsonb_get_text(from_json(attributes), 'success') AS tool_success
FROM otel_events
WHERE jsonb_get_text(from_json(attributes), 'prompt.id') IS NOT NULL;

-- Prompt → API call join (2min window, ts_ms is milliseconds)
CREATE STREAM prompt_lifecycle AS
SELECT
    p.prompt_id,
    CAST(p.prompt_length AS BIGINT) AS prompt_length,
    a.model,
    CAST(a.input_tokens AS BIGINT) AS input_tokens,
    CAST(a.output_tokens AS BIGINT) AS output_tokens,
    CAST(a.cost_usd AS DOUBLE) AS cost_usd,
    CAST(a.duration_ms AS BIGINT) AS api_duration_ms,
    a.ts_ms - p.ts_ms AS time_to_response_ms
FROM enriched_events p
JOIN enriched_events a
    ON p.prompt_id = a.prompt_id
    AND a.ts_ms BETWEEN p.ts_ms AND p.ts_ms + INTERVAL '2' MINUTE
WHERE p.event_name = 'user_prompt'
    AND a.event_name = 'api_request';

-- Prompt → tool result join (5min window)
CREATE STREAM prompt_tools AS
SELECT
    p.prompt_id,
    t.tool_name AS tool_name,
    t.tool_success AS tool_success,
    CAST(t.duration_ms AS BIGINT) AS tool_duration_ms,
    t.ts_ms - p.ts_ms AS prompt_to_tool_ms
FROM enriched_events p
JOIN enriched_events t
    ON p.prompt_id = t.prompt_id
    AND t.ts_ms BETWEEN p.ts_ms AND p.ts_ms + INTERVAL '5' MINUTE
WHERE p.event_name = 'user_prompt'
    AND t.event_name = 'tool_result';
