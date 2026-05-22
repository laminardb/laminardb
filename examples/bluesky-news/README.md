# Bluesky trending-hashtag detection

Real-time trending-hashtag and spike detection over the public
[Bluesky Jetstream](https://github.com/bluesky-social/jetstream) firehose —
end to end, in pure SQL, queried from Python over the Postgres wire protocol.

No connector code, no UDFs: the WebSocket source decodes nested Jetstream JSON
via `json.column.<col>` paths, and the analytics are plain materialized views.

## Pipeline (`laminar.toml`)

- **`bsky_jetstream_raw`** — `CREATE SOURCE … FROM WEBSOCKET` against Jetstream;
  nested fields (`commit.record.text`, …) are pulled out with `json.column.*`.
- **`bsky_posts`** — subscribable stream of English post-creates (the raw feed).
- **`bsky_hashtags_5s`** — `#hashtags` extracted from each post (tokenize +
  regexp), counted per 5-second window with `COUNT(DISTINCT author_did)` so one
  user spamming a tag can't dominate.
- **`bsky_hashtag_spikes`** — each 5s hashtag count divided by its 5-minute
  per-tag baseline via an `ASOF JOIN`. `spike_ratio > 5` flags a burst.

Windows are event-time, keyed on Jetstream's `time_us` (epoch microseconds,
decoded into a `TIMESTAMP` via `epoch_unit`). A bounded watermark (5s
out-of-orderness) advances from the live max event time, so windows close
continuously; backfill replayed on reconnect is older than the watermark and
is dropped as late (standard late-data handling).

## Run

```sh
# 1. Start the server against the live firehose (builds on first run).
cargo build --release -p laminar-server
./target/release/laminardb --config examples/bluesky-news/laminar.toml

# 2. In another shell, tail the trending hashtags and spikes.
pip install asyncpg
python examples/bluesky-news/news_spike.py
```

`news_spike.py` opens two `SUBSCRIBE` cursors over pgwire (`127.0.0.1:5432`):
the live top-10 hashtags per 5s bucket (`tag=uses/authors`, kept client-side
since `SUBSCRIBE` has no `ORDER BY`/`LIMIT`), and hashtags with
`spike_ratio > 5`. Ctrl-C to stop.

Trending hashtags appear within seconds. The spike baseline warms up over the
first 5 minutes; spikes get a ratio after that.

## What's tested

The engine constructs this demo relies on are covered by tests in
`crates/laminar-db` and `crates/laminar-connectors`: windowed aggregation over
`UNNEST` (`windowed_aggregate_over_lateral_unnest_emits`), `ASOF JOIN` in a
materialized view (`asof_join_in_materialized_view_emits_backward_match`),
windowed `COUNT(DISTINCT)` surviving a checkpoint
(`count_distinct_survives_midwindow_checkpoint`), event-time windows with a
`WATERMARK`, and `json.column` decoding of nested objects and string arrays
(`test_json_column_path_to_nested_string_array`, plus `epoch_unit`
numeric-timestamp decoding).
