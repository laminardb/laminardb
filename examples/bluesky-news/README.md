# Bluesky news-event detection & cashtag tracking

Real-time news-spike detection and equity-cashtag tracking over the public
[Bluesky Jetstream](https://github.com/bluesky-social/jetstream) firehose —
end to end, in pure SQL, queried from Python over the Postgres wire protocol.

No connector code, no UDFs: the WebSocket source decodes nested Jetstream JSON
via `json.column.<col>` paths, and the analytics are plain materialized views.

## Pipeline (`laminar.toml`)

- **`bsky_jetstream_raw`** — `CREATE SOURCE … FROM WEBSOCKET` against Jetstream;
  nested fields (`commit.record.text`, …) are pulled out with `json.column.*`.
- **`bsky_posts`** — subscribable stream of English post-creates (the raw feed).
- **`bsky_keyword_spikes`** — 5s term counts over a macro lexicon
  (`powell`, `fed`, `cpi`, …), divided by a 5-minute per-term baseline via an
  `ASOF JOIN`. `spike_ratio > 5` flags a burst.
- **`bsky_cashtags_1m`** — `$TICKER` tokens extracted from text (tokenize +
  regexp), counted per 1-minute window with `COUNT(DISTINCT author_did)`.

Windows tumble on `proctime()` (processing time) — deliberately no event-time
watermark, since Jetstream replays backfill on reconnect and would stall one.

## Run

```sh
# 1. Start the server against the live firehose (builds on first run).
cargo build --release -p laminar-server
./target/release/laminardb --config examples/bluesky-news/laminar.toml

# 2. In another shell, tail the spikes and cashtags.
pip install asyncpg
python examples/bluesky-news/news_spike.py
```

`news_spike.py` opens two `SUBSCRIBE` cursors over pgwire (`127.0.0.1:5432`):
spikes with `spike_ratio > 5`, and cashtags (top-10 per bucket kept client-side,
since `SUBSCRIBE` has no `ORDER BY`/`LIMIT`). Ctrl-C to stop.

The keyword baseline warms up over the first 5 minutes; spikes get a ratio
after that. Cashtag and spike-term volume depends on what's trending.

## What's tested

The engine constructs this demo relies on are covered by tests in
`crates/laminar-db` and `crates/laminar-connectors`: windowed aggregation over
`UNNEST`, `ASOF JOIN` across materialized views, windowed `COUNT(DISTINCT)`,
`proctime()` windows, and `json.column` decoding of nested objects and string
arrays. `keyword_spike_pipeline_detects_injected_spike` exercises the full
spike pipeline against an injected synthetic spike.
