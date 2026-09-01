# Bluesky trending hashtags

Real-time hashtag counts over the public
[Bluesky Jetstream](https://github.com/bluesky-social/jetstream) firehose, queried from Python over
the Postgres wire protocol.

The pipeline in `laminar.toml` creates:

- `bsky_jetstream_raw`, a WebSocket source that decodes nested Jetstream JSON;
- `bsky_posts`, a subscribable stream of English post creates;
- `bsky_hashtags_5s`, an event-time materialized view with five-second hashtag counts and distinct
  author counts.

The source watermark allows five seconds of out-of-order data. The public WebSocket feed is not
replayable after process failure, so this example explicitly uses `best_effort` delivery.

## Run

```sh
cargo build --release -p laminar-server
./target/release/laminardb --config examples/bluesky-news/laminar.toml

pip install asyncpg
python examples/bluesky-news/news_trending.py
```

`news_trending.py` subscribes to `bsky_hashtags_5s` on `127.0.0.1:5432` and maintains a bounded
top-ten list for the current window. Press Ctrl-C to stop.
