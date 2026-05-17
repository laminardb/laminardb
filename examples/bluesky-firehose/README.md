# Bluesky firehose → SUBSCRIBE over the Postgres wire protocol

LaminarDB speaks the Postgres wire protocol. Point any libpq client at it
and `SUBSCRIBE` to a live stream — no client library, no Kafka, no Docker.

This demo connects LaminarDB straight to the **public Bluesky Jetstream
firehose** (~30–60 posts/sec, no auth) and exposes two streaming Postgres
relations: `posts` (the raw stream) and `posts_per_min` (a 1-minute
windowed count over event time). Four things to try:

1. **Live throughput** — `SUBSCRIBE posts` and watch the rate.
2. **Server-side filter** — `SUBSCRIBE posts WHERE text LIKE '%…%'`.
3. **Reconnect without a gap** — kill the consumer, reconnect with
   `AS OF EPOCH`, and recover everything you missed during the downtime.
4. **Windowed aggregation over event time** — `SUBSCRIBE posts_per_min`
   for one row per minute, on Jetstream's trustworthy server clock.

## Prerequisites

- A LaminarDB server binary. `run.ps1` / `run.sh` build it if absent
  (Rust toolchain per `rust-toolchain.toml`), or reuse `target/release/laminardb`.
- A Postgres client — **either**:
  - `psql` (`winget install PostgreSQL.PostgreSQL` / `brew install libpq` /
    `apt install postgresql-client`), **or**
  - Python 3.10+ with `pip install "psycopg[binary]"` and the bundled
    `watch.py` (zero-install alternative; UTF-8 safe on Windows consoles).

No Docker. No external services. The firehose is public.

## Files

```
examples/bluesky-firehose/
├── laminar.toml     # 2 WEBSOCKET sources → `posts` stream + `posts_per_min` MV, over pgwire
├── run.ps1 / run.sh # start the server
├── watch.py         # psycopg SUBSCRIBE client (--from <relation>) with a rolling rate line
└── README.md        # you are here
```

## Run

```sh
# Windows
.\run.ps1
# Linux / macOS
./run.sh
```

The server connects to Jetstream and starts ingesting immediately. It
listens for Postgres clients on `127.0.0.1:5432` and serves an HTTP
control/metrics API on `127.0.0.1:7777`. Leave it running; open another
terminal for the client.

> Auth is **trust** on loopback (the default), so any `user=` works and
> no password is needed. See `crates/laminar-server/README.md` for MD5,
> TLS, and mTLS once you bind to a routable address.

## 1 — Live throughput

```sh
psql "host=127.0.0.1 port=5432 dbname=laminardb user=demo" -c "SUBSCRIBE posts"
```

or, without psql:

```sh
python watch.py
```

Rows stream as fast as Bluesky produces them. `watch.py` prints a
`--- N posts, R/sec ---` line every 2s — that rate *is* the end-to-end
ingest → decode → wire-protocol throughput, measured at the client.
Columns: `did, operation, collection, text, created_at`. `delete`
operations carry no record, so their `text`/`created_at` are NULL.

## 2 — Server-side WHERE

The predicate is compiled with DataFusion against the stream's schema
and evaluated **inside the server** — only matching rows hit the wire.

```sh
psql "host=127.0.0.1 port=5432 dbname=laminardb user=demo" \
  -c "SUBSCRIBE posts WHERE text LIKE '%bluesky%'"

# only post creations, skip deletes/tombstones
python watch.py "WHERE operation = 'create'"
```

## 3 — Reconnect without a gap

`posts` is declared `WITH ('retain_history' = '256mb')`, so LaminarDB
keeps a rolling window of recent history. A plain `SUBSCRIBE` is
tail-only — reconnect after a disconnect and everything emitted while
you were gone is lost. `SUBSCRIBE … AS OF EPOCH n` instead replays
everything retained after barrier `n`, then continues live.

```sh
# Terminal A — live tail. Note the timestamps, then Ctrl-C (consumer "crash").
python watch.py

# ...wait 20–30s. Posts keep flowing into LaminarDB; nobody is consuming...

# Reconnect tail-only: the gap is gone — first row is "now", you lost the window.
python watch.py

# Reconnect with replay: history streams back instantly (oldest first),
# including everything during the downtime, then it continues live.
python watch.py "AS OF EPOCH 0"
```

The replay anchor is the subscription-registry barrier epoch, not the
checkpoint id, and it is **not a fixed number** — the windowed
`posts_per_min` MV advances it as windows close. So don't hardcode it:
ask for `AS OF EPOCH 0` and LaminarDB tells you the earliest epoch it
still retains —

```
epoch 0 for stream 'posts' is no longer retained (earliest retained is 7)
```

— then subscribe with that number (`python watch.py "AS OF EPOCH 7"`).
The pruning error is the contract: a consumer that falls outside the
retention window is told so explicitly rather than silently skipping
data.

## 4 — Windowed aggregation over event time

`posts_per_min` is a 1-minute tumbling `COUNT(*)` over **event time** —
not wall-clock, and deliberately not the client's `createdAt` either.

```sh
psql "host=127.0.0.1 port=5432 dbname=laminardb user=demo" -c "SUBSCRIBE posts_per_min"

# or, without psql:
python watch.py --from posts_per_min
```

One final row per minute (`window_start, window_end, posts`), emitted
when the watermark crosses the window end (`EMIT ON WINDOW CLOSE`). First
row ~1–1.5 min in — that lag is the watermark being honest, not a stall.

**The lesson: window on a clock you trust.** `commit.record.createdAt`
is a device clock; a steady fraction of the public firehose runs minutes
off, which drags an event-time watermark ahead until normal posts look
"late" and get dropped. So `posts_per_min` uses Jetstream's
server-stamped `time_us` instead (trustworthy, ~wall-clock, monotone),
which makes a tight 15s bound safe. `time_us` is microseconds; the
decoder defaults numeric timestamps to millis, so
`'json.column.evt_us.epoch_unit' = 'micros'` scales it.

## How it works

`laminar.toml` is the whole pipeline — embedded SQL, run before the
server opens its ports:

- **`CREATE SOURCE bluesky_posts … FROM WEBSOCKET (…)`** — dials the
  `wss://` URL (auto-reconnect with backoff), decodes each nested JSON
  frame via `json.column.<col>` dotted paths, skips non-matching frames.
- **`CREATE STREAM posts AS … WITH ('retain_history' = '256mb')`** — a
  plain projection, **no watermark**; `SUBSCRIBE posts` targets it.
- **`CREATE SOURCE bluesky_posts_wm … WATERMARK FOR evt_us …`** — a
  *separate* connection on server `time_us` (see §4). Separate because a
  source `WATERMARK` late-drops rows for *every* consumer, and
  `WATERMARK` is `CREATE SOURCE`-only — so sharing it with `posts` would
  silently thin scenario 1. The extra connection costs ~30–60 rows/s.
- **`CREATE MATERIALIZED VIEW posts_per_min AS … EMIT ON WINDOW
  CLOSE`** — the 1-minute count, SUBSCRIBE-able over the wire.

`posts_per_min` emits ~one tiny row/minute. pgwire `feed()`s `DataRow`s
into an ~8 KB buffer and only flushes at end-of-response (never, for an
unbounded SUBSCRIBE) or when that buffer fills — so a sparse stream
would stall undelivered. LaminarDB drives the SUBSCRIBE sink itself and
flushes per batch, so each row arrives as its window closes.

## Notes

- The WebSocket source can't replay on recovery, so delivery is
  **at-most-once** for this source (logged at startup). Exactly-once
  applies to replayable sources (Kafka, etc.).
- `GET http://127.0.0.1:7777/metrics` exposes Prometheus counters —
  `laminardb_events_ingested_total` vs `_emitted_total` is the
  server-side view of the same throughput `watch.py` measures.
- `SUBSCRIBE` is also reachable via SQL-level cursors
  (`DECLARE … CURSOR FOR SUBSCRIBE …` + `FETCH`) and the extended-query
  binary format — see `crates/laminar-server/README.md`.
