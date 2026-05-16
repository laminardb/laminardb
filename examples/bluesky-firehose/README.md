# Bluesky firehose → SUBSCRIBE over the Postgres wire protocol

LaminarDB speaks the Postgres wire protocol. Point any libpq client at it
and `SUBSCRIBE` to a live stream — no client library, no Kafka, no Docker.

This demo connects LaminarDB straight to the **public Bluesky Jetstream
firehose** (~30–60 posts/sec, no auth) and exposes it as a streaming
Postgres relation called `posts`. Three things to try:

1. **Live throughput** — `SUBSCRIBE posts` and watch the rate.
2. **Server-side filter** — `SUBSCRIBE posts WHERE text LIKE '%…%'`.
3. **Reconnect without a gap** — kill the consumer, reconnect with
   `AS OF EPOCH`, and recover everything you missed during the downtime.

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
├── laminar.toml     # WEBSOCKET source on Jetstream → retained `posts` stream + pgwire
├── run.ps1 / run.sh # start the server
├── watch.py         # tiny psycopg SUBSCRIBE client with a rolling rate line
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
python watch.py "AS OF EPOCH 2"
```

Why `2`? The replay anchor is the subscription-registry barrier epoch,
not the checkpoint id. With no windowed operators it stays at `2`
("everything retained"). If you pick an evicted epoch, LaminarDB tells
you the earliest one it still has:

```
epoch 0 for stream 'posts' is no longer retained (earliest retained is 2)
```

— subscribe with that number. The pruning error is the contract: a
consumer that falls outside the retention window is told so explicitly
rather than silently skipping data.

## How it works

`laminar.toml` is the whole pipeline — embedded SQL, run before the
server opens its ports:

- **`CREATE SOURCE bluesky_posts … FROM WEBSOCKET (…)`** — the connector
  dials the Jetstream `wss://` URL, auto-reconnects with backoff (Bluesky
  drops idle sockets every few minutes; the log shows the reconnect),
  and decodes each JSON frame. Jetstream events are deeply nested, so
  `json.column.<col> = 'commit.record.text'` pulls scalar fields out by
  dotted path. Frames that don't match (identity/account events) are
  skipped silently. The URL pins `?wantedCollections=app.bsky.feed.post`,
  so this is **posts only**, not the entire firehose — drop that query
  string for all collections (likes/follows/reposts: ~thousands/sec, much
  higher throughput, mostly NULL `text`). See the note in `laminar.toml`.
- **`CREATE STREAM posts AS SELECT … WITH ('retain_history' = '256mb')`**
  — a plain projection with a retained history buffer. `SUBSCRIBE`
  targets this; `WHERE` is compiled against its schema.

**No event-time watermark — on purpose.** `commit.record.createdAt` is
client-supplied; a single device with a wrong clock would jump an
event-time watermark far into the future and stall every downstream
operator (real-time events would all look "late"). This demo is about
the wire protocol, not windowing, so it runs on processing order and
never stalls. If you add windowed aggregation here, derive event time
from a trustworthy field and expect to filter clock outliers.

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
