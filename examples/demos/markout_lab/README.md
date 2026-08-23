# LaminarDB Live Markout Lab

> **Prices are live public Binance BTCUSDT market data. Visitor fills are simulated, never sent to an exchange, and all PnL is hypothetical. If the exchange feed cannot connect, stalls, or disconnects, the demo terminates. There is no generated-data fallback.**

The Markout Lab turns a market-microstructure idea into a 30-second experiment anyone can try: choose a simulated BUY or SELL at the live touch, then watch whether the fill still looks favourable after 0, 1, 5, 15, and 30 seconds.

LaminarDB ingests the live quotes, binds the visitor action to event time, performs the temporal probes, and calculates the markout curve and scorecard in SQL. The browser presents those engine-emitted results; it does not calculate the financial metrics.

This is an embedded-mode product demonstration, not a trading system.

## Run it

From the repository root:

```bash
cargo run \
  --manifest-path examples/demos/markout_lab/Cargo.toml \
  --release
```

Open <http://127.0.0.1:8088>. No exchange credentials are needed, but the process needs outbound internet access to Binance's public market-data WebSocket.

The live-only options are:

```text
--host 127.0.0.1
--port 8088
--feed-url wss://data-stream.binance.vision/ws/btcusdt@ticker
--feed-start-timeout-secs 15
--feed-stale-after-secs 5
--maker-fee-bps 0.0
```

`--feed-url` also lets the integration suite supply an explicit Binance-shaped test endpoint. It is not a runtime fallback: one configured endpoint is used, and loss of that endpoint is fatal.

Docker Compose uses the repository as its build context:

```bash
docker compose \
  -f examples/demos/markout_lab/docker-compose.yml \
  up --build
```

The container binds to `0.0.0.0:8088` and also requires outbound HTTPS/WebSocket connectivity. It is deliberately not configured to restart into substitute data when the feed is lost.

## What people can do

The dashboard is scoped per browser session, so several visitors can use the same running demo without seeing one another's fills.

1. Watch the current BTC/USDT midpoint, bid, ask, and spread arrive from Binance.
2. Choose BUY or SELL and a small BTC amount. The backend—not the browser—binds the action to the latest fresh quote.
3. See the simulated fill immediately.
4. Watch each future checkpoint arrive progressively from LaminarDB.
5. Compare individual choices, the weighted markout curve, and the engine-produced scorecard.

The touch model simulates a passive BUY at the current bid or a passive SELL at the current ask. It does not model queue position, exchange matching, latency, partial fills, or actual execution probability.

## Data and engine path

```text
Binance BTCUSDT public WebSocket ──► validated Arrow quote batches ──┐
                                                                    │
browser BUY / SELL ──► bounded backend ──► simulated fill batches ──┤
                                                                    ▼
                                                         embedded LaminarDB
                                                                    │
                                          temporal probes + SQL economics
                                                                    │
                                           typed subscriptions + HTTP/SSE
                                                                    │
                                                                    ▼
                                                          browser presentation
```

[`pipeline.sql`](pipeline.sql) is compiled into the binary, executed by the embedded engine, and returned after endpoint/timeout configuration by `/api/pipeline`. The Rust bridge keeps a bounded reconnect snapshot and relays typed engine rows. JavaScript formats values and draws SVG coordinates only.

The quote connector is intentionally fail-closed:

- startup fails when the configured WebSocket cannot connect;
- malformed, crossed, wrong-symbol, or non-advancing ticker data faults the source;
- a read timeout or disconnect faults the source;
- the application observes an unavailable/faulted feed, shuts down LaminarDB, and exits with an error;
- it does not reconnect, replay, or generate substitute prices.

This demo applies to **embedded mode**. It adds no single-node-server or cluster admission surface.

## What LaminarDB calculates

All signs use the simulated liquidity-provider perspective:

```text
side_sign = +1 for BUY
side_sign = -1 for SELL
future_mid = (reference_bid + reference_ask) / 2
```

The SQL calculates:

```text
spread_capture_bps = side_sign × (mid_at_fill - fill_px) / fill_px × 10,000
post_fill_drift_bps = side_sign × (future_mid - mid_at_fill) / fill_px × 10,000
gross_markout_bps  = side_sign × (future_mid - fill_px) / fill_px × 10,000
gross_markout_pnl  = side_sign × quantity × (future_mid - fill_px)
fee_pnl            = -fee_bps × quantity × fill_px / 10,000
net_markout_pnl    = gross_markout_pnl + fee_pnl
```

Positive markout is favourable for the simulated side. A BUY followed by a higher midpoint is positive; a SELL followed by a lower midpoint is positive. Missing references remain nullable with `covered = false`; they are never presented as zero.

Aggregate curves are notional-weighted:

```text
weighted_net_markout_bps
  = 10,000 × SUM(net_markout_pnl) / SUM(quantity × fill_px)
```

## Event time

The Binance ticker's exchange event timestamp is the quote event time. A simulated fill receives the timestamp and sequence of the latest engine-emitted quote. LaminarDB emits a horizon only when the quote frontier passes that fill's 0, 1, 5, 15, or 30-second probe time, so pending results become ready progressively in real time.

The public feed is ephemeral and this demo has no checkpoint resume contract. Ordered source positions exist only to make the live temporal relationship explicit within the running process.

## Test and review

```bash
cargo fmt \
  --manifest-path examples/demos/markout_lab/Cargo.toml \
  -- --check

cargo clippy \
  --manifest-path examples/demos/markout_lab/Cargo.toml \
  --all-targets \
  -- -D warnings

cargo test \
  --manifest-path examples/demos/markout_lab/Cargo.toml \
  --all-targets

bash examples/demos/markout_lab/scripts/smoke.sh
```

The integration suite runs real embedded LaminarDB instances against an explicit local Binance-shaped WebSocket fixture. It asserts progressive horizons, sign convention, decomposition, nullable missing references, aggregate output, and fatal application shutdown after feed loss. The fixture is test infrastructure and is never selected by the demo at runtime.

The smoke script uses the default live Binance feed, creates a browser-scoped simulated fill, and waits for all five real-time horizons.

## Truthful limits

The UI persistently labels `LIVE BINANCE DATA`, `SIMULATED ORDERS`, `NO MONEY AT RISK`, and `ENGINE-CALCULATED RESULTS`. It makes no claim of real exchange execution, realised profit, queue accuracy, matching-engine fidelity, production readiness, or measured latency/throughput. It is hypothetical analysis, not financial advice.
