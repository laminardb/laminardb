# Crypto price × Bluesky sentiment

A live monitoring pipeline: BTCUSDT trades from Binance and crypto-tagged posts
from the Bluesky firehose, scored and correlated **entirely inside LaminarDB**.
The dashboard draws what the views emit and computes nothing.

What the engine does, end to end, from one [`pipeline.toml`](pipeline.toml):

- **Two WebSocket sources**, processing-time (wall-clock windows, no event-time
  skew to tune): Binance `btcusdt@trade` and the Bluesky Jetstream.
- **`ai_sentiment(text) → DOUBLE`** scored inline on a stream by **FinBERT**, a
  finance-tuned BERT running locally (ONNX) on Ring 1 (never blocking the hot
  path), batched and deduped through the foyer cache, every call in `laminar.ai_calls`.
- **1-minute tumbling windows** on each side (price OHLC-ish; mean sentiment + post count).
- **An MV-to-MV join** on `bucket_start`, emitting as both minutes close.
- **A rolling `CORR(price, mean_sentiment)` over 30 buckets**, computed in-engine
  by carrying the cross-moments (the engine's sliding correlation) — not an
  average of per-bucket values, not a recompute from rolled-up bars.

This is an engineering demo. It shows the price series, the sentiment series, and
their rolling correlation side by side. **It makes no claim that sentiment
predicts price or vice versa** — the rolling correlation is shown for the viewer
to interpret. The point is that the windowing, the join, the scoring, and the
correlation all run in the stream engine, correctly, from one config file.

## Run it

Sentiment runs on a **local ONNX model** (FinBERT) — no API key, no inference-time
network call. ONNX Runtime is loaded dynamically, so you supply the shared library
(>= 1.24) via `ORT_DYLIB_PATH`; the FinBERT weights (~440 MB) download once from
the Hugging Face CDN into `./models`, then load from disk on restart. The labels
come from the model's own `config.json`, so the first cold-cache run scores
correctly — no pre-staging, no restart.

```sh
# onnxruntime >= 1.24 (ONNX Runtime release, or your package manager)
export ORT_DYLIB_PATH=/path/to/libonnxruntime.so   # onnxruntime.dll on Windows
laminardb --config pipeline.toml
```

Tail any view directly over pgwire:

```sh
psql -h 127.0.0.1 -p 5432
=> SUBSCRIBE sentiment_price_1m;     -- bucket_start, price, mean_sentiment, posts, corr_30
=> SUBSCRIBE bsky_crypto;            -- did, text, ts, sentiment
```

See the scoring cost/volume:

```sql
SELECT * FROM laminar.ai_calls ORDER BY timestamp_ms DESC;
```

### Dashboard

```sh
pip install "psycopg[binary]"
python dashboard/bridge.py            # SUBSCRIBEs to the views over pgwire, serves the page
# open http://127.0.0.1:8088/
```

`bridge.py` is pure transport: it `SUBSCRIBE`s to `sentiment_price_1m` and
`bsky_crypto` over the Postgres wire protocol and forwards each row to the
browser as a Server-Sent Event. It computes nothing — every number is the
engine's.

## The degradation demo 

Make the scorer fail mid-run — point `ORT_DYLIB_PATH` at a missing library, or
feed a model whose forward pass blows the 60 s inference deadline. The AI
operator emits a **null** score on terminal failure — it never panics and never
stalls Ring 0. `mean_sentiment` goes null for the affected minutes, the `corr_30`
readout blanks, **and the price line and the post feed keep flowing.**
`laminar.ai_calls` records the failures (`status = 'error'`). When the model is
reachable again, scoring resumes.

## Why the UI is trustworthy

The dashboard formats and draws; it does not compute. Grep it:

```sh
grep -niE 'window|tumble|group by|corr|average|aggregate|sentiment\s*=' dashboard/index.html
```

The only matches are labels and the `corr` *display* element — no windowing,
no correlation math, no scoring. Every number on the screen came off a view.
