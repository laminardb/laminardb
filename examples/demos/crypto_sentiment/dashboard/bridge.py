#!/usr/bin/env python3
"""Thin pgwire → browser bridge for the crypto-sentiment dashboard.

Browsers cannot speak the Postgres wire protocol, so this process tails the
LaminarDB views over pgwire and forwards each row to the page as a
Server-Sent Event. It forwards rows **verbatim** — it computes nothing. All
aggregation, windowing, correlation, and sentiment scoring already happened in
the engine; this is transport only.

    pip install "psycopg[binary]"
    python bridge.py                      # tail the live server on :5432
    python bridge.py --simulate           # synthetic feed, for screen-recording
                                          #   (a recording aid; NOT the shipped path)

Then open http://127.0.0.1:8088/.
"""
import argparse
import json
import math
import queue
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
SUBSCRIBERS: "set[queue.Queue]" = set()
LOCK = threading.Lock()


def publish(event: str, payload: dict) -> None:
    """Fan a row out to every connected browser, unchanged."""
    # default=str renders the views' TIMESTAMP columns (Python datetimes) and any
    # Decimal as strings — json.dumps rejects both natively.
    msg = f"event: {event}\ndata: {json.dumps(payload, default=str)}\n\n"
    with LOCK:
        for q in list(SUBSCRIBERS):
            q.put(msg)


# ── pgwire feed (the shipped path) ─────────────────────────────────────────
def tail_view(dsn: str, sql: str, event: str, columns: list[str]) -> None:
    import psycopg  # imported lazily so --simulate needs no driver

    while True:
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    # SUBSCRIBE never returns; stream() yields rows as the view
                    # emits them (the server flushes per batch).
                    for row in cur.stream(sql):
                        publish(event, dict(zip(columns, row)))
        except Exception as exc:  # reconnect on drop; the feed is at-most-once
            print(f"[bridge] {event} reconnect after: {exc}")
            time.sleep(2)


def run_pgwire(host: str, port: int) -> None:
    dsn = f"host={host} port={port} dbname=laminar user=laminar"
    feeds = [
        # Column order matches each view's SELECT list.
        ("SUBSCRIBE sentiment_price_1m", "bar",
         ["bucket_start", "price", "mean_sentiment", "posts", "corr_30"]),
        ("SUBSCRIBE bsky_crypto", "post", ["did", "text", "ts", "sentiment"]),
    ]
    for sql, event, cols in feeds:
        threading.Thread(target=tail_view, args=(dsn, sql, event, cols), daemon=True).start()


# ── synthetic feed (recording aid only) ────────────────────────────────────
def run_simulate() -> None:
    print("[bridge] SIMULATED feed — recording aid, not the shipped demo path")

    def loop() -> None:
        t, price, posts = 0, 64000.0, [
            "BTC breaking out, this looks strong", "another red day for bitcoin, brutal",
            "ethereum merge talk heating up", "$btc just chopping sideways tbh",
            "loaded more $eth, conviction high", "bitcoin dominance creeping up again",
        ]
        while True:
            t += 1
            price += random.uniform(-120, 120)
            sentiment = max(-1.0, min(1.0, 0.4 * math.sin(t / 6) + random.uniform(-0.3, 0.3)))
            corr = max(-1.0, min(1.0, math.sin(t / 9)))
            publish("bar", {
                "bucket_start": time.strftime("%H:%M:00"), "price": round(price, 2),
                "mean_sentiment": round(sentiment, 3), "posts": random.randint(3, 25),
                "corr_30": round(corr, 3),
            })
            for _ in range(random.randint(1, 3)):
                publish("post", {"did": "did:plc:demo",
                                 "text": random.choice(posts),
                                 "sentiment": round(max(-1, min(1, sentiment + random.uniform(-0.4, 0.4))), 2)})
            time.sleep(2)

    threading.Thread(target=loop, daemon=True).start()


# ── SSE + static server ─────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):  # quiet
        pass

    def do_GET(self):
        if self.path.startswith("/events"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            q: queue.Queue = queue.Queue()
            with LOCK:
                SUBSCRIBERS.add(q)
            try:
                while True:
                    self.wfile.write(q.get().encode())
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with LOCK:
                    SUBSCRIBERS.discard(q)
        else:
            body = (HERE / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--simulate", action="store_true", help="synthetic feed (recording aid)")
    ap.add_argument("--pg-host", default="127.0.0.1")
    ap.add_argument("--pg-port", type=int, default=5432)
    ap.add_argument("--http-port", type=int, default=8088)
    args = ap.parse_args()

    if args.simulate:
        run_simulate()
    else:
        run_pgwire(args.pg_host, args.pg_port)

    print(f"[bridge] dashboard on http://127.0.0.1:{args.http_port}/")
    ThreadingHTTPServer(("127.0.0.1", args.http_port), Handler).serve_forever()


if __name__ == "__main__":
    main()
