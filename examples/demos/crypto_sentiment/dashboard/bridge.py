#!/usr/bin/env python3
"""Thin pgwire → browser bridge for the crypto-sentiment dashboard.

Browsers cannot speak the Postgres wire protocol, so this process tails the
LaminarDB views over pgwire and forwards each row to the page as a
Server-Sent Event. It forwards rows **verbatim** — it computes nothing. All
aggregation, windowing, correlation, and sentiment scoring already happened in
the engine; this is transport only.

    pip install "psycopg[binary]"
    python bridge.py            # tail the live LaminarDB views over pgwire

Then open http://127.0.0.1:8088/.
"""
import argparse
import json
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg

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


# ── pgwire feed ─────────────────────────────────────────────────────────────
def tail_view(dsn: str, sql: str, event: str, columns: list[str]) -> None:
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
            except OSError:
                # Client went away (browser tab closed, SSE reconnect, reload).
                # Covers BrokenPipe/ConnectionReset/ConnectionAborted (the last is
                # what Windows raises) — all expected, not worth a traceback.
                pass
            finally:
                with LOCK:
                    SUBSCRIBERS.discard(q)
        else:
            body = (HERE / "index.html").read_bytes()
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg-host", default="127.0.0.1")
    ap.add_argument("--pg-port", type=int, default=5432)
    ap.add_argument("--http-port", type=int, default=8088)
    args = ap.parse_args()

    run_pgwire(args.pg_host, args.pg_port)
    print(f"[bridge] dashboard on http://127.0.0.1:{args.http_port}/")
    ThreadingHTTPServer(("127.0.0.1", args.http_port), Handler).serve_forever()


if __name__ == "__main__":
    main()
