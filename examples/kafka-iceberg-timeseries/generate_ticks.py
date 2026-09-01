#!/usr/bin/env python3
"""
Synthetic crypto trade-tick generator for the Kafka → Iceberg demo.

Produces JSON records of the shape:

    {
      "ts": "2026-05-10T12:00:00.123Z",
      "symbol": "BTC-USD",
      "price": 65432.10,
      "qty": 0.0125
    }

Three symbols (BTC-USD, ETH-USD, SOL-USD) follow independent additive
Gaussian random walks. Records are partition-keyed by symbol so that the
per-partition order matches per-symbol time order — this is what makes
FIRST_VALUE / LAST_VALUE produce a usable open / close in the OHLC
materialized view (see demo.sql and AUDIT.md §1.5).

Usage:
    pip install confluent-kafka
    python generate_ticks.py --rate 100 --duration 180

Args:
    --brokers   Kafka bootstrap servers   (default: localhost:9092)
    --topic     Topic name                (default: crypto.ticks)
    --rate      Aggregate ticks/second    (default: 50.0)
    --duration  Seconds to run            (default: 120.0)
    --seed      RNG seed                  (default: 42)
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


SYMBOLS = {
    "BTC-USD": {"price": 65000.0, "vol": 0.0008, "qty_max": 0.5},
    "ETH-USD": {"price": 3200.0,  "vol": 0.0010, "qty_max": 5.0},
    "SOL-USD": {"price": 145.0,   "vol": 0.0012, "qty_max": 80.0},
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--brokers",  default="localhost:9092")
    p.add_argument("--topic",    default="crypto.ticks")
    p.add_argument("--rate",     type=float, default=50.0)
    p.add_argument("--duration", type=float, default=120.0)
    p.add_argument("--seed",     type=int,   default=42)
    return p.parse_args()


def step(state: dict) -> tuple[float, float]:
    drift = random.gauss(0.0, state["vol"]) * state["price"]
    state["price"] = max(0.01, state["price"] + drift)
    qty = round(random.uniform(0.001, state["qty_max"]), 4)
    return round(state["price"], 2), qty


def iso_now_ms() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    producer = Producer({
        "bootstrap.servers": args.brokers,
        "linger.ms": 5,
        "batch.size": 16384,
        "enable.idempotence": True,
        "compression.type": "lz4",
    })

    interval = 1.0 / args.rate
    deadline = time.monotonic() + args.duration
    sent = 0
    symbols = list(SYMBOLS.keys())

    print(
        f"producing to {args.brokers} topic={args.topic} "
        f"rate={args.rate}/s duration={args.duration}s"
    )

    next_emit = time.monotonic()
    while time.monotonic() < deadline:
        symbol = random.choice(symbols)
        price, qty = step(SYMBOLS[symbol])
        record = {
            "ts": iso_now_ms(),
            "symbol": symbol,
            "price": price,
            "qty": qty,
        }
        producer.produce(
            args.topic,
            key=symbol.encode("utf-8"),
            value=json.dumps(record).encode("utf-8"),
        )
        sent += 1
        if sent % 1000 == 0:
            producer.poll(0)

        next_emit += interval
        sleep_for = next_emit - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            # Falling behind — let producer breathe and reset cadence.
            producer.poll(0)
            next_emit = time.monotonic()

    producer.flush(timeout=10.0)
    print(f"sent {sent} ticks to {args.topic}")


if __name__ == "__main__":
    main()
