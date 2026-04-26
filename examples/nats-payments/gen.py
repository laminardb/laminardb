#!/usr/bin/env python3
"""
Multi-producer NATS publisher. Pace-limited to a target rate.

    pip install nats-py
    python gen.py                 # default: 10_000 msg/s, 4 producers
    python gen.py --rate 50000    # 50K/s
    python gen.py --rate 0        # flat-out, no pacing

Stop with Ctrl-C; prints sent count, elapsed, achieved rate.
"""

import argparse
import asyncio
import json
import random
import time
from datetime import datetime, timezone

import nats


NATS_URL = "nats://localhost:4222"
SUBJECT = "payments.events"

REGIONS = ["us-east", "us-west", "eu-west", "ap-south"]
METHODS = ["card", "bank_transfer", "crypto", "wallet"]
STATUSES = ["completed"] * 17 + ["failed"] * 2 + ["pending"] * 1


def make_event() -> bytes:
    return json.dumps(
        {
            "payment_id": f"pay-{random.randint(1_000_000, 9_999_999)}",
            "region":     random.choice(REGIONS),
            "method":     random.choice(METHODS),
            "amount_usd": round(random.uniform(5.0, 2000.0), 2),
            "status":     random.choice(STATUSES),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }
    ).encode()


async def producer(idx: int, target_per_sec: float, sent: list[int], stop: asyncio.Event):
    nc = await nats.connect(NATS_URL, pending_size=64 * 1024 * 1024)
    batch = 200
    start = time.monotonic()
    local = 0
    try:
        while not stop.is_set():
            for _ in range(batch):
                await nc.publish(SUBJECT, make_event())
            local += batch
            sent[idx] = local

            if target_per_sec > 0:
                expected = local / target_per_sec
                elapsed = time.monotonic() - start
                if expected > elapsed:
                    await asyncio.sleep(expected - elapsed)
    finally:
        await nc.flush()
        await nc.drain()


async def main(rate: int, producers: int):
    per_producer = rate / producers if rate > 0 else 0
    sent = [0] * producers
    stop = asyncio.Event()

    print(
        f"Publishing to {SUBJECT} — {producers} producers, "
        f"target {rate:,}/s ({per_producer:,.0f}/s each)"
        if rate > 0
        else f"Publishing to {SUBJECT} — {producers} producers, flat-out"
    )

    tasks = [
        asyncio.create_task(producer(i, per_producer, sent, stop))
        for i in range(producers)
    ]

    started = time.monotonic()
    last_total = 0
    last_t = started
    try:
        while True:
            await asyncio.sleep(1.0)
            total = sum(sent)
            now = time.monotonic()
            inst = (total - last_total) / (now - last_t)
            print(f"  sent={total:>10,d}  rate={inst:>10,.0f}/s")
            last_total, last_t = total, now
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.monotonic() - started
        total = sum(sent)
        print(
            f"\nstopped — sent {total:,d} in {elapsed:.1f}s "
            f"(avg {total/elapsed:,.0f}/s)"
        )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=10_000, help="target msg/sec (0 = flat-out)")
    ap.add_argument("--producers", type=int, default=4)
    args = ap.parse_args()
    try:
        asyncio.run(main(args.rate, args.producers))
    except KeyboardInterrupt:
        pass
