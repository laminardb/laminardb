#!/usr/bin/env python3
"""
Multi-producer NATS publisher. Two correlated streams:

* payments.initiated — a payment is created
* payments.scored    — fraud system returns a score 50ms-1s later;
                        2% never score (fraud service errored)

    pip install nats-py
    python gen.py                 # 10_000 payments/s
    python gen.py --rate 50000    # 50K/s

Stop with Ctrl-C; prints sent counts.
"""

import argparse
import asyncio
import json
import random
import time
from datetime import datetime, timezone

import nats


NATS_URL = "nats://localhost:4222"
PAYMENT_SUBJECT = "payments.initiated"
SCORE_SUBJECT   = "payments.scored"

REGIONS = ["us-east", "us-west", "eu-west", "ap-south"]
METHODS = ["card", "bank_transfer", "crypto", "wallet"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def score_outcome(score: float) -> str:
    if score >= 0.85:
        return "blocked"
    if score >= 0.50:
        return "review"
    return "approved"


async def payment_producer(rate: float, sent: list[int], idx: int,
                           score_q: asyncio.Queue, stop: asyncio.Event):
    nc = await nats.connect(NATS_URL, pending_size=64 * 1024 * 1024)
    batch = 200
    start = time.monotonic()
    local = 0
    try:
        while not stop.is_set():
            now = time.monotonic()
            for _ in range(batch):
                pid = random.randint(1_000_000_000, 9_999_999_999)
                region = random.choice(REGIONS)
                method = random.choice(METHODS)
                amount = round(random.uniform(5.0, 2000.0), 2)
                evt = {
                    "payment_id": pid,
                    "region":     region,
                    "method":     method,
                    "amount_usd": amount,
                    "event_time": now_iso(),
                }
                await nc.publish(PAYMENT_SUBJECT, json.dumps(evt).encode())

                # Schedule the fraud score follow-up.
                roll = random.random()
                if roll < 0.02:
                    continue                         # 2% never score
                if roll < 0.80:
                    delay = random.uniform(0.05, 0.20)   # 78% fast: 50-200ms
                else:
                    delay = random.uniform(0.20, 1.0)    # 20% slow: 200ms-1s
                # Score distribution: most low risk, a long tail high.
                score = min(1.0, max(0.0, random.gauss(0.20, 0.18)))
                score_q.put_nowait((now + delay, pid, region, score))
            local += batch
            sent[idx] = local

            if rate > 0:
                expected = local / rate
                elapsed = time.monotonic() - start
                if expected > elapsed:
                    await asyncio.sleep(expected - elapsed)
    finally:
        await nc.flush()
        await nc.drain()


async def score_producer(sent: list[int], score_q: asyncio.Queue,
                         stop: asyncio.Event):
    nc = await nats.connect(NATS_URL, pending_size=64 * 1024 * 1024)
    pending: list[tuple[float, int, str, float]] = []
    try:
        while not stop.is_set() or pending or not score_q.empty():
            while True:
                try:
                    pending.append(score_q.get_nowait())
                except asyncio.QueueEmpty:
                    break

            now = time.monotonic()
            ready, later = [], []
            for item in pending:
                (ready if item[0] <= now else later).append(item)
            pending = later

            for due_at, pid, region, score in ready:
                evt = {
                    "payment_id":  pid,
                    "region":      region,
                    "fraud_score": round(score, 3),
                    "outcome":     score_outcome(score),
                    "event_time":  now_iso(),
                }
                await nc.publish(SCORE_SUBJECT, json.dumps(evt).encode())
                sent[0] += 1

            if not ready:
                await asyncio.sleep(0.02)
    finally:
        await nc.flush()
        await nc.drain()


async def main(rate: int, producers: int):
    per_producer = rate / producers if rate > 0 else 0
    pay_sent   = [0] * producers
    score_sent = [0]
    stop       = asyncio.Event()
    score_q: asyncio.Queue = asyncio.Queue(maxsize=200_000)

    print(
        f"Publishing {PAYMENT_SUBJECT} + {SCORE_SUBJECT} — "
        f"{producers} producers, target {rate:,} payments/s "
        f"({per_producer:,.0f}/s each); 98% scored within 1s"
        if rate > 0
        else f"Flat-out, {producers} producers, 98% scored within 1s"
    )

    tasks = [
        asyncio.create_task(payment_producer(per_producer, pay_sent, i, score_q, stop))
        for i in range(producers)
    ]
    tasks.append(asyncio.create_task(score_producer(score_sent, score_q, stop)))

    started = time.monotonic()
    last_p, last_s, last_t = 0, 0, started
    try:
        while True:
            await asyncio.sleep(1.0)
            p = sum(pay_sent); s = score_sent[0]; now = time.monotonic()
            print(f"  payments={p:>10,d} ({(p-last_p)/(now-last_t):>9,.0f}/s)  "
                  f"scores={s:>10,d} ({(s-last_s)/(now-last_t):>9,.0f}/s)  "
                  f"queued={score_q.qsize():>6,d}")
            last_p, last_s, last_t = p, s, now
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        stop.set()
        await asyncio.sleep(0.2)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.monotonic() - started
        p, s = sum(pay_sent), score_sent[0]
        print(
            f"\nstopped — payments {p:,d} ({p/elapsed:,.0f}/s), "
            f"scores {s:,d} ({s/elapsed:,.0f}/s) over {elapsed:.1f}s"
        )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=10_000, help="payments/sec (0 = flat-out)")
    ap.add_argument("--producers", type=int, default=4)
    args = ap.parse_args()
    try:
        asyncio.run(main(args.rate, args.producers))
    except KeyboardInterrupt:
        pass
