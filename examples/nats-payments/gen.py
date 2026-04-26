#!/usr/bin/env python3
"""
Publishes fake payment events to NATS subject `payments.events` at ~50 msg/sec.

    pip install nats-py
    python gen.py

Stop with Ctrl-C.
"""

import asyncio
import json
import random
from datetime import datetime, timezone

import nats


NATS_URL = "nats://localhost:4222"
SUBJECT = "payments.events"

REGIONS = ["us-east", "us-west", "eu-west", "ap-south"]
METHODS = ["card", "bank_transfer", "crypto", "wallet"]

# Heavily weighted toward 'completed' so failed_count stays a minority signal.
STATUSES = ["completed"] * 17 + ["failed"] * 2 + ["pending"] * 1


async def main() -> None:
    nc = await nats.connect(NATS_URL)
    print(f"Connected to NATS — publishing to {SUBJECT}")
    count = 0
    try:
        while True:
            event = {
                "payment_id": f"pay-{random.randint(1_000_000, 9_999_999)}",
                "region":     random.choice(REGIONS),
                "method":     random.choice(METHODS),
                "amount_usd": round(random.uniform(5.0, 2000.0), 2),
                "status":     random.choice(STATUSES),
                "event_time": datetime.now(timezone.utc).isoformat(),
            }
            await nc.publish(SUBJECT, json.dumps(event).encode())
            count += 1
            if count % 100 == 0:
                print(
                    f"  {count} events  —  {event['region']:<8} "
                    f"{event['method']:<14} ${event['amount_usd']:>8.2f}"
                )
            await asyncio.sleep(0.02)
    finally:
        await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
