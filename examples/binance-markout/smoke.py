"""Capture samples from the four WebSocket sinks and print a summary."""
import asyncio
import json
import math
import sys
import time

import websockets

STREAMS = {
    "markouts": 9001,
    "toxicity": 9002,
    "by_side":  9003,
    "alerts":   9004,
}
DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 200.0

async def capture(name, port, store):
    url = f"ws://127.0.0.1:{port}/{name}"
    try:
        async with websockets.connect(url, open_timeout=5) as ws:
            store[name]["connected_at"] = time.time()
            t_end = time.time() + DURATION
            while time.time() < t_end:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=max(1.0, t_end - time.time()))
                except asyncio.TimeoutError:
                    continue
                try:
                    payload = json.loads(msg)
                except Exception as e:
                    store[name]["errors"].append(f"json: {e}")
                    continue
                # Unwrap the ServerMessage envelope: {"type":"data","data":[...]}
                if isinstance(payload, dict) and payload.get("type") == "data":
                    data = payload.get("data")
                    rows = data if isinstance(data, list) else [data]
                elif isinstance(payload, list):
                    rows = payload
                else:
                    rows = [payload]
                store[name]["rows"].extend(rows)
                if len(store[name]["rows"]) <= 3:
                    store[name]["samples"].append(rows[0] if rows else payload)
    except Exception as e:
        store[name]["errors"].append(f"conn: {e}")

async def main():
    store = {k: {"rows": [], "samples": [], "errors": [], "connected_at": None} for k in STREAMS}
    await asyncio.gather(*[capture(n, p, store) for n, p in STREAMS.items()])

    for name, s in store.items():
        print(f"\n=== {name} ===")
        print(f"  connected : {'yes' if s['connected_at'] else 'no'}")
        print(f"  rows      : {len(s['rows'])}")
        print(f"  errors    : {s['errors']}")
        if s["samples"]:
            print(f"  sample[0] : {json.dumps(s['samples'][0], default=str)[:300]}")
        if len(s["rows"]) > 1:
            print(f"  data[1]   : {json.dumps(s['rows'][1], default=str)[:500]}")

    print("\n=== markout analysis ===")
    markouts = store["markouts"]["rows"]
    offsets = {}
    for r in markouts:
        o = r.get("offset_ms")
        offsets.setdefault(o, 0)
        offsets[o] += 1
    print(f"  markouts rows  : {len(markouts)}")
    print(f"  by offset_ms   : {offsets}")
    def _finite(r):
        v = r.get("signed_markout_bps")
        return v if (isinstance(v, (int, float)) and math.isfinite(v)) else None

    null_cnt = sum(1 for r in markouts
                   if r.get("offset_ms") == 1000 and r.get("signed_markout_bps") is None)
    print(f"  1s rows with NULL markout : {null_cnt}  "
          f"(probes where book ref was outside buffer window)")

    vals_1s = [_finite(r) for r in markouts if r.get("offset_ms") == 1000]
    vals_1s = [v for v in vals_1s if v is not None]
    if vals_1s:
        vals_1s.sort()
        n = len(vals_1s)
        print(f"  1s markout n={n}  min={vals_1s[0]:.3f}  p50={vals_1s[n//2]:.3f}  "
              f"p95={vals_1s[int(n*0.95)]:.3f}  max={vals_1s[-1]:.3f}")
        buys = [_finite(r) for r in markouts
                if r.get("offset_ms") == 1000 and r.get("side") == "BUY"]
        buys = [v for v in buys if v is not None]
        sells = [_finite(r) for r in markouts
                 if r.get("offset_ms") == 1000 and r.get("side") == "SELL"]
        sells = [v for v in sells if v is not None]
        if buys: print(f"  BUY  avg 1s    : {sum(buys)/len(buys):.3f} bps  (n={len(buys)})")
        if sells: print(f"  SELL avg 1s    : {sum(sells)/len(sells):.3f} bps  (n={len(sells)})")

    print("\n=== toxicity windows ===")
    tox = store["toxicity"]["rows"]
    windows_by_offset = {}
    for r in tox:
        o = r.get("offset_ms")
        windows_by_offset.setdefault(o, []).append(r)
    for o, rows in sorted(windows_by_offset.items(), key=lambda x: (x[0] is None, x[0])):
        nonzero = [r for r in rows if (r.get("trade_count") or 0) > 0]
        print(f"  offset={o}  windows={len(rows)}  nonzero={len(nonzero)}")

asyncio.run(main())
