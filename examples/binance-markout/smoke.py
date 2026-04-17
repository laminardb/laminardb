"""Capture samples from the six WebSocket sinks, print a summary, and
run a handful of sanity checks. Exits non-zero on any CRITICAL failure."""
import asyncio
import json
import math
import sys
import time

import websockets

STREAMS = {
    "markouts": 9001,
    "curve":    9002,
    "toxicity": 9003,
    "alerts":   9004,
    "regime":   9005,
    "signal":   9006,
}
DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 200.0
EXPECTED_OFFSETS = (5000, 15000, 30000, 60000)

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
                # Only the "data" envelope carries real rows; "subscribed" is
                # an ack, anything else is protocol noise — drop it.
                if not (isinstance(payload, dict) and payload.get("type") == "data"):
                    continue
                data = payload.get("data")
                rows = data if isinstance(data, list) else [data] if data else []
                store[name]["rows"].extend(rows)
                if len(store[name]["samples"]) < 2 and rows:
                    store[name]["samples"].append(rows[0])
    except Exception as e:
        store[name]["errors"].append(f"conn: {e}")


def _finite(x):
    return x if (isinstance(x, (int, float)) and math.isfinite(x)) else None


def validate(store):
    """Return (ok_count, findings). Findings tagged CRITICAL/HIGH/OK."""
    findings = []
    markouts = store["markouts"]["rows"]

    # 1. All four horizons emit.
    by_offset = {}
    for r in markouts:
        o = r.get("offset_ms")
        by_offset.setdefault(o, []).append(r)
    for o in EXPECTED_OFFSETS:
        n = len(by_offset.get(o, []))
        if n == 0:
            findings.append(("CRITICAL", f"offset {o}: no markout rows received"))
        else:
            findings.append(("OK", f"offset {o}: {n} rows"))

    # 2. Magnitudes sane at 5s (|markout| < 50 bps typical).
    vals_5s = [_finite(r.get("signed_markout_bps")) for r in by_offset.get(5000, [])]
    vals_5s = [v for v in vals_5s if v is not None]
    if vals_5s:
        vals_5s.sort()
        n = len(vals_5s)
        p50 = vals_5s[n // 2]
        p95 = vals_5s[int(n * 0.95)]
        mx = max(abs(vals_5s[0]), abs(vals_5s[-1]))
        findings.append(("OK", f"5s markout n={n} p50={p50:+.2f} p95={p95:+.2f} |max|={mx:.2f} bps"))
        if mx > 100.0:
            findings.append(("CRITICAL", f"5s markout |max|={mx:.2f} bps > 100 — parsing/unit bug?"))

    # 3. BUY/SELL balance.
    buys_5s = [r for r in by_offset.get(5000, []) if r.get("side") == "BUY"]
    sells_5s = [r for r in by_offset.get(5000, []) if r.get("side") == "SELL"]
    if buys_5s and sells_5s:
        ratio = len(buys_5s) / max(1, len(sells_5s))
        findings.append(("OK", f"BUY/SELL ratio at 5s: {ratio:.2f} (buys={len(buys_5s)} sells={len(sells_5s)})"))
        if ratio > 5 or ratio < 0.2:
            findings.append(("HIGH", f"BUY/SELL ratio {ratio:.2f} is extreme — side-derivation check"))

    # 4. 5s average is finite and reasonable.
    buy_vals = [_finite(r.get("signed_markout_bps")) for r in buys_5s]
    buy_vals = [v for v in buy_vals if v is not None]
    sell_vals = [_finite(r.get("signed_markout_bps")) for r in sells_5s]
    sell_vals = [v for v in sell_vals if v is not None]
    if buy_vals:
        avg_buy = sum(buy_vals) / len(buy_vals)
        findings.append(("OK", f"BUY avg 5s markout = {avg_buy:+.3f} bps (n={len(buy_vals)})"))
    if sell_vals:
        avg_sell = sum(sell_vals) / len(sell_vals)
        findings.append(("OK", f"SELL avg 5s markout = {avg_sell:+.3f} bps (n={len(sell_vals)})"))

    # 5. Curve emitted at least once.
    curve = store["curve"]["rows"]
    if curve:
        findings.append(("OK", f"markout_curve rows={len(curve)} (1-min TUMBLE)"))
        for r in curve:
            side = r.get("side")
            a5 = _finite(r.get("avg_5s"))
            a60 = _finite(r.get("avg_60s"))
            cnt = r.get("trade_count")
            if a5 is not None and a60 is not None:
                findings.append(("OK", f"  curve side={side} avg_5s={a5:+.2f} avg_60s={a60:+.2f} n={cnt}"))
    else:
        findings.append(("HIGH", "markout_curve: no rows — window may not have closed yet"))

    # 6. Regime classifier.
    regime = store["regime"]["rows"]
    if regime:
        tags = {}
        for r in regime:
            tags[r.get("regime")] = tags.get(r.get("regime"), 0) + 1
        findings.append(("OK", f"flow_regime rows={len(regime)} tags={tags}"))
    else:
        findings.append(("HIGH", "flow_regime: no rows"))

    # 7. Quote signal shape.
    signal = store["signal"]["rows"]
    if signal:
        findings.append(("OK", f"quote_signal rows={len(signal)}"))
        for r in signal[:2]:
            findings.append(("OK", f"  sample: skew={_finite(r.get('skew_bps'))} "
                                   f"max_adv={_finite(r.get('max_adverse_rate'))} "
                                   f"n={r.get('trade_count')}"))
    else:
        findings.append(("HIGH", "quote_signal: no rows — 30s window may not have closed"))

    # 8. Toxicity windows: 4 offsets × rows.
    tox = store["toxicity"]["rows"]
    tox_by_offset = {}
    for r in tox:
        o = r.get("offset_ms")
        tox_by_offset.setdefault(o, []).append(r)
    for o in EXPECTED_OFFSETS:
        rows = tox_by_offset.get(o, [])
        nonzero = [r for r in rows if (r.get("trade_count") or 0) > 0]
        findings.append(("OK", f"toxicity offset={o}: windows={len(rows)} nonzero={len(nonzero)}"))

    return findings


async def main():
    store = {k: {"rows": [], "samples": [], "errors": [], "connected_at": None} for k in STREAMS}
    await asyncio.gather(*[capture(n, p, store) for n, p in STREAMS.items()])

    print("\n=== connections ===")
    for name, s in store.items():
        status = "yes" if s["connected_at"] else "NO"
        errs = "; ".join(s["errors"]) if s["errors"] else ""
        print(f"  {name:10s} connected={status}  rows={len(s['rows']):5d}  {errs}")

    print("\n=== samples ===")
    for name, s in store.items():
        if s["samples"]:
            print(f"  {name:10s}: {json.dumps(s['samples'][0], default=str)[:240]}")

    print("\n=== validation ===")
    findings = validate(store)
    crit = [f for f in findings if f[0] == "CRITICAL"]
    high = [f for f in findings if f[0] == "HIGH"]
    for tag, msg in findings:
        print(f"  [{tag:8s}] {msg}")

    print(f"\n{len(crit)} CRITICAL, {len(high)} HIGH findings")
    return 1 if crit else 0


exit_code = asyncio.run(main())
sys.exit(exit_code)
