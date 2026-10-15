"""Live terminal view of the FIX desk. LaminarDB's views compute every number; this script only
queries them over the HTTP SQL API and prints them.

    python watch.py [--url http://127.0.0.1:8080] [--interval 1]
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.request

QUERIES = {
    "feed": "SELECT * FROM feed_health",
    "positions": "SELECT account, symbol, net_qty, net_cash, turnover, fills FROM positions "
    "ORDER BY abs(net_cash) DESC LIMIT 10",
    "flow": "SELECT * FROM flow_1m ORDER BY minute DESC, notional DESC LIMIT 8",
    "rejects": "SELECT * FROM rejects_1m WHERE rejects > 0 ORDER BY minute DESC, rejects DESC LIMIT 4",
    "large": "SELECT * FROM large_fills ORDER BY transact_time DESC LIMIT 5",
    "bursts": "SELECT * FROM order_bursts ORDER BY last_order DESC LIMIT 4",
    "latency": "SELECT * FROM latency_1m ORDER BY minute DESC LIMIT 3",
}


def query(url, sql):
    request = urllib.request.Request(
        f"{url}/api/v1/sql",
        data=json.dumps({"sql": sql}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response).get("data") or []


def number(value, digits=0):
    return "-" if value is None else f"{float(value):,.{digits}f}"


def clock(value):
    return "-" if value is None else str(value).replace("T", " ")[11:19]


def table(title, header, rows):
    lines = [f"\x1b[1m{title}\x1b[0m", "  " + "".join(f"{h:>{w}}" if w > 0 else f"{h:<{-w}}" for h, w in header)]
    widths = [w for _, w in header]
    for row in rows:
        lines.append("  " + "".join(f"{c:>{w}}" if w > 0 else f"{c:<{-w}}" for c, w in zip(row, widths)))
    if not rows:
        lines.append("  (none yet)")
    return lines + [""]


def render(data):
    lines = []
    for feed in data["feed"]:
        lines.append(
            f"\x1b[1mFIX DESK\x1b[0m  {feed['session']}   reports {number(feed['reports'])}   "
            f"last seq {number(feed['last_seq'])}   series {feed['series']}   "
            f"resent {number(feed['resent'])}   possible gaps {number(feed['possible_gaps'])}   "
            f"last report {clock(feed['last_received'])}"
        )
    lines.append("")
    lines += table(
        "POSITIONS (largest cash)",
        [("account", -14), ("symbol", -8), ("net qty", 12), ("net cash", 16), ("turnover", 16), ("fills", 8)],
        [
            (r["account"], r["symbol"], number(r["net_qty"]), number(r["net_cash"], 2),
             number(r["turnover"], 2), number(r["fills"]))
            for r in data["positions"]
        ],
    )
    lines += table(
        "FLOW PER MINUTE (closed windows)",
        [("minute", -10), ("symbol", -8), ("orders", 8), ("fills", 8), ("cancels", 9),
         ("rejects", 9), ("volume", 10), ("notional", 16)],
        [
            (clock(r["minute"]), r["symbol"], number(r["orders"]), number(r["fills"]),
             number(r["cancels"]), number(r["rejects"]), number(r["volume"]), number(r["notional"], 2))
            for r in data["flow"]
        ],
    )
    lines += table(
        "LARGE FILLS (>= 1,000,000)",
        [("time", -10), ("account", -14), ("symbol", -8), ("side", -6), ("qty", 10), ("px", 10), ("notional", 16)],
        [
            (clock(r["transact_time"]), r["account"], r["symbol"], "BUY" if r["side"] == "1" else "SELL",
             number(r["last_qty"]), number(r["last_px"], 2), number(r["notional"], 2))
            for r in data["large"]
        ],
    )
    lines += table(
        "ORDER BURSTS (>= 10 orders, < 1 s apart)",
        [("from", -10), ("to", -10), ("account", -14), ("symbol", -8), ("orders", 8), ("quantity", 12)],
        [
            (clock(r["first_order"]), clock(r["last_order"]), r["account"], r["symbol"],
             number(r["orders"]), number(r["quantity"]))
            for r in data["bursts"]
        ],
    )
    lines += table(
        "REJECTS PER MINUTE",
        [("minute", -10), ("account", -14), ("orders", 8), ("rejects", 9)],
        [(clock(r["minute"]), r["account"], number(r["orders"]), number(r["rejects"])) for r in data["rejects"]],
    )
    lines += table(
        "DROP-COPY DELAY (TransactTime to journal)",
        [("minute", -10), ("reports", 9), ("avg ms", 10), ("max ms", 12)],
        [(clock(r["minute"]), number(r["reports"]), number(r["avg_ms"], 1), number(r["max_ms"], 1))
         for r in data["latency"]],
    )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()
    os.system("")  # enables ANSI escapes in the Windows console
    while True:
        try:
            screen = render({name: query(args.url, sql) for name, sql in QUERIES.items()})
        except (urllib.error.URLError, OSError, KeyError, ValueError) as error:
            screen = f"waiting for LaminarDB at {args.url}: {error}"
        print("\x1b[H\x1b[2J" + screen, flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
