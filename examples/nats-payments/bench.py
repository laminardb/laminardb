#!/usr/bin/env python3
"""
Scrapes laminardb's /metrics endpoint once a second and prints the
ingest and commit rates alongside running totals.

    python bench.py

Throughput numbers come from LaminarDB's Prometheus counters:
    nats_source_records_total       — events ingested by the source
    lakehouse_sink_rows_flushed_total — rows staged to Iceberg
    lakehouse_sink_commits_total      — committed snapshots

Stop with Ctrl-C; prints averages.
"""

import re
import sys
import time
import urllib.request

METRICS_URL = "http://127.0.0.1:8080/metrics"

PATTERNS = {
    "ingest":  re.compile(r"^laminardb_nats_source_records_total(?:\{[^}]*\})?\s+(\d+(?:\.\d+)?)", re.M),
    "flushed": re.compile(r"^laminardb_lakehouse_sink_rows_flushed_total(?:\{[^}]*\})?\s+(\d+(?:\.\d+)?)", re.M),
    "commits": re.compile(r"^laminardb_lakehouse_sink_commits_total(?:\{[^}]*\})?\s+(\d+(?:\.\d+)?)", re.M),
}


def scrape() -> dict[str, float]:
    with urllib.request.urlopen(METRICS_URL, timeout=2) as r:
        body = r.read().decode()
    out = {}
    for k, pat in PATTERNS.items():
        m = pat.search(body)
        out[k] = float(m.group(1)) if m else 0.0
    return out


def main():
    print(f"scraping {METRICS_URL} (Ctrl-C to stop)\n")
    print(f"{'time':>8}  {'ingest/s':>10}  {'flushed/s':>10}  {'commits':>7}  "
          f"{'total_in':>12}  {'total_out':>12}")
    try:
        prev = scrape()
        prev_t = time.monotonic()
        baseline = prev.copy()
        baseline_t = prev_t
        while True:
            time.sleep(1.0)
            cur = scrape()
            now = time.monotonic()
            dt = now - prev_t
            ingest_rate  = (cur["ingest"]  - prev["ingest"])  / dt
            flushed_rate = (cur["flushed"] - prev["flushed"]) / dt
            print(
                f"{time.strftime('%H:%M:%S'):>8}  "
                f"{ingest_rate:>10,.0f}  {flushed_rate:>10,.0f}  "
                f"{int(cur['commits']):>7,d}  "
                f"{int(cur['ingest']):>12,d}  {int(cur['flushed']):>12,d}"
            )
            prev = cur
            prev_t = now
    except KeyboardInterrupt:
        pass
    elapsed = time.monotonic() - baseline_t
    final = scrape()
    print(
        f"\n{elapsed:.1f}s — "
        f"ingested {int(final['ingest']) - int(baseline['ingest']):,d} "
        f"(avg {(final['ingest'] - baseline['ingest']) / elapsed:,.0f}/s), "
        f"flushed {int(final['flushed']) - int(baseline['flushed']):,d}, "
        f"commits {int(final['commits']) - int(baseline['commits']):,d}"
    )


if __name__ == "__main__":
    try:
        main()
    except urllib.error.URLError as e:
        sys.exit(f"could not reach {METRICS_URL}: {e}")
