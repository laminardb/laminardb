#!/usr/bin/env python3
"""Tiny pgwire SUBSCRIBE client for the Bluesky firehose demo.

LaminarDB speaks the Postgres wire protocol, so this is just a Postgres
client. Use `psql` if you have it (see README); this script is the
zero-install alternative — it prints each post plus a rolling
throughput line so you can watch the processing rate.

    pip install "psycopg[binary]"

    python watch.py                       # live tail of every post
    python watch.py "WHERE text LIKE '%bluesky%'"
    python watch.py "AS OF EPOCH 2"       # replay retained history, no gap

Anything you pass is appended after `SUBSCRIBE posts`, so WHERE / AS OF
EPOCH / WITH all work. Ctrl-C to stop.
"""
import sys, time, psycopg

DSN = "host=127.0.0.1 port=5432 dbname=laminardb user=demo"

def main() -> int:
    tail = " ".join(sys.argv[1:]).strip()
    sql = f"SUBSCRIBE posts {tail}".strip()
    print(f"-> {sql}\n", flush=True)

    n = 0
    t0 = last = time.time()
    try:
        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            for _did, _coll, text, created in cur.stream(sql):
                n += 1
                if text:
                    line = text.replace("\n", " ")
                    if len(line) > 96:
                        line = line[:96] + "…"
                    # errors="replace" keeps Windows consoles happy on emoji.
                    sys.stdout.buffer.write(
                        f"  {created}  {line}\n".encode("utf-8", "replace"))
                now = time.time()
                if now - last >= 2.0:
                    rate = n / (now - t0)
                    sys.stdout.buffer.write(
                        f"--- {n} posts, {rate:,.0f}/sec ---\n".encode())
                    last = now
                sys.stdout.flush()
    except KeyboardInterrupt:
        pass
    except psycopg.Error as e:
        print(f"\npgwire error: {e}", file=sys.stderr)
        return 1
    finally:
        dt = time.time() - t0
        if dt > 0:
            print(f"\n{n} posts in {dt:.0f}s ({n/dt:,.0f}/sec)", flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
