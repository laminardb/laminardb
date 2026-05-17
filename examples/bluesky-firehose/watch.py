#!/usr/bin/env python3
"""Tiny pgwire SUBSCRIBE client for the Bluesky firehose demo.

LaminarDB speaks the Postgres wire protocol, so this is just a Postgres
client. Use `psql` if you have it (see README); this script is the
zero-install alternative — it prints each row plus a rolling throughput
line so you can watch the rate.

    pip install "psycopg[binary]"

    python watch.py                          # live tail of every post
    python watch.py "WHERE text LIKE '%bluesky%'"
    python watch.py "AS OF EPOCH 0"          # replay retained history, no gap
    python watch.py --from posts_per_min     # 1-min windowed post counts

`--from <relation>` picks what to SUBSCRIBE (default `posts`). Anything
else you pass is appended after `SUBSCRIBE <relation>`, so WHERE / AS OF
EPOCH / WITH all work. Rows are rendered from the result columns, so any
relation (posts, posts_per_min, …) prints sensibly. Ctrl-C to stop.
"""
import sys, time, psycopg

DSN = "host=127.0.0.1 port=5432 dbname=laminardb user=demo"


def parse_args(argv):
    """Returns (relation, trailing_clause). `--from NAME` is optional."""
    relation = "posts"
    rest = []
    it = iter(argv)
    for a in it:
        if a == "--from":
            relation = next(it, relation)
        elif a.startswith("--from="):
            relation = a.split("=", 1)[1]
        else:
            rest.append(a)
    return relation, " ".join(rest).strip()


def fmt_post(cols):
    """Pretty one-liner for the `posts` schema (did, collection, text, created_at)."""
    text = cols.get("text")
    created = cols.get("created_at")
    if not text:
        return None
    line = text.replace("\n", " ")
    if len(line) > 96:
        line = line[:96] + "…"
    return f"  {created}  {line}"


def fmt_generic(cols):
    """col=val  col=val — works for posts_per_min and any other relation."""
    return "  " + "  ".join(f"{k}={v}" for k, v in cols.items())


def main() -> int:
    relation, tail = parse_args(sys.argv[1:])
    sql = f"SUBSCRIBE {relation} {tail}".strip()
    print(f"-> {sql}\n", flush=True)

    n = 0
    t0 = last = time.time()
    try:
        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            stream = cur.stream(sql)
            names = None
            for row in stream:
                if names is None:
                    names = [d.name for d in cur.description]
                cols = dict(zip(names, row))
                n += 1
                # Use the rich post formatter when the schema looks like
                # `posts`; otherwise print columns generically.
                line = fmt_post(cols) if "text" in cols else fmt_generic(cols)
                if line is not None:
                    # errors="replace" keeps Windows consoles happy on emoji.
                    sys.stdout.buffer.write(
                        (line + "\n").encode("utf-8", "replace"))
                now = time.time()
                if now - last >= 2.0:
                    rate = n / (now - t0)
                    sys.stdout.buffer.write(
                        f"--- {n} rows, {rate:,.0f}/sec ---\n".encode())
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
            print(f"\n{n} rows in {dt:.0f}s ({n/dt:,.0f}/sec)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
