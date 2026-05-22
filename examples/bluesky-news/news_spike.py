"""Bluesky news-spike & cashtag demo client.

Tails two materialized views over the Postgres wire protocol:
  - bsky_keyword_spikes: prints terms whose 5s count is >5x their baseline.
  - bsky_cashtags_1m: prints the top cashtags per 1-minute bucket.

SUBSCRIBE streams rows as the views advance; it has no ORDER BY/LIMIT, so the
cashtag top-list is kept client-side. Ctrl-C to stop.
"""

import asyncio

import asyncpg

DSN = "postgres://laminar@localhost:5432/laminar"
SPIKES = "SUBSCRIBE bsky_keyword_spikes WHERE spike_ratio > 5"
CASHTAGS = "SUBSCRIBE bsky_cashtags_1m"


async def tail_spikes(conn: asyncpg.Connection) -> None:
    async with conn.transaction():
        async for row in conn.cursor(SPIKES):
            print(
                f"[spike]   {row['bucket']}  {row['term']:<10}  "
                f"n={row['n']:<4} ratio={row['spike_ratio']:.1f}"
            )


async def tail_cashtags(conn: asyncpg.Connection) -> None:
    # The 1-min window emits on close, so a bucket's rows arrive together and
    # buckets advance in order. Keep only the current bucket's counts so memory
    # stays bounded over an unbounded subscription.
    current = None
    counts: dict[str, tuple[int, int]] = {}
    async with conn.transaction():
        async for row in conn.cursor(CASHTAGS):
            if row["bucket"] != current:
                current, counts = row["bucket"], {}
            counts[row["cashtag"]] = (row["mentions"], row["unique_authors"])
            top = sorted(counts.items(), key=lambda kv: kv[1][0], reverse=True)[:10]
            line = ", ".join(f"{tag}={m}({u})" for tag, (m, u) in top)
            print(f"[cashtag] {row['bucket']}  {line}")


async def main() -> None:
    spikes_conn, cashtags_conn = await asyncio.gather(
        asyncpg.connect(DSN), asyncpg.connect(DSN)
    )
    try:
        await asyncio.gather(tail_spikes(spikes_conn), tail_cashtags(cashtags_conn))
    finally:
        await asyncio.gather(spikes_conn.close(), cashtags_conn.close())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
