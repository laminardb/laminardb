"""Bluesky trending-hashtag demo client.

Tails two materialized views over the Postgres wire protocol:
  - bsky_hashtags_5s: top hashtags per 5-second window (live trending).
  - bsky_hashtag_spikes: hashtags whose 5s use-count is >5x their baseline.

SUBSCRIBE streams rows as the views advance; it has no ORDER BY/LIMIT, so the
per-window top-list is kept client-side. Ctrl-C to stop.
"""

import asyncio

import asyncpg

DSN = "postgres://laminar@localhost:5432/laminar"
TRENDING = "SUBSCRIBE bsky_hashtags_5s"
SPIKES = "SUBSCRIBE bsky_hashtag_spikes WHERE spike_ratio > 5"


# asyncpg cursors default to prefetch=50: they won't yield until 50 rows have
# batched, so a live tail appears to hang. prefetch=1 streams each row as it
# arrives.
async def tail_spikes(conn: asyncpg.Connection) -> None:
    async with conn.transaction():
        async for row in conn.cursor(SPIKES, prefetch=1):
            print(
                f"[spike]    {row['bucket']}  {row['tag']:<20} "
                f"uses={row['uses']:<4} ratio={row['spike_ratio']:.1f}"
            )


async def tail_trending(conn: asyncpg.Connection) -> None:
    # The 5s window emits on close, so a bucket's rows arrive together and
    # buckets advance in order. Keep only the current bucket's counts so memory
    # stays bounded over an unbounded subscription.
    current = None
    counts: dict[str, tuple[int, int]] = {}
    async with conn.transaction():
        async for row in conn.cursor(TRENDING, prefetch=1):
            if row["bucket"] != current:
                current, counts = row["bucket"], {}
            counts[row["tag"]] = (row["uses"], row["authors"])
            top = sorted(counts.items(), key=lambda kv: kv[1][0], reverse=True)[:10]
            line = ", ".join(f"{tag}={u}/{a}" for tag, (u, a) in top)
            print(f"[trending] {row['bucket']}  {line}")


async def main() -> None:
    spikes_conn, trending_conn = await asyncio.gather(
        asyncpg.connect(DSN), asyncpg.connect(DSN)
    )
    try:
        await asyncio.gather(tail_spikes(spikes_conn), tail_trending(trending_conn))
    finally:
        await asyncio.gather(spikes_conn.close(), trending_conn.close())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
