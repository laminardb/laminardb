"""Tail Bluesky hashtag windows over LaminarDB's Postgres wire protocol."""

import asyncio

import asyncpg

DSN = "postgres://laminar@localhost:5432/laminar"
TRENDING = "SUBSCRIBE bsky_hashtags_5s"


async def tail_trending(conn: asyncpg.Connection) -> None:
    current = None
    counts: dict[str, tuple[int, int]] = {}
    async with conn.transaction():
        async for row in conn.cursor(TRENDING, prefetch=1):
            if row["bucket"] != current:
                current, counts = row["bucket"], {}
            counts[row["tag"]] = (row["uses"], row["authors"])
            top = sorted(counts.items(), key=lambda item: item[1][0], reverse=True)[:10]
            line = ", ".join(f"{tag}={uses}/{authors}" for tag, (uses, authors) in top)
            print(f"[trending] {row['bucket']}  {line}")


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    try:
        await tail_trending(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
