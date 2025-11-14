# processor/worker.py
import os
import asyncio
import redis.asyncio as redis
import asyncpg
from datetime import datetime, timezone
import signal

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
STREAM_KEY = "analytics:events"
CONSUMER_GROUP = "analytics_group"
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "processor-1")
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://analytics:analytics@db:5432/analytics"
)

BATCH_SIZE = 20
POLL_TIMEOUT_MS = 2000
running = True


async def ensure_consumer_group(redis_client):
    """Create consumer group if not exists."""
    try:
        await redis_client.xgroup_create(
            STREAM_KEY, CONSUMER_GROUP, id='$', mkstream=True
        )
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            raise


async def process_batch(pool, redis_client, entries):
    async with pool.acquire() as conn:
        async with conn.transaction():

            for msg_id, fields in entries:
                site_id = fields.get("site_id")
                event_type = fields.get("event_type")
                path = fields.get("path") or "/"
                user_id = fields.get("user_id") or None

                ts_text = fields.get("timestamp")
                try:
                    occurred_at = datetime.fromisoformat(ts_text)
                except Exception:
                    occurred_at = datetime.now(timezone.utc)

                # Insert raw event
                await conn.execute("""
                    INSERT INTO events (site_id, event_type, path, user_id, occurred_at)
                    VALUES ($1, $2, $3, $4, $5)
                """, site_id, event_type, path, user_id, occurred_at)

                the_date = occurred_at.date()

                # Track unique users
                unique_inserted = False
                if user_id:
                    r = await conn.execute("""
                        INSERT INTO daily_unique_users (site_id, date, user_id)
                        VALUES ($1, $2, $3)
                        ON CONFLICT DO NOTHING
                    """, site_id, the_date, user_id)
                    unique_inserted = r.endswith(" 1")

                # Update stats
                if unique_inserted:
                    await conn.execute("""
                        INSERT INTO daily_stats (site_id, date, total_views, unique_users)
                        VALUES ($1, $2, 1, 1)
                        ON CONFLICT (site_id, date)
                        DO UPDATE SET 
                            total_views = daily_stats.total_views + 1,
                            unique_users = daily_stats.unique_users + 1
                    """, site_id, the_date)
                else:
                    await conn.execute("""
                        INSERT INTO daily_stats (site_id, date, total_views, unique_users)
                        VALUES ($1, $2, 1, 0)
                        ON CONFLICT (site_id, date)
                        DO UPDATE SET 
                            total_views = daily_stats.total_views + 1
                    """, site_id, the_date)

                # Path stats
                await conn.execute("""
                    INSERT INTO daily_path_views (site_id, date, path, views)
                    VALUES ($1, $2, $3, 1)
                    ON CONFLICT (site_id, date, path)
                    DO UPDATE SET views = daily_path_views.views + 1
                """, site_id, the_date, path)

                # ACK Redis event
                try:
                    await redis_client.xack(STREAM_KEY, CONSUMER_GROUP, msg_id)
                except Exception as e:
                    print("WARN: xack failed →", e)


async def consumer_loop():
    global running

    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    await ensure_consumer_group(redis_client)

    print("Processor READY — waiting for events...")

    while running:
        try:
            resp = await redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={STREAM_KEY: ">"},
                count=BATCH_SIZE,
                block=POLL_TIMEOUT_MS
            )

            if not resp:
                await asyncio.sleep(0.05)
                continue

            # resp: [('analytics:events', [(id, {fields}), ...])]
            entries = []
            _, messages = resp[0]

            for msg_id, data in messages:
                entries.append((msg_id, data))

            if entries:
                await process_batch(pool, redis_client, entries)

        except Exception as e:
            print("ERROR in processor:", e)
            await asyncio.sleep(1)

    await pool.close()
    await redis_client.close()


def handle_sigterm(*_):
    global running
    running = False


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, handle_sigterm)

    try:
        loop.run_until_complete(consumer_loop())
    finally:
        loop.close()
