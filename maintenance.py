"""
Maintenance tasks.

Weekly: CLUSTER + ANALYZE on the kills table to physically reorder rows by
solar_system_id and update planner statistics. The live listener is paused
during clustering (table is write-locked).

Every 6 hours (00:00, 06:00, 12:00, 18:00 UTC): Refresh materialized views
for aggregate statistics.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta

import redis.asyncio as aioredis

from config import config
from db import get_connection
from stream import publish_invalidation

_MATERIALIZED_VIEWS = [
    "mv_kills_per_system_top",
    "mv_kills_per_system_top_24h",
    "mv_kills_per_system_top_7d",
    "mv_kills_per_system_top_30d",
    "mv_kills_per_system_top_6m",
    "mv_kills_per_system_top_1y",
    "mv_kills_per_system_bottom",
]

_WEEKLY_MATERIALIZED_VIEWS = [
    "mv_farthest_kill_per_system",
]

logger = logging.getLogger(__name__)


async def maintenance_scheduler(
    shutdown_event: asyncio.Event,
    live_paused: asyncio.Event,
    redis: aioredis.Redis | None = None,
) -> None:
    logger.info(
        f"Maintenance scheduler started. "
        f"Runs weekly on day {config.maintenance.day} at {config.maintenance.hour:02d}:00 UTC."
    )

    while not shutdown_event.is_set():
        now = datetime.now(timezone.utc)
        next_run = _next_maintenance_time(now)
        wait_seconds = (next_run - now).total_seconds()

        logger.info(f"Next maintenance in {wait_seconds / 3600:.1f} hours.")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            pass

        try:
            await _run_maintenance(live_paused)
            if redis is not None:
                await publish_invalidation(redis, ["farthest_kill"])
        except Exception as e:
            logger.error(f"Maintenance error: {e}", exc_info=True)

    logger.info("Maintenance scheduler stopped.")


def _next_maintenance_time(now: datetime) -> datetime:
    target = now.replace(
        hour=config.maintenance.hour, minute=0, second=0, microsecond=0
    )

    days_ahead = config.maintenance.day - now.weekday()
    if days_ahead < 0 or (days_ahead == 0 and target <= now):
        days_ahead += 7

    return target + timedelta(days=days_ahead)


async def _run_maintenance(live_paused: asyncio.Event) -> None:
    logger.info("Starting weekly maintenance...")

    live_paused.set()
    await asyncio.sleep(2)

    try:
        logger.info("Running CLUSTER kills USING idx_kills_system...")
        await asyncio.get_event_loop().run_in_executor(None, _cluster_and_analyze)
        await asyncio.get_event_loop().run_in_executor(
            None, _refresh_weekly_materialized_views
        )
        logger.info("Maintenance complete.")
    finally:
        live_paused.clear()


async def mv_refresh_scheduler(
    shutdown_event: asyncio.Event,
    redis: aioredis.Redis | None = None,
) -> None:
    hours = ", ".join(f"{h:02d}:00" for h in config.maintenance.mv_refresh_hours)
    logger.info(f"Materialized view refresh scheduler started. Runs at {hours} UTC.")

    while not shutdown_event.is_set():
        now = datetime.now(timezone.utc)
        next_run = _next_mv_refresh_time(now)
        wait_seconds = (next_run - now).total_seconds()

        logger.info(
            f"Next materialized view refresh in {wait_seconds / 3600:.1f} hours."
        )

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            pass

        try:
            await asyncio.get_event_loop().run_in_executor(
                None, _refresh_materialized_views
            )
            if redis is not None:
                await publish_invalidation(redis, ["system_rankings"])
        except Exception as e:
            logger.error(f"Materialized view refresh error: {e}", exc_info=True)

    logger.info("Materialized view refresh scheduler stopped.")


def _next_mv_refresh_time(now: datetime) -> datetime:
    for hour in sorted(config.maintenance.mv_refresh_hours):
        candidate = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if candidate > now:
            return candidate
    first_hour = sorted(config.maintenance.mv_refresh_hours)[0]
    return (now + timedelta(days=1)).replace(
        hour=first_hour, minute=0, second=0, microsecond=0
    )


def _refresh_materialized_views() -> None:
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for view in _MATERIALIZED_VIEWS:
                logger.info(f"Refreshing materialized view {view}...")
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
                logger.info(f"Refreshed {view}.")


def _refresh_weekly_materialized_views() -> None:
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for view in _WEEKLY_MATERIALIZED_VIEWS:
                logger.info(f"Refreshing weekly materialized view {view}...")
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
                logger.info(f"Refreshed {view}.")


def _cluster_and_analyze() -> None:
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("CLUSTER kills USING idx_kills_system")
            cursor.execute("ANALYZE kills")
