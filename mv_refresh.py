"""
Materialized-view refresh scheduling.

All refreshes are online (REFRESH MATERIALIZED VIEW CONCURRENTLY — no lock that
blocks readers, and the live listener keeps running), on two configurable cadences:

  - "fast" (config.refresh.mv_refresh_hours; default 00:00/06:00/12:00/18:00 UTC):
    the per-system kill-count MVs behind system rankings.
  - "slow" (weekly, config.refresh.day/hour): the slow-changing MVs — farthest
    kill per system and the weapon-search set.

Both cadences share one refresh loop; they differ only in schedule, view list,
and cache-invalidation target. (kills stats stay current via autovacuum, and the
per-system bulk read is served by a covering index, so the old CLUSTER + ANALYZE
step — and its live-listener pause — are gone.)
"""

import asyncio
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone, timedelta

import redis.asyncio as aioredis

import metrics
from config import config
from db import get_connection
from stream import publish_invalidation

_FAST_VIEWS = [
    "mv_kills_per_system",
    "mv_kills_per_system_24h",
    "mv_kills_per_system_7d",
    "mv_kills_per_system_30d",
    "mv_kills_per_system_6m",
    "mv_kills_per_system_1y",
    "mv_alliance_status",
]

_SLOW_VIEWS = [
    "mv_farthest_kill_per_system",
    "mv_weapon_search",
]

logger = logging.getLogger(__name__)


async def fast_refresh_scheduler(
    shutdown_event: asyncio.Event,
    redis: aioredis.Redis | None = None,
) -> None:
    """Refresh the per-system kill-count MVs on the fast cadence (config.refresh.mv_refresh_hours)."""
    await _refresh_loop(
        shutdown_event,
        next_run=_next_fast_refresh_time,
        views=_FAST_VIEWS,
        invalidation=["system_rankings"],
        cadence="fast",
        redis=redis,
    )


async def slow_refresh_scheduler(
    shutdown_event: asyncio.Event,
    redis: aioredis.Redis | None = None,
) -> None:
    """Refresh the slow-changing MVs (farthest kill, weapon search) weekly (config.refresh.day/hour)."""
    await _refresh_loop(
        shutdown_event,
        next_run=_next_slow_refresh_time,
        views=_SLOW_VIEWS,
        invalidation=["farthest_kill"],
        cadence="slow",
        redis=redis,
    )


async def _refresh_loop(
    shutdown_event: asyncio.Event,
    *,
    next_run: Callable[[datetime], datetime],
    views: list[str],
    invalidation: list[str],
    cadence: str,
    redis: aioredis.Redis | None,
) -> None:
    """Wait for the next scheduled time, REFRESH the given MVs concurrently, and
    publish a cache invalidation. Shared by both cadences."""
    logger.info(f"{cadence} materialized-view refresh scheduler started.")

    while not shutdown_event.is_set():
        now = datetime.now(timezone.utc)
        wait_seconds = (next_run(now) - now).total_seconds()

        logger.info(f"Next {cadence} refresh in {wait_seconds / 3600:.1f} hours.")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            pass

        try:
            start = time.monotonic()
            await asyncio.get_event_loop().run_in_executor(None, _refresh_views, views)
            metrics.mv_refresh_duration_seconds.labels(cadence).observe(
                time.monotonic() - start
            )
            metrics.mv_refresh_runs.labels(cadence, "success").inc()
            metrics.mv_refresh_last_success_timestamp.labels(
                cadence
            ).set_to_current_time()
            if redis is not None:
                await publish_invalidation(redis, invalidation)
        except Exception as e:
            metrics.mv_refresh_runs.labels(cadence, "failed").inc()
            metrics.errors.labels("mv_refresh").inc()
            logger.error(f"{cadence} refresh error: {e}", exc_info=True)

    logger.info(f"{cadence} materialized-view refresh scheduler stopped.")


def _next_slow_refresh_time(now: datetime) -> datetime:
    target = now.replace(hour=config.refresh.hour, minute=0, second=0, microsecond=0)

    days_ahead = config.refresh.day - now.weekday()
    if days_ahead < 0 or (days_ahead == 0 and target <= now):
        days_ahead += 7

    return target + timedelta(days=days_ahead)


def _next_fast_refresh_time(now: datetime) -> datetime:
    for hour in sorted(config.refresh.mv_refresh_hours):
        candidate = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if candidate > now:
            return candidate
    first_hour = sorted(config.refresh.mv_refresh_hours)[0]
    return (now + timedelta(days=1)).replace(
        hour=first_hour, minute=0, second=0, microsecond=0
    )


def _refresh_views(views: list[str]) -> None:
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for view in views:
                logger.info(f"Refreshing materialized view {view}...")
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
                logger.info(f"Refreshed {view}.")
