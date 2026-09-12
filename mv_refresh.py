"""
Refresh scheduling: materialized views and the entity-leaderboard rollup.

Two cadences share one loop; each runs an ordered list of RefreshSteps:

  - "fast" (config.refresh.mv_refresh_interval_minutes; default every 360 min):
    roll dirty days into entity_kills_daily -> the windowed leaderboards ->
    the per-system kill-count MVs behind system rankings. The cheap board work
    runs first so the MV refresh's minutes don't delay it.
  - "slow" (weekly, config.refresh.day/hour): the all-time leaderboard -> the
    slow-changing MVs (farthest kill per system, the ship/weapon search sets).

Every step opens its own connection with the refresh session settings
(work_mem, TimeZone=UTC), runs in its own transaction(s), records its own
metrics, and publishes its own cache-invalidation targets on success. A failing
step is contained: the rest of the cycle still runs, and the cycle is reported
failed. MV refreshes are REFRESH MATERIALIZED VIEW CONCURRENTLY — no lock that
blocks readers, and the live listener keeps running.
"""

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

import redis.asyncio as aioredis

import leaderboard
import metrics
from config import config
from db import get_connection
from stream import publish_invalidation

_FAST_VIEWS = [
    "mv_kills_per_system",
    "mv_kills_per_system_daily",
    "mv_alliance_member_count",
]

_FAST_INVALIDATION = ["system_rankings", "system_kills", "global_kills"]

_SLOW_VIEWS = [
    "mv_farthest_kill_per_system",
    "mv_weapon_search",
    "mv_ship_search",
]

_SLOW_INVALIDATION = ["farthest_kill"]

_LEADERBOARD_INVALIDATION = ["leaderboards"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RefreshStep:
    name: str  # metric label: mv_refresh | entity_rollup | leaderboards
    run: Callable[[], bool]  # blocking; True = did work, False = skipped; raises = failed
    invalidation: list[str]  # published only when run() returns True


async def fast_refresh_scheduler(
    shutdown_event: asyncio.Event,
    redis: aioredis.Redis | None = None,
) -> None:
    """Run the fast steps on the fast cadence (config.refresh.mv_refresh_interval_minutes)."""
    await _refresh_loop(
        shutdown_event,
        next_run=_next_fast_refresh_time,
        steps=_fast_steps,
        cadence="fast",
        redis=redis,
    )


async def slow_refresh_scheduler(
    shutdown_event: asyncio.Event,
    redis: aioredis.Redis | None = None,
) -> None:
    """Run the slow steps weekly (config.refresh.day/hour)."""
    await _refresh_loop(
        shutdown_event,
        next_run=_next_slow_refresh_time,
        steps=_slow_steps,
        cadence="slow",
        redis=redis,
    )


def _fast_steps() -> list[RefreshStep]:
    steps: list[RefreshStep] = []
    if config.leaderboard.enabled:
        steps.append(RefreshStep("entity_rollup", _roll_entity_days, []))
        steps.append(
            RefreshStep(
                "leaderboards",
                lambda: _compute_leaderboards(leaderboard.FAST_WINDOWS),
                _LEADERBOARD_INVALIDATION,
            )
        )
    steps.append(
        RefreshStep("mv_refresh", lambda: _refresh_views(_FAST_VIEWS), _FAST_INVALIDATION)
    )
    return steps


def _slow_steps() -> list[RefreshStep]:
    steps: list[RefreshStep] = []
    if config.leaderboard.enabled:
        steps.append(
            RefreshStep(
                "leaderboards",
                lambda: _compute_leaderboards(leaderboard.SLOW_WINDOWS),
                _LEADERBOARD_INVALIDATION,
            )
        )
    steps.append(
        RefreshStep("mv_refresh", lambda: _refresh_views(_SLOW_VIEWS), _SLOW_INVALIDATION)
    )
    return steps


async def _refresh_loop(
    shutdown_event: asyncio.Event,
    *,
    next_run: Callable[[datetime], datetime],
    steps: Callable[[], list[RefreshStep]],
    cadence: str,
    redis: aioredis.Redis | None,
) -> None:
    """Wait for the next scheduled time, then run the cadence's steps in order.
    Shared by both cadences."""
    logger.info(f"{cadence} refresh scheduler started.")

    while not shutdown_event.is_set():
        now = datetime.now(timezone.utc)
        wait_seconds = (next_run(now) - now).total_seconds()

        logger.info(f"Next {cadence} refresh in {wait_seconds / 3600:.1f} hours.")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            pass

        await _run_cycle(steps(), cadence, redis)

    logger.info(f"{cadence} refresh scheduler stopped.")


async def _run_cycle(
    steps: list[RefreshStep], cadence: str, redis: aioredis.Redis | None
) -> bool:
    """Run every step in order (each in the executor), publishing a step's
    invalidation as soon as it succeeds. Returns True if any step failed."""
    start = time.monotonic()
    failed = False
    loop = asyncio.get_event_loop()
    for step in steps:
        outcome = await loop.run_in_executor(None, _run_step, step, cadence)
        if outcome == "failed":
            failed = True
        elif outcome == "success" and redis is not None and step.invalidation:
            await publish_invalidation(redis, step.invalidation)
    metrics.mv_refresh_duration_seconds.labels(cadence).observe(
        time.monotonic() - start
    )
    if failed:
        metrics.mv_refresh_runs.labels(cadence, "failed").inc()
    else:
        metrics.mv_refresh_runs.labels(cadence, "success").inc()
        metrics.mv_refresh_last_success_timestamp.labels(cadence).set_to_current_time()
    return failed


def _run_step(step: RefreshStep, cadence: str) -> str:
    """Run one step, contain any exception, record its metrics. Returns
    "success" (did work), "skipped" (nothing to do), or "failed"."""
    start = time.monotonic()
    try:
        did_work = step.run()
    except Exception as e:
        metrics.refresh_step_runs.labels(cadence, step.name, "failed").inc()
        metrics.refresh_step_duration_seconds.labels(cadence, step.name).observe(
            time.monotonic() - start
        )
        metrics.errors.labels(step.name).inc()
        logger.error(f"{cadence} refresh step {step.name} failed: {e}", exc_info=True)
        return "failed"
    outcome = "success" if did_work else "skipped"
    metrics.refresh_step_runs.labels(cadence, step.name, outcome).inc()
    metrics.refresh_step_duration_seconds.labels(cadence, step.name).observe(
        time.monotonic() - start
    )
    return outcome


def _next_slow_refresh_time(now: datetime) -> datetime:
    target = now.replace(hour=config.refresh.hour, minute=0, second=0, microsecond=0)

    days_ahead = config.refresh.day - now.weekday()
    if days_ahead < 0 or (days_ahead == 0 and target <= now):
        days_ahead += 7

    return target + timedelta(days=days_ahead)


def _next_fast_refresh_time(now: datetime) -> datetime:
    """Next wall-clock boundary that is a whole multiple of the fast interval
    since midnight UTC (e.g. 30 -> :00 and :30). The schedule re-anchors at
    midnight, so an interval that does not divide evenly into 1440 min leaves a
    short final bucket before 00:00; 30/60/120/360 divide cleanly."""
    interval = timedelta(minutes=config.refresh.mv_refresh_interval_minutes)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    n = int((now - midnight) // interval) + 1
    return midnight + n * interval


def _configure_refresh_session(conn) -> None:
    """Session settings for every refresh-side connection: a large work_mem
    (dedicated connection, so it is safe) and TimeZone=UTC so CURRENT_DATE is
    the UTC day the rollups are grouped on. Committed, so a later rolled-back
    transaction on this connection cannot revert them."""
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT set_config('work_mem', %s, false)", (config.refresh.work_mem,)
        )
        cursor.execute("SELECT set_config('TimeZone', 'UTC', false)")
    conn.commit()


def _refresh_views(views: list[str]) -> bool:
    with get_connection() as conn:
        conn.autocommit = True  # REFRESH ... CONCURRENTLY cannot run in a transaction
        _configure_refresh_session(conn)
        with conn.cursor() as cursor:
            for view in views:
                logger.info(f"Refreshing materialized view {view}...")
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
                logger.info(f"Refreshed {view}.")
    return True


def _roll_entity_days() -> bool:
    with get_connection() as conn:
        _configure_refresh_session(conn)
        return leaderboard.roll_dirty_days(conn)


def _compute_leaderboards(windows: list[str]) -> bool:
    with get_connection() as conn:
        _configure_refresh_session(conn)
        return leaderboard.compute_boards(conn, windows)
