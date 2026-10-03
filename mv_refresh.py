import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

import redis.asyncio as aioredis

import leaderboard
import metrics
import rollups
from config import config
from db import get_connection
from stream import publish_invalidation

_FAST_VIEWS = [
    "mv_kills_per_system",
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
    name: str
    run: Callable[[], bool]
    invalidation: list[str]


async def fast_refresh_scheduler(
    shutdown_event: asyncio.Event,
    redis: aioredis.Redis | None = None,
) -> None:
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
    await _refresh_loop(
        shutdown_event,
        next_run=_next_slow_refresh_time,
        steps=_slow_steps,
        cadence="slow",
        redis=redis,
    )


def _fast_steps(stop: Callable[[], bool] | None = None) -> list[RefreshStep]:
    # Not gated by leaderboard.enabled: the backend's panels read system_kills_daily
    steps: list[RefreshStep] = [
        RefreshStep("rollups", lambda: _roll_dirty_days(stop), _FAST_INVALIDATION),
    ]
    if config.leaderboard.enabled:
        steps.append(
            RefreshStep(
                "leaderboards",
                lambda: _compute_leaderboards(leaderboard.FAST_WINDOWS, stop),
                _LEADERBOARD_INVALIDATION,
            )
        )
    steps.append(
        RefreshStep(
            "mv_refresh", lambda: _refresh_views(_FAST_VIEWS, stop), _FAST_INVALIDATION
        )
    )
    return steps


def _slow_steps(stop: Callable[[], bool] | None = None) -> list[RefreshStep]:
    steps: list[RefreshStep] = []
    if config.leaderboard.enabled:
        steps.append(
            RefreshStep(
                "leaderboards",
                lambda: _compute_leaderboards(leaderboard.SLOW_WINDOWS, stop),
                _LEADERBOARD_INVALIDATION,
            )
        )
    steps.append(
        RefreshStep(
            "mv_refresh", lambda: _refresh_views(_SLOW_VIEWS, stop), _SLOW_INVALIDATION
        )
    )
    return steps


async def _refresh_loop(
    shutdown_event: asyncio.Event,
    *,
    next_run: Callable[[datetime], datetime],
    steps: Callable[[Callable[[], bool]], list[RefreshStep]],
    cadence: str,
    redis: aioredis.Redis | None,
) -> None:
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

        try:
            await _run_cycle(
                steps(shutdown_event.is_set), cadence, redis, stop=shutdown_event.is_set
            )
        except Exception as e:
            metrics.errors.labels("mv_refresh").inc()
            logger.error(f"{cadence} refresh cycle crashed: {e}", exc_info=True)

    logger.info(f"{cadence} refresh scheduler stopped.")


async def _run_cycle(
    steps: list[RefreshStep],
    cadence: str,
    redis: aioredis.Redis | None,
    stop: Callable[[], bool] | None = None,
) -> bool:
    start = time.monotonic()
    failed = False
    ran = 0
    loop = asyncio.get_running_loop()
    for step in steps:
        if stop is not None and stop():
            logger.info(
                f"{cadence} refresh: shutdown requested; skipping remaining steps."
            )
            break
        outcome = await loop.run_in_executor(None, _run_step, step, cadence)
        ran += 1
        if outcome == "failed":
            failed = True
        elif outcome == "success" and redis is not None and step.invalidation:
            await publish_invalidation(redis, step.invalidation)
    if ran == 0:
        return False
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
    target = now.replace(
        hour=config.refresh.slow_refresh_hour,
        minute=config.refresh.slow_refresh_minute,
        second=0,
        microsecond=0,
    )
    if target <= now:
        target += timedelta(days=1)
    return target


def _next_fast_refresh_time(now: datetime) -> datetime:
    interval = timedelta(minutes=config.refresh.mv_refresh_interval_minutes)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    n = int((now - midnight) // interval) + 1
    return midnight + n * interval


def _configure_refresh_session(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT set_config('work_mem', %s, false)", (config.refresh.work_mem,)
        )
        cursor.execute("SELECT set_config('TimeZone', 'UTC', false)")
    conn.commit()


def _refresh_views(views: list[str], stop: Callable[[], bool] | None = None) -> bool:
    refreshed = 0
    with get_connection() as conn:
        conn.autocommit = True  # REFRESH ... CONCURRENTLY cannot run in a transaction
        _configure_refresh_session(conn)
        with conn.cursor() as cursor:
            for view in views:
                if stop is not None and stop():
                    logger.info(
                        "Shutdown requested: %d of %d views refreshed; the rest next cycle.",
                        refreshed,
                        len(views),
                    )
                    break
                logger.info(f"Refreshing materialized view {view}...")
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
                refreshed += 1
                logger.info(f"Refreshed {view}.")
    return refreshed > 0


def _roll_dirty_days(stop: Callable[[], bool] | None = None) -> bool:
    with get_connection() as conn:
        _configure_refresh_session(conn)
        return rollups.roll_dirty_days(conn, should_stop=stop)


def _compute_leaderboards(
    windows: list[str], stop: Callable[[], bool] | None = None
) -> bool:
    with get_connection() as conn:
        _configure_refresh_session(conn)
        return leaderboard.compute_boards(conn, windows, should_stop=stop)
