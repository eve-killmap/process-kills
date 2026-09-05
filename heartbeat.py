"""Uptime Kuma push heartbeat.

Pushes to Uptime Kuma only when the live listener has actually processed kills
since the previous check, so a received heartbeat means the pipeline is doing
work -- not merely that the process is alive. During a lull (R2Z2 has no new
sequence yet) or an outage nothing is sent, and Uptime Kuma's own missed-
heartbeat timeout flips the monitor down.

Enabled by setting UPTIME_KUMA_PUSH_URL in .env (its presence is the switch);
the check cadence is config.heartbeat.interval.
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlencode

import aiohttp

import metrics
from config import config

logger = logging.getLogger(__name__)

# Live outcomes that count as "the pipeline did work": a real sequence entry was
# pulled from R2Z2 and handled. "skipped" (missing id/hash, no ESI data) is a
# degenerate case, not healthy processing, so it is excluded.
_WORK_OUTCOMES = frozenset({"inserted", "no_position", "duplicate"})
_PUSH_TIMEOUT = aiohttp.ClientTimeout(total=10)


def _live_work_total() -> float:
    """Cumulative count of live kills processed (work outcomes only), read from
    the existing kills_processed counter so the live path needs no extra hook."""
    total = 0.0
    for metric in metrics.kills_processed.collect():
        for sample in metric.samples:
            if (
                sample.name == "eve_killmap_kills_processed_total"
                and sample.labels.get("source") == "live"
                and sample.labels.get("outcome") in _WORK_OUTCOMES
            ):
                total += sample.value
    return total


def _build_push_url(url: str, status: str, msg: str) -> str:
    """Append Uptime Kuma's standard status/msg/ping params. Any query string
    already on the base URL is dropped first, so a full example URL copied from
    Kuma works as well as the bare push URL."""
    base = url.split("?", 1)[0]
    return f"{base}?{urlencode({'status': status, 'msg': msg, 'ping': ''})}"


async def _send(session: aiohttp.ClientSession, url: str) -> None:
    """Fire one push. Never raises; the push URL (a secret) is never logged."""
    try:
        async with session.get(url, timeout=_PUSH_TIMEOUT) as resp:
            if resp.status == 200:
                metrics.heartbeat_pushes.labels("success").inc()
            else:
                metrics.heartbeat_pushes.labels("failed").inc()
                logger.warning("Heartbeat push returned HTTP %s.", resp.status)
    except Exception as e:
        metrics.heartbeat_pushes.labels("failed").inc()
        logger.warning("Heartbeat push failed: %s", e)


async def _tick(
    session: aiohttp.ClientSession, url: str, last_seen: float, interval: int
) -> float:
    """One heartbeat check: push 'up' iff live work advanced since last_seen.
    Returns the new baseline (whether or not a push was sent)."""
    current = _live_work_total()
    delta = current - last_seen
    if delta > 0:
        await _send(session, _build_push_url(url, "up", f"{int(delta)} kills/{interval}s"))
    return current


async def heartbeat_scheduler(shutdown_event: asyncio.Event) -> None:
    url = config.uptime_kuma_push_url
    if not url:
        logger.info("Uptime Kuma heartbeat disabled (UPTIME_KUMA_PUSH_URL unset).")
        return
    interval = config.heartbeat.interval
    logger.info(
        "Uptime Kuma heartbeat enabled (every %ds, pushes on live kills processed).",
        interval,
    )
    last_seen = _live_work_total()
    async with aiohttp.ClientSession() as session:
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass
            try:
                last_seen = await _tick(session, url, last_seen, interval)
            except Exception as e:
                metrics.errors.labels("heartbeat").inc()
                logger.error("Heartbeat scheduler error: %s", e, exc_info=True)
    logger.info("Uptime Kuma heartbeat scheduler stopped.")
