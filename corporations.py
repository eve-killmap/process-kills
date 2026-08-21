"""Corporation metadata refresh (rolling, self-throttling).

One fetch/parse/upsert/reschedule path (refresh_corporations) shared by inline
ingestion (entities.resolve_and_store), the background scheduler, and the local
one-off backfill. A refresh that finds active=false freezes the corp terminal
(refresh_after = NULL): a closed corp's metadata is immutable forever.

Design: docs/superpowers/specs/2026-08-20-corporation-alliance-metadata-design.md
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import db
import metrics
from config import config

logger = logging.getLogger(__name__)

ACTIVE_REFRESH_INTERVAL = timedelta(hours=24)
CORP_RETRY_BACKOFF = timedelta(minutes=10)
CORP_REFRESH_JITTER_SECONDS = 3600  # spread a tick's batch so it doesn't re-clump


def parse_corporation(data: Mapping[str, Any]) -> dict:
    """ESI corp JSON -> corporations column dict. Missing fields -> None.

    `active` is True iff the ESI `state` field equals "active" (VERIFY the field
    name/values against a live fetch before the mass backfill; see the spec).
    """
    return {
        "name": data.get("name"),
        "ticker": data.get("ticker"),
        "alliance_id": data.get("alliance_id"),
        "date_founded": data.get("date_founded"),
        "member_count": data.get("member_count"),
        "active": data.get("state") == "active",
    }


def compute_corp_refresh_after(
    active: bool, now: datetime, jitter: timedelta
) -> datetime | None:
    """Success-path reschedule: active -> now+24h(+jitter); closed -> None (terminal)."""
    if not active:
        return None
    return now + ACTIVE_REFRESH_INTERVAL + jitter


async def refresh_corporations(
    conn, esi, corp_ids: Iterable[int], concurrency: int
) -> None:
    """Fetch + parse + upsert + reschedule a batch of corp ids. Never raises per
    corp. Shared by inline ingestion, the scheduler, and the local backfill."""
    corp_ids = list(corp_ids)
    if not corp_ids:
        return
    sem = asyncio.Semaphore(concurrency)

    async def one(cid: int):
        async with sem:
            try:
                data = await esi.get_corporation(cid)
                if data is None:
                    return cid, "not_found", None
                return cid, "ok", parse_corporation(data)
            except Exception:
                return cid, "error", None

    start = time.monotonic()
    results = await asyncio.gather(*[one(c) for c in corp_ids])
    now = datetime.now(timezone.utc)
    ok_rows = []
    for cid, outcome, parsed in results:
        if outcome == "ok":
            active = parsed["active"]
            jitter = timedelta(seconds=random.uniform(0, CORP_REFRESH_JITTER_SECONDS))
            refresh_after = compute_corp_refresh_after(active, now, jitter)
            ok_rows.append(
                (
                    cid,
                    parsed["name"],
                    parsed["ticker"],
                    parsed["alliance_id"],
                    parsed["date_founded"],
                    parsed["member_count"],
                    active,
                    refresh_after,
                )
            )
            metrics.corporations_refreshed.labels("active" if active else "closed").inc()
        elif outcome == "not_found":
            db.mark_corporation_closed(conn, cid)
            metrics.corporations_refreshed.labels("not_found").inc()
        else:  # transient error -> retry soon, leave data untouched
            db.set_corporation_refresh_after(conn, cid, now + CORP_RETRY_BACKOFF)
            metrics.corporations_refreshed.labels("error").inc()
    if ok_rows:
        db.upsert_corporations(conn, ok_rows)
    metrics.corporation_refresh_seconds.observe(time.monotonic() - start)


async def _refresh_due_batch(esi) -> None:
    with db.get_connection() as conn:
        metrics.corporations_pending.set(db.count_due_corporations(conn))
        due = db.get_due_corporations(conn, config.corporations.batch_size)
    if not due:
        return
    with db.get_connection() as conn:
        await refresh_corporations(conn, esi, due, config.corporations.max_concurrency)


async def corporation_refresh_scheduler(esi, shutdown_event) -> None:
    if not config.corporations.enabled:
        logger.info("Corporation refresh scheduler disabled (corporations.enabled=false).")
        return
    logger.info(
        "Corporation refresh scheduler started (every %ds, batch %d).",
        config.corporations.interval,
        config.corporations.batch_size,
    )
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=config.corporations.interval
            )
            break
        except asyncio.TimeoutError:
            pass
        try:
            await _refresh_due_batch(esi)
        except Exception as e:
            metrics.errors.labels("corporations").inc()
            logger.error("Corporation refresh scheduler error: %s", e, exc_info=True)
    logger.info("Corporation refresh scheduler stopped.")
