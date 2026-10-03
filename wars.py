from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import db
import metrics
from config import config

logger = logging.getLogger(__name__)


def parse_war(data: Mapping[str, Any]) -> dict[str, Any]:
    aggressor = data.get("aggressor") or {}
    defender = data.get("defender") or {}
    allies = data.get("allies") or []

    ally_corporation_ids = [
        a["corporation_id"] for a in allies if a.get("corporation_id") is not None
    ]
    ally_alliance_ids = [
        a["alliance_id"] for a in allies if a.get("alliance_id") is not None
    ]

    return {
        "war_id": data["id"],
        "declared": data.get("declared"),
        "started": data.get("started"),
        "finished": data.get("finished"),
        "retracted": data.get("retracted"),
        "mutual": data.get("mutual"),
        "open_for_allies": data.get("open_for_allies"),
        "aggressor_corporation_id": aggressor.get("corporation_id"),
        "aggressor_alliance_id": aggressor.get("alliance_id"),
        "aggressor_ships_killed": aggressor.get("ships_killed"),
        "aggressor_isk_destroyed": aggressor.get("isk_destroyed"),
        "defender_corporation_id": defender.get("corporation_id"),
        "defender_alliance_id": defender.get("alliance_id"),
        "defender_ships_killed": defender.get("ships_killed"),
        "defender_isk_destroyed": defender.get("isk_destroyed"),
        "ally_corporation_ids": ally_corporation_ids,
        "ally_alliance_ids": ally_alliance_ids,
    }


def compute_refresh_after(
    finished: datetime | None, now: datetime, expires: datetime | None
) -> datetime | None:
    if finished is None:
        return expires
    if finished > now:
        return finished + timedelta(minutes=1)
    return None


def war_outcome(refresh_after) -> str:
    return "finished" if refresh_after is None else "active"


WAR_RETRY_BACKOFF = timedelta(minutes=10)


async def war_scheduler(esi, shutdown_event) -> None:
    if not config.wars.enabled:
        logger.info("War scheduler disabled (wars.enabled=false).")
        return
    logger.info(
        "War scheduler started (every %ds, batch %d).",
        config.wars.interval,
        config.wars.batch_size,
    )
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=config.wars.interval)
            break
        except asyncio.TimeoutError:
            pass
        try:
            with db.get_connection() as conn:
                metrics.wars_pending.set(db.count_due_wars(conn))
                due = db.get_due_wars(conn, config.wars.batch_size)
            for war_id in due:
                if shutdown_event.is_set():
                    break
                await _refresh_one_war(esi, war_id)
        except Exception as e:
            metrics.errors.labels("wars").inc()
            logger.error("War scheduler error: %s", e, exc_info=True)
    logger.info("War scheduler stopped.")


async def _refresh_one_war(esi, war_id: int) -> None:
    try:
        data = await esi.fetch_war(war_id)
    except Exception as e:
        logger.warning("War %s fetch failed transiently, will retry: %s", war_id, e)
        with db.get_connection() as conn:
            db.set_war_refresh_after(
                conn, war_id, datetime.now(timezone.utc) + WAR_RETRY_BACKOFF
            )
        metrics.wars_resolved.labels("error").inc()
        return
    if data is None:
        with db.get_connection() as conn:
            db.upsert_war(conn, parse_war({"id": war_id}), None, None)
        metrics.wars_resolved.labels("not_found").inc()
        return
    row = parse_war(data)
    finished = _parse_iso(row["finished"])
    now = datetime.now(timezone.utc)
    expires = now + timedelta(hours=6)
    refresh_after = compute_refresh_after(finished, now, expires)
    with db.get_connection() as conn:
        db.upsert_war(conn, row, now, refresh_after)
    metrics.wars_resolved.labels(war_outcome(refresh_after)).inc()


def _parse_iso(value) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
