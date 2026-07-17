"""War info resolution (decoupled, rate-limited).

Unlike entity names, wars are not needed for the live broadcast and the ESI war
endpoint IS rate limited, so they are resolved by a background scheduler. A war
stub row (war_id only, refresh_after = now) is the queue entry; this module
fetches, parses, and upserts it, then sets refresh_after (NULL once terminal).

Design: docs/superpowers/specs/2026-07-17-entity-enrichment-precompute-design.md
"""

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
    """ESI war JSON -> a dict keyed like the `wars` table columns.

    Timestamp fields are left as ESI's ISO strings; psycopg2 casts them to
    TIMESTAMPTZ on insert. Missing fields become None.
    """
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
    """When to re-fetch a war. None means terminal (never again).

    - No `finished`: active -> honour ESI's Expires header.
    - `finished` in the future: ends later -> one more fetch just after it ends.
    - `finished` in the past: over -> None.
    """
    if finished is None:
        return expires
    if finished > now:
        return finished + timedelta(minutes=1)
    return None


def war_outcome(refresh_after) -> str:
    return "finished" if refresh_after is None else "active"


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
    data = await esi.fetch_war(war_id)
    if data is None:
        # 404 / unresolvable: mark terminal so we stop re-queuing it.
        with db.get_connection() as conn:
            db.upsert_war(conn, parse_war({"id": war_id}), None, None)
        metrics.wars_resolved.labels("not_found").inc()
        return
    row = parse_war(data)
    finished = _parse_iso(row["finished"])
    now = datetime.now(timezone.utc)
    # War fetch's Expires header is not surfaced by fetch_war; fall back to a
    # fixed active-war cadence when we cannot read it.
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
