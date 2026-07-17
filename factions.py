"""Faction reference data.

Factions are a small, near-static set (~100 NPC factions). We fetch the whole
list from ESI at startup and on a slow cadence, so factions never enter the
per-kill resolve path.

Design: docs/superpowers/specs/2026-07-17-entity-enrichment-precompute-design.md
"""

from __future__ import annotations

import asyncio
import logging

import db
import metrics
from config import config

logger = logging.getLogger(__name__)


def faction_rows(data: list[dict]) -> list[tuple[int, str]]:
    return [(f["faction_id"], f["name"]) for f in data]


async def _refresh_factions(esi) -> None:
    data = await esi.get_factions()
    rows = faction_rows(data)
    if not rows:
        metrics.factions_refreshed.labels("failed").inc()
        logger.warning("Faction refresh returned no rows; skipping upsert.")
        return
    with db.get_connection() as conn:
        db.upsert_factions(conn, rows)
    metrics.factions_refreshed.labels("success").inc()
    logger.info("Refreshed %d factions.", len(rows))


async def faction_scheduler(esi, shutdown_event) -> None:
    logger.info(
        "Faction scheduler started (every %d days).", config.factions.refresh_days
    )
    interval = config.factions.refresh_days * 86400
    # Refresh once at startup so the table is populated before kills reference it.
    try:
        await _refresh_factions(esi)
    except Exception as e:
        metrics.errors.labels("factions").inc()
        logger.error("Initial faction refresh failed: %s", e, exc_info=True)

    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass
        try:
            await _refresh_factions(esi)
        except Exception as e:
            metrics.errors.labels("factions").inc()
            logger.error("Faction refresh failed: %s", e, exc_info=True)
    logger.info("Faction scheduler stopped.")
