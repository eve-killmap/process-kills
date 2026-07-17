"""Standalone historical entity backfill.

Resolves names/tickers for every distinct character/corporation/alliance id in
the existing kills/kill_attackers tables, reusing the same resolve+upsert code
the live path uses. Idempotent and resumable: already-fresh ids are skipped, and
each kind walks id ranges by an ascending cursor. Runs to completion, then exits.
Safe to run concurrently with the live service (name endpoints are not rate
limited).

Usage:  python entities_backfill.py

Design: docs/superpowers/specs/2026-07-17-entity-enrichment-precompute-design.md
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import db
import entities
from config import config, require_database_url, setup_logging
from entities import EntityIds
from esi import ESIClient

logger = logging.getLogger(__name__)

_CHUNK = 5000


def next_cursor(ids: list[int]) -> int:
    return max(ids) if ids else 0


async def _backfill_kind(esi, reader, kind: str) -> None:
    after = 0
    total = 0
    while True:
        with db.get_connection() as conn:
            ids = reader(conn, after, _CHUNK)
        if not ids:
            break
        id_set = frozenset(ids)
        if kind == "character":
            unfresh_full = EntityIds(id_set, frozenset(), frozenset())
        elif kind == "corporation":
            unfresh_full = EntityIds(frozenset(), id_set, frozenset())
        else:
            unfresh_full = EntityIds(frozenset(), frozenset(), id_set)
        with db.get_connection() as conn:
            unfresh = entities.find_unfresh(
                conn, unfresh_full, datetime.now(timezone.utc),
                config.entities.refresh_after_days,
            )
            await entities.resolve_and_store(
                conn, esi, unfresh, config.entities.max_concurrency, backfill=True
            )
        total += len(ids)
        after = next_cursor(ids)
        logger.info("Backfill %s: processed up to id %d (%d seen).", kind, after, total)


async def run_backfill() -> None:
    setup_logging(config)
    require_database_url(config)
    shutdown = asyncio.Event()
    esi = ESIClient(shutdown)
    await esi.start()
    try:
        # Corporations dominate (one fetch each); do the cheap bulk kinds first.
        await _backfill_kind(esi, db.get_distinct_character_ids_after, "character")
        await _backfill_kind(esi, db.get_distinct_alliance_ids_after, "alliance")
        await _backfill_kind(esi, db.get_distinct_corporation_ids_after, "corporation")
        logger.info("Entity backfill complete.")
    finally:
        await esi.close()


if __name__ == "__main__":
    asyncio.run(run_backfill())
