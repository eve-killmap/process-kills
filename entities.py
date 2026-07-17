"""Entity name resolution at ingestion time.

IDs seen on a kill (victim + attackers) are resolved to names/tickers via ESI
and upserted into the reference tables. Factions are excluded here -- they are
fully prepopulated by the faction scheduler, so nothing resolves them per kill.

Design: docs/superpowers/specs/2026-07-17-entity-enrichment-precompute-design.md
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from schema import ParsedKill

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntityIds:
    characters: frozenset[int]
    corporations: frozenset[int]
    alliances: frozenset[int]

    @property
    def is_empty(self) -> bool:
        return not (self.characters or self.corporations or self.alliances)


def collect_entity_ids(parsed: ParsedKill) -> EntityIds:
    """Character/corp/alliance ids referenced by a parsed kill (victim + attackers).

    Factions are intentionally excluded (prepopulated elsewhere). None ids drop.
    """
    characters: set[int] = set()
    corporations: set[int] = set()
    alliances: set[int] = set()

    if parsed["victim_character_id"] is not None:
        characters.add(parsed["victim_character_id"])
    if parsed["victim_corporation_id"] is not None:
        corporations.add(parsed["victim_corporation_id"])
    if parsed["victim_alliance_id"] is not None:
        alliances.add(parsed["victim_alliance_id"])

    for atk in parsed["attackers"]:
        if atk["character_id"] is not None:
            characters.add(atk["character_id"])
        if atk["corporation_id"] is not None:
            corporations.add(atk["corporation_id"])
        if atk["alliance_id"] is not None:
            alliances.add(atk["alliance_id"])

    return EntityIds(
        frozenset(characters), frozenset(corporations), frozenset(alliances)
    )


def character_rows(
    requested: set[int], names: dict[int, str]
) -> list[tuple[int, str | None]]:
    """One row per requested id; None name = tombstone (ESI had no answer)."""
    return [(cid, names.get(cid)) for cid in requested]


def group_rows(
    requested: set[int], info: dict[int, tuple[str, str] | None]
) -> list[tuple[int, str | None, str | None]]:
    """One (id, name, ticker) row per requested id; (None, None) if unresolved."""
    rows: list[tuple[int, str | None, str | None]] = []
    for gid in requested:
        entry = info.get(gid)
        if entry is None:
            rows.append((gid, None, None))
        else:
            rows.append((gid, entry[0], entry[1]))
    return rows


import asyncio
import time
from datetime import datetime, timedelta, timezone

import db
import metrics
from config import config


def find_unfresh(
    conn, ids: EntityIds, now: datetime, refresh_after_days: int
) -> EntityIds:
    """Return the subset of ids that are absent or older than the TTL."""
    cutoff = now - timedelta(days=refresh_after_days)
    fresh_c = db.get_fresh_character_ids(conn, list(ids.characters), cutoff)
    fresh_corp = db.get_fresh_corporation_ids(conn, list(ids.corporations), cutoff)
    fresh_alli = db.get_fresh_alliance_ids(conn, list(ids.alliances), cutoff)
    return EntityIds(
        ids.characters - frozenset(fresh_c),
        ids.corporations - frozenset(fresh_corp),
        ids.alliances - frozenset(fresh_alli),
    )


async def resolve_and_store(
    conn, esi, unfresh: EntityIds, max_concurrency: int, backfill: bool = False
) -> None:
    """Resolve unfresh ids via ESI and upsert them (tombstoning failures)."""
    if unfresh.characters:
        start = time.monotonic()
        names = await esi.resolve_names(set(unfresh.characters))
        db.upsert_characters(conn, character_rows(set(unfresh.characters), names))
        metrics.entity_resolve_seconds.labels("character").observe(
            time.monotonic() - start
        )
        _count_name_outcomes("character", unfresh.characters, names, backfill)

    sem = asyncio.Semaphore(max_concurrency)

    async def one(kind, fetch, gid):
        async with sem:
            try:
                return gid, await fetch(gid)
            except Exception:
                metrics.entities_resolved.labels(kind, "error").inc()
                metrics.errors.labels("entities").inc()
                return gid, None

    if unfresh.corporations:
        start = time.monotonic()
        results = await asyncio.gather(
            *[one("corporation", esi.get_corporation, c) for c in unfresh.corporations]
        )
        info = {gid: val for gid, val in results}
        db.upsert_corporations(conn, group_rows(set(unfresh.corporations), info))
        metrics.entity_resolve_seconds.labels("corporation").observe(
            time.monotonic() - start
        )
        _count_group_outcomes("corporation", info, backfill)

    if unfresh.alliances:
        start = time.monotonic()
        results = await asyncio.gather(
            *[one("alliance", esi.get_alliance, a) for a in unfresh.alliances]
        )
        info = {gid: val for gid, val in results}
        db.upsert_alliances(conn, group_rows(set(unfresh.alliances), info))
        metrics.entity_resolve_seconds.labels("alliance").observe(
            time.monotonic() - start
        )
        _count_group_outcomes("alliance", info, backfill)


def _count_name_outcomes(kind, requested, names, backfill):
    for cid in requested:
        outcome = "resolved" if cid in names else "not_found"
        metrics.entities_resolved.labels(kind, outcome).inc()
        if backfill and outcome == "resolved":
            metrics.entities_backfilled.labels(kind).inc()


def _count_group_outcomes(kind, info, backfill):
    for gid, val in info.items():
        if val is None:
            metrics.entities_resolved.labels(kind, "not_found").inc()
        else:
            metrics.entities_resolved.labels(kind, "resolved").inc()
            if backfill:
                metrics.entities_backfilled.labels(kind).inc()


async def ensure_kill_entities(conn, esi, parsed: ParsedKill) -> bool:
    """Resolve a kill's entities inline. Never raises. False if queued for retry."""
    ids = collect_entity_ids(parsed)
    if ids.is_empty:
        return True
    try:
        unfresh = find_unfresh(
            conn, ids, datetime.now(timezone.utc), config.entities.refresh_after_days
        )
        if unfresh.is_empty:
            return True
        await asyncio.wait_for(
            resolve_and_store(conn, esi, unfresh, config.entities.max_concurrency),
            timeout=config.entities.resolve_timeout,
        )
        return True
    except Exception as e:
        metrics.entity_resolve_timeouts.inc()
        metrics.errors.labels("entities").inc()
        logger.warning(
            "Entity resolve failed for kill %s (queued for retry): %s",
            parsed["killmail_id"],
            e,
        )
        try:
            db.enqueue_entity_backlog(conn, parsed["killmail_id"])
        except Exception:
            logger.exception("Failed to enqueue entity backlog")
        return False


async def entity_backlog_scheduler(esi, shutdown_event) -> None:
    """Drain entity_resolve_backlog: re-resolve kills whose inline pass failed."""
    logger.info(
        "Entity backlog scheduler started (every %ds).",
        config.entities.backlog_interval,
    )
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=config.entities.backlog_interval
            )
            break
        except asyncio.TimeoutError:
            pass
        try:
            from db import get_connection

            with get_connection() as conn:
                metrics.entity_backlog_depth.set(db.count_entity_backlog(conn))
                batch = db.get_entity_backlog(conn, config.entities.max_concurrency)
            for killmail_id, _attempts in batch:
                with get_connection() as conn:
                    ids = db.get_kill_entity_ids(conn, killmail_id)
                    unfresh = find_unfresh(
                        conn,
                        ids,
                        datetime.now(timezone.utc),
                        config.entities.refresh_after_days,
                    )
                    await resolve_and_store(
                        conn, esi, unfresh, config.entities.max_concurrency
                    )
                    db.delete_entity_backlog(conn, killmail_id)
        except Exception as e:
            metrics.errors.labels("entity_backlog").inc()
            logger.error("Entity backlog drain error: %s", e, exc_info=True)
    logger.info("Entity backlog scheduler stopped.")
