"""Incremental daily rollups of the kills table, recomputed one UTC day at a time.

Two rollups share one mechanism:

  - entity_kills_daily  (facet_kind, role, facet_value, day) -> kill_count, from
    kill_facets; feeds the entity leaderboards (leaderboard.py).
  - system_kills_daily  (solar_system_id, day) -> kill_count, from kills; the
    backend's top-systems / system-kills / global-kills panels read it, and
    mv_kills_per_system (all-time) is a cheap aggregate over it.

Design: docs/superpowers/specs/2026-09-12-entity-leaderboards-design.md

Dirty days are derived from kills.inserted_time against a single watermark in
rollup_state, so every path that inserts into kills is covered without any code
at the insert sites, and a late-arriving kill is attributed to the day it
happened, not the day it arrived.

Timezone policy: "day" is killmail_time::date, and a day's bounds are plain
date casts — no AT TIME ZONE anywhere — so every session that touches these
tables MUST run with TimeZone = UTC. mv_refresh pins it on the refresh
connection; the backfill/completion script pins it too; maintenance SQL run by
hand must be run in a UTC session (SET timezone = 'UTC').

A facet-only repair (rows added to
kill_facets for existing kills) does not touch kills.inserted_time; re-roll
those days by hand or reset rollup_state. Nothing here triggers the historical
build; that is a hand-run statement per table (see sql/README.md) plus the
local completion script.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta

import metrics
from db import get_cursor

logger = logging.getLogger(__name__)

KINDS = [1, 2, 3, 4, 5, 6]  # character, corporation, alliance, faction, ship, weapon
ROLES = [0, 1]  # victim, attacker

# rollup_state key of the shared watermark. The stored value predates the system
# rollup joining the mechanism; it is an internal key, not a table name.
WATERMARK_NAME = "entity_kills_daily"

# A kill and its facet rows are committed in separate transactions
# (insert_kill, then insert_facets), so a roll can land between them and count
# a kill with no facets; because that kill's inserted_time is only seconds
# old, the overlap guarantees the day is re-rolled next cycle. 1h also covers
# any hand-run bulk load whose transaction is shorter than that.
DIRTY_OVERLAP = timedelta(hours=1)
# Not a hard cap — see roll_dirty_days for why a cap would stall the
# watermark. Just the threshold to warn that a cycle is unusually large
# (steady state is 1-2 dirty days).
DIRTY_DAYS_WARN = 10

_ENTITY_DELETE = (
    "DELETE FROM entity_kills_daily "
    "WHERE facet_kind = ANY(%(kinds)s) AND role = ANY(%(roles)s) AND day = %(day)s"
)
# kills via idx_kills_time_covering; kill_facets via ONE contiguous range of idx_facet_kill:
# killmail ids are monotonic in time, so a day's facet rows are adjacent in that
# index, and bounding the scan by the day's [min, max] killmail_id makes it a
# sequential index slice (hot for today) instead of a 1B-row table scan or
# per-kill random probes — on the production box the planner picks the table
# scan for the unbounded join. The join to kills stays as the exact day filter
# (ids at day boundaries interleave). No DISTINCT: collect_facets already dedups
# per kill.
# The bounds come from a subquery with OFFSET 0 — an optimization fence. A bare
# min/max over kills lets the planner walk kills_pkey in id order and stop at
# the first row inside the day; ids correlate with time, so for an old day that
# steps over tens of millions of rows first (measured 14 s). Fenced, the day's
# rows come from the killmail_time index range instead.
# The one definition of "the kills of day D", shared by every per-day statement
# so the two rollups (and the invariant between them) cannot drift apart. With
# the session in UTC these are the UTC day's bounds (module docstring).
_DAY_RANGE = (
    "k.killmail_time >= %(day)s::timestamptz "
    "AND k.killmail_time < (%(day)s + 1)::timestamptz"
)
_ENTITY_BOUNDS = (
    "SELECT min(killmail_id), max(killmail_id) FROM ("
    f"SELECT killmail_id FROM kills k WHERE {_DAY_RANGE} "
    "OFFSET 0) AS day_kills"
)
_ENTITY_INSERT = (
    "INSERT INTO entity_kills_daily (facet_kind, role, day, facet_value, kill_count) "
    "SELECT f.facet_kind, f.role, %(day)s, f.facet_value, COUNT(*) "
    "FROM kills k JOIN kill_facets f USING (killmail_id) "
    f"WHERE {_DAY_RANGE} "
    "AND f.killmail_id BETWEEN %(lo)s AND %(hi)s "
    "AND f.facet_kind = ANY(%(kinds)s) "
    "AND f.role = ANY(%(roles)s) "
    "GROUP BY f.facet_kind, f.role, f.facet_value"
)

_SYSTEM_DELETE = "DELETE FROM system_kills_daily WHERE day = %(day)s"
# One day of kills (a few thousand rows), index-only via idx_kills_time_covering
# (or the skip scan over the (solar_system_id, killmail_time) covering index).
_SYSTEM_INSERT = (
    "INSERT INTO system_kills_daily (solar_system_id, day, kill_count) "
    "SELECT k.solar_system_id, %(day)s, COUNT(*) FROM kills k "
    f"WHERE {_DAY_RANGE} "
    "GROUP BY k.solar_system_id"
)


def roll_entity_day(conn, day: date) -> int:
    """Recompute one UTC day of entity_kills_daily in a single transaction.
    Idempotent. Returns the number of rollup rows written (0 for a day with no
    kills, which then also holds no rows)."""
    params = {"kinds": KINDS, "roles": ROLES, "day": day}
    try:
        with get_cursor(conn) as cursor:
            cursor.execute(_ENTITY_DELETE, params)
            cursor.execute(_ENTITY_BOUNDS, params)
            lo, hi = cursor.fetchone()
            written = 0
            if lo is not None:
                cursor.execute(_ENTITY_INSERT, {**params, "lo": lo, "hi": hi})
                written = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return written


def roll_system_day(conn, day: date) -> int:
    """Recompute one UTC day of system_kills_daily in a single transaction.
    Idempotent. Returns the number of rollup rows written."""
    params = {"day": day}
    try:
        with get_cursor(conn) as cursor:
            cursor.execute(_SYSTEM_DELETE, params)
            cursor.execute(_SYSTEM_INSERT, params)
            written = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return written


def read_watermark(conn) -> datetime | None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            "SELECT watermark FROM rollup_state WHERE name = %s", (WATERMARK_NAME,)
        )
        row = cursor.fetchone()
    return row[0] if row else None


def set_watermark(conn, ts: datetime) -> None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            "INSERT INTO rollup_state (name, watermark) VALUES (%s, %s) "
            "ON CONFLICT (name) DO UPDATE SET watermark = EXCLUDED.watermark",
            (WATERMARK_NAME, ts),
        )
    conn.commit()


def db_now(conn) -> datetime:
    """The database clock — the watermark must never come from the app clock."""
    with get_cursor(conn) as cursor:
        cursor.execute("SELECT now()")
        return cursor.fetchone()[0]


def find_dirty_days(conn, since: datetime) -> list[date]:
    """UTC days with at least one kill inserted after `since`, oldest first."""
    with get_cursor(conn) as cursor:
        cursor.execute(
            "SELECT DISTINCT killmail_time::date "
            "FROM kills WHERE inserted_time > %s ORDER BY 1",
            (since,),
        )
        return [row[0] for row in cursor.fetchall()]


def roll_dirty_days(conn, should_stop: Callable[[], bool] | None = None) -> bool:
    """Fast-cycle step: recompute both rollups for every UTC day with kills
    inserted since the watermark (minus DIRTY_OVERLAP), then advance the
    watermark to the DB time captured before the scan. Returns False (skipped)
    when no watermark exists — the historical build has not run. Raises on
    failure, leaving the watermark untouched so the next cycle retries the
    same set (each per-day roll is its own committed transaction, so a partly
    rolled day is simply re-rolled).

    `should_stop` is polled between days (service shutdown): rolling stops,
    the watermark is NOT advanced, and the remaining days are picked up by the
    next cycle. Returns True if any day was rolled before stopping."""
    watermark = read_watermark(conn)
    if watermark is None:
        logger.info("No rollup watermark; run sql/backfill_entity_rollup.py. Skipping.")
        return False
    metrics.rollup_watermark_timestamp.set(watermark.timestamp())
    t0 = db_now(conn)
    days = find_dirty_days(conn, watermark - DIRTY_OVERLAP)
    if len(days) > DIRTY_DAYS_WARN:
        logger.warning(
            "Rollups: %d dirty days this cycle (steady state is 1-2); a bulk "
            "historical load will make this cycle slow.", len(days)
        )
    rolled = 0
    for day in days:
        if should_stop is not None and should_stop():
            logger.info(
                "Shutdown requested: %d of %d dirty days rolled; watermark not "
                "advanced, the rest re-roll next cycle.", rolled, len(days)
            )
            return rolled > 0
        start = time.monotonic()
        entity_rows = roll_entity_day(conn, day)
        system_rows = roll_system_day(conn, day)
        rolled += 1
        metrics.rollup_days_rolled.inc()
        logger.info(
            "Rolled %s: %d entity rows, %d system rows in %.1fs.",
            day,
            entity_rows,
            system_rows,
            time.monotonic() - start,
        )
    set_watermark(conn, t0)
    metrics.rollup_watermark_timestamp.set(t0.timestamp())
    return True
