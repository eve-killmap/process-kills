"""Entity leaderboards: an incremental per-entity daily rollup of kill_facets
(entity_kills_daily) and the precomputed top-N boards the backend reads
(entity_leaderboard).

Design: docs/superpowers/specs/2026-09-12-entity-leaderboards-design.md

The rollup is maintained by recomputing whole UTC days. Dirty days are derived
from kills.inserted_time against a watermark (rollup_state), so every insert
path (live, crosscheck, recheck, backfill, hand-run SQL) is covered without
any code at the insert sites. Nothing here triggers the historical build; that
is the local sql/ backfill script, which loops roll_day.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta

import metrics
from config import config
from db import get_cursor

logger = logging.getLogger(__name__)

KINDS = [1, 2, 3, 4, 5, 6]  # character, corporation, alliance, faction, ship, weapon
ROLES = [0, 1]  # victim, attacker
ROLLUP_NAME = "entity_kills_daily"

# NOW() is transaction-start time and ingestion holds its transaction open
# across a network wait, so a row can commit with an inserted_time older than a
# watermark captured meanwhile. Re-scanning this far back is free (idempotent).
DIRTY_OVERLAP = timedelta(hours=1)

# window_key -> interval literal for `day > CURRENT_DATE - %s::interval` — the
# backend's top-systems predicate, verbatim, so both panels agree at the edges.
# None = no day predicate (all-time).
WINDOWS: dict[str, str | None] = {
    "day": "1 day",
    "week": "7 days",
    "month": "30 days",
    "six_months": "6 months",
    "year": "1 year",
    "all": None,
}
FAST_WINDOWS = ["day", "week", "month", "six_months", "year"]
SLOW_WINDOWS = ["all"]

_ROLL_DELETE = (
    "DELETE FROM entity_kills_daily "
    "WHERE facet_kind = ANY(%(kinds)s) AND role = ANY(%(roles)s) AND day = %(day)s"
)
# kills via idx_kills_time, kill_facets via idx_facet_kill (a day's killmail ids
# are contiguous, so the join reads one contiguous slice of that index). No
# DISTINCT: collect_facets already dedups per kill.
_ROLL_INSERT = (
    "INSERT INTO entity_kills_daily (facet_kind, role, day, facet_value, kill_count) "
    "SELECT f.facet_kind, f.role, %(day)s, f.facet_value, COUNT(*) "
    "FROM kills k JOIN kill_facets f USING (killmail_id) "
    "WHERE k.killmail_time >= (%(day)s::timestamp AT TIME ZONE 'UTC') "
    "AND k.killmail_time < ((%(day)s + 1)::timestamp AT TIME ZONE 'UTC') "
    "AND f.facet_kind = ANY(%(kinds)s) "
    "GROUP BY f.facet_kind, f.role, f.facet_value"
)


def roll_day(conn, day: date) -> int:
    """Recompute one UTC day of entity_kills_daily in a single transaction.
    Idempotent. Returns the number of rollup rows written."""
    params = {"kinds": KINDS, "roles": ROLES, "day": day}
    try:
        with get_cursor(conn) as cursor:
            cursor.execute(_ROLL_DELETE, params)
            cursor.execute(_ROLL_INSERT, params)
            written = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return written


def read_watermark(conn) -> datetime | None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            "SELECT watermark FROM rollup_state WHERE name = %s", (ROLLUP_NAME,)
        )
        row = cursor.fetchone()
    return row[0] if row else None


def set_watermark(conn, ts: datetime) -> None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            "INSERT INTO rollup_state (name, watermark) VALUES (%s, %s) "
            "ON CONFLICT (name) DO UPDATE SET watermark = EXCLUDED.watermark",
            (ROLLUP_NAME, ts),
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
            "SELECT DISTINCT (killmail_time AT TIME ZONE 'UTC')::date "
            "FROM kills WHERE inserted_time > %s ORDER BY 1",
            (since,),
        )
        return [row[0] for row in cursor.fetchall()]
