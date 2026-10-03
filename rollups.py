from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta

import metrics
from db import get_cursor

logger = logging.getLogger(__name__)

KINDS = [1, 2, 3, 4, 5, 6]
ROLES = [0, 1]

# Stored key which predates the system rollup joining the mechanism (never rename)
WATERMARK_NAME = "entity_kills_daily"

# Kill and facet rows commit separately, the overlap re-rolls a day rolled in between
DIRTY_OVERLAP = timedelta(hours=1)

_ENTITY_DELETE = (
    "DELETE FROM entity_kills_daily "
    "WHERE facet_kind = ANY(%(kinds)s) AND role = ANY(%(roles)s) AND day = %(day)s"
)
# Shared by every per-day statement, sessions must be UTC
_DAY_RANGE = (
    "k.killmail_time >= %(day)s::timestamptz "
    "AND k.killmail_time < (%(day)s + 1)::timestamptz"
)
# OFFSET 0 is an optimization fence: a bare min/max walks kills_pkey from the start
_ENTITY_BOUNDS = (
    "SELECT min(killmail_id), max(killmail_id) FROM ("
    f"SELECT killmail_id FROM kills k WHERE {_DAY_RANGE} "
    "OFFSET 0) AS day_kills"
)
# The killmail_id bounds keep the facets join to one contiguous slice of idx_facet_kill
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
_SYSTEM_INSERT = (
    "INSERT INTO system_kills_daily (solar_system_id, day, kill_count) "
    "SELECT k.solar_system_id, %(day)s, COUNT(*) FROM kills k "
    f"WHERE {_DAY_RANGE} "
    "GROUP BY k.solar_system_id"
)


def roll_entity_day(conn, day: date) -> int:
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
    with get_cursor(conn) as cursor:
        cursor.execute("SELECT now()")
        return cursor.fetchone()[0]


def find_dirty_days(conn, since: datetime) -> list[date]:
    with get_cursor(conn) as cursor:
        cursor.execute(
            "SELECT DISTINCT killmail_time::date "
            "FROM kills WHERE inserted_time > %s ORDER BY 1",
            (since,),
        )
        return [row[0] for row in cursor.fetchall()]


def roll_dirty_days(conn, should_stop: Callable[[], bool] | None = None) -> bool:
    watermark = read_watermark(conn)
    if watermark is None:
        logger.info(
            "No rollup watermark; run local/backfill_entity_rollup.py. Skipping."
        )
        return False
    metrics.rollup_watermark_timestamp.set(watermark.timestamp())
    t0 = db_now(conn)
    days = find_dirty_days(conn, watermark - DIRTY_OVERLAP)
    rolled = entity_rows = system_rows = 0
    start = time.monotonic()
    for day in days:
        if should_stop is not None and should_stop():
            logger.info(
                "Shutdown requested: %d of %d dirty days rolled; watermark not "
                "advanced, the rest re-roll next cycle.",
                rolled,
                len(days),
            )
            return rolled > 0
        entity_rows += roll_entity_day(conn, day)
        system_rows += roll_system_day(conn, day)
        rolled += 1
        metrics.rollup_days_rolled.inc()
    set_watermark(conn, t0)
    metrics.rollup_watermark_timestamp.set(t0.timestamp())
    if days:
        logger.info(
            "Rollups: %d dirty days (%s to %s) rolled: %d entity rows, "
            "%d system rows in %.1fs.",
            rolled,
            days[0],
            days[-1],
            entity_rows,
            system_rows,
            time.monotonic() - start,
        )
    return True
