"""Entity leaderboards: an incremental per-entity daily rollup of kill_facets
(entity_kills_daily) and the precomputed top-N boards the backend reads
(entity_leaderboard).

Design: docs/superpowers/specs/2026-09-12-entity-leaderboards-design.md

The rollup is maintained by recomputing whole UTC days. Dirty days are derived
from kills.inserted_time against a watermark (rollup_state), so every path
that inserts into kills is covered without any code at the insert sites. A
facet-only repair (rows added to kill_facets for existing kills) does not
touch kills.inserted_time; re-roll those days by hand or reset rollup_state.
Nothing here triggers the historical build; that is the local sql/ backfill
script, which loops roll_day.
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
    "AND f.role = ANY(%(roles)s) "
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


def roll_dirty_days(conn) -> bool:
    """Fast-cycle step: recompute every UTC day with kills inserted since the
    watermark (minus DIRTY_OVERLAP), then advance the watermark to the DB time
    captured before the scan. Returns False (skipped) when no watermark exists —
    the historical build has not run. Raises on failure, leaving the watermark
    untouched so the next cycle retries the same set."""
    watermark = read_watermark(conn)
    if watermark is None:
        logger.info(
            "No entity rollup watermark; run sql/backfill_entity_rollup.py. Skipping."
        )
        return False
    metrics.entity_rollup_watermark_timestamp.set(watermark.timestamp())
    t0 = db_now(conn)
    days = find_dirty_days(conn, watermark - DIRTY_OVERLAP)
    if len(days) > DIRTY_DAYS_WARN:
        logger.warning(
            "Entity rollup: %d dirty days this cycle (steady state is 1-2); a bulk "
            "historical load will make this cycle slow.", len(days)
        )
    for day in days:
        start = time.monotonic()
        written = roll_day(conn, day)
        metrics.entity_rollup_days_rolled.inc()
        logger.info(
            "Rolled entity kills for %s: %d rows in %.1fs.",
            day,
            written,
            time.monotonic() - start,
        )
    set_watermark(conn, t0)
    metrics.entity_rollup_watermark_timestamp.set(t0.timestamp())
    return True


_BOARD_DELETE = "DELETE FROM entity_leaderboard WHERE window_key = %(window)s"
# {day_predicate} is either _DAY_PREDICATE or "" — a fixed fragment, chosen by
# window; the interval value itself is always a bound parameter.
_BOARD_INSERT = (
    "INSERT INTO entity_leaderboard "
    "(facet_kind, role, window_key, rank, facet_value, kill_count, computed_at) "
    "SELECT facet_kind, role, %(window)s, rank, facet_value, kill_count, now() "
    "FROM ("
    "SELECT facet_kind, role, facet_value, SUM(kill_count) AS kill_count, "
    "ROW_NUMBER() OVER (PARTITION BY facet_kind, role "
    "ORDER BY SUM(kill_count) DESC, facet_value) AS rank "
    "FROM entity_kills_daily "
    "WHERE facet_kind = ANY(%(kinds)s) AND role = ANY(%(roles)s){day_predicate} "
    "GROUP BY facet_kind, role, facet_value"
    ") ranked WHERE rank <= %(top_n)s"
)
_DAY_PREDICATE = " AND day > CURRENT_DATE - %(interval)s::interval"


def compute_board(conn, window_key: str) -> int:
    """Rewrite one window's boards for every (kind, role) in a single
    transaction, so readers never see a partial board. Returns rows written."""
    interval = WINDOWS[window_key]  # KeyError on an unknown window is a bug
    sql = _BOARD_INSERT.format(day_predicate=_DAY_PREDICATE if interval else "")
    params = {
        "window": window_key,
        "kinds": KINDS,
        "roles": ROLES,
        "interval": interval,
        "top_n": config.leaderboard.top_n,
    }
    try:
        with get_cursor(conn) as cursor:
            cursor.execute(_BOARD_DELETE, params)
            cursor.execute(sql, params)
            written = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return written


def compute_boards(conn, windows: list[str]) -> bool:
    """Refresh-step body for a set of windows. Returns False (skipped) when
    there is no rollup watermark — a partially built rollup must never produce
    boards. Each window is its own transaction; a failing window is logged and
    counted and the rest still run; raises at the end if any failed."""
    if read_watermark(conn) is None:
        logger.info("No entity rollup watermark; skipping leaderboards.")
        return False
    failed: list[str] = []
    for window_key in windows:
        start = time.monotonic()
        try:
            written = compute_board(conn, window_key)
        except Exception as e:
            failed.append(window_key)
            metrics.leaderboard_computations.labels(window_key, "failed").inc()
            logger.error("Leaderboard %s failed: %s", window_key, e, exc_info=True)
            continue
        elapsed = time.monotonic() - start
        metrics.leaderboard_computations.labels(window_key, "success").inc()
        metrics.leaderboard_compute_seconds.labels(window_key).observe(elapsed)
        metrics.leaderboard_last_success_timestamp.labels(
            window_key
        ).set_to_current_time()
        metrics.leaderboard_rows.labels(window_key).set(written)
        logger.info("Leaderboard %s: %d rows in %.1fs.", window_key, written, elapsed)
    # Any failure marks the whole step failed, so the cycle withholds the
    # leaderboards invalidation even though the other windows were rewritten —
    # the safe direction: the backend keeps serving its previous (complete)
    # boards until the next cycle.
    if failed:
        raise RuntimeError(f"leaderboard windows failed: {', '.join(failed)}")
    return True
