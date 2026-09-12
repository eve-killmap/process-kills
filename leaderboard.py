"""Entity leaderboards: the precomputed top-N boards the backend reads
(entity_leaderboard), computed from the entity_kills_daily rollup that
rollups.py maintains.

Design: docs/superpowers/specs/2026-09-12-entity-leaderboards-design.md
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

import metrics
from config import config
from db import get_cursor
from rollups import KINDS, ROLES, read_watermark

logger = logging.getLogger(__name__)

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


def compute_boards(
    conn, windows: list[str], should_stop: Callable[[], bool] | None = None
) -> bool:
    """Refresh-step body for a set of windows. Returns False (skipped) when
    there is no rollup watermark — a partially built rollup must never produce
    boards. Each window is its own transaction; a failing window is logged and
    counted and the rest still run; raises at the end if any failed.
    `should_stop` (service shutdown) is polled between windows; the rest are
    computed next cycle. Returns True if any window was written."""
    if read_watermark(conn) is None:
        logger.info("No rollup watermark; skipping leaderboards.")
        return False
    failed: list[str] = []
    written_windows = 0
    for window_key in windows:
        if should_stop is not None and should_stop():
            logger.info(
                "Shutdown requested: %d of %d leaderboard windows computed; the "
                "rest next cycle.", written_windows, len(windows)
            )
            break
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
        written_windows += 1
        logger.info("Leaderboard %s: %d rows in %.1fs.", window_key, written, elapsed)
    # Any failure marks the whole step failed, so the cycle withholds the
    # leaderboards invalidation even though the other windows were rewritten —
    # the safe direction: the backend keeps serving its previous (complete)
    # boards until the next cycle.
    if failed:
        raise RuntimeError(f"leaderboard windows failed: {', '.join(failed)}")
    return written_windows > 0
