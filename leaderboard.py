from __future__ import annotations

import logging
import time
from collections import Counter
from collections.abc import Callable

import metrics
from config import config
from db import get_cursor
from rollups import KINDS, ROLES, read_watermark

logger = logging.getLogger(__name__)

# Interval literals must match the backend's top-systems predicate verbatim
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

SCOPES = ["all", "players"]

NPC_CHARACTER_IDS = (3_000_000, 3_999_999)
NPC_CORPORATION_IDS = (1_000_000, 1_999_999)
# SDE category 11 "Entity", types.category_id is maintained by process-sde
NPC_TYPE_CATEGORY = 11

_BOARD_DELETE = "DELETE FROM entity_leaderboard WHERE window_key = %(window)s"
_BOARD_INSERT = (
    "INSERT INTO entity_leaderboard "
    "(facet_kind, role, window_key, scope, rank, facet_value, kill_count, computed_at) "
    "WITH totals AS MATERIALIZED ("
    "SELECT facet_kind, role, facet_value, SUM(kill_count) AS kill_count "
    "FROM entity_kills_daily "
    "WHERE facet_kind = ANY(%(kinds)s) AND role = ANY(%(roles)s){day_predicate} "
    "GROUP BY facet_kind, role, facet_value"
    "), players AS ("
    "SELECT * FROM totals t WHERE NOT ("
    "(facet_kind = 1 AND facet_value BETWEEN %(npc_character_min)s AND %(npc_character_max)s) "
    "OR (facet_kind = 2 AND facet_value BETWEEN %(npc_corporation_min)s AND %(npc_corporation_max)s) "
    "OR (facet_kind = 4 AND facet_value <> ALL(%(player_factions)s::bigint[])) "
    "OR (facet_kind IN (5, 6) AND EXISTS (SELECT 1 FROM types ty "
    "WHERE ty.id = t.facet_value AND ty.category_id = %(npc_category)s))"
    ")) "
    "SELECT facet_kind, role, %(window)s, 'all', rank, facet_value, kill_count, now() "
    "FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY facet_kind, role "
    "ORDER BY kill_count DESC, facet_value) AS rank FROM totals) r "
    "WHERE rank <= %(top_n)s "
    "UNION ALL "
    "SELECT facet_kind, role, %(window)s, 'players', rank, facet_value, kill_count, now() "
    "FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY facet_kind, role "
    "ORDER BY kill_count DESC, facet_value) AS rank FROM players) r "
    "WHERE rank <= %(top_n)s "
    "RETURNING scope"
)
_DAY_PREDICATE = " AND day > CURRENT_DATE - %(interval)s::interval"


def compute_board(conn, window_key: str) -> dict[str, int]:
    interval = WINDOWS[window_key]
    sql = _BOARD_INSERT.format(day_predicate=_DAY_PREDICATE if interval else "")
    params = {
        "window": window_key,
        "kinds": KINDS,
        "roles": ROLES,
        "interval": interval,
        "top_n": config.leaderboard.top_n,
        "npc_character_min": NPC_CHARACTER_IDS[0],
        "npc_character_max": NPC_CHARACTER_IDS[1],
        "npc_corporation_min": NPC_CORPORATION_IDS[0],
        "npc_corporation_max": NPC_CORPORATION_IDS[1],
        # psycopg2 adapts a list to an SQL array, a tuple would become a row
        "player_factions": list(config.leaderboard.player_factions),
        "npc_category": NPC_TYPE_CATEGORY,
    }
    try:
        with get_cursor(conn) as cursor:
            cursor.execute(_BOARD_DELETE, params)
            cursor.execute(sql, params)
            counts = Counter(scope for (scope,) in cursor.fetchall())
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {scope: counts.get(scope, 0) for scope in SCOPES}


def compute_boards(
    conn, windows: list[str], should_stop: Callable[[], bool] | None = None
) -> bool:
    if read_watermark(conn) is None:
        logger.info("No rollup watermark; skipping leaderboards.")
        return False
    failed: list[str] = []
    written_windows = 0
    for window_key in windows:
        if should_stop is not None and should_stop():
            logger.info(
                "Shutdown requested: %d of %d leaderboard windows computed; the "
                "rest next cycle.",
                written_windows,
                len(windows),
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
        for scope, rows in written.items():
            metrics.leaderboard_rows.labels(window_key, scope).set(rows)
        written_windows += 1
        logger.info(
            "Leaderboard %s: %d all / %d players rows in %.1fs.",
            window_key,
            written["all"],
            written["players"],
            elapsed,
        )
    if failed:
        raise RuntimeError(f"leaderboard windows failed: {', '.join(failed)}")
    return written_windows > 0
