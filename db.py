import logging
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extensions import connection, cursor as Cursor
from psycopg2.extras import execute_values

from config import config, require_database_url
from schema import ProcessedDate, RecheckCandidate

logger = logging.getLogger(__name__)


@contextmanager
def get_connection() -> Iterator[connection]:
    conn = psycopg2.connect(require_database_url(config))
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_cursor(conn: connection) -> Iterator[Cursor]:
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


def init_schema(conn: connection) -> None:
    schema_file = Path(__file__).parent / "schema.sql"
    schema_sql = schema_file.read_text(encoding="utf-8")

    with get_cursor(conn) as cursor:
        cursor.execute(schema_sql)
    conn.commit()


def insert_kills_batch(conn: connection, kills: Sequence[Mapping[str, Any]]) -> int:
    if not kills:
        return 0

    kill_values = []
    attacker_values = []

    for kill in kills:
        kill_values.append(
            (
                kill["killmail_id"],
                kill["killmail_hash"],
                kill["killmail_time"],
                kill["solar_system_id"],
                round(kill["position_x"]),
                round(kill["position_y"]),
                round(kill["position_z"]),
                kill.get("victim_character_id"),
                kill.get("victim_corporation_id"),
                kill.get("victim_alliance_id"),
                kill.get("victim_faction_id"),
                kill["victim_damage_taken"],
                kill["victim_ship_type_id"],
                kill.get("war_id"),
            )
        )

        for idx, attacker in enumerate(kill.get("attackers", [])):
            attacker_values.append(
                (
                    kill["killmail_id"],
                    idx,
                    attacker.get("character_id"),
                    attacker.get("corporation_id"),
                    attacker.get("alliance_id"),
                    attacker.get("faction_id"),
                    attacker.get("ship_type_id"),
                    attacker.get("weapon_type_id"),
                    attacker["damage_done"],
                    attacker["final_blow"],
                    attacker["security_status"],
                )
            )

    with get_cursor(conn) as cursor:
        inserted_rows = execute_values(
            cursor,
            """
            INSERT INTO kills (
                killmail_id, killmail_hash, killmail_time, solar_system_id,
                position_x, position_y, position_z,
                victim_character_id, victim_corporation_id, victim_alliance_id,
                victim_faction_id, victim_damage_taken,
                victim_ship_type_id, war_id
            ) VALUES %s
            ON CONFLICT (killmail_id) DO NOTHING
            RETURNING killmail_id
            """,
            kill_values,
            fetch=True,
        )
        inserted_kills = len(inserted_rows) if inserted_rows else 0

        if attacker_values:
            execute_values(
                cursor,
                """
                INSERT INTO kill_attackers (
                    killmail_id, attacker_index, character_id, corporation_id,
                    alliance_id, faction_id, ship_type_id, weapon_type_id,
                    damage_done, final_blow, security_status
                ) VALUES %s
                ON CONFLICT (killmail_id, attacker_index) DO NOTHING
                """,
                attacker_values,
            )

    conn.commit()
    return inserted_kills


def insert_kill(conn: connection, kill: Mapping[str, Any]) -> bool:
    with get_cursor(conn) as cursor:
        cursor.execute(
            """
            INSERT INTO kills (
                killmail_id, killmail_hash, killmail_time, solar_system_id,
                position_x, position_y, position_z,
                victim_character_id, victim_corporation_id, victim_alliance_id,
                victim_faction_id, victim_damage_taken,
                victim_ship_type_id, war_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (killmail_id) DO NOTHING
            RETURNING killmail_id
            """,
            (
                kill["killmail_id"],
                kill["killmail_hash"],
                kill["killmail_time"],
                kill["solar_system_id"],
                round(kill["position_x"]),
                round(kill["position_y"]),
                round(kill["position_z"]),
                kill.get("victim_character_id"),
                kill.get("victim_corporation_id"),
                kill.get("victim_alliance_id"),
                kill.get("victim_faction_id"),
                kill["victim_damage_taken"],
                kill["victim_ship_type_id"],
                kill.get("war_id"),
            ),
        )
        inserted = cursor.fetchone() is not None

        if inserted:
            attackers = kill.get("attackers", [])
            for idx, attacker in enumerate(attackers):
                cursor.execute(
                    """
                    INSERT INTO kill_attackers (
                        killmail_id, attacker_index, character_id, corporation_id,
                        alliance_id, faction_id, ship_type_id, weapon_type_id,
                        damage_done, final_blow, security_status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (killmail_id, attacker_index) DO NOTHING
                    """,
                    (
                        kill["killmail_id"],
                        idx,
                        attacker.get("character_id"),
                        attacker.get("corporation_id"),
                        attacker.get("alliance_id"),
                        attacker.get("faction_id"),
                        attacker.get("ship_type_id"),
                        attacker.get("weapon_type_id"),
                        attacker["damage_done"],
                        attacker["final_blow"],
                        attacker["security_status"],
                    ),
                )

    conn.commit()
    return inserted


def insert_no_position_kill(
    conn: connection, killmail_id: int, killmail_hash: str, killmail_time: str
) -> bool:
    with get_cursor(conn) as cursor:
        cursor.execute(
            """
            INSERT INTO kills_no_positions (killmail_id, killmail_hash, killmail_time)
            VALUES (%s, %s, %s)
            ON CONFLICT (killmail_id) DO NOTHING
            RETURNING killmail_id
            """,
            (killmail_id, killmail_hash, killmail_time),
        )
        inserted = cursor.fetchone() is not None
    conn.commit()
    return inserted


def insert_no_position_kills_batch(
    conn: connection, kills: Sequence[tuple[int, str, str]]
) -> int:
    if not kills:
        return 0
    with get_cursor(conn) as cursor:
        inserted_rows = execute_values(
            cursor,
            """
            INSERT INTO kills_no_positions (killmail_id, killmail_hash, killmail_time)
            VALUES %s
            ON CONFLICT (killmail_id) DO NOTHING
            RETURNING killmail_id
            """,
            kills,
            fetch=True,
        )
    conn.commit()
    return len(inserted_rows) if inserted_rows else 0


def get_existing_killmail_ids(conn: connection, killmail_ids: list[int]) -> set[int]:
    if not killmail_ids:
        return set()
    found: set[int] = set()
    with get_cursor(conn) as cursor:
        cursor.execute(
            "SELECT killmail_id FROM kills WHERE killmail_id = ANY(%s)", (killmail_ids,)
        )
        found.update(row[0] for row in cursor.fetchall())
        cursor.execute(
            "SELECT killmail_id FROM kills_no_positions WHERE killmail_id = ANY(%s)",
            (killmail_ids,),
        )
        found.update(row[0] for row in cursor.fetchall())
    return found


def delete_no_position_kill(conn: connection, killmail_id: int) -> None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            "DELETE FROM kills_no_positions WHERE killmail_id = %s", (killmail_id,)
        )
    conn.commit()


def update_no_position_last_checked(conn: connection, killmail_id: int) -> None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            "UPDATE kills_no_positions SET last_checked = NOW() WHERE killmail_id = %s",
            (killmail_id,),
        )
    conn.commit()


def get_recheck_candidates(
    conn: connection, limit: int = 500
) -> list[RecheckCandidate]:
    with get_cursor(conn) as cursor:
        cursor.execute(
            """
            SELECT killmail_id, killmail_hash, killmail_time, last_checked
            FROM kills_no_positions
            WHERE killmail_time > NOW() - INTERVAL '3 years'
            AND (
                (killmail_time > NOW() - INTERVAL '7 days'
                    AND last_checked < NOW() - INTERVAL '1 day')
                OR (killmail_time > NOW() - INTERVAL '30 days'
                    AND killmail_time <= NOW() - INTERVAL '7 days'
                    AND last_checked < NOW() - INTERVAL '3 days')
                OR (killmail_time > NOW() - INTERVAL '180 days'
                    AND killmail_time <= NOW() - INTERVAL '30 days'
                    AND last_checked < NOW() - INTERVAL '7 days')
                OR (killmail_time <= NOW() - INTERVAL '180 days'
                    AND last_checked < NOW() - INTERVAL '30 days')
            )
            ORDER BY killmail_time DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        return [
            {
                "killmail_id": row[0],
                "killmail_hash": row[1],
                "killmail_time": row[2],
                "last_checked": row[3],
            }
            for row in rows
        ]


def get_processed_date(conn: connection, date: str) -> ProcessedDate | None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            """
            SELECT date, total_kills, processed_kills, no_position_kills,
                   last_updated, error_message
            FROM processed_data
            WHERE date = %s
            """,
            (date,),
        )
        row = cursor.fetchone()
        if row:
            return {
                "date": row[0],
                "total_kills": row[1],
                "processed_kills": row[2],
                "no_position_kills": row[3],
                "last_updated": row[4],
                "error_message": row[5],
            }
    return None


def update_processed_date(
    conn: connection,
    date: str,
    total_kills: int,
    processed_kills: int = 0,
    no_position_kills: int = 0,
    error_message: str | None = None,
) -> None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            """
            INSERT INTO processed_data (
                date, total_kills, processed_kills, no_position_kills,
                last_updated, error_message
            ) VALUES (%s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (date) DO UPDATE SET
                total_kills = EXCLUDED.total_kills,
                processed_kills = EXCLUDED.processed_kills,
                no_position_kills = EXCLUDED.no_position_kills,
                last_updated = NOW(),
                error_message = EXCLUDED.error_message
            """,
            (date, total_kills, processed_kills, no_position_kills, error_message),
        )
    conn.commit()


def increment_processed_kills(conn: connection, date: str) -> None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            """
            INSERT INTO processed_data (date, total_kills, processed_kills, no_position_kills, last_updated)
            VALUES (%s, 1, 1, 0, NOW())
            ON CONFLICT (date) DO UPDATE SET
                processed_kills = processed_data.processed_kills + 1,
                total_kills = GREATEST(
                    processed_data.total_kills,
                    processed_data.processed_kills + 1 + processed_data.no_position_kills
                ),
                last_updated = NOW()
            """,
            (date,),
        )
    conn.commit()


def increment_no_position_kills(conn: connection, date: str) -> None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            """
            INSERT INTO processed_data (date, total_kills, processed_kills, no_position_kills, last_updated)
            VALUES (%s, 1, 0, 1, NOW())
            ON CONFLICT (date) DO UPDATE SET
                no_position_kills = processed_data.no_position_kills + 1,
                total_kills = GREATEST(
                    processed_data.total_kills,
                    processed_data.processed_kills + processed_data.no_position_kills + 1
                ),
                last_updated = NOW()
            """,
            (date,),
        )
    conn.commit()


def decrement_no_position_kills(conn: connection, date: str) -> None:
    with get_cursor(conn) as cursor:
        cursor.execute(
            """
            UPDATE processed_data SET
                no_position_kills = GREATEST(no_position_kills - 1, 0),
                last_updated = NOW()
            WHERE date = %s
            """,
            (date,),
        )
    conn.commit()


def get_live_sequence(conn: connection) -> int | None:
    with get_cursor(conn) as cursor:
        cursor.execute("SELECT sequence FROM live_state LIMIT 1")
        row = cursor.fetchone()
        return row[0] if row else None


def set_live_sequence(conn: connection, sequence: int) -> None:
    with get_cursor(conn) as cursor:
        cursor.execute("DELETE FROM live_state")
        cursor.execute("INSERT INTO live_state (sequence) VALUES (%s)", (sequence,))
    conn.commit()


def _fresh_ids(
    conn: connection, table: str, id_col: str, ids: list[int], cutoff: Any
) -> set[int]:
    # table/id_col are internal constants (never user input).
    if not ids:
        return set()
    with get_cursor(conn) as cursor:
        cursor.execute(
            f"SELECT {id_col} FROM {table} "
            f"WHERE {id_col} = ANY(%s) AND resolved_at > %s",
            (ids, cutoff),
        )
        return {row[0] for row in cursor.fetchall()}


def get_fresh_character_ids(conn, ids, cutoff):
    return _fresh_ids(conn, "characters", "character_id", ids, cutoff)


def get_fresh_corporation_ids(conn, ids, cutoff):
    return _fresh_ids(conn, "corporations", "corporation_id", ids, cutoff)


def get_fresh_alliance_ids(conn, ids, cutoff):
    return _fresh_ids(conn, "alliances", "alliance_id", ids, cutoff)


def upsert_characters(conn, rows):
    if not rows:
        return
    with get_cursor(conn) as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO characters (character_id, name) VALUES %s
            ON CONFLICT (character_id) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, characters.name),
                resolved_at = NOW()
            """,
            rows,
        )
    conn.commit()


def upsert_corporations(conn, rows):
    if not rows:
        return
    with get_cursor(conn) as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO corporations (corporation_id, name, ticker) VALUES %s
            ON CONFLICT (corporation_id) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, corporations.name),
                ticker = COALESCE(EXCLUDED.ticker, corporations.ticker),
                resolved_at = NOW()
            """,
            rows,
        )
    conn.commit()


def upsert_alliances(conn, rows):
    if not rows:
        return
    with get_cursor(conn) as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO alliances (alliance_id, name, ticker) VALUES %s
            ON CONFLICT (alliance_id) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, alliances.name),
                ticker = COALESCE(EXCLUDED.ticker, alliances.ticker),
                resolved_at = NOW()
            """,
            rows,
        )
    conn.commit()
