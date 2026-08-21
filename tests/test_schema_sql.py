# tests/test_schema_sql.py
import re
from pathlib import Path

SCHEMA = (Path(__file__).resolve().parent.parent / "schema.sql").read_text(
    encoding="utf-8"
)


def test_entity_tables_present():
    for table in (
        "characters",
        "corporations",
        "alliances",
        "factions",
        "wars",
        "entity_resolve_backlog",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in SCHEMA


def test_wars_partial_refresh_index_present():
    assert "idx_wars_refresh" in SCHEMA
    assert "WHERE refresh_after IS NOT NULL" in SCHEMA


def test_entity_names_are_text_not_varchar():
    # Names must be TEXT (no length caps) per the design.
    start = SCHEMA.index("CREATE TABLE IF NOT EXISTS characters")
    end = SCHEMA.index("CREATE TABLE IF NOT EXISTS wars")
    entity_block = SCHEMA[start:end]
    assert "VARCHAR" not in entity_block


# Collapse runs of spaces/tabs so assertions don't depend on the file's
# column-alignment padding (newlines are preserved).
_SCHEMA_NORM = re.sub(r"[ \t]+", " ", SCHEMA)


def test_kill_facets_table_and_index_present():
    assert "CREATE TABLE IF NOT EXISTS kill_facets" in SCHEMA
    # exact PK column order
    assert (
        "PRIMARY KEY (facet_kind, facet_value, role, "
        "solar_system_id, killmail_time, killmail_id)"
    ) in SCHEMA
    # reverse index name AND its exact column order (the ON-clause is unique to it)
    assert "CREATE INDEX IF NOT EXISTS idx_facet_kill" in SCHEMA
    assert "ON kill_facets (killmail_id, facet_kind, facet_value, role)" in _SCHEMA_NORM


def test_pg_trgm_search_indexes_present():
    assert "CREATE EXTENSION IF NOT EXISTS pg_trgm" in SCHEMA
    for name, table, col in [
        ("idx_characters_name_nospace_trgm", "characters", "name"),
        ("idx_corporations_name_nospace_trgm", "corporations", "name"),
        ("idx_corporations_ticker_nospace_trgm", "corporations", "ticker"),
        ("idx_alliances_name_nospace_trgm", "alliances", "name"),
        ("idx_alliances_ticker_nospace_trgm", "alliances", "ticker"),
        ("idx_factions_name_nospace_trgm", "factions", "name"),
    ]:
        assert (
            f"{name} ON {table} USING gin (replace({col}, ' ', '') gin_trgm_ops)"
            in _SCHEMA_NORM
        )


def test_corporations_refresh_cursor_present():
    start = SCHEMA.index("CREATE TABLE IF NOT EXISTS corporations")
    end = SCHEMA.index("CREATE TABLE IF NOT EXISTS alliances")
    corp_block = SCHEMA[start:end]
    assert "refresh_after TIMESTAMPTZ" in corp_block
    assert "idx_corporations_refresh" in SCHEMA
    # exactly one immutable partial predicate, like idx_wars_refresh
    assert "ON corporations (refresh_after)" in _SCHEMA_NORM
    assert "WHERE refresh_after IS NOT NULL" in SCHEMA
    # pin the predicate to this specific index (not just present somewhere in SCHEMA)
    assert re.search(
        r"idx_corporations_refresh\s+ON corporations \(refresh_after\)\s+WHERE refresh_after IS NOT NULL",
        SCHEMA,
    )


def test_corporation_metadata_indexes_present():
    # Name and ON-clause asserted separately (indexes may span two lines, and
    # _SCHEMA_NORM preserves newlines) — matches the idx_facet_kill convention.
    assert "idx_corporations_alliance_id" in SCHEMA
    assert "ON corporations (alliance_id)" in _SCHEMA_NORM
    # pin the predicate to this specific index (not just present somewhere in SCHEMA)
    assert re.search(
        r"idx_corporations_alliance_id\s+ON corporations \(alliance_id\)\s+WHERE alliance_id IS NOT NULL",
        SCHEMA,
    )
    assert "idx_corporations_date_founded" in SCHEMA
    assert "ON corporations (date_founded DESC NULLS LAST)" in _SCHEMA_NORM
    assert "idx_alliances_date_founded" in SCHEMA
    assert "ON alliances (date_founded DESC NULLS LAST)" in _SCHEMA_NORM


def test_mv_alliance_member_count_present():
    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_alliance_member_count" in SCHEMA
    # stores the raw SUM(member_count) per alliance; open/closed is derived in the
    # backend (member_count > 0), not stored. The active column/filter is gone.
    assert "COALESCE(SUM(member_count), 0) AS member_count" in _SCHEMA_NORM
    assert "active IS TRUE" not in SCHEMA
    assert "AS is_open" not in SCHEMA
    # pin the MV's FROM/WHERE/GROUP BY (no active filter)
    assert re.search(
        r"FROM corporations\s+WHERE alliance_id IS NOT NULL\s+GROUP BY alliance_id",
        SCHEMA,
    )
    assert "idx_mv_alliance_member_count_alliance" in SCHEMA
    assert "ON mv_alliance_member_count (alliance_id)" in _SCHEMA_NORM
    assert (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_alliance_member_count_alliance"
        in SCHEMA
    )
