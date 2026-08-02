# tests/test_schema_sql.py
from pathlib import Path

SCHEMA = (Path(__file__).resolve().parent.parent / "schema.sql").read_text(encoding="utf-8")


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


def test_kill_facets_table_and_index_present():
    assert "CREATE TABLE IF NOT EXISTS kill_facets" in SCHEMA
    assert "PRIMARY KEY (facet_kind, facet_value, role, solar_system_id, killmail_time, killmail_id)" in SCHEMA
    assert "CREATE INDEX IF NOT EXISTS ix_facet_kill" in SCHEMA


def test_pg_trgm_search_indexes_present():
    assert "CREATE EXTENSION IF NOT EXISTS pg_trgm" in SCHEMA
    for idx in ("ix_characters_name_trgm", "ix_corporations_name_trgm",
                "ix_corporations_ticker_trgm", "ix_alliances_name_trgm",
                "ix_alliances_ticker_trgm", "ix_factions_name_trgm"):
        assert idx in SCHEMA
    assert "gin_trgm_ops" in SCHEMA
