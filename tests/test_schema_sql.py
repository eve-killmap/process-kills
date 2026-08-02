# tests/test_schema_sql.py
import re
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


# Collapse runs of spaces/tabs so assertions don't depend on the file's
# column-alignment padding (newlines are preserved).
_SCHEMA_NORM = re.sub(r"[ \t]+", " ", SCHEMA)


def test_kill_facets_table_and_index_present():
    assert "CREATE TABLE IF NOT EXISTS kill_facets" in SCHEMA
    # exact PK column order
    assert ("PRIMARY KEY (facet_kind, facet_value, role, "
            "solar_system_id, killmail_time, killmail_id)") in SCHEMA
    # reverse index name AND its exact column order (the ON-clause is unique to it)
    assert "CREATE INDEX IF NOT EXISTS ix_facet_kill" in SCHEMA
    assert "ON kill_facets (killmail_id, facet_kind, facet_value, role)" in _SCHEMA_NORM


def test_pg_trgm_search_indexes_present():
    assert "CREATE EXTENSION IF NOT EXISTS pg_trgm" in SCHEMA
    # each trigram index name tied to its exact table + column + gin_trgm_ops
    for name, table, col in [
        ("ix_characters_name_trgm", "characters", "name"),
        ("ix_corporations_name_trgm", "corporations", "name"),
        ("ix_corporations_ticker_trgm", "corporations", "ticker"),
        ("ix_alliances_name_trgm", "alliances", "name"),
        ("ix_alliances_ticker_trgm", "alliances", "ticker"),
        ("ix_factions_name_trgm", "factions", "name"),
    ]:
        assert f"{name} ON {table} USING gin ({col} gin_trgm_ops)" in _SCHEMA_NORM
