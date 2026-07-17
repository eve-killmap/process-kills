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
