# tests/test_factions.py
from factions import faction_rows


def test_faction_rows_maps_id_and_name():
    data = [
        {"faction_id": 500001, "name": "Caldari State"},
        {"faction_id": 500002, "name": "Minmatar Republic"},
    ]
    assert faction_rows(data) == [(500001, "Caldari State"), (500002, "Minmatar Republic")]


def test_faction_rows_empty():
    assert faction_rows([]) == []
