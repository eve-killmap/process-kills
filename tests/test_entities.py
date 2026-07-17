from entities import EntityIds, collect_entity_ids


def _kill(**overrides):
    kill = {
        "killmail_id": 1,
        "killmail_hash": "h",
        "killmail_time": "2024-01-01T00:00:00Z",
        "solar_system_id": 30000142,
        "position_x": 0.0,
        "position_y": 0.0,
        "position_z": 0.0,
        "victim_character_id": 100,
        "victim_corporation_id": 200,
        "victim_alliance_id": 300,
        "victim_faction_id": 500,
        "victim_damage_taken": 1,
        "victim_ship_type_id": 587,
        "war_id": None,
        "attackers": [],
    }
    kill.update(overrides)
    return kill


def test_collects_victim_and_attacker_ids_excluding_factions():
    kill = _kill(
        attackers=[
            {
                "character_id": 101,
                "corporation_id": 201,
                "alliance_id": None,
                "faction_id": 500,
                "ship_type_id": 587,
                "weapon_type_id": 3074,
                "damage_done": 10,
                "final_blow": True,
                "security_status": 0.0,
            }
        ]
    )
    ids = collect_entity_ids(kill)
    assert ids.characters == frozenset({100, 101})
    assert ids.corporations == frozenset({200, 201})
    assert ids.alliances == frozenset({300})  # attacker alliance was None


def test_npc_victim_has_no_character_id():
    kill = _kill(victim_character_id=None, victim_corporation_id=None, victim_alliance_id=None)
    ids = collect_entity_ids(kill)
    assert ids.characters == frozenset()
    assert ids.corporations == frozenset()
    assert ids.alliances == frozenset()


def test_is_empty():
    assert EntityIds(frozenset(), frozenset(), frozenset()).is_empty
    assert not EntityIds(frozenset({1}), frozenset(), frozenset()).is_empty


from entities import character_rows, group_rows


def test_character_rows_tombstones_missing():
    rows = dict(character_rows({1, 2, 3}, {1: "Alice", 2: "Bob"}))
    assert rows == {1: "Alice", 2: "Bob", 3: None}


def test_group_rows_tombstones_missing_and_none():
    info = {10: ("CorpA", "AAA"), 11: None}
    rows = {r[0]: (r[1], r[2]) for r in group_rows({10, 11, 12}, info)}
    assert rows[10] == ("CorpA", "AAA")
    assert rows[11] == (None, None)
    assert rows[12] == (None, None)


import asyncio
from datetime import datetime, timezone

import db
import entities
from entities import EntityIds


def test_find_unfresh_subtracts_fresh(monkeypatch):
    monkeypatch.setattr(db, "get_fresh_character_ids", lambda c, ids, cut: {1})
    monkeypatch.setattr(db, "get_fresh_corporation_ids", lambda c, ids, cut: set())
    monkeypatch.setattr(db, "get_fresh_alliance_ids", lambda c, ids, cut: {30})
    ids = EntityIds(frozenset({1, 2}), frozenset({20}), frozenset({30}))
    out = entities.find_unfresh(None, ids, datetime.now(timezone.utc), 30)
    assert out.characters == frozenset({2})
    assert out.corporations == frozenset({20})
    assert out.alliances == frozenset()


def test_ensure_kill_entities_swallows_errors_and_enqueues(monkeypatch):
    enqueued = []

    async def boom(*a, **k):
        raise RuntimeError("esi down")

    monkeypatch.setattr(entities, "find_unfresh", lambda *a, **k: EntityIds(
        frozenset({1}), frozenset(), frozenset()))
    monkeypatch.setattr(entities, "resolve_and_store", boom)
    monkeypatch.setattr(db, "enqueue_entity_backlog", lambda c, kid: enqueued.append(kid))

    parsed = {
        "killmail_id": 777, "killmail_hash": "h", "killmail_time": "t",
        "solar_system_id": 1, "position_x": 0.0, "position_y": 0.0, "position_z": 0.0,
        "victim_character_id": 1, "victim_corporation_id": None,
        "victim_alliance_id": None, "victim_faction_id": None,
        "victim_damage_taken": 0, "victim_ship_type_id": 1, "war_id": None,
        "attackers": [],
    }
    ok = asyncio.run(entities.ensure_kill_entities(None, object(), parsed))
    assert ok is False
    assert enqueued == [777]


def test_ensure_kill_entities_noop_when_all_fresh(monkeypatch):
    # Kill HAS ids, but find_unfresh reports them all fresh -> no ESI, no enqueue.
    monkeypatch.setattr(entities, "find_unfresh", lambda *a, **k: EntityIds(
        frozenset(), frozenset(), frozenset()))
    resolved = []

    async def spy_resolve(*a, **k):
        resolved.append(True)

    monkeypatch.setattr(entities, "resolve_and_store", spy_resolve)
    called = []
    monkeypatch.setattr(db, "enqueue_entity_backlog", lambda c, kid: called.append(kid))
    parsed = {
        "killmail_id": 1, "killmail_hash": "h", "killmail_time": "t",
        "solar_system_id": 1, "position_x": 0.0, "position_y": 0.0, "position_z": 0.0,
        "victim_character_id": 5, "victim_corporation_id": None,
        "victim_alliance_id": None, "victim_faction_id": None,
        "victim_damage_taken": 0, "victim_ship_type_id": 1, "war_id": None,
        "attackers": [],
    }
    ok = asyncio.run(entities.ensure_kill_entities(None, object(), parsed))
    assert ok is True
    assert resolved == []  # all fresh -> resolve_and_store never called
    assert called == []


def test_resolve_and_store_counts_each_corp_once(monkeypatch):
    from prometheus_client import REGISTRY

    def val(outcome):
        return REGISTRY.get_sample_value(
            "eve_killmap_entities_resolved_total",
            {"kind": "corporation", "outcome": outcome},
        ) or 0.0

    class _Esi:
        async def resolve_names(self, ids):
            return {}
        async def get_corporation(self, cid):
            if cid == 1:
                return ("CorpOne", "ONE")   # resolved
            if cid == 2:
                return None                  # not_found (404)
            raise RuntimeError("boom")       # error (cid == 3)
        async def get_alliance(self, aid):
            return None

    monkeypatch.setattr(db, "upsert_corporations", lambda conn, rows: None)
    monkeypatch.setattr(db, "upsert_alliances", lambda conn, rows: None)
    monkeypatch.setattr(db, "upsert_characters", lambda conn, rows: None)

    before = {o: val(o) for o in ("resolved", "not_found", "error")}
    unfresh = EntityIds(frozenset(), frozenset({1, 2, 3}), frozenset())
    asyncio.run(entities.resolve_and_store(None, _Esi(), unfresh, max_concurrency=5))
    after = {o: val(o) for o in ("resolved", "not_found", "error")}

    assert after["resolved"] - before["resolved"] == 1
    assert after["not_found"] - before["not_found"] == 1
    assert after["error"] - before["error"] == 1


def test_ensure_kill_entities_never_raises_on_malformed_parsed(monkeypatch):
    monkeypatch.setattr(db, "enqueue_entity_backlog", lambda conn, kid: None)
    # Missing required keys -> collect_entity_ids would KeyError; must be caught.
    ok = asyncio.run(entities.ensure_kill_entities(None, object(), {"killmail_id": 5}))
    assert ok is False
