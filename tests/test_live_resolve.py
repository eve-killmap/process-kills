import asyncio
import contextlib

import live
import entities


def test_process_sequence_kill_is_async():
    assert asyncio.iscoroutinefunction(live._process_sequence_kill)


def test_live_listener_accepts_esi_kwarg():
    import inspect
    params = inspect.signature(live.live_listener).parameters
    assert "esi" in params


def test_process_sequence_kill_resolves_and_stubs_war_on_inserted(monkeypatch):
    calls = []

    @contextlib.contextmanager
    def _fake_get_connection():
        yield object()

    monkeypatch.setattr(live, "get_connection", _fake_get_connection)
    monkeypatch.setattr(live, "insert_kill", lambda conn, parsed: True)
    monkeypatch.setattr(live, "increment_processed_kills", lambda conn, date: None)
    monkeypatch.setattr(live, "insert_war_stub",
                        lambda conn, war_id: calls.append(("war_stub", war_id)))
    monkeypatch.setattr(live, "_record_freshness", lambda t: None)

    async def _fake_ensure(conn, esi, parsed):
        calls.append(("ensure", parsed["killmail_id"]))
        return True

    monkeypatch.setattr(entities, "ensure_kill_entities", _fake_ensure)

    data = {
        "killmail_id": 999,
        "hash": "abc",
        "esi": {
            "killmail_id": 999,
            "killmail_time": "2024-01-01T00:00:00Z",
            "solar_system_id": 30000142,
            "victim": {
                "ship_type_id": 587,
                "damage_taken": 1,
                "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                "character_id": 5,
                "corporation_id": 6,
            },
            "attackers": [],
            "war_id": 42,
        },
    }

    result, parsed = asyncio.run(live._process_sequence_kill(data, 1, object()))
    assert result == "inserted"
    assert ("ensure", 999) in calls          # entities resolved on the inserted path
    assert ("war_stub", 42) in calls         # war stub written
    # war stub is written before entity resolution, per the inserted-branch order
    assert calls.index(("war_stub", 42)) < calls.index(("ensure", 999))
