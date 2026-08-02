# tests/test_live_facets.py
import asyncio
import contextlib
import types

import facets as facets_mod
import live

# config.facets is a FROZEN dataclass, so we can't setattr its attribute; instead
# replace live's module-level `config` with a namespace. _process_sequence_kill's
# only config access is `config.facets.enabled`. esi=None skips entity resolution
# (a separate, DB-touching path) so this test isolates the facet hook.


def _run(monkeypatch, enabled, war_id=None, victim_extra=None):
    captured = {}

    @contextlib.contextmanager
    def _fake_conn():
        yield object()

    monkeypatch.setattr(live, "get_connection", _fake_conn)
    monkeypatch.setattr(live, "insert_kill", lambda conn, parsed: True)
    monkeypatch.setattr(live, "increment_processed_kills", lambda conn, date: None)
    monkeypatch.setattr(live, "insert_war_stub", lambda conn, war_id: None)
    monkeypatch.setattr(live, "_record_freshness", lambda t: None)
    monkeypatch.setattr(live, "insert_facets",
                        lambda conn, kid, sid, t, rows: captured.update(kid=kid, rows=rows))
    monkeypatch.setattr(live, "config",
                        types.SimpleNamespace(facets=types.SimpleNamespace(enabled=enabled)))

    victim = {"ship_type_id": 587, "damage_taken": 1,
              "position": {"x": 1.0, "y": 2.0, "z": 3.0}}
    victim.update(victim_extra or {})
    data = {
        "killmail_id": 999, "hash": "abc",
        "esi": {"killmail_id": 999, "killmail_time": "2024-01-01T00:00:00Z",
                "solar_system_id": 30000142, "victim": victim,
                "attackers": [], "war_id": war_id},
    }
    result, _parsed = asyncio.run(live._process_sequence_kill(data, 1, None))
    return result, captured


def test_inserted_kill_writes_facets_when_enabled(monkeypatch):
    result, captured = _run(monkeypatch, enabled=True,
                            victim_extra={"character_id": 5, "corporation_id": 6})
    assert result == "inserted"
    assert captured["kid"] == 999
    assert (facets_mod.SHIP, 587, facets_mod.VICTIM) in set(captured["rows"])


def test_facets_not_written_when_disabled(monkeypatch):
    result, captured = _run(monkeypatch, enabled=False)
    assert result == "inserted"
    assert captured == {}  # hook gated off, insert_facets never called
