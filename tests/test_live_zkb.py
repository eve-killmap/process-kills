import asyncio
import contextlib
import types

import live


def _run(monkeypatch, data):
    captured = {}

    @contextlib.contextmanager
    def _fake_conn():
        yield object()

    monkeypatch.setattr(live, "get_connection", _fake_conn)
    monkeypatch.setattr(live, "insert_kill", lambda conn, parsed: True)
    monkeypatch.setattr(live, "increment_processed_kills", lambda conn, date: None)
    monkeypatch.setattr(live, "insert_war_stub", lambda conn, war_id: None)
    monkeypatch.setattr(live, "_record_freshness", lambda t: None)
    monkeypatch.setattr(live, "insert_facets", lambda *a, **k: None)
    monkeypatch.setattr(
        live,
        "insert_zkb_metadata",
        lambda conn, kid, sid, t, z: captured.update(kid=kid, sid=sid, t=t, z=z),
    )
    monkeypatch.setattr(
        live,
        "config",
        types.SimpleNamespace(facets=types.SimpleNamespace(enabled=False)),
    )
    result, _ = asyncio.run(live._process_sequence_kill(data, 1, None))
    return result, captured


def _data(with_zkb=True):
    d = {
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
            },
            "attackers": [],
            "war_id": None,
        },
    }
    if with_zkb:
        d["zkb"] = {
            "totalValue": 123.0,
            "npc": False,
            "solo": True,
            "awox": False,
            "labels": ["pvp", "loc:nullsec"],
        }
    return d


def test_zkb_written_on_inserted_kill(monkeypatch):
    result, captured = _run(monkeypatch, _data(with_zkb=True))
    assert result == "inserted"
    assert captured["kid"] == 999 and captured["sid"] == 30000142
    assert captured["t"] == "2024-01-01T00:00:00Z"
    assert captured["z"]["total_value"] == 123.0
    assert captured["z"]["labels"] == ["pvp", "loc:nullsec"]


def test_no_zkb_object_skips_write(monkeypatch):
    result, captured = _run(monkeypatch, _data(with_zkb=False))
    assert result == "inserted"
    assert captured == {}  # no zkb in response -> insert_zkb_metadata never called


def test_zkb_write_failure_does_not_abort_inserted(monkeypatch):
    def _boom(conn, kid, sid, t, z):
        raise RuntimeError("zkb db down")

    @contextlib.contextmanager
    def _fake_conn():
        yield object()

    monkeypatch.setattr(live, "get_connection", _fake_conn)
    monkeypatch.setattr(live, "insert_kill", lambda conn, parsed: True)
    monkeypatch.setattr(live, "increment_processed_kills", lambda conn, date: None)
    monkeypatch.setattr(live, "insert_war_stub", lambda conn, war_id: None)
    monkeypatch.setattr(live, "_record_freshness", lambda t: None)
    monkeypatch.setattr(live, "insert_facets", lambda *a, **k: None)
    monkeypatch.setattr(live, "insert_zkb_metadata", _boom)
    monkeypatch.setattr(
        live,
        "config",
        types.SimpleNamespace(facets=types.SimpleNamespace(enabled=False)),
    )
    result, _ = asyncio.run(live._process_sequence_kill(_data(with_zkb=True), 1, None))
    assert result == "inserted"  # zkb failure swallowed; kill still succeeds
