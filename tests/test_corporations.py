from datetime import datetime, timedelta, timezone

from corporations import (
    ACTIVE_REFRESH_INTERVAL,
    compute_corp_refresh_after,
    parse_corporation,
)


def test_parse_corporation_maps_fields():
    data = {
        "name": "Test Corp",
        "ticker": "TEST",
        "alliance_id": 99000001,
        "date_founded": "2010-01-01T00:00:00Z",
        "member_count": 42,
    }
    assert parse_corporation(data) == {
        "name": "Test Corp",
        "ticker": "TEST",
        "alliance_id": 99000001,
        "date_founded": "2010-01-01T00:00:00Z",
        "member_count": 42,
    }


def test_parse_corporation_does_not_emit_active():
    # active is derived from member_count downstream, not stored in the parse dict.
    assert "active" not in parse_corporation({"member_count": 5})


def test_parse_corporation_missing_fields_default_to_none():
    row = parse_corporation({})
    assert row["name"] is None
    assert row["alliance_id"] is None
    assert row["member_count"] is None


def test_compute_refresh_after_active_adds_interval():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    assert compute_corp_refresh_after(5, now, timedelta(0)) == now + ACTIVE_REFRESH_INTERVAL
    assert ACTIVE_REFRESH_INTERVAL == timedelta(hours=24)


def test_compute_refresh_after_active_includes_jitter():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    jitter = timedelta(minutes=30)
    assert compute_corp_refresh_after(5, now, jitter) == now + ACTIVE_REFRESH_INTERVAL + jitter


def test_compute_refresh_after_zero_members_is_terminal():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    assert compute_corp_refresh_after(0, now, timedelta(0)) is None


def test_compute_refresh_after_null_members_is_terminal():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    assert compute_corp_refresh_after(None, now, timedelta(0)) is None


import asyncio
import contextlib

import corporations
import db


@contextlib.contextmanager
def _fake_conn():
    yield object()


def _run_refresh(monkeypatch, esi, ids):
    calls = {"upsert": [], "closed": [], "backoff": []}
    # Patch the db module attributes (corporations.py calls db.<fn> on the same
    # module object); monkeypatch auto-restores so nothing leaks to other tests.
    monkeypatch.setattr(db, "upsert_corporations", lambda conn, rows: calls["upsert"].extend(rows))
    monkeypatch.setattr(db, "mark_corporation_closed", lambda conn, cid: calls["closed"].append(cid))
    monkeypatch.setattr(db, "set_corporation_refresh_after", lambda conn, cid, ra: calls["backoff"].append((cid, ra)))
    asyncio.run(corporations.refresh_corporations(object(), esi, ids, concurrency=5))
    return calls


class _Esi:
    def __init__(self, mapping):
        self._m = mapping  # id -> "active"|"closed"|"404"|"error"

    async def get_corporation(self, cid):
        kind = self._m[cid]
        if kind == "404":
            return None
        if kind == "error":
            raise RuntimeError("transient 5xx")
        # active corp has members; "closed" corp has 0 members (terminal).
        return {"name": f"C{cid}", "ticker": "T", "alliance_id": 1,
                "member_count": 3 if kind == "active" else 0}


def test_refresh_active_upserts_with_future_refresh_after(monkeypatch):
    calls = _run_refresh(monkeypatch, _Esi({1: "active"}), [1])
    assert calls["closed"] == [] and calls["backoff"] == []
    (row,) = calls["upsert"]
    assert row[0] == 1 and row[6] is not None  # refresh_after set (future)


def test_refresh_closed_upserts_terminal(monkeypatch):
    calls = _run_refresh(monkeypatch, _Esi({2: "closed"}), [2])
    (row,) = calls["upsert"]
    assert row[0] == 2 and row[6] is None  # 0 members -> refresh_after NULL (terminal)


def test_refresh_404_marks_closed_no_upsert(monkeypatch):
    calls = _run_refresh(monkeypatch, _Esi({3: "404"}), [3])
    assert calls["closed"] == [3] and calls["upsert"] == []


def test_refresh_transient_error_sets_backoff_no_upsert(monkeypatch):
    calls = _run_refresh(monkeypatch, _Esi({4: "error"}), [4])
    assert calls["upsert"] == []
    assert [cid for cid, _ra in calls["backoff"]] == [4]


def test_refresh_non_dict_body_is_error_not_raise(monkeypatch):
    class _BadEsi:
        async def get_corporation(self, cid):
            return []  # 200 body that isn't a dict

    calls = _run_refresh(monkeypatch, _BadEsi(), [9])
    assert calls["upsert"] == []
    assert [cid for cid, _ra in calls["backoff"]] == [9]


def test_refresh_due_batch_refreshes_due_ids(monkeypatch):
    seen = {}
    monkeypatch.setattr(corporations.db, "get_connection", _fake_conn)
    monkeypatch.setattr(corporations.db, "count_due_corporations", lambda c: 2)
    monkeypatch.setattr(corporations.db, "get_due_corporations", lambda c, n: [10, 11])

    async def fake_refresh(conn, esi, ids, concurrency):
        seen["ids"] = list(ids)

    monkeypatch.setattr(corporations, "refresh_corporations", fake_refresh)
    asyncio.run(corporations._refresh_due_batch(object()))
    assert seen["ids"] == [10, 11]


def test_refresh_due_batch_noop_when_none_due(monkeypatch):
    seen = {}
    monkeypatch.setattr(corporations.db, "get_connection", _fake_conn)
    monkeypatch.setattr(corporations.db, "count_due_corporations", lambda c: 0)
    monkeypatch.setattr(corporations.db, "get_due_corporations", lambda c, n: [])

    async def fake_refresh(*a, **k):
        seen["called"] = True

    monkeypatch.setattr(corporations, "refresh_corporations", fake_refresh)
    asyncio.run(corporations._refresh_due_batch(object()))
    assert "called" not in seen


def test_scheduler_disabled_returns_immediately(monkeypatch):
    # config.corporations is a frozen dataclass, and so is the outer Config that
    # holds it -> `monkeypatch.setattr(config.config, "corporations", ...)` hits
    # the frozen instance's __setattr__ and raises FrozenInstanceError. Instead,
    # rebuild a whole replacement Config via dataclasses.replace (construction is
    # unaffected by frozen=True) and monkeypatch the module-level `config` name
    # that corporations.py resolves at call time (corporations.py does
    # `from config import config`, so this is what the scheduler sees).
    import config as _cfg
    from dataclasses import replace

    new_config = replace(
        _cfg.config, corporations=replace(_cfg.config.corporations, enabled=False)
    )
    monkeypatch.setattr(corporations, "config", new_config)
    ev = asyncio.Event()
    asyncio.run(corporations.corporation_refresh_scheduler(object(), ev))  # returns at once
