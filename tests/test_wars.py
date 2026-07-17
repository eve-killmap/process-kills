# tests/test_wars.py
import asyncio
import contextlib
from datetime import datetime, timedelta, timezone

import db
import wars
from wars import compute_refresh_after, parse_war


def test_parse_war_maps_columns_and_allies():
    data = {
        "id": 123,
        "declared": "2024-01-01T00:00:00Z",
        "started": "2024-01-02T00:00:00Z",
        "finished": None,
        "retracted": None,
        "mutual": False,
        "open_for_allies": True,
        "aggressor": {
            "corporation_id": 98000001,
            "alliance_id": None,
            "ships_killed": 5,
            "isk_destroyed": 1.5e9,
        },
        "defender": {
            "corporation_id": None,
            "alliance_id": 99000001,
            "ships_killed": 2,
            "isk_destroyed": 3.0e8,
        },
        "allies": [{"corporation_id": 98000002}, {"alliance_id": 99000002}],
    }
    row = parse_war(data)
    assert row["war_id"] == 123
    assert row["declared"] == "2024-01-01T00:00:00Z"
    assert row["started"] == "2024-01-02T00:00:00Z"
    assert row["finished"] is None
    assert row["retracted"] is None
    assert row["mutual"] is False
    assert row["open_for_allies"] is True
    assert row["aggressor_corporation_id"] == 98000001
    assert row["aggressor_alliance_id"] is None
    assert row["aggressor_ships_killed"] == 5
    assert row["aggressor_isk_destroyed"] == 1.5e9
    assert row["defender_corporation_id"] is None
    assert row["defender_alliance_id"] == 99000001
    assert row["defender_ships_killed"] == 2
    assert row["defender_isk_destroyed"] == 3.0e8
    assert row["ally_corporation_ids"] == [98000002]
    assert row["ally_alliance_ids"] == [99000002]


def test_refresh_after_active_uses_expires():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    expires = now + timedelta(hours=6)
    assert compute_refresh_after(None, now, expires) == expires


def test_refresh_after_future_finished_gets_one_more_fetch():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    finished = now + timedelta(days=2)
    assert compute_refresh_after(finished, now, None) == finished + timedelta(minutes=1)


def test_refresh_after_terminal_is_none():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    finished = now - timedelta(days=1)
    assert compute_refresh_after(finished, now, None) is None


from wars import war_outcome


def test_war_outcome_terminal_is_finished():
    assert war_outcome(None) == "finished"


def test_war_outcome_nonterminal_is_active():
    from datetime import datetime, timezone
    assert war_outcome(datetime(2030, 1, 1, tzinfo=timezone.utc)) == "active"


def test_refresh_one_war_transient_leaves_war_retryable(monkeypatch):
    calls = {}

    @contextlib.contextmanager
    def _conn():
        yield object()

    monkeypatch.setattr(db, "get_connection", _conn)
    monkeypatch.setattr(db, "set_war_refresh_after",
                        lambda c, wid, ra: calls.__setitem__("backoff", (wid, ra)))
    monkeypatch.setattr(db, "upsert_war",
                        lambda *a: calls.__setitem__("terminal", a))

    class _Esi:
        async def fetch_war(self, wid):
            raise RuntimeError("transient 5xx exhausted")

    asyncio.run(wars._refresh_one_war(_Esi(), 42))
    assert "backoff" in calls        # refresh_after pushed out -> still retryable
    assert "terminal" not in calls   # NOT marked terminal


def test_refresh_one_war_404_is_terminal(monkeypatch):
    calls = {}

    @contextlib.contextmanager
    def _conn():
        yield object()

    monkeypatch.setattr(db, "get_connection", _conn)
    monkeypatch.setattr(db, "set_war_refresh_after",
                        lambda c, wid, ra: calls.__setitem__("backoff", True))
    monkeypatch.setattr(db, "upsert_war",
                        lambda c, row, resolved, ra: calls.__setitem__("terminal_ra", ra))

    class _Esi:
        async def fetch_war(self, wid):
            return None  # genuine 404

    asyncio.run(wars._refresh_one_war(_Esi(), 42))
    assert calls.get("terminal_ra") is None   # refresh_after=None => terminal
    assert "backoff" not in calls
