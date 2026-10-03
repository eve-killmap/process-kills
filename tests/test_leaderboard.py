import logging
from dataclasses import replace
from datetime import datetime, timezone

import pytest
from prometheus_client import REGISTRY

import leaderboard


class _FakeCursor:

    def __init__(self, script):
        self._script = list(script)
        self.executed = []
        self.rowcount = -1
        self._rows = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        entry = self._script.pop(0) if self._script else []
        if isinstance(entry, int):
            self.rowcount = entry
            self._rows = []
        else:
            self._rows = list(entry)
            self.rowcount = len(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


class _FakeConn:
    def __init__(self, script=()):
        self.cur = _FakeCursor(script)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _metric(name, labels=None):
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def _pin_top_n(monkeypatch, n):
    pinned = replace(
        leaderboard.config,
        leaderboard=replace(leaderboard.config.leaderboard, top_n=n),
    )
    monkeypatch.setattr(leaderboard, "config", pinned)


def _pin_player_factions(monkeypatch, ids):
    pinned = replace(
        leaderboard.config,
        leaderboard=replace(leaderboard.config.leaderboard, player_factions=tuple(ids)),
    )
    monkeypatch.setattr(leaderboard, "config", pinned)


def test_windows_match_backend_intervals():
    assert leaderboard.WINDOWS == {
        "day": "1 day",
        "week": "7 days",
        "month": "30 days",
        "six_months": "6 months",
        "year": "1 year",
        "all": None,
    }
    assert leaderboard.FAST_WINDOWS + leaderboard.SLOW_WINDOWS == list(
        leaderboard.WINDOWS
    )


def test_scopes_and_npc_constants():
    assert leaderboard.SCOPES == ["all", "players"]
    assert leaderboard.NPC_CHARACTER_IDS == (3_000_000, 3_999_999)
    assert leaderboard.NPC_CORPORATION_IDS == (1_000_000, 1_999_999)
    assert leaderboard.NPC_TYPE_CATEGORY == 11


def test_compute_board_windowed_uses_backend_predicate(monkeypatch):
    _pin_top_n(monkeypatch, 25)
    _pin_player_factions(monkeypatch, (500001, 500010))
    conn = _FakeConn([0, [("all",)] * 275 + [("players",)] * 260])

    written = leaderboard.compute_board(conn, "week")

    assert written == {"all": 275, "players": 260}
    (del_sql, del_params), (ins_sql, ins_params) = conn.cur.executed
    assert del_sql == "DELETE FROM entity_leaderboard WHERE window_key = %(window)s"
    assert ins_sql.startswith("INSERT INTO entity_leaderboard")
    assert (
        "(facet_kind, role, window_key, scope, rank, facet_value, kill_count, computed_at)"
        in ins_sql
    )
    assert "WITH totals AS MATERIALIZED" in ins_sql
    assert ins_sql.count("FROM entity_kills_daily") == 1
    assert "day > CURRENT_DATE - %(interval)s::interval" in ins_sql
    assert "'" not in ins_sql.split("CURRENT_DATE")[1].split("::interval")[0]
    assert ins_sql.count("PARTITION BY facet_kind, role") == 2
    assert ins_sql.count("ORDER BY kill_count DESC, facet_value") == 2
    assert ins_sql.count("WHERE rank <= %(top_n)s") == 2
    assert "'all'" in ins_sql and "'players'" in ins_sql
    assert ins_sql.endswith("RETURNING scope")
    assert (
        "facet_kind = 1 AND facet_value BETWEEN %(npc_character_min)s AND %(npc_character_max)s"
        in ins_sql
    )
    assert (
        "facet_kind = 2 AND facet_value BETWEEN %(npc_corporation_min)s AND %(npc_corporation_max)s"
        in ins_sql
    )
    assert (
        "facet_kind = 4 AND facet_value <> ALL(%(player_factions)s::bigint[])"
        in ins_sql
    )
    assert (
        "facet_kind IN (5, 6) AND EXISTS (SELECT 1 FROM types ty WHERE ty.id = t.facet_value AND ty.category_id = %(npc_category)s)"
        in ins_sql
    )
    for literal in (
        "3000000",
        "3999999",
        "1000000",
        "1999999",
        "500001",
        "category_id = 11",
    ):
        assert literal not in ins_sql
    assert ins_params == {
        "window": "week",
        "kinds": [1, 2, 3, 4, 5, 6],
        "roles": [0, 1],
        "interval": "7 days",
        "top_n": 25,
        "npc_character_min": 3_000_000,
        "npc_character_max": 3_999_999,
        "npc_corporation_min": 1_000_000,
        "npc_corporation_max": 1_999_999,
        "player_factions": [500001, 500010],
        "npc_category": 11,
    }
    assert isinstance(ins_params["player_factions"], list)
    assert conn.commits == 1


def test_compute_board_all_time_has_no_day_predicate(monkeypatch):
    _pin_top_n(monkeypatch, 25)
    conn = _FakeConn([0, [("all",)] * 10])
    written = leaderboard.compute_board(conn, "all")
    ins_sql, ins_params = conn.cur.executed[1]
    assert "CURRENT_DATE" not in ins_sql
    assert ins_params["interval"] is None and ins_params["window"] == "all"
    assert written == {"all": 10, "players": 0}


def test_compute_board_honors_top_n_config(monkeypatch):
    _pin_top_n(monkeypatch, 7)
    conn = _FakeConn([0, []])
    leaderboard.compute_board(conn, "day")
    assert conn.cur.executed[1][1]["top_n"] == 7


def test_compute_board_unknown_window_is_a_bug():
    with pytest.raises(KeyError):
        leaderboard.compute_board(_FakeConn(), "fortnight")


def test_compute_board_rolls_back_on_failure(monkeypatch):
    _pin_top_n(monkeypatch, 25)
    conn = _FakeConn()

    def boom(sql, params=None):
        raise RuntimeError("db down")

    conn.cur.execute = boom
    with pytest.raises(RuntimeError):
        leaderboard.compute_board(conn, "day")
    assert conn.rollbacks == 1 and conn.commits == 0


def test_compute_boards_skips_without_watermark(monkeypatch):
    conn = _FakeConn([[]])
    monkeypatch.setattr(
        leaderboard, "compute_board", lambda c, w: pytest.fail("must not run")
    )
    assert leaderboard.compute_boards(conn, ["day"]) is False


def test_compute_boards_runs_every_window_and_records_metrics(monkeypatch, caplog):
    conn = _FakeConn()
    monkeypatch.setattr(
        leaderboard, "read_watermark", lambda c: datetime.now(timezone.utc)
    )
    ran = []
    monkeypatch.setattr(
        leaderboard,
        "compute_board",
        lambda c, w: ran.append(w) or {"all": 10, "players": 8},
    )
    before = _metric(
        "eve_killmap_leaderboard_computations_total",
        {"window": "day", "result": "success"},
    )
    caplog.set_level(logging.INFO, logger="leaderboard")

    assert leaderboard.compute_boards(conn, ["day", "week"]) is True

    assert ran == ["day", "week"]
    assert (
        _metric(
            "eve_killmap_leaderboard_computations_total",
            {"window": "day", "result": "success"},
        )
        == before + 1
    )
    assert (
        _metric(
            "eve_killmap_leaderboard_last_success_timestamp_seconds", {"window": "day"}
        )
        > 0
    )
    assert (
        _metric("eve_killmap_leaderboard_rows", {"window": "day", "scope": "all"}) == 10
    )
    assert (
        _metric("eve_killmap_leaderboard_rows", {"window": "day", "scope": "players"})
        == 8
    )
    assert "Leaderboard day: 10 all / 8 players rows" in caplog.text


def test_compute_boards_stops_between_windows_when_asked(monkeypatch):
    conn = _FakeConn()
    monkeypatch.setattr(
        leaderboard, "read_watermark", lambda c: datetime.now(timezone.utc)
    )
    ran = []
    monkeypatch.setattr(
        leaderboard,
        "compute_board",
        lambda c, w: ran.append(w) or {"all": 5, "players": 5},
    )
    calls = {"n": 0}

    def should_stop():
        calls["n"] += 1
        return calls["n"] > 1

    assert (
        leaderboard.compute_boards(conn, ["day", "week", "month"], should_stop) is True
    )
    assert ran == ["day"]
    assert leaderboard.compute_boards(conn, ["day"], lambda: True) is False


def test_compute_boards_continues_past_a_failing_window_then_raises(monkeypatch):
    conn = _FakeConn()
    monkeypatch.setattr(
        leaderboard, "read_watermark", lambda c: datetime.now(timezone.utc)
    )
    ran = []

    def board(c, w):
        ran.append(w)
        if w == "month":
            raise RuntimeError("spill")
        return {"all": 5, "players": 5}

    monkeypatch.setattr(leaderboard, "compute_board", board)
    before = _metric(
        "eve_killmap_leaderboard_computations_total",
        {"window": "month", "result": "failed"},
    )

    with pytest.raises(RuntimeError, match="month"):
        leaderboard.compute_boards(conn, ["day", "month", "year"])

    assert ran == ["day", "month", "year"]
    assert (
        _metric(
            "eve_killmap_leaderboard_computations_total",
            {"window": "month", "result": "failed"},
        )
        == before + 1
    )
