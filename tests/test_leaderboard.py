# tests/test_leaderboard.py
from dataclasses import replace
from datetime import datetime, timezone

import pytest
from prometheus_client import REGISTRY

import leaderboard


class _FakeCursor:
    """Scripted cursor: one script entry per execute() — a list of rows (SELECT)
    or an int rowcount (DML). Records every (sql, params)."""

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


def test_windows_match_backend_intervals():
    # Same keys + literals as the backend's _TOP_INTERVALS, plus all-time.
    assert leaderboard.WINDOWS == {
        "day": "1 day",
        "week": "7 days",
        "month": "30 days",
        "six_months": "6 months",
        "year": "1 year",
        "all": None,
    }
    assert leaderboard.FAST_WINDOWS + leaderboard.SLOW_WINDOWS == list(leaderboard.WINDOWS)


# compute_board ---------------------------------------------------------------


def test_compute_board_windowed_uses_backend_predicate(monkeypatch):
    _pin_top_n(monkeypatch, 25)
    conn = _FakeConn([0, 275])  # DELETE rowcount, INSERT rowcount

    written = leaderboard.compute_board(conn, "week")

    assert written == 275
    (del_sql, del_params), (ins_sql, ins_params) = conn.cur.executed
    assert del_sql == "DELETE FROM entity_leaderboard WHERE window_key = %(window)s"
    assert ins_sql.startswith("INSERT INTO entity_leaderboard")
    assert "day > CURRENT_DATE - %(interval)s::interval" in ins_sql
    assert "ORDER BY SUM(kill_count) DESC, facet_value" in ins_sql
    assert "PARTITION BY facet_kind, role" in ins_sql
    assert "WHERE rank <= %(top_n)s" in ins_sql
    assert "'" not in ins_sql.split("CURRENT_DATE")[1].split("::interval")[0]  # bound, not inlined
    assert ins_params == {
        "window": "week",
        "kinds": [1, 2, 3, 4, 5, 6],
        "roles": [0, 1],
        "interval": "7 days",
        "top_n": 25,
    }
    assert conn.commits == 1


def test_compute_board_all_time_has_no_day_predicate(monkeypatch):
    _pin_top_n(monkeypatch, 25)
    conn = _FakeConn([0, 275])
    leaderboard.compute_board(conn, "all")
    ins_sql, ins_params = conn.cur.executed[1]
    assert "CURRENT_DATE" not in ins_sql
    assert ins_params["interval"] is None and ins_params["window"] == "all"


def test_compute_board_honors_top_n_config(monkeypatch):
    _pin_top_n(monkeypatch, 7)
    conn = _FakeConn([0, 77])
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


# compute_boards --------------------------------------------------------------


def test_compute_boards_skips_without_watermark(monkeypatch):
    conn = _FakeConn([[]])  # rollups.read_watermark -> no row (the real guard SQL)
    monkeypatch.setattr(leaderboard, "compute_board", lambda c, w: pytest.fail("must not run"))
    assert leaderboard.compute_boards(conn, ["day"]) is False


def test_compute_boards_runs_every_window_and_records_metrics(monkeypatch):
    conn = _FakeConn()
    monkeypatch.setattr(leaderboard, "read_watermark", lambda c: datetime.now(timezone.utc))
    ran = []
    monkeypatch.setattr(leaderboard, "compute_board", lambda c, w: ran.append(w) or 10)
    before = _metric("eve_killmap_leaderboard_computations_total", {"window": "day", "result": "success"})

    assert leaderboard.compute_boards(conn, ["day", "week"]) is True

    assert ran == ["day", "week"]
    assert _metric("eve_killmap_leaderboard_computations_total", {"window": "day", "result": "success"}) == before + 1
    assert _metric("eve_killmap_leaderboard_last_success_timestamp_seconds", {"window": "day"}) > 0
    assert _metric("eve_killmap_leaderboard_rows", {"window": "day"}) == 10


def test_compute_boards_stops_between_windows_when_asked(monkeypatch):
    conn = _FakeConn()
    monkeypatch.setattr(leaderboard, "read_watermark", lambda c: datetime.now(timezone.utc))
    ran = []
    monkeypatch.setattr(leaderboard, "compute_board", lambda c, w: ran.append(w) or 5)
    calls = {"n": 0}

    def should_stop():
        calls["n"] += 1
        return calls["n"] > 1  # first window runs, then stop

    assert leaderboard.compute_boards(conn, ["day", "week", "month"], should_stop) is True
    assert ran == ["day"]
    # stopped before anything ran -> skipped
    assert leaderboard.compute_boards(conn, ["day"], lambda: True) is False


def test_compute_boards_continues_past_a_failing_window_then_raises(monkeypatch):
    conn = _FakeConn()
    monkeypatch.setattr(leaderboard, "read_watermark", lambda c: datetime.now(timezone.utc))
    ran = []

    def board(c, w):
        ran.append(w)
        if w == "month":
            raise RuntimeError("spill")
        return 5

    monkeypatch.setattr(leaderboard, "compute_board", board)
    before = _metric("eve_killmap_leaderboard_computations_total", {"window": "month", "result": "failed"})

    with pytest.raises(RuntimeError, match="month"):
        leaderboard.compute_boards(conn, ["day", "month", "year"])

    assert ran == ["day", "month", "year"]  # the failure did not stop the others
    assert _metric("eve_killmap_leaderboard_computations_total", {"window": "month", "result": "failed"}) == before + 1
