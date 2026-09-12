# tests/test_leaderboard.py
import logging
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

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
    assert leaderboard.KINDS == [1, 2, 3, 4, 5, 6]
    assert leaderboard.ROLES == [0, 1]
    assert leaderboard.DIRTY_OVERLAP == timedelta(hours=1)


def test_roll_day_deletes_then_inserts_in_one_transaction():
    # DELETE rowcount, the day's [min, max] killmail_id, INSERT rowcount
    conn = _FakeConn([0, [(100, 200)], 1234])
    day = date(2024, 1, 2)

    written = leaderboard.roll_day(conn, day)

    assert written == 1234
    (del_sql, del_params), (bounds_sql, bounds_params), (ins_sql, ins_params) = (
        conn.cur.executed
    )
    assert del_sql.startswith("DELETE FROM entity_kills_daily")
    assert "facet_kind = ANY(%(kinds)s)" in del_sql
    assert "day = %(day)s" in del_sql
    assert bounds_sql.startswith("SELECT min(killmail_id), max(killmail_id) FROM kills")
    assert ins_sql.startswith("INSERT INTO entity_kills_daily")
    assert "JOIN kill_facets f USING (killmail_id)" in ins_sql
    assert "k.killmail_time >= (%(day)s::timestamp AT TIME ZONE 'UTC')" in ins_sql
    assert "k.killmail_time < ((%(day)s + 1)::timestamp AT TIME ZONE 'UTC')" in ins_sql
    # one contiguous slice of idx_facet_kill, never a table scan or per-kill probes
    assert "f.killmail_id BETWEEN %(lo)s AND %(hi)s" in ins_sql
    assert "GROUP BY f.facet_kind, f.role, f.facet_value" in ins_sql
    base = {"kinds": [1, 2, 3, 4, 5, 6], "roles": [0, 1], "day": day}
    assert del_params == bounds_params == base
    assert ins_params == {**base, "lo": 100, "hi": 200}
    assert conn.commits == 1 and conn.rollbacks == 0


def test_roll_day_with_no_kills_inserts_nothing():
    conn = _FakeConn([0, [(None, None)]])  # DELETE rowcount, no kills that day
    assert leaderboard.roll_day(conn, date(2015, 1, 1)) == 0
    assert len(conn.cur.executed) == 2  # no INSERT issued
    assert conn.commits == 1 and conn.rollbacks == 0


def test_roll_day_rolls_back_and_reraises_on_failure():
    conn = _FakeConn()

    def boom(sql, params=None):
        raise RuntimeError("db down")

    conn.cur.execute = boom
    with pytest.raises(RuntimeError, match="db down"):
        leaderboard.roll_day(conn, date(2024, 1, 2))
    assert conn.rollbacks == 1 and conn.commits == 0


def test_read_watermark_none_when_absent():
    conn = _FakeConn([[]])
    assert leaderboard.read_watermark(conn) is None
    sql, params = conn.cur.executed[0]
    assert "FROM rollup_state WHERE name = %s" in sql
    assert params == ("entity_kills_daily",)


def test_read_watermark_returns_value():
    wm = datetime(2024, 1, 2, 3, tzinfo=timezone.utc)
    conn = _FakeConn([[(wm,)]])
    assert leaderboard.read_watermark(conn) == wm


def test_set_watermark_upserts_and_commits():
    conn = _FakeConn([1])
    ts = datetime(2024, 1, 2, 3, tzinfo=timezone.utc)
    leaderboard.set_watermark(conn, ts)
    sql, params = conn.cur.executed[0]
    assert sql.startswith("INSERT INTO rollup_state")
    assert "ON CONFLICT (name) DO UPDATE SET watermark = EXCLUDED.watermark" in sql
    assert params == ("entity_kills_daily", ts)
    assert conn.commits == 1


def test_db_now_uses_database_clock():
    now = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    conn = _FakeConn([[(now,)]])
    assert leaderboard.db_now(conn) == now
    assert conn.cur.executed[0][0] == "SELECT now()"


def test_find_dirty_days_returns_dates_since():
    since = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    conn = _FakeConn([[(date(2024, 1, 1),), (date(2024, 1, 2),)]])

    days = leaderboard.find_dirty_days(conn, since)

    assert days == [date(2024, 1, 1), date(2024, 1, 2)]
    sql, params = conn.cur.executed[0]
    assert "SELECT DISTINCT (killmail_time AT TIME ZONE 'UTC')::date" in sql
    assert "FROM kills WHERE inserted_time > %s" in sql
    assert params == (since,)


def _metric(name, labels=None):
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def _pin_top_n(monkeypatch, n):
    pinned = replace(
        leaderboard.config,
        leaderboard=replace(leaderboard.config.leaderboard, top_n=n),
    )
    monkeypatch.setattr(leaderboard, "config", pinned)


# roll_dirty_days -------------------------------------------------------------


def test_roll_dirty_days_skips_without_watermark(monkeypatch):
    conn = _FakeConn([[]])  # SELECT watermark -> no row
    called = []
    monkeypatch.setattr(leaderboard, "roll_day", lambda c, d: called.append(d))

    assert leaderboard.roll_dirty_days(conn) is False
    assert called == []
    assert len(conn.cur.executed) == 1
    assert conn.commits == 0


def test_roll_dirty_days_rolls_each_day_and_advances_watermark(monkeypatch):
    wm = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    t0 = datetime(2024, 1, 2, 12, tzinfo=timezone.utc)
    conn = _FakeConn()
    seen = {}
    monkeypatch.setattr(leaderboard, "read_watermark", lambda c: wm)
    monkeypatch.setattr(leaderboard, "db_now", lambda c: t0)
    monkeypatch.setattr(
        leaderboard,
        "find_dirty_days",
        lambda c, since: seen.setdefault("since", since)
        and [date(2024, 1, 1), date(2024, 1, 2)],
    )
    rolled = []
    monkeypatch.setattr(leaderboard, "roll_day", lambda c, d: rolled.append(d) or 7)
    monkeypatch.setattr(leaderboard, "set_watermark", lambda c, ts: seen.setdefault("wm", ts))
    before = _metric("eve_killmap_entity_rollup_days_rolled_total")

    assert leaderboard.roll_dirty_days(conn) is True

    assert seen["since"] == wm - timedelta(hours=1)
    assert rolled == [date(2024, 1, 1), date(2024, 1, 2)]
    assert seen["wm"] == t0
    assert _metric("eve_killmap_entity_rollup_days_rolled_total") == before + 2
    assert _metric("eve_killmap_entity_rollup_watermark_timestamp_seconds") == t0.timestamp()


def test_roll_dirty_days_does_not_advance_watermark_on_failure(monkeypatch):
    wm = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    conn = _FakeConn()
    monkeypatch.setattr(leaderboard, "read_watermark", lambda c: wm)
    monkeypatch.setattr(leaderboard, "db_now", lambda c: wm + timedelta(hours=2))
    monkeypatch.setattr(
        leaderboard, "find_dirty_days", lambda c, s: [date(2024, 1, 1), date(2024, 1, 2)]
    )

    def roll(c, d):
        if d == date(2024, 1, 2):
            raise RuntimeError("boom")
        return 1

    monkeypatch.setattr(leaderboard, "roll_day", roll)
    advanced = []
    monkeypatch.setattr(leaderboard, "set_watermark", lambda c, ts: advanced.append(ts))

    with pytest.raises(RuntimeError, match="boom"):
        leaderboard.roll_dirty_days(conn)
    assert advanced == []


def test_roll_dirty_days_warns_on_large_dirty_day_list(monkeypatch, caplog):
    wm = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    t0 = datetime(2024, 1, 2, 12, tzinfo=timezone.utc)
    conn = _FakeConn()
    monkeypatch.setattr(leaderboard, "read_watermark", lambda c: wm)
    monkeypatch.setattr(leaderboard, "db_now", lambda c: t0)
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(11)]
    monkeypatch.setattr(leaderboard, "find_dirty_days", lambda c, since: days)
    monkeypatch.setattr(leaderboard, "roll_day", lambda c, d: 0)
    advanced = []
    monkeypatch.setattr(leaderboard, "set_watermark", lambda c, ts: advanced.append(ts))

    with caplog.at_level(logging.WARNING):
        assert leaderboard.roll_dirty_days(conn) is True

    assert any(
        r.levelname == "WARNING" and "11 dirty days" in r.getMessage()
        for r in caplog.records
    )
    assert advanced == [t0]


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
    conn = _FakeConn([[]])
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
