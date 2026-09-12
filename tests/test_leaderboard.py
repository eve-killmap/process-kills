# tests/test_leaderboard.py
from datetime import date, datetime, timedelta, timezone

import pytest

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
    conn = _FakeConn([0, 1234])  # DELETE rowcount, INSERT rowcount
    day = date(2024, 1, 2)

    written = leaderboard.roll_day(conn, day)

    assert written == 1234
    (del_sql, del_params), (ins_sql, ins_params) = conn.cur.executed
    assert del_sql.startswith("DELETE FROM entity_kills_daily")
    assert "facet_kind = ANY(%(kinds)s)" in del_sql
    assert "day = %(day)s" in del_sql
    assert ins_sql.startswith("INSERT INTO entity_kills_daily")
    assert "JOIN kill_facets f USING (killmail_id)" in ins_sql
    assert "k.killmail_time >= (%(day)s::timestamp AT TIME ZONE 'UTC')" in ins_sql
    assert "k.killmail_time < ((%(day)s + 1)::timestamp AT TIME ZONE 'UTC')" in ins_sql
    assert "GROUP BY f.facet_kind, f.role, f.facet_value" in ins_sql
    assert del_params == ins_params == {"kinds": [1, 2, 3, 4, 5, 6], "roles": [0, 1], "day": day}
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
