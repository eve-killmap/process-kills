# tests/test_rollups.py
import logging
from datetime import date, datetime, timedelta, timezone

import pytest
from prometheus_client import REGISTRY

import rollups


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


def test_constants():
    assert rollups.KINDS == [1, 2, 3, 4, 5, 6]
    assert rollups.ROLES == [0, 1]
    assert rollups.DIRTY_OVERLAP == timedelta(hours=1)
    assert rollups.DIRTY_DAYS_WARN == 10
    assert rollups.WATERMARK_NAME == "entity_kills_daily"  # stored key; never renamed


def test_day_arithmetic_relies_on_a_utc_session():
    # Policy: no AT TIME ZONE in the rollup SQL; every session that touches these
    # tables runs with TimeZone = UTC (mv_refresh pins it; manual SQL must too).
    for sql in (rollups._ENTITY_BOUNDS, rollups._ENTITY_INSERT, rollups._SYSTEM_INSERT):
        assert "AT TIME ZONE" not in sql


def test_every_per_day_statement_shares_one_day_definition():
    # The invariant between the two rollups (victim-ship facets per day ==
    # kills per day) is only structural if all three use the same day bounds.
    assert rollups._DAY_RANGE == (
        "k.killmail_time >= %(day)s::timestamptz "
        "AND k.killmail_time < (%(day)s + 1)::timestamptz"
    )
    for sql in (rollups._ENTITY_BOUNDS, rollups._ENTITY_INSERT, rollups._SYSTEM_INSERT):
        assert rollups._DAY_RANGE in sql


# roll_entity_day -------------------------------------------------------------


def test_roll_entity_day_deletes_then_inserts_in_one_transaction():
    # DELETE rowcount, the day's [min, max] killmail_id, INSERT rowcount
    conn = _FakeConn([0, [(100, 200)], 1234])
    day = date(2024, 1, 2)

    written = rollups.roll_entity_day(conn, day)

    assert written == 1234
    (del_sql, del_params), (bounds_sql, bounds_params), (ins_sql, ins_params) = (
        conn.cur.executed
    )
    assert del_sql.startswith("DELETE FROM entity_kills_daily")
    assert "facet_kind = ANY(%(kinds)s)" in del_sql
    assert "day = %(day)s" in del_sql
    # fenced (OFFSET 0 subquery): a bare min/max walks kills_pkey in id order
    assert bounds_sql.startswith("SELECT min(killmail_id), max(killmail_id) FROM (")
    assert "OFFSET 0) AS day_kills" in bounds_sql
    assert ins_sql.startswith("INSERT INTO entity_kills_daily")
    assert "JOIN kill_facets f USING (killmail_id)" in ins_sql
    assert "k.killmail_time >= %(day)s::timestamptz" in ins_sql
    assert "k.killmail_time < (%(day)s + 1)::timestamptz" in ins_sql
    # one contiguous slice of idx_facet_kill, never a table scan or per-kill probes
    assert "f.killmail_id BETWEEN %(lo)s AND %(hi)s" in ins_sql
    assert "GROUP BY f.facet_kind, f.role, f.facet_value" in ins_sql
    base = {"kinds": [1, 2, 3, 4, 5, 6], "roles": [0, 1], "day": day}
    assert del_params == bounds_params == base
    assert ins_params == {**base, "lo": 100, "hi": 200}
    assert conn.commits == 1 and conn.rollbacks == 0


def test_roll_entity_day_with_no_kills_inserts_nothing():
    conn = _FakeConn([0, [(None, None)]])  # DELETE rowcount, no kills that day
    assert rollups.roll_entity_day(conn, date(2015, 1, 1)) == 0
    assert len(conn.cur.executed) == 2  # no INSERT issued
    assert conn.commits == 1 and conn.rollbacks == 0


def test_roll_entity_day_rolls_back_and_reraises_on_failure():
    conn = _FakeConn()

    def boom(sql, params=None):
        raise RuntimeError("db down")

    conn.cur.execute = boom
    with pytest.raises(RuntimeError, match="db down"):
        rollups.roll_entity_day(conn, date(2024, 1, 2))
    assert conn.rollbacks == 1 and conn.commits == 0


# roll_system_day -------------------------------------------------------------


def test_roll_system_day_deletes_then_inserts_in_one_transaction():
    conn = _FakeConn([0, 2100])  # DELETE rowcount, INSERT rowcount
    day = date(2024, 1, 2)

    written = rollups.roll_system_day(conn, day)

    assert written == 2100
    (del_sql, del_params), (ins_sql, ins_params) = conn.cur.executed
    assert del_sql == "DELETE FROM system_kills_daily WHERE day = %(day)s"
    assert ins_sql.startswith("INSERT INTO system_kills_daily (solar_system_id, day, kill_count)")
    assert "SELECT k.solar_system_id, %(day)s, COUNT(*) FROM kills k" in ins_sql
    assert "k.killmail_time >= %(day)s::timestamptz" in ins_sql
    assert "k.killmail_time < (%(day)s + 1)::timestamptz" in ins_sql
    assert ins_sql.endswith("GROUP BY k.solar_system_id")
    assert del_params == ins_params == {"day": day}
    assert conn.commits == 1 and conn.rollbacks == 0


def test_roll_system_day_rolls_back_and_reraises_on_failure():
    conn = _FakeConn()

    def boom(sql, params=None):
        raise RuntimeError("db down")

    conn.cur.execute = boom
    with pytest.raises(RuntimeError, match="db down"):
        rollups.roll_system_day(conn, date(2024, 1, 2))
    assert conn.rollbacks == 1 and conn.commits == 0


# watermark / clock / dirty days ----------------------------------------------


def test_read_watermark_none_when_absent():
    conn = _FakeConn([[]])
    assert rollups.read_watermark(conn) is None
    sql, params = conn.cur.executed[0]
    assert "FROM rollup_state WHERE name = %s" in sql
    assert params == ("entity_kills_daily",)


def test_read_watermark_returns_value():
    wm = datetime(2024, 1, 2, 3, tzinfo=timezone.utc)
    conn = _FakeConn([[(wm,)]])
    assert rollups.read_watermark(conn) == wm


def test_set_watermark_upserts_and_commits():
    conn = _FakeConn([1])
    ts = datetime(2024, 1, 2, 3, tzinfo=timezone.utc)
    rollups.set_watermark(conn, ts)
    sql, params = conn.cur.executed[0]
    assert sql.startswith("INSERT INTO rollup_state")
    assert "ON CONFLICT (name) DO UPDATE SET watermark = EXCLUDED.watermark" in sql
    assert params == ("entity_kills_daily", ts)
    assert conn.commits == 1


def test_db_now_uses_database_clock():
    now = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    conn = _FakeConn([[(now,)]])
    assert rollups.db_now(conn) == now
    assert conn.cur.executed[0][0] == "SELECT now()"


def test_find_dirty_days_returns_dates_since():
    since = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    conn = _FakeConn([[(date(2024, 1, 1),), (date(2024, 1, 2),)]])

    days = rollups.find_dirty_days(conn, since)

    assert days == [date(2024, 1, 1), date(2024, 1, 2)]
    sql, params = conn.cur.executed[0]
    assert "SELECT DISTINCT killmail_time::date" in sql
    assert "FROM kills WHERE inserted_time > %s" in sql
    assert params == (since,)


# roll_dirty_days -------------------------------------------------------------


def test_roll_dirty_days_skips_without_watermark(monkeypatch):
    conn = _FakeConn([[]])  # SELECT watermark -> no row
    called = []
    monkeypatch.setattr(rollups, "roll_entity_day", lambda c, d: called.append(d))
    monkeypatch.setattr(rollups, "roll_system_day", lambda c, d: called.append(d))

    assert rollups.roll_dirty_days(conn) is False
    assert called == []
    assert len(conn.cur.executed) == 1
    assert conn.commits == 0


def test_roll_dirty_days_rolls_both_tables_per_day_and_advances_watermark(monkeypatch):
    wm = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    t0 = datetime(2024, 1, 2, 12, tzinfo=timezone.utc)
    conn = _FakeConn()
    seen = {}
    monkeypatch.setattr(rollups, "read_watermark", lambda c: wm)
    monkeypatch.setattr(rollups, "db_now", lambda c: t0)
    monkeypatch.setattr(
        rollups,
        "find_dirty_days",
        lambda c, since: seen.setdefault("since", since)
        and [date(2024, 1, 1), date(2024, 1, 2)],
    )
    rolled = []
    monkeypatch.setattr(
        rollups, "roll_entity_day", lambda c, d: rolled.append(("entity", d)) or 7
    )
    monkeypatch.setattr(
        rollups, "roll_system_day", lambda c, d: rolled.append(("system", d)) or 3
    )
    monkeypatch.setattr(rollups, "set_watermark", lambda c, ts: seen.setdefault("wm", ts))
    before = _metric("eve_killmap_rollup_days_rolled_total")

    assert rollups.roll_dirty_days(conn) is True

    assert seen["since"] == wm - timedelta(hours=1)
    assert rolled == [
        ("entity", date(2024, 1, 1)),
        ("system", date(2024, 1, 1)),
        ("entity", date(2024, 1, 2)),
        ("system", date(2024, 1, 2)),
    ]
    assert seen["wm"] == t0
    assert _metric("eve_killmap_rollup_days_rolled_total") == before + 2
    assert _metric("eve_killmap_rollup_watermark_timestamp_seconds") == t0.timestamp()


def test_roll_dirty_days_does_not_advance_watermark_on_failure(monkeypatch):
    wm = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    conn = _FakeConn()
    monkeypatch.setattr(rollups, "read_watermark", lambda c: wm)
    monkeypatch.setattr(rollups, "db_now", lambda c: wm + timedelta(hours=2))
    monkeypatch.setattr(
        rollups, "find_dirty_days", lambda c, s: [date(2024, 1, 1), date(2024, 1, 2)]
    )
    monkeypatch.setattr(rollups, "roll_entity_day", lambda c, d: 1)

    def roll_system(c, d):
        if d == date(2024, 1, 2):
            raise RuntimeError("boom")
        return 1

    monkeypatch.setattr(rollups, "roll_system_day", roll_system)
    advanced = []
    monkeypatch.setattr(rollups, "set_watermark", lambda c, ts: advanced.append(ts))

    with pytest.raises(RuntimeError, match="boom"):
        rollups.roll_dirty_days(conn)
    assert advanced == []


def test_roll_dirty_days_stops_between_days_without_advancing_watermark(monkeypatch):
    wm = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    conn = _FakeConn()
    monkeypatch.setattr(rollups, "read_watermark", lambda c: wm)
    monkeypatch.setattr(rollups, "db_now", lambda c: wm + timedelta(hours=2))
    days = [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
    monkeypatch.setattr(rollups, "find_dirty_days", lambda c, s: days)
    rolled = []
    monkeypatch.setattr(rollups, "roll_entity_day", lambda c, d: rolled.append(d) or 1)
    monkeypatch.setattr(rollups, "roll_system_day", lambda c, d: 1)
    advanced = []
    monkeypatch.setattr(rollups, "set_watermark", lambda c, ts: advanced.append(ts))
    calls = {"n": 0}

    def should_stop():
        calls["n"] += 1
        return calls["n"] > 1  # first check passes, second (before day 2) stops

    assert rollups.roll_dirty_days(conn, should_stop=should_stop) is True  # day 1 was rolled
    assert rolled == [date(2024, 1, 1)]
    assert advanced == []  # the next cycle re-scans from the same watermark


def test_roll_dirty_days_returns_false_when_stopped_before_the_first_day(monkeypatch):
    wm = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    conn = _FakeConn()
    monkeypatch.setattr(rollups, "read_watermark", lambda c: wm)
    monkeypatch.setattr(rollups, "db_now", lambda c: wm + timedelta(hours=2))
    monkeypatch.setattr(rollups, "find_dirty_days", lambda c, s: [date(2024, 1, 1)])
    monkeypatch.setattr(rollups, "roll_entity_day", lambda c, d: pytest.fail("must not roll"))
    monkeypatch.setattr(rollups, "roll_system_day", lambda c, d: pytest.fail("must not roll"))
    advanced = []
    monkeypatch.setattr(rollups, "set_watermark", lambda c, ts: advanced.append(ts))

    # nothing was rolled -> "skipped", so the cycle publishes no invalidation
    assert rollups.roll_dirty_days(conn, should_stop=lambda: True) is False
    assert advanced == []


def test_roll_dirty_days_warns_on_large_dirty_day_list(monkeypatch, caplog):
    wm = datetime(2024, 1, 2, 10, tzinfo=timezone.utc)
    t0 = datetime(2024, 1, 2, 12, tzinfo=timezone.utc)
    conn = _FakeConn()
    monkeypatch.setattr(rollups, "read_watermark", lambda c: wm)
    monkeypatch.setattr(rollups, "db_now", lambda c: t0)
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(11)]
    monkeypatch.setattr(rollups, "find_dirty_days", lambda c, since: days)
    monkeypatch.setattr(rollups, "roll_entity_day", lambda c, d: 0)
    monkeypatch.setattr(rollups, "roll_system_day", lambda c, d: 0)
    advanced = []
    monkeypatch.setattr(rollups, "set_watermark", lambda c, ts: advanced.append(ts))

    with caplog.at_level(logging.WARNING):
        assert rollups.roll_dirty_days(conn) is True

    assert any(
        r.levelname == "WARNING" and "11 dirty days" in r.getMessage()
        for r in caplog.records
    )
    assert advanced == [t0]
