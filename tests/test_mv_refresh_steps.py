# tests/test_mv_refresh_steps.py
import asyncio
from dataclasses import replace

from prometheus_client import REGISTRY

import mv_refresh
from mv_refresh import RefreshStep


def _val(name, labels=None):
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def _pin_leaderboard(monkeypatch, enabled):
    pinned = replace(
        mv_refresh.config,
        leaderboard=replace(mv_refresh.config.leaderboard, enabled=enabled),
    )
    monkeypatch.setattr(mv_refresh, "config", pinned)


def _boom():
    raise RuntimeError("step exploded")


# _run_step -------------------------------------------------------------------


def test_run_step_success_records_metrics():
    labels = {"cadence": "fast", "step": "t_ok", "result": "success"}
    before = _val("eve_killmap_refresh_step_runs_total", labels)
    assert mv_refresh._run_step(RefreshStep("t_ok", lambda: True, ["x"]), "fast") == "success"
    assert _val("eve_killmap_refresh_step_runs_total", labels) == before + 1
    assert _val("eve_killmap_refresh_step_duration_seconds_count", {"cadence": "fast", "step": "t_ok"}) >= 1


def test_run_step_skipped_when_step_did_no_work():
    labels = {"cadence": "fast", "step": "t_skip", "result": "skipped"}
    before = _val("eve_killmap_refresh_step_runs_total", labels)
    assert mv_refresh._run_step(RefreshStep("t_skip", lambda: False, ["x"]), "fast") == "skipped"
    assert _val("eve_killmap_refresh_step_runs_total", labels) == before + 1


def test_run_step_failure_is_contained_and_counted():
    labels = {"cadence": "slow", "step": "t_bad", "result": "failed"}
    before = _val("eve_killmap_refresh_step_runs_total", labels)
    errors_before = _val("eve_killmap_errors_total", {"component": "t_bad"})
    assert mv_refresh._run_step(RefreshStep("t_bad", _boom, ["x"]), "slow") == "failed"
    assert _val("eve_killmap_refresh_step_runs_total", labels) == before + 1
    assert _val("eve_killmap_errors_total", {"component": "t_bad"}) == errors_before + 1


# _run_cycle ------------------------------------------------------------------


def test_run_cycle_publishes_only_for_successful_steps(monkeypatch):
    published = []

    async def fake_publish(client, targets):
        published.append(targets)

    monkeypatch.setattr(mv_refresh, "publish_invalidation", fake_publish)
    steps = [
        RefreshStep("a", lambda: True, ["t_a"]),
        RefreshStep("b", lambda: False, ["t_b"]),  # skipped -> no publish
        RefreshStep("c", _boom, ["t_c"]),  # failed -> no publish, cycle failed
        RefreshStep("d", lambda: True, []),  # nothing to publish
        RefreshStep("e", lambda: True, ["t_e"]),  # still runs after the failure
    ]
    failed = asyncio.run(mv_refresh._run_cycle(steps, "fast", redis=object()))
    assert failed is True
    assert published == [["t_a"], ["t_e"]]


def test_run_cycle_without_redis_publishes_nothing(monkeypatch):
    async def fail_publish(client, targets):
        raise AssertionError("must not publish without redis")

    monkeypatch.setattr(mv_refresh, "publish_invalidation", fail_publish)
    failed = asyncio.run(
        mv_refresh._run_cycle([RefreshStep("a", lambda: True, ["t"])], "fast", redis=None)
    )
    assert failed is False


def test_run_cycle_records_cycle_level_metrics(monkeypatch):
    ok = {"cadence": "fast", "result": "success"}
    bad = {"cadence": "fast", "result": "failed"}
    ok_before, bad_before = _val("eve_killmap_mv_refresh_runs_total", ok), _val("eve_killmap_mv_refresh_runs_total", bad)

    asyncio.run(mv_refresh._run_cycle([RefreshStep("a", lambda: True, [])], "fast", redis=None))
    assert _val("eve_killmap_mv_refresh_runs_total", ok) == ok_before + 1
    assert _val("eve_killmap_mv_refresh_last_success_timestamp_seconds", {"cadence": "fast"}) > 0

    asyncio.run(mv_refresh._run_cycle([RefreshStep("a", _boom, [])], "fast", redis=None))
    assert _val("eve_killmap_mv_refresh_runs_total", bad) == bad_before + 1


# step lists ------------------------------------------------------------------


def test_fast_steps_order_and_targets(monkeypatch):
    _pin_leaderboard(monkeypatch, True)
    steps = mv_refresh._fast_steps()
    assert [s.name for s in steps] == ["entity_rollup", "leaderboards", "mv_refresh"]
    assert steps[0].invalidation == []
    assert steps[1].invalidation == ["leaderboards"]
    assert steps[2].invalidation == mv_refresh._FAST_INVALIDATION


def test_slow_steps_order_and_targets(monkeypatch):
    _pin_leaderboard(monkeypatch, True)
    steps = mv_refresh._slow_steps()
    assert [s.name for s in steps] == ["leaderboards", "mv_refresh"]
    assert steps[0].invalidation == ["leaderboards"]
    assert steps[1].invalidation == mv_refresh._SLOW_INVALIDATION == ["farthest_kill"]


def test_leaderboard_steps_absent_when_disabled(monkeypatch):
    _pin_leaderboard(monkeypatch, False)
    assert [s.name for s in mv_refresh._fast_steps()] == ["mv_refresh"]
    assert [s.name for s in mv_refresh._slow_steps()] == ["mv_refresh"]


def test_step_bodies_use_their_own_configured_connection(monkeypatch):
    # Each step opens a fresh connection, configures the session (work_mem,
    # TimeZone=UTC, committed), and hands the conn to the leaderboard function.
    import contextlib

    class _Cur:
        def __init__(self):
            self.sql = []

        def execute(self, sql, params=None):
            self.sql.append((sql, params))

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Conn:
        def __init__(self):
            self.cur = _Cur()
            self.commits = 0
            self.autocommit = False

        def cursor(self):
            return self.cur

        def commit(self):
            self.commits += 1

    conns = []

    @contextlib.contextmanager
    def fake_get_connection():
        c = _Conn()
        conns.append(c)
        yield c

    monkeypatch.setattr(mv_refresh, "get_connection", fake_get_connection)
    seen = {}
    monkeypatch.setattr(mv_refresh.leaderboard, "roll_dirty_days", lambda conn: seen.setdefault("roll", conn) and True)
    monkeypatch.setattr(mv_refresh.leaderboard, "compute_boards", lambda conn, w: seen.setdefault("boards", (conn, w)) and True)
    _pin_leaderboard(monkeypatch, True)

    fast = mv_refresh._fast_steps()
    assert fast[0].run() is True and fast[1].run() is True

    assert seen["roll"] is conns[0] and seen["boards"][0] is conns[1]
    assert seen["boards"][1] == mv_refresh.leaderboard.FAST_WINDOWS
    for c in conns:
        sqls = [s for s, _ in c.cur.sql]
        assert any("set_config('work_mem'" in s for s in sqls)
        assert any("set_config('TimeZone', 'UTC', false)" in s for s in sqls)
        assert c.commits >= 1  # session settings survive a later rollback
