import asyncio

from prometheus_client import REGISTRY

import heartbeat
import metrics


def _val(name, labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


class _FakeResp:
    def __init__(self, status=200):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    def __init__(self, status=200):
        self.status = status
        self.urls = []

    def get(self, url, timeout=None):
        self.urls.append(url)
        return _FakeResp(self.status)


def test_live_work_total_counts_processing_outcomes():
    before = heartbeat._live_work_total()
    metrics.kills_processed.labels("live", "inserted").inc()
    metrics.kills_processed.labels("live", "no_position").inc()
    metrics.kills_processed.labels("live", "duplicate").inc()
    assert heartbeat._live_work_total() == before + 3


def test_live_work_total_excludes_skipped_and_nonlive():
    before = heartbeat._live_work_total()
    metrics.kills_processed.labels("live", "skipped").inc()  # degenerate, not work
    metrics.kills_processed.labels("backfill", "inserted").inc()  # not the live path
    assert heartbeat._live_work_total() == before


def test_build_push_url_appends_standard_params():
    url = heartbeat._build_push_url("https://k/api/push/tok", "up", "5 kills/60s")
    assert url.startswith("https://k/api/push/tok?")
    assert url.count("?") == 1
    assert "status=up" in url
    assert "ping=" in url


def test_build_push_url_strips_existing_query():
    # A full URL pasted from Kuma (with example params) must not double up "?".
    url = heartbeat._build_push_url(
        "https://k/api/push/tok?status=up&msg=OK&ping=", "up", "x"
    )
    assert url.count("?") == 1
    assert url.startswith("https://k/api/push/tok?")


def test_tick_pushes_only_when_work_advanced():
    session = _FakeSession()
    # No new work since last_seen -> no push.
    current = heartbeat._live_work_total()
    new = asyncio.run(heartbeat._tick(session, "https://k/api/push/tok", current, 60))
    assert new == current
    assert session.urls == []
    # New work -> exactly one push, baseline advances by the delta.
    metrics.kills_processed.labels("live", "inserted").inc()
    newer = asyncio.run(heartbeat._tick(session, "https://k/api/push/tok", new, 60))
    assert newer == new + 1
    assert len(session.urls) == 1
    assert session.urls[0].startswith("https://k/api/push/tok?status=up")


def test_send_records_success_and_failure():
    before_ok = _val("eve_killmap_heartbeat_pushes_total", {"result": "success"})
    asyncio.run(heartbeat._send(_FakeSession(status=200), "https://k/api/push/tok?x"))
    assert _val("eve_killmap_heartbeat_pushes_total", {"result": "success"}) == before_ok + 1

    before_bad = _val("eve_killmap_heartbeat_pushes_total", {"result": "failed"})
    asyncio.run(heartbeat._send(_FakeSession(status=500), "https://k/api/push/tok?x"))
    assert _val("eve_killmap_heartbeat_pushes_total", {"result": "failed"}) == before_bad + 1
