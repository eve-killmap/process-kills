# tests/test_esi_war.py
import asyncio

from esi import ESIClient, Priority


def test_priority_war_is_lowest():
    assert Priority.WAR > Priority.CROSSCHECK
    assert Priority.WAR > Priority.RECHECK


class _FakeResp:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload
        self.headers = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self):
        return self._payload

    async def text(self):
        return ""


class _FakeSession:
    def __init__(self, resp):
        self._resp = resp
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        return self._resp


def test_fetch_war_returns_json_via_queue():
    async def run():
        client = ESIClient(asyncio.Event())
        client._session = _FakeSession(_FakeResp(200, {"id": 42, "mutual": False}))
        client._worker_task = asyncio.create_task(client._queue_worker())
        try:
            return await client.fetch_war(42)
        finally:
            client._shutdown.set()
            client._worker_task.cancel()
            try:
                await client._worker_task
            except asyncio.CancelledError:
                pass

    result = asyncio.run(run())
    assert result == {"id": 42, "mutual": False}
