# tests/test_esi_entities.py
import asyncio

from esi import ESIClient


class _FakeResp:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self):
        return self._payload


class _FakeSession:
    """Records POST batches, returns one name row per posted id."""

    def __init__(self):
        self.batches = []

    def post(self, url, json):
        self.batches.append(list(json))
        return _FakeResp(200, [{"id": i, "name": f"name-{i}"} for i in json])


class _FakeGetSession:
    """Returns a preset response for .get(url)."""
    def __init__(self, resp):
        self._resp = resp
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        return self._resp


def _client_with_session(session):
    client = ESIClient(asyncio.Event())
    client._session = session
    return client


def test_resolve_names_empty_is_noop():
    client = _client_with_session(_FakeSession())
    assert asyncio.run(client.resolve_names(set())) == ({}, set())


def test_resolve_names_batches_by_1000():
    session = _FakeSession()
    client = _client_with_session(session)
    ids = set(range(1, 2501))  # 3 batches: 1000, 1000, 500
    resolved, failed = asyncio.run(client.resolve_names(ids))
    assert len(resolved) == 2500
    assert failed == set()
    assert resolved[1] == "name-1"
    assert sorted(len(b) for b in session.batches) == [500, 1000, 1000]


def test_get_corporation_success_returns_name_and_ticker():
    resp = _FakeResp(200, {"name": "Test Corp", "ticker": "TEST"})
    client = _client_with_session(_FakeGetSession(resp))
    assert asyncio.run(client.get_corporation(98000001)) == ("Test Corp", "TEST")


def test_get_corporation_404_returns_none():
    client = _client_with_session(_FakeGetSession(_FakeResp(404, {})))
    assert asyncio.run(client.get_corporation(98000001)) is None


def test_get_factions_non_200_returns_empty_list():
    client = _client_with_session(_FakeGetSession(_FakeResp(500, {})))
    assert asyncio.run(client.get_factions()) == []
