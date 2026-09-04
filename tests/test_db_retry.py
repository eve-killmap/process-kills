import dataclasses

import psycopg2
import pytest

import db as db_mod
from config import config as real_config


def _patch_dsn(monkeypatch):
    monkeypatch.setattr(db_mod, "require_database_url", lambda _c: "postgres://test")


def test_connect_with_retry_rides_out_transient(monkeypatch):
    # A package-day Postgres restart surfaces as OperationalError; the startup
    # connect must back off and succeed once the DB is back, not crash.
    _patch_dsn(monkeypatch)
    calls = {"n": 0}
    err = psycopg2.OperationalError("the database system is shutting down")

    def fake_connect(_dsn):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise err
        return "CONN"

    sleeps: list[float] = []
    monkeypatch.setattr(db_mod.psycopg2, "connect", fake_connect)
    monkeypatch.setattr(db_mod.time, "sleep", lambda d: sleeps.append(d))

    conn = db_mod._connect_with_retry()
    assert conn == "CONN"
    assert calls["n"] == 3  # 2 transient failures, then success
    assert sleeps == [1.0, 2.0]  # exponential backoff between attempts


def test_connect_with_retry_gives_up_after_budget(monkeypatch):
    _patch_dsn(monkeypatch)
    patched = dataclasses.replace(
        real_config,
        database=dataclasses.replace(
            real_config.database, connect_max_retry_seconds=0
        ),
    )
    monkeypatch.setattr(db_mod, "config", patched)
    calls = {"n": 0}

    def always_fail(_dsn):
        calls["n"] += 1
        raise psycopg2.OperationalError("shutting down")

    monkeypatch.setattr(db_mod.psycopg2, "connect", always_fail)
    monkeypatch.setattr(db_mod.time, "sleep", lambda _d: None)

    with pytest.raises(psycopg2.OperationalError):
        db_mod._connect_with_retry()
    assert calls["n"] == 1  # budget 0 -> one attempt, no retry


def test_get_connection_with_retry_yields_and_closes(monkeypatch):
    _patch_dsn(monkeypatch)

    class FakeConn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    fake = FakeConn()
    monkeypatch.setattr(db_mod.psycopg2, "connect", lambda _dsn: fake)

    with db_mod.get_connection_with_retry() as conn:
        assert conn is fake
    assert fake.closed  # connection is closed on exit, like get_connection()
