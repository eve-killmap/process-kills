from datetime import datetime, timedelta, timezone

from corporations import (
    ACTIVE_REFRESH_INTERVAL,
    compute_corp_refresh_after,
    parse_corporation,
)


def test_parse_corporation_maps_fields():
    data = {
        "name": "Test Corp",
        "ticker": "TEST",
        "alliance_id": 99000001,
        "date_founded": "2010-01-01T00:00:00Z",
        "member_count": 42,
        "state": "active",
    }
    assert parse_corporation(data) == {
        "name": "Test Corp",
        "ticker": "TEST",
        "alliance_id": 99000001,
        "date_founded": "2010-01-01T00:00:00Z",
        "member_count": 42,
        "active": True,
    }


def test_parse_corporation_inactive_state_is_not_active():
    assert parse_corporation({"state": "closed"})["active"] is False


def test_parse_corporation_missing_fields_default_to_none_and_inactive():
    row = parse_corporation({})
    assert row["name"] is None
    assert row["alliance_id"] is None
    assert row["member_count"] is None
    assert row["active"] is False  # absent state -> not active


def test_compute_refresh_after_active_adds_interval():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    assert compute_corp_refresh_after(True, now, timedelta(0)) == now + ACTIVE_REFRESH_INTERVAL
    assert ACTIVE_REFRESH_INTERVAL == timedelta(hours=24)


def test_compute_refresh_after_active_includes_jitter():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    jitter = timedelta(minutes=30)
    assert compute_corp_refresh_after(True, now, jitter) == now + ACTIVE_REFRESH_INTERVAL + jitter


def test_compute_refresh_after_closed_is_terminal():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    assert compute_corp_refresh_after(False, now, timedelta(0)) is None
