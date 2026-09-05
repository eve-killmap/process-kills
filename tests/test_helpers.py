import re
from dataclasses import replace
from datetime import datetime, timezone

import mv_refresh
from crosscheck import _fix_date
from live import _killmail_time_to_date
from mv_refresh import _next_slow_refresh_time, _next_fast_refresh_time

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def test_fix_date_reformats_compact_date():
    assert _fix_date("20240102") == "2024-01-02"


def test_killmail_time_to_date_parses_iso_with_z():
    assert _killmail_time_to_date("2024-01-02T03:04:05Z") == "2024-01-02"


def test_killmail_time_to_date_falls_back_to_today_on_bad_input():
    result = _killmail_time_to_date("not-a-timestamp")
    assert DATE_RE.match(result)
    assert result == datetime.now(timezone.utc).strftime("%Y-%m-%d")


def test_next_slow_refresh_time_picks_next_configured_weekday():
    # Default config: day=6 (Sunday), hour=4. 2024-01-03 is a Wednesday.
    now = datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc)
    nxt = _next_slow_refresh_time(now)
    assert nxt == datetime(2024, 1, 7, 4, 0, tzinfo=timezone.utc)
    assert nxt.weekday() == 6


def test_next_slow_refresh_time_same_day_before_hour():
    # Sunday 02:00 -> later the same Sunday at 04:00.
    now = datetime(2024, 1, 7, 2, 0, tzinfo=timezone.utc)
    assert _next_slow_refresh_time(now) == datetime(
        2024, 1, 7, 4, 0, tzinfo=timezone.utc
    )


def test_next_slow_refresh_time_same_day_after_hour_rolls_a_week():
    now = datetime(2024, 1, 7, 5, 0, tzinfo=timezone.utc)
    assert _next_slow_refresh_time(now) == datetime(
        2024, 1, 14, 4, 0, tzinfo=timezone.utc
    )


def _pin_interval(monkeypatch, minutes):
    # Pin the fast interval so these tests don't depend on the live config.yml.
    pinned = replace(
        mv_refresh.config,
        refresh=replace(mv_refresh.config.refresh, mv_refresh_interval_minutes=minutes),
    )
    monkeypatch.setattr(mv_refresh, "config", pinned)


def test_next_fast_refresh_time_picks_next_interval_boundary(monkeypatch):
    # 30-min interval: 07:12 -> next wall-clock half-hour, 07:30.
    _pin_interval(monkeypatch, 30)
    now = datetime(2024, 1, 3, 7, 12, tzinfo=timezone.utc)
    assert _next_fast_refresh_time(now) == datetime(
        2024, 1, 3, 7, 30, tzinfo=timezone.utc
    )


def test_next_fast_refresh_time_rolls_to_next_day(monkeypatch):
    # 360-min interval boundaries are 00/06/12/18; 19:00 -> next day 00:00.
    _pin_interval(monkeypatch, 360)
    now = datetime(2024, 1, 3, 19, 0, tzinfo=timezone.utc)
    assert _next_fast_refresh_time(now) == datetime(
        2024, 1, 4, 0, 0, tzinfo=timezone.utc
    )


def test_mv_alliance_member_count_in_fast_views():
    from mv_refresh import _FAST_VIEWS

    assert "mv_alliance_member_count" in _FAST_VIEWS


def test_kills_per_system_daily_in_fast_views():
    from mv_refresh import _FAST_VIEWS

    assert "mv_kills_per_system_daily" in _FAST_VIEWS
    for mv in (
        "mv_kills_per_system_24h",
        "mv_kills_per_system_7d",
        "mv_kills_per_system_30d",
        "mv_kills_per_system_6m",
        "mv_kills_per_system_1y",
    ):
        assert mv not in _FAST_VIEWS
    assert _FAST_VIEWS == [
        "mv_kills_per_system",
        "mv_kills_per_system_daily",
        "mv_alliance_member_count",
    ]


def test_fast_invalidation_targets():
    from mv_refresh import _FAST_INVALIDATION

    assert _FAST_INVALIDATION == ["system_rankings", "system_kills", "global_kills"]
