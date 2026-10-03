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


def _pin_slow_time(monkeypatch, hour, minute):
    pinned = replace(
        mv_refresh.config,
        refresh=replace(
            mv_refresh.config.refresh,
            slow_refresh_hour=hour,
            slow_refresh_minute=minute,
        ),
    )
    monkeypatch.setattr(mv_refresh, "config", pinned)


def test_next_slow_refresh_time_later_today_when_still_ahead(monkeypatch):
    _pin_slow_time(monkeypatch, 4, 0)
    now = datetime(2024, 1, 3, 2, 0, tzinfo=timezone.utc)
    assert _next_slow_refresh_time(now) == datetime(
        2024, 1, 3, 4, 0, tzinfo=timezone.utc
    )


def test_next_slow_refresh_time_tomorrow_once_passed(monkeypatch):
    _pin_slow_time(monkeypatch, 4, 0)
    now = datetime(2024, 1, 3, 5, 0, tzinfo=timezone.utc)
    assert _next_slow_refresh_time(now) == datetime(
        2024, 1, 4, 4, 0, tzinfo=timezone.utc
    )
    at = datetime(2024, 1, 3, 4, 0, tzinfo=timezone.utc)
    assert _next_slow_refresh_time(at) == datetime(
        2024, 1, 4, 4, 0, tzinfo=timezone.utc
    )


def test_next_slow_refresh_time_honours_minutes(monkeypatch):
    _pin_slow_time(monkeypatch, 11, 20)
    now = datetime(2024, 1, 3, 11, 5, tzinfo=timezone.utc)
    assert _next_slow_refresh_time(now) == datetime(
        2024, 1, 3, 11, 20, tzinfo=timezone.utc
    )
    now = datetime(2024, 1, 3, 11, 25, tzinfo=timezone.utc)
    assert _next_slow_refresh_time(now) == datetime(
        2024, 1, 4, 11, 20, tzinfo=timezone.utc
    )


def _pin_interval(monkeypatch, minutes):
    pinned = replace(
        mv_refresh.config,
        refresh=replace(mv_refresh.config.refresh, mv_refresh_interval_minutes=minutes),
    )
    monkeypatch.setattr(mv_refresh, "config", pinned)


def test_next_fast_refresh_time_picks_next_interval_boundary(monkeypatch):
    _pin_interval(monkeypatch, 30)
    now = datetime(2024, 1, 3, 7, 12, tzinfo=timezone.utc)
    assert _next_fast_refresh_time(now) == datetime(
        2024, 1, 3, 7, 30, tzinfo=timezone.utc
    )


def test_next_fast_refresh_time_rolls_to_next_day(monkeypatch):
    _pin_interval(monkeypatch, 360)
    now = datetime(2024, 1, 3, 19, 0, tzinfo=timezone.utc)
    assert _next_fast_refresh_time(now) == datetime(
        2024, 1, 4, 0, 0, tzinfo=timezone.utc
    )


def test_mv_alliance_member_count_in_fast_views():
    from mv_refresh import _FAST_VIEWS

    assert "mv_alliance_member_count" in _FAST_VIEWS


def test_fast_views_exclude_the_daily_rollup():
    from mv_refresh import _FAST_VIEWS

    assert "mv_kills_per_system_daily" not in _FAST_VIEWS
    assert _FAST_VIEWS == ["mv_kills_per_system", "mv_alliance_member_count"]


def test_fast_invalidation_targets():
    from mv_refresh import _FAST_INVALIDATION

    assert _FAST_INVALIDATION == ["system_rankings", "system_kills", "global_kills"]
