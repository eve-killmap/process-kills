import re
from datetime import datetime, timezone

from crosscheck import _fix_date
from live import _killmail_time_to_date
from maintenance import _next_maintenance_time, _next_mv_refresh_time

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def test_fix_date_reformats_compact_date():
    assert _fix_date("20240102") == "2024-01-02"


def test_killmail_time_to_date_parses_iso_with_z():
    assert _killmail_time_to_date("2024-01-02T03:04:05Z") == "2024-01-02"


def test_killmail_time_to_date_falls_back_to_today_on_bad_input():
    result = _killmail_time_to_date("not-a-timestamp")
    assert DATE_RE.match(result)
    assert result == datetime.now(timezone.utc).strftime("%Y-%m-%d")


def test_next_maintenance_time_picks_next_configured_weekday():
    # Default config: day=6 (Sunday), hour=4. 2024-01-03 is a Wednesday.
    now = datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc)
    nxt = _next_maintenance_time(now)
    assert nxt == datetime(2024, 1, 7, 4, 0, tzinfo=timezone.utc)
    assert nxt.weekday() == 6


def test_next_maintenance_time_same_day_before_hour():
    # Sunday 02:00 -> later the same Sunday at 04:00.
    now = datetime(2024, 1, 7, 2, 0, tzinfo=timezone.utc)
    assert _next_maintenance_time(now) == datetime(
        2024, 1, 7, 4, 0, tzinfo=timezone.utc
    )


def test_next_maintenance_time_same_day_after_hour_rolls_a_week():
    now = datetime(2024, 1, 7, 5, 0, tzinfo=timezone.utc)
    assert _next_maintenance_time(now) == datetime(
        2024, 1, 14, 4, 0, tzinfo=timezone.utc
    )


def test_next_mv_refresh_time_picks_next_slot_today():
    # Default mv_refresh_hours: [0, 6, 12, 18]; 07:00 -> 12:00 same day.
    now = datetime(2024, 1, 3, 7, 0, tzinfo=timezone.utc)
    assert _next_mv_refresh_time(now) == datetime(
        2024, 1, 3, 12, 0, tzinfo=timezone.utc
    )


def test_next_mv_refresh_time_rolls_to_next_day():
    now = datetime(2024, 1, 3, 19, 0, tzinfo=timezone.utc)
    assert _next_mv_refresh_time(now) == datetime(2024, 1, 4, 0, 0, tzinfo=timezone.utc)
