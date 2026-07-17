from entities_backfill import next_cursor


def test_next_cursor_is_max():
    assert next_cursor([5, 2, 9, 1]) == 9


def test_next_cursor_empty_returns_zero():
    assert next_cursor([]) == 0
