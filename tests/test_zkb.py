from zkb import extract_zkb, parse_zkb


def test_parse_zkb_maps_fields():
    z = {
        "locationID": 50002339,
        "hash": "abc",
        "points": 1,
        "fittedValue": 27827719.6,
        "droppedValue": 6038590.73,
        "destroyedValue": 24534515.83,
        "totalValue": 30573106.56,
        "totalDroppableValue": 6461356.55,
        "npc": False,
        "solo": False,
        "awox": False,
        "labels": ["tz:eu", "cat:6", "pvp", "loc:nullsec"],
    }
    assert parse_zkb(z) == {
        "fitted_value": 27827719.6,
        "dropped_value": 6038590.73,
        "destroyed_value": 24534515.83,
        "total_value": 30573106.56,
        "total_droppable_value": 6461356.55,
        "npc": False,
        "solo": False,
        "awox": False,
        "labels": ["tz:eu", "cat:6", "pvp", "loc:nullsec"],
    }


def test_parse_zkb_missing_fields_default():
    row = parse_zkb({})
    assert row["total_value"] is None
    assert row["npc"] is None
    assert row["labels"] == []  # labels default to empty list, never None


def test_parse_zkb_null_labels_becomes_empty_list():
    assert parse_zkb({"labels": None})["labels"] == []


def test_extract_zkb_from_killid_response():
    # zKillboard killID returns a one-element list: the kill plus its zkb envelope.
    resp = [{"killmail_id": 1, "solar_system_id": 30000142, "zkb": {"totalValue": 5.0}}]
    assert extract_zkb(resp) == {"totalValue": 5.0}


def test_extract_zkb_returns_none_when_absent_or_malformed():
    assert extract_zkb([]) is None  # empty list (unknown kill)
    assert extract_zkb(None) is None  # fetch failed
    assert extract_zkb({"zkb": {}}) is None  # not a list
    assert extract_zkb([{"killmail_id": 1}]) is None  # no zkb key
    assert extract_zkb([{"zkb": "nope"}]) is None  # zkb not an object
