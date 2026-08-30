from zkb import parse_zkb


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
