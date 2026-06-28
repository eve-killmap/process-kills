from esi import parse_kill


def _killmail(**overrides):
    data = {
        "killmail_id": 12345,
        "killmail_hash": "abc123",
        "killmail_time": "2024-01-02T03:04:05Z",
        "solar_system_id": 30000142,
        "victim": {
            "character_id": 100,
            "corporation_id": 200,
            "alliance_id": 300,
            "faction_id": 400,
            "damage_taken": 5000,
            "ship_type_id": 670,
            "position": {"x": 1.5, "y": -2.5, "z": 3.0},
        },
        "attackers": [
            {
                "character_id": 900,
                "corporation_id": 901,
                "damage_done": 5000,
                "final_blow": True,
                "security_status": -1.5,
                "ship_type_id": 11567,
            }
        ],
        "war_id": 77,
    }
    data.update(overrides)
    return data


def test_parse_kill_with_position_returns_record():
    parsed = parse_kill(_killmail())
    assert parsed is not None
    assert parsed["killmail_id"] == 12345
    assert parsed["killmail_hash"] == "abc123"
    assert parsed["solar_system_id"] == 30000142
    assert parsed["position_x"] == 1.5
    assert parsed["position_y"] == -2.5
    assert parsed["position_z"] == 3.0
    assert parsed["victim_character_id"] == 100
    assert parsed["victim_damage_taken"] == 5000
    assert parsed["victim_ship_type_id"] == 670
    assert parsed["war_id"] == 77
    assert len(parsed["attackers"]) == 1


def test_parse_kill_without_position_returns_none():
    km = _killmail()
    del km["victim"]["position"]
    assert parse_kill(km) is None


def test_parse_kill_empty_position_returns_none():
    assert (
        parse_kill(
            _killmail(victim={"ship_type_id": 1, "damage_taken": 0, "position": {}})
        )
        is None
    )


def test_parse_kill_fills_attacker_defaults():
    km = _killmail(
        attackers=[{"damage_done": 10, "final_blow": False, "security_status": 0.0}]
    )
    parsed = parse_kill(km)
    assert parsed is not None
    attacker = parsed["attackers"][0]
    assert attacker["character_id"] is None
    assert attacker["corporation_id"] is None
    assert attacker["ship_type_id"] is None
    assert attacker["weapon_type_id"] is None
    assert attacker["damage_done"] == 10
    assert attacker["final_blow"] is False


def test_parse_kill_missing_hash_defaults_to_empty():
    km = _killmail()
    del km["killmail_hash"]
    parsed = parse_kill(km)
    assert parsed is not None
    assert parsed["killmail_hash"] == ""


def test_parse_kill_optional_victim_ids_default_none():
    km = _killmail(
        victim={
            "damage_taken": 1,
            "ship_type_id": 2,
            "position": {"x": 0.0, "y": 0.0, "z": 1.0},
        }
    )
    parsed = parse_kill(km)
    assert parsed is not None
    assert parsed["victim_character_id"] is None
    assert parsed["victim_corporation_id"] is None
    assert parsed["victim_alliance_id"] is None
    assert parsed["victim_faction_id"] is None
