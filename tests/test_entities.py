from entities import EntityIds, collect_entity_ids


def _kill(**overrides):
    kill = {
        "killmail_id": 1,
        "killmail_hash": "h",
        "killmail_time": "2024-01-01T00:00:00Z",
        "solar_system_id": 30000142,
        "position_x": 0.0,
        "position_y": 0.0,
        "position_z": 0.0,
        "victim_character_id": 100,
        "victim_corporation_id": 200,
        "victim_alliance_id": 300,
        "victim_faction_id": 500,
        "victim_damage_taken": 1,
        "victim_ship_type_id": 587,
        "war_id": None,
        "attackers": [],
    }
    kill.update(overrides)
    return kill


def test_collects_victim_and_attacker_ids_excluding_factions():
    kill = _kill(
        attackers=[
            {
                "character_id": 101,
                "corporation_id": 201,
                "alliance_id": None,
                "faction_id": 500,
                "ship_type_id": 587,
                "weapon_type_id": 3074,
                "damage_done": 10,
                "final_blow": True,
                "security_status": 0.0,
            }
        ]
    )
    ids = collect_entity_ids(kill)
    assert ids.characters == frozenset({100, 101})
    assert ids.corporations == frozenset({200, 201})
    assert ids.alliances == frozenset({300})  # attacker alliance was None


def test_npc_victim_has_no_character_id():
    kill = _kill(victim_character_id=None, victim_corporation_id=None, victim_alliance_id=None)
    ids = collect_entity_ids(kill)
    assert ids.characters == frozenset()
    assert ids.corporations == frozenset()
    assert ids.alliances == frozenset()


def test_is_empty():
    assert EntityIds(frozenset(), frozenset(), frozenset()).is_empty
    assert not EntityIds(frozenset({1}), frozenset(), frozenset()).is_empty


from entities import character_rows, group_rows


def test_character_rows_tombstones_missing():
    rows = dict(character_rows({1, 2, 3}, {1: "Alice", 2: "Bob"}))
    assert rows == {1: "Alice", 2: "Bob", 3: None}


def test_group_rows_tombstones_missing_and_none():
    info = {10: ("CorpA", "AAA"), 11: None}
    rows = {r[0]: (r[1], r[2]) for r in group_rows({10, 11, 12}, info)}
    assert rows[10] == ("CorpA", "AAA")
    assert rows[11] == (None, None)
    assert rows[12] == (None, None)
