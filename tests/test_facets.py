# tests/test_facets.py
import facets
from facets import collect_facets, facet_kind_counts


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
        "victim_faction_id": None,
        "victim_damage_taken": 1,
        "victim_ship_type_id": 587,
        "war_id": None,
        "attackers": [],
    }
    kill.update(overrides)
    return kill


def _atk(**overrides):
    atk = {
        "character_id": None,
        "corporation_id": None,
        "alliance_id": None,
        "faction_id": None,
        "ship_type_id": None,
        "weapon_type_id": None,
        "damage_done": 1,
        "final_blow": True,
        "security_status": 0.0,
    }
    atk.update(overrides)
    return atk


def test_victim_facets_and_ship_always_present():
    got = set(collect_facets(_kill()))
    assert (facets.CHARACTER, 100, facets.VICTIM) in got
    assert (facets.CORPORATION, 200, facets.VICTIM) in got
    assert (facets.ALLIANCE, 300, facets.VICTIM) in got
    assert (facets.SHIP, 587, facets.VICTIM) in got  # victim ship always present
    assert not any(
        k == facets.FACTION for k, _v, _r in got
    )  # victim_faction_id was None


def test_war_facet_when_present():
    assert (facets.WAR, 42, facets.KILL) in set(collect_facets(_kill(war_id=42)))


def test_attacker_facets_deduped_across_attackers():
    kill = _kill(
        attackers=[
            _atk(
                character_id=101,
                corporation_id=200,
                ship_type_id=17738,
                weapon_type_id=3074,
            ),
            _atk(
                character_id=102,
                corporation_id=200,
                ship_type_id=17738,
                weapon_type_id=2929,
            ),
        ]
    )
    rows = collect_facets(kill)
    got = set(rows)
    # corp 200 attacker appears once despite two attackers; ship 17738 once
    assert rows.count((facets.CORPORATION, 200, facets.ATTACKER)) == 1
    assert rows.count((facets.SHIP, 17738, facets.ATTACKER)) == 1
    # both distinct characters and both distinct weapons are present
    assert (facets.CHARACTER, 101, facets.ATTACKER) in got
    assert (facets.CHARACTER, 102, facets.ATTACKER) in got
    assert (facets.WEAPON, 3074, facets.ATTACKER) in got
    assert (facets.WEAPON, 2929, facets.ATTACKER) in got


def test_same_id_both_roles_kept_separately():
    # corp 200 is the victim's corp AND an attacker's corp -> two rows, different roles
    kill = _kill(attackers=[_atk(corporation_id=200)])
    got = set(collect_facets(kill))
    assert (facets.CORPORATION, 200, facets.VICTIM) in got
    assert (facets.CORPORATION, 200, facets.ATTACKER) in got


def test_output_is_sorted_and_deduped():
    rows = collect_facets(
        _kill(attackers=[_atk(character_id=101), _atk(character_id=101)])
    )
    assert rows == sorted(set(rows))  # deterministic, no duplicates


def test_facet_kind_counts_by_name():
    rows = [
        (facets.CHARACTER, 1, facets.VICTIM),
        (facets.CHARACTER, 2, facets.ATTACKER),
        (facets.SHIP, 587, facets.VICTIM),
    ]
    assert facet_kind_counts(rows) == {"character": 2, "ship": 1}
