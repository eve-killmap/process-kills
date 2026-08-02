"""Kill facets: an inverted-index row per (kill, distinct filterable attribute).

`collect_facets` is the REFERENCE definition of which facet rows a kill produces;
the one-time SQL backfill mirrors it exactly. Pure — no
I/O — so it is unit-tested directly.

Design: docs/superpowers/specs/2026-08-01-kill-faceted-filtering-design.md
"""

from __future__ import annotations

from schema import ParsedKill

# facet_kind
CHARACTER = 1
CORPORATION = 2
ALLIANCE = 3
FACTION = 4
SHIP = 5
WEAPON = 6
WAR = 7

# role
VICTIM = 0
ATTACKER = 1
KILL = 2

FACET_KIND_NAMES: dict[int, str] = {
    CHARACTER: "character",
    CORPORATION: "corporation",
    ALLIANCE: "alliance",
    FACTION: "faction",
    SHIP: "ship",
    WEAPON: "weapon",
    WAR: "war",
}


def collect_facets(parsed: ParsedKill) -> list[tuple[int, int, int]]:
    """Deduplicated, sorted (facet_kind, facet_value, role) rows for a parsed kill."""
    facets: set[tuple[int, int, int]] = set()

    # Victim (role VICTIM). Ship is always present; the rest may be None.
    facets.add((SHIP, parsed["victim_ship_type_id"], VICTIM))
    if parsed["victim_character_id"] is not None:
        facets.add((CHARACTER, parsed["victim_character_id"], VICTIM))
    if parsed["victim_corporation_id"] is not None:
        facets.add((CORPORATION, parsed["victim_corporation_id"], VICTIM))
    if parsed["victim_alliance_id"] is not None:
        facets.add((ALLIANCE, parsed["victim_alliance_id"], VICTIM))
    if parsed["victim_faction_id"] is not None:
        facets.add((FACTION, parsed["victim_faction_id"], VICTIM))

    # War (kill-level).
    if parsed["war_id"] is not None:
        facets.add((WAR, parsed["war_id"], KILL))

    # Attackers (role ATTACKER); the set naturally dedups across attackers.
    for atk in parsed["attackers"]:
        if atk["character_id"] is not None:
            facets.add((CHARACTER, atk["character_id"], ATTACKER))
        if atk["corporation_id"] is not None:
            facets.add((CORPORATION, atk["corporation_id"], ATTACKER))
        if atk["alliance_id"] is not None:
            facets.add((ALLIANCE, atk["alliance_id"], ATTACKER))
        if atk["faction_id"] is not None:
            facets.add((FACTION, atk["faction_id"], ATTACKER))
        if atk["ship_type_id"] is not None:
            facets.add((SHIP, atk["ship_type_id"], ATTACKER))
        if atk["weapon_type_id"] is not None:
            facets.add((WEAPON, atk["weapon_type_id"], ATTACKER))

    return sorted(facets)


def facet_kind_counts(rows: list[tuple[int, int, int]]) -> dict[str, int]:
    """Count rows per kind name — for the facets_written metric."""
    counts: dict[str, int] = {}
    for kind, _value, _role in rows:
        name = FACET_KIND_NAMES[kind]
        counts[name] = counts.get(name, 0) + 1
    return counts
