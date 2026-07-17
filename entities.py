"""Entity name resolution at ingestion time.

IDs seen on a kill (victim + attackers) are resolved to names/tickers via ESI
and upserted into the reference tables. Factions are excluded here -- they are
fully prepopulated by the faction scheduler, so nothing resolves them per kill.

Design: docs/superpowers/specs/2026-07-17-entity-enrichment-precompute-design.md
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from schema import ParsedKill

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntityIds:
    characters: frozenset[int]
    corporations: frozenset[int]
    alliances: frozenset[int]

    @property
    def is_empty(self) -> bool:
        return not (self.characters or self.corporations or self.alliances)


def collect_entity_ids(parsed: ParsedKill) -> EntityIds:
    """Character/corp/alliance ids referenced by a parsed kill (victim + attackers).

    Factions are intentionally excluded (prepopulated elsewhere). None ids drop.
    """
    characters: set[int] = set()
    corporations: set[int] = set()
    alliances: set[int] = set()

    if parsed["victim_character_id"] is not None:
        characters.add(parsed["victim_character_id"])
    if parsed["victim_corporation_id"] is not None:
        corporations.add(parsed["victim_corporation_id"])
    if parsed["victim_alliance_id"] is not None:
        alliances.add(parsed["victim_alliance_id"])

    for atk in parsed["attackers"]:
        if atk["character_id"] is not None:
            characters.add(atk["character_id"])
        if atk["corporation_id"] is not None:
            corporations.add(atk["corporation_id"])
        if atk["alliance_id"] is not None:
            alliances.add(atk["alliance_id"])

    return EntityIds(
        frozenset(characters), frozenset(corporations), frozenset(alliances)
    )


def character_rows(
    requested: set[int], names: dict[int, str]
) -> list[tuple[int, str | None]]:
    """One row per requested id; None name = tombstone (ESI had no answer)."""
    return [(cid, names.get(cid)) for cid in requested]


def group_rows(
    requested: set[int], info: dict[int, tuple[str, str] | None]
) -> list[tuple[int, str | None, str | None]]:
    """One (id, name, ticker) row per requested id; (None, None) if unresolved."""
    rows: list[tuple[int, str | None, str | None]] = []
    for gid in requested:
        entry = info.get(gid)
        if entry is None:
            rows.append((gid, None, None))
        else:
            rows.append((gid, entry[0], entry[1]))
    return rows
