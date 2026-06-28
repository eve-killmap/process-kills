"""TypedDict shapes for external JSON and the pipeline's internal kill records.

The ESI / R2Z2 / zKillboard types describe the fields this service actually
reads, not every field those APIs publish; ``NotRequired`` marks fields that may
be absent (NPC/structure kills have no position, etc.). ``ParsedKill`` /
``ParsedAttacker`` are the internal contract between :func:`esi.parse_kill` and
the ``db`` insert helpers. All of these are static-only annotations: rows stay
plain dicts at runtime, so there is no parsing or memory cost.
"""

from __future__ import annotations

from datetime import datetime
from typing import NotRequired, TypedDict

# zKillboard history endpoints return flat JSON objects:
#   totals.json -> {"YYYYMMDD": expected_kill_count}
#   {date}.json -> {"killmail_id": "killmail_hash"}
ZkbTotals = dict[str, int]
ZkbDay = dict[str, str]


# --- ESI killmail data ------------------------------------------------------


class EsiPosition(TypedDict):
    x: float
    y: float
    z: float


class EsiVictim(TypedDict):
    damage_taken: int
    ship_type_id: int
    character_id: NotRequired[int]
    corporation_id: NotRequired[int]
    alliance_id: NotRequired[int]
    faction_id: NotRequired[int]
    position: NotRequired[EsiPosition]


class EsiAttacker(TypedDict):
    damage_done: int
    final_blow: bool
    security_status: float
    character_id: NotRequired[int]
    corporation_id: NotRequired[int]
    alliance_id: NotRequired[int]
    faction_id: NotRequired[int]
    ship_type_id: NotRequired[int]
    weapon_type_id: NotRequired[int]


class EsiKillmail(TypedDict):
    killmail_id: int
    killmail_time: str
    solar_system_id: int
    victim: EsiVictim
    attackers: list[EsiAttacker]
    war_id: NotRequired[int]
    # Injected by this service from the source feed; not part of the ESI body.
    killmail_hash: NotRequired[str]


# --- R2Z2 live ephemeral feed -----------------------------------------------


class EphemeralResponse(TypedDict):
    killmail_id: NotRequired[int]
    hash: NotRequired[str]
    esi: NotRequired[EsiKillmail]


class SequenceResponse(TypedDict):
    sequence: NotRequired[int]


# --- internal records -------------------------------------------------------


class ParsedAttacker(TypedDict):
    character_id: int | None
    corporation_id: int | None
    alliance_id: int | None
    faction_id: int | None
    ship_type_id: int | None
    weapon_type_id: int | None
    damage_done: int
    final_blow: bool
    security_status: float


class ParsedKill(TypedDict):
    killmail_id: int
    killmail_hash: str
    killmail_time: str
    solar_system_id: int
    position_x: float
    position_y: float
    position_z: float
    victim_character_id: int | None
    victim_corporation_id: int | None
    victim_alliance_id: int | None
    victim_faction_id: int | None
    victim_damage_taken: int
    victim_ship_type_id: int
    war_id: int | None
    attackers: list[ParsedAttacker]


class RecheckCandidate(TypedDict):
    killmail_id: int
    killmail_hash: str
    killmail_time: datetime
    last_checked: datetime


class ProcessedDate(TypedDict):
    date: str
    total_kills: int
    processed_kills: int
    no_position_kills: int
    last_updated: datetime | None
    error_message: str | None
