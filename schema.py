from __future__ import annotations

from datetime import datetime
from typing import NotRequired, TypedDict

ZkbTotals = dict[str, int]
ZkbDay = dict[str, str]


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
    # Injected by this service, not part of the ESI body
    killmail_hash: NotRequired[str]


class EphemeralResponse(TypedDict):
    killmail_id: NotRequired[int]
    hash: NotRequired[str]
    esi: NotRequired[EsiKillmail]


class SequenceResponse(TypedDict):
    sequence: NotRequired[int]


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


class ProcessedDate(TypedDict):
    date: str
    total_kills: int
    processed_kills: int
    no_position_kills: int
    last_updated: datetime | None
    error_message: str | None
