"""War info resolution (decoupled, rate-limited).

Unlike entity names, wars are not needed for the live broadcast and the ESI war
endpoint IS rate limited, so they are resolved by a background scheduler. A war
stub row (war_id only, refresh_after = now) is the queue entry; this module
fetches, parses, and upserts it, then sets refresh_after (NULL once terminal).

Design: docs/superpowers/specs/2026-07-17-entity-enrichment-precompute-design.md
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


def parse_war(data: Mapping[str, Any]) -> dict[str, Any]:
    """ESI war JSON -> a dict keyed like the `wars` table columns.

    Timestamp fields are left as ESI's ISO strings; psycopg2 casts them to
    TIMESTAMPTZ on insert. Missing fields become None.
    """
    aggressor = data.get("aggressor") or {}
    defender = data.get("defender") or {}
    allies = data.get("allies") or []

    ally_corporation_ids = [
        a["corporation_id"] for a in allies if a.get("corporation_id") is not None
    ]
    ally_alliance_ids = [
        a["alliance_id"] for a in allies if a.get("alliance_id") is not None
    ]

    return {
        "war_id": data["id"],
        "declared": data.get("declared"),
        "started": data.get("started"),
        "finished": data.get("finished"),
        "retracted": data.get("retracted"),
        "mutual": data.get("mutual"),
        "open_for_allies": data.get("open_for_allies"),
        "aggressor_corporation_id": aggressor.get("corporation_id"),
        "aggressor_alliance_id": aggressor.get("alliance_id"),
        "aggressor_ships_killed": aggressor.get("ships_killed"),
        "aggressor_isk_destroyed": aggressor.get("isk_destroyed"),
        "defender_corporation_id": defender.get("corporation_id"),
        "defender_alliance_id": defender.get("alliance_id"),
        "defender_ships_killed": defender.get("ships_killed"),
        "defender_isk_destroyed": defender.get("isk_destroyed"),
        "ally_corporation_ids": ally_corporation_ids,
        "ally_alliance_ids": ally_alliance_ids,
    }


def compute_refresh_after(
    finished: datetime | None, now: datetime, expires: datetime | None
) -> datetime | None:
    """When to re-fetch a war. None means terminal (never again).

    - No `finished`: active -> honour ESI's Expires header.
    - `finished` in the future: ends later -> one more fetch just after it ends.
    - `finished` in the past: over -> None.
    """
    if finished is None:
        return expires
    if finished > now:
        return finished + timedelta(minutes=1)
    return None
