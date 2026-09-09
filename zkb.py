from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def extract_zkb(response: Any) -> Mapping[str, Any] | None:
    """Pull the zkb object out of a zKillboard killID response (a one-element list
    holding the killmail plus its zkb envelope). None if absent or malformed."""
    if isinstance(response, list) and response and isinstance(response[0], Mapping):
        candidate = response[0].get("zkb")
        if isinstance(candidate, Mapping):
            return candidate
    return None


def parse_zkb(zkb: Mapping[str, Any]) -> dict:
    """zKB metadata object -> zkb_metadata column dict (the nine stored fields)."""
    return {
        "fitted_value": zkb.get("fittedValue"),
        "dropped_value": zkb.get("droppedValue"),
        "destroyed_value": zkb.get("destroyedValue"),
        "total_value": zkb.get("totalValue"),
        "total_droppable_value": zkb.get("totalDroppableValue"),
        "npc": zkb.get("npc"),
        "solo": zkb.get("solo"),
        "awox": zkb.get("awox"),
        "labels": zkb.get("labels") or [],
    }
