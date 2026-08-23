from __future__ import annotations

from collections.abc import Mapping
from typing import Any


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
