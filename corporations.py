"""Corporation metadata refresh (rolling, self-throttling).

One fetch/parse/upsert/reschedule path (refresh_corporations) shared by inline
ingestion (entities.resolve_and_store), the background scheduler, and the local
one-off backfill. A refresh that finds active=false freezes the corp terminal
(refresh_after = NULL): a closed corp's metadata is immutable forever.

Design: docs/superpowers/specs/2026-08-20-corporation-alliance-metadata-design.md
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import db
import metrics
from config import config

logger = logging.getLogger(__name__)

ACTIVE_REFRESH_INTERVAL = timedelta(hours=24)
CORP_RETRY_BACKOFF = timedelta(minutes=10)
CORP_REFRESH_JITTER_SECONDS = 3600  # spread a tick's batch so it doesn't re-clump


def parse_corporation(data: Mapping[str, Any]) -> dict:
    """ESI corp JSON -> corporations column dict. Missing fields -> None.

    `active` is True iff the ESI `state` field equals "active" (VERIFY the field
    name/values against a live fetch before the mass backfill; see the spec).
    """
    return {
        "name": data.get("name"),
        "ticker": data.get("ticker"),
        "alliance_id": data.get("alliance_id"),
        "date_founded": data.get("date_founded"),
        "member_count": data.get("member_count"),
        "active": data.get("state") == "active",
    }


def compute_corp_refresh_after(
    active: bool, now: datetime, jitter: timedelta
) -> datetime | None:
    """Success-path reschedule: active -> now+24h(+jitter); closed -> None (terminal)."""
    if not active:
        return None
    return now + ACTIVE_REFRESH_INTERVAL + jitter
