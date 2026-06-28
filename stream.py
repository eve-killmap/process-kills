"""Redis Streams publisher for live killmail events.

Publishes freshly inserted kills to the ``kills:live`` stream so the FastAPI
backend can tail it and push events to connected clients.
"""

import json
import logging
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

from config import config

logger = logging.getLogger(__name__)


def _killmail_epoch(killmail_time: str) -> float | None:
    try:
        dt = datetime.fromisoformat(killmail_time.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


async def connect_redis() -> aioredis.Redis:
    client = aioredis.from_url(config.redis_url, decode_responses=True)
    await client.ping()
    return client


async def publish_kill(client: aioredis.Redis, parsed: Mapping[str, Any]) -> None:
    if config.streaming.discard_older_than > 0:
        killmail_epoch = _killmail_epoch(parsed["killmail_time"])
        if killmail_epoch is not None:
            age = time.time() - killmail_epoch
            if age >= config.streaming.discard_older_than:
                return

    try:
        await client.xadd(
            config.streaming.stream_name,
            {"data": json.dumps(parsed, default=str)},
            maxlen=config.streaming.stream_max_length,
            approximate=True,
        )
    except Exception as e:
        logger.warning(f"Failed to publish kill {parsed['killmail_id']} to stream: {e}")
