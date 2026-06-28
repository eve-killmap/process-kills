"""Redis Streams publisher for live killmail events.

Publishes freshly inserted kills to the ``kills:live`` stream so the FastAPI
backend can tail it and push events to connected clients.
"""

import json
import logging
from collections.abc import Mapping
from typing import Any
import time

import redis.asyncio as aioredis

from config import config

logger = logging.getLogger(__name__)


async def connect_redis() -> aioredis.Redis:
    client = aioredis.from_url(config.redis_url, decode_responses=True)
    await client.ping()
    return client


async def publish_kill(client: aioredis.Redis, parsed: Mapping[str, Any]) -> None:
    if config.streaming.discard_older_than > 0:
        current_epoch = int(time.time())
        diff = current_epoch - int(parsed["killmail_time"])
        if diff >= config.streaming.discard_older_than:
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
