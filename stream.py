"""Redis Streams publisher for live killmail events.

Publishes freshly inserted kills to the ``kills:live`` stream so the FastAPI
backend can tail it and push events to connected clients, and publishes
cache-invalidation messages after materialized-view refreshes.
"""

import json
import logging
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

import metrics
from config import config

logger = logging.getLogger(__name__)


def killmail_epoch(killmail_time: str) -> float | None:
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


async def publish_invalidation(client: aioredis.Redis, targets: list[str]) -> None:
    try:
        await client.publish(
            config.streaming.invalidate_channel,
            json.dumps({"targets": targets}),
        )
        logger.info("Published cache invalidation: %s", targets)
        for target in targets:
            metrics.cache_invalidations_published.labels(target, "success").inc()
    except Exception as e:
        logger.warning("Failed to publish cache invalidation %s: %s", targets, e)
        for target in targets:
            metrics.cache_invalidations_published.labels(target, "failed").inc()


async def publish_kill(client: aioredis.Redis, parsed: Mapping[str, Any]) -> None:
    if config.streaming.discard_older_than > 0:
        epoch = killmail_epoch(parsed["killmail_time"])
        if epoch is not None:
            age = time.time() - epoch
            if age >= config.streaming.discard_older_than:
                metrics.stream_kills_discarded.inc()
                return

    try:
        await client.xadd(
            config.streaming.stream_name,
            {"data": json.dumps(parsed, default=str)},
            maxlen=config.streaming.stream_max_length,
            approximate=True,
        )
        metrics.stream_publishes.labels("success").inc()
    except Exception as e:
        metrics.stream_publishes.labels("failed").inc()
        logger.warning(f"Failed to publish kill {parsed['killmail_id']} to stream: {e}")
