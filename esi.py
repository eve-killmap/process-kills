"""Async ESI client with rate limiting and priority queue.

All ESI and zKillboard HTTP requests go through this module to respect the ESI
rate limit (3600 tokens per 15 minutes by default; see ``esi`` in config.yml).
"""

import asyncio
import logging
import time
from collections.abc import Mapping
from enum import IntEnum
from typing import Any

import aiohttp

import metrics
from config import config
from schema import ParsedKill

logger = logging.getLogger(__name__)


class Priority(IntEnum):
    CROSSCHECK = 0
    RECHECK = 1
    WAR = 2  # war backfill must never starve crosscheck


class ESIClient:
    TOKEN_COST_2XX = 2
    TOKEN_COST_3XX = 1
    TOKEN_COST_4XX = 5

    def __init__(self, shutdown_event: asyncio.Event):
        self._shutdown = shutdown_event
        self._session: aiohttp.ClientSession | None = None

        self._bucket_size = float(config.esi.rate_limit)
        self._tokens = self._bucket_size
        self._last_refill = time.monotonic()
        self._refill_rate = self._bucket_size / config.esi.rate_limit_window
        self._rate_limited_until = 0.0
        self._lock = asyncio.Lock()

        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._seq = 0
        self._worker_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            headers={"User-Agent": config.user_agent},
            timeout=aiohttp.ClientTimeout(total=30),
        )
        self._worker_task = asyncio.create_task(self._queue_worker())

    async def close(self) -> None:
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self._bucket_size, self._tokens + elapsed * self._refill_rate
        )
        self._last_refill = now
        metrics.esi_rate_limit_tokens.set(self._tokens)

    def _update_rate_limit(self, headers: Mapping[str, str]) -> None:
        """Update token state from ESI response headers."""
        limit_header = headers.get("x-ratelimit-limit")
        if limit_header:
            try:
                limit_str = limit_header.split("/")[0]
                limit = int(limit_str)
                if limit != self._bucket_size:
                    self._bucket_size = float(limit)
                    self._refill_rate = self._bucket_size / config.esi.rate_limit_window
                    logger.info(f"ESI rate limit bucket size: {limit}")
            except (ValueError, IndexError):
                pass

        remaining = headers.get("x-ratelimit-remaining")
        if remaining is not None:
            reported = float(remaining)
            self._tokens = reported
            self._last_refill = time.monotonic()
            metrics.esi_rate_limit_tokens.set(self._tokens)

    async def _wait_for_rate_limit(self) -> None:
        """Wait until we have enough tokens for a request (assumes 2xx cost)."""
        needed = self.TOKEN_COST_2XX
        async with self._lock:
            now = time.monotonic()
            if now < self._rate_limited_until:
                wait_time = self._rate_limited_until - now
                logger.info(f"Rate limit backoff: waiting {wait_time:.0f}s.")
                await asyncio.sleep(wait_time)

            self._refill_tokens()

            if self._tokens < needed:
                wait_time = (needed - self._tokens) / self._refill_rate
                logger.warning(
                    f"Rate limit low ({self._tokens:.0f} tokens). "
                    f"Waiting {wait_time:.1f}s for tokens."
                )
                await asyncio.sleep(wait_time)
                self._refill_tokens()

    async def _queue_worker(self) -> None:
        """Process queued ESI requests by priority. Each item carries a thunk."""
        while not self._shutdown.is_set():
            try:
                _, _, future, thunk = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            metrics.esi_queue_depth.set(self._queue.qsize())

            if future.cancelled():
                continue

            try:
                result = await thunk()
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)

    async def fetch_killmail(
        self,
        killmail_id: int,
        killmail_hash: str,
        priority: Priority = Priority.CROSSCHECK,
    ) -> dict[str, Any] | None:
        """Queue a killmail fetch and wait for the result."""
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._seq += 1

        async def thunk() -> dict[str, Any] | None:
            return await self._do_fetch_killmail(killmail_id, killmail_hash)

        await self._queue.put((priority, self._seq, future, thunk))
        metrics.esi_queue_depth.set(self._queue.qsize())
        return await future

    async def fetch_war(self, war_id: int) -> dict[str, Any] | None:
        """Queue a war fetch (lowest priority, rate-limited) and wait."""
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._seq += 1

        async def thunk() -> dict[str, Any] | None:
            return await self._do_fetch_war(war_id)

        await self._queue.put((Priority.WAR, self._seq, future, thunk))
        metrics.esi_queue_depth.set(self._queue.qsize())
        return await future

    async def _do_fetch_killmail(
        self,
        killmail_id: int,
        killmail_hash: str,
    ) -> dict[str, Any] | None:
        """Actually fetch a killmail from ESI, respecting rate limits."""
        await self._wait_for_rate_limit()
        assert self._session is not None  # set in start() before any fetch is queued

        url = config.sources.esi_killmail_url.format(
            killmail_id=killmail_id, killmail_hash=killmail_hash
        )

        for attempt in range(3):
            request_start = time.monotonic()
            try:
                async with self._session.get(url) as resp:
                    self._update_rate_limit(resp.headers)
                    elapsed = time.monotonic() - request_start

                    if resp.status == 200:
                        metrics.esi_requests.labels("success").inc()
                        metrics.esi_request_seconds.labels("success").observe(elapsed)
                        return await resp.json()
                    elif resp.status in (420, 429):
                        metrics.esi_requests.labels("rate_limited").inc()
                        metrics.esi_request_seconds.labels("rate_limited").observe(elapsed)
                        metrics.esi_rate_limited.inc()
                        retry_after = max(int(resp.headers.get("Retry-After", 60)), 60)
                        logger.warning(
                            f"ESI rate limited ({resp.status}). "
                            f"Waiting {retry_after}s before retrying."
                        )
                        self._tokens = 0
                        self._rate_limited_until = time.monotonic() + retry_after
                        metrics.esi_backoff_seconds.inc(retry_after)
                        await asyncio.sleep(retry_after)
                        continue
                    elif resp.status in (502, 503, 504):
                        metrics.esi_requests.labels("server_error").inc()
                        metrics.esi_request_seconds.labels("server_error").observe(elapsed)
                        logger.warning(
                            f"ESI returned {resp.status} for killmail {killmail_id}. "
                            f"Retrying in 5s (attempt {attempt + 1}/3)."
                        )
                        await asyncio.sleep(5)
                        continue
                    elif resp.status == 404:
                        metrics.esi_requests.labels("not_found").inc()
                        metrics.esi_request_seconds.labels("not_found").observe(elapsed)
                        logger.warning(f"Killmail {killmail_id} not found (404).")
                        return None
                    else:
                        metrics.esi_requests.labels("error").inc()
                        metrics.esi_request_seconds.labels("error").observe(elapsed)
                        text = await resp.text()
                        logger.error(
                            f"ESI returned {resp.status} for killmail {killmail_id}: {text}"
                        )
                        return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                metrics.esi_requests.labels("network_error").inc()
                logger.warning(
                    f"Network error fetching killmail {killmail_id}: {e}. "
                    f"Retrying in 5s (attempt {attempt + 1}/3)."
                )
                await asyncio.sleep(5)
                continue

        metrics.esi_requests.labels("exhausted").inc()
        logger.error(f"Failed to fetch killmail {killmail_id} after 3 attempts.")
        return None

    async def _do_fetch_war(self, war_id: int) -> dict[str, Any] | None:
        """Fetch a war from ESI, respecting rate limits. None on 404/failure."""
        await self._wait_for_rate_limit()
        assert self._session is not None

        url = config.sources.esi_war_url.format(war_id=war_id)

        for attempt in range(3):
            request_start = time.monotonic()
            try:
                async with self._session.get(url) as resp:
                    self._update_rate_limit(resp.headers)
                    elapsed = time.monotonic() - request_start
                    if resp.status == 200:
                        metrics.esi_requests.labels("success").inc()
                        metrics.esi_request_seconds.labels("success").observe(elapsed)
                        return await resp.json()
                    if resp.status in (420, 429):
                        metrics.esi_requests.labels("rate_limited").inc()
                        metrics.esi_request_seconds.labels("rate_limited").observe(elapsed)
                        metrics.esi_rate_limited.inc()
                        retry_after = max(int(resp.headers.get("Retry-After", 60)), 60)
                        logger.warning(
                            f"ESI war rate limited ({resp.status}). Waiting {retry_after}s."
                        )
                        self._tokens = 0
                        self._rate_limited_until = time.monotonic() + retry_after
                        metrics.esi_backoff_seconds.inc(retry_after)
                        await asyncio.sleep(retry_after)
                        continue
                    if resp.status in (502, 503, 504):
                        metrics.esi_requests.labels("server_error").inc()
                        metrics.esi_request_seconds.labels("server_error").observe(elapsed)
                        await asyncio.sleep(5)
                        continue
                    if resp.status == 404:
                        metrics.esi_requests.labels("not_found").inc()
                        metrics.esi_request_seconds.labels("not_found").observe(elapsed)
                        logger.warning(f"War {war_id} not found (404).")
                        return None
                    metrics.esi_requests.labels("error").inc()
                    metrics.esi_request_seconds.labels("error").observe(elapsed)
                    logger.error(f"ESI returned {resp.status} for war {war_id}.")
                    return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                metrics.esi_requests.labels("network_error").inc()
                logger.warning(f"Network error fetching war {war_id}: {e}. Retrying.")
                await asyncio.sleep(5)
                continue

        metrics.esi_requests.labels("exhausted").inc()
        logger.error(f"Failed to fetch war {war_id} after 3 attempts.")
        raise RuntimeError(f"war {war_id} fetch exhausted (transient)")

    async def fetch_url(self, url: str, timeout: int = 120) -> Any | None:
        """Fetch a non-ESI URL (zKillboard, etc). No rate limiting applied."""
        req_timeout = aiohttp.ClientTimeout(total=timeout)
        assert self._session is not None  # set in start() before any fetch is queued
        for attempt in range(3):
            try:
                async with self._session.get(url, timeout=req_timeout) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)
                    else:
                        text = await resp.text()
                        logger.warning(
                            f"GET {url} returned {resp.status}: {text}. "
                            f"Retrying (attempt {attempt + 1}/3)."
                        )
                        await asyncio.sleep(5)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"Network error fetching {url}: {e}. "
                    f"Retrying (attempt {attempt + 1}/3)."
                )
                await asyncio.sleep(5)

        logger.error(f"Failed to fetch {url} after 3 attempts.")
        return None

    async def resolve_names(self, ids: set[int]) -> tuple[dict[int, str], set[int]]:
        """Bulk-resolve names. Returns (resolved, failed): `failed` holds ids from a
        WHOLE batch that failed transiently (non-200 / network) and must be retried,
        not tombstoned. An id simply absent from a 200 response is the bulk
        404-equivalent -- it is left out of both dicts so the caller tombstones it.
        Not rate limited."""
        if not ids:
            return {}, set()
        assert self._session is not None
        result: dict[int, str] = {}
        failed: set[int] = set()
        id_list = list(ids)
        for i in range(0, len(id_list), 1000):
            batch = id_list[i : i + 1000]
            try:
                async with self._session.post(
                    config.sources.esi_names_url, json=batch
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            f"universe/names returned {resp.status} for {len(batch)} ids."
                        )
                        failed.update(batch)
                        continue
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Network error resolving names: {e}")
                failed.update(batch)
                continue
            for item in data:
                result[item["id"]] = item["name"]
        return result, failed

    async def _get_named_entity(
        self, url: str, kind: str, entity_id: int
    ) -> tuple[str, str] | None:
        """GET an entity returning {name, ticker}. None ONLY on 404 (genuine
        absent -> tombstone). Raises on transient failure (5xx / network / other
        non-200) so the caller retries instead of tombstoning. Not rate limited."""
        assert self._session is not None
        async with self._session.get(url) as resp:
            if resp.status == 404:
                return None
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(
                    f"{kind} {entity_id} returned {resp.status}: {text[:200]}"
                )
            data = await resp.json()
        return data["name"], data["ticker"]

    async def get_corporation(self, corporation_id: int) -> tuple[str, str] | None:
        url = config.sources.esi_corporation_url.format(corporation_id=corporation_id)
        return await self._get_named_entity(url, "corporation", corporation_id)

    async def get_alliance(self, alliance_id: int) -> tuple[str, str] | None:
        url = config.sources.esi_alliance_url.format(alliance_id=alliance_id)
        return await self._get_named_entity(url, "alliance", alliance_id)

    async def get_factions(self) -> list[dict[str, Any]]:
        assert self._session is not None
        try:
            async with self._session.get(config.sources.esi_factions_url) as resp:
                if resp.status != 200:
                    logger.warning(f"universe/factions returned {resp.status}.")
                    return []
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Network error fetching factions: {e}")
            return []


def parse_kill(kill_data: Mapping[str, Any]) -> ParsedKill | None:
    """Parse ESI killmail data into our internal format. Returns None if no position."""
    victim: Mapping[str, Any] = kill_data.get("victim", {})
    position = victim.get("position")

    if not position:
        return None

    attackers: list[Any] = []
    for attacker in kill_data.get("attackers", []):
        attackers.append(
            {
                "character_id": attacker.get("character_id"),
                "corporation_id": attacker.get("corporation_id"),
                "alliance_id": attacker.get("alliance_id"),
                "faction_id": attacker.get("faction_id"),
                "ship_type_id": attacker.get("ship_type_id"),
                "weapon_type_id": attacker.get("weapon_type_id"),
                "damage_done": attacker.get("damage_done", 0),
                "final_blow": attacker.get("final_blow", False),
                "security_status": attacker.get("security_status", 0.0),
            }
        )

    return {
        "killmail_id": kill_data["killmail_id"],
        "killmail_hash": kill_data.get("killmail_hash", ""),
        "killmail_time": kill_data["killmail_time"],
        "solar_system_id": kill_data["solar_system_id"],
        "position_x": position["x"],
        "position_y": position["y"],
        "position_z": position["z"],
        "victim_character_id": victim.get("character_id"),
        "victim_corporation_id": victim.get("corporation_id"),
        "victim_alliance_id": victim.get("alliance_id"),
        "victim_faction_id": victim.get("faction_id"),
        "victim_damage_taken": victim.get("damage_taken", 0),
        "victim_ship_type_id": victim["ship_type_id"],
        "war_id": kill_data.get("war_id"),
        "attackers": attackers,
    }
