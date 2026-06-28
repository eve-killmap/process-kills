import asyncio
import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import aiohttp

import stream
from config import BEGIN_DATE, config
from esi import parse_kill
from schema import ParsedKill
from db import (
    get_connection,
    get_live_sequence,
    set_live_sequence,
    insert_kill,
    insert_no_position_kill,
    increment_processed_kills,
    increment_no_position_kills,
)

logger = logging.getLogger(__name__)


async def live_listener(
    shutdown_event: asyncio.Event,
    live_paused: asyncio.Event | None = None,
    redis: Any = None,
) -> None:
    logger.info("Live listener started.")

    session_timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(
        headers={"User-Agent": config.user_agent},
        timeout=session_timeout,
    ) as session:
        with get_connection() as conn:
            sequence = get_live_sequence(conn)

        if sequence is None:
            sequence = await _fetch_initial_sequence(session, shutdown_event)
            if sequence is None:
                logger.error("Failed to fetch initial sequence. Live listener exiting.")
                return
            logger.info(f"Live listener starting at sequence {sequence}.")
            with get_connection() as conn:
                set_live_sequence(conn, sequence)
        else:
            logger.info(f"Live listener resuming from sequence {sequence}.")

        while not shutdown_event.is_set():
            if live_paused and live_paused.is_set():
                logger.info("Live listener paused for maintenance.")
                while live_paused.is_set() and not shutdown_event.is_set():
                    await asyncio.sleep(1)
                if shutdown_event.is_set():
                    break
                logger.info("Live listener resumed.")

            try:
                url = config.sources.r2z2_ephemeral_url.format(sequence=sequence)
                status, data = await _fetch_sequence(session, url)

                if status is None:
                    await _interruptible_sleep(5.0, shutdown_event)
                    continue

                if status == 404:
                    await _interruptible_sleep(config.live.retry_delay, shutdown_event)
                    continue

                result, parsed = _process_sequence_kill(data or {}, sequence)
                if result == "inserted" and parsed is not None and redis is not None:
                    await stream.publish_kill(redis, parsed)

                sequence += 1
                with get_connection() as conn:
                    set_live_sequence(conn, sequence)

                await _interruptible_sleep(config.live.poll_delay, shutdown_event)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Live listener error: {e}", exc_info=True)
                await _interruptible_sleep(5.0, shutdown_event)

    logger.info("Live listener stopped.")


async def _fetch_initial_sequence(
    session: aiohttp.ClientSession,
    shutdown_event: asyncio.Event,
) -> int | None:
    for attempt in range(3):
        if shutdown_event.is_set():
            return None
        try:
            async with session.get(config.sources.r2z2_sequence_url) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return data.get("sequence")
                logger.warning(
                    f"sequence.json returned {resp.status} (attempt {attempt + 1}/3)"
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(
                f"Error fetching sequence.json: {e} (attempt {attempt + 1}/3)"
            )
        await _interruptible_sleep(5.0, shutdown_event)
    return None


async def _fetch_sequence(
    session: aiohttp.ClientSession,
    url: str,
) -> tuple[int | None, dict[str, Any] | None]:
    for attempt in range(3):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return 200, data
                if resp.status == 404:
                    return 404, None
                if resp.status == 429:
                    logger.warning("R2Z2 rate limited (429). Waiting 10s.")
                    await asyncio.sleep(10)
                    continue
                logger.warning(
                    f"R2Z2 returned {resp.status} for {url} (attempt {attempt + 1}/3)"
                )
                await asyncio.sleep(5)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(
                f"Network error fetching {url}: {e} (attempt {attempt + 1}/3)"
            )
            await asyncio.sleep(5)

    logger.error(f"Failed to fetch {url} after 3 attempts.")
    return None, None


def _process_sequence_kill(
    data: Mapping[str, Any], sequence: int
) -> tuple[str, ParsedKill | None]:
    killmail_id = data.get("killmail_id")
    killmail_hash = data.get("hash")

    if not killmail_id or not killmail_hash:
        logger.warning(f"Live: Sequence {sequence} missing killmail_id or hash")
        return "skipped", None

    kill_data = data.get("esi")
    if kill_data is None:
        logger.warning(f"Live: No ESI data for killmail {killmail_id} (seq {sequence})")
        return "skipped", None

    kill_data["killmail_hash"] = killmail_hash

    killmail_time = kill_data.get("killmail_time", "")
    date_str = _killmail_time_to_date(killmail_time)

    date_compare = int(date_str.replace("-", ""))
    if date_compare < BEGIN_DATE:
        logger.debug(
            f"Live: Skipping killmail {killmail_id} (seq {sequence}), before {BEGIN_DATE}"
        )
        return "skipped", None

    parsed = parse_kill(kill_data)

    with get_connection() as conn:
        if parsed:
            inserted = insert_kill(conn, parsed)
            if inserted:
                increment_processed_kills(conn, date_str)
                logger.debug(f"Live: Inserted killmail {killmail_id} (seq {sequence})")
                return "inserted", parsed
        else:
            inserted = insert_no_position_kill(
                conn, killmail_id, killmail_hash, killmail_time
            )
            if inserted:
                increment_no_position_kills(conn, date_str)
                logger.debug(
                    f"Live: No position for killmail {killmail_id} (seq {sequence})"
                )
                return "skipped", None

    logger.warning(f"Live: Duplicate killmail {killmail_id} (seq {sequence}), skipping")
    return "duplicate", None


async def _interruptible_sleep(seconds: float, shutdown_event: asyncio.Event) -> None:
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


def _killmail_time_to_date(killmail_time: str) -> str:
    try:
        dt = datetime.fromisoformat(killmail_time.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
