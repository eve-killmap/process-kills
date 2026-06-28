"""No-position kill rechecking.

Periodically re-fetches kills that previously had no position data, using a
tiered strategy based on kill age. This pass is optional and disabled by default
(``recheck.enabled`` in config.yml); ``main`` only schedules it when enabled.
"""

import asyncio
import logging

from config import config
from esi import ESIClient, Priority, parse_kill
from db import (
    get_connection,
    get_recheck_candidates,
    insert_kill,
    delete_no_position_kill,
    update_no_position_last_checked,
    increment_processed_kills,
    decrement_no_position_kills,
)

logger = logging.getLogger(__name__)


async def no_position_rechecking(
    esi_client: ESIClient, shutdown_event: asyncio.Event
) -> None:
    logger.info("No-position rechecking started.")

    while not shutdown_event.is_set():
        try:
            await _recheck_cycle(esi_client, shutdown_event)
        except Exception as e:
            logger.error(f"Recheck cycle error: {e}", exc_info=True)

        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=config.recheck.interval_seconds
            )
            break
        except asyncio.TimeoutError:
            pass

    logger.info("No-position rechecking stopped.")


async def _recheck_cycle(esi_client: ESIClient, shutdown_event: asyncio.Event) -> None:
    with get_connection() as conn:
        candidates = get_recheck_candidates(conn, limit=config.recheck.batch_limit)

    if not candidates:
        logger.debug("No recheck candidates found.")
        return

    logger.info(f"Rechecking {len(candidates)} kills without position data...")

    gained_position = 0
    still_no_position = 0

    for candidate in candidates:
        if shutdown_event.is_set():
            break

        killmail_id = candidate["killmail_id"]
        killmail_hash = candidate["killmail_hash"]
        killmail_time = candidate["killmail_time"]

        kill_data = await esi_client.fetch_killmail(
            killmail_id, killmail_hash, Priority.RECHECK
        )

        if kill_data is None:
            with get_connection() as conn:
                update_no_position_last_checked(conn, killmail_id)
            still_no_position += 1
            continue

        kill_data["killmail_hash"] = killmail_hash
        parsed = parse_kill(kill_data)

        with get_connection() as conn:
            if parsed:
                inserted = insert_kill(conn, parsed)
                if inserted:
                    date_str = killmail_time.strftime("%Y-%m-%d")
                    increment_processed_kills(conn, date_str)
                    decrement_no_position_kills(conn, date_str)
                delete_no_position_kill(conn, killmail_id)
                gained_position += 1
                logger.info(f"Recheck: Killmail {killmail_id} now has position data!")
            else:
                update_no_position_last_checked(conn, killmail_id)
                still_no_position += 1

    logger.info(
        f"Recheck cycle complete: {gained_position} gained position, "
        f"{still_no_position} still without."
    )
