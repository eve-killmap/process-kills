import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta

import metrics
from config import BEGIN_DATE, config
from esi import ESIClient, Priority, parse_kill
from db import (
    get_connection,
    get_processed_date,
    update_processed_date,
    insert_kill,
    insert_no_position_kill,
    get_existing_killmail_ids,
    increment_processed_kills,
    increment_no_position_kills,
)

logger = logging.getLogger(__name__)


def _fix_date(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y%m%d")
    return dt.strftime("%Y-%m-%d")


async def crosscheck_scheduler(
    esi_client: ESIClient, shutdown_event: asyncio.Event
) -> None:
    hour = config.crosscheck.hour
    logger.info(f"Cross-check scheduler started. Runs daily at {hour:02d}:00 UTC.")

    while not shutdown_event.is_set():
        now = datetime.now(timezone.utc)
        next_run = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)

        wait_seconds = (next_run - now).total_seconds()
        logger.info(f"Next cross-check in {wait_seconds / 3600:.1f} hours.")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            pass

        try:
            start = time.monotonic()
            await run_crosscheck(esi_client, shutdown_event)
            metrics.crosscheck_duration_seconds.observe(time.monotonic() - start)
            metrics.crosscheck_runs.labels("success").inc()
            metrics.crosscheck_last_success_timestamp.set_to_current_time()
        except Exception as e:
            metrics.crosscheck_runs.labels("failed").inc()
            metrics.errors.labels("crosscheck").inc()
            logger.error(f"Cross-check error: {e}", exc_info=True)

    logger.info("Cross-check scheduler stopped.")


async def run_crosscheck(esi_client: ESIClient, shutdown_event: asyncio.Event) -> None:
    logger.info("Starting cross-check...")

    totals = await esi_client.fetch_url(config.sources.zkb_totals_url)
    if totals is None:
        metrics.zkb_requests.labels("totals", "failed").inc()
        logger.error("Failed to fetch zKillboard totals. Skipping cross-check.")
        return
    metrics.zkb_requests.labels("totals", "success").inc()

    dates_to_check: list[tuple[str, str, int]] = []
    with get_connection() as conn:
        for date_str, expected_count in totals.items():
            if int(date_str) < BEGIN_DATE:
                continue
            if expected_count <= 0:
                continue

            formatted_date = _fix_date(date_str)
            processed = get_processed_date(conn, formatted_date)

            if processed is not None:
                accounted_for = (
                    processed["processed_kills"] + processed["no_position_kills"]
                )
                if (
                    accounted_for >= expected_count
                    and processed["error_message"] is None
                ):
                    if expected_count > processed["total_kills"]:
                        update_processed_date(
                            conn,
                            formatted_date,
                            total_kills=expected_count,
                            processed_kills=processed["processed_kills"],
                            no_position_kills=processed["no_position_kills"],
                        )
                    continue

            dates_to_check.append((date_str, formatted_date, expected_count))

    metrics.crosscheck_dates_pending.set(len(dates_to_check))

    if not dates_to_check:
        logger.info("Cross-check complete: all dates reconciled.")
        return

    logger.info(f"Cross-check: {len(dates_to_check)} dates need reconciliation.")

    for date_raw, date_formatted, expected_count in dates_to_check:
        if shutdown_event.is_set():
            break

        await _crosscheck_date(
            esi_client, shutdown_event, date_raw, date_formatted, expected_count
        )

    logger.info("Cross-check finished.")


async def _crosscheck_date(
    esi_client: ESIClient,
    shutdown_event: asyncio.Event,
    date_raw: str,
    date_formatted: str,
    expected_count: int,
) -> None:
    logger.info(f"Cross-checking {date_formatted} (expected: {expected_count})...")

    url = config.sources.zkb_day_url.format(date=date_raw)
    day_data = await esi_client.fetch_url(url)
    if day_data is None:
        metrics.zkb_requests.labels("day", "failed").inc()
        logger.error(f"Failed to fetch killmail list for {date_formatted}.")
        return
    metrics.zkb_requests.labels("day", "success").inc()

    all_ids = [int(kid) for kid in day_data.keys()]
    with get_connection() as conn:
        existing_ids = get_existing_killmail_ids(conn, all_ids)

    missing = {
        kid_str: khash
        for kid_str, khash in day_data.items()
        if int(kid_str) not in existing_ids
    }

    if not missing:
        with get_connection() as conn:
            processed = get_processed_date(conn, date_formatted)
            if processed:
                update_processed_date(
                    conn,
                    date_formatted,
                    total_kills=expected_count,
                    processed_kills=processed["processed_kills"],
                    no_position_kills=processed["no_position_kills"],
                )
        logger.info(f"Cross-check {date_formatted}: all kills already present.")
        return

    metrics.crosscheck_missing_kills.inc(len(missing))
    logger.info(f"Cross-check {date_formatted}: {len(missing)} missing kills to fetch.")

    new_kills = 0
    new_no_position = 0

    for killmail_id_str, killmail_hash in missing.items():
        if shutdown_event.is_set():
            break

        killmail_id = int(killmail_id_str)

        kill_data = await esi_client.fetch_killmail(
            killmail_id, killmail_hash, Priority.CROSSCHECK
        )
        if kill_data is None:
            continue

        kill_data["killmail_hash"] = killmail_hash
        killmail_time = kill_data.get("killmail_time", "")
        parsed = parse_kill(kill_data)

        with get_connection() as conn:
            if parsed:
                inserted = insert_kill(conn, parsed)
                if inserted:
                    increment_processed_kills(conn, date_formatted)
                    new_kills += 1
                    metrics.kills_processed.labels("crosscheck", "inserted").inc()
                    metrics.attackers_inserted.inc(len(parsed["attackers"]))
            else:
                inserted = insert_no_position_kill(
                    conn, killmail_id, killmail_hash, killmail_time
                )
                if inserted:
                    increment_no_position_kills(conn, date_formatted)
                    new_no_position += 1
                    metrics.kills_processed.labels("crosscheck", "no_position").inc()

    with get_connection() as conn:
        processed = get_processed_date(conn, date_formatted)
        if processed:
            update_processed_date(
                conn,
                date_formatted,
                total_kills=expected_count,
                processed_kills=processed["processed_kills"],
                no_position_kills=processed["no_position_kills"],
            )
        else:
            update_processed_date(
                conn,
                date_formatted,
                total_kills=expected_count,
                processed_kills=new_kills,
                no_position_kills=new_no_position,
            )

    logger.info(
        f"Cross-check {date_formatted}: "
        f"{new_kills} new kills, {new_no_position} new no-position."
    )
