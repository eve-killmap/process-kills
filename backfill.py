"""Standalone, archival backfill script.

This script backfilled the kills database with killmails that occurred *before*
this project existed, in one bulk pass over EVERef's daily killmail archives.

It is standalone and does not participate in the live service (``main.py``): the
live listener and cross-checker keep the database current going forward. It is
kept only for archival/reproducibility purposes and is not run routinely. It
still shares this project's ``config`` and ``db`` modules.
"""

import json
import logging
import tarfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from config import (
    BEGIN_DATE,
    config,
    ensure_data_dirs,
    setup_logging,
)
from db import (
    get_connection,
    init_schema,
    insert_kills_batch,
    insert_no_position_kills_batch,
    update_processed_date,
    get_processed_date,
)
from esi import parse_kill

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers["User-Agent"] = config.user_agent


def fetch_totals() -> dict[str, int]:
    resp = SESSION.get(config.sources.zkb_totals_url, timeout=config.backfill.timeout)
    resp.raise_for_status()
    return resp.json()


def download_archive(date_str: str) -> Path:
    date = datetime.strptime(date_str, "%Y-%m-%d")
    year = str(date.year)

    file_name = f"killmails-{date_str}.tar.bz2"
    day_url = config.backfill.day_path.format(year=year, date=date_str)
    url = config.backfill.killmail_base_url + day_url
    dest = config.paths.data_dir / file_name

    if dest.exists():
        return dest

    last_exception: Exception | None = None
    for attempt in range(config.backfill.max_retries):
        try:
            with SESSION.get(url, stream=True, timeout=config.backfill.timeout) as r:
                r.raise_for_status()
                tmp_path = dest.with_suffix(dest.suffix + ".part")
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                tmp_path.rename(dest)
                return dest
        except Exception as e:
            last_exception = e
            logger.warning(
                f"Download attempt {attempt + 1}/{config.backfill.max_retries} "
                f"failed for {date_str}: {e}"
            )
            time.sleep(config.backfill.sleep_between_retries)

    assert last_exception is not None
    raise last_exception


def process_archive(archive_path: Path, date_str: str, expected_count: int) -> None:
    kill_batch = []
    no_pos_batch = []
    processed_kills = 0
    no_position_kills = 0
    batch_size = config.processing.batch_size

    with get_connection() as conn:
        with tarfile.open(archive_path, "r:bz2") as tar:
            for member in tar.getmembers():
                try:
                    f = tar.extractfile(member)
                    if f is None:
                        continue

                    content = f.read().decode("utf-8")
                    kill_data = json.loads(content)

                    killmail_id = kill_data.get("killmail_id")
                    if not killmail_id:
                        continue

                    parsed = parse_kill(kill_data)

                    if parsed:
                        kill_batch.append(parsed)
                        if len(kill_batch) >= batch_size:
                            insert_kills_batch(conn, kill_batch)
                            processed_kills += len(kill_batch)
                            kill_batch = []
                    else:
                        killmail_hash = kill_data.get("killmail_hash", "")
                        killmail_time = kill_data.get("killmail_time", "")
                        no_pos_batch.append((killmail_id, killmail_hash, killmail_time))
                        if len(no_pos_batch) >= batch_size:
                            insert_no_position_kills_batch(conn, no_pos_batch)
                            no_position_kills += len(no_pos_batch)
                            no_pos_batch = []

                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    logger.error(f"Failed to parse {member.name}: {e}")
                    continue

        if kill_batch:
            insert_kills_batch(conn, kill_batch)
            processed_kills += len(kill_batch)

        if no_pos_batch:
            insert_no_position_kills_batch(conn, no_pos_batch)
            no_position_kills += len(no_pos_batch)

        update_processed_date(
            conn,
            date_str,
            total_kills=expected_count,
            processed_kills=processed_kills,
            no_position_kills=no_position_kills,
        )

    logger.info(
        f"{date_str}: {processed_kills} kills inserted, "
        f"{no_position_kills} no-position, {expected_count} expected."
    )


def main() -> None:
    setup_logging(config)
    ensure_data_dirs(config)
    logger.info("Backfill script starting...")

    with get_connection() as conn:
        init_schema(conn)

    logger.info("Fetching totals from zKillboard...")
    totals = fetch_totals()

    begin = datetime.strptime(str(BEGIN_DATE), "%Y%m%d")
    end = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=1)

    current = begin
    dates: list[tuple[str, int]] = []
    while current <= end:
        date_yyyymmdd = current.strftime("%Y%m%d")
        date_formatted = current.strftime("%Y-%m-%d")
        expected = totals.get(date_yyyymmdd, 0)
        if expected > 0:
            dates.append((date_formatted, expected))
        current += timedelta(days=1)

    logger.info(
        f"Processing {len(dates)} dates from {dates[0][0]} to {dates[-1][0]}..."
    )

    processed_dates = 0
    skipped_dates = 0
    failed_dates = 0

    for date_str, expected_count in dates:
        with get_connection() as conn:
            existing = get_processed_date(conn, date_str)
        if existing and existing["error_message"] is None:
            accounted = existing["processed_kills"] + existing["no_position_kills"]
            if accounted >= expected_count:
                skipped_dates += 1
                continue

        try:
            logger.info(f"Processing {date_str} ({expected_count} expected)...")
            archive_path = download_archive(date_str)
            process_archive(archive_path, date_str, expected_count)

            if archive_path.exists():
                archive_path.unlink()

            processed_dates += 1

        except Exception as e:
            logger.error(f"Failed to process {date_str}: {e}")
            with get_connection() as conn:
                update_processed_date(
                    conn,
                    date_str,
                    total_kills=expected_count,
                    error_message=str(e),
                )
            failed_dates += 1

    logger.info(
        f"Backfill complete: {processed_dates} dates processed, "
        f"{skipped_dates} skipped, {failed_dates} failed."
    )


if __name__ == "__main__":
    main()
