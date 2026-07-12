import argparse
import asyncio
import logging
import signal
import sys
from types import FrameType

from config import ConfigError, config, require_database_url, setup_logging
from db import get_connection, init_schema
from esi import ESIClient
from live import live_listener
import metrics
import stream as kill_stream
from crosscheck import crosscheck_scheduler
from recheck import no_position_rechecking
from maintenance import maintenance_scheduler, mv_refresh_scheduler

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EVE Killmap kill-ingestion service (live listener, "
        "cross-checker, optional rechecking, and maintenance)."
    )
    return parser.parse_args()


async def main() -> None:
    setup_logging(config)

    logger.info("EVE Killmap DB service starting...")

    try:
        require_database_url(config)
    except ConfigError as e:
        logger.error(str(e))
        sys.exit(1)

    metrics.start_metrics_server(config)

    logger.info("Initializing database schema...")
    with get_connection() as conn:
        init_schema(conn)

    shutdown_event = asyncio.Event()

    def handle_signal(sig: int, _frame: FrameType | None) -> None:
        logger.info(f"Received signal {sig}, shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    esi_client = ESIClient(shutdown_event)
    await esi_client.start()

    redis_client = None
    if config.redis_url:
        try:
            redis_client = await kill_stream.connect_redis()
            logger.info("Connected to Redis stream.")
        except Exception as e:
            logger.warning(
                f"Could not connect to Redis ({e}). Live kills will not be streamed."
            )
    metrics.redis_connected.set(1 if redis_client is not None else 0)

    live_paused = asyncio.Event()

    tasks = [
        live_listener(shutdown_event, live_paused, redis=redis_client),
        crosscheck_scheduler(esi_client, shutdown_event),
        maintenance_scheduler(shutdown_event, live_paused, redis=redis_client),
        mv_refresh_scheduler(shutdown_event, redis=redis_client),
    ]
    if config.recheck.enabled:
        tasks.append(no_position_rechecking(esi_client, shutdown_event))

    recheck_state = "enabled" if config.recheck.enabled else "disabled"
    logger.info(
        f"Service started. Running live listener, cross-checker, weekly "
        f"maintenance (no-position rechecking {recheck_state})."
    )

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Shutting down ESI client...")
        await esi_client.close()
        if redis_client:
            await redis_client.aclose()
        logger.info("Service stopped.")


if __name__ == "__main__":
    parse_args()  # handle --help before starting the service
    asyncio.run(main())
