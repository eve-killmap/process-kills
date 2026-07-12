"""Prometheus metrics for the process-kills ingestion service.

Application-level only: throughput, freshness, external-dependency health, and
job status. PostgreSQL internals are covered by a separate DB exporter. Metric
objects are module-level singletons (the intended prometheus_client pattern) and
are cheap no-ops until :func:`start_metrics_server` binds the scrape endpoint.
Default process / platform / GC collectors are exported automatically.

Instrumentation call sites must never raise into business logic; the
``.inc()`` / ``.observe()`` / ``.set()`` calls here do not raise under normal use.

Metric names are prefixed ``eve_killmap_`` (no per-service segment); Prometheus
distinguishes services by the scrape target's ``job``/``instance`` labels.
"""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

from config import SERVICE_VERSION, Config

logger = logging.getLogger(__name__)

# Lag spans seconds (live) to days (cross-check backfilling old kills).
_LAG_BUCKETS = (1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 10800, 21600, 86400)
# Batch jobs run for seconds to hours.
_DURATION_BUCKETS = (1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600)


# Throughput and Freshness

kills_processed = Counter(
    "eve_killmap_kills_processed",
    "Killmails processed, by pipeline source and outcome.",
    ["source", "outcome"],  # source: live|crosscheck|recheck  outcome: inserted|no_position|duplicate|skipped
)
attackers_inserted = Counter(
    "eve_killmap_attackers_inserted",
    "Attacker rows inserted alongside kills.",
)
kill_processing_seconds = Histogram(
    "eve_killmap_kill_processing_seconds",
    "Time to process a single killmail (parse + insert + publish).",
    ["source"],
)
killmail_lag_seconds = Histogram(
    "eve_killmap_killmail_lag_seconds",
    "Age of a killmail (now - killmail_time) when it is processed.",
    ["source"],
    buckets=_LAG_BUCKETS,
)
last_processed_killmail_timestamp = Gauge(
    "eve_killmap_last_processed_killmail_timestamp_seconds",
    "killmail_time (unix seconds) of the most recently processed kill.",
)


# Live Listener

live_sequence = Gauge(
    "eve_killmap_live_sequence",
    "Current R2Z2 live sequence cursor.",
)
live_sequence_fetches = Counter(
    "eve_killmap_live_sequence_fetches",
    "R2Z2 ephemeral sequence fetches, by result.",
    ["status"],  # ok|not_found|error
)
live_sequence_fetch_seconds = Histogram(
    "eve_killmap_live_sequence_fetch_seconds",
    "Latency of an R2Z2 sequence fetch.",
)
live_listener_paused = Gauge(
    "eve_killmap_live_listener_paused",
    "1 while the live listener is paused for maintenance, else 0.",
)
live_pause_seconds = Counter(
    "eve_killmap_live_pause_seconds",
    "Cumulative seconds the live listener has been paused for maintenance.",
)


# ESI client

esi_requests = Counter(
    "eve_killmap_esi_requests",
    "ESI killmail fetch responses, by outcome.",
    ["outcome"],  # success|not_found|rate_limited|server_error|error|network_error|exhausted
)
esi_request_seconds = Histogram(
    "eve_killmap_esi_request_seconds",
    "Latency of a single ESI killmail HTTP request, by outcome.",
    ["outcome"],
)
esi_rate_limit_tokens = Gauge(
    "eve_killmap_esi_rate_limit_tokens",
    "Remaining tokens in the ESI rate-limit bucket.",
)
esi_rate_limited = Counter(
    "eve_killmap_esi_rate_limited",
    "Times ESI returned a rate-limit status (420/429).",
)
esi_backoff_seconds = Counter(
    "eve_killmap_esi_backoff_seconds",
    "Cumulative seconds slept on ESI rate-limit backoff.",
)
esi_queue_depth = Gauge(
    "eve_killmap_esi_queue_depth",
    "Pending killmail fetches in the ESI priority queue.",
)


# zKillboard

zkb_requests = Counter(
    "eve_killmap_zkb_requests",
    "zKillboard history fetches, by endpoint and outcome.",
    ["endpoint", "outcome"],  # endpoint: totals|day  outcome: success|failed
)


# Crosscheck

crosscheck_runs = Counter(
    "eve_killmap_crosscheck_runs",
    "Cross-check runs, by result.",
    ["result"],  # success|failed
)
crosscheck_duration_seconds = Histogram(
    "eve_killmap_crosscheck_duration_seconds",
    "Duration of a cross-check run.",
    buckets=_DURATION_BUCKETS,
)
crosscheck_last_success_timestamp = Gauge(
    "eve_killmap_crosscheck_last_success_timestamp_seconds",
    "Unix time of the last successful cross-check.",
)
crosscheck_dates_pending = Gauge(
    "eve_killmap_crosscheck_dates_pending",
    "Dates still needing reconciliation after the last cross-check.",
)
crosscheck_missing_kills = Counter(
    "eve_killmap_crosscheck_missing_kills",
    "Kills found missing from the DB during cross-check (and then fetched).",
)


# Recheck

recheck_runs = Counter(
    "eve_killmap_recheck_runs",
    "No-position recheck cycles, by result.",
    ["result"],  # success|failed
)
recheck_candidates = Gauge(
    "eve_killmap_recheck_candidates",
    "No-position kills examined in the last recheck cycle.",
)
recheck_gained_position = Counter(
    "eve_killmap_recheck_gained_position",
    "No-position kills that gained a position on recheck.",
)
recheck_still_no_position = Counter(
    "eve_killmap_recheck_still_no_position",
    "No-position kills still missing a position after recheck.",
)
recheck_last_run_timestamp = Gauge(
    "eve_killmap_recheck_last_run_timestamp_seconds",
    "Unix time of the last recheck cycle.",
)


# Maintenance

maintenance_runs = Counter(
    "eve_killmap_maintenance_runs",
    "Maintenance job runs, by task and result.",
    ["task", "result"],  # task: mv_refresh|weekly  result: success|failed
)
maintenance_duration_seconds = Histogram(
    "eve_killmap_maintenance_duration_seconds",
    "Duration of a maintenance task.",
    ["task"],
    buckets=_DURATION_BUCKETS,
)
maintenance_last_success_timestamp = Gauge(
    "eve_killmap_maintenance_last_success_timestamp_seconds",
    "Unix time of the last successful run of a maintenance task.",
    ["task"],
)


# Streaming and Cache Invalidation

stream_publishes = Counter(
    "eve_killmap_stream_publishes",
    "Live kill stream publishes, by result.",
    ["result"],  # success|failed
)
stream_kills_discarded = Counter(
    "eve_killmap_stream_kills_discarded",
    "Kills not streamed because older than streaming.discard_older_than.",
)
cache_invalidations_published = Counter(
    "eve_killmap_cache_invalidations_published",
    "Cache-invalidation messages published, by target and result.",
    ["target", "result"],  # target: system_rankings|farthest_kill  result: success|failed
)
redis_connected = Gauge(
    "eve_killmap_redis_connected",
    "1 if the Redis client connected at startup, else 0.",
)


# Health and Meta

errors = Counter(
    "eve_killmap_errors",
    "Unhandled errors caught in a scheduler/loop, by component.",
    ["component"],  # live|crosscheck|recheck|maintenance|mv_refresh
)
service_start_timestamp = Gauge(
    "eve_killmap_service_start_timestamp_seconds",
    "Unix time the service started.",
)
service_info = Info(
    "eve_killmap_service",
    "Static service information (version).",
)


_started = False


def start_metrics_server(config: Config) -> None:
    """Start the Prometheus scrape endpoint if enabled (idempotent)."""
    global _started
    if not config.metrics.enabled:
        logger.info("Prometheus metrics exporter disabled (metrics.enabled=false).")
        return
    if _started:
        return

    service_info.info({"version": SERVICE_VERSION})
    service_start_timestamp.set_to_current_time()

    start_http_server(config.metrics.port, addr=config.metrics.host)
    _started = True
    logger.info(
        "Prometheus metrics exporter listening on %s:%d",
        config.metrics.host,
        config.metrics.port,
    )
