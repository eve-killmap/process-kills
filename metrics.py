from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

from config import SERVICE_VERSION, Config

logger = logging.getLogger(__name__)

_LAG_BUCKETS = (1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 10800, 21600, 86400)
_DURATION_BUCKETS = (1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600)


kills_processed = Counter(
    "eve_killmap_kills_processed",
    "Killmails processed, by pipeline source and outcome.",
    [
        "source",
        "outcome",
    ],
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


live_sequence = Gauge(
    "eve_killmap_live_sequence",
    "Current R2Z2 live sequence cursor.",
)
live_sequence_fetches = Counter(
    "eve_killmap_live_sequence_fetches",
    "R2Z2 ephemeral sequence fetches, by result.",
    ["status"],
)
live_sequence_fetch_seconds = Histogram(
    "eve_killmap_live_sequence_fetch_seconds",
    "Latency of an R2Z2 sequence fetch.",
)


esi_requests = Counter(
    "eve_killmap_esi_requests",
    "ESI killmail fetch responses, by outcome.",
    ["outcome"],
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


zkb_requests = Counter(
    "eve_killmap_zkb_requests",
    "zKillboard history fetches, by endpoint and outcome.",
    ["endpoint", "outcome"],
)


crosscheck_runs = Counter(
    "eve_killmap_crosscheck_runs",
    "Cross-check runs, by result.",
    ["result"],
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


mv_refresh_runs = Counter(
    "eve_killmap_mv_refresh_runs",
    "Refresh cycles (all steps of a cadence), by cadence and result.",
    ["cadence", "result"],
)
mv_refresh_duration_seconds = Histogram(
    "eve_killmap_mv_refresh_duration_seconds",
    "Duration of a whole refresh cycle (all steps of a cadence).",
    ["cadence"],
    buckets=_DURATION_BUCKETS,
)
mv_refresh_last_success_timestamp = Gauge(
    "eve_killmap_mv_refresh_last_success_timestamp_seconds",
    "Unix time of the last fully successful refresh cycle, by cadence.",
    ["cadence"],
)
refresh_step_runs = Counter(
    "eve_killmap_refresh_step_runs",
    "Refresh-cycle steps, by cadence, step and result.",
    [
        "cadence",
        "step",
        "result",
    ],
)
refresh_step_duration_seconds = Histogram(
    "eve_killmap_refresh_step_duration_seconds",
    "Duration of one refresh-cycle step.",
    ["cadence", "step"],
    buckets=_DURATION_BUCKETS,
)


rollup_days_rolled = Counter(
    "eve_killmap_rollup_days_rolled",
    "UTC days recomputed in entity_kills_daily + system_kills_daily "
    "(dozens per fast cycle is normal: late-arriving kills dirty old days).",
)
rollup_watermark_timestamp = Gauge(
    "eve_killmap_rollup_watermark_timestamp_seconds",
    "Unix time of the shared rollup watermark (now - this = rollup lag).",
)
leaderboard_computations = Counter(
    "eve_killmap_leaderboard_computations",
    "Leaderboard window recomputations, by window and result.",
    [
        "window",
        "result",
    ],
)
leaderboard_compute_seconds = Histogram(
    "eve_killmap_leaderboard_compute_seconds",
    "Duration of one leaderboard window recomputation.",
    ["window"],
    buckets=_DURATION_BUCKETS,
)
leaderboard_last_success_timestamp = Gauge(
    "eve_killmap_leaderboard_last_success_timestamp_seconds",
    "Unix time of the last successful recomputation, by window.",
    ["window"],
)
leaderboard_rows = Gauge(
    "eve_killmap_leaderboard_rows",
    "Rows in the last successfully written board, by window and scope (0 = board went empty).",
    ["window", "scope"],
)


stream_publishes = Counter(
    "eve_killmap_stream_publishes",
    "Live kill stream publishes, by result.",
    ["result"],
)
stream_kills_discarded = Counter(
    "eve_killmap_stream_kills_discarded",
    "Kills not streamed because older than streaming.discard_older_than.",
)
cache_invalidations_published = Counter(
    "eve_killmap_cache_invalidations_published",
    "Cache-invalidation messages published, by target and result.",
    [
        "target",
        "result",
    ],
)
redis_connected = Gauge(
    "eve_killmap_redis_connected",
    "1 if the Redis client connected at startup, else 0.",
)


heartbeat_pushes = Counter(
    "eve_killmap_heartbeat_pushes",
    "Uptime Kuma push-heartbeat results, by result.",
    ["result"],
)


errors = Counter(
    "eve_killmap_errors",
    "Unhandled errors caught in a scheduler/loop, by component.",
    ["component"],
)
service_start_timestamp = Gauge(
    "eve_killmap_service_start_timestamp_seconds",
    "Unix time the service started.",
)
service_info = Info(
    "eve_killmap_service",
    "Static service information (version).",
)


entities_resolved = Counter(
    "eve_killmap_entities_resolved",
    "Entities resolved via ESI, by kind and outcome.",
    [
        "kind",
        "outcome",
    ],
)
entity_resolve_seconds = Histogram(
    "eve_killmap_entity_resolve_seconds",
    "Time to resolve+store a kind of entity for one kill (inline path cost).",
    ["kind"],
)
entity_resolve_timeouts = Counter(
    "eve_killmap_entity_resolve_timeouts",
    "Inline entity resolutions that exceeded resolve_timeout and were queued.",
)
entity_backlog_depth = Gauge(
    "eve_killmap_entity_backlog_depth",
    "Rows in entity_resolve_backlog (should be ~0 in steady state).",
)
entities_backfilled = Counter(
    "eve_killmap_entities_backfilled",
    "Entities resolved by the historical backfill, by kind.",
    ["kind"],
)
wars_resolved = Counter(
    "eve_killmap_wars_resolved",
    "War refreshes, by outcome.",
    ["outcome"],
)
wars_pending = Gauge(
    "eve_killmap_wars_pending",
    "Wars due for refresh (refresh_after <= now); drain progress.",
)
factions_refreshed = Counter(
    "eve_killmap_factions_refreshed",
    "Faction table refresh runs, by result.",
    ["result"],
)
facets_written = Counter(
    "eve_killmap_facets_written",
    "Facet rows written at ingestion, by facet kind.",
    ["kind"],
)
facets_write_seconds = Histogram(
    "eve_killmap_facets_write_seconds",
    "Time to write all facet rows for one kill (insert_facets round-trip). "
    "The _count series doubles as the per-kill facet-write throughput.",
)


zkb_written = Counter(
    "eve_killmap_zkb_written",
    "zKillboard metadata rows written at ingestion.",
)


corporations_refreshed = Counter(
    "eve_killmap_corporations_refreshed",
    "Corporation metadata refreshes, by outcome.",
    ["outcome"],
)
corporations_pending = Gauge(
    "eve_killmap_corporations_pending",
    "Corporations due for refresh (refresh_after <= now, non-terminal).",
)
corporation_refresh_seconds = Histogram(
    "eve_killmap_corporation_refresh_seconds",
    "Duration of one corporation-refresh batch (fetch + upsert).",
    buckets=_DURATION_BUCKETS,
)


_started = False


def start_metrics_server(config: Config) -> None:
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
