from prometheus_client import REGISTRY

import metrics
from config import load_config


def _val(name, labels=None):
    return REGISTRY.get_sample_value(name, labels or {})


def test_kills_processed_counter_increments():
    labels = {"source": "live", "outcome": "inserted"}
    before = _val("eve_killmap_kills_processed_total", labels) or 0.0
    metrics.kills_processed.labels("live", "inserted").inc()
    after = _val("eve_killmap_kills_processed_total", labels)
    assert after == before + 1


def test_attackers_inserted_counter_increments_by_amount():
    before = _val("eve_killmap_attackers_inserted_total") or 0.0
    metrics.attackers_inserted.inc(5)
    assert _val("eve_killmap_attackers_inserted_total") == before + 5


def test_cache_invalidation_counter_is_labeled():
    labels = {"target": "system_rankings", "result": "success"}
    before = _val("eve_killmap_cache_invalidations_published_total", labels) or 0.0
    metrics.cache_invalidations_published.labels("system_rankings", "success").inc()
    assert _val("eve_killmap_cache_invalidations_published_total", labels) == before + 1


def test_gauge_can_be_set():
    metrics.live_sequence.set(12345)
    assert _val("eve_killmap_live_sequence") == 12345


def test_start_metrics_server_disabled_is_noop(tmp_path):
    # metrics.enabled defaults to False -> must not bind a socket or raise.
    cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    assert cfg.metrics.enabled is False
    metrics.start_metrics_server(cfg)
    assert metrics._started is False


def test_entities_resolved_counter_is_labeled():
    labels = {"kind": "character", "outcome": "resolved"}
    before = _val("eve_killmap_entities_resolved_total", labels) or 0.0
    metrics.entities_resolved.labels("character", "resolved").inc()
    assert _val("eve_killmap_entities_resolved_total", labels) == before + 1


def test_wars_pending_gauge_can_be_set():
    metrics.wars_pending.set(42)
    assert _val("eve_killmap_wars_pending") == 42


def test_entity_backlog_depth_gauge_can_be_set():
    metrics.entity_backlog_depth.set(3)
    assert _val("eve_killmap_entity_backlog_depth") == 3


def test_facets_written_counter_is_labeled():
    labels = {"kind": "character"}
    before = _val("eve_killmap_facets_written_total", labels) or 0.0
    metrics.facets_written.labels("character").inc(3)
    assert _val("eve_killmap_facets_written_total", labels) == before + 3


def test_corporations_refreshed_counter_is_labeled():
    labels = {"outcome": "active"}
    before = _val("eve_killmap_corporations_refreshed_total", labels) or 0.0
    metrics.corporations_refreshed.labels("active").inc()
    assert _val("eve_killmap_corporations_refreshed_total", labels) == before + 1


def test_corporations_pending_gauge_can_be_set():
    metrics.corporations_pending.set(7)
    assert _val("eve_killmap_corporations_pending") == 7


def test_zkb_written_counter_increments():
    before = _val("eve_killmap_zkb_written_total") or 0.0
    metrics.zkb_written.inc()
    assert _val("eve_killmap_zkb_written_total") == before + 1


def test_refresh_step_runs_counter_is_labeled():
    labels = {"cadence": "fast", "step": "rollups", "result": "skipped"}
    before = _val("eve_killmap_refresh_step_runs_total", labels) or 0.0
    metrics.refresh_step_runs.labels("fast", "rollups", "skipped").inc()
    assert _val("eve_killmap_refresh_step_runs_total", labels) == before + 1


def test_refresh_step_duration_histogram_is_labeled():
    labels = {"cadence": "slow", "step": "leaderboards"}
    before = _val("eve_killmap_refresh_step_duration_seconds_count", labels) or 0.0
    metrics.refresh_step_duration_seconds.labels("slow", "leaderboards").observe(2.5)
    assert _val("eve_killmap_refresh_step_duration_seconds_count", labels) == before + 1


def test_rollup_metrics():
    before = _val("eve_killmap_rollup_days_rolled_total") or 0.0
    metrics.rollup_days_rolled.inc()
    assert _val("eve_killmap_rollup_days_rolled_total") == before + 1
    metrics.rollup_watermark_timestamp.set(1_700_000_000)
    assert _val("eve_killmap_rollup_watermark_timestamp_seconds") == 1_700_000_000
    # the pre-rename names must be gone (stale series would mislead the dashboard)
    assert not hasattr(metrics, "entity_rollup_days_rolled")
    assert not hasattr(metrics, "entity_rollup_watermark_timestamp")


def test_leaderboard_metrics_are_labeled_by_window():
    labels = {"window": "year", "result": "success"}
    before = _val("eve_killmap_leaderboard_computations_total", labels) or 0.0
    metrics.leaderboard_computations.labels("year", "success").inc()
    assert _val("eve_killmap_leaderboard_computations_total", labels) == before + 1
    metrics.leaderboard_compute_seconds.labels("year").observe(3.0)
    assert _val("eve_killmap_leaderboard_compute_seconds_count", {"window": "year"}) >= 1
    metrics.leaderboard_last_success_timestamp.labels("all").set(1_700_000_000)
    assert (
        _val("eve_killmap_leaderboard_last_success_timestamp_seconds", {"window": "all"})
        == 1_700_000_000
    )
    metrics.leaderboard_rows.labels("year").set(42)
    assert _val("eve_killmap_leaderboard_rows", {"window": "year"}) == 42
