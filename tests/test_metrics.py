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
