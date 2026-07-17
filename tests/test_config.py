import logging
from pathlib import Path

import pytest

from config import (
    ConfigError,
    load_config,
    require_database_url,
    setup_logging,
)


def write_yaml(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "config.yml"
    path.write_text(text, encoding="utf-8")
    return path


def test_defaults_used_when_no_yaml_and_empty_env(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "missing.yml", env={}, base_dir=tmp_path)

    assert cfg.logging.level == "INFO"
    assert cfg.logging.backup_count == 5
    assert cfg.esi.rate_limit == 3600
    assert cfg.esi.rate_limit_window == 900
    assert cfg.live.poll_delay == pytest.approx(0.1)
    assert cfg.live.retry_delay == pytest.approx(6.0)
    assert cfg.crosscheck.hour == 1
    assert cfg.maintenance.day == 6
    assert cfg.maintenance.hour == 4
    assert cfg.maintenance.mv_refresh_hours == [0, 6, 12, 18]
    assert cfg.streaming.stream_name == "kills:live"
    assert cfg.streaming.stream_max_length == 1000
    assert cfg.streaming.discard_older_than == 7200
    assert cfg.streaming.invalidate_channel == "cache:invalidate"
    assert cfg.processing.batch_size == 1000
    assert cfg.recheck.enabled is False
    assert cfg.database_url is None
    assert cfg.redis_url == "redis://localhost:6379"


def test_recheck_disabled_by_default_and_toggleable(tmp_path):
    default_cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    assert default_cfg.recheck.enabled is False

    yaml_path = write_yaml(tmp_path, "recheck:\n  enabled: true\n  batch_limit: 50\n")
    enabled_cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert enabled_cfg.recheck.enabled is True
    assert enabled_cfg.recheck.batch_limit == 50
    assert enabled_cfg.recheck.interval_seconds == 3600  # untouched default


def test_data_dir_defaults_under_base_dir(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    assert cfg.paths.data_dir == tmp_path / "data"


def test_yaml_overrides_defaults(tmp_path):
    yaml_path = write_yaml(
        tmp_path,
        """
        logging:
          level: WARNING
          backup_count: 9
        esi:
          rate_limit: 1200
        crosscheck:
          hour: 5
        processing:
          batch_size: 250
        """,
    )

    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)

    assert cfg.logging.level == "WARNING"
    assert cfg.logging.backup_count == 9
    assert cfg.esi.rate_limit == 1200
    assert cfg.crosscheck.hour == 5
    assert cfg.processing.batch_size == 250
    # Untouched defaults remain
    assert cfg.esi.rate_limit_window == 900


def test_env_overrides_yaml_and_defaults(tmp_path):
    yaml_path = write_yaml(tmp_path, "logging:\n  level: WARNING\n")
    env = {
        "LOG_LEVEL": "DEBUG",
        "DATA_DIR": str(tmp_path / "custom_out"),
        "DATABASE_URL": "postgresql://u:p@host/db",
        "REDIS_URL": "redis://example:6380",
        "USER_AGENT": "test-agent/1.0",
        "LOG_FILE": "custom.log",
    }

    cfg = load_config(yaml_path=yaml_path, env=env, base_dir=tmp_path)

    assert cfg.logging.level == "DEBUG"
    assert cfg.paths.data_dir == tmp_path / "custom_out"
    assert cfg.database_url == "postgresql://u:p@host/db"
    assert cfg.redis_url == "redis://example:6380"
    assert cfg.user_agent == "test-agent/1.0"
    assert cfg.logging.file == tmp_path / "custom.log"


def test_env_defaults_to_os_environ_when_not_passed(tmp_path, monkeypatch):
    # env=None means "use the real process environment" (so .env is honored);
    # only an explicit dict (even empty) opts out.
    monkeypatch.setenv("DATABASE_URL", "postgresql://from/environ")
    cfg = load_config(yaml_path=tmp_path / "x.yml", base_dir=tmp_path)
    assert cfg.database_url == "postgresql://from/environ"


def test_default_user_agent_is_pii_free(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    assert "@" not in cfg.user_agent
    assert "process-kills" in cfg.user_agent


def test_log_level_is_case_insensitive(tmp_path):
    cfg = load_config(
        yaml_path=tmp_path / "x.yml", env={"LOG_LEVEL": "debug"}, base_dir=tmp_path
    )
    assert cfg.logging.level == "DEBUG"


def test_invalid_log_level_raises(tmp_path):
    yaml_path = write_yaml(tmp_path, "logging:\n  level: CHATTY\n")
    with pytest.raises(ConfigError, match="log level"):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_malformed_yaml_raises_config_error(tmp_path):
    yaml_path = write_yaml(tmp_path, "logging: : :\n  - broken\n")
    with pytest.raises(ConfigError):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_yaml_top_level_must_be_mapping(tmp_path):
    yaml_path = write_yaml(tmp_path, "- just\n- a\n- list\n")
    with pytest.raises(ConfigError, match="mapping"):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_poll_delay_accepts_unsigned_exponent_strings(tmp_path):
    # PyYAML parses `1e-1` (no sign in the exponent) as a string, so the loader
    # must coerce numeric strings to floats.
    yaml_path = write_yaml(tmp_path, "live:\n  poll_delay: 1e-1\n")
    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert cfg.live.poll_delay == pytest.approx(0.1)


def test_non_numeric_poll_delay_raises(tmp_path):
    yaml_path = write_yaml(tmp_path, "live:\n  poll_delay: not-a-number\n")
    with pytest.raises(ConfigError, match="number"):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_crosscheck_hour_out_of_range_raises(tmp_path):
    yaml_path = write_yaml(tmp_path, "crosscheck:\n  hour: 24\n")
    with pytest.raises(ConfigError, match="crosscheck.hour"):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_maintenance_day_out_of_range_raises(tmp_path):
    yaml_path = write_yaml(tmp_path, "maintenance:\n  day: 7\n")
    with pytest.raises(ConfigError, match="maintenance.day"):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_mv_refresh_hours_must_be_int_list(tmp_path):
    yaml_path = write_yaml(tmp_path, "maintenance:\n  mv_refresh_hours: 6\n")
    with pytest.raises(ConfigError, match="mv_refresh_hours"):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_streaming_max_length_above_max_raises(tmp_path):
    yaml_path = write_yaml(tmp_path, "streaming:\n  stream_max_length: 10000\n")
    with pytest.raises(ConfigError, match="stream_max_length"):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_invalidate_channel_default_and_override(tmp_path):
    default_cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    assert default_cfg.streaming.invalidate_channel == "cache:invalidate"

    yaml_path = write_yaml(tmp_path, "streaming:\n  invalidate_channel: cache:flush\n")
    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert cfg.streaming.invalidate_channel == "cache:flush"


def test_metrics_defaults_and_override(tmp_path):
    default_cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    assert default_cfg.metrics.enabled is False
    assert default_cfg.metrics.host == "0.0.0.0"
    assert default_cfg.metrics.port == 9108

    yaml_path = write_yaml(
        tmp_path, "metrics:\n  enabled: true\n  host: 127.0.0.1\n  port: 9200\n"
    )
    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert cfg.metrics.enabled is True
    assert cfg.metrics.host == "127.0.0.1"
    assert cfg.metrics.port == 9200


def test_metrics_invalid_port_raises(tmp_path):
    yaml_path = write_yaml(tmp_path, "metrics:\n  port: 70000\n")
    with pytest.raises(ConfigError, match="metrics.port"):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_negative_batch_size_raises(tmp_path):
    yaml_path = write_yaml(tmp_path, "processing:\n  batch_size: 0\n")
    with pytest.raises(ConfigError, match="batch_size"):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_require_database_url_raises_when_missing(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    with pytest.raises(ConfigError, match="DATABASE_URL"):
        require_database_url(cfg)


def test_require_database_url_returns_value(tmp_path):
    cfg = load_config(
        yaml_path=tmp_path / "x.yml",
        env={"DATABASE_URL": "postgresql://u:p@h/db"},
        base_dir=tmp_path,
    )
    assert require_database_url(cfg) == "postgresql://u:p@h/db"


def test_setup_logging_applies_level(tmp_path):
    cfg = load_config(
        yaml_path=tmp_path / "x.yml",
        env={"LOG_LEVEL": "DEBUG", "LOG_FILE": str(tmp_path / "app.log")},
        base_dir=tmp_path,
    )
    root = logging.getLogger()
    original_level = root.level
    original_handlers = root.handlers[:]
    try:
        setup_logging(cfg)
        assert root.level == logging.DEBUG
    finally:
        for handler in root.handlers[:]:
            handler.close()
        root.handlers[:] = original_handlers
        root.setLevel(original_level)


def test_esi_entity_source_urls_have_defaults(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    assert cfg.sources.esi_names_url == "https://esi.evetech.net/universe/names/"
    assert cfg.sources.esi_corporation_url == "https://esi.evetech.net/corporations/{corporation_id}/"
    assert cfg.sources.esi_alliance_url == "https://esi.evetech.net/alliances/{alliance_id}/"
    assert cfg.sources.esi_factions_url == "https://esi.evetech.net/universe/factions/"
    assert cfg.sources.esi_war_url == "https://esi.evetech.net/wars/{war_id}/"
