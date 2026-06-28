"""Application configuration.

Configuration is resolved with the following precedence (lowest to highest):

  1. Defaults defined in this module (they reproduce the original behavior, so
     both ``config.yml`` and ``.env`` are optional).
  2. Values from the YAML config file (``config.yml`` by default), for
     non-secret, operator-tweakable settings.
  3. Environment variables / ``.env``, for secrets and machine-specific
     overrides (``DATABASE_URL``, ``REDIS_URL``, ``USER_AGENT``, ``LOG_FILE``,
     ``LOG_LEVEL``, ``DATA_DIR``).

Secrets (e.g. ``DATABASE_URL``) live only in the environment and are never
written to the log.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config.yml"

VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

# True constants (data-contract values, not operator-tweakable):

# Date CCP began recording positional data in killmails (YYYYMMDD). Kills before
# this never carry a position, so they are never expected to gain one.
BEGIN_DATE = 20151103

# Generic, PII-free default. Operators must override USER_AGENT in .env with a
# real contact per CCP's API rules.
_DEFAULT_USER_AGENT = (
    "eve-killmap:process-kills/1.0.0 (+https://github.com/eve-killmap/process-kills)"
)

_DEFAULT_REDIS_URL = "redis://localhost:6379"

_DEFAULT_SOURCES = {
    "r2z2_sequence_url": "https://r2z2.zkillboard.com/ephemeral/sequence.json",
    "r2z2_ephemeral_url": "https://r2z2.zkillboard.com/ephemeral/{sequence}.json",
    "esi_killmail_url": "https://esi.evetech.net/killmails/{killmail_id}/{killmail_hash}/",
    "zkb_totals_url": "https://r2z2.zkillboard.com/history/totals.json",
    "zkb_day_url": "https://r2z2.zkillboard.com/history/{date}.json",
}

_DEFAULT_BACKFILL = {
    "killmail_base_url": "https://data.everef.net/killmails",
    "day_path": "/{year}/killmails-{date}.tar.bz2",
}


class ConfigError(Exception):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    file: Path
    max_bytes: int
    backup_count: int


@dataclass(frozen=True)
class SourcesConfig:
    r2z2_sequence_url: str
    r2z2_ephemeral_url: str
    esi_killmail_url: str
    zkb_totals_url: str
    zkb_day_url: str


@dataclass(frozen=True)
class EsiConfig:
    rate_limit: int
    rate_limit_window: int


@dataclass(frozen=True)
class LiveConfig:
    poll_delay: float
    retry_delay: float


@dataclass(frozen=True)
class RecheckConfig:
    enabled: bool
    interval_seconds: int
    batch_limit: int


@dataclass(frozen=True)
class CrosscheckConfig:
    hour: int


@dataclass(frozen=True)
class MaintenanceConfig:
    day: int
    hour: int
    mv_refresh_hours: list[int]


@dataclass(frozen=True)
class StreamingConfig:
    stream_name: str
    stream_max_length: int
    discard_older_than: int


@dataclass(frozen=True)
class ProcessingConfig:
    batch_size: int


@dataclass(frozen=True)
class BackfillConfig:
    max_retries: int
    timeout: int
    sleep_between_retries: int
    killmail_base_url: str
    day_path: str


@dataclass(frozen=True)
class Paths:
    base_dir: Path
    data_dir: Path


@dataclass(frozen=True)
class Config:
    paths: Paths
    logging: LoggingConfig
    sources: SourcesConfig
    esi: EsiConfig
    live: LiveConfig
    recheck: RecheckConfig
    crosscheck: CrosscheckConfig
    maintenance: MaintenanceConfig
    streaming: StreamingConfig
    processing: ProcessingConfig
    backfill: BackfillConfig
    user_agent: str
    database_url: str | None
    redis_url: str


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    section = data.get(name) or {}
    if not isinstance(section, dict):
        raise ConfigError(f"Config section '{name}' must be a mapping")
    return section


def _as_int(
    value: Any, label: str, *, minimum: int | None = None, maximum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"Config value '{label}' must be an integer, got {value!r}")
    if minimum is not None and value < minimum:
        raise ConfigError(f"Config value '{label}' must be >= {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ConfigError(f"Config value '{label}' must be <= {maximum}, got {value}")
    return value


def _as_int_list(
    value: Any, label: str, *, minimum: int | None = None, maximum: int | None = None
) -> list[int]:
    if not isinstance(value, list):
        raise ConfigError(f"Config value '{label}' must be a list of integers")
    return [
        _as_int(item, f"{label}[{i}]", minimum=minimum, maximum=maximum)
        for i, item in enumerate(value)
    ]


def _as_positive_float(value: Any, label: str) -> float:
    # Accept ints, floats, and numeric strings. PyYAML parses exponent literals
    # without a sign (e.g. "1.5e11") as strings, so coerce here.
    if isinstance(value, bool):
        raise ConfigError(f"Config value '{label}' must be a number, got {value!r}")
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ConfigError(
            f"Config value '{label}' must be a number, got {value!r}"
        ) from None
    if result <= 0:
        raise ConfigError(f"Config value '{label}' must be > 0, got {result}")
    return result


def _load_yaml(yaml_path: Path) -> dict[str, Any]:
    if not yaml_path.exists():
        return {}
    try:
        loaded = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"Could not parse config file {yaml_path}: {exc}") from exc
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ConfigError(f"Config file {yaml_path} must contain a top-level mapping")
    return loaded


def load_config(
    yaml_path: Path | None = None,
    env: Mapping[str, str] | None = None,
    base_dir: Path | None = None,
) -> Config:
    """Build a :class:`Config` from defaults, the YAML file, and the environment.

    Raises :class:`ConfigError` for missing/invalid values. ``DATABASE_URL`` is
    not required here; validate it lazily via :func:`require_database_url`.
    """
    base_dir = base_dir or BASE_DIR
    env = os.environ if env is None else env
    data = _load_yaml(yaml_path or DEFAULT_CONFIG_PATH)

    log_cfg = _section(data, "logging")
    src_cfg = _section(data, "sources")
    esi_cfg = _section(data, "esi")
    live_cfg = _section(data, "live")
    recheck_cfg = _section(data, "recheck")
    cross_cfg = _section(data, "crosscheck")
    maint_cfg = _section(data, "maintenance")
    stream_cfg = _section(data, "streaming")
    proc_cfg = _section(data, "processing")
    backfill_cfg = _section(data, "backfill")

    level = (env.get("LOG_LEVEL") or log_cfg.get("level") or "INFO").upper()
    if level not in VALID_LOG_LEVELS:
        raise ConfigError(
            f"Invalid log level {level!r}; expected one of {sorted(VALID_LOG_LEVELS)}"
        )

    log_file = Path(env.get("LOG_FILE") or log_cfg.get("file") or "eve-killmap.log")
    if not log_file.is_absolute():
        log_file = base_dir / log_file

    logging_config = LoggingConfig(
        level=level,
        file=log_file,
        max_bytes=_as_int(
            log_cfg.get("max_bytes", 10 * 1024 * 1024), "logging.max_bytes", minimum=1
        ),
        backup_count=_as_int(
            log_cfg.get("backup_count", 5), "logging.backup_count", minimum=0
        ),
    )

    sources_config = SourcesConfig(
        r2z2_sequence_url=src_cfg.get("r2z2_sequence_url")
        or _DEFAULT_SOURCES["r2z2_sequence_url"],
        r2z2_ephemeral_url=src_cfg.get("r2z2_ephemeral_url")
        or _DEFAULT_SOURCES["r2z2_ephemeral_url"],
        esi_killmail_url=src_cfg.get("esi_killmail_url")
        or _DEFAULT_SOURCES["esi_killmail_url"],
        zkb_totals_url=src_cfg.get("zkb_totals_url")
        or _DEFAULT_SOURCES["zkb_totals_url"],
        zkb_day_url=src_cfg.get("zkb_day_url") or _DEFAULT_SOURCES["zkb_day_url"],
    )

    esi_config = EsiConfig(
        rate_limit=_as_int(
            esi_cfg.get("rate_limit", 3600), "esi.rate_limit", minimum=1
        ),
        rate_limit_window=_as_int(
            esi_cfg.get("rate_limit_window", 900), "esi.rate_limit_window", minimum=1
        ),
    )

    live_config = LiveConfig(
        poll_delay=_as_positive_float(
            live_cfg.get("poll_delay", 0.1), "live.poll_delay"
        ),
        retry_delay=_as_positive_float(
            live_cfg.get("retry_delay", 6.0), "live.retry_delay"
        ),
    )

    recheck_config = RecheckConfig(
        enabled=bool(recheck_cfg.get("enabled", False)),
        interval_seconds=_as_int(
            recheck_cfg.get("interval_seconds", 3600),
            "recheck.interval_seconds",
            minimum=1,
        ),
        batch_limit=_as_int(
            recheck_cfg.get("batch_limit", 500), "recheck.batch_limit", minimum=1
        ),
    )

    crosscheck_config = CrosscheckConfig(
        hour=_as_int(
            cross_cfg.get("hour", 1), "crosscheck.hour", minimum=0, maximum=23
        ),
    )

    maintenance_config = MaintenanceConfig(
        day=_as_int(maint_cfg.get("day", 6), "maintenance.day", minimum=0, maximum=6),
        hour=_as_int(
            maint_cfg.get("hour", 4), "maintenance.hour", minimum=0, maximum=23
        ),
        mv_refresh_hours=_as_int_list(
            maint_cfg.get("mv_refresh_hours", [0, 6, 12, 18]),
            "maintenance.mv_refresh_hours",
            minimum=0,
            maximum=23,
        ),
    )

    streaming_config = StreamingConfig(
        stream_name=stream_cfg.get("stream_name", "kills:live"),
        stream_max_length=_as_int(
            stream_cfg.get("stream_max_length", 1000),
            "streaming.stream_max_length",
            maximum=5000,
        ),
        discard_older_than=_as_int(
            stream_cfg.get("discard_older_than", 7200), "streaming.discard_older_than"
        ),
    )

    processing_config = ProcessingConfig(
        batch_size=_as_int(
            proc_cfg.get("batch_size", 1000), "processing.batch_size", minimum=1
        ),
    )

    backfill_config = BackfillConfig(
        max_retries=_as_int(
            backfill_cfg.get("max_retries", 5), "backfill.max_retries", minimum=1
        ),
        timeout=_as_int(backfill_cfg.get("timeout", 30), "backfill.timeout", minimum=1),
        sleep_between_retries=_as_int(
            backfill_cfg.get("sleep_between_retries", 2),
            "backfill.sleep_between_retries",
            minimum=0,
        ),
        killmail_base_url=backfill_cfg.get("killmail_base_url")
        or _DEFAULT_BACKFILL["killmail_base_url"],
        day_path=backfill_cfg.get("day_path") or _DEFAULT_BACKFILL["day_path"],
    )

    data_dir = Path(env.get("DATA_DIR") or base_dir / "data")
    paths = Paths(base_dir=base_dir, data_dir=data_dir)

    return Config(
        paths=paths,
        logging=logging_config,
        sources=sources_config,
        esi=esi_config,
        live=live_config,
        recheck=recheck_config,
        crosscheck=crosscheck_config,
        maintenance=maintenance_config,
        streaming=streaming_config,
        processing=processing_config,
        backfill=backfill_config,
        user_agent=env.get("USER_AGENT") or _DEFAULT_USER_AGENT,
        database_url=env.get("DATABASE_URL") or None,
        redis_url=env.get("REDIS_URL") or _DEFAULT_REDIS_URL,
    )


def require_database_url(config: Config) -> str:
    """Return the database URL or raise :class:`ConfigError` if unset."""
    if not config.database_url:
        raise ConfigError("DATABASE_URL is required but not set (define it in .env)")
    return config.database_url


def ensure_data_dirs(config: Config) -> None:
    """Create the working data directory if it does not yet exist."""
    config.paths.data_dir.mkdir(parents=True, exist_ok=True)


def setup_logging(config: Config) -> None:
    """Configure root logging from ``config`` (idempotent)."""
    root_logger = logging.getLogger()
    root_logger.setLevel(config.logging.level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    )

    config.logging.file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        config.logging.file,
        maxBytes=config.logging.max_bytes,
        backupCount=config.logging.backup_count,
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


# Module-level singleton: loaded once from .env + config.yml for normal app use.
# Tests should call load_config() directly with explicit arguments instead.
load_dotenv(BASE_DIR / ".env")
config = load_config()
