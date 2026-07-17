# process-kills

This project is part of the larger [EVE Killmap](https://eve-killmap.com) project
and is the ingestion service that keeps the killmail database current. It feeds
the [FastAPI backend](https://github.com/eve-killmap/backend) (which serves the
[frontend client](https://github.com/eve-killmap/frontend)) and shares its kill
schema with them; type/name metadata for those kills is produced separately by
[process-sde](https://github.com/eve-killmap/process-sde).

It runs as a long-lived asyncio service that:

- **listens** to zKillboard's R2Z2 live ephemeral feed and inserts each killmail
  (kills with a position go to `kills` + `kill_attackers`; kills without a
  position are recorded in `kills_no_positions`),
- **publishes** freshly inserted kills to a Redis stream (`kills:live`) for the
  backend to tail,
- **cross-checks** daily against zKillboard per-day totals and backfills any
  missing killmails via ESI,
- **maintains** the database (weekly `CLUSTER` + `ANALYZE`, periodic materialized
  view refreshes).

All ESI / zKillboard traffic is funneled through a single rate-limited async
client that respects ESI's token budget.

## Requirements

- Python 3.12+
- PostgreSQL (the kills database)
- Redis (optional; only for live streaming to the backend)
- Prometheus (optional; scrapes the metrics endpoint when `metrics.enabled`)

## Setup

```sh
python -m venv venv
venv/Scripts/activate            # Windows; use source venv/bin/activate on POSIX
pip install -r requirements.txt  # add -dev for the test suite: requirements-dev.txt

cp .env.example .env             # then edit secrets (DATABASE_URL, REDIS_URL, USER_AGENT, ...)
cp config.example.yml config.yml # optional; omit to use built-in defaults
```

## Running

```sh
python main.py        # start the ingestion service (live + cross-check + maintenance)
python main.py --help # show usage and exit
```

The service creates its schema on startup and runs until it receives `SIGINT`
or `SIGTERM`. Logs go to the rotating file at `LOG_FILE` (default
`./process-kills.log`) and to stdout.

## Configuration

Settings are resolved with the precedence **code defaults < `config.yml` <
environment / `.env`**. Because every value has a built-in default that
reproduces the original behavior, both `config.yml` and `.env` are optional
(though `DATABASE_URL` is required to actually run the service).

### `.env`: secrets and machine/deployment-specific values

| Variable       | Purpose                                                       |
| -------------- | ------------------------------------------------------------- |
| `DATABASE_URL` | PostgreSQL connection string (required to run).               |
| `REDIS_URL`    | Redis connection string for the live stream (default `redis://localhost:6379`). |
| `USER_AGENT`   | Contact-bearing User-Agent for ESI/zKillboard (CCP rule).     |
| `LOG_FILE`     | Log file path (default `./process-kills.log`).                |
| `LOG_LEVEL`    | Optional override of `logging.level` from `config.yml`.       |
| `DATA_DIR`     | Working directory for the backfill script (default `./data`). |

Secrets are never written to the log.

### `config.yml`: non-secret, operator-tweakable settings

Sections: `logging` (level, rotation), `sources` (upstream endpoint URLs), `esi`
(token-bucket rate limit), `live` (poll/retry delays), `recheck` (the optional
no-position rechecking pass; see below), `crosscheck` (daily run hour),
`maintenance` (weekly cluster day/hour, materialized-view refresh hours),
`processing` (batch size), `backfill` (download settings for the standalone
script), and `metrics` (Prometheus exporter; see below). See
[`config.example.yml`](config.example.yml) for the full documented set. Invalid
or out-of-range values fail fast with a clear `ConfigError`.

## Metrics (optional)

With `metrics.enabled: true`, the service exposes a Prometheus scrape endpoint at
`http://<metrics.host>:<metrics.port>/metrics` (default `0.0.0.0:9108`) for
Prometheus to pull. It is **disabled by default** (enabling opens a listening
socket). All metrics are **application-level**: throughput
(`eve_killmap_kills_processed_total`), freshness (`eve_killmap_killmail_lag_seconds`),
ESI/zKillboard dependency health, job status (cross-check / recheck / maintenance),
streaming + cache-invalidation, and an `eve_killmap_errors_total` catch-all,
plus the client's standard process/GC collectors (Linux only). PostgreSQL
internals are intentionally left to a separate DB exporter. Metric names are
prefixed `eve_killmap_`; distinguish services by the Prometheus scrape target's
`job`/`instance` labels.

## No-position rechecking (optional)

Killmails for NPC/structure deaths often arrive without a position and are stored
in `kills_no_positions`. With `recheck.enabled: true`, the service periodically
re-fetches those killmails from ESI in case they have since gained a position,
promoting any that do into the main `kills` table. This pass is **disabled by
default**; no-position kills are recorded either way.

## Entity enrichment

New kills have their entity IDs (characters, corporations, alliances) resolved to
names/tickers via ESI **at ingestion** and stored in reference tables
(`characters`, `corporations`, `alliances`), so the backend reads
names with pure SQL instead of calling ESI at request time. Faction names live in
a separate `factions` table that a background scheduler fully prepopulates on a
slow cadence (`factions.refresh_days`), so factions are never resolved per kill.
Names are resolved inline (before a kill is streamed); a resolve that times out is
queued in `entity_resolve_backlog` and retried by a background drain. Characters,
corporations, and alliances are all re-resolved only when seen again after
`entities.refresh_after_days`.

**War info** (`wars` table) is resolved by a separate background scheduler because
the ESI war endpoint is rate limited: a kill only writes a war *stub*, which the
scheduler later fills in. Terminal (finished) wars are never re-fetched.

### Backfilling historical entities

`entities_backfill.py` resolves names for all pre-existing kills. It is idempotent,
resumable, and safe to run alongside the live service (name endpoints are not rate
limited):

```sh
python entities_backfill.py
```

Historical **wars** backfill themselves: seed one stub per distinct `war_id` and
let the war scheduler drain it. In a `psql` shell:

```sql
INSERT INTO wars (war_id)
SELECT DISTINCT war_id FROM kills WHERE war_id IS NOT NULL
ON CONFLICT (war_id) DO NOTHING;
```

## Backfill (standalone)

[`backfill.py`](backfill.py) is a **standalone, archival** script that originally
seeded the database with killmails from *before* this service existed, in one
bulk pass over [EVERef](https://everef.net)'s daily killmail archives. It is not
part of the live service and is not run routinely. The live listener and
cross-checker keep the database current going forward, but it is kept for
archival/reproducibility purposes. It shares this project's `config` and `db`
modules and writes downloaded archives under `DATA_DIR`.

```sh
python backfill.py
```

## Testing

```sh
pip install -r requirements-dev.txt
python -m pytest
```

The suite covers configuration loading/precedence/validation (including the
recheck toggle), `parse_kill`'s position handling, and the pure scheduling/date
helpers. It needs no network access, credentials, or database.
