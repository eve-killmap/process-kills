# process-kills

The killmail processing service behind [EVE Killmap](https://eve-killmap.com). It listens to zKillboard's live feed, fetches each killmail from ESI, and writes it to the shared PostgreSQL database. It also performs cross-checking against zKillboard's per-day totals to backfill anything missed, resolves entity names, and maintains the derived tables (kill facets, daily rollups, leaderboards and materialized views) that the [backend](https://github.com/eve-killmap/backend) serves to the [frontend](https://github.com/eve-killmap/frontend).

Requires Python 3.12+ and PostgreSQL. Redis is optional. Install `requirements.txt`, copy `.env.example` to `.env` and fill in `DATABASE_URL` and the other values, optionally copy `config.example.yml` to `config.yml`, then start the service with `python main.py`. It creates its schema on startup and runs until it receives SIGINT or SIGTERM. The test suite (`requirements-dev.txt`, `python -m pytest`) needs no network, credentials, or database.

## Bugs and feature requests

Please report bugs and request features in the [frontend repository](https://github.com/eve-killmap/frontend/issues), which is the main entry point for the project. Open an issue here only if you are sure the problem originates within this project.
