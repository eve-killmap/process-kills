-- Kills table to store raw killmail data

CREATE TABLE IF NOT EXISTS kills (
    killmail_id BIGINT PRIMARY KEY,
    killmail_hash VARCHAR(64) NOT NULL,
    killmail_time TIMESTAMPTZ NOT NULL,
    solar_system_id INTEGER NOT NULL,
    position_x DOUBLE PRECISION NOT NULL,
    position_y DOUBLE PRECISION NOT NULL,
    position_z DOUBLE PRECISION NOT NULL,
    victim_character_id BIGINT,
    victim_corporation_id INTEGER,
    victim_alliance_id INTEGER,
    victim_faction_id INTEGER,
    victim_damage_taken BIGINT NOT NULL,
    victim_ship_type_id INTEGER NOT NULL,
    war_id BIGINT,
    inserted_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kills_system ON kills (solar_system_id);
CREATE INDEX IF NOT EXISTS idx_kills_time ON kills (killmail_time);
CREATE INDEX IF NOT EXISTS idx_kills_system_time ON kills (solar_system_id, killmail_time);
CREATE INDEX IF NOT EXISTS idx_kills_system_inserted ON kills (solar_system_id, inserted_time);

-- Attackers table to store information about attackers in each killmail

CREATE TABLE IF NOT EXISTS kill_attackers (
    killmail_id BIGINT NOT NULL REFERENCES kills(killmail_id) ON DELETE CASCADE,
    attacker_index SMALLINT NOT NULL,
    character_id BIGINT,
    corporation_id INTEGER,
    alliance_id INTEGER,
    faction_id INTEGER,
    ship_type_id INTEGER,
    weapon_type_id INTEGER,
    damage_done INTEGER NOT NULL,
    final_blow BOOLEAN NOT NULL,
    security_status NUMERIC(4,2) NOT NULL,
    PRIMARY KEY (killmail_id, attacker_index)
);

CREATE INDEX IF NOT EXISTS idx_attackers_killmail ON kill_attackers (killmail_id);

-- Table to store killmails without position data

CREATE TABLE IF NOT EXISTS kills_no_positions (
    killmail_id BIGINT PRIMARY KEY,
    killmail_hash VARCHAR(64) NOT NULL,
    killmail_time TIMESTAMPTZ NOT NULL,
    last_checked TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_knp_time ON kills_no_positions (killmail_time);
CREATE INDEX IF NOT EXISTS idx_knp_last_checked ON kills_no_positions (last_checked);

-- Processed data table to track the processing status of killmails by date

CREATE TABLE IF NOT EXISTS processed_data (
    date VARCHAR(10) PRIMARY KEY,
    total_kills INTEGER NOT NULL,
    processed_kills INTEGER NOT NULL DEFAULT 0,
    no_position_kills INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_processed_data_data ON processed_data (date);

-- Table to store the current live sequence

CREATE TABLE IF NOT EXISTS live_state (
    sequence BIGINT NOT NULL
);

-- Materialized view for top 50 systems (all-time)

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kills_per_system_top AS
SELECT
    solar_system_id,
    COUNT(*) AS kill_count
FROM kills
GROUP BY solar_system_id
ORDER BY kill_count DESC
LIMIT 50;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kills_per_system_top_system
    ON mv_kills_per_system_top (solar_system_id);

-- Materialized view for top 50 systems (last 24h)

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kills_per_system_top_24h AS
SELECT
    solar_system_id,
    COUNT(*) AS kill_count
FROM kills
WHERE killmail_time >= NOW() - INTERVAL '24 hours'
GROUP BY solar_system_id
ORDER BY kill_count DESC
LIMIT 50;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kills_per_system_top_24h_system
    ON mv_kills_per_system_top_24h (solar_system_id);

-- Materialized view for top 50 systems (last 7d)

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kills_per_system_top_7d AS
SELECT
    solar_system_id,
    COUNT(*) AS kill_count
FROM kills
WHERE killmail_time >= NOW() - INTERVAL '7 days'
GROUP BY solar_system_id
ORDER BY kill_count DESC
LIMIT 50;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kills_per_system_top_7d_system
    ON mv_kills_per_system_top_7d (solar_system_id);

-- Materialized view for top 50 systems (last 30d)

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kills_per_system_top_30d AS
SELECT
    solar_system_id,
    COUNT(*) AS kill_count
FROM kills
WHERE killmail_time >= NOW() - INTERVAL '30 days'
GROUP BY solar_system_id
ORDER BY kill_count DESC
LIMIT 50;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kills_per_system_top_30d_system
    ON mv_kills_per_system_top_30d (solar_system_id);

-- Materialized view for top 50 systems (last 6m)

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kills_per_system_top_6m AS
SELECT
    solar_system_id,
    COUNT(*) AS kill_count
FROM kills
WHERE killmail_time >= NOW() - INTERVAL '6 months'
GROUP BY solar_system_id
ORDER BY kill_count DESC
LIMIT 50;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kills_per_system_top_6m_system
    ON mv_kills_per_system_top_6m (solar_system_id);

-- Materialized view for top 50 systems (last 1y)

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kills_per_system_top_1y AS
SELECT
    solar_system_id,
    COUNT(*) AS kill_count
FROM kills
WHERE killmail_time >= NOW() - INTERVAL '1 year'
GROUP BY solar_system_id
ORDER BY kill_count DESC
LIMIT 50;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kills_per_system_top_1y_system
    ON mv_kills_per_system_top_1y (solar_system_id);

-- Materialized view for bottom 50 systems (all-time)

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kills_per_system_bottom AS
SELECT
    solar_system_id,
    COUNT(*) AS kill_count
FROM kills
WHERE solar_system_id < 32000001
GROUP BY solar_system_id
ORDER BY kill_count ASC
LIMIT 50;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kills_per_system_bottom_system
    ON mv_kills_per_system_bottom (solar_system_id);

-- Materialized view for farthest kill 

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_farthest_kill_per_system AS
SELECT
    solar_system_id,
    ROUND(SQRT(MAX(position_x^2 + position_y^2 + position_z^2))) AS farthest_kill
FROM kills
GROUP BY solar_system_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_farthest_kill_per_system_system
    ON mv_farthest_kill_per_system (solar_system_id);

-- Entity reference tables (resolved at ingestion). NULL name = tombstone
-- (resolved, but ESI had no answer). Row existence stops us retrying forever.

CREATE TABLE IF NOT EXISTS characters (
    character_id BIGINT PRIMARY KEY,
    resolved_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name         TEXT
);

CREATE TABLE IF NOT EXISTS corporations (
    corporation_id INTEGER PRIMARY KEY,
    resolved_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name           TEXT,
    ticker         TEXT
);

CREATE TABLE IF NOT EXISTS alliances (
    alliance_id INTEGER PRIMARY KEY,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name        TEXT,
    ticker      TEXT
);

CREATE TABLE IF NOT EXISTS factions (
    faction_id INTEGER PRIMARY KEY,
    name       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wars (
    war_id                   BIGINT PRIMARY KEY,
    declared                 TIMESTAMPTZ,
    started                  TIMESTAMPTZ,
    finished                 TIMESTAMPTZ,
    retracted                TIMESTAMPTZ,
    mutual                   BOOLEAN,
    open_for_allies          BOOLEAN,
    aggressor_corporation_id INTEGER,
    aggressor_alliance_id    INTEGER,
    aggressor_ships_killed   INTEGER,
    aggressor_isk_destroyed  DOUBLE PRECISION,
    defender_corporation_id  INTEGER,
    defender_alliance_id     INTEGER,
    defender_ships_killed    INTEGER,
    defender_isk_destroyed   DOUBLE PRECISION,
    ally_corporation_ids     INTEGER[],
    ally_alliance_ids        INTEGER[],
    resolved_at              TIMESTAMPTZ,
    refresh_after            TIMESTAMPTZ DEFAULT NOW()
);

-- Only non-terminal wars are indexed (immutable predicate, no NOW()).
CREATE INDEX IF NOT EXISTS idx_wars_refresh ON wars (refresh_after)
    WHERE refresh_after IS NOT NULL;

CREATE TABLE IF NOT EXISTS entity_resolve_backlog (
    killmail_id BIGINT PRIMARY KEY,
    queued_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts    SMALLINT NOT NULL DEFAULT 0
);
