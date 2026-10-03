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

CREATE INDEX IF NOT EXISTS idx_kills_time_covering
    ON kills (killmail_time) INCLUDE (killmail_id, solar_system_id);
CREATE INDEX IF NOT EXISTS idx_kills_system_time_covering
    ON kills (solar_system_id, killmail_time)
    INCLUDE (killmail_id, position_x, position_y, position_z, victim_ship_type_id);
CREATE INDEX IF NOT EXISTS idx_kills_system_inserted_covering
    ON kills (solar_system_id, inserted_time)
    INCLUDE (killmail_id, position_x, position_y, position_z, killmail_time, victim_ship_type_id);

ALTER TABLE kills SET (
    autovacuum_vacuum_insert_scale_factor = 0,
    autovacuum_vacuum_insert_threshold    = 250000,
    autovacuum_analyze_scale_factor       = 0,
    autovacuum_analyze_threshold          = 250000
);

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

ALTER TABLE kill_attackers SET (
    autovacuum_vacuum_insert_scale_factor = 0,
    autovacuum_vacuum_insert_threshold    = 2000000,
    autovacuum_analyze_scale_factor       = 0,
    autovacuum_analyze_threshold          = 2000000
);

CREATE TABLE IF NOT EXISTS zkb_metadata (
    killmail_id           BIGINT PRIMARY KEY REFERENCES kills(killmail_id) ON DELETE CASCADE,
    solar_system_id       INTEGER NOT NULL,
    killmail_time         TIMESTAMPTZ NOT NULL,
    fitted_value          DOUBLE PRECISION,
    dropped_value         DOUBLE PRECISION,
    destroyed_value       DOUBLE PRECISION,
    total_value           DOUBLE PRECISION,
    total_droppable_value DOUBLE PRECISION,
    npc                   BOOLEAN,
    solo                  BOOLEAN,
    awox                  BOOLEAN,
    labels                TEXT[]
);

CREATE INDEX IF NOT EXISTS idx_zkb_system_time ON zkb_metadata (solar_system_id, killmail_time);
CREATE INDEX IF NOT EXISTS idx_zkb_labels ON zkb_metadata USING gin (labels);

ALTER TABLE zkb_metadata SET (
    autovacuum_vacuum_insert_scale_factor = 0,
    autovacuum_vacuum_insert_threshold    = 250000,
    autovacuum_analyze_scale_factor       = 0,
    autovacuum_analyze_threshold          = 250000
);

CREATE TABLE IF NOT EXISTS kills_no_positions (
    killmail_id BIGINT PRIMARY KEY,
    killmail_hash VARCHAR(64) NOT NULL,
    killmail_time TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_knp_time ON kills_no_positions (killmail_time);

CREATE TABLE IF NOT EXISTS processed_data (
    date VARCHAR(10) PRIMARY KEY,
    total_kills INTEGER NOT NULL,
    processed_kills INTEGER NOT NULL DEFAULT 0,
    no_position_kills INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_processed_data_data ON processed_data (date);

CREATE TABLE IF NOT EXISTS live_state (
    sequence BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_kills_daily (
    solar_system_id INTEGER NOT NULL,
    day             DATE    NOT NULL,
    kill_count      INTEGER NOT NULL,
    PRIMARY KEY (solar_system_id, day) INCLUDE (kill_count)
);

CREATE INDEX IF NOT EXISTS idx_system_kills_daily_day
    ON system_kills_daily (day) INCLUDE (solar_system_id, kill_count);

ALTER TABLE system_kills_daily SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.005
);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kills_per_system AS
SELECT
    solar_system_id,
    SUM(kill_count) AS kill_count
FROM system_kills_daily
GROUP BY solar_system_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kills_per_system_system
    ON mv_kills_per_system (solar_system_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_farthest_kill_per_system AS
SELECT
    solar_system_id,
    ROUND(SQRT(MAX(position_x^2 + position_y^2 + position_z^2))) AS farthest_kill
FROM kills
GROUP BY solar_system_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_farthest_kill_per_system_system
    ON mv_farthest_kill_per_system (solar_system_id);

CREATE TABLE IF NOT EXISTS characters (
    character_id BIGINT PRIMARY KEY,
    resolved_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name         TEXT
);

CREATE TABLE IF NOT EXISTS corporations (
    corporation_id INTEGER PRIMARY KEY,
    resolved_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name           TEXT,
    ticker         TEXT,
    alliance_id    INTEGER,
    date_founded   TIMESTAMPTZ,
    member_count   INTEGER,
    refresh_after TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alliances (
    alliance_id  INTEGER PRIMARY KEY,
    resolved_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name         TEXT,
    ticker       TEXT,
    date_founded TIMESTAMPTZ
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

CREATE INDEX IF NOT EXISTS idx_wars_refresh ON wars (refresh_after)
    WHERE refresh_after IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_corporations_refresh ON corporations (refresh_after)
    WHERE refresh_after IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_corporations_alliance_id ON corporations (alliance_id)
    WHERE alliance_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_corporations_date_founded
    ON corporations (date_founded DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_alliances_date_founded
    ON alliances (date_founded DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS entity_resolve_backlog (
    killmail_id BIGINT PRIMARY KEY,
    queued_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts    SMALLINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS kill_facets (
    facet_kind      SMALLINT    NOT NULL,
    facet_value     BIGINT      NOT NULL,
    role            SMALLINT    NOT NULL,
    solar_system_id INTEGER     NOT NULL,
    killmail_time   TIMESTAMPTZ NOT NULL,
    killmail_id     BIGINT      NOT NULL,
    PRIMARY KEY (facet_kind, facet_value, role, solar_system_id, killmail_time, killmail_id)
);

CREATE INDEX IF NOT EXISTS idx_facet_kill
    ON kill_facets (killmail_id, facet_kind, facet_value, role);

ALTER TABLE kill_facets SET (
    autovacuum_vacuum_insert_scale_factor = 0,
    autovacuum_vacuum_insert_threshold    = 5000000,
    autovacuum_analyze_scale_factor       = 0,
    autovacuum_analyze_threshold          = 5000000
);

ALTER TABLE kill_facets ALTER COLUMN killmail_id SET (n_distinct = -0.06);

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_characters_name_nospace_trgm     ON characters   USING gin (replace(name,   ' ', '') gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_corporations_name_nospace_trgm   ON corporations USING gin (replace(name,   ' ', '') gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_corporations_ticker_nospace_trgm ON corporations USING gin (replace(ticker, ' ', '') gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_alliances_name_nospace_trgm      ON alliances    USING gin (replace(name,   ' ', '') gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_alliances_ticker_nospace_trgm    ON alliances    USING gin (replace(ticker, ' ', '') gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_factions_name_nospace_trgm       ON factions     USING gin (replace(name,   ' ', '') gin_trgm_ops);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ship_search AS
SELECT t.id AS type_id, t.name
FROM types t
WHERE EXISTS (
    SELECT 1 FROM kill_facets f
    WHERE f.facet_kind = 5 AND f.facet_value = t.id
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_ship_search_type_id
    ON mv_ship_search (type_id);

CREATE INDEX IF NOT EXISTS idx_mv_ship_search_name_trgm
    ON mv_ship_search USING gin (name gin_trgm_ops);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_weapon_search AS
SELECT t.id AS type_id, t.meta_group_id, t.name
FROM types t
WHERE EXISTS (
    SELECT 1 FROM kill_facets f
    WHERE f.facet_kind = 6 AND f.role = 1 AND f.facet_value = t.id
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_weapon_search_type_id
    ON mv_weapon_search (type_id);

CREATE INDEX IF NOT EXISTS idx_mv_weapon_search_name_trgm
    ON mv_weapon_search USING gin (name gin_trgm_ops);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_alliance_member_count AS
SELECT
    alliance_id,
    COALESCE(SUM(member_count), 0) AS member_count
FROM corporations
WHERE alliance_id IS NOT NULL
GROUP BY alliance_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_alliance_member_count_alliance
    ON mv_alliance_member_count (alliance_id);

CREATE TABLE IF NOT EXISTS entity_kills_daily (
    facet_kind  SMALLINT NOT NULL,
    role        SMALLINT NOT NULL,
    day         DATE     NOT NULL,
    facet_value BIGINT   NOT NULL,
    kill_count  INTEGER  NOT NULL,
    PRIMARY KEY (facet_kind, role, day, facet_value) INCLUDE (kill_count)
);

ALTER TABLE entity_kills_daily SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.005
);

CREATE TABLE IF NOT EXISTS entity_leaderboard (
    facet_kind  SMALLINT    NOT NULL,
    role        SMALLINT    NOT NULL,
    window_key  TEXT        NOT NULL,
    scope       TEXT        NOT NULL,
    rank        SMALLINT    NOT NULL,
    facet_value BIGINT      NOT NULL,
    kill_count  BIGINT      NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (facet_kind, role, window_key, scope, rank)
);

CREATE TABLE IF NOT EXISTS rollup_state (
    name      TEXT        PRIMARY KEY,
    watermark TIMESTAMPTZ NOT NULL
);
