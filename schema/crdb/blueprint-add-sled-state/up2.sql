set local disallow_full_table_scans = off;

-- At this point in history, all sleds are considered active and this table only
-- exists to support transitioning active-but-expunged sleds to
-- 'decommissioned'. We'll fill in this table for all historical blueprints by
-- inserting rows for every sled for which a given blueprint had a zone config
-- with the state set to 'active'.
INSERT INTO bp_sled_state (
    SELECT DISTINCT
        blueprint_id,
        sled_id,
        'active'::sled_state
    FROM bp_sled_omicron_zones
);
