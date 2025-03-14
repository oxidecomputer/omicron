SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.bp_sled_metadata AS bp_metadata
    SET sled_agent_generation = (
        SELECT generation FROM omicron.public.bp_sled_omicron_zones AS bp_zones
        WHERE
            bp_zones.blueprint_id = bp_metadata.blueprint_id
            AND bp_zones.sled_id = bp_metadata.sled_id
    );
