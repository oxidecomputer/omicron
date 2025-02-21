SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.bp_omicron_zone
    SET disposition_expunged_as_of_generation = (
        SELECT generation
        FROM omicron.public.bp_sled_omicron_zones
        WHERE
            blueprint_id = omicron.public.bp_omicron_zone.blueprint_id
            AND sled_id = omicron.public.bp_omicron_zone.sled_id
    )
    WHERE disposition = 'expunged';
