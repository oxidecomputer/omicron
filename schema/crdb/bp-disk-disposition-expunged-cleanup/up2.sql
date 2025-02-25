SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.bp_omicron_physical_disk
    SET disposition_expunged_as_of_generation = (
        SELECT generation
        FROM omicron.public.bp_sled_omicron_physical_disks
        WHERE
            blueprint_id = omicron.public.bp_omicron_physical_disk.blueprint_id
            AND sled_id = omicron.public.bp_omicron_physical_disk.sled_id
    )
    WHERE disposition = 'expunged';
