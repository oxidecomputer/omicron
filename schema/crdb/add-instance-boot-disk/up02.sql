SET LOCAL disallow_full_table_scans = off;

 UPDATE instance SET boot_disk_id = d.id FROM (
    -- "min(id)" is incredibly gross, but because of `HAVING COUNT(*) = 1`
    -- below, this is the minimum of a single id. There just needs to be some
    -- aggregate function used to select the non-aggregated `id` from the group.
    SELECT attach_instance_id, min(id) as id
    FROM disk
    WHERE disk_state = 'attached' AND attach_instance_id IS NOT NULL
    GROUP BY attach_instance_id
    HAVING COUNT(*) = 1
) d
WHERE instance.id = d.attach_instance_id;
