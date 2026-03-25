SET LOCAL disallow_full_table_scans = off;

DELETE FROM omicron.public.subnet_pool_silo_link
WHERE subnet_pool_id NOT IN (
    SELECT id
    FROM omicron.public.subnet_pool
    WHERE time_deleted IS NULL
);

DELETE FROM omicron.public.subnet_pool_silo_link
WHERE silo_id NOT IN (
    SELECT id
    FROM omicron.public.silo
    WHERE time_deleted IS NULL
);
