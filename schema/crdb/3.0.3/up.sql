BEGIN;

SELECT CAST(
    IF(
        (
            SELECT version = '3.0.2' and target_version = '3.0.3'
            FROM omicron.public.db_metadata WHERE singleton = true
        ),
        'true',
        'Invalid starting version for schema change'
    ) AS BOOL
);

ALTER TABLE omicron.public.ip_pool
    DROP COLUMN IF EXISTS internal;

COMMIT;
