BEGIN;

SELECT CAST(
    IF(
        (
            SELECT version = '3.0.0' and target_version = '3.0.1'
            FROM omicron.public.db_metadata WHERE singleton = true
        ),
        'true',
        'Invalid starting version for schema change'
    ) AS BOOL
);

ALTER TABLE omicron.public.ip_pool
    ADD COLUMN IF NOT EXISTS is_default BOOLEAN NOT NULL DEFAULT FALSE,
    DROP CONSTRAINT IF EXISTS internal_pools_have_null_silo_and_project;

COMMIT;

-- needs to be in its own transaction because of this thrilling bug
-- https://github.com/cockroachdb/cockroach/issues/83593
BEGIN;

SELECT CAST(
    IF(
        (
            SELECT version = '3.0.0' and target_version = '3.0.1'
            FROM omicron.public.db_metadata WHERE singleton = true
        ),
        'true',
        'Invalid starting version for schema change'
    ) AS BOOL
);

CREATE UNIQUE INDEX IF NOT EXISTS one_default_pool_per_scope ON omicron.public.ip_pool (
    COALESCE(silo_id, '00000000-0000-0000-0000-000000000000'::uuid), 
    COALESCE(project_id, '00000000-0000-0000-0000-000000000000'::uuid) 
) WHERE
    is_default = true AND time_deleted IS NULL;

COMMIT;
