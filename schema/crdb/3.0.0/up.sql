BEGIN;

SELECT CAST(
    IF(
        (
            SELECT version = '2.0.0' and target_version = '3.0.0'
            FROM omicron.public.db_metadata WHERE singleton = true
        ),
        'true',
        'Invalid starting version for schema change'
    ) AS BOOL
);

ALTER TABLE omicron.public.ip_pool
    ADD COLUMN IF NOT EXISTS "default" BOOLEAN NOT NULL DEFAULT FALSE,

    ADD COLUMN IF NOT EXISTS silo_ID UUID,
    ADD COLUMN IF NOT EXISTS project_id UUID,
   
    -- if silo_id is null, then project_id must be null
    ADD CONSTRAINT IF NOT EXISTS project_implies_silo CHECK (
      NOT ((silo_id IS NULL) AND (project_id IS NOT NULL))
    );
COMMIT;

-- needs to be in its own transaction because of this thrilling bug
-- https://github.com/cockroachdb/cockroach/issues/83593
BEGIN;

SELECT CAST(
    IF(
        (
            SELECT version = '2.0.0' and target_version = '3.0.0'
            FROM omicron.public.db_metadata WHERE singleton = true
        ),
        'true',
        'Invalid starting version for schema change'
    ) AS BOOL
);

CREATE UNIQUE INDEX IF NOT EXISTS one_default_pool_per_scope ON omicron.public.ip_pool (
    silo_id, project_id
) WHERE
    "default" AND time_deleted IS NULL;

COMMIT;
