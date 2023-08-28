-- CRDB documentation recommends the following:
--  "Execute schema changes either as single statements (as an implicit transaction),
--   or in an explicit transaction consisting of the single schema change statement."
--
-- For each schema change, we transactionally:
-- 1. Check the current version
-- 2. Apply the idempotent update

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
    ADD COLUMN IF NOT EXISTS silo_ID UUID,
    ADD COLUMN IF NOT EXISTS project_id UUID,
    
    -- if silo_id is null, then project_id must be null
    ADD CONSTRAINT IF NOT EXISTS project_implies_silo CHECK (
      NOT ((silo_id IS NULL) AND (project_id IS NOT NULL))
    ),

    -- if internal = true, non-null silo_id and project_id are not allowed 
    ADD CONSTRAINT IF NOT EXISTS internal_pools_have_null_silo_and_project CHECK (
       NOT (INTERNAL AND ((silo_id IS NOT NULL) OR (project_id IS NOT NULL)))
    );

COMMIT;
