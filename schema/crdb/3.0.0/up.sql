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
    ADD COLUMN IF NOT EXISTS project_id UUID;

COMMIT;
