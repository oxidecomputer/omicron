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
            SELECT version = '1.0.0' and target_version = '2.0.0'
            FROM omicron.public.db_metadata WHERE singleton = true
        ),
        'true',
        'Invalid starting version for schema change'
    ) AS BOOL
);

ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS boot_on_fault BOOL NOT NULL DEFAULT false;

COMMIT;
