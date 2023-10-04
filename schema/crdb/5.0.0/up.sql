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
            SELECT version = '4.0.0' and target_version = '5.0.0'
            FROM omicron.public.db_metadata WHERE singleton = true
        ),
        'true',
        'Invalid starting version for schema change'
    ) AS BOOL
);

ALTER TABLE omicron.public.region_snapshot
    ADD COLUMN IF NOT EXISTS deleting BOOL NOT NULL DEFAULT false;

COMMIT;

-- Do this in two stages, otherwise you'll see:
--
-- ERROR: transaction committed but schema change aborted with error: (23502): null value in column "deleting" violates not-null constraint
-- SQLSTATE: XXA00

BEGIN;

ALTER TABLE omicron.public.region_snapshot
    ALTER COLUMN deleting DROP DEFAULT;

COMMIT;

