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
            SELECT version = '3.0.3' and target_version = '4.0.0'
            FROM omicron.public.db_metadata WHERE singleton = true
        ),
        'true',
        'Invalid starting version for schema change'
    ) AS BOOL
);

CREATE TABLE IF NOT EXISTS omicron.public.resource_lock (
    resource_id UUID PRIMARY KEY,
    lock_id UUID NOT NULL
);

COMMIT;
