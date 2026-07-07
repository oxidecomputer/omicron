-- Backfill any silo_user rows that have NULL user_provision_type.
-- The original migration (196) only backfilled non-deleted rows,
-- leaving already-deleted rows with NULL.

SET LOCAL disallow_full_table_scans = 'off';

-- First, backfill from the parent silo where possible.
UPDATE omicron.public.silo_user
SET user_provision_type = silo.user_provision_type
FROM omicron.public.silo
WHERE silo.id = silo_user.silo_id
  AND silo_user.user_provision_type IS NULL;

-- If the parent silo was also deleted, the JOIN above won't match.
-- Use a fallback value for any remaining NULLs. These are all
-- soft-deleted rows, so the exact value doesn't matter operationally.
UPDATE omicron.public.silo_user
SET user_provision_type = 'jit'
WHERE user_provision_type IS NULL;

ALTER TABLE omicron.public.silo_user
  ALTER COLUMN user_provision_type SET NOT NULL;
