-- Backfill any silo_group rows that have NULL user_provision_type.
-- Same situation as silo_user (migration 196 only backfilled non-deleted rows).

SET LOCAL disallow_full_table_scans = 'off';

-- First, backfill from the parent silo where possible.
UPDATE omicron.public.silo_group
SET user_provision_type = silo.user_provision_type
FROM omicron.public.silo
WHERE silo.id = silo_group.silo_id
  AND silo_group.user_provision_type IS NULL;

-- If the parent silo was also deleted, the JOIN above won't match.
-- Use a fallback value for any remaining NULLs. These are all
-- soft-deleted rows, so the exact value doesn't matter operationally.
UPDATE omicron.public.silo_group
SET user_provision_type = 'jit'
WHERE user_provision_type IS NULL;

ALTER TABLE omicron.public.silo_group
  ALTER COLUMN user_provision_type SET NOT NULL;
