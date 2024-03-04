-- This is a follow-up to the previous migration, done separately to ensure
-- that the updated values for sled_policy are committed before the
-- provision_state column is dropped.

ALTER TABLE omicron.public.sled
    DROP COLUMN IF EXISTS provision_state,
    ALTER COLUMN sled_policy SET NOT NULL,
    ALTER COLUMN sled_state DROP DEFAULT;
