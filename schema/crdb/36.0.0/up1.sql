-- Add the "internal_dns_version" column to the "blueprint" table.
-- This query will end up setting the internal DNS version for any existing
-- blueprints to 1.  This is always safe because it's the smallest possible
-- value and if a value is too small, the end result is simply needing to
-- regenerate the blueprint in order to be able to execute it.  (On the other
-- hand, using a value that's too large could cause corruption.)
ALTER TABLE omicron.public.blueprint
    ADD COLUMN IF NOT EXISTS internal_dns_version INT8 NOT NULL DEFAULT 1;
