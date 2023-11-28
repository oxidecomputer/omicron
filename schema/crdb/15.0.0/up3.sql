-- Drop the default column value for provision_state -- it should always be set
-- by Nexus.
ALTER TABLE omicron.public.sled
    ALTER COLUMN provision_state
    DROP DEFAULT;
