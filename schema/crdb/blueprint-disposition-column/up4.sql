-- Drop the default for the disposition now that in_service is set.
ALTER TABLE omicron.public.bp_omicron_zone
    ALTER COLUMN disposition DROP DEFAULT;
