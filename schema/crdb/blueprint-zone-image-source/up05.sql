-- Drop the default value for image_source. Unclear whether it can be done in
-- the same command as the one which adds the column, so it's best to do it
-- separately.
ALTER TABLE omicron.public.bp_omicron_zone
    ALTER COLUMN image_source DROP DEFAULT;
