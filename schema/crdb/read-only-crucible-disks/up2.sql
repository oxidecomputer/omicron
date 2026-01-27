-- Now that the column has been added, drop the default, as it was only used for
-- backfilling existing data.
ALTER TABLE
    omicron.public.disk_type_crucible
ALTER COLUMN
    read_only
DROP DEFAULT;
