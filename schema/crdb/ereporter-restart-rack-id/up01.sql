-- Add the `rack_id` column to `ereporter_restart`. This is currently nullable,
-- as it will be backfilled in the next step of the migration.
ALTER TABLE omicron.public.ereporter_restart
    ADD COLUMN IF NOT EXISTS rack_id UUID;
