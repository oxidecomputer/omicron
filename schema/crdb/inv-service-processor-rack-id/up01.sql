-- Add the `rack_id` column to `inv_service_processor`. This is currently
-- nullable, as it will be backfilled in the next step of the migration.
ALTER TABLE omicron.public.inv_service_processor
    ADD COLUMN IF NOT EXISTS rack_id UUID;
