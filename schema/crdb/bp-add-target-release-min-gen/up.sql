ALTER TABLE omicron.public.blueprint
    ADD COLUMN IF NOT EXISTS target_release_minimum_generation INT8;
