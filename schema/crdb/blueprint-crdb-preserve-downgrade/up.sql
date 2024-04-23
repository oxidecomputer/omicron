ALTER TABLE omicron.public.blueprint
    ADD COLUMN IF NOT EXISTS cockroachdb_preserve_downgrade TEXT;
