ALTER TABLE omicron.public.blueprint
    ADD COLUMN IF NOT EXISTS cockroachdb_fingerprint TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS cockroachdb_setting_preserve_downgrade TEXT;
