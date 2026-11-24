-- All blueprints prior to this migration implicitly had the initial wicket SSH
-- auth setting (generation 1, unauthenticated) for every sled.
ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS wicket_ssh_auth_generation INT8 NOT NULL DEFAULT 1;
