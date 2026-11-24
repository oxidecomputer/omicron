-- All sled configs prior to this migration implicitly had the initial wicket
-- SSH auth setting (generation 1, unauthenticated).
ALTER TABLE omicron.public.inv_omicron_sled_config
    ADD COLUMN IF NOT EXISTS wicket_ssh_auth_generation INT8 NOT NULL DEFAULT 1;
