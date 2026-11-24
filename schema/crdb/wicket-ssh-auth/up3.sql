-- Existing collections predate this status; NULL (which we interpret as "no
-- switch zone") is the only value available for them, even for scrimlets.
ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS wicket_ssh_auth_effective_gen INT8,
    ADD COLUMN IF NOT EXISTS wicket_ssh_auth_last_error TEXT;
