ALTER TABLE omicron.public.inv_sled_agent
    ADD CONSTRAINT IF NOT EXISTS
    wicket_ssh_auth_last_error_only_if_have_effective_gen CHECK (
        wicket_ssh_auth_last_error IS NULL
        OR wicket_ssh_auth_effective_gen IS NOT NULL
    );
