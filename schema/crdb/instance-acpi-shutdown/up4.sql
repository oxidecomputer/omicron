ALTER TABLE omicron.public.instance
    ADD CONSTRAINT IF NOT EXISTS shutdown_policy_valid CHECK (
        (
            (shutdown_policy_action = 'power_button')
            AND (shutdown_policy_timeout IS NOT NULL)
            AND (shutdown_policy_timeout >= INTERVAL '1' SECOND)
        ) OR (
            (shutdown_policy_action = 'hard_off')
            AND (shutdown_policy_timeout IS NULL)
        )
    )
