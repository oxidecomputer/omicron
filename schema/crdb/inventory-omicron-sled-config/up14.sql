ALTER TABLE omicron.public.inv_sled_agent
    ADD CONSTRAINT IF NOT EXISTS
    reconciler_status_sled_config_present_if_running CHECK (
        (reconciler_status_kind = 'running'
            AND reconciler_status_sled_config IS NOT NULL)
        OR
        (reconciler_status_kind != 'running'
            AND reconciler_status_sled_config IS NULL)
    );
