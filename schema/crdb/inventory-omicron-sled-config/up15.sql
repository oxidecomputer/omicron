ALTER TABLE omicron.public.inv_sled_agent
    ADD CONSTRAINT IF NOT EXISTS
    reconciler_status_timestamp_present_unless_not_yet_run CHECK (
        (reconciler_status_kind = 'not-yet-run'
            AND reconciler_status_timestamp IS NULL)
        OR
        (reconciler_status_kind != 'not-yet-run'
            AND reconciler_status_timestamp IS NOT NULL)
    );
