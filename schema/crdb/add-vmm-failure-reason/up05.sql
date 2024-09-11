ALTER TABLE omicron.public.instance
    ADD CONSTRAINT IF NOT EXISTS failure_reason_only_if_failed
    CHECK (
        ((state != 'failed') AND (last_failure_reason IS NULL)) OR
        (state = 'failed')
    );
