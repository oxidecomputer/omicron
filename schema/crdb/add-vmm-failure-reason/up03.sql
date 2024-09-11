ALTER TABLE omicron.public.vmm
    ADD CONSTRAINT IF NOT EXISTS failure_reason_only_if_failed
    CHECK (
        ((state != 'failed') AND (failure_reason IS NULL)) OR
        (state = 'failed')
    );
