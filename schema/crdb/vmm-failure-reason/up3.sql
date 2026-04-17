ALTER TABLE omicron.public.vmm
    ADD CONSTRAINT IF NOT EXISTS failure_reason_iff_failed CHECK (
        (failure_reason IS NOT NULL AND state = 'failed')
            OR (failure_reason IS NULL)
    );
