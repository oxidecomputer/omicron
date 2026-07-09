ALTER TABLE omicron.public.vmm
    ADD CONSTRAINT IF NOT EXISTS failure_reason_iff_failed CHECK (
        (state = 'failed' AND failure_reason IS NOT NULL)
            OR (state != 'failed' AND failure_reason IS NULL)
    );
