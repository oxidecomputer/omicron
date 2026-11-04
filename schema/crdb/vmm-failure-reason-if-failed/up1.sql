ALTER TABLE omicron.public.vmm
    ADD CONSTRAINT IF NOT EXISTS failure_reason_if_failed CHECK (
        state != 'failed' OR failure_reason IS NOT NULL
    );
