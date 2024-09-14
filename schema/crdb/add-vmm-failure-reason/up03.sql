ALTER TABLE omicron.public.vmm
    ADD CONSTRAINT IF NOT EXISTS failure_reason_only_if_failed
    CHECK (
        ((state != 'failed') AND (failure_reason IS NULL)) OR
        (state = 'failed') OR
        /*
         * Destroyed is the only state that a Failed VMM may transition to,
         * so allow the failure reason to remain when transitioning from Failed
         * to Destroyed.
         */
        (state = 'destroyed')
    );
