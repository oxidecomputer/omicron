ALTER TABLE omicron.public.saga
    ADD CONSTRAINT IF NOT EXISTS not_abandoned_requires_no_metadata CHECK (
        saga_state = 'abandoned'
        OR (
            abandon_time IS NULL
            AND abandon_reason IS NULL
            AND abandon_comment IS NULL
        )
    );
