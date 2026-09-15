ALTER TABLE omicron.public.saga
    ADD CONSTRAINT IF NOT EXISTS abandoned_requires_metadata CHECK (
        saga_state != 'abandoned'
        OR (
            abandon_time IS NOT NULL
            AND abandon_reason IS NOT NULL
            AND abandon_comment IS NOT NULL
        )
    );
