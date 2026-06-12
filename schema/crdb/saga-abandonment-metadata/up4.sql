ALTER TABLE omicron.public.saga
    ADD CONSTRAINT IF NOT EXISTS abandoned_requires_metadata CHECK (
        saga_state != 'abandoned'
        OR (
            time_abandoned IS NOT NULL
            AND reason_abandoned IS NOT NULL
            AND abandon_information IS NOT NULL
        )
    );
