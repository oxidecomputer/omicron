CREATE INDEX IF NOT EXISTS lookup_saga_by_state ON omicron.public.saga (
    stale_saga_state, id
);
