CREATE INDEX IF NOT EXISTS lookup_saga_by_state ON omicron.public.saga (
    saga_state, id
);
