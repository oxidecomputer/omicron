CREATE INDEX IF NOT EXISTS
    lookup_bundle_by_state_and_creation
ON omicron.public.support_bundle (
    state,
    time_created
);
