CREATE INDEX IF NOT EXISTS lookup_bundle_by_state ON omicron.public.support_bundle (
    state
) WHERE state = 'failing' OR state = 'destroying';

