CREATE TABLE IF NOT EXISTS omicron.public.support_bundle (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    reason_for_creation TEXT NOT NULL,
    reason_for_failure TEXT,
    state omicron.public.support_bundle_state NOT NULL,
    zpool_id UUID NOT NULL,
    dataset_id UUID NOT NULL,

    -- The Nexus which is in charge of collecting the support bundle,
    -- and later managing its storage.
    assigned_nexus UUID
);

