CREATE TABLE IF NOT EXISTS omicron.public.fm_fact_smf_service (
    -- Stable UUID for this fact across sitreps.
    id UUID NOT NULL,
    -- Sitrep this row belongs to.
    sitrep_id UUID NOT NULL,
    -- UUID of the case this fact attaches to.
    case_id UUID NOT NULL,
    -- UUID of the sitrep in which this fact was first added. Preserved
    -- unchanged when the fact is carried forward into a child sitrep.
    -- Debug-only.
    created_sitrep_id UUID NOT NULL,
    -- Free-form, debug-only comment.
    comment TEXT NOT NULL,

    -- The SMF service instance this fact is about. Common to every kind of
    -- SMF service fact (the case is keyed by it), so it is always present
    -- regardless of `kind`.
    sled_id UUID NOT NULL,
    zone_name TEXT NOT NULL,
    fmri TEXT NOT NULL,

    -- Which SMF service fact this row represents.
    kind omicron.public.fm_fact_smf_service_kind NOT NULL,

    -- The inventory collection in which the service was observed, and when
    -- the sled agent observed it.
    observed_in_inv UUID NOT NULL,
    time_observed TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (sitrep_id, id)
);
