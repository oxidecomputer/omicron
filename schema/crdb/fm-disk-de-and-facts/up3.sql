CREATE TABLE IF NOT EXISTS omicron.public.fm_fact_physical_disk (
    -- Stable UUID for this fact across sitreps.
    id UUID NOT NULL,
    -- Sitrep this row belongs to.
    sitrep_id UUID NOT NULL,
    -- UUID of the case this fact attaches to.
    case_id UUID NOT NULL,
    -- UUID of the sitrep in which this fact was first added. Preserved
    -- unchanged when the fact is carried forward into a child sitrep, so
    -- this can be used to tell at a glance how long a fact has been
    -- attached to its case. Debug-only.
    created_sitrep_id UUID NOT NULL,
    -- Free-form, debug-only comment.
    comment TEXT NOT NULL,

    -- The physical disk this fact is about. Common to every kind of
    -- physical-disk fact (the case is keyed by it), so it is always present
    -- regardless of `kind`.
    physical_disk_id UUID NOT NULL,

    -- Which physical-disk fact this row represents. The columns below are
    -- populated according to this discriminant (see the CHECK constraint).
    kind omicron.public.fm_fact_physical_disk_kind NOT NULL,

    -- Columns for a 'zpool_unhealthy' fact. NULL for any other kind.
    zpool_id UUID,
    last_seen_health omicron.public.inv_zpool_health,
    observed_in_inv UUID,
    time_observed TIMESTAMPTZ,

    PRIMARY KEY (sitrep_id, id),

    -- Each variant validates that the columns it expects are present.
    -- Future variants should add their own constraint like this one,
    -- leaving existing constraints untouched.
    CONSTRAINT zpool_unhealthy_columns_present CHECK (
        kind != 'zpool_unhealthy' OR (
            zpool_id IS NOT NULL
            AND last_seen_health IS NOT NULL
            AND observed_in_inv IS NOT NULL
            AND time_observed IS NOT NULL
        )
    )
);
