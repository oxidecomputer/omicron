CREATE TABLE IF NOT EXISTS omicron.public.fm_fact_certificate (
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

    -- The silo this fact is about. Common to every kind of certificate fact
    -- (the case is keyed by it), so it is always present regardless of
    -- `kind`.
    --
    -- Fact payloads carry only the fields that define the condition; data
    -- that merely describes the silo or certificate (e.g., their names) is
    -- looked up from the silo and certificate tables when a case is acted on.
    silo_id UUID NOT NULL,

    -- Which certificate fact this row represents. The columns below are
    -- populated according to this discriminant (see the CHECK constraints).
    kind omicron.public.fm_fact_certificate_kind NOT NULL,

    -- Columns shared by the 'best_certificate_expiring' and
    -- 'best_certificate_expired' kinds: the silo's best certificate (latest
    -- leaf `not_after`) when the fact was recorded, and that `not_after`.
    certificate_id UUID,
    not_after TIMESTAMPTZ,

    PRIMARY KEY (sitrep_id, id),

    -- Each kind's constraint checks only that its own columns are present,
    -- not that others are NULL, so future kinds may share columns.
    CONSTRAINT best_certificate_expiring_columns_present CHECK (
        kind != 'best_certificate_expiring' OR (
            certificate_id IS NOT NULL
            AND not_after IS NOT NULL
        )
    ),
    CONSTRAINT best_certificate_expired_columns_present CHECK (
        kind != 'best_certificate_expired' OR (
            certificate_id IS NOT NULL
            AND not_after IS NOT NULL
        )
    )
);
