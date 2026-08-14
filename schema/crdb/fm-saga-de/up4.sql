CREATE TABLE IF NOT EXISTS omicron.public.fm_fact_saga (
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

    -- The saga this fact is about. Common to every kind of saga fact (the
    -- case is keyed by it), so it is always present regardless of `kind`.
    --
    -- Fact payloads carry only the fields that define the condition; data
    -- that merely describes the saga (e.g., its name) is looked up from the
    -- saga table when a case is acted on.
    saga_id UUID NOT NULL,

    -- Which saga fact this row represents. The columns below are populated
    -- according to this discriminant (see the CHECK constraint).
    kind omicron.public.fm_fact_saga_kind NOT NULL,

    -- Columns for a 'not_progressing' fact. NULL for any other kind.
    saga_state omicron.public.saga_state,
    last_event_time TIMESTAMPTZ,

    -- Columns for an 'owner_not_current_generation' fact. NULL for any other
    -- kind.
    current_sec UUID,
    orphan_reason omicron.public.fm_fact_saga_orphan_reason,

    PRIMARY KEY (sitrep_id, id),

    -- Each kind's constraint checks only that its own columns are present,
    -- not that others are NULL, so future kinds may share columns.
    CONSTRAINT not_progressing_columns_present CHECK (
        kind != 'not_progressing' OR (
            saga_state IN ('running', 'unwinding')
            AND last_event_time IS NOT NULL
        )
    ),
    CONSTRAINT owner_not_current_generation_columns_present CHECK (
        kind != 'owner_not_current_generation' OR (
            current_sec IS NOT NULL
            AND orphan_reason IS NOT NULL
        )
    )
);
