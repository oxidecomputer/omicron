-- The history of current sitreps.
--
-- The sitrep with the highest `version` in this table is the current sitrep.
CREATE TABLE IF NOT EXISTS omicron.public.fm_sitrep_history (
    -- Monotonically increasing version for all FM sitreps.
    version INT8 PRIMARY KEY,

    -- Effectively a foreign key into the `fm_sitrep` table, but may
    -- reference a fm_sitrep that has been deleted (if this sitrep is
    --  no longer current; the current sitrep must not be deleted).
    sitrep_id UUID NOT NULL,

    -- Timestamp for when this sitrep was made current.
    time_made_current TIMESTAMPTZ NOT NULL
);
