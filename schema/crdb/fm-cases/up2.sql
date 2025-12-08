CREATE TABLE IF NOT EXISTS omicron.public.fm_case (
    -- Case UUID
    id UUID NOT NULL,
    -- UUID of the sitrep in which the case had this state.
    sitrep_id UUID NOT NULL,

    de omicron.public.diagnosis_engine NOT NULL,

    -- UUID of the sitrep in which the case was created.
    created_sitrep_id UUID NOT NULL,

    -- UUID of the sitrep in which the case was closed. If this is not NULL,
    -- then the case has been closed.
    closed_sitrep_id UUID,

    comment TEXT NOT NULL,

    PRIMARY KEY (sitrep_id, id)
);
