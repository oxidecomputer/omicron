CREATE TABLE IF NOT EXISTS omicron.public.fm_active_problem (
    id UUID NOT NULL,
    sitrep_id UUID NOT NULL,
    requested_sitrep_id UUID NOT NULL,

    PRIMARY KEY (sitrep_id, id)
);
