CREATE TABLE IF NOT EXISTS omicron.public.fm_problem_request (
    id UUID NOT NULL,
    sitrep_id UUID NOT NULL,
    requested_sitrep_id UUID NOT NULL,
    comment TEXT NOT NULL,

    PRIMARY KEY (sitrep_id, id)
);
