CREATE TABLE IF NOT EXISTS omicron.public.fm_case_active_problem (
    case_id UUID NOT NULL,
    problem_id UUID NOT NULL,
    sitrep_id UUID NOT NULL,
    created_sitrep_id UUID NOT NULL,

    comment TEXT NOT NULL,

    PRIMARY KEY (sitrep_id, case_id, problem_id)
);
