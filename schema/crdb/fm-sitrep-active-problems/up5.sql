CREATE TABLE IF NOT EXISTS omicron.public.fm_active_problem_revision (
    problem_id UUID NOT NULL,
    revision INT8 NOT NULL,

    sitrep_id UUID NOT NULL,
    created_sitrep_id UUID NOT NULL,

    time_resolved TIMESTAMPTZ,

    code INT8 NOT NULL,

    summary STRING NOT NULL,
    description STRING NOT NULL,
    action_required STRING NOT NULL,

    comment TEXT NOT NULL,

    CONSTRAINT revision_is_non_zero CHECK (revision > 0),
    CONSTRAINT problem_code_is_non_negative CHECK (code >= 0),

    PRIMARY KEY (sitrep_id, problem_id, revision)
);
