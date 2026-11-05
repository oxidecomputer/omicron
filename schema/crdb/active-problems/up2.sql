CREATE TABLE IF NOT EXISTS omicron.public.active_problem_revision (
    problem_id UUID NOT NULL,
    revision INT8 NOT NULL,

    time_created TIMESTAMPTZ NOT NULL,

    time_resolved TIMESTAMPTZ,

    code INT8 NOT NULL,

    summary STRING NOT NULL,
    description STRING NOT NULL,
    action_required STRING NOT NULL,

    CONSTRAINT revision_is_non_zero CHECK (revision > 0),
    CONSTRAINT problem_code_is_non_zero CHECK (code > 0),

    PRIMARY KEY (problem_id, revision)
);
