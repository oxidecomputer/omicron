CREATE TABLE IF NOT EXISTS omicron.public.problem_support_bundle (
    problem_id UUID NOT NULL,
    support_bundle_id UUID NOT NULL,
    revision INT8 NOT NULL,

    CONSTRAINT revision_is_non_zero CHECK (revision > 0),

    PRIMARY KEY (problem_id, support_bundle_id)
);
