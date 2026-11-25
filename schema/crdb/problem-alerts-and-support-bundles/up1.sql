CREATE TABLE IF NOT EXISTS omicron.public.problem_alert (
    problem_id UUID NOT NULL,
    alert_id UUID NOT NULL,
    revision INT8 NOT NULL,

    CONSTRAINT revision_is_non_zero CHECK (revision > 0),

    PRIMARY KEY (problem_id, alert_id)
);
