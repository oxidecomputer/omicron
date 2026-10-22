CREATE TABLE IF NOT EXISTS omicron.public.fm_active_problem_alert (
    problem_id UUID NOT NULL,
    alert_id UUID NOT NULL,
    sitrep_id UUID NOT NULL,
    created_sitrep_id UUID NOT NULL,
    revision INT8 NOT NULL,

    comment TEXT NOT NULL,

    CONSTRAINT revision_is_non_zero CHECK (revision > 0),

    PRIMARY KEY (sitrep_id, problem_id, alert_id)
);
