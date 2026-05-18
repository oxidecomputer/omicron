CREATE TABLE IF NOT EXISTS omicron.public.fm_case_fact (
    id UUID NOT NULL,
    sitrep_id UUID NOT NULL,
    case_id UUID NOT NULL,
    payload JSONB NOT NULL,
    comment TEXT NOT NULL,
    PRIMARY KEY (sitrep_id, id)
);
