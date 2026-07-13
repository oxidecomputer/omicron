CREATE TABLE IF NOT EXISTS omicron.public.fm_config (
    version INT8 PRIMARY KEY,
    sitrep_limit INT8 NOT NULL,
    sitrep_deletion_threshold INT8 NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    CONSTRAINT versions_are_positive CHECK (version > 0),
    CONSTRAINT sitrep_min_limit CHECK (sitrep_limit >= 3),
    CONSTRAINT sitrep_deletion_threshold_validity CHECK (
        sitrep_deletion_threshold >= 2 AND
        sitrep_deletion_threshold < sitrep_limit
    )
);
