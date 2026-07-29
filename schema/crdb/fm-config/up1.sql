CREATE TABLE IF NOT EXISTS omicron.public.fm_config (
    version INT8 PRIMARY KEY,
    sitrep_limit INT8 NOT NULL,
    history_pruning_threshold INT8 NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    CONSTRAINT versions_are_positive CHECK (version > 0),
    CONSTRAINT sitrep_min_limit CHECK (sitrep_limit >= 5),
    CONSTRAINT history_pruning_threshold_validity CHECK (
        history_pruning_threshold >= 2 AND
        history_pruning_threshold < sitrep_limit
    )
);
