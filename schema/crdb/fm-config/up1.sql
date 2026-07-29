CREATE TABLE IF NOT EXISTS omicron.public.fm_config (
    version INT8 PRIMARY KEY,
    comment TEXT NOT NULL,
    analysis_enabled BOOL NOT NULL,
    sitrep_limit INT8 NOT NULL,
    history_pruning_threshold INT8 NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    CONSTRAINT versions_are_positive CHECK (version > 0),
    CONSTRAINT comment_required CHECK (comment != '' AND comment != ' '),
    CONSTRAINT sitrep_min_limit CHECK (sitrep_limit >= 5),
    CONSTRAINT history_pruning_threshold_validity CHECK (
        history_pruning_threshold >= 2 AND
        history_pruning_threshold < sitrep_limit
    )
);
