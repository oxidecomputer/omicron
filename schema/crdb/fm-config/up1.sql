CREATE TABLE IF NOT EXISTS omicron.public.fm_config (
    version INT8 PRIMARY KEY,
    comment TEXT NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    analysis_enabled BOOL,
    sitrep_limit INT8,
    history_pruning_threshold INT8,

    CONSTRAINT versions_are_positive CHECK (version > 0),
    CONSTRAINT comment_required CHECK (comment != '' AND comment != ' '),

    CONSTRAINT sitrep_limit_validity CHECK (
        sitrep_limit IS NULL OR (
            sitrep_limit >= 5 AND
            sitrep_limit <= 5000
        )
    ),
    CONSTRAINT history_pruning_threshold_validity CHECK (
        history_pruning_threshold IS NULL OR (
            history_pruning_threshold <= 5000 AND
            history_pruning_threshold >= 2
        )
    ),
    CONSTRAINT history_limit_is_less_than_sirep_limit CHECK (
        (history_pruning_threshold IS NULL OR sitrep_limit IS NULL) OR
            history_pruning_threshold < sitrep_limit
    )
);
