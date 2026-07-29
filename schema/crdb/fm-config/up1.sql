CREATE TABLE IF NOT EXISTS omicron.public.fm_config (
    version INT8 PRIMARY KEY,
    comment TEXT NOT NULL,
    analysis_enabled BOOL NOT NULL,
    sitrep_limit INT8 NOT NULL,
    history_pruning_threshold INT8 NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    CONSTRAINT versions_are_positive CHECK (version > 0),
    CONSTRAINT comment_required CHECK (comment != '' AND comment != ' '),

    CONSTRAINT min_sitrep_limit CHECK (sitrep_limit >= 5),
    CONSTRAINT max_sitrep_limit CHECK (sitrep_limit <= 5000),

    CONSTRAINT min_history_pruning_threshold CHECK (
        history_pruning_threshold >= 2
    ),
    CONSTRAINT max_history_pruning_threshold CHECK (
        history_pruning_threshold <= 5000
    ),
    CONSTRAINT history_limit_is_less_than_sirep_limit CHECK (
        history_pruning_threshold < sitrep_limit
    )
);
