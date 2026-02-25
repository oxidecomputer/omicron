CREATE TABLE IF NOT EXISTS omicron.public.support_bundle_config (
    -- Singleton pattern: only one row allowed
    singleton BOOL PRIMARY KEY DEFAULT TRUE CHECK (singleton = TRUE),

    -- Percentage (0-100) of total datasets to keep free for new allocations.
    -- Calculated as CEIL(total_datasets * target_free_percent / 100).
    -- Example: 10% of 100 datasets = 10 free, 10% of 5 datasets = 1 free.
    target_free_percent INT8 NOT NULL
        CHECK (target_free_percent >= 0 AND target_free_percent <= 100),

    -- Percentage (0-100) of total datasets to retain as bundles (minimum).
    -- Calculated as CEIL(total_datasets * min_keep_percent / 100).
    -- Prevents aggressive cleanup on small systems.
    min_keep_percent INT8 NOT NULL
        CHECK (min_keep_percent >= 0 AND min_keep_percent <= 100),

    time_modified TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
